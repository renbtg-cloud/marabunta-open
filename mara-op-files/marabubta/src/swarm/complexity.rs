// Marabunta - Licensed under the MIT License.
//! Complexity and management layer foundation types.
//!
//! This module provides the core types that every management-layer module
//! imports: complexity styles, event severities, concern domains, managed
//! actions, alert filters, management visions, and assessment logic.
//!
//! The complexity model recognizes that different operational concerns
//! require different levels of detail and interaction modes. A NOC operator
//! watching a dashboard needs glanceable summaries, while a security auditor
//! investigating an incident needs investigative deep-dives. Visions capture
//! these persona-specific configurations and can be switched dynamically.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ============================================================================
// ComplexityStyle
// ============================================================================

/// How information should be presented to the operator.
///
/// Each style represents a different level of detail and interaction
/// complexity. Visions combine one or more styles to define the operator
/// experience for a given persona.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ComplexityStyle {
    /// At-a-glance status indicators (green/amber/red, counts).
    Glanceable,
    /// Scrollable lists and tables with drill-down.
    Browseable,
    /// Time-series charts and real-time metrics.
    Observable,
    /// Multi-step workflows with confirmation gates.
    Orchestrated,
    /// Deep-dive analysis with cross-referenced data.
    Investigative,
    /// Automation-ready views with CLI/API emphasis.
    Scriptable,
}

impl ComplexityStyle {
    /// Returns all complexity style variants.
    pub fn all() -> Vec<ComplexityStyle> {
        vec![
            ComplexityStyle::Glanceable,
            ComplexityStyle::Browseable,
            ComplexityStyle::Observable,
            ComplexityStyle::Orchestrated,
            ComplexityStyle::Investigative,
            ComplexityStyle::Scriptable,
        ]
    }

    /// Human-readable description of this style.
    pub fn description(&self) -> &'static str {
        match self {
            ComplexityStyle::Glanceable => {
                "At-a-glance status indicators: green/amber/red lights and summary counts"
            }
            ComplexityStyle::Browseable => {
                "Scrollable lists and tables with drill-down into individual items"
            }
            ComplexityStyle::Observable => {
                "Time-series charts, real-time metrics, and streaming dashboards"
            }
            ComplexityStyle::Orchestrated => {
                "Multi-step workflows with confirmation gates and rollback support"
            }
            ComplexityStyle::Investigative => {
                "Deep-dive analysis with cross-referenced data and correlation tools"
            }
            ComplexityStyle::Scriptable => {
                "Automation-ready views emphasizing CLI, API, and machine-readable output"
            }
        }
    }
}

impl fmt::Display for ComplexityStyle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ComplexityStyle::Glanceable => "glanceable",
            ComplexityStyle::Browseable => "browseable",
            ComplexityStyle::Observable => "observable",
            ComplexityStyle::Orchestrated => "orchestrated",
            ComplexityStyle::Investigative => "investigative",
            ComplexityStyle::Scriptable => "scriptable",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for ComplexityStyle {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "glanceable" => Ok(ComplexityStyle::Glanceable),
            "browseable" => Ok(ComplexityStyle::Browseable),
            "observable" => Ok(ComplexityStyle::Observable),
            "orchestrated" => Ok(ComplexityStyle::Orchestrated),
            "investigative" => Ok(ComplexityStyle::Investigative),
            "scriptable" => Ok(ComplexityStyle::Scriptable),
            other => Err(format!("unknown complexity style: '{}'", other)),
        }
    }
}

// ============================================================================
// EventSeverity
// ============================================================================

/// Severity level for management events.
///
/// Ordered from least severe (Info) to most severe (Critical). The numeric
/// discriminant enables direct comparison via `is_at_least`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum EventSeverity {
    /// Informational event, normal operation.
    Info = 0,
    /// Noteworthy condition that may warrant attention.
    Notice = 1,
    /// Warning: something unusual that may need attention.
    Warning = 2,
    /// Error: a failure that affects functionality.
    Error = 3,
    /// Critical: a severe failure requiring immediate action.
    Critical = 4,
}

impl EventSeverity {
    /// Returns true if this severity is at least as severe as `other`.
    pub fn is_at_least(&self, other: &EventSeverity) -> bool {
        (*self as u8) >= (*other as u8)
    }

    /// ANSI color code for terminal rendering.
    pub fn color_code(&self) -> &'static str {
        match self {
            EventSeverity::Info => "\x1b[36m",       // cyan
            EventSeverity::Notice => "\x1b[34m",     // blue
            EventSeverity::Warning => "\x1b[33m",    // yellow
            EventSeverity::Error => "\x1b[31m",      // red
            EventSeverity::Critical => "\x1b[1;31m", // bold red
        }
    }

    /// Text label for rich-text severity rendering.
    pub fn emoji(&self) -> &'static str {
        match self {
            EventSeverity::Info => "info",
            EventSeverity::Notice => "notice",
            EventSeverity::Warning => "warning",
            EventSeverity::Error => "error",
            EventSeverity::Critical => "critical",
        }
    }
}

impl fmt::Display for EventSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            EventSeverity::Info => "info",
            EventSeverity::Notice => "notice",
            EventSeverity::Warning => "warning",
            EventSeverity::Error => "error",
            EventSeverity::Critical => "critical",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for EventSeverity {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "info" => Ok(EventSeverity::Info),
            "notice" => Ok(EventSeverity::Notice),
            "warning" | "warn" => Ok(EventSeverity::Warning),
            "error" | "err" => Ok(EventSeverity::Error),
            "critical" | "crit" => Ok(EventSeverity::Critical),
            other => Err(format!("unknown event severity: '{}'", other)),
        }
    }
}

// ============================================================================
// ConcernDomain
// ============================================================================

/// A broad operational domain that a management action or event belongs to.
///
/// Concern domains partition the management surface into orthogonal areas.
/// Each vision selects which domains are visible, and alert filters can
/// restrict events to specific domains.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ConcernDomain {
    /// Node health, liveness, failure detection, partitions.
    Health,
    /// Job execution, chunk distribution, work queues.
    Work,
    /// Data residency, blob storage, replication.
    Data,
    /// Fleet management, rolling updates, node lifecycle.
    Fleet,
    /// Authentication, authorization, policy enforcement.
    Security,
    /// Energy costs, resource budgets, capacity planning.
    Cost,
    /// Psyche facets, archetypes, personality-driven behavior.
    Psyche,
    /// Multi-swarm membranes, treaties, capacity lending.
    MultiSwarm,
}

impl ConcernDomain {
    /// Returns all concern domain variants.
    pub fn all() -> Vec<ConcernDomain> {
        vec![
            ConcernDomain::Health,
            ConcernDomain::Work,
            ConcernDomain::Data,
            ConcernDomain::Fleet,
            ConcernDomain::Security,
            ConcernDomain::Cost,
            ConcernDomain::Psyche,
            ConcernDomain::MultiSwarm,
        ]
    }

    /// Human-readable description of this domain.
    pub fn description(&self) -> &'static str {
        match self {
            ConcernDomain::Health => {
                "Node health, liveness detection, gossip connectivity, and partition status"
            }
            ConcernDomain::Work => {
                "Job execution, chunk distribution, work queues, and completion tracking"
            }
            ConcernDomain::Data => {
                "Data residency enforcement, blob storage, replication, and transfer"
            }
            ConcernDomain::Fleet => {
                "Fleet management, rolling updates, node cordon/drain, and lifecycle"
            }
            ConcernDomain::Security => {
                "Authentication, authorization, policy enforcement, and audit trails"
            }
            ConcernDomain::Cost => {
                "Energy cost estimation, resource budgets, and capacity planning"
            }
            ConcernDomain::Psyche => {
                "Psyche facets, archetypes, and personality-driven swarm behavior"
            }
            ConcernDomain::MultiSwarm => {
                "Multi-swarm membranes, treaties, capacity lending, and border crossings"
            }
        }
    }

    /// Default complexity styles that are most natural for this domain.
    pub fn default_styles(&self) -> Vec<ComplexityStyle> {
        match self {
            ConcernDomain::Health => {
                vec![ComplexityStyle::Glanceable, ComplexityStyle::Observable]
            }
            ConcernDomain::Work => {
                vec![ComplexityStyle::Browseable, ComplexityStyle::Observable]
            }
            ConcernDomain::Data => {
                vec![ComplexityStyle::Browseable, ComplexityStyle::Investigative]
            }
            ConcernDomain::Fleet => {
                vec![ComplexityStyle::Orchestrated, ComplexityStyle::Browseable]
            }
            ConcernDomain::Security => {
                vec![ComplexityStyle::Investigative, ComplexityStyle::Browseable]
            }
            ConcernDomain::Cost => {
                vec![ComplexityStyle::Observable, ComplexityStyle::Glanceable]
            }
            ConcernDomain::Psyche => {
                vec![ComplexityStyle::Observable, ComplexityStyle::Investigative]
            }
            ConcernDomain::MultiSwarm => {
                vec![ComplexityStyle::Orchestrated, ComplexityStyle::Investigative]
            }
        }
    }
}

impl fmt::Display for ConcernDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ConcernDomain::Health => "health",
            ConcernDomain::Work => "work",
            ConcernDomain::Data => "data",
            ConcernDomain::Fleet => "fleet",
            ConcernDomain::Security => "security",
            ConcernDomain::Cost => "cost",
            ConcernDomain::Psyche => "psyche",
            ConcernDomain::MultiSwarm => "multi_swarm",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for ConcernDomain {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "health" => Ok(ConcernDomain::Health),
            "work" => Ok(ConcernDomain::Work),
            "data" => Ok(ConcernDomain::Data),
            "fleet" => Ok(ConcernDomain::Fleet),
            "security" => Ok(ConcernDomain::Security),
            "cost" => Ok(ConcernDomain::Cost),
            "psyche" => Ok(ConcernDomain::Psyche),
            "multi_swarm" | "multiswarm" => Ok(ConcernDomain::MultiSwarm),
            other => Err(format!("unknown concern domain: '{}'", other)),
        }
    }
}

// ============================================================================
// ManagedAction
// ============================================================================

/// An action that can be performed through the management interface.
///
/// Each action maps to one concern domain and is either read-only
/// (safe to perform without side effects) or mutating (requires
/// confirmation and/or specific permissions).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ManagedAction {
    /// View the list of known nodes and their status.
    ViewNodes,
    /// Drain a node (stop scheduling new work, wait for in-progress chunks).
    DrainNode,
    /// Cordon a node (mark as unschedulable without draining).
    CordonNode,
    /// Un-cordon a previously cordoned node.
    UncordonNode,
    /// Quarantine a node (isolate from swarm participation).
    QuarantineNode,
    /// View the list of jobs and their status.
    ViewJobs,
    /// Submit a new job to the swarm.
    SubmitJob,
    /// Cancel a running job.
    CancelJob,
    /// Diagnose a failing or stuck job.
    DiagnoseJob,
    /// View completed job results.
    ViewJobResults,
    /// View fleet-wide overview.
    ViewFleet,
    /// Start a rolling update across the fleet.
    StartRollingUpdate,
    /// Pause an in-progress rolling update.
    PauseUpdate,
    /// Resume a paused rolling update.
    ResumeUpdate,
    /// Rollback a rolling update.
    RollbackUpdate,
    /// View active policies.
    ViewPolicies,
    /// Set or modify a policy.
    SetPolicy,
    /// Audit data residency compliance.
    AuditResidency,
    /// View API tokens.
    ViewTokens,
    /// Create a new API token.
    CreateToken,
    /// Revoke an existing API token.
    RevokeToken,
    /// View energy cost estimates and history.
    ViewEnergy,
    /// Set an energy cost schedule.
    SetEnergySchedule,
    /// View management visions.
    ViewVisions,
    /// Apply (switch to) a management vision.
    ApplyVision,
    /// View the swarm psyche state.
    ViewPsyche,
    /// Manage psyche archetypes.
    ManageArchetypes,
    /// View inter-swarm constellation state.
    ViewConstellation,
    /// Propose a agreement with another swarm.
    ProposeAgreement,
    /// Amend an existing agreement.
    AmendAgreement,
    /// Sever (tear down) the membrane to another swarm.
    SeverMembrane,
    /// Lend capacity to another swarm.
    LendCapacity,
    /// View border crossings and inter-swarm traffic.
    ViewCrossings,
    /// View recent management events.
    ViewEvents,
    /// Stream live management events.
    StreamEvents,
    /// Meta-action: view all domains (grants all View* and Stream*).
    ViewAll,
    /// Meta-action: manage everything (grants all actions).
    ManageAll,
}

impl ManagedAction {
    /// Returns true if this action is read-only (no side effects).
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            ManagedAction::ViewNodes
                | ManagedAction::ViewJobs
                | ManagedAction::ViewJobResults
                | ManagedAction::ViewFleet
                | ManagedAction::ViewPolicies
                | ManagedAction::ViewTokens
                | ManagedAction::ViewEnergy
                | ManagedAction::ViewVisions
                | ManagedAction::ViewPsyche
                | ManagedAction::ViewConstellation
                | ManagedAction::ViewCrossings
                | ManagedAction::ViewEvents
                | ManagedAction::StreamEvents
                | ManagedAction::ViewAll
        )
    }

    /// Returns the concern domain this action belongs to.
    pub fn required_domain(&self) -> ConcernDomain {
        match self {
            ManagedAction::ViewNodes
            | ManagedAction::DrainNode
            | ManagedAction::CordonNode
            | ManagedAction::UncordonNode
            | ManagedAction::QuarantineNode => ConcernDomain::Health,

            ManagedAction::ViewJobs
            | ManagedAction::SubmitJob
            | ManagedAction::CancelJob
            | ManagedAction::DiagnoseJob
            | ManagedAction::ViewJobResults => ConcernDomain::Work,

            ManagedAction::ViewFleet
            | ManagedAction::StartRollingUpdate
            | ManagedAction::PauseUpdate
            | ManagedAction::ResumeUpdate
            | ManagedAction::RollbackUpdate => ConcernDomain::Fleet,

            ManagedAction::ViewPolicies
            | ManagedAction::SetPolicy
            | ManagedAction::AuditResidency
            | ManagedAction::ViewTokens
            | ManagedAction::CreateToken
            | ManagedAction::RevokeToken => ConcernDomain::Security,

            ManagedAction::ViewEnergy
            | ManagedAction::SetEnergySchedule => ConcernDomain::Cost,

            ManagedAction::ViewVisions
            | ManagedAction::ApplyVision
            | ManagedAction::ViewPsyche
            | ManagedAction::ManageArchetypes => ConcernDomain::Fleet,

            ManagedAction::ViewConstellation
            | ManagedAction::ProposeAgreement
            | ManagedAction::AmendAgreement
            | ManagedAction::SeverMembrane
            | ManagedAction::LendCapacity
            | ManagedAction::ViewCrossings => ConcernDomain::Data,

            ManagedAction::ViewEvents
            | ManagedAction::StreamEvents => ConcernDomain::Health,

            ManagedAction::ViewAll
            | ManagedAction::ManageAll => ConcernDomain::Fleet,
        }
    }
}

impl fmt::Display for ManagedAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ManagedAction::ViewNodes => "view_nodes",
            ManagedAction::DrainNode => "drain_node",
            ManagedAction::CordonNode => "cordon_node",
            ManagedAction::UncordonNode => "uncordon_node",
            ManagedAction::QuarantineNode => "quarantine_node",
            ManagedAction::ViewJobs => "view_jobs",
            ManagedAction::SubmitJob => "submit_job",
            ManagedAction::CancelJob => "cancel_job",
            ManagedAction::DiagnoseJob => "diagnose_job",
            ManagedAction::ViewJobResults => "view_job_results",
            ManagedAction::ViewFleet => "view_fleet",
            ManagedAction::StartRollingUpdate => "start_rolling_update",
            ManagedAction::PauseUpdate => "pause_update",
            ManagedAction::ResumeUpdate => "resume_update",
            ManagedAction::RollbackUpdate => "rollback_update",
            ManagedAction::ViewPolicies => "view_policies",
            ManagedAction::SetPolicy => "set_policy",
            ManagedAction::AuditResidency => "audit_residency",
            ManagedAction::ViewTokens => "view_tokens",
            ManagedAction::CreateToken => "create_token",
            ManagedAction::RevokeToken => "revoke_token",
            ManagedAction::ViewEnergy => "view_energy",
            ManagedAction::SetEnergySchedule => "set_energy_schedule",
            ManagedAction::ViewVisions => "view_visions",
            ManagedAction::ApplyVision => "apply_vision",
            ManagedAction::ViewPsyche => "view_psyche",
            ManagedAction::ManageArchetypes => "manage_archetypes",
            ManagedAction::ViewConstellation => "view_constellation",
            ManagedAction::ProposeAgreement => "propose_agreement",
            ManagedAction::AmendAgreement => "amend_agreement",
            ManagedAction::SeverMembrane => "sever_membrane",
            ManagedAction::LendCapacity => "lend_capacity",
            ManagedAction::ViewCrossings => "view_crossings",
            ManagedAction::ViewEvents => "view_events",
            ManagedAction::StreamEvents => "stream_events",
            ManagedAction::ViewAll => "view_all",
            ManagedAction::ManageAll => "manage_all",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for ManagedAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "view_nodes" => Ok(ManagedAction::ViewNodes),
            "drain_node" => Ok(ManagedAction::DrainNode),
            "cordon_node" => Ok(ManagedAction::CordonNode),
            "uncordon_node" => Ok(ManagedAction::UncordonNode),
            "quarantine_node" => Ok(ManagedAction::QuarantineNode),
            "view_jobs" => Ok(ManagedAction::ViewJobs),
            "submit_job" => Ok(ManagedAction::SubmitJob),
            "cancel_job" => Ok(ManagedAction::CancelJob),
            "diagnose_job" => Ok(ManagedAction::DiagnoseJob),
            "view_job_results" => Ok(ManagedAction::ViewJobResults),
            "view_fleet" => Ok(ManagedAction::ViewFleet),
            "start_rolling_update" => Ok(ManagedAction::StartRollingUpdate),
            "pause_update" => Ok(ManagedAction::PauseUpdate),
            "resume_update" => Ok(ManagedAction::ResumeUpdate),
            "rollback_update" => Ok(ManagedAction::RollbackUpdate),
            "view_policies" => Ok(ManagedAction::ViewPolicies),
            "set_policy" => Ok(ManagedAction::SetPolicy),
            "audit_residency" => Ok(ManagedAction::AuditResidency),
            "view_tokens" => Ok(ManagedAction::ViewTokens),
            "create_token" => Ok(ManagedAction::CreateToken),
            "revoke_token" => Ok(ManagedAction::RevokeToken),
            "view_energy" => Ok(ManagedAction::ViewEnergy),
            "set_energy_schedule" => Ok(ManagedAction::SetEnergySchedule),
            "view_visions" => Ok(ManagedAction::ViewVisions),
            "apply_vision" => Ok(ManagedAction::ApplyVision),
            "view_psyche" => Ok(ManagedAction::ViewPsyche),
            "manage_archetypes" => Ok(ManagedAction::ManageArchetypes),
            "view_constellation" => Ok(ManagedAction::ViewConstellation),
            "propose_agreement" => Ok(ManagedAction::ProposeAgreement),
            "amend_agreement" => Ok(ManagedAction::AmendAgreement),
            "sever_membrane" => Ok(ManagedAction::SeverMembrane),
            "lend_capacity" => Ok(ManagedAction::LendCapacity),
            "view_crossings" => Ok(ManagedAction::ViewCrossings),
            "view_events" => Ok(ManagedAction::ViewEvents),
            "stream_events" => Ok(ManagedAction::StreamEvents),
            "view_all" => Ok(ManagedAction::ViewAll),
            "manage_all" => Ok(ManagedAction::ManageAll),
            other => Err(format!("unknown managed action: '{}'", other)),
        }
    }
}

// ============================================================================
// ComplexityHint (enum for event classification)
// ============================================================================

/// Hint for UI rendering complexity of individual events.
///
/// Tells dashboards and CLI tools how much detail to show for an event.
/// This is distinct from [`ComplexityStyle`] which describes the overall
/// interaction mode of a vision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComplexityHint {
    /// Simple single-line event, suitable for compact views.
    Simple,
    /// Moderate detail, may include a few key-value pairs.
    Moderate,
    /// Detailed event with nested structure, graphs, or large payloads.
    Detailed,
    /// Expert-level event with raw diagnostics.
    Expert,
}

impl fmt::Display for ComplexityHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ComplexityHint::Simple => "simple",
            ComplexityHint::Moderate => "moderate",
            ComplexityHint::Detailed => "detailed",
            ComplexityHint::Expert => "expert",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for ComplexityHint {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simple" => Ok(ComplexityHint::Simple),
            "moderate" => Ok(ComplexityHint::Moderate),
            "detailed" => Ok(ComplexityHint::Detailed),
            "expert" => Ok(ComplexityHint::Expert),
            other => Err(format!("unknown complexity hint: '{}'", other)),
        }
    }
}

// ============================================================================
// ComplexityAssessment (rich assessment struct)
// ============================================================================

/// A rich assessment of the complexity level that a piece of information or
/// event should be rendered at.
///
/// Complexity assessments are produced by the [`ComplexityAssessor`] to guide
/// the vision engine in deciding how to present information to the operator.
/// They carry more context than a simple [`ComplexityHint`] enum variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexityAssessment {
    /// Which styles are appropriate for this information.
    pub styles: Vec<ComplexityStyle>,
    /// Human-readable reason for the complexity assessment.
    pub reason: String,
    /// Optional suggested action the operator might take.
    pub suggested_action: Option<String>,
    /// Whether this assessment was automatically assessed (true) or manually set (false).
    pub auto_assessed: bool,
    /// Optional urgency level.
    pub urgency: Option<EventSeverity>,
}

impl Default for ComplexityAssessment {
    fn default() -> Self {
        Self {
            styles: vec![ComplexityStyle::Glanceable],
            reason: String::new(),
            auto_assessed: true,
            suggested_action: None,
            urgency: None,
        }
    }
}

impl ComplexityAssessment {
    /// Create a simple assessment for routine, low-complexity information.
    pub fn simple(reason: impl Into<String>) -> Self {
        Self {
            styles: vec![ComplexityStyle::Glanceable],
            reason: reason.into(),
            auto_assessed: true,
            suggested_action: None,
            urgency: None,
        }
    }

    /// Create an assessment indicating the information needs investigation.
    pub fn needs_investigation(reason: impl Into<String>) -> Self {
        Self {
            styles: vec![ComplexityStyle::Investigative],
            reason: reason.into(),
            auto_assessed: true,
            suggested_action: None,
            urgency: Some(EventSeverity::Warning),
        }
    }

    /// Create an assessment indicating the information needs orchestrated action.
    pub fn needs_orchestration(reason: impl Into<String>) -> Self {
        Self {
            styles: vec![ComplexityStyle::Orchestrated],
            reason: reason.into(),
            auto_assessed: true,
            suggested_action: None,
            urgency: Some(EventSeverity::Warning),
        }
    }

    /// Create an assessment with multiple applicable styles.
    pub fn multi(styles: Vec<ComplexityStyle>, reason: impl Into<String>) -> Self {
        Self {
            styles,
            reason: reason.into(),
            auto_assessed: true,
            suggested_action: None,
            urgency: None,
        }
    }
}

// ============================================================================
// AlertFilter
// ============================================================================

/// Filters that determine which management events are visible.
///
/// An alert filter matches events by domain, severity, complexity style,
/// keyword presence, and archetype exclusion. All criteria that are set
/// must match for the event to pass the filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertFilter {
    /// If set, only events from these domains pass.
    pub domains: Option<Vec<ConcernDomain>>,
    /// If set, only events at or above this severity pass.
    pub min_severity: Option<EventSeverity>,
    /// If set, only events whose complexity hint includes one of these styles pass.
    pub styles: Option<Vec<ComplexityStyle>>,
    /// Events tagged with any of these archetypes are excluded.
    #[serde(default)]
    pub exclude_archetypes: Vec<String>,
    /// If non-empty, at least one keyword must appear in the event summary.
    #[serde(default)]
    pub include_keywords: Vec<String>,
}

impl AlertFilter {
    /// A filter that matches all events.
    pub fn default_all() -> Self {
        Self {
            domains: None,
            min_severity: None,
            styles: None,
            exclude_archetypes: Vec::new(),
            include_keywords: Vec::new(),
        }
    }

    /// A filter that passes only warnings and above.
    pub fn warnings_and_above() -> Self {
        Self {
            domains: None,
            min_severity: Some(EventSeverity::Warning),
            styles: None,
            exclude_archetypes: Vec::new(),
            include_keywords: Vec::new(),
        }
    }

    /// Check if an event matches this filter.
    ///
    /// All criteria that are set must match. A `None` criterion always passes.
    ///
    /// - `domain`: the event's concern domain
    /// - `severity`: the event's severity
    /// - `style`: the event's primary complexity style (if any)
    /// - `summary`: the event's summary text
    /// - `active_archetypes`: currently active psyche archetypes
    pub fn matches(
        &self,
        domain: &ConcernDomain,
        severity: &EventSeverity,
        style: Option<&ComplexityStyle>,
        summary: &str,
        active_archetypes: &[String],
    ) -> bool {
        // Domain filter
        if let Some(ref domains) = self.domains {
            if !domains.contains(domain) {
                return false;
            }
        }

        // Severity filter
        if let Some(ref min_sev) = self.min_severity {
            if !severity.is_at_least(min_sev) {
                return false;
            }
        }

        // Style filter
        if let Some(ref styles) = self.styles {
            if let Some(s) = style {
                if !styles.contains(s) {
                    return false;
                }
            } else {
                // No style on event but filter requires specific styles
                return false;
            }
        }

        // Archetype exclusion
        if !self.exclude_archetypes.is_empty() {
            for archetype in active_archetypes {
                if self.exclude_archetypes.contains(archetype) {
                    return false;
                }
            }
        }

        // Keyword inclusion
        if !self.include_keywords.is_empty() {
            let summary_lower = summary.to_lowercase();
            let has_keyword = self
                .include_keywords
                .iter()
                .any(|kw| summary_lower.contains(&kw.to_lowercase()));
            if !has_keyword {
                return false;
            }
        }

        true
    }
}

// ============================================================================
// ManagementVision
// ============================================================================

/// A named configuration that defines an operator's view of the swarm.
///
/// Visions combine concern domains, complexity styles, allowed actions,
/// alert filters, and dashboard layout preferences into a cohesive
/// management persona. The system ships with predefined visions for
/// common personas and supports custom visions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementVision {
    /// Unique name for this vision.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Which concern domains this vision covers.
    pub concern_domains: Vec<ConcernDomain>,
    /// Default complexity styles for this vision.
    pub default_styles: Vec<ComplexityStyle>,
    /// Actions the operator may perform in this vision.
    pub allowed_actions: Vec<ManagedAction>,
    /// Alert filter configuration.
    pub alert_filters: Vec<AlertFilter>,
    /// Dashboard layout identifier (e.g., "grid", "timeline", "map").
    pub dashboard_layout: String,
    /// How often the dashboard auto-refreshes (seconds).
    pub auto_refresh_secs: u64,
    /// Whether to show facet-level detail (per-node, per-chunk, etc.).
    pub show_facet_detail: bool,
    /// Maximum events shown in the event stream.
    pub max_events_shown: usize,
    /// When this vision was created.
    pub created_at: DateTime<Utc>,
    /// When this vision was last updated.
    pub updated_at: DateTime<Utc>,
}

impl ManagementVision {
    /// Check if an action is allowed in this vision.
    ///
    /// Special rules:
    /// - `ManageAll` in allowed_actions grants every action.
    /// - `ViewAll` in allowed_actions grants all read-only actions plus `StreamEvents`.
    pub fn is_action_allowed(&self, action: &ManagedAction) -> bool {
        // ManageAll grants everything
        if self.allowed_actions.contains(&ManagedAction::ManageAll) {
            return true;
        }

        // ViewAll grants all read-only actions
        if action.is_read_only() && self.allowed_actions.contains(&ManagedAction::ViewAll) {
            return true;
        }

        self.allowed_actions.contains(action)
    }

    /// Check if a concern domain is visible in this vision.
    pub fn is_domain_visible(&self, domain: &ConcernDomain) -> bool {
        self.concern_domains.contains(domain)
    }

    /// Check if an event should be shown based on domain and severity.
    ///
    /// Returns true if the domain is visible AND at least one alert filter
    /// passes for the given domain and severity combination. If no alert
    /// filters are configured, all events in visible domains pass.
    pub fn should_show_event(&self, domain: &ConcernDomain, severity: &EventSeverity) -> bool {
        if !self.is_domain_visible(domain) {
            return false;
        }

        if self.alert_filters.is_empty() {
            return true;
        }

        self.alert_filters
            .iter()
            .any(|filter| filter.matches(domain, severity, None, "", &[]))
    }

    /// Validate this vision, returning a list of errors if invalid.
    ///
    /// Validation checks:
    /// - Name must not be empty
    /// - Name must not contain whitespace
    /// - Must have at least one concern domain
    /// - Must have at least one default style
    /// - Must have at least one allowed action
    /// - auto_refresh_secs must be at least 1
    /// - max_events_shown must be at least 1
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.name.is_empty() {
            errors.push("name must not be empty".to_string());
        }

        if self.name.contains(char::is_whitespace) {
            errors.push("name must not contain whitespace".to_string());
        }

        if self.concern_domains.is_empty() {
            errors.push("must have at least one concern domain".to_string());
        }

        if self.default_styles.is_empty() {
            errors.push("must have at least one default style".to_string());
        }

        if self.allowed_actions.is_empty() {
            errors.push("must have at least one allowed action".to_string());
        }

        if self.auto_refresh_secs < 1 {
            errors.push("auto_refresh_secs must be at least 1".to_string());
        }

        if self.max_events_shown < 1 {
            errors.push("max_events_shown must be at least 1".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// ============================================================================
// Predefined visions
// ============================================================================

/// Names of the predefined visions that cannot be deleted.
const PREDEFINED_VISION_NAMES: &[&str] = &[
    "noc-operator",
    "capacity-planner",
    "security-auditor",
    "developer",
    "executive",
    "swarm-whisperer",
];

/// Returns the set of predefined management visions.
///
/// These visions cover common operational personas and are always available.
/// They cannot be deleted from the vision store but can be overridden with
/// a custom vision of the same name.
pub fn predefined_visions() -> Vec<ManagementVision> {
    let now = Utc::now();

    vec![
        // NOC Operator: Health + Fleet focused, quick actions
        ManagementVision {
            name: "noc-operator".to_string(),
            description:
                "Network Operations Center operator view with health and fleet focus".to_string(),
            concern_domains: vec![ConcernDomain::Health, ConcernDomain::Fleet],
            default_styles: vec![ComplexityStyle::Glanceable, ComplexityStyle::Orchestrated],
            allowed_actions: vec![
                ManagedAction::ViewNodes,
                ManagedAction::DrainNode,
                ManagedAction::CordonNode,
                ManagedAction::UncordonNode,
                ManagedAction::QuarantineNode,
                ManagedAction::ViewFleet,
                ManagedAction::StartRollingUpdate,
                ManagedAction::PauseUpdate,
                ManagedAction::ResumeUpdate,
                ManagedAction::RollbackUpdate,
                ManagedAction::ViewEvents,
                ManagedAction::StreamEvents,
            ],
            alert_filters: vec![AlertFilter::warnings_and_above()],
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: true,
            max_events_shown: 50,
            created_at: now,
            updated_at: now,
        },
        // Capacity Planner: Cost + Fleet focused, observational
        ManagementVision {
            name: "capacity-planner".to_string(),
            description: "Capacity planning view with cost and fleet analytics".to_string(),
            concern_domains: vec![ConcernDomain::Cost, ConcernDomain::Fleet],
            default_styles: vec![ComplexityStyle::Observable, ComplexityStyle::Browseable],
            allowed_actions: vec![
                ManagedAction::ViewEnergy,
                ManagedAction::SetEnergySchedule,
                ManagedAction::ViewFleet,
                ManagedAction::ViewNodes,
                ManagedAction::ViewEvents,
            ],
            alert_filters: vec![AlertFilter::default_all()],
            dashboard_layout: "timeline".to_string(),
            auto_refresh_secs: 30,
            show_facet_detail: true,
            max_events_shown: 100,
            created_at: now,
            updated_at: now,
        },
        // Security Auditor: Security + Data focused, investigative
        ManagementVision {
            name: "security-auditor".to_string(),
            description: "Security audit view with deep investigation capabilities".to_string(),
            concern_domains: vec![ConcernDomain::Security, ConcernDomain::Data],
            default_styles: vec![ComplexityStyle::Browseable, ComplexityStyle::Investigative],
            allowed_actions: vec![
                ManagedAction::AuditResidency,
                ManagedAction::ViewPolicies,
                ManagedAction::SetPolicy,
                ManagedAction::ViewTokens,
                ManagedAction::CreateToken,
                ManagedAction::RevokeToken,
                ManagedAction::ViewEvents,
                ManagedAction::StreamEvents,
            ],
            alert_filters: vec![AlertFilter::default_all()],
            dashboard_layout: "table".to_string(),
            auto_refresh_secs: 60,
            show_facet_detail: true,
            max_events_shown: 200,
            created_at: now,
            updated_at: now,
        },
        // Developer: Work focused, browseable + investigative
        ManagementVision {
            name: "developer".to_string(),
            description: "Developer view for job submission, diagnosis, and results".to_string(),
            concern_domains: vec![ConcernDomain::Work],
            default_styles: vec![ComplexityStyle::Browseable, ComplexityStyle::Investigative],
            allowed_actions: vec![
                ManagedAction::ViewJobs,
                ManagedAction::SubmitJob,
                ManagedAction::CancelJob,
                ManagedAction::DiagnoseJob,
                ManagedAction::ViewJobResults,
                ManagedAction::ViewEvents,
            ],
            alert_filters: vec![AlertFilter {
                domains: Some(vec![ConcernDomain::Work]),
                min_severity: Some(EventSeverity::Warning),
                styles: None,
                exclude_archetypes: Vec::new(),
                include_keywords: Vec::new(),
            }],
            dashboard_layout: "split".to_string(),
            auto_refresh_secs: 10,
            show_facet_detail: true,
            max_events_shown: 50,
            created_at: now,
            updated_at: now,
        },
        // Executive: Cost only, glanceable, read-only
        ManagementVision {
            name: "executive".to_string(),
            description: "Executive summary with cost overview and critical alerts only"
                .to_string(),
            concern_domains: vec![ConcernDomain::Cost],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewAll],
            alert_filters: vec![AlertFilter {
                domains: None,
                min_severity: Some(EventSeverity::Critical),
                styles: None,
                exclude_archetypes: Vec::new(),
                include_keywords: Vec::new(),
            }],
            dashboard_layout: "summary".to_string(),
            auto_refresh_secs: 300,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: now,
            updated_at: now,
        },
        // Swarm Whisperer: all domains, all styles, full control
        ManagementVision {
            name: "swarm-whisperer".to_string(),
            description:
                "Full-access view for swarm operators who need complete visibility and control"
                    .to_string(),
            concern_domains: ConcernDomain::all(),
            default_styles: ComplexityStyle::all(),
            allowed_actions: vec![ManagedAction::ManageAll],
            alert_filters: vec![AlertFilter::default_all()],
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 3,
            show_facet_detail: true,
            max_events_shown: 200,
            created_at: now,
            updated_at: now,
        },
    ]
}

// ============================================================================
// VisionStore
// ============================================================================

/// Concurrent store for management visions.
///
/// Backed by a [`DashMap`] for lock-free concurrent reads. Predefined
/// visions are loaded on construction and cannot be deleted.
pub struct VisionStore {
    /// Map from vision name to vision.
    visions: DashMap<String, ManagementVision>,
}

impl VisionStore {
    /// Create a new vision store, pre-loaded with predefined visions.
    pub fn new() -> Self {
        let store = Self {
            visions: DashMap::new(),
        };
        for vision in predefined_visions() {
            store.visions.insert(vision.name.clone(), vision);
        }
        store
    }

    /// Get a vision by name.
    pub fn get(&self, name: &str) -> Option<ManagementVision> {
        self.visions.get(name).map(|v| v.clone())
    }

    /// List all visions.
    pub fn list(&self) -> Vec<ManagementVision> {
        self.visions
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Insert or update a vision, validating it first.
    ///
    /// Returns `Ok(())` on success, or `Err` with validation errors.
    pub fn upsert(&self, vision: ManagementVision) -> Result<(), Vec<String>> {
        vision.validate()?;
        self.visions.insert(vision.name.clone(), vision);
        Ok(())
    }

    /// Delete a vision by name.
    ///
    /// Returns `Err` if the vision is a predefined vision that cannot be deleted.
    /// Returns `Ok(true)` if the vision was found and deleted, `Ok(false)` if not found.
    pub fn delete(&self, name: &str) -> Result<bool, String> {
        if Self::is_predefined(name) {
            return Err(format!("cannot delete predefined vision '{}'", name));
        }
        Ok(self.visions.remove(name).is_some())
    }

    /// Check if a vision name is one of the predefined visions.
    pub fn is_predefined(name: &str) -> bool {
        PREDEFINED_VISION_NAMES.contains(&name)
    }

    /// Count the number of visions in the store.
    pub fn count(&self) -> usize {
        self.visions.len()
    }

    /// Filter events for a specific vision.
    ///
    /// Given a vision name, domain, severity, and summary, returns true
    /// if the event should be shown according to that vision's filters.
    pub fn filter_events(
        &self,
        vision_name: &str,
        domain: &ConcernDomain,
        severity: &EventSeverity,
        summary: &str,
    ) -> bool {
        match self.get(vision_name) {
            Some(vision) => {
                if !vision.is_domain_visible(domain) {
                    return false;
                }
                if vision.alert_filters.is_empty() {
                    return true;
                }
                vision
                    .alert_filters
                    .iter()
                    .any(|f| f.matches(domain, severity, None, summary, &[]))
            }
            None => false,
        }
    }
}

impl Default for VisionStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ComplexityAssessor
// ============================================================================

/// Static methods for assessing the complexity of swarm state.
///
/// The assessor examines various aspects of the swarm and returns
/// [`ComplexityAssessment`] values that guide how information should be
/// presented. All methods are pure functions with no side effects.
pub struct ComplexityAssessor;

impl ComplexityAssessor {
    /// Assess a node's current state.
    ///
    /// - `load`: the node's current load (0.0-1.0)
    /// - `is_suspect`: whether the failure detector considers the node suspect
    /// - `is_dead`: whether the failure detector considers the node dead
    /// - `chunks_in_progress`: number of chunks currently executing
    pub fn assess_node(
        load: f32,
        is_suspect: bool,
        is_dead: bool,
        chunks_in_progress: usize,
    ) -> ComplexityAssessment {
        if is_dead {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Investigative, ComplexityStyle::Orchestrated],
                reason: "Node declared dead by failure detector".to_string(),
                suggested_action: Some(
                    "Check network connectivity and node logs".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Error),
            }
        } else if is_suspect {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable, ComplexityStyle::Investigative],
                reason: "Node is suspect, may be experiencing issues".to_string(),
                suggested_action: Some(
                    "Monitor node health and gossip connectivity".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            }
        } else if load > 0.9 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable],
                reason: format!("Node under heavy load ({:.0}%)", load * 100.0),
                suggested_action: Some(
                    "Consider draining some work from this node".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            }
        } else if chunks_in_progress > 8 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Browseable],
                reason: format!(
                    "Node executing {} chunks concurrently",
                    chunks_in_progress
                ),
                suggested_action: None,
                auto_assessed: true,
                urgency: Some(EventSeverity::Info),
            }
        } else {
            ComplexityAssessment::simple("Node operating normally")
        }
    }

    /// Assess a job's current state.
    ///
    /// - `chunks_total`: total chunks in the job
    /// - `chunks_completed`: chunks completed successfully
    /// - `chunks_failed`: chunks that have failed
    /// - `elapsed_secs`: seconds since job submission
    pub fn assess_job(
        chunks_total: u32,
        chunks_completed: u32,
        chunks_failed: u32,
        elapsed_secs: u64,
    ) -> ComplexityAssessment {
        if chunks_total == 0 {
            return ComplexityAssessment::simple("Empty job");
        }

        let completion_pct = (chunks_completed as f64 / chunks_total as f64) * 100.0;
        let failure_rate = if chunks_completed + chunks_failed > 0 {
            chunks_failed as f64 / (chunks_completed + chunks_failed) as f64
        } else {
            0.0
        };

        if failure_rate > 0.5 && chunks_failed >= 3 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Investigative],
                reason: format!(
                    "High failure rate ({:.0}%) across {} failed chunks",
                    failure_rate * 100.0,
                    chunks_failed
                ),
                suggested_action: Some(
                    "Diagnose failing chunks and check node health".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Error),
            }
        } else if elapsed_secs > 600 && completion_pct < 25.0 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Investigative, ComplexityStyle::Observable],
                reason: format!(
                    "Job stalled: {:.0}% complete after {} seconds",
                    completion_pct, elapsed_secs
                ),
                suggested_action: Some(
                    "Check for blocked chunks or resource contention".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            }
        } else if chunks_total > 100 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable, ComplexityStyle::Browseable],
                reason: format!(
                    "Large job: {} chunks, {:.0}% complete",
                    chunks_total, completion_pct
                ),
                suggested_action: None,
                auto_assessed: true,
                urgency: Some(EventSeverity::Info),
            }
        } else {
            ComplexityAssessment::simple(format!(
                "Job {:.0}% complete ({}/{})",
                completion_pct, chunks_completed, chunks_total
            ))
        }
    }

    /// Assess the overall swarm health.
    ///
    /// - `total_nodes`: total known nodes
    /// - `alive_nodes`: nodes in Alive status
    /// - `suspect_nodes`: nodes in Suspect status
    /// - `dead_nodes`: nodes in Dead status
    pub fn assess_swarm(
        total_nodes: usize,
        alive_nodes: usize,
        suspect_nodes: usize,
        dead_nodes: usize,
    ) -> ComplexityAssessment {
        if total_nodes == 0 {
            return ComplexityAssessment {
                styles: vec![ComplexityStyle::Glanceable],
                reason: "No nodes in the swarm".to_string(),
                suggested_action: Some(
                    "Bootstrap the swarm with seed nodes".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            };
        }

        let healthy_ratio = alive_nodes as f64 / total_nodes as f64;

        if dead_nodes > total_nodes / 3 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Orchestrated, ComplexityStyle::Investigative],
                reason: format!(
                    "Significant node loss: {} dead out of {} total",
                    dead_nodes, total_nodes
                ),
                suggested_action: Some(
                    "Investigate possible network partition or outage".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Critical),
            }
        } else if suspect_nodes > total_nodes / 4 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable, ComplexityStyle::Investigative],
                reason: format!(
                    "Many suspect nodes: {} out of {} total",
                    suspect_nodes, total_nodes
                ),
                suggested_action: Some(
                    "Monitor gossip health and check for network issues".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            }
        } else if healthy_ratio > 0.95 {
            ComplexityAssessment::simple(format!(
                "Swarm healthy: {}/{} nodes alive",
                alive_nodes, total_nodes
            ))
        } else {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable],
                reason: format!(
                    "Swarm partially degraded: {}/{} alive, {} suspect, {} dead",
                    alive_nodes, total_nodes, suspect_nodes, dead_nodes
                ),
                suggested_action: None,
                auto_assessed: true,
                urgency: Some(EventSeverity::Info),
            }
        }
    }

    /// Assess an inter-swarm membrane state.
    ///
    /// - `treaties_active`: number of active treaties
    /// - `border_crossings_per_min`: rate of cross-swarm traffic
    /// - `violations_recent`: number of recent policy violations
    pub fn assess_membrane(
        treaties_active: usize,
        border_crossings_per_min: f64,
        violations_recent: usize,
    ) -> ComplexityAssessment {
        if violations_recent > 5 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Investigative, ComplexityStyle::Orchestrated],
                reason: format!(
                    "{} recent membrane violations detected",
                    violations_recent
                ),
                suggested_action: Some(
                    "Review agreement terms and audit border crossings".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Error),
            }
        } else if treaties_active == 0 {
            ComplexityAssessment::simple("No active inter-swarm treaties")
        } else if border_crossings_per_min > 100.0 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable],
                reason: format!(
                    "High cross-swarm traffic: {:.0} crossings/min across {} treaties",
                    border_crossings_per_min, treaties_active
                ),
                suggested_action: None,
                auto_assessed: true,
                urgency: Some(EventSeverity::Info),
            }
        } else {
            ComplexityAssessment::simple(format!(
                "{} treaties active, {:.1} crossings/min",
                treaties_active, border_crossings_per_min
            ))
        }
    }

    /// Assess a rolling update's state.
    ///
    /// - `total_nodes`: nodes targeted for update
    /// - `updated_nodes`: nodes already updated
    /// - `failed_nodes`: nodes that failed to update
    /// - `is_paused`: whether the update is paused
    pub fn assess_rolling_update(
        total_nodes: usize,
        updated_nodes: usize,
        failed_nodes: usize,
        is_paused: bool,
    ) -> ComplexityAssessment {
        if is_paused {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Orchestrated],
                reason: format!(
                    "Rolling update paused: {}/{} updated, {} failed",
                    updated_nodes, total_nodes, failed_nodes
                ),
                suggested_action: Some(
                    "Review failures before resuming update".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            }
        } else if failed_nodes > 0
            && failed_nodes as f64 / total_nodes.max(1) as f64 > 0.1
        {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Orchestrated, ComplexityStyle::Investigative],
                reason: format!(
                    "Rolling update experiencing failures: {} out of {} failed",
                    failed_nodes, total_nodes
                ),
                suggested_action: Some(
                    "Consider pausing update and investigating failures".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Error),
            }
        } else if updated_nodes == total_nodes && total_nodes > 0 {
            ComplexityAssessment::simple(format!(
                "Rolling update complete: all {} nodes updated",
                total_nodes
            ))
        } else {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable, ComplexityStyle::Orchestrated],
                reason: format!(
                    "Rolling update in progress: {}/{} updated",
                    updated_nodes, total_nodes
                ),
                suggested_action: None,
                auto_assessed: true,
                urgency: Some(EventSeverity::Info),
            }
        }
    }

    /// Assess SLA compliance.
    ///
    /// - `target_uptime_pct`: the SLA target (e.g., 99.9)
    /// - `actual_uptime_pct`: the measured uptime
    /// - `budget_remaining_minutes`: how much downtime budget remains
    pub fn assess_sla(
        target_uptime_pct: f64,
        actual_uptime_pct: f64,
        budget_remaining_minutes: f64,
    ) -> ComplexityAssessment {
        if actual_uptime_pct < target_uptime_pct {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Investigative, ComplexityStyle::Orchestrated],
                reason: format!(
                    "SLA violated: {:.3}% actual vs {:.3}% target",
                    actual_uptime_pct, target_uptime_pct
                ),
                suggested_action: Some(
                    "Investigate root cause and plan remediation".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Critical),
            }
        } else if budget_remaining_minutes < 10.0 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable],
                reason: format!(
                    "SLA budget critically low: {:.1} minutes remaining",
                    budget_remaining_minutes
                ),
                suggested_action: Some(
                    "Avoid risky changes until budget recovers".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            }
        } else {
            ComplexityAssessment::simple(format!(
                "SLA on track: {:.3}% uptime, {:.0}min budget remaining",
                actual_uptime_pct, budget_remaining_minutes
            ))
        }
    }

    /// Assess capacity utilization.
    ///
    /// - `total_cpu_cores`: total CPU cores across the fleet
    /// - `used_cpu_cores`: CPU cores currently in use
    /// - `total_memory_gb`: total memory in GB
    /// - `used_memory_gb`: memory currently in use in GB
    pub fn assess_capacity(
        total_cpu_cores: u32,
        used_cpu_cores: u32,
        total_memory_gb: f64,
        used_memory_gb: f64,
    ) -> ComplexityAssessment {
        let cpu_util = if total_cpu_cores > 0 {
            used_cpu_cores as f64 / total_cpu_cores as f64
        } else {
            0.0
        };
        let mem_util = if total_memory_gb > 0.0 {
            used_memory_gb / total_memory_gb
        } else {
            0.0
        };

        if cpu_util > 0.9 || mem_util > 0.9 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable, ComplexityStyle::Orchestrated],
                reason: format!(
                    "Near capacity: CPU {:.0}%, Memory {:.0}%",
                    cpu_util * 100.0,
                    mem_util * 100.0
                ),
                suggested_action: Some(
                    "Add nodes or reduce workload to prevent saturation".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            }
        } else if cpu_util < 0.2 && mem_util < 0.2 && total_cpu_cores > 0 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable],
                reason: format!(
                    "Underutilized: CPU {:.0}%, Memory {:.0}%",
                    cpu_util * 100.0,
                    mem_util * 100.0
                ),
                suggested_action: Some(
                    "Consider reducing fleet size to save costs".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Info),
            }
        } else {
            ComplexityAssessment::simple(format!(
                "Capacity balanced: CPU {:.0}%, Memory {:.0}%",
                cpu_util * 100.0,
                mem_util * 100.0
            ))
        }
    }

    /// Assess an alert storm condition.
    ///
    /// - `alerts_per_minute`: rate of incoming alerts
    /// - `distinct_domains`: how many concern domains have active alerts
    /// - `critical_count`: number of critical-severity alerts
    pub fn assess_alert_storm(
        alerts_per_minute: f64,
        distinct_domains: usize,
        critical_count: usize,
    ) -> ComplexityAssessment {
        if alerts_per_minute > 50.0 && distinct_domains >= 3 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Orchestrated, ComplexityStyle::Investigative],
                reason: format!(
                    "Alert storm: {:.0} alerts/min across {} domains ({} critical)",
                    alerts_per_minute, distinct_domains, critical_count
                ),
                suggested_action: Some(
                    "Triage by domain and suppress non-critical alerts temporarily".to_string(),
                ),
                auto_assessed: true,
                urgency: Some(EventSeverity::Critical),
            }
        } else if critical_count > 3 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Investigative],
                reason: format!(
                    "{} critical alerts active, {:.0} alerts/min total",
                    critical_count, alerts_per_minute
                ),
                suggested_action: Some("Focus on critical alerts first".to_string()),
                auto_assessed: true,
                urgency: Some(EventSeverity::Critical),
            }
        } else if alerts_per_minute > 20.0 {
            ComplexityAssessment {
                styles: vec![ComplexityStyle::Observable],
                reason: format!("Elevated alert rate: {:.0}/min", alerts_per_minute),
                suggested_action: None,
                auto_assessed: true,
                urgency: Some(EventSeverity::Warning),
            }
        } else {
            ComplexityAssessment::simple(format!(
                "Normal alert rate: {:.1}/min",
                alerts_per_minute
            ))
        }
    }
}

// ============================================================================
// VisionEngine
// ============================================================================

/// Engine that recommends and manages the active management vision.
///
/// The vision engine maintains an optional override (operator-selected
/// vision) and provides automatic recommendations based on the swarm's
/// current psyche archetype and alert state.
pub struct VisionEngine {
    /// Backing store for all visions.
    vision_store: Arc<VisionStore>,
    /// Operator-selected vision override (takes precedence over recommendation).
    current_override: Arc<RwLock<Option<String>>>,
}

impl VisionEngine {
    /// Create a new vision engine backed by the given store.
    pub fn new(vision_store: Arc<VisionStore>) -> Self {
        Self {
            vision_store,
            current_override: Arc::new(RwLock::new(None)),
        }
    }

    /// Set an operator-selected vision override.
    pub fn set_override(&self, vision_name: Option<String>) {
        *self.current_override.write() = vision_name;
    }

    /// Get the current override, if any.
    pub fn get_override(&self) -> Option<String> {
        self.current_override.read().clone()
    }

    /// Get a reference to the backing vision store.
    pub fn vision_store(&self) -> &Arc<VisionStore> {
        &self.vision_store
    }

    /// Recommend a vision based on the swarm's current psyche and alert state.
    ///
    /// The recommendation logic:
    /// - If `any_critical` is true, recommend "noc-operator" (critical alerts
    ///   need immediate attention with orchestrated actions).
    /// - If `dominant_archetype` is "guardian", recommend "security-auditor".
    /// - If `dominant_archetype` is "explorer", recommend "swarm-whisperer".
    /// - If `dominant_archetype` is "optimizer", recommend "capacity-planner".
    /// - If `dominant_archetype` is "nurturing", recommend "developer".
    /// - Otherwise, recommend "noc-operator" as the default.
    pub fn recommended_vision(
        &self,
        dominant_archetype: Option<&str>,
        any_critical: bool,
    ) -> ManagementVision {
        let name = if any_critical {
            "noc-operator"
        } else if let Some(archetype) = dominant_archetype {
            match archetype.to_lowercase().as_str() {
                "guardian" => "security-auditor",
                "explorer" => "swarm-whisperer",
                "optimizer" => "capacity-planner",
                "nurturing" => "developer",
                _ => "noc-operator",
            }
        } else {
            "noc-operator"
        };

        self.vision_store.get(name).unwrap_or_else(|| {
            // Fallback: should not happen since predefined visions are always loaded
            predefined_visions()
                .into_iter()
                .next()
                .expect("predefined_visions must have at least one entry")
        })
    }

    /// Get the effective vision: the override if set, otherwise the recommendation.
    pub fn effective_vision(
        &self,
        dominant_archetype: Option<&str>,
        any_critical: bool,
    ) -> ManagementVision {
        if let Some(ref name) = *self.current_override.read() {
            if let Some(vision) = self.vision_store.get(name) {
                return vision;
            }
        }
        self.recommended_vision(dominant_archetype, any_critical)
    }
}

// ============================================================================
// ConcernPriority
// ============================================================================

/// Utility for ranking concern domains by priority.
///
/// Domain priority is determined by a weighted combination of factors:
/// how many active alerts exist in each domain, whether critical events
/// are present, and the domain's inherent operational urgency.
pub struct ConcernPriority;

impl ConcernPriority {
    /// Rank domains by priority score (highest first).
    ///
    /// - `alert_counts`: map from domain to (total_alerts, critical_alerts)
    ///
    /// Returns a sorted list of (domain, priority_score) pairs.
    pub fn rank_domains(
        alert_counts: &HashMap<ConcernDomain, (usize, usize)>,
    ) -> Vec<(ConcernDomain, f64)> {
        let mut ranked: Vec<(ConcernDomain, f64)> = ConcernDomain::all()
            .into_iter()
            .map(|domain| {
                let base_weight = Self::inherent_weight(&domain);
                let (total, critical) =
                    alert_counts.get(&domain).copied().unwrap_or((0, 0));
                let alert_score = total as f64 * 1.0 + critical as f64 * 5.0;
                let priority = base_weight + alert_score;
                (domain, priority)
            })
            .collect();

        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        ranked
    }

    /// Inherent operational urgency weight for each domain.
    fn inherent_weight(domain: &ConcernDomain) -> f64 {
        match domain {
            ConcernDomain::Health => 10.0,
            ConcernDomain::Security => 8.0,
            ConcernDomain::Work => 6.0,
            ConcernDomain::Fleet => 5.0,
            ConcernDomain::Data => 4.0,
            ConcernDomain::Psyche => 3.0,
            ConcernDomain::Cost => 2.0,
            ConcernDomain::MultiSwarm => 2.0,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- ComplexityStyle tests --

    #[test]
    fn complexity_style_display_roundtrip() {
        for style in ComplexityStyle::all() {
            let s = style.to_string();
            let parsed: ComplexityStyle = s.parse().expect("should parse back");
            assert_eq!(style, parsed);
        }
    }

    #[test]
    fn complexity_style_from_str_case_insensitive() {
        assert_eq!(
            ComplexityStyle::from_str("GLANCEABLE").expect("parse"),
            ComplexityStyle::Glanceable
        );
        assert_eq!(
            ComplexityStyle::from_str("Browseable").expect("parse"),
            ComplexityStyle::Browseable
        );
        assert_eq!(
            ComplexityStyle::from_str("oBsErVaBlE").expect("parse"),
            ComplexityStyle::Observable
        );
    }

    #[test]
    fn complexity_style_from_str_invalid() {
        assert!(ComplexityStyle::from_str("unknown").is_err());
    }

    #[test]
    fn complexity_style_all_count() {
        assert_eq!(ComplexityStyle::all().len(), 6);
    }

    #[test]
    fn complexity_style_description_non_empty() {
        for style in ComplexityStyle::all() {
            assert!(!style.description().is_empty());
        }
    }

    #[test]
    fn complexity_style_ordering() {
        assert!(ComplexityStyle::Glanceable < ComplexityStyle::Browseable);
        assert!(ComplexityStyle::Browseable < ComplexityStyle::Observable);
    }

    #[test]
    fn complexity_style_serde_roundtrip() {
        for style in ComplexityStyle::all() {
            let json = serde_json::to_string(&style).expect("serialize");
            let parsed: ComplexityStyle =
                serde_json::from_str(&json).expect("deserialize");
            assert_eq!(style, parsed);
        }
    }

    // -- EventSeverity tests --

    #[test]
    fn event_severity_display_roundtrip() {
        let severities = [
            EventSeverity::Info,
            EventSeverity::Warning,
            EventSeverity::Error,
            EventSeverity::Critical,
        ];
        for sev in &severities {
            let s = sev.to_string();
            let parsed: EventSeverity = s.parse().expect("should parse back");
            assert_eq!(*sev, parsed);
        }
    }

    #[test]
    fn event_severity_from_str_aliases() {
        assert_eq!(
            EventSeverity::from_str("warn").expect("parse"),
            EventSeverity::Warning
        );
        assert_eq!(
            EventSeverity::from_str("err").expect("parse"),
            EventSeverity::Error
        );
        assert_eq!(
            EventSeverity::from_str("crit").expect("parse"),
            EventSeverity::Critical
        );
    }

    #[test]
    fn event_severity_is_at_least() {
        assert!(EventSeverity::Critical.is_at_least(&EventSeverity::Info));
        assert!(EventSeverity::Error.is_at_least(&EventSeverity::Warning));
        assert!(EventSeverity::Warning.is_at_least(&EventSeverity::Warning));
        assert!(!EventSeverity::Info.is_at_least(&EventSeverity::Warning));
    }

    #[test]
    fn event_severity_color_code_non_empty() {
        let severities = [
            EventSeverity::Info,
            EventSeverity::Warning,
            EventSeverity::Error,
            EventSeverity::Critical,
        ];
        for sev in &severities {
            assert!(!sev.color_code().is_empty());
            assert!(sev.color_code().starts_with('\x1b'));
        }
    }

    #[test]
    fn event_severity_emoji_non_empty() {
        let severities = [
            EventSeverity::Info,
            EventSeverity::Warning,
            EventSeverity::Error,
            EventSeverity::Critical,
        ];
        for sev in &severities {
            assert!(!sev.emoji().is_empty());
        }
    }

    #[test]
    fn event_severity_ordering() {
        assert!(EventSeverity::Info < EventSeverity::Warning);
        assert!(EventSeverity::Warning < EventSeverity::Error);
        assert!(EventSeverity::Error < EventSeverity::Critical);
    }

    #[test]
    fn event_severity_serde_roundtrip() {
        let severities = [
            EventSeverity::Info,
            EventSeverity::Warning,
            EventSeverity::Error,
            EventSeverity::Critical,
        ];
        for sev in &severities {
            let json = serde_json::to_string(sev).expect("serialize");
            let parsed: EventSeverity =
                serde_json::from_str(&json).expect("deserialize");
            assert_eq!(*sev, parsed);
        }
    }

    // -- ConcernDomain tests --

    #[test]
    fn concern_domain_display_roundtrip() {
        for domain in ConcernDomain::all() {
            let s = domain.to_string();
            let parsed: ConcernDomain = s.parse().expect("should parse back");
            assert_eq!(domain, parsed);
        }
    }

    #[test]
    fn concern_domain_all_count() {
        assert_eq!(ConcernDomain::all().len(), 8);
    }

    #[test]
    fn concern_domain_description_non_empty() {
        for domain in ConcernDomain::all() {
            assert!(!domain.description().is_empty());
        }
    }

    #[test]
    fn concern_domain_default_styles_non_empty() {
        for domain in ConcernDomain::all() {
            assert!(!domain.default_styles().is_empty());
        }
    }

    #[test]
    fn concern_domain_from_str_case_insensitive() {
        assert_eq!(
            ConcernDomain::from_str("HEALTH").expect("parse"),
            ConcernDomain::Health
        );
        assert_eq!(
            ConcernDomain::from_str("Security").expect("parse"),
            ConcernDomain::Security
        );
    }

    #[test]
    fn concern_domain_serde_roundtrip() {
        for domain in ConcernDomain::all() {
            let json = serde_json::to_string(&domain).expect("serialize");
            let parsed: ConcernDomain =
                serde_json::from_str(&json).expect("deserialize");
            assert_eq!(domain, parsed);
        }
    }

    // -- ManagedAction tests --

    #[test]
    fn managed_action_read_only_view_actions() {
        let view_actions = [
            ManagedAction::ViewNodes,
            ManagedAction::ViewJobs,
            ManagedAction::ViewJobResults,
            ManagedAction::ViewFleet,
            ManagedAction::ViewPolicies,
            ManagedAction::ViewTokens,
            ManagedAction::ViewEnergy,
            ManagedAction::ViewVisions,
            ManagedAction::ViewPsyche,
            ManagedAction::ViewConstellation,
            ManagedAction::ViewCrossings,
            ManagedAction::ViewEvents,
            ManagedAction::StreamEvents,
            ManagedAction::ViewAll,
        ];
        for action in &view_actions {
            assert!(action.is_read_only(), "{} should be read-only", action);
        }
    }

    #[test]
    fn managed_action_write_actions_not_read_only() {
        let write_actions = [
            ManagedAction::DrainNode,
            ManagedAction::SubmitJob,
            ManagedAction::CancelJob,
            ManagedAction::SetPolicy,
            ManagedAction::CreateToken,
            ManagedAction::RevokeToken,
            ManagedAction::StartRollingUpdate,
            ManagedAction::ManageAll,
        ];
        for action in &write_actions {
            assert!(
                !action.is_read_only(),
                "{} should not be read-only",
                action
            );
        }
    }

    #[test]
    fn managed_action_display_roundtrip() {
        let actions = [
            ManagedAction::ViewNodes,
            ManagedAction::DrainNode,
            ManagedAction::SubmitJob,
            ManagedAction::ManageAll,
            ManagedAction::ViewAll,
        ];
        for action in &actions {
            let s = action.to_string();
            let parsed: ManagedAction = s.parse().expect("should parse back");
            assert_eq!(*action, parsed);
        }
    }

    #[test]
    fn managed_action_required_domain() {
        assert_eq!(
            ManagedAction::ViewNodes.required_domain(),
            ConcernDomain::Health
        );
        assert_eq!(
            ManagedAction::SubmitJob.required_domain(),
            ConcernDomain::Work
        );
        assert_eq!(
            ManagedAction::SetPolicy.required_domain(),
            ConcernDomain::Security
        );
        assert_eq!(
            ManagedAction::ViewEnergy.required_domain(),
            ConcernDomain::Cost
        );
        assert_eq!(
            ManagedAction::StartRollingUpdate.required_domain(),
            ConcernDomain::Fleet
        );
        assert_eq!(
            ManagedAction::AuditResidency.required_domain(),
            ConcernDomain::Security
        );
    }

    // -- ComplexityAssessment tests --

    #[test]
    fn complexity_assessment_default() {
        let hint = ComplexityAssessment::default();
        assert_eq!(hint.styles, vec![ComplexityStyle::Glanceable]);
        assert!(hint.auto_assessed);
        assert!(hint.suggested_action.is_none());
        assert!(hint.urgency.is_none());
    }

    #[test]
    fn complexity_assessment_simple() {
        let hint = ComplexityAssessment::simple("test reason");
        assert_eq!(hint.styles, vec![ComplexityStyle::Glanceable]);
        assert_eq!(hint.reason, "test reason");
        assert!(hint.auto_assessed);
    }

    #[test]
    fn complexity_assessment_needs_investigation() {
        let hint = ComplexityAssessment::needs_investigation("something wrong");
        assert_eq!(hint.styles, vec![ComplexityStyle::Investigative]);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
    }

    #[test]
    fn complexity_assessment_needs_orchestration() {
        let hint = ComplexityAssessment::needs_orchestration("complex change");
        assert_eq!(hint.styles, vec![ComplexityStyle::Orchestrated]);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
    }

    #[test]
    fn complexity_assessment_multi() {
        let hint = ComplexityAssessment::multi(
            vec![ComplexityStyle::Observable, ComplexityStyle::Browseable],
            "multi style",
        );
        assert_eq!(hint.styles.len(), 2);
        assert!(hint.styles.contains(&ComplexityStyle::Observable));
        assert!(hint.styles.contains(&ComplexityStyle::Browseable));
    }

    // -- AlertFilter tests --

    #[test]
    fn alert_filter_default_all_matches_everything() {
        let filter = AlertFilter::default_all();
        assert!(filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            None,
            "anything",
            &[],
        ));
    }

    #[test]
    fn alert_filter_warnings_and_above_rejects_info() {
        let filter = AlertFilter::warnings_and_above();
        assert!(!filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            None,
            "test",
            &[],
        ));
        assert!(filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Warning,
            None,
            "test",
            &[],
        ));
        assert!(filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Critical,
            None,
            "test",
            &[],
        ));
    }

    #[test]
    fn alert_filter_domain_restriction() {
        let filter = AlertFilter {
            domains: Some(vec![ConcernDomain::Work]),
            min_severity: None,
            styles: None,
            exclude_archetypes: Vec::new(),
            include_keywords: Vec::new(),
        };
        assert!(filter.matches(
            &ConcernDomain::Work,
            &EventSeverity::Info,
            None,
            "",
            &[],
        ));
        assert!(!filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            None,
            "",
            &[],
        ));
    }

    #[test]
    fn alert_filter_style_restriction() {
        let filter = AlertFilter {
            domains: None,
            min_severity: None,
            styles: Some(vec![ComplexityStyle::Glanceable]),
            exclude_archetypes: Vec::new(),
            include_keywords: Vec::new(),
        };
        assert!(filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            Some(&ComplexityStyle::Glanceable),
            "",
            &[],
        ));
        assert!(!filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            Some(&ComplexityStyle::Investigative),
            "",
            &[],
        ));
        // No style provided but filter requires one
        assert!(!filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            None,
            "",
            &[],
        ));
    }

    #[test]
    fn alert_filter_archetype_exclusion() {
        let filter = AlertFilter {
            domains: None,
            min_severity: None,
            styles: None,
            exclude_archetypes: vec!["guardian".to_string()],
            include_keywords: Vec::new(),
        };
        assert!(filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            None,
            "",
            &[],
        ));
        assert!(!filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            None,
            "",
            &["guardian".to_string()],
        ));
    }

    #[test]
    fn alert_filter_keyword_inclusion() {
        let filter = AlertFilter {
            domains: None,
            min_severity: None,
            styles: None,
            exclude_archetypes: Vec::new(),
            include_keywords: vec!["partition".to_string(), "timeout".to_string()],
        };
        assert!(filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Error,
            None,
            "network partition detected",
            &[],
        ));
        assert!(!filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Error,
            None,
            "node joined swarm",
            &[],
        ));
    }

    #[test]
    fn alert_filter_keyword_case_insensitive() {
        let filter = AlertFilter {
            domains: None,
            min_severity: None,
            styles: None,
            exclude_archetypes: Vec::new(),
            include_keywords: vec!["ERROR".to_string()],
        };
        assert!(filter.matches(
            &ConcernDomain::Health,
            &EventSeverity::Info,
            None,
            "an error occurred",
            &[],
        ));
    }

    // -- ManagementVision tests --

    #[test]
    fn management_vision_action_allowed_direct() {
        let vision = ManagementVision {
            name: "test".to_string(),
            description: "test".to_string(),
            concern_domains: vec![ConcernDomain::Health],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewNodes, ManagedAction::DrainNode],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(vision.is_action_allowed(&ManagedAction::ViewNodes));
        assert!(vision.is_action_allowed(&ManagedAction::DrainNode));
        assert!(!vision.is_action_allowed(&ManagedAction::SubmitJob));
    }

    #[test]
    fn management_vision_manage_all_grants_everything() {
        let vision = ManagementVision {
            name: "test".to_string(),
            description: "test".to_string(),
            concern_domains: vec![ConcernDomain::Health],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ManageAll],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(vision.is_action_allowed(&ManagedAction::DrainNode));
        assert!(vision.is_action_allowed(&ManagedAction::SubmitJob));
        assert!(vision.is_action_allowed(&ManagedAction::ViewNodes));
        assert!(vision.is_action_allowed(&ManagedAction::SetPolicy));
    }

    #[test]
    fn management_vision_view_all_grants_read_only() {
        let vision = ManagementVision {
            name: "test".to_string(),
            description: "test".to_string(),
            concern_domains: vec![ConcernDomain::Cost],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewAll],
            alert_filters: Vec::new(),
            dashboard_layout: "summary".to_string(),
            auto_refresh_secs: 60,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(vision.is_action_allowed(&ManagedAction::ViewNodes));
        assert!(vision.is_action_allowed(&ManagedAction::ViewJobs));
        assert!(vision.is_action_allowed(&ManagedAction::StreamEvents));
        assert!(!vision.is_action_allowed(&ManagedAction::DrainNode));
        assert!(!vision.is_action_allowed(&ManagedAction::SubmitJob));
    }

    #[test]
    fn management_vision_domain_visibility() {
        let vision = ManagementVision {
            name: "test".to_string(),
            description: "test".to_string(),
            concern_domains: vec![ConcernDomain::Health, ConcernDomain::Fleet],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewNodes],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(vision.is_domain_visible(&ConcernDomain::Health));
        assert!(vision.is_domain_visible(&ConcernDomain::Fleet));
        assert!(!vision.is_domain_visible(&ConcernDomain::Security));
    }

    #[test]
    fn management_vision_should_show_event() {
        let vision = ManagementVision {
            name: "test".to_string(),
            description: "test".to_string(),
            concern_domains: vec![ConcernDomain::Health],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewNodes],
            alert_filters: vec![AlertFilter::warnings_and_above()],
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        // Health + Warning = passes
        assert!(
            vision.should_show_event(&ConcernDomain::Health, &EventSeverity::Warning)
        );
        // Health + Info = fails (below Warning)
        assert!(
            !vision.should_show_event(&ConcernDomain::Health, &EventSeverity::Info)
        );
        // Security + Warning = fails (domain not visible)
        assert!(
            !vision.should_show_event(&ConcernDomain::Security, &EventSeverity::Warning)
        );
    }

    #[test]
    fn management_vision_validate_valid() {
        let now = Utc::now();
        let vision = ManagementVision {
            name: "test-vision".to_string(),
            description: "A test vision".to_string(),
            concern_domains: vec![ConcernDomain::Health],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewNodes],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: true,
            max_events_shown: 50,
            created_at: now,
            updated_at: now,
        };
        assert!(vision.validate().is_ok());
    }

    #[test]
    fn management_vision_validate_empty_name() {
        let now = Utc::now();
        let vision = ManagementVision {
            name: "".to_string(),
            description: "test".to_string(),
            concern_domains: vec![ConcernDomain::Health],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewNodes],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: now,
            updated_at: now,
        };
        let errors = vision.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("name must not be empty")));
    }

    #[test]
    fn management_vision_validate_whitespace_name() {
        let now = Utc::now();
        let vision = ManagementVision {
            name: "bad name".to_string(),
            description: "test".to_string(),
            concern_domains: vec![ConcernDomain::Health],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewNodes],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: now,
            updated_at: now,
        };
        let errors = vision.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("whitespace")));
    }

    #[test]
    fn management_vision_validate_no_domains() {
        let now = Utc::now();
        let vision = ManagementVision {
            name: "test".to_string(),
            description: "test".to_string(),
            concern_domains: Vec::new(),
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewNodes],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: now,
            updated_at: now,
        };
        let errors = vision.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("concern domain")));
    }

    // -- Predefined visions tests --

    #[test]
    fn predefined_visions_count() {
        assert_eq!(predefined_visions().len(), 6);
    }

    #[test]
    fn predefined_visions_all_valid() {
        for vision in predefined_visions() {
            assert!(
                vision.validate().is_ok(),
                "predefined vision '{}' is invalid: {:?}",
                vision.name,
                vision.validate()
            );
        }
    }

    #[test]
    fn predefined_visions_unique_names() {
        let visions = predefined_visions();
        let names: Vec<&str> = visions.iter().map(|v| v.name.as_str()).collect();
        let mut unique = names.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(names.len(), unique.len());
    }

    // -- VisionStore tests --

    #[test]
    fn vision_store_new_has_predefined() {
        let store = VisionStore::new();
        assert_eq!(store.count(), 6);
        assert!(store.get("noc-operator").is_some());
        assert!(store.get("swarm-whisperer").is_some());
    }

    #[test]
    fn vision_store_get_nonexistent() {
        let store = VisionStore::new();
        assert!(store.get("nonexistent").is_none());
    }

    #[test]
    fn vision_store_upsert_custom() {
        let store = VisionStore::new();
        let now = Utc::now();
        let custom = ManagementVision {
            name: "custom-vision".to_string(),
            description: "my custom vision".to_string(),
            concern_domains: vec![ConcernDomain::Work],
            default_styles: vec![ComplexityStyle::Browseable],
            allowed_actions: vec![ManagedAction::ViewJobs],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 10,
            show_facet_detail: true,
            max_events_shown: 25,
            created_at: now,
            updated_at: now,
        };
        assert!(store.upsert(custom).is_ok());
        assert_eq!(store.count(), 7);
        let fetched = store.get("custom-vision").expect("should exist");
        assert_eq!(fetched.description, "my custom vision");
    }

    #[test]
    fn vision_store_upsert_invalid_rejected() {
        let store = VisionStore::new();
        let now = Utc::now();
        let invalid = ManagementVision {
            name: "".to_string(),
            description: "".to_string(),
            concern_domains: Vec::new(),
            default_styles: Vec::new(),
            allowed_actions: Vec::new(),
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 0,
            show_facet_detail: false,
            max_events_shown: 0,
            created_at: now,
            updated_at: now,
        };
        assert!(store.upsert(invalid).is_err());
        // Count unchanged
        assert_eq!(store.count(), 6);
    }

    #[test]
    fn vision_store_delete_custom() {
        let store = VisionStore::new();
        let now = Utc::now();
        let custom = ManagementVision {
            name: "temp-vision".to_string(),
            description: "temp".to_string(),
            concern_domains: vec![ConcernDomain::Health],
            default_styles: vec![ComplexityStyle::Glanceable],
            allowed_actions: vec![ManagedAction::ViewNodes],
            alert_filters: Vec::new(),
            dashboard_layout: "grid".to_string(),
            auto_refresh_secs: 5,
            show_facet_detail: false,
            max_events_shown: 10,
            created_at: now,
            updated_at: now,
        };
        store.upsert(custom).expect("upsert");
        assert_eq!(store.count(), 7);

        let result = store.delete("temp-vision");
        assert_eq!(result, Ok(true));
        assert_eq!(store.count(), 6);
    }

    #[test]
    fn vision_store_cannot_delete_predefined() {
        let store = VisionStore::new();
        let result = store.delete("noc-operator");
        assert!(result.is_err());
        assert_eq!(store.count(), 6);
    }

    #[test]
    fn vision_store_delete_nonexistent() {
        let store = VisionStore::new();
        let result = store.delete("does-not-exist");
        assert_eq!(result, Ok(false));
    }

    #[test]
    fn vision_store_is_predefined() {
        assert!(VisionStore::is_predefined("noc-operator"));
        assert!(VisionStore::is_predefined("swarm-whisperer"));
        assert!(!VisionStore::is_predefined("custom"));
    }

    #[test]
    fn vision_store_list() {
        let store = VisionStore::new();
        let list = store.list();
        assert_eq!(list.len(), 6);
    }

    #[test]
    fn vision_store_filter_events() {
        let store = VisionStore::new();
        // noc-operator covers Health and Fleet, with warnings_and_above filter
        assert!(store.filter_events(
            "noc-operator",
            &ConcernDomain::Health,
            &EventSeverity::Error,
            "test",
        ));
        assert!(!store.filter_events(
            "noc-operator",
            &ConcernDomain::Health,
            &EventSeverity::Info,
            "test",
        ));
        assert!(!store.filter_events(
            "noc-operator",
            &ConcernDomain::Security,
            &EventSeverity::Error,
            "test",
        ));
        // Nonexistent vision returns false
        assert!(!store.filter_events(
            "no-such-vision",
            &ConcernDomain::Health,
            &EventSeverity::Error,
            "test",
        ));
    }

    // -- ComplexityAssessor tests --

    #[test]
    fn assessor_node_dead() {
        let hint = ComplexityAssessor::assess_node(0.5, false, true, 0);
        assert_eq!(hint.urgency, Some(EventSeverity::Error));
        assert!(hint.styles.contains(&ComplexityStyle::Investigative));
    }

    #[test]
    fn assessor_node_suspect() {
        let hint = ComplexityAssessor::assess_node(0.5, true, false, 0);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
    }

    #[test]
    fn assessor_node_high_load() {
        let hint = ComplexityAssessor::assess_node(0.95, false, false, 2);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
        assert!(hint.reason.contains("heavy load"));
    }

    #[test]
    fn assessor_node_normal() {
        let hint = ComplexityAssessor::assess_node(0.3, false, false, 2);
        assert!(hint.urgency.is_none());
    }

    #[test]
    fn assessor_job_high_failure_rate() {
        let hint = ComplexityAssessor::assess_job(10, 2, 5, 100);
        assert_eq!(hint.urgency, Some(EventSeverity::Error));
        assert!(hint.styles.contains(&ComplexityStyle::Investigative));
    }

    #[test]
    fn assessor_job_stalled() {
        let hint = ComplexityAssessor::assess_job(100, 10, 0, 700);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
    }

    #[test]
    fn assessor_job_large() {
        let hint = ComplexityAssessor::assess_job(200, 150, 0, 60);
        assert_eq!(hint.urgency, Some(EventSeverity::Info));
        assert!(hint.styles.contains(&ComplexityStyle::Observable));
    }

    #[test]
    fn assessor_job_empty() {
        let hint = ComplexityAssessor::assess_job(0, 0, 0, 0);
        assert_eq!(hint.reason, "Empty job");
    }

    #[test]
    fn assessor_swarm_no_nodes() {
        let hint = ComplexityAssessor::assess_swarm(0, 0, 0, 0);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
    }

    #[test]
    fn assessor_swarm_healthy() {
        let hint = ComplexityAssessor::assess_swarm(100, 98, 2, 0);
        assert!(hint.urgency.is_none());
    }

    #[test]
    fn assessor_swarm_many_dead() {
        let hint = ComplexityAssessor::assess_swarm(12, 4, 0, 8);
        assert_eq!(hint.urgency, Some(EventSeverity::Critical));
    }

    #[test]
    fn assessor_membrane_violations() {
        let hint = ComplexityAssessor::assess_membrane(3, 50.0, 10);
        assert_eq!(hint.urgency, Some(EventSeverity::Error));
    }

    #[test]
    fn assessor_rolling_update_paused() {
        let hint = ComplexityAssessor::assess_rolling_update(50, 20, 3, true);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
        assert!(hint.styles.contains(&ComplexityStyle::Orchestrated));
    }

    #[test]
    fn assessor_rolling_update_complete() {
        let hint = ComplexityAssessor::assess_rolling_update(50, 50, 0, false);
        assert!(hint.urgency.is_none());
    }

    #[test]
    fn assessor_sla_violated() {
        let hint = ComplexityAssessor::assess_sla(99.9, 99.5, 5.0);
        assert_eq!(hint.urgency, Some(EventSeverity::Critical));
    }

    #[test]
    fn assessor_sla_budget_low() {
        let hint = ComplexityAssessor::assess_sla(99.9, 99.95, 5.0);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
    }

    #[test]
    fn assessor_capacity_near_max() {
        let hint = ComplexityAssessor::assess_capacity(100, 95, 256.0, 240.0);
        assert_eq!(hint.urgency, Some(EventSeverity::Warning));
    }

    #[test]
    fn assessor_capacity_underutilized() {
        let hint = ComplexityAssessor::assess_capacity(100, 10, 256.0, 20.0);
        assert_eq!(hint.urgency, Some(EventSeverity::Info));
    }

    #[test]
    fn assessor_alert_storm() {
        let hint = ComplexityAssessor::assess_alert_storm(60.0, 4, 5);
        assert_eq!(hint.urgency, Some(EventSeverity::Critical));
    }

    #[test]
    fn assessor_alert_storm_normal() {
        let hint = ComplexityAssessor::assess_alert_storm(5.0, 2, 0);
        assert!(hint.urgency.is_none());
    }

    // -- VisionEngine tests --

    #[test]
    fn vision_engine_recommended_default() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        let vision = engine.recommended_vision(None, false);
        assert_eq!(vision.name, "noc-operator");
    }

    #[test]
    fn vision_engine_recommended_critical_overrides_archetype() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        let vision = engine.recommended_vision(Some("explorer"), true);
        assert_eq!(vision.name, "noc-operator");
    }

    #[test]
    fn vision_engine_recommended_guardian() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        let vision = engine.recommended_vision(Some("guardian"), false);
        assert_eq!(vision.name, "security-auditor");
    }

    #[test]
    fn vision_engine_recommended_explorer() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        let vision = engine.recommended_vision(Some("explorer"), false);
        assert_eq!(vision.name, "swarm-whisperer");
    }

    #[test]
    fn vision_engine_recommended_optimizer() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        let vision = engine.recommended_vision(Some("optimizer"), false);
        assert_eq!(vision.name, "capacity-planner");
    }

    #[test]
    fn vision_engine_recommended_nurturing() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        let vision = engine.recommended_vision(Some("nurturing"), false);
        assert_eq!(vision.name, "developer");
    }

    #[test]
    fn vision_engine_effective_with_override() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        engine.set_override(Some("executive".to_string()));
        let vision = engine.effective_vision(Some("guardian"), false);
        assert_eq!(vision.name, "executive");
    }

    #[test]
    fn vision_engine_effective_with_invalid_override_falls_back() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        engine.set_override(Some("nonexistent".to_string()));
        let vision = engine.effective_vision(None, false);
        // Falls back to recommended since override vision does not exist
        assert_eq!(vision.name, "noc-operator");
    }

    #[test]
    fn vision_engine_effective_without_override() {
        let store = Arc::new(VisionStore::new());
        let engine = VisionEngine::new(store);
        assert!(engine.get_override().is_none());
        let vision = engine.effective_vision(Some("optimizer"), false);
        assert_eq!(vision.name, "capacity-planner");
    }

    // -- ConcernPriority tests --

    #[test]
    fn concern_priority_rank_with_no_alerts() {
        let alert_counts = HashMap::new();
        let ranked = ConcernPriority::rank_domains(&alert_counts);
        assert_eq!(ranked.len(), 8);
        // Health should be first (highest inherent weight)
        assert_eq!(ranked[0].0, ConcernDomain::Health);
        // Cost should be last (lowest inherent weight)
        assert!(matches!(ranked[7].0, ConcernDomain::Cost | ConcernDomain::MultiSwarm));
    }

    #[test]
    fn concern_priority_rank_with_critical_alerts() {
        let mut alert_counts = HashMap::new();
        // Give Cost domain many critical alerts to override inherent weight
        alert_counts.insert(ConcernDomain::Cost, (10, 5));
        let ranked = ConcernPriority::rank_domains(&alert_counts);
        // Cost should now be near the top: 2.0 + 10*1 + 5*5 = 37.0
        // Health has: 10.0 + 0 = 10.0
        assert_eq!(ranked[0].0, ConcernDomain::Cost);
    }

    #[test]
    fn concern_priority_rank_returns_all_domains() {
        let alert_counts = HashMap::new();
        let ranked = ConcernPriority::rank_domains(&alert_counts);
        let domains: Vec<ConcernDomain> = ranked.iter().map(|(d, _)| *d).collect();
        for domain in ConcernDomain::all() {
            assert!(
                domains.contains(&domain),
                "missing domain: {}",
                domain
            );
        }
    }

    // -- Serde roundtrip integration tests --

    #[test]
    fn management_vision_serde_roundtrip() {
        for vision in predefined_visions() {
            let json = serde_json::to_string(&vision).expect("serialize");
            let parsed: ManagementVision =
                serde_json::from_str(&json).expect("deserialize");
            assert_eq!(parsed.name, vision.name);
            assert_eq!(parsed.concern_domains, vision.concern_domains);
            assert_eq!(parsed.default_styles, vision.default_styles);
            assert_eq!(parsed.allowed_actions, vision.allowed_actions);
            assert_eq!(parsed.auto_refresh_secs, vision.auto_refresh_secs);
        }
    }

    #[test]
    fn complexity_assessment_serde_roundtrip() {
        let hint = ComplexityAssessment {
            styles: vec![ComplexityStyle::Observable, ComplexityStyle::Investigative],
            reason: "test reason".to_string(),
            suggested_action: Some("do something".to_string()),
            auto_assessed: true,
            urgency: Some(EventSeverity::Warning),
        };
        let json = serde_json::to_string(&hint).expect("serialize");
        let parsed: ComplexityAssessment =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.styles, hint.styles);
        assert_eq!(parsed.reason, hint.reason);
        assert_eq!(parsed.suggested_action, hint.suggested_action);
        assert_eq!(parsed.urgency, hint.urgency);
    }

    #[test]
    fn alert_filter_serde_roundtrip() {
        let filter = AlertFilter {
            domains: Some(vec![ConcernDomain::Health, ConcernDomain::Work]),
            min_severity: Some(EventSeverity::Warning),
            styles: Some(vec![ComplexityStyle::Glanceable]),
            exclude_archetypes: vec!["guardian".to_string()],
            include_keywords: vec!["partition".to_string()],
        };
        let json = serde_json::to_string(&filter).expect("serialize");
        let parsed: AlertFilter =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.domains, filter.domains);
        assert_eq!(parsed.min_severity, filter.min_severity);
        assert_eq!(parsed.styles, filter.styles);
        assert_eq!(parsed.exclude_archetypes, filter.exclude_archetypes);
        assert_eq!(parsed.include_keywords, filter.include_keywords);
    }

    #[test]
    fn managed_action_serde_roundtrip() {
        let actions = [
            ManagedAction::ViewNodes,
            ManagedAction::DrainNode,
            ManagedAction::ManageAll,
            ManagedAction::ViewAll,
            ManagedAction::SubmitJob,
            ManagedAction::ProposeAgreement,
        ];
        for action in &actions {
            let json = serde_json::to_string(action).expect("serialize");
            let parsed: ManagedAction =
                serde_json::from_str(&json).expect("deserialize");
            assert_eq!(*action, parsed);
        }
    }
}
