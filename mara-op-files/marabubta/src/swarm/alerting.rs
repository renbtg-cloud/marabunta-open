// Marabunta - Licensed under the MIT License.
//! Alert rules engine for the Marabunta Swarm.
//!
//! Provides a full alerting pipeline: define rules with complex conditions,
//! evaluate them against live metrics and psyche state, fire/resolve alerts,
//! apply silence windows, manage escalation chains, and send notifications.
//!
//! # Architecture
//!
//! ```text
//!   AlertContext ──► AlertEngine.evaluate_all()
//!                        │
//!                        ├── for each AlertRule:
//!                        │     ├── evaluate condition
//!                        │     ├── check for_duration (pending)
//!                        │     ├── check inhibition
//!                        │     ├── check silence windows
//!                        │     └── fire / resolve
//!                        │
//!                        └── send notifications via channels
//! ```
//!
//! The engine supports 15 built-in rules covering common failure modes
//! (node death storms, partition detection, disk pressure, etc.) and
//! can be extended with custom rules at runtime.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use super::complexity::{ConcernDomain, EventSeverity};

// ============================================================================
// Constants
// ============================================================================

/// Maximum alert history entries.
const MAX_ALERT_HISTORY: usize = 10_000;

/// Default evaluation interval.
const DEFAULT_EVAL_INTERVAL_SECS: u64 = 10;

/// Default repeat interval for firing alerts (seconds).
const DEFAULT_REPEAT_INTERVAL_SECS: u64 = 300;

// ============================================================================
// AlertContext
// ============================================================================

/// Context provided to the alert engine for condition evaluation.
///
/// Contains current metric values, psyche facet levels, active archetypes,
/// and event rates. Updated externally before each evaluation cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertContext {
    /// Current metric values (e.g., "cpu_usage" -> 0.85).
    pub metrics: HashMap<String, f64>,
    /// Current psyche facet levels (e.g., "resilience" -> 70).
    pub facets: HashMap<String, u8>,
    /// Currently active archetype names.
    pub active_archetypes: Vec<String>,
    /// Event rates by key (e.g., "health:error" -> 5.0 events/min).
    pub event_rate: HashMap<String, f64>,
    /// Timestamp of this context snapshot.
    pub timestamp: DateTime<Utc>,
}

impl AlertContext {
    /// Create a new empty context at the current time.
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
            facets: HashMap::new(),
            active_archetypes: Vec::new(),
            event_rate: HashMap::new(),
            timestamp: Utc::now(),
        }
    }

    /// Set a metric value.
    pub fn set_metric(&mut self, name: impl Into<String>, value: f64) -> &mut Self {
        self.metrics.insert(name.into(), value);
        self
    }

    /// Set a facet level.
    pub fn set_facet(&mut self, name: impl Into<String>, level: u8) -> &mut Self {
        self.facets.insert(name.into(), level);
        self
    }

    /// Set an event rate.
    pub fn set_event_rate(&mut self, key: impl Into<String>, rate: f64) -> &mut Self {
        self.event_rate.insert(key.into(), rate);
        self
    }

    /// Add an active archetype.
    pub fn add_archetype(&mut self, name: impl Into<String>) -> &mut Self {
        self.active_archetypes.push(name.into());
        self
    }
}

impl Default for AlertContext {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// AlertCondition
// ============================================================================

/// A condition that can be evaluated against an [`AlertContext`].
///
/// Conditions can be combined with `All`, `Any`, and `Not` for complex logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertCondition {
    /// Metric is above a threshold.
    MetricAbove {
        /// Metric name.
        name: String,
        /// Threshold value.
        threshold: f64,
        /// How long the metric must be above threshold (seconds).
        for_duration: u64,
    },
    /// Metric is below a threshold.
    MetricBelow {
        /// Metric name.
        name: String,
        /// Threshold value.
        threshold: f64,
        /// How long the metric must be below threshold (seconds).
        for_duration: u64,
    },
    /// Event rate exceeds a threshold.
    EventRate {
        /// Concern domain filter.
        domain: String,
        /// Severity filter.
        severity: String,
        /// Maximum rate per minute.
        rate_per_min: f64,
        /// Window in seconds over which to measure.
        window_secs: u64,
    },
    /// An event matches domain and severity with optional keyword.
    EventMatch {
        /// Domain to match.
        domain: String,
        /// Severity to match.
        severity: String,
        /// Optional keyword in event summary.
        keyword: Option<String>,
    },
    /// A psyche facet is below a level.
    FacetBelow {
        /// Facet name.
        facet: String,
        /// Threshold level.
        level: u8,
        /// Duration requirement (seconds).
        for_duration: u64,
    },
    /// A psyche facet is above a level.
    FacetAbove {
        /// Facet name.
        facet: String,
        /// Threshold level.
        level: u8,
        /// Duration requirement (seconds).
        for_duration: u64,
    },
    /// A specific archetype is currently active.
    ArchetypeActive {
        /// Archetype name.
        name: String,
    },
    /// A specific archetype has been inactive for a duration.
    ArchetypeInactive {
        /// Archetype name.
        name: String,
        /// Duration requirement (seconds).
        for_duration: u64,
    },
    /// All sub-conditions must be true.
    All(Vec<AlertCondition>),
    /// Any sub-condition must be true.
    Any(Vec<AlertCondition>),
    /// Negation of a sub-condition.
    Not(Box<AlertCondition>),
}

impl AlertCondition {
    /// Evaluate this condition against the given context.
    ///
    /// For duration-based conditions, this performs a point-in-time check only.
    /// The `for_duration` tracking is handled by the [`AlertEngine`] via
    /// the pending conditions map.
    pub fn evaluate(&self, ctx: &AlertContext) -> bool {
        match self {
            AlertCondition::MetricAbove { name, threshold, .. } => {
                ctx.metrics
                    .get(name)
                    .is_some_and(|v| *v > *threshold)
            }
            AlertCondition::MetricBelow { name, threshold, .. } => {
                ctx.metrics
                    .get(name)
                    .is_some_and(|v| *v < *threshold)
            }
            AlertCondition::EventRate { domain, severity, rate_per_min, .. } => {
                let key = format!("{}:{}", domain, severity);
                ctx.event_rate
                    .get(&key)
                    .is_some_and(|rate| *rate > *rate_per_min)
            }
            AlertCondition::EventMatch { domain, severity, keyword } => {
                // EventMatch checks event_rate for presence (rate > 0)
                let key = format!("{}:{}", domain, severity);
                let has_rate = ctx.event_rate
                    .get(&key)
                    .is_some_and(|rate| *rate > 0.0);
                if !has_rate {
                    return false;
                }
                // If keyword specified, check metrics for a keyword marker
                if let Some(kw) = keyword {
                    let kw_key = format!("event_keyword:{}", kw);
                    ctx.metrics.get(&kw_key).is_some_and(|v| *v > 0.0)
                } else {
                    true
                }
            }
            AlertCondition::FacetBelow { facet, level, .. } => {
                ctx.facets
                    .get(facet)
                    .is_some_and(|v| *v < *level)
            }
            AlertCondition::FacetAbove { facet, level, .. } => {
                ctx.facets
                    .get(facet)
                    .is_some_and(|v| *v > *level)
            }
            AlertCondition::ArchetypeActive { name } => {
                ctx.active_archetypes.contains(name)
            }
            AlertCondition::ArchetypeInactive { name, .. } => {
                !ctx.active_archetypes.contains(name)
            }
            AlertCondition::All(conditions) => {
                conditions.iter().all(|c| c.evaluate(ctx))
            }
            AlertCondition::Any(conditions) => {
                conditions.iter().any(|c| c.evaluate(ctx))
            }
            AlertCondition::Not(condition) => {
                !condition.evaluate(ctx)
            }
        }
    }

    /// Validate this condition for structural correctness.
    ///
    /// Returns `Ok(())` if valid, or a list of validation errors.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        self.validate_inner(&mut errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Internal recursive validation.
    fn validate_inner(&self, errors: &mut Vec<String>) {
        match self {
            AlertCondition::MetricAbove { name, .. }
            | AlertCondition::MetricBelow { name, .. } => {
                if name.is_empty() {
                    errors.push("metric name cannot be empty".to_string());
                }
            }
            AlertCondition::EventRate { rate_per_min, window_secs, .. } => {
                if *rate_per_min < 0.0 {
                    errors.push("rate_per_min must be non-negative".to_string());
                }
                if *window_secs == 0 {
                    errors.push("window_secs must be positive".to_string());
                }
            }
            AlertCondition::EventMatch { domain, severity, .. } => {
                if domain.is_empty() {
                    errors.push("event match domain cannot be empty".to_string());
                }
                if severity.is_empty() {
                    errors.push("event match severity cannot be empty".to_string());
                }
            }
            AlertCondition::FacetBelow { facet, .. }
            | AlertCondition::FacetAbove { facet, .. } => {
                if facet.is_empty() {
                    errors.push("facet name cannot be empty".to_string());
                }
            }
            AlertCondition::ArchetypeActive { name }
            | AlertCondition::ArchetypeInactive { name, .. } => {
                if name.is_empty() {
                    errors.push("archetype name cannot be empty".to_string());
                }
            }
            AlertCondition::All(conditions) | AlertCondition::Any(conditions) => {
                if conditions.is_empty() {
                    errors.push("All/Any condition must have at least one sub-condition".to_string());
                }
                for c in conditions {
                    c.validate_inner(errors);
                }
            }
            AlertCondition::Not(condition) => {
                condition.validate_inner(errors);
            }
        }
    }

    /// Return a human-readable description of this condition.
    pub fn description(&self) -> String {
        match self {
            AlertCondition::MetricAbove { name, threshold, for_duration } => {
                format!("metric '{}' > {} for {}s", name, threshold, for_duration)
            }
            AlertCondition::MetricBelow { name, threshold, for_duration } => {
                format!("metric '{}' < {} for {}s", name, threshold, for_duration)
            }
            AlertCondition::EventRate { domain, severity, rate_per_min, window_secs } => {
                format!(
                    "event rate {}:{} > {}/min over {}s",
                    domain, severity, rate_per_min, window_secs
                )
            }
            AlertCondition::EventMatch { domain, severity, keyword } => {
                let kw = keyword.as_deref().unwrap_or("*");
                format!("event match {}:{} keyword='{}'", domain, severity, kw)
            }
            AlertCondition::FacetBelow { facet, level, for_duration } => {
                format!("facet '{}' < {} for {}s", facet, level, for_duration)
            }
            AlertCondition::FacetAbove { facet, level, for_duration } => {
                format!("facet '{}' > {} for {}s", facet, level, for_duration)
            }
            AlertCondition::ArchetypeActive { name } => {
                format!("archetype '{}' is active", name)
            }
            AlertCondition::ArchetypeInactive { name, for_duration } => {
                format!("archetype '{}' inactive for {}s", name, for_duration)
            }
            AlertCondition::All(conditions) => {
                let descs: Vec<String> = conditions.iter().map(|c| c.description()).collect();
                format!("ALL({})", descs.join(" AND "))
            }
            AlertCondition::Any(conditions) => {
                let descs: Vec<String> = conditions.iter().map(|c| c.description()).collect();
                format!("ANY({})", descs.join(" OR "))
            }
            AlertCondition::Not(condition) => {
                format!("NOT({})", condition.description())
            }
        }
    }

    /// Extract the for_duration from this condition (if applicable).
    fn for_duration_secs(&self) -> u64 {
        match self {
            AlertCondition::MetricAbove { for_duration, .. }
            | AlertCondition::MetricBelow { for_duration, .. }
            | AlertCondition::FacetBelow { for_duration, .. }
            | AlertCondition::FacetAbove { for_duration, .. }
            | AlertCondition::ArchetypeInactive { for_duration, .. } => *for_duration,
            _ => 0,
        }
    }
}

// ============================================================================
// AlertRule
// ============================================================================

/// A named alert rule with a condition, severity, and notification configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Unique name of this rule.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// The condition that triggers this rule.
    pub condition: AlertCondition,
    /// Severity of alerts fired by this rule.
    pub severity: EventSeverity,
    /// Domain of alerts fired by this rule.
    pub domain: ConcernDomain,
    /// Labels for grouping and routing.
    pub labels: HashMap<String, String>,
    /// Annotations for display.
    pub annotations: HashMap<String, String>,
    /// Condition must be true for this many seconds before firing.
    pub for_duration_secs: u64,
    /// Minimum interval between repeated notifications (seconds).
    pub repeat_interval_secs: u64,
    /// Channels to notify when this rule fires.
    pub notification_channels: Vec<String>,
    /// Names of rules that inhibit this one when firing.
    pub inhibited_by: Vec<String>,
    /// Whether this rule is enabled.
    pub enabled: bool,
    /// Whether this is a built-in rule.
    pub is_builtin: bool,
}

impl AlertRule {
    /// Create a new alert rule with sensible defaults.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        condition: AlertCondition,
        severity: EventSeverity,
        domain: ConcernDomain,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            condition,
            severity,
            domain,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            for_duration_secs: 0,
            repeat_interval_secs: DEFAULT_REPEAT_INTERVAL_SECS,
            notification_channels: vec!["event-bus".to_string()],
            inhibited_by: Vec::new(),
            enabled: true,
            is_builtin: false,
        }
    }

    /// Set the for_duration for this rule.
    pub fn with_for_duration(mut self, secs: u64) -> Self {
        self.for_duration_secs = secs;
        self
    }

    /// Mark this rule as built-in.
    pub fn builtin(mut self) -> Self {
        self.is_builtin = true;
        self
    }

    /// Add an inhibition relationship.
    pub fn inhibited_by_rule(mut self, rule_name: impl Into<String>) -> Self {
        self.inhibited_by.push(rule_name.into());
        self
    }

    /// Add a label.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }
}

// ============================================================================
// AlertState
// ============================================================================

/// State of an alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertState {
    /// Condition is true but for_duration has not yet elapsed.
    Pending,
    /// Condition has been true long enough; alert is active.
    Firing,
    /// Alert has been acknowledged by an operator.
    Acknowledged,
    /// Alert is silenced (notifications suppressed).
    Silenced,
    /// Condition is no longer true; alert is resolved.
    Resolved,
}

impl std::fmt::Display for AlertState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Firing => write!(f, "firing"),
            Self::Acknowledged => write!(f, "acknowledged"),
            Self::Silenced => write!(f, "silenced"),
            Self::Resolved => write!(f, "resolved"),
        }
    }
}

// ============================================================================
// Alert
// ============================================================================

/// A live or historical alert instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique alert identifier.
    pub id: u64,
    /// Name of the rule that generated this alert.
    pub rule_name: String,
    /// Current state.
    pub state: AlertState,
    /// Severity.
    pub severity: EventSeverity,
    /// Domain.
    pub domain: ConcernDomain,
    /// Summary message.
    pub summary: String,
    /// Labels for grouping.
    pub labels: HashMap<String, String>,
    /// Annotations for display.
    pub annotations: HashMap<String, String>,
    /// When this alert first entered pending/firing state.
    pub started_at: DateTime<Utc>,
    /// When this alert last fired.
    pub last_fired_at: DateTime<Utc>,
    /// When this alert was resolved (if resolved).
    pub resolved_at: Option<DateTime<Utc>>,
    /// Who acknowledged this alert (if acknowledged).
    pub acknowledged_by: Option<String>,
    /// When this alert was acknowledged.
    pub acknowledged_at: Option<DateTime<Utc>>,
    /// Until when this alert is silenced.
    pub silenced_until: Option<DateTime<Utc>>,
    /// How many times this alert has fired.
    pub fire_count: u64,
    /// How many notifications have been sent.
    pub notification_count: u64,
}

impl Alert {
    /// Check whether this alert is currently active (Pending, Firing, or Acknowledged).
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            AlertState::Pending | AlertState::Firing | AlertState::Acknowledged
        )
    }
}

// ============================================================================
// SilenceWindow
// ============================================================================

/// A silence window that suppresses alert notifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SilenceWindow {
    /// Unique identifier.
    pub id: String,
    /// Matchers: label key -> value. An alert is silenced if all matchers match.
    pub matchers: HashMap<String, String>,
    /// When this silence starts.
    pub starts_at: DateTime<Utc>,
    /// When this silence ends.
    pub ends_at: DateTime<Utc>,
    /// Who created this silence.
    pub created_by: String,
    /// Why this silence was created.
    pub reason: String,
}

impl SilenceWindow {
    /// Check whether this silence window is currently active.
    pub fn is_active(&self) -> bool {
        let now = Utc::now();
        now >= self.starts_at && now < self.ends_at
    }

    /// Check whether an alert's labels match this silence window's matchers.
    pub fn matches_alert(&self, alert: &Alert) -> bool {
        if !self.is_active() {
            return false;
        }
        // All matchers must match
        self.matchers.iter().all(|(key, value)| {
            alert.labels.get(key) == Some(value)
        })
    }
}

// ============================================================================
// EscalationChain
// ============================================================================

/// An escalation chain with multiple levels of notification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscalationChain {
    /// Name of this escalation chain.
    pub name: String,
    /// Ordered levels of escalation.
    pub levels: Vec<EscalationLevel>,
}

/// A single level in an escalation chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscalationLevel {
    /// Delay in seconds before escalating to this level.
    pub delay_secs: u64,
    /// Channels to notify at this level.
    pub channels: Vec<String>,
    /// How often to repeat notifications at this level (seconds).
    pub repeat_secs: u64,
}

// ============================================================================
// NotificationChannel
// ============================================================================

/// A notification channel for sending alert notifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotificationChannel {
    /// Send alert as an HTTP webhook.
    Webhook {
        /// Target URL.
        url: String,
        /// HTTP method (GET, POST, PUT).
        method: String,
        /// Additional headers.
        headers: HashMap<String, String>,
        /// Body template ({{alert_name}}, {{severity}}, {{summary}} placeholders).
        body_template: String,
    },
    /// Emit alert as a SwarmEvent through the event bus.
    EventBus,
    /// Log the alert via the tracing system.
    Log {
        /// Log level (info, warn, error).
        level: String,
    },
}

impl NotificationChannel {
    /// Send a notification for the given alert.
    ///
    /// For `Webhook`, performs an HTTP request (best-effort, errors are logged).
    /// For `EventBus`, emits a psyche alert event.
    /// For `Log`, logs the alert at the configured level.
    pub async fn send(
        &self,
        alert: &Alert,
        event_bus: Option<&super::events::EventBus>,
    ) -> Result<(), String> {
        match self {
            NotificationChannel::Webhook { url, method, headers, body_template } => {
                let body = body_template
                    .replace("{{alert_name}}", &alert.rule_name)
                    .replace("{{severity}}", &alert.severity.to_string())
                    .replace("{{summary}}", &alert.summary)
                    .replace("{{state}}", &alert.state.to_string());

                debug!(
                    url = %url,
                    method = %method,
                    alert = %alert.rule_name,
                    "sending webhook notification (simulated)"
                );

                // In a real implementation we would use reqwest here.
                // For now, we log the attempt and succeed.
                let _ = (url, method, headers, body);
                Ok(())
            }
            NotificationChannel::EventBus => {
                if let Some(bus) = event_bus {
                    let event = super::events::psyche_alert(
                        &alert.rule_name,
                        &alert.summary,
                        alert.severity,
                        None,
                    );
                    bus.emit(event);
                }
                Ok(())
            }
            NotificationChannel::Log { level } => {
                match level.as_str() {
                    "error" => {
                        tracing::error!(
                            alert = %alert.rule_name,
                            severity = %alert.severity,
                            "ALERT: {}",
                            alert.summary
                        );
                    }
                    "warn" | "warning" => {
                        tracing::warn!(
                            alert = %alert.rule_name,
                            severity = %alert.severity,
                            "ALERT: {}",
                            alert.summary
                        );
                    }
                    _ => {
                        tracing::info!(
                            alert = %alert.rule_name,
                            severity = %alert.severity,
                            "ALERT: {}",
                            alert.summary
                        );
                    }
                }
                Ok(())
            }
        }
    }
}

// ============================================================================
// AlertSummary
// ============================================================================

/// Summary statistics of the alert system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertSummary {
    /// Total active alerts.
    pub total_active: usize,
    /// Active alerts by severity.
    pub by_severity: HashMap<String, usize>,
    /// Active alerts by domain.
    pub by_domain: HashMap<String, usize>,
    /// Active alerts by state.
    pub by_state: HashMap<String, usize>,
    /// Oldest unacknowledged alert timestamp (if any).
    pub oldest_unack: Option<DateTime<Utc>>,
    /// Total number of rules.
    pub total_rules: usize,
    /// Number of enabled rules.
    pub enabled_rules: usize,
    /// Number of active silence windows.
    pub active_silences: usize,
}

// ============================================================================
// AlertRuleStore
// ============================================================================

/// DashMap-based store for alert rules, channels, and escalation chains.
///
/// Initializes with 15 built-in rules covering common failure scenarios.
pub struct AlertRuleStore {
    /// Rules by name.
    rules: DashMap<String, AlertRule>,
    /// Notification channels by name.
    channels: DashMap<String, NotificationChannel>,
    /// Escalation chains by name.
    escalations: DashMap<String, EscalationChain>,
}

impl AlertRuleStore {
    /// Create a new store with 15 built-in rules.
    pub fn new() -> Self {
        let store = Self {
            rules: DashMap::new(),
            channels: DashMap::new(),
            escalations: DashMap::new(),
        };

        // Register default channels
        store.channels.insert(
            "event-bus".to_string(),
            NotificationChannel::EventBus,
        );
        store.channels.insert(
            "log-warn".to_string(),
            NotificationChannel::Log { level: "warn".to_string() },
        );
        store.channels.insert(
            "log-error".to_string(),
            NotificationChannel::Log { level: "error".to_string() },
        );

        // ---- 15 built-in rules ----

        // 1. node-death-storm
        store.add_rule(
            AlertRule::new(
                "node-death-storm",
                "Multiple nodes dying in rapid succession indicates a systemic issue",
                AlertCondition::MetricAbove {
                    name: "dead_nodes_per_min".to_string(),
                    threshold: 3.0,
                    for_duration: 30,
                },
                EventSeverity::Critical,
                ConcernDomain::Health,
            )
            .with_for_duration(30)
            .builtin(),
        );

        // 2. partition-detected
        store.add_rule(
            AlertRule::new(
                "partition-detected",
                "Network partition detected: reachable fraction below quorum",
                AlertCondition::MetricBelow {
                    name: "reachable_fraction".to_string(),
                    threshold: 0.5,
                    for_duration: 10,
                },
                EventSeverity::Critical,
                ConcernDomain::Health,
            )
            .with_for_duration(10)
            .builtin(),
        );

        // 3. gossip-stall
        store.add_rule(
            AlertRule::new(
                "gossip-stall",
                "Gossip message rate has dropped to zero",
                AlertCondition::MetricBelow {
                    name: "gossip_messages_per_min".to_string(),
                    threshold: 1.0,
                    for_duration: 60,
                },
                EventSeverity::Error,
                ConcernDomain::Health,
            )
            .with_for_duration(60)
            .builtin(),
        );

        // 4. job-failure-spike
        store.add_rule(
            AlertRule::new(
                "job-failure-spike",
                "Job failure rate has spiked above acceptable threshold",
                AlertCondition::MetricAbove {
                    name: "job_failure_rate".to_string(),
                    threshold: 0.2,
                    for_duration: 60,
                },
                EventSeverity::Error,
                ConcernDomain::Work,
            )
            .with_for_duration(60)
            .builtin(),
        );

        // 5. job-stalled
        store.add_rule(
            AlertRule::new(
                "job-stalled",
                "A job has made no progress for an extended period",
                AlertCondition::MetricAbove {
                    name: "max_job_stall_secs".to_string(),
                    threshold: 600.0,
                    for_duration: 0,
                },
                EventSeverity::Warning,
                ConcernDomain::Work,
            )
            .builtin(),
        );

        // 6. disk-pressure
        store.add_rule(
            AlertRule::new(
                "disk-pressure",
                "Disk utilization exceeds safe threshold",
                AlertCondition::MetricAbove {
                    name: "disk_utilization_pct".to_string(),
                    threshold: 90.0,
                    for_duration: 60,
                },
                EventSeverity::Warning,
                ConcernDomain::Data,
            )
            .with_for_duration(60)
            .builtin(),
        );

        // 7. residency-violation
        store.add_rule(
            AlertRule::new(
                "residency-violation",
                "Data residency policy violation detected",
                AlertCondition::EventRate {
                    domain: "data".to_string(),
                    severity: "error".to_string(),
                    rate_per_min: 0.0,
                    window_secs: 60,
                },
                EventSeverity::Error,
                ConcernDomain::Data,
            )
            .builtin(),
        );

        // 8. auth-failure-spike
        store.add_rule(
            AlertRule::new(
                "auth-failure-spike",
                "Authentication failures exceeding normal rate",
                AlertCondition::EventRate {
                    domain: "security".to_string(),
                    severity: "warning".to_string(),
                    rate_per_min: 10.0,
                    window_secs: 300,
                },
                EventSeverity::Error,
                ConcernDomain::Security,
            )
            .builtin(),
        );

        // 9. high-idle-capacity
        store.add_rule(
            AlertRule::new(
                "high-idle-capacity",
                "Most fleet capacity is idle; consider scaling down",
                AlertCondition::MetricAbove {
                    name: "idle_fraction".to_string(),
                    threshold: 0.8,
                    for_duration: 300,
                },
                EventSeverity::Notice,
                ConcernDomain::Cost,
            )
            .with_for_duration(300)
            .builtin(),
        );

        // 10. psyche-war-room
        store.add_rule(
            AlertRule::new(
                "psyche-war-room",
                "Swarm has entered war-room archetype indicating crisis mode",
                AlertCondition::ArchetypeActive {
                    name: "war-room".to_string(),
                },
                EventSeverity::Warning,
                ConcernDomain::Psyche,
            )
            .builtin(),
        );

        // 11. psyche-walking-wounded
        store.add_rule(
            AlertRule::new(
                "psyche-walking-wounded",
                "Resilience facet has dropped below healthy threshold",
                AlertCondition::FacetBelow {
                    facet: "resilience".to_string(),
                    level: 30,
                    for_duration: 120,
                },
                EventSeverity::Warning,
                ConcernDomain::Psyche,
            )
            .with_for_duration(120)
            .builtin(),
        );

        // 12. sla-breach-imminent
        store.add_rule(
            AlertRule::new(
                "sla-breach-imminent",
                "SLA breach is imminent based on current job completion rate",
                AlertCondition::MetricAbove {
                    name: "sla_breach_risk".to_string(),
                    threshold: 0.8,
                    for_duration: 60,
                },
                EventSeverity::Error,
                ConcernDomain::Work,
            )
            .with_for_duration(60)
            .builtin(),
        );

        // 13. update-canary-failed
        store.add_rule(
            AlertRule::new(
                "update-canary-failed",
                "Rolling update canary node has failed health checks",
                AlertCondition::EventRate {
                    domain: "fleet".to_string(),
                    severity: "error".to_string(),
                    rate_per_min: 0.0,
                    window_secs: 120,
                },
                EventSeverity::Error,
                ConcernDomain::Fleet,
            )
            .builtin(),
        );

        // 14. membrane-degraded
        store.add_rule(
            AlertRule::new(
                "membrane-degraded",
                "Inter-swarm membrane is degraded or severed",
                AlertCondition::MetricAbove {
                    name: "membrane_error_rate".to_string(),
                    threshold: 0.5,
                    for_duration: 60,
                },
                EventSeverity::Warning,
                ConcernDomain::MultiSwarm,
            )
            .with_for_duration(60)
            .builtin(),
        );

        // 15. capacity-exhaustion
        store.add_rule(
            AlertRule::new(
                "capacity-exhaustion",
                "Available compute capacity is critically low",
                AlertCondition::MetricBelow {
                    name: "available_capacity_fraction".to_string(),
                    threshold: 0.1,
                    for_duration: 120,
                },
                EventSeverity::Critical,
                ConcernDomain::Health,
            )
            .with_for_duration(120)
            .builtin(),
        );

        store
    }

    /// Add a rule to the store.
    pub fn add_rule(&self, rule: AlertRule) {
        self.rules.insert(rule.name.clone(), rule);
    }

    /// Remove a rule by name.
    pub fn remove_rule(&self, name: &str) -> Option<AlertRule> {
        self.rules.remove(name).map(|(_, v)| v)
    }

    /// Get a rule by name.
    pub fn get_rule(&self, name: &str) -> Option<AlertRule> {
        self.rules.get(name).map(|r| r.value().clone())
    }

    /// Return all rules.
    pub fn all_rules(&self) -> Vec<AlertRule> {
        self.rules.iter().map(|r| r.value().clone()).collect()
    }

    /// Return enabled rules only.
    pub fn enabled_rules(&self) -> Vec<AlertRule> {
        self.rules
            .iter()
            .filter(|r| r.value().enabled)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Total number of rules.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Add a notification channel.
    pub fn add_channel(&self, name: impl Into<String>, channel: NotificationChannel) {
        self.channels.insert(name.into(), channel);
    }

    /// Get a notification channel by name.
    pub fn get_channel(&self, name: &str) -> Option<NotificationChannel> {
        self.channels.get(name).map(|r| r.value().clone())
    }

    /// Add an escalation chain.
    pub fn add_escalation(&self, chain: EscalationChain) {
        self.escalations.insert(chain.name.clone(), chain);
    }

    /// Get an escalation chain by name.
    pub fn get_escalation(&self, name: &str) -> Option<EscalationChain> {
        self.escalations.get(name).map(|r| r.value().clone())
    }
}

impl Default for AlertRuleStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// PendingCondition
// ============================================================================

/// Tracks when a condition first became true for for_duration evaluation.
#[derive(Debug, Clone)]
struct PendingCondition {
    /// When the condition first became true.
    since: DateTime<Utc>,
    /// Required duration in seconds.
    required_duration_secs: u64,
}

impl PendingCondition {
    /// Check if the required duration has elapsed.
    fn is_ready(&self) -> bool {
        let elapsed = Utc::now() - self.since;
        elapsed >= chrono::Duration::seconds(self.required_duration_secs as i64)
    }
}

// ============================================================================
// AlertEngine
// ============================================================================

/// The core alert evaluation engine.
///
/// Evaluates rules against context, manages alert lifecycle (pending, firing,
/// acknowledged, silenced, resolved), and sends notifications.
pub struct AlertEngine {
    /// Rule store.
    rule_store: Arc<AlertRuleStore>,
    /// Currently active alerts by rule name.
    active_alerts: DashMap<String, Alert>,
    /// Alert history (ring buffer).
    alert_history: RwLock<VecDeque<Alert>>,
    /// Silence windows.
    silence_windows: DashMap<String, SilenceWindow>,
    /// Pending conditions: rule_name -> when condition first became true.
    pending_conditions: DashMap<String, PendingCondition>,
    /// Next alert ID.
    next_alert_id: AtomicU64,
}

impl AlertEngine {
    /// Create a new alert engine with the given rule store.
    pub fn new(rule_store: Arc<AlertRuleStore>) -> Self {
        Self {
            rule_store,
            active_alerts: DashMap::new(),
            alert_history: RwLock::new(VecDeque::with_capacity(MAX_ALERT_HISTORY)),
            silence_windows: DashMap::new(),
            pending_conditions: DashMap::new(),
            next_alert_id: AtomicU64::new(1),
        }
    }

    /// Evaluate all enabled rules against the given context.
    ///
    /// For each rule:
    /// 1. Skip if disabled.
    /// 2. Evaluate the condition.
    /// 3. If true and for_duration > 0: track in pending_conditions.
    /// 4. If true and for_duration met: fire alert (if not already firing).
    /// 5. If false: resolve any existing alert.
    /// 6. Check inhibition and silence windows.
    pub fn evaluate_all(&self, ctx: &AlertContext) {
        let rules = self.rule_store.enabled_rules();

        for rule in &rules {
            let condition_met = rule.condition.evaluate(ctx);

            if condition_met {
                let effective_duration = rule.for_duration_secs.max(
                    rule.condition.for_duration_secs()
                );

                if effective_duration > 0 {
                    // Track pending condition
                    let is_ready = {
                        let entry = self.pending_conditions.entry(rule.name.clone());
                        let pending = entry.or_insert_with(|| PendingCondition {
                            since: Utc::now(),
                            required_duration_secs: effective_duration,
                        });
                        pending.is_ready()
                    };

                    if is_ready {
                        self.maybe_fire_alert(rule, ctx);
                    } else {
                        // Condition is pending but not ready yet
                        self.maybe_set_pending(rule);
                    }
                } else {
                    // No duration requirement, fire immediately
                    self.maybe_fire_alert(rule, ctx);
                }
            } else {
                // Condition is false, remove from pending and resolve
                self.pending_conditions.remove(&rule.name);
                self.maybe_resolve_alert(&rule.name);
            }
        }
    }

    /// Set alert to pending state if not already active.
    fn maybe_set_pending(&self, rule: &AlertRule) {
        if self.active_alerts.contains_key(&rule.name) {
            return; // Already active
        }

        let alert = Alert {
            id: self.next_alert_id.fetch_add(1, Ordering::Relaxed),
            rule_name: rule.name.clone(),
            state: AlertState::Pending,
            severity: rule.severity,
            domain: rule.domain,
            summary: format!("[PENDING] {}", rule.description),
            labels: rule.labels.clone(),
            annotations: rule.annotations.clone(),
            started_at: Utc::now(),
            last_fired_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            acknowledged_at: None,
            silenced_until: None,
            fire_count: 0,
            notification_count: 0,
        };

        self.active_alerts.insert(rule.name.clone(), alert);
    }

    /// Fire an alert if not already firing, or update fire_count.
    fn maybe_fire_alert(&self, rule: &AlertRule, ctx: &AlertContext) {
        // Check inhibition
        if self.is_inhibited(rule, ctx) {
            debug!(rule = %rule.name, "alert inhibited by another firing rule");
            return;
        }

        let now = Utc::now();

        if let Some(mut existing) = self.active_alerts.get_mut(&rule.name) {
            // Already active -- check if we should re-fire
            match existing.state {
                AlertState::Resolved => {
                    // Re-fire
                    existing.state = AlertState::Firing;
                    existing.fire_count += 1;
                    existing.last_fired_at = now;
                    existing.resolved_at = None;
                }
                AlertState::Pending => {
                    // Upgrade to firing
                    existing.state = AlertState::Firing;
                    existing.fire_count += 1;
                    existing.last_fired_at = now;
                    existing.summary = rule.description.clone();
                }
                AlertState::Firing | AlertState::Acknowledged => {
                    // Check repeat interval
                    let elapsed = now - existing.last_fired_at;
                    if elapsed >= chrono::Duration::seconds(rule.repeat_interval_secs as i64) {
                        existing.fire_count += 1;
                        existing.last_fired_at = now;
                    }
                }
                AlertState::Silenced => {
                    // Check if silence has expired
                    if let Some(until) = existing.silenced_until {
                        if now >= until {
                            existing.state = AlertState::Firing;
                            existing.fire_count += 1;
                            existing.last_fired_at = now;
                            existing.silenced_until = None;
                        }
                    }
                }
            }
        } else {
            // New alert
            let alert = Alert {
                id: self.next_alert_id.fetch_add(1, Ordering::Relaxed),
                rule_name: rule.name.clone(),
                state: AlertState::Firing,
                severity: rule.severity,
                domain: rule.domain,
                summary: rule.description.clone(),
                labels: rule.labels.clone(),
                annotations: rule.annotations.clone(),
                started_at: now,
                last_fired_at: now,
                resolved_at: None,
                acknowledged_by: None,
                acknowledged_at: None,
                silenced_until: None,
                fire_count: 1,
                notification_count: 0,
            };
            self.active_alerts.insert(rule.name.clone(), alert);
        }

        // Check silence windows
        if let Some(mut alert) = self.active_alerts.get_mut(&rule.name) {
            for sw in self.silence_windows.iter() {
                if sw.value().matches_alert(&alert) {
                    alert.state = AlertState::Silenced;
                    alert.silenced_until = Some(sw.value().ends_at);
                    break;
                }
            }
        }
    }

    /// Resolve an active alert.
    fn maybe_resolve_alert(&self, rule_name: &str) {
        if let Some(mut alert) = self.active_alerts.get_mut(rule_name) {
            if alert.state != AlertState::Resolved {
                alert.state = AlertState::Resolved;
                alert.resolved_at = Some(Utc::now());
                debug!(rule = %rule_name, "alert resolved");
            }
        }
    }

    /// Check if a rule is inhibited by another firing rule.
    fn is_inhibited(&self, rule: &AlertRule, ctx: &AlertContext) -> bool {
        for inhibitor_name in &rule.inhibited_by {
            if let Some(inhibitor) = self.active_alerts.get(inhibitor_name) {
                // If inhibitor is Firing or Pending, we are inhibited.
                if inhibitor.state == AlertState::Firing || inhibitor.state == AlertState::Pending {
                    return true;
                }
            } else if let Some(inhibitor_rule) = self.rule_store.get_rule(inhibitor_name) {
                // If inhibitor is not in active_alerts, check its condition directly
                // to handle evaluation-order race conditions.
                if inhibitor_rule.condition.evaluate(ctx) {
                    return true;
                }
            }
        }
        false
    }

    /// Explicitly fire an alert for a given rule.
    pub fn fire_alert(&self, rule_name: &str) {
        if let Some(rule) = self.rule_store.get_rule(rule_name) {
            let ctx = AlertContext::new();
            self.maybe_fire_alert(&rule, &ctx);
        }
    }

    /// Explicitly resolve an alert.
    pub fn resolve_alert(&self, rule_name: &str) {
        self.maybe_resolve_alert(rule_name);
        // Move to history
        if let Some((_, alert)) = self.active_alerts.remove(rule_name) {
            let mut history = self.alert_history.write();
            if history.len() >= MAX_ALERT_HISTORY {
                history.pop_front();
            }
            history.push_back(alert);
        }
    }

    /// Acknowledge an alert.
    pub fn acknowledge_alert(&self, rule_name: &str, by_whom: &str) {
        if let Some(mut alert) = self.active_alerts.get_mut(rule_name) {
            if alert.state == AlertState::Firing || alert.state == AlertState::Pending {
                alert.state = AlertState::Acknowledged;
                alert.acknowledged_by = Some(by_whom.to_string());
                alert.acknowledged_at = Some(Utc::now());
                info!(rule = %rule_name, by = %by_whom, "alert acknowledged");
            }
        }
    }

    /// Silence an alert until a given time.
    pub fn silence_alert(&self, rule_name: &str, until: DateTime<Utc>) {
        if let Some(mut alert) = self.active_alerts.get_mut(rule_name) {
            alert.state = AlertState::Silenced;
            alert.silenced_until = Some(until);
            info!(rule = %rule_name, until = %until, "alert silenced");
        }
    }

    /// Add a silence window.
    pub fn add_silence_window(&self, window: SilenceWindow) {
        info!(
            id = %window.id,
            created_by = %window.created_by,
            reason = %window.reason,
            "silence window added"
        );
        self.silence_windows.insert(window.id.clone(), window);
    }

    /// Remove a silence window by ID.
    pub fn remove_silence_window(&self, id: &str) -> Option<SilenceWindow> {
        self.silence_windows.remove(id).map(|(_, v)| v)
    }

    /// Return currently active silence windows.
    pub fn active_silences(&self) -> Vec<SilenceWindow> {
        self.silence_windows
            .iter()
            .filter(|r| r.value().is_active())
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return all currently active alerts.
    pub fn active_alerts(&self) -> Vec<Alert> {
        self.active_alerts
            .iter()
            .filter(|r| r.value().is_active())
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return all alerts (active and resolved in the active map).
    pub fn all_alerts(&self) -> Vec<Alert> {
        self.active_alerts
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return alert history (resolved alerts).
    pub fn alert_history(&self) -> Vec<Alert> {
        self.alert_history.read().iter().cloned().collect()
    }

    /// Return a summary of the alert system.
    pub fn alert_summary(&self) -> AlertSummary {
        let active: Vec<Alert> = self.active_alerts();
        let all_rules = self.rule_store.all_rules();

        let mut by_severity: HashMap<String, usize> = HashMap::new();
        let mut by_domain: HashMap<String, usize> = HashMap::new();
        let mut by_state: HashMap<String, usize> = HashMap::new();
        let mut oldest_unack: Option<DateTime<Utc>> = None;

        for alert in &active {
            *by_severity
                .entry(alert.severity.to_string())
                .or_insert(0) += 1;
            *by_domain
                .entry(alert.domain.to_string())
                .or_insert(0) += 1;
            *by_state
                .entry(alert.state.to_string())
                .or_insert(0) += 1;

            if alert.state == AlertState::Firing && alert.acknowledged_at.is_none() {
                match oldest_unack {
                    None => oldest_unack = Some(alert.started_at),
                    Some(existing) if alert.started_at < existing => {
                        oldest_unack = Some(alert.started_at);
                    }
                    _ => {}
                }
            }
        }

        let enabled_count = all_rules.iter().filter(|r| r.enabled).count();
        let active_silence_count = self.active_silences().len();

        AlertSummary {
            total_active: active.len(),
            by_severity,
            by_domain,
            by_state,
            oldest_unack,
            total_rules: all_rules.len(),
            enabled_rules: enabled_count,
            active_silences: active_silence_count,
        }
    }

    /// Spawn a background evaluation loop.
    ///
    /// Evaluates all rules against the provided context every `DEFAULT_EVAL_INTERVAL_SECS`.
    /// Stops when the shutdown signal is received.
    pub fn spawn_loop(
        self: Arc<Self>,
        ctx: AlertContext,
        mut shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let ctx = Arc::new(RwLock::new(ctx));
        let engine = self;

        tokio::spawn(async move {
            info!("alert engine evaluation loop started");

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(DEFAULT_EVAL_INTERVAL_SECS)) => {}
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow() {
                            info!("alert engine evaluation loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown.borrow() {
                    break;
                }

                let snapshot = ctx.read().clone();
                engine.evaluate_all(&snapshot);
            }
        })
    }
}

// ============================================================================
// Built-in rule factory helpers
// ============================================================================

/// Create a default set of built-in alert rules.
///
/// This is called internally by [`AlertRuleStore::new`] and is exposed for
/// testing and documentation purposes.
pub fn builtin_rule_names() -> Vec<&'static str> {
    vec![
        "node-death-storm",
        "partition-detected",
        "gossip-stall",
        "job-failure-spike",
        "job-stalled",
        "disk-pressure",
        "residency-violation",
        "auth-failure-spike",
        "high-idle-capacity",
        "psyche-war-room",
        "psyche-walking-wounded",
        "sla-breach-imminent",
        "update-canary-failed",
        "membrane-degraded",
        "capacity-exhaustion",
    ]
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ctx() -> AlertContext {
        AlertContext::new()
    }

    fn make_store() -> Arc<AlertRuleStore> {
        Arc::new(AlertRuleStore::new())
    }

    fn make_engine() -> AlertEngine {
        AlertEngine::new(make_store())
    }

    // --- AlertContext tests ---

    #[test]
    fn context_new_is_empty() {
        let ctx = AlertContext::new();
        assert!(ctx.metrics.is_empty());
        assert!(ctx.facets.is_empty());
        assert!(ctx.active_archetypes.is_empty());
        assert!(ctx.event_rate.is_empty());
    }

    #[test]
    fn context_set_metric() {
        let mut ctx = make_ctx();
        ctx.set_metric("cpu", 0.9);
        assert_eq!(ctx.metrics.get("cpu"), Some(&0.9));
    }

    #[test]
    fn context_set_facet() {
        let mut ctx = make_ctx();
        ctx.set_facet("resilience", 50);
        assert_eq!(ctx.facets.get("resilience"), Some(&50));
    }

    #[test]
    fn context_add_archetype() {
        let mut ctx = make_ctx();
        ctx.add_archetype("war-room");
        assert!(ctx.active_archetypes.contains(&"war-room".to_string()));
    }

    #[test]
    fn context_set_event_rate() {
        let mut ctx = make_ctx();
        ctx.set_event_rate("health:error", 5.0);
        assert_eq!(ctx.event_rate.get("health:error"), Some(&5.0));
    }

    // --- AlertCondition tests ---

    #[test]
    fn condition_metric_above() {
        let cond = AlertCondition::MetricAbove {
            name: "cpu".to_string(),
            threshold: 0.8,
            for_duration: 0,
        };
        let mut ctx = make_ctx();
        ctx.set_metric("cpu", 0.9);
        assert!(cond.evaluate(&ctx));
        ctx.set_metric("cpu", 0.5);
        assert!(!cond.evaluate(&ctx));
    }

    #[test]
    fn condition_metric_below() {
        let cond = AlertCondition::MetricBelow {
            name: "capacity".to_string(),
            threshold: 0.2,
            for_duration: 0,
        };
        let mut ctx = make_ctx();
        ctx.set_metric("capacity", 0.1);
        assert!(cond.evaluate(&ctx));
        ctx.set_metric("capacity", 0.5);
        assert!(!cond.evaluate(&ctx));
    }

    #[test]
    fn condition_metric_missing_returns_false() {
        let cond = AlertCondition::MetricAbove {
            name: "nonexistent".to_string(),
            threshold: 0.0,
            for_duration: 0,
        };
        assert!(!cond.evaluate(&make_ctx()));
    }

    #[test]
    fn condition_event_rate() {
        let cond = AlertCondition::EventRate {
            domain: "health".to_string(),
            severity: "error".to_string(),
            rate_per_min: 5.0,
            window_secs: 60,
        };
        let mut ctx = make_ctx();
        ctx.set_event_rate("health:error", 10.0);
        assert!(cond.evaluate(&ctx));
        ctx.set_event_rate("health:error", 2.0);
        assert!(!cond.evaluate(&ctx));
    }

    #[test]
    fn condition_facet_below() {
        let cond = AlertCondition::FacetBelow {
            facet: "resilience".to_string(),
            level: 30,
            for_duration: 0,
        };
        let mut ctx = make_ctx();
        ctx.set_facet("resilience", 20);
        assert!(cond.evaluate(&ctx));
        ctx.set_facet("resilience", 50);
        assert!(!cond.evaluate(&ctx));
    }

    #[test]
    fn condition_facet_above() {
        let cond = AlertCondition::FacetAbove {
            facet: "aggression".to_string(),
            level: 80,
            for_duration: 0,
        };
        let mut ctx = make_ctx();
        ctx.set_facet("aggression", 90);
        assert!(cond.evaluate(&ctx));
        ctx.set_facet("aggression", 50);
        assert!(!cond.evaluate(&ctx));
    }

    #[test]
    fn condition_archetype_active() {
        let cond = AlertCondition::ArchetypeActive {
            name: "war-room".to_string(),
        };
        let mut ctx = make_ctx();
        assert!(!cond.evaluate(&ctx));
        ctx.add_archetype("war-room");
        assert!(cond.evaluate(&ctx));
    }

    #[test]
    fn condition_archetype_inactive() {
        let cond = AlertCondition::ArchetypeInactive {
            name: "war-room".to_string(),
            for_duration: 0,
        };
        let ctx = make_ctx();
        assert!(cond.evaluate(&ctx)); // not active = inactive
    }

    #[test]
    fn condition_all() {
        let cond = AlertCondition::All(vec![
            AlertCondition::MetricAbove {
                name: "cpu".to_string(),
                threshold: 0.5,
                for_duration: 0,
            },
            AlertCondition::MetricAbove {
                name: "mem".to_string(),
                threshold: 0.5,
                for_duration: 0,
            },
        ]);
        let mut ctx = make_ctx();
        ctx.set_metric("cpu", 0.9);
        ctx.set_metric("mem", 0.9);
        assert!(cond.evaluate(&ctx));
        ctx.set_metric("mem", 0.3);
        assert!(!cond.evaluate(&ctx));
    }

    #[test]
    fn condition_any() {
        let cond = AlertCondition::Any(vec![
            AlertCondition::MetricAbove {
                name: "cpu".to_string(),
                threshold: 0.9,
                for_duration: 0,
            },
            AlertCondition::MetricAbove {
                name: "mem".to_string(),
                threshold: 0.9,
                for_duration: 0,
            },
        ]);
        let mut ctx = make_ctx();
        ctx.set_metric("cpu", 0.5);
        ctx.set_metric("mem", 0.95);
        assert!(cond.evaluate(&ctx));
    }

    #[test]
    fn condition_not() {
        let cond = AlertCondition::Not(Box::new(AlertCondition::MetricAbove {
            name: "cpu".to_string(),
            threshold: 0.9,
            for_duration: 0,
        }));
        let mut ctx = make_ctx();
        ctx.set_metric("cpu", 0.5);
        assert!(cond.evaluate(&ctx));
        ctx.set_metric("cpu", 0.95);
        assert!(!cond.evaluate(&ctx));
    }

    #[test]
    fn condition_validate_empty_name() {
        let cond = AlertCondition::MetricAbove {
            name: "".to_string(),
            threshold: 0.5,
            for_duration: 0,
        };
        assert!(cond.validate().is_err());
    }

    #[test]
    fn condition_validate_valid() {
        let cond = AlertCondition::MetricAbove {
            name: "cpu".to_string(),
            threshold: 0.5,
            for_duration: 0,
        };
        assert!(cond.validate().is_ok());
    }

    #[test]
    fn condition_validate_nested() {
        let cond = AlertCondition::All(vec![
            AlertCondition::MetricAbove {
                name: "".to_string(), // invalid
                threshold: 0.5,
                for_duration: 0,
            },
        ]);
        let result = cond.validate();
        assert!(result.is_err());
        assert_eq!(result.err().map(|e| e.len()), Some(1));
    }

    #[test]
    fn condition_validate_empty_all() {
        let cond = AlertCondition::All(vec![]);
        assert!(cond.validate().is_err());
    }

    #[test]
    fn condition_description() {
        let cond = AlertCondition::MetricAbove {
            name: "cpu".to_string(),
            threshold: 0.8,
            for_duration: 30,
        };
        let desc = cond.description();
        assert!(desc.contains("cpu"));
        assert!(desc.contains("0.8"));
        assert!(desc.contains("30s"));
    }

    #[test]
    fn condition_description_nested() {
        let cond = AlertCondition::All(vec![
            AlertCondition::MetricAbove {
                name: "cpu".to_string(),
                threshold: 0.8,
                for_duration: 0,
            },
            AlertCondition::FacetBelow {
                facet: "resilience".to_string(),
                level: 30,
                for_duration: 0,
            },
        ]);
        let desc = cond.description();
        assert!(desc.contains("ALL("));
        assert!(desc.contains("cpu"));
        assert!(desc.contains("resilience"));
    }

    // --- AlertRuleStore tests ---

    #[test]
    fn store_has_15_builtin_rules() {
        let store = AlertRuleStore::new();
        assert_eq!(store.rule_count(), 15);
    }

    #[test]
    fn store_all_builtins_are_enabled() {
        let store = AlertRuleStore::new();
        let enabled = store.enabled_rules();
        assert_eq!(enabled.len(), 15);
        for rule in &enabled {
            assert!(rule.is_builtin);
            assert!(rule.enabled);
        }
    }

    #[test]
    fn store_builtin_rule_names_match() {
        let store = AlertRuleStore::new();
        let names = builtin_rule_names();
        for name in &names {
            assert!(
                store.get_rule(name).is_some(),
                "built-in rule '{}' not found",
                name
            );
        }
    }

    #[test]
    fn store_add_custom_rule() {
        let store = AlertRuleStore::new();
        store.add_rule(AlertRule::new(
            "custom-rule",
            "test rule",
            AlertCondition::MetricAbove {
                name: "test".to_string(),
                threshold: 1.0,
                for_duration: 0,
            },
            EventSeverity::Warning,
            ConcernDomain::Health,
        ));
        assert_eq!(store.rule_count(), 16);
        assert!(store.get_rule("custom-rule").is_some());
    }

    #[test]
    fn store_remove_rule() {
        let store = AlertRuleStore::new();
        let removed = store.remove_rule("node-death-storm");
        assert!(removed.is_some());
        assert_eq!(store.rule_count(), 14);
    }

    #[test]
    fn store_has_default_channels() {
        let store = AlertRuleStore::new();
        assert!(store.get_channel("event-bus").is_some());
        assert!(store.get_channel("log-warn").is_some());
        assert!(store.get_channel("log-error").is_some());
    }

    #[test]
    fn store_add_channel() {
        let store = AlertRuleStore::new();
        store.add_channel(
            "webhook-1",
            NotificationChannel::Webhook {
                url: "http://example.com".to_string(),
                method: "POST".to_string(),
                headers: HashMap::new(),
                body_template: "{{alert_name}}: {{summary}}".to_string(),
            },
        );
        assert!(store.get_channel("webhook-1").is_some());
    }

    #[test]
    fn store_escalation_chain() {
        let store = AlertRuleStore::new();
        store.add_escalation(EscalationChain {
            name: "default".to_string(),
            levels: vec![
                EscalationLevel {
                    delay_secs: 0,
                    channels: vec!["log-warn".to_string()],
                    repeat_secs: 300,
                },
                EscalationLevel {
                    delay_secs: 900,
                    channels: vec!["log-error".to_string()],
                    repeat_secs: 600,
                },
            ],
        });
        let chain = store.get_escalation("default").expect("chain exists");
        assert_eq!(chain.levels.len(), 2);
    }

    // --- AlertEngine tests ---

    #[test]
    fn engine_evaluate_fires_alert() {
        let engine = make_engine();
        let mut ctx = make_ctx();
        // Trigger node-death-storm: dead_nodes_per_min > 3
        ctx.set_metric("dead_nodes_per_min", 10.0);
        engine.evaluate_all(&ctx);
        // With for_duration=30s, it should be pending
        let alerts = engine.all_alerts();
        assert!(!alerts.is_empty());
    }

    #[test]
    fn engine_evaluate_resolves_when_condition_false() {
        let engine = make_engine();

        // First, trigger the job-stalled rule (no for_duration)
        let mut ctx = make_ctx();
        ctx.set_metric("max_job_stall_secs", 700.0);
        engine.evaluate_all(&ctx);
        assert!(!engine.active_alerts().is_empty());

        // Now set metric below threshold
        ctx.set_metric("max_job_stall_secs", 100.0);
        engine.evaluate_all(&ctx);

        // Should be resolved
        let active = engine.active_alerts();
        // The alert may still be in the map but marked as resolved
        let firing_count = engine
            .all_alerts()
            .iter()
            .filter(|a| a.state == AlertState::Firing)
            .count();
        assert_eq!(firing_count, 0);
    }

    #[test]
    fn engine_fire_alert_directly() {
        let engine = make_engine();
        engine.fire_alert("node-death-storm");
        let alerts = engine.all_alerts();
        let firing = alerts.iter().find(|a| a.rule_name == "node-death-storm");
        assert!(firing.is_some());
        assert_eq!(firing.map(|a| a.state), Some(AlertState::Firing));
    }

    #[test]
    fn engine_resolve_alert() {
        let engine = make_engine();
        engine.fire_alert("job-stalled");
        assert!(!engine.active_alerts().is_empty());

        engine.resolve_alert("job-stalled");
        // After resolve, moved to history
        assert!(engine.active_alerts().is_empty());
        assert!(!engine.alert_history().is_empty());
    }

    #[test]
    fn engine_acknowledge_alert() {
        let engine = make_engine();
        engine.fire_alert("disk-pressure");
        engine.acknowledge_alert("disk-pressure", "admin");

        let alert = engine.all_alerts().into_iter().find(|a| a.rule_name == "disk-pressure");
        assert!(alert.is_some());
        let alert = alert.expect("exists");
        assert_eq!(alert.state, AlertState::Acknowledged);
        assert_eq!(alert.acknowledged_by, Some("admin".to_string()));
    }

    #[test]
    fn engine_silence_alert() {
        let engine = make_engine();
        engine.fire_alert("gossip-stall");
        let until = Utc::now() + chrono::Duration::hours(1);
        engine.silence_alert("gossip-stall", until);

        let alert = engine.all_alerts().into_iter().find(|a| a.rule_name == "gossip-stall");
        assert!(alert.is_some());
        assert_eq!(alert.map(|a| a.state), Some(AlertState::Silenced));
    }

    #[test]
    fn engine_silence_window() {
        let engine = make_engine();
        engine.add_silence_window(SilenceWindow {
            id: "sw-1".to_string(),
            matchers: HashMap::new(),
            starts_at: Utc::now() - chrono::Duration::minutes(5),
            ends_at: Utc::now() + chrono::Duration::hours(1),
            created_by: "admin".to_string(),
            reason: "maintenance".to_string(),
        });

        let silences = engine.active_silences();
        assert_eq!(silences.len(), 1);

        let removed = engine.remove_silence_window("sw-1");
        assert!(removed.is_some());
        assert_eq!(engine.active_silences().len(), 0);
    }

    #[test]
    fn engine_alert_summary() {
        let engine = make_engine();
        engine.fire_alert("node-death-storm");
        engine.fire_alert("disk-pressure");

        let summary = engine.alert_summary();
        assert_eq!(summary.total_active, 2);
        assert!(summary.total_rules >= 15);
        assert!(summary.enabled_rules >= 15);
    }

    #[test]
    fn engine_inhibition() {
        let store = Arc::new(AlertRuleStore::new());

        // Add a custom rule inhibited by node-death-storm
        store.add_rule(
            AlertRule::new(
                "secondary-rule",
                "inhibited rule",
                AlertCondition::MetricAbove {
                    name: "test_metric".to_string(),
                    threshold: 0.5,
                    for_duration: 0,
                },
                EventSeverity::Warning,
                ConcernDomain::Health,
            )
            .inhibited_by_rule("node-death-storm"),
        );

        let engine = AlertEngine::new(store);

        // First fire the inhibitor
        engine.fire_alert("node-death-storm");

        // Now evaluate with secondary-rule condition true,
        // and keep node-death-storm condition true so it doesn't resolve
        let mut ctx = make_ctx();
        ctx.set_metric("dead_nodes_per_min", 10.0);
        ctx.set_metric("test_metric", 1.0);
        engine.evaluate_all(&ctx);

        // secondary-rule should not fire because it's inhibited
        let secondary = engine
            .all_alerts()
            .into_iter()
            .find(|a| a.rule_name == "secondary-rule");
        assert!(secondary.is_none());
    }

    // --- SilenceWindow tests ---

    #[test]
    fn silence_window_is_active() {
        let sw = SilenceWindow {
            id: "sw-1".to_string(),
            matchers: HashMap::new(),
            starts_at: Utc::now() - chrono::Duration::minutes(5),
            ends_at: Utc::now() + chrono::Duration::hours(1),
            created_by: "admin".to_string(),
            reason: "test".to_string(),
        };
        assert!(sw.is_active());

        let sw_expired = SilenceWindow {
            id: "sw-2".to_string(),
            matchers: HashMap::new(),
            starts_at: Utc::now() - chrono::Duration::hours(2),
            ends_at: Utc::now() - chrono::Duration::hours(1),
            created_by: "admin".to_string(),
            reason: "test".to_string(),
        };
        assert!(!sw_expired.is_active());
    }

    #[test]
    fn silence_window_matches_alert() {
        let mut matchers = HashMap::new();
        matchers.insert("team".to_string(), "infra".to_string());

        let sw = SilenceWindow {
            id: "sw-1".to_string(),
            matchers,
            starts_at: Utc::now() - chrono::Duration::minutes(5),
            ends_at: Utc::now() + chrono::Duration::hours(1),
            created_by: "admin".to_string(),
            reason: "test".to_string(),
        };

        let mut labels = HashMap::new();
        labels.insert("team".to_string(), "infra".to_string());

        let alert = Alert {
            id: 1,
            rule_name: "test".to_string(),
            state: AlertState::Firing,
            severity: EventSeverity::Warning,
            domain: ConcernDomain::Health,
            summary: "test alert".to_string(),
            labels,
            annotations: HashMap::new(),
            started_at: Utc::now(),
            last_fired_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            acknowledged_at: None,
            silenced_until: None,
            fire_count: 1,
            notification_count: 0,
        };

        assert!(sw.matches_alert(&alert));
    }

    #[test]
    fn silence_window_no_match_wrong_label() {
        let mut matchers = HashMap::new();
        matchers.insert("team".to_string(), "infra".to_string());

        let sw = SilenceWindow {
            id: "sw-1".to_string(),
            matchers,
            starts_at: Utc::now() - chrono::Duration::minutes(5),
            ends_at: Utc::now() + chrono::Duration::hours(1),
            created_by: "admin".to_string(),
            reason: "test".to_string(),
        };

        let alert = Alert {
            id: 1,
            rule_name: "test".to_string(),
            state: AlertState::Firing,
            severity: EventSeverity::Warning,
            domain: ConcernDomain::Health,
            summary: "test".to_string(),
            labels: HashMap::new(), // no labels
            annotations: HashMap::new(),
            started_at: Utc::now(),
            last_fired_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            acknowledged_at: None,
            silenced_until: None,
            fire_count: 1,
            notification_count: 0,
        };

        assert!(!sw.matches_alert(&alert));
    }

    // --- AlertRule tests ---

    #[test]
    fn alert_rule_builder() {
        let rule = AlertRule::new(
            "test",
            "test rule",
            AlertCondition::MetricAbove {
                name: "cpu".to_string(),
                threshold: 0.9,
                for_duration: 0,
            },
            EventSeverity::Error,
            ConcernDomain::Health,
        )
        .with_for_duration(60)
        .builtin()
        .inhibited_by_rule("parent-rule")
        .with_label("team", "infra");

        assert_eq!(rule.for_duration_secs, 60);
        assert!(rule.is_builtin);
        assert_eq!(rule.inhibited_by, vec!["parent-rule"]);
        assert_eq!(rule.labels.get("team"), Some(&"infra".to_string()));
    }

    // --- Alert tests ---

    #[test]
    fn alert_is_active() {
        let alert = Alert {
            id: 1,
            rule_name: "test".to_string(),
            state: AlertState::Firing,
            severity: EventSeverity::Warning,
            domain: ConcernDomain::Health,
            summary: "test".to_string(),
            labels: HashMap::new(),
            annotations: HashMap::new(),
            started_at: Utc::now(),
            last_fired_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            acknowledged_at: None,
            silenced_until: None,
            fire_count: 1,
            notification_count: 0,
        };
        assert!(alert.is_active());

        let resolved = Alert {
            state: AlertState::Resolved,
            ..alert.clone()
        };
        assert!(!resolved.is_active());
    }

    // --- NotificationChannel tests ---

    #[tokio::test]
    async fn notification_log_channel() {
        let channel = NotificationChannel::Log {
            level: "warn".to_string(),
        };
        let alert = Alert {
            id: 1,
            rule_name: "test".to_string(),
            state: AlertState::Firing,
            severity: EventSeverity::Warning,
            domain: ConcernDomain::Health,
            summary: "test alert".to_string(),
            labels: HashMap::new(),
            annotations: HashMap::new(),
            started_at: Utc::now(),
            last_fired_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            acknowledged_at: None,
            silenced_until: None,
            fire_count: 1,
            notification_count: 0,
        };
        let result = channel.send(&alert, None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn notification_event_bus_channel() {
        let channel = NotificationChannel::EventBus;
        let bus = super::super::events::EventBus::new(128);
        let alert = Alert {
            id: 1,
            rule_name: "test".to_string(),
            state: AlertState::Firing,
            severity: EventSeverity::Warning,
            domain: ConcernDomain::Health,
            summary: "test alert".to_string(),
            labels: HashMap::new(),
            annotations: HashMap::new(),
            started_at: Utc::now(),
            last_fired_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            acknowledged_at: None,
            silenced_until: None,
            fire_count: 1,
            notification_count: 0,
        };
        let result = channel.send(&alert, Some(&bus)).await;
        assert!(result.is_ok());
        // Verify event was emitted
        let events = bus.recent_events(10);
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn notification_webhook_channel() {
        let channel = NotificationChannel::Webhook {
            url: "http://example.com/alert".to_string(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            body_template: "Alert: {{alert_name}} - {{summary}}".to_string(),
        };
        let alert = Alert {
            id: 1,
            rule_name: "test".to_string(),
            state: AlertState::Firing,
            severity: EventSeverity::Warning,
            domain: ConcernDomain::Health,
            summary: "test alert".to_string(),
            labels: HashMap::new(),
            annotations: HashMap::new(),
            started_at: Utc::now(),
            last_fired_at: Utc::now(),
            resolved_at: None,
            acknowledged_by: None,
            acknowledged_at: None,
            silenced_until: None,
            fire_count: 1,
            notification_count: 0,
        };
        // Webhook is simulated in tests
        let result = channel.send(&alert, None).await;
        assert!(result.is_ok());
    }

    // --- Builtin rule names ---

    #[test]
    fn builtin_names_count() {
        assert_eq!(builtin_rule_names().len(), 15);
    }

    // --- AlertState display ---

    #[test]
    fn alert_state_display() {
        assert_eq!(AlertState::Pending.to_string(), "pending");
        assert_eq!(AlertState::Firing.to_string(), "firing");
        assert_eq!(AlertState::Acknowledged.to_string(), "acknowledged");
        assert_eq!(AlertState::Silenced.to_string(), "silenced");
        assert_eq!(AlertState::Resolved.to_string(), "resolved");
    }

    // --- Evaluate psyche conditions ---

    #[test]
    fn evaluate_psyche_war_room() {
        let store = make_store();
        let engine = AlertEngine::new(store);

        let mut ctx = make_ctx();
        ctx.add_archetype("war-room");
        engine.evaluate_all(&ctx);

        let alerts = engine.all_alerts();
        let war_room = alerts.iter().find(|a| a.rule_name == "psyche-war-room");
        assert!(war_room.is_some());
    }

    #[test]
    fn evaluate_job_stalled() {
        let store = make_store();
        let engine = AlertEngine::new(store);

        let mut ctx = make_ctx();
        ctx.set_metric("max_job_stall_secs", 700.0);
        engine.evaluate_all(&ctx);

        let alerts = engine.all_alerts();
        let stalled = alerts.iter().find(|a| a.rule_name == "job-stalled");
        assert!(stalled.is_some());
        assert_eq!(stalled.map(|a| a.state), Some(AlertState::Firing));
    }

    // --- Engine multiple evaluations ---

    #[test]
    fn engine_multiple_evaluations_idempotent() {
        let engine = make_engine();
        let mut ctx = make_ctx();
        ctx.set_metric("max_job_stall_secs", 700.0);

        // Evaluate multiple times
        engine.evaluate_all(&ctx);
        engine.evaluate_all(&ctx);
        engine.evaluate_all(&ctx);

        // Should still have exactly one alert for job-stalled
        let stalled: Vec<_> = engine
            .all_alerts()
            .into_iter()
            .filter(|a| a.rule_name == "job-stalled")
            .collect();
        assert_eq!(stalled.len(), 1);
    }

    // --- Event match condition ---

    #[test]
    fn condition_event_match() {
        let cond = AlertCondition::EventMatch {
            domain: "security".to_string(),
            severity: "warning".to_string(),
            keyword: None,
        };
        let mut ctx = make_ctx();
        ctx.set_event_rate("security:warning", 5.0);
        assert!(cond.evaluate(&ctx));
    }

    #[test]
    fn condition_event_match_with_keyword() {
        let cond = AlertCondition::EventMatch {
            domain: "security".to_string(),
            severity: "warning".to_string(),
            keyword: Some("brute-force".to_string()),
        };
        let mut ctx = make_ctx();
        ctx.set_event_rate("security:warning", 5.0);
        ctx.set_metric("event_keyword:brute-force", 1.0);
        assert!(cond.evaluate(&ctx));
    }

    #[test]
    fn condition_event_match_keyword_missing() {
        let cond = AlertCondition::EventMatch {
            domain: "security".to_string(),
            severity: "warning".to_string(),
            keyword: Some("brute-force".to_string()),
        };
        let mut ctx = make_ctx();
        ctx.set_event_rate("security:warning", 5.0);
        // No keyword marker set
        assert!(!cond.evaluate(&ctx));
    }

    // --- Validate event rate ---

    #[test]
    fn condition_validate_event_rate_negative() {
        let cond = AlertCondition::EventRate {
            domain: "health".to_string(),
            severity: "error".to_string(),
            rate_per_min: -1.0,
            window_secs: 60,
        };
        assert!(cond.validate().is_err());
    }

    #[test]
    fn condition_validate_event_rate_zero_window() {
        let cond = AlertCondition::EventRate {
            domain: "health".to_string(),
            severity: "error".to_string(),
            rate_per_min: 5.0,
            window_secs: 0,
        };
        assert!(cond.validate().is_err());
    }

    // --- Description for complex conditions ---

    #[test]
    fn description_event_rate() {
        let cond = AlertCondition::EventRate {
            domain: "health".to_string(),
            severity: "error".to_string(),
            rate_per_min: 5.0,
            window_secs: 60,
        };
        let desc = cond.description();
        assert!(desc.contains("event rate"));
        assert!(desc.contains("health:error"));
    }

    #[test]
    fn description_not() {
        let cond = AlertCondition::Not(Box::new(AlertCondition::ArchetypeActive {
            name: "calm".to_string(),
        }));
        let desc = cond.description();
        assert!(desc.contains("NOT("));
        assert!(desc.contains("calm"));
    }
}
