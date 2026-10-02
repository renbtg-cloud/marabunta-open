// Marabunta - Licensed under the MIT License.
//! Alert Detection System for Marabunta Compute Control Plane
//!
//! This module provides a comprehensive alerting system that continuously monitors
//! the cluster for various conditions and generates actionable alerts with suggested
//! remediation actions.

use chrono::{DateTime, Datelike, Duration, NaiveTime, Timelike, Utc, Weekday};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::interval;
use tracing::{debug, info};
use uuid::Uuid;

use crate::common::types::*;

// ============================================================================
// Alert Types and Conditions
// ============================================================================

/// Unique identifier for an alert
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AlertId(pub Uuid);

impl AlertId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AlertId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for AlertId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "alert-{}", &self.0.to_string()[..8])
    }
}

/// Types of alert conditions that can be detected
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AlertCondition {
    /// School/factory about to close based on schedule
    ClusterGoingOffline {
        cluster_name: String,
        region_id: RegionId,
        shutdown_in_mins: u64,
        affected_nodes: Vec<WorkerId>,
    },

    /// Node not assigned to any job for too long
    NodeIdleTooLong {
        worker_id: WorkerId,
        idle_duration_mins: u64,
        threshold_mins: u64,
    },

    /// Region latency exceeds threshold
    LatencySpike {
        region_id: RegionId,
        region_name: String,
        current_latency_ms: u64,
        threshold_ms: u64,
        baseline_ms: u64,
    },

    /// Job failing >X% of work units
    HighFailureRate {
        job_id: JobId,
        job_name: String,
        failure_rate: f64,
        threshold: f64,
        failed_tasks: u32,
        total_tasks: u32,
    },

    /// Phone nodes below battery threshold
    BatteryLow {
        worker_id: WorkerId,
        battery_percent: u8,
        threshold_percent: u8,
        is_charging: bool,
    },

    /// Nodes reporting thermal/battery throttling
    ThrottlingDetected {
        worker_id: WorkerId,
        throttle_type: ThrottleType,
        severity: ThrottleSeverity,
    },

    /// Sudden loss of compute capacity
    CapacityDrop {
        region_id: Option<RegionId>,
        lost_workers: Vec<WorkerId>,
        capacity_drop_percent: f64,
        previous_capacity: f64,
        current_capacity: f64,
    },

    /// Job making no progress
    JobStalled {
        job_id: JobId,
        job_name: String,
        stalled_duration_mins: u64,
        last_progress_at: DateTime<Utc>,
    },
}

/// Types of throttling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThrottleType {
    Thermal,
    Battery,
    Memory,
    Network,
}

/// Severity of throttling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThrottleSeverity {
    Minor,    // <20% reduction
    Moderate, // 20-50% reduction
    Severe,   // >50% reduction
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

// ============================================================================
// Suggested Actions
// ============================================================================

/// A suggested action to remediate an alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuggestedAction {
    pub action_id: String,
    pub label: String,
    pub description: String,
    pub action_type: ActionType,
    pub parameters: HashMap<String, serde_json::Value>,
    pub estimated_impact: String,
    pub requires_confirmation: bool,
}

/// Types of actions that can be taken
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActionType {
    /// Migrate job to different nodes/region
    MigrateJob {
        job_id: JobId,
        target_region: Option<RegionId>,
    },

    /// Checkpoint and prepare for shutdown
    CheckpointJob { job_id: JobId },

    /// Ignore the alert
    Ignore,

    /// Add node to specific job
    AssignNodeToJob { worker_id: WorkerId, job_id: JobId },

    /// Add node to default pool
    AddToDefaultPool { worker_id: WorkerId },

    /// Exclude region from scheduling
    ExcludeRegion { region_id: RegionId },

    /// Switch to relay routing
    SwitchToRelay { region_id: RegionId },

    /// Cancel job
    CancelJob { job_id: JobId },

    /// Reassign tasks
    ReassignTasks { task_ids: Vec<TaskId> },

    /// Reduce node workload
    ReduceWorkload {
        worker_id: WorkerId,
        target_load_percent: u8,
    },

    /// Scale out job to more nodes
    ScaleOut {
        job_id: JobId,
        additional_nodes: u32,
    },
}

/// Template for generating suggested actions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionTemplate {
    pub label: String,
    pub description: String,
    pub action_type: ActionType,
    pub estimated_impact: String,
    pub requires_confirmation: bool,
}

// ============================================================================
// Alert Rules
// ============================================================================

/// Configuration for an alert rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub rule_id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub condition_type: String, // matches AlertCondition type discriminant
    pub severity: Severity,
    pub cooldown_secs: u64,
    pub auto_dismiss_secs: Option<u64>,
    pub suggested_actions: Vec<ActionTemplate>,
    pub parameters: HashMap<String, serde_json::Value>,
}

impl AlertRule {
    /// Create default rules for the system
    pub fn default_rules() -> Vec<AlertRule> {
        vec![
            // Cluster going offline
            AlertRule {
                rule_id: "cluster_offline".to_string(),
                name: "Cluster Going Offline".to_string(),
                description: "Scheduled shutdown detected for cluster".to_string(),
                enabled: true,
                condition_type: "cluster_going_offline".to_string(),
                severity: Severity::Warning,
                cooldown_secs: 1800, // 30 minutes
                auto_dismiss_secs: None,
                suggested_actions: vec![
                    ActionTemplate {
                        label: "Migrate Jobs Now".to_string(),
                        description: "Immediately migrate all jobs to available regions"
                            .to_string(),
                        action_type: ActionType::Ignore, // Will be replaced with actual job
                        estimated_impact: "Jobs will continue running in new region".to_string(),
                        requires_confirmation: true,
                    },
                    ActionTemplate {
                        label: "Checkpoint and Wait".to_string(),
                        description: "Save progress and resume when cluster returns".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "Jobs will pause until cluster comes back online"
                            .to_string(),
                        requires_confirmation: false,
                    },
                    ActionTemplate {
                        label: "Ignore".to_string(),
                        description: "Continue running and handle failures as they occur"
                            .to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "May result in task failures and retries".to_string(),
                        requires_confirmation: false,
                    },
                ],
                parameters: HashMap::new(),
            },
            // Node idle too long
            AlertRule {
                rule_id: "node_idle".to_string(),
                name: "Node Idle Too Long".to_string(),
                description: "Worker not assigned work for extended period".to_string(),
                enabled: true,
                condition_type: "node_idle_too_long".to_string(),
                severity: Severity::Info,
                cooldown_secs: 3600,           // 1 hour
                auto_dismiss_secs: Some(7200), // Auto-dismiss after 2 hours
                suggested_actions: vec![],
                parameters: {
                    let mut p = HashMap::new();
                    p.insert("threshold_mins".to_string(), serde_json::json!(30));
                    p
                },
            },
            // Latency spike
            AlertRule {
                rule_id: "latency_spike".to_string(),
                name: "Latency Spike".to_string(),
                description: "Region experiencing high latency".to_string(),
                enabled: true,
                condition_type: "latency_spike".to_string(),
                severity: Severity::Warning,
                cooldown_secs: 600, // 10 minutes
                auto_dismiss_secs: Some(3600),
                suggested_actions: vec![
                    ActionTemplate {
                        label: "Exclude Region".to_string(),
                        description: "Stop scheduling new work to this region".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "Region will drain existing work".to_string(),
                        requires_confirmation: true,
                    },
                    ActionTemplate {
                        label: "Switch to Relay".to_string(),
                        description: "Route traffic through relay server".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "May reduce latency but increase costs".to_string(),
                        requires_confirmation: false,
                    },
                ],
                parameters: {
                    let mut p = HashMap::new();
                    p.insert("threshold_ms".to_string(), serde_json::json!(500));
                    p.insert("baseline_multiplier".to_string(), serde_json::json!(2.0));
                    p
                },
            },
            // High failure rate
            AlertRule {
                rule_id: "high_failure_rate".to_string(),
                name: "High Job Failure Rate".to_string(),
                description: "Job experiencing high task failure rate".to_string(),
                enabled: true,
                condition_type: "high_failure_rate".to_string(),
                severity: Severity::Error,
                cooldown_secs: 300, // 5 minutes
                auto_dismiss_secs: None,
                suggested_actions: vec![
                    ActionTemplate {
                        label: "Cancel Job".to_string(),
                        description: "Stop the failing job to prevent wasted resources".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "Job will be cancelled".to_string(),
                        requires_confirmation: true,
                    },
                    ActionTemplate {
                        label: "Scale Out".to_string(),
                        description: "Add more nodes to distribute the load".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "May reduce failures if caused by resource constraints"
                            .to_string(),
                        requires_confirmation: false,
                    },
                ],
                parameters: {
                    let mut p = HashMap::new();
                    p.insert("threshold_percent".to_string(), serde_json::json!(25.0));
                    p.insert("min_tasks".to_string(), serde_json::json!(10));
                    p
                },
            },
            // Battery low
            AlertRule {
                rule_id: "battery_low".to_string(),
                name: "Low Battery".to_string(),
                description: "Mobile device running low on battery".to_string(),
                enabled: true,
                condition_type: "battery_low".to_string(),
                severity: Severity::Warning,
                cooldown_secs: 1800, // 30 minutes
                auto_dismiss_secs: Some(7200),
                suggested_actions: vec![
                    ActionTemplate {
                        label: "Reduce Workload".to_string(),
                        description: "Lower CPU usage to conserve battery".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "Device will process tasks more slowly".to_string(),
                        requires_confirmation: false,
                    },
                    ActionTemplate {
                        label: "Migrate Tasks".to_string(),
                        description: "Move tasks to other devices".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "Device will finish current work and idle".to_string(),
                        requires_confirmation: false,
                    },
                ],
                parameters: {
                    let mut p = HashMap::new();
                    p.insert("threshold_percent".to_string(), serde_json::json!(20));
                    p
                },
            },
            // Throttling detected
            AlertRule {
                rule_id: "throttling".to_string(),
                name: "Throttling Detected".to_string(),
                description: "Device experiencing performance throttling".to_string(),
                enabled: true,
                condition_type: "throttling_detected".to_string(),
                severity: Severity::Warning,
                cooldown_secs: 900, // 15 minutes
                auto_dismiss_secs: Some(3600),
                suggested_actions: vec![ActionTemplate {
                    label: "Reduce Workload".to_string(),
                    description: "Lower task intensity to reduce throttling".to_string(),
                    action_type: ActionType::Ignore,
                    estimated_impact: "Performance will stabilize at lower rate".to_string(),
                    requires_confirmation: false,
                }],
                parameters: HashMap::new(),
            },
            // Capacity drop
            AlertRule {
                rule_id: "capacity_drop".to_string(),
                name: "Capacity Drop".to_string(),
                description: "Sudden loss of compute capacity".to_string(),
                enabled: true,
                condition_type: "capacity_drop".to_string(),
                severity: Severity::Critical,
                cooldown_secs: 300, // 5 minutes
                auto_dismiss_secs: None,
                suggested_actions: vec![ActionTemplate {
                    label: "Reassign Tasks".to_string(),
                    description: "Redistribute work from lost nodes".to_string(),
                    action_type: ActionType::Ignore,
                    estimated_impact: "Work will continue on remaining nodes".to_string(),
                    requires_confirmation: false,
                }],
                parameters: {
                    let mut p = HashMap::new();
                    p.insert("threshold_percent".to_string(), serde_json::json!(20.0));
                    p
                },
            },
            // Job stalled
            AlertRule {
                rule_id: "job_stalled".to_string(),
                name: "Job Stalled".to_string(),
                description: "Job making no progress".to_string(),
                enabled: true,
                condition_type: "job_stalled".to_string(),
                severity: Severity::Error,
                cooldown_secs: 600, // 10 minutes
                auto_dismiss_secs: None,
                suggested_actions: vec![
                    ActionTemplate {
                        label: "Restart Tasks".to_string(),
                        description: "Force restart of stalled tasks".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "Tasks will restart from last checkpoint".to_string(),
                        requires_confirmation: true,
                    },
                    ActionTemplate {
                        label: "Cancel Job".to_string(),
                        description: "Cancel the stalled job".to_string(),
                        action_type: ActionType::Ignore,
                        estimated_impact: "Job will be cancelled".to_string(),
                        requires_confirmation: true,
                    },
                ],
                parameters: {
                    let mut p = HashMap::new();
                    p.insert("threshold_mins".to_string(), serde_json::json!(60));
                    p
                },
            },
        ]
    }
}

// ============================================================================
// Alert Data Structures
// ============================================================================

/// An active alert in the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: AlertId,
    pub rule_id: String,
    pub condition: AlertCondition,
    pub severity: Severity,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub dismissed_at: Option<DateTime<Utc>>,
    pub dismissed_by: Option<String>,
    pub auto_dismissed: bool,
    pub suggested_actions: Vec<SuggestedAction>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Alert {
    pub fn new(rule: &AlertRule, condition: AlertCondition) -> Self {
        let suggested_actions = generate_suggested_actions(&condition, &rule.suggested_actions);

        Self {
            id: AlertId::new(),
            rule_id: rule.rule_id.clone(),
            condition,
            severity: rule.severity,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            dismissed_at: None,
            dismissed_by: None,
            auto_dismissed: false,
            suggested_actions,
            metadata: HashMap::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        self.dismissed_at.is_none()
    }

    pub fn dismiss(&mut self, dismissed_by: Option<String>) {
        self.dismissed_at = Some(Utc::now());
        self.dismissed_by = dismissed_by;
        self.auto_dismissed = false;
    }

    pub fn auto_dismiss(&mut self) {
        self.dismissed_at = Some(Utc::now());
        self.auto_dismissed = true;
    }
}

// ============================================================================
// Alert Events
// ============================================================================

/// Events emitted by the alert system
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AlertEvent {
    /// New alert created
    AlertCreated { alert: Alert },

    /// Alert updated (e.g., condition changed)
    AlertUpdated { alert: Alert },

    /// Alert dismissed
    AlertDismissed {
        alert_id: AlertId,
        dismissed_by: Option<String>,
        auto_dismissed: bool,
    },

    /// Action taken in response to alert
    ActionTaken {
        alert_id: AlertId,
        action: SuggestedAction,
        result: ActionResult,
    },
}

/// Result of taking an action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
    pub success: bool,
    pub message: String,
    pub timestamp: DateTime<Utc>,
}

// ============================================================================
// Schedule Pattern Detection
// ============================================================================

/// Schedule pattern for predicting offline periods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulePattern {
    pub pattern_id: String,
    pub name: String,
    pub pattern_type: SchedulePatternType,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchedulePatternType {
    /// Regular weekly schedule (e.g., school hours)
    WeeklySchedule {
        days: Vec<Weekday>,
        start_time: NaiveTime,
        end_time: NaiveTime,
    },

    /// Shift-based schedule
    ShiftSchedule { shifts: Vec<ShiftPeriod> },

    /// Detected from historical data
    HistoricalPattern { offline_periods: Vec<OfflinePeriod> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShiftPeriod {
    pub days: Vec<Weekday>,
    pub start_time: NaiveTime,
    pub end_time: NaiveTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflinePeriod {
    pub day_of_week: Weekday,
    pub start_time: NaiveTime,
    pub end_time: NaiveTime,
    pub frequency: f64, // 0.0 - 1.0
}

impl SchedulePattern {
    /// Create a default school hours pattern
    pub fn school_hours() -> Self {
        Self {
            pattern_id: "school_hours".to_string(),
            name: "School Hours".to_string(),
            pattern_type: SchedulePatternType::WeeklySchedule {
                days: vec![
                    Weekday::Mon,
                    Weekday::Tue,
                    Weekday::Wed,
                    Weekday::Thu,
                    Weekday::Fri,
                ],
                start_time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
                end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            },
            confidence: 0.9,
        }
    }

    /// Create a factory shift pattern
    pub fn factory_shifts() -> Self {
        Self {
            pattern_id: "factory_shifts".to_string(),
            name: "Factory Shifts".to_string(),
            pattern_type: SchedulePatternType::ShiftSchedule {
                shifts: vec![
                    ShiftPeriod {
                        days: vec![
                            Weekday::Mon,
                            Weekday::Tue,
                            Weekday::Wed,
                            Weekday::Thu,
                            Weekday::Fri,
                        ],
                        start_time: NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
                        end_time: NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
                    },
                    ShiftPeriod {
                        days: vec![
                            Weekday::Mon,
                            Weekday::Tue,
                            Weekday::Wed,
                            Weekday::Thu,
                            Weekday::Fri,
                        ],
                        start_time: NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
                        end_time: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
                    },
                ],
            },
            confidence: 0.85,
        }
    }

    /// Check if a shutdown is imminent based on this pattern
    pub fn time_until_offline(&self, now: &DateTime<Utc>) -> Option<Duration> {
        match &self.pattern_type {
            SchedulePatternType::WeeklySchedule { days, end_time, .. } => {
                let current_day = now.weekday();
                if !days.contains(&current_day) {
                    return None;
                }

                let current_time = now.time();
                let end_naive = *end_time;

                if current_time < end_naive {
                    // Calculate time until end
                    let seconds_until = (end_naive.num_seconds_from_midnight() as i64)
                        - (current_time.num_seconds_from_midnight() as i64);
                    Some(Duration::seconds(seconds_until))
                } else {
                    None
                }
            }
            SchedulePatternType::ShiftSchedule { shifts } => {
                let current_day = now.weekday();
                let current_time = now.time();

                for shift in shifts {
                    if shift.days.contains(&current_day) && current_time < shift.end_time {
                        let seconds_until = (shift.end_time.num_seconds_from_midnight() as i64)
                            - (current_time.num_seconds_from_midnight() as i64);
                        return Some(Duration::seconds(seconds_until));
                    }
                }
                None
            }
            SchedulePatternType::HistoricalPattern { offline_periods } => {
                let current_day = now.weekday();
                let current_time = now.time();

                for period in offline_periods {
                    if period.day_of_week == current_day
                        && current_time < period.start_time
                        && period.frequency > 0.5
                    {
                        let seconds_until = (period.start_time.num_seconds_from_midnight() as i64)
                            - (current_time.num_seconds_from_midnight() as i64);
                        return Some(Duration::seconds(seconds_until));
                    }
                }
                None
            }
        }
    }
}

// ============================================================================
// Alert Store
// ============================================================================

/// In-memory storage for alerts with persistence support
pub struct AlertStore {
    /// Active alerts by ID
    alerts: DashMap<AlertId, Alert>,

    /// Alert history (limited size)
    history: Arc<RwLock<VecDeque<Alert>>>,

    /// Maximum history size
    max_history: usize,

    /// Last alert time per rule (for cooldown)
    last_alert_time: DashMap<String, DateTime<Utc>>,

    /// Event broadcaster
    event_tx: broadcast::Sender<AlertEvent>,
}

impl AlertStore {
    pub fn new(max_history: usize) -> (Self, broadcast::Receiver<AlertEvent>) {
        let (event_tx, event_rx) = broadcast::channel(1000);

        let store = Self {
            alerts: DashMap::new(),
            history: Arc::new(RwLock::new(VecDeque::with_capacity(max_history))),
            max_history,
            last_alert_time: DashMap::new(),
            event_tx,
        };

        (store, event_rx)
    }

    /// Create a new alert
    pub fn create_alert(&self, rule: &AlertRule, condition: AlertCondition) -> Option<Alert> {
        // Check cooldown
        if let Some(last_time) = self.last_alert_time.get(&rule.rule_id) {
            let elapsed = Utc::now().signed_duration_since(*last_time);
            if elapsed.num_seconds() < rule.cooldown_secs as i64 {
                debug!(
                    "Alert {} in cooldown, skipping ({}s remaining)",
                    rule.rule_id,
                    rule.cooldown_secs as i64 - elapsed.num_seconds()
                );
                return None;
            }
        }

        let alert = Alert::new(rule, condition);
        let alert_id = alert.id;

        // Update last alert time
        self.last_alert_time
            .insert(rule.rule_id.clone(), Utc::now());

        // Store alert
        self.alerts.insert(alert_id, alert.clone());

        // Emit event
        let _ = self.event_tx.send(AlertEvent::AlertCreated {
            alert: alert.clone(),
        });

        info!("Created alert: {} ({})", alert_id, rule.name);

        Some(alert)
    }

    /// Dismiss an alert
    pub fn dismiss_alert(&self, alert_id: AlertId, dismissed_by: Option<String>) -> bool {
        if let Some(mut entry) = self.alerts.get_mut(&alert_id) {
            entry.dismiss(dismissed_by.clone());

            // Move to history
            let alert = entry.clone();
            self.move_to_history(alert.clone());

            // Remove from active
            drop(entry);
            self.alerts.remove(&alert_id);

            // Emit event
            let _ = self.event_tx.send(AlertEvent::AlertDismissed {
                alert_id,
                dismissed_by,
                auto_dismissed: false,
            });

            info!("Dismissed alert: {}", alert_id);
            true
        } else {
            false
        }
    }

    /// Auto-dismiss alerts based on rules
    pub fn auto_dismiss_expired(&self, rules: &HashMap<String, AlertRule>) {
        let now = Utc::now();
        let mut to_dismiss = Vec::new();

        for entry in self.alerts.iter() {
            let alert = entry.value();
            if let Some(rule) = rules.get(&alert.rule_id) {
                if let Some(auto_dismiss_secs) = rule.auto_dismiss_secs {
                    let age = now.signed_duration_since(alert.created_at);
                    if age.num_seconds() >= auto_dismiss_secs as i64 {
                        to_dismiss.push(alert.id);
                    }
                }
            }
        }

        for alert_id in to_dismiss {
            if let Some(mut entry) = self.alerts.get_mut(&alert_id) {
                entry.auto_dismiss();

                let alert = entry.clone();
                self.move_to_history(alert.clone());

                drop(entry);
                self.alerts.remove(&alert_id);

                let _ = self.event_tx.send(AlertEvent::AlertDismissed {
                    alert_id,
                    dismissed_by: None,
                    auto_dismissed: true,
                });

                debug!("Auto-dismissed alert: {}", alert_id);
            }
        }
    }

    /// Get all active alerts
    pub fn get_active_alerts(&self) -> Vec<Alert> {
        self.alerts
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get alerts by severity
    pub fn get_alerts_by_severity(&self, severity: Severity) -> Vec<Alert> {
        self.alerts
            .iter()
            .filter(|entry| entry.value().severity == severity)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get alert by ID
    pub fn get_alert(&self, alert_id: AlertId) -> Option<Alert> {
        self.alerts
            .get(&alert_id)
            .map(|entry| entry.value().clone())
    }

    /// Get alert history
    pub fn get_history(&self, limit: usize) -> Vec<Alert> {
        let history = self.history.read();
        history.iter().take(limit).cloned().collect()
    }

    /// Clear all alerts (for testing)
    pub fn clear(&self) {
        self.alerts.clear();
        self.history.write().clear();
        self.last_alert_time.clear();
    }

    /// Move alert to history
    fn move_to_history(&self, alert: Alert) {
        let mut history = self.history.write();

        // Add to front
        history.push_front(alert);

        // Trim if too large
        while history.len() > self.max_history {
            history.pop_back();
        }
    }

    /// Subscribe to alert events
    pub fn subscribe(&self) -> broadcast::Receiver<AlertEvent> {
        self.event_tx.subscribe()
    }
}

// ============================================================================
// Alert Detector
// ============================================================================

/// Configuration for the alert detector
#[derive(Debug, Clone)]
pub struct AlertDetectorConfig {
    pub check_interval_secs: u64,
    pub node_idle_threshold_mins: u64,
    pub latency_threshold_ms: u64,
    pub failure_rate_threshold: f64,
    pub battery_threshold_percent: u8,
    pub capacity_drop_threshold: f64,
    pub job_stalled_threshold_mins: u64,
    pub cluster_shutdown_warning_mins: u64,
}

impl Default for AlertDetectorConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 30,
            node_idle_threshold_mins: 30,
            latency_threshold_ms: 500,
            failure_rate_threshold: 0.25,
            battery_threshold_percent: 20,
            capacity_drop_threshold: 0.20,
            job_stalled_threshold_mins: 60,
            cluster_shutdown_warning_mins: 30,
        }
    }
}

/// Main alert detection system
pub struct AlertDetector {
    config: AlertDetectorConfig,
    rules: Arc<RwLock<HashMap<String, AlertRule>>>,
    store: Arc<AlertStore>,
    patterns: Arc<RwLock<HashMap<String, SchedulePattern>>>,

    // State for detection
    worker_idle_since: Arc<DashMap<WorkerId, DateTime<Utc>>>,
    job_last_progress: Arc<DashMap<JobId, DateTime<Utc>>>,
    region_latency_baseline: Arc<DashMap<RegionId, u64>>,
    last_capacity: Arc<RwLock<f64>>,
}

impl AlertDetector {
    pub fn new(config: AlertDetectorConfig, store: Arc<AlertStore>) -> Self {
        let rules = AlertRule::default_rules()
            .into_iter()
            .map(|r| (r.rule_id.clone(), r))
            .collect();

        let mut patterns = HashMap::new();
        patterns.insert("school_hours".to_string(), SchedulePattern::school_hours());
        patterns.insert(
            "factory_shifts".to_string(),
            SchedulePattern::factory_shifts(),
        );

        Self {
            config,
            rules: Arc::new(RwLock::new(rules)),
            store,
            patterns: Arc::new(RwLock::new(patterns)),
            worker_idle_since: Arc::new(DashMap::new()),
            job_last_progress: Arc::new(DashMap::new()),
            region_latency_baseline: Arc::new(DashMap::new()),
            last_capacity: Arc::new(RwLock::new(0.0)),
        }
    }

    /// Start the detection loop
    pub async fn run(self: Arc<Self>) {
        let mut ticker = interval(std::time::Duration::from_secs(
            self.config.check_interval_secs,
        ));

        info!(
            "Starting alert detector (interval: {}s)",
            self.config.check_interval_secs
        );

        loop {
            ticker.tick().await;

            // Auto-dismiss expired alerts
            let rules = self.rules.read().clone();
            self.store.auto_dismiss_expired(&rules);

            // Run detection checks
            // In a real implementation, these would query actual cluster state
            // For now, we provide the structure for integration

            debug!("Running alert detection checks");
        }
    }

    /// Check for cluster going offline
    pub fn check_cluster_offline(
        &self,
        cluster_name: String,
        region_id: RegionId,
        workers: Vec<WorkerId>,
    ) {
        let now = Utc::now();
        let patterns = self.patterns.read();

        for pattern in patterns.values() {
            if let Some(duration) = pattern.time_until_offline(&now) {
                let mins = duration.num_minutes();
                if mins > 0 && mins <= self.config.cluster_shutdown_warning_mins as i64 {
                    let condition = AlertCondition::ClusterGoingOffline {
                        cluster_name: cluster_name.clone(),
                        region_id,
                        shutdown_in_mins: mins as u64,
                        affected_nodes: workers.clone(),
                    };

                    let rules = self.rules.read();
                    if let Some(rule) = rules.get("cluster_offline") {
                        self.store.create_alert(rule, condition);
                    }
                }
            }
        }
    }

    /// Check for idle nodes
    pub fn check_node_idle(&self, worker_id: WorkerId, is_idle: bool) {
        if is_idle {
            // Record idle start if not already tracking
            self.worker_idle_since
                .entry(worker_id)
                .or_insert_with(Utc::now);

            // Check if idle too long
            if let Some(entry) = self.worker_idle_since.get(&worker_id) {
                let idle_duration = Utc::now().signed_duration_since(*entry.value());
                let mins = idle_duration.num_minutes();

                if mins >= self.config.node_idle_threshold_mins as i64 {
                    let condition = AlertCondition::NodeIdleTooLong {
                        worker_id,
                        idle_duration_mins: mins as u64,
                        threshold_mins: self.config.node_idle_threshold_mins,
                    };

                    let rules = self.rules.read();
                    if let Some(rule) = rules.get("node_idle") {
                        self.store.create_alert(rule, condition);
                    }
                }
            }
        } else {
            // Node is busy, clear idle tracking
            self.worker_idle_since.remove(&worker_id);
        }
    }

    /// Check for latency spikes
    pub fn check_latency_spike(
        &self,
        region_id: RegionId,
        region_name: String,
        current_latency_ms: u64,
    ) {
        // Update baseline
        self.region_latency_baseline
            .entry(region_id)
            .and_modify(|baseline| {
                // Exponential moving average
                *baseline = (*baseline * 9 + current_latency_ms) / 10;
            })
            .or_insert(current_latency_ms);

        // Check threshold
        if current_latency_ms > self.config.latency_threshold_ms {
            if let Some(baseline) = self.region_latency_baseline.get(&region_id) {
                let condition = AlertCondition::LatencySpike {
                    region_id,
                    region_name,
                    current_latency_ms,
                    threshold_ms: self.config.latency_threshold_ms,
                    baseline_ms: *baseline.value(),
                };

                let rules = self.rules.read();
                if let Some(rule) = rules.get("latency_spike") {
                    self.store.create_alert(rule, condition);
                }
            }
        }
    }

    /// Check for high failure rate
    pub fn check_failure_rate(
        &self,
        job_id: JobId,
        job_name: String,
        failed_tasks: u32,
        total_tasks: u32,
    ) {
        if total_tasks > 0 {
            let failure_rate = failed_tasks as f64 / total_tasks as f64;

            if failure_rate > self.config.failure_rate_threshold {
                let condition = AlertCondition::HighFailureRate {
                    job_id,
                    job_name,
                    failure_rate,
                    threshold: self.config.failure_rate_threshold,
                    failed_tasks,
                    total_tasks,
                };

                let rules = self.rules.read();
                if let Some(rule) = rules.get("high_failure_rate") {
                    self.store.create_alert(rule, condition);
                }
            }
        }
    }

    /// Check for low battery
    pub fn check_battery_low(&self, worker_id: WorkerId, battery_percent: u8, is_charging: bool) {
        if !is_charging && battery_percent < self.config.battery_threshold_percent {
            let condition = AlertCondition::BatteryLow {
                worker_id,
                battery_percent,
                threshold_percent: self.config.battery_threshold_percent,
                is_charging,
            };

            let rules = self.rules.read();
            if let Some(rule) = rules.get("battery_low") {
                self.store.create_alert(rule, condition);
            }
        }
    }

    /// Check for throttling
    pub fn check_throttling(
        &self,
        worker_id: WorkerId,
        throttle_type: ThrottleType,
        severity: ThrottleSeverity,
    ) {
        let condition = AlertCondition::ThrottlingDetected {
            worker_id,
            throttle_type,
            severity,
        };

        let rules = self.rules.read();
        if let Some(rule) = rules.get("throttling") {
            self.store.create_alert(rule, condition);
        }
    }

    /// Check for capacity drop
    pub fn check_capacity_drop(
        &self,
        region_id: Option<RegionId>,
        current_capacity: f64,
        lost_workers: Vec<WorkerId>,
    ) {
        let previous_capacity = *self.last_capacity.read();

        if previous_capacity > 0.0 {
            let drop = (previous_capacity - current_capacity) / previous_capacity;

            if drop > self.config.capacity_drop_threshold {
                let condition = AlertCondition::CapacityDrop {
                    region_id,
                    lost_workers,
                    capacity_drop_percent: drop * 100.0,
                    previous_capacity,
                    current_capacity,
                };

                let rules = self.rules.read();
                if let Some(rule) = rules.get("capacity_drop") {
                    self.store.create_alert(rule, condition);
                }
            }
        }

        *self.last_capacity.write() = current_capacity;
    }

    /// Check for stalled job
    pub fn check_job_stalled(&self, job_id: JobId, job_name: String, has_progress: bool) {
        if has_progress {
            self.job_last_progress.insert(job_id, Utc::now());
        } else if let Some(entry) = self.job_last_progress.get(&job_id) {
            let stalled_duration = Utc::now().signed_duration_since(*entry.value());
            let mins = stalled_duration.num_minutes();

            if mins >= self.config.job_stalled_threshold_mins as i64 {
                let condition = AlertCondition::JobStalled {
                    job_id,
                    job_name,
                    stalled_duration_mins: mins as u64,
                    last_progress_at: *entry.value(),
                };

                let rules = self.rules.read();
                if let Some(rule) = rules.get("job_stalled") {
                    self.store.create_alert(rule, condition);
                }
            }
        }
    }

    /// Add a custom schedule pattern
    pub fn add_schedule_pattern(&self, pattern: SchedulePattern) {
        self.patterns
            .write()
            .insert(pattern.pattern_id.clone(), pattern);
    }

    /// Get current rules
    pub fn get_rules(&self) -> Vec<AlertRule> {
        self.rules.read().values().cloned().collect()
    }

    /// Update a rule
    pub fn update_rule(&self, rule: AlertRule) {
        self.rules.write().insert(rule.rule_id.clone(), rule);
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate context-aware suggested actions for an alert
fn generate_suggested_actions(
    condition: &AlertCondition,
    templates: &[ActionTemplate],
) -> Vec<SuggestedAction> {
    let mut actions = Vec::new();

    // Generate condition-specific actions
    match condition {
        AlertCondition::ClusterGoingOffline {
            cluster_name,
            region_id,
            shutdown_in_mins,
            affected_nodes,
        } => {
            actions.push(SuggestedAction {
                action_id: format!("migrate_{}", region_id),
                label: "Migrate Jobs Now".to_string(),
                description: format!(
                    "Migrate all {} jobs from {} before shutdown in {} minutes",
                    affected_nodes.len(),
                    cluster_name,
                    shutdown_in_mins
                ),
                action_type: ActionType::MigrateJob {
                    job_id: JobId::new(), // Would be filled in with actual job
                    target_region: None,
                },
                parameters: {
                    let mut p = HashMap::new();
                    p.insert("region_id".to_string(), serde_json::json!(region_id));
                    p.insert(
                        "shutdown_in_mins".to_string(),
                        serde_json::json!(shutdown_in_mins),
                    );
                    p
                },
                estimated_impact: "Jobs will continue in new region with minimal interruption"
                    .to_string(),
                requires_confirmation: true,
            });

            actions.push(SuggestedAction {
                action_id: format!("checkpoint_{}", region_id),
                label: "Checkpoint and Wait".to_string(),
                description: "Save progress and resume when cluster returns".to_string(),
                action_type: ActionType::CheckpointJob {
                    job_id: JobId::new(),
                },
                parameters: HashMap::new(),
                estimated_impact: "Jobs will pause until cluster returns online".to_string(),
                requires_confirmation: false,
            });

            actions.push(SuggestedAction {
                action_id: "ignore".to_string(),
                label: "Ignore".to_string(),
                description: "Continue and handle failures as they occur".to_string(),
                action_type: ActionType::Ignore,
                parameters: HashMap::new(),
                estimated_impact: "May result in task failures and retries".to_string(),
                requires_confirmation: false,
            });
        }

        AlertCondition::NodeIdleTooLong {
            worker_id,
            idle_duration_mins,
            ..
        } => {
            actions.push(SuggestedAction {
                action_id: format!("assign_{}", worker_id),
                label: "Add to Job Queue".to_string(),
                description: format!(
                    "Assign idle node (idle for {} mins) to pending jobs",
                    idle_duration_mins
                ),
                action_type: ActionType::AddToDefaultPool {
                    worker_id: *worker_id,
                },
                parameters: HashMap::new(),
                estimated_impact: "Node will be available for job assignment".to_string(),
                requires_confirmation: false,
            });
        }

        AlertCondition::LatencySpike {
            region_id,
            region_name,
            ..
        } => {
            actions.push(SuggestedAction {
                action_id: format!("exclude_{}", region_id),
                label: "Exclude Region".to_string(),
                description: format!("Stop scheduling new work to {}", region_name),
                action_type: ActionType::ExcludeRegion {
                    region_id: *region_id,
                },
                parameters: HashMap::new(),
                estimated_impact: "Region will drain existing work".to_string(),
                requires_confirmation: true,
            });

            actions.push(SuggestedAction {
                action_id: format!("relay_{}", region_id),
                label: "Switch to Relay".to_string(),
                description: "Route traffic through relay server".to_string(),
                action_type: ActionType::SwitchToRelay {
                    region_id: *region_id,
                },
                parameters: HashMap::new(),
                estimated_impact: "May reduce latency but increase costs".to_string(),
                requires_confirmation: false,
            });
        }

        AlertCondition::HighFailureRate {
            job_id, job_name, ..
        } => {
            actions.push(SuggestedAction {
                action_id: format!("cancel_{}", job_id),
                label: "Cancel Job".to_string(),
                description: format!("Cancel failing job: {}", job_name),
                action_type: ActionType::CancelJob { job_id: *job_id },
                parameters: HashMap::new(),
                estimated_impact: "Job will be cancelled".to_string(),
                requires_confirmation: true,
            });

            actions.push(SuggestedAction {
                action_id: format!("scale_{}", job_id),
                label: "Scale Out".to_string(),
                description: "Add more nodes to distribute load".to_string(),
                action_type: ActionType::ScaleOut {
                    job_id: *job_id,
                    additional_nodes: 10,
                },
                parameters: HashMap::new(),
                estimated_impact: "May reduce failures if caused by resource constraints"
                    .to_string(),
                requires_confirmation: false,
            });
        }

        AlertCondition::BatteryLow { worker_id, .. } => {
            actions.push(SuggestedAction {
                action_id: format!("reduce_{}", worker_id),
                label: "Reduce Workload".to_string(),
                description: "Lower CPU usage to conserve battery".to_string(),
                action_type: ActionType::ReduceWorkload {
                    worker_id: *worker_id,
                    target_load_percent: 25,
                },
                parameters: HashMap::new(),
                estimated_impact: "Device will process tasks more slowly".to_string(),
                requires_confirmation: false,
            });
        }

        AlertCondition::ThrottlingDetected { worker_id, .. } => {
            actions.push(SuggestedAction {
                action_id: format!("reduce_{}", worker_id),
                label: "Reduce Workload".to_string(),
                description: "Lower task intensity to reduce throttling".to_string(),
                action_type: ActionType::ReduceWorkload {
                    worker_id: *worker_id,
                    target_load_percent: 50,
                },
                parameters: HashMap::new(),
                estimated_impact: "Performance will stabilize at lower rate".to_string(),
                requires_confirmation: false,
            });
        }

        AlertCondition::CapacityDrop { lost_workers, .. } => {
            if !lost_workers.is_empty() {
                actions.push(SuggestedAction {
                    action_id: "reassign_tasks".to_string(),
                    label: "Reassign Tasks".to_string(),
                    description: format!(
                        "Redistribute work from {} lost nodes",
                        lost_workers.len()
                    ),
                    action_type: ActionType::ReassignTasks {
                        task_ids: Vec::new(),
                    },
                    parameters: HashMap::new(),
                    estimated_impact: "Work will continue on remaining nodes".to_string(),
                    requires_confirmation: false,
                });
            }
        }

        AlertCondition::JobStalled {
            job_id, job_name, ..
        } => {
            actions.push(SuggestedAction {
                action_id: format!("restart_{}", job_id),
                label: "Restart Tasks".to_string(),
                description: format!("Force restart stalled tasks in {}", job_name),
                action_type: ActionType::ReassignTasks {
                    task_ids: Vec::new(),
                },
                parameters: HashMap::new(),
                estimated_impact: "Tasks will restart from last checkpoint".to_string(),
                requires_confirmation: true,
            });

            actions.push(SuggestedAction {
                action_id: format!("cancel_{}", job_id),
                label: "Cancel Job".to_string(),
                description: "Cancel the stalled job".to_string(),
                action_type: ActionType::CancelJob { job_id: *job_id },
                parameters: HashMap::new(),
                estimated_impact: "Job will be cancelled".to_string(),
                requires_confirmation: true,
            });
        }
    }

    // Add template-based actions
    for template in templates {
        actions.push(SuggestedAction {
            action_id: Uuid::new_v4().to_string(),
            label: template.label.clone(),
            description: template.description.clone(),
            action_type: template.action_type.clone(),
            parameters: HashMap::new(),
            estimated_impact: template.estimated_impact.clone(),
            requires_confirmation: template.requires_confirmation,
        });
    }

    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_pattern_school_hours() {
        let pattern = SchedulePattern::school_hours();

        // Create a datetime for Monday 3pm
        let dt = Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(15, 0, 0).unwrap())
            .and_local_timezone(Utc)
            .unwrap();

        let time_until = pattern.time_until_offline(&dt);
        assert!(time_until.is_some());

        // Should be 2 hours (until 5pm)
        let duration = time_until.unwrap();
        assert_eq!(duration.num_hours(), 2);
    }

    #[test]
    fn test_alert_creation() {
        let (store, _rx) = AlertStore::new(100);
        let rule = AlertRule::default_rules().into_iter().next().unwrap();

        let condition = AlertCondition::NodeIdleTooLong {
            worker_id: WorkerId::new(),
            idle_duration_mins: 45,
            threshold_mins: 30,
        };

        let alert = store.create_alert(&rule, condition);
        assert!(alert.is_some());

        let alerts = store.get_active_alerts();
        assert_eq!(alerts.len(), 1);
    }

    #[test]
    fn test_alert_cooldown() {
        let (store, _rx) = AlertStore::new(100);
        let rule = AlertRule::default_rules().into_iter().next().unwrap();

        let condition = AlertCondition::NodeIdleTooLong {
            worker_id: WorkerId::new(),
            idle_duration_mins: 45,
            threshold_mins: 30,
        };

        // First alert should succeed
        let alert1 = store.create_alert(&rule, condition.clone());
        assert!(alert1.is_some());

        // Second alert within cooldown should fail
        let alert2 = store.create_alert(&rule, condition);
        assert!(alert2.is_none());
    }

    #[test]
    fn test_alert_dismiss() {
        let (store, _rx) = AlertStore::new(100);
        let rule = AlertRule::default_rules().into_iter().next().unwrap();

        let condition = AlertCondition::NodeIdleTooLong {
            worker_id: WorkerId::new(),
            idle_duration_mins: 45,
            threshold_mins: 30,
        };

        let alert = store.create_alert(&rule, condition).unwrap();
        assert_eq!(store.get_active_alerts().len(), 1);

        store.dismiss_alert(alert.id, Some("test_user".to_string()));
        assert_eq!(store.get_active_alerts().len(), 0);

        // Should be in history
        let history = store.get_history(10);
        assert_eq!(history.len(), 1);
    }
}
