// Marabunta - Licensed under the MIT License.
//! Graceful degradation management
//!
//! Manages system behavior under stress conditions, automatically reducing
//! functionality to maintain core operations when resources are constrained.

use chrono::Duration;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::policy::NotificationChannel;
use super::types::ResourceType;

/// Current degradation level of the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum DegradationLevel {
    /// Everything working normally
    #[default]
    Normal,
    /// Minor issues detected, increased monitoring
    Elevated,
    /// Reduced functionality active
    Degraded,
    /// Minimal functionality only
    Critical,
    /// Survival mode - only essential operations
    Emergency,
}


impl DegradationLevel {
    /// Get a human-readable description of this level
    pub fn description(&self) -> &'static str {
        match self {
            Self::Normal => "Normal operations",
            Self::Elevated => "Elevated - minor issues detected, monitoring",
            Self::Degraded => "Degraded - reduced functionality active",
            Self::Critical => "Critical - minimal functionality only",
            Self::Emergency => "Emergency - survival mode, essential operations only",
        }
    }

    /// Get all levels in order
    pub fn all() -> &'static [DegradationLevel] {
        &[
            Self::Normal,
            Self::Elevated,
            Self::Degraded,
            Self::Critical,
            Self::Emergency,
        ]
    }
}

/// Conditions that trigger degradation level changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DegradationCondition {
    /// Failure rate exceeds threshold
    FailureRate {
        /// Failures per minute
        rate: f64,
        /// Time window to measure
        window: Duration,
    },
    /// Node availability falls below threshold
    NodeAvailability {
        /// Minimum percentage of nodes that must be available
        min_percent: f64,
    },
    /// Job queue depth exceeds threshold
    QueueDepth {
        /// Maximum number of jobs in queue
        max_jobs: u32,
    },
    /// Resource utilization exceeds threshold
    ResourceUtilization {
        /// Resource type to check
        resource: ResourceType,
        /// Maximum utilization percentage
        max_percent: f64,
    },
    /// Coordinator health below threshold
    CoordinatorHealth {
        /// Minimum number of healthy coordinators
        min_healthy: u32,
    },
    /// All conditions must be true
    And(Vec<DegradationCondition>),
    /// Any condition must be true
    Or(Vec<DegradationCondition>),
}

impl DegradationCondition {
    /// Create a failure rate condition
    pub fn failure_rate(rate: f64, window_secs: i64) -> Self {
        Self::FailureRate {
            rate,
            window: Duration::seconds(window_secs),
        }
    }

    /// Create a node availability condition
    pub fn node_availability(min_percent: f64) -> Self {
        Self::NodeAvailability { min_percent }
    }

    /// Create a queue depth condition
    pub fn queue_depth(max_jobs: u32) -> Self {
        Self::QueueDepth { max_jobs }
    }

    /// Create a resource utilization condition
    pub fn resource_utilization(resource: ResourceType, max_percent: f64) -> Self {
        Self::ResourceUtilization {
            resource,
            max_percent,
        }
    }

    /// Evaluate this condition against system state
    pub fn evaluate(&self, state: &SystemState) -> bool {
        match self {
            Self::FailureRate { rate, .. } => state.failure_rate >= *rate,
            Self::NodeAvailability { min_percent } => state.node_availability < *min_percent,
            Self::QueueDepth { max_jobs } => state.queue_depth >= *max_jobs,
            Self::ResourceUtilization {
                resource,
                max_percent,
            } => state
                .resource_utilization
                .get(resource)
                .map(|u| *u >= *max_percent)
                .unwrap_or(false),
            Self::CoordinatorHealth { min_healthy } => state.healthy_coordinators < *min_healthy,
            Self::And(conditions) => conditions.iter().all(|c| c.evaluate(state)),
            Self::Or(conditions) => conditions.iter().any(|c| c.evaluate(state)),
        }
    }
}

/// Thresholds for each degradation level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DegradationThresholds {
    /// Condition for elevated level
    pub elevated: DegradationCondition,
    /// Condition for degraded level
    pub degraded: DegradationCondition,
    /// Condition for critical level
    pub critical: DegradationCondition,
    /// Condition for emergency level
    pub emergency: DegradationCondition,
}

impl Default for DegradationThresholds {
    fn default() -> Self {
        Self {
            elevated: DegradationCondition::Or(vec![
                DegradationCondition::failure_rate(5.0, 300), // 5 failures/min for 5 min
                DegradationCondition::node_availability(90.0), // <90% nodes available
            ]),
            degraded: DegradationCondition::Or(vec![
                DegradationCondition::failure_rate(10.0, 300), // 10 failures/min
                DegradationCondition::node_availability(70.0), // <70% nodes
                DegradationCondition::queue_depth(10000),      // 10k jobs queued
            ]),
            critical: DegradationCondition::Or(vec![
                DegradationCondition::failure_rate(20.0, 300), // 20 failures/min
                DegradationCondition::node_availability(50.0), // <50% nodes
                DegradationCondition::CoordinatorHealth { min_healthy: 2 },
            ]),
            emergency: DegradationCondition::Or(vec![
                DegradationCondition::node_availability(25.0), // <25% nodes
                DegradationCondition::CoordinatorHealth { min_healthy: 1 },
            ]),
        }
    }
}

/// Actions to take when degradation level changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DegradationAction {
    /// Disable a specific feature
    DisableFeature { feature: String },

    /// Reject new jobs (except matching patterns)
    RejectNewJobs { except: Vec<String> },

    /// Reject jobs below priority threshold
    RejectLowPriority { threshold: i32 },

    /// Cancel oldest jobs to reduce load
    CancelOldestJobs { count: u32 },

    /// Pause non-critical jobs
    PauseNonCriticalJobs,

    /// Limit concurrent task execution
    LimitConcurrency { max_tasks: u32 },

    /// Disable checkpointing to save resources
    DisableCheckpointing,

    /// Reduce monitoring frequency
    ReduceMonitoringFrequency { factor: f64 },

    /// Prioritize system jobs over user jobs
    PrioritizeSystemJobs,

    /// Bypass non-essential policies
    BypassNonEssentialPolicies,

    /// Send an alert
    Alert {
        message: String,
        channels: Vec<NotificationChannel>,
    },

    /// Custom action
    Custom {
        action_id: String,
        params: HashMap<String, String>,
    },
}

impl DegradationAction {
    /// Get a description of this action
    pub fn description(&self) -> String {
        match self {
            Self::DisableFeature { feature } => format!("Disable feature: {}", feature),
            Self::RejectNewJobs { except } => {
                if except.is_empty() {
                    "Reject all new jobs".to_string()
                } else {
                    format!("Reject new jobs except: {:?}", except)
                }
            }
            Self::RejectLowPriority { threshold } => {
                format!("Reject jobs with priority below {}", threshold)
            }
            Self::CancelOldestJobs { count } => format!("Cancel {} oldest jobs", count),
            Self::PauseNonCriticalJobs => "Pause non-critical jobs".to_string(),
            Self::LimitConcurrency { max_tasks } => {
                format!("Limit concurrency to {} tasks", max_tasks)
            }
            Self::DisableCheckpointing => "Disable checkpointing".to_string(),
            Self::ReduceMonitoringFrequency { factor } => {
                format!("Reduce monitoring frequency by {}x", factor)
            }
            Self::PrioritizeSystemJobs => "Prioritize system jobs".to_string(),
            Self::BypassNonEssentialPolicies => "Bypass non-essential policies".to_string(),
            Self::Alert { message, .. } => format!("Alert: {}", message),
            Self::Custom { action_id, .. } => format!("Custom action: {}", action_id),
        }
    }

    /// Check if this action involves feature disabling
    pub fn disables_feature(&self, feature: &str) -> bool {
        match self {
            Self::DisableFeature { feature: f } => f == feature,
            _ => false,
        }
    }
}

/// Current system state for degradation evaluation
#[derive(Debug, Clone, Default)]
pub struct SystemState {
    /// Current failure rate (failures per minute)
    pub failure_rate: f64,
    /// Percentage of nodes available (0-100)
    pub node_availability: f64,
    /// Number of jobs in queue
    pub queue_depth: u32,
    /// Resource utilization by type (0-100 percent)
    pub resource_utilization: HashMap<ResourceType, f64>,
    /// Number of healthy coordinator nodes
    pub healthy_coordinators: u32,
}

impl SystemState {
    /// Create a new system state
    pub fn new() -> Self {
        Self::default()
    }

    /// Set failure rate
    pub fn with_failure_rate(mut self, rate: f64) -> Self {
        self.failure_rate = rate;
        self
    }

    /// Set node availability
    pub fn with_node_availability(mut self, percent: f64) -> Self {
        self.node_availability = percent;
        self
    }

    /// Set queue depth
    pub fn with_queue_depth(mut self, depth: u32) -> Self {
        self.queue_depth = depth;
        self
    }

    /// Set resource utilization
    pub fn with_resource_utilization(mut self, resource: ResourceType, percent: f64) -> Self {
        self.resource_utilization.insert(resource, percent);
        self
    }

    /// Set healthy coordinator count
    pub fn with_healthy_coordinators(mut self, count: u32) -> Self {
        self.healthy_coordinators = count;
        self
    }
}

/// Result of a degradation level transition
#[derive(Debug, Clone)]
pub struct DegradationTransition {
    /// Previous level
    pub from: DegradationLevel,
    /// New level
    pub to: DegradationLevel,
    /// Reason for the transition
    pub reason: String,
    /// Actions that were activated
    pub actions_activated: Vec<DegradationAction>,
    /// Actions that were deactivated
    pub actions_deactivated: Vec<DegradationAction>,
}

impl DegradationTransition {
    /// Check if this represents an actual change
    pub fn is_change(&self) -> bool {
        self.from != self.to
    }

    /// Check if this is an escalation (getting worse)
    pub fn is_escalation(&self) -> bool {
        self.to > self.from
    }

    /// Check if this is a de-escalation (getting better)
    pub fn is_deescalation(&self) -> bool {
        self.to < self.from
    }
}

/// Manages graceful degradation when system is under stress
pub struct DegradationManager {
    /// Current degradation level
    current_level: DegradationLevel,
    /// Thresholds for level transitions
    thresholds: DegradationThresholds,
    /// Actions to take at each level
    actions: HashMap<DegradationLevel, Vec<DegradationAction>>,
    /// Currently active actions
    active_actions: Vec<DegradationAction>,
    /// Whether level was manually forced
    forced: bool,
    /// Disabled features (for quick lookup)
    disabled_features: Vec<String>,
    /// Job rejection patterns
    rejection_config: JobRejectionConfig,
}

#[derive(Debug, Clone, Default)]
struct JobRejectionConfig {
    /// Whether to reject all new jobs
    reject_all: bool,
    /// Patterns for jobs that are still accepted
    except_patterns: Vec<String>,
    /// Priority threshold (jobs below this are rejected)
    priority_threshold: Option<i32>,
}

impl DegradationManager {
    /// Create a new degradation manager with the given thresholds
    pub fn new(thresholds: DegradationThresholds) -> Self {
        Self {
            current_level: DegradationLevel::Normal,
            thresholds,
            actions: HashMap::new(),
            active_actions: Vec::new(),
            forced: false,
            disabled_features: Vec::new(),
            rejection_config: JobRejectionConfig::default(),
        }
    }

    /// Create a degradation manager with default thresholds
    pub fn with_defaults() -> Self {
        let mut manager = Self::new(DegradationThresholds::default());
        manager.set_default_actions();
        manager
    }

    /// Set up default actions for each level
    fn set_default_actions(&mut self) {
        // Elevated: just increase monitoring
        self.actions.insert(
            DegradationLevel::Elevated,
            vec![DegradationAction::Alert {
                message: "System entering elevated state".to_string(),
                channels: vec![NotificationChannel::Log {
                    level: "warn".to_string(),
                }],
            }],
        );

        // Degraded: start shedding load
        self.actions.insert(
            DegradationLevel::Degraded,
            vec![
                DegradationAction::RejectLowPriority { threshold: 50 },
                DegradationAction::ReduceMonitoringFrequency { factor: 2.0 },
                DegradationAction::Alert {
                    message: "System in degraded state".to_string(),
                    channels: vec![NotificationChannel::Log {
                        level: "error".to_string(),
                    }],
                },
            ],
        );

        // Critical: aggressive load shedding
        self.actions.insert(
            DegradationLevel::Critical,
            vec![
                DegradationAction::RejectLowPriority { threshold: 80 },
                DegradationAction::PauseNonCriticalJobs,
                DegradationAction::LimitConcurrency { max_tasks: 1000 },
                DegradationAction::DisableCheckpointing,
                DegradationAction::Alert {
                    message: "CRITICAL: System in critical state".to_string(),
                    channels: vec![NotificationChannel::Log {
                        level: "error".to_string(),
                    }],
                },
            ],
        );

        // Emergency: survival mode
        self.actions.insert(
            DegradationLevel::Emergency,
            vec![
                DegradationAction::RejectNewJobs {
                    except: vec!["system-*".to_string()],
                },
                DegradationAction::CancelOldestJobs { count: 100 },
                DegradationAction::PrioritizeSystemJobs,
                DegradationAction::BypassNonEssentialPolicies,
                DegradationAction::Alert {
                    message: "EMERGENCY: System in survival mode".to_string(),
                    channels: vec![NotificationChannel::Log {
                        level: "error".to_string(),
                    }],
                },
            ],
        );
    }

    /// Set actions for a degradation level
    pub fn set_actions(&mut self, level: DegradationLevel, actions: Vec<DegradationAction>) {
        self.actions.insert(level, actions);
    }

    /// Evaluate current system state and update degradation level
    pub fn evaluate(&mut self, state: &SystemState) -> DegradationTransition {
        if self.forced {
            // Don't change level if manually forced
            return DegradationTransition {
                from: self.current_level,
                to: self.current_level,
                reason: "Level manually forced".to_string(),
                actions_activated: Vec::new(),
                actions_deactivated: Vec::new(),
            };
        }

        let new_level = self.determine_level(state);
        self.transition_to(new_level, "automatic evaluation")
    }

    /// Determine the appropriate level based on system state
    fn determine_level(&self, state: &SystemState) -> DegradationLevel {
        // Check from highest to lowest
        if self.thresholds.emergency.evaluate(state) {
            return DegradationLevel::Emergency;
        }
        if self.thresholds.critical.evaluate(state) {
            return DegradationLevel::Critical;
        }
        if self.thresholds.degraded.evaluate(state) {
            return DegradationLevel::Degraded;
        }
        if self.thresholds.elevated.evaluate(state) {
            return DegradationLevel::Elevated;
        }
        DegradationLevel::Normal
    }

    /// Get current degradation level
    pub fn current_level(&self) -> DegradationLevel {
        self.current_level
    }

    /// Get active degradation actions
    pub fn active_actions(&self) -> &[DegradationAction] {
        &self.active_actions
    }

    /// Check if a feature is currently disabled
    pub fn is_feature_disabled(&self, feature: &str) -> bool {
        self.disabled_features.iter().any(|f| f == feature)
    }

    /// Check if a job should be accepted at current degradation level
    pub fn should_accept_job(&self, job_priority: i32, job_pattern: &str) -> bool {
        // If rejecting all jobs, check exceptions
        if self.rejection_config.reject_all {
            for pattern in &self.rejection_config.except_patterns {
                if glob_match(pattern, job_pattern) {
                    return true;
                }
            }
            return false;
        }

        // Check priority threshold
        if let Some(threshold) = self.rejection_config.priority_threshold {
            if job_priority < threshold {
                return false;
            }
        }

        true
    }

    /// Force a degradation level (for manual intervention)
    pub fn force_level(&mut self, level: DegradationLevel) -> DegradationTransition {
        self.forced = true;
        self.transition_to(level, "manual override")
    }

    /// Reset to normal (after manual intervention)
    pub fn reset(&mut self) -> DegradationTransition {
        self.forced = false;
        self.transition_to(DegradationLevel::Normal, "manual reset")
    }

    /// Internal transition logic
    fn transition_to(
        &mut self,
        new_level: DegradationLevel,
        reason: &str,
    ) -> DegradationTransition {
        let old_level = self.current_level;

        if new_level == old_level {
            return DegradationTransition {
                from: old_level,
                to: new_level,
                reason: "No change needed".to_string(),
                actions_activated: Vec::new(),
                actions_deactivated: Vec::new(),
            };
        }

        info!(
            "Degradation level transitioning from {:?} to {:?}: {}",
            old_level, new_level, reason
        );

        // Collect actions to activate and deactivate
        let old_actions = self.get_actions_for_level(old_level);
        let new_actions = self.get_actions_for_level(new_level);

        // Actions to deactivate (in old but not in new)
        let actions_deactivated: Vec<_> = old_actions
            .iter()
            .filter(|a| !new_actions.iter().any(|na| action_eq(a, na)))
            .cloned()
            .collect();

        // Actions to activate (in new but not in old)
        let actions_activated: Vec<_> = new_actions
            .iter()
            .filter(|a| !old_actions.iter().any(|oa| action_eq(a, oa)))
            .cloned()
            .collect();

        // Apply deactivations
        for action in &actions_deactivated {
            self.deactivate_action(action);
        }

        // Apply activations
        for action in &actions_activated {
            self.activate_action(action);
        }

        // Update current level and active actions
        self.current_level = new_level;
        self.active_actions = new_actions;

        DegradationTransition {
            from: old_level,
            to: new_level,
            reason: reason.to_string(),
            actions_activated,
            actions_deactivated,
        }
    }

    /// Get all actions that should be active at a given level
    fn get_actions_for_level(&self, level: DegradationLevel) -> Vec<DegradationAction> {
        let mut actions = Vec::new();

        // Accumulate actions from all levels up to and including the target level
        for l in DegradationLevel::all() {
            if *l > level {
                break;
            }
            if *l == DegradationLevel::Normal {
                continue;
            }
            if let Some(level_actions) = self.actions.get(l) {
                actions.extend(level_actions.iter().cloned());
            }
        }

        actions
    }

    /// Activate a degradation action
    fn activate_action(&mut self, action: &DegradationAction) {
        debug!("Activating degradation action: {}", action.description());

        match action {
            DegradationAction::DisableFeature { feature } => {
                if !self.disabled_features.contains(feature) {
                    self.disabled_features.push(feature.clone());
                }
            }
            DegradationAction::RejectNewJobs { except } => {
                self.rejection_config.reject_all = true;
                self.rejection_config.except_patterns = except.clone();
            }
            DegradationAction::RejectLowPriority { threshold } => {
                self.rejection_config.priority_threshold = Some(*threshold);
            }
            _ => {
                // Other actions are handled by the caller
            }
        }
    }

    /// Deactivate a degradation action
    fn deactivate_action(&mut self, action: &DegradationAction) {
        debug!("Deactivating degradation action: {}", action.description());

        match action {
            DegradationAction::DisableFeature { feature } => {
                self.disabled_features.retain(|f| f != feature);
            }
            DegradationAction::RejectNewJobs { .. } => {
                self.rejection_config.reject_all = false;
                self.rejection_config.except_patterns.clear();
            }
            DegradationAction::RejectLowPriority { .. } => {
                self.rejection_config.priority_threshold = None;
            }
            _ => {
                // Other actions are handled by the caller
            }
        }
    }
}

/// Simple glob-style pattern matching
fn glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return value.ends_with(suffix);
    }
    pattern == value
}

/// Check if two actions are equivalent
fn action_eq(a: &DegradationAction, b: &DegradationAction) -> bool {
    // Simple discriminant comparison for most cases
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_degradation_level_ordering() {
        assert!(DegradationLevel::Normal < DegradationLevel::Elevated);
        assert!(DegradationLevel::Elevated < DegradationLevel::Degraded);
        assert!(DegradationLevel::Degraded < DegradationLevel::Critical);
        assert!(DegradationLevel::Critical < DegradationLevel::Emergency);
    }

    #[test]
    fn test_condition_evaluation() {
        let state = SystemState::new()
            .with_failure_rate(15.0)
            .with_node_availability(80.0)
            .with_queue_depth(5000);

        // Should trigger: 15 > 10
        let high_rate = DegradationCondition::failure_rate(10.0, 300);
        assert!(high_rate.evaluate(&state));

        // Should not trigger: 80 > 70
        let low_avail = DegradationCondition::node_availability(70.0);
        assert!(!low_avail.evaluate(&state));

        // Should trigger: 80 < 90
        let high_avail = DegradationCondition::node_availability(90.0);
        assert!(high_avail.evaluate(&state));

        // Combined OR: should trigger
        let combined = DegradationCondition::Or(vec![
            DegradationCondition::failure_rate(20.0, 300), // false
            DegradationCondition::node_availability(90.0), // true
        ]);
        assert!(combined.evaluate(&state));

        // Combined AND: should not trigger
        let combined = DegradationCondition::And(vec![
            DegradationCondition::failure_rate(10.0, 300), // true
            DegradationCondition::node_availability(70.0), // false
        ]);
        assert!(!combined.evaluate(&state));
    }

    #[test]
    fn test_degradation_transition() {
        let mut manager = DegradationManager::with_defaults();

        assert_eq!(manager.current_level(), DegradationLevel::Normal);

        // Trigger elevated state
        let state = SystemState::new()
            .with_failure_rate(7.0)
            .with_node_availability(95.0)
            .with_healthy_coordinators(3);

        let transition = manager.evaluate(&state);
        assert!(transition.is_escalation());
        assert_eq!(transition.to, DegradationLevel::Elevated);

        // Trigger degraded state
        let state = SystemState::new()
            .with_failure_rate(12.0)
            .with_node_availability(80.0)
            .with_healthy_coordinators(3);

        let transition = manager.evaluate(&state);
        assert!(transition.is_escalation());
        assert_eq!(transition.to, DegradationLevel::Degraded);

        // Return to normal
        let state = SystemState::new()
            .with_failure_rate(1.0)
            .with_node_availability(99.0)
            .with_healthy_coordinators(5);

        let transition = manager.evaluate(&state);
        assert!(transition.is_deescalation());
        assert_eq!(transition.to, DegradationLevel::Normal);
    }

    #[test]
    fn test_force_level() {
        let mut manager = DegradationManager::with_defaults();

        // Force emergency level
        let transition = manager.force_level(DegradationLevel::Emergency);
        assert_eq!(transition.to, DegradationLevel::Emergency);

        // Evaluate should not change forced level
        let state = SystemState::new()
            .with_failure_rate(1.0)
            .with_node_availability(99.0)
            .with_healthy_coordinators(5);

        let transition = manager.evaluate(&state);
        assert!(!transition.is_change());
        assert_eq!(manager.current_level(), DegradationLevel::Emergency);

        // Reset should return to normal
        let transition = manager.reset();
        assert!(transition.is_deescalation());
        assert_eq!(transition.to, DegradationLevel::Normal);
    }

    #[test]
    fn test_job_acceptance() {
        let mut manager = DegradationManager::with_defaults();

        // At normal level, all jobs accepted
        assert!(manager.should_accept_job(10, "any-job"));
        assert!(manager.should_accept_job(100, "any-job"));

        // Move to degraded - low priority jobs rejected
        let state = SystemState::new()
            .with_failure_rate(12.0)
            .with_node_availability(65.0)
            .with_healthy_coordinators(3);

        manager.evaluate(&state);
        assert_eq!(manager.current_level(), DegradationLevel::Degraded);

        // Priority 60 > threshold 50, should be accepted
        assert!(manager.should_accept_job(60, "test-job"));
        // Priority 30 < threshold 50, should be rejected
        assert!(!manager.should_accept_job(30, "test-job"));

        // Move to emergency - only system jobs accepted
        let state = SystemState::new()
            .with_node_availability(20.0)
            .with_healthy_coordinators(1);

        manager.evaluate(&state);
        assert_eq!(manager.current_level(), DegradationLevel::Emergency);

        // System jobs accepted
        assert!(manager.should_accept_job(10, "system-monitor"));
        assert!(manager.should_accept_job(100, "system-health"));
        // Non-system jobs rejected
        assert!(!manager.should_accept_job(100, "user-job"));
    }

    #[test]
    fn test_feature_disabling() {
        let mut manager = DegradationManager::new(DegradationThresholds::default());

        manager.set_actions(
            DegradationLevel::Degraded,
            vec![DegradationAction::DisableFeature {
                feature: "expensive-analytics".to_string(),
            }],
        );

        assert!(!manager.is_feature_disabled("expensive-analytics"));

        // Trigger degraded state
        let state = SystemState::new()
            .with_failure_rate(15.0)
            .with_node_availability(65.0)
            .with_healthy_coordinators(3);

        manager.evaluate(&state);

        assert!(manager.is_feature_disabled("expensive-analytics"));

        // Return to normal
        let state = SystemState::new()
            .with_failure_rate(1.0)
            .with_node_availability(99.0)
            .with_healthy_coordinators(5);

        manager.evaluate(&state);

        assert!(!manager.is_feature_disabled("expensive-analytics"));
    }

    #[test]
    fn test_action_accumulation() {
        let mut manager = DegradationManager::with_defaults();

        // At critical level, should have actions from elevated, degraded, and critical
        let state = SystemState::new()
            .with_failure_rate(25.0)
            .with_node_availability(45.0)
            .with_healthy_coordinators(2);

        let transition = manager.evaluate(&state);
        assert_eq!(transition.to, DegradationLevel::Critical);

        // Should have multiple actions from different levels
        assert!(!manager.active_actions().is_empty());

        // Check that we have actions from multiple levels
        let has_limit_concurrency = manager
            .active_actions()
            .iter()
            .any(|a| matches!(a, DegradationAction::LimitConcurrency { .. }));
        assert!(has_limit_concurrency);
    }

    #[test]
    fn test_transition_actions() {
        let mut manager = DegradationManager::with_defaults();

        // Move directly to critical
        let state = SystemState::new()
            .with_failure_rate(25.0)
            .with_node_availability(45.0)
            .with_healthy_coordinators(2);

        let transition = manager.evaluate(&state);

        // Should have activated actions from elevated, degraded, and critical
        assert!(!transition.actions_activated.is_empty());
        assert!(transition.actions_deactivated.is_empty());

        // Move back to normal
        let state = SystemState::new()
            .with_failure_rate(1.0)
            .with_node_availability(99.0)
            .with_healthy_coordinators(5);

        let transition = manager.evaluate(&state);

        // Should have deactivated all actions
        assert!(transition.actions_activated.is_empty());
        assert!(!transition.actions_deactivated.is_empty());
    }
}
