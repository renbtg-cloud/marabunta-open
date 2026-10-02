// Marabunta - Licensed under the MIT License.
//! Recovery policies for matching failures to recovery strategies
//!
//! Policies define which recovery strategies should be applied to which failures,
//! along with notification, logging, and circuit breaker configurations.

use chrono::Duration;
use serde::{Deserialize, Serialize};

use super::recovery::RecoveryStrategy;
use super::types::{Failure, FailureSeverity, FailureType};

/// A recovery policy that matches failures and specifies recovery behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryPolicy {
    /// Unique identifier for this policy
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Description of what this policy does
    pub description: String,
    /// Matcher to determine which failures this policy handles
    pub matches: FailureMatcher,
    /// Recovery strategy to apply
    pub strategy: RecoveryStrategy,
    /// Notification configuration
    pub notification: NotificationPolicy,
    /// Logging configuration
    pub logging: LoggingPolicy,
    /// Optional circuit breaker configuration
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    /// Authority domain (for multi-tenant governance)
    pub authority_domain: String,
    /// Priority (higher values are tried first)
    pub priority: i32,
}

impl RecoveryPolicy {
    /// Create a new policy with minimal configuration
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        matches: FailureMatcher,
        strategy: RecoveryStrategy,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            description: String::new(),
            matches,
            strategy,
            notification: NotificationPolicy::default(),
            logging: LoggingPolicy::default(),
            circuit_breaker: None,
            authority_domain: "default".to_string(),
            priority: 0,
        }
    }

    /// Set the description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the notification policy
    pub fn with_notification(mut self, notification: NotificationPolicy) -> Self {
        self.notification = notification;
        self
    }

    /// Set the logging policy
    pub fn with_logging(mut self, logging: LoggingPolicy) -> Self {
        self.logging = logging;
        self
    }

    /// Set the circuit breaker configuration
    pub fn with_circuit_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = Some(config);
        self
    }

    /// Set the authority domain
    pub fn with_authority_domain(mut self, domain: impl Into<String>) -> Self {
        self.authority_domain = domain.into();
        self
    }

    /// Set the priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Check if this policy matches a given failure
    pub fn matches_failure(&self, failure: &Failure) -> bool {
        self.matches.matches(failure)
    }
}

/// Matcher for determining which failures a policy handles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailureMatcher {
    /// Match specific failure types
    FailureType(Vec<FailureType>),
    /// Match by severity levels
    Severity(Vec<FailureSeverity>),
    /// Match by job ID pattern (glob-style)
    JobPattern(String),
    /// Match by task ID pattern
    TaskPattern(String),
    /// Match by node ID pattern
    NodePattern(String),
    /// Match by principal (user/service) pattern
    PrincipalPattern(String),
    /// Logical AND of multiple matchers
    And(Vec<FailureMatcher>),
    /// Logical OR of multiple matchers
    Or(Vec<FailureMatcher>),
    /// Logical NOT of a matcher
    Not(Box<FailureMatcher>),
    /// Match all failures
    All,
}

impl FailureMatcher {
    /// Create a matcher for specific failure types
    pub fn failure_types(types: Vec<FailureType>) -> Self {
        Self::FailureType(types)
    }

    /// Create a matcher for severity levels
    pub fn severities(levels: Vec<FailureSeverity>) -> Self {
        Self::Severity(levels)
    }

    /// Create a matcher for job ID pattern
    pub fn job_pattern(pattern: impl Into<String>) -> Self {
        Self::JobPattern(pattern.into())
    }

    /// Create an AND matcher
    pub fn and(matchers: Vec<FailureMatcher>) -> Self {
        Self::And(matchers)
    }

    /// Create an OR matcher
    pub fn or(matchers: Vec<FailureMatcher>) -> Self {
        Self::Or(matchers)
    }

    /// Create a NOT matcher
    pub fn not(matcher: FailureMatcher) -> Self {
        Self::Not(Box::new(matcher))
    }

    /// Check if this matcher matches a given failure
    pub fn matches(&self, failure: &Failure) -> bool {
        match self {
            Self::FailureType(types) => types.iter().any(|t| failure.failure_type.matches(t)),
            Self::Severity(levels) => levels.contains(&failure.severity),
            Self::JobPattern(pattern) => {
                if let Some(job_id) = &failure.context.job_id {
                    glob_match(pattern, job_id)
                } else {
                    false
                }
            }
            Self::TaskPattern(pattern) => {
                if let Some(task_id) = &failure.context.task_id {
                    glob_match(pattern, task_id)
                } else {
                    false
                }
            }
            Self::NodePattern(pattern) => {
                if let Some(node_id) = &failure.context.node_id {
                    glob_match(pattern, node_id)
                } else {
                    false
                }
            }
            Self::PrincipalPattern(pattern) => {
                if let Some(principal) = &failure.context.principal_id {
                    glob_match(pattern, principal)
                } else {
                    false
                }
            }
            Self::And(matchers) => matchers.iter().all(|m| m.matches(failure)),
            Self::Or(matchers) => matchers.iter().any(|m| m.matches(failure)),
            Self::Not(matcher) => !matcher.matches(failure),
            Self::All => true,
        }
    }
}

/// Simple glob-style pattern matching
fn glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if pattern.contains('*') {
        // Handle prefix match (e.g., "job-*")
        if let Some(prefix) = pattern.strip_suffix('*') {
            return value.starts_with(prefix);
        }

        // Handle suffix match (e.g., "*-production")
        if let Some(suffix) = pattern.strip_prefix('*') {
            return value.ends_with(suffix);
        }

        // Handle contains match (e.g., "*test*")
        if pattern.starts_with('*') && pattern.ends_with('*') {
            let middle = &pattern[1..pattern.len() - 1];
            return value.contains(middle);
        }

        // Handle more complex patterns with single * in middle
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            return value.starts_with(parts[0]) && value.ends_with(parts[1]);
        }
    }

    // Exact match
    pattern == value
}

/// Notification policy for a recovery policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPolicy {
    /// Whether to notify when failure occurs
    pub notify_on_failure: bool,
    /// Whether to notify when recovery succeeds
    pub notify_on_recovery: bool,
    /// Whether to notify when failure is escalated
    pub notify_on_escalation: bool,
    /// Channels to send notifications to
    pub channels: Vec<NotificationChannel>,
    /// Minimum time between notifications (throttling)
    pub throttle: Option<Duration>,
}

impl Default for NotificationPolicy {
    fn default() -> Self {
        Self {
            notify_on_failure: false,
            notify_on_recovery: false,
            notify_on_escalation: true,
            channels: Vec::new(),
            throttle: None,
        }
    }
}

impl NotificationPolicy {
    /// Create a notification policy that notifies on all events
    pub fn notify_all(channels: Vec<NotificationChannel>) -> Self {
        Self {
            notify_on_failure: true,
            notify_on_recovery: true,
            notify_on_escalation: true,
            channels,
            throttle: None,
        }
    }

    /// Create a notification policy that only notifies on escalation
    pub fn escalation_only(channels: Vec<NotificationChannel>) -> Self {
        Self {
            notify_on_failure: false,
            notify_on_recovery: false,
            notify_on_escalation: true,
            channels,
            throttle: None,
        }
    }

    /// Set throttling
    pub fn with_throttle(mut self, throttle: Duration) -> Self {
        self.throttle = Some(throttle);
        self
    }
}

/// Channel for sending notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NotificationChannel {
    /// Email notification
    Email { recipients: Vec<String> },
    /// Webhook notification
    Webhook { url: String, secret: Option<String> },
    /// Slack notification
    Slack { channel: String },
    /// Log notification (write to logs)
    Log { level: String },
    /// Custom notification handler
    Custom { handler_id: String },
}

impl NotificationChannel {
    /// Create an email notification channel
    pub fn email(recipients: Vec<String>) -> Self {
        Self::Email { recipients }
    }

    /// Create a webhook notification channel
    pub fn webhook(url: impl Into<String>) -> Self {
        Self::Webhook {
            url: url.into(),
            secret: None,
        }
    }

    /// Create a Slack notification channel
    pub fn slack(channel: impl Into<String>) -> Self {
        Self::Slack {
            channel: channel.into(),
        }
    }

    /// Create a log notification channel
    pub fn log(level: impl Into<String>) -> Self {
        Self::Log {
            level: level.into(),
        }
    }
}

/// Logging policy for a recovery policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingPolicy {
    /// Whether to log failures
    pub log_failures: bool,
    /// Whether to log recovery attempts
    pub log_recovery_attempts: bool,
    /// Whether to log successful recoveries
    pub log_success: bool,
    /// Whether to include stack traces in logs
    pub include_stack_trace: bool,
    /// How long to retain logs
    pub retention: Duration,
}

impl Default for LoggingPolicy {
    fn default() -> Self {
        Self {
            log_failures: true,
            log_recovery_attempts: true,
            log_success: true,
            include_stack_trace: false,
            retention: Duration::days(30),
        }
    }
}

impl LoggingPolicy {
    /// Create a verbose logging policy
    pub fn verbose() -> Self {
        Self {
            log_failures: true,
            log_recovery_attempts: true,
            log_success: true,
            include_stack_trace: true,
            retention: Duration::days(90),
        }
    }

    /// Create a minimal logging policy
    pub fn minimal() -> Self {
        Self {
            log_failures: true,
            log_recovery_attempts: false,
            log_success: false,
            include_stack_trace: false,
            retention: Duration::days(7),
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of failures before circuit opens
    pub failure_threshold: u32,
    /// Number of successes needed to close circuit
    pub success_threshold: u32,
    /// Time to wait in open state before trying half-open
    pub timeout: Duration,
    /// Scope of the circuit breaker
    pub scope: CircuitBreakerScope,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            timeout: Duration::seconds(60),
            scope: CircuitBreakerScope::Global,
        }
    }
}

impl CircuitBreakerConfig {
    /// Create a per-node circuit breaker
    pub fn per_node(failure_threshold: u32, timeout_secs: i64) -> Self {
        Self {
            failure_threshold,
            success_threshold: 3,
            timeout: Duration::seconds(timeout_secs),
            scope: CircuitBreakerScope::PerNode,
        }
    }

    /// Create a per-job circuit breaker
    pub fn per_job(failure_threshold: u32, timeout_secs: i64) -> Self {
        Self {
            failure_threshold,
            success_threshold: 3,
            timeout: Duration::seconds(timeout_secs),
            scope: CircuitBreakerScope::PerJob,
        }
    }

    /// Create a global circuit breaker
    pub fn global(failure_threshold: u32, timeout_secs: i64) -> Self {
        Self {
            failure_threshold,
            success_threshold: 3,
            timeout: Duration::seconds(timeout_secs),
            scope: CircuitBreakerScope::Global,
        }
    }
}

/// Scope of a circuit breaker
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitBreakerScope {
    /// Separate circuit breaker per node
    PerNode,
    /// Separate circuit breaker per job
    PerJob,
    /// Separate circuit breaker per task type
    PerTaskType,
    /// Single global circuit breaker
    Global,
}

/// Default policies for common scenarios
pub mod defaults {
    use super::*;

    /// Create a default policy for task crashes
    pub fn task_crash_policy() -> RecoveryPolicy {
        RecoveryPolicy::new(
            "default-task-crash",
            "Default Task Crash Policy",
            FailureMatcher::FailureType(vec![FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            }]),
            RecoveryStrategy::exponential_backoff(3),
        )
        .with_description("Retry crashed tasks with exponential backoff")
        .with_priority(0)
    }

    /// Create a default policy for node failures
    pub fn node_failure_policy() -> RecoveryPolicy {
        RecoveryPolicy::new(
            "default-node-failure",
            "Default Node Failure Policy",
            FailureMatcher::Or(vec![
                FailureMatcher::FailureType(vec![FailureType::NodeUnreachable {
                    node_id: String::new(),
                }]),
                FailureMatcher::FailureType(vec![FailureType::NodeCrash {
                    node_id: String::new(),
                    error: None,
                }]),
            ]),
            RecoveryStrategy::retry_different_node(3),
        )
        .with_description("Retry on different node when node fails")
        .with_circuit_breaker(CircuitBreakerConfig::per_node(5, 300))
        .with_priority(10)
    }

    /// Create a default policy for critical failures
    pub fn critical_failure_policy() -> RecoveryPolicy {
        RecoveryPolicy::new(
            "default-critical",
            "Default Critical Failure Policy",
            FailureMatcher::Severity(vec![FailureSeverity::Critical]),
            RecoveryStrategy::Abort {
                save_partial_results: true,
                notify: true,
            },
        )
        .with_description("Abort and notify on critical failures")
        .with_notification(NotificationPolicy::notify_all(vec![
            NotificationChannel::log("error"),
        ]))
        .with_priority(100)
    }

    /// Create a default policy for OOM failures
    pub fn oom_policy() -> RecoveryPolicy {
        RecoveryPolicy::new(
            "default-oom",
            "Default OOM Policy",
            FailureMatcher::FailureType(vec![FailureType::TaskOom {
                memory_used: 0,
                memory_limit: 0,
            }]),
            RecoveryStrategy::cascade(vec![
                RecoveryStrategy::retry_different_node(2),
                RecoveryStrategy::Abort {
                    save_partial_results: true,
                    notify: true,
                },
            ]),
        )
        .with_description("Retry OOM tasks on nodes with more memory")
        .with_priority(20)
    }

    /// Get all default policies, sorted by priority (descending)
    pub fn all() -> Vec<RecoveryPolicy> {
        vec![
            critical_failure_policy(), // priority 100
            oom_policy(),              // priority 20
            node_failure_policy(),     // priority 10
            task_crash_policy(),       // priority 0
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure::types::FailureContext;

    #[test]
    fn test_glob_match() {
        // Exact match
        assert!(glob_match("job-123", "job-123"));
        assert!(!glob_match("job-123", "job-456"));

        // Wildcard match
        assert!(glob_match("*", "anything"));

        // Prefix match
        assert!(glob_match("job-*", "job-123"));
        assert!(glob_match("job-*", "job-abc"));
        assert!(!glob_match("job-*", "task-123"));

        // Suffix match
        assert!(glob_match("*-production", "job-production"));
        assert!(!glob_match("*-production", "job-staging"));
    }

    #[test]
    fn test_failure_matcher_all() {
        let matcher = FailureMatcher::All;
        let failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: Some(1),
                stderr: None,
            },
            FailureContext::new(),
        );
        assert!(matcher.matches(&failure));
    }

    #[test]
    fn test_failure_matcher_severity() {
        let matcher =
            FailureMatcher::Severity(vec![FailureSeverity::High, FailureSeverity::Critical]);

        let low_failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: Some(1),
                stderr: None,
            },
            FailureContext::new(),
        );
        assert!(!matcher.matches(&low_failure));

        let critical_failure = Failure::new(FailureType::QuorumLost, FailureContext::new());
        assert!(matcher.matches(&critical_failure));
    }

    #[test]
    fn test_failure_matcher_job_pattern() {
        let matcher = FailureMatcher::JobPattern("ml-*".to_string());

        let matching = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::for_job("ml-training-123"),
        );
        assert!(matcher.matches(&matching));

        let not_matching = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::for_job("etl-job-456"),
        );
        assert!(!matcher.matches(&not_matching));
    }

    #[test]
    fn test_failure_matcher_and() {
        let matcher = FailureMatcher::And(vec![
            FailureMatcher::Severity(vec![FailureSeverity::Low]),
            FailureMatcher::JobPattern("test-*".to_string()),
        ]);

        let matching = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::for_job("test-job"),
        );
        assert!(matcher.matches(&matching));

        let wrong_severity =
            Failure::new(FailureType::QuorumLost, FailureContext::for_job("test-job"));
        assert!(!matcher.matches(&wrong_severity));
    }

    #[test]
    fn test_failure_matcher_or() {
        let matcher = FailureMatcher::Or(vec![
            FailureMatcher::JobPattern("ml-*".to_string()),
            FailureMatcher::JobPattern("etl-*".to_string()),
        ]);

        let ml = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::for_job("ml-job"),
        );
        assert!(matcher.matches(&ml));

        let etl = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::for_job("etl-job"),
        );
        assert!(matcher.matches(&etl));

        let other = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::for_job("other-job"),
        );
        assert!(!matcher.matches(&other));
    }

    #[test]
    fn test_failure_matcher_not() {
        let matcher =
            FailureMatcher::Not(Box::new(FailureMatcher::JobPattern("test-*".to_string())));

        let test_job = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::for_job("test-job"),
        );
        assert!(!matcher.matches(&test_job));

        let prod_job = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::for_job("prod-job"),
        );
        assert!(matcher.matches(&prod_job));
    }

    #[test]
    fn test_policy_matches_failure() {
        let policy = RecoveryPolicy::new(
            "test",
            "Test Policy",
            FailureMatcher::Severity(vec![FailureSeverity::Critical]),
            RecoveryStrategy::abort(true),
        );

        let critical = Failure::new(FailureType::QuorumLost, FailureContext::new());
        assert!(policy.matches_failure(&critical));

        let low = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::new(),
        );
        assert!(!policy.matches_failure(&low));
    }

    #[test]
    fn test_notification_channel_builders() {
        let email = NotificationChannel::email(vec!["test@example.com".to_string()]);
        match email {
            NotificationChannel::Email { recipients } => {
                assert_eq!(recipients.len(), 1);
            }
            _ => panic!("Wrong channel type"),
        }

        let webhook = NotificationChannel::webhook("https://example.com/hook");
        match webhook {
            NotificationChannel::Webhook { url, secret } => {
                assert_eq!(url, "https://example.com/hook");
                assert!(secret.is_none());
            }
            _ => panic!("Wrong channel type"),
        }
    }

    #[test]
    fn test_default_policies() {
        let policies = defaults::all();
        assert!(!policies.is_empty());

        // Policies should be sorted by priority
        let priorities: Vec<i32> = policies.iter().map(|p| p.priority).collect();
        let mut sorted = priorities.clone();
        sorted.sort_by(|a, b| b.cmp(a)); // descending
        assert_eq!(priorities, sorted);
    }
}
