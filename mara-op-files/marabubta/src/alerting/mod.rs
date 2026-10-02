// Marabunta - Licensed under the MIT License.
//! Alerting System for Marabunta Compute
//!
//! This module provides a comprehensive alerting system for monitoring and responding
//! to various conditions in the distributed computing cluster.
//!
//! # Features
//!
//! - **Alert Types**: Critical, Warning, and Info severity levels
//! - **Alert Rules**: Threshold-based, rate-based, and absence-based conditions
//! - **Alert Manager**: Evaluates rules against metrics and fires alerts
//! - **Alert Sinks**: Multiple notification channels (webhook, email, PagerDuty)
//! - **Deduplication**: Prevents alert fatigue with intelligent grouping
//! - **Silence Rules**: Temporarily suppress alerts during maintenance
//! - **Inhibition Rules**: Suppress alerts based on other firing alerts
//!
//! # Example
//!
//! ```rust,ignore
//! use marabunta_compute::alerting::{
//!     Alert, AlertManager, AlertManagerConfig, AlertRule, AlertSeverity,
//!     RuleCondition, ThresholdCondition, WebhookSink,
//! };
//!
//! // Create an alert manager
//! let config = AlertManagerConfig::default();
//! let (manager, event_rx) = AlertManager::new(config);
//!
//! // Add a webhook sink
//! let sink = WebhookSink::new("https://hooks.example.com/alerts");
//! manager.add_sink(Box::new(sink));
//!
//! // Add an alert rule
//! let rule = AlertRule::new("high_cpu")
//!     .with_condition(RuleCondition::Threshold(ThresholdCondition {
//!         metric_name: "cpu_usage".to_string(),
//!         operator: ThresholdOperator::GreaterThan,
//!         value: 90.0,
//!         duration: Duration::from_secs(300),
//!     }))
//!     .with_severity(AlertSeverity::Critical);
//! manager.add_rule(rule);
//!
//! // Evaluate metrics
//! let metrics = vec![("cpu_usage", 95.0)];
//! manager.evaluate_metrics(&metrics).await;
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};
use uuid::Uuid;

// ============================================================================
// Alert Severity
// ============================================================================

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum AlertSeverity {
    /// Informational alerts, no action required
    Info,
    /// Warning alerts, should be investigated
    #[default]
    Warning,
    /// Critical alerts, requires immediate attention
    Critical,
}

impl AlertSeverity {
    /// Returns a numeric priority (higher = more severe)
    pub fn priority(&self) -> u32 {
        match self {
            AlertSeverity::Info => 0,
            AlertSeverity::Warning => 1,
            AlertSeverity::Critical => 2,
        }
    }

    /// Returns the severity as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertSeverity::Info => "info",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Critical => "critical",
        }
    }
}


impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "INFO"),
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

// ============================================================================
// Alert
// ============================================================================

/// Unique identifier for an alert instance
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

/// A single alert instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique identifier for this alert instance
    pub id: AlertId,
    /// Severity level of the alert
    pub severity: AlertSeverity,
    /// Source that generated this alert (e.g., rule name, component)
    pub source: String,
    /// Human-readable message describing the alert
    pub message: String,
    /// When the alert was first fired
    pub timestamp: DateTime<Utc>,
    /// When the alert was last updated
    pub updated_at: DateTime<Utc>,
    /// Labels for grouping and routing alerts
    pub labels: HashMap<String, String>,
    /// Additional annotations for context
    pub annotations: HashMap<String, String>,
    /// Fingerprint for deduplication
    pub fingerprint: String,
    /// Status of the alert
    pub status: AlertStatus,
    /// Number of times this alert has fired
    pub fire_count: u64,
    /// When the alert was resolved (if applicable)
    pub resolved_at: Option<DateTime<Utc>>,
    /// ID of the rule that generated this alert
    pub rule_id: Option<String>,
    /// Generator URL for more context
    pub generator_url: Option<String>,
}

impl Alert {
    /// Create a new alert
    pub fn new(severity: AlertSeverity, source: impl Into<String>, message: impl Into<String>) -> Self {
        let source = source.into();
        let message = message.into();
        let now = Utc::now();

        let mut alert = Self {
            id: AlertId::new(),
            severity,
            source: source.clone(),
            message: message.clone(),
            timestamp: now,
            updated_at: now,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            fingerprint: String::new(),
            status: AlertStatus::Firing,
            fire_count: 1,
            resolved_at: None,
            rule_id: None,
            generator_url: None,
        };

        // Calculate fingerprint
        alert.fingerprint = alert.calculate_fingerprint();
        alert
    }

    /// Add a label to the alert
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self.fingerprint = self.calculate_fingerprint();
        self
    }

    /// Add multiple labels
    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels.extend(labels);
        self.fingerprint = self.calculate_fingerprint();
        self
    }

    /// Add an annotation
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Set the rule ID
    pub fn with_rule_id(mut self, rule_id: impl Into<String>) -> Self {
        self.rule_id = Some(rule_id.into());
        self
    }

    /// Set the generator URL
    pub fn with_generator_url(mut self, url: impl Into<String>) -> Self {
        self.generator_url = Some(url.into());
        self
    }

    /// Calculate a fingerprint for deduplication
    fn calculate_fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.source.as_bytes());

        // Include sorted labels in fingerprint
        let mut label_keys: Vec<_> = self.labels.keys().collect();
        label_keys.sort();
        for key in label_keys {
            hasher.update(key.as_bytes());
            hasher.update(self.labels[key].as_bytes());
        }

        format!("{:x}", hasher.finalize())
    }

    /// Mark the alert as resolved
    pub fn resolve(&mut self) {
        self.status = AlertStatus::Resolved;
        self.resolved_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    /// Check if the alert is currently firing
    pub fn is_firing(&self) -> bool {
        self.status == AlertStatus::Firing
    }

    /// Check if the alert has been resolved
    pub fn is_resolved(&self) -> bool {
        self.status == AlertStatus::Resolved
    }

    /// Update the alert with new information
    pub fn update(&mut self) {
        self.fire_count += 1;
        self.updated_at = Utc::now();
    }
}

/// Status of an alert
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertStatus {
    /// Alert is currently firing
    Firing,
    /// Alert has been resolved
    Resolved,
    /// Alert is silenced
    Silenced,
    /// Alert is inhibited by another alert
    Inhibited,
}

// ============================================================================
// Alert Rules
// ============================================================================

/// Unique identifier for an alert rule
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuleId(pub String);

impl RuleId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for RuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An alert rule that defines conditions for firing alerts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Unique identifier for the rule
    pub id: RuleId,
    /// Human-readable name
    pub name: String,
    /// Description of what this rule monitors
    pub description: String,
    /// Condition that triggers the alert
    pub condition: RuleCondition,
    /// Severity of alerts generated by this rule
    pub severity: AlertSeverity,
    /// Labels to add to alerts
    pub labels: HashMap<String, String>,
    /// Annotations to add to alerts
    pub annotations: HashMap<String, String>,
    /// Whether the rule is enabled
    pub enabled: bool,
    /// How long the condition must be true before firing
    pub for_duration: std::time::Duration,
    /// Cooldown period between alerts
    pub cooldown: std::time::Duration,
    /// Last time this rule fired
    #[serde(skip)]
    pub last_fired: Option<DateTime<Utc>>,
    /// When the condition first became true
    #[serde(skip)]
    pub pending_since: Option<DateTime<Utc>>,
}

impl AlertRule {
    /// Create a new alert rule
    pub fn new(id: impl Into<String>) -> Self {
        let id_str = id.into();
        Self {
            id: RuleId::new(id_str.clone()),
            name: id_str,
            description: String::new(),
            condition: RuleCondition::Threshold(ThresholdCondition::default()),
            severity: AlertSeverity::Warning,
            labels: HashMap::new(),
            annotations: HashMap::new(),
            enabled: true,
            for_duration: std::time::Duration::from_secs(0),
            cooldown: std::time::Duration::from_secs(300),
            last_fired: None,
            pending_since: None,
        }
    }

    /// Set the rule name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the rule description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the condition
    pub fn with_condition(mut self, condition: RuleCondition) -> Self {
        self.condition = condition;
        self
    }

    /// Set the severity
    pub fn with_severity(mut self, severity: AlertSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Add a label
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Set the for duration
    pub fn with_for_duration(mut self, duration: std::time::Duration) -> Self {
        self.for_duration = duration;
        self
    }

    /// Set the cooldown
    pub fn with_cooldown(mut self, duration: std::time::Duration) -> Self {
        self.cooldown = duration;
        self
    }

    /// Enable or disable the rule
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Check if the rule is in cooldown
    pub fn is_in_cooldown(&self) -> bool {
        if let Some(last_fired) = self.last_fired {
            let elapsed = Utc::now().signed_duration_since(last_fired);
            elapsed < Duration::from_std(self.cooldown).unwrap_or(Duration::seconds(300))
        } else {
            false
        }
    }

    /// Check if the for duration has been met
    pub fn for_duration_met(&self) -> bool {
        if self.for_duration.is_zero() {
            return true;
        }
        if let Some(pending_since) = self.pending_since {
            let elapsed = Utc::now().signed_duration_since(pending_since);
            elapsed >= Duration::from_std(self.for_duration).unwrap_or(Duration::zero())
        } else {
            false
        }
    }
}

/// Conditions that can trigger an alert
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleCondition {
    /// Threshold-based condition (metric crosses a value)
    Threshold(ThresholdCondition),
    /// Rate-based condition (rate of change exceeds threshold)
    Rate(RateCondition),
    /// Absence-based condition (metric is missing)
    Absence(AbsenceCondition),
    /// Composite condition (combination of other conditions)
    Composite(CompositeCondition),
}

/// Threshold comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdOperator {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
}

impl ThresholdOperator {
    /// Evaluate the operator with two values
    pub fn evaluate(&self, actual: f64, threshold: f64) -> bool {
        match self {
            ThresholdOperator::GreaterThan => actual > threshold,
            ThresholdOperator::GreaterThanOrEqual => actual >= threshold,
            ThresholdOperator::LessThan => actual < threshold,
            ThresholdOperator::LessThanOrEqual => actual <= threshold,
            ThresholdOperator::Equal => (actual - threshold).abs() < f64::EPSILON,
            ThresholdOperator::NotEqual => (actual - threshold).abs() >= f64::EPSILON,
        }
    }
}

impl std::fmt::Display for ThresholdOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ThresholdOperator::GreaterThan => write!(f, ">"),
            ThresholdOperator::GreaterThanOrEqual => write!(f, ">="),
            ThresholdOperator::LessThan => write!(f, "<"),
            ThresholdOperator::LessThanOrEqual => write!(f, "<="),
            ThresholdOperator::Equal => write!(f, "=="),
            ThresholdOperator::NotEqual => write!(f, "!="),
        }
    }
}

/// Threshold-based condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdCondition {
    /// Name of the metric to check
    pub metric_name: String,
    /// Comparison operator
    pub operator: ThresholdOperator,
    /// Threshold value
    pub value: f64,
    /// Optional label matchers
    pub label_matchers: HashMap<String, String>,
}

impl Default for ThresholdCondition {
    fn default() -> Self {
        Self {
            metric_name: String::new(),
            operator: ThresholdOperator::GreaterThan,
            value: 0.0,
            label_matchers: HashMap::new(),
        }
    }
}

impl ThresholdCondition {
    /// Create a new threshold condition
    pub fn new(metric_name: impl Into<String>, operator: ThresholdOperator, value: f64) -> Self {
        Self {
            metric_name: metric_name.into(),
            operator,
            value,
            label_matchers: HashMap::new(),
        }
    }

    /// Add a label matcher
    pub fn with_label_matcher(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.label_matchers.insert(key.into(), value.into());
        self
    }
}

/// Rate-based condition (checks rate of change)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateCondition {
    /// Name of the metric to check
    pub metric_name: String,
    /// Comparison operator
    pub operator: ThresholdOperator,
    /// Rate threshold (change per second)
    pub rate_threshold: f64,
    /// Time window for rate calculation
    pub window: std::time::Duration,
    /// Optional label matchers
    pub label_matchers: HashMap<String, String>,
}

impl RateCondition {
    /// Create a new rate condition
    pub fn new(
        metric_name: impl Into<String>,
        operator: ThresholdOperator,
        rate_threshold: f64,
        window: std::time::Duration,
    ) -> Self {
        Self {
            metric_name: metric_name.into(),
            operator,
            rate_threshold,
            window,
            label_matchers: HashMap::new(),
        }
    }
}

/// Absence-based condition (fires when metric is missing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbsenceCondition {
    /// Name of the metric to check
    pub metric_name: String,
    /// How long the metric can be absent before alerting
    pub absent_for: std::time::Duration,
    /// Optional label matchers
    pub label_matchers: HashMap<String, String>,
}

impl AbsenceCondition {
    /// Create a new absence condition
    pub fn new(metric_name: impl Into<String>, absent_for: std::time::Duration) -> Self {
        Self {
            metric_name: metric_name.into(),
            absent_for,
            label_matchers: HashMap::new(),
        }
    }
}

/// Composite condition (combines multiple conditions)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeCondition {
    /// Logical operator to combine conditions
    pub operator: CompositeOperator,
    /// Child conditions
    pub conditions: Vec<RuleCondition>,
}

/// Operators for combining conditions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompositeOperator {
    /// All conditions must be true
    And,
    /// At least one condition must be true
    Or,
    /// Negate the condition
    Not,
}

// ============================================================================
// Alert Sinks
// ============================================================================

/// Error type for sink operations
#[derive(Debug, Clone)]
pub struct SinkError {
    pub message: String,
    pub retryable: bool,
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SinkError: {}", self.message)
    }
}

impl std::error::Error for SinkError {}

impl SinkError {
    pub fn new(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: message.into(),
            retryable,
        }
    }
}

/// Trait for alert notification sinks
#[async_trait]
pub trait AlertSink: Send + Sync {
    /// Get the name of this sink
    fn name(&self) -> &str;

    /// Send alerts to the sink
    async fn send(&self, alerts: &[Alert]) -> Result<(), SinkError>;

    /// Check if the sink is healthy
    async fn health_check(&self) -> Result<(), SinkError> {
        Ok(())
    }
}

/// Webhook sink for sending alerts via HTTP POST
#[derive(Debug, Clone)]
pub struct WebhookSink {
    /// Name of the sink
    pub name: String,
    /// Webhook URL
    pub url: String,
    /// HTTP headers to include
    pub headers: HashMap<String, String>,
    /// Timeout for requests
    pub timeout: std::time::Duration,
    /// Maximum retries
    pub max_retries: u32,
}

impl WebhookSink {
    /// Create a new webhook sink
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            name: "webhook".to_string(),
            url: url.into(),
            headers: HashMap::new(),
            timeout: std::time::Duration::from_secs(30),
            max_retries: 3,
        }
    }

    /// Set the sink name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Add a header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the timeout
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl AlertSink for WebhookSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, alerts: &[Alert]) -> Result<(), SinkError> {
        // Serialize alerts to JSON
        let payload = serde_json::to_string(&WebhookPayload {
            alerts: alerts.to_vec(),
            version: "1.0".to_string(),
            group_key: alerts.first().map(|a| a.fingerprint.clone()).unwrap_or_default(),
            status: if alerts.iter().any(|a| a.is_firing()) {
                "firing".to_string()
            } else {
                "resolved".to_string()
            },
        })
        .map_err(|e| SinkError::new(format!("Failed to serialize alerts: {}", e), false))?;

        // Build request
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|e| SinkError::new(format!("Failed to create HTTP client: {}", e), false))?;

        let mut request = client.post(&self.url).body(payload);

        // Add headers
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }
        request = request.header("Content-Type", "application/json");

        // Send with retries
        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            match request.try_clone().unwrap().send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        debug!("Webhook sent successfully to {}", self.url);
                        return Ok(());
                    } else {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_default();
                        last_error = Some(SinkError::new(
                            format!("Webhook returned {}: {}", status, body),
                            status.is_server_error(),
                        ));
                    }
                }
                Err(e) => {
                    last_error = Some(SinkError::new(
                        format!("Webhook request failed: {}", e),
                        true,
                    ));
                }
            }

            if attempt < self.max_retries {
                tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt))).await;
            }
        }

        Err(last_error.unwrap_or_else(|| SinkError::new("Unknown error", false)))
    }
}

/// Webhook payload structure
#[derive(Debug, Serialize, Deserialize)]
struct WebhookPayload {
    alerts: Vec<Alert>,
    version: String,
    group_key: String,
    status: String,
}

/// Email sink for sending alerts via SMTP
#[derive(Debug, Clone)]
pub struct EmailSink {
    /// Name of the sink
    pub name: String,
    /// SMTP server host
    pub smtp_host: String,
    /// SMTP server port
    pub smtp_port: u16,
    /// Username for authentication
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// From address
    pub from: String,
    /// To addresses
    pub to: Vec<String>,
    /// Use TLS
    pub use_tls: bool,
}

impl EmailSink {
    /// Create a new email sink
    pub fn new(smtp_host: impl Into<String>, from: impl Into<String>, to: Vec<String>) -> Self {
        Self {
            name: "email".to_string(),
            smtp_host: smtp_host.into(),
            smtp_port: 587,
            username: None,
            password: None,
            from: from.into(),
            to,
            use_tls: true,
        }
    }

    /// Set SMTP port
    pub fn with_port(mut self, port: u16) -> Self {
        self.smtp_port = port;
        self
    }

    /// Set authentication credentials
    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// Format alerts as an email body
    fn format_email_body(&self, alerts: &[Alert]) -> String {
        let mut body = String::new();
        body.push_str("Marabunta Compute Alert Notification\n");
        body.push_str("================================\n\n");

        for alert in alerts {
            body.push_str(&format!(
                "[{}] {}\n",
                alert.severity,
                alert.source
            ));
            body.push_str(&format!("Message: {}\n", alert.message));
            body.push_str(&format!("Status: {:?}\n", alert.status));
            body.push_str(&format!("Time: {}\n", alert.timestamp.format("%Y-%m-%d %H:%M:%S UTC")));

            if !alert.labels.is_empty() {
                body.push_str("Labels:\n");
                for (key, value) in &alert.labels {
                    body.push_str(&format!("  {}: {}\n", key, value));
                }
            }
            body.push_str("\n---\n\n");
        }

        body
    }
}

#[async_trait]
impl AlertSink for EmailSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, alerts: &[Alert]) -> Result<(), SinkError> {
        // Note: In a real implementation, you would use lettre or similar
        // For now, we'll simulate the email sending
        let subject = format!(
            "[{}] {} alert(s) from Marabunta Compute",
            if alerts.iter().any(|a| a.severity == AlertSeverity::Critical) {
                "CRITICAL"
            } else if alerts.iter().any(|a| a.severity == AlertSeverity::Warning) {
                "WARNING"
            } else {
                "INFO"
            },
            alerts.len()
        );

        let body = self.format_email_body(alerts);

        info!(
            "Email sink: Would send email to {:?} with subject: {} (body length: {})",
            self.to,
            subject,
            body.len()
        );

        // Simulate network delay
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Ok(())
    }
}

/// PagerDuty-compatible sink for incident management
#[derive(Debug, Clone)]
pub struct PagerDutySink {
    /// Name of the sink
    pub name: String,
    /// PagerDuty Events API URL
    pub api_url: String,
    /// Integration/routing key
    pub routing_key: String,
    /// Custom details to include
    pub custom_details: HashMap<String, String>,
}

impl PagerDutySink {
    /// Create a new PagerDuty sink
    pub fn new(routing_key: impl Into<String>) -> Self {
        Self {
            name: "pagerduty".to_string(),
            api_url: "https://events.pagerduty.com/v2/enqueue".to_string(),
            routing_key: routing_key.into(),
            custom_details: HashMap::new(),
        }
    }

    /// Set a custom API URL
    pub fn with_api_url(mut self, url: impl Into<String>) -> Self {
        self.api_url = url.into();
        self
    }

    /// Add custom details
    pub fn with_custom_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom_details.insert(key.into(), value.into());
        self
    }

    /// Convert alert severity to PagerDuty severity
    fn severity_to_pagerduty(&self, severity: AlertSeverity) -> &'static str {
        match severity {
            AlertSeverity::Critical => "critical",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Info => "info",
        }
    }
}

/// PagerDuty Events API v2 payload
#[derive(Debug, Serialize)]
struct PagerDutyPayload {
    routing_key: String,
    event_action: String,
    dedup_key: String,
    payload: PagerDutyEventPayload,
}

#[derive(Debug, Serialize)]
struct PagerDutyEventPayload {
    summary: String,
    source: String,
    severity: String,
    timestamp: String,
    custom_details: HashMap<String, String>,
}

#[async_trait]
impl AlertSink for PagerDutySink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, alerts: &[Alert]) -> Result<(), SinkError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| SinkError::new(format!("Failed to create HTTP client: {}", e), false))?;

        for alert in alerts {
            let event_action = if alert.is_resolved() {
                "resolve"
            } else {
                "trigger"
            };

            let mut custom_details = self.custom_details.clone();
            custom_details.extend(alert.labels.clone());
            custom_details.insert("alert_id".to_string(), alert.id.to_string());
            custom_details.insert("fire_count".to_string(), alert.fire_count.to_string());

            let payload = PagerDutyPayload {
                routing_key: self.routing_key.clone(),
                event_action: event_action.to_string(),
                dedup_key: alert.fingerprint.clone(),
                payload: PagerDutyEventPayload {
                    summary: format!("[{}] {}: {}", alert.severity, alert.source, alert.message),
                    source: alert.source.clone(),
                    severity: self.severity_to_pagerduty(alert.severity).to_string(),
                    timestamp: alert.timestamp.to_rfc3339(),
                    custom_details,
                },
            };

            let response = client
                .post(&self.api_url)
                .json(&payload)
                .send()
                .await
                .map_err(|e| SinkError::new(format!("PagerDuty request failed: {}", e), true))?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(SinkError::new(
                    format!("PagerDuty returned {}: {}", status, body),
                    status.is_server_error(),
                ));
            }
        }

        debug!("PagerDuty events sent successfully");
        Ok(())
    }
}

// ============================================================================
// Silence Rules
// ============================================================================

/// A rule for silencing alerts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SilenceRule {
    /// Unique identifier
    pub id: String,
    /// Label matchers for alerts to silence
    pub matchers: Vec<LabelMatcher>,
    /// When the silence starts
    pub starts_at: DateTime<Utc>,
    /// When the silence ends
    pub ends_at: DateTime<Utc>,
    /// Who created this silence
    pub created_by: String,
    /// Comment explaining the silence
    pub comment: String,
    /// Whether the silence is active
    pub active: bool,
}

impl SilenceRule {
    /// Create a new silence rule
    pub fn new(
        matchers: Vec<LabelMatcher>,
        starts_at: DateTime<Utc>,
        ends_at: DateTime<Utc>,
        created_by: impl Into<String>,
        comment: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            matchers,
            starts_at,
            ends_at,
            created_by: created_by.into(),
            comment: comment.into(),
            active: true,
        }
    }

    /// Check if the silence is currently active
    pub fn is_active(&self) -> bool {
        if !self.active {
            return false;
        }
        let now = Utc::now();
        now >= self.starts_at && now <= self.ends_at
    }

    /// Check if an alert matches this silence
    pub fn matches(&self, alert: &Alert) -> bool {
        if !self.is_active() {
            return false;
        }
        self.matchers.iter().all(|m| m.matches(&alert.labels))
    }
}

/// A label matcher for filtering alerts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelMatcher {
    /// Label name to match
    pub name: String,
    /// Value to match
    pub value: String,
    /// Type of match
    pub match_type: MatchType,
}

impl LabelMatcher {
    /// Create an exact match
    pub fn exact(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            match_type: MatchType::Equal,
        }
    }

    /// Create a regex match
    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: pattern.into(),
            match_type: MatchType::Regex,
        }
    }

    /// Check if labels match
    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        match labels.get(&self.name) {
            Some(value) => match self.match_type {
                MatchType::Equal => value == &self.value,
                MatchType::NotEqual => value != &self.value,
                MatchType::Regex => {
                    regex::Regex::new(&self.value)
                        .map(|r| r.is_match(value))
                        .unwrap_or(false)
                }
                MatchType::NotRegex => {
                    regex::Regex::new(&self.value)
                        .map(|r| !r.is_match(value))
                        .unwrap_or(true)
                }
            },
            None => matches!(self.match_type, MatchType::NotEqual | MatchType::NotRegex),
        }
    }
}

/// Types of label matching
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchType {
    /// Exact match
    Equal,
    /// Not equal
    NotEqual,
    /// Regular expression match
    Regex,
    /// Not matching regular expression
    NotRegex,
}

// ============================================================================
// Inhibition Rules
// ============================================================================

/// A rule for inhibiting alerts based on other firing alerts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InhibitRule {
    /// Unique identifier
    pub id: String,
    /// Matchers for source alerts (the alert that inhibits)
    pub source_matchers: Vec<LabelMatcher>,
    /// Matchers for target alerts (the alert being inhibited)
    pub target_matchers: Vec<LabelMatcher>,
    /// Labels that must match between source and target
    pub equal_labels: Vec<String>,
}

impl InhibitRule {
    /// Create a new inhibit rule
    pub fn new(
        source_matchers: Vec<LabelMatcher>,
        target_matchers: Vec<LabelMatcher>,
        equal_labels: Vec<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            source_matchers,
            target_matchers,
            equal_labels,
        }
    }

    /// Check if a source alert matches this rule
    pub fn matches_source(&self, alert: &Alert) -> bool {
        self.source_matchers.iter().all(|m| m.matches(&alert.labels))
    }

    /// Check if a target alert matches this rule
    pub fn matches_target(&self, alert: &Alert) -> bool {
        self.target_matchers.iter().all(|m| m.matches(&alert.labels))
    }

    /// Check if equal labels match between source and target
    pub fn labels_match(&self, source: &Alert, target: &Alert) -> bool {
        self.equal_labels.iter().all(|label| {
            source.labels.get(label) == target.labels.get(label)
        })
    }

    /// Check if a target alert should be inhibited by a source alert
    pub fn should_inhibit(&self, source: &Alert, target: &Alert) -> bool {
        source.is_firing()
            && self.matches_source(source)
            && self.matches_target(target)
            && self.labels_match(source, target)
    }
}

// ============================================================================
// Alert Grouping
// ============================================================================

/// Configuration for alert grouping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupingConfig {
    /// Labels to group alerts by
    pub group_by: Vec<String>,
    /// How long to wait before sending initial notification
    pub group_wait: std::time::Duration,
    /// How long to wait before sending updates
    pub group_interval: std::time::Duration,
    /// How long to wait before resending a notification
    pub repeat_interval: std::time::Duration,
}

impl Default for GroupingConfig {
    fn default() -> Self {
        Self {
            group_by: vec!["alertname".to_string(), "severity".to_string()],
            group_wait: std::time::Duration::from_secs(30),
            group_interval: std::time::Duration::from_secs(300),
            repeat_interval: std::time::Duration::from_secs(3600),
        }
    }
}

/// A group of alerts
#[derive(Debug, Clone)]
pub struct AlertGroup {
    /// Group key (hash of group labels)
    pub key: String,
    /// Group labels
    pub labels: HashMap<String, String>,
    /// Alerts in this group
    pub alerts: Vec<Alert>,
    /// When the group was created
    pub created_at: DateTime<Utc>,
    /// When the group was last updated
    pub updated_at: DateTime<Utc>,
    /// When the last notification was sent
    pub last_notified: Option<DateTime<Utc>>,
}

impl AlertGroup {
    /// Create a new alert group
    pub fn new(labels: HashMap<String, String>) -> Self {
        let key = Self::calculate_key(&labels);
        Self {
            key,
            labels,
            alerts: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_notified: None,
        }
    }

    /// Calculate the group key from labels
    fn calculate_key(labels: &HashMap<String, String>) -> String {
        let mut hasher = Sha256::new();
        let mut keys: Vec<_> = labels.keys().collect();
        keys.sort();
        for key in keys {
            hasher.update(key.as_bytes());
            hasher.update(labels[key].as_bytes());
        }
        format!("{:x}", hasher.finalize())[..16].to_string()
    }

    /// Add an alert to the group
    pub fn add_alert(&mut self, alert: Alert) {
        // Check if alert already exists (by fingerprint)
        if let Some(existing) = self.alerts.iter_mut().find(|a| a.fingerprint == alert.fingerprint) {
            existing.update();
        } else {
            self.alerts.push(alert);
        }
        self.updated_at = Utc::now();
    }

    /// Remove resolved alerts older than the retention period
    pub fn prune_resolved(&mut self, retention: std::time::Duration) {
        let cutoff = Utc::now() - Duration::from_std(retention).unwrap_or(Duration::hours(1));
        self.alerts.retain(|a| a.is_firing() || a.resolved_at.map(|t| t > cutoff).unwrap_or(true));
    }

    /// Check if the group has any firing alerts
    pub fn has_firing_alerts(&self) -> bool {
        self.alerts.iter().any(|a| a.is_firing())
    }

    /// Get the highest severity in the group
    pub fn max_severity(&self) -> AlertSeverity {
        self.alerts
            .iter()
            .map(|a| a.severity)
            .max()
            .unwrap_or(AlertSeverity::Info)
    }
}

// ============================================================================
// Alert Manager
// ============================================================================

/// Configuration for the alert manager
#[derive(Debug, Clone)]
pub struct AlertManagerConfig {
    /// Grouping configuration
    pub grouping: GroupingConfig,
    /// Maximum number of alerts to keep in history
    pub max_history: usize,
    /// How often to run maintenance tasks
    pub maintenance_interval: std::time::Duration,
    /// How long to keep resolved alerts
    pub resolved_retention: std::time::Duration,
    /// How often to check for expired silences
    pub silence_check_interval: std::time::Duration,
}

impl Default for AlertManagerConfig {
    fn default() -> Self {
        Self {
            grouping: GroupingConfig::default(),
            max_history: 10000,
            maintenance_interval: std::time::Duration::from_secs(60),
            resolved_retention: std::time::Duration::from_secs(3600),
            silence_check_interval: std::time::Duration::from_secs(60),
        }
    }
}

/// Events emitted by the alert manager
#[derive(Debug, Clone)]
pub enum AlertManagerEvent {
    /// New alert was created
    AlertCreated(Alert),
    /// Alert was updated
    AlertUpdated(Alert),
    /// Alert was resolved
    AlertResolved(Alert),
    /// Alert was silenced
    AlertSilenced { alert_id: AlertId, silence_id: String },
    /// Alert was inhibited
    AlertInhibited { alert_id: AlertId, source_alert_id: AlertId },
    /// Notifications sent to sinks
    NotificationsSent { group_key: String, sink_count: usize },
}

/// Metric value for evaluation
#[derive(Debug, Clone)]
pub struct MetricValue {
    /// Metric name
    pub name: String,
    /// Current value
    pub value: f64,
    /// Labels associated with this metric
    pub labels: HashMap<String, String>,
    /// When this value was recorded
    pub timestamp: DateTime<Utc>,
}

impl MetricValue {
    /// Create a new metric value
    pub fn new(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            value,
            labels: HashMap::new(),
            timestamp: Utc::now(),
        }
    }

    /// Add a label
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }
}

/// The main alert manager
pub struct AlertManager {
    /// Configuration
    config: AlertManagerConfig,
    /// Active alerts by fingerprint
    alerts: DashMap<String, Alert>,
    /// Alert groups by key
    groups: Arc<RwLock<HashMap<String, AlertGroup>>>,
    /// Alert rules
    rules: DashMap<String, AlertRule>,
    /// Silence rules
    silences: DashMap<String, SilenceRule>,
    /// Inhibit rules
    inhibitions: Arc<RwLock<Vec<InhibitRule>>>,
    /// Alert sinks
    sinks: Arc<RwLock<Vec<Box<dyn AlertSink>>>>,
    /// Metric history for rate calculations
    metric_history: DashMap<String, VecDeque<MetricValue>>,
    /// Last metric update times for absence detection
    last_metric_update: DashMap<String, DateTime<Utc>>,
    /// Event broadcaster
    event_tx: broadcast::Sender<AlertManagerEvent>,
    /// Alert history
    history: Arc<RwLock<VecDeque<Alert>>>,
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new(config: AlertManagerConfig) -> (Arc<Self>, broadcast::Receiver<AlertManagerEvent>) {
        let (event_tx, event_rx) = broadcast::channel(1000);

        let manager = Arc::new(Self {
            config,
            alerts: DashMap::new(),
            groups: Arc::new(RwLock::new(HashMap::new())),
            rules: DashMap::new(),
            silences: DashMap::new(),
            inhibitions: Arc::new(RwLock::new(Vec::new())),
            sinks: Arc::new(RwLock::new(Vec::new())),
            metric_history: DashMap::new(),
            last_metric_update: DashMap::new(),
            event_tx,
            history: Arc::new(RwLock::new(VecDeque::new())),
        });

        (manager, event_rx)
    }

    /// Add an alert sink
    pub fn add_sink(&self, sink: Box<dyn AlertSink>) {
        self.sinks.write().push(sink);
    }

    /// Add an alert rule
    pub fn add_rule(&self, rule: AlertRule) {
        info!("Adding alert rule: {}", rule.id);
        self.rules.insert(rule.id.0.clone(), rule);
    }

    /// Remove an alert rule
    pub fn remove_rule(&self, rule_id: &str) {
        self.rules.remove(rule_id);
    }

    /// Get all rules
    pub fn get_rules(&self) -> Vec<AlertRule> {
        self.rules.iter().map(|r| r.value().clone()).collect()
    }

    /// Add a silence rule
    pub fn add_silence(&self, silence: SilenceRule) -> String {
        let id = silence.id.clone();
        info!("Adding silence rule: {}", id);
        self.silences.insert(id.clone(), silence);
        id
    }

    /// Remove a silence rule
    pub fn remove_silence(&self, silence_id: &str) -> bool {
        self.silences.remove(silence_id).is_some()
    }

    /// Get all active silences
    pub fn get_silences(&self) -> Vec<SilenceRule> {
        self.silences
            .iter()
            .filter(|s| s.is_active())
            .map(|s| s.value().clone())
            .collect()
    }

    /// Add an inhibit rule
    pub fn add_inhibit_rule(&self, rule: InhibitRule) {
        self.inhibitions.write().push(rule);
    }

    /// Get all active alerts
    pub fn get_active_alerts(&self) -> Vec<Alert> {
        self.alerts
            .iter()
            .filter(|a| a.is_firing())
            .map(|a| a.value().clone())
            .collect()
    }

    /// Get alert by fingerprint
    pub fn get_alert(&self, fingerprint: &str) -> Option<Alert> {
        self.alerts.get(fingerprint).map(|a| a.value().clone())
    }

    /// Get alert history
    pub fn get_history(&self, limit: usize) -> Vec<Alert> {
        self.history.read().iter().take(limit).cloned().collect()
    }

    /// Get alert groups
    pub fn get_groups(&self) -> Vec<AlertGroup> {
        self.groups.read().values().cloned().collect()
    }

    /// Fire an alert
    pub async fn fire_alert(&self, alert: Alert) {
        let fingerprint = alert.fingerprint.clone();

        // Check if silenced
        for silence in self.silences.iter() {
            if silence.matches(&alert) {
                info!("Alert {} silenced by {}", alert.id, silence.id);
                let mut silenced_alert = alert.clone();
                silenced_alert.status = AlertStatus::Silenced;
                self.alerts.insert(fingerprint.clone(), silenced_alert.clone());
                let _ = self.event_tx.send(AlertManagerEvent::AlertSilenced {
                    alert_id: alert.id,
                    silence_id: silence.id.clone(),
                });
                return;
            }
        }

        // Check if inhibited
        for existing in self.alerts.iter() {
            if existing.is_firing() {
                for rule in self.inhibitions.read().iter() {
                    if rule.should_inhibit(existing.value(), &alert) {
                        info!("Alert {} inhibited by {}", alert.id, existing.id);
                        let mut inhibited_alert = alert.clone();
                        inhibited_alert.status = AlertStatus::Inhibited;
                        self.alerts.insert(fingerprint.clone(), inhibited_alert.clone());
                        let _ = self.event_tx.send(AlertManagerEvent::AlertInhibited {
                            alert_id: alert.id,
                            source_alert_id: existing.id,
                        });
                        return;
                    }
                }
            }
        }

        // Check if alert already exists
        if let Some(mut existing) = self.alerts.get_mut(&fingerprint) {
            existing.update();
            let _ = self.event_tx.send(AlertManagerEvent::AlertUpdated(existing.clone()));
        } else {
            info!("New alert: {} - {}", alert.id, alert.message);
            self.alerts.insert(fingerprint.clone(), alert.clone());
            let _ = self.event_tx.send(AlertManagerEvent::AlertCreated(alert.clone()));
        }

        // Add to group
        self.add_to_group(&alert);

        // Send notifications
        self.send_notifications().await;
    }

    /// Resolve an alert
    pub fn resolve_alert(&self, fingerprint: &str) {
        if let Some(mut alert) = self.alerts.get_mut(fingerprint) {
            alert.resolve();
            info!("Alert resolved: {}", alert.id);
            let _ = self.event_tx.send(AlertManagerEvent::AlertResolved(alert.clone()));

            // Add to history
            let mut history = self.history.write();
            history.push_front(alert.clone());
            while history.len() > self.config.max_history {
                history.pop_back();
            }
        }
    }

    /// Add an alert to its group
    fn add_to_group(&self, alert: &Alert) {
        let mut group_labels = HashMap::new();
        for key in &self.config.grouping.group_by {
            if let Some(value) = alert.labels.get(key) {
                group_labels.insert(key.clone(), value.clone());
            }
        }

        let key = AlertGroup::calculate_key(&group_labels);
        let mut groups = self.groups.write();

        let group = groups.entry(key.clone()).or_insert_with(|| {
            AlertGroup::new(group_labels)
        });
        group.add_alert(alert.clone());
    }

    /// Evaluate metrics against rules
    pub async fn evaluate_metrics(&self, metrics: &[MetricValue]) {
        // Update metric history
        for metric in metrics {
            self.last_metric_update.insert(metric.name.clone(), Utc::now());

            let mut history = self.metric_history
                .entry(metric.name.clone())
                .or_default();
            history.push_back(metric.clone());

            // Keep last 100 values
            while history.len() > 100 {
                history.pop_front();
            }
        }

        // Evaluate rules
        for mut rule_entry in self.rules.iter_mut() {
            let rule = rule_entry.value_mut();
            if !rule.enabled {
                continue;
            }

            let triggered = self.evaluate_condition(&rule.condition, metrics);

            if triggered {
                // Mark as pending
                if rule.pending_since.is_none() {
                    rule.pending_since = Some(Utc::now());
                }

                // Check if for duration is met
                if rule.for_duration_met() && !rule.is_in_cooldown() {
                    let alert = self.create_alert_from_rule(rule, metrics);
                    rule.last_fired = Some(Utc::now());
                    rule.pending_since = None;

                    // Fire the alert
                    self.fire_alert(alert).await;
                }
            } else {
                rule.pending_since = None;

                // Resolve any existing alerts for this rule
                let rule_id = rule.id.0.clone();
                for alert in self.alerts.iter() {
                    if alert.rule_id.as_deref() == Some(&rule_id) && alert.is_firing() {
                        self.resolve_alert(&alert.fingerprint);
                    }
                }
            }
        }

        // Check for absent metrics
        self.check_absent_metrics().await;
    }

    /// Evaluate a single condition
    fn evaluate_condition(&self, condition: &RuleCondition, metrics: &[MetricValue]) -> bool {
        match condition {
            RuleCondition::Threshold(threshold) => {
                for metric in metrics {
                    if metric.name == threshold.metric_name {
                        // Check label matchers
                        let labels_match = threshold.label_matchers.iter().all(|(k, v)| {
                            metric.labels.get(k).map(|mv| mv == v).unwrap_or(false)
                        });

                        if labels_match && threshold.operator.evaluate(metric.value, threshold.value) {
                            return true;
                        }
                    }
                }
                false
            }
            RuleCondition::Rate(rate) => {
                if let Some(history) = self.metric_history.get(&rate.metric_name) {
                    let window = Duration::from_std(rate.window).unwrap_or(Duration::minutes(5));
                    let cutoff = Utc::now() - window;

                    let values: Vec<_> = history
                        .iter()
                        .filter(|v| v.timestamp >= cutoff)
                        .collect();

                    if values.len() >= 2 {
                        let first = values.first().unwrap();
                        let last = values.last().unwrap();
                        let time_diff = (last.timestamp - first.timestamp).num_seconds() as f64;

                        if time_diff > 0.0 {
                            let rate_value = (last.value - first.value) / time_diff;
                            return rate.operator.evaluate(rate_value, rate.rate_threshold);
                        }
                    }
                }
                false
            }
            RuleCondition::Absence(absence) => {
                if let Some(last_update) = self.last_metric_update.get(&absence.metric_name) {
                    let elapsed = Utc::now() - *last_update;
                    let absent_for = Duration::from_std(absence.absent_for).unwrap_or(Duration::minutes(5));
                    elapsed > absent_for
                } else {
                    true // Never seen this metric
                }
            }
            RuleCondition::Composite(composite) => {
                match composite.operator {
                    CompositeOperator::And => {
                        composite.conditions.iter().all(|c| self.evaluate_condition(c, metrics))
                    }
                    CompositeOperator::Or => {
                        composite.conditions.iter().any(|c| self.evaluate_condition(c, metrics))
                    }
                    CompositeOperator::Not => {
                        composite.conditions.first()
                            .map(|c| !self.evaluate_condition(c, metrics))
                            .unwrap_or(true)
                    }
                }
            }
        }
    }

    /// Create an alert from a rule
    fn create_alert_from_rule(&self, rule: &AlertRule, metrics: &[MetricValue]) -> Alert {
        let message = self.format_alert_message(rule, metrics);

        let mut alert = Alert::new(rule.severity, rule.name.clone(), message)
            .with_rule_id(rule.id.0.clone())
            .with_labels(rule.labels.clone());

        // Add alertname label
        alert.labels.insert("alertname".to_string(), rule.name.clone());

        // Add severity label
        alert.labels.insert("severity".to_string(), rule.severity.as_str().to_string());

        // Add annotations
        alert.annotations = rule.annotations.clone();

        alert
    }

    /// Format an alert message
    fn format_alert_message(&self, rule: &AlertRule, metrics: &[MetricValue]) -> String {
        let mut message = rule.description.clone();

        if message.is_empty() {
            message = format!("Alert rule '{}' triggered", rule.name);
        }

        // Add metric values to message
        match &rule.condition {
            RuleCondition::Threshold(threshold) => {
                if let Some(metric) = metrics.iter().find(|m| m.name == threshold.metric_name) {
                    message = format!(
                        "{} ({} {} {} {})",
                        message,
                        threshold.metric_name,
                        metric.value,
                        threshold.operator,
                        threshold.value
                    );
                }
            }
            RuleCondition::Absence(absence) => {
                message = format!("{} (metric '{}' is absent)", message, absence.metric_name);
            }
            _ => {}
        }

        message
    }

    /// Check for absent metrics and fire alerts
    async fn check_absent_metrics(&self) {
        for rule_entry in self.rules.iter() {
            let rule = rule_entry.value();
            if !rule.enabled {
                continue;
            }

            if let RuleCondition::Absence(absence) = &rule.condition {
                if let Some(last_update) = self.last_metric_update.get(&absence.metric_name) {
                    let elapsed = Utc::now() - *last_update;
                    let absent_for = Duration::from_std(absence.absent_for).unwrap_or(Duration::minutes(5));

                    if elapsed > absent_for && !rule.is_in_cooldown() {
                        let alert = Alert::new(
                            rule.severity,
                            rule.name.clone(),
                            format!("Metric '{}' has been absent for {:?}", absence.metric_name, elapsed),
                        )
                        .with_rule_id(rule.id.0.clone())
                        .with_labels(rule.labels.clone());

                        self.fire_alert(alert).await;
                    }
                }
            }
        }
    }

    /// Send notifications to all sinks
    async fn send_notifications(&self) {
        let groups = self.groups.read().clone();
        let sinks = self.sinks.read();

        for (key, group) in groups.iter() {
            // Check if we should send notifications
            let should_send = if group.last_notified.is_none() {
                // Never notified, check group_wait
                let elapsed = Utc::now() - group.created_at;
                elapsed >= Duration::from_std(self.config.grouping.group_wait).unwrap_or(Duration::seconds(30))
            } else {
                // Check repeat_interval
                let elapsed = Utc::now() - group.last_notified.unwrap();
                elapsed >= Duration::from_std(self.config.grouping.repeat_interval).unwrap_or(Duration::hours(1))
            };

            if should_send && group.has_firing_alerts() {
                let alerts: Vec<_> = group.alerts.iter().filter(|a| a.is_firing()).cloned().collect();

                if !alerts.is_empty() {
                    let mut success_count = 0;
                    for sink in sinks.iter() {
                        match sink.send(&alerts).await {
                            Ok(()) => {
                                success_count += 1;
                                debug!("Notifications sent to sink: {}", sink.name());
                            }
                            Err(e) => {
                                error!("Failed to send to sink {}: {}", sink.name(), e);
                            }
                        }
                    }

                    if success_count > 0 {
                        let _ = self.event_tx.send(AlertManagerEvent::NotificationsSent {
                            group_key: key.clone(),
                            sink_count: success_count,
                        });
                    }
                }
            }
        }

        // Update last_notified
        drop(sinks);
        let mut groups = self.groups.write();
        for group in groups.values_mut() {
            if group.has_firing_alerts() {
                group.last_notified = Some(Utc::now());
            }
        }
    }

    /// Run maintenance tasks
    pub async fn maintenance(&self) {
        // Prune resolved alerts from groups
        let mut groups = self.groups.write();
        for group in groups.values_mut() {
            group.prune_resolved(self.config.resolved_retention);
        }

        // Remove empty groups
        groups.retain(|_, g| !g.alerts.is_empty());

        // Remove expired silences
        self.silences.retain(|_, s| s.ends_at > Utc::now());

        // Clean up resolved alerts
        let cutoff = Utc::now() - Duration::from_std(self.config.resolved_retention).unwrap_or(Duration::hours(1));
        self.alerts.retain(|_, a| a.is_firing() || a.resolved_at.map(|t| t > cutoff).unwrap_or(true));
    }

    /// Start the alert manager background tasks
    pub async fn run(self: Arc<Self>, mut shutdown: mpsc::Receiver<()>) {
        let mut maintenance_interval = tokio::time::interval(self.config.maintenance_interval);

        info!("Alert manager started");

        loop {
            tokio::select! {
                _ = maintenance_interval.tick() => {
                    self.maintenance().await;
                }
                _ = shutdown.recv() => {
                    info!("Alert manager shutting down");
                    break;
                }
            }
        }
    }
}

// ============================================================================
// Integration with Metrics Module
// ============================================================================

/// Collector that integrates with the existing metrics module
pub struct MetricsAlertCollector {
    /// Reference to the alert manager
    manager: Arc<AlertManager>,
    /// Metric name mappings
    metric_mappings: HashMap<String, String>,
}

impl MetricsAlertCollector {
    /// Create a new metrics alert collector
    pub fn new(manager: Arc<AlertManager>) -> Self {
        let mut metric_mappings = HashMap::new();
        // Map Prometheus metric names to internal names
        metric_mappings.insert("marabunta_node_cpu_usage".to_string(), "cpu_usage".to_string());
        metric_mappings.insert("marabunta_node_memory_bytes".to_string(), "memory_bytes".to_string());
        metric_mappings.insert("marabunta_queue_depth".to_string(), "queue_depth".to_string());
        metric_mappings.insert("marabunta_tasks_total".to_string(), "tasks_total".to_string());
        metric_mappings.insert("marabunta_node_last_heartbeat_seconds".to_string(), "heartbeat_age".to_string());

        Self {
            manager,
            metric_mappings,
        }
    }

    /// Collect metrics and evaluate alerts
    pub async fn collect_and_evaluate(&self, metrics: Vec<(String, f64, HashMap<String, String>)>) {
        let metric_values: Vec<MetricValue> = metrics
            .into_iter()
            .map(|(name, value, labels)| {
                let mapped_name = self.metric_mappings.get(&name).cloned().unwrap_or(name);
                MetricValue {
                    name: mapped_name,
                    value,
                    labels,
                    timestamp: Utc::now(),
                }
            })
            .collect();

        self.manager.evaluate_metrics(&metric_values).await;
    }
}

/// Create standard alert rules for Marabunta Compute
pub fn create_standard_rules() -> Vec<AlertRule> {
    vec![
        // High CPU usage
        AlertRule::new("high_cpu_usage")
            .with_name("High CPU Usage")
            .with_description("Node CPU usage is critically high")
            .with_condition(RuleCondition::Threshold(ThresholdCondition::new(
                "cpu_usage",
                ThresholdOperator::GreaterThan,
                90.0,
            )))
            .with_severity(AlertSeverity::Critical)
            .with_for_duration(std::time::Duration::from_secs(300))
            .with_label("category", "resource"),

        // Warning CPU usage
        AlertRule::new("warning_cpu_usage")
            .with_name("Warning CPU Usage")
            .with_description("Node CPU usage is elevated")
            .with_condition(RuleCondition::Threshold(ThresholdCondition::new(
                "cpu_usage",
                ThresholdOperator::GreaterThan,
                80.0,
            )))
            .with_severity(AlertSeverity::Warning)
            .with_for_duration(std::time::Duration::from_secs(600))
            .with_label("category", "resource"),

        // High memory usage
        AlertRule::new("high_memory_usage")
            .with_name("High Memory Usage")
            .with_description("Node memory usage is critically high")
            .with_condition(RuleCondition::Threshold(ThresholdCondition::new(
                "memory_percent",
                ThresholdOperator::GreaterThan,
                95.0,
            )))
            .with_severity(AlertSeverity::Critical)
            .with_for_duration(std::time::Duration::from_secs(120))
            .with_label("category", "resource"),

        // Queue depth high
        AlertRule::new("high_queue_depth")
            .with_name("High Queue Depth")
            .with_description("Task queue is backing up")
            .with_condition(RuleCondition::Threshold(ThresholdCondition::new(
                "queue_depth",
                ThresholdOperator::GreaterThan,
                1000.0,
            )))
            .with_severity(AlertSeverity::Warning)
            .with_for_duration(std::time::Duration::from_secs(300))
            .with_label("category", "performance"),

        // Node heartbeat missing
        AlertRule::new("node_heartbeat_missing")
            .with_name("Node Heartbeat Missing")
            .with_description("Node has stopped sending heartbeats")
            .with_condition(RuleCondition::Threshold(ThresholdCondition::new(
                "heartbeat_age",
                ThresholdOperator::GreaterThan,
                60.0,
            )))
            .with_severity(AlertSeverity::Critical)
            .with_for_duration(std::time::Duration::from_secs(30))
            .with_label("category", "availability"),

        // High task failure rate
        AlertRule::new("high_failure_rate")
            .with_name("High Task Failure Rate")
            .with_description("Task failure rate is elevated")
            .with_condition(RuleCondition::Rate(RateCondition::new(
                "task_failures",
                ThresholdOperator::GreaterThan,
                0.1, // More than 10% failure rate per second
                std::time::Duration::from_secs(300),
            )))
            .with_severity(AlertSeverity::Warning)
            .with_label("category", "reliability"),

        // Metrics missing (absence alert)
        AlertRule::new("metrics_missing")
            .with_name("Metrics Collection Failed")
            .with_description("Metrics collection has stopped")
            .with_condition(RuleCondition::Absence(AbsenceCondition::new(
                "heartbeat_age",
                std::time::Duration::from_secs(300),
            )))
            .with_severity(AlertSeverity::Warning)
            .with_label("category", "monitoring"),
    ]
}

/// Create standard inhibition rules
pub fn create_standard_inhibitions() -> Vec<InhibitRule> {
    vec![
        // Node offline inhibits all other node alerts
        InhibitRule::new(
            vec![LabelMatcher::exact("alertname", "node_heartbeat_missing")],
            vec![LabelMatcher::regex("category", "resource|performance")],
            vec!["node_id".to_string()],
        ),
        // Critical alerts inhibit warning alerts for the same resource
        InhibitRule::new(
            vec![LabelMatcher::exact("severity", "critical")],
            vec![LabelMatcher::exact("severity", "warning")],
            vec!["alertname".to_string(), "node_id".to_string()],
        ),
    ]
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_creation() {
        let alert = Alert::new(AlertSeverity::Critical, "test_source", "Test message")
            .with_label("node_id", "node-1")
            .with_label("region", "us-west");

        assert_eq!(alert.severity, AlertSeverity::Critical);
        assert_eq!(alert.source, "test_source");
        assert_eq!(alert.message, "Test message");
        assert_eq!(alert.labels.get("node_id"), Some(&"node-1".to_string()));
        assert!(alert.is_firing());
        assert!(!alert.fingerprint.is_empty());
    }

    #[test]
    fn test_alert_fingerprint_uniqueness() {
        let alert1 = Alert::new(AlertSeverity::Warning, "source1", "message")
            .with_label("key", "value1");

        let alert2 = Alert::new(AlertSeverity::Warning, "source1", "message")
            .with_label("key", "value2");

        let alert3 = Alert::new(AlertSeverity::Warning, "source1", "message")
            .with_label("key", "value1");

        assert_ne!(alert1.fingerprint, alert2.fingerprint);
        assert_eq!(alert1.fingerprint, alert3.fingerprint);
    }

    #[test]
    fn test_alert_resolve() {
        let mut alert = Alert::new(AlertSeverity::Warning, "test", "test");
        assert!(alert.is_firing());
        assert!(!alert.is_resolved());

        alert.resolve();
        assert!(!alert.is_firing());
        assert!(alert.is_resolved());
        assert!(alert.resolved_at.is_some());
    }

    #[test]
    fn test_threshold_operator() {
        assert!(ThresholdOperator::GreaterThan.evaluate(10.0, 5.0));
        assert!(!ThresholdOperator::GreaterThan.evaluate(5.0, 10.0));

        assert!(ThresholdOperator::LessThan.evaluate(5.0, 10.0));
        assert!(ThresholdOperator::GreaterThanOrEqual.evaluate(10.0, 10.0));
        assert!(ThresholdOperator::LessThanOrEqual.evaluate(10.0, 10.0));
        assert!(ThresholdOperator::Equal.evaluate(10.0, 10.0));
        assert!(ThresholdOperator::NotEqual.evaluate(10.0, 5.0));
    }

    #[test]
    fn test_alert_rule_creation() {
        let rule = AlertRule::new("test_rule")
            .with_name("Test Rule")
            .with_description("A test rule")
            .with_condition(RuleCondition::Threshold(ThresholdCondition::new(
                "cpu_usage",
                ThresholdOperator::GreaterThan,
                90.0,
            )))
            .with_severity(AlertSeverity::Critical)
            .with_for_duration(std::time::Duration::from_secs(300))
            .with_cooldown(std::time::Duration::from_secs(600))
            .with_label("team", "platform");

        assert_eq!(rule.id.0, "test_rule");
        assert_eq!(rule.name, "Test Rule");
        assert_eq!(rule.severity, AlertSeverity::Critical);
        assert!(rule.enabled);
    }

    #[test]
    fn test_silence_rule() {
        let silence = SilenceRule::new(
            vec![LabelMatcher::exact("node_id", "node-1")],
            Utc::now() - Duration::hours(1),
            Utc::now() + Duration::hours(1),
            "admin",
            "Maintenance window",
        );

        assert!(silence.is_active());

        let matching_alert = Alert::new(AlertSeverity::Warning, "test", "test")
            .with_label("node_id", "node-1");

        let non_matching_alert = Alert::new(AlertSeverity::Warning, "test", "test")
            .with_label("node_id", "node-2");

        assert!(silence.matches(&matching_alert));
        assert!(!silence.matches(&non_matching_alert));
    }

    #[test]
    fn test_silence_expired() {
        let silence = SilenceRule::new(
            vec![LabelMatcher::exact("node_id", "node-1")],
            Utc::now() - Duration::hours(2),
            Utc::now() - Duration::hours(1), // Expired
            "admin",
            "Past maintenance",
        );

        assert!(!silence.is_active());
    }

    #[test]
    fn test_label_matcher() {
        let labels: HashMap<String, String> = [
            ("node_id".to_string(), "node-123".to_string()),
            ("region".to_string(), "us-west-2".to_string()),
        ].into_iter().collect();

        // Exact match
        let exact = LabelMatcher::exact("node_id", "node-123");
        assert!(exact.matches(&labels));

        // Non-match
        let non_match = LabelMatcher::exact("node_id", "node-456");
        assert!(!non_match.matches(&labels));

        // Regex match
        let regex = LabelMatcher::regex("region", "us-.*");
        assert!(regex.matches(&labels));

        // Missing label
        let missing = LabelMatcher::exact("cluster", "prod");
        assert!(!missing.matches(&labels));
    }

    #[test]
    fn test_inhibit_rule() {
        let rule = InhibitRule::new(
            vec![LabelMatcher::exact("alertname", "NodeDown")],
            vec![LabelMatcher::exact("alertname", "HighCPU")],
            vec!["node_id".to_string()],
        );

        let source_alert = Alert::new(AlertSeverity::Critical, "test", "test")
            .with_label("alertname", "NodeDown")
            .with_label("node_id", "node-1");

        let target_alert = Alert::new(AlertSeverity::Warning, "test", "test")
            .with_label("alertname", "HighCPU")
            .with_label("node_id", "node-1");

        let unrelated_alert = Alert::new(AlertSeverity::Warning, "test", "test")
            .with_label("alertname", "HighCPU")
            .with_label("node_id", "node-2");

        assert!(rule.should_inhibit(&source_alert, &target_alert));
        assert!(!rule.should_inhibit(&source_alert, &unrelated_alert));
    }

    #[test]
    fn test_alert_group() {
        let labels: HashMap<String, String> = [
            ("alertname".to_string(), "HighCPU".to_string()),
            ("severity".to_string(), "warning".to_string()),
        ].into_iter().collect();

        let mut group = AlertGroup::new(labels.clone());
        assert!(group.alerts.is_empty());

        let alert1 = Alert::new(AlertSeverity::Warning, "test", "test1")
            .with_label("instance", "server1");
        let alert2 = Alert::new(AlertSeverity::Warning, "test", "test2")
            .with_label("instance", "server2");

        group.add_alert(alert1);
        group.add_alert(alert2);

        assert_eq!(group.alerts.len(), 2);
        assert!(group.has_firing_alerts());
        assert_eq!(group.max_severity(), AlertSeverity::Warning);
    }

    #[test]
    fn test_grouping_config_default() {
        let config = GroupingConfig::default();
        assert!(config.group_by.contains(&"alertname".to_string()));
        assert!(config.group_by.contains(&"severity".to_string()));
    }

    #[test]
    fn test_severity_ordering() {
        assert!(AlertSeverity::Critical > AlertSeverity::Warning);
        assert!(AlertSeverity::Warning > AlertSeverity::Info);
        assert_eq!(AlertSeverity::Critical.priority(), 2);
        assert_eq!(AlertSeverity::Warning.priority(), 1);
        assert_eq!(AlertSeverity::Info.priority(), 0);
    }

    #[test]
    fn test_threshold_condition() {
        let condition = ThresholdCondition::new("cpu_usage", ThresholdOperator::GreaterThan, 80.0)
            .with_label_matcher("region", "us-west");

        assert_eq!(condition.metric_name, "cpu_usage");
        assert_eq!(condition.value, 80.0);
        assert_eq!(condition.label_matchers.get("region"), Some(&"us-west".to_string()));
    }

    #[test]
    fn test_rate_condition() {
        let condition = RateCondition::new(
            "requests",
            ThresholdOperator::GreaterThan,
            100.0,
            std::time::Duration::from_secs(60),
        );

        assert_eq!(condition.metric_name, "requests");
        assert_eq!(condition.rate_threshold, 100.0);
    }

    #[test]
    fn test_absence_condition() {
        let condition = AbsenceCondition::new("heartbeat", std::time::Duration::from_secs(60));
        assert_eq!(condition.metric_name, "heartbeat");
    }

    #[test]
    fn test_composite_condition() {
        let composite = CompositeCondition {
            operator: CompositeOperator::And,
            conditions: vec![
                RuleCondition::Threshold(ThresholdCondition::new(
                    "cpu",
                    ThresholdOperator::GreaterThan,
                    80.0,
                )),
                RuleCondition::Threshold(ThresholdCondition::new(
                    "memory",
                    ThresholdOperator::GreaterThan,
                    80.0,
                )),
            ],
        };

        assert_eq!(composite.conditions.len(), 2);
    }

    #[test]
    fn test_webhook_sink_creation() {
        let sink = WebhookSink::new("https://example.com/webhook")
            .with_name("my_webhook")
            .with_header("Authorization", "Bearer token")
            .with_timeout(std::time::Duration::from_secs(60));

        assert_eq!(sink.name, "my_webhook");
        assert_eq!(sink.url, "https://example.com/webhook");
        assert_eq!(sink.headers.get("Authorization"), Some(&"Bearer token".to_string()));
    }

    #[test]
    fn test_email_sink_creation() {
        let sink = EmailSink::new(
            "smtp.example.com",
            "alerts@example.com",
            vec!["team@example.com".to_string()],
        )
        .with_port(465)
        .with_auth("user", "pass");

        assert_eq!(sink.smtp_host, "smtp.example.com");
        assert_eq!(sink.smtp_port, 465);
        assert_eq!(sink.from, "alerts@example.com");
    }

    #[test]
    fn test_pagerduty_sink_creation() {
        let sink = PagerDutySink::new("routing_key_123")
            .with_api_url("https://custom.pagerduty.com/v2/enqueue")
            .with_custom_detail("environment", "production");

        assert_eq!(sink.routing_key, "routing_key_123");
        assert!(sink.custom_details.contains_key("environment"));
    }

    #[test]
    fn test_standard_rules() {
        let rules = create_standard_rules();
        assert!(!rules.is_empty());

        // Verify we have expected rules
        let rule_names: Vec<_> = rules.iter().map(|r| r.id.0.as_str()).collect();
        assert!(rule_names.contains(&"high_cpu_usage"));
        assert!(rule_names.contains(&"high_memory_usage"));
        assert!(rule_names.contains(&"high_queue_depth"));
    }

    #[test]
    fn test_standard_inhibitions() {
        let inhibitions = create_standard_inhibitions();
        assert!(!inhibitions.is_empty());
    }

    #[tokio::test]
    async fn test_alert_manager_creation() {
        let config = AlertManagerConfig::default();
        let (manager, _rx) = AlertManager::new(config);

        assert!(manager.get_active_alerts().is_empty());
        assert!(manager.get_rules().is_empty());
        assert!(manager.get_silences().is_empty());
    }

    #[tokio::test]
    async fn test_alert_manager_add_rule() {
        let config = AlertManagerConfig::default();
        let (manager, _rx) = AlertManager::new(config);

        let rule = AlertRule::new("test_rule")
            .with_severity(AlertSeverity::Warning);

        manager.add_rule(rule);

        let rules = manager.get_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].id.0, "test_rule");
    }

    #[tokio::test]
    async fn test_alert_manager_fire_alert() {
        let config = AlertManagerConfig::default();
        let (manager, mut rx) = AlertManager::new(config);

        let alert = Alert::new(AlertSeverity::Critical, "test", "Test alert");
        manager.fire_alert(alert.clone()).await;

        let alerts = manager.get_active_alerts();
        assert_eq!(alerts.len(), 1);

        // Check event was emitted
        if let Ok(event) = rx.try_recv() {
            assert!(matches!(event, AlertManagerEvent::AlertCreated(_)));
        }
    }

    #[tokio::test]
    async fn test_alert_manager_silence() {
        let config = AlertManagerConfig::default();
        let (manager, _rx) = AlertManager::new(config);

        let silence = SilenceRule::new(
            vec![LabelMatcher::exact("test_label", "test_value")],
            Utc::now() - Duration::hours(1),
            Utc::now() + Duration::hours(1),
            "tester",
            "Testing silence",
        );
        manager.add_silence(silence);

        let alert = Alert::new(AlertSeverity::Warning, "test", "test")
            .with_label("test_label", "test_value");
        manager.fire_alert(alert).await;

        // Alert should be silenced
        let alerts = manager.get_active_alerts();
        assert!(alerts.is_empty() || alerts[0].status == AlertStatus::Silenced);
    }

    #[tokio::test]
    async fn test_alert_manager_resolve() {
        let config = AlertManagerConfig::default();
        let (manager, _rx) = AlertManager::new(config);

        let alert = Alert::new(AlertSeverity::Warning, "test", "test");
        let fingerprint = alert.fingerprint.clone();
        manager.fire_alert(alert).await;

        assert_eq!(manager.get_active_alerts().len(), 1);

        manager.resolve_alert(&fingerprint);

        // Alert should now be resolved
        if let Some(alert) = manager.get_alert(&fingerprint) {
            assert!(alert.is_resolved());
        }
    }

    #[tokio::test]
    async fn test_alert_manager_evaluate_threshold() {
        let config = AlertManagerConfig::default();
        let (manager, _rx) = AlertManager::new(config);

        // Add a threshold rule
        let rule = AlertRule::new("high_cpu")
            .with_condition(RuleCondition::Threshold(ThresholdCondition::new(
                "cpu_usage",
                ThresholdOperator::GreaterThan,
                80.0,
            )))
            .with_severity(AlertSeverity::Warning);
        manager.add_rule(rule);

        // Evaluate with high CPU
        let metrics = vec![MetricValue::new("cpu_usage", 95.0)];
        manager.evaluate_metrics(&metrics).await;

        // Should have fired an alert
        assert!(!manager.get_active_alerts().is_empty());
    }

    #[tokio::test]
    async fn test_metric_value_creation() {
        let metric = MetricValue::new("test_metric", 42.0)
            .with_label("instance", "localhost:9090");

        assert_eq!(metric.name, "test_metric");
        assert_eq!(metric.value, 42.0);
        assert_eq!(metric.labels.get("instance"), Some(&"localhost:9090".to_string()));
    }

    #[test]
    fn test_alert_manager_config_default() {
        let config = AlertManagerConfig::default();
        assert_eq!(config.max_history, 10000);
        assert_eq!(config.maintenance_interval, std::time::Duration::from_secs(60));
    }

    #[test]
    fn test_sink_error() {
        let error = SinkError::new("Test error", true);
        assert_eq!(error.message, "Test error");
        assert!(error.retryable);
    }

    #[test]
    fn test_alert_update() {
        let mut alert = Alert::new(AlertSeverity::Warning, "test", "test");
        let original_fire_count = alert.fire_count;
        let original_updated_at = alert.updated_at;

        std::thread::sleep(std::time::Duration::from_millis(10));
        alert.update();

        assert_eq!(alert.fire_count, original_fire_count + 1);
        assert!(alert.updated_at > original_updated_at);
    }
}
