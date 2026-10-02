// Marabunta - Licensed under the MIT License.
//! Health Checking and Alerting
//!
//! This module provides comprehensive health monitoring for infrastructure nodes,
//! including configurable thresholds, alert generation, deduplication, and escalation.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use super::metrics::{MetricsAnomaly, NodeMetrics};
use super::node::{NodeId, NodeStatus, SlaTier};

/// Configuration for health checking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Interval between health checks.
    pub heartbeat_interval: std::time::Duration,
    /// Time without heartbeat before marking node offline.
    pub heartbeat_timeout: std::time::Duration,
    /// CPU usage threshold for warning (percentage).
    pub cpu_warning_threshold: f32,
    /// CPU usage threshold for critical alert (percentage).
    pub cpu_critical_threshold: f32,
    /// Memory usage threshold for warning (percentage).
    pub memory_warning_threshold: f32,
    /// Memory usage threshold for critical alert (percentage).
    pub memory_critical_threshold: f32,
    /// Task failure rate threshold for alerts (0.0 - 1.0).
    pub task_failure_rate_threshold: f32,
    /// Temperature threshold for warning (Celsius).
    pub temperature_warning_threshold: f32,
    /// Temperature threshold for critical alert (Celsius).
    pub temperature_critical_threshold: f32,
    /// Minimum time between duplicate alerts.
    pub alert_cooldown: std::time::Duration,
    /// Time window for sustained high usage detection.
    pub sustained_usage_window: std::time::Duration,
    /// Enable anomaly detection.
    pub anomaly_detection_enabled: bool,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: std::time::Duration::from_secs(10),
            heartbeat_timeout: std::time::Duration::from_secs(30),
            cpu_warning_threshold: 80.0,
            cpu_critical_threshold: 95.0,
            memory_warning_threshold: 85.0,
            memory_critical_threshold: 95.0,
            task_failure_rate_threshold: 0.1, // 10%
            temperature_warning_threshold: 75.0,
            temperature_critical_threshold: 90.0,
            alert_cooldown: std::time::Duration::from_secs(300), // 5 minutes
            sustained_usage_window: std::time::Duration::from_secs(300),
            anomaly_detection_enabled: true,
        }
    }
}

/// Health state of a single node.
#[derive(Debug, Clone)]
pub struct HealthState {
    /// Node identifier.
    pub node_id: NodeId,
    /// Current status.
    pub status: NodeStatus,
    /// Last metrics received.
    pub last_metrics: Option<NodeMetrics>,
    /// Last heartbeat time.
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// Active alerts for this node.
    pub active_alerts: Vec<Alert>,
    /// SLA tier for determining urgency.
    pub sla_tier: SlaTier,
    /// Uptime tracking for SLA.
    pub uptime_tracker: UptimeTracker,
    /// Last time each alert type was sent (for deduplication).
    alert_cooldowns: HashMap<AlertType, DateTime<Utc>>,
    /// High CPU usage start time (for sustained usage detection).
    high_cpu_since: Option<DateTime<Utc>>,
    /// High memory usage start time.
    #[allow(dead_code)]
    high_memory_since: Option<DateTime<Utc>>,
}

impl HealthState {
    /// Creates a new health state for a node.
    pub fn new(node_id: NodeId, sla_tier: SlaTier) -> Self {
        Self {
            node_id,
            status: NodeStatus::offline("Not yet seen"),
            last_metrics: None,
            last_heartbeat: None,
            active_alerts: Vec::new(),
            sla_tier,
            uptime_tracker: UptimeTracker::new(),
            alert_cooldowns: HashMap::new(),
            high_cpu_since: None,
            high_memory_since: None,
        }
    }

    /// Checks if an alert type is in cooldown.
    fn is_in_cooldown(&self, alert_type: &AlertType, cooldown: std::time::Duration) -> bool {
        if let Some(last_sent) = self.alert_cooldowns.get(alert_type) {
            let cooldown_duration = Duration::from_std(cooldown).unwrap_or(Duration::minutes(5));
            Utc::now() - *last_sent < cooldown_duration
        } else {
            false
        }
    }

    /// Records that an alert was sent.
    fn record_alert_sent(&mut self, alert_type: AlertType) {
        self.alert_cooldowns.insert(alert_type, Utc::now());
    }

    /// Clears an alert from the active list.
    pub fn clear_alert(&mut self, alert_type: AlertType) {
        self.active_alerts.retain(|a| a.alert_type() != alert_type);
        self.alert_cooldowns.remove(&alert_type);
    }
}

/// Tracks uptime for SLA compliance.
#[derive(Debug, Clone, Default)]
pub struct UptimeTracker {
    /// Total tracked time in seconds.
    total_tracked_seconds: u64,
    /// Total uptime in seconds.
    uptime_seconds: u64,
    /// Last status change time.
    last_status_change: Option<DateTime<Utc>>,
    /// Whether currently online.
    is_online: bool,
}

impl UptimeTracker {
    /// Creates a new uptime tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a status change.
    pub fn record_status_change(&mut self, now: DateTime<Utc>, is_online: bool) {
        if let Some(last_change) = self.last_status_change {
            let duration = (now - last_change).num_seconds().max(0) as u64;
            self.total_tracked_seconds += duration;
            if self.is_online {
                self.uptime_seconds += duration;
            }
        }
        self.last_status_change = Some(now);
        self.is_online = is_online;
    }

    /// Returns the current uptime percentage.
    pub fn uptime_percentage(&self) -> f64 {
        if self.total_tracked_seconds == 0 {
            100.0 // No data yet, assume 100%
        } else {
            (self.uptime_seconds as f64 / self.total_tracked_seconds as f64) * 100.0
        }
    }
}

/// Alert severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational, no action required.
    Info,
    /// Warning, should be investigated.
    Warning,
    /// Critical, requires immediate attention.
    Critical,
}

impl AlertSeverity {
    /// Returns a numeric priority (higher = more severe).
    pub fn priority(&self) -> u32 {
        match self {
            AlertSeverity::Info => 0,
            AlertSeverity::Warning => 1,
            AlertSeverity::Critical => 2,
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

/// Types of alerts for deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertType {
    NodeOffline,
    NodeDegraded,
    HighCpuUsage,
    HighMemoryUsage,
    HighTaskFailureRate,
    TemperatureWarning,
    SlaAtRisk,
    Anomaly,
}

/// Alert generated by health checking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Alert {
    /// Node has gone offline.
    NodeOffline {
        node_id: NodeId,
        last_seen: DateTime<Utc>,
        sla_tier: SlaTier,
    },
    /// Node is degraded but still partially functional.
    NodeDegraded {
        node_id: NodeId,
        reason: String,
        since: DateTime<Utc>,
    },
    /// CPU usage is consistently high.
    HighCpuUsage {
        node_id: NodeId,
        usage: f32,
        duration: std::time::Duration,
        severity: AlertSeverity,
    },
    /// Memory usage is critically high.
    HighMemoryUsage {
        node_id: NodeId,
        usage: f32,
        severity: AlertSeverity,
    },
    /// Task failure rate exceeds threshold.
    HighTaskFailureRate {
        node_id: NodeId,
        rate: f32,
        threshold: f32,
    },
    /// Temperature exceeds safe limits.
    TemperatureWarning {
        node_id: NodeId,
        temperature: f32,
        threshold: f32,
        severity: AlertSeverity,
    },
    /// SLA compliance is at risk.
    SlaAtRisk {
        node_id: NodeId,
        current_uptime: f64,
        required_uptime: f64,
        sla_tier: SlaTier,
    },
    /// Anomaly detected in metrics.
    Anomaly {
        node_id: NodeId,
        anomaly: MetricsAnomaly,
    },
}

impl Alert {
    /// Returns the alert type for deduplication.
    pub fn alert_type(&self) -> AlertType {
        match self {
            Alert::NodeOffline { .. } => AlertType::NodeOffline,
            Alert::NodeDegraded { .. } => AlertType::NodeDegraded,
            Alert::HighCpuUsage { .. } => AlertType::HighCpuUsage,
            Alert::HighMemoryUsage { .. } => AlertType::HighMemoryUsage,
            Alert::HighTaskFailureRate { .. } => AlertType::HighTaskFailureRate,
            Alert::TemperatureWarning { .. } => AlertType::TemperatureWarning,
            Alert::SlaAtRisk { .. } => AlertType::SlaAtRisk,
            Alert::Anomaly { .. } => AlertType::Anomaly,
        }
    }

    /// Returns the severity of the alert.
    pub fn severity(&self) -> AlertSeverity {
        match self {
            Alert::NodeOffline { sla_tier, .. } => {
                if matches!(sla_tier, SlaTier::Critical | SlaTier::Production) {
                    AlertSeverity::Critical
                } else {
                    AlertSeverity::Warning
                }
            }
            Alert::NodeDegraded { .. } => AlertSeverity::Warning,
            Alert::HighCpuUsage { severity, .. } => *severity,
            Alert::HighMemoryUsage { severity, .. } => *severity,
            Alert::HighTaskFailureRate {
                rate, threshold, ..
            } => {
                if *rate > threshold * 2.0 {
                    AlertSeverity::Critical
                } else {
                    AlertSeverity::Warning
                }
            }
            Alert::TemperatureWarning { severity, .. } => *severity,
            Alert::SlaAtRisk { sla_tier, .. } => {
                if matches!(sla_tier, SlaTier::Critical) {
                    AlertSeverity::Critical
                } else {
                    AlertSeverity::Warning
                }
            }
            Alert::Anomaly { .. } => AlertSeverity::Warning,
        }
    }

    /// Returns the node ID associated with the alert.
    pub fn node_id(&self) -> NodeId {
        match self {
            Alert::NodeOffline { node_id, .. } => *node_id,
            Alert::NodeDegraded { node_id, .. } => *node_id,
            Alert::HighCpuUsage { node_id, .. } => *node_id,
            Alert::HighMemoryUsage { node_id, .. } => *node_id,
            Alert::HighTaskFailureRate { node_id, .. } => *node_id,
            Alert::TemperatureWarning { node_id, .. } => *node_id,
            Alert::SlaAtRisk { node_id, .. } => *node_id,
            Alert::Anomaly { node_id, .. } => *node_id,
        }
    }
}

impl std::fmt::Display for Alert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Alert::NodeOffline {
                node_id, last_seen, ..
            } => {
                write!(
                    f,
                    "[{}] Node {} is offline (last seen: {})",
                    self.severity(),
                    node_id,
                    last_seen.format("%Y-%m-%d %H:%M:%S")
                )
            }
            Alert::NodeDegraded {
                node_id, reason, ..
            } => {
                write!(
                    f,
                    "[{}] Node {} is degraded: {}",
                    self.severity(),
                    node_id,
                    reason
                )
            }
            Alert::HighCpuUsage {
                node_id,
                usage,
                duration,
                ..
            } => {
                write!(
                    f,
                    "[{}] Node {} has high CPU usage: {:.1}% for {:?}",
                    self.severity(),
                    node_id,
                    usage,
                    duration
                )
            }
            Alert::HighMemoryUsage { node_id, usage, .. } => {
                write!(
                    f,
                    "[{}] Node {} has high memory usage: {:.1}%",
                    self.severity(),
                    node_id,
                    usage
                )
            }
            Alert::HighTaskFailureRate { node_id, rate, .. } => {
                write!(
                    f,
                    "[{}] Node {} has high task failure rate: {:.1}%",
                    self.severity(),
                    node_id,
                    rate * 100.0
                )
            }
            Alert::TemperatureWarning {
                node_id,
                temperature,
                ..
            } => {
                write!(
                    f,
                    "[{}] Node {} temperature warning: {:.1}C",
                    self.severity(),
                    node_id,
                    temperature
                )
            }
            Alert::SlaAtRisk {
                node_id,
                current_uptime,
                required_uptime,
                ..
            } => {
                write!(
                    f,
                    "[{}] Node {} SLA at risk: {:.2}% (required: {:.2}%)",
                    self.severity(),
                    node_id,
                    current_uptime,
                    required_uptime
                )
            }
            Alert::Anomaly { node_id, anomaly } => {
                write!(
                    f,
                    "[{}] Node {} anomaly: {}",
                    self.severity(),
                    node_id,
                    anomaly
                )
            }
        }
    }
}

/// Health checker that monitors infrastructure nodes.
pub struct HealthChecker {
    /// Health state for each node.
    nodes: Arc<RwLock<HashMap<NodeId, HealthState>>>,
    /// Configuration.
    config: HealthConfig,
    /// Channel to send alerts.
    alert_tx: mpsc::Sender<Alert>,
    /// Shutdown signal.
    shutdown: Arc<RwLock<bool>>,
}

impl HealthChecker {
    /// Creates a new health checker.
    pub fn new(config: HealthConfig, alert_tx: mpsc::Sender<Alert>) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            config,
            alert_tx,
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Registers a node for health monitoring.
    pub async fn register_node(&self, node_id: NodeId, sla_tier: SlaTier) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id, HealthState::new(node_id, sla_tier));
    }

    /// Unregisters a node from health monitoring.
    pub async fn unregister_node(&self, node_id: &NodeId) {
        let mut nodes = self.nodes.write().await;
        nodes.remove(node_id);
    }

    /// Records a heartbeat from a node.
    pub async fn record_heartbeat(&self, node_id: &NodeId, metrics: NodeMetrics) {
        let mut nodes = self.nodes.write().await;
        if let Some(state) = nodes.get_mut(node_id) {
            let now = Utc::now();
            let was_online = matches!(state.status, NodeStatus::Online { .. });

            state.last_heartbeat = Some(now);
            state.last_metrics = Some(metrics.clone());

            // Update status to online if it was offline
            if !was_online {
                state.status = NodeStatus::online();
                state.uptime_tracker.record_status_change(now, true);
                // Clear offline alert
                state.clear_alert(AlertType::NodeOffline);
            }

            // Check metrics and generate alerts
            drop(nodes); // Release lock before async operations
            self.check_metrics(node_id, &metrics).await;
        }
    }

    /// Checks metrics and generates alerts if thresholds are exceeded.
    async fn check_metrics(&self, node_id: &NodeId, metrics: &NodeMetrics) {
        let mut nodes = self.nodes.write().await;
        let Some(state) = nodes.get_mut(node_id) else {
            return;
        };

        let now = Utc::now();

        // Check CPU usage
        let cpu_usage = metrics.cpu_usage_percent;
        if cpu_usage >= self.config.cpu_critical_threshold {
            // Track sustained high CPU
            if state.high_cpu_since.is_none() {
                state.high_cpu_since = Some(now);
            }

            let duration = state.high_cpu_since.map(|t| now - t).unwrap_or_default();

            if duration
                >= Duration::from_std(self.config.sustained_usage_window).unwrap_or_default()
                && !state.is_in_cooldown(&AlertType::HighCpuUsage, self.config.alert_cooldown) {
                    let alert = Alert::HighCpuUsage {
                        node_id: *node_id,
                        usage: cpu_usage,
                        duration: duration.to_std().unwrap_or_default(),
                        severity: AlertSeverity::Critical,
                    };
                    state.record_alert_sent(AlertType::HighCpuUsage);
                    state.active_alerts.push(alert.clone());
                    let _ = self.alert_tx.send(alert).await;
                }
        } else if cpu_usage >= self.config.cpu_warning_threshold {
            if state.high_cpu_since.is_none() {
                state.high_cpu_since = Some(now);
            }
        } else {
            state.high_cpu_since = None;
            state.clear_alert(AlertType::HighCpuUsage);
        }

        // Check memory usage
        let memory_usage = metrics.memory_usage_percent();
        if memory_usage >= self.config.memory_critical_threshold {
            if !state.is_in_cooldown(&AlertType::HighMemoryUsage, self.config.alert_cooldown) {
                let alert = Alert::HighMemoryUsage {
                    node_id: *node_id,
                    usage: memory_usage,
                    severity: AlertSeverity::Critical,
                };
                state.record_alert_sent(AlertType::HighMemoryUsage);
                state.active_alerts.push(alert.clone());
                let _ = self.alert_tx.send(alert).await;
            }
        } else if memory_usage >= self.config.memory_warning_threshold {
            if !state.is_in_cooldown(&AlertType::HighMemoryUsage, self.config.alert_cooldown) {
                let alert = Alert::HighMemoryUsage {
                    node_id: *node_id,
                    usage: memory_usage,
                    severity: AlertSeverity::Warning,
                };
                state.record_alert_sent(AlertType::HighMemoryUsage);
                state.active_alerts.push(alert.clone());
                let _ = self.alert_tx.send(alert).await;
            }
        } else {
            state.clear_alert(AlertType::HighMemoryUsage);
        }

        // Check task failure rate
        let failure_rate = metrics.task_failure_rate();
        if failure_rate > self.config.task_failure_rate_threshold {
            if !state.is_in_cooldown(&AlertType::HighTaskFailureRate, self.config.alert_cooldown) {
                let alert = Alert::HighTaskFailureRate {
                    node_id: *node_id,
                    rate: failure_rate,
                    threshold: self.config.task_failure_rate_threshold,
                };
                state.record_alert_sent(AlertType::HighTaskFailureRate);
                state.active_alerts.push(alert.clone());
                let _ = self.alert_tx.send(alert).await;
            }
        } else {
            state.clear_alert(AlertType::HighTaskFailureRate);
        }

        // Check temperature
        if let Some(temp) = metrics.temperature_celsius {
            if temp >= self.config.temperature_critical_threshold {
                if !state.is_in_cooldown(&AlertType::TemperatureWarning, self.config.alert_cooldown)
                {
                    let alert = Alert::TemperatureWarning {
                        node_id: *node_id,
                        temperature: temp,
                        threshold: self.config.temperature_critical_threshold,
                        severity: AlertSeverity::Critical,
                    };
                    state.record_alert_sent(AlertType::TemperatureWarning);
                    state.active_alerts.push(alert.clone());
                    let _ = self.alert_tx.send(alert).await;
                }
            } else if temp >= self.config.temperature_warning_threshold {
                if !state.is_in_cooldown(&AlertType::TemperatureWarning, self.config.alert_cooldown)
                {
                    let alert = Alert::TemperatureWarning {
                        node_id: *node_id,
                        temperature: temp,
                        threshold: self.config.temperature_warning_threshold,
                        severity: AlertSeverity::Warning,
                    };
                    state.record_alert_sent(AlertType::TemperatureWarning);
                    state.active_alerts.push(alert.clone());
                    let _ = self.alert_tx.send(alert).await;
                }
            } else {
                state.clear_alert(AlertType::TemperatureWarning);
            }
        }

        // Check SLA compliance
        let current_uptime = state.uptime_tracker.uptime_percentage();
        let required_uptime = state.sla_tier.required_uptime();
        if current_uptime < required_uptime && required_uptime > 0.0 {
            if !state.is_in_cooldown(&AlertType::SlaAtRisk, self.config.alert_cooldown) {
                let alert = Alert::SlaAtRisk {
                    node_id: *node_id,
                    current_uptime,
                    required_uptime,
                    sla_tier: state.sla_tier,
                };
                state.record_alert_sent(AlertType::SlaAtRisk);
                state.active_alerts.push(alert.clone());
                let _ = self.alert_tx.send(alert).await;
            }
        } else {
            state.clear_alert(AlertType::SlaAtRisk);
        }
    }

    /// Checks for nodes that have missed heartbeats.
    pub async fn check_heartbeat_timeouts(&self) {
        let now = Utc::now();
        let timeout =
            Duration::from_std(self.config.heartbeat_timeout).unwrap_or(Duration::seconds(30));

        let mut nodes = self.nodes.write().await;
        let mut alerts_to_send = Vec::new();

        for (node_id, state) in nodes.iter_mut() {
            if let Some(last_heartbeat) = state.last_heartbeat {
                if now - last_heartbeat > timeout {
                    // Node is offline
                    if matches!(
                        state.status,
                        NodeStatus::Online { .. } | NodeStatus::Degraded { .. }
                    ) {
                        state.status = NodeStatus::offline("Heartbeat timeout");
                        state.uptime_tracker.record_status_change(now, false);

                        if !state
                            .is_in_cooldown(&AlertType::NodeOffline, self.config.alert_cooldown)
                        {
                            let alert = Alert::NodeOffline {
                                node_id: *node_id,
                                last_seen: last_heartbeat,
                                sla_tier: state.sla_tier,
                            };
                            state.record_alert_sent(AlertType::NodeOffline);
                            state.active_alerts.push(alert.clone());
                            alerts_to_send.push(alert);
                        }
                    }
                }
            }
        }

        // Send alerts outside the lock
        drop(nodes);
        for alert in alerts_to_send {
            let _ = self.alert_tx.send(alert).await;
        }
    }

    /// Processes anomalies detected in metrics.
    pub async fn process_anomalies(&self, node_id: &NodeId, anomalies: Vec<MetricsAnomaly>) {
        if anomalies.is_empty() || !self.config.anomaly_detection_enabled {
            return;
        }

        let mut nodes = self.nodes.write().await;
        let Some(state) = nodes.get_mut(node_id) else {
            return;
        };

        for anomaly in anomalies {
            if !state.is_in_cooldown(&AlertType::Anomaly, self.config.alert_cooldown) {
                let alert = Alert::Anomaly {
                    node_id: *node_id,
                    anomaly,
                };
                state.record_alert_sent(AlertType::Anomaly);
                state.active_alerts.push(alert.clone());
                let _ = self.alert_tx.send(alert).await;
            }
        }
    }

    /// Gets the health state of a specific node.
    pub async fn get_node_health(&self, node_id: &NodeId) -> Option<HealthState> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    /// Gets health states of all nodes.
    pub async fn get_all_health(&self) -> HashMap<NodeId, HealthState> {
        let nodes = self.nodes.read().await;
        nodes.clone()
    }

    /// Returns the number of nodes being monitored.
    pub async fn node_count(&self) -> usize {
        let nodes = self.nodes.read().await;
        nodes.len()
    }

    /// Starts the health check loop.
    ///
    /// This should be spawned as a background task.
    pub async fn run(&self) {
        let interval = self.config.heartbeat_interval;
        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;

            if *self.shutdown.read().await {
                break;
            }

            self.check_heartbeat_timeouts().await;
        }
    }

    /// Signals the health checker to shut down.
    pub async fn shutdown(&self) {
        let mut shutdown = self.shutdown.write().await;
        *shutdown = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_checker_registration() {
        let (tx, _rx) = mpsc::channel(100);
        let checker = HealthChecker::new(HealthConfig::default(), tx);

        let node_id = NodeId::new();
        checker.register_node(node_id, SlaTier::Production).await;

        assert_eq!(checker.node_count().await, 1);

        let health = checker.get_node_health(&node_id).await;
        assert!(health.is_some());
        assert!(matches!(health.unwrap().status, NodeStatus::Offline { .. }));
    }

    #[tokio::test]
    async fn test_heartbeat_updates_status() {
        let (tx, mut rx) = mpsc::channel(100);
        let checker = HealthChecker::new(HealthConfig::default(), tx);

        let node_id = NodeId::new();
        checker.register_node(node_id, SlaTier::Production).await;

        // Record heartbeat
        let metrics = NodeMetrics::new();
        checker.record_heartbeat(&node_id, metrics).await;

        let health = checker.get_node_health(&node_id).await.unwrap();
        assert!(matches!(health.status, NodeStatus::Online { .. }));
        assert!(health.last_heartbeat.is_some());

        // No alerts should be generated for normal metrics
        // Drain channel to check
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_high_memory_alert() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = HealthConfig {
            memory_warning_threshold: 80.0,
            memory_critical_threshold: 90.0,
            ..Default::default()
        };
        let checker = HealthChecker::new(config, tx);

        let node_id = NodeId::new();
        checker.register_node(node_id, SlaTier::Production).await;

        // Send metrics with high memory
        let mut metrics = NodeMetrics::new();
        metrics.memory_used_bytes = 92 * 1024 * 1024 * 1024; // 92 GB
        metrics.memory_available_bytes = 8 * 1024 * 1024 * 1024; // 8 GB (92% usage)
        checker.record_heartbeat(&node_id, metrics).await;

        // Should receive a high memory alert
        let alert = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(alert, Alert::HighMemoryUsage { .. }));
    }

    #[tokio::test]
    async fn test_task_failure_rate_alert() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = HealthConfig {
            task_failure_rate_threshold: 0.1,
            ..Default::default()
        };
        let checker = HealthChecker::new(config, tx);

        let node_id = NodeId::new();
        checker.register_node(node_id, SlaTier::Production).await;

        // Send metrics with high failure rate
        let mut metrics = NodeMetrics::new();
        metrics.tasks_completed = 80;
        metrics.tasks_failed = 20; // 20% failure rate
        checker.record_heartbeat(&node_id, metrics).await;

        let alert = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(alert, Alert::HighTaskFailureRate { rate, .. } if rate > 0.15));
    }

    #[tokio::test]
    async fn test_temperature_alert() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = HealthConfig {
            temperature_warning_threshold: 75.0,
            temperature_critical_threshold: 90.0,
            ..Default::default()
        };
        let checker = HealthChecker::new(config, tx);

        let node_id = NodeId::new();
        checker.register_node(node_id, SlaTier::Production).await;

        // Send metrics with high temperature
        let mut metrics = NodeMetrics::new();
        metrics.temperature_celsius = Some(95.0);
        checker.record_heartbeat(&node_id, metrics).await;

        let alert = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            alert,
            Alert::TemperatureWarning {
                severity: AlertSeverity::Critical,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_heartbeat_timeout() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = HealthConfig {
            heartbeat_timeout: std::time::Duration::from_millis(50),
            ..Default::default()
        };
        let checker = HealthChecker::new(config, tx);

        let node_id = NodeId::new();
        checker.register_node(node_id, SlaTier::Critical).await;

        // Record initial heartbeat
        let metrics = NodeMetrics::new();
        checker.record_heartbeat(&node_id, metrics).await;

        // Wait for timeout
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Check for timeout
        checker.check_heartbeat_timeouts().await;

        let alert = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(alert, Alert::NodeOffline { .. }));
    }

    #[tokio::test]
    async fn test_alert_cooldown() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = HealthConfig {
            memory_warning_threshold: 80.0,
            memory_critical_threshold: 90.0,
            alert_cooldown: std::time::Duration::from_secs(60),
            ..Default::default()
        };
        let checker = HealthChecker::new(config, tx);

        let node_id = NodeId::new();
        checker.register_node(node_id, SlaTier::Production).await;

        // Send first high memory metrics
        let mut metrics = NodeMetrics::new();
        metrics.memory_used_bytes = 95 * 1024 * 1024 * 1024;
        metrics.memory_available_bytes = 5 * 1024 * 1024 * 1024;
        checker.record_heartbeat(&node_id, metrics.clone()).await;

        // Should receive first alert
        let _ = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .unwrap();

        // Send second heartbeat with same high memory
        checker.record_heartbeat(&node_id, metrics).await;

        // Should NOT receive another alert due to cooldown
        let result = tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;
        assert!(result.is_err()); // Timeout = no alert
    }

    #[test]
    fn test_uptime_tracker() {
        let mut tracker = UptimeTracker::new();
        let start = Utc::now();

        // Go online
        tracker.record_status_change(start, true);

        // Go offline after 90 seconds of being online
        let t1 = start + Duration::seconds(90);
        tracker.record_status_change(t1, false);

        // Record a status after 10 seconds of being offline
        let t2 = t1 + Duration::seconds(10);
        tracker.record_status_change(t2, false);

        // Total tracked = 100 seconds
        // Uptime = 90 seconds
        // Uptime should be 90%
        let uptime = tracker.uptime_percentage();
        assert!((uptime - 90.0).abs() < 1.0, "Expected ~90%, got {}", uptime);
    }

    #[test]
    fn test_alert_severity() {
        let node_id = NodeId::new();

        let critical_offline = Alert::NodeOffline {
            node_id,
            last_seen: Utc::now(),
            sla_tier: SlaTier::Critical,
        };
        assert_eq!(critical_offline.severity(), AlertSeverity::Critical);

        let standard_offline = Alert::NodeOffline {
            node_id,
            last_seen: Utc::now(),
            sla_tier: SlaTier::Standard,
        };
        assert_eq!(standard_offline.severity(), AlertSeverity::Warning);
    }
}
