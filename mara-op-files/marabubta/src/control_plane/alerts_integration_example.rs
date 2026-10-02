// Marabunta - Licensed under the MIT License.
//! Example integration of the Alert Detection System
//!
//! This module demonstrates how to integrate the alert detection system
//! with the Marabunta Compute control plane and WebSocket broadcasting.

use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::info;

use crate::common::{JobId, RegionId, WorkerId};
use crate::control_plane::alerts::{
    AlertDetector, AlertDetectorConfig, AlertEvent, AlertStore, SchedulePattern,
};
use crate::control_plane::websocket::{broadcast_event, DashboardEvent, Alert as WsAlert, AlertSeverity};

/// Initialize the alert detection system
pub async fn init_alert_system() -> (Arc<AlertStore>, Arc<AlertDetector>) {
    // Create alert store with max 1000 historical alerts
    let (store, mut event_rx) = AlertStore::new(1000);
    let store = Arc::new(store);

    // Create detector with custom configuration
    let config = AlertDetectorConfig {
        check_interval_secs: 30,
        node_idle_threshold_mins: 30,
        latency_threshold_ms: 500,
        failure_rate_threshold: 0.25,
        battery_threshold_percent: 20,
        capacity_drop_threshold: 0.20,
        job_stalled_threshold_mins: 60,
        cluster_shutdown_warning_mins: 30,
    };

    let detector = Arc::new(AlertDetector::new(config, store.clone()));

    // Add custom schedule patterns
    detector.add_schedule_pattern(SchedulePattern::school_hours());
    detector.add_schedule_pattern(SchedulePattern::factory_shifts());

    // Spawn background task to forward alert events to WebSocket
    let store_clone = store.clone();
    tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            forward_alert_to_websocket(event).await;
        }
    });

    // Start the detection loop
    let detector_clone = detector.clone();
    tokio::spawn(async move {
        detector_clone.run().await;
    });

    info!("Alert detection system initialized");

    (store, detector)
}

/// Forward alert events to WebSocket for dashboard updates
async fn forward_alert_to_websocket(event: AlertEvent) {
    match event {
        AlertEvent::AlertCreated { alert } => {
            // Convert our detailed alert to the simpler WebSocket alert format
            let ws_alert = WsAlert {
                id: alert.id.to_string(),
                severity: match alert.severity {
                    crate::control_plane::alerts::Severity::Info => AlertSeverity::Info,
                    crate::control_plane::alerts::Severity::Warning => AlertSeverity::Warning,
                    crate::control_plane::alerts::Severity::Error => AlertSeverity::Error,
                    crate::control_plane::alerts::Severity::Critical => AlertSeverity::Critical,
                },
                title: format!("{:?}", alert.condition),
                message: format_alert_message(&alert.condition),
                source: alert.rule_id.clone(),
                created_at: alert.created_at,
            };

            broadcast_event(DashboardEvent::AlertCreated {
                alert: ws_alert,
                timestamp: alert.created_at,
            })
            .await;
        }
        AlertEvent::AlertDismissed { alert_id, .. } => {
            broadcast_event(DashboardEvent::AlertDismissed {
                alert_id: alert_id.to_string(),
                timestamp: chrono::Utc::now(),
            })
            .await;
        }
        AlertEvent::AlertUpdated { .. } => {
            // Could broadcast an update event if needed
        }
        AlertEvent::ActionTaken { .. } => {
            // Could broadcast action results if needed
        }
    }
}

/// Format alert condition into a human-readable message
fn format_alert_message(condition: &crate::control_plane::alerts::AlertCondition) -> String {
    use crate::control_plane::alerts::AlertCondition;

    match condition {
        AlertCondition::ClusterGoingOffline {
            cluster_name,
            shutdown_in_mins,
            affected_nodes,
            ..
        } => {
            format!(
                "Cluster '{}' will shutdown in {} minutes, affecting {} nodes",
                cluster_name,
                shutdown_in_mins,
                affected_nodes.len()
            )
        }
        AlertCondition::NodeIdleTooLong {
            worker_id,
            idle_duration_mins,
            ..
        } => {
            format!(
                "Node {} has been idle for {} minutes",
                worker_id, idle_duration_mins
            )
        }
        AlertCondition::LatencySpike {
            region_name,
            current_latency_ms,
            baseline_ms,
            ..
        } => {
            format!(
                "Region '{}' latency spiked to {}ms (baseline: {}ms)",
                region_name, current_latency_ms, baseline_ms
            )
        }
        AlertCondition::HighFailureRate {
            job_name,
            failure_rate,
            failed_tasks,
            total_tasks,
            ..
        } => {
            format!(
                "Job '{}' has {:.1}% failure rate ({}/{} tasks failed)",
                job_name,
                failure_rate * 100.0,
                failed_tasks,
                total_tasks
            )
        }
        AlertCondition::BatteryLow {
            worker_id,
            battery_percent,
            ..
        } => {
            format!(
                "Node {} battery critically low at {}%",
                worker_id, battery_percent
            )
        }
        AlertCondition::ThrottlingDetected {
            worker_id,
            throttle_type,
            severity,
        } => {
            format!(
                "Node {} experiencing {:?} throttling ({:?})",
                worker_id, throttle_type, severity
            )
        }
        AlertCondition::CapacityDrop {
            capacity_drop_percent,
            lost_workers,
            ..
        } => {
            format!(
                "Capacity dropped {:.1}% ({} nodes lost)",
                capacity_drop_percent,
                lost_workers.len()
            )
        }
        AlertCondition::JobStalled {
            job_name,
            stalled_duration_mins,
            ..
        } => {
            format!(
                "Job '{}' has been stalled for {} minutes",
                job_name, stalled_duration_mins
            )
        }
    }
}

/// Example: Integrate with worker heartbeat monitoring
pub async fn monitor_worker_heartbeats(
    detector: Arc<AlertDetector>,
    worker_id: WorkerId,
    is_idle: bool,
) {
    // Called from worker heartbeat handler
    detector.check_node_idle(worker_id, is_idle);
}

/// Example: Monitor job progress
pub async fn monitor_job_progress(
    detector: Arc<AlertDetector>,
    job_id: JobId,
    job_name: String,
    completed_tasks: u32,
    total_tasks: u32,
    failed_tasks: u32,
    has_recent_progress: bool,
) {
    // Check for stalled jobs
    detector.check_job_stalled(job_id, job_name.clone(), has_recent_progress);

    // Check for high failure rate
    detector.check_failure_rate(job_id, job_name, failed_tasks, total_tasks);
}

/// Example: Monitor region latency
pub async fn monitor_region_latency(
    detector: Arc<AlertDetector>,
    region_id: RegionId,
    region_name: String,
    latency_ms: u64,
) {
    detector.check_latency_spike(region_id, region_name, latency_ms);
}

/// Example: Monitor battery levels for mobile devices
pub async fn monitor_battery_status(
    detector: Arc<AlertDetector>,
    worker_id: WorkerId,
    battery_percent: u8,
    is_charging: bool,
) {
    detector.check_battery_low(worker_id, battery_percent, is_charging);
}

/// Example: Monitor cluster schedules
pub async fn monitor_cluster_schedules(
    detector: Arc<AlertDetector>,
    cluster_name: String,
    region_id: RegionId,
    workers: Vec<WorkerId>,
) {
    detector.check_cluster_offline(cluster_name, region_id, workers);
}

/// Example: Monitor capacity changes
pub async fn monitor_capacity_changes(
    detector: Arc<AlertDetector>,
    region_id: Option<RegionId>,
    current_capacity: f64,
    lost_workers: Vec<WorkerId>,
) {
    detector.check_capacity_drop(region_id, current_capacity, lost_workers);
}

/// Example: Background task that periodically checks all conditions
pub async fn run_periodic_checks(
    detector: Arc<AlertDetector>,
    store: Arc<AlertStore>,
) {
    let mut ticker = interval(Duration::from_secs(60));

    loop {
        ticker.tick().await;

        // Get current cluster state (would come from actual state management)
        // For demonstration, we show the structure

        // Example: Check all active workers
        // for worker in get_all_workers() {
        //     detector.check_node_idle(worker.id, worker.is_idle());
        //     if let Some(battery) = worker.battery_status() {
        //         detector.check_battery_low(worker.id, battery.percent, battery.charging);
        //     }
        // }

        // Example: Check all active jobs
        // for job in get_all_jobs() {
        //     let progress = job.has_recent_progress();
        //     detector.check_job_stalled(job.id, job.name.clone(), progress);
        //
        //     let (failed, total) = job.task_counts();
        //     detector.check_failure_rate(job.id, job.name.clone(), failed, total);
        // }

        // Example: Check all regions
        // for region in get_all_regions() {
        //     let latency = region.current_latency_ms();
        //     detector.check_latency_spike(region.id, region.name.clone(), latency);
        // }

        info!("Periodic alert checks completed");
    }
}

/// Example: REST API endpoint to get active alerts
pub async fn get_active_alerts_handler(
    store: Arc<AlertStore>,
) -> Result<serde_json::Value, String> {
    let alerts = store.get_active_alerts();

    Ok(serde_json::json!({
        "alerts": alerts,
        "count": alerts.len(),
    }))
}

/// Example: REST API endpoint to dismiss an alert
pub async fn dismiss_alert_handler(
    store: Arc<AlertStore>,
    alert_id: String,
    user: String,
) -> Result<serde_json::Value, String> {
    let alert_id = alert_id
        .parse()
        .map_err(|_| "Invalid alert ID".to_string())?;

    if store.dismiss_alert(alert_id, Some(user)) {
        Ok(serde_json::json!({
            "success": true,
            "message": "Alert dismissed"
        }))
    } else {
        Err("Alert not found".to_string())
    }
}

/// Example: REST API endpoint to get alert history
pub async fn get_alert_history_handler(
    store: Arc<AlertStore>,
    limit: Option<usize>,
) -> Result<serde_json::Value, String> {
    let history = store.get_history(limit.unwrap_or(100));

    Ok(serde_json::json!({
        "history": history,
        "count": history.len(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_alert_system_initialization() {
        let (store, detector) = init_alert_system().await;

        // Verify store is empty initially
        assert_eq!(store.get_active_alerts().len(), 0);

        // Verify detector has default rules
        let rules = detector.get_rules();
        assert!(!rules.is_empty());
    }

    #[tokio::test]
    async fn test_worker_monitoring() {
        let (store, detector) = init_alert_system().await;
        let worker_id = WorkerId::new();

        // Simulate idle worker
        detector.check_node_idle(worker_id, true);

        // Should not create alert immediately (below threshold)
        assert_eq!(store.get_active_alerts().len(), 0);
    }

    #[tokio::test]
    async fn test_job_monitoring() {
        let (store, detector) = init_alert_system().await;
        let job_id = JobId::new();

        // Simulate high failure rate
        detector.check_failure_rate(job_id, "test-job".to_string(), 30, 100);

        // Should create alert for >25% failure rate
        tokio::time::sleep(Duration::from_millis(100)).await;
        let alerts = store.get_active_alerts();
        assert!(!alerts.is_empty());
    }
}
