// Marabunta - Licensed under the MIT License.
//! Example integration of WebSocket streaming with Marabunta Compute control plane
//!
//! This example demonstrates how to set up a complete dashboard server with
//! WebSocket real-time event streaming.

use axum::{
    routing::get,
    Router,
};
use std::net::SocketAddr;
use tokio::time::{interval, Duration};
use tracing::{info, Level};
use tracing_subscriber;

use crate::control_plane::{
    init_websocket_manager, broadcast_event, DashboardEvent,
    NodeMetrics,
};
use crate::coordinator::dual_mode::dashboard::{Alert, AlertSeverity};

/// Example dashboard server with WebSocket support
#[allow(dead_code)]
pub async fn run_dashboard_server() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    // Initialize WebSocket manager with a generated token
    let ws_manager = init_websocket_manager(None);

    // Build router
    let app = Router::new()
        .route("/", get(dashboard_home))
        .route("/metrics", get(dashboard_metrics))
        .merge(ws_manager.router());

    // Spawn background task to generate demo events
    tokio::spawn(generate_demo_events());

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    info!("Dashboard server listening on {}", addr);
    info!("WebSocket endpoint: ws://{}/ws/dashboard?token=your-secure-token-here", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

/// Dashboard home page
async fn dashboard_home() -> &'static str {
    r#"
    <!DOCTYPE html>
    <html>
    <head>
        <title>Marabunta Compute Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            #events { border: 1px solid #ccc; padding: 10px; height: 400px; overflow-y: scroll; }
            .event { margin: 5px 0; padding: 5px; border-left: 3px solid #007bff; }
            .alert { border-left-color: #dc3545; }
            .status-change { border-left-color: #28a745; }
            .progress { border-left-color: #ffc107; }
        </style>
    </head>
    <body>
        <h1>Marabunta Compute Real-Time Dashboard</h1>
        <div>
            <h2>Connection Status: <span id="status">Disconnected</span></h2>
            <button onclick="connect()">Connect</button>
            <button onclick="disconnect()">Disconnect</button>
        </div>
        <div>
            <h2>Subscriptions</h2>
            <button onclick="subscribeAll()">Subscribe to All</button>
            <button onclick="subscribeJob('job-test-001')">Subscribe to Job test-001</button>
            <button onclick="unsubscribeAll()">Unsubscribe All</button>
        </div>
        <div>
            <h2>Events</h2>
            <div id="events"></div>
        </div>

        <script>
            let ws = null;

            function connect() {
                const token = 'your-secure-token-here';
                ws = new WebSocket(`ws://${window.location.host}/ws/dashboard?token=${token}`);

                ws.onopen = () => {
                    document.getElementById('status').textContent = 'Connected';
                    addEvent('Connected to dashboard', 'status-change');
                };

                ws.onclose = () => {
                    document.getElementById('status').textContent = 'Disconnected';
                    addEvent('Disconnected from dashboard', 'status-change');
                };

                ws.onerror = (error) => {
                    console.error('WebSocket error:', error);
                    addEvent('WebSocket error', 'alert');
                };

                ws.onmessage = (event) => {
                    const data = JSON.parse(event.data);
                    handleEvent(data);
                };
            }

            function disconnect() {
                if (ws) {
                    ws.close();
                    ws = null;
                }
            }

            function subscribeAll() {
                if (!ws) return;
                ws.send(JSON.stringify({
                    type: 'subscribe',
                    filter: { all: true }
                }));
                addEvent('Subscribed to all events', 'status-change');
            }

            function subscribeJob(jobId) {
                if (!ws) return;
                ws.send(JSON.stringify({
                    type: 'subscribe',
                    filter: { jobs: [jobId] }
                }));
                addEvent(`Subscribed to job ${jobId}`, 'status-change');
            }

            function unsubscribeAll() {
                if (!ws) return;
                ws.send(JSON.stringify({
                    type: 'unsubscribe',
                    filter: { all: true }
                }));
                addEvent('Unsubscribed from all events', 'status-change');
            }

            function handleEvent(data) {
                let message = '';
                let eventClass = '';

                switch(data.type) {
                    case 'node_status_changed':
                        message = `Node ${data.node_id}: ${data.old_status} → ${data.new_status}`;
                        eventClass = 'status-change';
                        break;
                    case 'node_metrics_update':
                        message = `Node ${data.node_id}: CPU ${data.metrics.cpu_usage.toFixed(1)}%, ` +
                                 `Mem ${data.metrics.memory_usage.toFixed(1)}%`;
                        eventClass = 'event';
                        break;
                    case 'job_progress':
                        message = `Job ${data.job_id}: ${data.completed}/${data.total} ` +
                                 `(${data.throughput.toFixed(2)} units/sec)`;
                        eventClass = 'progress';
                        break;
                    case 'alert_created':
                        message = `Alert [${data.alert.severity}]: ${data.alert.message}`;
                        eventClass = 'alert';
                        break;
                    case 'work_unit_completed':
                        message = `Work unit completed: Job ${data.job_id} on ${data.node_id} ` +
                                 `(${data.duration_ms}ms)`;
                        eventClass = 'event';
                        break;
                    case 'work_unit_failed':
                        message = `Work unit failed: Job ${data.job_id} on ${data.node_id} - ${data.error}`;
                        eventClass = 'alert';
                        break;
                    case 'heartbeat':
                        // Don't display heartbeats
                        return;
                    default:
                        message = JSON.stringify(data);
                        eventClass = 'event';
                }

                addEvent(message, eventClass);
            }

            function addEvent(message, className) {
                const eventsDiv = document.getElementById('events');
                const eventDiv = document.createElement('div');
                eventDiv.className = `event ${className}`;
                eventDiv.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
                eventsDiv.insertBefore(eventDiv, eventsDiv.firstChild);

                // Keep only last 100 events
                while (eventsDiv.children.length > 100) {
                    eventsDiv.removeChild(eventsDiv.lastChild);
                }
            }

            // Auto-connect on page load
            connect();
        </script>
    </body>
    </html>
    "#
}

/// Dashboard metrics endpoint
async fn dashboard_metrics() -> String {
    if let Some(manager) = crate::control_plane::get_websocket_manager() {
        let metrics = manager.metrics();
        format!(
            "Active Connections: {}\n\
             Total Connections: {}\n\
             Messages Sent: {}\n\
             Messages Received: {}\n",
            metrics.active_connections,
            metrics.total_connections,
            metrics.messages_sent,
            metrics.messages_received
        )
    } else {
        "WebSocket manager not initialized".to_string()
    }
}

/// Generate demo events for testing
async fn generate_demo_events() {
    let mut interval = interval(Duration::from_secs(5));
    let mut counter = 0u64;

    loop {
        interval.tick().await;
        counter += 1;

        // Cycle through different event types
        match counter % 6 {
            0 => {
                // Node status change
                broadcast_event(DashboardEvent::NodeStatusChanged {
                    node_id: format!("worker-{}", counter % 10),
                    old_status: "ready".to_string(),
                    new_status: "busy".to_string(),
                    timestamp: chrono::Utc::now(),
                });
            }
            1 => {
                // Node metrics
                broadcast_event(DashboardEvent::NodeMetricsUpdate {
                    node_id: format!("worker-{}", counter % 10),
                    metrics: NodeMetrics {
                        cpu_usage: 45.0 + (counter as f64 % 50.0),
                        memory_usage: 60.0 + (counter as f64 % 30.0),
                        disk_usage: 30.0,
                        network_rx_bytes: counter * 1024,
                        network_tx_bytes: counter * 512,
                        active_tasks: 3,
                        completed_tasks: counter * 10,
                        failed_tasks: counter / 100,
                    },
                    timestamp: chrono::Utc::now(),
                });
            }
            2 => {
                // Job progress
                broadcast_event(DashboardEvent::JobProgress {
                    job_id: "job-test-001".to_string(),
                    completed: counter,
                    total: 1000,
                    throughput: 10.5,
                    timestamp: chrono::Utc::now(),
                });
            }
            3 => {
                // Work unit completed
                broadcast_event(DashboardEvent::WorkUnitCompleted {
                    job_id: "job-test-001".to_string(),
                    node_id: format!("worker-{}", counter % 10),
                    duration_ms: 150 + (counter % 100),
                    timestamp: chrono::Utc::now(),
                });
            }
            4 => {
                // Alert (less frequent)
                if counter % 12 == 0 {
                    use crate::infrastructure::NodeId;
                    broadcast_event(DashboardEvent::AlertCreated {
                        alert: Alert {
                            id: format!("alert-{}", counter),
                            severity: if counter % 24 == 0 {
                                AlertSeverity::Critical
                            } else {
                                AlertSeverity::Warning
                            },
                            node_id: Some(NodeId::new()),
                            message: format!("Worker-{} CPU usage exceeded 90%", counter % 10),
                            raised_at: chrono::Utc::now(),
                            acknowledged: false,
                        },
                        timestamp: chrono::Utc::now(),
                    });
                }
            }
            _ => {
                // Region capacity changed
                broadcast_event(DashboardEvent::RegionCapacityChanged {
                    region_id: "region-us-west".to_string(),
                    old_flops: 1000.0,
                    new_flops: 1100.0 + (counter as f64 % 200.0),
                    timestamp: chrono::Utc::now(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dashboard_metrics() {
        init_websocket_manager(None);
        let metrics = dashboard_metrics().await;
        assert!(metrics.contains("Active Connections"));
    }
}
