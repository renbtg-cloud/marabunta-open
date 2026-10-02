// Marabunta - Licensed under the MIT License.
//! WebSocket real-time streaming for the Marabunta Compute control plane
//!
//! This module provides real-time event streaming to dashboards and monitoring clients
//! using WebSocket connections. Events are broadcast to all connected clients with
//! support for subscription filtering.
//!
//! # Features
//!
//! - WebSocket endpoint at `/ws/dashboard`
//! - Token-based authentication via query parameters
//! - Heartbeat/ping-pong keepalive mechanism
//! - Event filtering and subscriptions
//! - Broadcast channel for efficient multi-client updates
//! - Connection tracking and metrics
//!
//! # Example Client Usage
//!
//! ```javascript
//! // Connect to the WebSocket endpoint with authentication
//! const ws = new WebSocket('ws://localhost:8080/ws/dashboard?token=your-auth-token');
//!
//! ws.onopen = () => {
//!   console.log('Connected to Marabunta Compute dashboard');
//!
//!   // Subscribe to specific job updates
//!   ws.send(JSON.stringify({
//!     type: 'subscribe',
//!     filter: {
//!       jobs: ['job-12345678'],
//!       regions: ['region-abcd1234']
//!     }
//!   }));
//! };
//!
//! ws.onmessage = (event) => {
//!   const data = JSON.parse(event.data);
//!
//!   switch(data.type) {
//!     case 'node_status_changed':
//!       console.log(`Node ${data.node_id} changed from ${data.old_status} to ${data.new_status}`);
//!       break;
//!     case 'job_progress':
//!       console.log(`Job ${data.job_id}: ${data.completed}/${data.total} (${data.throughput} units/sec)`);
//!       break;
//!     case 'alert_created':
//!       console.warn('New alert:', data.alert);
//!       break;
//!   }
//! };
//!
//! // Unsubscribe from job updates
//! ws.send(JSON.stringify({
//!   type: 'unsubscribe',
//!   filter: { jobs: ['job-12345678'] }
//! }));
//! ```
//!
//! # Server Usage
//!
//! ```rust,no_run
//! use marabunta_compute::control_plane::websocket::{
//!     WebSocketManager, DashboardEvent, broadcast_event
//! };
//! use axum::{Router, routing::get};
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create WebSocket manager
//!     let ws_manager = WebSocketManager::new(1000);
//!
//!     // Build router with WebSocket endpoint
//!     let app = Router::new()
//!         .route("/ws/dashboard", get(ws_manager.handler()));
//!
//!     // In your business logic, broadcast events
//!     broadcast_event(DashboardEvent::NodeStatusChanged {
//!         node_id: "worker-abc123".to_string(),
//!         old_status: "ready".to_string(),
//!         new_status: "busy".to_string(),
//!     }).await;
//! }
//! ```

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::coordinator::dual_mode::dashboard::Alert;

/// Maximum number of queued events per broadcast channel
const BROADCAST_CAPACITY: usize = 1000;

/// Heartbeat interval for ping/pong keepalive
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Generate a cryptographically random WebSocket authentication token.
fn generate_ws_auth_token() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 32] = rng.gen();
    format!("hws_{}", hex::encode(bytes))
}

/// Maximum time without pong response before disconnecting
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

/// Events that can be streamed to dashboard clients
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DashboardEvent {
    /// Node status has changed
    NodeStatusChanged {
        node_id: String,
        old_status: String,
        new_status: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },

    /// Node metrics update
    NodeMetricsUpdate {
        node_id: String,
        metrics: NodeMetrics,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },

    /// Job progress update
    JobProgress {
        job_id: String,
        completed: u64,
        total: u64,
        throughput: f64,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },

    /// New alert created
    AlertCreated {
        alert: Alert,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },

    /// Alert dismissed
    AlertDismissed {
        alert_id: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },

    /// Region capacity changed
    RegionCapacityChanged {
        region_id: String,
        old_flops: f64,
        new_flops: f64,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },

    /// Work unit completed successfully
    WorkUnitCompleted {
        job_id: String,
        node_id: String,
        duration_ms: u64,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },

    /// Work unit failed
    WorkUnitFailed {
        job_id: String,
        node_id: String,
        error: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },

    /// Heartbeat/keepalive event
    Heartbeat {
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
}

/// Node metrics for dashboard display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_usage: f64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub active_tasks: u32,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
}

// Alert and AlertSeverity are re-exported from coordinator::dual_mode::dashboard

/// Client subscription filter
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubscriptionFilter {
    /// Subscribe to specific jobs (empty = all jobs)
    #[serde(default)]
    pub jobs: Vec<String>,

    /// Subscribe to specific regions (empty = all regions)
    #[serde(default)]
    pub regions: Vec<String>,

    /// Subscribe to specific nodes (empty = all nodes)
    #[serde(default)]
    pub nodes: Vec<String>,

    /// Subscribe to all events (overrides other filters)
    #[serde(default)]
    pub all: bool,
}

impl SubscriptionFilter {
    /// Check if an event passes this filter
    pub fn matches(&self, event: &DashboardEvent) -> bool {
        // If subscribed to all, accept everything
        if self.all {
            return true;
        }

        // If no filters set, accept everything
        if self.jobs.is_empty() && self.regions.is_empty() && self.nodes.is_empty() {
            return true;
        }

        // Check event-specific filters
        match event {
            DashboardEvent::JobProgress { job_id, .. }
            | DashboardEvent::WorkUnitCompleted { job_id, .. }
            | DashboardEvent::WorkUnitFailed { job_id, .. } => {
                self.jobs.is_empty() || self.jobs.contains(job_id)
            }

            DashboardEvent::NodeStatusChanged { node_id, .. }
            | DashboardEvent::NodeMetricsUpdate { node_id, .. } => {
                self.nodes.is_empty() || self.nodes.contains(node_id)
            }

            DashboardEvent::RegionCapacityChanged { region_id, .. } => {
                self.regions.is_empty() || self.regions.contains(region_id)
            }

            // Alerts always pass through
            DashboardEvent::AlertCreated { .. } | DashboardEvent::AlertDismissed { .. } => true,

            // Heartbeats always pass through
            DashboardEvent::Heartbeat { .. } => true,
        }
    }

    /// Merge another filter into this one
    pub fn merge(&mut self, other: &SubscriptionFilter) {
        if other.all {
            self.all = true;
            return;
        }

        for job in &other.jobs {
            if !self.jobs.contains(job) {
                self.jobs.push(job.clone());
            }
        }

        for region in &other.regions {
            if !self.regions.contains(region) {
                self.regions.push(region.clone());
            }
        }

        for node in &other.nodes {
            if !self.nodes.contains(node) {
                self.nodes.push(node.clone());
            }
        }
    }

    /// Remove items from another filter
    pub fn remove(&mut self, other: &SubscriptionFilter) {
        if other.all {
            self.all = false;
            self.jobs.clear();
            self.regions.clear();
            self.nodes.clear();
            return;
        }

        self.jobs.retain(|j| !other.jobs.contains(j));
        self.regions.retain(|r| !other.regions.contains(r));
        self.nodes.retain(|n| !other.nodes.contains(n));
    }
}

/// Client messages sent to the server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Subscribe to events matching filter
    Subscribe { filter: SubscriptionFilter },

    /// Unsubscribe from events matching filter
    Unsubscribe { filter: SubscriptionFilter },

    /// Pong response to server ping
    Pong { timestamp: DateTime<Utc> },
}

/// Query parameters for WebSocket authentication
#[derive(Debug, Deserialize)]
pub struct WsAuthQuery {
    token: String,
}

/// Connection information for tracking
#[derive(Debug, Clone)]
struct ConnectionInfo {
    #[allow(dead_code)]
    id: Uuid,
    #[allow(dead_code)]
    connected_at: DateTime<Utc>,
    last_activity: DateTime<Utc>,
    filter: SubscriptionFilter,
    messages_sent: u64,
    messages_received: u64,
}

/// WebSocket connection manager
#[derive(Clone)]
pub struct WebSocketManager {
    /// Broadcast channel for events
    event_tx: broadcast::Sender<DashboardEvent>,

    /// Active connections
    connections: Arc<DashMap<Uuid, ConnectionInfo>>,

    /// Total connections counter
    total_connections: Arc<AtomicU64>,

    /// Current active connections
    active_connections: Arc<AtomicUsize>,

    /// Messages sent counter
    messages_sent: Arc<AtomicU64>,

    /// Messages received counter
    messages_received: Arc<AtomicU64>,

    /// Authentication token (simple implementation)
    auth_token: Arc<RwLock<String>>,
}

/// WebSocket handler function for axum
async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsAuthQuery>,
    State(manager): State<WebSocketManager>,
) -> impl IntoResponse {
    // Authenticate
    let token = manager.auth_token.read().await;
    if query.token != *token {
        warn!("WebSocket authentication failed");
        return axum::http::StatusCode::UNAUTHORIZED.into_response();
    }
    drop(token);

    info!("WebSocket authentication successful");

    // Upgrade to WebSocket
    let manager_clone = manager.clone();
    ws.on_upgrade(move |socket| manager_clone.handle_socket(socket))
        .into_response()
}

impl WebSocketManager {
    /// Create a new WebSocket manager
    pub fn new(broadcast_capacity: usize, auth_token: Option<String>) -> Self {
        let token = match auth_token {
            Some(t) => {
                tracing::info!("WebSocket auth token provided via configuration");
                t
            }
            None => {
                let generated = generate_ws_auth_token();
                tracing::warn!(
                    "No WebSocket auth token configured — generated ephemeral token. \
                     Set `websocket_auth_token` in config for persistent access."
                );
                eprintln!("[SECURITY] Generated WebSocket auth token: {generated}");
                generated
            }
        };

        let (event_tx, _) = broadcast::channel(broadcast_capacity);

        Self {
            event_tx,
            connections: Arc::new(DashMap::new()),
            total_connections: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicUsize::new(0)),
            messages_sent: Arc::new(AtomicU64::new(0)),
            messages_received: Arc::new(AtomicU64::new(0)),
            auth_token: Arc::new(RwLock::new(token)),
        }
    }

    /// Set the authentication token
    pub async fn set_auth_token(&self, token: String) {
        *self.auth_token.write().await = token;
    }

    /// Broadcast an event to all connected clients
    pub fn broadcast(&self, event: DashboardEvent) {
        match self.event_tx.send(event.clone()) {
            Ok(receiver_count) => {
                debug!(
                    "Broadcast event to {} receivers: {:?}",
                    receiver_count, event
                );
            }
            Err(_) => {
                // No receivers, that's okay
                debug!("No receivers for event: {:?}", event);
            }
        }
    }

    /// Get current metrics
    pub fn metrics(&self) -> WebSocketMetrics {
        WebSocketMetrics {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
        }
    }

    /// Get all connection info (internal use)
    #[allow(dead_code)]
    fn connections(&self) -> Vec<ConnectionInfo> {
        self.connections
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Create an Axum router with the WebSocket endpoint
    pub fn router(self) -> Router {
        Router::new()
            .route("/ws/dashboard", get(ws_handler))
            .with_state(self)
    }

    /// Handle a WebSocket connection
    async fn handle_socket(self, socket: WebSocket) -> () {
        let conn_id = Uuid::new_v4();
        let now = Utc::now();

        // Create connection info
        let conn_info = ConnectionInfo {
            id: conn_id,
            connected_at: now,
            last_activity: now,
            filter: SubscriptionFilter::default(),
            messages_sent: 0,
            messages_received: 0,
        };

        self.connections.insert(conn_id, conn_info);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);

        info!("WebSocket client connected: {}", conn_id);

        // Split socket into sender and receiver
        let (sender, receiver) = socket.split();

        // Create broadcast receiver for this connection
        let event_rx = self.event_tx.subscribe();

        // Spawn task to handle incoming messages
        let recv_handle = tokio::spawn(Self::handle_client_messages(
            self.clone(),
            conn_id,
            receiver,
        ));

        // Spawn task to send events to client
        let send_handle = tokio::spawn(Self::handle_event_stream(
            self.clone(),
            conn_id,
            sender,
            event_rx,
        ));

        // Wait for either task to complete (disconnect)
        tokio::select! {
            _ = recv_handle => {
                debug!("Receiver task completed for {}", conn_id);
            }
            _ = send_handle => {
                debug!("Sender task completed for {}", conn_id);
            }
        }

        // Cleanup
        self.connections.remove(&conn_id);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        info!("WebSocket client disconnected: {}", conn_id);
    }

    /// Handle incoming client messages
    async fn handle_client_messages(
        manager: WebSocketManager,
        conn_id: Uuid,
        mut receiver: SplitStream<WebSocket>,
    ) {
        while let Some(msg_result) = receiver.next().await {
            match msg_result {
                Ok(msg) => {
                    manager.messages_received.fetch_add(1, Ordering::Relaxed);

                    // Update last activity
                    if let Some(mut conn) = manager.connections.get_mut(&conn_id) {
                        conn.last_activity = Utc::now();
                        conn.messages_received += 1;
                    }

                    // Process message
                    match msg {
                        Message::Text(text) => {
                            debug!("Received text message from {}: {}", conn_id, text);

                            match serde_json::from_str::<ClientMessage>(&text) {
                                Ok(client_msg) => {
                                    manager.handle_client_message(conn_id, client_msg).await;
                                }
                                Err(e) => {
                                    warn!("Failed to parse client message: {}", e);
                                }
                            }
                        }
                        Message::Close(_) => {
                            info!("Client {} requested close", conn_id);
                            break;
                        }
                        Message::Pong(_) => {
                            debug!("Received pong from {}", conn_id);
                        }
                        _ => {
                            debug!("Received other message type from {}", conn_id);
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket error for {}: {}", conn_id, e);
                    break;
                }
            }
        }

        debug!("Client message handler exiting for {}", conn_id);
    }

    /// Handle client control messages
    async fn handle_client_message(&self, conn_id: Uuid, msg: ClientMessage) {
        match msg {
            ClientMessage::Subscribe { filter } => {
                info!("Client {} subscribing to: {:?}", conn_id, filter);

                if let Some(mut conn) = self.connections.get_mut(&conn_id) {
                    conn.filter.merge(&filter);
                }
            }

            ClientMessage::Unsubscribe { filter } => {
                info!("Client {} unsubscribing from: {:?}", conn_id, filter);

                if let Some(mut conn) = self.connections.get_mut(&conn_id) {
                    conn.filter.remove(&filter);
                }
            }

            ClientMessage::Pong { timestamp } => {
                debug!("Received pong from {} at {}", conn_id, timestamp);
            }
        }
    }

    /// Handle event stream to client
    async fn handle_event_stream(
        manager: WebSocketManager,
        conn_id: Uuid,
        mut sender: SplitSink<WebSocket, Message>,
        mut event_rx: broadcast::Receiver<DashboardEvent>,
    ) {
        let mut heartbeat = interval(HEARTBEAT_INTERVAL);
        
        // 🛑 WEBSOCKET TSUNAMI FIX: Throttle high-frequency events per client
        let mut high_freq_events_this_second = 0;
        let mut last_second = Utc::now().timestamp();

        loop {
            tokio::select! {
                // Send heartbeat
                _ = heartbeat.tick() => {
                    let ping_event = DashboardEvent::Heartbeat {
                        timestamp: Utc::now(),
                    };

                    if let Err(e) = manager.send_event(&mut sender, &ping_event).await {
                        error!("Failed to send heartbeat to {}: {}", conn_id, e);
                        break;
                    }

                    // Check for timeout
                    if let Some(conn) = manager.connections.get(&conn_id) {
                        let idle_duration = Utc::now().signed_duration_since(conn.last_activity);
                        if idle_duration.to_std().unwrap_or_default() > CLIENT_TIMEOUT {
                            warn!("Client {} timed out", conn_id);
                            break;
                        }
                    }
                }

                // Receive and forward events
                event = event_rx.recv() => {
                    match event {
                        Ok(event) => {
                            let current_second = Utc::now().timestamp();
                            if current_second > last_second {
                                high_freq_events_this_second = 0;
                                last_second = current_second;
                            }

                            // Identify if this is a high-frequency, low-priority event
                            let is_high_freq = matches!(
                                event,
                                DashboardEvent::NodeStatusChanged { .. } | DashboardEvent::NodeMetricsUpdate { .. }
                            );

                            // Drop excessive high-frequency events to prevent UI crash
                            if is_high_freq && high_freq_events_this_second > 50 {
                                continue;
                            }

                            // Check filter
                            let should_send = if let Some(conn) = manager.connections.get(&conn_id) {
                                conn.filter.matches(&event)
                            } else {
                                false
                            };

                            if should_send {
                                if let Err(e) = manager.send_event(&mut sender, &event).await {
                                    error!("Failed to send event to {}: {}", conn_id, e);
                                    break;
                                }

                                if is_high_freq {
                                    high_freq_events_this_second += 1;
                                }

                                // Update metrics
                                if let Some(mut conn) = manager.connections.get_mut(&conn_id) {
                                    conn.messages_sent += 1;
                                }
                                manager.messages_sent.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!("Client {} lagged, skipped {} events", conn_id, skipped);
                            // Continue receiving
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("Event broadcast channel closed");
                            break;
                        }
                    }
                }
            }
        }

        debug!("Event stream handler exiting for {}", conn_id);
    }

    /// Send an event to a client
    async fn send_event(
        &self,
        sender: &mut SplitSink<WebSocket, Message>,
        event: &DashboardEvent,
    ) -> Result<(), axum::Error> {
        let json = serde_json::to_string(event)
            .map_err(|e| axum::Error::new(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        sender
            .send(Message::Text(json))
            .await
            .map_err(|e| axum::Error::new(std::io::Error::new(std::io::ErrorKind::Other, e)))
    }

    /// Gracefully shutdown all connections
    pub async fn shutdown(&self) {
        info!("Shutting down WebSocket manager");

        // Get all connection IDs
        let conn_ids: Vec<Uuid> = self.connections.iter().map(|e| *e.key()).collect();

        // We can't force close from here, but we can log
        info!("Waiting for {} connections to close", conn_ids.len());

        // In a real implementation, you might want to send a close message
        // or wait for connections to naturally close
    }
}

/// WebSocket metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketMetrics {
    pub active_connections: usize,
    pub total_connections: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
}

/// Global WebSocket manager instance
static GLOBAL_WS_MANAGER: once_cell::sync::OnceCell<WebSocketManager> =
    once_cell::sync::OnceCell::new();

/// Initialize the global WebSocket manager
pub fn init_websocket_manager(auth_token: Option<String>) -> WebSocketManager {
    GLOBAL_WS_MANAGER
        .get_or_init(|| WebSocketManager::new(BROADCAST_CAPACITY, auth_token))
        .clone()
}

/// Get the global WebSocket manager
pub fn get_websocket_manager() -> Option<WebSocketManager> {
    GLOBAL_WS_MANAGER.get().cloned()
}

/// Broadcast an event to all connected clients
pub fn broadcast_event(event: DashboardEvent) {
    if let Some(manager) = get_websocket_manager() {
        manager.broadcast(event);
    } else {
        debug!("WebSocket manager not initialized, event not broadcast");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::dual_mode::dashboard::AlertSeverity;

    #[test]
    fn test_subscription_filter_matches_all() {
        let filter = SubscriptionFilter {
            all: true,
            ..Default::default()
        };

        let event = DashboardEvent::JobProgress {
            job_id: "job-123".to_string(),
            completed: 50,
            total: 100,
            throughput: 10.0,
            timestamp: Utc::now(),
        };

        assert!(filter.matches(&event));
    }

    #[test]
    fn test_subscription_filter_matches_job() {
        let filter = SubscriptionFilter {
            jobs: vec!["job-123".to_string()],
            ..Default::default()
        };

        let event1 = DashboardEvent::JobProgress {
            job_id: "job-123".to_string(),
            completed: 50,
            total: 100,
            throughput: 10.0,
            timestamp: Utc::now(),
        };

        let event2 = DashboardEvent::JobProgress {
            job_id: "job-456".to_string(),
            completed: 50,
            total: 100,
            throughput: 10.0,
            timestamp: Utc::now(),
        };

        assert!(filter.matches(&event1));
        assert!(!filter.matches(&event2));
    }

    #[test]
    fn test_subscription_filter_alerts_always_pass() {
        let filter = SubscriptionFilter {
            jobs: vec!["job-123".to_string()],
            ..Default::default()
        };

        let event = DashboardEvent::AlertCreated {
            alert: Alert {
                id: "alert-1".to_string(),
                severity: AlertSeverity::Warning,
                node_id: None,
                message: "This is a test alert".to_string(),
                raised_at: Utc::now(),
                acknowledged: false,
            },
            timestamp: Utc::now(),
        };

        assert!(filter.matches(&event));
    }

    #[test]
    fn test_subscription_filter_merge() {
        let mut filter1 = SubscriptionFilter {
            jobs: vec!["job-123".to_string()],
            regions: vec!["region-1".to_string()],
            ..Default::default()
        };

        let filter2 = SubscriptionFilter {
            jobs: vec!["job-456".to_string()],
            nodes: vec!["node-1".to_string()],
            ..Default::default()
        };

        filter1.merge(&filter2);

        assert_eq!(filter1.jobs.len(), 2);
        assert_eq!(filter1.regions.len(), 1);
        assert_eq!(filter1.nodes.len(), 1);
        assert!(filter1.jobs.contains(&"job-123".to_string()));
        assert!(filter1.jobs.contains(&"job-456".to_string()));
    }

    #[test]
    fn test_subscription_filter_remove() {
        let mut filter1 = SubscriptionFilter {
            jobs: vec!["job-123".to_string(), "job-456".to_string()],
            regions: vec!["region-1".to_string()],
            ..Default::default()
        };

        let filter2 = SubscriptionFilter {
            jobs: vec!["job-456".to_string()],
            ..Default::default()
        };

        filter1.remove(&filter2);

        assert_eq!(filter1.jobs.len(), 1);
        assert!(filter1.jobs.contains(&"job-123".to_string()));
        assert!(!filter1.jobs.contains(&"job-456".to_string()));
    }

    #[tokio::test]
    async fn test_websocket_manager_creation() {
        let manager = WebSocketManager::new(100, None);
        let metrics = manager.metrics();

        assert_eq!(metrics.active_connections, 0);
        assert_eq!(metrics.total_connections, 0);
        assert_eq!(metrics.messages_sent, 0);
        assert_eq!(metrics.messages_received, 0);
    }

    #[tokio::test]
    async fn test_default_token_not_hardcoded() {
        let manager = WebSocketManager::new(100, None);
        let token = manager.auth_token.read().await;
        assert_ne!(*token, "default-token");
        assert!(token.starts_with("hws_"));
        assert!(token.len() >= 68); // "hws_" (4) + 64 hex chars
    }

    #[tokio::test]
    async fn test_explicit_token_accepted() {
        let manager = WebSocketManager::new(100, Some("my-secure-token-1234567890".to_string()));
        let token = manager.auth_token.read().await;
        assert_eq!(*token, "my-secure-token-1234567890");
    }

    #[tokio::test]
    async fn test_websocket_manager_broadcast() {
        let manager = WebSocketManager::new(100, None);

        let event = DashboardEvent::NodeStatusChanged {
            node_id: "node-1".to_string(),
            old_status: "ready".to_string(),
            new_status: "busy".to_string(),
            timestamp: Utc::now(),
        };

        // Broadcasting with no receivers should not panic
        manager.broadcast(event);
    }

    #[tokio::test]
    async fn test_websocket_manager_auth_token() {
        let manager = WebSocketManager::new(100, Some("test-token-12345678".to_string()));

        manager.set_auth_token("new-test-token-123456".to_string()).await;

        let token = manager.auth_token.read().await;
        assert_eq!(*token, "new-test-token-123456");
    }
}
