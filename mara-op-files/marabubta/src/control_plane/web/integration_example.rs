// Marabunta - Licensed under the MIT License.
//! Example integration of the web dashboard into the coordinator
//!
//! This file demonstrates how to integrate the web dashboard and WebSocket
//! endpoints into your Marabunta Compute coordinator.

use axum::Router;
use std::sync::Arc;

use crate::control_plane::web::create_dashboard_router;
use crate::control_plane::websocket::WebSocketManager;
use crate::coordinator::api::{create_router as create_api_router, ApiState};

/// Example: Create a complete coordinator router with dashboard and WebSocket support
///
/// # Usage in coordinator binary
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use marabunta_compute::coordinator::state::ClusterState;
/// use marabunta_compute::coordinator::api::ApiState;
/// use marabunta_compute::control_plane::web::create_dashboard_router;
/// use marabunta_compute::control_plane::websocket::WebSocketManager;
/// use axum::Router;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Initialize cluster state
///     let cluster_state = Arc::new(ClusterState::new());
///
///     // Create API state
///     let api_state = Arc::new(ApiState {
///         cluster: cluster_state.clone(),
///     });
///
///     // Create WebSocket manager
///     let ws_manager = WebSocketManager::new(1000);
///
///     // Build complete router
///     let app = Router::new()
///         // API routes
///         .merge(create_api_router(api_state))
///         // Web dashboard (serves HTML at /dashboard)
///         .merge(create_dashboard_router())
///         // WebSocket endpoint (at /ws/dashboard)
///         .merge(ws_manager.router());
///
///     // Start server
///     let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
///     println!("Coordinator running at http://localhost:8080");
///     println!("Dashboard available at http://localhost:8080/dashboard");
///     axum::serve(listener, app).await?;
///
///     Ok(())
/// }
/// ```

pub fn create_complete_coordinator_router(
    api_state: Arc<ApiState>,
    ws_manager: WebSocketManager,
) -> Router {
    Router::new()
        // Existing API routes
        .merge(create_api_router(api_state))
        // Web dashboard at /dashboard
        .merge(create_dashboard_router())
        // WebSocket endpoint at /ws/dashboard
        .merge(ws_manager.router())
}

/// Example: Broadcasting events to the dashboard
///
/// # Usage in your coordinator code
///
/// ```rust,no_run
/// use marabunta_compute::control_plane::websocket::{DashboardEvent, broadcast_event};
/// use chrono::Utc;
///
/// // When a job is submitted
/// async fn on_job_submitted(job_id: &str, total_tasks: u64) {
///     broadcast_event(DashboardEvent::JobProgress {
///         job_id: job_id.to_string(),
///         completed: 0,
///         total: total_tasks,
///         throughput: 0.0,
///         timestamp: Utc::now(),
///     });
/// }
///
/// // When a node status changes
/// async fn on_node_status_changed(node_id: &str, old: &str, new: &str) {
///     broadcast_event(DashboardEvent::NodeStatusChanged {
///         node_id: node_id.to_string(),
///         old_status: old.to_string(),
///         new_status: new.to_string(),
///         timestamp: Utc::now(),
///     });
/// }
///
/// // When creating an alert
/// async fn on_alert_created(alert: marabunta_compute::control_plane::websocket::Alert) {
///     broadcast_event(DashboardEvent::AlertCreated {
///         alert,
///         timestamp: Utc::now(),
///     });
/// }
/// ```
pub fn example_broadcast_usage() {
    // This is just a documentation function
    // See the function docs for actual usage examples
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::coordinator::state::ClusterState;
    use crate::coordinator::master_connection::MasterRegistry;
    use crate::coordinator::persistence::MemoryPersistenceBackend;

    #[tokio::test]
    async fn test_complete_router_creation() {
        let cluster_state = Arc::new(ClusterState::<MemoryPersistenceBackend>::new());
        let master_registry = Arc::new(MasterRegistry::new(Duration::from_secs(30)));
        let api_state = Arc::new(ApiState {
            cluster: cluster_state,
            master_registry,
        });
        let ws_manager = WebSocketManager::new(100, None);

        let _router = create_complete_coordinator_router(api_state, ws_manager);
        // Router creation should succeed
    }
}
