// Marabunta - Licensed under the MIT License.
//! Web Dashboard for Marabunta Compute Control Plane
//!
//! This module provides a self-contained HTML dashboard for monitoring
//! and managing the Marabunta Compute cluster. The dashboard is served as a
//! single-file HTML page with embedded JavaScript and CSS.
//!
//! # Quick Start
//!
//! Add the dashboard to your coordinator:
//!
//! ```rust,no_run
//! use axum::Router;
//! use marabunta_compute::control_plane::web::create_dashboard_router;
//! use marabunta_compute::control_plane::websocket::WebSocketManager;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create WebSocket manager for real-time updates
//!     let ws_manager = WebSocketManager::new(1000);
//!
//!     // Build router with dashboard and WebSocket
//!     let app = Router::new()
//!         .merge(create_dashboard_router())
//!         .merge(ws_manager.router());
//!
//!     // Serve on port 8080
//!     let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
//!     println!("Dashboard: http://localhost:8080/dashboard");
//!     axum::serve(listener, app).await.unwrap();
//! }
//! ```

pub mod integration_example;

use axum::{response::Html, routing::get, Router};

pub use integration_example::create_complete_coordinator_router;

/// The embedded dashboard HTML content
const DASHBOARD_HTML: &str = include_str!("dashboard.html");

/// Handler for the dashboard route
async fn dashboard_handler() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

/// Create the web dashboard router
///
/// This router provides:
/// - GET /dashboard - The main dashboard HTML page
///
/// # Example
///
/// ```rust,no_run
/// use axum::Router;
/// use marabunta_compute::control_plane::web::create_dashboard_router;
///
/// #[tokio::main]
/// async fn main() {
///     let app = Router::new()
///         .merge(create_dashboard_router());
///
///     let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
///     axum::serve(listener, app).await.unwrap();
/// }
/// ```
pub fn create_dashboard_router() -> Router {
    Router::new().route("/dashboard", get(dashboard_handler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // for oneshot

    #[tokio::test]
    async fn test_dashboard_route() {
        let app = create_dashboard_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("Marabunta Compute"));
        assert!(html.contains("dashboard"));
    }
}
