// Marabunta - Licensed under the MIT License.
//! Switchboard REST API
//!
//! Axum HTTP endpoints for mathematical expression processing:
//!
//! - `POST /api/switchboard/submit`      — Ingest, classify, and return an option menu
//! - `POST /api/switchboard/execute`     — Execute a selected handler
//! - `GET  /api/switchboard/session/:id` — Retrieve session state
//!
//! The router is constructed via [`create_switchboard_router`] which accepts a
//! shared [`SwitchboardApiState`].

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use super::config::SwitchboardConfig;
use super::errors::SwitchboardError;
use super::handlers::HandlerRegistry;
use super::pipeline::{self, ExecuteResponse, PipelineResponse};
use super::session::{SessionStore, SwitchboardSession};
use super::types::{HandlerId, InputFormat, OptionMenu, RawInput, SessionId};

// ============================================================================
// API State
// ============================================================================

/// Shared state for the switchboard API endpoints.
///
/// Wrapped in `Arc` when passed to the Axum router so all handlers share
/// the same registry, session store, and configuration.
pub struct SwitchboardApiState {
    /// Handler registry with all registered compute backends.
    pub registry: HandlerRegistry,
    /// Session store for tracking pipeline state.
    pub session_store: SessionStore,
    /// Switchboard configuration.
    pub config: SwitchboardConfig,
}

/// Wrapper that holds the state in an Arc and implements Clone for Axum.
///
/// Axum requires `Clone` on the state type passed to `with_state()`.
/// Since `SwitchboardApiState` contains non-Clone types, we wrap it in
/// `Arc` and clone only the pointer.
#[derive(Clone)]
pub struct SharedState(pub Arc<SwitchboardApiState>);

// ============================================================================
// Router
// ============================================================================

/// Creates the Axum router for the switchboard REST API.
///
/// # Endpoints
///
/// | Method | Path                              | Description                       |
/// |--------|-----------------------------------|-----------------------------------|
/// | POST   | `/api/switchboard/submit`         | Submit expression for processing  |
/// | POST   | `/api/switchboard/execute`        | Execute a selected handler        |
/// | GET    | `/api/switchboard/session/:id`    | Get session status                |
pub fn create_switchboard_router(state: Arc<SwitchboardApiState>) -> Router {
    let shared = SharedState(state);

    Router::new()
        .route("/api/switchboard/submit", post(submit_expression))
        .route("/api/switchboard/execute", post(execute_handler))
        .route("/api/switchboard/session/:id", get(get_session))
        .with_state(shared)
}

// ============================================================================
// Request / Response types
// ============================================================================

/// Request body for `POST /api/switchboard/submit`.
#[derive(Debug, Deserialize)]
pub struct SubmitRequest {
    /// Input format: `"plain_text"`, `"latex"`, `"mathml"`, `"ascii_math"`, or `"napkin_photo"`.
    pub format: InputFormat,
    /// The mathematical expression content.
    pub content: String,
    /// Optional metadata key-value pairs.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Response body for `POST /api/switchboard/submit`.
#[derive(Debug, Serialize)]
pub struct SubmitResponse {
    /// Session ID for follow-up requests.
    pub session_id: String,
    /// Classification confidence (0.0 - 1.0).
    pub confidence: f32,
    /// Option menu with ranked handler choices.
    pub menu: OptionMenu,
}

impl From<PipelineResponse> for SubmitResponse {
    fn from(resp: PipelineResponse) -> Self {
        Self {
            session_id: resp.session_id.to_string(),
            confidence: resp.confidence,
            menu: resp.menu,
        }
    }
}

/// Request body for `POST /api/switchboard/execute`.
#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    /// Session ID from a previous submit response.
    pub session_id: String,
    /// Handler ID chosen from the option menu.
    pub handler_id: String,
    /// Optional parameters for the handler.
    #[serde(default)]
    pub params: HashMap<String, String>,
}

/// Response body for `POST /api/switchboard/execute`.
#[derive(Debug, Serialize)]
pub struct ExecuteApiResponse {
    /// Session ID.
    pub session_id: String,
    /// Whether the computation succeeded.
    pub success: bool,
    /// Result in LaTeX format (if available).
    pub result_latex: Option<String>,
    /// Result in plain text format (if available).
    pub result_plain: Option<String>,
    /// Computation steps.
    pub steps: Vec<StepDto>,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Backend that produced the result.
    pub backend: String,
}

/// A single computation step in the response.
#[derive(Debug, Serialize)]
pub struct StepDto {
    pub step_number: u32,
    pub description: String,
    pub expression: String,
}

impl From<ExecuteResponse> for ExecuteApiResponse {
    fn from(resp: ExecuteResponse) -> Self {
        Self {
            session_id: resp.session_id.to_string(),
            success: resp.result.success,
            result_latex: resp.result.result_latex,
            result_plain: resp.result.result_plain,
            steps: resp
                .result
                .steps
                .into_iter()
                .map(|s| StepDto {
                    step_number: s.step_number,
                    description: s.description,
                    expression: s.expression,
                })
                .collect(),
            duration_ms: resp.result.duration_ms,
            backend: resp.result.backend,
        }
    }
}

/// Response body for `GET /api/switchboard/session/:id`.
#[derive(Debug, Serialize)]
pub struct SessionResponse {
    /// Session ID.
    pub session_id: String,
    /// Current session state.
    pub state: String,
    /// Number of expressions in this session.
    pub expression_count: usize,
    /// Number of compute results.
    pub result_count: usize,
    /// When the session was created.
    pub created_at: String,
    /// Last activity timestamp.
    pub last_activity: String,
    /// Optional user ID.
    pub user_id: Option<String>,
}

impl From<SwitchboardSession> for SessionResponse {
    fn from(session: SwitchboardSession) -> Self {
        Self {
            session_id: session.id.to_string(),
            state: session.state.to_string(),
            expression_count: session.expressions.len(),
            result_count: session.results.len(),
            created_at: session.created_at.to_rfc3339(),
            last_activity: session.last_activity.to_rfc3339(),
            user_id: session.user_id,
        }
    }
}

/// Standard error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

// ============================================================================
// Handlers
// ============================================================================

/// `POST /api/switchboard/submit`
///
/// Ingests, classifies, and matches handlers for the given expression.
/// Returns a session ID and an option menu for the user to choose from.
async fn submit_expression(
    State(state): State<SharedState>,
    Json(req): Json<SubmitRequest>,
) -> impl IntoResponse {
    info!(
        format = %req.format,
        content_len = req.content.len(),
        "switchboard API: submit expression"
    );

    let mut raw_input = RawInput::new(req.format, req.content);
    for (k, v) in req.metadata {
        raw_input = raw_input.with_metadata(k, v);
    }

    match pipeline::process_expression(
        raw_input,
        &state.0.registry,
        &state.0.session_store,
        &state.0.config,
    )
    .await
    {
        Ok(response) => {
            info!(
                session_id = %response.session_id,
                option_count = response.menu.options.len(),
                "switchboard API: submit succeeded"
            );
            let body: SubmitResponse = response.into();
            (StatusCode::OK, Json(serde_json::to_value(body).unwrap())).into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

/// `POST /api/switchboard/execute`
///
/// Executes a handler selected from the option menu on a previously submitted
/// expression.
async fn execute_handler(
    State(state): State<SharedState>,
    Json(req): Json<ExecuteRequest>,
) -> impl IntoResponse {
    info!(
        session_id = %req.session_id,
        handler_id = %req.handler_id,
        "switchboard API: execute handler"
    );

    let session_id = match uuid::Uuid::parse_str(&req.session_id) {
        Ok(uuid) => SessionId::from(uuid),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::to_value(ErrorResponse {
                    error: "invalid session_id format".to_string(),
                    code: "invalid_session_id".to_string(),
                })
                .unwrap()),
            )
                .into_response();
        }
    };

    let handler_id = HandlerId::from(req.handler_id.as_str());

    match pipeline::execute_handler(
        &session_id,
        &handler_id,
        req.params,
        &state.0.registry,
        &state.0.session_store,
    )
    .await
    {
        Ok(response) => {
            info!(
                session_id = %response.session_id,
                success = response.result.success,
                "switchboard API: execute succeeded"
            );
            let body: ExecuteApiResponse = response.into();
            (StatusCode::OK, Json(serde_json::to_value(body).unwrap())).into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

/// `GET /api/switchboard/session/:id`
///
/// Returns the current state of a switchboard session.
async fn get_session(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    info!(session_id = %id, "switchboard API: get session");

    let session_id = match uuid::Uuid::parse_str(&id) {
        Ok(uuid) => SessionId::from(uuid),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::to_value(ErrorResponse {
                    error: "invalid session_id format".to_string(),
                    code: "invalid_session_id".to_string(),
                })
                .unwrap()),
            )
                .into_response();
        }
    };

    match state.0.session_store.get_session(&session_id) {
        Ok(session) => {
            let body: SessionResponse = session.into();
            (StatusCode::OK, Json(serde_json::to_value(body).unwrap())).into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

// ============================================================================
// Error mapping
// ============================================================================

/// Maps a `SwitchboardError` to an appropriate HTTP status code and JSON body.
fn map_error(err: SwitchboardError) -> (StatusCode, Json<serde_json::Value>) {
    let (status, code) = match &err {
        SwitchboardError::InputTooLarge { .. } => {
            (StatusCode::PAYLOAD_TOO_LARGE, "input_too_large")
        }
        SwitchboardError::InvalidFormat(_) => (StatusCode::BAD_REQUEST, "invalid_format"),
        SwitchboardError::UnsupportedInput(_) => (StatusCode::BAD_REQUEST, "unsupported_input"),
        SwitchboardError::ClassificationFailed(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "classification_failed")
        }
        SwitchboardError::SessionNotFound(_) => (StatusCode::NOT_FOUND, "session_not_found"),
        SwitchboardError::SessionExpired(_) => (StatusCode::GONE, "session_expired"),
        SwitchboardError::HandlerNotFound(_) => (StatusCode::NOT_FOUND, "handler_not_found"),
        SwitchboardError::OracleError { .. } => {
            (StatusCode::INTERNAL_SERVER_ERROR, "oracle_error")
        }
        SwitchboardError::OracleTimeout { .. } => (StatusCode::GATEWAY_TIMEOUT, "oracle_timeout"),
        SwitchboardError::RateLimited(_) => (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        SwitchboardError::MaxSessionsReached(_) => {
            (StatusCode::SERVICE_UNAVAILABLE, "max_sessions_reached")
        }
        SwitchboardError::BudgetExceeded { .. } => (StatusCode::GATEWAY_TIMEOUT, "budget_exceeded"),
        SwitchboardError::SanitizationRejected(_) => {
            (StatusCode::BAD_REQUEST, "sanitization_rejected")
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
    };

    error!(
        status = %status,
        code = code,
        error = %err,
        "switchboard API error"
    );

    (
        status,
        Json(
            serde_json::to_value(ErrorResponse {
                error: err.to_string(),
                code: code.to_string(),
            })
            .unwrap(),
        ),
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn make_state() -> Arc<SwitchboardApiState> {
        let config = SwitchboardConfig::default();
        let registry = pipeline::default_registry(&config);
        let session_store = SessionStore::new();

        Arc::new(SwitchboardApiState {
            registry,
            session_store,
            config,
        })
    }

    fn make_router() -> Router {
        let state = make_state();
        create_switchboard_router(state)
    }

    #[tokio::test]
    async fn test_submit_plain_text() {
        let app = make_router();

        let body = serde_json::json!({
            "format": "plain_text",
            "content": "x^2 + 3*x + 2 = 0"
        });

        let req = Request::builder()
            .method("POST")
            .uri("/api/switchboard/submit")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert!(json.get("session_id").is_some());
        assert!(json.get("confidence").is_some());
        assert!(json.get("menu").is_some());
    }

    #[tokio::test]
    async fn test_submit_latex() {
        let app = make_router();

        let body = serde_json::json!({
            "format": "latex",
            "content": r"\int_0^1 x^2 dx"
        });

        let req = Request::builder()
            .method("POST")
            .uri("/api/switchboard/submit")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_session_not_found() {
        let app = make_router();

        let fake_id = uuid::Uuid::new_v4();
        let req = Request::builder()
            .method("GET")
            .uri(&format!("/api/switchboard/session/{}", fake_id))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_session_invalid_id() {
        let app = make_router();

        let req = Request::builder()
            .method("GET")
            .uri("/api/switchboard/session/not-a-uuid")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_execute_invalid_session_id() {
        let app = make_router();

        let body = serde_json::json!({
            "session_id": "not-a-uuid",
            "handler_id": "explain"
        });

        let req = Request::builder()
            .method("POST")
            .uri("/api/switchboard/execute")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_submit_and_execute_roundtrip() {
        let state = make_state();
        let app = create_switchboard_router(state);

        // Step 1: Submit an expression.
        let submit_body = serde_json::json!({
            "format": "plain_text",
            "content": "2 + 2"
        });

        let submit_req = Request::builder()
            .method("POST")
            .uri("/api/switchboard/submit")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&submit_body).unwrap()))
            .unwrap();

        let submit_resp = app.clone().oneshot(submit_req).await.unwrap();
        assert_eq!(submit_resp.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(submit_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let submit_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        let session_id = submit_json["session_id"].as_str().unwrap().to_string();

        // Step 2: Execute the "explain" handler.
        let exec_body = serde_json::json!({
            "session_id": session_id,
            "handler_id": "explain"
        });

        let exec_req = Request::builder()
            .method("POST")
            .uri("/api/switchboard/execute")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&exec_body).unwrap()))
            .unwrap();

        let exec_resp = app.clone().oneshot(exec_req).await.unwrap();
        assert_eq!(exec_resp.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(exec_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let exec_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(exec_json["success"], true);
        assert!(exec_json.get("result_plain").is_some());

        // Step 3: Get the session state.
        let get_req = Request::builder()
            .method("GET")
            .uri(&format!("/api/switchboard/session/{}", session_id))
            .body(Body::empty())
            .unwrap();

        let get_resp = app.oneshot(get_req).await.unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let session_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(session_json["state"], "completed");
    }
}
