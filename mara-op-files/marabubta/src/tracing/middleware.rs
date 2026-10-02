// Marabunta - Licensed under the MIT License.
//! Axum middleware for distributed tracing
//!
//! Provides automatic trace context propagation via W3C Trace Context headers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::{
    body::Body,
    http::{header::HeaderName, Request, Response},
};
use tower::{Layer, Service};

use super::context::TraceContext;
use super::exporter::TraceExporter;
use super::span::{Span, SpanKind, SpanStatus};

/// W3C Trace Context header names
pub const W3C_TRACEPARENT: &str = "traceparent";
pub const W3C_TRACESTATE: &str = "tracestate";

/// Header name types for Axum
static TRACEPARENT_HEADER: HeaderName = HeaderName::from_static("traceparent");
static TRACESTATE_HEADER: HeaderName = HeaderName::from_static("tracestate");

/// Configuration for the tracing middleware
#[derive(Debug, Clone)]
pub struct TracingMiddlewareConfig {
    /// Whether to propagate trace context in responses
    pub propagate_context: bool,
    /// Whether to create server spans for incoming requests
    pub create_server_spans: bool,
    /// Paths to exclude from tracing
    pub exempt_paths: Vec<String>,
    /// Service name for span attributes
    pub service_name: String,
    /// Whether to record request/response bodies (can be expensive)
    pub record_bodies: bool,
    /// Maximum body size to record (in bytes)
    pub max_body_size: usize,
    /// Whether to add HTTP semantic convention attributes
    pub http_semantics: bool,
}

impl Default for TracingMiddlewareConfig {
    fn default() -> Self {
        Self {
            propagate_context: true,
            create_server_spans: true,
            exempt_paths: vec![
                "/health".to_string(),
                "/ready".to_string(),
                "/metrics".to_string(),
            ],
            service_name: "marabunta-compute".to_string(),
            record_bodies: false,
            max_body_size: 1024,
            http_semantics: true,
        }
    }
}

impl TracingMiddlewareConfig {
    /// Create a new config with service name
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            ..Default::default()
        }
    }

    /// Add an exempt path
    pub fn with_exempt_path(mut self, path: impl Into<String>) -> Self {
        self.exempt_paths.push(path.into());
        self
    }

    /// Set whether to propagate context
    pub fn propagate_context(mut self, propagate: bool) -> Self {
        self.propagate_context = propagate;
        self
    }

    /// Set whether to create server spans
    pub fn create_server_spans(mut self, create: bool) -> Self {
        self.create_server_spans = create;
        self
    }

    /// Check if a path is exempt from tracing
    pub fn is_exempt(&self, path: &str) -> bool {
        self.exempt_paths
            .iter()
            .any(|p| path.starts_with(p.as_str()))
    }
}

/// Tower layer for adding tracing middleware
#[derive(Clone)]
pub struct TracingLayer<E: TraceExporter + Clone + 'static> {
    exporter: Arc<E>,
    config: TracingMiddlewareConfig,
}

impl<E: TraceExporter + Clone + 'static> TracingLayer<E> {
    /// Create a new tracing layer with an exporter
    pub fn new(exporter: E) -> Self {
        Self {
            exporter: Arc::new(exporter),
            config: TracingMiddlewareConfig::default(),
        }
    }

    /// Create a new tracing layer with config
    pub fn with_config(exporter: E, config: TracingMiddlewareConfig) -> Self {
        Self {
            exporter: Arc::new(exporter),
            config,
        }
    }
}

impl<S, E: TraceExporter + Clone + 'static> Layer<S> for TracingLayer<E> {
    type Service = TracingMiddleware<S, E>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingMiddleware {
            inner,
            exporter: self.exporter.clone(),
            config: self.config.clone(),
        }
    }
}

/// Tracing middleware service
#[derive(Clone)]
pub struct TracingMiddleware<S, E: TraceExporter + Clone + 'static> {
    inner: S,
    exporter: Arc<E>,
    config: TracingMiddlewareConfig,
}

impl<S, E: TraceExporter + Clone + 'static> TracingMiddleware<S, E> {
    /// Extract trace context from request headers
    fn extract_context<B>(&self, req: &Request<B>) -> Option<TraceContext> {
        let traceparent = req
            .headers()
            .get(&TRACEPARENT_HEADER)
            .and_then(|v| v.to_str().ok())?;

        let tracestate = req
            .headers()
            .get(&TRACESTATE_HEADER)
            .and_then(|v| v.to_str().ok());

        TraceContext::from_headers(traceparent, tracestate)
    }

    /// Create a server span for the request
    fn create_server_span<B>(&self, req: &Request<B>, parent_ctx: Option<&TraceContext>) -> Span {
        let trace_ctx = match parent_ctx {
            Some(ctx) => ctx.new_child(),
            None => TraceContext::new(),
        };

        let operation_name = format!("{} {}", req.method(), req.uri().path());
        let mut span = Span::new_with_context(operation_name, SpanKind::Server, trace_ctx);

        // Add service name
        span.set_attribute("service.name", self.config.service_name.clone());

        if self.config.http_semantics {
            // HTTP semantic conventions (OpenTelemetry)
            span.set_attribute("http.method", req.method().to_string());
            span.set_attribute("http.url", req.uri().to_string());
            span.set_attribute(
                "http.target",
                req.uri()
                    .path_and_query()
                    .map_or_else(|| req.uri().path().to_string(), |pq| pq.to_string()),
            );
            span.set_attribute("http.scheme", req.uri().scheme_str().unwrap_or("http"));

            if let Some(host) = req.headers().get("host").and_then(|v| v.to_str().ok()) {
                span.set_attribute("http.host", host);
            }

            if let Some(user_agent) = req
                .headers()
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
            {
                span.set_attribute("http.user_agent", user_agent);
            }

            // Client info
            if let Some(forwarded_for) = req
                .headers()
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
            {
                span.set_attribute(
                    "http.client_ip",
                    forwarded_for.split(',').next().unwrap_or(""),
                );
            }
        }

        span
    }

    /// Inject trace context into response headers
    fn inject_context(&self, response: &mut Response<Body>, ctx: &TraceContext) {
        if self.config.propagate_context {
            let headers = response.headers_mut();

            if let Ok(traceparent) = ctx.to_traceparent().parse() {
                headers.insert(&TRACEPARENT_HEADER, traceparent);
            }

            let tracestate = ctx.to_tracestate();
            if !tracestate.is_empty() {
                if let Ok(value) = tracestate.parse() {
                    headers.insert(&TRACESTATE_HEADER, value);
                }
            }
        }
    }
}

impl<S, E, ReqBody> Service<Request<ReqBody>> for TracingMiddleware<S, E>
where
    S: Service<Request<ReqBody>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: Send,
    ReqBody: Send + 'static,
    E: TraceExporter + Clone + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let path = req.uri().path().to_string();
        let is_exempt = self.config.is_exempt(&path);
        let create_spans = self.config.create_server_spans && !is_exempt;
        let propagate = self.config.propagate_context;

        // Extract incoming trace context
        let parent_ctx = if !is_exempt {
            self.extract_context(&req)
        } else {
            None
        };

        // Create server span if configured
        let span = if create_spans {
            Some(self.create_server_span(&req, parent_ctx.as_ref()))
        } else {
            None
        };

        let mut inner = self.inner.clone();
        let exporter = self.exporter.clone();
        let http_semantics = self.config.http_semantics;

        Box::pin(async move {
            let result = inner.call(req).await;

            match result {
                Ok(mut response) => {
                    if let Some(mut span) = span {
                        // Add response attributes
                        if http_semantics {
                            span.set_attribute(
                                "http.status_code",
                                response.status().as_u16() as i64,
                            );
                        }

                        // Set span status based on HTTP status
                        let status = response.status();
                        if status.is_success() {
                            span.set_status(SpanStatus::Ok);
                        } else if status.is_client_error() || status.is_server_error() {
                            span.set_error(format!("HTTP {}", status.as_u16()));
                        }

                        span.end();

                        // Inject trace context into response
                        if propagate {
                            let ctx = span.trace_context().clone();
                            let headers = response.headers_mut();

                            if let Ok(traceparent) = ctx.to_traceparent().parse() {
                                headers.insert(&TRACEPARENT_HEADER, traceparent);
                            }

                            let tracestate = ctx.to_tracestate();
                            if !tracestate.is_empty() {
                                if let Ok(value) = tracestate.parse() {
                                    headers.insert(&TRACESTATE_HEADER, value);
                                }
                            }
                        }

                        // Export the span (fire and forget)
                        let _ = exporter.export(&[span]).await;
                    }

                    Ok(response)
                }
                Err(err) => {
                    if let Some(mut span) = span {
                        span.set_error("Request failed");
                        span.end();
                        let _ = exporter.export(&[span]).await;
                    }
                    Err(err)
                }
            }
        })
    }
}

/// Extract trace context from request headers
///
/// Use this in handlers to get the trace context from the incoming request.
pub fn extract_trace_context<B>(req: &Request<B>) -> Option<TraceContext> {
    let traceparent = req
        .headers()
        .get(&TRACEPARENT_HEADER)
        .and_then(|v| v.to_str().ok())?;

    let tracestate = req
        .headers()
        .get(&TRACESTATE_HEADER)
        .and_then(|v| v.to_str().ok());

    TraceContext::from_headers(traceparent, tracestate)
}

/// Inject trace context into response headers
///
/// Use this to propagate trace context in outgoing responses.
pub fn inject_trace_context(response: &mut Response<Body>, ctx: &TraceContext) {
    let headers = response.headers_mut();

    if let Ok(traceparent) = ctx.to_traceparent().parse() {
        headers.insert(&TRACEPARENT_HEADER, traceparent);
    }

    let tracestate = ctx.to_tracestate();
    if !tracestate.is_empty() {
        if let Ok(value) = tracestate.parse() {
            headers.insert(&TRACESTATE_HEADER, value);
        }
    }
}

/// Create trace context headers for outgoing requests
///
/// Use this when making HTTP requests to other services.
pub fn trace_headers(ctx: &TraceContext) -> Vec<(&'static str, String)> {
    let mut headers = vec![(W3C_TRACEPARENT, ctx.to_traceparent())];

    let tracestate = ctx.to_tracestate();
    if !tracestate.is_empty() {
        headers.push((W3C_TRACESTATE, tracestate));
    }

    headers
}

/// Builder for creating traced HTTP requests
pub struct TracedRequestBuilder {
    ctx: TraceContext,
}

impl TracedRequestBuilder {
    /// Create a new builder with a trace context
    pub fn new(ctx: TraceContext) -> Self {
        Self { ctx }
    }

    /// Create a new builder with a new trace context
    pub fn new_trace() -> Self {
        Self {
            ctx: TraceContext::new(),
        }
    }

    /// Create a child context for an outgoing request
    pub fn child_context(&self) -> TraceContext {
        self.ctx.new_child()
    }

    /// Get the traceparent header value
    pub fn traceparent(&self) -> String {
        self.ctx.to_traceparent()
    }

    /// Get the tracestate header value
    pub fn tracestate(&self) -> String {
        self.ctx.to_tracestate()
    }

    /// Get headers for adding to a request
    pub fn headers(&self) -> Vec<(&'static str, String)> {
        trace_headers(&self.ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracing::StdoutExporter;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
        Router,
    };
    use tower::ServiceExt;

    async fn test_handler() -> &'static str {
        "Hello, World!"
    }

    #[test]
    fn test_middleware_config_default() {
        let config = TracingMiddlewareConfig::default();

        assert!(config.propagate_context);
        assert!(config.create_server_spans);
        assert!(config.http_semantics);
        assert!(!config.exempt_paths.is_empty());
    }

    #[test]
    fn test_middleware_config_exempt_paths() {
        let config = TracingMiddlewareConfig::default()
            .with_exempt_path("/api/internal")
            .with_exempt_path("/debug");

        assert!(config.is_exempt("/health"));
        assert!(config.is_exempt("/metrics"));
        assert!(config.is_exempt("/api/internal"));
        assert!(config.is_exempt("/api/internal/status"));
        assert!(!config.is_exempt("/api/public"));
    }

    #[test]
    fn test_middleware_config_builder() {
        let config = TracingMiddlewareConfig::new("my-service")
            .propagate_context(false)
            .create_server_spans(true)
            .with_exempt_path("/custom");

        assert_eq!(config.service_name, "my-service");
        assert!(!config.propagate_context);
        assert!(config.create_server_spans);
        assert!(config.is_exempt("/custom"));
    }

    #[tokio::test]
    async fn test_tracing_middleware() {
        let exporter = StdoutExporter::compact();
        let layer = TracingLayer::new(exporter);

        let app = Router::new().route("/test", get(test_handler)).layer(layer);

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Should have traceparent header in response
        assert!(response.headers().contains_key("traceparent"));
    }

    #[tokio::test]
    async fn test_tracing_middleware_propagation() {
        let exporter = StdoutExporter::compact();
        let layer = TracingLayer::new(exporter);

        let app = Router::new().route("/test", get(test_handler)).layer(layer);

        // Send request with trace context
        let ctx = TraceContext::new();
        let traceparent = ctx.to_traceparent();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("traceparent", &traceparent)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Response should have traceparent header (child span)
        let response_traceparent = response
            .headers()
            .get("traceparent")
            .unwrap()
            .to_str()
            .unwrap();

        // Should have same trace ID (different span ID)
        let parts: Vec<&str> = traceparent.split('-').collect();
        let response_parts: Vec<&str> = response_traceparent.split('-').collect();
        assert_eq!(parts[1], response_parts[1]); // Same trace ID
    }

    #[tokio::test]
    async fn test_tracing_middleware_exempt_path() {
        let exporter = StdoutExporter::compact();
        let config = TracingMiddlewareConfig::default();
        let layer = TracingLayer::with_config(exporter, config);

        let app = Router::new()
            .route("/health", get(test_handler))
            .layer(layer);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Should NOT have traceparent header (exempt path)
        assert!(!response.headers().contains_key("traceparent"));
    }

    #[test]
    fn test_extract_trace_context() {
        let ctx = TraceContext::new();
        let request = Request::builder()
            .header("traceparent", ctx.to_traceparent())
            .body(Body::empty())
            .unwrap();

        let extracted = extract_trace_context(&request).unwrap();
        assert_eq!(extracted.trace_id(), ctx.trace_id());
    }

    #[test]
    fn test_trace_headers() {
        let ctx = TraceContext::new();
        ctx.trace_state().clone(); // Access state

        let headers = trace_headers(&ctx);
        assert!(!headers.is_empty());
        assert_eq!(headers[0].0, "traceparent");
    }

    #[test]
    fn test_traced_request_builder() {
        let builder = TracedRequestBuilder::new_trace();

        let traceparent = builder.traceparent();
        assert!(!traceparent.is_empty());

        let child = builder.child_context();
        let child_traceparent = child.to_traceparent();

        // Same trace ID
        let parts: Vec<&str> = traceparent.split('-').collect();
        let child_parts: Vec<&str> = child_traceparent.split('-').collect();
        assert_eq!(parts[1], child_parts[1]);

        // Different span ID
        assert_ne!(parts[2], child_parts[2]);
    }

    #[test]
    fn test_inject_trace_context() {
        let ctx = TraceContext::new();
        let mut response = Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap();

        inject_trace_context(&mut response, &ctx);

        assert!(response.headers().contains_key("traceparent"));
        let traceparent = response
            .headers()
            .get("traceparent")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(traceparent, ctx.to_traceparent());
    }
}
