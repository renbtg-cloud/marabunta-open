// Marabunta - Licensed under the MIT License.
//! Axum middleware for rate limiting API routes
//!
//! This module provides middleware that integrates the rate limiter with Axum
//! to automatically enforce rate limits on API endpoints.
//!
//! # Features
//!
//! - Returns 429 Too Many Requests when rate limited
//! - Includes Retry-After header
//! - Includes X-RateLimit-* headers for transparency
//! - Supports per-client and global limits
//! - Extracts client identity from IP, API key, or auth token
//!
//! # Example
//!
//! ```rust,no_run
//! use axum::Router;
//! use std::sync::Arc;
//! use marabunta_compute::ratelimit::{RateLimiter, RateLimitLayer};
//!
//! // Create rate limiter
//! let limiter = Arc::new(RateLimiter::new());
//!
//! // Create router with rate limiting
//! let app = Router::new()
//!     .route("/api/jobs", axum::routing::get(|| async { "jobs" }))
//!     .layer(RateLimitLayer::new(limiter));
//! ```

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{header, Request, Response, StatusCode},
    response::IntoResponse,
};
use serde_json::json;
use tower::{Layer, Service};

use super::limiter::RateLimiter;
use super::types::{AuthLevel, RateLimitInfo, RateLimitKey, RateLimitResult};

/// Header names for rate limit information.
pub mod headers {
    /// Maximum requests allowed in the window
    pub const X_RATELIMIT_LIMIT: &str = "X-RateLimit-Limit";
    /// Requests remaining in the current window
    pub const X_RATELIMIT_REMAINING: &str = "X-RateLimit-Remaining";
    /// Unix timestamp when the rate limit resets
    pub const X_RATELIMIT_RESET: &str = "X-RateLimit-Reset";
    /// Seconds until the rate limit resets
    pub const X_RATELIMIT_RESET_AFTER: &str = "X-RateLimit-Reset-After";
    /// Retry after this many seconds (on 429)
    pub const RETRY_AFTER: &str = "Retry-After";
}

/// Layer for applying rate limiting to Axum routes.
#[derive(Clone)]
pub struct RateLimitLayer {
    limiter: Arc<RateLimiter>,
    config: RateLimitMiddlewareConfig,
}

impl RateLimitLayer {
    /// Create a new rate limit layer with default configuration.
    pub fn new(limiter: Arc<RateLimiter>) -> Self {
        Self {
            limiter,
            config: RateLimitMiddlewareConfig::default(),
        }
    }

    /// Create a new rate limit layer with custom configuration.
    pub fn with_config(limiter: Arc<RateLimiter>, config: RateLimitMiddlewareConfig) -> Self {
        Self { limiter, config }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitMiddleware {
            inner,
            limiter: self.limiter.clone(),
            config: self.config.clone(),
        }
    }
}

/// Configuration for the rate limit middleware.
#[derive(Debug, Clone)]
pub struct RateLimitMiddlewareConfig {
    /// Header to extract API key from
    pub api_key_header: String,
    /// Header to extract auth token from
    pub auth_header: String,
    /// Whether to include rate limit headers in responses
    pub include_headers: bool,
    /// Whether to check global limits in addition to per-client limits
    pub check_global_limits: bool,
    /// Custom error message for rate limited responses
    pub error_message: String,
    /// Trusted proxy headers for IP extraction
    pub trusted_proxy_headers: Vec<String>,
}

impl Default for RateLimitMiddlewareConfig {
    fn default() -> Self {
        Self {
            api_key_header: "X-API-Key".to_string(),
            auth_header: "Authorization".to_string(),
            include_headers: true,
            check_global_limits: true,
            error_message: "Rate limit exceeded. Please try again later.".to_string(),
            trusted_proxy_headers: vec!["X-Forwarded-For".to_string(), "X-Real-IP".to_string()],
        }
    }
}

/// Middleware service for rate limiting.
#[derive(Clone)]
pub struct RateLimitMiddleware<S> {
    inner: S,
    limiter: Arc<RateLimiter>,
    config: RateLimitMiddlewareConfig,
}

impl<S> RateLimitMiddleware<S> {
    /// Extract client IP from request.
    fn extract_client_ip<B>(&self, req: &Request<B>) -> Option<IpAddr> {
        // Try trusted proxy headers first
        for header_name in &self.config.trusted_proxy_headers {
            if let Some(value) = req.headers().get(header_name) {
                if let Ok(s) = value.to_str() {
                    // X-Forwarded-For can contain multiple IPs, take the first
                    let ip_str = s.split(',').next()?.trim();
                    if let Ok(ip) = ip_str.parse() {
                        return Some(ip);
                    }
                }
            }
        }

        // Fall back to connection info
        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip())
    }

    /// Extract API key from request headers.
    fn extract_api_key<B>(&self, req: &Request<B>) -> Option<String> {
        req.headers()
            .get(&self.config.api_key_header)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    /// Extract auth level from request.
    fn extract_auth_level<B>(&self, req: &Request<B>) -> AuthLevel {
        // Check for API key first
        if let Some(api_key) = self.extract_api_key(req) {
            // In production, validate the API key and determine tier
            if api_key.starts_with("admin_") {
                return AuthLevel::Admin;
            } else if api_key.starts_with("premium_") {
                return AuthLevel::Premium;
            } else {
                return AuthLevel::Authenticated;
            }
        }

        // Check for auth header
        if let Some(auth) = req.headers().get(&self.config.auth_header) {
            if auth.to_str().ok().is_some() {
                return AuthLevel::Authenticated;
            }
        }

        AuthLevel::Anonymous
    }

    /// Build the rate limit key from request.
    fn build_key<B>(&self, req: &Request<B>) -> RateLimitKey {
        let endpoint = req.uri().path().to_string();

        // Prefer API key over IP
        if let Some(api_key) = self.extract_api_key(req) {
            return RateLimitKey::by_api_key(api_key, endpoint);
        }

        // Fall back to IP
        if let Some(ip) = self.extract_client_ip(req) {
            return RateLimitKey::by_ip(ip, endpoint);
        }

        // Last resort: global key
        RateLimitKey::global(endpoint)
    }

    /// Build rate limit error response.
    fn rate_limit_response(
        &self,
        retry_after: Duration,
        limit_info: &RateLimitInfo,
    ) -> Response<Body> {
        let retry_secs = retry_after.as_secs().max(1);

        let body = json!({
            "error": self.config.error_message,
            "status": 429,
            "retry_after_secs": retry_secs,
            "rate_limit": {
                "limit": limit_info.limit,
                "remaining": 0,
                "reset_at": limit_info.reset_at,
            }
        });

        let mut response = Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(header::CONTENT_TYPE, "application/json")
            .header(headers::RETRY_AFTER, retry_secs.to_string());

        if self.config.include_headers {
            response = response
                .header(headers::X_RATELIMIT_LIMIT, limit_info.limit.to_string())
                .header(headers::X_RATELIMIT_REMAINING, "0")
                .header(headers::X_RATELIMIT_RESET, limit_info.reset_at.to_string())
                .header(
                    headers::X_RATELIMIT_RESET_AFTER,
                    limit_info.reset_after_secs.to_string(),
                );
        }

        response.body(Body::from(body.to_string())).unwrap()
    }

    /// Add rate limit headers to a response.
    #[allow(dead_code)]
    fn add_rate_limit_headers(&self, response: &mut Response<Body>, limit_info: &RateLimitInfo) {
        if self.config.include_headers {
            let headers = response.headers_mut();
            headers.insert(
                headers::X_RATELIMIT_LIMIT,
                limit_info.limit.to_string().parse().unwrap(),
            );
            headers.insert(
                headers::X_RATELIMIT_REMAINING,
                limit_info.remaining.to_string().parse().unwrap(),
            );
            headers.insert(
                headers::X_RATELIMIT_RESET,
                limit_info.reset_at.to_string().parse().unwrap(),
            );
            headers.insert(
                headers::X_RATELIMIT_RESET_AFTER,
                limit_info.reset_after_secs.to_string().parse().unwrap(),
            );
        }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for RateLimitMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let key = self.build_key(&req);
        let auth_level = self.extract_auth_level(&req);
        let config = self.config.clone();

        // Check rate limit
        let result = if config.check_global_limits {
            self.limiter.check_hierarchical(&key, auth_level)
        } else {
            self.limiter.check_with_auth(&key, auth_level)
        };

        let limiter_config = self.limiter.config();
        let limit = limiter_config.get_limit(key.endpoint(), auth_level);

        match result {
            RateLimitResult::Limited { retry_after } => {
                let limit_info = RateLimitInfo::new(limit.burst_size, 0, retry_after);
                let response = self.rate_limit_response(retry_after, &limit_info);
                Box::pin(async move { Ok(response) })
            }
            RateLimitResult::Allowed {
                remaining,
                reset_after,
            } => {
                let limit_info = RateLimitInfo::new(limit.burst_size, remaining, reset_after);
                let include_headers = config.include_headers;

                let mut inner = self.inner.clone();
                Box::pin(async move {
                    let mut response = inner.call(req).await?;

                    if include_headers {
                        let headers = response.headers_mut();
                        headers.insert(
                            headers::X_RATELIMIT_LIMIT,
                            limit_info.limit.to_string().parse().unwrap(),
                        );
                        headers.insert(
                            headers::X_RATELIMIT_REMAINING,
                            limit_info.remaining.to_string().parse().unwrap(),
                        );
                        headers.insert(
                            headers::X_RATELIMIT_RESET,
                            limit_info.reset_at.to_string().parse().unwrap(),
                        );
                        headers.insert(
                            headers::X_RATELIMIT_RESET_AFTER,
                            limit_info.reset_after_secs.to_string().parse().unwrap(),
                        );
                    }

                    Ok(response)
                })
            }
        }
    }
}

/// Extractor for rate limit information.
///
/// Can be used in handlers to get information about the current rate limit state.
#[derive(Debug, Clone)]
pub struct RateLimitState {
    /// Current rate limit info
    pub info: RateLimitInfo,
    /// The key used for rate limiting
    pub key: RateLimitKey,
    /// Auth level detected
    pub auth_level: AuthLevel,
}

/// Response helper for manually returning rate limit errors.
pub struct RateLimitedResponse {
    retry_after: Duration,
    message: Option<String>,
}

impl RateLimitedResponse {
    /// Create a new rate limited response.
    pub fn new(retry_after: Duration) -> Self {
        Self {
            retry_after,
            message: None,
        }
    }

    /// Set a custom error message.
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}

impl IntoResponse for RateLimitedResponse {
    fn into_response(self) -> axum::response::Response {
        let retry_secs = self.retry_after.as_secs().max(1);
        let message = self
            .message
            .unwrap_or_else(|| "Rate limit exceeded. Please try again later.".to_string());

        let body = json!({
            "error": message,
            "status": 429,
            "retry_after_secs": retry_secs,
        });

        Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(header::CONTENT_TYPE, "application/json")
            .header(headers::RETRY_AFTER, retry_secs.to_string())
            .body(Body::from(body.to_string()))
            .unwrap()
            .into_response()
    }
}

/// Builder for creating rate limit middleware with fluent API.
pub struct RateLimitLayerBuilder {
    limiter: Arc<RateLimiter>,
    config: RateLimitMiddlewareConfig,
}

impl RateLimitLayerBuilder {
    /// Create a new builder.
    pub fn new(limiter: Arc<RateLimiter>) -> Self {
        Self {
            limiter,
            config: RateLimitMiddlewareConfig::default(),
        }
    }

    /// Set the API key header name.
    pub fn api_key_header(mut self, header: impl Into<String>) -> Self {
        self.config.api_key_header = header.into();
        self
    }

    /// Set the auth header name.
    pub fn auth_header(mut self, header: impl Into<String>) -> Self {
        self.config.auth_header = header.into();
        self
    }

    /// Enable or disable rate limit headers in responses.
    pub fn include_headers(mut self, include: bool) -> Self {
        self.config.include_headers = include;
        self
    }

    /// Enable or disable global limit checking.
    pub fn check_global_limits(mut self, check: bool) -> Self {
        self.config.check_global_limits = check;
        self
    }

    /// Set a custom error message.
    pub fn error_message(mut self, message: impl Into<String>) -> Self {
        self.config.error_message = message.into();
        self
    }

    /// Add a trusted proxy header.
    pub fn trusted_proxy_header(mut self, header: impl Into<String>) -> Self {
        self.config.trusted_proxy_headers.push(header.into());
        self
    }

    /// Build the layer.
    pub fn build(self) -> RateLimitLayer {
        RateLimitLayer {
            limiter: self.limiter,
            config: self.config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn test_rate_limit_middleware_allows_requests() {
        let limiter = Arc::new(RateLimiter::new());
        let layer = RateLimitLayer::new(limiter);

        let app = Router::new().route("/test", get(test_handler)).layer(layer);

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Check rate limit headers are present
        assert!(response.headers().contains_key(headers::X_RATELIMIT_LIMIT));
        assert!(response
            .headers()
            .contains_key(headers::X_RATELIMIT_REMAINING));
    }

    #[tokio::test]
    async fn test_rate_limit_middleware_blocks_when_exhausted() {
        let mut config = super::super::limiter::RateLimiterConfig::default();
        config.default_limit = super::super::types::RateLimit::new(100.0, 2);
        // Clear overrides so the default_limit is used
        config.auth_level_limits.clear();
        config.endpoint_limits.clear();

        let limiter = Arc::new(RateLimiter::with_config(config));
        let layer = RateLimitLayer::new(limiter.clone());

        let app = Router::new().route("/test", get(test_handler)).layer(layer);

        // Make 2 requests to exhaust the limit
        for _ in 0..2 {
            let app_clone = app.clone();
            let response = app_clone
                .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // 3rd request should be rate limited
        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(response.headers().contains_key(headers::RETRY_AFTER));
    }

    #[tokio::test]
    async fn test_rate_limit_middleware_exempt_paths() {
        let mut config = super::super::limiter::RateLimiterConfig::default();
        config.default_limit = super::super::types::RateLimit::new(100.0, 1);
        config.exempt_paths = vec!["/health".to_string()];

        let limiter = Arc::new(RateLimiter::with_config(config));
        let layer = RateLimitLayer::new(limiter);

        let app = Router::new()
            .route("/health", get(test_handler))
            .layer(layer);

        // Make many requests to exempt path - all should succeed
        for _ in 0..10 {
            let app_clone = app.clone();
            let response = app_clone
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn test_rate_limit_with_api_key() {
        let limiter = Arc::new(RateLimiter::new());
        let layer = RateLimitLayer::new(limiter);

        let app = Router::new().route("/test", get(test_handler)).layer(layer);

        // Request with admin API key should work
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("X-API-Key", "admin_test_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_rate_limited_response() {
        let response =
            RateLimitedResponse::new(Duration::from_secs(30)).with_message("Custom error message");

        // Convert to response
        let axum_response = response.into_response();
        assert_eq!(axum_response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn test_middleware_config_defaults() {
        let config = RateLimitMiddlewareConfig::default();

        assert_eq!(config.api_key_header, "X-API-Key");
        assert_eq!(config.auth_header, "Authorization");
        assert!(config.include_headers);
        assert!(config.check_global_limits);
    }

    #[test]
    fn test_rate_limit_layer_builder() {
        let limiter = Arc::new(RateLimiter::new());

        let layer = RateLimitLayerBuilder::new(limiter)
            .api_key_header("X-Custom-Key")
            .include_headers(false)
            .error_message("Custom error")
            .build();

        assert_eq!(layer.config.api_key_header, "X-Custom-Key");
        assert!(!layer.config.include_headers);
        assert_eq!(layer.config.error_message, "Custom error");
    }
}
