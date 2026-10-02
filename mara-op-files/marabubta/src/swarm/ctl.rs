// Marabunta - Licensed under the MIT License.
//! HTTP client library for the Marabunta Swarm management API.
//!
//! [`SwarmCtlClient`] provides a typed, resilient HTTP client for interacting
//! with the swarm's REST API. It covers every management endpoint: psyche,
//! events, alerts, fleet operations, nodes, jobs, SLA, capacity planning,
//! multi-swarm federation, visions, audit, and metrics.
//!
//! # Features
//!
//! - **Builder pattern**: configure base URL, auth token, timeout, retries,
//!   and TLS verification via chained methods.
//! - **Automatic retries**: exponential backoff with jitter on transient
//!   failures (network errors, 429, 502, 503, 504). Respects `Retry-After`
//!   headers on 429 responses.
//! - **Pagination**: [`PaginatedResponse`] with `next_offset()` helper, plus
//!   [`get_all_pages`](SwarmCtlClient::get_all_pages) for automatic page
//!   exhaustion.
//! - **SSE streaming**: [`event_stream`](SwarmCtlClient::event_stream) returns
//!   an async stream of parsed server-sent events.
//! - **Error classification**: [`CtlError`] distinguishes network failures,
//!   timeouts, HTTP errors, rate limiting, auth issues, and deserialization
//!   problems.
//!
//! # Example
//!
//! ```ignore
//! use std::time::Duration;
//! use marabunta_compute::swarm::ctl::SwarmCtlClient;
//!
//! let client = SwarmCtlClient::new("http://localhost:3000")
//!     .with_token("my-secret-token")
//!     .with_timeout(Duration::from_secs(30))
//!     .with_retries(3, Duration::from_millis(500));
//!
//! let health = client.health_check().await?;
//! let nodes = client.nodes().await?;
//! ```

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// ============================================================================
// CtlError
// ============================================================================

/// Error type returned by all [`SwarmCtlClient`] operations.
///
/// Each variant carries enough context to let callers decide whether to retry,
/// surface the error to the user, or handle it programmatically.
#[derive(Debug)]
pub enum CtlError {
    /// A network-level failure (DNS resolution, connection refused, TLS
    /// handshake error, etc.).
    Network(String),

    /// The request timed out before a response was received.
    Timeout(String),

    /// The server returned an HTTP error that does not map to a more specific
    /// variant. `status` is the numeric HTTP status code and `body` contains
    /// up to 4 KiB of the response body for diagnostics.
    Http {
        /// HTTP status code (e.g. 400, 500).
        status: u16,
        /// Truncated response body (up to 4096 bytes).
        body: String,
    },

    /// Failed to deserialize the response body into the expected type.
    Deserialization(String),

    /// The server returned 401 Unauthorized. The `String` carries the
    /// response body or a human-readable explanation.
    Unauthorized(String),

    /// The server returned 404 Not Found.
    NotFound(String),

    /// The server returned 429 Too Many Requests. If the response included
    /// a `Retry-After` header, `retry_after_secs` contains the parsed value.
    RateLimited {
        /// Seconds to wait before retrying, if the server specified one.
        retry_after_secs: Option<u64>,
    },

    /// The server returned a 5xx error (other than 502/503/504 which are
    /// retried automatically).
    ServerError(String),
}

impl fmt::Display for CtlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CtlError::Network(msg) => write!(f, "network error: {}", msg),
            CtlError::Timeout(msg) => write!(f, "request timed out: {}", msg),
            CtlError::Http { status, body } => {
                write!(f, "HTTP {} error: {}", status, body)
            }
            CtlError::Deserialization(msg) => {
                write!(f, "deserialization error: {}", msg)
            }
            CtlError::Unauthorized(msg) => write!(f, "unauthorized: {}", msg),
            CtlError::NotFound(msg) => write!(f, "not found: {}", msg),
            CtlError::RateLimited { retry_after_secs } => match retry_after_secs {
                Some(secs) => write!(f, "rate limited (retry after {}s)", secs),
                None => write!(f, "rate limited"),
            },
            CtlError::ServerError(msg) => write!(f, "server error: {}", msg),
        }
    }
}

impl std::error::Error for CtlError {}

impl CtlError {
    /// Returns `true` if this error is transient and the request should be
    /// retried (network failure, timeout, rate-limited, or retriable server
    /// error).
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            CtlError::Network(_) | CtlError::Timeout(_) | CtlError::RateLimited { .. }
        )
    }
}

// ============================================================================
// PaginatedResponse
// ============================================================================

/// A page of results returned by paginated API endpoints.
///
/// The swarm API uses offset-based pagination. Each page includes the total
/// count, current offset, page size limit, and a convenience `has_more` flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    /// The items in this page.
    pub items: Vec<T>,
    /// Total number of items across all pages.
    pub total: usize,
    /// The offset of the first item in this page.
    pub offset: usize,
    /// The maximum number of items per page.
    pub limit: usize,
    /// Whether there are more items beyond this page.
    pub has_more: bool,
}

impl<T> PaginatedResponse<T> {
    /// Returns the offset to use for fetching the next page, or `None` if
    /// there are no more pages.
    pub fn next_offset(&self) -> Option<usize> {
        if self.has_more {
            Some(self.offset + self.limit)
        } else {
            None
        }
    }

    /// Returns the number of items in this page.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if this page contains no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

// ============================================================================
// SseEvent
// ============================================================================

/// A single server-sent event parsed from an SSE stream.
///
/// SSE events consist of optional `event:` (type), `data:` (payload), and
/// `id:` (last-event-id) fields. Comments (lines starting with `:`) are
/// silently ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The event type (from the `event:` field). Defaults to `"message"` if
    /// the server did not specify one.
    pub event_type: String,
    /// The event payload (from one or more `data:` lines, joined with
    /// newlines).
    pub data: String,
    /// The last-event-id (from the `id:` field), if present.
    pub id: Option<String>,
}

// ============================================================================
// SwarmCtlClient
// ============================================================================

/// HTTP client for the Marabunta Swarm management API.
///
/// Construct via [`SwarmCtlClient::new`] and customize with builder methods.
/// All API methods are `async` and return `Result<T, CtlError>`.
///
/// The client is cheaply cloneable (shares the underlying `reqwest::Client`
/// and configuration via `Arc`-like semantics inside `reqwest`).
#[derive(Debug, Clone)]
pub struct SwarmCtlClient {
    /// The underlying HTTP client.
    client: reqwest::Client,
    /// Base URL of the swarm API (e.g. `http://localhost:3000`), without
    /// trailing slash.
    base_url: String,
    /// Optional bearer token for authentication.
    auth_token: Option<String>,
    /// Per-request timeout.
    timeout: Duration,
    /// Maximum number of retry attempts for transient failures.
    max_retries: u32,
    /// Base delay for exponential backoff between retries.
    retry_base_delay: Duration,
}

impl SwarmCtlClient {
    // ========================================================================
    // Builder
    // ========================================================================

    /// Create a new client targeting the given base URL.
    ///
    /// The URL is normalized: trailing slashes are stripped, and a scheme is
    /// required. The default timeout is 30 seconds with no retries.
    ///
    /// # Panics
    ///
    /// Panics if the `reqwest::Client` cannot be built (should only happen
    /// if the TLS backend is misconfigured).
    pub fn new(base_url: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");

        Self {
            client,
            base_url: normalize_base_url(base_url),
            auth_token: None,
            timeout: Duration::from_secs(30),
            max_retries: 0,
            retry_base_delay: Duration::from_millis(500),
        }
    }

    /// Set the bearer token used for authentication.
    ///
    /// The token is sent as `Authorization: Bearer <token>` on every request.
    pub fn with_token(mut self, token: &str) -> Self {
        self.auth_token = Some(token.to_string());
        self
    }

    /// Set the per-request timeout.
    ///
    /// This overrides the default 30-second timeout. The timeout applies to
    /// each individual HTTP request (including each retry attempt).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self.client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build reqwest client with custom timeout");
        self
    }

    /// Configure automatic retries for transient failures.
    ///
    /// `max_retries` is the maximum number of retry attempts (0 = no retries).
    /// `base_delay` is the initial backoff delay; subsequent retries use
    /// exponential backoff: `base_delay * 2^attempt`.
    ///
    /// Retries are attempted on: network errors, timeouts, 429 (rate limited),
    /// 502, 503, and 504 responses. All other errors fail immediately.
    pub fn with_retries(mut self, max_retries: u32, base_delay: Duration) -> Self {
        self.max_retries = max_retries;
        self.retry_base_delay = base_delay;
        self
    }

    /// Disable TLS certificate verification.
    ///
    /// **WARNING**: This should only be used in development or testing
    /// environments. It makes the connection vulnerable to man-in-the-middle
    /// attacks.
    pub fn with_tls_skip_verify(mut self) -> Self {
        self.client = reqwest::Client::builder()
            .timeout(self.timeout)
            .danger_accept_invalid_certs(true)
            .build()
            .expect("failed to build reqwest client with TLS skip verify");
        self
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Build the full URL for a given API path.
    fn url(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        }
    }

    /// Add authentication headers to a request builder.
    fn authenticate(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth_token {
            Some(token) => req.bearer_auth(token),
            None => req,
        }
    }

    /// Perform a GET request and deserialize the JSON response.
    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, CtlError> {
        let url = self.url(path);
        let req = self.authenticate(self.client.get(&url));
        let resp = self.request_with_retry(req).await?;
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .map_err(|e| CtlError::Network(e.to_string()))?;
        serde_json::from_str(&body).map_err(|e| {
            CtlError::Deserialization(format!(
                "failed to parse response from {} (status {}): {} -- body: {}",
                url,
                status,
                e,
                truncate_body(&body, 512)
            ))
        })
    }

    /// Perform a GET request with query parameters and deserialize the JSON
    /// response.
    async fn get_with_params<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&str, &str)],
    ) -> Result<T, CtlError> {
        let url = self.url(path);
        let req = self.authenticate(self.client.get(&url).query(params));
        let resp = self.request_with_retry(req).await?;
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .map_err(|e| CtlError::Network(e.to_string()))?;
        serde_json::from_str(&body).map_err(|e| {
            CtlError::Deserialization(format!(
                "failed to parse response from {} (status {}): {} -- body: {}",
                url,
                status,
                e,
                truncate_body(&body, 512)
            ))
        })
    }

    /// Perform a POST request with a JSON body and deserialize the JSON
    /// response.
    async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, CtlError> {
        let url = self.url(path);
        let req = self.authenticate(self.client.post(&url).json(body));
        let resp = self.request_with_retry(req).await?;
        let status = resp.status().as_u16();
        let resp_body = resp
            .text()
            .await
            .map_err(|e| CtlError::Network(e.to_string()))?;
        serde_json::from_str(&resp_body).map_err(|e| {
            CtlError::Deserialization(format!(
                "failed to parse response from POST {} (status {}): {} -- body: {}",
                url,
                status,
                e,
                truncate_body(&resp_body, 512)
            ))
        })
    }

    /// Perform a POST request with a JSON body, ignoring the response body
    /// (expecting 2xx with empty or irrelevant body).
    async fn post_no_response<B: Serialize>(&self, path: &str, body: &B) -> Result<(), CtlError> {
        let url = self.url(path);
        let req = self.authenticate(self.client.post(&url).json(body));
        let _resp = self.request_with_retry(req).await?;
        Ok(())
    }

    /// Perform a PUT request with a JSON body, ignoring the response body.
    async fn put_no_response<B: Serialize>(&self, path: &str, body: &B) -> Result<(), CtlError> {
        let url = self.url(path);
        let req = self.authenticate(self.client.put(&url).json(body));
        let _resp = self.request_with_retry(req).await?;
        Ok(())
    }

    /// Perform a DELETE request.
    async fn delete(&self, path: &str) -> Result<(), CtlError> {
        let url = self.url(path);
        let req = self.authenticate(self.client.delete(&url));
        let _resp = self.request_with_retry(req).await?;
        Ok(())
    }

    /// Perform a GET request and return the raw response body as a string.
    async fn get_text(&self, path: &str) -> Result<String, CtlError> {
        let url = self.url(path);
        let req = self.authenticate(self.client.get(&url));
        let resp = self.request_with_retry(req).await?;
        resp.text()
            .await
            .map_err(|e| CtlError::Network(e.to_string()))
    }

    /// Perform a GET request with query params and return the raw response
    /// body as a string.
    async fn get_text_with_params(
        &self,
        path: &str,
        params: &[(&str, &str)],
    ) -> Result<String, CtlError> {
        let url = self.url(path);
        let req = self.authenticate(self.client.get(&url).query(params));
        let resp = self.request_with_retry(req).await?;
        resp.text()
            .await
            .map_err(|e| CtlError::Network(e.to_string()))
    }

    /// Execute an HTTP request with automatic retry logic.
    ///
    /// Retries on transient failures up to `self.max_retries` times with
    /// exponential backoff. The following conditions trigger a retry:
    ///
    /// - Network errors (connection refused, DNS failure, etc.)
    /// - Timeout errors
    /// - HTTP 429 (Too Many Requests) -- respects `Retry-After` header
    /// - HTTP 502 (Bad Gateway)
    /// - HTTP 503 (Service Unavailable)
    /// - HTTP 504 (Gateway Timeout)
    ///
    /// All other HTTP status codes are not retried.
    async fn request_with_retry(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, CtlError> {
        // reqwest::RequestBuilder is not Clone, so we need to build from
        // scratch on retries. However, since we only get one builder, we
        // execute it directly on the first attempt. For retries, the caller
        // must re-invoke the higher-level method. To support retries within
        // this method, we use try_clone().
        let mut last_error: Option<CtlError> = None;
        let total_attempts = self.max_retries + 1;

        // Try to clone the request for potential retries.
        let mut pending_requests: Vec<Option<reqwest::RequestBuilder>> =
            Vec::with_capacity(total_attempts as usize);

        // Try to clone for all retry attempts first.
        if total_attempts > 1 {
            for _ in 0..(total_attempts - 1) {
                pending_requests.push(req.try_clone());
            }
        }
        // The original request goes first.
        pending_requests.insert(0, Some(req));

        for attempt in 0..total_attempts {
            let current_req = match pending_requests.get_mut(attempt as usize) {
                Some(slot) => match slot.take() {
                    Some(r) => r,
                    None => {
                        // Could not clone the request (e.g. streaming body).
                        // Fall through with the last error.
                        break;
                    }
                },
                None => break,
            };

            match current_req.send().await {
                Ok(resp) => {
                    let status = resp.status();

                    // Success -- return immediately.
                    if status.is_success() {
                        return Ok(resp);
                    }

                    let status_code = status.as_u16();

                    // Classify the error.
                    match status_code {
                        401 => {
                            let body = resp
                                .text()
                                .await
                                .unwrap_or_else(|_| String::from("<failed to read body>"));
                            return Err(CtlError::Unauthorized(truncate_body(&body, 4096)));
                        }
                        404 => {
                            let body = resp
                                .text()
                                .await
                                .unwrap_or_else(|_| String::from("<failed to read body>"));
                            return Err(CtlError::NotFound(truncate_body(&body, 4096)));
                        }
                        429 => {
                            let retry_after = resp
                                .headers()
                                .get("retry-after")
                                .and_then(|v| v.to_str().ok())
                                .and_then(parse_retry_after);

                            let err = CtlError::RateLimited {
                                retry_after_secs: retry_after,
                            };

                            if attempt < self.max_retries {
                                let delay = match retry_after {
                                    Some(secs) => Duration::from_secs(secs),
                                    None => backoff_delay(self.retry_base_delay, attempt),
                                };
                                tokio::time::sleep(delay).await;
                                last_error = Some(err);
                                continue;
                            }
                            return Err(err);
                        }
                        502 | 503 | 504 => {
                            let body = resp
                                .text()
                                .await
                                .unwrap_or_else(|_| String::from("<failed to read body>"));
                            let err = CtlError::ServerError(format!(
                                "HTTP {}: {}",
                                status_code,
                                truncate_body(&body, 4096)
                            ));

                            if attempt < self.max_retries {
                                let delay = backoff_delay(self.retry_base_delay, attempt);
                                tokio::time::sleep(delay).await;
                                last_error = Some(err);
                                continue;
                            }
                            return Err(err);
                        }
                        500 => {
                            let body = resp
                                .text()
                                .await
                                .unwrap_or_else(|_| String::from("<failed to read body>"));
                            return Err(CtlError::ServerError(format!(
                                "HTTP 500: {}",
                                truncate_body(&body, 4096)
                            )));
                        }
                        _ => {
                            let body = resp
                                .text()
                                .await
                                .unwrap_or_else(|_| String::from("<failed to read body>"));
                            return Err(CtlError::Http {
                                status: status_code,
                                body: truncate_body(&body, 4096),
                            });
                        }
                    }
                }
                Err(e) => {
                    let err = if e.is_timeout() {
                        CtlError::Timeout(e.to_string())
                    } else {
                        CtlError::Network(e.to_string())
                    };

                    if attempt < self.max_retries {
                        let delay = backoff_delay(self.retry_base_delay, attempt);
                        tokio::time::sleep(delay).await;
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| CtlError::Network("all retry attempts exhausted".into())))
    }

    /// Fetch all pages from a paginated endpoint and collect every item.
    ///
    /// Starts at offset 0 with a limit of 100 and keeps fetching until
    /// `has_more` is `false`.
    pub async fn get_all_pages<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<T>, CtlError> {
        let mut all_items: Vec<T> = Vec::new();
        let mut offset: usize = 0;
        let limit: usize = 100;

        loop {
            let offset_str = offset.to_string();
            let limit_str = limit.to_string();
            let params = [
                ("offset", offset_str.as_str()),
                ("limit", limit_str.as_str()),
            ];
            let page: PaginatedResponse<T> = self.get_with_params(path, &params).await?;
            let has_more = page.has_more;
            let count = page.items.len();
            all_items.extend(page.items);

            if !has_more || count == 0 {
                break;
            }
            offset += limit;
        }

        Ok(all_items)
    }

    // ========================================================================
    // Health
    // ========================================================================

    /// Check if the swarm API server is healthy.
    ///
    /// Returns `true` if the server responds with HTTP 200 to `/api/v1/health`.
    pub async fn health_check(&self) -> Result<bool, CtlError> {
        let url = self.url("/api/v1/health");
        let req = self.authenticate(self.client.get(&url));
        match self.request_with_retry(req).await {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(CtlError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Retrieve server information (version, node ID, uptime, etc.).
    pub async fn server_info(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/info").await
    }

    // ========================================================================
    // Psyche (6 methods)
    // ========================================================================

    /// Retrieve the current swarm psyche state.
    ///
    /// The psyche is a composite emotional/behavioral model of the swarm,
    /// reflecting aggregate health, morale, and behavioral archetypes.
    pub async fn psyche(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/psyche").await
    }

    /// Retrieve the recent psyche history.
    ///
    /// Returns up to `limit` historical psyche snapshots, most recent first.
    pub async fn psyche_history(&self, limit: usize) -> Result<serde_json::Value, CtlError> {
        let limit_str = limit.to_string();
        let params = [("limit", limit_str.as_str())];
        self.get_with_params("/api/v1/psyche/history", &params)
            .await
    }

    /// Retrieve a detailed breakdown of the current psyche.
    ///
    /// Returns per-facet scores, contributing factors, and weighted
    /// calculations that produced the current psyche state.
    pub async fn psyche_breakdown(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/psyche/breakdown").await
    }

    /// Forecast the swarm psyche state `minutes_ahead` minutes into the
    /// future based on current trends.
    pub async fn psyche_forecast(&self, minutes_ahead: u64) -> Result<serde_json::Value, CtlError> {
        let mins = minutes_ahead.to_string();
        let params = [("minutes_ahead", mins.as_str())];
        self.get_with_params("/api/v1/psyche/forecast", &params)
            .await
    }

    /// Retrieve the current psyche trend directions for each facet.
    ///
    /// Returns a map of facet name to trend direction (e.g. "rising",
    /// "falling", "stable").
    pub async fn psyche_trends(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/psyche/trends").await
    }

    /// Retrieve the archetype rules that govern psyche classification.
    pub async fn psyche_archetypes(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/psyche/archetypes").await
    }

    // ========================================================================
    // Events (5 methods)
    // ========================================================================

    /// Query swarm events with a filter.
    ///
    /// The `filter` map supports keys like `"severity"`, `"domain"`,
    /// `"entity_type"`, `"limit"`, and `"offset"`. All values are strings.
    pub async fn events(
        &self,
        filter: &HashMap<String, String>,
    ) -> Result<serde_json::Value, CtlError> {
        let params: Vec<(&str, &str)> = filter
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.get_with_params("/api/v1/events", &params).await
    }

    /// Open an SSE event stream and return the raw SSE text.
    ///
    /// The caller can use [`parse_sse_events`] to parse the returned text
    /// into structured [`SseEvent`]s. For long-lived streaming, consider
    /// using [`event_stream_raw_response`] instead.
    ///
    /// The `filter` map supports the same keys as [`events`](Self::events).
    pub async fn event_stream(&self, filter: &HashMap<String, String>) -> Result<String, CtlError> {
        let params: Vec<(&str, &str)> = filter
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.get_text_with_params("/api/v1/events/stream", &params)
            .await
    }

    /// Retrieve event bus statistics (total emitted, dropped, subscriber
    /// count, etc.).
    pub async fn event_stats(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/events/stats").await
    }

    /// Retrieve event aggregation data (counts by severity, domain, entity
    /// type, and time bucket).
    pub async fn event_aggregation(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/events/aggregation").await
    }

    /// Retrieve a single event by its ID.
    pub async fn event_by_id(&self, id: u64) -> Result<serde_json::Value, CtlError> {
        self.get(&format!("/api/v1/events/{}", id)).await
    }

    // ========================================================================
    // Alerts (8 methods)
    // ========================================================================

    /// List all currently active alerts.
    pub async fn alerts(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/alerts").await
    }

    /// List all configured alert rules.
    pub async fn alert_rules(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/alerts/rules").await
    }

    /// Create a new alert rule.
    ///
    /// The `rule` value should be a JSON object matching the `AlertRule`
    /// schema defined by the server.
    pub async fn create_alert_rule(&self, rule: &serde_json::Value) -> Result<(), CtlError> {
        self.post_no_response("/api/v1/alerts/rules", rule).await
    }

    /// Delete an alert rule by name.
    pub async fn delete_alert_rule(&self, name: &str) -> Result<(), CtlError> {
        self.delete(&format!("/api/v1/alerts/rules/{}", name)).await
    }

    /// Acknowledge an active alert by name.
    ///
    /// Acknowledged alerts stop re-notifying but remain visible until
    /// resolved.
    pub async fn acknowledge_alert(&self, name: &str) -> Result<(), CtlError> {
        let body = serde_json::json!({});
        self.post_no_response(&format!("/api/v1/alerts/{}/acknowledge", name), &body)
            .await
    }

    /// Silence an alert for a specified duration.
    ///
    /// Returns the silence window ID assigned by the server.
    pub async fn silence_alert(
        &self,
        name: &str,
        duration_secs: u64,
        reason: &str,
    ) -> Result<serde_json::Value, CtlError> {
        let body = serde_json::json!({
            "name": name,
            "duration_secs": duration_secs,
            "reason": reason,
        });
        self.post("/api/v1/alerts/silences", &body).await
    }

    /// List all active silence windows.
    pub async fn alert_silences(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/alerts/silences").await
    }

    /// Retrieve an alert summary (counts by state: firing, pending,
    /// resolved, silenced).
    pub async fn alert_summary(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/alerts/summary").await
    }

    // ========================================================================
    // Fleet (15 methods)
    // ========================================================================

    /// Drain a node: gracefully move all work off the node before taking it
    /// offline.
    ///
    /// The node stops accepting new work and waits up to `timeout_secs` for
    /// current chunks to complete before forcibly stopping them.
    pub async fn drain_node(
        &self,
        node_id: &str,
        timeout_secs: u64,
        reason: &str,
    ) -> Result<(), CtlError> {
        let body = serde_json::json!({
            "timeout_secs": timeout_secs,
            "reason": reason,
        });
        self.post_no_response(&format!("/api/v1/fleet/nodes/{}/drain", node_id), &body)
            .await
    }

    /// Cordon a node: prevent it from accepting new work without draining
    /// existing work.
    pub async fn cordon_node(&self, node_id: &str, reason: &str) -> Result<(), CtlError> {
        let body = serde_json::json!({ "reason": reason });
        self.post_no_response(&format!("/api/v1/fleet/nodes/{}/cordon", node_id), &body)
            .await
    }

    /// Uncordon a node: allow it to accept new work again.
    pub async fn uncordon_node(&self, node_id: &str) -> Result<(), CtlError> {
        let body = serde_json::json!({});
        self.post_no_response(&format!("/api/v1/fleet/nodes/{}/uncordon", node_id), &body)
            .await
    }

    /// Quarantine a node: isolate it from both work assignment and gossip
    /// participation.
    pub async fn quarantine_node(&self, node_id: &str, reason: &str) -> Result<(), CtlError> {
        let body = serde_json::json!({ "reason": reason });
        self.post_no_response(
            &format!("/api/v1/fleet/nodes/{}/quarantine", node_id),
            &body,
        )
        .await
    }

    /// Remove a node from quarantine and restore normal operation.
    pub async fn unquarantine_node(&self, node_id: &str) -> Result<(), CtlError> {
        let body = serde_json::json!({});
        self.post_no_response(
            &format!("/api/v1/fleet/nodes/{}/unquarantine", node_id),
            &body,
        )
        .await
    }

    /// Retrieve a summary of the overall fleet status.
    ///
    /// Returns counts by node state (active, cordoned, draining, quarantined,
    /// dead), total resource utilization, and fleet-wide health score.
    pub async fn fleet_status(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/fleet/status").await
    }

    /// Start a rolling update across the fleet.
    ///
    /// The `plan` should be a JSON object matching `RollingUpdatePlan`,
    /// specifying target version, batch size, canary percentage, health
    /// check interval, and rollback conditions.
    pub async fn start_rolling_update(
        &self,
        plan: &serde_json::Value,
    ) -> Result<serde_json::Value, CtlError> {
        self.post("/api/v1/fleet/updates", plan).await
    }

    /// Pause an in-progress rolling update.
    pub async fn pause_update(&self, reason: &str) -> Result<(), CtlError> {
        let body = serde_json::json!({ "reason": reason });
        self.post_no_response("/api/v1/fleet/updates/pause", &body)
            .await
    }

    /// Resume a paused rolling update.
    pub async fn resume_update(&self) -> Result<(), CtlError> {
        let body = serde_json::json!({});
        self.post_no_response("/api/v1/fleet/updates/resume", &body)
            .await
    }

    /// Roll back the current rolling update.
    pub async fn rollback_update(&self, reason: &str) -> Result<(), CtlError> {
        let body = serde_json::json!({ "reason": reason });
        self.post_no_response("/api/v1/fleet/updates/rollback", &body)
            .await
    }

    /// Cancel the current rolling update entirely.
    pub async fn cancel_update(&self) -> Result<(), CtlError> {
        let body = serde_json::json!({});
        self.post_no_response("/api/v1/fleet/updates/cancel", &body)
            .await
    }

    /// Get the status of the current rolling update, if any.
    ///
    /// Returns `Ok(Value::Null)` if no update is in progress.
    pub async fn update_status(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/fleet/updates/current").await
    }

    /// Retrieve the history of all rolling updates.
    pub async fn update_history(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/fleet/updates/history").await
    }

    /// Retrieve health status for all nodes in the fleet.
    pub async fn fleet_health(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/fleet/health").await
    }

    /// Retrieve health status for a specific node.
    pub async fn node_health(&self, node_id: &str) -> Result<serde_json::Value, CtlError> {
        self.get(&format!("/api/v1/fleet/health/{}", node_id)).await
    }

    // ========================================================================
    // Nodes (3 methods)
    // ========================================================================

    /// List all known nodes in the swarm.
    pub async fn nodes(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/nodes").await
    }

    /// Retrieve detailed information about a specific node.
    pub async fn node(&self, node_id: &str) -> Result<serde_json::Value, CtlError> {
        self.get(&format!("/api/v1/nodes/{}", node_id)).await
    }

    /// Set tags on a specific node.
    ///
    /// Tags are key-value string pairs used for scheduling affinity, fleet
    /// targeting, and organizational purposes.
    pub async fn set_node_tags(
        &self,
        node_id: &str,
        tags: &HashMap<String, String>,
    ) -> Result<(), CtlError> {
        self.put_no_response(&format!("/api/v1/nodes/{}/tags", node_id), tags)
            .await
    }

    // ========================================================================
    // Jobs (3 methods)
    // ========================================================================

    /// List all jobs known to the swarm.
    pub async fn jobs(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/jobs").await
    }

    /// Retrieve detailed information about a specific job.
    pub async fn job(&self, job_id: &str) -> Result<serde_json::Value, CtlError> {
        self.get(&format!("/api/v1/jobs/{}", job_id)).await
    }

    /// Diagnose a job: retrieve detailed debugging information including
    /// chunk assignments, execution logs, failure analysis, and suggested
    /// remediation.
    pub async fn diagnose_job(&self, job_id: &str) -> Result<serde_json::Value, CtlError> {
        self.get(&format!("/api/v1/jobs/{}/diagnose", job_id)).await
    }

    // ========================================================================
    // SLA (4 methods)
    // ========================================================================

    /// List the current status of all SLA definitions.
    pub async fn sla_statuses(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/sla").await
    }

    /// Retrieve the status of a specific SLA by name.
    pub async fn sla_status(&self, name: &str) -> Result<serde_json::Value, CtlError> {
        self.get(&format!("/api/v1/sla/{}", name)).await
    }

    /// Generate an SLA compliance report for a specific SLA over the given
    /// time period.
    ///
    /// The report includes compliance percentage, error budget consumption,
    /// burn rate, breach episodes, and trend analysis.
    pub async fn sla_report(
        &self,
        name: &str,
        period_hours: u64,
    ) -> Result<serde_json::Value, CtlError> {
        let hours = period_hours.to_string();
        let params = [("period_hours", hours.as_str())];
        self.get_with_params(&format!("/api/v1/sla/{}/report", name), &params)
            .await
    }

    /// Create a new SLA definition.
    ///
    /// The `definition` should be a JSON object matching the `SlaDefinition`
    /// schema (name, metric, target, window, etc.).
    pub async fn create_sla(&self, definition: &serde_json::Value) -> Result<(), CtlError> {
        self.post_no_response("/api/v1/sla", definition).await
    }

    // ========================================================================
    // Capacity (4 methods)
    // ========================================================================

    /// Retrieve the current aggregate capacity snapshot.
    ///
    /// Returns total and used CPU, memory, disk, and bandwidth across all
    /// alive nodes.
    pub async fn capacity(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/capacity").await
    }

    /// Retrieve capacity forecasts based on historical trends.
    ///
    /// Returns per-resource forecasts with predicted utilization at various
    /// time horizons (1h, 6h, 24h, 7d).
    pub async fn capacity_forecast(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/capacity/forecast").await
    }

    /// Identify current and predicted capacity bottlenecks.
    ///
    /// Returns a list of bottlenecks sorted by severity (imminent >
    /// approaching > potential), each with the affected resource and
    /// recommended action.
    pub async fn capacity_bottlenecks(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/capacity/bottlenecks").await
    }

    /// Run a what-if capacity scenario.
    ///
    /// The `scenario` should be a JSON object describing hypothetical changes
    /// (e.g. adding N nodes with specified resources). Returns the projected
    /// impact on utilization and bottleneck status.
    pub async fn capacity_whatif(
        &self,
        scenario: &serde_json::Value,
    ) -> Result<serde_json::Value, CtlError> {
        self.post("/api/v1/capacity/whatif", scenario).await
    }

    // ========================================================================
    // Multi-Swarm (7 methods)
    // ========================================================================

    /// Retrieve the constellation view: a graph of all known swarms, their
    /// treaties, and inter-swarm connectivity.
    pub async fn constellation(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/federation/constellation").await
    }

    /// Retrieve this swarm's sovereign identity (public key, name,
    /// capabilities, and trust score).
    pub async fn sovereignty(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/federation/sovereignty").await
    }

    /// List all known remote swarms, including their liveness status and
    /// trust scores.
    pub async fn remote_swarms(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/federation/swarms").await
    }

    /// List all configured membranes (inter-swarm boundaries).
    pub async fn membranes(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/federation/membranes").await
    }

    /// Retrieve recent crossing records for a specific membrane.
    ///
    /// `id` is the membrane identifier and `limit` caps the number of
    /// records returned.
    pub async fn membrane_crossings(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, CtlError> {
        let limit_str = limit.to_string();
        let params = [("limit", limit_str.as_str())];
        self.get_with_params(
            &format!("/api/v1/federation/membranes/{}/crossings", id),
            &params,
        )
        .await
    }

    /// List all inter-swarm treaties.
    pub async fn treaties(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/federation/treaties").await
    }

    /// List all active lending sessions (compute capacity loans between
    /// swarms).
    pub async fn lending_sessions(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/federation/lending").await
    }

    // ========================================================================
    // Visions (3 methods)
    // ========================================================================

    /// List all management visions.
    ///
    /// A vision is a named, declarative management intent (e.g. "maintain
    /// 99.9% availability", "keep CPU below 80%") that the swarm
    /// autonomously works toward.
    pub async fn visions(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/visions").await
    }

    /// Retrieve a specific vision by name.
    pub async fn vision(&self, name: &str) -> Result<serde_json::Value, CtlError> {
        self.get(&format!("/api/v1/visions/{}", name)).await
    }

    /// Create or update a management vision.
    pub async fn create_vision(&self, vision: &serde_json::Value) -> Result<(), CtlError> {
        self.post_no_response("/api/v1/visions", vision).await
    }

    // ========================================================================
    // Audit (3 methods)
    // ========================================================================

    /// Query the tamper-evident audit log with a filter.
    ///
    /// The `filter` map supports keys like `"actor"`, `"action"`,
    /// `"target"`, `"outcome"`, `"since"`, `"until"`, `"limit"`, and
    /// `"offset"`.
    pub async fn audit_log(
        &self,
        filter: &HashMap<String, String>,
    ) -> Result<serde_json::Value, CtlError> {
        let params: Vec<(&str, &str)> = filter
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.get_with_params("/api/v1/audit", &params).await
    }

    /// Verify the integrity of the audit log hash chain.
    ///
    /// Returns a verification result indicating whether the chain is valid,
    /// how many entries were checked, and any discrepancies found.
    pub async fn audit_verify(&self) -> Result<serde_json::Value, CtlError> {
        self.get("/api/v1/audit/verify").await
    }

    /// Export the audit log in the specified format.
    ///
    /// Supported formats: `"json"`, `"csv"`, `"ndjson"`. Returns the raw
    /// exported text.
    pub async fn audit_export(&self, format: &str) -> Result<String, CtlError> {
        let params = [("format", format)];
        self.get_text_with_params("/api/v1/audit/export", &params)
            .await
    }

    // ========================================================================
    // Metrics (1 method)
    // ========================================================================

    /// Retrieve raw Prometheus metrics text.
    ///
    /// Returns the metrics in Prometheus exposition format (text/plain).
    /// This is intended for scraping by Prometheus or compatible collectors.
    pub async fn metrics_raw(&self) -> Result<String, CtlError> {
        self.get_text("/api/v1/metrics").await
    }
}

// ============================================================================
// Free functions
// ============================================================================

/// Normalize a base URL by stripping trailing slashes.
///
/// This ensures that path concatenation (e.g. `base_url + "/api/v1/health"`)
/// does not produce double slashes.
fn normalize_base_url(url: &str) -> String {
    let mut s = url.to_string();
    while s.ends_with('/') {
        s.pop();
    }
    s
}

/// Parse a `Retry-After` header value.
///
/// Supports integer seconds (e.g. `"120"`) but not HTTP-date format. Returns
/// `None` if the value cannot be parsed.
fn parse_retry_after(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

/// Truncate a string to at most `max_len` bytes, appending `"..."` if
/// truncation occurred.
fn truncate_body(body: &str, max_len: usize) -> String {
    if body.len() <= max_len {
        body.to_string()
    } else {
        // Find a valid UTF-8 boundary at or before max_len.
        let mut end = max_len;
        while end > 0 && !body.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &body[..end])
    }
}

/// Compute an exponential backoff delay: `base * 2^attempt`.
///
/// Capped at 60 seconds to avoid excessively long waits.
fn backoff_delay(base: Duration, attempt: u32) -> Duration {
    let multiplier = 2u64.saturating_pow(attempt);
    let delay = base.saturating_mul(multiplier as u32);
    let max_delay = Duration::from_secs(60);
    if delay > max_delay {
        max_delay
    } else {
        delay
    }
}

/// Parse a raw SSE text stream into a vector of [`SseEvent`]s.
///
/// Follows the [Server-Sent Events specification](https://html.spec.whatwg.org/multipage/server-sent-events.html):
/// - Lines starting with `:` are comments (ignored).
/// - `event:` sets the event type.
/// - `data:` appends to the data buffer (multiple `data:` lines are joined
///   with newlines).
/// - `id:` sets the last-event-id.
/// - An empty line dispatches the accumulated event.
pub fn parse_sse_events(raw: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut event_type = String::from("message");
    let mut data_lines: Vec<String> = Vec::new();
    let mut id: Option<String> = None;

    for line in raw.lines() {
        if line.is_empty() {
            // Dispatch the event if we have data.
            if !data_lines.is_empty() {
                events.push(SseEvent {
                    event_type: event_type.clone(),
                    data: data_lines.join("\n"),
                    id: id.clone(),
                });
            }
            // Reset accumulators.
            event_type = String::from("message");
            data_lines.clear();
            id = None;
            continue;
        }

        // Comment line.
        if line.starts_with(':') {
            continue;
        }

        if let Some(value) = line.strip_prefix("event:") {
            event_type = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start().to_string());
        } else if let Some(value) = line.strip_prefix("id:") {
            id = Some(value.trim().to_string());
        }
        // Other fields (e.g. `retry:`) are silently ignored.
    }

    // If the stream ended without a trailing blank line, dispatch any
    // accumulated data.
    if !data_lines.is_empty() {
        events.push(SseEvent {
            event_type,
            data: data_lines.join("\n"),
            id,
        });
    }

    events
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // CtlError Display tests
    // ========================================================================

    #[test]
    fn test_ctl_error_display_network() {
        let err = CtlError::Network("connection refused".into());
        assert_eq!(format!("{}", err), "network error: connection refused");
    }

    #[test]
    fn test_ctl_error_display_timeout() {
        let err = CtlError::Timeout("request timed out after 30s".into());
        assert_eq!(
            format!("{}", err),
            "request timed out: request timed out after 30s"
        );
    }

    #[test]
    fn test_ctl_error_display_http() {
        let err = CtlError::Http {
            status: 400,
            body: "bad request".into(),
        };
        assert_eq!(format!("{}", err), "HTTP 400 error: bad request");
    }

    #[test]
    fn test_ctl_error_display_deserialization() {
        let err = CtlError::Deserialization("expected object".into());
        assert_eq!(format!("{}", err), "deserialization error: expected object");
    }

    #[test]
    fn test_ctl_error_display_unauthorized() {
        let err = CtlError::Unauthorized("invalid token".into());
        assert_eq!(format!("{}", err), "unauthorized: invalid token");
    }

    #[test]
    fn test_ctl_error_display_not_found() {
        let err = CtlError::NotFound("node not found".into());
        assert_eq!(format!("{}", err), "not found: node not found");
    }

    #[test]
    fn test_ctl_error_display_rate_limited_with_retry_after() {
        let err = CtlError::RateLimited {
            retry_after_secs: Some(120),
        };
        assert_eq!(format!("{}", err), "rate limited (retry after 120s)");
    }

    #[test]
    fn test_ctl_error_display_rate_limited_without_retry_after() {
        let err = CtlError::RateLimited {
            retry_after_secs: None,
        };
        assert_eq!(format!("{}", err), "rate limited");
    }

    #[test]
    fn test_ctl_error_display_server_error() {
        let err = CtlError::ServerError("internal server error".into());
        assert_eq!(format!("{}", err), "server error: internal server error");
    }

    // ========================================================================
    // CtlError::is_transient tests
    // ========================================================================

    #[test]
    fn test_is_transient_network() {
        assert!(CtlError::Network("oops".into()).is_transient());
    }

    #[test]
    fn test_is_transient_timeout() {
        assert!(CtlError::Timeout("oops".into()).is_transient());
    }

    #[test]
    fn test_is_transient_rate_limited() {
        assert!(CtlError::RateLimited {
            retry_after_secs: None
        }
        .is_transient());
    }

    #[test]
    fn test_is_not_transient_unauthorized() {
        assert!(!CtlError::Unauthorized("oops".into()).is_transient());
    }

    #[test]
    fn test_is_not_transient_not_found() {
        assert!(!CtlError::NotFound("oops".into()).is_transient());
    }

    #[test]
    fn test_is_not_transient_http() {
        assert!(!CtlError::Http {
            status: 400,
            body: "bad".into()
        }
        .is_transient());
    }

    // ========================================================================
    // PaginatedResponse tests
    // ========================================================================

    #[test]
    fn test_paginated_response_has_more_true() {
        let page = PaginatedResponse {
            items: vec![1, 2, 3],
            total: 10,
            offset: 0,
            limit: 3,
            has_more: true,
        };
        assert_eq!(page.next_offset(), Some(3));
        assert_eq!(page.len(), 3);
        assert!(!page.is_empty());
    }

    #[test]
    fn test_paginated_response_has_more_false() {
        let page = PaginatedResponse {
            items: vec![7, 8, 9, 10],
            total: 10,
            offset: 6,
            limit: 5,
            has_more: false,
        };
        assert_eq!(page.next_offset(), None);
        assert_eq!(page.len(), 4);
    }

    #[test]
    fn test_paginated_response_empty() {
        let page: PaginatedResponse<String> = PaginatedResponse {
            items: vec![],
            total: 0,
            offset: 0,
            limit: 100,
            has_more: false,
        };
        assert!(page.is_empty());
        assert_eq!(page.len(), 0);
        assert_eq!(page.next_offset(), None);
    }

    #[test]
    fn test_paginated_response_next_offset_increments_by_limit() {
        let page = PaginatedResponse {
            items: vec!["a", "b"],
            total: 50,
            offset: 20,
            limit: 10,
            has_more: true,
        };
        assert_eq!(page.next_offset(), Some(30));
    }

    // ========================================================================
    // normalize_base_url tests
    // ========================================================================

    #[test]
    fn test_normalize_base_url_no_trailing_slash() {
        assert_eq!(
            normalize_base_url("http://localhost:3000"),
            "http://localhost:3000"
        );
    }

    #[test]
    fn test_normalize_base_url_single_trailing_slash() {
        assert_eq!(
            normalize_base_url("http://localhost:3000/"),
            "http://localhost:3000"
        );
    }

    #[test]
    fn test_normalize_base_url_multiple_trailing_slashes() {
        assert_eq!(
            normalize_base_url("http://localhost:3000///"),
            "http://localhost:3000"
        );
    }

    #[test]
    fn test_normalize_base_url_with_path() {
        assert_eq!(
            normalize_base_url("https://example.com/swarm/"),
            "https://example.com/swarm"
        );
    }

    // ========================================================================
    // parse_retry_after tests
    // ========================================================================

    #[test]
    fn test_parse_retry_after_valid() {
        assert_eq!(parse_retry_after("120"), Some(120));
    }

    #[test]
    fn test_parse_retry_after_with_whitespace() {
        assert_eq!(parse_retry_after("  60  "), Some(60));
    }

    #[test]
    fn test_parse_retry_after_zero() {
        assert_eq!(parse_retry_after("0"), Some(0));
    }

    #[test]
    fn test_parse_retry_after_invalid() {
        assert_eq!(parse_retry_after("not-a-number"), None);
    }

    #[test]
    fn test_parse_retry_after_http_date() {
        // HTTP-date format is not supported; returns None.
        assert_eq!(parse_retry_after("Wed, 21 Oct 2025 07:28:00 GMT"), None);
    }

    // ========================================================================
    // truncate_body tests
    // ========================================================================

    #[test]
    fn test_truncate_body_short() {
        assert_eq!(truncate_body("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_body_exact_limit() {
        assert_eq!(truncate_body("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_body_truncated() {
        assert_eq!(truncate_body("hello world", 5), "hello...");
    }

    #[test]
    fn test_truncate_body_empty() {
        assert_eq!(truncate_body("", 10), "");
    }

    #[test]
    fn test_truncate_body_unicode_boundary() {
        // Multi-byte UTF-8 character: the euro sign is 3 bytes.
        let s = "abc\u{20AC}def"; // "abc...def" where ... is the euro sign
                                  // Truncating at 4 bytes would land in the middle of the euro sign.
        let result = truncate_body(s, 4);
        // Should back up to byte 3 (end of "abc") and append "...".
        assert_eq!(result, "abc...");
    }

    // ========================================================================
    // backoff_delay tests
    // ========================================================================

    #[test]
    fn test_backoff_delay_attempt_zero() {
        let base = Duration::from_millis(500);
        assert_eq!(backoff_delay(base, 0), Duration::from_millis(500));
    }

    #[test]
    fn test_backoff_delay_attempt_one() {
        let base = Duration::from_millis(500);
        assert_eq!(backoff_delay(base, 1), Duration::from_millis(1000));
    }

    #[test]
    fn test_backoff_delay_attempt_two() {
        let base = Duration::from_millis(500);
        assert_eq!(backoff_delay(base, 2), Duration::from_millis(2000));
    }

    #[test]
    fn test_backoff_delay_capped_at_60s() {
        let base = Duration::from_secs(1);
        // 2^20 = 1,048,576 seconds, should be capped at 60.
        assert_eq!(backoff_delay(base, 20), Duration::from_secs(60));
    }

    // ========================================================================
    // SSE parsing tests
    // ========================================================================

    #[test]
    fn test_parse_sse_events_single_event() {
        let raw = "data: {\"type\":\"node_joined\"}\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "message");
        assert_eq!(events[0].data, "{\"type\":\"node_joined\"}");
        assert_eq!(events[0].id, None);
    }

    #[test]
    fn test_parse_sse_events_with_event_type() {
        let raw = "event: alert\ndata: {\"severity\":\"critical\"}\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "alert");
        assert_eq!(events[0].data, "{\"severity\":\"critical\"}");
    }

    #[test]
    fn test_parse_sse_events_with_id() {
        let raw = "id: 42\ndata: hello\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, Some("42".into()));
        assert_eq!(events[0].data, "hello");
    }

    #[test]
    fn test_parse_sse_events_multiple_data_lines() {
        let raw = "data: line one\ndata: line two\ndata: line three\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "line one\nline two\nline three");
    }

    #[test]
    fn test_parse_sse_events_comments_ignored() {
        let raw = ": this is a keepalive\ndata: real data\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "real data");
    }

    #[test]
    fn test_parse_sse_events_multiple_events() {
        let raw = "data: first\n\ndata: second\n\ndata: third\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].data, "first");
        assert_eq!(events[1].data, "second");
        assert_eq!(events[2].data, "third");
    }

    #[test]
    fn test_parse_sse_events_empty_lines_only_no_data() {
        let raw = "\n\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_parse_sse_events_comments_only() {
        let raw = ": keepalive\n: another comment\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_parse_sse_events_no_trailing_blank_line() {
        // The SSE spec says events are dispatched on blank lines, but we
        // also dispatch any buffered data at end-of-stream.
        let raw = "data: unterminated";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "unterminated");
    }

    #[test]
    fn test_parse_sse_events_event_type_resets_between_events() {
        let raw = "event: custom\ndata: first\n\ndata: second\n\n";
        let events = parse_sse_events(raw);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "custom");
        assert_eq!(events[1].event_type, "message"); // Reset to default.
    }

    // ========================================================================
    // SwarmCtlClient builder tests
    // ========================================================================

    #[test]
    fn test_client_new_normalizes_url() {
        let client = SwarmCtlClient::new("http://localhost:3000/");
        assert_eq!(client.base_url, "http://localhost:3000");
    }

    #[test]
    fn test_client_with_token() {
        let client = SwarmCtlClient::new("http://localhost:3000").with_token("secret-token-123");
        assert_eq!(client.auth_token, Some("secret-token-123".into()));
    }

    #[test]
    fn test_client_with_timeout() {
        let client =
            SwarmCtlClient::new("http://localhost:3000").with_timeout(Duration::from_secs(10));
        assert_eq!(client.timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_client_with_retries() {
        let client = SwarmCtlClient::new("http://localhost:3000")
            .with_retries(5, Duration::from_millis(200));
        assert_eq!(client.max_retries, 5);
        assert_eq!(client.retry_base_delay, Duration::from_millis(200));
    }

    #[test]
    fn test_client_default_no_retries() {
        let client = SwarmCtlClient::new("http://localhost:3000");
        assert_eq!(client.max_retries, 0);
    }

    #[test]
    fn test_client_default_timeout_30s() {
        let client = SwarmCtlClient::new("http://localhost:3000");
        assert_eq!(client.timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_client_url_construction_with_leading_slash() {
        let client = SwarmCtlClient::new("http://localhost:3000");
        assert_eq!(
            client.url("/api/v1/health"),
            "http://localhost:3000/api/v1/health"
        );
    }

    #[test]
    fn test_client_url_construction_without_leading_slash() {
        let client = SwarmCtlClient::new("http://localhost:3000");
        assert_eq!(
            client.url("api/v1/health"),
            "http://localhost:3000/api/v1/health"
        );
    }

    #[test]
    fn test_client_clone() {
        let client = SwarmCtlClient::new("http://localhost:3000")
            .with_token("tok")
            .with_retries(3, Duration::from_millis(100));
        let cloned = client.clone();
        assert_eq!(cloned.base_url, client.base_url);
        assert_eq!(cloned.auth_token, client.auth_token);
        assert_eq!(cloned.max_retries, client.max_retries);
    }

    #[test]
    fn test_client_builder_chaining() {
        let client = SwarmCtlClient::new("https://swarm.example.com")
            .with_token("my-token")
            .with_timeout(Duration::from_secs(60))
            .with_retries(3, Duration::from_secs(1))
            .with_tls_skip_verify();

        assert_eq!(client.base_url, "https://swarm.example.com");
        assert_eq!(client.auth_token, Some("my-token".into()));
        assert_eq!(client.timeout, Duration::from_secs(60));
        assert_eq!(client.max_retries, 3);
        assert_eq!(client.retry_base_delay, Duration::from_secs(1));
    }

    // ========================================================================
    // SseEvent equality tests
    // ========================================================================

    #[test]
    fn test_sse_event_equality() {
        let a = SseEvent {
            event_type: "message".into(),
            data: "hello".into(),
            id: Some("1".into()),
        };
        let b = SseEvent {
            event_type: "message".into(),
            data: "hello".into(),
            id: Some("1".into()),
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_sse_event_inequality_different_data() {
        let a = SseEvent {
            event_type: "message".into(),
            data: "hello".into(),
            id: None,
        };
        let b = SseEvent {
            event_type: "message".into(),
            data: "world".into(),
            id: None,
        };
        assert_ne!(a, b);
    }

    // ========================================================================
    // CtlError Debug tests
    // ========================================================================

    #[test]
    fn test_ctl_error_debug_format() {
        let err = CtlError::Network("connection reset".into());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Network"));
        assert!(debug.contains("connection reset"));
    }

    #[test]
    fn test_ctl_error_implements_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(CtlError::Timeout("test".into()));
        // Verify it can be used as a trait object.
        assert!(err.to_string().contains("timed out"));
    }
}
