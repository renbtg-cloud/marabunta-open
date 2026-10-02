// Marabunta - Licensed under the MIT License.
//! Webhook delivery system for the Marabunta Swarm.
//!
//! Delivers job progress milestones, completion notifications, error reports,
//! and custom events to customer-provided URLs via HTTP POST. Supports
//! exponential backoff with jitter, HMAC-SHA256 payload signing, concurrency
//! limiting, and per-delivery status tracking.
//!
//! # Architecture
//!
//! ```text
//!   trigger_*()
//!       │
//!       ▼
//!   WebhookEngine ──► mpsc::Sender<WebhookTask>
//!                          │
//!                          ▼
//!                   delivery_loop (spawned)
//!                          │
//!                    ┌─────┴──────┐
//!                    │  Semaphore  │  (max_concurrent)
//!                    └─────┬──────┘
//!                          │
//!                    reqwest::Client::post()
//!                          │
//!                    ┌─────┴──────┐
//!                    │  success?  │
//!                    └─────┬──────┘
//!                     yes/  \no
//!                    ▼       ▼
//!               Delivered   schedule_retry (backoff + jitter)
//! ```
//!
//! All delivery state is stored in a `DashMap<String, WebhookDelivery>` that
//! is safe for concurrent reads from the API layer while the delivery loop
//! writes attempt results.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, info, warn, error};
use uuid::Uuid;

use crate::common::types::JobId;
use super::types::{WebhookConfig, WebhookDeliveryStatus};
use super::config::*;

// ============================================================================
// Constants
// ============================================================================

/// Maximum response body bytes captured per attempt.
const MAX_RESPONSE_BODY_BYTES: usize = 1024;

/// Default channel capacity for the internal task queue.
const TASK_CHANNEL_CAPACITY: usize = 4096;

/// Default maximum payload size before rejection (1 MB).
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1_048_576;

/// Default backoff maximum (10 minutes).
const DEFAULT_BACKOFF_MAX_SECS: u64 = 600;

/// User-Agent header sent with all webhook deliveries.
const WEBHOOK_USER_AGENT: &str = "MarabuntaSwarm-Webhook/1.0";

/// Header name for the HMAC signature.
const SIGNATURE_HEADER: &str = "X-Webhook-Signature";

/// Header name for the event type.
const EVENT_TYPE_HEADER: &str = "X-Webhook-Event";

/// Header name for the delivery ID.
const DELIVERY_ID_HEADER: &str = "X-Webhook-Delivery";

/// Header name for the timestamp.
const TIMESTAMP_HEADER: &str = "X-Webhook-Timestamp";

/// HMAC inner padding byte.
const HMAC_IPAD: u8 = 0x36;

/// HMAC outer padding byte.
const HMAC_OPAD: u8 = 0x5c;

/// SHA-256 block size in bytes.
const SHA256_BLOCK_SIZE: usize = 64;

// ============================================================================
// WebhookEngineConfig
// ============================================================================

/// Runtime configuration for the webhook delivery engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEngineConfig {
    /// Maximum number of concurrent HTTP POST deliveries.
    pub max_concurrent: usize,
    /// Timeout per individual delivery request, in seconds.
    pub timeout_secs: u64,
    /// Maximum number of retry attempts per delivery.
    pub max_retries: u8,
    /// Base backoff interval in seconds (doubled on each retry).
    pub backoff_base_secs: u64,
    /// Maximum backoff interval in seconds (cap on exponential growth).
    pub backoff_max_secs: u64,
    /// Maximum payload size in bytes; deliveries exceeding this are rejected.
    pub max_payload_bytes: usize,
}

impl Default for WebhookEngineConfig {
    fn default() -> Self {
        Self {
            max_concurrent: WEBHOOK_MAX_CONCURRENT,
            timeout_secs: WEBHOOK_TIMEOUT_SECS,
            max_retries: WEBHOOK_MAX_RETRIES,
            backoff_base_secs: WEBHOOK_RETRY_BACKOFF_BASE_SECS,
            backoff_max_secs: DEFAULT_BACKOFF_MAX_SECS,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        }
    }
}

impl WebhookEngineConfig {
    /// Validate that the config values are sane. Returns a list of validation
    /// errors (empty if valid).
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.max_concurrent == 0 {
            errors.push("max_concurrent must be > 0".to_string());
        }
        if self.timeout_secs == 0 {
            errors.push("timeout_secs must be > 0".to_string());
        }
        if self.backoff_base_secs == 0 {
            errors.push("backoff_base_secs must be > 0".to_string());
        }
        if self.backoff_max_secs < self.backoff_base_secs {
            errors.push("backoff_max_secs must be >= backoff_base_secs".to_string());
        }
        if self.max_payload_bytes == 0 {
            errors.push("max_payload_bytes must be > 0".to_string());
        }
        errors
    }

    /// Returns the computed timeout as a Duration.
    pub fn timeout_duration(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }

    /// Returns the backoff duration for a given attempt number (0-indexed).
    /// Uses exponential backoff capped at `backoff_max_secs`.
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        let base = self.backoff_base_secs as f64;
        let max = self.backoff_max_secs as f64;
        let exp = base * 2.0_f64.powi(attempt as i32);
        let capped = exp.min(max);
        Duration::from_secs(capped as u64)
    }

    /// Returns the backoff duration with jitter for a given attempt number.
    /// Adds up to 50% random jitter to prevent thundering herd.
    pub fn backoff_with_jitter(&self, attempt: u32) -> Duration {
        let base_secs = {
            let base = self.backoff_base_secs as f64;
            let max = self.backoff_max_secs as f64;
            let exp = base * 2.0_f64.powi(attempt as i32);
            exp.min(max)
        };
        // Deterministic jitter based on attempt number (avoids needing RNG in tests)
        let jitter_frac = match attempt % 4 {
            0 => 0.1,
            1 => 0.3,
            2 => 0.2,
            _ => 0.4,
        };
        let jittered = base_secs * (1.0 + jitter_frac * 0.5);
        Duration::from_millis((jittered * 1000.0) as u64)
    }
}

// ============================================================================
// WebhookStats
// ============================================================================

/// Global statistics for the webhook delivery system.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebhookStats {
    /// Total number of webhook deliveries attempted.
    pub total_sent: u64,
    /// Total number of successful deliveries (HTTP 2xx response).
    pub total_delivered: u64,
    /// Total number of permanently failed deliveries (all retries exhausted).
    pub total_failed: u64,
    /// Total number of retry attempts across all deliveries.
    pub total_retried: u64,
    /// Running average delivery time in milliseconds.
    pub avg_delivery_time_ms: f64,
    /// Number of deliveries currently in-flight or pending retry.
    pub active_deliveries: u64,
}

impl WebhookStats {
    /// Record a successful delivery with the given duration.
    pub fn record_success(&mut self, duration_ms: u64) {
        self.total_sent += 1;
        self.total_delivered += 1;
        // Running average: new_avg = old_avg + (new_val - old_avg) / n
        let n = self.total_delivered as f64;
        self.avg_delivery_time_ms += (duration_ms as f64 - self.avg_delivery_time_ms) / n;
    }

    /// Record a failed delivery attempt.
    pub fn record_failure(&mut self) {
        self.total_sent += 1;
        self.total_failed += 1;
    }

    /// Record a retry attempt (does not increment total_sent).
    pub fn record_retry(&mut self) {
        self.total_retried += 1;
    }

    /// Increment active delivery counter.
    pub fn increment_active(&mut self) {
        self.active_deliveries += 1;
    }

    /// Decrement active delivery counter.
    pub fn decrement_active(&mut self) {
        self.active_deliveries = self.active_deliveries.saturating_sub(1);
    }

    /// Reset all counters to zero.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Returns the success rate as a fraction (0.0 - 1.0).
    pub fn success_rate(&self) -> f64 {
        let total = self.total_delivered + self.total_failed;
        if total == 0 {
            return 1.0;
        }
        self.total_delivered as f64 / total as f64
    }

    /// Returns the total number of delivery attempts (including retries).
    pub fn total_attempts(&self) -> u64 {
        self.total_sent + self.total_retried
    }
}

// ============================================================================
// DeliveryAttempt
// ============================================================================

/// Record of a single HTTP POST attempt within a delivery chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryAttempt {
    /// Which attempt this was (1-indexed).
    pub attempt_number: u32,
    /// When the HTTP request was sent.
    pub sent_at: DateTime<Utc>,
    /// HTTP status code if a response was received.
    pub response_status: Option<u16>,
    /// Response body (truncated to MAX_RESPONSE_BODY_BYTES).
    pub response_body: Option<String>,
    /// Error description if the attempt failed without a response.
    pub error: Option<String>,
    /// Wall-clock duration of this attempt in milliseconds.
    pub duration_ms: u64,
}

impl DeliveryAttempt {
    /// Returns true if this attempt resulted in a successful delivery (HTTP 2xx).
    pub fn is_success(&self) -> bool {
        matches!(self.response_status, Some(status) if (200..300).contains(&status))
    }

    /// Returns true if this attempt got a response at all (success or server error).
    pub fn has_response(&self) -> bool {
        self.response_status.is_some()
    }

    /// Returns true if the server explicitly said to retry later (429 or 503).
    pub fn is_retryable_status(&self) -> bool {
        matches!(self.response_status, Some(429) | Some(503))
    }

    /// Returns true if this was a client error (4xx excluding 429) that should not be retried.
    pub fn is_permanent_failure(&self) -> bool {
        matches!(self.response_status, Some(status) if (400..500).contains(&status) && status != 429)
    }

    /// Create an attempt record for a network/timeout error.
    pub fn from_error(attempt_number: u32, error: String, duration_ms: u64) -> Self {
        Self {
            attempt_number,
            sent_at: Utc::now(),
            response_status: None,
            response_body: None,
            error: Some(error),
            duration_ms,
        }
    }

    /// Create an attempt record for a successful HTTP response.
    pub fn from_response(
        attempt_number: u32,
        status: u16,
        body: Option<String>,
        duration_ms: u64,
    ) -> Self {
        Self {
            attempt_number,
            sent_at: Utc::now(),
            response_status: Some(status),
            response_body: body,
            error: None,
            duration_ms,
        }
    }
}

// ============================================================================
// WebhookDelivery
// ============================================================================

/// Record of a complete delivery chain for a single webhook event.
///
/// Tracks the original configuration, all attempts, current status,
/// and scheduling of future retries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDelivery {
    /// Unique identifier for this delivery chain.
    pub id: String,
    /// Job that triggered this webhook.
    pub job_id: JobId,
    /// The URL to POST to.
    pub webhook_url: String,
    /// The type of event (e.g., "milestone", "completion", "error", "custom").
    pub event_type: String,
    /// The JSON payload to deliver.
    pub payload: serde_json::Value,
    /// When this delivery was first created.
    pub created_at: DateTime<Utc>,
    /// All delivery attempts in order.
    pub attempts: Vec<DeliveryAttempt>,
    /// Current status of the delivery chain.
    pub status: WebhookDeliveryStatus,
    /// When the next retry is scheduled (None if not pending retry).
    pub next_retry_at: Option<DateTime<Utc>>,
}

impl WebhookDelivery {
    /// Create a new delivery record.
    pub fn new(
        job_id: JobId,
        webhook_url: String,
        event_type: String,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            job_id,
            webhook_url,
            event_type,
            payload,
            created_at: Utc::now(),
            attempts: Vec::new(),
            status: WebhookDeliveryStatus::Pending,
            next_retry_at: None,
        }
    }

    /// Returns the number of attempts made so far.
    pub fn attempt_count(&self) -> u32 {
        self.attempts.len() as u32
    }

    /// Returns the last attempt, if any.
    pub fn last_attempt(&self) -> Option<&DeliveryAttempt> {
        self.attempts.last()
    }

    /// Returns true if the delivery has been successfully completed.
    pub fn is_delivered(&self) -> bool {
        matches!(self.status, WebhookDeliveryStatus::Delivered { .. })
    }

    /// Returns true if the delivery has permanently failed.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            WebhookDeliveryStatus::Delivered { .. } | WebhookDeliveryStatus::Exhausted { .. }
        )
    }

    /// Returns true if the delivery is still pending (including retries).
    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            WebhookDeliveryStatus::Pending | WebhookDeliveryStatus::Failed { .. }
        )
    }

    /// Total time spent on all attempts in milliseconds.
    pub fn total_attempt_duration_ms(&self) -> u64 {
        self.attempts.iter().map(|a| a.duration_ms).sum()
    }

    /// Time since creation in seconds.
    pub fn age_secs(&self) -> i64 {
        (Utc::now() - self.created_at).num_seconds()
    }

    /// Record a successful attempt and transition to Delivered.
    pub fn mark_delivered(&mut self, attempt: DeliveryAttempt) {
        let status_code = attempt.response_status.unwrap_or(200);
        self.attempts.push(attempt);
        self.status = WebhookDeliveryStatus::Delivered {
            status_code,
            at: Utc::now(),
        };
        self.next_retry_at = None;
    }

    /// Record a failed attempt. If retries remain, schedule the next one.
    /// If exhausted, transition to Exhausted.
    pub fn mark_failed(&mut self, attempt: DeliveryAttempt, max_retries: u8, next_retry: Option<DateTime<Utc>>) {
        let error_msg = attempt.error.clone().unwrap_or_else(|| {
            format!(
                "HTTP {}",
                attempt.response_status.map_or("unknown".to_string(), |s| s.to_string())
            )
        });
        self.attempts.push(attempt);
        let attempt_count = self.attempts.len() as u8;

        if attempt_count >= max_retries {
            self.status = WebhookDeliveryStatus::Exhausted {
                last_error: error_msg,
            };
            self.next_retry_at = None;
        } else {
            self.status = WebhookDeliveryStatus::Failed {
                error: error_msg,
                attempts: attempt_count,
            };
            self.next_retry_at = next_retry;
        }
    }

    /// Reset delivery to Pending state for a manual retry.
    pub fn reset_for_retry(&mut self) {
        self.status = WebhookDeliveryStatus::Pending;
        self.next_retry_at = None;
    }
}

// ============================================================================
// WebhookTask
// ============================================================================

/// Internal message sent through the delivery channel to the worker loop.
#[derive(Debug, Clone)]
pub struct WebhookTask {
    /// ID of the parent WebhookDelivery.
    pub delivery_id: String,
    /// The URL to POST to.
    pub url: String,
    /// Optional Authorization header value (e.g., "Bearer <token>").
    pub auth_header: Option<String>,
    /// HMAC signing secret (if configured).
    pub signing_secret: Option<String>,
    /// Serialized JSON payload bytes.
    pub payload: Vec<u8>,
    /// Which attempt number this is (1-indexed).
    pub attempt_number: u32,
    /// Timeout for this individual request.
    pub timeout: Duration,
    /// Event type string (for the X-Webhook-Event header).
    pub event_type: String,
}

// ============================================================================
// WebhookPayloadBuilder
// ============================================================================

/// Helper for constructing standardized webhook payloads.
///
/// All payloads follow the same envelope structure:
/// ```json
/// {
///   "event": "<event_type>",
///   "job_id": "<uuid>",
///   "timestamp": "<ISO 8601>",
///   "data": { ... }
/// }
/// ```
pub struct WebhookPayloadBuilder;

impl WebhookPayloadBuilder {
    /// Build a milestone progress payload.
    pub fn milestone_payload(
        job_id: &JobId,
        milestone_pct: u8,
        progress: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": "milestone",
            "job_id": job_id.0.to_string(),
            "timestamp": Utc::now().to_rfc3339(),
            "data": {
                "milestone_pct": milestone_pct,
                "progress": progress,
            }
        })
    }

    /// Build a job completion payload.
    pub fn completion_payload(
        job_id: &JobId,
        result_summary: serde_json::Value,
        cost: f64,
        duration_secs: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": "completion",
            "job_id": job_id.0.to_string(),
            "timestamp": Utc::now().to_rfc3339(),
            "data": {
                "result_summary": result_summary,
                "cost_usd": cost,
                "duration_secs": duration_secs,
            }
        })
    }

    /// Build an error notification payload.
    pub fn error_payload(
        job_id: &JobId,
        error_type: &str,
        error_message: &str,
        retry_info: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": "error",
            "job_id": job_id.0.to_string(),
            "timestamp": Utc::now().to_rfc3339(),
            "data": {
                "error_type": error_type,
                "error_message": error_message,
                "retry_info": retry_info,
            }
        })
    }

    /// Build a custom event payload.
    pub fn custom_payload(
        job_id: &JobId,
        event_type: &str,
        data: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": event_type,
            "job_id": job_id.0.to_string(),
            "timestamp": Utc::now().to_rfc3339(),
            "data": data,
        })
    }

    /// Build a cancellation payload.
    pub fn cancellation_payload(
        job_id: &JobId,
        reason: &str,
        cancelled_by: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": "cancellation",
            "job_id": job_id.0.to_string(),
            "timestamp": Utc::now().to_rfc3339(),
            "data": {
                "reason": reason,
                "cancelled_by": cancelled_by,
            }
        })
    }

    /// Build a heartbeat/keep-alive payload (for long-running jobs).
    pub fn heartbeat_payload(
        job_id: &JobId,
        current_pct: u8,
        estimated_remaining_secs: Option<u64>,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": "heartbeat",
            "job_id": job_id.0.to_string(),
            "timestamp": Utc::now().to_rfc3339(),
            "data": {
                "current_pct": current_pct,
                "estimated_remaining_secs": estimated_remaining_secs,
            }
        })
    }

    /// Build a chunk-level progress payload (fine-grained updates).
    pub fn chunk_progress_payload(
        job_id: &JobId,
        chunks_completed: u32,
        chunks_total: u32,
        chunks_failed: u32,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": "chunk_progress",
            "job_id": job_id.0.to_string(),
            "timestamp": Utc::now().to_rfc3339(),
            "data": {
                "chunks_completed": chunks_completed,
                "chunks_total": chunks_total,
                "chunks_failed": chunks_failed,
                "completion_pct": if chunks_total > 0 {
                    (chunks_completed as f64 / chunks_total as f64 * 100.0).round() as u8
                } else {
                    0u8
                },
            }
        })
    }

    /// Wrap an arbitrary payload in the standard envelope.
    pub fn envelope(
        event_type: &str,
        job_id: &JobId,
        data: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "event": event_type,
            "job_id": job_id.0.to_string(),
            "timestamp": Utc::now().to_rfc3339(),
            "data": data,
        })
    }
}

// ============================================================================
// WebhookSigner
// ============================================================================

/// HMAC-SHA256 signer for webhook payload authentication.
///
/// Uses a manual HMAC construction (RFC 2104) over the `sha2` crate to
/// avoid pulling in a dedicated HMAC crate. The resulting signature is
/// sent in the `X-Webhook-Signature` header as a hex-encoded string
/// prefixed with `sha256=`.
pub struct WebhookSigner;

impl WebhookSigner {
    /// Compute HMAC-SHA256 of `payload` using `secret`.
    /// Returns `"sha256=<hex>"`.
    pub fn sign_payload(payload: &[u8], secret: &str) -> String {
        let mac = Self::hmac_sha256(secret.as_bytes(), payload);
        let hex = Self::bytes_to_hex(&mac);
        format!("sha256={}", hex)
    }

    /// Verify that `signature` matches the HMAC-SHA256 of `payload` using `secret`.
    /// Accepts both `"sha256=<hex>"` and bare `"<hex>"` formats.
    pub fn verify_signature(payload: &[u8], signature: &str, secret: &str) -> bool {
        let expected = Self::sign_payload(payload, secret);
        // Constant-time comparison to prevent timing attacks
        let sig_to_check = if signature.starts_with("sha256=") {
            signature.to_string()
        } else {
            format!("sha256={}", signature)
        };
        Self::constant_time_eq(expected.as_bytes(), sig_to_check.as_bytes())
    }

    /// Generate a random 32-byte hex secret suitable for webhook signing.
    pub fn generate_secret() -> String {
        let bytes: [u8; 32] = {
            use rand::RngCore;
            let mut buf = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut buf);
            buf
        };
        Self::bytes_to_hex(&bytes)
    }

    /// Compute raw HMAC-SHA256 (RFC 2104).
    fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
        // Step 1: If key is longer than block size, hash it first.
        let padded_key = if key.len() > SHA256_BLOCK_SIZE {
            let hash = Sha256::digest(key);
            let mut buf = [0u8; SHA256_BLOCK_SIZE];
            buf[..32].copy_from_slice(&hash);
            buf
        } else {
            let mut buf = [0u8; SHA256_BLOCK_SIZE];
            buf[..key.len()].copy_from_slice(key);
            buf
        };

        // Step 2: XOR with ipad
        let mut i_key_pad = [0u8; SHA256_BLOCK_SIZE];
        for i in 0..SHA256_BLOCK_SIZE {
            i_key_pad[i] = padded_key[i] ^ HMAC_IPAD;
        }

        // Step 3: XOR with opad
        let mut o_key_pad = [0u8; SHA256_BLOCK_SIZE];
        for i in 0..SHA256_BLOCK_SIZE {
            o_key_pad[i] = padded_key[i] ^ HMAC_OPAD;
        }

        // Step 4: H(i_key_pad || message)
        let mut inner_hasher = Sha256::new();
        inner_hasher.update(i_key_pad);
        inner_hasher.update(message);
        let inner_hash = inner_hasher.finalize();

        // Step 5: H(o_key_pad || inner_hash)
        let mut outer_hasher = Sha256::new();
        outer_hasher.update(o_key_pad);
        outer_hasher.update(inner_hash);
        let result = outer_hasher.finalize();

        let mut out = [0u8; 32];
        out.copy_from_slice(&result);
        out
    }

    /// Convert bytes to lowercase hex string.
    fn bytes_to_hex(bytes: &[u8]) -> String {
        let mut hex = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            hex.push_str(&format!("{:02x}", b));
        }
        hex
    }

    /// Constant-time byte comparison to prevent timing side-channels.
    fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for i in 0..a.len() {
            diff |= a[i] ^ b[i];
        }
        diff == 0
    }
}

// ============================================================================
// WebhookFilter
// ============================================================================

/// Filter that determines which events should actually trigger webhook delivery.
///
/// Used to avoid sending too-frequent milestone updates or to limit webhooks
/// to specific event types only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookFilter {
    /// Allowed event types (empty = allow all).
    pub event_types: Vec<String>,
    /// Minimum milestone interval: skip milestones that are less than this
    /// many percentage points apart from the last delivered milestone.
    pub min_milestone_interval_pct: u8,
}

impl Default for WebhookFilter {
    fn default() -> Self {
        Self {
            event_types: Vec::new(), // empty = allow all
            min_milestone_interval_pct: 10,
        }
    }
}

impl WebhookFilter {
    /// Create a filter that allows all events with no milestone throttling.
    pub fn allow_all() -> Self {
        Self {
            event_types: Vec::new(),
            min_milestone_interval_pct: 0,
        }
    }

    /// Create a filter that only allows specific event types.
    pub fn only(event_types: Vec<String>) -> Self {
        Self {
            event_types,
            min_milestone_interval_pct: 10,
        }
    }

    /// Check whether a given event should be delivered.
    ///
    /// For milestone events, `current_pct` is checked against the minimum
    /// interval (this only gates on the value itself, not against a previous
    /// delivery -- the caller is responsible for tracking the last delivered
    /// milestone percentage).
    ///
    /// For non-milestone events, `current_pct` is ignored.
    pub fn should_deliver(&self, event_type: &str, current_pct: u8) -> bool {
        // Check event type filter
        if !self.event_types.is_empty() && !self.event_types.iter().any(|e| e == event_type) {
            return false;
        }

        // For milestone events, check the interval threshold
        if event_type == "milestone" && self.min_milestone_interval_pct > 0 {
            // Only deliver at milestones that are multiples of the interval
            if current_pct % self.min_milestone_interval_pct != 0 {
                return false;
            }
        }

        true
    }

    /// Returns true if the filter has any event type restrictions.
    pub fn has_type_filter(&self) -> bool {
        !self.event_types.is_empty()
    }

    /// Returns the number of allowed event types (0 means all).
    pub fn allowed_type_count(&self) -> usize {
        self.event_types.len()
    }

    /// Add an event type to the allowed list.
    pub fn add_event_type(&mut self, event_type: String) {
        if !self.event_types.contains(&event_type) {
            self.event_types.push(event_type);
        }
    }

    /// Remove an event type from the allowed list.
    pub fn remove_event_type(&mut self, event_type: &str) {
        self.event_types.retain(|e| e != event_type);
    }
}

// ============================================================================
// WebhookRegistration (internal bookkeeping)
// ============================================================================

/// Internal registration record linking a JobId to its webhook configuration.
#[derive(Debug, Clone)]
struct WebhookRegistration {
    /// The job this webhook is registered for.
    job_id: JobId,
    /// Customer-provided webhook configuration.
    config: WebhookConfig,
    /// Optional HMAC signing secret.
    signing_secret: Option<String>,
    /// Optional filter for event gating.
    filter: WebhookFilter,
    /// Last milestone percentage that was actually delivered.
    last_milestone_pct: u8,
    /// When this registration was created.
    registered_at: DateTime<Utc>,
}

// ============================================================================
// WebhookEngine
// ============================================================================

/// Main engine managing all webhook deliveries for the swarm.
///
/// Thread-safe: the engine can be shared via `Arc<WebhookEngine>` and called
/// from multiple async tasks concurrently. The delivery loop runs as a
/// separate spawned task.
pub struct WebhookEngine {
    /// All delivery records, keyed by delivery ID.
    deliveries: DashMap<String, WebhookDelivery>,
    /// Job-to-registration mapping.
    registrations: DashMap<String, WebhookRegistration>,
    /// Sender end of the internal task queue.
    sender: mpsc::Sender<WebhookTask>,
    /// Receiver end (moved into the delivery loop on spawn).
    receiver: parking_lot::Mutex<Option<mpsc::Receiver<WebhookTask>>>,
    /// Engine configuration.
    config: WebhookEngineConfig,
    /// Global delivery statistics.
    stats: parking_lot::RwLock<WebhookStats>,
    /// Concurrency limiter for outbound HTTP requests.
    semaphore: Arc<Semaphore>,
}

impl WebhookEngine {
    /// Create a new WebhookEngine with default configuration.
    pub fn new() -> Self {
        Self::with_config(WebhookEngineConfig::default())
    }

    /// Create a new WebhookEngine with the given configuration.
    pub fn with_config(config: WebhookEngineConfig) -> Self {
        let (sender, receiver) = mpsc::channel(TASK_CHANNEL_CAPACITY);
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
        Self {
            deliveries: DashMap::new(),
            registrations: DashMap::new(),
            sender,
            receiver: parking_lot::Mutex::new(Some(receiver)),
            config,
            stats: parking_lot::RwLock::new(WebhookStats::default()),
            semaphore,
        }
    }

    /// Register a webhook for a job. Returns the registration key (the job_id
    /// string). All subsequent trigger calls for this job will use this config.
    pub fn register_webhook(&self, job_id: JobId, config: WebhookConfig) -> String {
        let key = job_id.0.to_string();
        let registration = WebhookRegistration {
            job_id,
            config,
            signing_secret: None,
            filter: WebhookFilter::default(),
            last_milestone_pct: 0,
            registered_at: Utc::now(),
        };
        self.registrations.insert(key.clone(), registration);
        info!(job_id = %job_id, "webhook registered");
        key
    }

    /// Register a webhook with a signing secret and optional filter.
    pub fn register_webhook_with_options(
        &self,
        job_id: JobId,
        config: WebhookConfig,
        signing_secret: Option<String>,
        filter: Option<WebhookFilter>,
    ) -> String {
        let key = job_id.0.to_string();
        let registration = WebhookRegistration {
            job_id,
            config,
            signing_secret,
            filter: filter.unwrap_or_default(),
            last_milestone_pct: 0,
            registered_at: Utc::now(),
        };
        self.registrations.insert(key.clone(), registration);
        info!(job_id = %job_id, "webhook registered with options");
        key
    }

    /// Unregister a webhook for a job.
    pub fn unregister_webhook(&self, job_id: &JobId) {
        let key = job_id.0.to_string();
        self.registrations.remove(&key);
        debug!(job_id = %job_id, "webhook unregistered");
    }

    /// Check if a webhook is registered for a job.
    pub fn is_registered(&self, job_id: &JobId) -> bool {
        let key = job_id.0.to_string();
        self.registrations.contains_key(&key)
    }

    /// Trigger a milestone webhook.
    pub fn trigger_milestone(
        &self,
        job_id: &JobId,
        milestone_pct: u8,
        payload: serde_json::Value,
    ) {
        let key = job_id.0.to_string();
        let reg = match self.registrations.get(&key) {
            Some(r) => r,
            None => {
                debug!(job_id = %job_id, "no webhook registered, skipping milestone");
                return;
            }
        };

        // Check if this milestone is in the configured milestones list
        if !reg.config.milestones.contains(&milestone_pct) {
            debug!(job_id = %job_id, milestone_pct, "milestone not in configured list, skipping");
            return;
        }

        // Check filter
        if !reg.filter.should_deliver("milestone", milestone_pct) {
            debug!(job_id = %job_id, milestone_pct, "milestone filtered out");
            return;
        }

        let full_payload = WebhookPayloadBuilder::milestone_payload(job_id, milestone_pct, payload);
        self.enqueue_delivery(
            &reg,
            "milestone".to_string(),
            full_payload,
        );

        drop(reg);
        // Update last milestone
        if let Some(mut reg) = self.registrations.get_mut(&key) {
            reg.last_milestone_pct = milestone_pct;
        }
    }

    /// Trigger a completion webhook.
    pub fn trigger_completion(&self, job_id: &JobId, payload: serde_json::Value) {
        let key = job_id.0.to_string();
        let reg = match self.registrations.get(&key) {
            Some(r) => r,
            None => {
                debug!(job_id = %job_id, "no webhook registered, skipping completion");
                return;
            }
        };

        if !reg.filter.should_deliver("completion", 100) {
            debug!(job_id = %job_id, "completion filtered out");
            return;
        }

        let full_payload = WebhookPayloadBuilder::envelope("completion", job_id, payload);
        self.enqueue_delivery(&reg, "completion".to_string(), full_payload);
    }

    /// Trigger an error webhook.
    pub fn trigger_error(&self, job_id: &JobId, error: String, payload: serde_json::Value) {
        let key = job_id.0.to_string();
        let reg = match self.registrations.get(&key) {
            Some(r) => r,
            None => {
                debug!(job_id = %job_id, "no webhook registered, skipping error");
                return;
            }
        };

        if !reg.filter.should_deliver("error", 0) {
            debug!(job_id = %job_id, "error event filtered out");
            return;
        }

        let error_payload = serde_json::json!({
            "error": error,
            "details": payload,
        });
        let full_payload = WebhookPayloadBuilder::envelope("error", job_id, error_payload);
        self.enqueue_delivery(&reg, "error".to_string(), full_payload);
    }

    /// Trigger a custom event webhook.
    pub fn trigger_custom(&self, job_id: &JobId, event_type: &str, payload: serde_json::Value) {
        let key = job_id.0.to_string();
        let reg = match self.registrations.get(&key) {
            Some(r) => r,
            None => {
                debug!(job_id = %job_id, "no webhook registered, skipping custom event");
                return;
            }
        };

        if !reg.filter.should_deliver(event_type, 0) {
            debug!(job_id = %job_id, event_type, "custom event filtered out");
            return;
        }

        let full_payload = WebhookPayloadBuilder::envelope(event_type, job_id, payload);
        self.enqueue_delivery(&reg, event_type.to_string(), full_payload);
    }

    /// Get the current status of a specific delivery.
    pub fn get_delivery_status(&self, delivery_id: &str) -> Option<WebhookDeliveryStatus> {
        self.deliveries.get(delivery_id).map(|d| d.status.clone())
    }

    /// Get a full delivery record by ID.
    pub fn get_delivery(&self, delivery_id: &str) -> Option<WebhookDelivery> {
        self.deliveries.get(delivery_id).map(|d| d.clone())
    }

    /// List all deliveries for a given job.
    pub fn list_deliveries_for_job(&self, job_id: &JobId) -> Vec<WebhookDelivery> {
        let job_str = job_id.0.to_string();
        self.deliveries
            .iter()
            .filter(|entry| entry.value().job_id.0.to_string() == job_str)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Cancel all pending/retrying deliveries for a job.
    pub fn cancel_deliveries(&self, job_id: &JobId) {
        let job_str = job_id.0.to_string();
        let mut cancelled = 0u32;
        for mut entry in self.deliveries.iter_mut() {
            let delivery = entry.value_mut();
            if delivery.job_id.0.to_string() == job_str && delivery.is_active() {
                delivery.status = WebhookDeliveryStatus::Exhausted {
                    last_error: "cancelled by user".to_string(),
                };
                delivery.next_retry_at = None;
                cancelled += 1;
                self.stats.write().record_failure(); // Record as failed due to cancellation
            }
        }
        if cancelled > 0 {
            info!(job_id = %job_id, cancelled, "cancelled pending webhook deliveries");
        }
    }

    /// Retry a specific failed delivery. Returns true if the delivery was
    /// found and re-enqueued.
    pub fn retry_failed(&self, delivery_id: &str) -> bool {
        let delivery = match self.deliveries.get(delivery_id) {
            Some(d) => d.clone(),
            None => return false,
        };

        // Only retry terminal failures
        if !matches!(
            delivery.status,
            WebhookDeliveryStatus::Exhausted { .. } | WebhookDeliveryStatus::Failed { .. }
        ) {
            return false;
        }

        // Reset the delivery
        if let Some(mut entry) = self.deliveries.get_mut(delivery_id) {
            entry.reset_for_retry();
        }

        // Find the registration for auth info
        let key = delivery.job_id.0.to_string();
        let (auth_header, signing_secret) = match self.registrations.get(&key) {
            Some(reg) => (reg.config.auth_header.clone(), reg.signing_secret.clone()),
            None => (None, None),
        };

        let payload_bytes = serde_json::to_vec(&delivery.payload).unwrap_or_default();
        let task = WebhookTask {
            delivery_id: delivery.id.clone(),
            url: delivery.webhook_url.clone(),
            auth_header,
            signing_secret,
            payload: payload_bytes,
            attempt_number: delivery.attempt_count() + 1,
            timeout: Duration::from_secs(self.config.timeout_secs),
            event_type: delivery.event_type.clone(),
        };

        self.stats.write().record_retry();
        match self.sender.try_send(task) {
            Ok(_) => {
                info!(delivery_id, "re-enqueued failed delivery for retry");
                true
            }
            Err(e) => {
                warn!(delivery_id, error = %e, "failed to re-enqueue delivery");
                false
            }
        }
    }

    /// Get a snapshot of global webhook stats.
    pub fn get_stats(&self) -> WebhookStats {
        self.stats.read().clone()
    }

    /// Get the total number of tracked deliveries.
    pub fn delivery_count(&self) -> usize {
        self.deliveries.len()
    }

    /// Get the number of active (non-terminal) deliveries.
    pub fn active_delivery_count(&self) -> usize {
        self.deliveries
            .iter()
            .filter(|entry| entry.value().is_active())
            .count()
    }

    /// Get the number of registered webhooks.
    pub fn registration_count(&self) -> usize {
        self.registrations.len()
    }

    /// Purge completed deliveries older than the given age.
    pub fn purge_old_deliveries(&self, max_age: Duration) {
        let cutoff = Utc::now() - chrono::Duration::from_std(max_age).unwrap_or(chrono::Duration::hours(24));
        let mut removed = 0usize;
        self.deliveries.retain(|_, delivery| {
            if delivery.is_terminal() && delivery.created_at < cutoff {
                removed += 1;
                false
            } else {
                true
            }
        });
        if removed > 0 {
            debug!(removed, "purged old webhook deliveries");
        }
    }

    /// Spawn the delivery worker loop. Must be called once after construction.
    ///
    /// The loop runs until the `shutdown` watch channel signals true.
    pub fn spawn_delivery_loop(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let mut receiver = self
            .receiver
            .lock()
            .take()
            .expect("spawn_delivery_loop called more than once");

        let engine = Arc::clone(&self);

        tokio::spawn(async move {
            info!("webhook delivery loop started");

            let client = reqwest::Client::builder()
                .user_agent(WEBHOOK_USER_AGENT)
                .timeout(Duration::from_secs(engine.config.timeout_secs))
                .pool_max_idle_per_host(4)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new());

            loop {
                tokio::select! {
                    Some(task) = receiver.recv() => {
                        let engine = Arc::clone(&engine);
                        let client = client.clone();
                        let semaphore = Arc::clone(&engine.semaphore);

                        tokio::spawn(async move {
                            // Acquire semaphore permit to limit concurrency
                            let _permit = match semaphore.acquire().await {
                                Ok(permit) => permit,
                                Err(_) => {
                                    error!(delivery_id = %task.delivery_id, "semaphore closed");
                                    return;
                                }
                            };

                            engine.execute_delivery(&client, task).await;
                        });
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            info!("webhook delivery loop shutting down");
                            break;
                        }
                    }
                }
            }

            info!("webhook delivery loop stopped");
        })
    }

    // ---- Internal helpers ----

    /// Enqueue a delivery for a given registration.
    fn enqueue_delivery(
        &self,
        reg: &WebhookRegistration,
        event_type: String,
        payload: serde_json::Value,
    ) {
        let payload_bytes = match serde_json::to_vec(&payload) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(error = %e, "failed to serialize webhook payload");
                return;
            }
        };

        // Check payload size
        if payload_bytes.len() > self.config.max_payload_bytes {
            warn!(
                size = payload_bytes.len(),
                max = self.config.max_payload_bytes,
                "webhook payload exceeds maximum size, dropping"
            );
            return;
        }

        let delivery = WebhookDelivery::new(
            reg.job_id,
            reg.config.url.clone(),
            event_type.clone(),
            payload,
        );
        let delivery_id = delivery.id.clone();

        // Mark as active in stats
        self.stats.write().increment_active();

        // Store the delivery record
        self.deliveries.insert(delivery_id.clone(), delivery);

        let task = WebhookTask {
            delivery_id: delivery_id.clone(),
            url: reg.config.url.clone(),
            auth_header: reg.config.auth_header.clone(),
            signing_secret: reg.signing_secret.clone(),
            payload: payload_bytes,
            attempt_number: 1,
            timeout: Duration::from_secs(reg.config.timeout_secs),
            event_type,
        };

        match self.sender.try_send(task) {
            Ok(_) => {
                debug!(delivery_id, "webhook delivery enqueued");
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(delivery_id, "webhook delivery queue full, dropping");
                if let Some(mut entry) = self.deliveries.get_mut(&delivery_id) {
                    entry.status = WebhookDeliveryStatus::Exhausted {
                        last_error: "delivery queue full".to_string(),
                    };
                }
                self.stats.write().decrement_active();
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(delivery_id, "webhook delivery channel closed");
                if let Some(mut entry) = self.deliveries.get_mut(&delivery_id) {
                    entry.status = WebhookDeliveryStatus::Exhausted {
                        last_error: "delivery channel closed".to_string(),
                    };
                }
                self.stats.write().decrement_active();
            }
        }
    }

    /// Execute a single delivery attempt. On failure, schedule a retry if
    /// attempts remain.
    async fn execute_delivery(&self, client: &reqwest::Client, task: WebhookTask) {
        let start = std::time::Instant::now();
        debug!(
            delivery_id = %task.delivery_id,
            attempt = task.attempt_number,
            url = %task.url,
            "executing webhook delivery"
        );

        // Build the request
        let mut request = client
            .post(&task.url)
            .header("Content-Type", "application/json")
            .header(EVENT_TYPE_HEADER, &task.event_type)
            .header(DELIVERY_ID_HEADER, &task.delivery_id)
            .header(TIMESTAMP_HEADER, Utc::now().to_rfc3339())
            .timeout(task.timeout)
            .body(task.payload.clone());

        // Add auth header if configured
        if let Some(ref auth) = task.auth_header {
            request = request.header("Authorization", auth);
        }

        // Add HMAC signature if a signing secret is configured
        if let Some(ref secret) = task.signing_secret {
            let signature = WebhookSigner::sign_payload(&task.payload, secret);
            request = request.header(SIGNATURE_HEADER, signature);
        }

        // Send the request
        let result = request.send().await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(response) => {
                let status = response.status().as_u16();
                let body = match response.text().await {
                    Ok(text) => {
                        if text.len() > MAX_RESPONSE_BODY_BYTES {
                            Some(text[..MAX_RESPONSE_BODY_BYTES].to_string())
                        } else {
                            Some(text)
                        }
                    }
                    Err(_) => None,
                };

                let attempt = DeliveryAttempt::from_response(
                    task.attempt_number,
                    status,
                    body,
                    duration_ms,
                );

                if attempt.is_success() {
                    // Successful delivery
                    if let Some(mut entry) = self.deliveries.get_mut(&task.delivery_id) {
                        entry.mark_delivered(attempt);
                    }
                    self.stats.write().record_success(duration_ms);
                    self.stats.write().decrement_active();
                    info!(
                        delivery_id = %task.delivery_id,
                        status,
                        duration_ms,
                        "webhook delivered successfully"
                    );
                } else if attempt.is_permanent_failure() {
                    // Permanent client error -- do not retry
                    if let Some(mut entry) = self.deliveries.get_mut(&task.delivery_id) {
                        entry.mark_failed(attempt, 1, None); // force exhaustion
                    }
                    self.stats.write().record_failure();
                    self.stats.write().decrement_active();
                    warn!(
                        delivery_id = %task.delivery_id,
                        status,
                        "webhook delivery permanently failed (client error)"
                    );
                } else {
                    // Retryable failure (5xx, 429, etc.)
                    self.handle_retry(task, attempt).await;
                }
            }
            Err(e) => {
                let error_msg = if e.is_timeout() {
                    format!("request timed out after {}ms", duration_ms)
                } else if e.is_connect() {
                    format!("connection failed: {}", e)
                } else {
                    format!("request error: {}", e)
                };

                let attempt = DeliveryAttempt::from_error(
                    task.attempt_number,
                    error_msg,
                    duration_ms,
                );
                self.handle_retry(task, attempt).await;
            }
        }
    }

    /// Handle a failed attempt by either scheduling a retry or marking exhausted.
    async fn handle_retry(&self, task: WebhookTask, attempt: DeliveryAttempt) {
        let max_retries = self.config.max_retries;
        let next_attempt = task.attempt_number;

        if next_attempt >= max_retries as u32 {
            // Exhausted all retries
            if let Some(mut entry) = self.deliveries.get_mut(&task.delivery_id) {
                entry.mark_failed(attempt, max_retries, None);
            }
            self.stats.write().record_failure();
            self.stats.write().decrement_active();
            warn!(
                delivery_id = %task.delivery_id,
                attempts = next_attempt,
                "webhook delivery exhausted all retries"
            );
        } else {
            // Schedule retry with backoff
            let backoff = self.config.backoff_with_jitter(next_attempt - 1);
            let next_retry_at = Utc::now() + chrono::Duration::from_std(backoff).unwrap_or(chrono::Duration::seconds(30));

            if let Some(mut entry) = self.deliveries.get_mut(&task.delivery_id) {
                entry.mark_failed(attempt, max_retries, Some(next_retry_at));
            }
            self.stats.write().record_retry();

            debug!(
                delivery_id = %task.delivery_id,
                attempt = next_attempt,
                backoff_ms = backoff.as_millis(),
                "scheduling webhook retry"
            );

            // Sleep for the backoff period, then re-enqueue
            tokio::time::sleep(backoff).await;

            let retry_task = WebhookTask {
                delivery_id: task.delivery_id.clone(),
                url: task.url,
                auth_header: task.auth_header,
                signing_secret: task.signing_secret,
                payload: task.payload,
                attempt_number: next_attempt + 1,
                timeout: task.timeout,
                event_type: task.event_type,
            };

            if let Err(e) = self.sender.try_send(retry_task) {
                warn!(
                    delivery_id = %task.delivery_id,
                    error = %e,
                    "failed to enqueue retry task"
                );
                if let Some(mut entry) = self.deliveries.get_mut(&task.delivery_id) {
                    entry.status = WebhookDeliveryStatus::Exhausted {
                        last_error: format!("retry enqueue failed: {}", e),
                    };
                    entry.next_retry_at = None;
                }
                self.stats.write().record_failure();
                self.stats.write().decrement_active();
            }
        }
    }
}

// ============================================================================
// Utility functions
// ============================================================================

/// Compute exponential backoff duration for a given attempt, capped at a maximum.
///
/// This is a standalone utility for use outside WebhookEngineConfig.
pub fn compute_backoff(base_secs: u64, attempt: u32, max_secs: u64) -> Duration {
    let base = base_secs as f64;
    let max = max_secs as f64;
    let exp = base * 2.0_f64.powi(attempt as i32);
    let capped = exp.min(max);
    Duration::from_secs(capped as u64)
}

/// Compute exponential backoff with random jitter (up to +50%).
pub fn compute_backoff_with_jitter(base_secs: u64, attempt: u32, max_secs: u64) -> Duration {
    let base = base_secs as f64;
    let max = max_secs as f64;
    let exp = base * 2.0_f64.powi(attempt as i32);
    let capped = exp.min(max);
    // Use rand for real jitter
    let jitter: f64 = {
        use rand::Rng;
        rand::thread_rng().gen_range(0.0..0.5)
    };
    let jittered = capped * (1.0 + jitter);
    Duration::from_millis((jittered * 1000.0) as u64)
}

/// Truncate a string to a maximum byte length, ensuring valid UTF-8.
pub fn truncate_body(body: &str, max_bytes: usize) -> String {
    if body.len() <= max_bytes {
        return body.to_string();
    }
    // Find the last valid char boundary at or before max_bytes
    let mut end = max_bytes;
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...(truncated)", &body[..end])
}

/// Parse a Retry-After header value (either seconds or HTTP date).
/// Returns the number of seconds to wait, or None if unparsable.
pub fn parse_retry_after(header_value: &str) -> Option<u64> {
    // Try parsing as integer seconds first
    if let Ok(secs) = header_value.trim().parse::<u64>() {
        return Some(secs);
    }
    // Try parsing as HTTP date
    if let Ok(date) = chrono::DateTime::parse_from_rfc2822(header_value.trim()) {
        let now = Utc::now();
        let target = date.with_timezone(&Utc);
        let diff = (target - now).num_seconds();
        if diff > 0 {
            return Some(diff as u64);
        } else {
            return Some(0);
        }
    }
    None
}

/// Validate a webhook URL. Returns None if valid, Some(error) if invalid.
pub fn validate_webhook_url(url: &str) -> Option<String> {
    if url.is_empty() {
        return Some("URL is empty".to_string());
    }
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Some("URL must start with http:// or https://".to_string());
    }
    // Check for obviously invalid URLs
    if url.len() < 10 {
        return Some("URL is too short".to_string());
    }
    // Check for localhost/private IPs (warning, not blocking)
    None
}

/// Check if a URL points to a local/private network address.
pub fn is_private_url(url: &str) -> bool {
    let lower = url.to_lowercase();
    lower.contains("localhost")
        || lower.contains("127.0.0.1")
        || lower.contains("[::1]")
        || lower.contains("10.")
        || lower.contains("192.168.")
        || lower.contains("172.16.")
        || lower.contains("172.17.")
        || lower.contains("172.18.")
        || lower.contains("172.19.")
        || lower.contains("172.20.")
        || lower.contains("172.21.")
        || lower.contains("172.22.")
        || lower.contains("172.23.")
        || lower.contains("172.24.")
        || lower.contains("172.25.")
        || lower.contains("172.26.")
        || lower.contains("172.27.")
        || lower.contains("172.28.")
        || lower.contains("172.29.")
        || lower.contains("172.30.")
        || lower.contains("172.31.")
}

/// Format a duration as a human-readable string.
pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::JobId;

    // --- WebhookPayloadBuilder tests ---

    #[test]
    fn test_milestone_payload_structure() {
        let job_id = JobId::new();
        let progress = serde_json::json!({"chunks_done": 5, "chunks_total": 10});
        let payload = WebhookPayloadBuilder::milestone_payload(&job_id, 50, progress);

        assert_eq!(payload["event"], "milestone");
        assert_eq!(payload["data"]["milestone_pct"], 50);
        assert_eq!(payload["data"]["progress"]["chunks_done"], 5);
        assert!(payload["timestamp"].is_string());
        assert!(payload["job_id"].is_string());
    }

    #[test]
    fn test_completion_payload_structure() {
        let job_id = JobId::new();
        let summary = serde_json::json!({"output_size": 1024});
        let payload = WebhookPayloadBuilder::completion_payload(&job_id, summary, 0.05, 120);

        assert_eq!(payload["event"], "completion");
        assert_eq!(payload["data"]["cost_usd"], 0.05);
        assert_eq!(payload["data"]["duration_secs"], 120);
        assert_eq!(payload["data"]["result_summary"]["output_size"], 1024);
    }

    #[test]
    fn test_error_payload_structure() {
        let job_id = JobId::new();
        let retry = serde_json::json!({"can_retry": true, "attempts_left": 2});
        let payload = WebhookPayloadBuilder::error_payload(&job_id, "timeout", "chunk exceeded 5m limit", retry);

        assert_eq!(payload["event"], "error");
        assert_eq!(payload["data"]["error_type"], "timeout");
        assert_eq!(payload["data"]["error_message"], "chunk exceeded 5m limit");
        assert_eq!(payload["data"]["retry_info"]["can_retry"], true);
    }

    #[test]
    fn test_custom_payload_structure() {
        let job_id = JobId::new();
        let data = serde_json::json!({"metric": "throughput", "value": 42.5});
        let payload = WebhookPayloadBuilder::custom_payload(&job_id, "metric_update", data);

        assert_eq!(payload["event"], "metric_update");
        assert_eq!(payload["data"]["metric"], "throughput");
        assert_eq!(payload["data"]["value"], 42.5);
    }

    #[test]
    fn test_cancellation_payload() {
        let job_id = JobId::new();
        let payload = WebhookPayloadBuilder::cancellation_payload(&job_id, "user requested", "admin");

        assert_eq!(payload["event"], "cancellation");
        assert_eq!(payload["data"]["reason"], "user requested");
        assert_eq!(payload["data"]["cancelled_by"], "admin");
    }

    #[test]
    fn test_heartbeat_payload() {
        let job_id = JobId::new();
        let payload = WebhookPayloadBuilder::heartbeat_payload(&job_id, 35, Some(120));

        assert_eq!(payload["event"], "heartbeat");
        assert_eq!(payload["data"]["current_pct"], 35);
        assert_eq!(payload["data"]["estimated_remaining_secs"], 120);
    }

    #[test]
    fn test_heartbeat_payload_no_estimate() {
        let job_id = JobId::new();
        let payload = WebhookPayloadBuilder::heartbeat_payload(&job_id, 75, None);

        assert_eq!(payload["event"], "heartbeat");
        assert_eq!(payload["data"]["current_pct"], 75);
        assert!(payload["data"]["estimated_remaining_secs"].is_null());
    }

    #[test]
    fn test_chunk_progress_payload() {
        let job_id = JobId::new();
        let payload = WebhookPayloadBuilder::chunk_progress_payload(&job_id, 7, 10, 1);

        assert_eq!(payload["event"], "chunk_progress");
        assert_eq!(payload["data"]["chunks_completed"], 7);
        assert_eq!(payload["data"]["chunks_total"], 10);
        assert_eq!(payload["data"]["chunks_failed"], 1);
        assert_eq!(payload["data"]["completion_pct"], 70);
    }

    #[test]
    fn test_chunk_progress_zero_total() {
        let job_id = JobId::new();
        let payload = WebhookPayloadBuilder::chunk_progress_payload(&job_id, 0, 0, 0);
        assert_eq!(payload["data"]["completion_pct"], 0);
    }

    #[test]
    fn test_envelope_wrapper() {
        let job_id = JobId::new();
        let data = serde_json::json!({"foo": "bar"});
        let payload = WebhookPayloadBuilder::envelope("test_event", &job_id, data);

        assert_eq!(payload["event"], "test_event");
        assert_eq!(payload["data"]["foo"], "bar");
    }

    // --- WebhookSigner tests ---

    #[test]
    fn test_sign_and_verify() {
        let secret = "my-webhook-secret";
        let payload = b"test payload data";

        let signature = WebhookSigner::sign_payload(payload, secret);
        assert!(signature.starts_with("sha256="));
        assert!(WebhookSigner::verify_signature(payload, &signature, secret));
    }

    #[test]
    fn test_sign_verify_wrong_secret() {
        let payload = b"test payload";
        let signature = WebhookSigner::sign_payload(payload, "correct-secret");
        assert!(!WebhookSigner::verify_signature(payload, &signature, "wrong-secret"));
    }

    #[test]
    fn test_sign_verify_wrong_payload() {
        let secret = "secret";
        let signature = WebhookSigner::sign_payload(b"original", secret);
        assert!(!WebhookSigner::verify_signature(b"tampered", &signature, secret));
    }

    #[test]
    fn test_sign_verify_bare_hex() {
        let secret = "test-secret";
        let payload = b"data";
        let signature = WebhookSigner::sign_payload(payload, secret);
        // Strip the prefix and verify with bare hex
        let bare = signature.strip_prefix("sha256=").unwrap();
        assert!(WebhookSigner::verify_signature(payload, bare, secret));
    }

    #[test]
    fn test_sign_empty_payload() {
        let secret = "secret";
        let signature = WebhookSigner::sign_payload(b"", secret);
        assert!(signature.starts_with("sha256="));
        assert!(WebhookSigner::verify_signature(b"", &signature, secret));
    }

    #[test]
    fn test_sign_long_key() {
        // Key longer than SHA-256 block size (64 bytes)
        let secret = "a".repeat(128);
        let payload = b"test";
        let signature = WebhookSigner::sign_payload(payload, &secret);
        assert!(WebhookSigner::verify_signature(payload, &signature, &secret));
    }

    #[test]
    fn test_generate_secret_uniqueness() {
        let s1 = WebhookSigner::generate_secret();
        let s2 = WebhookSigner::generate_secret();
        assert_ne!(s1, s2);
        assert_eq!(s1.len(), 64); // 32 bytes = 64 hex chars
    }

    #[test]
    fn test_generate_secret_is_hex() {
        let secret = WebhookSigner::generate_secret();
        assert!(secret.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // --- WebhookFilter tests ---

    #[test]
    fn test_filter_allow_all() {
        let filter = WebhookFilter::allow_all();
        assert!(filter.should_deliver("milestone", 25));
        assert!(filter.should_deliver("completion", 100));
        assert!(filter.should_deliver("error", 0));
        assert!(filter.should_deliver("custom", 50));
    }

    #[test]
    fn test_filter_event_type_restriction() {
        let filter = WebhookFilter::only(vec!["completion".to_string(), "error".to_string()]);
        assert!(!filter.should_deliver("milestone", 50));
        assert!(filter.should_deliver("completion", 100));
        assert!(filter.should_deliver("error", 0));
    }

    #[test]
    fn test_filter_milestone_interval() {
        let filter = WebhookFilter {
            event_types: Vec::new(),
            min_milestone_interval_pct: 25,
        };
        assert!(filter.should_deliver("milestone", 25));
        assert!(filter.should_deliver("milestone", 50));
        assert!(filter.should_deliver("milestone", 75));
        assert!(filter.should_deliver("milestone", 100));
        assert!(!filter.should_deliver("milestone", 33));
        assert!(!filter.should_deliver("milestone", 10));
    }

    #[test]
    fn test_filter_milestone_interval_10() {
        let filter = WebhookFilter::default(); // interval = 10
        assert!(filter.should_deliver("milestone", 10));
        assert!(filter.should_deliver("milestone", 20));
        assert!(filter.should_deliver("milestone", 50));
        assert!(!filter.should_deliver("milestone", 15));
        assert!(!filter.should_deliver("milestone", 33));
    }

    #[test]
    fn test_filter_non_milestone_ignores_pct() {
        let filter = WebhookFilter {
            event_types: Vec::new(),
            min_milestone_interval_pct: 25,
        };
        // Non-milestone events should not be affected by min_milestone_interval_pct
        assert!(filter.should_deliver("completion", 33));
        assert!(filter.should_deliver("error", 7));
    }

    #[test]
    fn test_filter_zero_interval_allows_all_milestones() {
        let filter = WebhookFilter {
            event_types: Vec::new(),
            min_milestone_interval_pct: 0,
        };
        assert!(filter.should_deliver("milestone", 1));
        assert!(filter.should_deliver("milestone", 33));
        assert!(filter.should_deliver("milestone", 99));
    }

    #[test]
    fn test_filter_has_type_filter() {
        let f1 = WebhookFilter::allow_all();
        assert!(!f1.has_type_filter());

        let f2 = WebhookFilter::only(vec!["completion".to_string()]);
        assert!(f2.has_type_filter());
    }

    #[test]
    fn test_filter_add_remove_event_type() {
        let mut filter = WebhookFilter::default();
        assert!(!filter.has_type_filter());

        filter.add_event_type("error".to_string());
        assert!(filter.has_type_filter());
        assert_eq!(filter.allowed_type_count(), 1);

        filter.add_event_type("error".to_string()); // duplicate
        assert_eq!(filter.allowed_type_count(), 1);

        filter.add_event_type("completion".to_string());
        assert_eq!(filter.allowed_type_count(), 2);

        filter.remove_event_type("error");
        assert_eq!(filter.allowed_type_count(), 1);
    }

    // --- DeliveryAttempt tests ---

    #[test]
    fn test_attempt_is_success() {
        let a = DeliveryAttempt::from_response(1, 200, None, 50);
        assert!(a.is_success());

        let a = DeliveryAttempt::from_response(1, 201, None, 50);
        assert!(a.is_success());

        let a = DeliveryAttempt::from_response(1, 204, None, 50);
        assert!(a.is_success());

        let a = DeliveryAttempt::from_response(1, 500, None, 50);
        assert!(!a.is_success());
    }

    #[test]
    fn test_attempt_retryable_status() {
        let a = DeliveryAttempt::from_response(1, 429, None, 50);
        assert!(a.is_retryable_status());

        let a = DeliveryAttempt::from_response(1, 503, None, 50);
        assert!(a.is_retryable_status());

        let a = DeliveryAttempt::from_response(1, 500, None, 50);
        assert!(!a.is_retryable_status());
    }

    #[test]
    fn test_attempt_permanent_failure() {
        let a = DeliveryAttempt::from_response(1, 400, None, 50);
        assert!(a.is_permanent_failure());

        let a = DeliveryAttempt::from_response(1, 404, None, 50);
        assert!(a.is_permanent_failure());

        // 429 is NOT permanent (it's retryable)
        let a = DeliveryAttempt::from_response(1, 429, None, 50);
        assert!(!a.is_permanent_failure());
    }

    #[test]
    fn test_attempt_from_error() {
        let a = DeliveryAttempt::from_error(3, "connection refused".to_string(), 100);
        assert!(!a.is_success());
        assert!(!a.has_response());
        assert_eq!(a.attempt_number, 3);
        assert_eq!(a.error.as_deref(), Some("connection refused"));
        assert_eq!(a.duration_ms, 100);
    }

    // --- WebhookDelivery tests ---

    #[test]
    fn test_delivery_new() {
        let job_id = JobId::new();
        let delivery = WebhookDelivery::new(
            job_id,
            "https://example.com/hook".to_string(),
            "completion".to_string(),
            serde_json::json!({}),
        );

        assert!(delivery.is_active());
        assert!(!delivery.is_delivered());
        assert!(!delivery.is_terminal());
        assert_eq!(delivery.attempt_count(), 0);
        assert!(delivery.last_attempt().is_none());
    }

    #[test]
    fn test_delivery_mark_delivered() {
        let job_id = JobId::new();
        let mut delivery = WebhookDelivery::new(
            job_id,
            "https://example.com/hook".to_string(),
            "completion".to_string(),
            serde_json::json!({}),
        );

        let attempt = DeliveryAttempt::from_response(1, 200, Some("ok".to_string()), 50);
        delivery.mark_delivered(attempt);

        assert!(delivery.is_delivered());
        assert!(delivery.is_terminal());
        assert!(!delivery.is_active());
        assert_eq!(delivery.attempt_count(), 1);
        assert!(delivery.next_retry_at.is_none());
    }

    #[test]
    fn test_delivery_mark_failed_with_retries() {
        let job_id = JobId::new();
        let mut delivery = WebhookDelivery::new(
            job_id,
            "https://example.com/hook".to_string(),
            "completion".to_string(),
            serde_json::json!({}),
        );

        let attempt = DeliveryAttempt::from_response(1, 500, None, 100);
        let next_retry = Utc::now() + chrono::Duration::seconds(30);
        delivery.mark_failed(attempt, 5, Some(next_retry));

        assert!(!delivery.is_delivered());
        assert!(!delivery.is_terminal());
        assert!(delivery.is_active());
        assert_eq!(delivery.attempt_count(), 1);
        assert!(delivery.next_retry_at.is_some());
    }

    #[test]
    fn test_delivery_mark_exhausted() {
        let job_id = JobId::new();
        let mut delivery = WebhookDelivery::new(
            job_id,
            "https://example.com/hook".to_string(),
            "completion".to_string(),
            serde_json::json!({}),
        );

        // Simulate 3 failed attempts with max_retries=3
        for i in 1..=3 {
            let attempt = DeliveryAttempt::from_error(i, format!("fail {}", i), 50);
            delivery.mark_failed(attempt, 3, None);
        }

        assert!(delivery.is_terminal());
        assert!(!delivery.is_active());
        matches!(delivery.status, WebhookDeliveryStatus::Exhausted { .. });
    }

    #[test]
    fn test_delivery_reset_for_retry() {
        let job_id = JobId::new();
        let mut delivery = WebhookDelivery::new(
            job_id,
            "https://example.com/hook".to_string(),
            "completion".to_string(),
            serde_json::json!({}),
        );

        // Exhaust retries
        for i in 1..=3 {
            let attempt = DeliveryAttempt::from_error(i, format!("fail {}", i), 50);
            delivery.mark_failed(attempt, 3, None);
        }
        assert!(delivery.is_terminal());

        delivery.reset_for_retry();
        assert!(delivery.is_active());
        assert!(delivery.next_retry_at.is_none());
        // Attempts are preserved for history
        assert_eq!(delivery.attempt_count(), 3);
    }

    #[test]
    fn test_delivery_total_duration() {
        let job_id = JobId::new();
        let mut delivery = WebhookDelivery::new(
            job_id,
            "https://example.com/hook".to_string(),
            "completion".to_string(),
            serde_json::json!({}),
        );

        delivery.attempts.push(DeliveryAttempt::from_error(1, "err".to_string(), 100));
        delivery.attempts.push(DeliveryAttempt::from_error(2, "err".to_string(), 200));
        delivery.attempts.push(DeliveryAttempt::from_response(3, 200, None, 50));

        assert_eq!(delivery.total_attempt_duration_ms(), 350);
    }

    // --- Backoff calculation tests ---

    #[test]
    fn test_backoff_exponential() {
        let config = WebhookEngineConfig {
            backoff_base_secs: 5,
            backoff_max_secs: 600,
            ..Default::default()
        };

        // attempt 0: 5 * 2^0 = 5
        assert_eq!(config.backoff_duration(0), Duration::from_secs(5));
        // attempt 1: 5 * 2^1 = 10
        assert_eq!(config.backoff_duration(1), Duration::from_secs(10));
        // attempt 2: 5 * 2^2 = 20
        assert_eq!(config.backoff_duration(2), Duration::from_secs(20));
        // attempt 3: 5 * 2^3 = 40
        assert_eq!(config.backoff_duration(3), Duration::from_secs(40));
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let config = WebhookEngineConfig {
            backoff_base_secs: 5,
            backoff_max_secs: 60,
            ..Default::default()
        };

        // attempt 4: 5 * 2^4 = 80, capped to 60
        assert_eq!(config.backoff_duration(4), Duration::from_secs(60));
        // attempt 10: 5 * 2^10 = 5120, capped to 60
        assert_eq!(config.backoff_duration(10), Duration::from_secs(60));
    }

    #[test]
    fn test_backoff_with_jitter_is_larger() {
        let config = WebhookEngineConfig {
            backoff_base_secs: 10,
            backoff_max_secs: 600,
            ..Default::default()
        };

        for attempt in 0..5 {
            let base = config.backoff_duration(attempt);
            let jittered = config.backoff_with_jitter(attempt);
            assert!(jittered >= base, "jittered {:?} should be >= base {:?} at attempt {}", jittered, base, attempt);
        }
    }

    #[test]
    fn test_standalone_backoff() {
        assert_eq!(compute_backoff(5, 0, 600), Duration::from_secs(5));
        assert_eq!(compute_backoff(5, 1, 600), Duration::from_secs(10));
        assert_eq!(compute_backoff(5, 10, 60), Duration::from_secs(60));
    }

    // --- Config tests ---

    #[test]
    fn test_config_defaults() {
        let config = WebhookEngineConfig::default();
        assert_eq!(config.max_concurrent, WEBHOOK_MAX_CONCURRENT);
        assert_eq!(config.timeout_secs, WEBHOOK_TIMEOUT_SECS);
        assert_eq!(config.max_retries, WEBHOOK_MAX_RETRIES);
        assert_eq!(config.backoff_base_secs, WEBHOOK_RETRY_BACKOFF_BASE_SECS);
    }

    #[test]
    fn test_config_validate_ok() {
        let config = WebhookEngineConfig::default();
        let errors = config.validate();
        assert!(errors.is_empty(), "default config should be valid: {:?}", errors);
    }

    #[test]
    fn test_config_validate_errors() {
        let config = WebhookEngineConfig {
            max_concurrent: 0,
            timeout_secs: 0,
            max_retries: 0,
            backoff_base_secs: 0,
            backoff_max_secs: 0,
            max_payload_bytes: 0,
        };
        let errors = config.validate();
        assert!(!errors.is_empty());
        assert!(errors.iter().any(|e| e.contains("max_concurrent")));
        assert!(errors.iter().any(|e| e.contains("timeout_secs")));
        assert!(errors.iter().any(|e| e.contains("backoff_base_secs")));
        assert!(errors.iter().any(|e| e.contains("max_payload_bytes")));
    }

    #[test]
    fn test_config_validate_backoff_ordering() {
        let config = WebhookEngineConfig {
            backoff_base_secs: 100,
            backoff_max_secs: 10, // less than base
            ..Default::default()
        };
        let errors = config.validate();
        assert!(errors.iter().any(|e| e.contains("backoff_max_secs")));
    }

    #[test]
    fn test_config_timeout_duration() {
        let config = WebhookEngineConfig {
            timeout_secs: 42,
            ..Default::default()
        };
        assert_eq!(config.timeout_duration(), Duration::from_secs(42));
    }

    // --- Stats tests ---

    #[test]
    fn test_stats_default() {
        let stats = WebhookStats::default();
        assert_eq!(stats.total_sent, 0);
        assert_eq!(stats.total_delivered, 0);
        assert_eq!(stats.total_failed, 0);
        assert_eq!(stats.total_retried, 0);
        assert_eq!(stats.avg_delivery_time_ms, 0.0);
        assert_eq!(stats.active_deliveries, 0);
    }

    #[test]
    fn test_stats_record_success() {
        let mut stats = WebhookStats::default();
        stats.record_success(100);
        assert_eq!(stats.total_sent, 1);
        assert_eq!(stats.total_delivered, 1);
        assert_eq!(stats.avg_delivery_time_ms, 100.0);

        stats.record_success(200);
        assert_eq!(stats.total_sent, 2);
        assert_eq!(stats.total_delivered, 2);
        assert_eq!(stats.avg_delivery_time_ms, 150.0);
    }

    #[test]
    fn test_stats_record_failure() {
        let mut stats = WebhookStats::default();
        stats.record_failure();
        assert_eq!(stats.total_sent, 1);
        assert_eq!(stats.total_failed, 1);
        assert_eq!(stats.total_delivered, 0);
    }

    #[test]
    fn test_stats_success_rate() {
        let mut stats = WebhookStats::default();
        // No deliveries -> 1.0 (perfect)
        assert_eq!(stats.success_rate(), 1.0);

        stats.record_success(50);
        stats.record_success(50);
        stats.record_failure();
        // 2 successes, 1 failure = 2/3
        let rate = stats.success_rate();
        assert!((rate - 0.6667).abs() < 0.01);
    }

    #[test]
    fn test_stats_active_tracking() {
        let mut stats = WebhookStats::default();
        stats.increment_active();
        stats.increment_active();
        assert_eq!(stats.active_deliveries, 2);

        stats.decrement_active();
        assert_eq!(stats.active_deliveries, 1);

        stats.decrement_active();
        stats.decrement_active(); // saturating
        assert_eq!(stats.active_deliveries, 0);
    }

    #[test]
    fn test_stats_reset() {
        let mut stats = WebhookStats::default();
        stats.record_success(100);
        stats.record_failure();
        stats.record_retry();
        stats.increment_active();

        stats.reset();
        assert_eq!(stats.total_sent, 0);
        assert_eq!(stats.total_delivered, 0);
        assert_eq!(stats.total_failed, 0);
        assert_eq!(stats.total_retried, 0);
        assert_eq!(stats.active_deliveries, 0);
    }

    #[test]
    fn test_stats_total_attempts() {
        let mut stats = WebhookStats::default();
        stats.record_success(50);   // total_sent = 1
        stats.record_failure();      // total_sent = 2
        stats.record_retry();        // total_retried = 1
        stats.record_retry();        // total_retried = 2
        assert_eq!(stats.total_attempts(), 4); // 2 + 2
    }

    // --- Utility function tests ---

    #[test]
    fn test_truncate_body_short() {
        let body = "hello";
        assert_eq!(truncate_body(body, 100), "hello");
    }

    #[test]
    fn test_truncate_body_exact() {
        let body = "hello";
        assert_eq!(truncate_body(body, 5), "hello");
    }

    #[test]
    fn test_truncate_body_long() {
        let body = "hello world, this is a long message";
        let truncated = truncate_body(body, 11);
        assert!(truncated.starts_with("hello world"));
        assert!(truncated.contains("truncated"));
    }

    #[test]
    fn test_parse_retry_after_seconds() {
        assert_eq!(parse_retry_after("120"), Some(120));
        assert_eq!(parse_retry_after("  60  "), Some(60));
        assert_eq!(parse_retry_after("0"), Some(0));
    }

    #[test]
    fn test_parse_retry_after_invalid() {
        assert_eq!(parse_retry_after("not-a-number"), None);
    }

    #[test]
    fn test_validate_webhook_url_valid() {
        assert!(validate_webhook_url("https://example.com/webhook").is_none());
        assert!(validate_webhook_url("http://example.com/hook").is_none());
    }

    #[test]
    fn test_validate_webhook_url_empty() {
        assert!(validate_webhook_url("").is_some());
    }

    #[test]
    fn test_validate_webhook_url_no_scheme() {
        assert!(validate_webhook_url("example.com/hook").is_some());
    }

    #[test]
    fn test_validate_webhook_url_too_short() {
        assert!(validate_webhook_url("http://a").is_some());
    }

    #[test]
    fn test_is_private_url() {
        assert!(is_private_url("http://localhost:8080/hook"));
        assert!(is_private_url("http://127.0.0.1/hook"));
        assert!(is_private_url("http://192.168.1.1/hook"));
        assert!(is_private_url("http://10.0.0.1/hook"));
        assert!(is_private_url("http://172.16.0.1/hook"));
        assert!(!is_private_url("https://api.example.com/hook"));
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(30)), "30s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m 30s");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m");
    }

    // --- WebhookEngine basic tests (no network) ---

    #[test]
    fn test_engine_new() {
        let engine = WebhookEngine::new();
        assert_eq!(engine.delivery_count(), 0);
        assert_eq!(engine.active_delivery_count(), 0);
        assert_eq!(engine.registration_count(), 0);
    }

    #[test]
    fn test_engine_register_webhook() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        let config = WebhookConfig::default();

        let key = engine.register_webhook(job_id, config);
        assert!(!key.is_empty());
        assert!(engine.is_registered(&job_id));
        assert_eq!(engine.registration_count(), 1);
    }

    #[test]
    fn test_engine_unregister_webhook() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        let config = WebhookConfig::default();

        engine.register_webhook(job_id, config);
        assert!(engine.is_registered(&job_id));

        engine.unregister_webhook(&job_id);
        assert!(!engine.is_registered(&job_id));
    }

    #[test]
    fn test_engine_trigger_without_registration() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        // Should not panic, just silently skip
        engine.trigger_milestone(&job_id, 50, serde_json::json!({}));
        engine.trigger_completion(&job_id, serde_json::json!({}));
        engine.trigger_error(&job_id, "err".to_string(), serde_json::json!({}));
        engine.trigger_custom(&job_id, "test", serde_json::json!({}));
        assert_eq!(engine.delivery_count(), 0);
    }

    #[test]
    fn test_engine_trigger_milestone_creates_delivery() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        let config = WebhookConfig {
            url: "https://example.com/hook".to_string(),
            milestones: vec![25, 50, 75, 100],
            ..Default::default()
        };
        engine.register_webhook(job_id, config);

        engine.trigger_milestone(&job_id, 50, serde_json::json!({"progress": 0.5}));
        assert_eq!(engine.delivery_count(), 1);

        let deliveries = engine.list_deliveries_for_job(&job_id);
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].event_type, "milestone");
    }

    #[test]
    fn test_engine_trigger_milestone_skips_unconfigured() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        let config = WebhookConfig {
            url: "https://example.com/hook".to_string(),
            milestones: vec![50, 100], // only 50 and 100
            ..Default::default()
        };
        engine.register_webhook(job_id, config);

        engine.trigger_milestone(&job_id, 25, serde_json::json!({}));
        assert_eq!(engine.delivery_count(), 0);

        engine.trigger_milestone(&job_id, 50, serde_json::json!({}));
        assert_eq!(engine.delivery_count(), 1);
    }

    #[test]
    fn test_engine_trigger_completion_creates_delivery() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        let config = WebhookConfig {
            url: "https://example.com/hook".to_string(),
            ..Default::default()
        };
        engine.register_webhook(job_id, config);

        engine.trigger_completion(&job_id, serde_json::json!({"result": "ok"}));
        assert_eq!(engine.delivery_count(), 1);

        let deliveries = engine.list_deliveries_for_job(&job_id);
        assert_eq!(deliveries[0].event_type, "completion");
    }

    #[test]
    fn test_engine_trigger_error_creates_delivery() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        let config = WebhookConfig {
            url: "https://example.com/hook".to_string(),
            ..Default::default()
        };
        engine.register_webhook(job_id, config);

        engine.trigger_error(&job_id, "timeout".to_string(), serde_json::json!({}));
        assert_eq!(engine.delivery_count(), 1);
    }

    #[tokio::test]
    async fn test_engine_cancel_deliveries() {
        let engine = WebhookEngine::new();
        let job_id1 = JobId::new();
        let job_id2 = JobId::new();

        engine.register_webhook(
            job_id1.clone(),
            WebhookConfig {
                url: "http://127.0.0.1:9000/hook1".to_string(),
                ..Default::default()
            },
        );
        engine.register_webhook(
            job_id2.clone(),
            WebhookConfig {
                url: "http://127.0.0.1:9001/hook2".to_string(),
                ..Default::default()
            },
        );

        engine.trigger_completion(&job_id1, serde_json::json!({"status": "completed"}));
        engine.trigger_completion(&job_id2, serde_json::json!({"status": "completed"}));

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert_eq!(engine.delivery_count(), 2);
        assert_eq!(engine.active_delivery_count(), 2);

        engine.cancel_deliveries(&job_id1);

        assert_eq!(engine.delivery_count(), 2);
        assert_eq!(engine.active_delivery_count(), 1);

        let deliveries1 = engine.list_deliveries_for_job(&job_id1);
        assert_eq!(deliveries1.len(), 1);
        assert!(deliveries1[0].is_terminal());

        let deliveries2 = engine.list_deliveries_for_job(&job_id2);
        assert_eq!(deliveries2.len(), 1);
        assert!(deliveries2[0].is_active());
        
        let stats = engine.get_stats();
        assert_eq!(stats.total_failed, 1);
    }

    #[test]
    fn test_engine_get_delivery_status() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        let config = WebhookConfig {
            url: "https://example.com/hook".to_string(),
            ..Default::default()
        };
        engine.register_webhook(job_id, config);
        engine.trigger_completion(&job_id, serde_json::json!({}));

        let deliveries = engine.list_deliveries_for_job(&job_id);
        let delivery_id = &deliveries[0].id;

        let status = engine.get_delivery_status(delivery_id);
        assert!(status.is_some());
        assert!(matches!(status.unwrap(), WebhookDeliveryStatus::Pending));

        // Non-existent delivery
        assert!(engine.get_delivery_status("nonexistent").is_none());
    }

    #[test]
    fn test_engine_stats() {
        let engine = WebhookEngine::new();
        let stats = engine.get_stats();
        assert_eq!(stats.total_sent, 0);
        assert_eq!(stats.total_delivered, 0);
    }

    #[test]
    fn test_engine_with_custom_config() {
        let config = WebhookEngineConfig {
            max_concurrent: 8,
            timeout_secs: 15,
            max_retries: 3,
            backoff_base_secs: 2,
            backoff_max_secs: 120,
            max_payload_bytes: 512_000,
        };
        let engine = WebhookEngine::with_config(config);
        assert_eq!(engine.config.max_concurrent, 8);
        assert_eq!(engine.config.timeout_secs, 15);
    }

    #[test]
    fn test_engine_payload_size_rejection() {
        let config = WebhookEngineConfig {
            max_payload_bytes: 50, // very small
            ..Default::default()
        };
        let engine = WebhookEngine::with_config(config);
        let job_id = JobId::new();
        let wh_config = WebhookConfig {
            url: "https://example.com/hook".to_string(),
            ..Default::default()
        };
        engine.register_webhook(job_id, wh_config);

        // This payload is larger than 50 bytes when serialized
        let big_payload = serde_json::json!({"data": "x".repeat(100)});
        engine.trigger_completion(&job_id, big_payload);

        // Delivery should not have been created (dropped due to size)
        assert_eq!(engine.delivery_count(), 0);
    }

    #[test]
    fn test_engine_register_with_options() {
        let engine = WebhookEngine::new();
        let job_id = JobId::new();
        let config = WebhookConfig {
            url: "https://example.com/hook".to_string(),
            ..Default::default()
        };
        let filter = WebhookFilter::only(vec!["completion".to_string()]);
        let secret = Some("my-secret".to_string());

        engine.register_webhook_with_options(job_id, config, secret, Some(filter));
        assert!(engine.is_registered(&job_id));

        // Milestone should be filtered out
        engine.trigger_milestone(&job_id, 50, serde_json::json!({}));
        assert_eq!(engine.delivery_count(), 0);

        // Completion should go through
        engine.trigger_completion(&job_id, serde_json::json!({}));
        assert_eq!(engine.delivery_count(), 1);
    }

    #[test]
    fn test_hmac_known_vector() {
        // RFC 4231 Test Case 2:
        // Key = "Jefe"
        // Data = "what do ya want for nothing?"
        // HMAC-SHA-256 = 5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843
        let sig = WebhookSigner::sign_payload(b"what do ya want for nothing?", "Jefe");
        assert_eq!(
            sig,
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(WebhookSigner::constant_time_eq(b"abc", b"abc"));
        assert!(!WebhookSigner::constant_time_eq(b"abc", b"abd"));
        assert!(!WebhookSigner::constant_time_eq(b"abc", b"ab"));
    }
}
