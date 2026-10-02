// Marabunta - Licensed under the MIT License.
//! Error types for the Switchboard module.
//!
//! Comprehensive error handling covering input validation, session management,
//! oracle integration, rate limiting, and pipeline execution failures.

use thiserror::Error;

/// Error type for all Switchboard operations.
///
/// Covers the full lifecycle from input ingestion through classification,
/// routing, oracle invocation, and session management. Each variant provides
/// structured context for debugging and user-facing error messages.
#[derive(Debug, Error)]
pub enum SwitchboardError {
    /// Input exceeds configured size limits (prevents DoS via large payloads).
    #[error("input too large: {size} bytes exceeds maximum {max}")]
    InputTooLarge { size: usize, max: usize },

    /// Input format is malformed or unparseable.
    #[error("invalid input format: {0}")]
    InvalidFormat(String),

    /// Input type is recognized but not supported by this switchboard instance.
    #[error("unsupported input: {0}")]
    UnsupportedInput(String),

    /// Classification stage failed to determine input type or route.
    #[error("classification failed: {0}")]
    ClassificationFailed(String),

    /// Session ID does not exist in the session store.
    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// Session exists but has exceeded its TTL.
    #[error("session expired: {0}")]
    SessionExpired(String),

    /// No registered handler for the classified input type.
    #[error("handler not found: {0}")]
    HandlerNotFound(String),

    /// Oracle backend returned an error during inference or processing.
    #[error("oracle error: {backend}: {message}")]
    OracleError { backend: String, message: String },

    /// Oracle call exceeded configured timeout.
    #[error("oracle timeout after {timeout_ms}ms: {backend}")]
    OracleTimeout {
        backend: String,
        timeout_ms: u64,
    },

    /// Rate limit exceeded for client, endpoint, or resource.
    #[error("rate limited: {0}")]
    RateLimited(String),

    /// Image decoding, resizing, or format conversion failed.
    #[error("image processing error: {0}")]
    ImageError(String),

    /// Mathpix OCR API returned an error status.
    #[error("mathpix API error: {status}: {message}")]
    MathpixError { status: u16, message: String },

    /// Input failed sanitization checks (e.g., SQL injection, XSS, SSRF).
    #[error("sanitization rejected: {0}")]
    SanitizationRejected(String),

    /// Pipeline execution time exceeded allocated budget.
    #[error("pipeline budget exceeded: {elapsed_ms}ms > {budget_ms}ms")]
    BudgetExceeded {
        elapsed_ms: u64,
        budget_ms: u64,
    },

    /// Maximum concurrent sessions reached (backpressure).
    #[error("max sessions reached: {0}")]
    MaxSessionsReached(usize),

    /// Catch-all for internal errors (panics, unexpected states, etc.).
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type alias for Switchboard operations.
pub type SwitchboardResult<T> = Result<T, SwitchboardError>;

// ============================================================================
// From implementations for common error types
// ============================================================================

impl From<std::io::Error> for SwitchboardError {
    fn from(err: std::io::Error) -> Self {
        Self::Internal(format!("io error: {}", err))
    }
}

impl From<serde_json::Error> for SwitchboardError {
    fn from(err: serde_json::Error) -> Self {
        Self::Internal(format!("json error: {}", err))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_input_too_large_display() {
        let err = SwitchboardError::InputTooLarge {
            size: 10_000_000,
            max: 5_000_000,
        };
        assert_eq!(
            err.to_string(),
            "input too large: 10000000 bytes exceeds maximum 5000000"
        );
    }

    #[test]
    fn test_oracle_error_display() {
        let err = SwitchboardError::OracleError {
            backend: "claude-opus-4".to_string(),
            message: "rate limit exceeded".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "oracle error: claude-opus-4: rate limit exceeded"
        );
    }

    #[test]
    fn test_oracle_timeout_display() {
        let err = SwitchboardError::OracleTimeout {
            backend: "gpt-4".to_string(),
            timeout_ms: 30000,
        };
        assert_eq!(
            err.to_string(),
            "oracle timeout after 30000ms: gpt-4"
        );
    }

    #[test]
    fn test_budget_exceeded_display() {
        let err = SwitchboardError::BudgetExceeded {
            elapsed_ms: 5500,
            budget_ms: 5000,
        };
        assert_eq!(
            err.to_string(),
            "pipeline budget exceeded: 5500ms > 5000ms"
        );
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let sb_err: SwitchboardError = io_err.into();
        match sb_err {
            SwitchboardError::Internal(msg) => {
                assert!(msg.contains("io error"));
                assert!(msg.contains("file not found"));
            }
            _ => panic!("expected Internal variant"),
        }
    }

    #[test]
    fn test_from_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("{invalid json").unwrap_err();
        let sb_err: SwitchboardError = json_err.into();
        match sb_err {
            SwitchboardError::Internal(msg) => {
                assert!(msg.contains("json error"));
            }
            _ => panic!("expected Internal variant"),
        }
    }
}
