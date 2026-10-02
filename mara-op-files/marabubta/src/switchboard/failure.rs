// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use std::fmt;

/// 7-level failure hierarchy for graduated degradation.
/// L0 = perfect, L6 = total failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureLevel {
    /// Total success, all backends responded
    L0,
    /// Partial success, primary backend succeeded but secondary failed
    L1,
    /// Fallback used, primary failed but fallback succeeded
    L2,
    /// Degraded result, only partial answer available
    L3,
    /// Classification only, couldn't compute but classified the expression
    L4,
    /// Input accepted, couldn't classify but stored for retry
    L5,
    /// Total failure, nothing worked
    L6,
}

impl FailureLevel {
    /// Returns true if this failure level is considered acceptable.
    /// L0-L3 are acceptable, L4-L6 require escalation.
    pub fn is_acceptable(&self) -> bool {
        matches!(self, Self::L0 | Self::L1 | Self::L2 | Self::L3)
    }

    /// Returns the severity score for ordering (0 = best, 6 = worst).
    fn severity(&self) -> u8 {
        match self {
            Self::L0 => 0,
            Self::L1 => 1,
            Self::L2 => 2,
            Self::L3 => 3,
            Self::L4 => 4,
            Self::L5 => 5,
            Self::L6 => 6,
        }
    }
}

impl fmt::Display for FailureLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::L0 => write!(f, "L0 (Total Success)"),
            Self::L1 => write!(f, "L1 (Partial Success)"),
            Self::L2 => write!(f, "L2 (Fallback Used)"),
            Self::L3 => write!(f, "L3 (Degraded Result)"),
            Self::L4 => write!(f, "L4 (Classification Only)"),
            Self::L5 => write!(f, "L5 (Input Accepted)"),
            Self::L6 => write!(f, "L6 (Total Failure)"),
        }
    }
}

impl PartialOrd for FailureLevel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FailureLevel {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.severity().cmp(&other.severity())
    }
}

/// A suggested action when failure occurs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suggestion {
    pub action: String,
    pub reason: String,
    pub handler_id: Option<String>,
}

/// Complete failure response with level, message, and recovery suggestions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureResponse {
    pub level: FailureLevel,
    pub message: String,
    pub partial_result: Option<String>,
    pub suggestions: Vec<Suggestion>,
    pub retry_after_ms: Option<u64>,
    pub original_error: Option<String>,
}

impl FailureResponse {
    /// Creates a new failure response with the given level and message.
    pub fn new(level: FailureLevel, message: impl Into<String>) -> Self {
        Self {
            level,
            message: message.into(),
            partial_result: None,
            suggestions: Vec::new(),
            retry_after_ms: None,
            original_error: None,
        }
    }

    /// Adds a partial result (whatever we managed to compute).
    pub fn with_partial(mut self, result: String) -> Self {
        self.partial_result = Some(result);
        self
    }

    /// Adds a suggested action with reason and optional handler.
    pub fn with_suggestion(mut self, action: String, reason: String) -> Self {
        self.suggestions.push(Suggestion {
            action,
            reason,
            handler_id: None,
        });
        self
    }

    /// Adds a suggested action with a specific handler ID.
    pub fn with_handler_suggestion(
        mut self,
        action: String,
        reason: String,
        handler_id: String,
    ) -> Self {
        self.suggestions.push(Suggestion {
            action,
            reason,
            handler_id: Some(handler_id),
        });
        self
    }

    /// Sets the retry delay for transient failures.
    pub fn with_retry(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
        self
    }

    /// Attaches the underlying error detail.
    pub fn with_error(mut self, error: String) -> Self {
        self.original_error = Some(error);
        self
    }
}

/// Records a single failure attempt for context tracking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureAttempt {
    pub backend: String,
    pub error: String,
    pub duration_ms: u64,
    pub level: FailureLevel,
}

/// Tracks cascading failures across multiple backend attempts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FailureContext {
    pub attempts: Vec<FailureAttempt>,
}

impl FailureContext {
    /// Creates a new empty failure context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a failure attempt.
    pub fn record(&mut self, backend: String, error: String, duration_ms: u64) {
        // Infer failure level based on error patterns
        let level = Self::infer_level(&error);
        self.attempts.push(FailureAttempt {
            backend,
            error,
            duration_ms,
            level,
        });
    }

    /// Records a failure attempt with explicit level.
    pub fn record_with_level(
        &mut self,
        backend: String,
        error: String,
        duration_ms: u64,
        level: FailureLevel,
    ) {
        self.attempts.push(FailureAttempt {
            backend,
            error,
            duration_ms,
            level,
        });
    }

    /// Returns the worst (highest severity) failure level encountered.
    pub fn worst_level(&self) -> FailureLevel {
        self.attempts
            .iter()
            .map(|a| a.level)
            .max()
            .unwrap_or(FailureLevel::L0)
    }

    /// Returns the total duration across all attempts.
    pub fn total_duration_ms(&self) -> u64 {
        self.attempts.iter().map(|a| a.duration_ms).sum()
    }

    /// Builds a composite failure response from all attempts.
    pub fn to_response(&self) -> FailureResponse {
        if self.attempts.is_empty() {
            return FailureResponse::new(FailureLevel::L0, "No failures recorded");
        }

        let worst = self.worst_level();
        let total_ms = self.total_duration_ms();
        let backends: Vec<_> = self.attempts.iter().map(|a| a.backend.as_str()).collect();

        let message = if self.attempts.len() == 1 {
            format!(
                "Backend '{}' failed: {}",
                self.attempts[0].backend, self.attempts[0].error
            )
        } else {
            format!(
                "{} backend(s) failed [{}] after {}ms",
                self.attempts.len(),
                backends.join(", "),
                total_ms
            )
        };

        let mut response = FailureResponse::new(worst, message);

        // Attach original error from worst failure
        if let Some(worst_attempt) = self.attempts.iter().max_by_key(|a| a.level) {
            response = response.with_error(format!(
                "{}: {}",
                worst_attempt.backend, worst_attempt.error
            ));
        }

        // Add suggestions based on failure patterns
        if self.has_timeout_pattern() {
            response = response
                .with_suggestion(
                    "Increase timeout".into(),
                    "Multiple backends timed out".into(),
                )
                .with_retry(5000);
        }

        if self.has_connection_pattern() {
            response = response
                .with_suggestion(
                    "Check network connectivity".into(),
                    "Connection failures detected".into(),
                )
                .with_retry(3000);
        }

        if self.has_overload_pattern() {
            response = response
                .with_suggestion(
                    "Wait and retry".into(),
                    "Backends appear overloaded".into(),
                )
                .with_retry(10000);
        }

        // If total failure, suggest alternative approaches
        if worst == FailureLevel::L6 {
            response = response.with_suggestion(
                "Try a different input format".into(),
                "All backends rejected the request".into(),
            );
        }

        response
    }

    /// Infers failure level from error message patterns.
    fn infer_level(error: &str) -> FailureLevel {
        let lower = error.to_lowercase();

        if lower.contains("timeout") || lower.contains("deadline") {
            FailureLevel::L5
        } else if lower.contains("connection") || lower.contains("network") {
            FailureLevel::L6
        } else if lower.contains("overload") || lower.contains("capacity") {
            FailureLevel::L5
        } else if lower.contains("parse") || lower.contains("invalid") {
            FailureLevel::L4
        } else {
            FailureLevel::L6
        }
    }

    /// Checks if multiple attempts show timeout pattern.
    fn has_timeout_pattern(&self) -> bool {
        self.attempts
            .iter()
            .filter(|a| a.error.to_lowercase().contains("timeout"))
            .count()
            >= 2
    }

    /// Checks if multiple attempts show connection failures.
    fn has_connection_pattern(&self) -> bool {
        self.attempts
            .iter()
            .filter(|a| a.error.to_lowercase().contains("connection"))
            .count()
            >= 2
    }

    /// Checks if backends appear overloaded.
    fn has_overload_pattern(&self) -> bool {
        self.attempts
            .iter()
            .any(|a| a.error.to_lowercase().contains("overload"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failure_level_ordering() {
        assert!(FailureLevel::L0 < FailureLevel::L1);
        assert!(FailureLevel::L1 < FailureLevel::L2);
        assert!(FailureLevel::L2 < FailureLevel::L3);
        assert!(FailureLevel::L3 < FailureLevel::L4);
        assert!(FailureLevel::L4 < FailureLevel::L5);
        assert!(FailureLevel::L5 < FailureLevel::L6);

        // Test transitivity
        assert!(FailureLevel::L0 < FailureLevel::L6);
        assert!(FailureLevel::L2 < FailureLevel::L5);
    }

    #[test]
    fn test_is_acceptable() {
        assert!(FailureLevel::L0.is_acceptable());
        assert!(FailureLevel::L1.is_acceptable());
        assert!(FailureLevel::L2.is_acceptable());
        assert!(FailureLevel::L3.is_acceptable());
        assert!(!FailureLevel::L4.is_acceptable());
        assert!(!FailureLevel::L5.is_acceptable());
        assert!(!FailureLevel::L6.is_acceptable());
    }

    #[test]
    fn test_failure_response_builder() {
        let response = FailureResponse::new(FailureLevel::L2, "Primary backend failed")
            .with_partial("2 + 2".to_string())
            .with_suggestion(
                "Use fallback".to_string(),
                "Primary unavailable".to_string(),
            )
            .with_retry(1000)
            .with_error("Connection refused".to_string());

        assert_eq!(response.level, FailureLevel::L2);
        assert_eq!(response.message, "Primary backend failed");
        assert_eq!(response.partial_result, Some("2 + 2".to_string()));
        assert_eq!(response.suggestions.len(), 1);
        assert_eq!(response.retry_after_ms, Some(1000));
        assert_eq!(response.original_error, Some("Connection refused".to_string()));
    }

    #[test]
    fn test_failure_context_recording() {
        let mut ctx = FailureContext::new();
        ctx.record("backend1".to_string(), "Timeout".to_string(), 500);
        ctx.record("backend2".to_string(), "Connection error".to_string(), 300);

        assert_eq!(ctx.attempts.len(), 2);
        assert_eq!(ctx.total_duration_ms(), 800);
    }

    #[test]
    fn test_worst_level_computation() {
        let mut ctx = FailureContext::new();
        ctx.record_with_level("b1".to_string(), "err1".to_string(), 100, FailureLevel::L2);
        ctx.record_with_level("b2".to_string(), "err2".to_string(), 200, FailureLevel::L5);
        ctx.record_with_level("b3".to_string(), "err3".to_string(), 150, FailureLevel::L1);

        assert_eq!(ctx.worst_level(), FailureLevel::L5);
    }

    #[test]
    fn test_context_to_response() {
        let mut ctx = FailureContext::new();
        ctx.record("backend1".to_string(), "Connection timeout".to_string(), 500);
        ctx.record("backend2".to_string(), "Connection timeout".to_string(), 600);

        let response = ctx.to_response();
        assert_eq!(response.level, FailureLevel::L5);
        assert!(response.message.contains("2 backend(s) failed"));
        assert!(response.retry_after_ms.is_some());
        assert!(!response.suggestions.is_empty());
    }

    #[test]
    fn test_serde_round_trip_failure_level() {
        let level = FailureLevel::L3;
        let json = serde_json::to_string(&level).unwrap();
        assert_eq!(json, "\"l3\"");
        let deserialized: FailureLevel = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, level);
    }

    #[test]
    fn test_serde_round_trip_failure_response() {
        let response = FailureResponse::new(FailureLevel::L4, "Classification failed")
            .with_partial("partial result".to_string())
            .with_suggestion("retry".to_string(), "transient error".to_string())
            .with_retry(2000);

        let json = serde_json::to_string(&response).unwrap();
        let deserialized: FailureResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, response);
    }
}
