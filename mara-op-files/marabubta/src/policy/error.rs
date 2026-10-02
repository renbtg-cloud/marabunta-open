// Marabunta - Licensed under the MIT License.
//! Error types for the policy module
//!
//! Provides detailed error types for policy operations with error codes,
//! context, and actionable suggestions.

use thiserror::Error;

use super::ir::PolicyId;
use crate::error::{ErrorCode, MarabuntaError};

/// Errors that can occur during policy operations
#[derive(Error, Debug)]
pub enum PolicyError {
    /// Policy with this ID already exists
    #[error("[E403] Policy already exists: {0}")]
    PolicyAlreadyExists(PolicyId),

    /// Policy with this ID was not found
    #[error("[E401] Policy not found: {0}")]
    PolicyNotFound(PolicyId),

    /// Policy is invalid
    #[error("[E403] Invalid policy: {0}")]
    InvalidPolicy(String),

    /// Policy condition is invalid
    #[error("[E403] Invalid condition: {0}")]
    InvalidCondition(String),

    /// Policy effect is invalid
    #[error("[E403] Invalid effect: {0}")]
    InvalidEffect(String),

    /// Regex compilation failed
    #[error("[E403] Invalid regex pattern: {pattern}: {message}")]
    InvalidRegex { pattern: String, message: String },

    /// Cron expression is invalid
    #[error("[E403] Invalid cron expression: {0}")]
    InvalidCronExpression(String),

    /// Policy conflict detected
    #[error("[E402] Policy conflict: {0}")]
    PolicyConflict(String),

    /// Override not allowed
    #[error("[E405] Override not allowed for policy {policy_id}: {reason}")]
    OverrideNotAllowed { policy_id: PolicyId, reason: String },

    /// Evaluation failed
    #[error("[E400] Evaluation failed: {0}")]
    EvaluationFailed(String),

    /// Serialization/deserialization error
    #[error("[E902] Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Authority error
    #[error("[E404] Insufficient authority: {0}")]
    InsufficientAuthority(String),

    /// Policy violation
    #[error("[E400] Policy violation: {policy_id} ({policy_name}): {reason}")]
    PolicyViolation {
        policy_id: PolicyId,
        policy_name: String,
        action: String,
        reason: String,
    },

    /// Internal error
    #[error("[E900] Internal error: {0}")]
    Internal(String),
}

impl PolicyError {
    /// Get the error code for this error
    pub fn error_code(&self) -> ErrorCode {
        match self {
            PolicyError::PolicyNotFound(_) => ErrorCode::POLICY_NOT_FOUND,
            PolicyError::PolicyConflict(_) => ErrorCode::POLICY_CONFLICT,
            PolicyError::PolicyAlreadyExists(_)
            | PolicyError::InvalidPolicy(_)
            | PolicyError::InvalidCondition(_)
            | PolicyError::InvalidEffect(_)
            | PolicyError::InvalidRegex { .. }
            | PolicyError::InvalidCronExpression(_) => ErrorCode::POLICY_INVALID,
            PolicyError::OverrideNotAllowed { .. } => ErrorCode::OVERRIDE_NOT_ALLOWED,
            PolicyError::EvaluationFailed(_) | PolicyError::PolicyViolation { .. } => {
                ErrorCode::POLICY_VIOLATION
            }
            PolicyError::InsufficientAuthority(_) => ErrorCode::INSUFFICIENT_AUTHORITY,
            PolicyError::SerializationError(_) => ErrorCode::SERIALIZATION_ERROR,
            PolicyError::Internal(_) => ErrorCode::INTERNAL_ERROR,
        }
    }

    /// Get suggestions for resolving this error
    pub fn suggestions(&self) -> Vec<&'static str> {
        match self {
            PolicyError::PolicyNotFound(_) => vec![
                "Verify the policy ID is correct",
                "List available policies with 'marabunta policy list'",
                "The policy may have been deleted or expired",
            ],
            PolicyError::PolicyAlreadyExists(_) => vec![
                "Use a different policy ID",
                "Update the existing policy with 'marabunta policy update'",
                "Delete the existing policy first with 'marabunta policy delete'",
            ],
            PolicyError::InvalidPolicy(_) => vec![
                "Review the policy definition for syntax errors",
                "Ensure all required fields are present",
                "Check the policy schema documentation",
            ],
            PolicyError::InvalidCondition(_) => vec![
                "Review the condition syntax",
                "Valid conditions: Always, Never, JobMatches, SubmitterMatches, TimeWindow, etc.",
                "Ensure all required condition fields are present",
            ],
            PolicyError::InvalidEffect(_) => vec![
                "Review the effect syntax",
                "Valid effects: Prefer, Require, Exclude, Affinity, SetPriority, etc.",
                "Ensure weight values are between 0.0 and 1.0",
            ],
            PolicyError::InvalidRegex { .. } => vec![
                "Check regex syntax - common issues include unescaped special characters",
                "Test your regex at https://regex101.com/",
                "Use .* for wildcard matching, not *",
            ],
            PolicyError::InvalidCronExpression(_) => vec![
                "Use standard cron format: minute hour day month weekday",
                "Example: '0 9 * * 1-5' for weekdays at 9am",
                "Use * for any value, ranges with -, lists with ,",
            ],
            PolicyError::PolicyConflict(_) => vec![
                "Review the conflicting policies",
                "Adjust policy priorities to resolve conflicts",
                "Use 'marabunta policy conflicts' to analyze policy interactions",
                "Consider merging or refactoring overlapping policies",
            ],
            PolicyError::OverrideNotAllowed { .. } => vec![
                "This policy is marked as mandatory and cannot be overridden",
                "Contact the policy owner to request an exception",
                "Check if a different approach can achieve your goal",
            ],
            PolicyError::EvaluationFailed(_) => vec![
                "Check the policy conditions and context",
                "Ensure required context fields are provided",
                "Review policy logs for detailed failure information",
            ],
            PolicyError::PolicyViolation { .. } => vec![
                "Review the policy requirements",
                "Modify your request to comply with the policy",
                "Contact the policy owner for exceptions",
                "Check if alternative approaches are allowed",
            ],
            PolicyError::InsufficientAuthority(_) => vec![
                "Request delegation from a principal with authority",
                "Contact your administrator for access",
                "Verify you're using the correct credentials",
            ],
            PolicyError::SerializationError(_) => vec![
                "This may indicate corrupted policy data",
                "Try recreating the policy",
                "Report this issue if it persists",
            ],
            PolicyError::Internal(_) => vec![
                "This is likely a bug - please report it",
                "Include the full error message and context in your report",
            ],
        }
    }

    /// Format the error with colored output for CLI display
    pub fn format_cli(&self, use_color: bool) -> String {
        let mut output = String::new();

        // Error header
        if use_color {
            output.push_str("\x1b[1;31m");
        }
        output.push_str(&format!("error[{}]", self.error_code()));
        if use_color {
            output.push_str("\x1b[0m");
        }
        output.push_str(": ");

        // Error message
        if use_color {
            output.push_str("\x1b[1m");
        }
        // Extract message without [Exxx] prefix
        let msg = self.to_string();
        let msg = if msg.starts_with('[') {
            msg.split(']')
                .skip(1)
                .collect::<Vec<_>>()
                .join("]")
                .trim_start()
                .to_string()
        } else {
            msg
        };
        output.push_str(&msg);
        if use_color {
            output.push_str("\x1b[0m");
        }
        output.push('\n');

        // Add policy violation details
        if let PolicyError::PolicyViolation {
            policy_id,
            policy_name,
            action,
            reason,
        } = self
        {
            output.push('\n');
            if use_color {
                output.push_str("\x1b[36m");
            }
            output.push_str(&format!(
                "  Policy ID:   {}\n  Policy Name: {}\n  Action:      {}\n  Reason:      {}\n",
                policy_id, policy_name, action, reason
            ));
            if use_color {
                output.push_str("\x1b[0m");
            }
        }

        // Suggestions
        let suggestions = self.suggestions();
        if !suggestions.is_empty() {
            output.push('\n');
            if use_color {
                output.push_str("\x1b[1;32m");
            }
            output.push_str("help");
            if use_color {
                output.push_str("\x1b[0m");
            }
            output.push_str(": ");

            for (i, suggestion) in suggestions.iter().enumerate() {
                if i > 0 {
                    output.push_str("\n      ");
                }
                output.push_str(suggestion);
            }
            output.push('\n');
        }

        // Documentation link
        if use_color {
            output.push_str("\x1b[2m");
        }
        output.push_str(&format!(
            "\nFor more info, see https://marabunta-compute.io/docs/errors/{}\n",
            self.error_code()
        ));
        if use_color {
            output.push_str("\x1b[0m");
        }

        output
    }

    /// Create a policy violation error
    pub fn violation(
        policy_id: impl Into<String>,
        policy_name: impl Into<String>,
        action: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        PolicyError::PolicyViolation {
            policy_id: policy_id.into(),
            policy_name: policy_name.into(),
            action: action.into(),
            reason: reason.into(),
        }
    }
}

/// Convert PolicyError to MarabuntaError
impl From<PolicyError> for MarabuntaError {
    fn from(err: PolicyError) -> Self {
        match err {
            PolicyError::PolicyViolation {
                policy_id,
                policy_name,
                action,
                reason,
            } => MarabuntaError::policy_violation(&policy_id, &policy_name, &action, &reason),
            PolicyError::PolicyNotFound(id) => MarabuntaError::policy_not_found(id),
            PolicyError::PolicyConflict(msg) => {
                MarabuntaError::new(ErrorCode::POLICY_CONFLICT, "Policy conflict detected")
                    .with_field("Details", msg)
            }
            PolicyError::InsufficientAuthority(msg) => {
                MarabuntaError::new(ErrorCode::INSUFFICIENT_AUTHORITY, "Insufficient authority")
                    .with_field("Details", msg)
            }
            PolicyError::OverrideNotAllowed { policy_id, reason } => {
                MarabuntaError::new(ErrorCode::OVERRIDE_NOT_ALLOWED, "Override not allowed")
                    .with_field("Policy", policy_id)
                    .with_field("Reason", reason)
            }
            PolicyError::InvalidRegex { pattern, message } => {
                MarabuntaError::new(ErrorCode::POLICY_INVALID, "Invalid regex pattern")
                    .with_field("Pattern", pattern)
                    .with_field("Error", message)
                    .with_suggestion("Test your regex at https://regex101.com/")
            }
            PolicyError::InvalidCronExpression(expr) => {
                MarabuntaError::new(ErrorCode::POLICY_INVALID, "Invalid cron expression")
                    .with_field("Expression", expr)
                    .with_suggestion("Use format: minute hour day month weekday")
            }
            _ => MarabuntaError::new(err.error_code(), err.to_string()),
        }
    }
}

/// Result type for policy operations
pub type PolicyResult<T> = Result<T, PolicyError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = PolicyError::PolicyNotFound("test-policy".to_string());
        assert!(err.to_string().contains("Policy not found"));
        assert!(err.to_string().contains("test-policy"));
        assert!(err.to_string().contains("[E401]"));

        let err = PolicyError::InvalidRegex {
            pattern: "[invalid".to_string(),
            message: "unclosed bracket".to_string(),
        };
        assert!(err.to_string().contains("Invalid regex pattern"));
        assert!(err.to_string().contains("[E403]"));
    }

    #[test]
    fn test_error_code() {
        assert_eq!(
            PolicyError::PolicyNotFound("test".to_string()).error_code(),
            ErrorCode::POLICY_NOT_FOUND
        );
        assert_eq!(
            PolicyError::PolicyConflict("test".to_string()).error_code(),
            ErrorCode::POLICY_CONFLICT
        );
        assert_eq!(
            PolicyError::InvalidPolicy("test".to_string()).error_code(),
            ErrorCode::POLICY_INVALID
        );
    }

    #[test]
    fn test_suggestions() {
        let err = PolicyError::InvalidRegex {
            pattern: "[invalid".to_string(),
            message: "unclosed bracket".to_string(),
        };
        let suggestions = err.suggestions();
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.contains("regex101")));
    }

    #[test]
    fn test_policy_violation() {
        let err = PolicyError::violation(
            "prod-only",
            "Production Only",
            "submit_job",
            "Non-production user",
        );

        if let PolicyError::PolicyViolation {
            policy_id,
            policy_name,
            action,
            reason,
        } = err
        {
            assert_eq!(policy_id, "prod-only");
            assert_eq!(policy_name, "Production Only");
            assert_eq!(action, "submit_job");
            assert_eq!(reason, "Non-production user");
        } else {
            panic!("Expected PolicyViolation");
        }
    }

    #[test]
    fn test_cli_format() {
        let err = PolicyError::violation(
            "gpu-required",
            "GPU Required Policy",
            "schedule_task",
            "Job requires GPU but no GPU nodes available",
        );
        let formatted = err.format_cli(false);

        assert!(formatted.contains("error[E400]"));
        assert!(formatted.contains("Policy ID:"));
        assert!(formatted.contains("gpu-required"));
        assert!(formatted.contains("help"));
    }

    #[test]
    fn test_conversion_to_marabunta_error() {
        let policy_err = PolicyError::PolicyNotFound("missing-policy".to_string());
        let marabunta_err: MarabuntaError = policy_err.into();

        assert_eq!(marabunta_err.code, ErrorCode::POLICY_NOT_FOUND);
    }
}
