// Marabunta - Licensed under the MIT License.
//! Error types for the quota system
//!
//! Provides detailed error types for quota operations with error codes,
//! context, and actionable suggestions.

use thiserror::Error;

use super::types::{AccountId, QuotaId, QuotaResource};
use crate::error::{ErrorCode, MarabuntaError};

/// Error type for quota operations
#[derive(Error, Debug, Clone)]
pub enum QuotaError {
    /// Quota not found
    #[error("[E303] Quota not found: {0}")]
    QuotaNotFound(QuotaId),

    /// Quota already exists
    #[error("[E403] Quota already exists: {0}")]
    QuotaAlreadyExists(QuotaId),

    /// Account not found
    #[error("[E304] Account not found: {0}")]
    AccountNotFound(AccountId),

    /// Account already exists
    #[error("[E403] Account already exists: {0}")]
    AccountAlreadyExists(AccountId),

    /// Quota exceeded
    #[error(
        "[E300] Quota exceeded for resource {resource:?}: requested {requested}, available {available}"
    )]
    QuotaExceeded {
        resource: QuotaResource,
        requested: f64,
        available: f64,
    },

    /// Insufficient allocation
    #[error("[E301] Insufficient allocation for resource {resource:?}: requested {requested}, allocated {allocated}")]
    InsufficientAllocation {
        resource: QuotaResource,
        requested: f64,
        allocated: f64,
    },

    /// Reservation not found
    #[error("[E303] Reservation not found: {0}")]
    ReservationNotFound(String),

    /// Reservation expired
    #[error("[E302] Reservation expired: {0}")]
    ReservationExpired(String),

    /// Transfer not allowed
    #[error("[E305] Transfer not allowed: {reason}")]
    TransferNotAllowed { reason: String },

    /// Invalid quota configuration
    #[error("[E403] Invalid quota configuration: {0}")]
    InvalidConfiguration(String),

    /// Principal not authorized
    #[error("[E404] Principal {principal} not authorized for account {account}")]
    NotAuthorized {
        principal: String,
        account: AccountId,
    },

    /// Allocation not found
    #[error("[E303] No allocation found for quota {quota_id} in account {account_id}")]
    AllocationNotFound {
        quota_id: QuotaId,
        account_id: AccountId,
    },

    /// Allocation expired
    #[error("[E302] Allocation for quota {quota_id} has expired")]
    AllocationExpired { quota_id: QuotaId },

    /// Invalid amount
    #[error("[E500] Invalid amount: {0}")]
    InvalidAmount(String),

    /// Internal error
    #[error("[E900] Internal error: {0}")]
    Internal(String),
}

impl QuotaError {
    /// Get the error code for this error
    pub fn error_code(&self) -> ErrorCode {
        match self {
            QuotaError::QuotaExceeded { .. } => ErrorCode::QUOTA_EXCEEDED,
            QuotaError::InsufficientAllocation { .. } => ErrorCode::INSUFFICIENT_BALANCE,
            QuotaError::AllocationExpired { .. } | QuotaError::ReservationExpired(_) => {
                ErrorCode::ALLOCATION_EXPIRED
            }
            QuotaError::ReservationNotFound(_)
            | QuotaError::QuotaNotFound(_)
            | QuotaError::AllocationNotFound { .. } => ErrorCode::RESERVATION_NOT_FOUND,
            QuotaError::AccountNotFound(_) => ErrorCode::ACCOUNT_NOT_FOUND,
            QuotaError::TransferNotAllowed { .. } => ErrorCode::TRANSFER_NOT_ALLOWED,
            QuotaError::NotAuthorized { .. } => ErrorCode::INSUFFICIENT_AUTHORITY,
            QuotaError::InvalidConfiguration(_)
            | QuotaError::QuotaAlreadyExists(_)
            | QuotaError::AccountAlreadyExists(_) => ErrorCode::POLICY_INVALID,
            QuotaError::InvalidAmount(_) => ErrorCode::INVALID_INPUT,
            QuotaError::Internal(_) => ErrorCode::INTERNAL_ERROR,
        }
    }

    /// Get suggestions for resolving this error
    pub fn suggestions(&self) -> Vec<&'static str> {
        match self {
            QuotaError::QuotaExceeded { resource, .. } => {
                let base = vec![
                    "Wait for quota to replenish",
                    "Request a quota increase from your administrator",
                ];
                let resource_specific: Vec<&'static str> = match resource {
                    QuotaResource::CpuHours => vec![
                        "Reduce CPU requirements for your jobs",
                        "Use 'marabunta quota usage' to check current consumption",
                    ],
                    QuotaResource::MemoryGbHours | QuotaResource::MemoryGBHours => vec![
                        "Reduce memory requirements for your jobs",
                        "Consider using checkpointing to free memory periodically",
                    ],
                    QuotaResource::GpuHours => vec![
                        "Wait for GPU quota to replenish (usually monthly)",
                        "Consider using CPU-only jobs if possible",
                    ],
                    QuotaResource::StorageGb | QuotaResource::StorageGB => vec![
                        "Clean up old job results with 'marabunta results cleanup'",
                        "Remove unused checkpoints",
                    ],
                    QuotaResource::NetworkGb | QuotaResource::NetworkEgressGB => vec![
                        "Optimize data transfer by compressing payloads",
                        "Use local caching where possible",
                    ],
                    QuotaResource::JobCount | QuotaResource::ConcurrentJobs | QuotaResource::TotalJobsPerPeriod => vec![
                        "Wait for current jobs to complete",
                        "Cancel unnecessary pending jobs",
                    ],
                    QuotaResource::TaskCount | QuotaResource::ConcurrentTasks => vec![
                        "Reduce number of tasks per job",
                        "Batch multiple operations into fewer tasks",
                    ],
                    QuotaResource::Composite { .. } => {
                        vec!["Review composite quota components and reduce usage of individual resources"]
                    }
                    QuotaResource::Custom(_) => {
                        vec!["Contact your administrator for custom resource limits"]
                    }
                };
                [base, resource_specific].concat()
            }
            QuotaError::InsufficientAllocation { .. } => vec![
                "Earn tokens by contributing compute resources",
                "Check 'marabunta tokens balance' for current balance",
                "Use 'marabunta tokens claim' to claim pending rewards",
                "Reduce resource requirements for the job",
            ],
            QuotaError::AccountNotFound(_) => vec![
                "Verify the account ID is correct",
                "Contact your administrator to create the account",
                "Check 'marabunta quota accounts' for available accounts",
            ],
            QuotaError::QuotaNotFound(_) => vec![
                "Verify the quota ID is correct",
                "List available quotas with 'marabunta quota list'",
            ],
            QuotaError::AllocationNotFound { .. } => vec![
                "Request an allocation from the quota manager",
                "Contact your administrator for access",
            ],
            QuotaError::AllocationExpired { .. } | QuotaError::ReservationExpired(_) => vec![
                "Request a new allocation or reservation",
                "Allocations typically need periodic renewal",
            ],
            QuotaError::ReservationNotFound(_) => vec![
                "Verify the reservation ID is correct",
                "The reservation may have expired or been cancelled",
            ],
            QuotaError::TransferNotAllowed { .. } => vec![
                "Check transfer policies with your administrator",
                "Ensure both source and destination accounts are valid",
            ],
            QuotaError::NotAuthorized { .. } => vec![
                "Request access from the account owner",
                "Use an account you have authorization for",
            ],
            QuotaError::InvalidConfiguration(_) => vec![
                "Review quota configuration format",
                "Ensure all required fields are present",
            ],
            QuotaError::QuotaAlreadyExists(_) | QuotaError::AccountAlreadyExists(_) => vec![
                "Use the existing quota/account instead",
                "Choose a different ID if creating new",
            ],
            QuotaError::InvalidAmount(_) => vec![
                "Amounts must be positive numbers",
                "Check the expected units for this resource",
            ],
            QuotaError::Internal(_) => vec![
                "This is likely a bug - please report it",
                "Include the full error message in your report",
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

        // Add usage details for quota exceeded
        if let QuotaError::QuotaExceeded {
            resource,
            requested,
            available,
        } = self
        {
            output.push('\n');
            if use_color {
                output.push_str("\x1b[36m");
            }
            output.push_str(&format!(
                "  Resource:  {:?}\n  Requested: {:.2}\n  Available: {:.2}\n  Shortfall: {:.2}\n",
                resource,
                requested,
                available,
                requested - available
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
}

/// Convert QuotaError to MarabuntaError
impl From<QuotaError> for MarabuntaError {
    fn from(err: QuotaError) -> Self {
        match err {
            QuotaError::QuotaExceeded {
                resource,
                requested,
                available,
            } => MarabuntaError::quota_exceeded(
                format!("{:?}", resource),
                requested,
                available,
                "default",
            ),
            QuotaError::InsufficientAllocation {
                resource,
                requested,
                allocated,
            } => MarabuntaError::insufficient_balance(requested, allocated, format!("{:?}", resource)),
            QuotaError::AccountNotFound(id) => MarabuntaError::account_not_found(id.to_string()),
            QuotaError::NotAuthorized { principal, account } => {
                MarabuntaError::insufficient_authority(&principal, "access_quota", account.to_string())
            }
            QuotaError::TransferNotAllowed { reason } => {
                MarabuntaError::new(ErrorCode::TRANSFER_NOT_ALLOWED, "Transfer not allowed")
                    .with_field("Reason", reason)
                    .with_suggestion("Check transfer policies with your administrator")
            }
            QuotaError::AllocationExpired { quota_id } => {
                MarabuntaError::new(ErrorCode::ALLOCATION_EXPIRED, "Allocation expired")
                    .with_field("Quota", quota_id.to_string())
                    .with_suggestion("Request a new allocation")
            }
            _ => MarabuntaError::new(err.error_code(), err.to_string()),
        }
    }
}

/// Result type for quota operations
pub type QuotaResult<T> = Result<T, QuotaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_exceeded_display() {
        let err = QuotaError::QuotaExceeded {
            resource: QuotaResource::CpuHours,
            requested: 100.0,
            available: 50.0,
        };
        let msg = err.to_string();
        assert!(msg.contains("[E300]"));
        assert!(msg.contains("Quota exceeded"));
        assert!(msg.contains("CpuHours"));
    }

    #[test]
    fn test_error_code() {
        assert_eq!(
            QuotaError::QuotaExceeded {
                resource: QuotaResource::CpuHours,
                requested: 100.0,
                available: 50.0,
            }
            .error_code(),
            ErrorCode::QUOTA_EXCEEDED
        );

        assert_eq!(
            QuotaError::AccountNotFound(AccountId::from("test")).error_code(),
            ErrorCode::ACCOUNT_NOT_FOUND
        );
    }

    #[test]
    fn test_suggestions() {
        let err = QuotaError::QuotaExceeded {
            resource: QuotaResource::CpuHours,
            requested: 100.0,
            available: 50.0,
        };
        let suggestions = err.suggestions();
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.contains("quota")));
    }

    #[test]
    fn test_cli_format() {
        let err = QuotaError::QuotaExceeded {
            resource: QuotaResource::MemoryGbHours,
            requested: 200.0,
            available: 100.0,
        };
        let formatted = err.format_cli(false);

        assert!(formatted.contains("error[E300]"));
        assert!(formatted.contains("Shortfall: 100.00"));
        assert!(formatted.contains("help"));
    }

    #[test]
    fn test_conversion_to_marabunta_error() {
        let quota_err = QuotaError::QuotaExceeded {
            resource: QuotaResource::GpuHours,
            requested: 50.0,
            available: 10.0,
        };
        let marabunta_err: MarabuntaError = quota_err.into();

        assert_eq!(marabunta_err.code, ErrorCode::QUOTA_EXCEEDED);
    }
}
