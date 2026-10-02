// Marabunta - Licensed under the MIT License.
//! Error types for the governance system.

use thiserror::Error;

use super::principal::PrincipalId;

/// Errors that can occur in the governance system.
#[derive(Error, Debug, Clone)]
pub enum GovernanceError {
    // Principal errors
    #[error("Principal not found: {0}")]
    PrincipalNotFound(PrincipalId),

    #[error("Principal already exists: {0}")]
    PrincipalAlreadyExists(PrincipalId),

    #[error("Invalid principal: {0}")]
    InvalidPrincipal(String),

    // Delegation errors
    #[error("Delegation not found: {0}")]
    DelegationNotFound(String),

    #[error("Delegation already exists: {0}")]
    DelegationAlreadyExists(String),

    #[error("Cannot delegate: {0}")]
    CannotDelegate(String),

    #[error("Delegation cycle detected: {0}")]
    DelegationCycle(String),

    #[error("Delegation expired: {0}")]
    DelegationExpired(String),

    #[error("Delegation revoked: {0}")]
    DelegationRevoked(String),

    #[error("Exceeds delegation constraints: {0}")]
    ExceedsDelegationConstraints(String),

    // Authority errors
    #[error("No authority over domain: {0}")]
    NoAuthority(String),

    #[error("Insufficient priority: required {required}, have {actual}")]
    InsufficientPriority { required: u32, actual: u32 },

    #[error("Cannot override: {0}")]
    CannotOverride(String),

    // General errors
    #[error("Invalid pattern: {0}")]
    InvalidPattern(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Errors specific to authority checking.
#[derive(Error, Debug, Clone)]
pub enum AuthorityError {
    #[error("Principal not found: {0}")]
    PrincipalNotFound(PrincipalId),

    #[error("Delegation not found: {0}")]
    DelegationNotFound(String),

    #[error("Cannot delegate: {0}")]
    CannotDelegate(String),

    #[error("Delegation cycle detected involving: {0}")]
    DelegationCycle(String),

    #[error("Delegator has no authority over domain: {0}")]
    DelegatorNoAuthority(String),

    #[error("Cannot redelegate: redelegation not allowed by parent delegation")]
    CannotRedelegate,

    #[error("Max redelegation depth exceeded: {max_depth}")]
    MaxRedelegationDepthExceeded { max_depth: u32 },

    #[error("Priority {requested} exceeds max allowed {max_allowed}")]
    PriorityExceedsMax { requested: u32, max_allowed: u32 },

    #[error("Delegation expired")]
    DelegationExpired,

    #[error("Delegation revoked")]
    DelegationRevoked,
}

/// Result type for governance operations.
pub type GovernanceResult<T> = Result<T, GovernanceError>;
