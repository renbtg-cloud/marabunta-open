// Marabunta - Licensed under the MIT License.
//! Error types for the tenancy module.

use thiserror::Error;

use super::types::TenantId;

/// Errors that can occur in the tenancy module.
#[derive(Debug, Error)]
pub enum TenancyError {
    /// Tenant not found.
    #[error("Tenant not found: {0}")]
    TenantNotFound(TenantId),

    /// Tenant already exists.
    #[error("Tenant already exists: {0}")]
    TenantAlreadyExists(TenantId),

    /// Tenant slug already taken.
    #[error("Tenant slug already taken: {0}")]
    SlugTaken(String),

    /// Invalid tenant hierarchy.
    #[error("Invalid tenant hierarchy: {0}")]
    InvalidHierarchy(String),

    /// Parent tenant not found.
    #[error("Parent tenant not found: {0}")]
    ParentNotFound(TenantId),

    /// Tenant is not active.
    #[error("Tenant is not active: {0}")]
    TenantNotActive(TenantId),

    /// User not authorized.
    #[error("User not authorized: {0}")]
    Unauthorized(String),

    /// User not a member of tenant.
    #[error("User {0} is not a member of tenant {1}")]
    NotMember(String, TenantId),

    /// Quota exceeded.
    #[error("Quota exceeded for tenant {0}: {1}")]
    QuotaExceeded(TenantId, String),

    /// Policy violation.
    #[error("Policy violation for tenant {0}: {1}")]
    PolicyViolation(TenantId, String),

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Cross-tenant access denied.
    #[error("Cross-tenant access denied: tenant {0} cannot access resources of tenant {1}")]
    CrossTenantAccessDenied(TenantId, TenantId),

    /// Persistence error.
    #[error("Persistence error: {0}")]
    Persistence(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for tenancy operations.
pub type TenancyResult<T> = Result<T, TenancyError>;

impl From<crate::storage::persistence::PersistenceError> for TenancyError {
    fn from(err: crate::storage::persistence::PersistenceError) -> Self {
        TenancyError::Persistence(err.to_string())
    }
}
