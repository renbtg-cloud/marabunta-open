// Marabunta - Licensed under the MIT License.
//! Error types for the security module.

use thiserror::Error;

/// Errors that can occur in the security system.
#[derive(Error, Debug)]
pub enum SecurityError {
    // TLS errors
    #[error("TLS configuration error: {0}")]
    TlsConfig(String),

    #[error("Certificate error: {0}")]
    Certificate(String),

    #[error("Private key error: {0}")]
    PrivateKey(String),

    #[error("Certificate validation failed: {0}")]
    CertificateValidation(String),

    #[error("TLS handshake failed: {0}")]
    TlsHandshake(String),

    // Authentication errors
    #[error("Authentication required")]
    AuthenticationRequired,

    #[error("Invalid token")]
    InvalidToken,

    #[error("Token expired")]
    TokenExpired,

    #[error("Token revoked")]
    TokenRevoked,

    #[error("Insufficient permissions: required {required:?}, have {actual:?}")]
    InsufficientPermissions { required: String, actual: String },

    #[error("Token not found: {0}")]
    TokenNotFound(String),

    #[error("Token already exists: {0}")]
    TokenAlreadyExists(String),

    // Node authentication errors
    #[error("Invalid registration token")]
    InvalidRegistrationToken,

    #[error("Registration token expired")]
    RegistrationTokenExpired,

    #[error("Registration token already used")]
    RegistrationTokenUsed,

    #[error("Node identity verification failed: {0}")]
    NodeVerificationFailed(String),

    #[error("Node not registered: {0}")]
    NodeNotRegistered(String),

    #[error("Node already registered: {0}")]
    NodeAlreadyRegistered(String),

    #[error("Mutual TLS required")]
    MutualTlsRequired,

    #[error("Client certificate required")]
    ClientCertificateRequired,

    // Storage errors
    #[error("Storage error: {0}")]
    Storage(String),

    // Crypto errors
    #[error("Cryptographic error: {0}")]
    Crypto(String),

    // I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    // Internal errors
    #[error("Internal security error: {0}")]
    Internal(String),
}

/// Result type for security operations.
pub type SecurityResult<T> = Result<T, SecurityError>;
