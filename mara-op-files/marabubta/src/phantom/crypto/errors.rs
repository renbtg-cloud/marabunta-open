// Marabunta - Licensed under the MIT License.
//! Cryptographic error types for Phantom Protocol.
//!
//! This module defines all error types used throughout the cryptographic primitives.
//! All errors are designed to be opaque to prevent information leakage about
//! cryptographic operations.
//!
//! # Security Notes
//!
//! - Error messages intentionally avoid revealing sensitive information
//! - Timing-safe error handling should be used when processing these errors
//! - Do not log detailed error information in production environments

use thiserror::Error;

/// Errors that can occur during cryptographic operations.
///
/// These errors are designed to provide useful debugging information during
/// development while remaining secure in production (no key material leakage).
#[derive(Debug, Error)]
pub enum CryptoError {
    /// Key size does not match expected value.
    ///
    /// This error occurs when a key of incorrect size is provided to a
    /// cryptographic operation.
    #[error("Invalid key size: expected {expected}, got {actual}")]
    InvalidKeySize {
        /// Expected key size in bytes
        expected: usize,
        /// Actual key size provided
        actual: usize,
    },

    /// RSA modulus size is invalid or too small for security.
    ///
    /// Minimum recommended size is 2048 bits for production use.
    #[error("Invalid RSA modulus size: {0} bits (minimum 2048 required)")]
    InvalidModulusSize(usize),

    /// Signature verification failed.
    ///
    /// This error is intentionally vague to prevent oracle attacks.
    /// Do not add additional details about why verification failed.
    #[error("Signature verification failed")]
    SignatureVerificationFailed,

    /// The blinding factor is invalid (typically zero or not coprime to modulus).
    #[error("Invalid blinding factor")]
    InvalidBlindingFactor,

    /// The blinding factor's modular inverse could not be computed.
    ///
    /// This typically means the blinding factor is not coprime to the modulus.
    #[error("Blinding factor inverse does not exist")]
    BlindingFactorInverseNotFound,

    /// Range proof verification failed.
    ///
    /// The value does not fall within the claimed range.
    #[error("Range proof verification failed")]
    RangeProofFailed,

    /// Value is outside the allowed range for proof generation.
    #[error("Value {value} is outside allowed range [{min}, {max}]")]
    ValueOutOfRange {
        /// The value that was out of range
        value: u64,
        /// Minimum allowed value
        min: u64,
        /// Maximum allowed value
        max: u64,
    },

    /// Ring signature is invalid.
    ///
    /// This could mean the signature structure is malformed or verification failed.
    #[error("Ring signature invalid")]
    InvalidRingSignature,

    /// Ring is empty or has invalid structure.
    #[error("Invalid ring: {0}")]
    InvalidRing(String),

    /// Signer index is out of bounds for the provided ring.
    #[error("Signer index {index} out of bounds for ring of size {ring_size}")]
    SignerIndexOutOfBounds {
        /// The provided signer index
        index: usize,
        /// The size of the ring
        ring_size: usize,
    },

    /// The key image has already been used (double-spend attempt).
    ///
    /// Key images are unique per private key and prevent the same key
    /// from signing multiple times in the same context.
    #[error("Key image already spent")]
    KeyImageSpent,

    /// A general cryptographic operation failed.
    ///
    /// This is used for unexpected failures that don't fit other categories.
    #[error("Cryptographic operation failed: {0}")]
    OperationFailed(String),

    /// Random number generation failed.
    ///
    /// This is a critical error that typically indicates a system problem.
    #[error("Random number generation failed")]
    RngFailure,

    /// Hash function operation failed.
    #[error("Hash operation failed: {0}")]
    HashFailed(String),

    /// Serialization or deserialization failed.
    #[error("Serialization failed: {0}")]
    SerializationFailed(String),

    /// Prime generation failed after maximum attempts.
    #[error("Failed to generate prime after {attempts} attempts")]
    PrimeGenerationFailed {
        /// Number of attempts made
        attempts: usize,
    },

    /// Invalid curve point (not on curve or at infinity).
    #[error("Invalid elliptic curve point")]
    InvalidCurvePoint,

    /// Elliptic curve operation failed.
    #[error("Curve operation failed: {0}")]
    CurveOperationFailed(String),
}

/// Errors specific to token operations.
#[derive(Debug, Error)]
pub enum TokenError {
    /// Token signature is invalid.
    #[error("Invalid token signature")]
    InvalidSignature,

    /// Token has expired.
    #[error("Token has expired")]
    Expired,

    /// Token has already been redeemed.
    #[error("Token already redeemed")]
    AlreadyRedeemed,

    /// Token wallet is corrupted or invalid.
    #[error("Wallet corruption detected: {0}")]
    WalletCorrupted(String),

    /// Encryption/decryption operation failed.
    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),

    /// Password is incorrect for decryption.
    #[error("Invalid password")]
    InvalidPassword,

    /// Insufficient tokens for requested operation.
    #[error("Insufficient tokens: have {have}, need {need}")]
    InsufficientTokens {
        /// Number of tokens available
        have: u64,
        /// Number of tokens needed
        need: u64,
    },

    /// Token format is invalid or corrupted.
    #[error("Invalid token format: {0}")]
    InvalidFormat(String),

    /// Underlying cryptographic error.
    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),

    /// I/O error during token operations.
    #[error("I/O error: {0}")]
    IoError(String),
}

impl From<std::io::Error> for TokenError {
    fn from(err: std::io::Error) -> Self {
        TokenError::IoError(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crypto_error_display() {
        let err = CryptoError::InvalidKeySize {
            expected: 32,
            actual: 16,
        };
        assert_eq!(err.to_string(), "Invalid key size: expected 32, got 16");

        let err = CryptoError::SignatureVerificationFailed;
        assert_eq!(err.to_string(), "Signature verification failed");

        let err = CryptoError::KeyImageSpent;
        assert_eq!(err.to_string(), "Key image already spent");
    }

    #[test]
    fn test_token_error_display() {
        let err = TokenError::InvalidSignature;
        assert_eq!(err.to_string(), "Invalid token signature");

        let err = TokenError::InsufficientTokens { have: 5, need: 10 };
        assert_eq!(err.to_string(), "Insufficient tokens: have 5, need 10");
    }

    #[test]
    fn test_crypto_error_to_token_error() {
        let crypto_err = CryptoError::SignatureVerificationFailed;
        let token_err: TokenError = crypto_err.into();
        assert!(matches!(token_err, TokenError::CryptoError(_)));
    }
}
