// Marabunta - Licensed under the MIT License.
//! Backup error types
//!
//! This module defines error types for backup and restore operations.

use thiserror::Error;

/// Errors that can occur during backup operations
#[derive(Error, Debug)]
pub enum BackupError {
    /// IO error (file operations)
    #[error("IO error: {0}")]
    Io(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Storage backend error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Backup not found
    #[error("Backup not found: {0}")]
    NotFound(String),

    /// Invalid checksum
    #[error("Invalid checksum: {0}")]
    InvalidChecksum(String),

    /// Incompatible backup format version
    #[error("Incompatible backup version: expected {expected}, found {found}")]
    IncompatibleVersion { expected: u32, found: u32 },

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Feature not implemented
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// Backup in progress
    #[error("Backup already in progress")]
    BackupInProgress,

    /// Restore failed
    #[error("Restore failed: {0}")]
    RestoreFailed(String),

    /// Validation error
    #[error("Validation error: {0}")]
    Validation(String),

    /// Compression error
    #[error("Compression error: {0}")]
    Compression(String),

    /// Timeout
    #[error("Operation timed out: {0}")]
    Timeout(String),
}

impl From<std::io::Error> for BackupError {
    fn from(e: std::io::Error) -> Self {
        BackupError::Io(e.to_string())
    }
}

impl From<serde_json::Error> for BackupError {
    fn from(e: serde_json::Error) -> Self {
        BackupError::Serialization(e.to_string())
    }
}

impl From<base64::DecodeError> for BackupError {
    fn from(e: base64::DecodeError) -> Self {
        BackupError::Serialization(format!("Base64 decode error: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = BackupError::Io("test error".to_string());
        assert_eq!(err.to_string(), "IO error: test error");

        let err = BackupError::InvalidChecksum("mismatch".to_string());
        assert_eq!(err.to_string(), "Invalid checksum: mismatch");

        let err = BackupError::IncompatibleVersion {
            expected: 1,
            found: 2,
        };
        assert_eq!(
            err.to_string(),
            "Incompatible backup version: expected 1, found 2"
        );
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let backup_err: BackupError = io_err.into();
        assert!(matches!(backup_err, BackupError::Io(_)));
    }

    #[test]
    fn test_error_from_serde() {
        let json = "{invalid json}";
        let serde_err: Result<String, _> = serde_json::from_str(json);
        if let Err(e) = serde_err {
            let backup_err: BackupError = e.into();
            assert!(matches!(backup_err, BackupError::Serialization(_)));
        }
    }
}
