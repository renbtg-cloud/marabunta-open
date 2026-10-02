// Marabunta - Licensed under the MIT License.
//! Compression error types
//!
//! This module defines error types for compression and decompression operations.

use thiserror::Error;

/// Errors that can occur during compression operations
#[derive(Error, Debug)]
pub enum CompressionError {
    /// Unknown or unsupported compression algorithm
    #[error("Unknown compression algorithm: {0}")]
    UnknownAlgorithm(String),

    /// Failed to detect compression format from data
    #[error("Unable to detect compression format from data header")]
    UnableToDetectFormat,

    /// Compression operation failed
    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    /// Decompression operation failed
    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    /// Invalid compression level
    #[error(
        "Invalid compression level {level} for algorithm {algorithm}: valid range is {min}-{max}"
    )]
    InvalidLevel {
        algorithm: String,
        level: i32,
        min: i32,
        max: i32,
    },

    /// Data too large for in-memory operation
    #[error("Data too large ({size} bytes) for in-memory compression, use streaming API")]
    DataTooLarge { size: usize },

    /// IO error during streaming operation
    #[error("IO error during compression: {0}")]
    Io(String),

    /// Invalid compressed data format
    #[error("Invalid compressed data format: {0}")]
    InvalidFormat(String),

    /// Checksum mismatch after decompression
    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    /// Dictionary not found for dictionary-based compression
    #[error("Compression dictionary not found: {0}")]
    DictionaryNotFound(String),

    /// Insufficient buffer for decompression output
    #[error("Buffer too small: need at least {required} bytes, got {available}")]
    BufferTooSmall { required: usize, available: usize },

    /// Operation timed out
    #[error("Compression operation timed out after {0}ms")]
    Timeout(u64),

    /// Internal error
    #[error("Internal compression error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for CompressionError {
    fn from(e: std::io::Error) -> Self {
        CompressionError::Io(e.to_string())
    }
}

/// Result type alias for compression operations
pub type CompressionResult<T> = Result<T, CompressionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = CompressionError::UnknownAlgorithm("brotli".to_string());
        assert_eq!(err.to_string(), "Unknown compression algorithm: brotli");

        let err = CompressionError::InvalidLevel {
            algorithm: "zstd".to_string(),
            level: 25,
            min: 1,
            max: 22,
        };
        assert!(err.to_string().contains("25"));
        assert!(err.to_string().contains("zstd"));

        let err = CompressionError::BufferTooSmall {
            required: 1000,
            available: 500,
        };
        assert!(err.to_string().contains("1000"));
        assert!(err.to_string().contains("500"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let comp_err: CompressionError = io_err.into();
        assert!(matches!(comp_err, CompressionError::Io(_)));
    }
}
