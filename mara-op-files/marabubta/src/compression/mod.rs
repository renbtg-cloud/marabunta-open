// Marabunta - Licensed under the MIT License.
//! Compression Module for Marabunta Compute
//!
//! This module provides comprehensive compression support for data transfer and storage,
//! including multiple compression algorithms, automatic level selection, streaming support,
//! and HTTP Content-Encoding integration.
//!
//! # Supported Algorithms
//!
//! - **Zstd**: High compression ratio with excellent speed, best for general use
//! - **Lz4**: Ultra-fast compression, best for real-time streaming
//! - **Gzip**: Wide compatibility, best for HTTP and legacy systems
//!
//! # Features
//!
//! - Automatic compression level selection based on data size
//! - Streaming compression for large payloads (memory efficient)
//! - Content-Encoding support for HTTP responses
//! - Integration with backup and checkpoint storage
//!
//! # Example Usage
//!
//! ## Basic Compression
//!
//! ```rust,no_run
//! use marabunta_compute::compression::{Compressor, Algorithm, CompressionLevel};
//!
//! // Create a compressor with automatic level selection
//! let compressor = Compressor::new(Algorithm::Zstd, CompressionLevel::Auto);
//!
//! let data = b"Hello, world! This is some data to compress.";
//! let compressed = compressor.compress(data).unwrap();
//! let decompressed = compressor.decompress(&compressed).unwrap();
//!
//! assert_eq!(data.as_slice(), decompressed.as_slice());
//! ```
//!
//! ## Streaming Compression
//!
//! ```rust,no_run
//! use marabunta_compute::compression::{StreamingCompressor, Algorithm};
//! use std::io::Write;
//!
//! let mut output = Vec::new();
//! let mut stream = StreamingCompressor::new(Algorithm::Lz4, &mut output).unwrap();
//!
//! stream.write_all(b"First chunk of data").unwrap();
//! stream.write_all(b"Second chunk of data").unwrap();
//! stream.finish().unwrap();
//! ```
//!
//! ## HTTP Content-Encoding
//!
//! ```rust,no_run
//! use marabunta_compute::compression::{ContentEncoder, AcceptEncoding};
//!
//! // Parse Accept-Encoding header
//! let accept = AcceptEncoding::parse("gzip, deflate, br, zstd");
//!
//! // Select best encoding and compress
//! let encoder = ContentEncoder::from_accept(&accept);
//! let (compressed, encoding_header) = encoder.encode(b"Response body").unwrap();
//! ```

pub mod algorithms;
pub mod errors;
pub mod http;
pub mod streaming;

// Re-exports for convenient access
pub use algorithms::{Algorithm, CompressionLevel, Compressor};
pub use errors::CompressionError;
pub use http::{AcceptEncoding, ContentEncoder, ContentEncoding};
pub use streaming::{StreamingCompressor, StreamingDecompressor};

/// Default chunk size for streaming compression (64KB)
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Maximum size for in-memory compression before switching to streaming (16MB)
pub const STREAMING_THRESHOLD: usize = 16 * 1024 * 1024;

/// Size thresholds for automatic compression level selection
pub mod size_thresholds {
    /// Data smaller than this uses fast compression (1KB)
    pub const FAST_THRESHOLD: usize = 1024;
    /// Data smaller than this uses default compression (1MB)
    pub const DEFAULT_THRESHOLD: usize = 1024 * 1024;
    /// Data smaller than this uses balanced compression (16MB)
    pub const BALANCED_THRESHOLD: usize = 16 * 1024 * 1024;
    /// Larger data uses maximum compression
    pub const MAX_THRESHOLD: usize = 16 * 1024 * 1024;
}

/// Statistics about a compression operation
#[derive(Debug, Clone)]
pub struct CompressionStats {
    /// Original (uncompressed) size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Compression ratio (compressed / original)
    pub ratio: f64,
    /// Time taken to compress in microseconds
    pub duration_us: u64,
    /// Throughput in MB/s
    pub throughput_mbps: f64,
    /// Algorithm used
    pub algorithm: Algorithm,
    /// Compression level used
    pub level: i32,
}

impl CompressionStats {
    /// Create new compression stats
    pub fn new(
        original_size: usize,
        compressed_size: usize,
        duration_us: u64,
        algorithm: Algorithm,
        level: i32,
    ) -> Self {
        let ratio = if original_size > 0 {
            compressed_size as f64 / original_size as f64
        } else {
            1.0
        };

        let throughput_mbps = if duration_us > 0 {
            (original_size as f64 / 1_000_000.0) / (duration_us as f64 / 1_000_000.0)
        } else {
            0.0
        };

        Self {
            original_size,
            compressed_size,
            ratio,
            duration_us,
            throughput_mbps,
            algorithm,
            level,
        }
    }

    /// Get space savings as a percentage
    pub fn space_savings_percent(&self) -> f64 {
        (1.0 - self.ratio) * 100.0
    }

    /// Check if compression was effective (data got smaller)
    pub fn is_effective(&self) -> bool {
        self.ratio < 1.0
    }
}

impl std::fmt::Display for CompressionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} -> {} ({:.1}% savings, {:.1} MB/s)",
            self.algorithm,
            format_size(self.original_size),
            format_size(self.compressed_size),
            self.space_savings_percent(),
            self.throughput_mbps
        )
    }
}

/// Format a size in bytes to a human-readable string
fn format_size(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Select the best compression algorithm for given data characteristics
pub fn select_algorithm(data_size: usize, prefer_speed: bool, http_compatible: bool) -> Algorithm {
    if http_compatible {
        // For HTTP, gzip has the best compatibility
        // zstd is increasingly supported but gzip is safer
        if prefer_speed {
            Algorithm::Gzip
        } else {
            Algorithm::Zstd
        }
    } else if prefer_speed {
        // LZ4 is the fastest
        Algorithm::Lz4
    } else if data_size > STREAMING_THRESHOLD {
        // For large data, zstd offers the best ratio
        Algorithm::Zstd
    } else {
        // Default to zstd for balanced performance
        Algorithm::Zstd
    }
}

/// Compress data with automatic algorithm and level selection
pub fn compress_auto(data: &[u8]) -> Result<(Vec<u8>, CompressionStats), CompressionError> {
    let algorithm = select_algorithm(data.len(), false, false);
    let compressor = Compressor::new(algorithm, CompressionLevel::Auto);
    compressor.compress_with_stats(data)
}

/// Decompress data, auto-detecting the algorithm from the header
pub fn decompress_auto(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    let algorithm = Algorithm::detect(data)?;
    let compressor = Compressor::new(algorithm, CompressionLevel::Auto);
    compressor.decompress(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats::new(1000, 500, 1000, Algorithm::Zstd, 3);

        assert_eq!(stats.original_size, 1000);
        assert_eq!(stats.compressed_size, 500);
        assert!((stats.ratio - 0.5).abs() < 0.001);
        assert!((stats.space_savings_percent() - 50.0).abs() < 0.1);
        assert!(stats.is_effective());
    }

    #[test]
    fn test_compression_stats_display() {
        let stats = CompressionStats::new(1024 * 1024, 512 * 1024, 10000, Algorithm::Zstd, 3);
        let display = format!("{}", stats);
        assert!(display.contains("zstd"));
        assert!(display.contains("MB"));
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(1500), "1.46 KB");
        assert_eq!(format_size(1_500_000), "1.43 MB");
        assert_eq!(format_size(1_500_000_000), "1.40 GB");
    }

    #[test]
    fn test_select_algorithm() {
        // Speed preference should select LZ4
        assert_eq!(select_algorithm(1000, true, false), Algorithm::Lz4);

        // HTTP compatible with speed should be gzip
        assert_eq!(select_algorithm(1000, true, true), Algorithm::Gzip);

        // HTTP compatible without speed preference should be zstd
        assert_eq!(select_algorithm(1000, false, true), Algorithm::Zstd);

        // Large data should use zstd
        assert_eq!(
            select_algorithm(STREAMING_THRESHOLD + 1, false, false),
            Algorithm::Zstd
        );
    }

    #[test]
    fn test_compress_decompress_auto() {
        let original = b"Hello, this is test data that should compress well! ".repeat(100);

        let (compressed, stats) = compress_auto(&original).unwrap();
        assert!(stats.is_effective());

        let decompressed = decompress_auto(&compressed).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }
}
