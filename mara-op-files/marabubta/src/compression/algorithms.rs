// Marabunta - Licensed under the MIT License.
//! Compression algorithms implementation
//!
//! This module provides implementations for multiple compression algorithms:
//! - Zstd: High compression ratio with excellent speed
//! - Lz4: Ultra-fast compression
//! - Gzip: Wide compatibility

use std::io::{Read, Write};
use std::time::Instant;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression as GzipCompression;
use serde::{Deserialize, Serialize};

use super::errors::{CompressionError, CompressionResult};
use super::size_thresholds;
use super::CompressionStats;

/// Supported compression algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Algorithm {
    /// Zstandard - high compression ratio with excellent speed
    Zstd,
    /// LZ4 - ultra-fast compression
    Lz4,
    /// Gzip - wide compatibility
    Gzip,
    /// No compression (passthrough)
    None,
}

impl Algorithm {
    /// Magic bytes for each compression format
    pub fn magic_bytes(&self) -> &'static [u8] {
        match self {
            Algorithm::Zstd => &[0x28, 0xB5, 0x2F, 0xFD],
            Algorithm::Lz4 => &[0x04, 0x22, 0x4D, 0x18],
            Algorithm::Gzip => &[0x1F, 0x8B],
            Algorithm::None => &[],
        }
    }

    /// Detect algorithm from compressed data header
    pub fn detect(data: &[u8]) -> CompressionResult<Self> {
        if data.len() >= 4 && &data[..4] == Algorithm::Zstd.magic_bytes() {
            Ok(Algorithm::Zstd)
        } else if data.len() >= 4 && &data[..4] == Algorithm::Lz4.magic_bytes() {
            Ok(Algorithm::Lz4)
        } else if data.len() >= 2 && &data[..2] == Algorithm::Gzip.magic_bytes() {
            Ok(Algorithm::Gzip)
        } else {
            Err(CompressionError::UnableToDetectFormat)
        }
    }

    /// Get the file extension for this algorithm
    pub fn extension(&self) -> &'static str {
        match self {
            Algorithm::Zstd => "zst",
            Algorithm::Lz4 => "lz4",
            Algorithm::Gzip => "gz",
            Algorithm::None => "",
        }
    }

    /// Get the MIME type for this algorithm
    pub fn mime_type(&self) -> &'static str {
        match self {
            Algorithm::Zstd => "application/zstd",
            Algorithm::Lz4 => "application/x-lz4",
            Algorithm::Gzip => "application/gzip",
            Algorithm::None => "application/octet-stream",
        }
    }

    /// Get the Content-Encoding header value
    pub fn content_encoding(&self) -> &'static str {
        match self {
            Algorithm::Zstd => "zstd",
            Algorithm::Lz4 => "lz4",
            Algorithm::Gzip => "gzip",
            Algorithm::None => "identity",
        }
    }

    /// Get valid compression level range for this algorithm
    pub fn level_range(&self) -> (i32, i32) {
        match self {
            Algorithm::Zstd => (1, 22),
            Algorithm::Lz4 => (1, 12), // LZ4 acceleration levels (1 = fastest)
            Algorithm::Gzip => (1, 9),
            Algorithm::None => (0, 0),
        }
    }

    /// Get the default compression level
    pub fn default_level(&self) -> i32 {
        match self {
            Algorithm::Zstd => 3,
            Algorithm::Lz4 => 1,
            Algorithm::Gzip => 6,
            Algorithm::None => 0,
        }
    }

    /// Get the fast compression level (prioritize speed)
    pub fn fast_level(&self) -> i32 {
        match self {
            Algorithm::Zstd => 1,
            Algorithm::Lz4 => 1,
            Algorithm::Gzip => 1,
            Algorithm::None => 0,
        }
    }

    /// Get the balanced compression level
    pub fn balanced_level(&self) -> i32 {
        match self {
            Algorithm::Zstd => 5,
            Algorithm::Lz4 => 4,
            Algorithm::Gzip => 6,
            Algorithm::None => 0,
        }
    }

    /// Get the maximum compression level (prioritize ratio)
    pub fn max_level(&self) -> i32 {
        match self {
            Algorithm::Zstd => 19,
            Algorithm::Lz4 => 12,
            Algorithm::Gzip => 9,
            Algorithm::None => 0,
        }
    }
}

impl std::fmt::Display for Algorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Algorithm::Zstd => write!(f, "zstd"),
            Algorithm::Lz4 => write!(f, "lz4"),
            Algorithm::Gzip => write!(f, "gzip"),
            Algorithm::None => write!(f, "none"),
        }
    }
}

impl std::str::FromStr for Algorithm {
    type Err = CompressionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "zstd" | "zstandard" => Ok(Algorithm::Zstd),
            "lz4" => Ok(Algorithm::Lz4),
            "gzip" | "gz" => Ok(Algorithm::Gzip),
            "none" | "identity" => Ok(Algorithm::None),
            _ => Err(CompressionError::UnknownAlgorithm(s.to_string())),
        }
    }
}

/// Compression level selection strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionLevel {
    /// Fastest compression (lowest ratio)
    Fast,
    /// Default/balanced compression
    Default,
    /// Higher compression ratio
    Balanced,
    /// Maximum compression (slowest)
    Max,
    /// Auto-select based on data size
    Auto,
    /// Custom level (algorithm-specific)
    Custom(i32),
}

impl CompressionLevel {
    /// Convert to a concrete level for the given algorithm
    pub fn to_level(&self, algorithm: Algorithm) -> i32 {
        match self {
            CompressionLevel::Fast => algorithm.fast_level(),
            CompressionLevel::Default => algorithm.default_level(),
            CompressionLevel::Balanced => algorithm.balanced_level(),
            CompressionLevel::Max => algorithm.max_level(),
            CompressionLevel::Auto => algorithm.default_level(), // Will be adjusted by Compressor
            CompressionLevel::Custom(level) => *level,
        }
    }

    /// Select level based on data size (for Auto mode)
    pub fn for_data_size(data_size: usize, algorithm: Algorithm) -> i32 {
        if data_size < size_thresholds::FAST_THRESHOLD {
            algorithm.max_level() // Small data: maximize ratio
        } else if data_size < size_thresholds::DEFAULT_THRESHOLD {
            algorithm.balanced_level()
        } else if data_size < size_thresholds::BALANCED_THRESHOLD {
            algorithm.default_level()
        } else {
            algorithm.fast_level() // Large data: prioritize speed
        }
    }
}

/// Compressor for synchronous compression/decompression
#[derive(Debug, Clone)]
pub struct Compressor {
    algorithm: Algorithm,
    level: CompressionLevel,
}

impl Compressor {
    /// Create a new compressor
    pub fn new(algorithm: Algorithm, level: CompressionLevel) -> Self {
        Self { algorithm, level }
    }

    /// Create a compressor with automatic settings
    pub fn auto() -> Self {
        Self::new(Algorithm::Zstd, CompressionLevel::Auto)
    }

    /// Get the algorithm
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Get the compression level
    pub fn level(&self) -> CompressionLevel {
        self.level
    }

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        if matches!(self.algorithm, Algorithm::None) {
            return Ok(data.to_vec());
        }

        let level = match self.level {
            CompressionLevel::Auto => CompressionLevel::for_data_size(data.len(), self.algorithm),
            _ => self.level.to_level(self.algorithm),
        };

        self.validate_level(level)?;

        match self.algorithm {
            Algorithm::Zstd => self.compress_zstd(data, level),
            Algorithm::Lz4 => self.compress_lz4(data),
            Algorithm::Gzip => self.compress_gzip(data, level),
            Algorithm::None => Ok(data.to_vec()),
        }
    }

    /// Compress data and return statistics
    pub fn compress_with_stats(
        &self,
        data: &[u8],
    ) -> CompressionResult<(Vec<u8>, CompressionStats)> {
        let start = Instant::now();
        let compressed = self.compress(data)?;
        let duration = start.elapsed();

        let level = match self.level {
            CompressionLevel::Auto => CompressionLevel::for_data_size(data.len(), self.algorithm),
            _ => self.level.to_level(self.algorithm),
        };

        let stats = CompressionStats::new(
            data.len(),
            compressed.len(),
            duration.as_micros() as u64,
            self.algorithm,
            level,
        );

        Ok((compressed, stats))
    }

    /// Decompress data
    pub fn decompress(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        if matches!(self.algorithm, Algorithm::None) {
            return Ok(data.to_vec());
        }

        match self.algorithm {
            Algorithm::Zstd => self.decompress_zstd(data),
            Algorithm::Lz4 => self.decompress_lz4(data),
            Algorithm::Gzip => self.decompress_gzip(data),
            Algorithm::None => Ok(data.to_vec()),
        }
    }

    /// Decompress data into an existing buffer
    /// Returns the number of bytes written
    pub fn decompress_into(
        &self,
        compressed: &[u8],
        output: &mut [u8],
    ) -> CompressionResult<usize> {
        let decompressed = self.decompress(compressed)?;

        if output.len() < decompressed.len() {
            return Err(CompressionError::BufferTooSmall {
                required: decompressed.len(),
                available: output.len(),
            });
        }

        output[..decompressed.len()].copy_from_slice(&decompressed);
        Ok(decompressed.len())
    }

    /// Validate that the compression level is valid for the algorithm
    fn validate_level(&self, level: i32) -> CompressionResult<()> {
        let (min, max) = self.algorithm.level_range();
        if level < min || level > max {
            return Err(CompressionError::InvalidLevel {
                algorithm: self.algorithm.to_string(),
                level,
                min,
                max,
            });
        }
        Ok(())
    }

    // Zstd implementation using the zstd crate
    fn compress_zstd(&self, data: &[u8], level: i32) -> CompressionResult<Vec<u8>> {
        zstd::encode_all(std::io::Cursor::new(data), level)
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))
    }

    fn decompress_zstd(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        zstd::decode_all(std::io::Cursor::new(data))
            .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
    }

    // LZ4 implementation using lz4_flex crate
    fn compress_lz4(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        // lz4_flex provides frame format compression which includes magic bytes
        let compressed = lz4_flex::frame::FrameEncoder::new(Vec::new());
        let mut encoder = compressed;
        encoder
            .write_all(data)
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;
        encoder
            .finish()
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))
    }

    fn decompress_lz4(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        let mut decoder = lz4_flex::frame::FrameDecoder::new(std::io::Cursor::new(data));
        let mut output = Vec::new();
        decoder
            .read_to_end(&mut output)
            .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;
        Ok(output)
    }

    // Gzip implementation (using flate2)
    fn compress_gzip(&self, data: &[u8], level: i32) -> CompressionResult<Vec<u8>> {
        let mut encoder = GzEncoder::new(Vec::new(), GzipCompression::new(level as u32));
        encoder
            .write_all(data)
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;
        encoder
            .finish()
            .map_err(|e| CompressionError::CompressionFailed(e.to_string()))
    }

    fn decompress_gzip(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        let mut decoder = GzDecoder::new(data);
        let mut output = Vec::new();
        decoder
            .read_to_end(&mut output)
            .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;
        Ok(output)
    }
}

impl Default for Compressor {
    fn default() -> Self {
        Self::auto()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_magic_bytes() {
        assert_eq!(Algorithm::Zstd.magic_bytes(), &[0x28, 0xB5, 0x2F, 0xFD]);
        assert_eq!(Algorithm::Lz4.magic_bytes(), &[0x04, 0x22, 0x4D, 0x18]);
        assert_eq!(Algorithm::Gzip.magic_bytes(), &[0x1F, 0x8B]);
    }

    #[test]
    fn test_algorithm_from_str() {
        assert_eq!("zstd".parse::<Algorithm>().unwrap(), Algorithm::Zstd);
        assert_eq!("lz4".parse::<Algorithm>().unwrap(), Algorithm::Lz4);
        assert_eq!("gzip".parse::<Algorithm>().unwrap(), Algorithm::Gzip);
        assert_eq!("gz".parse::<Algorithm>().unwrap(), Algorithm::Gzip);
        assert!("invalid".parse::<Algorithm>().is_err());
    }

    #[test]
    fn test_algorithm_display() {
        assert_eq!(Algorithm::Zstd.to_string(), "zstd");
        assert_eq!(Algorithm::Lz4.to_string(), "lz4");
        assert_eq!(Algorithm::Gzip.to_string(), "gzip");
    }

    #[test]
    fn test_algorithm_detect_zstd() {
        let compressor = Compressor::new(Algorithm::Zstd, CompressionLevel::Default);
        let original = b"Test data for detection";
        let compressed = compressor.compress(original).unwrap();

        assert_eq!(Algorithm::detect(&compressed).unwrap(), Algorithm::Zstd);
    }

    #[test]
    fn test_algorithm_detect_lz4() {
        let compressor = Compressor::new(Algorithm::Lz4, CompressionLevel::Default);
        let original = b"Test data for detection";
        let compressed = compressor.compress(original).unwrap();

        assert_eq!(Algorithm::detect(&compressed).unwrap(), Algorithm::Lz4);
    }

    #[test]
    fn test_algorithm_detect_gzip() {
        let compressor = Compressor::new(Algorithm::Gzip, CompressionLevel::Default);
        let original = b"Test data for detection";
        let compressed = compressor.compress(original).unwrap();

        assert_eq!(Algorithm::detect(&compressed).unwrap(), Algorithm::Gzip);
    }

    #[test]
    fn test_algorithm_detect_unknown() {
        let unknown_data = [0x00, 0x00, 0x00, 0x00];
        assert!(Algorithm::detect(&unknown_data).is_err());
    }

    #[test]
    fn test_compression_level_to_level() {
        assert_eq!(CompressionLevel::Fast.to_level(Algorithm::Zstd), 1);
        assert_eq!(CompressionLevel::Default.to_level(Algorithm::Zstd), 3);
        assert_eq!(CompressionLevel::Max.to_level(Algorithm::Zstd), 19);
        assert_eq!(CompressionLevel::Custom(10).to_level(Algorithm::Zstd), 10);
    }

    #[test]
    fn test_compress_decompress_gzip() {
        let compressor = Compressor::new(Algorithm::Gzip, CompressionLevel::Default);
        let original = b"Hello, this is test data that should compress well! ".repeat(100);

        let compressed = compressor.compress(&original).unwrap();
        assert!(compressed.len() < original.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let compressor = Compressor::new(Algorithm::Zstd, CompressionLevel::Default);
        let original = b"Hello, this is test data that should compress well! ".repeat(100);

        let compressed = compressor.compress(&original).unwrap();
        assert!(compressed.len() < original.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let compressor = Compressor::new(Algorithm::Lz4, CompressionLevel::Default);
        let original = b"Hello, this is test data that should compress well! ".repeat(100);

        let compressed = compressor.compress(&original).unwrap();
        assert!(compressed.len() < original.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_none() {
        let compressor = Compressor::new(Algorithm::None, CompressionLevel::Default);
        let original = b"Test data";

        let compressed = compressor.compress(original).unwrap();
        assert_eq!(original.as_slice(), compressed.as_slice());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_compress_with_stats() {
        let compressor = Compressor::new(Algorithm::Gzip, CompressionLevel::Default);
        let original = b"Hello, world! ".repeat(1000);

        let (compressed, stats) = compressor.compress_with_stats(&original).unwrap();

        assert_eq!(stats.original_size, original.len());
        assert_eq!(stats.compressed_size, compressed.len());
        assert!(stats.ratio < 1.0);
        assert!(stats.is_effective());
        assert_eq!(stats.algorithm, Algorithm::Gzip);
    }

    #[test]
    fn test_decompress_into() {
        let compressor = Compressor::new(Algorithm::Gzip, CompressionLevel::Default);
        let original = b"Hello, world!";

        let compressed = compressor.compress(original).unwrap();

        let mut buffer = vec![0u8; 100];
        let size = compressor
            .decompress_into(&compressed, &mut buffer)
            .unwrap();

        assert_eq!(size, original.len());
        assert_eq!(&buffer[..size], original);
    }

    #[test]
    fn test_decompress_into_buffer_too_small() {
        let compressor = Compressor::new(Algorithm::Gzip, CompressionLevel::Default);
        let original = b"Hello, world! This is a longer message.";

        let compressed = compressor.compress(original).unwrap();

        let mut buffer = vec![0u8; 5];
        let result = compressor.decompress_into(&compressed, &mut buffer);

        assert!(matches!(
            result,
            Err(CompressionError::BufferTooSmall { .. })
        ));
    }

    #[test]
    fn test_auto_level_selection() {
        // Small data should use high compression
        let level_small = CompressionLevel::for_data_size(100, Algorithm::Zstd);
        assert_eq!(level_small, Algorithm::Zstd.max_level());

        // Large data should use fast compression
        let level_large = CompressionLevel::for_data_size(100_000_000, Algorithm::Zstd);
        assert_eq!(level_large, Algorithm::Zstd.fast_level());
    }

    #[test]
    fn test_invalid_level() {
        let compressor = Compressor::new(Algorithm::Zstd, CompressionLevel::Custom(100));
        let result = compressor.compress(b"test");

        assert!(matches!(result, Err(CompressionError::InvalidLevel { .. })));
    }

    #[test]
    fn test_compress_empty_data() {
        for algo in [Algorithm::Zstd, Algorithm::Lz4, Algorithm::Gzip] {
            let compressor = Compressor::new(algo, CompressionLevel::Default);
            let original: &[u8] = b"";

            let compressed = compressor.compress(original).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();

            assert_eq!(original, decompressed.as_slice());
        }
    }

    #[test]
    fn test_compress_binary_data() {
        for algo in [Algorithm::Zstd, Algorithm::Lz4, Algorithm::Gzip] {
            let compressor = Compressor::new(algo, CompressionLevel::Default);
            let original: Vec<u8> = (0..=255).cycle().take(10000).collect();

            let compressed = compressor.compress(&original).unwrap();
            let decompressed = compressor.decompress(&compressed).unwrap();

            assert_eq!(original, decompressed);
        }
    }

    #[test]
    fn test_compression_levels() {
        let original = b"Test data for compression level comparison ".repeat(1000);

        for algo in [Algorithm::Zstd, Algorithm::Gzip] {
            let fast = Compressor::new(algo, CompressionLevel::Fast);
            let max = Compressor::new(algo, CompressionLevel::Max);

            let fast_compressed = fast.compress(&original).unwrap();
            let max_compressed = max.compress(&original).unwrap();

            // Max compression should generally produce smaller output
            // (or at least not significantly larger)
            assert!(
                max_compressed.len() <= fast_compressed.len() + 100,
                "Max compression produced unexpectedly large output for {:?}",
                algo
            );

            // Both should decompress correctly
            let fast_decompressed = fast.decompress(&fast_compressed).unwrap();
            let max_decompressed = max.decompress(&max_compressed).unwrap();

            assert_eq!(original.as_slice(), fast_decompressed.as_slice());
            assert_eq!(original.as_slice(), max_decompressed.as_slice());
        }
    }
}
