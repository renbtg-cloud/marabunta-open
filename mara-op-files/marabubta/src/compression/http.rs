// Marabunta - Licensed under the MIT License.
//! HTTP Content-Encoding support
//!
//! This module provides integration with HTTP responses through Content-Encoding
//! header support, allowing automatic compression negotiation based on
//! Accept-Encoding headers.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::algorithms::{Algorithm, CompressionLevel, Compressor};
use super::errors::{CompressionError, CompressionResult};

/// Content encoding types for HTTP
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentEncoding {
    /// Identity encoding (no compression)
    Identity,
    /// Gzip compression
    Gzip,
    /// Zstandard compression
    Zstd,
    /// LZ4 compression (non-standard but increasingly supported)
    Lz4,
    /// Brotli compression (not implemented, but recognized)
    #[serde(rename = "br")]
    Brotli,
    /// Deflate compression (not implemented, but recognized)
    Deflate,
}

impl ContentEncoding {
    /// Convert to Algorithm (if supported)
    pub fn to_algorithm(&self) -> Option<Algorithm> {
        match self {
            ContentEncoding::Identity => Some(Algorithm::None),
            ContentEncoding::Gzip => Some(Algorithm::Gzip),
            ContentEncoding::Zstd => Some(Algorithm::Zstd),
            ContentEncoding::Lz4 => Some(Algorithm::Lz4),
            ContentEncoding::Brotli => None,  // Not implemented
            ContentEncoding::Deflate => None, // Not implemented
        }
    }

    /// Get the header value for this encoding
    pub fn header_value(&self) -> &'static str {
        match self {
            ContentEncoding::Identity => "identity",
            ContentEncoding::Gzip => "gzip",
            ContentEncoding::Zstd => "zstd",
            ContentEncoding::Lz4 => "lz4",
            ContentEncoding::Brotli => "br",
            ContentEncoding::Deflate => "deflate",
        }
    }

    /// Check if this encoding is supported for compression
    pub fn is_supported(&self) -> bool {
        self.to_algorithm().is_some()
    }

    /// Default priority for encoding selection (higher = preferred)
    pub fn default_priority(&self) -> u8 {
        match self {
            ContentEncoding::Zstd => 100,   // Best ratio + speed
            ContentEncoding::Lz4 => 90,     // Fastest
            ContentEncoding::Gzip => 80,    // Most compatible
            ContentEncoding::Brotli => 70,  // Good but not implemented
            ContentEncoding::Deflate => 60, // Legacy
            ContentEncoding::Identity => 0, // No compression
        }
    }
}

impl std::fmt::Display for ContentEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.header_value())
    }
}

impl std::str::FromStr for ContentEncoding {
    type Err = CompressionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "identity" | "" => Ok(ContentEncoding::Identity),
            "gzip" | "x-gzip" => Ok(ContentEncoding::Gzip),
            "zstd" => Ok(ContentEncoding::Zstd),
            "lz4" | "x-lz4" => Ok(ContentEncoding::Lz4),
            "br" | "brotli" => Ok(ContentEncoding::Brotli),
            "deflate" => Ok(ContentEncoding::Deflate),
            _ => Err(CompressionError::UnknownAlgorithm(s.to_string())),
        }
    }
}

/// Parsed Accept-Encoding header
///
/// Represents the client's encoding preferences from the Accept-Encoding header.
#[derive(Debug, Clone, Default)]
pub struct AcceptEncoding {
    /// Encodings with their quality values (0.0 to 1.0)
    encodings: HashMap<ContentEncoding, f32>,
    /// Whether identity is explicitly rejected (q=0)
    identity_rejected: bool,
    /// Whether wildcard (*) is present
    wildcard: Option<f32>,
}

impl AcceptEncoding {
    /// Create empty Accept-Encoding (identity only)
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse an Accept-Encoding header value
    ///
    /// Example: "gzip, deflate, br;q=0.9, zstd;q=1.0, *;q=0.5"
    pub fn parse(header: &str) -> Self {
        let mut result = Self::new();

        for part in header.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let (encoding, quality) = Self::parse_encoding_part(part);

            if encoding == "*" {
                result.wildcard = Some(quality);
            } else if let Ok(enc) = encoding.parse::<ContentEncoding>() {
                if quality == 0.0 && matches!(enc, ContentEncoding::Identity) {
                    result.identity_rejected = true;
                }
                result.encodings.insert(enc, quality);
            }
        }

        result
    }

    /// Parse a single encoding part (e.g., "gzip;q=0.9")
    fn parse_encoding_part(part: &str) -> (&str, f32) {
        let mut parts = part.splitn(2, ';');
        let encoding = parts.next().unwrap_or("").trim();

        let quality = parts
            .next()
            .and_then(|q| {
                let q = q.trim();
                if q.starts_with("q=") {
                    q[2..].parse::<f32>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(1.0)
            .max(0.0)
            .min(1.0);

        (encoding, quality)
    }

    /// Check if an encoding is accepted
    pub fn accepts(&self, encoding: ContentEncoding) -> bool {
        if let Some(&q) = self.encodings.get(&encoding) {
            q > 0.0
        } else if let Some(wq) = self.wildcard {
            wq > 0.0
        } else {
            // Identity is always accepted unless explicitly rejected
            matches!(encoding, ContentEncoding::Identity) && !self.identity_rejected
        }
    }

    /// Get the quality value for an encoding
    pub fn quality(&self, encoding: ContentEncoding) -> f32 {
        self.encodings
            .get(&encoding)
            .copied()
            .or(self.wildcard)
            .unwrap_or({
                if matches!(encoding, ContentEncoding::Identity) && !self.identity_rejected {
                    1.0
                } else {
                    0.0
                }
            })
    }

    /// Get the best supported encoding based on quality and priority
    pub fn best_encoding(&self) -> ContentEncoding {
        let mut best = ContentEncoding::Identity;
        let mut best_score = 0.0f32;

        let candidates = [
            ContentEncoding::Zstd,
            ContentEncoding::Lz4,
            ContentEncoding::Gzip,
        ];

        for &enc in &candidates {
            if !enc.is_supported() {
                continue;
            }

            let quality = self.quality(enc);
            if quality <= 0.0 {
                continue;
            }

            // Score = quality * priority / 100
            let score = quality * (enc.default_priority() as f32 / 100.0);
            if score > best_score {
                best = enc;
                best_score = score;
            }
        }

        // Fall back to identity if nothing else is accepted
        if best_score == 0.0 && !self.identity_rejected {
            best = ContentEncoding::Identity;
        }

        best
    }

    /// Check if compression is worthwhile (any encoding with q > 0)
    pub fn wants_compression(&self) -> bool {
        self.encodings
            .iter()
            .any(|(enc, &q)| q > 0.0 && !matches!(enc, ContentEncoding::Identity))
    }
}

/// HTTP content encoder for response compression
#[derive(Debug, Clone)]
pub struct ContentEncoder {
    encoding: ContentEncoding,
    level: CompressionLevel,
    min_size: usize,
}

impl ContentEncoder {
    /// Create a new content encoder with the specified encoding
    pub fn new(encoding: ContentEncoding) -> Self {
        Self {
            encoding,
            level: CompressionLevel::Default,
            min_size: 1024, // Don't compress responses smaller than 1KB
        }
    }

    /// Create a content encoder based on Accept-Encoding header
    pub fn from_accept(accept: &AcceptEncoding) -> Self {
        Self::new(accept.best_encoding())
    }

    /// Set the compression level
    pub fn with_level(mut self, level: CompressionLevel) -> Self {
        self.level = level;
        self
    }

    /// Set the minimum size for compression
    pub fn with_min_size(mut self, min_size: usize) -> Self {
        self.min_size = min_size;
        self
    }

    /// Get the encoding type
    pub fn encoding(&self) -> ContentEncoding {
        self.encoding
    }

    /// Get the Content-Encoding header value
    pub fn content_encoding_header(&self) -> &'static str {
        self.encoding.header_value()
    }

    /// Encode (compress) data for HTTP response
    ///
    /// Returns the compressed data and the Content-Encoding header value.
    /// If the data is too small or compression is not beneficial, returns
    /// the original data with "identity" encoding.
    pub fn encode(&self, data: &[u8]) -> CompressionResult<(Vec<u8>, &'static str)> {
        // Skip compression for small data
        if data.len() < self.min_size {
            return Ok((data.to_vec(), "identity"));
        }

        // Skip if no compression requested
        if matches!(self.encoding, ContentEncoding::Identity) {
            return Ok((data.to_vec(), "identity"));
        }

        // Get the algorithm
        let algorithm = self
            .encoding
            .to_algorithm()
            .ok_or_else(|| CompressionError::UnknownAlgorithm(self.encoding.to_string()))?;

        // Compress
        let compressor = Compressor::new(algorithm, self.level);
        let compressed = compressor.compress(data)?;

        // Only use compressed data if it's actually smaller
        if compressed.len() < data.len() {
            Ok((compressed, self.content_encoding_header()))
        } else {
            Ok((data.to_vec(), "identity"))
        }
    }

    /// Encode data, returning compression statistics
    pub fn encode_with_stats(
        &self,
        data: &[u8],
    ) -> CompressionResult<(Vec<u8>, &'static str, Option<super::CompressionStats>)> {
        if data.len() < self.min_size || matches!(self.encoding, ContentEncoding::Identity) {
            return Ok((data.to_vec(), "identity", None));
        }

        let algorithm = self
            .encoding
            .to_algorithm()
            .ok_or_else(|| CompressionError::UnknownAlgorithm(self.encoding.to_string()))?;

        let compressor = Compressor::new(algorithm, self.level);
        let (compressed, stats) = compressor.compress_with_stats(data)?;

        if compressed.len() < data.len() {
            Ok((compressed, self.content_encoding_header(), Some(stats)))
        } else {
            Ok((data.to_vec(), "identity", None))
        }
    }

    /// Decode (decompress) data from HTTP request/response
    pub fn decode(encoding: ContentEncoding, data: &[u8]) -> CompressionResult<Vec<u8>> {
        if matches!(encoding, ContentEncoding::Identity) {
            return Ok(data.to_vec());
        }

        let algorithm = encoding
            .to_algorithm()
            .ok_or_else(|| CompressionError::UnknownAlgorithm(encoding.to_string()))?;

        let compressor = Compressor::new(algorithm, CompressionLevel::Default);
        compressor.decompress(data)
    }
}

impl Default for ContentEncoder {
    fn default() -> Self {
        Self::new(ContentEncoding::Gzip)
    }
}

/// Middleware-style response compressor
///
/// This struct provides a convenient way to compress HTTP responses
/// with automatic content-type detection and size thresholds.
#[derive(Debug, Clone)]
pub struct ResponseCompressor {
    /// Minimum response size to compress
    min_size: usize,
    /// Maximum response size to compress (avoid OOM for huge responses)
    max_size: usize,
    /// Content types to compress
    compressible_types: Vec<String>,
    /// Default compression level
    level: CompressionLevel,
}

impl ResponseCompressor {
    /// Create a new response compressor with default settings
    pub fn new() -> Self {
        Self {
            min_size: 1024,
            max_size: 50 * 1024 * 1024, // 50MB
            compressible_types: Self::default_compressible_types(),
            level: CompressionLevel::Default,
        }
    }

    /// Default list of compressible content types
    fn default_compressible_types() -> Vec<String> {
        vec![
            "text/".to_string(),
            "application/json".to_string(),
            "application/javascript".to_string(),
            "application/xml".to_string(),
            "application/xhtml+xml".to_string(),
            "application/rss+xml".to_string(),
            "application/atom+xml".to_string(),
            "image/svg+xml".to_string(),
            "application/wasm".to_string(),
        ]
    }

    /// Set minimum compression size
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_size = size;
        self
    }

    /// Set maximum compression size
    pub fn with_max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Set compression level
    pub fn with_level(mut self, level: CompressionLevel) -> Self {
        self.level = level;
        self
    }

    /// Add a compressible content type
    pub fn add_compressible_type(mut self, content_type: &str) -> Self {
        self.compressible_types.push(content_type.to_string());
        self
    }

    /// Check if a content type should be compressed
    pub fn should_compress(&self, content_type: &str, content_length: usize) -> bool {
        // Check size bounds
        if content_length < self.min_size || content_length > self.max_size {
            return false;
        }

        // Check content type
        let ct_lower = content_type.to_lowercase();
        self.compressible_types
            .iter()
            .any(|t| ct_lower.starts_with(t) || ct_lower.contains(t))
    }

    /// Compress a response if appropriate
    pub fn compress_response(
        &self,
        accept: &AcceptEncoding,
        content_type: &str,
        body: &[u8],
    ) -> CompressionResult<(Vec<u8>, Option<&'static str>)> {
        if !self.should_compress(content_type, body.len()) {
            return Ok((body.to_vec(), None));
        }

        if !accept.wants_compression() {
            return Ok((body.to_vec(), None));
        }

        let encoder = ContentEncoder::from_accept(accept).with_level(self.level);
        let (compressed, encoding) = encoder.encode(body)?;

        if encoding == "identity" {
            Ok((compressed, None))
        } else {
            Ok((compressed, Some(encoding)))
        }
    }
}

impl Default for ResponseCompressor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_encoding_parse() {
        assert_eq!(
            "gzip".parse::<ContentEncoding>().unwrap(),
            ContentEncoding::Gzip
        );
        assert_eq!(
            "zstd".parse::<ContentEncoding>().unwrap(),
            ContentEncoding::Zstd
        );
        assert_eq!(
            "lz4".parse::<ContentEncoding>().unwrap(),
            ContentEncoding::Lz4
        );
        assert_eq!(
            "br".parse::<ContentEncoding>().unwrap(),
            ContentEncoding::Brotli
        );
        assert_eq!(
            "identity".parse::<ContentEncoding>().unwrap(),
            ContentEncoding::Identity
        );
    }

    #[test]
    fn test_content_encoding_to_algorithm() {
        assert_eq!(ContentEncoding::Gzip.to_algorithm(), Some(Algorithm::Gzip));
        assert_eq!(ContentEncoding::Zstd.to_algorithm(), Some(Algorithm::Zstd));
        assert_eq!(ContentEncoding::Lz4.to_algorithm(), Some(Algorithm::Lz4));
        assert_eq!(
            ContentEncoding::Identity.to_algorithm(),
            Some(Algorithm::None)
        );
        assert_eq!(ContentEncoding::Brotli.to_algorithm(), None);
    }

    #[test]
    fn test_accept_encoding_parse() {
        let accept = AcceptEncoding::parse("gzip, deflate, br;q=0.9, zstd;q=1.0");

        assert!(accept.accepts(ContentEncoding::Gzip));
        assert!(accept.accepts(ContentEncoding::Zstd));
        assert!((accept.quality(ContentEncoding::Gzip) - 1.0).abs() < 0.001);
        assert!((accept.quality(ContentEncoding::Brotli) - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_accept_encoding_wildcard() {
        let accept = AcceptEncoding::parse("*;q=0.5");

        assert!(accept.accepts(ContentEncoding::Gzip));
        assert!(accept.accepts(ContentEncoding::Zstd));
        assert!((accept.quality(ContentEncoding::Gzip) - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_accept_encoding_identity_rejected() {
        let accept = AcceptEncoding::parse("gzip, identity;q=0");

        assert!(accept.accepts(ContentEncoding::Gzip));
        assert!(!accept.accepts(ContentEncoding::Identity));
    }

    #[test]
    fn test_accept_encoding_best_encoding() {
        // Prefer zstd when available
        let accept = AcceptEncoding::parse("gzip, zstd, lz4");
        assert_eq!(accept.best_encoding(), ContentEncoding::Zstd);

        // Respect quality values
        let accept = AcceptEncoding::parse("gzip;q=1.0, zstd;q=0.5");
        assert_eq!(accept.best_encoding(), ContentEncoding::Gzip);

        // Fall back to identity when nothing accepted
        let accept = AcceptEncoding::parse("");
        assert_eq!(accept.best_encoding(), ContentEncoding::Identity);
    }

    #[test]
    fn test_content_encoder_encode() {
        let encoder = ContentEncoder::new(ContentEncoding::Gzip).with_min_size(10);
        let data = b"Hello, this is test data that should compress! ".repeat(100);

        let (compressed, encoding) = encoder.encode(&data).unwrap();

        assert_eq!(encoding, "gzip");
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_content_encoder_skip_small() {
        let encoder = ContentEncoder::new(ContentEncoding::Gzip).with_min_size(1000);
        let data = b"Small data";

        let (result, encoding) = encoder.encode(data).unwrap();

        assert_eq!(encoding, "identity");
        assert_eq!(result, data);
    }

    #[test]
    fn test_content_encoder_decode() {
        let data = b"Test data for encoding ".repeat(100);

        // Encode with gzip
        let compressor = Compressor::new(Algorithm::Gzip, CompressionLevel::Default);
        let compressed = compressor.compress(&data).unwrap();

        // Decode
        let decompressed = ContentEncoder::decode(ContentEncoding::Gzip, &compressed).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_response_compressor_should_compress() {
        let compressor = ResponseCompressor::new();

        assert!(compressor.should_compress("text/html", 2000));
        assert!(compressor.should_compress("application/json", 2000));
        assert!(!compressor.should_compress("text/html", 100)); // Too small
        assert!(!compressor.should_compress("image/png", 2000)); // Not compressible
    }

    #[test]
    fn test_response_compressor_compress() {
        let compressor = ResponseCompressor::new().with_min_size(10);
        let accept = AcceptEncoding::parse("gzip");
        let body = b"Test response body ".repeat(100);

        let (compressed, encoding) = compressor
            .compress_response(&accept, "application/json", &body)
            .unwrap();

        assert!(encoding.is_some());
        assert_eq!(encoding.unwrap(), "gzip");
        assert!(compressed.len() < body.len());
    }

    #[test]
    fn test_content_encoder_from_accept() {
        let accept = AcceptEncoding::parse("gzip, zstd;q=0.9");
        let encoder = ContentEncoder::from_accept(&accept);

        // Should prefer zstd despite lower quality due to higher priority
        assert_eq!(encoder.encoding(), ContentEncoding::Zstd);
    }
}
