#![allow(unexpected_cfgs)]
// Marabunta - Licensed under the MIT License.
//! Streaming compression for large payloads
//!
//! This module provides memory-efficient streaming compression and decompression
//! for handling large data that cannot fit entirely in memory.

use std::io::{self, Read, Write};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression as GzipCompression;

use super::algorithms::{Algorithm, CompressionLevel};
use super::errors::{CompressionError, CompressionResult};
use super::DEFAULT_CHUNK_SIZE;

/// Streaming compressor that wraps a writer
///
/// Data written to this compressor will be compressed and written to the underlying writer.
pub struct StreamingCompressor<W: Write> {
    inner: StreamingCompressorInner<W>,
    algorithm: Algorithm,
    bytes_in: u64,
}

enum StreamingCompressorInner<W: Write> {
    Gzip(GzEncoder<W>),
    Zstd(zstd::stream::write::Encoder<'static, W>),
    Lz4(lz4_flex::frame::FrameEncoder<W>),
    None(W),
}

impl<W: Write> StreamingCompressor<W> {
    /// Create a new streaming compressor
    pub fn new(algorithm: Algorithm, writer: W) -> CompressionResult<Self> {
        Self::with_level(algorithm, CompressionLevel::Default, writer)
    }

    /// Create a new streaming compressor with a specific compression level
    pub fn with_level(
        algorithm: Algorithm,
        level: CompressionLevel,
        writer: W,
    ) -> CompressionResult<Self> {
        let concrete_level = level.to_level(algorithm);

        let inner = match algorithm {
            Algorithm::Gzip => StreamingCompressorInner::Gzip(GzEncoder::new(
                writer,
                GzipCompression::new(concrete_level as u32),
            )),
            Algorithm::Zstd => {
                let encoder = zstd::stream::write::Encoder::new(writer, concrete_level)
                    .map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;
                StreamingCompressorInner::Zstd(encoder)
            }
            Algorithm::Lz4 => {
                StreamingCompressorInner::Lz4(lz4_flex::frame::FrameEncoder::new(writer))
            }
            Algorithm::None => StreamingCompressorInner::None(writer),
        };

        Ok(Self {
            inner,
            algorithm,
            bytes_in: 0,
        })
    }

    /// Get the compression algorithm
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Get the number of uncompressed bytes written
    pub fn bytes_in(&self) -> u64 {
        self.bytes_in
    }

    /// Finish compression and return the underlying writer
    pub fn finish(self) -> CompressionResult<W> {
        match self.inner {
            StreamingCompressorInner::Gzip(encoder) => encoder
                .finish()
                .map_err(|e| CompressionError::CompressionFailed(e.to_string())),
            StreamingCompressorInner::Zstd(encoder) => encoder
                .finish()
                .map_err(|e| CompressionError::CompressionFailed(e.to_string())),
            StreamingCompressorInner::Lz4(encoder) => encoder
                .finish()
                .map_err(|e| CompressionError::CompressionFailed(e.to_string())),
            StreamingCompressorInner::None(writer) => Ok(writer),
        }
    }
}

impl<W: Write> Write for StreamingCompressor<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = match &mut self.inner {
            StreamingCompressorInner::Gzip(encoder) => encoder.write(buf)?,
            StreamingCompressorInner::Zstd(encoder) => encoder.write(buf)?,
            StreamingCompressorInner::Lz4(encoder) => encoder.write(buf)?,
            StreamingCompressorInner::None(writer) => writer.write(buf)?,
        };
        self.bytes_in += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            StreamingCompressorInner::Gzip(encoder) => encoder.flush(),
            StreamingCompressorInner::Zstd(encoder) => encoder.flush(),
            StreamingCompressorInner::Lz4(encoder) => encoder.flush(),
            StreamingCompressorInner::None(writer) => writer.flush(),
        }
    }
}

/// Streaming decompressor that wraps a reader
///
/// Data read from this decompressor will be decompressed from the underlying reader.
pub struct StreamingDecompressor<R: Read> {
    inner: StreamingDecompressorInner<R>,
    algorithm: Algorithm,
    bytes_out: u64,
}

enum StreamingDecompressorInner<R: Read> {
    Gzip(GzDecoder<R>),
    Zstd(zstd::stream::read::Decoder<'static, io::BufReader<R>>),
    Lz4(lz4_flex::frame::FrameDecoder<R>),
    None(R),
}

impl<R: Read> StreamingDecompressor<R> {
    /// Create a new streaming decompressor
    pub fn new(algorithm: Algorithm, reader: R) -> CompressionResult<Self> {
        let inner = match algorithm {
            Algorithm::Gzip => StreamingDecompressorInner::Gzip(GzDecoder::new(reader)),
            Algorithm::Zstd => {
                let decoder = zstd::stream::read::Decoder::new(reader)
                    .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;
                StreamingDecompressorInner::Zstd(decoder)
            }
            Algorithm::Lz4 => {
                StreamingDecompressorInner::Lz4(lz4_flex::frame::FrameDecoder::new(reader))
            }
            Algorithm::None => StreamingDecompressorInner::None(reader),
        };

        Ok(Self {
            inner,
            algorithm,
            bytes_out: 0,
        })
    }

    /// Get the compression algorithm
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Get the number of decompressed bytes produced
    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }

    /// Get the inner reader (consumes the decompressor)
    pub fn into_inner(self) -> R {
        match self.inner {
            StreamingDecompressorInner::Gzip(decoder) => decoder.into_inner(),
            StreamingDecompressorInner::Zstd(decoder) => decoder.finish().into_inner(),
            StreamingDecompressorInner::Lz4(decoder) => decoder.into_inner(),
            StreamingDecompressorInner::None(reader) => reader,
        }
    }
}

impl<R: Read> Read for StreamingDecompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = match &mut self.inner {
            StreamingDecompressorInner::Gzip(decoder) => decoder.read(buf)?,
            StreamingDecompressorInner::Zstd(decoder) => decoder.read(buf)?,
            StreamingDecompressorInner::Lz4(decoder) => decoder.read(buf)?,
            StreamingDecompressorInner::None(reader) => reader.read(buf)?,
        };
        self.bytes_out += read as u64;
        Ok(read)
    }
}

/// Copy data from reader to writer with streaming compression
pub fn compress_stream<R: Read, W: Write>(
    reader: &mut R,
    writer: W,
    algorithm: Algorithm,
    level: CompressionLevel,
) -> CompressionResult<(u64, u64)> {
    let mut compressor = StreamingCompressor::with_level(algorithm, level, writer)?;
    let mut buffer = vec![0u8; DEFAULT_CHUNK_SIZE];
    let mut total_in = 0u64;

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        compressor.write_all(&buffer[..read])?;
        total_in += read as u64;
    }

    let _writer = compressor.finish()?;

    Ok((total_in, 0)) // bytes_out would require tracking in the writer
}

/// Copy data from reader to writer with streaming decompression
pub fn decompress_stream<R: Read, W: Write>(
    reader: R,
    writer: &mut W,
    algorithm: Algorithm,
) -> CompressionResult<(u64, u64)> {
    let mut decompressor = StreamingDecompressor::new(algorithm, reader)?;
    let mut buffer = vec![0u8; DEFAULT_CHUNK_SIZE];
    let mut total_out = 0u64;

    loop {
        let read = decompressor.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        total_out += read as u64;
    }

    Ok((0, total_out)) // bytes_in would require tracking in the reader
}

/// Async streaming compressor for use with tokio
#[cfg(feature = "tokio")]
pub mod async_streaming {
    use super::*;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    /// Compress data asynchronously from an async reader to an async writer
    pub async fn compress_async<R, W>(
        reader: &mut R,
        writer: &mut W,
        algorithm: Algorithm,
        level: CompressionLevel,
    ) -> CompressionResult<(u64, u64)>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        // For async, we read chunks and compress them synchronously
        // This is a simple approach; for full async support, use tokio-zstd etc.
        let mut buffer = vec![0u8; DEFAULT_CHUNK_SIZE];
        let mut all_data = Vec::new();

        loop {
            let read = reader.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            all_data.extend_from_slice(&buffer[..read]);
        }

        let compressor = super::super::Compressor::new(algorithm, level);
        let compressed = compressor.compress(&all_data)?;

        writer.write_all(&compressed).await?;

        Ok((all_data.len() as u64, compressed.len() as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_streaming_gzip_compress_decompress() {
        let original = b"Hello, this is streaming test data! ".repeat(1000);

        // Compress
        let mut compressed = Vec::new();
        {
            let mut compressor =
                StreamingCompressor::new(Algorithm::Gzip, &mut compressed).unwrap();
            compressor.write_all(&original).unwrap();
            compressor.finish().unwrap();
        }

        assert!(compressed.len() < original.len());

        // Decompress
        let mut decompressed = Vec::new();
        {
            let cursor = Cursor::new(&compressed);
            let mut decompressor = StreamingDecompressor::new(Algorithm::Gzip, cursor).unwrap();
            decompressor.read_to_end(&mut decompressed).unwrap();
        }

        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_streaming_zstd_compress_decompress() {
        let original = b"Hello, this is streaming zstd test data! ".repeat(100);

        // Compress
        let mut compressed = Vec::new();
        {
            let mut compressor =
                StreamingCompressor::new(Algorithm::Zstd, &mut compressed).unwrap();
            compressor.write_all(&original).unwrap();
            compressor.finish().unwrap();
        }

        assert!(compressed.len() < original.len());

        // Decompress
        let mut decompressed = Vec::new();
        {
            let cursor = Cursor::new(&compressed);
            let mut decompressor = StreamingDecompressor::new(Algorithm::Zstd, cursor).unwrap();
            decompressor.read_to_end(&mut decompressed).unwrap();
        }

        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_streaming_lz4_compress_decompress() {
        let original = b"Hello, this is streaming LZ4 test data! ".repeat(100);

        // Compress
        let mut compressed = Vec::new();
        {
            let mut compressor = StreamingCompressor::new(Algorithm::Lz4, &mut compressed).unwrap();
            compressor.write_all(&original).unwrap();
            compressor.finish().unwrap();
        }

        assert!(compressed.len() < original.len());

        // Decompress
        let mut decompressed = Vec::new();
        {
            let cursor = Cursor::new(&compressed);
            let mut decompressor = StreamingDecompressor::new(Algorithm::Lz4, cursor).unwrap();
            decompressor.read_to_end(&mut decompressed).unwrap();
        }

        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_streaming_none() {
        let original = b"Test data without compression";

        // Compress (passthrough)
        let mut compressed = Vec::new();
        {
            let mut compressor =
                StreamingCompressor::new(Algorithm::None, &mut compressed).unwrap();
            compressor.write_all(original).unwrap();
            compressor.finish().unwrap();
        }

        assert_eq!(original.as_slice(), compressed.as_slice());

        // Decompress (passthrough)
        let mut decompressed = Vec::new();
        {
            let cursor = Cursor::new(&compressed);
            let mut decompressor = StreamingDecompressor::new(Algorithm::None, cursor).unwrap();
            decompressor.read_to_end(&mut decompressed).unwrap();
        }

        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_streaming_with_level() {
        let original = b"Test data with custom level ".repeat(100);

        let mut compressed = Vec::new();
        {
            let mut compressor = StreamingCompressor::with_level(
                Algorithm::Gzip,
                CompressionLevel::Max,
                &mut compressed,
            )
            .unwrap();
            compressor.write_all(&original).unwrap();
            compressor.finish().unwrap();
        }

        assert!(compressed.len() < original.len());
    }

    #[test]
    fn test_streaming_multiple_writes() {
        let chunk1 = b"First chunk of data. ";
        let chunk2 = b"Second chunk of data. ";
        let chunk3 = b"Third chunk of data. ";

        let mut compressed = Vec::new();
        {
            let mut compressor =
                StreamingCompressor::new(Algorithm::Gzip, &mut compressed).unwrap();
            compressor.write_all(chunk1).unwrap();
            compressor.write_all(chunk2).unwrap();
            compressor.write_all(chunk3).unwrap();
            compressor.finish().unwrap();
        }

        let mut decompressed = Vec::new();
        {
            let cursor = Cursor::new(&compressed);
            let mut decompressor = StreamingDecompressor::new(Algorithm::Gzip, cursor).unwrap();
            decompressor.read_to_end(&mut decompressed).unwrap();
        }

        let expected: Vec<u8> = [chunk1.as_slice(), chunk2, chunk3].concat();
        assert_eq!(expected, decompressed);
    }

    #[test]
    fn test_compress_stream_helper() {
        let original = b"Stream helper test data ".repeat(100);
        let mut reader = Cursor::new(&original);
        let mut compressed = Vec::new();

        let (bytes_in, _) = compress_stream(
            &mut reader,
            &mut compressed,
            Algorithm::Gzip,
            CompressionLevel::Default,
        )
        .unwrap();

        assert_eq!(bytes_in as usize, original.len());
        assert!(compressed.len() < original.len());
    }

    #[test]
    fn test_decompress_stream_helper() {
        // First compress some data
        let original = b"Stream helper test data ".repeat(100);
        let mut compressed = Vec::new();
        {
            let mut compressor =
                StreamingCompressor::new(Algorithm::Gzip, &mut compressed).unwrap();
            compressor.write_all(&original).unwrap();
            compressor.finish().unwrap();
        }

        // Now test the decompress_stream helper
        let reader = Cursor::new(&compressed);
        let mut decompressed = Vec::new();

        let (_, bytes_out) = decompress_stream(reader, &mut decompressed, Algorithm::Gzip).unwrap();

        assert_eq!(bytes_out as usize, original.len());
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_bytes_tracking() {
        let original = b"Test data for tracking bytes written ".repeat(10);

        let mut compressed = Vec::new();
        let compressor = StreamingCompressor::new(Algorithm::Zstd, &mut compressed).unwrap();
        let mut compressor = compressor;

        assert_eq!(compressor.bytes_in(), 0);

        compressor.write_all(&original).unwrap();
        assert_eq!(compressor.bytes_in() as usize, original.len());

        compressor.finish().unwrap();
    }

    #[test]
    fn test_large_streaming_data() {
        // Test with data larger than DEFAULT_CHUNK_SIZE
        let original: Vec<u8> = (0..=255).cycle().take(DEFAULT_CHUNK_SIZE * 3).collect();

        let mut compressed = Vec::new();
        {
            let mut compressor =
                StreamingCompressor::new(Algorithm::Zstd, &mut compressed).unwrap();
            compressor.write_all(&original).unwrap();
            compressor.finish().unwrap();
        }

        let mut decompressed = Vec::new();
        {
            let cursor = Cursor::new(&compressed);
            let mut decompressor = StreamingDecompressor::new(Algorithm::Zstd, cursor).unwrap();
            decompressor.read_to_end(&mut decompressed).unwrap();
        }

        assert_eq!(original, decompressed);
    }

    #[test]
    fn test_streaming_empty_data() {
        for algo in [Algorithm::Zstd, Algorithm::Lz4, Algorithm::Gzip] {
            let original: &[u8] = b"";

            let mut compressed = Vec::new();
            {
                let mut compressor = StreamingCompressor::new(algo, &mut compressed).unwrap();
                compressor.write_all(original).unwrap();
                compressor.finish().unwrap();
            }

            let mut decompressed = Vec::new();
            {
                let cursor = Cursor::new(&compressed);
                let mut decompressor = StreamingDecompressor::new(algo, cursor).unwrap();
                decompressor.read_to_end(&mut decompressed).unwrap();
            }

            assert_eq!(original, decompressed.as_slice(), "Failed for {:?}", algo);
        }
    }
}
