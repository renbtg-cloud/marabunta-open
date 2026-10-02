// Marabunta - Licensed under the MIT License.
//! Kafka detector.
//!
//! Probes port 9092 (default) by sending an ApiVersions request
//! (API key 18, version 0). This is the one Kafka request that works
//! without authentication on most brokers. Parses the response for
//! a valid Kafka protocol frame.

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{tcp_connect, Detector, DetectionStatus, SoftwareCapability};

/// Kafka detector using the ApiVersions request.
pub struct KafkaDetector {
    pub host: String,
    pub port: u16,
}

impl Default for KafkaDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 9092,
        }
    }
}

/// Build a Kafka ApiVersions request (API key 18, version 0).
///
/// Format:
///   Size (4 bytes, big-endian) — length of the rest
///   Request header:
///     api_key (2 bytes) = 18
///     api_version (2 bytes) = 0
///     correlation_id (4 bytes) = 1
///     client_id: short string — length(2 bytes) + bytes
pub(crate) fn build_api_versions_request() -> Vec<u8> {
    let client_id = b"marabunta-probe";
    let client_id_len = client_id.len() as i16;

    // Header: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id
    let header_len = 2 + 2 + 4 + 2 + client_id.len();

    let mut msg = Vec::with_capacity(4 + header_len);
    // Size prefix (does not include itself)
    msg.extend_from_slice(&(header_len as i32).to_be_bytes());
    // API key = 18 (ApiVersions)
    msg.extend_from_slice(&18_i16.to_be_bytes());
    // API version = 0
    msg.extend_from_slice(&0_i16.to_be_bytes());
    // Correlation ID = 1
    msg.extend_from_slice(&1_i32.to_be_bytes());
    // Client ID
    msg.extend_from_slice(&client_id_len.to_be_bytes());
    msg.extend_from_slice(client_id);

    msg
}

/// Parse a Kafka ApiVersions response. Returns the number of API keys
/// supported (as a rough indicator of broker capabilities).
pub(crate) fn parse_api_versions_response(data: &[u8]) -> Option<(i32, usize)> {
    // Response format:
    //   Size (4 bytes) — already consumed before calling this
    //   correlation_id (4 bytes)
    //   error_code (2 bytes)
    //   api_count (4 bytes) — array length
    //   For each: api_key(2) + min_version(2) + max_version(2)

    if data.len() < 10 {
        return None;
    }

    let correlation_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let error_code = i16::from_be_bytes([data[4], data[5]]);

    if error_code != 0 {
        // Non-zero error, but still Kafka
        return Some((correlation_id, 0));
    }

    // api_count might be encoded differently based on version
    // For v0 response: it's a 4-byte count (int32)
    if data.len() >= 10 {
        let api_count = i32::from_be_bytes([data[6], data[7], data[8], data[9]]) as usize;
        return Some((correlation_id, api_count));
    }

    Some((correlation_id, 0))
}

#[async_trait]
impl Detector for KafkaDetector {
    fn name(&self) -> &str {
        "kafka"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let mut stream = tcp_connect(&self.host, self.port).await?;

        // Send ApiVersions request
        let request = build_api_versions_request();
        stream.write_all(&request).await?;

        // Read response size (4 bytes)
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await?;
        let resp_size = i32::from_be_bytes(size_buf) as usize;

        if resp_size == 0 || resp_size > 65536 {
            return Ok(None);
        }

        // Read response body
        let mut body = vec![0u8; resp_size.min(8192)];
        let to_read = resp_size.min(8192);
        stream.read_exact(&mut body[..to_read]).await?;

        // Parse response
        if let Some((corr_id, api_count)) = parse_api_versions_response(&body) {
            if corr_id != 1 {
                return Ok(None);
            }

            let mut metadata = HashMap::new();
            metadata.insert("api_count".into(), api_count.to_string());
            metadata.insert("protocol".into(), "kafka".into());

            Ok(Some(SoftwareCapability {
                software_type: "kafka".into(),
                version: None, // Kafka doesn't expose version in ApiVersions v0
                port: self.port,
                status: DetectionStatus::Running,
                detected_at: Utc::now(),
                metadata,
            }))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Build a mock ApiVersions response.
    fn build_mock_api_versions_response(api_count: i32) -> Vec<u8> {
        let mut body = Vec::new();
        // correlation_id = 1
        body.extend_from_slice(&1_i32.to_be_bytes());
        // error_code = 0
        body.extend_from_slice(&0_i16.to_be_bytes());
        // api_count
        body.extend_from_slice(&api_count.to_be_bytes());
        // For each API key: api_key(2) + min(2) + max(2)
        for i in 0..api_count.min(5) {
            body.extend_from_slice(&(i as i16).to_be_bytes()); // api_key
            body.extend_from_slice(&0_i16.to_be_bytes()); // min_version
            body.extend_from_slice(&10_i16.to_be_bytes()); // max_version
        }

        let mut msg = Vec::new();
        msg.extend_from_slice(&(body.len() as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }

    async fn mock_kafka_server(api_count: i32) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let response = build_mock_api_versions_response(api_count);
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                stream.write_all(&response).await.unwrap();
            }
        });
        port
    }

    #[tokio::test]
    async fn test_detect_kafka() {
        let port = mock_kafka_server(42).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = KafkaDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "kafka");
        assert_eq!(cap.status, DetectionStatus::Running);
        assert_eq!(cap.metadata.get("api_count").unwrap(), "42");
    }

    #[tokio::test]
    async fn test_detect_kafka_invalid_response() {
        // Server that sends garbage
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                // Send a frame with wrong correlation_id
                let mut body = Vec::new();
                body.extend_from_slice(&99_i32.to_be_bytes()); // wrong correlation_id
                body.extend_from_slice(&0_i16.to_be_bytes());
                body.extend_from_slice(&0_i32.to_be_bytes());
                let mut msg = Vec::new();
                msg.extend_from_slice(&(body.len() as i32).to_be_bytes());
                msg.extend_from_slice(&body);
                stream.write_all(&msg).await.unwrap();
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = KafkaDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_detect_kafka_connection_refused() {
        let detector = KafkaDetector {
            host: "127.0.0.1".into(),
            port: 19995,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
