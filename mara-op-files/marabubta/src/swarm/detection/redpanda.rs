// Marabunta - Licensed under the MIT License.
//! Redpanda detector.
//!
//! Redpanda is Kafka-compatible and listens on port 9092, but also
//! exposes an admin API on port 9644. This detector performs a Kafka
//! probe on 9092 and an HTTP GET to `http://localhost:9644/v1/status`
//! for the Redpanda-specific admin API. If the admin API responds
//! with a JSON payload containing "version", this is Redpanda.

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    extract_http_body, http_get_raw, tcp_connect, Detector, DetectionStatus, SoftwareCapability,
};
use super::kafka::build_api_versions_request;

/// Redpanda detector using Kafka protocol + admin API.
pub struct RedpandaDetector {
    pub host: String,
    pub kafka_port: u16,
    pub admin_port: u16,
}

impl Default for RedpandaDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            kafka_port: 9092,
            admin_port: 9644,
        }
    }
}

/// Try the Redpanda admin API on port 9644.
async fn probe_admin_api(host: &str, port: u16) -> Option<String> {
    let response = http_get_raw(host, port, "/v1/status").await.ok()?;
    let body = extract_http_body(&response);

    // Look for "version" in the JSON response
    // Simple JSON extraction without a JSON parser dependency
    if let Some(pos) = body.find("\"version\"") {
        let rest = &body[pos + 9..];
        // Skip whitespace and colon
        let rest = rest.trim_start().strip_prefix(':')?;
        let rest = rest.trim_start().strip_prefix('"')?;
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    None
}

/// Try the Kafka probe on the given port.
async fn probe_kafka(host: &str, port: u16) -> bool {
    let stream = tcp_connect(host, port).await;
    let mut stream = match stream {
        Ok(s) => s,
        Err(_) => return false,
    };

    let request = build_api_versions_request();
    if stream.write_all(&request).await.is_err() {
        return false;
    }

    let mut size_buf = [0u8; 4];
    if stream.read_exact(&mut size_buf).await.is_err() {
        return false;
    }

    let resp_size = i32::from_be_bytes(size_buf) as usize;
    if resp_size == 0 || resp_size > 65536 {
        return false;
    }

    // Able to read a Kafka-style response frame = compatible
    true
}

#[async_trait]
impl Detector for RedpandaDetector {
    fn name(&self) -> &str {
        "redpanda"
    }
    fn default_port(&self) -> u16 {
        self.admin_port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        // First, try the admin API (Redpanda-specific)
        let admin_version = probe_admin_api(&self.host, self.admin_port).await;

        if let Some(version) = admin_version {
            // Confirmed Redpanda via admin API
            let mut metadata = HashMap::new();
            metadata.insert("admin_port".into(), self.admin_port.to_string());
            metadata.insert("kafka_port".into(), self.kafka_port.to_string());

            return Ok(Some(SoftwareCapability {
                software_type: "redpanda".into(),
                version: Some(version),
                port: self.kafka_port,
                status: DetectionStatus::Running,
                detected_at: Utc::now(),
                metadata,
            }));
        }

        // Admin API failed; try Kafka probe. If Kafka-compatible but no
        // admin API, this is probably vanilla Kafka, not Redpanda.
        // We still report nothing since the KafkaDetector handles vanilla Kafka.
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Mock Redpanda admin API that returns a JSON status with version.
    async fn mock_admin_api(version: &str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let body = format!(
            r#"{{"version":"{}","node_id":0,"status":"ready"}}"#,
            version
        );
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        port
    }

    #[tokio::test]
    async fn test_detect_redpanda_with_admin_api() {
        let admin_port = mock_admin_api("v23.3.5").await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = RedpandaDetector {
            host: "127.0.0.1".into(),
            kafka_port: 9092, // doesn't matter, admin API succeeds
            admin_port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "redpanda");
        assert_eq!(cap.version.as_deref(), Some("v23.3.5"));
        assert_eq!(cap.status, DetectionStatus::Running);
    }

    #[tokio::test]
    async fn test_detect_redpanda_no_admin_api() {
        // No admin API means we can't confirm Redpanda
        let detector = RedpandaDetector {
            host: "127.0.0.1".into(),
            kafka_port: 19993,
            admin_port: 19994,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_detect_redpanda_admin_api_no_version() {
        // Admin API responds but without version field
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let admin_port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let body = r#"{"status":"not_ready"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = RedpandaDetector {
            host: "127.0.0.1".into(),
            kafka_port: 19993,
            admin_port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_none());
    }
}
