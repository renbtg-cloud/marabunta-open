// Marabunta - Licensed under the MIT License.
//! MySQL detector.
//!
//! Probes port 3306 (default). MySQL sends an initial handshake packet
//! immediately upon TCP connection. We read the 4-byte header, then parse
//! the version string from the payload (null-terminated string starting
//! at byte 1 of the payload).

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::AsyncReadExt;

use super::{tcp_connect, Detector, DetectionStatus, SoftwareCapability};

/// MySQL detector using the initial handshake packet.
pub struct MysqlDetector {
    pub host: String,
    pub port: u16,
}

impl Default for MysqlDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 3306,
        }
    }
}

#[async_trait]
impl Detector for MysqlDetector {
    fn name(&self) -> &str {
        "mysql"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let mut stream = tcp_connect(&self.host, self.port).await?;

        // MySQL server sends initial handshake packet immediately.
        // 4-byte header: 3 bytes payload length (little-endian) + 1 byte sequence id
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await?;

        let payload_len = (header[0] as usize)
            | ((header[1] as usize) << 8)
            | ((header[2] as usize) << 16);
        let _seq_id = header[3];

        if payload_len == 0 || payload_len > 65535 {
            return Ok(None);
        }

        // Read payload
        let mut payload = vec![0u8; payload_len.min(1024)];
        let to_read = payload_len.min(1024);
        stream.read_exact(&mut payload[..to_read]).await?;

        // First byte is protocol version (should be 10 for modern MySQL)
        let protocol_version = payload[0];
        if protocol_version != 10 && protocol_version != 9 {
            return Ok(None);
        }

        // Version string: null-terminated starting at byte 1
        let version = if let Some(null_pos) = payload[1..].iter().position(|&b| b == 0) {
            let version_bytes = &payload[1..1 + null_pos];
            Some(String::from_utf8_lossy(version_bytes).to_string())
        } else {
            None
        };

        let mut metadata = HashMap::new();
        metadata.insert("protocol_version".into(), protocol_version.to_string());

        Ok(Some(SoftwareCapability {
            software_type: "mysql".into(),
            version,
            port: self.port,
            status: DetectionStatus::Running,
            detected_at: Utc::now(),
            metadata,
        }))
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

    /// Build a mock MySQL initial handshake packet.
    fn build_mysql_handshake(version: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(10); // protocol version 10
        payload.extend_from_slice(version.as_bytes());
        payload.push(0); // null terminator for version
        // Connection ID (4 bytes)
        payload.extend_from_slice(&[1, 0, 0, 0]);
        // Auth plugin data part 1 (8 bytes) + filler (1 byte)
        payload.extend_from_slice(&[0u8; 9]);

        let len = payload.len();
        let mut packet = Vec::new();
        // 3-byte little-endian length + 1 byte sequence id
        packet.push((len & 0xFF) as u8);
        packet.push(((len >> 8) & 0xFF) as u8);
        packet.push(((len >> 16) & 0xFF) as u8);
        packet.push(0); // sequence id 0
        packet.extend_from_slice(&payload);
        packet
    }

    async fn mock_mysql_server(version: &str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let packet = build_mysql_handshake(version);
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                stream.write_all(&packet).await.unwrap();
            }
        });
        port
    }

    #[tokio::test]
    async fn test_detect_mysql() {
        let port = mock_mysql_server("8.0.36").await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = MysqlDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "mysql");
        assert_eq!(cap.version.as_deref(), Some("8.0.36"));
        assert_eq!(cap.status, DetectionStatus::Running);
    }

    #[tokio::test]
    async fn test_detect_mysql_malformed() {
        // Server sends a packet with invalid protocol version
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                // payload: protocol version 99 (invalid) + some bytes
                let payload = [99u8, 0, 0, 0, 0];
                let len = payload.len();
                let header = [(len & 0xFF) as u8, 0, 0, 0];
                stream.write_all(&header).await.unwrap();
                stream.write_all(&payload).await.unwrap();
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = MysqlDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_detect_mysql_connection_refused() {
        let detector = MysqlDetector {
            host: "127.0.0.1".into(),
            port: 19998,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
