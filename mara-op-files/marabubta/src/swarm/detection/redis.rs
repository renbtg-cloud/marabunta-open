// Marabunta - Licensed under the MIT License.
//! Redis detector.
//!
//! Probes port 6379 (default) using the RESP (Redis Serialization Protocol).
//! Sends `PING\r\n` and expects `+PONG\r\n`. Then sends `INFO SERVER\r\n`
//! to extract the `redis_version` field.
//!
//! If AUTH is required, PING returns `-NOAUTH ...` which still confirms
//! Redis presence (status: Available, version: None).

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{tcp_connect, Detector, DetectionStatus, SoftwareCapability};

/// Redis detector using RESP protocol.
pub struct RedisDetector {
    pub host: String,
    pub port: u16,
}

impl Default for RedisDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 6379,
        }
    }
}

/// Parse the redis_version from an INFO SERVER response.
fn parse_redis_version(info: &str) -> Option<String> {
    for line in info.lines() {
        let line = line.trim().trim_end_matches('\r');
        if let Some(ver) = line.strip_prefix("redis_version:") {
            return Some(ver.trim().to_string());
        }
    }
    None
}

/// Parse additional metadata from INFO SERVER response.
fn parse_redis_metadata(info: &str) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    let keys_of_interest = [
        "redis_mode",
        "used_memory_human",
        "connected_clients",
        "os",
    ];

    for line in info.lines() {
        let line = line.trim().trim_end_matches('\r');
        if let Some((key, val)) = line.split_once(':') {
            if keys_of_interest.contains(&key) {
                metadata.insert(key.to_string(), val.trim().to_string());
            }
        }
    }
    metadata
}

#[async_trait]
impl Detector for RedisDetector {
    fn name(&self) -> &str {
        "redis"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let mut stream = tcp_connect(&self.host, self.port).await?;

        // Send PING
        stream.write_all(b"PING\r\n").await?;

        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }

        let response = String::from_utf8_lossy(&buf[..n]);

        if response.starts_with("+PONG") {
            // Redis confirmed, try to get version
            stream.write_all(b"INFO SERVER\r\n").await?;

            let mut info_buf = vec![0u8; 8192];
            let mut total = 0;
            // Read response in chunks until we get enough data
            loop {
                let n = stream.read(&mut info_buf[total..]).await?;
                if n == 0 {
                    break;
                }
                total += n;
                if total >= 8192 {
                    break;
                }
                // Check if we have a complete response
                let so_far = String::from_utf8_lossy(&info_buf[..total]);
                if so_far.contains("redis_version:") && so_far.ends_with("\r\n") {
                    break;
                }
            }

            let info = String::from_utf8_lossy(&info_buf[..total]);
            let version = parse_redis_version(&info);
            let mut metadata = parse_redis_metadata(&info);
            metadata.insert("mode".into(), "standalone".into());

            Ok(Some(SoftwareCapability {
                software_type: "redis".into(),
                version,
                port: self.port,
                status: DetectionStatus::Running,
                detected_at: Utc::now(),
                metadata,
            }))
        } else if response.starts_with("-NOAUTH") || response.starts_with("-ERR") {
            // Redis is there but requires authentication
            Ok(Some(SoftwareCapability {
                software_type: "redis".into(),
                version: None,
                port: self.port,
                status: DetectionStatus::Available,
                detected_at: Utc::now(),
                metadata: HashMap::new(),
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

    async fn mock_redis_server(version: &str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let version = version.to_string();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 512];
                let n = stream.read(&mut buf).await.unwrap();
                let cmd = String::from_utf8_lossy(&buf[..n]);

                if cmd.contains("PING") {
                    stream.write_all(b"+PONG\r\n").await.unwrap();

                    // Read INFO command
                    let n = stream.read(&mut buf).await.unwrap();
                    let cmd = String::from_utf8_lossy(&buf[..n]);
                    if cmd.contains("INFO") {
                        let info = format!(
                            "$200\r\n# Server\r\nredis_version:{}\r\nredis_mode:standalone\r\nused_memory_human:1.2M\r\n\r\n",
                            version
                        );
                        stream.write_all(info.as_bytes()).await.unwrap();
                    }
                }
            }
        });
        port
    }

    async fn mock_redis_auth_required() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;
                stream
                    .write_all(b"-NOAUTH Authentication required.\r\n")
                    .await
                    .unwrap();
            }
        });
        port
    }

    async fn mock_redis_wrong_response() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;
                stream
                    .write_all(b"NOT-A-REDIS-RESPONSE")
                    .await
                    .unwrap();
            }
        });
        port
    }

    #[tokio::test]
    async fn test_detect_redis() {
        let port = mock_redis_server("7.2.4").await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = RedisDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "redis");
        assert_eq!(cap.version.as_deref(), Some("7.2.4"));
        assert_eq!(cap.status, DetectionStatus::Running);
    }

    #[tokio::test]
    async fn test_detect_redis_auth_required() {
        let port = mock_redis_auth_required().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = RedisDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "redis");
        assert!(cap.version.is_none());
        assert_eq!(cap.status, DetectionStatus::Available);
    }

    #[tokio::test]
    async fn test_detect_redis_wrong_response() {
        let port = mock_redis_wrong_response().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = RedisDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_detect_redis_connection_refused() {
        let detector = RedisDetector {
            host: "127.0.0.1".into(),
            port: 19996,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
