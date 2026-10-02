// Marabunta - Licensed under the MIT License.
//! PostgreSQL detector.
//!
//! Probes port 5432 (default) using the PG wire protocol v3.0.
//! Sends a startup message with user "marabunta_probe" and reads the
//! server response to confirm PostgreSQL and extract the version from
//! ParameterStatus messages.

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{tcp_connect, Detector, DetectionStatus, SoftwareCapability};

/// PostgreSQL detector using PG wire protocol v3.0.
pub struct PostgresqlDetector {
    pub host: String,
    pub port: u16,
}

impl Default for PostgresqlDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
        }
    }
}

/// Build a PG startup message (protocol version 3.0).
fn build_startup_message(user: &[u8], database: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    // We'll fill in the length prefix after building the body.
    let body_len = 4 + 4 // length + protocol version
        + 5 + user.len() + 1       // "user\0" + user + \0
        + 9 + database.len() + 1   // "database\0" + db + \0
        + 1; // terminal \0
    msg.extend_from_slice(&(body_len as i32).to_be_bytes());
    msg.extend_from_slice(&196608_i32.to_be_bytes()); // v3.0
    msg.extend_from_slice(b"user\0");
    msg.extend_from_slice(user);
    msg.push(0);
    msg.extend_from_slice(b"database\0");
    msg.extend_from_slice(database);
    msg.push(0);
    msg.push(0); // terminal null
    msg
}

/// Parse PG wire messages from a response buffer, extracting ParameterStatus
/// key-value pairs. Returns (version, metadata).
fn parse_pg_response(buf: &[u8], n: usize) -> (Option<String>, HashMap<String, String>) {
    let mut version = None;
    let mut metadata = HashMap::new();
    let mut pos = 0;

    while pos + 5 <= n {
        let msg_type = buf[pos];
        if pos + 5 > n {
            break;
        }
        let len = i32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]])
            as usize;

        if len < 4 || pos + 1 + len > n {
            break;
        }

        if msg_type == b'S' {
            // ParameterStatus message
            let payload = &buf[pos + 5..pos + 1 + len];
            if let Some(null_pos) = payload.iter().position(|&b| b == 0) {
                let key = String::from_utf8_lossy(&payload[..null_pos]);
                let val_start = null_pos + 1;
                if val_start < payload.len() {
                    let val_end = payload[val_start..]
                        .iter()
                        .position(|&b| b == 0)
                        .map(|p| val_start + p)
                        .unwrap_or(payload.len());
                    let val = String::from_utf8_lossy(&payload[val_start..val_end]);
                    if key == "server_version" {
                        version = Some(val.to_string());
                    }
                    metadata.insert(key.to_string(), val.to_string());
                }
            }
        }

        pos += 1 + len;
    }

    (version, metadata)
}

#[async_trait]
impl Detector for PostgresqlDetector {
    fn name(&self) -> &str {
        "postgresql"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let mut stream = tcp_connect(&self.host, self.port).await?;

        // Send PG startup message
        let msg = build_startup_message(b"marabunta_probe", b"postgres");
        stream.write_all(&msg).await?;

        // Read response
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }

        let msg_type = buf[0] as char;

        match msg_type {
            // 'R' = Authentication message (confirms PG protocol)
            // 'E' = ErrorResponse (still confirms PG is there)
            'R' | 'E' => {
                let (version, metadata) = parse_pg_response(&buf, n);

                Ok(Some(SoftwareCapability {
                    software_type: "postgresql".into(),
                    version,
                    port: self.port,
                    status: if msg_type == 'R' {
                        DetectionStatus::Running
                    } else {
                        DetectionStatus::Available
                    },
                    detected_at: Utc::now(),
                    metadata,
                }))
            }
            _ => Ok(None), // Not PostgreSQL
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    /// Spawn a mock PG server that sends AuthenticationOk + ParameterStatus.
    async fn mock_pg_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                // Read startup message (discard)
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;

                // Send AuthenticationOk: 'R', len=8, auth_type=0
                let auth_ok: [u8; 9] = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
                stream.write_all(&auth_ok).await.unwrap();

                // Send ParameterStatus: key="server_version", val="16.2"
                let key = b"server_version\0";
                let val = b"16.2\0";
                let len = (4 + key.len() + val.len()) as i32;
                stream.write_all(&[b'S']).await.unwrap();
                stream.write_all(&len.to_be_bytes()).await.unwrap();
                stream.write_all(key).await.unwrap();
                stream.write_all(val).await.unwrap();
            }
        });
        port
    }

    /// Spawn a mock that sends an ErrorResponse (auth required).
    async fn mock_pg_auth_required() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;

                // ErrorResponse: 'E', length, severity, message
                let body = b"SFATAL\0Mpassword authentication failed\0\0";
                let len = (4 + body.len()) as i32;
                stream.write_all(&[b'E']).await.unwrap();
                stream.write_all(&len.to_be_bytes()).await.unwrap();
                stream.write_all(body).await.unwrap();
            }
        });
        port
    }

    /// Spawn a mock that sends garbage (not PG protocol).
    async fn mock_not_pg() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;
                stream
                    .write_all(b"NOT-A-PG-SERVER-RESPONSE")
                    .await
                    .unwrap();
            }
        });
        port
    }

    #[tokio::test]
    async fn test_detect_postgresql() {
        let port = mock_pg_server().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = PostgresqlDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "postgresql");
        assert_eq!(cap.version.as_deref(), Some("16.2"));
        assert_eq!(cap.status, DetectionStatus::Running);
    }

    #[tokio::test]
    async fn test_detect_postgresql_auth_required() {
        let port = mock_pg_auth_required().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = PostgresqlDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "postgresql");
        assert_eq!(cap.status, DetectionStatus::Available);
    }

    #[tokio::test]
    async fn test_detect_postgresql_wrong_protocol() {
        let port = mock_not_pg().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = PostgresqlDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_detect_postgresql_connection_refused() {
        let detector = PostgresqlDetector {
            host: "127.0.0.1".into(),
            port: 19999,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
