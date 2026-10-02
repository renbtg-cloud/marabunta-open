// Marabunta - Licensed under the MIT License.
//! CockroachDB detector.
//!
//! Probes port 26257 (default) using the PG wire protocol v3.0 (same as
//! PostgreSQL). CockroachDB speaks the PG wire protocol but identifies
//! itself via the `crdb_version` parameter in ParameterStatus messages.
//! If `crdb_version` is not found, falls back to checking if
//! `server_version` contains "CockroachDB".

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{tcp_connect, Detector, DetectionStatus, SoftwareCapability};

/// CockroachDB detector using PG wire protocol.
pub struct CockroachDbDetector {
    pub host: String,
    pub port: u16,
}

impl Default for CockroachDbDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 26257,
        }
    }
}

/// Build a PG startup message (protocol version 3.0).
fn build_startup_message(user: &[u8], database: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    let body_len = 4 + 4
        + 5 + user.len() + 1
        + 9 + database.len() + 1
        + 1;
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

/// Parse PG wire messages, extracting ParameterStatus key-value pairs.
/// Returns (is_crdb, crdb_version, server_version, metadata).
fn parse_crdb_response(
    buf: &[u8],
    n: usize,
) -> (bool, Option<String>, Option<String>, HashMap<String, String>) {
    let mut is_crdb = false;
    let mut crdb_version = None;
    let mut server_version = None;
    let mut metadata = HashMap::new();
    let mut pos = 0;

    while pos + 5 <= n {
        let msg_type = buf[pos];
        let len = i32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]])
            as usize;

        if len < 4 || pos + 1 + len > n {
            break;
        }

        if msg_type == b'S' {
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

                    if key == "crdb_version" {
                        is_crdb = true;
                        crdb_version = Some(val.to_string());
                    }
                    if key == "server_version" {
                        server_version = Some(val.to_string());
                        if val.contains("CockroachDB") {
                            is_crdb = true;
                        }
                    }
                    metadata.insert(key.to_string(), val.to_string());
                }
            }
        }

        pos += 1 + len;
    }

    (is_crdb, crdb_version, server_version, metadata)
}

#[async_trait]
impl Detector for CockroachDbDetector {
    fn name(&self) -> &str {
        "cockroachdb"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let mut stream = tcp_connect(&self.host, self.port).await?;

        let msg = build_startup_message(b"marabunta_probe", b"defaultdb");
        stream.write_all(&msg).await?;

        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }

        let msg_type = buf[0] as char;

        match msg_type {
            'R' | 'E' => {
                let (is_crdb, crdb_version, server_version, metadata) =
                    parse_crdb_response(&buf, n);

                if !is_crdb {
                    // Responds to PG wire but not CockroachDB
                    return Ok(None);
                }

                let version = crdb_version.or(server_version);

                Ok(Some(SoftwareCapability {
                    software_type: "cockroachdb".into(),
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
            _ => Ok(None),
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

    /// Spawn a mock CockroachDB server that sends AuthOk + crdb_version param.
    async fn mock_crdb_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;

                // AuthenticationOk
                let auth_ok: [u8; 9] = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
                stream.write_all(&auth_ok).await.unwrap();

                // ParameterStatus: crdb_version = CockroachDB CCL v24.1.0
                let key = b"crdb_version\0";
                let val = b"CockroachDB CCL v24.1.0\0";
                let len = (4 + key.len() + val.len()) as i32;
                stream.write_all(&[b'S']).await.unwrap();
                stream.write_all(&len.to_be_bytes()).await.unwrap();
                stream.write_all(key).await.unwrap();
                stream.write_all(val).await.unwrap();

                // ParameterStatus: server_version = 13.0.0
                let key2 = b"server_version\0";
                let val2 = b"13.0.0\0";
                let len2 = (4 + key2.len() + val2.len()) as i32;
                stream.write_all(&[b'S']).await.unwrap();
                stream.write_all(&len2.to_be_bytes()).await.unwrap();
                stream.write_all(key2).await.unwrap();
                stream.write_all(val2).await.unwrap();
            }
        });
        port
    }

    /// Spawn a mock PG-compatible server without CRDB markers.
    async fn mock_plain_pg_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;

                // AuthenticationOk only, no crdb_version param
                let auth_ok: [u8; 9] = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
                stream.write_all(&auth_ok).await.unwrap();

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

    #[tokio::test]
    async fn test_detect_cockroachdb() {
        let port = mock_crdb_server().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = CockroachDbDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "cockroachdb");
        assert_eq!(
            cap.version.as_deref(),
            Some("CockroachDB CCL v24.1.0")
        );
        assert_eq!(cap.status, DetectionStatus::Running);
        assert_eq!(
            cap.metadata.get("crdb_version").unwrap(),
            "CockroachDB CCL v24.1.0"
        );
    }

    #[tokio::test]
    async fn test_detect_cockroachdb_not_crdb() {
        let port = mock_plain_pg_server().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = CockroachDbDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        // Should return None since this is plain PostgreSQL, not CockroachDB
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_detect_cockroachdb_connection_refused() {
        let detector = CockroachDbDetector {
            host: "127.0.0.1".into(),
            port: 19995,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
