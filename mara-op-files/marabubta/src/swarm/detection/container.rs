// Marabunta - Licensed under the MIT License.
//! Container runtime detector (Docker / Podman).
//!
//! Checks for the existence of Unix domain sockets:
//!   - `/var/run/docker.sock` (Docker)
//!   - `/run/podman/podman.sock` (Podman)
//!
//! For each socket found, connects and sends `GET /version HTTP/1.1`
//! to retrieve version information from the container runtime API.

use std::collections::HashMap;
use std::path::Path;
use async_trait::async_trait;
use chrono::Utc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{Detector, DetectionStatus, SoftwareCapability};

/// Container runtime detector for Docker and Podman.
pub struct ContainerDetector {
    pub docker_socket: String,
    pub podman_socket: String,
}

impl Default for ContainerDetector {
    fn default() -> Self {
        Self {
            docker_socket: "/var/run/docker.sock".into(),
            podman_socket: "/run/podman/podman.sock".into(),
        }
    }
}

/// Probe a container runtime via its Unix socket.
async fn probe_unix_socket(
    socket_path: &str,
    runtime_name: &str,
) -> Option<(String, Option<String>, HashMap<String, String>)> {
    if !Path::new(socket_path).exists() {
        return None;
    }

    let mut stream = tokio::net::UnixStream::connect(socket_path).await.ok()?;

    let request = "GET /version HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).await.ok()?;

    let mut response = Vec::with_capacity(4096);
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf).await.ok()?;
        if n == 0 {
            break;
        }
        response.extend_from_slice(&buf[..n]);
        if response.len() > 65536 {
            break;
        }
    }

    let response_str = String::from_utf8_lossy(&response);

    let body = if let Some(pos) = response_str.find("\r\n\r\n") {
        &response_str[pos + 4..]
    } else {
        return Some((runtime_name.into(), None, HashMap::new()));
    };

    let version = extract_json_string(body, "Version");
    let api_version = extract_json_string(body, "ApiVersion");
    let os = extract_json_string(body, "Os");
    let arch = extract_json_string(body, "Arch");

    let mut metadata = HashMap::new();
    if let Some(ref av) = api_version {
        metadata.insert("api_version".into(), av.clone());
    }
    if let Some(ref o) = os {
        metadata.insert("os".into(), o.clone());
    }
    if let Some(ref a) = arch {
        metadata.insert("arch".into(), a.clone());
    }
    metadata.insert("socket_path".into(), socket_path.into());

    Some((runtime_name.into(), version, metadata))
}

/// Simple JSON string value extractor for `"key":"value"` patterns.
fn extract_json_string(json: &str, key: &str) -> Option<String> {
    let pattern = format!("\"{}\"", key);
    let pos = json.find(&pattern)?;
    let rest = &json[pos + pattern.len()..];
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?;
    let rest = rest.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

#[async_trait]
impl Detector for ContainerDetector {
    fn name(&self) -> &str {
        "container"
    }
    fn default_port(&self) -> u16 {
        0 // Uses Unix sockets, not TCP ports
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        // Try Docker first
        if let Some((name, version, metadata)) =
            probe_unix_socket(&self.docker_socket, "docker").await
        {
            return Ok(Some(SoftwareCapability {
                software_type: name,
                version,
                port: 0,
                status: DetectionStatus::Running,
                detected_at: Utc::now(),
                metadata,
            }));
        }

        // Try Podman
        if let Some((name, version, metadata)) =
            probe_unix_socket(&self.podman_socket, "podman").await
        {
            return Ok(Some(SoftwareCapability {
                software_type: name,
                version,
                port: 0,
                status: DetectionStatus::Running,
                detected_at: Utc::now(),
                metadata,
            }));
        }

        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json_string() {
        let json = r#"{"Version":"25.0.3","ApiVersion":"1.44","Os":"linux","Arch":"amd64"}"#;
        assert_eq!(extract_json_string(json, "Version"), Some("25.0.3".into()));
        assert_eq!(extract_json_string(json, "ApiVersion"), Some("1.44".into()));
        assert_eq!(extract_json_string(json, "Os"), Some("linux".into()));
        assert_eq!(extract_json_string(json, "Arch"), Some("amd64".into()));
        assert_eq!(extract_json_string(json, "Missing"), None);
    }

    #[test]
    fn test_extract_json_string_with_spaces() {
        let json = r#"{ "Version" : "25.0.3" , "ApiVersion" : "1.44" }"#;
        assert_eq!(extract_json_string(json, "Version"), Some("25.0.3".into()));
    }

    #[tokio::test]
    async fn test_detect_container_no_sockets() {
        let detector = ContainerDetector {
            docker_socket: "/tmp/nonexistent_docker_test_sock_marabunta".into(),
            podman_socket: "/tmp/nonexistent_podman_test_sock_marabunta".into(),
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_detect_container_mock_docker_socket() {
        let socket_path = format!("/tmp/marabunta_test_docker_{}.sock", std::process::id());
        let _ = std::fs::remove_file(&socket_path);

        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let sp = socket_path.clone();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;

                let body =
                    r#"{"Version":"25.0.3","ApiVersion":"1.44","Os":"linux","Arch":"amd64"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = ContainerDetector {
            docker_socket: socket_path.clone(),
            podman_socket: "/tmp/nonexistent_podman_test_sock_marabunta".into(),
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "docker");
        assert_eq!(cap.version.as_deref(), Some("25.0.3"));
        assert_eq!(cap.status, DetectionStatus::Running);
        assert_eq!(cap.metadata.get("api_version").unwrap(), "1.44");

        let _ = std::fs::remove_file(&sp);
    }

    #[tokio::test]
    async fn test_detect_container_mock_podman_socket() {
        let socket_path = format!("/tmp/marabunta_test_podman_{}.sock", std::process::id());
        let _ = std::fs::remove_file(&socket_path);

        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let sp = socket_path.clone();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;

                let body =
                    r#"{"Version":"4.9.0","ApiVersion":"4.9.0","Os":"linux","Arch":"amd64"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = ContainerDetector {
            docker_socket: "/tmp/nonexistent_docker_test_sock_marabunta".into(),
            podman_socket: socket_path.clone(),
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "podman");
        assert_eq!(cap.version.as_deref(), Some("4.9.0"));
        assert_eq!(cap.status, DetectionStatus::Running);

        let _ = std::fs::remove_file(&sp);
    }
}
