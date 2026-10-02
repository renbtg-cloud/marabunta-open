// Marabunta - Licensed under the MIT License.
//! Web server detector (Nginx / Caddy / Apache).
//!
//! Probes port 80 (default) with an HTTP HEAD request and parses the
//! `Server` header to identify the web server software and version.
//! Recognizes Nginx, Apache (httpd), and Caddy.

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;

use super::{
    extract_http_header, http_get_raw, Detector, DetectionStatus, SoftwareCapability,
};

/// Web server detector via HTTP Server header.
pub struct WebServerDetector {
    pub host: String,
    pub port: u16,
}

impl Default for WebServerDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 80,
        }
    }
}

/// Parse a Server header value into (software_type, version).
fn parse_server_header(header: &str) -> Option<(String, Option<String>)> {
    let lower = header.to_lowercase();

    if lower.starts_with("nginx") {
        let version = header
            .split('/')
            .nth(1)
            .map(|v| v.split_whitespace().next().unwrap_or(v).to_string());
        Some(("nginx".into(), version))
    } else if lower.starts_with("apache") || lower.contains("httpd") {
        let version = header
            .split('/')
            .nth(1)
            .map(|v| v.split_whitespace().next().unwrap_or(v).to_string());
        Some(("apache".into(), version))
    } else if lower.starts_with("caddy") {
        let version = header
            .split('/')
            .nth(1)
            .map(|v| v.split_whitespace().next().unwrap_or(v).to_string());
        Some(("caddy".into(), version))
    } else {
        // Unknown server — still detected as a web server
        Some(("webserver".into(), None))
    }
}

#[async_trait]
impl Detector for WebServerDetector {
    fn name(&self) -> &str {
        "webserver"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let response = http_get_raw(&self.host, self.port, "/").await?;

        let server_header = extract_http_header(&response, "Server");

        let (software_type, version) = match server_header {
            Some(h) => match parse_server_header(h) {
                Some(parsed) => parsed,
                None => return Ok(None),
            },
            None => {
                // No Server header but port is open and speaks HTTP
                ("webserver".into(), None)
            }
        };

        let mut metadata = HashMap::new();
        if let Some(h) = server_header {
            metadata.insert("server_header".into(), h.to_string());
        }

        Ok(Some(SoftwareCapability {
            software_type,
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn mock_webserver(server_header: &str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let header = server_header.to_string();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let body = "<html><body>OK</body></html>";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nServer: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    header,
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        port
    }

    #[test]
    fn test_parse_server_header_nginx() {
        let (sw, ver) = parse_server_header("nginx/1.25.3").unwrap();
        assert_eq!(sw, "nginx");
        assert_eq!(ver.as_deref(), Some("1.25.3"));
    }

    #[test]
    fn test_parse_server_header_apache() {
        let (sw, ver) = parse_server_header("Apache/2.4.58 (Ubuntu)").unwrap();
        assert_eq!(sw, "apache");
        assert_eq!(ver.as_deref(), Some("2.4.58"));
    }

    #[test]
    fn test_parse_server_header_caddy() {
        let (sw, ver) = parse_server_header("Caddy/2.7.6").unwrap();
        assert_eq!(sw, "caddy");
        assert_eq!(ver.as_deref(), Some("2.7.6"));
    }

    #[test]
    fn test_parse_server_header_unknown() {
        let (sw, ver) = parse_server_header("MyCustomServer").unwrap();
        assert_eq!(sw, "webserver");
        assert!(ver.is_none());
    }

    #[tokio::test]
    async fn test_detect_nginx() {
        let port = mock_webserver("nginx/1.25.3").await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = WebServerDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "nginx");
        assert_eq!(cap.version.as_deref(), Some("1.25.3"));
        assert_eq!(cap.status, DetectionStatus::Running);
    }

    #[tokio::test]
    async fn test_detect_apache() {
        let port = mock_webserver("Apache/2.4.58 (Ubuntu)").await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = WebServerDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "apache");
        assert_eq!(cap.version.as_deref(), Some("2.4.58"));
    }

    #[tokio::test]
    async fn test_detect_webserver_connection_refused() {
        let detector = WebServerDetector {
            host: "127.0.0.1".into(),
            port: 19993,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
