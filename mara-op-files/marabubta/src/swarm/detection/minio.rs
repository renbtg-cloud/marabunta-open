// Marabunta - Licensed under the MIT License.
//! MinIO detector.
//!
//! Probes port 9000 (default) with an HTTP GET to `/minio/health/live`.
//! MinIO responds with 200 OK and a `Server: MinIO` header. Falls back
//! to checking the `Server` header on any S3-style error response.

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;

use super::{
    extract_http_body, extract_http_header, http_get_raw, Detector, DetectionStatus,
    SoftwareCapability,
};

/// MinIO detector via health endpoint and Server header.
pub struct MinioDetector {
    pub host: String,
    pub port: u16,
}

impl Default for MinioDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 9000,
        }
    }
}

/// Simple XML tag value extractor for `<Tag>value</Tag>` patterns.
fn xml_extract(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)?;
    Some(xml[start..start + end].to_string())
}

#[async_trait]
impl Detector for MinioDetector {
    fn name(&self) -> &str {
        "minio"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        // Try the health endpoint first
        let response = http_get_raw(&self.host, self.port, "/minio/health/live").await?;

        let server_header = extract_http_header(&response, "Server");

        // Check if Server header contains MinIO
        let is_minio = server_header
            .map(|h| h.to_lowercase().contains("minio"))
            .unwrap_or(false);

        if !is_minio {
            // Try root path — MinIO returns S3-style XML error with Server header
            let root_response = http_get_raw(&self.host, self.port, "/").await?;
            let root_server = extract_http_header(&root_response, "Server");
            let root_is_minio = root_server
                .map(|h| h.to_lowercase().contains("minio"))
                .unwrap_or(false);

            if !root_is_minio {
                // Check for S3-style XML error body that might identify MinIO
                let body = extract_http_body(&root_response);
                if !body.contains("MinIO") && !body.contains("S3") {
                    return Ok(None);
                }
            }
        }

        // Extract version from Server header if available
        let version = server_header.and_then(|h| {
            // MinIO Server header format: "MinIO" or sometimes includes date-based version
            let parts: Vec<&str> = h.split('/').collect();
            if parts.len() > 1 {
                Some(parts[1].trim().to_string())
            } else {
                None
            }
        });

        let body = extract_http_body(&response);

        let mut metadata = HashMap::new();
        if let Some(h) = server_header {
            metadata.insert("server_header".into(), h.to_string());
        }
        // Try to extract S3 region from error response
        if let Some(region) = xml_extract(body, "Region") {
            metadata.insert("region".into(), region);
        }

        Ok(Some(SoftwareCapability {
            software_type: "minio".into(),
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

    async fn mock_minio_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let response = "HTTP/1.1 200 OK\r\nServer: MinIO\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        port
    }

    async fn mock_minio_server_with_version() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let response = "HTTP/1.1 200 OK\r\nServer: MinIO/RELEASE.2024-01-16T16-07-38Z\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        port
    }

    #[test]
    fn test_xml_extract() {
        let xml = "<Error><Code>AccessDenied</Code><Region>us-east-1</Region></Error>";
        assert_eq!(xml_extract(xml, "Code"), Some("AccessDenied".into()));
        assert_eq!(xml_extract(xml, "Region"), Some("us-east-1".into()));
        assert_eq!(xml_extract(xml, "Missing"), None);
    }

    #[tokio::test]
    async fn test_detect_minio() {
        let port = mock_minio_server().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = MinioDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "minio");
        assert_eq!(cap.status, DetectionStatus::Running);
    }

    #[tokio::test]
    async fn test_detect_minio_with_version() {
        let port = mock_minio_server_with_version().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = MinioDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "minio");
        assert!(cap.version.is_some());
        assert!(cap.version.unwrap().contains("RELEASE"));
    }

    #[tokio::test]
    async fn test_detect_minio_connection_refused() {
        let detector = MinioDetector {
            host: "127.0.0.1".into(),
            port: 19994,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
