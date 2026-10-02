// Marabunta - Licensed under the MIT License.
//! Elasticsearch detector.
//!
//! Probes port 9200 (default) with an HTTP GET to `/`. Parses the JSON
//! response for `version.number` and `cluster_name`. The tagline
//! "You Know, for Search" confirms Elasticsearch; OpenSearch returns
//! a different tagline which is recorded in metadata.

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;

use super::{extract_http_body, http_get_raw, Detector, DetectionStatus, SoftwareCapability};

/// Elasticsearch detector via HTTP JSON endpoint.
pub struct ElasticsearchDetector {
    pub host: String,
    pub port: u16,
}

impl Default for ElasticsearchDetector {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 9200,
        }
    }
}

/// Simple JSON string value extractor (avoids serde_json dependency for detection).
/// Looks for `"key" : "value"` or `"key": "value"` patterns.
fn json_extract_string(json: &str, key: &str) -> Option<String> {
    let pattern = format!("\"{}\"", key);
    let pos = json.find(&pattern)?;
    let rest = &json[pos + pattern.len()..];
    // Skip whitespace and colon
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?;
    let rest = rest.trim_start();

    if let Some(rest) = rest.strip_prefix('"') {
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    } else if rest.starts_with('{') {
        // Nested object -- look inside for "number"
        None
    } else {
        None
    }
}

/// Extract version.number from nested JSON without full parser.
fn extract_version_number(json: &str) -> Option<String> {
    // Find the "version" object
    let pos = json.find("\"version\"")?;
    let rest = &json[pos..];
    let brace_start = rest.find('{')?;
    let sub = &rest[brace_start..];
    // Now find "number" within this sub-object
    json_extract_string(sub, "number")
}

#[async_trait]
impl Detector for ElasticsearchDetector {
    fn name(&self) -> &str {
        "elasticsearch"
    }
    fn default_port(&self) -> u16 {
        self.port
    }

    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
        let response = http_get_raw(&self.host, self.port, "/").await?;
        let body = extract_http_body(&response);

        // Verify this looks like an ES/OpenSearch response
        if !body.contains("\"tagline\"") && !body.contains("\"version\"") {
            return Ok(None);
        }

        let version = extract_version_number(body);
        let cluster_name = json_extract_string(body, "cluster_name");
        let tagline = json_extract_string(body, "tagline");

        let mut metadata = HashMap::new();
        if let Some(ref cn) = cluster_name {
            metadata.insert("cluster_name".into(), cn.clone());
        }
        if let Some(ref tl) = tagline {
            metadata.insert("tagline".into(), tl.clone());
        }

        // Detect if this is OpenSearch vs Elasticsearch
        let software_type = if tagline
            .as_deref()
            .map(|t| t.contains("OpenSearch"))
            .unwrap_or(false)
        {
            "opensearch"
        } else {
            "elasticsearch"
        };

        Ok(Some(SoftwareCapability {
            software_type: software_type.into(),
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

    async fn mock_es_server(version: &str, cluster_name: &str, tagline: &str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let body = format!(
            r#"{{
  "name": "node-1",
  "cluster_name": "{}",
  "version": {{
    "number": "{}",
    "build_flavor": "default"
  }},
  "tagline": "{}"
}}"#,
            cluster_name, version, tagline
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
    async fn test_detect_elasticsearch() {
        let port =
            mock_es_server("8.12.0", "my-cluster", "You Know, for Search").await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = ElasticsearchDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "elasticsearch");
        assert_eq!(cap.version.as_deref(), Some("8.12.0"));
        assert_eq!(cap.metadata.get("cluster_name").unwrap(), "my-cluster");
    }

    #[tokio::test]
    async fn test_detect_opensearch() {
        let port =
            mock_es_server("2.12.0", "os-cluster", "The OpenSearch Project").await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let detector = ElasticsearchDetector {
            host: "127.0.0.1".into(),
            port,
        };
        let result = detector.detect().await.unwrap();
        assert!(result.is_some());
        let cap = result.unwrap();
        assert_eq!(cap.software_type, "opensearch");
        assert_eq!(cap.version.as_deref(), Some("2.12.0"));
    }

    #[tokio::test]
    async fn test_detect_elasticsearch_connection_refused() {
        let detector = ElasticsearchDetector {
            host: "127.0.0.1".into(),
            port: 19992,
        };
        let result = detector.detect().await;
        assert!(result.is_err() || result.unwrap().is_none());
    }
}
