// Marabunta - Licensed under the MIT License.
//! Software Detection Framework for the Marabunta Swarm.
//!
//! Implements non-invasive detection of pre-existing software on each node.
//! Each detector scans for a specific piece of software (database, broker,
//! container runtime, web server) by attempting TCP connections and protocol
//! handshakes. No writes, no schema creation, no configuration changes.
//!
//! # Design Axiom
//!
//! "Detect, don't demand." The swarm never requires external software, but
//! when software is already running on a node, the swarm leverages it
//! opportunistically. Detection is the bridge between "zero dependencies"
//! and "maximal capability."

pub mod postgresql;
pub mod mysql;
pub mod mongodb;
pub mod redis;
pub mod kafka;
pub mod redpanda;
pub mod elasticsearch;
pub mod mqtt;
pub mod container;
pub mod webserver;
pub mod minio;
pub mod cockroachdb;

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::{timeout, Duration};

// ---------------------------------------------------------------------------
// Core trait
// ---------------------------------------------------------------------------

/// Trait implemented by each software detector.
///
/// Detectors are non-invasive: they perform read-only operations (TCP connect,
/// protocol handshake, version query) and never modify detected software.
#[async_trait]
pub trait Detector: Send + Sync {
    /// Attempt to detect the software. Returns `None` if not found.
    async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>>;

    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Default port this detector probes.
    fn default_port(&self) -> u16;
}

// ---------------------------------------------------------------------------
// Detection result types
// ---------------------------------------------------------------------------

/// Detection confidence level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DetectionStatus {
    /// Confirmed active and responding to protocol handshake.
    Running,
    /// Port open or socket exists, but protocol not fully verified.
    Available,
    /// Process or socket found but not responding.
    Stopped,
    /// Inconclusive result.
    Unknown,
}

/// A detected piece of software on this node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoftwareCapability {
    /// Canonical software identifier: "postgresql", "redis", "kafka", etc.
    pub software_type: String,
    /// Detected version string, if extractable.
    pub version: Option<String>,
    /// Port where the software was found.
    pub port: u16,
    /// Detection confidence level.
    pub status: DetectionStatus,
    /// When this detection occurred.
    pub detected_at: DateTime<Utc>,
    /// Additional key-value metadata (cluster_name, wire_version, etc.)
    pub metadata: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Capability advertisement (gossip broadcast structure)
// ---------------------------------------------------------------------------

/// The complete advertisement a node broadcasts via gossip.
/// Matches the JSON structure from spec S05.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityAdvertisement {
    pub node_id: String,
    pub timestamp: DateTime<Utc>,
    pub hardware: HardwareInfo,
    pub detected_software: Vec<SoftwareCapability>,
    pub deployable: bool,
    pub current_roles: Vec<String>,
    pub human_assigned: bool,
    pub load: LoadInfo,
}

/// Hardware information for capability advertisements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareInfo {
    pub os: String,
    pub arch: String,
    pub cpu_cores: u32,
    pub ram_gb: u32,
    pub disk_free_gb: u64,
    pub gpu: Option<String>,
}

/// Current load information for capability advertisements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadInfo {
    pub cpu_percent: f32,
    pub ram_percent: f32,
    pub network_mbps: f32,
}

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/// Timeout for each individual detector probe.
const DETECTOR_TIMEOUT: Duration = Duration::from_secs(2);

/// Run all detectors concurrently. Each gets a 2-second timeout.
/// Failed or timed-out detectors are silently omitted (software not present).
pub async fn run_all_detectors(
    detectors: &[Box<dyn Detector>],
) -> Vec<SoftwareCapability> {
    use futures::future::join_all;

    let futures: Vec<_> = detectors
        .iter()
        .map(|det| {
            let name = det.name().to_string();
            async move {
                match timeout(DETECTOR_TIMEOUT, det.detect()).await {
                    Ok(Ok(Some(cap))) => {
                        tracing::info!(detector = %name, "detected: {}", cap.software_type);
                        Some(cap)
                    }
                    Ok(Ok(None)) => None,
                    Ok(Err(e)) => {
                        tracing::debug!(detector = %name, "probe failed: {e}");
                        None
                    }
                    Err(_) => {
                        tracing::debug!(detector = %name, "probe timed out");
                        None
                    }
                }
            }
        })
        .collect();

    join_all(futures).await.into_iter().flatten().collect()
}

/// Build the default set of all 12 detectors with standard ports.
pub fn default_detectors() -> Vec<Box<dyn Detector>> {
    vec![
        // CockroachDB before PostgreSQL (both use PG wire protocol)
        Box::new(cockroachdb::CockroachDbDetector::default()),
        Box::new(postgresql::PostgresqlDetector::default()),
        Box::new(mysql::MysqlDetector::default()),
        Box::new(mongodb::MongoDbDetector::default()),
        Box::new(redis::RedisDetector::default()),
        Box::new(kafka::KafkaDetector::default()),
        Box::new(redpanda::RedpandaDetector::default()),
        Box::new(elasticsearch::ElasticsearchDetector::default()),
        Box::new(mqtt::MqttDetector::default()),
        Box::new(container::ContainerDetector::default()),
        Box::new(webserver::WebServerDetector::default()),
        Box::new(minio::MinioDetector::default()),
    ]
}

// ---------------------------------------------------------------------------
// Detection Scheduler
// ---------------------------------------------------------------------------

/// Periodic re-scan scheduler. Runs `run_all_detectors()` on an interval
/// and invokes a callback when results change.
pub struct DetectionScheduler {
    detectors: Vec<Box<dyn Detector>>,
    interval: Duration,
    last_results: tokio::sync::Mutex<Vec<SoftwareCapability>>,
    on_change: Box<dyn Fn(Vec<SoftwareCapability>) + Send + Sync>,
}

impl DetectionScheduler {
    pub fn new(
        detectors: Vec<Box<dyn Detector>>,
        interval: Duration,
        on_change: impl Fn(Vec<SoftwareCapability>) + Send + Sync + 'static,
    ) -> Self {
        Self {
            detectors,
            interval,
            last_results: tokio::sync::Mutex::new(Vec::new()),
            on_change: Box::new(on_change),
        }
    }

    /// Run the periodic scan loop. Call from `tokio::spawn`.
    pub async fn run(&self) {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            ticker.tick().await;
            let new_results = run_all_detectors(&self.detectors).await;

            let mut last = self.last_results.lock().await;
            if Self::has_changed(&last, &new_results) {
                tracing::info!("software detection change detected, broadcasting");
                (self.on_change)(new_results.clone());
            }
            *last = new_results;
        }
    }

    /// Trigger an immediate out-of-cycle scan.
    pub async fn trigger_immediate_scan(&self) -> Vec<SoftwareCapability> {
        let new_results = run_all_detectors(&self.detectors).await;
        let mut last = self.last_results.lock().await;
        if Self::has_changed(&last, &new_results) {
            (self.on_change)(new_results.clone());
        }
        *last = new_results.clone();
        new_results
    }

    /// Compare two result sets by software_type + port + status + version.
    fn has_changed(old: &[SoftwareCapability], new: &[SoftwareCapability]) -> bool {
        if old.len() != new.len() {
            return true;
        }
        for n in new {
            let matched = old.iter().any(|o| {
                o.software_type == n.software_type
                    && o.port == n.port
                    && o.status == n.status
                    && o.version == n.version
            });
            if !matched {
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Helper: TCP connect with timeout
// ---------------------------------------------------------------------------

/// Helper to connect to a TCP host:port with the standard detector timeout.
pub(crate) async fn tcp_connect(
    host: &str,
    port: u16,
) -> anyhow::Result<tokio::net::TcpStream> {
    let addr = format!("{}:{}", host, port);
    let stream = timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    .map_err(|_| anyhow::anyhow!("connection timeout to {}", addr))?
    .map_err(|e| anyhow::anyhow!("connection failed to {}: {}", addr, e))?;
    Ok(stream)
}

/// Helper to send an HTTP GET and read the raw response.
pub(crate) async fn http_get_raw(
    host: &str,
    port: u16,
    path: &str,
) -> anyhow::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tcp_connect(host, port).await?;

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\nUser-Agent: marabunta-probe/1.0\r\n\r\n",
        path, host, port
    );
    stream.write_all(request.as_bytes()).await?;

    let mut response = Vec::with_capacity(4096);
    let mut buf = [0u8; 4096];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        response.extend_from_slice(&buf[..n]);
        if response.len() > 65536 {
            break; // Don't read more than 64KB
        }
    }

    Ok(String::from_utf8_lossy(&response).to_string())
}

/// Extract the HTTP response body from a raw HTTP response string.
pub(crate) fn extract_http_body(response: &str) -> &str {
    if let Some(pos) = response.find("\r\n\r\n") {
        &response[pos + 4..]
    } else {
        ""
    }
}

/// Extract an HTTP header value from a raw HTTP response string.
pub(crate) fn extract_http_header<'a>(response: &'a str, header_name: &str) -> Option<&'a str> {
    let lower_header = header_name.to_lowercase();
    for line in response.lines() {
        if line.is_empty() || line == "\r" {
            break;
        }
        if let Some(colon_pos) = line.find(':') {
            let key = line[..colon_pos].trim().to_lowercase();
            if key == lower_header {
                return Some(line[colon_pos + 1..].trim().trim_end_matches('\r'));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detection_status_serde() {
        let status = DetectionStatus::Running;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, r#""running""#);

        let back: DetectionStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, DetectionStatus::Running);
    }

    #[test]
    fn test_software_capability_serde() {
        let cap = SoftwareCapability {
            software_type: "postgresql".into(),
            version: Some("16.2".into()),
            port: 5432,
            status: DetectionStatus::Running,
            detected_at: Utc::now(),
            metadata: HashMap::new(),
        };
        let json = serde_json::to_string(&cap).unwrap();
        assert!(json.contains("postgresql"));
        assert!(json.contains("16.2"));
    }

    #[test]
    fn test_capability_advertisement_serde() {
        let adv = CapabilityAdvertisement {
            node_id: "test-node".into(),
            timestamp: Utc::now(),
            hardware: HardwareInfo {
                os: "linux".into(),
                arch: "amd64".into(),
                cpu_cores: 8,
                ram_gb: 32,
                disk_free_gb: 500,
                gpu: None,
            },
            detected_software: vec![],
            deployable: true,
            current_roles: vec!["aggregator".into()],
            human_assigned: false,
            load: LoadInfo {
                cpu_percent: 5.0,
                ram_percent: 20.0,
                network_mbps: 1.0,
            },
        };
        let json = serde_json::to_string_pretty(&adv).unwrap();
        assert!(json.contains("test-node"));
        assert!(json.contains("aggregator"));
    }

    #[test]
    fn test_has_changed_same() {
        let a = vec![SoftwareCapability {
            software_type: "redis".into(),
            version: Some("7.2".into()),
            port: 6379,
            status: DetectionStatus::Running,
            detected_at: Utc::now(),
            metadata: HashMap::new(),
        }];
        let b = vec![SoftwareCapability {
            software_type: "redis".into(),
            version: Some("7.2".into()),
            port: 6379,
            status: DetectionStatus::Running,
            detected_at: Utc::now(), // different timestamp, same key fields
            metadata: HashMap::new(),
        }];
        assert!(!DetectionScheduler::has_changed(&a, &b));
    }

    #[test]
    fn test_has_changed_different_version() {
        let a = vec![SoftwareCapability {
            software_type: "redis".into(),
            version: Some("7.2".into()),
            port: 6379,
            status: DetectionStatus::Running,
            detected_at: Utc::now(),
            metadata: HashMap::new(),
        }];
        let b = vec![SoftwareCapability {
            software_type: "redis".into(),
            version: Some("7.4".into()),
            port: 6379,
            status: DetectionStatus::Running,
            detected_at: Utc::now(),
            metadata: HashMap::new(),
        }];
        assert!(DetectionScheduler::has_changed(&a, &b));
    }

    #[test]
    fn test_has_changed_different_length() {
        let a = vec![];
        let b = vec![SoftwareCapability {
            software_type: "redis".into(),
            version: Some("7.2".into()),
            port: 6379,
            status: DetectionStatus::Running,
            detected_at: Utc::now(),
            metadata: HashMap::new(),
        }];
        assert!(DetectionScheduler::has_changed(&a, &b));
    }

    #[test]
    fn test_extract_http_body() {
        let resp = "HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n{\"status\":\"ok\"}";
        assert_eq!(extract_http_body(resp), "{\"status\":\"ok\"}");
    }

    #[test]
    fn test_extract_http_header() {
        let resp = "HTTP/1.1 200 OK\r\nServer: nginx/1.25.3\r\nContent-Type: text/html\r\n\r\nbody";
        assert_eq!(extract_http_header(resp, "Server"), Some("nginx/1.25.3"));
        assert_eq!(
            extract_http_header(resp, "content-type"),
            Some("text/html")
        );
        assert_eq!(extract_http_header(resp, "X-Missing"), None);
    }

    #[test]
    fn test_default_detectors_list() {
        let detectors = default_detectors();
        assert_eq!(detectors.len(), 12);
        // CockroachDB should be first (before PostgreSQL)
        assert_eq!(detectors[0].name(), "cockroachdb");
        assert_eq!(detectors[1].name(), "postgresql");
    }

    #[tokio::test]
    async fn test_run_all_detectors_empty() {
        let detectors: Vec<Box<dyn Detector>> = vec![];
        let results = run_all_detectors(&detectors).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_run_all_detectors_with_timeout() {
        // A detector that hangs should be timed out silently.
        struct SlowDetector;

        #[async_trait]
        impl Detector for SlowDetector {
            fn name(&self) -> &str { "slow" }
            fn default_port(&self) -> u16 { 0 }
            async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(None)
            }
        }

        let detectors: Vec<Box<dyn Detector>> = vec![Box::new(SlowDetector)];
        let results = run_all_detectors(&detectors).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_run_all_detectors_mixed() {
        struct FoundDetector;
        struct NotFoundDetector;
        struct ErrorDetector;

        #[async_trait]
        impl Detector for FoundDetector {
            fn name(&self) -> &str { "found" }
            fn default_port(&self) -> u16 { 1234 }
            async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
                Ok(Some(SoftwareCapability {
                    software_type: "test-found".into(),
                    version: Some("1.0".into()),
                    port: 1234,
                    status: DetectionStatus::Running,
                    detected_at: Utc::now(),
                    metadata: HashMap::new(),
                }))
            }
        }

        #[async_trait]
        impl Detector for NotFoundDetector {
            fn name(&self) -> &str { "notfound" }
            fn default_port(&self) -> u16 { 0 }
            async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
                Ok(None)
            }
        }

        #[async_trait]
        impl Detector for ErrorDetector {
            fn name(&self) -> &str { "error" }
            fn default_port(&self) -> u16 { 0 }
            async fn detect(&self) -> anyhow::Result<Option<SoftwareCapability>> {
                Err(anyhow::anyhow!("probe failed"))
            }
        }

        let detectors: Vec<Box<dyn Detector>> = vec![
            Box::new(FoundDetector),
            Box::new(NotFoundDetector),
            Box::new(ErrorDetector),
        ];
        let results = run_all_detectors(&detectors).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].software_type, "test-found");
    }

    #[tokio::test]
    async fn test_detection_scheduler_immediate_scan() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let change_count = Arc::new(AtomicUsize::new(0));
        let cc = change_count.clone();

        let scheduler = DetectionScheduler::new(
            vec![], // no detectors = empty results
            Duration::from_secs(3600), // long interval, won't fire
            move |_| {
                cc.fetch_add(1, Ordering::SeqCst);
            },
        );

        let results = scheduler.trigger_immediate_scan().await;
        assert!(results.is_empty());
        // First scan with empty vs empty = no change
        assert_eq!(change_count.load(Ordering::SeqCst), 0);
    }
}
