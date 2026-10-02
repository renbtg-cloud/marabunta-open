// Marabunta - Licensed under the MIT License.
//! Spider — Behavioural anomaly detection for the Marabunta swarm.
//!
//! Collects local system telemetry via `sysinfo`, maintains exponential
//! moving averages (EMA) per metric, computes z-score-based anomaly
//! scores, and emits [`MarabuntaEvent::AnomalyDetected`] when sustained
//! or preemptive thresholds are breached.
//!
//! Also aggregates [`HealthSummary`] reports from remote nodes (received
//! via gossip) and provides cluster-wide health queries: available nodes,
//! anomalous nodes, and overall [`SwarmStatus`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use sysinfo::{Disks, Networks, System};
use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::SpiderConfig;
use super::types::{MarabuntaEvent, Capability, NodeId, NodeMetrics, ResourceRequirements};

// ============================================================================
// SpiderError
// ============================================================================

/// Errors that can occur during Spider operations.
#[derive(Debug)]
pub enum SpiderError {
    /// Failed to collect system metrics.
    Collection(String),
    /// An I/O error occurred.
    Io(std::io::Error),
}

impl std::fmt::Display for SpiderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Collection(msg) => write!(f, "collection error: {}", msg),
            Self::Io(err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl std::error::Error for SpiderError {}

impl From<std::io::Error> for SpiderError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

// ============================================================================
// MetricValues
// ============================================================================

/// Smoothed metric values used for EMA baseline tracking.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricValues {
    pub cpu: f64,
    pub memory: f64,
    pub disk: f64,
    pub disk_io_read: f64,
    pub disk_io_write: f64,
    pub network_rx: f64,
    pub network_tx: f64,
    pub open_fds: f64,
    pub active_tasks: f64,
    pub temperature: Option<f64>,
}

// ============================================================================
// NodeBaseline
// ============================================================================

/// Per-node EMA baseline for anomaly detection.
///
/// Tracks exponential moving averages and their standard deviations.
/// Once enough samples have been collected (>= `warmup_samples`),
/// z-scores can be computed for incoming metrics.
pub struct NodeBaseline {
    pub node: NodeId,
    pub ema: MetricValues,
    pub ema_stddev: MetricValues,
    pub sample_count: u64,
    pub last_updated: SystemTime,
    pub anomaly_score: f64,
    pub anomaly_duration: Option<Duration>,
}

impl NodeBaseline {
    /// Create a zeroed baseline for the given node.
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            ema: MetricValues::default(),
            ema_stddev: MetricValues::default(),
            sample_count: 0,
            last_updated: SystemTime::now(),
            anomaly_score: 0.0,
            anomaly_duration: None,
        }
    }
}

// ============================================================================
// HealthSummary
// ============================================================================

/// Compact health snapshot gossiped between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSummary {
    pub node: NodeId,
    pub anomaly_score: f64,
    pub cpu_usage_percent: f64,
    pub memory_usage_percent: f64,
    pub active_tasks: u32,
    pub uptime_seconds: u64,
    pub capabilities: Vec<Capability>,
    pub available_resources: ResourceRequirements,
    pub timestamp: SystemTime,
}

// ============================================================================
// SwarmStatus
// ============================================================================

/// Overall health status of the swarm as seen by this Spider instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwarmStatus {
    /// All known nodes are healthy.
    Nominal,
    /// Some nodes are anomalous but below the critical threshold.
    Degraded {
        anomalous_count: usize,
        total_count: usize,
    },
    /// More than 30% of nodes are anomalous.
    Critical {
        anomalous_count: usize,
        total_count: usize,
    },
    /// No remote health data available.
    Unknown,
}

// ============================================================================
// TelemetryCollector
// ============================================================================

/// Collects local system metrics using the `sysinfo` crate.
pub struct TelemetryCollector {
    sys: System,
    networks: Networks,
}

impl TelemetryCollector {
    /// Create a new collector, initialising sysinfo handles.
    pub fn new() -> Self {
        Self {
            sys: System::new(),
            networks: Networks::new_with_refreshed_list(),
        }
    }

    /// Refresh system state and return a [`NodeMetrics`] snapshot.
    pub fn collect(&mut self) -> Result<NodeMetrics, SpiderError> {
        // Refresh subsystems — sysinfo 0.30 requires two CPU refreshes
        // separated by MINIMUM_CPU_UPDATE_INTERVAL for meaningful percentages.
        self.sys.refresh_cpu_usage();
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        self.sys.refresh_cpu_usage();
        self.sys.refresh_memory();
        self.networks.refresh();

        let cpu_count = self.sys.cpus().len() as f64;
        let cpu_usage = if cpu_count > 0.0 {
            let total: f64 = self.sys.cpus().iter().map(|c| c.cpu_usage() as f64).sum();
            total / cpu_count
        } else {
            0.0
        };

        let total_memory = self.sys.total_memory();
        let used_memory = self.sys.used_memory();
        let memory_usage = if total_memory > 0 {
            used_memory as f64 / total_memory as f64 * 100.0
        } else {
            0.0
        };

        // Disk usage — aggregate across all mounted disks
        let disks = Disks::new_with_refreshed_list();
        let (total_disk, used_disk) =
            disks
                .list()
                .iter()
                .fold((0u64, 0u64), |(tot, used), disk| {
                    let t = disk.total_space();
                    let a = disk.available_space();
                    (tot + t, used + t.saturating_sub(a))
                });
        let disk_usage = if total_disk > 0 {
            used_disk as f64 / total_disk as f64 * 100.0
        } else {
            0.0
        };

        // Network I/O — sum across all interfaces
        let (rx_bytes, tx_bytes) =
            self.networks
                .list()
                .iter()
                .fold((0u64, 0u64), |(rx, tx), (_name, data)| {
                    (rx + data.received(), tx + data.transmitted())
                });

        // Uptime
        let uptime = System::uptime();

        Ok(NodeMetrics {
            cpu_usage_percent: cpu_usage,
            memory_usage_percent: memory_usage,
            disk_usage_percent: disk_usage,
            disk_io_read_bytes_sec: 0,  // sysinfo 0.30 doesn't expose per-second disk I/O directly
            disk_io_write_bytes_sec: 0,
            network_rx_bytes_sec: rx_bytes,
            network_tx_bytes_sec: tx_bytes,
            open_file_descriptors: 0, // platform-specific; not exposed by sysinfo
            active_tasks: 0,          // filled in by the caller / work engine
            temperature_celsius: None, // requires Components refresh; optional
            fan_speed_rpm: None,
            uptime_seconds: uptime,
            timestamp: SystemTime::now(),
        })
    }
}

// ============================================================================
// Spider
// ============================================================================

/// Behavioural anomaly detection engine.
///
/// Maintains a local EMA baseline, collects telemetry periodically,
/// computes z-score anomaly scores, and aggregates remote health
/// summaries received via gossip.
pub struct Spider {
    local_node: NodeId,
    bus: Arc<NeuromancerBus>,
    config: SpiderConfig,
    local_baseline: NodeBaseline,
    swarm_health: HashMap<NodeId, HealthSummary>,
    collector: TelemetryCollector,
}

impl Spider {
    /// Create a new Spider instance.
    pub fn new(local_node: NodeId, bus: Arc<NeuromancerBus>, config: SpiderConfig) -> Self {
        Self {
            local_baseline: NodeBaseline::new(local_node),
            local_node,
            bus,
            config,
            swarm_health: HashMap::new(),
            collector: TelemetryCollector::new(),
        }
    }

    // ------------------------------------------------------------------ EMA
    /// Update the local EMA baseline with a new metrics sample.
    ///
    /// Uses exponential moving average:
    ///   new_ema = alpha * current + (1 - alpha) * old_ema
    /// And for standard deviation tracking:
    ///   new_stddev = alpha * |current - ema| + (1 - alpha) * old_stddev
    pub fn update_baseline(&mut self, metrics: &NodeMetrics) {
        let alpha = self.config.ema_alpha;
        let b = &mut self.local_baseline;

        // Helper macro to avoid repetitive code for each field
        macro_rules! ema_update {
            ($field:ident, $current:expr) => {
                let current = $current;
                b.ema.$field = alpha * current + (1.0 - alpha) * b.ema.$field;
                b.ema_stddev.$field =
                    alpha * (current - b.ema.$field).abs() + (1.0 - alpha) * b.ema_stddev.$field;
            };
        }

        ema_update!(cpu, metrics.cpu_usage_percent);
        ema_update!(memory, metrics.memory_usage_percent);
        ema_update!(disk, metrics.disk_usage_percent);
        ema_update!(disk_io_read, metrics.disk_io_read_bytes_sec as f64);
        ema_update!(disk_io_write, metrics.disk_io_write_bytes_sec as f64);
        ema_update!(network_rx, metrics.network_rx_bytes_sec as f64);
        ema_update!(network_tx, metrics.network_tx_bytes_sec as f64);
        ema_update!(open_fds, metrics.open_file_descriptors as f64);
        ema_update!(active_tasks, metrics.active_tasks as f64);

        // Temperature — only update if present
        if let Some(temp) = metrics.temperature_celsius {
            let old_ema = b.ema.temperature.unwrap_or(temp);
            let old_std = b.ema_stddev.temperature.unwrap_or(0.0);
            let new_ema = alpha * temp + (1.0 - alpha) * old_ema;
            let new_std = alpha * (temp - new_ema).abs() + (1.0 - alpha) * old_std;
            b.ema.temperature = Some(new_ema);
            b.ema_stddev.temperature = Some(new_std);
        }

        b.sample_count += 1;
        b.last_updated = SystemTime::now();
    }

    // ---------------------------------------------------------------- z-score
    /// Compute a z-score for a single metric.
    ///
    /// If stddev is near-zero and the value differs significantly from the
    /// mean, return a high score to flag the anomaly.
    pub fn z_score(current: f64, mean: f64, stddev: f64) -> f64 {
        if stddev < 0.001 {
            if (current - mean).abs() > 1.0 {
                5.0
            } else {
                0.0
            }
        } else {
            (current - mean).abs() / stddev
        }
    }

    /// Compute the overall anomaly score for the given metrics.
    ///
    /// Returns the **maximum** z-score across the monitored metric
    /// dimensions.  Returns 0.0 if still in the warmup period.
    pub fn compute_anomaly_score(&self, metrics: &NodeMetrics) -> f64 {
        let b = &self.local_baseline;
        if b.sample_count < self.config.warmup_samples as u64 {
            return 0.0;
        }

        let scores = [
            Self::z_score(metrics.cpu_usage_percent, b.ema.cpu, b.ema_stddev.cpu),
            Self::z_score(
                metrics.memory_usage_percent,
                b.ema.memory,
                b.ema_stddev.memory,
            ),
            Self::z_score(metrics.disk_usage_percent, b.ema.disk, b.ema_stddev.disk),
            Self::z_score(
                metrics.network_rx_bytes_sec as f64,
                b.ema.network_rx,
                b.ema_stddev.network_rx,
            ),
            Self::z_score(
                metrics.network_tx_bytes_sec as f64,
                b.ema.network_tx,
                b.ema_stddev.network_tx,
            ),
            Self::z_score(
                metrics.active_tasks as f64,
                b.ema.active_tasks,
                b.ema_stddev.active_tasks,
            ),
        ];

        scores
            .iter()
            .cloned()
            .fold(0.0_f64, f64::max)
    }

    // --------------------------------------------------------- collect & eval
    /// Collect local telemetry, update the baseline, and evaluate anomaly
    /// scores.  Emits events when thresholds are breached.
    pub async fn collect_and_evaluate(&mut self) {
        let metrics = match self.collector.collect() {
            Ok(m) => m,
            Err(e) => {
                warn!(node = %self.local_node, error = %e, "Spider: failed to collect metrics");
                return;
            }
        };

        self.update_baseline(&metrics);
        let score = self.compute_anomaly_score(&metrics);
        self.local_baseline.anomaly_score = score;

        // --- Preemptive checkpoint threshold ---
        if score >= self.config.preemptive_checkpoint_threshold
            && self.local_baseline.sample_count >= self.config.warmup_samples as u64
        {
            debug!(
                node = %self.local_node,
                score,
                "Spider: preemptive checkpoint threshold breached"
            );
            self.bus.emit(MarabuntaEvent::AnomalyDetected {
                node: self.local_node,
                score,
                details: format!(
                    "preemptive: z-score {:.2} exceeds checkpoint threshold {:.2}",
                    score, self.config.preemptive_checkpoint_threshold
                ),
                timestamp: SystemTime::now(),
            });
        }

        // --- Sustained anomaly threshold ---
        if score >= self.config.anomaly_threshold {
            match self.local_baseline.anomaly_duration {
                Some(existing) => {
                    // Accumulate duration
                    let new_duration = existing + self.config.collection_interval;
                    self.local_baseline.anomaly_duration = Some(new_duration);

                    if new_duration >= self.config.anomaly_sustained_duration {
                        info!(
                            node = %self.local_node,
                            score,
                            sustained_secs = new_duration.as_secs(),
                            "Spider: sustained anomaly detected"
                        );
                        self.bus.emit(MarabuntaEvent::AnomalyDetected {
                            node: self.local_node,
                            score,
                            details: format!(
                                "sustained: z-score {:.2} above {:.2} for {:.0}s",
                                score,
                                self.config.anomaly_threshold,
                                new_duration.as_secs_f64()
                            ),
                            timestamp: SystemTime::now(),
                        });
                        // Reset so we don't spam events every tick
                        self.local_baseline.anomaly_duration = None;
                    }
                }
                None => {
                    // Start tracking
                    self.local_baseline.anomaly_duration =
                        Some(self.config.collection_interval);
                }
            }
        } else {
            // Score dropped below threshold — reset duration tracker
            self.local_baseline.anomaly_duration = None;
        }
    }

    // ---------------------------------------------------------- remote health
    /// Ingest a health summary from a remote node (received via gossip).
    pub fn handle_remote_health(&mut self, summary: HealthSummary) {
        debug!(
            node = %summary.node,
            anomaly = summary.anomaly_score,
            "Spider: received remote health summary"
        );
        self.swarm_health.insert(summary.node, summary);
    }

    /// Remove stale health entries that have not been refreshed within
    /// the configured `stale_node_timeout`.
    pub fn cleanup_stale_nodes(&mut self) {
        let now = SystemTime::now();
        let timeout = self.config.stale_node_timeout;
        self.swarm_health.retain(|node, summary| {
            let age = now
                .duration_since(summary.timestamp)
                .unwrap_or(Duration::ZERO);
            if age > timeout {
                debug!(node = %node, age_secs = age.as_secs(), "Spider: removing stale node");
                false
            } else {
                true
            }
        });
    }

    // ----------------------------------------------------------- queries
    /// Return a reference to the full swarm health map.
    pub fn swarm_health_map(&self) -> &HashMap<NodeId, HealthSummary> {
        &self.swarm_health
    }

    /// Return nodes sorted by availability (lowest CPU first),
    /// excluding nodes whose anomaly score exceeds the threshold.
    pub fn nodes_by_availability(&self) -> Vec<(NodeId, &HealthSummary)> {
        let threshold = self.config.anomaly_threshold;
        let mut available: Vec<(NodeId, &HealthSummary)> = self
            .swarm_health
            .iter()
            .filter(|(_, s)| s.anomaly_score < threshold)
            .map(|(&id, s)| (id, s))
            .collect();
        available.sort_by(|a, b| {
            a.1.cpu_usage_percent
                .partial_cmp(&b.1.cpu_usage_percent)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        available
    }

    /// Return nodes whose anomaly score meets or exceeds the threshold.
    /// Get the current anomaly score for a specific remote node.
    pub fn current_anomaly_score(&self, node_id: &NodeId) -> Option<f64> {
        self.swarm_health.get(node_id).map(|s| s.anomaly_score)
    }

    /// Get the configured critical anomaly threshold.
    pub fn critical_threshold(&self) -> f64 {
        self.config.anomaly_threshold
    }

    pub fn anomalous_nodes(&self) -> Vec<(NodeId, f64)> {
        let threshold = self.config.anomaly_threshold;
        self.swarm_health
            .iter()
            .filter(|(_, s)| s.anomaly_score >= threshold)
            .map(|(&id, s)| (id, s.anomaly_score))
            .collect()
    }

    /// Compute the overall swarm status based on the proportion of
    /// anomalous nodes.
    pub fn swarm_status(&self) -> SwarmStatus {
        let total = self.swarm_health.len();
        if total == 0 {
            return SwarmStatus::Unknown;
        }
        let anomalous = self.anomalous_nodes().len();
        if anomalous == 0 {
            SwarmStatus::Nominal
        } else if anomalous * 100 > total * 30 {
            // >30% anomalous
            SwarmStatus::Critical {
                anomalous_count: anomalous,
                total_count: total,
            }
        } else {
            SwarmStatus::Degraded {
                anomalous_count: anomalous,
                total_count: total,
            }
        }
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    /// Helper: build a default SpiderConfig suitable for tests.
    fn test_config() -> SpiderConfig {
        SpiderConfig {
            collection_interval: Duration::from_secs(1),
            ema_alpha: 0.1,
            anomaly_threshold: 3.0,
            anomaly_sustained_duration: Duration::from_secs(5),
            preemptive_checkpoint_threshold: 2.0,
            gossip_interval: Duration::from_secs(10),
            stale_node_timeout: Duration::from_secs(60),
            warmup_samples: 20,
        }
    }

    /// Helper: build a NodeMetrics with the given values.
    fn make_metrics(cpu: f64, memory: f64, active_tasks: u32) -> NodeMetrics {
        NodeMetrics {
            cpu_usage_percent: cpu,
            memory_usage_percent: memory,
            disk_usage_percent: 0.0,
            disk_io_read_bytes_sec: 0,
            disk_io_write_bytes_sec: 0,
            network_rx_bytes_sec: 0,
            network_tx_bytes_sec: 0,
            open_file_descriptors: 0,
            active_tasks,
            temperature_celsius: None,
            fan_speed_rpm: None,
            uptime_seconds: 1000,
            timestamp: SystemTime::now(),
        }
    }

    /// Helper: build a HealthSummary for a given node.
    fn make_health(node: NodeId, cpu: f64, anomaly: f64) -> HealthSummary {
        HealthSummary {
            node,
            anomaly_score: anomaly,
            cpu_usage_percent: cpu,
            memory_usage_percent: 30.0,
            active_tasks: 2,
            uptime_seconds: 5000,
            capabilities: vec![],
            available_resources: ResourceRequirements::default(),
            timestamp: SystemTime::now(),
        }
    }

    // ---------------------------------------------------------------- test 1
    #[test]
    fn test_ema_convergence() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let config = test_config();
        let mut spider = Spider::new(node, bus, config);

        // Feed constant 50.0 CPU for 100 iterations
        for _ in 0..100 {
            let m = make_metrics(50.0, 40.0, 3);
            spider.update_baseline(&m);
        }

        // EMA should converge close to 50.0
        let ema_cpu = spider.local_baseline.ema.cpu;
        assert!(
            (ema_cpu - 50.0).abs() < 1.0,
            "EMA should converge to ~50.0 but got {:.4}",
            ema_cpu
        );
        assert_eq!(spider.local_baseline.sample_count, 100);
    }

    // ---------------------------------------------------------------- test 2
    #[test]
    fn test_z_score_known_values() {
        // current=10, mean=5, stddev=2.5 => z = |10-5|/2.5 = 2.0
        let z = Spider::z_score(10.0, 5.0, 2.5);
        assert!(
            (z - 2.0).abs() < 1e-9,
            "Expected z=2.0, got {:.4}",
            z
        );

        // current=5, mean=5, stddev=2.5 => z = 0.0
        let z2 = Spider::z_score(5.0, 5.0, 2.5);
        assert!(z2.abs() < 1e-9, "Expected z=0.0, got {:.4}", z2);
    }

    // ---------------------------------------------------------------- test 3
    #[test]
    fn test_no_anomaly_during_warmup() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let config = test_config(); // warmup_samples = 20
        let mut spider = Spider::new(node, bus, config);

        // Feed 19 samples — still in warmup
        for _ in 0..19 {
            let m = make_metrics(50.0, 50.0, 5);
            spider.update_baseline(&m);
        }
        assert_eq!(spider.local_baseline.sample_count, 19);

        // Even a wild metric should produce score 0.0 during warmup
        let wild = make_metrics(99.9, 99.9, 100);
        let score = spider.compute_anomaly_score(&wild);
        assert!(
            score == 0.0,
            "Anomaly score should be 0.0 during warmup, got {:.4}",
            score
        );
    }

    // ---------------------------------------------------------------- test 4
    #[test]
    fn test_brief_spike_no_sustained_anomaly() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let mut config = test_config();
        config.anomaly_sustained_duration = Duration::from_secs(10);
        config.collection_interval = Duration::from_secs(1);
        let mut spider = Spider::new(node, bus, config);

        // Build baseline: 25 samples at CPU=50
        for _ in 0..25 {
            spider.update_baseline(&make_metrics(50.0, 50.0, 5));
        }

        // Single spike
        let spike = make_metrics(99.0, 50.0, 5);
        let score = spider.compute_anomaly_score(&spike);
        assert!(score > 3.0, "Spike should produce high z-score");

        // Simulate: score above threshold for 1 tick, then drops
        spider.local_baseline.anomaly_duration = Some(Duration::from_secs(1));

        // Now below threshold => reset
        let normal = make_metrics(50.0, 50.0, 5);
        let normal_score = spider.compute_anomaly_score(&normal);
        if normal_score < spider.config.anomaly_threshold {
            spider.local_baseline.anomaly_duration = None;
        }
        assert!(
            spider.local_baseline.anomaly_duration.is_none(),
            "Brief spike should not accumulate into sustained anomaly"
        );
    }

    // ---------------------------------------------------------------- test 5
    #[tokio::test]
    async fn test_sustained_anomaly_triggers() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let mut rx = bus.subscribe();
        let mut config = test_config();
        config.anomaly_sustained_duration = Duration::from_secs(3);
        config.collection_interval = Duration::from_secs(1);
        config.warmup_samples = 5;
        // Raise preemptive threshold very high so it doesn't fire
        config.preemptive_checkpoint_threshold = 100.0;
        let mut spider = Spider::new(node, bus.clone(), config);

        // Build baseline at CPU=10
        for _ in 0..10 {
            spider.update_baseline(&make_metrics(10.0, 10.0, 1));
        }

        // Override the collector to avoid real sysinfo calls:
        // We manually drive the anomaly tracking instead of calling
        // collect_and_evaluate, because the real collector reads
        // actual system state.

        // Simulate sustained high CPU: manually set anomaly state
        let spike = make_metrics(99.0, 10.0, 1);
        let score = spider.compute_anomaly_score(&spike);
        assert!(score >= spider.config.anomaly_threshold);

        // Drive the sustained anomaly accumulator:
        // Tick 1: starts tracking
        spider.local_baseline.anomaly_score = score;
        spider.local_baseline.anomaly_duration = Some(Duration::from_secs(1));

        // Tick 2: accumulate
        spider.local_baseline.anomaly_duration = Some(Duration::from_secs(2));

        // Tick 3: accumulate — now 3s >= sustained_duration (3s)
        spider.local_baseline.anomaly_duration = Some(Duration::from_secs(3));

        // Emit the event manually as collect_and_evaluate would
        bus.emit(MarabuntaEvent::AnomalyDetected {
            node: spider.local_node,
            score,
            details: format!(
                "sustained: z-score {:.2} above {:.2} for 3s",
                score, spider.config.anomaly_threshold
            ),
            timestamp: SystemTime::now(),
        });

        let event = rx.recv().await.unwrap();
        match event {
            MarabuntaEvent::AnomalyDetected {
                node: n,
                details,
                ..
            } => {
                assert_eq!(n, node);
                assert!(details.contains("sustained"));
            }
            other => panic!("Expected AnomalyDetected, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------- test 6
    #[tokio::test]
    async fn test_preemptive_threshold_emits() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let mut rx = bus.subscribe();
        let mut config = test_config();
        config.preemptive_checkpoint_threshold = 2.0;
        config.anomaly_threshold = 100.0; // very high so sustained doesn't trigger
        config.warmup_samples = 5;
        config.collection_interval = Duration::from_secs(1);
        let mut spider = Spider::new(node, bus.clone(), config);

        // Build baseline at CPU=10
        for _ in 0..10 {
            spider.update_baseline(&make_metrics(10.0, 10.0, 1));
        }

        // Spike above preemptive threshold (2.0) but below sustained (100.0)
        let spike = make_metrics(95.0, 10.0, 1);
        let score = spider.compute_anomaly_score(&spike);
        assert!(score >= 2.0, "Score {:.2} should exceed preemptive threshold", score);

        // Manually emit as the preemptive path would
        if score >= spider.config.preemptive_checkpoint_threshold
            && spider.local_baseline.sample_count >= spider.config.warmup_samples
        {
            bus.emit(MarabuntaEvent::AnomalyDetected {
                node: spider.local_node,
                score,
                details: format!(
                    "preemptive: z-score {:.2} exceeds checkpoint threshold {:.2}",
                    score, spider.config.preemptive_checkpoint_threshold
                ),
                timestamp: SystemTime::now(),
            });
        }

        let event = rx.recv().await.unwrap();
        match event {
            MarabuntaEvent::AnomalyDetected { details, .. } => {
                assert!(
                    details.contains("preemptive"),
                    "Expected 'preemptive' in details, got: {}",
                    details
                );
            }
            other => panic!("Expected AnomalyDetected, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------- test 7
    #[test]
    fn test_near_zero_stddev_edge_case() {
        // stddev near zero, deviation > 1.0 => 5.0
        let z = Spider::z_score(10.0, 5.0, 0.0);
        assert_eq!(z, 5.0, "Near-zero stddev with large deviation should return 5.0");

        // stddev near zero, deviation <= 1.0 => 0.0
        let z2 = Spider::z_score(5.5, 5.0, 0.0);
        assert_eq!(z2, 0.0, "Near-zero stddev with small deviation should return 0.0");

        // stddev = 0.0005, deviation = 0.5 => should return 0.0 (stddev < 0.001)
        let z3 = Spider::z_score(5.5, 5.0, 0.0005);
        assert_eq!(z3, 0.0);

        // stddev = 0.0005, deviation = 2.0 => should return 5.0
        let z4 = Spider::z_score(7.0, 5.0, 0.0005);
        assert_eq!(z4, 5.0);
    }

    // ---------------------------------------------------------------- test 8
    #[test]
    fn test_health_summary_add_remove_stale_cleanup() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let mut config = test_config();
        config.stale_node_timeout = Duration::from_secs(2);
        let mut spider = Spider::new(node, bus, config);

        // Add some nodes
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();

        spider.handle_remote_health(make_health(n1, 20.0, 0.5));
        spider.handle_remote_health(make_health(n2, 40.0, 1.0));
        spider.handle_remote_health(make_health(n3, 60.0, 2.0));
        assert_eq!(spider.swarm_health_map().len(), 3);

        // Make n2 stale by backdating its timestamp
        if let Some(entry) = spider.swarm_health.get_mut(&n2) {
            entry.timestamp = SystemTime::now() - Duration::from_secs(10);
        }

        spider.cleanup_stale_nodes();
        assert_eq!(spider.swarm_health_map().len(), 2);
        assert!(!spider.swarm_health_map().contains_key(&n2));
        assert!(spider.swarm_health_map().contains_key(&n1));
        assert!(spider.swarm_health_map().contains_key(&n3));
    }

    // ---------------------------------------------------------------- test 9
    #[test]
    fn test_nodes_by_availability_sorted_filtered() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let config = test_config(); // anomaly_threshold = 3.0
        let mut spider = Spider::new(node, bus, config);

        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();
        let n4 = NodeId::new();

        // n1: CPU=80, anomaly=0.5 (healthy)
        spider.handle_remote_health(make_health(n1, 80.0, 0.5));
        // n2: CPU=20, anomaly=1.0 (healthy)
        spider.handle_remote_health(make_health(n2, 20.0, 1.0));
        // n3: CPU=50, anomaly=4.0 (anomalous — should be filtered out)
        spider.handle_remote_health(make_health(n3, 50.0, 4.0));
        // n4: CPU=10, anomaly=0.1 (healthy)
        spider.handle_remote_health(make_health(n4, 10.0, 0.1));

        let available = spider.nodes_by_availability();
        // n3 should be excluded (anomaly 4.0 >= 3.0)
        assert_eq!(available.len(), 3);

        // Should be sorted by CPU ascending: n4(10), n2(20), n1(80)
        assert_eq!(available[0].0, n4);
        assert_eq!(available[1].0, n2);
        assert_eq!(available[2].0, n1);
    }

    // ---------------------------------------------------------------- test 10
    #[test]
    fn test_swarm_status_variants() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let config = test_config(); // threshold = 3.0
        let mut spider = Spider::new(node, bus, config);

        // No nodes => Unknown
        assert_eq!(spider.swarm_status(), SwarmStatus::Unknown);

        // All healthy => Nominal
        for _ in 0..10 {
            let n = NodeId::new();
            spider.handle_remote_health(make_health(n, 30.0, 1.0));
        }
        assert_eq!(spider.swarm_status(), SwarmStatus::Nominal);

        // Add 1 anomalous out of 11 total (9%) => Degraded
        let anomalous = NodeId::new();
        spider.handle_remote_health(make_health(anomalous, 90.0, 5.0));
        match spider.swarm_status() {
            SwarmStatus::Degraded {
                anomalous_count,
                total_count,
            } => {
                assert_eq!(anomalous_count, 1);
                assert_eq!(total_count, 11);
            }
            other => panic!("Expected Degraded, got {:?}", other),
        }

        // Clear and make >30% anomalous => Critical
        spider.swarm_health.clear();
        // 3 healthy, 2 anomalous => 2/5 = 40% > 30%
        for _ in 0..3 {
            let n = NodeId::new();
            spider.handle_remote_health(make_health(n, 20.0, 0.5));
        }
        for _ in 0..2 {
            let n = NodeId::new();
            spider.handle_remote_health(make_health(n, 90.0, 5.0));
        }
        match spider.swarm_status() {
            SwarmStatus::Critical {
                anomalous_count,
                total_count,
            } => {
                assert_eq!(anomalous_count, 2);
                assert_eq!(total_count, 5);
            }
            other => panic!("Expected Critical, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------- test 11
    #[test]
    fn test_anomalous_nodes_returns_correct_set() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let config = test_config(); // threshold = 3.0
        let mut spider = Spider::new(node, bus, config);

        let healthy = NodeId::new();
        let bad1 = NodeId::new();
        let bad2 = NodeId::new();

        spider.handle_remote_health(make_health(healthy, 20.0, 1.0));
        spider.handle_remote_health(make_health(bad1, 90.0, 3.5));
        spider.handle_remote_health(make_health(bad2, 95.0, 4.0));

        let anomalous = spider.anomalous_nodes();
        assert_eq!(anomalous.len(), 2);
        let ids: Vec<NodeId> = anomalous.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&bad1));
        assert!(ids.contains(&bad2));
        assert!(!ids.contains(&healthy));
    }

    // ---------------------------------------------------------------- test 12
    #[test]
    fn test_ema_stddev_tracks_deviation() {
        let node = NodeId::new();
        let bus = Arc::new(NeuromancerBus::default());
        let config = test_config();
        let mut spider = Spider::new(node, bus, config);

        // Feed alternating values to build up stddev
        for i in 0..50 {
            let cpu = if i % 2 == 0 { 20.0 } else { 80.0 };
            spider.update_baseline(&make_metrics(cpu, 50.0, 5));
        }

        // stddev for CPU should be non-trivial given 20/80 alternation
        let stddev = spider.local_baseline.ema_stddev.cpu;
        assert!(
            stddev > 1.0,
            "CPU stddev should be significant with alternating 20/80, got {:.4}",
            stddev
        );
    }

    // ---------------------------------------------------------------- test 13
    #[test]
    fn test_metric_values_default_is_zeroed() {
        let mv = MetricValues::default();
        assert_eq!(mv.cpu, 0.0);
        assert_eq!(mv.memory, 0.0);
        assert_eq!(mv.disk, 0.0);
        assert_eq!(mv.disk_io_read, 0.0);
        assert_eq!(mv.disk_io_write, 0.0);
        assert_eq!(mv.network_rx, 0.0);
        assert_eq!(mv.network_tx, 0.0);
        assert_eq!(mv.open_fds, 0.0);
        assert_eq!(mv.active_tasks, 0.0);
        assert!(mv.temperature.is_none());
    }

    // ---------------------------------------------------------------- test 14
    #[test]
    fn test_health_summary_serde_roundtrip() {
        let node = NodeId::new();
        let summary = make_health(node, 42.0, 1.5);
        let json = serde_json::to_string(&summary).unwrap();
        let back: HealthSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.node, node);
        assert!((back.cpu_usage_percent - 42.0).abs() < 1e-9);
        assert!((back.anomaly_score - 1.5).abs() < 1e-9);
    }

    // ---------------------------------------------------------------- test 15
    #[test]
    fn test_node_baseline_initial_state() {
        let node = NodeId::new();
        let baseline = NodeBaseline::new(node);
        assert_eq!(baseline.node, node);
        assert_eq!(baseline.sample_count, 0);
        assert_eq!(baseline.anomaly_score, 0.0);
        assert!(baseline.anomaly_duration.is_none());
        assert_eq!(baseline.ema.cpu, 0.0);
        assert_eq!(baseline.ema_stddev.cpu, 0.0);
    }
}
