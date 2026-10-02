// Marabunta - Licensed under the MIT License.
//! SLA definitions, monitoring, breach detection, and burn-rate alerting.
//!
//! This module provides a complete service-level agreement (SLA) framework
//! for the Marabunta Swarm. Operators define SLAs via [`SlaDefinition`], and
//! the [`SlaMonitor`] continuously evaluates compliance, tracks error budgets,
//! detects breaches, and computes burn rates for proactive alerting.
//!
//! # Architecture
//!
//! ```text
//!   SlaStore            SlaMonitor
//!   (DashMap)  ------>  check_all()  -----> SlaStatus (per SLA)
//!                           |
//!                           v
//!                     status_cache
//!                     breach_history
//!                     data_points
//! ```
//!
//! The monitor runs a periodic loop (via [`spawn_loop`](SlaMonitor::spawn_loop))
//! that measures each metric, computes compliance, detects breaches, and
//! optionally emits events for downstream alerting.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::complexity::EventSeverity;
use super::knowledge::KnowledgeStore;
use super::metrics::SwarmMetrics;

// ============================================================================
// Constants
// ============================================================================

/// Default SLA check interval (how often the monitor re-evaluates all SLAs).
const SLA_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Maximum number of breach episodes retained per SLA.
const MAX_BREACH_HISTORY: usize = 500;

/// Maximum number of data points retained per SLA for trend calculation.
const MAX_DATA_POINTS: usize = 8640; // 24h at 10s intervals

/// Burn rate threshold that triggers a fast-burn alert (consuming error
/// budget more than 14.4x faster than sustainable).
const BURN_RATE_FAST_THRESHOLD: f64 = 14.4;

/// Burn rate threshold for a slow-burn alert (2x faster than sustainable).
const BURN_RATE_SLOW_THRESHOLD: f64 = 2.0;

// ============================================================================
// SlaMetric
// ============================================================================

/// The metric that an SLA tracks.
///
/// Each variant maps to a concrete measurement that the [`SlaMonitor`]
/// knows how to sample. The `Custom` variant allows user-defined metrics
/// identified by a string key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlaMetric {
    /// Fraction of time the swarm has been operational (0.0 to 100.0 percent).
    Availability,
    /// Percentage of chunks that completed successfully (0.0 to 100.0).
    ChunkSuccessRate,
    /// 99th percentile chunk latency in milliseconds.
    ChunkLatencyP99Ms,
    /// 95th percentile chunk latency in milliseconds.
    ChunkLatencyP95Ms,
    /// Job completion rate as a percentage.
    JobCompletionRate,
    /// Time for gossip state to converge across the swarm (seconds).
    GossipConvergenceSecs,
    /// 99th percentile API request latency in milliseconds.
    ApiLatencyP99Ms,
    /// Event delivery latency in milliseconds.
    EventDeliveryLatencyMs,
    /// Data residency compliance percentage (100.0 = fully compliant).
    DataResidencyCompliance,
    /// A custom metric identified by a string key.
    Custom(String),
}

impl fmt::Display for SlaMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Availability => write!(f, "availability"),
            Self::ChunkSuccessRate => write!(f, "chunk_success_rate"),
            Self::ChunkLatencyP99Ms => write!(f, "chunk_latency_p99_ms"),
            Self::ChunkLatencyP95Ms => write!(f, "chunk_latency_p95_ms"),
            Self::JobCompletionRate => write!(f, "job_completion_rate"),
            Self::GossipConvergenceSecs => write!(f, "gossip_convergence_secs"),
            Self::ApiLatencyP99Ms => write!(f, "api_latency_p99_ms"),
            Self::EventDeliveryLatencyMs => write!(f, "event_delivery_latency_ms"),
            Self::DataResidencyCompliance => write!(f, "data_residency_compliance"),
            Self::Custom(key) => write!(f, "custom:{}", key),
        }
    }
}

impl FromStr for SlaMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "availability" => Ok(Self::Availability),
            "chunk_success_rate" => Ok(Self::ChunkSuccessRate),
            "chunk_latency_p99_ms" => Ok(Self::ChunkLatencyP99Ms),
            "chunk_latency_p95_ms" => Ok(Self::ChunkLatencyP95Ms),
            "job_completion_rate" => Ok(Self::JobCompletionRate),
            "gossip_convergence_secs" => Ok(Self::GossipConvergenceSecs),
            "api_latency_p99_ms" => Ok(Self::ApiLatencyP99Ms),
            "event_delivery_latency_ms" => Ok(Self::EventDeliveryLatencyMs),
            "data_residency_compliance" => Ok(Self::DataResidencyCompliance),
            other => {
                if let Some(key) = other.strip_prefix("custom:") {
                    Ok(Self::Custom(key.to_string()))
                } else {
                    Err(format!("unknown SLA metric: {}", other))
                }
            }
        }
    }
}

// ============================================================================
// SlaWindow
// ============================================================================

/// The time window over which an SLA is evaluated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SlaWindow {
    /// A rolling window of the specified number of hours.
    Rolling {
        /// Number of hours in the rolling window.
        hours: u32,
    },
    /// A calendar-aligned window (e.g., "day", "week", "month").
    Calendar {
        /// The calendar period (e.g., "day", "week", "month").
        period: String,
    },
}

impl SlaWindow {
    /// Get the window duration in hours.
    ///
    /// For calendar periods, returns a reasonable approximation.
    pub fn hours(&self) -> u32 {
        match self {
            Self::Rolling { hours } => *hours,
            Self::Calendar { period } => match period.as_str() {
                "day" => 24,
                "week" => 168,
                "month" => 720,
                _ => 24,
            },
        }
    }
}

impl fmt::Display for SlaWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rolling { hours } => write!(f, "rolling_{}h", hours),
            Self::Calendar { period } => write!(f, "calendar_{}", period),
        }
    }
}

// ============================================================================
// SlaDefinition
// ============================================================================

/// Definition of a service-level agreement.
///
/// An SLA specifies a target value for a specific metric, measured over
/// a time window, with an associated breach severity. The monitor checks
/// compliance periodically and triggers alerts when the target is breached.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaDefinition {
    /// Unique name for this SLA (e.g., "swarm-availability").
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Which metric this SLA tracks.
    pub metric: SlaMetric,
    /// Target value for the metric. For rate/percentage metrics, this is
    /// the minimum acceptable value (e.g., 99.9 for 99.9% availability).
    /// For latency metrics, this is the maximum acceptable value.
    pub target: f64,
    /// Time window over which the SLA is evaluated.
    pub window: SlaWindow,
    /// Severity of a breach event.
    pub breach_severity: EventSeverity,
    /// Whether this SLA is currently enabled.
    pub enabled: bool,
    /// When this SLA was created.
    pub created_at: DateTime<Utc>,
}

impl SlaDefinition {
    /// Check whether the given measured value meets the SLA target.
    ///
    /// For availability, success-rate, and compliance metrics, the measured
    /// value must be >= the target. For latency metrics, the measured value
    /// must be <= the target.
    pub fn is_compliant(&self, measured: f64) -> bool {
        match &self.metric {
            SlaMetric::ChunkLatencyP99Ms
            | SlaMetric::ChunkLatencyP95Ms
            | SlaMetric::ApiLatencyP99Ms
            | SlaMetric::GossipConvergenceSecs
            | SlaMetric::EventDeliveryLatencyMs => measured <= self.target,
            _ => measured >= self.target,
        }
    }

    /// Returns whether this metric is a "lower is better" metric (latency-like).
    pub fn is_upper_bound_metric(&self) -> bool {
        matches!(
            &self.metric,
            SlaMetric::ChunkLatencyP99Ms
                | SlaMetric::ChunkLatencyP95Ms
                | SlaMetric::ApiLatencyP99Ms
                | SlaMetric::GossipConvergenceSecs
                | SlaMetric::EventDeliveryLatencyMs
        )
    }
}

// ============================================================================
// SlaStatus
// ============================================================================

/// Current compliance status of a single SLA.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaStatus {
    /// SLA name.
    pub name: String,
    /// Most recent measured value of the metric.
    pub current_value: f64,
    /// Target value.
    pub target: f64,
    /// Compliance percentage (0.0 to 100.0).
    pub compliance_pct: f64,
    /// Whether the SLA is currently in breach.
    pub in_breach: bool,
    /// Remaining error budget as a fraction (0.0 to 1.0). When this reaches
    /// 0.0, the SLA's error budget for the window is exhausted.
    pub error_budget_remaining: f64,
    /// Current burn rate: how many multiples of the sustainable error budget
    /// consumption rate the SLA is currently burning. A value of 1.0 means
    /// the budget is being consumed at exactly the sustainable rate.
    pub burn_rate: f64,
    /// Direction the burn rate is trending: positive = accelerating,
    /// negative = decelerating, near-zero = stable.
    pub burn_rate_trend: f64,
    /// Total seconds spent in breach during the current window.
    pub time_in_breach_secs: f64,
    /// Number of distinct breach episodes in the current window.
    pub breach_count: u32,
    /// Timestamp of the most recent breach, if any.
    pub last_breach_at: Option<DateTime<Utc>>,
    /// Start of the current evaluation window.
    pub window_start: DateTime<Utc>,
    /// End of the current evaluation window.
    pub window_end: DateTime<Utc>,
    /// Number of data points collected in the current window.
    pub data_points: u32,
}

// ============================================================================
// SlaBreachEpisode
// ============================================================================

/// A contiguous period during which an SLA was in breach.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaBreachEpisode {
    /// When the breach started.
    pub started_at: DateTime<Utc>,
    /// When the breach ended (None if still ongoing).
    pub ended_at: Option<DateTime<Utc>>,
    /// Duration of the breach in seconds (0 if still ongoing).
    pub duration_secs: f64,
    /// Worst (most violating) measured value during the episode.
    pub worst_value: f64,
    /// Human-readable root cause description, if determinable.
    pub root_cause: String,
}

// ============================================================================
// SlaReport
// ============================================================================

/// A retrospective SLA compliance report covering a specific time period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaReport {
    /// SLA name.
    pub name: String,
    /// Start of the reporting period.
    pub period_start: DateTime<Utc>,
    /// End of the reporting period.
    pub period_end: DateTime<Utc>,
    /// Target value.
    pub target: f64,
    /// Achieved value (average over the period).
    pub achieved: f64,
    /// Whether the SLA was met for the full reporting period.
    pub compliance_met: bool,
    /// Total downtime (breach time) in seconds.
    pub total_downtime_secs: f64,
    /// Breach episodes during the reporting period.
    pub breach_episodes: Vec<SlaBreachEpisode>,
    /// Hourly measured values (timestamp_ms, value).
    pub hourly_values: Vec<(i64, f64)>,
}

// ============================================================================
// SlaOverallHealth
// ============================================================================

/// Aggregate health summary across all SLAs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaOverallHealth {
    /// Total number of defined SLAs.
    pub total_slas: u32,
    /// Number currently in compliance.
    pub in_compliance: u32,
    /// Number currently in breach.
    pub in_breach: u32,
    /// The lowest compliance percentage across all SLAs.
    pub lowest_compliance: f64,
    /// The highest burn rate across all SLAs.
    pub highest_burn_rate: f64,
}

// ============================================================================
// TimestampedValue
// ============================================================================

/// A single data point with a timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimestampedValue {
    /// When the value was measured.
    timestamp: DateTime<Utc>,
    /// The measured value.
    value: f64,
}

// ============================================================================
// SlaStore
// ============================================================================

/// Thread-safe store for SLA definitions.
///
/// Pre-loads five default SLAs on creation. Callers can add, remove, or
/// update definitions at any time.
pub struct SlaStore {
    /// Map from SLA name to its definition.
    definitions: DashMap<String, SlaDefinition>,
}

impl SlaStore {
    /// Create a new store with five default SLA definitions.
    pub fn new() -> Self {
        let store = Self {
            definitions: DashMap::new(),
        };
        store.load_defaults();
        store
    }

    /// Create an empty store with no default SLAs.
    pub fn empty() -> Self {
        Self {
            definitions: DashMap::new(),
        }
    }

    /// Load the five default SLA definitions into the store.
    fn load_defaults(&self) {
        let now = Utc::now();

        // 1. swarm-availability: >= 99.9%, rolling 24h, Critical
        self.upsert(SlaDefinition {
            name: "swarm-availability".to_string(),
            description: "Swarm cluster availability target".to_string(),
            metric: SlaMetric::Availability,
            target: 99.9,
            window: SlaWindow::Rolling { hours: 24 },
            breach_severity: EventSeverity::Critical,
            enabled: true,
            created_at: now,
        });

        // 2. chunk-success-rate: >= 99.0%, rolling 1h, Error
        self.upsert(SlaDefinition {
            name: "chunk-success-rate".to_string(),
            description: "Chunk execution success rate target".to_string(),
            metric: SlaMetric::ChunkSuccessRate,
            target: 99.0,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Error,
            enabled: true,
            created_at: now,
        });

        // 3. chunk-latency-p99: <= 30000ms, rolling 1h, Warning
        self.upsert(SlaDefinition {
            name: "chunk-latency-p99".to_string(),
            description: "99th percentile chunk latency target".to_string(),
            metric: SlaMetric::ChunkLatencyP99Ms,
            target: 30000.0,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Warning,
            enabled: true,
            created_at: now,
        });

        // 4. api-latency-p99: <= 1000ms, rolling 1h, Warning
        self.upsert(SlaDefinition {
            name: "api-latency-p99".to_string(),
            description: "99th percentile API latency target".to_string(),
            metric: SlaMetric::ApiLatencyP99Ms,
            target: 1000.0,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Warning,
            enabled: true,
            created_at: now,
        });

        // 5. data-residency: >= 100.0%, rolling 24h, Critical
        self.upsert(SlaDefinition {
            name: "data-residency".to_string(),
            description: "Data residency compliance requirement".to_string(),
            metric: SlaMetric::DataResidencyCompliance,
            target: 100.0,
            window: SlaWindow::Rolling { hours: 24 },
            breach_severity: EventSeverity::Critical,
            enabled: true,
            created_at: now,
        });
    }

    /// Insert or update an SLA definition.
    pub fn upsert(&self, definition: SlaDefinition) {
        self.definitions
            .insert(definition.name.clone(), definition);
    }

    /// Remove an SLA definition by name.
    ///
    /// Returns the removed definition, or `None` if not found.
    pub fn remove(&self, name: &str) -> Option<SlaDefinition> {
        self.definitions.remove(name).map(|(_, v)| v)
    }

    /// Get an SLA definition by name.
    pub fn get(&self, name: &str) -> Option<SlaDefinition> {
        self.definitions.get(name).map(|r| r.value().clone())
    }

    /// Get all SLA definitions.
    pub fn all(&self) -> Vec<SlaDefinition> {
        self.definitions.iter().map(|r| r.value().clone()).collect()
    }

    /// Get the number of stored definitions.
    pub fn count(&self) -> usize {
        self.definitions.len()
    }

    /// Check if a definition with the given name exists.
    pub fn contains(&self, name: &str) -> bool {
        self.definitions.contains_key(name)
    }
}

// ============================================================================
// SlaMonitor
// ============================================================================

/// Monitors SLA compliance, detects breaches, and computes burn rates.
///
/// The monitor reads metric values from the [`SwarmMetrics`] and
/// [`KnowledgeStore`] (when available), evaluates each SLA definition
/// from the [`SlaStore`], and maintains a per-SLA status cache,
/// breach history, and data-point time series.
///
/// # Usage
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use marabunta_compute::swarm::sla::{SlaStore, SlaMonitor};
///
/// let store = Arc::new(SlaStore::new());
/// let monitor = SlaMonitor::new(store);
/// monitor.check_all();
/// let health = monitor.overall_health();
/// ```
pub struct SlaMonitor {
    /// Source of SLA definitions.
    sla_store: Arc<SlaStore>,
    /// Optional metrics source for reading Prometheus-style values.
    metrics: Option<Arc<SwarmMetrics>>,
    /// Optional knowledge store for node/job/chunk counts.
    knowledge: Option<Arc<KnowledgeStore>>,
    /// Per-SLA cached status (name -> SlaStatus).
    status_cache: DashMap<String, SlaStatus>,
    /// Per-SLA breach history (name -> VecDeque of episodes).
    breach_history: DashMap<String, VecDeque<SlaBreachEpisode>>,
    /// Per-SLA data points for trend calculation (name -> VecDeque of values).
    data_points: DashMap<String, VecDeque<TimestampedValue>>,
    /// Previous burn rates for trend calculation (name -> previous burn rate).
    prev_burn_rates: DashMap<String, f64>,
    /// When the monitor was created (for uptime-based availability).
    started_at: Instant,
    /// Chrono timestamp of when the monitor was created.
    started_at_chrono: DateTime<Utc>,
}

impl SlaMonitor {
    /// Create a new SLA monitor with the given store.
    ///
    /// Metrics and knowledge sources are not attached; use
    /// [`with_metrics`](Self::with_metrics) and
    /// [`with_knowledge`](Self::with_knowledge) to attach them.
    pub fn new(sla_store: Arc<SlaStore>) -> Self {
        Self {
            sla_store,
            metrics: None,
            knowledge: None,
            status_cache: DashMap::new(),
            breach_history: DashMap::new(),
            data_points: DashMap::new(),
            prev_burn_rates: DashMap::new(),
            started_at: Instant::now(),
            started_at_chrono: Utc::now(),
        }
    }

    /// Attach a metrics source for reading Prometheus-style values.
    pub fn with_metrics(mut self, metrics: Arc<SwarmMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attach a knowledge store for node/job/chunk counts.
    pub fn with_knowledge(mut self, knowledge: Arc<KnowledgeStore>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// Evaluate all enabled SLAs, updating status caches and breach history.
    ///
    /// This is the main entry point called periodically by the monitor loop.
    pub fn check_all(&self) {
        let definitions = self.sla_store.all();
        let now = Utc::now();

        for def in definitions {
            if !def.enabled {
                continue;
            }

            let measured = self.measure(&def.metric);

            // Record the data point.
            self.record_data_point(&def.name, now, measured);

            // Calculate compliance and status.
            let status = self.evaluate(&def, measured, now);

            // Detect breach transitions.
            self.detect_breach_transition(&def, &status, now);

            // Update previous burn rate for trend.
            self.prev_burn_rates.insert(def.name.clone(), status.burn_rate);

            // Store the status.
            self.status_cache.insert(def.name.clone(), status);
        }
    }

    /// Measure the current value for a given SLA metric.
    ///
    /// Routes to the appropriate data source. When actual sources are not
    /// available, returns reasonable defaults (e.g., 100.0 for availability).
    pub fn measure(&self, metric: &SlaMetric) -> f64 {
        match metric {
            SlaMetric::Availability => {
                // If knowledge store is available, compute from live node ratio.
                if let Some(ref knowledge) = self.knowledge {
                    let all_nodes = knowledge.node_count();
                    if all_nodes == 0 {
                        return 100.0;
                    }
                    let live_nodes = knowledge.get_live_nodes().len();
                    (live_nodes as f64 / all_nodes as f64) * 100.0
                } else {
                    100.0
                }
            }
            SlaMetric::ChunkSuccessRate => {
                if let Some(ref metrics) = self.metrics {
                    let chunk_types = ["shell", "python", "function"];
                    let mut success = 0u64;
                    let mut failure = 0u64;
                    for &t in &chunk_types {
                        success += metrics
                            .chunks_executed_total
                            .with_label_values(&["success", t])
                            .get();
                        failure += metrics
                            .chunks_executed_total
                            .with_label_values(&["failure", t])
                            .get();
                    }
                    let total = success + failure;
                    if total == 0 {
                        return 100.0;
                    }
                    (success as f64 / total as f64) * 100.0
                } else {
                    100.0
                }
            }
            SlaMetric::ChunkLatencyP99Ms => {
                if let Some(ref metrics) = self.metrics {
                    let buckets = super::metrics::PercentileCalculator::extract_buckets(
                        &metrics.chunk_duration_seconds,
                        &["shell"],
                    );
                    super::metrics::PercentileCalculator::p99(&buckets)
                        .map(|secs| secs * 1000.0) // seconds to ms
                        .unwrap_or(0.0)
                } else {
                    0.0
                }
            }
            SlaMetric::ChunkLatencyP95Ms => {
                if let Some(ref metrics) = self.metrics {
                    let buckets = super::metrics::PercentileCalculator::extract_buckets(
                        &metrics.chunk_duration_seconds,
                        &["shell"],
                    );
                    super::metrics::PercentileCalculator::p95(&buckets)
                        .map(|secs| secs * 1000.0)
                        .unwrap_or(0.0)
                } else {
                    0.0
                }
            }
            SlaMetric::JobCompletionRate => {
                if let Some(ref metrics) = self.metrics {
                    metrics.job_completion_rate.get() * 100.0
                } else {
                    100.0
                }
            }
            SlaMetric::GossipConvergenceSecs => {
                // No direct source; estimate from reachable fraction.
                if let Some(ref metrics) = self.metrics {
                    let reachable = metrics.partition_reachable_fraction.get();
                    if reachable >= 0.99 {
                        0.5 // Fast convergence
                    } else if reachable >= 0.9 {
                        2.0
                    } else {
                        10.0 // Slow convergence
                    }
                } else {
                    1.0
                }
            }
            SlaMetric::ApiLatencyP99Ms => {
                if let Some(ref metrics) = self.metrics {
                    let buckets = super::metrics::PercentileCalculator::extract_buckets(
                        &metrics.api_request_duration_seconds,
                        &["GET", "/api/v1/nodes"],
                    );
                    super::metrics::PercentileCalculator::p99(&buckets)
                        .map(|secs| secs * 1000.0)
                        .unwrap_or(0.0)
                } else {
                    0.0
                }
            }
            SlaMetric::EventDeliveryLatencyMs => {
                // No direct source; return healthy default.
                5.0
            }
            SlaMetric::DataResidencyCompliance => {
                // If metrics available, check if any violations occurred.
                if let Some(ref metrics) = self.metrics {
                    // Sum all residency violations. If zero, compliance is 100%.
                    // Otherwise, estimate compliance from ratio.
                    // Since we do not know total checks, treat any violation as
                    // a slight reduction.
                    let text = metrics.encode();
                    let violation_count = text
                        .lines()
                        .filter(|line| {
                            line.contains("swarm_residency_violations_total")
                                && !line.starts_with('#')
                                && !line.contains(" 0")
                        })
                        .count();
                    if violation_count == 0 {
                        100.0
                    } else {
                        // Each violation reduces compliance. A rough model:
                        // compliance = max(0, 100 - violations * 0.1)
                        (100.0 - violation_count as f64 * 0.1).max(0.0)
                    }
                } else {
                    100.0
                }
            }
            SlaMetric::Custom(_) => {
                // Custom metrics require external measurement integration.
                // Return a neutral default.
                0.0
            }
        }
    }

    /// Evaluate a single SLA definition against a measured value.
    fn evaluate(
        &self,
        def: &SlaDefinition,
        measured: f64,
        now: DateTime<Utc>,
    ) -> SlaStatus {
        let window_hours = def.window.hours() as i64;
        let window_start = now - ChronoDuration::hours(window_hours);
        let window_end = now;

        let in_breach = !def.is_compliant(measured);

        // Calculate compliance percentage from data points in the window.
        let (compliance_pct, data_point_count, time_in_breach) =
            self.calculate_window_compliance(&def.name, def, window_start, now);

        // Calculate error budget.
        // For a 99.9% availability SLA over 24h, the error budget is 0.1% of 24h.
        let error_budget_total = if def.is_upper_bound_metric() {
            // For latency SLAs, budget is fraction of measurements that can exceed target.
            (100.0 - 99.0) / 100.0 // Allow 1% of measurements to exceed
        } else {
            (100.0 - def.target) / 100.0
        };

        let budget_consumed = if error_budget_total > 0.0 {
            let violation_fraction = time_in_breach / (window_hours as f64 * 3600.0).max(1.0);
            (violation_fraction / error_budget_total).min(1.0)
        } else if in_breach { 1.0 } else { 0.0 };

        let error_budget_remaining = (1.0 - budget_consumed).max(0.0);

        // Calculate burn rate.
        let burn_rate = if error_budget_total > 0.0 && window_hours > 0 {
            let expected_consumption_per_sec =
                error_budget_total / (window_hours as f64 * 3600.0);
            let actual_consumption_per_sec = if time_in_breach > 0.0 {
                budget_consumed * error_budget_total
                    / self.started_at.elapsed().as_secs_f64().max(1.0)
            } else {
                0.0
            };
            if expected_consumption_per_sec > 0.0 {
                actual_consumption_per_sec / expected_consumption_per_sec
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Burn rate trend: difference from previous burn rate.
        let burn_rate_trend = self
            .prev_burn_rates
            .get(&def.name)
            .map(|prev| burn_rate - *prev)
            .unwrap_or(0.0);

        // Count breach episodes from history.
        let breach_count = self
            .breach_history
            .get(&def.name)
            .map(|h| {
                h.iter()
                    .filter(|ep| {
                        ep.started_at >= window_start
                    })
                    .count() as u32
            })
            .unwrap_or(0);

        // Last breach timestamp.
        let last_breach_at = self
            .breach_history
            .get(&def.name)
            .and_then(|h| h.back().map(|ep| ep.started_at));

        SlaStatus {
            name: def.name.clone(),
            current_value: measured,
            target: def.target,
            compliance_pct,
            in_breach,
            error_budget_remaining,
            burn_rate,
            burn_rate_trend,
            time_in_breach_secs: time_in_breach,
            breach_count,
            last_breach_at,
            window_start,
            window_end,
            data_points: data_point_count,
        }
    }

    /// Calculate compliance percentage from stored data points within a window.
    fn calculate_window_compliance(
        &self,
        sla_name: &str,
        def: &SlaDefinition,
        window_start: DateTime<Utc>,
        _now: DateTime<Utc>,
    ) -> (f64, u32, f64) {
        let points = match self.data_points.get(sla_name) {
            Some(pts) => pts,
            None => return (100.0, 0, 0.0),
        };

        let window_points: Vec<&TimestampedValue> = points
            .iter()
            .filter(|p| p.timestamp >= window_start)
            .collect();

        let total = window_points.len();
        if total == 0 {
            return (100.0, 0, 0.0);
        }

        let compliant_count = window_points
            .iter()
            .filter(|p| def.is_compliant(p.value))
            .count();

        let compliance_pct = (compliant_count as f64 / total as f64) * 100.0;

        // Estimate time in breach: each data point represents ~SLA_CHECK_INTERVAL seconds.
        let non_compliant = total - compliant_count;
        let time_in_breach = non_compliant as f64 * SLA_CHECK_INTERVAL.as_secs_f64();

        (compliance_pct, total as u32, time_in_breach)
    }

    /// Record a data point for a given SLA.
    fn record_data_point(&self, sla_name: &str, timestamp: DateTime<Utc>, value: f64) {
        let mut points = self
            .data_points
            .entry(sla_name.to_string())
            .or_insert_with(|| VecDeque::with_capacity(MAX_DATA_POINTS));
        if points.len() >= MAX_DATA_POINTS {
            points.pop_front();
        }
        points.push_back(TimestampedValue { timestamp, value });
    }

    /// Detect and record breach transitions (entering or exiting breach state).
    fn detect_breach_transition(
        &self,
        def: &SlaDefinition,
        status: &SlaStatus,
        now: DateTime<Utc>,
    ) {
        let was_in_breach = self
            .status_cache
            .get(&def.name)
            .map(|prev| prev.in_breach)
            .unwrap_or(false);

        let mut history = self
            .breach_history
            .entry(def.name.clone())
            .or_insert_with(|| VecDeque::with_capacity(MAX_BREACH_HISTORY));

        if status.in_breach && !was_in_breach {
            // Entering breach state.
            if history.len() >= MAX_BREACH_HISTORY {
                history.pop_front();
            }
            let root_cause = format!(
                "{} measured at {:.2}, target is {:.2}",
                def.metric, status.current_value, def.target
            );
            history.push_back(SlaBreachEpisode {
                started_at: now,
                ended_at: None,
                duration_secs: 0.0,
                worst_value: status.current_value,
                root_cause,
            });
            warn!(
                sla = %def.name,
                metric = %def.metric,
                value = status.current_value,
                target = def.target,
                severity = %def.breach_severity,
                "SLA breach detected"
            );
        } else if status.in_breach && was_in_breach {
            // Still in breach -- update the worst value.
            if let Some(episode) = history.back_mut() {
                if episode.ended_at.is_none() {
                    let elapsed = (now - episode.started_at).num_seconds().max(0) as f64;
                    episode.duration_secs = elapsed;
                    // Track worst value depending on metric direction.
                    if def.is_upper_bound_metric() {
                        if status.current_value > episode.worst_value {
                            episode.worst_value = status.current_value;
                        }
                    } else if status.current_value < episode.worst_value {
                        episode.worst_value = status.current_value;
                    }
                }
            }
        } else if !status.in_breach && was_in_breach {
            // Exiting breach state.
            if let Some(episode) = history.back_mut() {
                if episode.ended_at.is_none() {
                    let elapsed = (now - episode.started_at).num_seconds().max(0) as f64;
                    episode.ended_at = Some(now);
                    episode.duration_secs = elapsed;
                    info!(
                        sla = %def.name,
                        duration_secs = elapsed,
                        "SLA breach resolved"
                    );
                }
            }
        }
    }

    /// Get the current status of a specific SLA by name.
    ///
    /// Returns `None` if no check has been performed yet for this SLA.
    pub fn status(&self, name: &str) -> Option<SlaStatus> {
        self.status_cache.get(name).map(|r| r.value().clone())
    }

    /// Get the current status of all monitored SLAs.
    pub fn all_statuses(&self) -> Vec<SlaStatus> {
        self.status_cache.iter().map(|r| r.value().clone()).collect()
    }

    /// Generate a retrospective compliance report for a given SLA.
    ///
    /// `hours` specifies how far back to report.
    pub fn generate_report(&self, name: &str, hours: u32) -> Option<SlaReport> {
        let def = self.sla_store.get(name)?;
        let now = Utc::now();
        let period_start = now - ChronoDuration::hours(hours as i64);

        // Collect data points within the period.
        let points = self.data_points.get(name)?;
        let period_points: Vec<&TimestampedValue> = points
            .iter()
            .filter(|p| p.timestamp >= period_start)
            .collect();

        if period_points.is_empty() {
            return Some(SlaReport {
                name: def.name.clone(),
                period_start,
                period_end: now,
                target: def.target,
                achieved: def.target,
                compliance_met: true,
                total_downtime_secs: 0.0,
                breach_episodes: Vec::new(),
                hourly_values: Vec::new(),
            });
        }

        // Calculate achieved value (average).
        let sum: f64 = period_points.iter().map(|p| p.value).sum();
        let achieved = sum / period_points.len() as f64;

        // Determine compliance.
        let compliance_met = def.is_compliant(achieved);

        // Calculate downtime.
        let non_compliant = period_points
            .iter()
            .filter(|p| !def.is_compliant(p.value))
            .count();
        let total_downtime_secs = non_compliant as f64 * SLA_CHECK_INTERVAL.as_secs_f64();

        // Collect breach episodes from history.
        let breach_episodes = self
            .breach_history
            .get(name)
            .map(|h| {
                h.iter()
                    .filter(|ep| ep.started_at >= period_start)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        // Build hourly values by bucketing data points.
        let mut hourly_map: HashMap<i64, Vec<f64>> = HashMap::new();
        for p in &period_points {
            let hour_ts = p.timestamp.timestamp() / 3600 * 3600;
            hourly_map
                .entry(hour_ts * 1000) // milliseconds
                .or_default()
                .push(p.value);
        }

        let mut hourly_values: Vec<(i64, f64)> = hourly_map
            .into_iter()
            .map(|(ts, values)| {
                let avg = values.iter().sum::<f64>() / values.len() as f64;
                (ts, avg)
            })
            .collect();
        hourly_values.sort_by_key(|&(ts, _)| ts);

        Some(SlaReport {
            name: def.name.clone(),
            period_start,
            period_end: now,
            target: def.target,
            achieved,
            compliance_met,
            total_downtime_secs,
            breach_episodes,
            hourly_values,
        })
    }

    /// Compute aggregate health across all SLAs.
    pub fn overall_health(&self) -> SlaOverallHealth {
        let statuses = self.all_statuses();
        let total = statuses.len() as u32;
        let in_breach = statuses.iter().filter(|s| s.in_breach).count() as u32;
        let in_compliance = total.saturating_sub(in_breach);

        let lowest_compliance = statuses
            .iter()
            .map(|s| s.compliance_pct)
            .fold(f64::MAX, f64::min);

        let highest_burn_rate = statuses
            .iter()
            .map(|s| s.burn_rate)
            .fold(0.0_f64, f64::max);

        SlaOverallHealth {
            total_slas: total,
            in_compliance,
            in_breach,
            lowest_compliance: if total == 0 { 100.0 } else { lowest_compliance },
            highest_burn_rate: if total == 0 { 0.0 } else { highest_burn_rate },
        }
    }

    /// Get breach history for a given SLA.
    pub fn breach_history(&self, name: &str) -> Vec<SlaBreachEpisode> {
        self.breach_history
            .get(name)
            .map(|h| h.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get the number of data points stored for a given SLA.
    pub fn data_point_count(&self, name: &str) -> usize {
        self.data_points
            .get(name)
            .map(|p| p.len())
            .unwrap_or(0)
    }

    /// Check whether any SLA has a burn rate exceeding the fast-burn threshold.
    pub fn has_fast_burn_alert(&self) -> bool {
        self.status_cache
            .iter()
            .any(|r| r.value().burn_rate >= BURN_RATE_FAST_THRESHOLD)
    }

    /// Check whether any SLA has a burn rate exceeding the slow-burn threshold.
    pub fn has_slow_burn_alert(&self) -> bool {
        self.status_cache
            .iter()
            .any(|r| r.value().burn_rate >= BURN_RATE_SLOW_THRESHOLD)
    }

    /// Spawn the SLA monitoring loop as a background task.
    ///
    /// The loop checks all SLAs at `SLA_CHECK_INTERVAL` until the shutdown
    /// signal is received.
    pub fn spawn_loop(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
        outbound_tx: tokio::sync::mpsc::Sender<(std::net::SocketAddr, crate::swarm::types::SwarmMessage)>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("SLA monitor loop started");
            let outbound_tx_clone = outbound_tx.clone();

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(SLA_CHECK_INTERVAL) => {}
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            info!("SLA monitor loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown_rx.borrow() {
                    info!("SLA monitor loop shutting down");
                    break;
                }

                self.check_all();

                if let Some(ref knowledge) = self.knowledge {
                    // 🛑 THERMODYNAMIC CIVIC DUTY (Orphaned Audit Fix via Metronome)
                    // If ZKPs have been sitting in the queue for > 60 Blocks (~5 minutes) because no edge nodes 
                    // are asking for work, we must proactively force idle nodes to verify them so workers get paid.
                    let stale_zkps = knowledge.peek_stale_zkps(60);
                    if !stale_zkps.is_empty() {
                        let mut nodes: Vec<_> = knowledge.get_all_nodes().into_iter()
                            .filter(|n| n.status == crate::swarm::types::NodeStatus::Alive && n.address.is_some())
                            .collect();
                            
                        if !nodes.is_empty() {
                            use rand::seq::SliceRandom;
                            let mut rng = rand::thread_rng();
                            
                            for (chunk_id, job_id, proof) in stale_zkps {
                                if let Some(node) = nodes.choose(&mut rng) {
                                    tracing::warn!("⚖️ CIVIC DUTY: Proactively pushing orphaned ZKP (Chunk {}) to Node {} for audit.", chunk_id.0, node.node_id.0);
                                    let _ = outbound_tx_clone.try_send((node.address.unwrap(), crate::swarm::types::SwarmMessage::VerifyThisProof {
                                        chunk_id,
                                        job_id,
                                        proof,
                                    }));
                                }
                            }
                        } else {
                            // No one is alive to audit. Put them back in the queue.
                            for (c, j, p) in stale_zkps {
                                knowledge.add_unverified_zkp(c, j, p);
                            }
                        }
                    }

                    // 🛑 DATA AVAILABILITY SAMPLING (DAS) TRIGGER
                    // We look at chunks that have been reported as Completed
                    let chunks: Vec<_> = knowledge.get_all_pending_settlement_claims().into_iter()
                        .filter(|(_, msg)| matches!(msg, crate::swarm::types::SwarmMessage::ChunkResult { .. }))
                        .collect();
                    
                    if !chunks.is_empty() {
                        use rand::seq::SliceRandom;
                        use rand::{thread_rng, Rng};
                        
                        let mut rng = thread_rng();
                        if let Some((_, msg)) = chunks.choose(&mut rng) {
                            if let crate::swarm::types::SwarmMessage::ChunkResult { result, from, .. } = msg {
                                if let Some(blob_hash) = result.output_blob_hash.clone() {
                                    if let Some(target_node) = knowledge.get_node(from) {
                                        if let Some(addr) = target_node.address {
                                            let offset = rng.gen_range(0..1024); // Random byte offset
                                            let length = 1024; // 1KB sample
                                            
                                            tracing::info!(
                                                target = %from.0,
                                                hash = %crate::swarm::blobstore::hash_hex(&blob_hash),
                                                "🔍 DATA AVAILABILITY SAMPLING: Issuing challenge to storage node."
                                            );
                                            
                                            let _ = outbound_tx_clone.try_send((addr, crate::swarm::types::SwarmMessage::ProveBlobAvailability {
                                                hash: blob_hash,
                                                offset,
                                                length,
                                                reply_to: knowledge.self_id(),
                                            }));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Log fast-burn alerts.
                for entry in self.status_cache.iter() {
                    let status = entry.value();
                    if status.burn_rate >= BURN_RATE_FAST_THRESHOLD {
                        warn!(
                            sla = %status.name,
                            burn_rate = status.burn_rate,
                            budget_remaining = status.error_budget_remaining,
                            "FAST BURN: error budget being consumed rapidly"
                        );
                    } else if status.burn_rate >= BURN_RATE_SLOW_THRESHOLD {
                        debug!(
                            sla = %status.name,
                            burn_rate = status.burn_rate,
                            "slow burn: error budget consumption above sustainable rate"
                        );
                    }
                }
            }
        })
    }

    /// Clear all cached status, breach history, and data points.
    ///
    /// Useful for testing or when SLA definitions are bulk-reloaded.
    pub fn clear(&self) {
        self.status_cache.clear();
        self.breach_history.clear();
        self.data_points.clear();
        self.prev_burn_rates.clear();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // -- SlaMetric Display/FromStr --

    #[test]
    fn sla_metric_display_roundtrip() {
        let metrics = vec![
            SlaMetric::Availability,
            SlaMetric::ChunkSuccessRate,
            SlaMetric::ChunkLatencyP99Ms,
            SlaMetric::ChunkLatencyP95Ms,
            SlaMetric::JobCompletionRate,
            SlaMetric::GossipConvergenceSecs,
            SlaMetric::ApiLatencyP99Ms,
            SlaMetric::EventDeliveryLatencyMs,
            SlaMetric::DataResidencyCompliance,
        ];

        for metric in &metrics {
            let s = metric.to_string();
            let parsed: SlaMetric = s.parse().expect("should parse");
            assert_eq!(&parsed, metric, "roundtrip failed for {}", s);
        }
    }

    #[test]
    fn sla_metric_custom_display_roundtrip() {
        let metric = SlaMetric::Custom("my_custom_metric".to_string());
        let s = metric.to_string();
        assert_eq!(s, "custom:my_custom_metric");
        let parsed: SlaMetric = s.parse().expect("should parse custom");
        assert_eq!(parsed, metric);
    }

    #[test]
    fn sla_metric_from_str_unknown() {
        let result = "nonexistent_metric".parse::<SlaMetric>();
        assert!(result.is_err());
    }

    // -- SlaWindow --

    #[test]
    fn sla_window_rolling_hours() {
        let w = SlaWindow::Rolling { hours: 24 };
        assert_eq!(w.hours(), 24);
        assert_eq!(w.to_string(), "rolling_24h");
    }

    #[test]
    fn sla_window_calendar_period() {
        let w = SlaWindow::Calendar {
            period: "month".to_string(),
        };
        assert_eq!(w.hours(), 720);
        assert_eq!(w.to_string(), "calendar_month");
    }

    #[test]
    fn sla_window_calendar_unknown_period() {
        let w = SlaWindow::Calendar {
            period: "quarter".to_string(),
        };
        assert_eq!(w.hours(), 24); // fallback
    }

    // -- SlaDefinition --

    #[test]
    fn sla_definition_compliance_availability() {
        let def = SlaDefinition {
            name: "test".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::Availability,
            target: 99.9,
            window: SlaWindow::Rolling { hours: 24 },
            breach_severity: EventSeverity::Critical,
            enabled: true,
            created_at: Utc::now(),
        };
        assert!(def.is_compliant(99.95));
        assert!(def.is_compliant(99.9));
        assert!(!def.is_compliant(99.8));
    }

    #[test]
    fn sla_definition_compliance_latency() {
        let def = SlaDefinition {
            name: "test".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::ChunkLatencyP99Ms,
            target: 30000.0,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Warning,
            enabled: true,
            created_at: Utc::now(),
        };
        assert!(def.is_compliant(25000.0)); // Under target
        assert!(def.is_compliant(30000.0)); // Equal to target
        assert!(!def.is_compliant(31000.0)); // Over target
    }

    #[test]
    fn sla_definition_is_upper_bound_metric() {
        let latency_def = SlaDefinition {
            name: "test".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::ApiLatencyP99Ms,
            target: 1000.0,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Warning,
            enabled: true,
            created_at: Utc::now(),
        };
        assert!(latency_def.is_upper_bound_metric());

        let avail_def = SlaDefinition {
            name: "test2".to_string(),
            description: "test2".to_string(),
            metric: SlaMetric::Availability,
            target: 99.9,
            window: SlaWindow::Rolling { hours: 24 },
            breach_severity: EventSeverity::Critical,
            enabled: true,
            created_at: Utc::now(),
        };
        assert!(!avail_def.is_upper_bound_metric());
    }

    // -- SlaStore --

    #[test]
    fn sla_store_loads_defaults() {
        let store = SlaStore::new();
        assert_eq!(store.count(), 5);
        assert!(store.contains("swarm-availability"));
        assert!(store.contains("chunk-success-rate"));
        assert!(store.contains("chunk-latency-p99"));
        assert!(store.contains("api-latency-p99"));
        assert!(store.contains("data-residency"));
    }

    #[test]
    fn sla_store_empty() {
        let store = SlaStore::empty();
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn sla_store_upsert_and_get() {
        let store = SlaStore::empty();
        store.upsert(SlaDefinition {
            name: "custom-sla".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::Custom("test".to_string()),
            target: 95.0,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Warning,
            enabled: true,
            created_at: Utc::now(),
        });
        assert_eq!(store.count(), 1);
        let def = store.get("custom-sla");
        assert!(def.is_some());
        assert!((def.as_ref().map(|d| d.target).unwrap_or(0.0) - 95.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sla_store_remove() {
        let store = SlaStore::new();
        assert!(store.contains("swarm-availability"));
        let removed = store.remove("swarm-availability");
        assert!(removed.is_some());
        assert!(!store.contains("swarm-availability"));
        assert_eq!(store.count(), 4);
    }

    #[test]
    fn sla_store_all() {
        let store = SlaStore::new();
        let all = store.all();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn sla_store_default_targets() {
        let store = SlaStore::new();

        let avail = store.get("swarm-availability").expect("should exist");
        assert!((avail.target - 99.9).abs() < f64::EPSILON);
        assert_eq!(avail.breach_severity, EventSeverity::Critical);

        let chunk = store.get("chunk-success-rate").expect("should exist");
        assert!((chunk.target - 99.0).abs() < f64::EPSILON);
        assert_eq!(chunk.breach_severity, EventSeverity::Error);

        let latency = store.get("chunk-latency-p99").expect("should exist");
        assert!((latency.target - 30000.0).abs() < f64::EPSILON);

        let api = store.get("api-latency-p99").expect("should exist");
        assert!((api.target - 1000.0).abs() < f64::EPSILON);

        let residency = store.get("data-residency").expect("should exist");
        assert!((residency.target - 100.0).abs() < f64::EPSILON);
        assert_eq!(residency.breach_severity, EventSeverity::Critical);
    }

    // -- SlaMonitor basic --

    #[test]
    fn monitor_check_all_no_panic() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        // Should produce statuses for all 5 defaults.
        let statuses = monitor.all_statuses();
        assert_eq!(statuses.len(), 5);
    }

    #[test]
    fn monitor_status_after_check() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        let status = monitor.status("swarm-availability");
        assert!(status.is_some());
        let status = status.expect("should have status");
        assert_eq!(status.name, "swarm-availability");
        assert!((status.target - 99.9).abs() < f64::EPSILON);
    }

    #[test]
    fn monitor_default_availability_is_compliant() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        let status = monitor.status("swarm-availability").expect("should exist");
        // Without knowledge store, measure returns 100.0 which is >= 99.9
        assert!(!status.in_breach, "should not be in breach with default 100.0");
    }

    #[test]
    fn monitor_data_points_recorded() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        assert!(
            monitor.data_point_count("swarm-availability") > 0,
            "should have data points after check"
        );
    }

    #[test]
    fn monitor_overall_health_defaults() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        let health = monitor.overall_health();
        assert_eq!(health.total_slas, 5);
        // With defaults (no actual sources), most should be compliant.
        assert!(health.in_compliance > 0);
    }

    #[test]
    fn monitor_overall_health_empty() {
        let store = Arc::new(SlaStore::empty());
        let monitor = SlaMonitor::new(store);
        let health = monitor.overall_health();
        assert_eq!(health.total_slas, 0);
        assert_eq!(health.in_compliance, 0);
        assert_eq!(health.in_breach, 0);
        assert!((health.lowest_compliance - 100.0).abs() < f64::EPSILON);
        assert!((health.highest_burn_rate - 0.0).abs() < f64::EPSILON);
    }

    // -- SlaMonitor with metrics --

    #[test]
    fn monitor_with_metrics_measures_chunk_success_rate() {
        let metrics = Arc::new(super::super::metrics::SwarmMetrics::new());
        metrics.record_chunk_execution(true, "shell", 1.0);
        metrics.record_chunk_execution(true, "shell", 1.0);
        metrics.record_chunk_execution(false, "shell", 1.0);

        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store).with_metrics(metrics);

        let value = monitor.measure(&SlaMetric::ChunkSuccessRate);
        // 2 success / 3 total = 66.67%
        assert!(
            (value - 66.666).abs() < 1.0,
            "expected ~66.67, got {}",
            value
        );
    }

    #[test]
    fn monitor_with_metrics_chunk_success_all_success() {
        let metrics = Arc::new(super::super::metrics::SwarmMetrics::new());
        for _ in 0..10 {
            metrics.record_chunk_execution(true, "python", 1.0);
        }

        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store).with_metrics(metrics);

        let value = monitor.measure(&SlaMetric::ChunkSuccessRate);
        assert!((value - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn monitor_with_metrics_chunk_success_no_data() {
        let metrics = Arc::new(super::super::metrics::SwarmMetrics::new());
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store).with_metrics(metrics);

        let value = monitor.measure(&SlaMetric::ChunkSuccessRate);
        assert!((value - 100.0).abs() < f64::EPSILON); // default when no data
    }

    // -- Breach detection --

    #[test]
    fn monitor_detects_breach_and_resolution() {
        let store = Arc::new(SlaStore::empty());
        store.upsert(SlaDefinition {
            name: "test-avail".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::Availability,
            target: 99.9,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Critical,
            enabled: true,
            created_at: Utc::now(),
        });

        // Without knowledge store, measure returns 100.0 (compliant).
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        let status = monitor.status("test-avail").expect("should exist");
        assert!(!status.in_breach);

        // Force a breach by manipulating data points directly and re-checking.
        // We simulate this by inserting a custom SLA that will measure Custom metric
        // which returns 0.0 (non-compliant for a lower-bound target of 99.9).
        let store2 = Arc::new(SlaStore::empty());
        store2.upsert(SlaDefinition {
            name: "custom-breach".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::Custom("always_zero".to_string()),
            target: 50.0, // Custom metric returns 0.0, so this will breach
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Error,
            enabled: true,
            created_at: Utc::now(),
        });
        let monitor2 = SlaMonitor::new(store2);
        monitor2.check_all();
        let status2 = monitor2.status("custom-breach").expect("should exist");
        assert!(status2.in_breach, "custom metric returning 0.0 should breach target 50.0");
    }

    #[test]
    fn monitor_breach_history_recorded() {
        let store = Arc::new(SlaStore::empty());
        store.upsert(SlaDefinition {
            name: "breach-test".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::Custom("zero_metric".to_string()),
            target: 50.0,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Warning,
            enabled: true,
            created_at: Utc::now(),
        });

        let monitor = SlaMonitor::new(store);
        monitor.check_all();

        let history = monitor.breach_history("breach-test");
        assert_eq!(history.len(), 1, "should have one breach episode");
        assert!(history[0].ended_at.is_none(), "breach should be ongoing");
    }

    #[test]
    fn monitor_breach_episode_worst_value() {
        let store = Arc::new(SlaStore::empty());
        store.upsert(SlaDefinition {
            name: "worst-test".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::Custom("zero".to_string()),
            target: 50.0,
            window: SlaWindow::Rolling { hours: 1 },
            breach_severity: EventSeverity::Warning,
            enabled: true,
            created_at: Utc::now(),
        });

        let monitor = SlaMonitor::new(store);
        // Check multiple times (still in breach each time)
        monitor.check_all();
        monitor.check_all();
        monitor.check_all();

        let history = monitor.breach_history("worst-test");
        assert_eq!(history.len(), 1, "should be one continuous episode");
        assert!((history[0].worst_value - 0.0).abs() < f64::EPSILON);
    }

    // -- Burn rate --

    #[test]
    fn monitor_no_burn_alerts_when_compliant() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        assert!(!monitor.has_fast_burn_alert());
        assert!(!monitor.has_slow_burn_alert());
    }

    // -- Report generation --

    #[test]
    fn monitor_generate_report_basic() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        monitor.check_all();

        let report = monitor.generate_report("swarm-availability", 1);
        assert!(report.is_some());
        let report = report.expect("should have report");
        assert_eq!(report.name, "swarm-availability");
        assert!((report.target - 99.9).abs() < f64::EPSILON);
        assert!(report.compliance_met || !report.breach_episodes.is_empty());
    }

    #[test]
    fn monitor_generate_report_nonexistent_sla() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        let report = monitor.generate_report("nonexistent", 1);
        assert!(report.is_none());
    }

    #[test]
    fn monitor_generate_report_no_data_points() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        // Without check_all(), no SLA definitions are registered, so
        // generate_report correctly returns None.
        let report = monitor.generate_report("swarm-availability", 1);
        assert!(report.is_none(), "no SLAs registered => None");
    }

    // -- Clear --

    #[test]
    fn monitor_clear_resets_all() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        assert!(!monitor.all_statuses().is_empty());

        monitor.clear();
        assert!(monitor.all_statuses().is_empty());
        assert_eq!(monitor.data_point_count("swarm-availability"), 0);
    }

    // -- Disabled SLAs --

    #[test]
    fn monitor_skips_disabled_slas() {
        let store = Arc::new(SlaStore::empty());
        store.upsert(SlaDefinition {
            name: "disabled-sla".to_string(),
            description: "test".to_string(),
            metric: SlaMetric::Availability,
            target: 99.9,
            window: SlaWindow::Rolling { hours: 24 },
            breach_severity: EventSeverity::Warning,
            enabled: false,
            created_at: Utc::now(),
        });

        let monitor = SlaMonitor::new(store);
        monitor.check_all();
        assert!(monitor.status("disabled-sla").is_none());
    }

    // -- Measure individual metrics --

    #[test]
    fn measure_availability_default() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        let val = monitor.measure(&SlaMetric::Availability);
        assert!((val - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn measure_event_delivery_latency_default() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        let val = monitor.measure(&SlaMetric::EventDeliveryLatencyMs);
        assert!((val - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn measure_custom_metric_default() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        let val = monitor.measure(&SlaMetric::Custom("anything".to_string()));
        assert!((val - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn measure_data_residency_default() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        let val = monitor.measure(&SlaMetric::DataResidencyCompliance);
        assert!((val - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn measure_job_completion_rate_default() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        let val = monitor.measure(&SlaMetric::JobCompletionRate);
        assert!((val - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn measure_gossip_convergence_default() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        let val = monitor.measure(&SlaMetric::GossipConvergenceSecs);
        assert!((val - 1.0).abs() < f64::EPSILON);
    }

    // -- Multiple checks accumulate data points --

    #[test]
    fn multiple_checks_accumulate_data() {
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store);
        for _ in 0..5 {
            monitor.check_all();
        }
        assert_eq!(monitor.data_point_count("swarm-availability"), 5);
    }

    // -- Serialization --

    #[test]
    fn sla_definition_serialization_roundtrip() {
        let def = SlaDefinition {
            name: "test-sla".to_string(),
            description: "A test SLA".to_string(),
            metric: SlaMetric::Availability,
            target: 99.5,
            window: SlaWindow::Rolling { hours: 12 },
            breach_severity: EventSeverity::Error,
            enabled: true,
            created_at: Utc::now(),
        };

        let json = serde_json::to_string(&def).expect("serialize");
        let parsed: SlaDefinition = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.name, "test-sla");
        assert!((parsed.target - 99.5).abs() < f64::EPSILON);
    }

    #[test]
    fn sla_status_serialization() {
        let status = SlaStatus {
            name: "test".to_string(),
            current_value: 99.95,
            target: 99.9,
            compliance_pct: 100.0,
            in_breach: false,
            error_budget_remaining: 1.0,
            burn_rate: 0.0,
            burn_rate_trend: 0.0,
            time_in_breach_secs: 0.0,
            breach_count: 0,
            last_breach_at: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            data_points: 10,
        };

        let json = serde_json::to_string(&status).expect("serialize");
        let parsed: SlaStatus = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.name, "test");
        assert!(!parsed.in_breach);
    }

    #[test]
    fn sla_breach_episode_serialization() {
        let episode = SlaBreachEpisode {
            started_at: Utc::now(),
            ended_at: Some(Utc::now()),
            duration_secs: 120.0,
            worst_value: 98.5,
            root_cause: "node failures".to_string(),
        };

        let json = serde_json::to_string(&episode).expect("serialize");
        let parsed: SlaBreachEpisode = serde_json::from_str(&json).expect("deserialize");
        assert!((parsed.duration_secs - 120.0).abs() < f64::EPSILON);
        assert_eq!(parsed.root_cause, "node failures");
    }

    #[test]
    fn sla_report_serialization() {
        let report = SlaReport {
            name: "test".to_string(),
            period_start: Utc::now(),
            period_end: Utc::now(),
            target: 99.9,
            achieved: 99.95,
            compliance_met: true,
            total_downtime_secs: 0.0,
            breach_episodes: Vec::new(),
            hourly_values: vec![(1000, 99.95)],
        };

        let json = serde_json::to_string(&report).expect("serialize");
        let parsed: SlaReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.name, "test");
        assert!(parsed.compliance_met);
    }

    #[test]
    fn sla_overall_health_serialization() {
        let health = SlaOverallHealth {
            total_slas: 5,
            in_compliance: 4,
            in_breach: 1,
            lowest_compliance: 95.0,
            highest_burn_rate: 3.5,
        };

        let json = serde_json::to_string(&health).expect("serialize");
        let parsed: SlaOverallHealth = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.total_slas, 5);
        assert_eq!(parsed.in_breach, 1);
    }

    // -- Measure with metrics attached --

    #[test]
    fn measure_gossip_convergence_with_metrics() {
        let metrics = Arc::new(super::super::metrics::SwarmMetrics::new());
        metrics.partition_reachable_fraction.set(0.999);

        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store).with_metrics(metrics);

        let val = monitor.measure(&SlaMetric::GossipConvergenceSecs);
        assert!((val - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn measure_gossip_convergence_degraded() {
        let metrics = Arc::new(super::super::metrics::SwarmMetrics::new());
        metrics.partition_reachable_fraction.set(0.92);

        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store).with_metrics(metrics);

        let val = monitor.measure(&SlaMetric::GossipConvergenceSecs);
        assert!((val - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn measure_gossip_convergence_poor() {
        let metrics = Arc::new(super::super::metrics::SwarmMetrics::new());
        metrics.partition_reachable_fraction.set(0.5);

        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store).with_metrics(metrics);

        let val = monitor.measure(&SlaMetric::GossipConvergenceSecs);
        assert!((val - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn measure_data_residency_with_metrics_no_violations() {
        let metrics = Arc::new(super::super::metrics::SwarmMetrics::new());
        let store = Arc::new(SlaStore::new());
        let monitor = SlaMonitor::new(store).with_metrics(metrics);

        let val = monitor.measure(&SlaMetric::DataResidencyCompliance);
        assert!((val - 100.0).abs() < f64::EPSILON);
    }
}
