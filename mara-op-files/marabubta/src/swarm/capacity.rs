// Marabunta - Licensed under the MIT License.
//! Capacity planning, forecasting, and right-sizing for the Marabunta Swarm.
//!
//! The [`CapacityPlanner`] periodically samples resource utilization from the
//! [`KnowledgeStore`], maintains a time-series history, and derives forecasts
//! using linear regression. Operators can run what-if scenarios to evaluate
//! the impact of adding nodes, and receive right-sizing recommendations for
//! under- or over-provisioned nodes.
//!
//! # Architecture
//!
//! ```text
//!   KnowledgeStore
//!        │
//!   sample() ──► VecDeque<CapacityDataPoint>  (ring buffer, up to 86,400)
//!        │
//!   forecast() ◄── linear regression on history
//!   identify_bottlenecks() ◄── threshold checks on forecasts
//!   what_if() ◄── hypothetical resource adjustments
//!   right_size_recommendations() ◄── per-node analysis
//! ```
//!
//! The planner can optionally be wired to an `EventBus` (when it exists) to
//! emit alerts when bottlenecks are detected.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, info};

use super::knowledge::KnowledgeStore;
use super::types::NodeId;

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of data points to retain in the history ring buffer.
/// At one sample every 30 seconds, this covers 30 days.
const MAX_HISTORY_SIZE: usize = 86_400;

/// Default sample interval for the background loop.
const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

/// Utilization threshold for "imminent" bottleneck severity.
const IMMINENT_THRESHOLD: f64 = 0.95;

/// Utilization threshold for "approaching" bottleneck severity.
const APPROACHING_THRESHOLD: f64 = 0.85;

/// Utilization threshold for "potential" bottleneck severity.
const POTENTIAL_THRESHOLD: f64 = 0.75;

// ============================================================================
// CapacityDataPoint
// ============================================================================

/// A single point-in-time sample of aggregate swarm resource utilization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityDataPoint {
    /// When this sample was taken.
    pub timestamp: DateTime<Utc>,
    /// Total CPU cores across all alive nodes.
    pub total_cpu: u32,
    /// Used CPU cores (estimated from load).
    pub used_cpu: f64,
    /// Total memory across all alive nodes (MB).
    pub total_memory: u64,
    /// Used memory across all alive nodes (MB).
    pub used_memory: u64,
    /// Total disk across all alive nodes (MB).
    pub total_disk: u64,
    /// Used disk across all alive nodes (MB).
    pub used_disk: u64,
    /// Total bandwidth across all alive nodes (Mbps).
    pub total_bandwidth: f64,
    /// Estimated used bandwidth (Mbps).
    pub used_bandwidth: f64,
    /// Number of alive nodes.
    pub alive_nodes: usize,
    /// Total nodes (including suspect/dead).
    pub total_nodes: usize,
    /// Number of active jobs.
    pub active_jobs: usize,
    /// Number of pending (unassigned) chunks.
    pub pending_chunks: usize,
}

impl CapacityDataPoint {
    /// CPU utilization as a fraction (0.0 - 1.0).
    pub fn cpu_utilization(&self) -> f64 {
        if self.total_cpu == 0 {
            0.0
        } else {
            (self.used_cpu / self.total_cpu as f64).clamp(0.0, 1.0)
        }
    }

    /// Memory utilization as a fraction (0.0 - 1.0).
    pub fn memory_utilization(&self) -> f64 {
        if self.total_memory == 0 {
            0.0
        } else {
            (self.used_memory as f64 / self.total_memory as f64).clamp(0.0, 1.0)
        }
    }

    /// Disk utilization as a fraction (0.0 - 1.0).
    pub fn disk_utilization(&self) -> f64 {
        if self.total_disk == 0 {
            0.0
        } else {
            (self.used_disk as f64 / self.total_disk as f64).clamp(0.0, 1.0)
        }
    }

    /// Bandwidth utilization as a fraction (0.0 - 1.0).
    pub fn bandwidth_utilization(&self) -> f64 {
        if self.total_bandwidth <= 0.0 {
            0.0
        } else {
            (self.used_bandwidth / self.total_bandwidth).clamp(0.0, 1.0)
        }
    }
}

// ============================================================================
// Trend
// ============================================================================

/// Direction of a resource utilization trend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trend {
    /// Usage is increasing over time.
    Rising,
    /// Usage is roughly stable.
    Stable,
    /// Usage is decreasing over time.
    Falling,
}

impl fmt::Display for Trend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Trend::Rising => write!(f, "rising"),
            Trend::Stable => write!(f, "stable"),
            Trend::Falling => write!(f, "falling"),
        }
    }
}

// ============================================================================
// ResourceForecast
// ============================================================================

/// A forecast for a specific resource type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceForecast {
    /// Name of the resource ("cpu", "memory", "disk", "bandwidth").
    pub resource: String,
    /// Current utilization (0.0 - 1.0).
    pub current_utilization: f64,
    /// Projected utilization at various time horizons.
    /// Keys: "1h", "24h", "7d".
    pub projected_utilization: HashMap<String, f64>,
    /// Estimated days until exhaustion (utilization >= 1.0), or None if stable/declining.
    pub days_to_exhaustion: Option<f64>,
    /// Daily growth rate as a fraction (e.g. 0.01 = 1% per day).
    pub growth_rate_per_day: f64,
    /// Overall trend direction.
    pub trend: Trend,
    /// Confidence in the forecast (0.0 - 1.0), based on data availability.
    pub confidence: f64,
}

// ============================================================================
// BottleneckSeverity
// ============================================================================

/// Severity of an identified bottleneck.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BottleneckSeverity {
    /// Resource will be exhausted in the near future.
    Potential,
    /// Resource is approaching exhaustion.
    Approaching,
    /// Resource exhaustion is imminent.
    Imminent,
}

impl fmt::Display for BottleneckSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BottleneckSeverity::Potential => write!(f, "potential"),
            BottleneckSeverity::Approaching => write!(f, "approaching"),
            BottleneckSeverity::Imminent => write!(f, "imminent"),
        }
    }
}

// ============================================================================
// Bottleneck
// ============================================================================

/// An identified resource bottleneck.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bottleneck {
    /// Which resource is bottlenecked.
    pub resource: String,
    /// How severe the bottleneck is.
    pub severity: BottleneckSeverity,
    /// Current utilization of the resource.
    pub current_utilization: f64,
    /// Projected exhaustion timeline.
    pub projected_exhaustion: Option<f64>,
    /// Recommended action to mitigate.
    pub recommendation: String,
}

// ============================================================================
// WhatIfScenario / WhatIfResult
// ============================================================================

/// A hypothetical scenario for capacity planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhatIfScenario {
    /// Human-readable name for the scenario.
    pub name: String,
    /// Number of nodes to add (can be negative for removal).
    pub add_nodes: i32,
    /// Additional CPU cores to add.
    pub add_cpu_cores: i32,
    /// Additional memory to add (MB).
    pub add_memory_mb: i64,
    /// Additional disk to add (MB).
    pub add_disk_mb: i64,
    /// Expected job growth percentage (e.g. 50 = 50% more jobs).
    pub job_growth_pct: f64,
}

/// Result of evaluating a what-if scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhatIfResult {
    /// The scenario that was evaluated.
    pub scenario: WhatIfScenario,
    /// Projected utilization for each resource under the scenario.
    pub projected_utilization: HashMap<String, f64>,
    /// Bottlenecks that would exist under the scenario.
    pub bottlenecks: Vec<Bottleneck>,
    /// Estimated change in cost as a percentage.
    pub estimated_cost_change_pct: f64,
    /// Estimated days of headroom before exhaustion.
    pub estimated_headroom_days: Option<f64>,
}

// ============================================================================
// RiskLevel
// ============================================================================

/// Risk level for right-sizing recommendations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    /// Low risk -- recommendation can be applied safely.
    Low,
    /// Medium risk -- some workloads may be affected.
    Medium,
    /// High risk -- significant workload impact possible.
    High,
}

impl fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RiskLevel::Low => write!(f, "low"),
            RiskLevel::Medium => write!(f, "medium"),
            RiskLevel::High => write!(f, "high"),
        }
    }
}

// ============================================================================
// RightSizeRecommendation
// ============================================================================

/// A recommendation to resize a specific node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RightSizeRecommendation {
    /// Which node the recommendation is for.
    pub node_id: NodeId,
    /// Current hardware class description.
    pub current_class: String,
    /// Recommended hardware class description.
    pub recommended_class: String,
    /// Why the recommendation is being made.
    pub reason: String,
    /// Estimated savings (negative = more expensive).
    pub savings_estimate: f64,
    /// Risk level of applying this recommendation.
    pub risk_level: RiskLevel,
}

// ============================================================================
// Linear regression helper
// ============================================================================

/// Simple linear regression on a time-series of (timestamp, value) pairs.
///
/// Returns `(slope, intercept, r_squared)` where slope is the rate of change
/// per second, intercept is the value at t=0 (of the normalized time series),
/// and r_squared measures the goodness of fit.
///
/// Returns `None` if there are fewer than 2 data points.
pub fn linear_regression(data: &[(f64, f64)]) -> Option<(f64, f64, f64)> {
    let n = data.len();
    if n < 2 {
        return None;
    }

    let n_f = n as f64;
    let sum_x: f64 = data.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = data.iter().map(|(_, y)| y).sum();
    let sum_xy: f64 = data.iter().map(|(x, y)| x * y).sum();
    let sum_xx: f64 = data.iter().map(|(x, _)| x * x).sum();
    let sum_yy: f64 = data.iter().map(|(_, y)| y * y).sum();

    let denom = n_f * sum_xx - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        return Some((0.0, sum_y / n_f, 0.0));
    }

    let slope = (n_f * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n_f;

    // Compute R-squared.
    let ss_tot = sum_yy - sum_y * sum_y / n_f;
    let ss_res: f64 = data
        .iter()
        .map(|(x, y)| {
            let predicted = slope * x + intercept;
            (y - predicted) * (y - predicted)
        })
        .sum();

    let r_squared = if ss_tot.abs() < f64::EPSILON {
        1.0
    } else {
        1.0 - ss_res / ss_tot
    };

    Some((slope, intercept, r_squared.clamp(0.0, 1.0)))
}

// ============================================================================
// CapacityPlanner
// ============================================================================

/// Capacity planner that samples, forecasts, and recommends resource changes.
///
/// The planner maintains a ring buffer of [`CapacityDataPoint`] samples and
/// performs linear regression to project future utilization. It can be run
/// as a background loop via [`spawn_loop`](Self::spawn_loop).
pub struct CapacityPlanner {
    /// Time-series history of capacity samples.
    history: parking_lot::Mutex<VecDeque<CapacityDataPoint>>,
    /// Reference to the knowledge store for sampling.
    knowledge: Option<Arc<KnowledgeStore>>,
    /// Sample interval for the background loop.
    sample_interval: Duration,
    /// Maximum history size.
    max_history: usize,
}

impl CapacityPlanner {
    /// Create a new capacity planner.
    pub fn new() -> Self {
        Self {
            history: parking_lot::Mutex::new(VecDeque::new()),
            knowledge: None,
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            max_history: MAX_HISTORY_SIZE,
        }
    }

    /// Create a new capacity planner with a knowledge store.
    pub fn with_knowledge(mut self, knowledge: Arc<KnowledgeStore>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// Set a custom sample interval.
    pub fn with_sample_interval(mut self, interval: Duration) -> Self {
        self.sample_interval = interval;
        self
    }

    /// Set a custom maximum history size.
    pub fn with_max_history(mut self, max: usize) -> Self {
        self.max_history = max;
        self
    }

    /// Sample current resource utilization from the knowledge store and add
    /// the data point to the history.
    ///
    /// If no knowledge store is configured, this is a no-op and returns `None`.
    pub fn sample(&self) -> Option<CapacityDataPoint> {
        let knowledge = self.knowledge.as_ref()?;

        let all_nodes = knowledge.get_all_nodes();
        let live_nodes = knowledge.get_live_nodes();
        let active_jobs = knowledge.get_active_jobs();
        let pending_chunks = knowledge.get_unassigned_chunks().len();

        let mut total_cpu: u32 = 0;
        let mut used_cpu: f64 = 0.0;
        let mut total_memory: u64 = 0;
        let mut used_memory: u64 = 0;
        let mut total_disk: u64 = 0;
        let mut used_disk: u64 = 0;
        let mut total_bandwidth: f64 = 0.0;

        for node in &live_nodes {
            let cap = &node.capacity;
            total_cpu += cap.cpu_cores;
            // cpu_available is 0.0=fully loaded, 1.0=idle, so used = cores * (1 - available).
            used_cpu += cap.cpu_cores as f64 * (1.0 - cap.cpu_available as f64);
            total_memory += cap.memory_total_mb;
            used_memory += cap.memory_total_mb.saturating_sub(cap.memory_available_mb);
            total_disk += cap.disk_total_mb;
            used_disk += cap.disk_total_mb.saturating_sub(cap.disk_available_mb);
            total_bandwidth += cap.network_bandwidth_mbps as f64;
        }

        // Estimate used bandwidth as proportional to load.
        let avg_load = if live_nodes.is_empty() {
            0.0
        } else {
            live_nodes.iter().map(|n| n.load as f64).sum::<f64>() / live_nodes.len() as f64
        };
        let used_bandwidth = total_bandwidth * avg_load;

        let data_point = CapacityDataPoint {
            timestamp: Utc::now(),
            total_cpu,
            used_cpu,
            total_memory,
            used_memory,
            total_disk,
            used_disk,
            total_bandwidth,
            used_bandwidth,
            alive_nodes: live_nodes.len(),
            total_nodes: all_nodes.len(),
            active_jobs: active_jobs.len(),
            pending_chunks,
        };

        let mut history = self.history.lock();
        history.push_back(data_point.clone());
        while history.len() > self.max_history {
            history.pop_front();
        }

        Some(data_point)
    }

    /// Add a data point directly to the history (for testing or external data).
    pub fn add_data_point(&self, point: CapacityDataPoint) {
        let mut history = self.history.lock();
        history.push_back(point);
        while history.len() > self.max_history {
            history.pop_front();
        }
    }

    /// Forecast a specific resource using linear regression on the history.
    ///
    /// Returns `None` if there is insufficient history.
    pub fn forecast(&self, resource: &str) -> Option<ResourceForecast> {
        let history = self.history.lock();
        if history.len() < 2 {
            return None;
        }

        // Extract the utilization time series for the requested resource.
        let base_time = history.front().map(|p| p.timestamp)?;
        let series: Vec<(f64, f64)> = history
            .iter()
            .map(|p| {
                let t = (p.timestamp - base_time).num_seconds() as f64;
                let u = match resource {
                    "cpu" => p.cpu_utilization(),
                    "memory" => p.memory_utilization(),
                    "disk" => p.disk_utilization(),
                    "bandwidth" => p.bandwidth_utilization(),
                    _ => 0.0,
                };
                (t, u)
            })
            .collect();

        let (slope, intercept, r_squared) = linear_regression(&series)?;

        let current_utilization = series.last().map(|(_, u)| *u).unwrap_or(0.0);

        // Project utilization at different horizons.
        let last_t = series.last().map(|(t, _)| *t).unwrap_or(0.0);
        let project = |hours: f64| -> f64 {
            let t = last_t + hours * 3600.0;
            (slope * t + intercept).clamp(0.0, 1.0)
        };

        let mut projected = HashMap::new();
        projected.insert("1h".to_string(), project(1.0));
        projected.insert("24h".to_string(), project(24.0));
        projected.insert("7d".to_string(), project(168.0));

        // Days to exhaustion.
        let days_to_exhaustion = if slope > 1e-12 {
            let remaining = 1.0 - current_utilization;
            let seconds_to_full = remaining / slope;
            let days = seconds_to_full / 86400.0;
            if days > 0.0 && days < 365.0 * 10.0 {
                Some(days)
            } else {
                None
            }
        } else {
            None
        };

        // Growth rate per day.
        let growth_rate_per_day = slope * 86400.0;

        // Trend determination.
        let trend = if growth_rate_per_day > 0.001 {
            Trend::Rising
        } else if growth_rate_per_day < -0.001 {
            Trend::Falling
        } else {
            Trend::Stable
        };

        // Confidence based on R-squared and data count.
        let data_factor = (history.len() as f64 / 100.0).min(1.0);
        let confidence = (r_squared * data_factor).clamp(0.0, 1.0);

        Some(ResourceForecast {
            resource: resource.to_string(),
            current_utilization,
            projected_utilization: projected,
            days_to_exhaustion,
            growth_rate_per_day,
            trend,
            confidence,
        })
    }

    /// Forecast all four resource types (cpu, memory, disk, bandwidth).
    pub fn all_forecasts(&self) -> Vec<ResourceForecast> {
        let resources = ["cpu", "memory", "disk", "bandwidth"];
        resources
            .iter()
            .filter_map(|r| self.forecast(r))
            .collect()
    }

    /// Identify current and projected bottlenecks across all resources.
    pub fn identify_bottlenecks(&self) -> Vec<Bottleneck> {
        let mut bottlenecks = Vec::new();

        for forecast in self.all_forecasts() {
            let max_projected = forecast
                .projected_utilization
                .values()
                .copied()
                .fold(0.0_f64, f64::max);

            let effective_util = forecast.current_utilization.max(max_projected);

            if effective_util >= IMMINENT_THRESHOLD {
                bottlenecks.push(Bottleneck {
                    resource: forecast.resource.clone(),
                    severity: BottleneckSeverity::Imminent,
                    current_utilization: forecast.current_utilization,
                    projected_exhaustion: forecast.days_to_exhaustion,
                    recommendation: format!(
                        "Immediately add {} capacity or reduce workload",
                        forecast.resource
                    ),
                });
            } else if effective_util >= APPROACHING_THRESHOLD {
                bottlenecks.push(Bottleneck {
                    resource: forecast.resource.clone(),
                    severity: BottleneckSeverity::Approaching,
                    current_utilization: forecast.current_utilization,
                    projected_exhaustion: forecast.days_to_exhaustion,
                    recommendation: format!(
                        "Plan to add {} capacity within the next week",
                        forecast.resource
                    ),
                });
            } else if effective_util >= POTENTIAL_THRESHOLD {
                bottlenecks.push(Bottleneck {
                    resource: forecast.resource.clone(),
                    severity: BottleneckSeverity::Potential,
                    current_utilization: forecast.current_utilization,
                    projected_exhaustion: forecast.days_to_exhaustion,
                    recommendation: format!(
                        "Monitor {} utilization and plan for growth",
                        forecast.resource
                    ),
                });
            }
        }

        // Sort by severity (most severe first).
        bottlenecks.sort_by(|a, b| b.severity.cmp(&a.severity));
        bottlenecks
    }

    /// Evaluate a hypothetical what-if scenario against the current state.
    pub fn what_if(&self, scenario: &WhatIfScenario) -> WhatIfResult {
        let history = self.history.lock();
        let latest = history.back();

        let (current_cpu_util, current_mem_util, current_disk_util) = match latest {
            Some(p) => (p.cpu_utilization(), p.memory_utilization(), p.disk_utilization()),
            None => (0.0, 0.0, 0.0),
        };

        let (total_cpu, total_memory, total_disk) = match latest {
            Some(p) => (p.total_cpu as f64, p.total_memory as f64, p.total_disk as f64),
            None => (1.0, 1.0, 1.0),
        };

        // Apply scenario adjustments.
        let new_total_cpu = (total_cpu + scenario.add_cpu_cores as f64).max(1.0);
        let new_total_memory = (total_memory + scenario.add_memory_mb as f64).max(1.0);
        let new_total_disk = (total_disk + scenario.add_disk_mb as f64).max(1.0);

        // Factor in job growth (increases used resources proportionally).
        let growth_factor = 1.0 + scenario.job_growth_pct / 100.0;

        let used_cpu = current_cpu_util * total_cpu * growth_factor;
        let used_memory = current_mem_util * total_memory * growth_factor;
        let used_disk = current_disk_util * total_disk * growth_factor;

        let new_cpu_util = (used_cpu / new_total_cpu).clamp(0.0, 1.0);
        let new_mem_util = (used_memory / new_total_memory).clamp(0.0, 1.0);
        let new_disk_util = (used_disk / new_total_disk).clamp(0.0, 1.0);

        let mut projected = HashMap::new();
        projected.insert("cpu".to_string(), new_cpu_util);
        projected.insert("memory".to_string(), new_mem_util);
        projected.insert("disk".to_string(), new_disk_util);

        // Check for bottlenecks in the scenario.
        let mut bottlenecks = Vec::new();
        for (resource, util) in &projected {
            if *util >= IMMINENT_THRESHOLD {
                bottlenecks.push(Bottleneck {
                    resource: resource.clone(),
                    severity: BottleneckSeverity::Imminent,
                    current_utilization: *util,
                    projected_exhaustion: None,
                    recommendation: format!("Scenario still has {} bottleneck", resource),
                });
            } else if *util >= APPROACHING_THRESHOLD {
                bottlenecks.push(Bottleneck {
                    resource: resource.clone(),
                    severity: BottleneckSeverity::Approaching,
                    current_utilization: *util,
                    projected_exhaustion: None,
                    recommendation: format!("Scenario has approaching {} bottleneck", resource),
                });
            }
        }

        // Estimate cost change (proportional to resource change).
        let resource_change_factor = new_total_cpu / total_cpu;
        let cost_change_pct = (resource_change_factor - 1.0) * 100.0;

        // Estimate headroom days based on the worst utilization.
        let worst_util = projected.values().copied().fold(0.0_f64, f64::max);
        let headroom = if worst_util < 1.0 {
            // Rough estimate: at current growth rate, days until the worst resource hits 1.0.
            let remaining = 1.0 - worst_util;
            let growth = scenario.job_growth_pct.abs().max(1.0) / 100.0;
            Some(remaining / growth * 30.0) // Scale to days assuming monthly growth
        } else {
            Some(0.0)
        };

        WhatIfResult {
            scenario: scenario.clone(),
            projected_utilization: projected,
            bottlenecks,
            estimated_cost_change_pct: cost_change_pct,
            estimated_headroom_days: headroom,
        }
    }

    /// Generate right-sizing recommendations for nodes in the swarm.
    ///
    /// Analyzes each node's load relative to its capacity and recommends
    /// up-sizing (if overloaded) or down-sizing (if underutilized).
    pub fn right_size_recommendations(&self) -> Vec<RightSizeRecommendation> {
        let knowledge = match &self.knowledge {
            Some(k) => k,
            None => return Vec::new(),
        };

        let live_nodes = knowledge.get_live_nodes();
        let mut recommendations = Vec::new();

        for node in &live_nodes {
            let cap = &node.capacity;
            let load = node.load;

            // Classify current hardware.
            let current_class = classify_hardware(cap.cpu_cores, cap.memory_total_mb);

            // Memory utilization.
            let mem_util = if cap.memory_total_mb > 0 {
                1.0 - (cap.memory_available_mb as f64 / cap.memory_total_mb as f64)
            } else {
                0.0
            };

            // CPU utilization.
            let cpu_util = 1.0 - cap.cpu_available as f64;

            // Overloaded: high load AND high resource utilization.
            if load > 0.85 && (cpu_util > 0.85 || mem_util > 0.85) {
                let recommended = if cpu_util > mem_util {
                    recommend_upsize_cpu(cap.cpu_cores)
                } else {
                    recommend_upsize_memory(cap.memory_total_mb)
                };

                recommendations.push(RightSizeRecommendation {
                    node_id: node.node_id,
                    current_class: current_class.clone(),
                    recommended_class: recommended,
                    reason: format!(
                        "Node is overloaded: load={:.0}%, cpu={:.0}%, memory={:.0}%",
                        load * 100.0,
                        cpu_util * 100.0,
                        mem_util * 100.0
                    ),
                    savings_estimate: -20.0, // Upsize costs more
                    risk_level: RiskLevel::Medium,
                });
            }
            // Underutilized: low load AND low resource utilization.
            else if load < 0.15 && cpu_util < 0.15 && mem_util < 0.15 {
                let recommended = recommend_downsize(cap.cpu_cores, cap.memory_total_mb);

                recommendations.push(RightSizeRecommendation {
                    node_id: node.node_id,
                    current_class: current_class.clone(),
                    recommended_class: recommended,
                    reason: format!(
                        "Node is underutilized: load={:.0}%, cpu={:.0}%, memory={:.0}%",
                        load * 100.0,
                        cpu_util * 100.0,
                        mem_util * 100.0
                    ),
                    savings_estimate: 30.0,
                    risk_level: RiskLevel::Low,
                });
            }
        }

        recommendations
    }

    /// Return the history for the last N hours.
    pub fn history(&self, hours: f64) -> Vec<CapacityDataPoint> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(Duration::from_secs_f64(hours * 3600.0))
                .unwrap_or_else(|_| chrono::Duration::seconds(0));
        let history = self.history.lock();
        history
            .iter()
            .filter(|p| p.timestamp >= cutoff)
            .cloned()
            .collect()
    }

    /// Return the full history.
    pub fn full_history(&self) -> Vec<CapacityDataPoint> {
        self.history.lock().iter().cloned().collect()
    }

    /// Number of data points in history.
    pub fn history_len(&self) -> usize {
        self.history.lock().len()
    }

    /// Export the history as CSV.
    pub fn export_csv(&self) -> String {
        let history = self.history.lock();
        let mut csv = String::from(
            "timestamp,total_cpu,used_cpu,total_memory,used_memory,total_disk,used_disk,\
             total_bandwidth,used_bandwidth,alive_nodes,total_nodes,active_jobs,pending_chunks\n",
        );

        for p in history.iter() {
            csv.push_str(&format!(
                "{},{},{:.2},{},{},{},{},{:.2},{:.2},{},{},{},{}\n",
                p.timestamp.to_rfc3339(),
                p.total_cpu,
                p.used_cpu,
                p.total_memory,
                p.used_memory,
                p.total_disk,
                p.used_disk,
                p.total_bandwidth,
                p.used_bandwidth,
                p.alive_nodes,
                p.total_nodes,
                p.active_jobs,
                p.pending_chunks,
            ));
        }

        csv
    }

    /// Spawn a background sampling loop.
    ///
    /// Samples at the configured interval and stops when the shutdown
    /// signal is received.
    pub fn spawn_loop(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let interval = self.sample_interval;
        tokio::spawn(async move {
            info!(
                interval_secs = interval.as_secs(),
                "capacity planner loop started"
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            info!("capacity planner loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown_rx.borrow() {
                    break;
                }

                if let Some(point) = self.sample() {
                    debug!(
                        cpu_util = format!("{:.1}%", point.cpu_utilization() * 100.0),
                        mem_util = format!("{:.1}%", point.memory_utilization() * 100.0),
                        alive_nodes = point.alive_nodes,
                        active_jobs = point.active_jobs,
                        "capacity sample recorded"
                    );
                }
            }
        })
    }
}

impl Default for CapacityPlanner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Hardware classification helpers
// ============================================================================

/// Classify hardware based on CPU cores and memory.
fn classify_hardware(cores: u32, memory_mb: u64) -> String {
    if cores >= 64 || memory_mb >= 256_000 {
        "large".to_string()
    } else if cores >= 16 || memory_mb >= 64_000 {
        "medium".to_string()
    } else if cores >= 4 || memory_mb >= 8_000 {
        "small".to_string()
    } else {
        "micro".to_string()
    }
}

/// Recommend an upsize for CPU-bound nodes.
fn recommend_upsize_cpu(current_cores: u32) -> String {
    let target = (current_cores * 2).max(4);
    format!("{}-core (from {} cores)", target, current_cores)
}

/// Recommend an upsize for memory-bound nodes.
fn recommend_upsize_memory(current_mb: u64) -> String {
    let target = (current_mb * 2).max(4096);
    format!("{} MB (from {} MB)", target, current_mb)
}

/// Recommend a downsize for underutilized nodes.
fn recommend_downsize(current_cores: u32, current_mb: u64) -> String {
    let target_cores = (current_cores / 2).max(1);
    let target_mb = (current_mb / 2).max(1024);
    format!(
        "{}-core/{} MB (from {}-core/{} MB)",
        target_cores, target_mb, current_cores, current_mb
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_data_point(
        timestamp: DateTime<Utc>,
        cpu_util: f64,
        mem_util: f64,
        disk_util: f64,
    ) -> CapacityDataPoint {
        let total_cpu = 100;
        let total_memory = 100_000_u64;
        let total_disk = 500_000_u64;

        CapacityDataPoint {
            timestamp,
            total_cpu,
            used_cpu: total_cpu as f64 * cpu_util,
            total_memory,
            used_memory: (total_memory as f64 * mem_util) as u64,
            total_disk,
            used_disk: (total_disk as f64 * disk_util) as u64,
            total_bandwidth: 1000.0,
            used_bandwidth: 500.0,
            alive_nodes: 10,
            total_nodes: 12,
            active_jobs: 5,
            pending_chunks: 3,
        }
    }

    // ---------------------------------------------------------------
    // Data point tests
    // ---------------------------------------------------------------

    #[test]
    fn test_data_point_utilization() {
        let p = make_data_point(Utc::now(), 0.5, 0.6, 0.3);
        assert!((p.cpu_utilization() - 0.5).abs() < 0.01);
        assert!((p.memory_utilization() - 0.6).abs() < 0.01);
        assert!((p.disk_utilization() - 0.3).abs() < 0.01);
        assert!((p.bandwidth_utilization() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_data_point_utilization_zero_total() {
        let p = CapacityDataPoint {
            timestamp: Utc::now(),
            total_cpu: 0,
            used_cpu: 0.0,
            total_memory: 0,
            used_memory: 0,
            total_disk: 0,
            used_disk: 0,
            total_bandwidth: 0.0,
            used_bandwidth: 0.0,
            alive_nodes: 0,
            total_nodes: 0,
            active_jobs: 0,
            pending_chunks: 0,
        };
        assert!((p.cpu_utilization() - 0.0).abs() < f64::EPSILON);
        assert!((p.memory_utilization() - 0.0).abs() < f64::EPSILON);
        assert!((p.disk_utilization() - 0.0).abs() < f64::EPSILON);
        assert!((p.bandwidth_utilization() - 0.0).abs() < f64::EPSILON);
    }

    // ---------------------------------------------------------------
    // Linear regression tests
    // ---------------------------------------------------------------

    #[test]
    fn test_linear_regression_perfect_line() {
        let data = vec![(0.0, 0.0), (1.0, 2.0), (2.0, 4.0), (3.0, 6.0)];
        let (slope, intercept, r_sq) = linear_regression(&data).expect("should succeed");
        assert!((slope - 2.0).abs() < 0.001);
        assert!(intercept.abs() < 0.001);
        assert!((r_sq - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_linear_regression_flat_line() {
        let data = vec![(0.0, 5.0), (1.0, 5.0), (2.0, 5.0)];
        let (slope, intercept, r_sq) = linear_regression(&data).expect("should succeed");
        assert!(slope.abs() < 0.001);
        assert!((intercept - 5.0).abs() < 0.001);
        // R-squared for a flat line (no variance) should be 1.0 (our special case).
        assert!((r_sq - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_linear_regression_too_few_points() {
        assert!(linear_regression(&[(0.0, 1.0)]).is_none());
        assert!(linear_regression(&[]).is_none());
    }

    #[test]
    fn test_linear_regression_negative_slope() {
        let data = vec![(0.0, 10.0), (1.0, 8.0), (2.0, 6.0), (3.0, 4.0)];
        let (slope, _, _) = linear_regression(&data).expect("should succeed");
        assert!(slope < 0.0);
        assert!((slope - (-2.0)).abs() < 0.001);
    }

    #[test]
    fn test_linear_regression_noisy_data() {
        let data = vec![
            (0.0, 1.0),
            (1.0, 2.5),
            (2.0, 3.0),
            (3.0, 3.5),
            (4.0, 5.0),
        ];
        let (slope, _, r_sq) = linear_regression(&data).expect("should succeed");
        assert!(slope > 0.0);
        assert!(r_sq > 0.8); // Should be a reasonable fit.
    }

    // ---------------------------------------------------------------
    // CapacityPlanner basic tests
    // ---------------------------------------------------------------

    #[test]
    fn test_planner_add_data_point() {
        let planner = CapacityPlanner::new();
        let point = make_data_point(Utc::now(), 0.5, 0.5, 0.5);
        planner.add_data_point(point);
        assert_eq!(planner.history_len(), 1);
    }

    #[test]
    fn test_planner_max_history() {
        let planner = CapacityPlanner::new().with_max_history(5);
        for i in 0..10 {
            let point = make_data_point(
                Utc::now() + chrono::Duration::seconds(i),
                0.5,
                0.5,
                0.5,
            );
            planner.add_data_point(point);
        }
        assert_eq!(planner.history_len(), 5);
    }

    #[test]
    fn test_planner_sample_no_knowledge() {
        let planner = CapacityPlanner::new();
        assert!(planner.sample().is_none());
    }

    // ---------------------------------------------------------------
    // Forecast tests
    // ---------------------------------------------------------------

    #[test]
    fn test_forecast_insufficient_data() {
        let planner = CapacityPlanner::new();
        planner.add_data_point(make_data_point(Utc::now(), 0.5, 0.5, 0.5));
        assert!(planner.forecast("cpu").is_none());
    }

    #[test]
    fn test_forecast_rising_cpu() {
        let planner = CapacityPlanner::new();
        let base = Utc::now();

        for i in 0..10 {
            let util = 0.3 + i as f64 * 0.05; // Rising from 30% to 75%
            let point = make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                util,
                0.4,
                0.2,
            );
            planner.add_data_point(point);
        }

        let forecast = planner.forecast("cpu").expect("should have forecast");
        assert_eq!(forecast.resource, "cpu");
        assert!(forecast.trend == Trend::Rising);
        assert!(forecast.growth_rate_per_day > 0.0);
        assert!(forecast.days_to_exhaustion.is_some());
    }

    #[test]
    fn test_forecast_stable_memory() {
        let planner = CapacityPlanner::new();
        let base = Utc::now();

        for i in 0..20 {
            let point = make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                0.5,
                0.5, // Constant 50%
                0.3,
            );
            planner.add_data_point(point);
        }

        let forecast = planner.forecast("memory").expect("should have forecast");
        assert_eq!(forecast.trend, Trend::Stable);
    }

    #[test]
    fn test_forecast_falling_disk() {
        let planner = CapacityPlanner::new();
        let base = Utc::now();

        for i in 0..10 {
            let util = 0.8 - i as f64 * 0.05; // Falling from 80% to 35%
            let point = make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                0.5,
                0.4,
                util,
            );
            planner.add_data_point(point);
        }

        let forecast = planner.forecast("disk").expect("should have forecast");
        assert_eq!(forecast.trend, Trend::Falling);
        assert!(forecast.growth_rate_per_day < 0.0);
        assert!(forecast.days_to_exhaustion.is_none());
    }

    #[test]
    fn test_all_forecasts() {
        let planner = CapacityPlanner::new();
        let base = Utc::now();

        for i in 0..5 {
            let point = make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                0.5,
                0.6,
                0.3,
            );
            planner.add_data_point(point);
        }

        let forecasts = planner.all_forecasts();
        assert_eq!(forecasts.len(), 4); // cpu, memory, disk, bandwidth
    }

    // ---------------------------------------------------------------
    // Bottleneck detection tests
    // ---------------------------------------------------------------

    #[test]
    fn test_identify_bottlenecks_none() {
        let planner = CapacityPlanner::new();
        let base = Utc::now();

        for i in 0..5 {
            let point = make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                0.3,
                0.2,
                0.1,
            );
            planner.add_data_point(point);
        }

        let bottlenecks = planner.identify_bottlenecks();
        assert!(bottlenecks.is_empty());
    }

    #[test]
    fn test_identify_bottlenecks_imminent() {
        let planner = CapacityPlanner::new();
        let base = Utc::now();

        for i in 0..5 {
            let point = make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                0.97,
                0.2,
                0.1,
            );
            planner.add_data_point(point);
        }

        let bottlenecks = planner.identify_bottlenecks();
        assert!(!bottlenecks.is_empty());
        let cpu_bottleneck = bottlenecks.iter().find(|b| b.resource == "cpu");
        assert!(cpu_bottleneck.is_some());
        assert_eq!(
            cpu_bottleneck.map(|b| b.severity),
            Some(BottleneckSeverity::Imminent)
        );
    }

    #[test]
    fn test_identify_bottlenecks_approaching() {
        let planner = CapacityPlanner::new();
        let base = Utc::now();

        for i in 0..5 {
            let point = make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                0.5,
                0.88,
                0.1,
            );
            planner.add_data_point(point);
        }

        let bottlenecks = planner.identify_bottlenecks();
        let mem_bottleneck = bottlenecks.iter().find(|b| b.resource == "memory");
        assert!(mem_bottleneck.is_some());
        assert_eq!(
            mem_bottleneck.map(|b| b.severity),
            Some(BottleneckSeverity::Approaching)
        );
    }

    // ---------------------------------------------------------------
    // What-if scenario tests
    // ---------------------------------------------------------------

    #[test]
    fn test_what_if_add_nodes() {
        let planner = CapacityPlanner::new();
        let point = make_data_point(Utc::now(), 0.8, 0.7, 0.5);
        planner.add_data_point(point);

        let scenario = WhatIfScenario {
            name: "double capacity".to_string(),
            add_nodes: 10,
            add_cpu_cores: 100, // Double the CPU
            add_memory_mb: 100_000,
            add_disk_mb: 500_000,
            job_growth_pct: 0.0,
        };

        let result = planner.what_if(&scenario);
        let cpu_util = result.projected_utilization.get("cpu").copied().unwrap_or(1.0);
        // Doubling capacity with same workload should roughly halve utilization.
        assert!(cpu_util < 0.5, "CPU util should be <50%, got {:.1}%", cpu_util * 100.0);
    }

    #[test]
    fn test_what_if_job_growth() {
        let planner = CapacityPlanner::new();
        let point = make_data_point(Utc::now(), 0.4, 0.4, 0.3);
        planner.add_data_point(point);

        let scenario = WhatIfScenario {
            name: "50% job growth".to_string(),
            add_nodes: 0,
            add_cpu_cores: 0,
            add_memory_mb: 0,
            add_disk_mb: 0,
            job_growth_pct: 50.0,
        };

        let result = planner.what_if(&scenario);
        let cpu_util = result.projected_utilization.get("cpu").copied().unwrap_or(0.0);
        // 40% * 1.5 = 60%
        assert!((cpu_util - 0.6).abs() < 0.05, "Expected ~60%, got {:.1}%", cpu_util * 100.0);
    }

    #[test]
    fn test_what_if_empty_history() {
        let planner = CapacityPlanner::new();
        let scenario = WhatIfScenario {
            name: "empty".to_string(),
            add_nodes: 0,
            add_cpu_cores: 0,
            add_memory_mb: 0,
            add_disk_mb: 0,
            job_growth_pct: 0.0,
        };

        let result = planner.what_if(&scenario);
        assert!(result.projected_utilization.contains_key("cpu"));
    }

    // ---------------------------------------------------------------
    // Right-sizing tests
    // ---------------------------------------------------------------

    #[test]
    fn test_right_size_no_knowledge() {
        let planner = CapacityPlanner::new();
        let recs = planner.right_size_recommendations();
        assert!(recs.is_empty());
    }

    // ---------------------------------------------------------------
    // Export CSV tests
    // ---------------------------------------------------------------

    #[test]
    fn test_export_csv_empty() {
        let planner = CapacityPlanner::new();
        let csv = planner.export_csv();
        assert!(csv.contains("timestamp"));
        assert!(csv.lines().count() == 1); // Header only
    }

    #[test]
    fn test_export_csv_with_data() {
        let planner = CapacityPlanner::new();
        planner.add_data_point(make_data_point(Utc::now(), 0.5, 0.5, 0.5));
        planner.add_data_point(make_data_point(Utc::now(), 0.6, 0.6, 0.6));

        let csv = planner.export_csv();
        assert_eq!(csv.lines().count(), 3); // Header + 2 rows
    }

    // ---------------------------------------------------------------
    // History query tests
    // ---------------------------------------------------------------

    #[test]
    fn test_history_filter_by_hours() {
        let planner = CapacityPlanner::new();
        let now = Utc::now();

        // Add a point from 2 hours ago.
        planner.add_data_point(make_data_point(
            now - chrono::Duration::hours(2),
            0.5,
            0.5,
            0.5,
        ));
        // Add a recent point.
        planner.add_data_point(make_data_point(now, 0.6, 0.6, 0.6));

        let recent = planner.history(1.0);
        assert_eq!(recent.len(), 1); // Only the recent one

        let all = planner.history(3.0);
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_full_history() {
        let planner = CapacityPlanner::new();
        for i in 0..5 {
            planner.add_data_point(make_data_point(
                Utc::now() + chrono::Duration::seconds(i),
                0.5,
                0.5,
                0.5,
            ));
        }
        assert_eq!(planner.full_history().len(), 5);
    }

    // ---------------------------------------------------------------
    // Hardware classification tests
    // ---------------------------------------------------------------

    #[test]
    fn test_classify_hardware() {
        assert_eq!(classify_hardware(1, 512), "micro");
        assert_eq!(classify_hardware(4, 8192), "small");
        assert_eq!(classify_hardware(16, 65536), "medium");
        assert_eq!(classify_hardware(64, 262144), "large");
    }

    #[test]
    fn test_recommend_upsize_cpu() {
        let rec = recommend_upsize_cpu(4);
        assert!(rec.contains("8"));
        assert!(rec.contains("4"));
    }

    #[test]
    fn test_recommend_downsize() {
        let rec = recommend_downsize(8, 16384);
        assert!(rec.contains("4"));
        assert!(rec.contains("8192"));
    }

    // ---------------------------------------------------------------
    // Trend display
    // ---------------------------------------------------------------

    #[test]
    fn test_trend_display() {
        assert_eq!(format!("{}", Trend::Rising), "rising");
        assert_eq!(format!("{}", Trend::Stable), "stable");
        assert_eq!(format!("{}", Trend::Falling), "falling");
    }

    // ---------------------------------------------------------------
    // BottleneckSeverity display and ordering
    // ---------------------------------------------------------------

    #[test]
    fn test_bottleneck_severity_display() {
        assert_eq!(format!("{}", BottleneckSeverity::Imminent), "imminent");
        assert_eq!(format!("{}", BottleneckSeverity::Approaching), "approaching");
        assert_eq!(format!("{}", BottleneckSeverity::Potential), "potential");
    }

    #[test]
    fn test_bottleneck_severity_ordering() {
        assert!(BottleneckSeverity::Imminent > BottleneckSeverity::Approaching);
        assert!(BottleneckSeverity::Approaching > BottleneckSeverity::Potential);
    }

    // ---------------------------------------------------------------
    // RiskLevel display
    // ---------------------------------------------------------------

    #[test]
    fn test_risk_level_display() {
        assert_eq!(format!("{}", RiskLevel::Low), "low");
        assert_eq!(format!("{}", RiskLevel::Medium), "medium");
        assert_eq!(format!("{}", RiskLevel::High), "high");
    }

    // ---------------------------------------------------------------
    // Planner default
    // ---------------------------------------------------------------

    #[test]
    fn test_planner_default() {
        let planner = CapacityPlanner::default();
        assert_eq!(planner.history_len(), 0);
    }

    // ---------------------------------------------------------------
    // Forecast confidence scales with data
    // ---------------------------------------------------------------

    #[test]
    fn test_forecast_confidence_scales() {
        let planner = CapacityPlanner::new();
        let base = Utc::now();

        // Few data points.
        for i in 0..5 {
            planner.add_data_point(make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                0.5,
                0.5,
                0.5,
            ));
        }

        let forecast_small = planner.forecast("cpu").expect("should have forecast");

        // Many data points.
        for i in 5..200 {
            planner.add_data_point(make_data_point(
                base + chrono::Duration::seconds(i * 3600),
                0.5,
                0.5,
                0.5,
            ));
        }

        let forecast_large = planner.forecast("cpu").expect("should have forecast");

        // More data should not decrease confidence for a flat line.
        assert!(forecast_large.confidence >= forecast_small.confidence);
    }

    // ---------------------------------------------------------------
    // What-if cost change
    // ---------------------------------------------------------------

    #[test]
    fn test_what_if_cost_change() {
        let planner = CapacityPlanner::new();
        planner.add_data_point(make_data_point(Utc::now(), 0.5, 0.5, 0.5));

        let scenario = WhatIfScenario {
            name: "double cpu".to_string(),
            add_nodes: 0,
            add_cpu_cores: 100, // Double the 100 base
            add_memory_mb: 0,
            add_disk_mb: 0,
            job_growth_pct: 0.0,
        };

        let result = planner.what_if(&scenario);
        // Doubling CPU from 100 to 200 => 100% cost increase.
        assert!(result.estimated_cost_change_pct > 0.0);
    }
}
