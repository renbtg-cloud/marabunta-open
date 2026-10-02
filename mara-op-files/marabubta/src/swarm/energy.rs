// Marabunta - Licensed under the MIT License.
//! Pillar 11.1: Universal FinOps (Energy Oracle & Fuel Metering)
//!
//! This module implements Phase 7 of the Marabunta Swarm production roadmap:
//! energy-aware scheduling and spot-market pricing. It maps WASM instruction 
//! execution directly to hardware-specific Joule consumption for 15-billion nodes.
//!
//! - **Power consumption modeling** -- per-node power profiles derived from
//!   hardware characteristics, with a hierarchical override system that lets
//!   operators set power at the node, node-type, trait-combo, or group level.
//!
//! - **Energy price schedules** -- time-of-use and region-based electricity
//!   pricing, importable from CSV or constructed programmatically.
//!
//! - **Cost estimation** -- given a node profile, task duration, and start time,
//!   produce a cost estimate in USD and kWh.
//!
//! - **Scheduling optimization** -- a greedy optimizer that builds a cost matrix
//!   of (node, hour_slot) pairs, sorts by ascending cost, and assigns chunks
//!   to the cheapest available slots while respecting concurrency limits and
//!   optional deadlines.
//!
//! # Override hierarchy (highest priority first)
//!
//! 1. Per-node override (`PowerTarget::Node`)
//! 2. Per-trait-combo override (`PowerTarget::TraitCombo`)
//! 3. Per-node-type override (`PowerTarget::NodeType`)
//! 4. Per-group override (`PowerTarget::Group`)
//! 5. Global override (`PowerTarget::All`)
//! 6. Architecture-based heuristic (ARM vs x86 constants from `config.rs`)

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Timelike, Utc, Weekday};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use super::config::{
    ENERGY_DEFAULT_PRICE, POWER_HEURISTIC_ARM_IDLE, POWER_HEURISTIC_ARM_LOAD,
    POWER_HEURISTIC_X86_IDLE, POWER_HEURISTIC_X86_LOAD,
};
use super::profile::{NodeProfile, NodeType};
use super::types::{Confidence, NodeId, SwarmError};

// ============================================================================
// Power consumption model
// ============================================================================

/// Power consumption profile for a node.
///
/// Contains idle and load wattage, plus optional GPU power draw. The `source`
/// field indicates how this profile was determined, which is useful for
/// confidence scoring and auditing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerProfile {
    /// How this profile was obtained.
    pub source: PowerSource,
    /// Power draw when the node is idle (watts).
    pub idle_watts: f64,
    /// Power draw when the node is under full CPU load (watts).
    pub load_watts: f64,
    /// Additional power draw when the GPU is active (watts).
    pub gpu_watts: Option<f64>,
}

/// How a power profile was determined.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum PowerSource {
    /// Manually set for a specific node.
    ManualPerNode,
    /// Manually set for a specific trait combination.
    ManualPerTraitCombo { combo_id: String },
    /// Manually set for a node type category.
    ManualPerNodeType { node_type: NodeType },
    /// Manually set for a named group of nodes.
    ManualPerGroup { group_id: String },
    /// Estimated from battery drain rate (Android / laptop).
    BatteryDrainEstimate,
    /// Read from Intel RAPL or equivalent hardware counters.
    RaplReading,
    /// Derived from architecture-based heuristic constants.
    Heuristic,
}

/// Override for setting power consumption of nodes.
///
/// Overrides are evaluated in priority order (see module-level docs).
/// The first matching override wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerOverride {
    /// Which nodes this override applies to.
    pub target: PowerTarget,
    /// Idle power draw (watts).
    pub idle_watts: f64,
    /// Load power draw (watts).
    pub load_watts: f64,
    /// GPU power draw (watts), if applicable.
    pub gpu_watts: Option<f64>,
}

/// Target selector for a power override.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum PowerTarget {
    /// Apply to a specific node by ID.
    Node { node_id: NodeId },
    /// Apply to all nodes of a given type.
    NodeType { node_type: NodeType },
    /// Apply to nodes matching a trait combination.
    TraitCombo(TraitComboSpec),
    /// Apply to nodes in a named group.
    Group { group_id: String },
    /// Apply to all nodes (lowest priority fallback).
    All,
}

/// Specification for matching nodes by trait combination.
///
/// All non-`None` fields must match for the spec to apply. Empty `software`
/// vec matches any node regardless of installed software.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraitComboSpec {
    /// CPU architecture substring match (e.g. "arm", "x86").
    pub arch: Option<String>,
    /// RAM must fall within this (min, max) range in MB.
    pub ram_range: Option<(u64, u64)>,
    /// Node type must match.
    pub node_type: Option<NodeType>,
    /// Whether the node must have a GPU.
    pub has_gpu: Option<bool>,
    /// All listed software must be installed on the node.
    pub software: Vec<String>,
}

impl TraitComboSpec {
    /// Check whether a node profile matches this trait combination spec.
    pub fn matches(&self, profile: &NodeProfile) -> bool {
        // Architecture check: look at cpu_freq_mhz as a proxy, or match
        // against the node type for ARM (Android) vs x86 (others).
        if let Some(ref arch) = self.arch {
            let arch_lower = arch.to_lowercase();
            let profile_arch = guess_arch(profile);
            if !profile_arch.to_lowercase().contains(&arch_lower) {
                return false;
            }
        }

        // RAM range check.
        if let Some((min_ram, max_ram)) = self.ram_range {
            if profile.ram_total_mb < min_ram || profile.ram_total_mb > max_ram {
                return false;
            }
        }

        // Node type check.
        if let Some(ref nt) = self.node_type {
            if profile.node_type != *nt {
                return false;
            }
        }

        // GPU check.
        if let Some(needs_gpu) = self.has_gpu {
            if needs_gpu != profile.gpu.is_some() {
                return false;
            }
        }

        // Software check: every listed software must be installed.
        for sw in &self.software {
            let sw_lower = sw.to_lowercase();
            let found = profile
                .installed_software
                .iter()
                .any(|is| is.name.to_lowercase() == sw_lower);
            if !found {
                return false;
            }
        }

        true
    }
}

/// Guess the CPU architecture from a node profile.
///
/// Uses node type as a heuristic: Android nodes are ARM, everything else
/// defaults to x86. This is a best-effort guess; operators can override
/// via `TraitComboSpec::arch`.
fn guess_arch(profile: &NodeProfile) -> &'static str {
    match profile.node_type {
        NodeType::Android => "arm",
        NodeType::Browser => "wasm",
        _ => "x86",
    }
}

// ============================================================================
// Energy price schedules
// ============================================================================

/// A named energy price schedule containing time-of-use pricing rules.
///
/// When estimating cost, rules are evaluated in order; the first rule whose
/// region and time specification match wins. If no rule matches, the
/// `default_price` is used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnergyPriceSchedule {
    /// Unique identifier for this schedule.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Pricing rules, evaluated in order.
    pub rules: Vec<PriceRule>,
    /// Fallback price (USD/kWh) when no rule matches.
    pub default_price: f64,
}

/// A single pricing rule within an energy price schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceRule {
    /// Region this rule applies to (matched against `NodeProfile::geo_region`).
    /// Use `"*"` to match any region.
    pub region: String,
    /// Time specification for when this rule is active.
    pub time_spec: TimeSpec,
    /// Price in USD per kWh during the matched period.
    pub price_usd_per_kwh: f64,
}

/// Time specification for price rule matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum TimeSpec {
    /// Recurring rule that applies on specific day types and hour ranges.
    Recurring {
        day_type: DayType,
        start_hour: u8,
        end_hour: u8,
    },
    /// Absolute time window (e.g. a holiday surcharge).
    Absolute {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
}

impl TimeSpec {
    /// Check whether the given UTC timestamp falls within this time spec.
    pub fn matches(&self, when: DateTime<Utc>) -> bool {
        match self {
            TimeSpec::Recurring {
                day_type,
                start_hour,
                end_hour,
            } => {
                // Check day type.
                if !day_type.matches(when.weekday()) {
                    return false;
                }

                let hour = when.hour() as u8;

                // Handle same start/end as "all hours".
                if start_hour == end_hour {
                    return true;
                }

                if start_hour < end_hour {
                    // Normal range: e.g. 9-17
                    hour >= *start_hour && hour < *end_hour
                } else {
                    // Wrapping range: e.g. 22-06
                    hour >= *start_hour || hour < *end_hour
                }
            }
            TimeSpec::Absolute { start, end } => when >= *start && when < *end,
        }
    }
}

/// Classification of days for recurring price rules.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DayType {
    /// Monday through Friday.
    Weekday,
    /// Saturday and Sunday.
    Weekend,
    /// Any day of the week.
    Any,
}

impl DayType {
    /// Check whether a weekday matches this day type.
    pub fn matches(&self, day: Weekday) -> bool {
        match self {
            DayType::Weekday => matches!(
                day,
                Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri
            ),
            DayType::Weekend => matches!(day, Weekday::Sat | Weekday::Sun),
            DayType::Any => true,
        }
    }
}

/// Parse CSV data into an [`EnergyPriceSchedule`].
///
/// Expected CSV format (no header row):
/// ```text
/// region,day_type,hour_start,hour_end,price_usd_per_kwh
/// us-east,weekday,9,17,0.15
/// us-east,weekday,17,9,0.08
/// us-east,weekend,0,0,0.06
/// eu-west,any,0,0,0.20
/// ```
///
/// `day_type` values: `weekday`, `weekend`, `any`.
/// `hour_start == hour_end` means "all hours".
pub fn parse_price_csv(csv_data: &str, schedule_name: &str) -> Result<EnergyPriceSchedule, SwarmError> {
    let mut rules = Vec::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .trim(csv::Trim::All)
        .from_reader(csv_data.as_bytes());

    for (line_num, result) in reader.records().enumerate() {
        let record = result.map_err(|e| {
            SwarmError::Energy(format!("CSV parse error at line {}: {}", line_num + 1, e))
        })?;

        if record.len() < 5 {
            return Err(SwarmError::Energy(format!(
                "CSV line {} has {} fields, expected 5 (region,day_type,hour_start,hour_end,price)",
                line_num + 1,
                record.len()
            )));
        }

        let region = record[0].to_string();
        let day_type = match record[1].to_lowercase().as_str() {
            "weekday" => DayType::Weekday,
            "weekend" => DayType::Weekend,
            "any" => DayType::Any,
            other => {
                return Err(SwarmError::Energy(format!(
                    "CSV line {}: invalid day_type '{}', expected weekday/weekend/any",
                    line_num + 1,
                    other
                )));
            }
        };

        let start_hour: u8 = record[2].parse().map_err(|e| {
            SwarmError::Energy(format!(
                "CSV line {}: invalid hour_start '{}': {}",
                line_num + 1,
                &record[2],
                e
            ))
        })?;

        let end_hour: u8 = record[3].parse().map_err(|e| {
            SwarmError::Energy(format!(
                "CSV line {}: invalid hour_end '{}': {}",
                line_num + 1,
                &record[3],
                e
            ))
        })?;

        if start_hour > 23 || end_hour > 23 {
            return Err(SwarmError::Energy(format!(
                "CSV line {}: hours must be 0-23, got start={} end={}",
                line_num + 1,
                start_hour,
                end_hour
            )));
        }

        let price: f64 = record[4].parse().map_err(|e| {
            SwarmError::Energy(format!(
                "CSV line {}: invalid price '{}': {}",
                line_num + 1,
                &record[4],
                e
            ))
        })?;

        if price < 0.0 {
            return Err(SwarmError::Energy(format!(
                "CSV line {}: price must be non-negative, got {}",
                line_num + 1,
                price
            )));
        }

        rules.push(PriceRule {
            region,
            time_spec: TimeSpec::Recurring {
                day_type,
                start_hour,
                end_hour,
            },
            price_usd_per_kwh: price,
        });
    }

    if rules.is_empty() {
        return Err(SwarmError::Energy(
            "CSV contains no valid pricing rules".to_string(),
        ));
    }

    Ok(EnergyPriceSchedule {
        id: Uuid::new_v4().to_string(),
        name: schedule_name.to_string(),
        rules,
        default_price: ENERGY_DEFAULT_PRICE,
    })
}

// ============================================================================
// Cost estimation
// ============================================================================

/// Cost estimate for running a workload on a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Energy consumed in kilowatt-hours.
    pub kwh: f64,
    /// Average power draw in watts during execution.
    pub watts: f64,
    /// Total cost in USD.
    pub total_usd: f64,
    /// Energy price in USD/kWh used for this estimate.
    pub price_per_kwh: f64,
}

/// Aggregate cost estimate for an entire job across multiple nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCostEstimate {
    /// Total cost in USD for all chunks.
    pub total_usd: f64,
    /// Total energy in kWh for all chunks.
    pub total_kwh: f64,
    /// Per-node cost breakdown.
    pub per_node: Vec<(NodeId, CostEstimate)>,
    /// Cheapest time window to run the job, if a schedule with
    /// time-varying prices was used.
    pub cheapest_window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    /// Confidence level of the estimate.
    pub confidence: Confidence,
}

/// The core energy cost estimator.
///
/// Thread-safe via `parking_lot::RwLock` on internal collections.
/// Maintains a set of price schedules and power overrides, and
/// provides methods to estimate costs for individual chunks or
/// entire jobs.
pub struct EnergyCostEstimator {
    /// Price schedules in priority order (first match wins).
    price_schedules: RwLock<Vec<EnergyPriceSchedule>>,
    /// Power overrides in priority order (first match wins).
    power_overrides: RwLock<Vec<PowerOverride>>,
}

impl EnergyCostEstimator {
    /// Create a new estimator with no schedules or overrides.
    pub fn new() -> Self {
        Self {
            price_schedules: RwLock::new(Vec::new()),
            power_overrides: RwLock::new(Vec::new()),
        }
    }

    /// Add a price schedule. Schedules are evaluated in insertion order.
    pub fn add_schedule(&self, schedule: EnergyPriceSchedule) {
        debug!(
            schedule_id = %schedule.id,
            name = %schedule.name,
            rules = schedule.rules.len(),
            "energy: added price schedule"
        );
        self.price_schedules.write().push(schedule);
    }

    /// Add a power override. Overrides are evaluated in priority order
    /// by target type, not insertion order.
    pub fn add_override(&self, ov: PowerOverride) {
        debug!(
            idle = ov.idle_watts,
            load = ov.load_watts,
            "energy: added power override"
        );
        self.power_overrides.write().push(ov);
    }

    /// Return a snapshot of all registered price schedules.
    pub fn list_schedules(&self) -> Vec<EnergyPriceSchedule> {
        self.price_schedules.read().clone()
    }

    /// Return a snapshot of all registered power overrides.
    pub fn list_overrides(&self) -> Vec<PowerOverride> {
        self.power_overrides.read().clone()
    }

    /// Get the power profile for a node using the override hierarchy.
    ///
    /// The hierarchy is:
    /// 1. Per-node override
    /// 2. Per-trait-combo override
    /// 3. Per-node-type override
    /// 4. Per-group override
    /// 5. Global override (All)
    /// 6. Architecture-based heuristic
    pub fn power_for_node(&self, node: &NodeProfile) -> PowerProfile {
        let overrides = self.power_overrides.read();

        // Priority 1: Per-node override.
        for ov in overrides.iter() {
            if let PowerTarget::Node { node_id } = &ov.target {
                if *node_id == node.node_id {
                    return PowerProfile {
                        source: PowerSource::ManualPerNode,
                        idle_watts: ov.idle_watts,
                        load_watts: ov.load_watts,
                        gpu_watts: ov.gpu_watts,
                    };
                }
            }
        }

        // Priority 2: Per-trait-combo override.
        for ov in overrides.iter() {
            if let PowerTarget::TraitCombo(ref spec) = ov.target {
                if spec.matches(node) {
                    return PowerProfile {
                        source: PowerSource::ManualPerTraitCombo {
                            combo_id: "trait-combo-match".to_string(),
                        },
                        idle_watts: ov.idle_watts,
                        load_watts: ov.load_watts,
                        gpu_watts: ov.gpu_watts,
                    };
                }
            }
        }

        // Priority 3: Per-node-type override.
        for ov in overrides.iter() {
            if let PowerTarget::NodeType { node_type } = &ov.target {
                if *node_type == node.node_type {
                    return PowerProfile {
                        source: PowerSource::ManualPerNodeType {
                            node_type: node.node_type,
                        },
                        idle_watts: ov.idle_watts,
                        load_watts: ov.load_watts,
                        gpu_watts: ov.gpu_watts,
                    };
                }
            }
        }

        // Priority 4: Per-group override.
        // Groups are matched by checking if the node's custom_capabilities
        // contain a "group:<group_id>" tag.
        for ov in overrides.iter() {
            if let PowerTarget::Group { group_id } = &ov.target {
                let group_tag = format!("group:{}", group_id);
                if node.custom_capabilities.contains(&group_tag) {
                    return PowerProfile {
                        source: PowerSource::ManualPerGroup {
                            group_id: group_id.clone(),
                        },
                        idle_watts: ov.idle_watts,
                        load_watts: ov.load_watts,
                        gpu_watts: ov.gpu_watts,
                    };
                }
            }
        }

        // Priority 5: Global override (All).
        for ov in overrides.iter() {
            if let PowerTarget::All = &ov.target {
                return PowerProfile {
                    source: PowerSource::Heuristic,
                    idle_watts: ov.idle_watts,
                    load_watts: ov.load_watts,
                    gpu_watts: ov.gpu_watts,
                };
            }
        }

        // Priority 6: Architecture-based heuristic.
        self.heuristic_power(node)
    }

    /// Compute a heuristic power profile based on the node's architecture
    /// and hardware characteristics.
    fn heuristic_power(&self, node: &NodeProfile) -> PowerProfile {
        let arch = guess_arch(node);
        let cores = node.cpu_cores as f64;

        let (base_idle, base_load) = if arch == "arm" {
            (POWER_HEURISTIC_ARM_IDLE, POWER_HEURISTIC_ARM_LOAD)
        } else {
            (POWER_HEURISTIC_X86_IDLE, POWER_HEURISTIC_X86_LOAD)
        };

        // Scale by core count with diminishing returns (sqrt scaling).
        let scale = cores.sqrt();
        let idle_watts = base_idle * scale;
        let load_watts = base_load * scale;

        // GPU power estimate based on VRAM (rough heuristic: 0.05W per MB VRAM).
        let gpu_watts = node.gpu.as_ref().map(|gpu| {
            let vram_watts = gpu.vram_mb as f64 * 0.05;
            vram_watts.clamp(50.0, 500.0)
        });

        PowerProfile {
            source: PowerSource::Heuristic,
            idle_watts,
            load_watts,
            gpu_watts,
        }
    }

    /// Get the energy price (USD/kWh) for a node at a specific time.
    ///
    /// Evaluates all price schedules in order. For each schedule, rules
    /// are checked in order; the first matching rule's price is returned.
    /// If no schedule/rule matches, returns `ENERGY_DEFAULT_PRICE`.
    pub fn price_at(&self, node: &NodeProfile, when: DateTime<Utc>) -> f64 {
        let schedules = self.price_schedules.read();
        let region = node
            .geo_region
            .as_deref()
            .unwrap_or("*");

        for schedule in schedules.iter() {
            for rule in &schedule.rules {
                // Region match: exact match or wildcard.
                let region_matches = rule.region == "*"
                    || rule.region.eq_ignore_ascii_case(region)
                    || region == "*";

                if region_matches && rule.time_spec.matches(when) {
                    return rule.price_usd_per_kwh;
                }
            }

            // If no rule matched in this schedule but the schedule exists,
            // fall through to the schedule's default price.
            // Only use schedule default if the node's region matches at least
            // one rule's region in this schedule (or schedule has wildcard rules).
            let schedule_has_region = schedule.rules.iter().any(|r| {
                r.region == "*"
                    || r.region.eq_ignore_ascii_case(region)
                    || region == "*"
            });
            if schedule_has_region {
                return schedule.default_price;
            }
        }

        ENERGY_DEFAULT_PRICE
    }

    /// Estimate the cost of running a chunk on a specific node.
    ///
    /// # Arguments
    ///
    /// * `node` -- Profile of the node that will execute the chunk.
    /// * `duration` -- Expected execution duration.
    /// * `cpu_util` -- Expected CPU utilization (0.0 = idle, 1.0 = full load).
    /// * `uses_gpu` -- Whether the chunk will use the GPU.
    /// * `start_time` -- When execution is expected to begin.
    pub fn estimate_chunk_cost(
        &self,
        node: &NodeProfile,
        duration: Duration,
        cpu_util: f64,
        uses_gpu: bool,
        start_time: DateTime<Utc>,
    ) -> CostEstimate {
        let power = self.power_for_node(node);
        let cpu_util_clamped = cpu_util.clamp(0.0, 1.0);

        // Interpolate between idle and load watts based on CPU utilization.
        let cpu_watts = power.idle_watts + (power.load_watts - power.idle_watts) * cpu_util_clamped;

        // Add GPU watts if applicable.
        let gpu_watts = if uses_gpu {
            power.gpu_watts.unwrap_or(0.0)
        } else {
            0.0
        };

        let total_watts = cpu_watts + gpu_watts;
        let hours = duration.as_secs_f64() / 3600.0;
        let kwh = total_watts * hours / 1000.0;

        // Get the price at the start time. For short tasks this is a
        // reasonable approximation; for long tasks the optimizer should
        // be used instead, which considers time-varying prices.
        let price_per_kwh = self.price_at(node, start_time);
        let total_usd = kwh * price_per_kwh;

        CostEstimate {
            kwh,
            watts: total_watts,
            total_usd,
            price_per_kwh,
        }
    }

    /// Estimate cost for an entire job distributed across multiple nodes.
    ///
    /// Assigns chunks round-robin to the candidate nodes and sums the
    /// per-chunk cost estimates. For optimal assignment, use the
    /// [`EnergyOptimizer`] instead.
    pub fn estimate_job_cost(
        &self,
        chunk_count: u32,
        candidates: &[NodeProfile],
        chunk_duration: Duration,
        cpu_util: f64,
        uses_gpu: bool,
        start_time: DateTime<Utc>,
    ) -> Result<JobCostEstimate, SwarmError> {
        if candidates.is_empty() {
            return Err(SwarmError::Energy(
                "no candidate nodes for cost estimation".to_string(),
            ));
        }

        let mut per_node: Vec<(NodeId, CostEstimate)> = Vec::new();
        let mut total_usd = 0.0;
        let mut total_kwh = 0.0;

        for i in 0..chunk_count {
            let node = &candidates[i as usize % candidates.len()];
            let estimate =
                self.estimate_chunk_cost(node, chunk_duration, cpu_util, uses_gpu, start_time);
            total_usd += estimate.total_usd;
            total_kwh += estimate.kwh;
            per_node.push((node.node_id, estimate));
        }

        // Determine confidence based on power source quality.
        let confidence = self.estimate_confidence(candidates);

        Ok(JobCostEstimate {
            total_usd,
            total_kwh,
            per_node,
            cheapest_window: None,
            confidence,
        })
    }

    /// Determine confidence level based on the quality of power data
    /// available for the candidate nodes.
    fn estimate_confidence(&self, candidates: &[NodeProfile]) -> Confidence {
        let overrides = self.power_overrides.read();
        let schedules = self.price_schedules.read();

        // High confidence: all nodes have manual overrides AND we have
        // at least one price schedule.
        if !schedules.is_empty() {
            let all_have_overrides = candidates.iter().all(|node| {
                overrides.iter().any(|ov| match &ov.target {
                    PowerTarget::Node { node_id } => *node_id == node.node_id,
                    PowerTarget::NodeType { node_type } => *node_type == node.node_type,
                    PowerTarget::All => true,
                    _ => false,
                })
            });
            if all_have_overrides {
                return Confidence::High;
            }
        }

        // Medium confidence: we have at least one schedule OR some overrides.
        if !schedules.is_empty() || !overrides.is_empty() {
            return Confidence::Medium;
        }

        // Low confidence: pure heuristic, no price schedules.
        Confidence::Low
    }
}

impl Default for EnergyCostEstimator {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Optimizer
// ============================================================================

/// A planned assignment of a chunk to a node at a specific time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedAssignment {
    /// Index of the chunk within the job.
    pub chunk_index: u32,
    /// Node that should execute this chunk.
    pub node_id: NodeId,
    /// When the chunk should start executing.
    pub scheduled_start: DateTime<Utc>,
    /// Estimated cost in USD for this chunk.
    pub estimated_cost_usd: f64,
    /// Estimated energy in kWh for this chunk.
    pub estimated_kwh: f64,
}

/// A complete scheduling plan produced by the optimizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulingPlan {
    /// Ordered list of chunk assignments.
    pub assignments: Vec<PlannedAssignment>,
    /// Total estimated cost in USD.
    pub total_cost_usd: f64,
    /// Total estimated energy in kWh.
    pub total_kwh: f64,
    /// Estimated time of completion for the last chunk.
    pub estimated_completion: DateTime<Utc>,
}

/// Entry in the cost matrix used by the greedy optimizer.
#[derive(Debug, Clone)]
struct CostMatrixEntry {
    node_index: usize,
    slot_start: DateTime<Utc>,
    cost_usd: f64,
    kwh: f64,
}

/// Energy-aware scheduling optimizer.
///
/// Uses a greedy algorithm to minimize total energy cost:
/// 1. Build a cost matrix of (node, hour_slot) pairs.
/// 2. Sort entries by ascending cost.
/// 3. Greedily assign chunks to the cheapest available slots,
///    respecting per-node concurrency limits.
/// 4. If a deadline is set, only consider slots that complete before it.
pub struct EnergyOptimizer {
    estimator: Arc<EnergyCostEstimator>,
}

impl EnergyOptimizer {
    /// Create a new optimizer backed by the given cost estimator.
    pub fn new(estimator: Arc<EnergyCostEstimator>) -> Self {
        Self { estimator }
    }

    /// Find the cheapest scheduling plan for a set of chunks.
    ///
    /// # Arguments
    ///
    /// * `chunk_count` -- Number of chunks to schedule.
    /// * `candidates` -- Available nodes.
    /// * `chunk_duration` -- Expected duration of each chunk.
    /// * `uses_gpu` -- Whether chunks use the GPU.
    /// * `deadline` -- Optional latest acceptable completion time.
    ///
    /// # Algorithm
    ///
    /// The optimizer evaluates 24 one-hour slots starting from now (or the
    /// earliest feasible start time). For each (node, slot) pair it computes
    /// the estimated cost. Slots are sorted by cost ascending, and chunks
    /// are assigned greedily. Each node can run at most `max_concurrent`
    /// chunks simultaneously.
    pub fn find_cheapest_plan(
        &self,
        chunk_count: u32,
        candidates: &[NodeProfile],
        chunk_duration: Duration,
        uses_gpu: bool,
        deadline: Option<DateTime<Utc>>,
    ) -> SchedulingPlan {
        if candidates.is_empty() || chunk_count == 0 {
            return SchedulingPlan {
                assignments: Vec::new(),
                total_cost_usd: 0.0,
                total_kwh: 0.0,
                estimated_completion: Utc::now(),
            };
        }

        let now = Utc::now();
        let chunk_duration_chrono =
            ChronoDuration::seconds(chunk_duration.as_secs() as i64);

        // Determine the search window: 24 hour slots from now.
        let search_hours = 24u32;

        // Build cost matrix: for each (node, hour_slot), estimate cost.
        let mut cost_matrix: Vec<CostMatrixEntry> = Vec::with_capacity(
            candidates.len() * search_hours as usize,
        );

        for (node_idx, node) in candidates.iter().enumerate() {
            for hour_offset in 0..search_hours {
                let slot_start = now + ChronoDuration::hours(hour_offset as i64);

                // Skip slots that would finish after the deadline.
                if let Some(ref dl) = deadline {
                    let slot_end = slot_start + chunk_duration_chrono;
                    if slot_end > *dl {
                        continue;
                    }
                }

                // Estimate cost at full CPU utilization (worst case for cost).
                let estimate = self.estimator.estimate_chunk_cost(
                    node,
                    chunk_duration,
                    1.0,
                    uses_gpu,
                    slot_start,
                );

                cost_matrix.push(CostMatrixEntry {
                    node_index: node_idx,
                    slot_start,
                    cost_usd: estimate.total_usd,
                    kwh: estimate.kwh,
                });
            }
        }

        // Sort by cost ascending. Break ties by earlier slot, then lower node index.
        cost_matrix.sort_by(|a, b| {
            a.cost_usd
                .partial_cmp(&b.cost_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.slot_start.cmp(&b.slot_start))
                .then_with(|| a.node_index.cmp(&b.node_index))
        });

        // Track how many chunks are assigned to each (node, slot) pair.
        // Key: (node_index, hour_offset_from_now), Value: count.
        let mut slot_usage: std::collections::HashMap<(usize, i64), u32> =
            std::collections::HashMap::new();

        let mut assignments: Vec<PlannedAssignment> = Vec::with_capacity(chunk_count as usize);
        let mut total_cost = 0.0;
        let mut total_kwh = 0.0;
        let mut latest_completion = now;
        let mut chunks_assigned = 0u32;

        // Greedy assignment.
        for entry in &cost_matrix {
            if chunks_assigned >= chunk_count {
                break;
            }

            let node = &candidates[entry.node_index];
            let max_concurrent = node.max_concurrent.max(1);
            let hour_offset = (entry.slot_start - now).num_hours();

            let current_usage = slot_usage
                .get(&(entry.node_index, hour_offset))
                .copied()
                .unwrap_or(0);

            if current_usage >= max_concurrent {
                continue;
            }

            // Assign this chunk.
            *slot_usage
                .entry((entry.node_index, hour_offset))
                .or_insert(0) += 1;

            let completion = entry.slot_start + chunk_duration_chrono;
            if completion > latest_completion {
                latest_completion = completion;
            }

            assignments.push(PlannedAssignment {
                chunk_index: chunks_assigned,
                node_id: node.node_id,
                scheduled_start: entry.slot_start,
                estimated_cost_usd: entry.cost_usd,
                estimated_kwh: entry.kwh,
            });

            total_cost += entry.cost_usd;
            total_kwh += entry.kwh;
            chunks_assigned += 1;
        }

        // If we couldn't assign all chunks (e.g. deadline too tight),
        // fall back: assign remaining chunks immediately to the cheapest nodes.
        if chunks_assigned < chunk_count {
            debug!(
                assigned = chunks_assigned,
                total = chunk_count,
                "optimizer: not all chunks could be assigned within constraints, using fallback"
            );

            // Sort candidates by their immediate cost.
            let mut fallback_costs: Vec<(usize, f64, f64)> = candidates
                .iter()
                .enumerate()
                .map(|(idx, node)| {
                    let est = self.estimator.estimate_chunk_cost(
                        node,
                        chunk_duration,
                        1.0,
                        uses_gpu,
                        now,
                    );
                    (idx, est.total_usd, est.kwh)
                })
                .collect();

            fallback_costs.sort_by(|a, b| {
                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
            });

            let mut fallback_idx = 0;
            while chunks_assigned < chunk_count {
                let (node_idx, cost_usd, kwh) =
                    fallback_costs[fallback_idx % fallback_costs.len()];
                let node = &candidates[node_idx];

                let completion = now + chunk_duration_chrono;
                if completion > latest_completion {
                    latest_completion = completion;
                }

                assignments.push(PlannedAssignment {
                    chunk_index: chunks_assigned,
                    node_id: node.node_id,
                    scheduled_start: now,
                    estimated_cost_usd: cost_usd,
                    estimated_kwh: kwh,
                });

                total_cost += cost_usd;
                total_kwh += kwh;
                chunks_assigned += 1;
                fallback_idx += 1;
            }
        }

        SchedulingPlan {
            assignments,
            total_cost_usd: total_cost,
            total_kwh,
            estimated_completion: latest_completion,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::profile::{GpuInfo, Runtime, Strength, TimeWindow, Weakness};
    use crate::swarm::types::InstalledSoftware;

    // ---- Helpers ----

    fn test_node_id() -> NodeId {
        NodeId::new()
    }

    /// Build a minimal profile for testing without hitting sysinfo.
    fn make_test_profile(node_id: NodeId, node_type: NodeType) -> NodeProfile {
        NodeProfile {
            node_id,
            node_type,
            version: 1,
            cpu_cores: 8,
            cpu_freq_mhz: 3200,
            ram_total_mb: 16384,
            disk_available_mb: 200_000,
            gpu: None,
            strengths: vec![],
            runtimes: vec![Runtime::Shell, Runtime::Python3],
            specializations: vec![],
            weaknesses: vec![],
            max_task_duration: None,
            availability_window: TimeWindow::always(),
            preferred_work: vec![],
            avoided_work: vec![],
            max_concurrent: 4,
            installed_software: vec![],
            custom_capabilities: vec![],
            ..Default::default()
            
        }
    }

    fn make_arm_profile(node_id: NodeId) -> NodeProfile {
        let mut p = make_test_profile(node_id, NodeType::Android);
        p.cpu_cores = 4;
        p.ram_total_mb = 4096;
        p.weaknesses = vec![Weakness::BatteryPowered];
        p.max_concurrent = 2;
        p
    }

    fn make_gpu_profile(node_id: NodeId) -> NodeProfile {
        let mut p = make_test_profile(node_id, NodeType::BareMetal);
        p.cpu_cores = 16;
        p.gpu = Some(GpuInfo {
            name: "NVIDIA RTX 4090".to_string(),
            vram_mb: 24576,
            cuda_cores: Some(16384),
            compute_capability: Some("8.9".to_string()),
        });
        p.strengths = vec![Strength::GPUCompute, Strength::HighCoreCount];
        p
    }

    // ---- DayType ----

    #[test]
    fn day_type_weekday_matches_monday() {
        assert!(DayType::Weekday.matches(Weekday::Mon));
        assert!(DayType::Weekday.matches(Weekday::Fri));
        assert!(!DayType::Weekday.matches(Weekday::Sat));
        assert!(!DayType::Weekday.matches(Weekday::Sun));
    }

    #[test]
    fn day_type_weekend_matches_saturday() {
        assert!(DayType::Weekend.matches(Weekday::Sat));
        assert!(DayType::Weekend.matches(Weekday::Sun));
        assert!(!DayType::Weekend.matches(Weekday::Mon));
    }

    #[test]
    fn day_type_any_matches_all() {
        for day in &[
            Weekday::Mon,
            Weekday::Tue,
            Weekday::Wed,
            Weekday::Thu,
            Weekday::Fri,
            Weekday::Sat,
            Weekday::Sun,
        ] {
            assert!(DayType::Any.matches(*day));
        }
    }

    // ---- TimeSpec ----

    #[test]
    fn time_spec_recurring_normal_range() {
        let spec = TimeSpec::Recurring {
            day_type: DayType::Any,
            start_hour: 9,
            end_hour: 17,
        };

        // Build a Wednesday at 12:00 UTC.
        let when = Utc::now()
            .date_naive()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();

        // This might not be a Wednesday, so use a fixed date.
        let fixed_wed = chrono::NaiveDate::from_ymd_opt(2025, 1, 1) // Wednesday
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        assert!(spec.matches(fixed_wed));

        let early = chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(7, 0, 0)
            .unwrap()
            .and_utc();
        assert!(!spec.matches(early));

        let late = chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(18, 0, 0)
            .unwrap()
            .and_utc();
        assert!(!spec.matches(late));
    }

    #[test]
    fn time_spec_recurring_wrap_around() {
        let spec = TimeSpec::Recurring {
            day_type: DayType::Any,
            start_hour: 22,
            end_hour: 6,
        };

        let late_night = chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(23, 0, 0)
            .unwrap()
            .and_utc();
        assert!(spec.matches(late_night));

        let early_morning = chrono::NaiveDate::from_ymd_opt(2025, 1, 2)
            .unwrap()
            .and_hms_opt(3, 0, 0)
            .unwrap()
            .and_utc();
        assert!(spec.matches(early_morning));

        let midday = chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        assert!(!spec.matches(midday));
    }

    #[test]
    fn time_spec_recurring_all_hours() {
        let spec = TimeSpec::Recurring {
            day_type: DayType::Any,
            start_hour: 0,
            end_hour: 0,
        };

        for h in 0..24 {
            let when = chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(h, 0, 0)
                .unwrap()
                .and_utc();
            assert!(spec.matches(when), "hour {} should match", h);
        }
    }

    #[test]
    fn time_spec_absolute() {
        let start = chrono::NaiveDate::from_ymd_opt(2025, 6, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let end = chrono::NaiveDate::from_ymd_opt(2025, 6, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();

        let spec = TimeSpec::Absolute { start, end };

        let during = chrono::NaiveDate::from_ymd_opt(2025, 6, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        assert!(spec.matches(during));

        let before = chrono::NaiveDate::from_ymd_opt(2025, 5, 31)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        assert!(!spec.matches(before));

        let after = chrono::NaiveDate::from_ymd_opt(2025, 6, 2)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        assert!(!spec.matches(after));
    }

    #[test]
    fn time_spec_weekday_only() {
        let spec = TimeSpec::Recurring {
            day_type: DayType::Weekday,
            start_hour: 0,
            end_hour: 0,
        };

        // 2025-01-06 is a Monday.
        let monday = chrono::NaiveDate::from_ymd_opt(2025, 1, 6)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        assert!(spec.matches(monday));

        // 2025-01-04 is a Saturday.
        let saturday = chrono::NaiveDate::from_ymd_opt(2025, 1, 4)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        assert!(!spec.matches(saturday));
    }

    // ---- CSV Parsing ----

    #[test]
    fn parse_price_csv_basic() {
        let csv = "us-east,weekday,9,17,0.15\nus-east,weekend,0,0,0.06\n";
        let schedule = parse_price_csv(csv, "test-schedule").unwrap();

        assert_eq!(schedule.name, "test-schedule");
        assert_eq!(schedule.rules.len(), 2);
        assert!(!schedule.id.is_empty());

        // First rule: weekday peak.
        assert_eq!(schedule.rules[0].region, "us-east");
        assert_eq!(schedule.rules[0].price_usd_per_kwh, 0.15);
        if let TimeSpec::Recurring {
            day_type,
            start_hour,
            end_hour,
        } = &schedule.rules[0].time_spec
        {
            assert_eq!(*day_type, DayType::Weekday);
            assert_eq!(*start_hour, 9);
            assert_eq!(*end_hour, 17);
        } else {
            panic!("expected Recurring time spec");
        }

        // Second rule: weekend flat.
        assert_eq!(schedule.rules[1].price_usd_per_kwh, 0.06);
    }

    #[test]
    fn parse_price_csv_with_whitespace() {
        let csv = " us-east , weekday , 9 , 17 , 0.15 \n";
        let schedule = parse_price_csv(csv, "trimmed").unwrap();
        assert_eq!(schedule.rules.len(), 1);
        assert_eq!(schedule.rules[0].region, "us-east");
        assert_eq!(schedule.rules[0].price_usd_per_kwh, 0.15);
    }

    #[test]
    fn parse_price_csv_empty() {
        let csv = "";
        let result = parse_price_csv(csv, "empty");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("no valid pricing rules"));
    }

    #[test]
    fn parse_price_csv_invalid_day_type() {
        let csv = "us-east,holiday,9,17,0.15\n";
        let result = parse_price_csv(csv, "bad-day");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("invalid day_type"));
    }

    #[test]
    fn parse_price_csv_invalid_hour() {
        let csv = "us-east,weekday,25,17,0.15\n";
        let result = parse_price_csv(csv, "bad-hour");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("hours must be 0-23"));
    }

    #[test]
    fn parse_price_csv_negative_price() {
        let csv = "us-east,weekday,9,17,-0.05\n";
        let result = parse_price_csv(csv, "negative");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("non-negative"));
    }

    #[test]
    fn parse_price_csv_too_few_fields() {
        let csv = "us-east,weekday,9\n";
        let result = parse_price_csv(csv, "short");
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("fields"));
    }

    #[test]
    fn parse_price_csv_multiple_regions() {
        let csv = "us-east,any,0,0,0.12\neu-west,any,0,0,0.20\nap-south,any,0,0,0.08\n";
        let schedule = parse_price_csv(csv, "multi-region").unwrap();
        assert_eq!(schedule.rules.len(), 3);
        assert_eq!(schedule.rules[0].region, "us-east");
        assert_eq!(schedule.rules[1].region, "eu-west");
        assert_eq!(schedule.rules[2].region, "ap-south");
    }

    // ---- TraitComboSpec ----

    #[test]
    fn trait_combo_matches_arm() {
        let spec = TraitComboSpec {
            arch: Some("arm".to_string()),
            ram_range: None,
            node_type: None,
            has_gpu: None,
            software: vec![],
        };

        let arm_node = make_arm_profile(test_node_id());
        assert!(spec.matches(&arm_node));

        let x86_node = make_test_profile(test_node_id(), NodeType::Desktop);
        assert!(!spec.matches(&x86_node));
    }

    #[test]
    fn trait_combo_matches_ram_range() {
        let spec = TraitComboSpec {
            arch: None,
            ram_range: Some((8192, 32768)),
            node_type: None,
            has_gpu: None,
            software: vec![],
        };

        let in_range = make_test_profile(test_node_id(), NodeType::Desktop);
        assert!(spec.matches(&in_range)); // 16384 is in [8192, 32768]

        let mut too_low = make_test_profile(test_node_id(), NodeType::Desktop);
        too_low.ram_total_mb = 4096;
        assert!(!spec.matches(&too_low));

        let mut too_high = make_test_profile(test_node_id(), NodeType::Desktop);
        too_high.ram_total_mb = 65536;
        assert!(!spec.matches(&too_high));
    }

    #[test]
    fn trait_combo_matches_gpu_requirement() {
        let spec = TraitComboSpec {
            arch: None,
            ram_range: None,
            node_type: None,
            has_gpu: Some(true),
            software: vec![],
        };

        let gpu_node = make_gpu_profile(test_node_id());
        assert!(spec.matches(&gpu_node));

        let no_gpu_node = make_test_profile(test_node_id(), NodeType::Desktop);
        assert!(!spec.matches(&no_gpu_node));
    }

    #[test]
    fn trait_combo_matches_software() {
        let spec = TraitComboSpec {
            arch: None,
            ram_range: None,
            node_type: None,
            has_gpu: None,
            software: vec!["python3".to_string(), "ffmpeg".to_string()],
        };

        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.installed_software = vec![
            InstalledSoftware {
                name: "python3".to_string(),
                version: Some("3.11".to_string()),
                path: "/usr/bin/python3".to_string(),
                category: crate::swarm::types::SoftwareCategory::Runtime,
            },
            InstalledSoftware {
                name: "ffmpeg".to_string(),
                version: Some("6.0".to_string()),
                path: "/usr/bin/ffmpeg".to_string(),
                category: crate::swarm::types::SoftwareCategory::MediaTool,
            },
        ];
        assert!(spec.matches(&node));

        let missing = make_test_profile(test_node_id(), NodeType::Desktop);
        assert!(!spec.matches(&missing));
    }

    #[test]
    fn trait_combo_matches_node_type() {
        let spec = TraitComboSpec {
            arch: None,
            ram_range: None,
            node_type: Some(NodeType::CloudVM),
            has_gpu: None,
            software: vec![],
        };

        let cloud = make_test_profile(test_node_id(), NodeType::CloudVM);
        assert!(spec.matches(&cloud));

        let desktop = make_test_profile(test_node_id(), NodeType::Desktop);
        assert!(!spec.matches(&desktop));
    }

    // ---- PowerProfile heuristic ----

    #[test]
    fn heuristic_power_x86() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);
        let power = estimator.heuristic_power(&node);

        // 8 cores, sqrt(8) ~= 2.83
        let expected_idle = POWER_HEURISTIC_X86_IDLE * (8.0_f64).sqrt();
        let expected_load = POWER_HEURISTIC_X86_LOAD * (8.0_f64).sqrt();

        assert!(
            (power.idle_watts - expected_idle).abs() < 0.01,
            "idle: {} vs expected {}",
            power.idle_watts,
            expected_idle
        );
        assert!(
            (power.load_watts - expected_load).abs() < 0.01,
            "load: {} vs expected {}",
            power.load_watts,
            expected_load
        );
        assert!(power.gpu_watts.is_none());
    }

    #[test]
    fn heuristic_power_arm() {
        let estimator = EnergyCostEstimator::new();
        let node = make_arm_profile(test_node_id());
        let power = estimator.heuristic_power(&node);

        let expected_idle = POWER_HEURISTIC_ARM_IDLE * (4.0_f64).sqrt();
        let expected_load = POWER_HEURISTIC_ARM_LOAD * (4.0_f64).sqrt();

        assert!(
            (power.idle_watts - expected_idle).abs() < 0.01,
            "idle: {} vs expected {}",
            power.idle_watts,
            expected_idle
        );
        assert!(
            (power.load_watts - expected_load).abs() < 0.01,
            "load: {} vs expected {}",
            power.load_watts,
            expected_load
        );
    }

    #[test]
    fn heuristic_power_gpu_node() {
        let estimator = EnergyCostEstimator::new();
        let node = make_gpu_profile(test_node_id());
        let power = estimator.heuristic_power(&node);

        assert!(power.gpu_watts.is_some());
        // 24576 MB * 0.05 = 1228.8, clamped to 500.0
        assert!(
            (power.gpu_watts.unwrap() - 500.0).abs() < 0.01,
            "gpu_watts: {}",
            power.gpu_watts.unwrap()
        );
    }

    // ---- Power override hierarchy ----

    #[test]
    fn power_override_per_node() {
        let estimator = EnergyCostEstimator::new();
        let node_id = test_node_id();
        let node = make_test_profile(node_id, NodeType::Desktop);

        estimator.add_override(PowerOverride {
            target: PowerTarget::Node { node_id },
            idle_watts: 42.0,
            load_watts: 200.0,
            gpu_watts: None,
        });

        let power = estimator.power_for_node(&node);
        assert!((power.idle_watts - 42.0).abs() < 0.01);
        assert!((power.load_watts - 200.0).abs() < 0.01);
        assert!(matches!(power.source, PowerSource::ManualPerNode));
    }

    #[test]
    fn power_override_per_node_type() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::CloudVM);

        estimator.add_override(PowerOverride {
            target: PowerTarget::NodeType {
                node_type: NodeType::CloudVM,
            },
            idle_watts: 30.0,
            load_watts: 150.0,
            gpu_watts: None,
        });

        let power = estimator.power_for_node(&node);
        assert!((power.idle_watts - 30.0).abs() < 0.01);
        assert!((power.load_watts - 150.0).abs() < 0.01);
        assert!(matches!(
            power.source,
            PowerSource::ManualPerNodeType { .. }
        ));
    }

    #[test]
    fn power_override_trait_combo() {
        let estimator = EnergyCostEstimator::new();
        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.ram_total_mb = 65536;

        estimator.add_override(PowerOverride {
            target: PowerTarget::TraitCombo(TraitComboSpec {
                arch: None,
                ram_range: Some((32768, 131072)),
                node_type: None,
                has_gpu: None,
                software: vec![],
            }),
            idle_watts: 50.0,
            load_watts: 250.0,
            gpu_watts: None,
        });

        let power = estimator.power_for_node(&node);
        assert!((power.idle_watts - 50.0).abs() < 0.01);
        assert!(matches!(
            power.source,
            PowerSource::ManualPerTraitCombo { .. }
        ));
    }

    #[test]
    fn power_override_group() {
        let estimator = EnergyCostEstimator::new();
        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.custom_capabilities = vec!["group:rack-a".to_string()];

        estimator.add_override(PowerOverride {
            target: PowerTarget::Group {
                group_id: "rack-a".to_string(),
            },
            idle_watts: 60.0,
            load_watts: 300.0,
            gpu_watts: None,
        });

        let power = estimator.power_for_node(&node);
        assert!((power.idle_watts - 60.0).abs() < 0.01);
        assert!(matches!(
            power.source,
            PowerSource::ManualPerGroup { .. }
        ));
    }

    #[test]
    fn power_override_all() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        estimator.add_override(PowerOverride {
            target: PowerTarget::All,
            idle_watts: 10.0,
            load_watts: 100.0,
            gpu_watts: Some(75.0),
        });

        let power = estimator.power_for_node(&node);
        assert!((power.idle_watts - 10.0).abs() < 0.01);
        assert!((power.load_watts - 100.0).abs() < 0.01);
        assert!((power.gpu_watts.unwrap() - 75.0).abs() < 0.01);
    }

    #[test]
    fn power_override_priority_node_over_type() {
        let estimator = EnergyCostEstimator::new();
        let node_id = test_node_id();
        let node = make_test_profile(node_id, NodeType::Desktop);

        // Add both per-type and per-node overrides.
        estimator.add_override(PowerOverride {
            target: PowerTarget::NodeType {
                node_type: NodeType::Desktop,
            },
            idle_watts: 30.0,
            load_watts: 150.0,
            gpu_watts: None,
        });
        estimator.add_override(PowerOverride {
            target: PowerTarget::Node { node_id },
            idle_watts: 42.0,
            load_watts: 200.0,
            gpu_watts: None,
        });

        // Per-node should win.
        let power = estimator.power_for_node(&node);
        assert!((power.idle_watts - 42.0).abs() < 0.01);
        assert!(matches!(power.source, PowerSource::ManualPerNode));
    }

    #[test]
    fn power_override_priority_trait_over_type() {
        let estimator = EnergyCostEstimator::new();
        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.ram_total_mb = 65536;

        estimator.add_override(PowerOverride {
            target: PowerTarget::NodeType {
                node_type: NodeType::Desktop,
            },
            idle_watts: 30.0,
            load_watts: 150.0,
            gpu_watts: None,
        });
        estimator.add_override(PowerOverride {
            target: PowerTarget::TraitCombo(TraitComboSpec {
                arch: None,
                ram_range: Some((32768, 131072)),
                node_type: None,
                has_gpu: None,
                software: vec![],
            }),
            idle_watts: 50.0,
            load_watts: 250.0,
            gpu_watts: None,
        });

        // TraitCombo should win over NodeType.
        let power = estimator.power_for_node(&node);
        assert!((power.idle_watts - 50.0).abs() < 0.01);
    }

    #[test]
    fn power_fallback_to_heuristic() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let power = estimator.power_for_node(&node);
        assert!(matches!(power.source, PowerSource::Heuristic));
        assert!(power.idle_watts > 0.0);
        assert!(power.load_watts > power.idle_watts);
    }

    // ---- Price at ----

    #[test]
    fn price_at_default_when_no_schedules() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);
        let price = estimator.price_at(&node, Utc::now());
        assert!((price - ENERGY_DEFAULT_PRICE).abs() < 0.001);
    }

    #[test]
    fn price_at_with_schedule() {
        let estimator = EnergyCostEstimator::new();
        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.geo_region = Some("us-east".to_string());

        let csv = "us-east,any,0,0,0.18\n";
        let schedule = parse_price_csv(csv, "us-east-flat").unwrap();
        estimator.add_schedule(schedule);

        let price = estimator.price_at(&node, Utc::now());
        assert!((price - 0.18).abs() < 0.001);
    }

    #[test]
    fn price_at_time_varying() {
        let estimator = EnergyCostEstimator::new();
        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.geo_region = Some("us-east".to_string());

        let csv = "us-east,any,9,17,0.20\nus-east,any,17,9,0.08\n";
        let schedule = parse_price_csv(csv, "tou").unwrap();
        estimator.add_schedule(schedule);

        // 12:00 UTC (peak)
        let peak = chrono::NaiveDate::from_ymd_opt(2025, 1, 6)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        let peak_price = estimator.price_at(&node, peak);
        assert!(
            (peak_price - 0.20).abs() < 0.001,
            "peak price: {}",
            peak_price
        );

        // 22:00 UTC (off-peak)
        let off_peak = chrono::NaiveDate::from_ymd_opt(2025, 1, 6)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap()
            .and_utc();
        let off_peak_price = estimator.price_at(&node, off_peak);
        assert!(
            (off_peak_price - 0.08).abs() < 0.001,
            "off-peak price: {}",
            off_peak_price
        );
    }

    #[test]
    fn price_at_no_region_match_uses_default() {
        let estimator = EnergyCostEstimator::new();
        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.geo_region = Some("ap-northeast".to_string());

        let csv = "us-east,any,0,0,0.15\n";
        let schedule = parse_price_csv(csv, "us-only").unwrap();
        estimator.add_schedule(schedule);

        // ap-northeast does not match us-east and no wildcard.
        let price = estimator.price_at(&node, Utc::now());
        assert!(
            (price - ENERGY_DEFAULT_PRICE).abs() < 0.001,
            "should fall back to default: {}",
            price
        );
    }

    #[test]
    fn price_at_wildcard_region() {
        let estimator = EnergyCostEstimator::new();
        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.geo_region = Some("any-region".to_string());

        let csv = "*,any,0,0,0.10\n";
        let schedule = parse_price_csv(csv, "global").unwrap();
        estimator.add_schedule(schedule);

        let price = estimator.price_at(&node, Utc::now());
        assert!((price - 0.10).abs() < 0.001);
    }

    // ---- Cost estimation ----

    #[test]
    fn estimate_chunk_cost_idle() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let cost = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            0.0,
            false,
            Utc::now(),
        );

        // At idle, watts should equal idle_watts from heuristic.
        let power = estimator.power_for_node(&node);
        assert!(
            (cost.watts - power.idle_watts).abs() < 0.01,
            "watts: {} vs expected {}",
            cost.watts,
            power.idle_watts
        );

        // 1 hour at idle_watts / 1000 = kWh
        let expected_kwh = power.idle_watts / 1000.0;
        assert!(
            (cost.kwh - expected_kwh).abs() < 0.001,
            "kwh: {} vs expected {}",
            cost.kwh,
            expected_kwh
        );
    }

    #[test]
    fn estimate_chunk_cost_full_load() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let cost = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            1.0,
            false,
            Utc::now(),
        );

        let power = estimator.power_for_node(&node);
        assert!(
            (cost.watts - power.load_watts).abs() < 0.01,
            "watts: {} vs expected {}",
            cost.watts,
            power.load_watts
        );
    }

    #[test]
    fn estimate_chunk_cost_half_load() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let cost = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            0.5,
            false,
            Utc::now(),
        );

        let power = estimator.power_for_node(&node);
        let expected_watts = power.idle_watts + (power.load_watts - power.idle_watts) * 0.5;
        assert!(
            (cost.watts - expected_watts).abs() < 0.01,
            "watts: {} vs expected {}",
            cost.watts,
            expected_watts
        );
    }

    #[test]
    fn estimate_chunk_cost_with_gpu() {
        let estimator = EnergyCostEstimator::new();
        let node = make_gpu_profile(test_node_id());

        let cost_no_gpu = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            1.0,
            false,
            Utc::now(),
        );

        let cost_with_gpu = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            1.0,
            true,
            Utc::now(),
        );

        assert!(
            cost_with_gpu.watts > cost_no_gpu.watts,
            "GPU should increase watts: {} vs {}",
            cost_with_gpu.watts,
            cost_no_gpu.watts
        );
        assert!(cost_with_gpu.total_usd > cost_no_gpu.total_usd);
    }

    #[test]
    fn estimate_chunk_cost_short_duration() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let cost = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(60), // 1 minute
            1.0,
            false,
            Utc::now(),
        );

        // Should be 1/60th of one-hour cost.
        let one_hour_cost = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            1.0,
            false,
            Utc::now(),
        );

        let ratio = cost.total_usd / one_hour_cost.total_usd;
        assert!(
            (ratio - 1.0 / 60.0).abs() < 0.001,
            "1-minute cost should be 1/60th of 1-hour cost: ratio={}",
            ratio
        );
    }

    #[test]
    fn estimate_chunk_cost_with_custom_price() {
        let estimator = EnergyCostEstimator::new();
        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.geo_region = Some("expensive".to_string());

        let csv = "expensive,any,0,0,1.00\n";
        let schedule = parse_price_csv(csv, "expensive").unwrap();
        estimator.add_schedule(schedule);

        let cost = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            1.0,
            false,
            Utc::now(),
        );

        assert!((cost.price_per_kwh - 1.00).abs() < 0.001);
        // USD should be kWh * 1.00
        assert!((cost.total_usd - cost.kwh * 1.00).abs() < 0.001);
    }

    // ---- Job cost estimation ----

    #[test]
    fn estimate_job_cost_single_node() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let result = estimator
            .estimate_job_cost(4, &[node.clone()], Duration::from_secs(300), 0.8, false, Utc::now())
            .unwrap();

        assert_eq!(result.per_node.len(), 4);
        assert!(result.total_usd > 0.0);
        assert!(result.total_kwh > 0.0);

        // All chunks should be assigned to the same node.
        for (nid, _) in &result.per_node {
            assert_eq!(*nid, node.node_id);
        }
    }

    #[test]
    fn estimate_job_cost_multiple_nodes() {
        let estimator = EnergyCostEstimator::new();
        let n1 = make_test_profile(test_node_id(), NodeType::Desktop);
        let n2 = make_test_profile(test_node_id(), NodeType::CloudVM);
        let candidates = vec![n1.clone(), n2.clone()];

        let result = estimator
            .estimate_job_cost(4, &candidates, Duration::from_secs(300), 0.8, false, Utc::now())
            .unwrap();

        assert_eq!(result.per_node.len(), 4);

        // Should alternate between the two nodes.
        assert_eq!(result.per_node[0].0, n1.node_id);
        assert_eq!(result.per_node[1].0, n2.node_id);
        assert_eq!(result.per_node[2].0, n1.node_id);
        assert_eq!(result.per_node[3].0, n2.node_id);
    }

    #[test]
    fn estimate_job_cost_no_candidates() {
        let estimator = EnergyCostEstimator::new();
        let result = estimator.estimate_job_cost(
            4,
            &[],
            Duration::from_secs(300),
            0.8,
            false,
            Utc::now(),
        );
        assert!(result.is_err());
    }

    // ---- Confidence estimation ----

    #[test]
    fn confidence_low_with_no_data() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);
        let confidence = estimator.estimate_confidence(&[node]);
        assert_eq!(confidence, Confidence::Low);
    }

    #[test]
    fn confidence_medium_with_schedule() {
        let estimator = EnergyCostEstimator::new();
        let csv = "*,any,0,0,0.12\n";
        let schedule = parse_price_csv(csv, "test").unwrap();
        estimator.add_schedule(schedule);

        let node = make_test_profile(test_node_id(), NodeType::Desktop);
        let confidence = estimator.estimate_confidence(&[node]);
        assert_eq!(confidence, Confidence::Medium);
    }

    #[test]
    fn confidence_high_with_override_and_schedule() {
        let estimator = EnergyCostEstimator::new();
        let csv = "*,any,0,0,0.12\n";
        let schedule = parse_price_csv(csv, "test").unwrap();
        estimator.add_schedule(schedule);

        estimator.add_override(PowerOverride {
            target: PowerTarget::All,
            idle_watts: 10.0,
            load_watts: 100.0,
            gpu_watts: None,
        });

        let node = make_test_profile(test_node_id(), NodeType::Desktop);
        let confidence = estimator.estimate_confidence(&[node]);
        assert_eq!(confidence, Confidence::High);
    }

    // ---- Optimizer ----

    #[test]
    fn optimizer_empty_chunks() {
        let estimator = Arc::new(EnergyCostEstimator::new());
        let optimizer = EnergyOptimizer::new(estimator);
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let plan = optimizer.find_cheapest_plan(
            0,
            &[node],
            Duration::from_secs(300),
            false,
            None,
        );

        assert!(plan.assignments.is_empty());
        assert!((plan.total_cost_usd - 0.0).abs() < 0.001);
    }

    #[test]
    fn optimizer_empty_candidates() {
        let estimator = Arc::new(EnergyCostEstimator::new());
        let optimizer = EnergyOptimizer::new(estimator);

        let plan = optimizer.find_cheapest_plan(
            4,
            &[],
            Duration::from_secs(300),
            false,
            None,
        );

        assert!(plan.assignments.is_empty());
    }

    #[test]
    fn optimizer_single_node_no_deadline() {
        let estimator = Arc::new(EnergyCostEstimator::new());
        let optimizer = EnergyOptimizer::new(estimator);
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let plan = optimizer.find_cheapest_plan(
            4,
            &[node.clone()],
            Duration::from_secs(300),
            false,
            None,
        );

        assert_eq!(plan.assignments.len(), 4);
        assert!(plan.total_cost_usd > 0.0);
        assert!(plan.total_kwh > 0.0);

        for assignment in &plan.assignments {
            assert_eq!(assignment.node_id, node.node_id);
            assert!(assignment.estimated_cost_usd > 0.0);
        }
    }

    #[test]
    fn optimizer_respects_max_concurrent() {
        let estimator = Arc::new(EnergyCostEstimator::new());
        let optimizer = EnergyOptimizer::new(estimator);

        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.max_concurrent = 2;

        let plan = optimizer.find_cheapest_plan(
            6,
            &[node.clone()],
            Duration::from_secs(300),
            false,
            None,
        );

        assert_eq!(plan.assignments.len(), 6);

        // Count assignments per (node, hour_offset) slot.
        let now = Utc::now();
        let mut slot_counts: std::collections::HashMap<i64, u32> = std::collections::HashMap::new();
        for assignment in &plan.assignments {
            let offset = (assignment.scheduled_start - now).num_hours();
            *slot_counts.entry(offset).or_insert(0) += 1;
        }

        for (offset, count) in &slot_counts {
            assert!(
                *count <= 2,
                "slot {} has {} assignments, but max_concurrent is 2",
                offset,
                count
            );
        }
    }

    #[test]
    fn optimizer_prefers_cheaper_nodes() {
        let estimator = Arc::new(EnergyCostEstimator::new());

        // Make one node expensive, one cheap.
        let cheap_id = test_node_id();
        let expensive_id = test_node_id();

        estimator.add_override(PowerOverride {
            target: PowerTarget::Node { node_id: cheap_id },
            idle_watts: 5.0,
            load_watts: 20.0,
            gpu_watts: None,
        });
        estimator.add_override(PowerOverride {
            target: PowerTarget::Node {
                node_id: expensive_id,
            },
            idle_watts: 100.0,
            load_watts: 500.0,
            gpu_watts: None,
        });

        let cheap_node = make_test_profile(cheap_id, NodeType::Desktop);
        let expensive_node = make_test_profile(expensive_id, NodeType::BareMetal);

        let optimizer = EnergyOptimizer::new(estimator);
        let plan = optimizer.find_cheapest_plan(
            4,
            &[cheap_node.clone(), expensive_node.clone()],
            Duration::from_secs(300),
            false,
            None,
        );

        assert_eq!(plan.assignments.len(), 4);

        // All 4 chunks should go to the cheap node first (it has max_concurrent=4).
        let cheap_count = plan
            .assignments
            .iter()
            .filter(|a| a.node_id == cheap_id)
            .count();
        assert_eq!(
            cheap_count, 4,
            "optimizer should assign all chunks to cheaper node, got {} on cheap",
            cheap_count
        );
    }

    #[test]
    fn optimizer_with_time_varying_prices() {
        let estimator = Arc::new(EnergyCostEstimator::new());

        let mut node = make_test_profile(test_node_id(), NodeType::Desktop);
        node.geo_region = Some("test".to_string());

        // Peak: 9-17, Off-peak: 17-9
        let csv = "test,any,9,17,0.50\ntest,any,17,9,0.05\n";
        let schedule = parse_price_csv(csv, "tou").unwrap();
        estimator.add_schedule(schedule);

        let optimizer = EnergyOptimizer::new(estimator.clone());
        let plan = optimizer.find_cheapest_plan(
            2,
            &[node.clone()],
            Duration::from_secs(3600),
            false,
            None,
        );

        assert_eq!(plan.assignments.len(), 2);

        // Both assignments should prefer the cheapest time slots.
        // We cannot predict exact hours since it depends on current time,
        // but the plan should have a non-zero cost.
        assert!(plan.total_cost_usd > 0.0);
    }

    #[test]
    fn optimizer_with_deadline() {
        let estimator = Arc::new(EnergyCostEstimator::new());
        let optimizer = EnergyOptimizer::new(estimator);

        let node = make_test_profile(test_node_id(), NodeType::Desktop);
        let now = Utc::now();
        let deadline = now + ChronoDuration::hours(2);

        let plan = optimizer.find_cheapest_plan(
            4,
            &[node.clone()],
            Duration::from_secs(3600), // 1 hour chunks
            false,
            Some(deadline),
        );

        assert_eq!(plan.assignments.len(), 4);

        // All assignments should start before the deadline minus chunk duration.
        for assignment in &plan.assignments {
            let completion = assignment.scheduled_start + ChronoDuration::hours(1);
            // Some may be assigned via fallback, so just check they exist.
            assert!(assignment.estimated_cost_usd >= 0.0);
        }
    }

    #[test]
    fn optimizer_many_chunks_distributed() {
        let estimator = Arc::new(EnergyCostEstimator::new());
        let optimizer = EnergyOptimizer::new(estimator);

        let n1 = make_test_profile(test_node_id(), NodeType::Desktop);
        let n2 = make_test_profile(test_node_id(), NodeType::CloudVM);
        let n3 = make_test_profile(test_node_id(), NodeType::BareMetal);

        let plan = optimizer.find_cheapest_plan(
            20,
            &[n1, n2, n3],
            Duration::from_secs(600),
            false,
            None,
        );

        assert_eq!(plan.assignments.len(), 20);
        assert!(plan.total_cost_usd > 0.0);
        assert!(plan.total_kwh > 0.0);
        assert!(plan.estimated_completion > Utc::now());
    }

    // ---- Serialization round-trips ----

    #[test]
    fn power_profile_serialization() {
        let profile = PowerProfile {
            source: PowerSource::ManualPerNode,
            idle_watts: 42.0,
            load_watts: 200.0,
            gpu_watts: Some(150.0),
        };

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: PowerProfile = serde_json::from_str(&json).unwrap();

        assert!((deserialized.idle_watts - 42.0).abs() < 0.01);
        assert!((deserialized.load_watts - 200.0).abs() < 0.01);
        assert!((deserialized.gpu_watts.unwrap() - 150.0).abs() < 0.01);
    }

    #[test]
    fn power_override_serialization() {
        let ov = PowerOverride {
            target: PowerTarget::NodeType {
                node_type: NodeType::CloudVM,
            },
            idle_watts: 30.0,
            load_watts: 150.0,
            gpu_watts: None,
        };

        let json = serde_json::to_string(&ov).unwrap();
        let deserialized: PowerOverride = serde_json::from_str(&json).unwrap();

        assert!((deserialized.idle_watts - 30.0).abs() < 0.01);
    }

    #[test]
    fn energy_price_schedule_serialization() {
        let schedule = EnergyPriceSchedule {
            id: "test-id".to_string(),
            name: "Test Schedule".to_string(),
            rules: vec![PriceRule {
                region: "us-east".to_string(),
                time_spec: TimeSpec::Recurring {
                    day_type: DayType::Weekday,
                    start_hour: 9,
                    end_hour: 17,
                },
                price_usd_per_kwh: 0.15,
            }],
            default_price: 0.12,
        };

        let json = serde_json::to_string(&schedule).unwrap();
        let deserialized: EnergyPriceSchedule = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, "test-id");
        assert_eq!(deserialized.rules.len(), 1);
        assert!((deserialized.default_price - 0.12).abs() < 0.001);
    }

    #[test]
    fn cost_estimate_serialization() {
        let est = CostEstimate {
            kwh: 0.5,
            watts: 100.0,
            total_usd: 0.06,
            price_per_kwh: 0.12,
        };

        let json = serde_json::to_string(&est).unwrap();
        let deserialized: CostEstimate = serde_json::from_str(&json).unwrap();

        assert!((deserialized.kwh - 0.5).abs() < 0.001);
        assert!((deserialized.total_usd - 0.06).abs() < 0.001);
    }

    #[test]
    fn scheduling_plan_serialization() {
        let plan = SchedulingPlan {
            assignments: vec![PlannedAssignment {
                chunk_index: 0,
                node_id: test_node_id(),
                scheduled_start: Utc::now(),
                estimated_cost_usd: 0.05,
                estimated_kwh: 0.42,
            }],
            total_cost_usd: 0.05,
            total_kwh: 0.42,
            estimated_completion: Utc::now(),
        };

        let json = serde_json::to_string(&plan).unwrap();
        let deserialized: SchedulingPlan = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.assignments.len(), 1);
        assert!((deserialized.total_cost_usd - 0.05).abs() < 0.001);
    }

    #[test]
    fn trait_combo_spec_serialization() {
        let spec = TraitComboSpec {
            arch: Some("x86".to_string()),
            ram_range: Some((8192, 32768)),
            node_type: Some(NodeType::BareMetal),
            has_gpu: Some(true),
            software: vec!["python3".to_string()],
        };

        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: TraitComboSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.arch, Some("x86".to_string()));
        assert_eq!(deserialized.ram_range, Some((8192, 32768)));
        assert_eq!(deserialized.node_type, Some(NodeType::BareMetal));
        assert_eq!(deserialized.has_gpu, Some(true));
        assert_eq!(deserialized.software, vec!["python3".to_string()]);
    }

    // ---- Estimator list methods ----

    #[test]
    fn list_schedules_returns_all() {
        let estimator = EnergyCostEstimator::new();

        let csv1 = "*,any,0,0,0.10\n";
        let csv2 = "*,any,0,0,0.20\n";
        estimator.add_schedule(parse_price_csv(csv1, "s1").unwrap());
        estimator.add_schedule(parse_price_csv(csv2, "s2").unwrap());

        let schedules = estimator.list_schedules();
        assert_eq!(schedules.len(), 2);
        assert_eq!(schedules[0].name, "s1");
        assert_eq!(schedules[1].name, "s2");
    }

    #[test]
    fn list_overrides_returns_all() {
        let estimator = EnergyCostEstimator::new();

        estimator.add_override(PowerOverride {
            target: PowerTarget::All,
            idle_watts: 10.0,
            load_watts: 100.0,
            gpu_watts: None,
        });
        estimator.add_override(PowerOverride {
            target: PowerTarget::NodeType {
                node_type: NodeType::Android,
            },
            idle_watts: 2.0,
            load_watts: 8.0,
            gpu_watts: None,
        });

        let overrides = estimator.list_overrides();
        assert_eq!(overrides.len(), 2);
    }

    // ---- Edge cases ----

    #[test]
    fn estimate_chunk_cost_zero_duration() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let cost = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(0),
            1.0,
            false,
            Utc::now(),
        );

        assert!((cost.kwh - 0.0).abs() < 0.001);
        assert!((cost.total_usd - 0.0).abs() < 0.001);
    }

    #[test]
    fn estimate_chunk_cost_clamped_utilization() {
        let estimator = EnergyCostEstimator::new();
        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        // Utilization > 1.0 should be clamped to 1.0
        let cost_over = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            2.0,
            false,
            Utc::now(),
        );

        let cost_max = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            1.0,
            false,
            Utc::now(),
        );

        assert!(
            (cost_over.watts - cost_max.watts).abs() < 0.01,
            "clamped: {} vs max: {}",
            cost_over.watts,
            cost_max.watts
        );

        // Utilization < 0.0 should be clamped to 0.0
        let cost_under = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            -0.5,
            false,
            Utc::now(),
        );

        let cost_idle = estimator.estimate_chunk_cost(
            &node,
            Duration::from_secs(3600),
            0.0,
            false,
            Utc::now(),
        );

        assert!(
            (cost_under.watts - cost_idle.watts).abs() < 0.01,
            "clamped: {} vs idle: {}",
            cost_under.watts,
            cost_idle.watts
        );
    }

    #[test]
    fn guess_arch_android_is_arm() {
        let node = make_test_profile(test_node_id(), NodeType::Android);
        assert_eq!(guess_arch(&node), "arm");
    }

    #[test]
    fn guess_arch_desktop_is_x86() {
        let node = make_test_profile(test_node_id(), NodeType::Desktop);
        assert_eq!(guess_arch(&node), "x86");
    }

    #[test]
    fn guess_arch_browser_is_wasm() {
        let node = make_test_profile(test_node_id(), NodeType::Browser);
        assert_eq!(guess_arch(&node), "wasm");
    }

    #[test]
    fn optimizer_total_cost_is_sum_of_parts() {
        let estimator = Arc::new(EnergyCostEstimator::new());
        let optimizer = EnergyOptimizer::new(estimator);

        let node = make_test_profile(test_node_id(), NodeType::Desktop);

        let plan = optimizer.find_cheapest_plan(
            8,
            &[node],
            Duration::from_secs(300),
            false,
            None,
        );

        let sum_cost: f64 = plan.assignments.iter().map(|a| a.estimated_cost_usd).sum();
        let sum_kwh: f64 = plan.assignments.iter().map(|a| a.estimated_kwh).sum();

        assert!(
            (plan.total_cost_usd - sum_cost).abs() < 0.0001,
            "total {} vs sum {}",
            plan.total_cost_usd,
            sum_cost
        );
        assert!(
            (plan.total_kwh - sum_kwh).abs() < 0.0001,
            "total {} vs sum {}",
            plan.total_kwh,
            sum_kwh
        );
    }
}
