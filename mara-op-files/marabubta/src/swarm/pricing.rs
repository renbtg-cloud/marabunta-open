// Marabunta - Licensed under the MIT License.
//! Pricing and cost estimation engine for the Marabunta Swarm.
//!
//! This module provides a comprehensive pricing subsystem that handles:
//!
//! - **Cost estimation**: Pre-submission cost projections based on job size,
//!   priority, verification strategy, and target swarm tier.
//! - **Real-time billing**: Tracks per-chunk costs as a job executes, including
//!   retries absorbed transparently by the swarm.
//! - **Receipt generation**: Final itemised receipts with cloud-comparison
//!   savings figures.
//! - **Module-aware pricing**: Pre-computed pricing profiles for all 22 CR
//!   compute modules, including cloud-equivalent cost baselines.
//! - **Economy scheduling**: Off-peak detection for Economy-tier jobs.
//!
//! # Integration
//!
//! The [`PricingEngine`] is constructed with an optional [`KnowledgeStore`]
//! reference. When present, the engine can query live swarm state (node
//! counts, active jobs, etc.) to refine its estimates. When absent, it falls
//! back to configuration-driven defaults.
//!
//! # Thread safety
//!
//! All public types are `Send + Sync`. The [`PricingEngine`] uses [`DashMap`]
//! for active billing records and [`parking_lot::RwLock`] for aggregate stats.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc, Timelike, Duration as ChronoDuration};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::common::types::JobId;
use super::types::{NodeClass, PriorityLevel, CostEstimate, VerificationStrategy, SwarmTier};
use super::config::*;
use super::knowledge::KnowledgeStore;

// ============================================================================
// CostEstimateRequest
// ============================================================================

/// Input parameters for computing a job cost estimate.
///
/// Callers fill in the fields they know; the pricing engine uses sensible
/// defaults for anything left unset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimateRequest {
    /// Total number of work items (rows, files, simulations, etc.).
    pub total_items: u64,
    /// Average size of each item in bytes (used for transfer cost).
    pub item_size_bytes: u64,
    /// Estimated wall-clock duration per item in milliseconds.
    pub per_item_duration_ms: u64,
    /// Customer priority level.
    pub priority: PriorityLevel,
    /// Verification strategy to apply.
    pub verification: VerificationStrategy,
    /// Target swarm tier (determines effective node count).
    pub target_tier: Option<SwarmTier>,
    /// Optional module identifier for module-aware pricing.
    pub module_id: Option<String>,
}

impl Default for CostEstimateRequest {
    fn default() -> Self {
        Self {
            total_items: 1,
            item_size_bytes: 0,
            per_item_duration_ms: 1000,
            priority: PriorityLevel::Standard,
            verification: VerificationStrategy::None,
            target_tier: None,
            module_id: None,
        }
    }
}

impl CostEstimateRequest {
    /// Create a new request with the given item count and default settings.
    pub fn new(total_items: u64) -> Self {
        Self {
            total_items,
            ..Default::default()
        }
    }

    /// Builder: set item size in bytes.
    pub fn with_item_size(mut self, bytes: u64) -> Self {
        self.item_size_bytes = bytes;
        self
    }

    /// Builder: set per-item duration in milliseconds.
    pub fn with_duration_ms(mut self, ms: u64) -> Self {
        self.per_item_duration_ms = ms;
        self
    }

    /// Builder: set priority level.
    pub fn with_priority(mut self, priority: PriorityLevel) -> Self {
        self.priority = priority;
        self
    }

    /// Builder: set verification strategy.
    pub fn with_verification(mut self, v: VerificationStrategy) -> Self {
        self.verification = v;
        self
    }

    /// Builder: set target swarm tier.
    pub fn with_tier(mut self, tier: SwarmTier) -> Self {
        self.target_tier = Some(tier);
        self
    }

    /// Builder: set module identifier.
    pub fn with_module(mut self, module_id: impl Into<String>) -> Self {
        self.module_id = Some(module_id.into());
        self
    }

    /// Total data transfer in bytes (items x item_size).
    pub fn total_transfer_bytes(&self) -> u64 {
        self.total_items.saturating_mul(self.item_size_bytes)
    }

    /// Total compute time in milliseconds (items x per_item_duration).
    pub fn total_compute_ms(&self) -> u64 {
        self.total_items.saturating_mul(self.per_item_duration_ms)
    }

    /// Total compute time in hours.
    pub fn total_compute_hours(&self) -> f64 {
        self.total_compute_ms() as f64 / 3_600_000.0
    }
}

// ============================================================================
// ClassRate
// ============================================================================

/// Per-class pricing rates.
///
/// Each [`NodeClass`] has different cost characteristics reflecting the
/// hardware contributed to the swarm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassRate {
    /// Cost per node-hour in USD.
    pub usd_per_hour: f64,
    /// Cost per GB of data transferred in USD.
    pub usd_per_gb_transfer: f64,
    /// Cost per individual chunk processed in USD.
    pub usd_per_chunk: f64,
}

impl ClassRate {
    /// Create a new rate with the given per-hour cost and derived transfer/chunk costs.
    pub fn new(usd_per_hour: f64, usd_per_gb_transfer: f64, usd_per_chunk: f64) -> Self {
        Self {
            usd_per_hour,
            usd_per_gb_transfer,
            usd_per_chunk,
        }
    }

    /// Create a rate from just the per-hour cost, with sensible defaults for transfer and chunk.
    pub fn from_hourly(usd_per_hour: f64) -> Self {
        Self {
            usd_per_hour,
            usd_per_gb_transfer: usd_per_hour * 0.1,
            usd_per_chunk: usd_per_hour / 3600.0,
        }
    }

    /// Total cost for the given duration and transfer.
    pub fn compute_cost(&self, duration_hours: f64, transfer_gb: f64, chunks: u64) -> f64 {
        let time_cost = self.usd_per_hour * duration_hours;
        let transfer_cost = self.usd_per_gb_transfer * transfer_gb;
        let chunk_cost = self.usd_per_chunk * chunks as f64;
        time_cost + transfer_cost + chunk_cost
    }

    /// Scale all rates by a multiplier (e.g. for priority adjustment).
    pub fn scaled(&self, factor: f64) -> Self {
        Self {
            usd_per_hour: self.usd_per_hour * factor,
            usd_per_gb_transfer: self.usd_per_gb_transfer * factor,
            usd_per_chunk: self.usd_per_chunk * factor,
        }
    }
}

// ============================================================================
// RateCard
// ============================================================================

/// Pricing rates per node class, plus global fee parameters.
///
/// The rate card defines the base cost structure for the swarm. Different
/// node classes have different rates reflecting their hardware capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateCard {
    /// Per-class pricing rates.
    pub rates: HashMap<NodeClass, ClassRate>,
    /// Platform transaction fee as a fraction (e.g. 0.02 = 2%).
    pub transaction_fee_pct: f64,
    /// Minimum charge in USD for any job (floor).
    pub minimum_charge_usd: f64,
}

impl RateCard {
    /// Create the default rate card with standard pricing.
    ///
    /// Rates are derived from `PRICING_BASE_NODE_HOUR_USD` scaled by each
    /// class's compute weight.
    pub fn default_card() -> Self {
        let base = PRICING_BASE_NODE_HOUR_USD;
        let mut rates = HashMap::new();

        rates.insert(NodeClass::Edge, ClassRate::new(
            base * NodeClass::Edge.compute_weight(),
            0.00001,
            0.0000001,
        ));
        rates.insert(NodeClass::Light, ClassRate::new(
            base * NodeClass::Light.compute_weight(),
            0.00005,
            0.0000005,
        ));
        rates.insert(NodeClass::Standard, ClassRate::new(
            base * NodeClass::Standard.compute_weight(),
            0.0002,
            0.000002,
        ));
        rates.insert(NodeClass::Enterprise, ClassRate::new(
            base * NodeClass::Enterprise.compute_weight(),
            0.0008,
            0.000008,
        ));

        Self {
            rates,
            transaction_fee_pct: PRICING_TRANSACTION_FEE_PCT,
            minimum_charge_usd: 0.001,
        }
    }

    /// Create a custom rate card from a map of per-class rates.
    pub fn custom(rates: HashMap<NodeClass, ClassRate>) -> Self {
        Self {
            rates,
            transaction_fee_pct: PRICING_TRANSACTION_FEE_PCT,
            minimum_charge_usd: 0.001,
        }
    }

    /// Look up the rate for a given node class, falling back to Standard if not found.
    pub fn rate_for(&self, class: NodeClass) -> &ClassRate {
        self.rates.get(&class).unwrap_or_else(|| {
            self.rates.get(&NodeClass::Standard).expect("RateCard missing Standard class")
        })
    }

    /// Builder: set the transaction fee percentage.
    pub fn with_transaction_fee(mut self, pct: f64) -> Self {
        self.transaction_fee_pct = pct;
        self
    }

    /// Builder: set the minimum charge.
    pub fn with_minimum_charge(mut self, usd: f64) -> Self {
        self.minimum_charge_usd = usd;
        self
    }

    /// Builder: override the rate for a specific class.
    pub fn with_class_rate(mut self, class: NodeClass, rate: ClassRate) -> Self {
        self.rates.insert(class, rate);
        self
    }

    /// Return the blended average hourly rate across all classes.
    pub fn blended_hourly_rate(&self) -> f64 {
        if self.rates.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.rates.values().map(|r| r.usd_per_hour).sum();
        sum / self.rates.len() as f64
    }

    /// Return the cheapest hourly rate across all classes.
    pub fn cheapest_hourly_rate(&self) -> f64 {
        self.rates.values()
            .map(|r| r.usd_per_hour)
            .fold(f64::MAX, f64::min)
    }

    /// Return the most expensive hourly rate across all classes.
    pub fn most_expensive_hourly_rate(&self) -> f64 {
        self.rates.values()
            .map(|r| r.usd_per_hour)
            .fold(0.0_f64, f64::max)
    }

    /// Apply the transaction fee to a raw cost.
    pub fn apply_transaction_fee(&self, raw_cost: f64) -> f64 {
        raw_cost * (1.0 + self.transaction_fee_pct)
    }

    /// Enforce the minimum charge on a computed cost.
    pub fn enforce_minimum(&self, cost: f64) -> f64 {
        cost.max(self.minimum_charge_usd)
    }
}

impl Default for RateCard {
    fn default() -> Self {
        Self::default_card()
    }
}

// ============================================================================
// JobBilling
// ============================================================================

/// Active billing tracker for a running job.
///
/// Created when a job begins execution, updated as chunks complete, and
/// finalized into a [`JobReceipt`] when the job is done.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobBilling {
    /// The job being billed.
    pub job_id: JobId,
    /// When billing started.
    pub started_at: DateTime<Utc>,
    /// The pre-submission cost estimate.
    pub estimate: CostEstimate,
    /// Number of chunks billed so far.
    pub chunks_billed: u64,
    /// Number of retries absorbed (not charged to the user).
    pub retries_absorbed: u64,
    /// Running total cost in USD.
    pub total_cost_usd: f64,
    /// Cost broken down by node class.
    pub cost_by_class: HashMap<NodeClass, f64>,
    /// Cost attributed to verification overhead.
    pub verification_cost_usd: f64,
}

impl JobBilling {
    /// Create a new billing record for a job.
    pub fn new(job_id: JobId, estimate: CostEstimate) -> Self {
        Self {
            job_id,
            started_at: Utc::now(),
            estimate,
            chunks_billed: 0,
            retries_absorbed: 0,
            total_cost_usd: 0.0,
            cost_by_class: HashMap::new(),
            verification_cost_usd: 0.0,
        }
    }

    /// Elapsed billing duration.
    pub fn elapsed(&self) -> ChronoDuration {
        Utc::now() - self.started_at
    }

    /// Elapsed billing duration in seconds.
    pub fn elapsed_secs(&self) -> i64 {
        self.elapsed().num_seconds()
    }

    /// Average cost per chunk so far.
    pub fn avg_cost_per_chunk(&self) -> f64 {
        if self.chunks_billed == 0 {
            return 0.0;
        }
        self.total_cost_usd / self.chunks_billed as f64
    }

    /// Projected total cost based on current rate and estimated total chunks.
    pub fn projected_total(&self) -> f64 {
        if self.chunks_billed == 0 {
            return self.estimate.estimated_cost_usd;
        }
        let avg = self.avg_cost_per_chunk();
        avg * self.estimate.estimated_chunks as f64
    }

    /// Whether the actual cost has exceeded the estimate.
    pub fn is_over_estimate(&self) -> bool {
        self.total_cost_usd > self.estimate.estimated_cost_usd
    }

    /// Ratio of actual to estimated cost (1.0 = on-budget).
    pub fn budget_ratio(&self) -> f64 {
        if self.estimate.estimated_cost_usd <= 0.0 {
            return 0.0;
        }
        self.total_cost_usd / self.estimate.estimated_cost_usd
    }

    /// Dominant node class by cost contribution.
    pub fn dominant_class(&self) -> Option<NodeClass> {
        self.cost_by_class
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(class, _)| *class)
    }

    /// Number of distinct node classes that have contributed.
    pub fn class_count(&self) -> usize {
        self.cost_by_class.len()
    }
}

// ============================================================================
// JobReceipt
// ============================================================================

/// Final receipt after a job completes.
///
/// Immutable record of what was charged, including cloud-comparison savings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobReceipt {
    /// Unique receipt identifier.
    pub receipt_id: String,
    /// The job this receipt is for.
    pub job_id: JobId,
    /// When billing started.
    pub started_at: DateTime<Utc>,
    /// When billing was finalized.
    pub completed_at: DateTime<Utc>,
    /// Total cost in USD.
    pub total_cost_usd: f64,
    /// Pre-submission estimate in USD.
    pub estimated_cost_usd: f64,
    /// Total chunks processed.
    pub chunks_processed: u64,
    /// Retries absorbed (not charged).
    pub retries_absorbed: u64,
    /// Cost attributed to verification.
    pub verification_cost_usd: f64,
    /// Cost broken down by node class.
    pub cost_by_class: HashMap<NodeClass, f64>,
    /// Estimated savings vs cloud in USD.
    pub savings_vs_cloud_usd: f64,
    /// Priority level used.
    pub priority: PriorityLevel,
}

impl JobReceipt {
    /// Duration of the job from start to completion.
    pub fn duration(&self) -> ChronoDuration {
        self.completed_at - self.started_at
    }

    /// Duration in human-readable seconds.
    pub fn duration_secs(&self) -> i64 {
        self.duration().num_seconds()
    }

    /// Ratio of actual cost to estimate (< 1.0 = under budget).
    pub fn estimate_accuracy(&self) -> f64 {
        if self.estimated_cost_usd <= 0.0 {
            return 0.0;
        }
        self.total_cost_usd / self.estimated_cost_usd
    }

    /// Whether the actual cost was under the estimate.
    pub fn was_under_budget(&self) -> bool {
        self.total_cost_usd <= self.estimated_cost_usd
    }

    /// Savings percentage vs cloud cost.
    pub fn savings_pct(&self) -> f64 {
        if self.savings_vs_cloud_usd <= 0.0 && self.total_cost_usd <= 0.0 {
            return 0.0;
        }
        let cloud_cost = self.total_cost_usd + self.savings_vs_cloud_usd;
        if cloud_cost <= 0.0 {
            return 0.0;
        }
        (self.savings_vs_cloud_usd / cloud_cost) * 100.0
    }

    /// Generate a one-line summary string.
    pub fn summary(&self) -> String {
        format!(
            "Receipt {}: job={}, cost=${:.6}, chunks={}, retries={}, savings=${:.6} ({:.1}%)",
            self.receipt_id,
            self.job_id,
            self.total_cost_usd,
            self.chunks_processed,
            self.retries_absorbed,
            self.savings_vs_cloud_usd,
            self.savings_pct(),
        )
    }

    /// Verification cost as percentage of total.
    pub fn verification_pct(&self) -> f64 {
        if self.total_cost_usd <= 0.0 {
            return 0.0;
        }
        (self.verification_cost_usd / self.total_cost_usd) * 100.0
    }

    /// Cost per chunk.
    pub fn cost_per_chunk(&self) -> f64 {
        if self.chunks_processed == 0 {
            return 0.0;
        }
        self.total_cost_usd / self.chunks_processed as f64
    }
}

// ============================================================================
// CloudComparison
// ============================================================================

/// Cost comparison between the Marabunta swarm and cloud providers.
///
/// Used both for pre-submission estimates and post-completion receipts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudComparison {
    /// Estimated Marabunta swarm cost in USD.
    pub cr_cost_usd: f64,
    /// Estimated AWS equivalent cost in USD.
    pub aws_cost_usd: f64,
    /// Estimated Azure equivalent cost in USD.
    pub azure_cost_usd: f64,
    /// Estimated GCP equivalent cost in USD.
    pub gcp_cost_usd: f64,
    /// Best savings percentage (CR vs cheapest cloud).
    pub savings_pct: f64,
    /// Estimated CR completion time in seconds.
    pub cr_time_estimate: u64,
    /// Estimated cloud completion time in seconds.
    pub cloud_time_estimate: u64,
}

impl CloudComparison {
    /// Return the best savings percentage across all cloud providers.
    pub fn best_savings_pct(&self) -> f64 {
        let cheapest_cloud = self.aws_cost_usd
            .min(self.azure_cost_usd)
            .min(self.gcp_cost_usd);
        if cheapest_cloud <= 0.0 {
            return 0.0;
        }
        ((cheapest_cloud - self.cr_cost_usd) / cheapest_cloud) * 100.0
    }

    /// Generate a human-readable summary.
    pub fn summary_text(&self) -> String {
        let cheapest_cloud = self.cheapest_cloud_cost();
        let savings = self.best_savings_pct();
        format!(
            "CR: ${:.6} | AWS: ${:.6} | Azure: ${:.6} | GCP: ${:.6} | Savings: {:.1}% vs cheapest (${:.6})",
            self.cr_cost_usd,
            self.aws_cost_usd,
            self.azure_cost_usd,
            self.gcp_cost_usd,
            savings,
            cheapest_cloud,
        )
    }

    /// Return the cheapest cloud provider cost.
    pub fn cheapest_cloud_cost(&self) -> f64 {
        self.aws_cost_usd
            .min(self.azure_cost_usd)
            .min(self.gcp_cost_usd)
    }

    /// Return the most expensive cloud provider cost.
    pub fn most_expensive_cloud_cost(&self) -> f64 {
        self.aws_cost_usd
            .max(self.azure_cost_usd)
            .max(self.gcp_cost_usd)
    }

    /// Return the name of the cheapest cloud provider.
    pub fn cheapest_cloud_name(&self) -> &'static str {
        let min = self.cheapest_cloud_cost();
        if (self.aws_cost_usd - min).abs() < f64::EPSILON {
            "AWS"
        } else if (self.azure_cost_usd - min).abs() < f64::EPSILON {
            "Azure"
        } else {
            "GCP"
        }
    }

    /// Savings amount in USD vs the cheapest cloud.
    pub fn savings_usd(&self) -> f64 {
        self.cheapest_cloud_cost() - self.cr_cost_usd
    }

    /// Time savings in seconds (positive = CR is faster).
    pub fn time_savings_secs(&self) -> i64 {
        self.cloud_time_estimate as i64 - self.cr_time_estimate as i64
    }

    /// CR speedup factor vs cloud.
    pub fn speedup_factor(&self) -> f64 {
        if self.cr_time_estimate == 0 {
            return 0.0;
        }
        self.cloud_time_estimate as f64 / self.cr_time_estimate as f64
    }
}

// ============================================================================
// PricingStats
// ============================================================================

/// Aggregate pricing statistics across all jobs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PricingStats {
    /// Total number of jobs that have been billed.
    pub total_jobs_billed: u64,
    /// Total revenue across all jobs in USD.
    pub total_revenue_usd: f64,
    /// Total retries absorbed across all jobs.
    pub total_retries_absorbed: u64,
    /// Rolling average savings percentage vs cloud.
    pub avg_savings_pct: f64,
    /// Total node-hours billed.
    pub total_node_hours: f64,
}

impl PricingStats {
    /// Average revenue per job.
    pub fn avg_revenue_per_job(&self) -> f64 {
        if self.total_jobs_billed == 0 {
            return 0.0;
        }
        self.total_revenue_usd / self.total_jobs_billed as f64
    }

    /// Average retries per job.
    pub fn avg_retries_per_job(&self) -> f64 {
        if self.total_jobs_billed == 0 {
            return 0.0;
        }
        self.total_retries_absorbed as f64 / self.total_jobs_billed as f64
    }

    /// Revenue per node-hour.
    pub fn revenue_per_node_hour(&self) -> f64 {
        if self.total_node_hours <= 0.0 {
            return 0.0;
        }
        self.total_revenue_usd / self.total_node_hours
    }

    /// Merge another stats snapshot into this one.
    pub fn merge(&mut self, other: &PricingStats) {
        let prev_jobs = self.total_jobs_billed;
        self.total_jobs_billed += other.total_jobs_billed;
        self.total_revenue_usd += other.total_revenue_usd;
        self.total_retries_absorbed += other.total_retries_absorbed;
        self.total_node_hours += other.total_node_hours;
        // Weighted average of savings
        if self.total_jobs_billed > 0 {
            self.avg_savings_pct = (self.avg_savings_pct * prev_jobs as f64
                + other.avg_savings_pct * other.total_jobs_billed as f64)
                / self.total_jobs_billed as f64;
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// ModulePricingProfile
// ============================================================================

/// Pre-computed pricing hints for a specific CR compute module.
///
/// These profiles are used by [`PricingEngine::estimate_for_module`] to
/// provide more accurate cost estimates for known workload types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModulePricingProfile {
    /// Module identifier (e.g. "cr.montecarlo").
    pub module_id: String,
    /// Base cost per item in USD (at Standard priority).
    pub base_cost_per_item_usd: f64,
    /// Typical wall-clock duration per item in milliseconds.
    pub typical_item_duration_ms: u64,
    /// Typical memory usage per item in MB.
    pub memory_per_item_mb: u64,
    /// Cloud-equivalent cost per item in USD (for comparison).
    pub cloud_cost_per_item_usd: f64,
    /// Whether this module supports Economy scheduling.
    pub supports_economy: bool,
}

impl ModulePricingProfile {
    /// Create a new module pricing profile.
    pub fn new(
        module_id: impl Into<String>,
        base_cost_per_item_usd: f64,
        typical_item_duration_ms: u64,
        memory_per_item_mb: u64,
        cloud_cost_per_item_usd: f64,
        supports_economy: bool,
    ) -> Self {
        Self {
            module_id: module_id.into(),
            base_cost_per_item_usd,
            typical_item_duration_ms,
            memory_per_item_mb,
            cloud_cost_per_item_usd,
            supports_economy,
        }
    }

    /// Estimated cost for a given number of items at the given priority.
    pub fn estimate_cost(&self, items: u64, priority: PriorityLevel) -> f64 {
        self.base_cost_per_item_usd * items as f64 * priority.cost_multiplier()
    }

    /// Cloud-equivalent cost for a given number of items.
    pub fn cloud_cost(&self, items: u64) -> f64 {
        self.cloud_cost_per_item_usd * items as f64
    }

    /// Savings percentage for a given number of items at the given priority.
    pub fn savings_pct(&self, items: u64, priority: PriorityLevel) -> f64 {
        let cr = self.estimate_cost(items, priority);
        let cloud = self.cloud_cost(items);
        if cloud <= 0.0 {
            return 0.0;
        }
        ((cloud - cr) / cloud) * 100.0
    }

    /// Estimated total duration for the given number of items, assuming
    /// parallelism across the given number of effective nodes.
    pub fn estimated_duration_secs(&self, items: u64, effective_nodes: u64) -> u64 {
        let nodes = effective_nodes.max(1);
        let total_ms = items.saturating_mul(self.typical_item_duration_ms);
        let parallel_ms = total_ms / nodes;
        (parallel_ms / 1000).max(1)
    }

    /// Return the default pricing profiles for all 22 CR modules.
    ///
    /// These values are empirically derived from benchmark data and represent
    /// typical costs for representative workloads.
    pub fn default_profiles() -> HashMap<String, ModulePricingProfile> {
        let mut m = HashMap::new();

        // Computation modules
        m.insert("cr.montecarlo".into(), Self::new(
            "cr.montecarlo", 0.0000015, 50, 64, 0.0000080, true,
        ));
        m.insert("cr.brute".into(), Self::new(
            "cr.brute", 0.0000040, 200, 128, 0.0000200, true,
        ));
        m.insert("cr.evolve".into(), Self::new(
            "cr.evolve", 0.0000025, 150, 256, 0.0000120, true,
        ));
        m.insert("cr.crunch".into(), Self::new(
            "cr.crunch", 0.0000020, 100, 128, 0.0000100, true,
        ));

        // Data processing modules
        m.insert("cr.classify".into(), Self::new(
            "cr.classify", 0.0000030, 80, 512, 0.0000150, true,
        ));
        m.insert("cr.validate".into(), Self::new(
            "cr.validate", 0.0000010, 30, 64, 0.0000050, true,
        ));

        // Media modules
        m.insert("cr.render".into(), Self::new(
            "cr.render", 0.0000080, 500, 1024, 0.0000400, false,
        ));
        m.insert("cr.transcode".into(), Self::new(
            "cr.transcode", 0.0000050, 300, 512, 0.0000250, false,
        ));
        m.insert("cr.ocr".into(), Self::new(
            "cr.ocr", 0.0000025, 120, 256, 0.0000120, true,
        ));

        // Science modules
        m.insert("cr.dock".into(), Self::new(
            "cr.dock", 0.0000100, 1000, 2048, 0.0000500, true,
        ));
        m.insert("cr.sweep".into(), Self::new(
            "cr.sweep", 0.0000035, 180, 256, 0.0000170, true,
        ));
        m.insert("cr.replicate".into(), Self::new(
            "cr.replicate", 0.0000020, 90, 128, 0.0000100, true,
        ));
        m.insert("cr.bootstrap".into(), Self::new(
            "cr.bootstrap", 0.0000018, 70, 128, 0.0000090, true,
        ));

        // AI / ML modules
        m.insert("cr.inference".into(), Self::new(
            "cr.inference", 0.0000060, 250, 1024, 0.0000300, false,
        ));
        m.insert("cr.embed".into(), Self::new(
            "cr.embed", 0.0000040, 150, 512, 0.0000200, true,
        ));
        m.insert("cr.tune".into(), Self::new(
            "cr.tune", 0.0000120, 800, 2048, 0.0000600, false,
        ));
        m.insert("cr.eval".into(), Self::new(
            "cr.eval", 0.0000030, 100, 256, 0.0000150, true,
        ));

        // Finance modules
        m.insert("cr.risk".into(), Self::new(
            "cr.risk", 0.0000035, 120, 256, 0.0000170, true,
        ));
        m.insert("cr.price".into(), Self::new(
            "cr.price", 0.0000028, 100, 128, 0.0000140, true,
        ));

        // Enterprise modules
        m.insert("cr.match".into(), Self::new(
            "cr.match", 0.0000022, 80, 256, 0.0000110, true,
        ));
        m.insert("cr.route".into(), Self::new(
            "cr.route", 0.0000018, 60, 128, 0.0000090, true,
        ));
        m.insert("cr.scan".into(), Self::new(
            "cr.scan", 0.0000015, 50, 128, 0.0000075, true,
        ));
        m.insert("cr.grade".into(), Self::new(
            "cr.grade", 0.0000012, 40, 64, 0.0000060, true,
        ));

        m
    }

    /// Look up a module profile by ID, returning None if unknown.
    pub fn lookup(module_id: &str) -> Option<ModulePricingProfile> {
        Self::default_profiles().remove(module_id)
    }

    /// List all known module IDs.
    pub fn all_module_ids() -> Vec<String> {
        Self::default_profiles().keys().cloned().collect()
    }
}

// ============================================================================
// EconomyScheduler
// ============================================================================

/// Determines whether the current time is within the off-peak window
/// for Economy-tier jobs.
///
/// Off-peak hours are defined by [`SCHEDULER_OFFPEAK_START_HOUR`] and
/// [`SCHEDULER_OFFPEAK_END_HOUR`] in UTC. Economy jobs are only dispatched
/// during off-peak windows to fill idle capacity at reduced cost.
pub struct EconomyScheduler;

impl EconomyScheduler {
    /// Check if the current UTC time is within the off-peak window.
    pub fn is_offpeak_now() -> bool {
        let now = Utc::now();
        Self::is_offpeak_at(now.hour())
    }

    /// Check if a specific UTC hour falls within the off-peak window.
    ///
    /// The window wraps around midnight. For example, with start=22 and end=6,
    /// hours 22, 23, 0, 1, 2, 3, 4, 5 are off-peak.
    pub fn is_offpeak_at(hour_utc: u32) -> bool {
        let start = SCHEDULER_OFFPEAK_START_HOUR;
        let end = SCHEDULER_OFFPEAK_END_HOUR;

        if start <= end {
            // Simple range: e.g., 2..6
            hour_utc >= start && hour_utc < end
        } else {
            // Wraps midnight: e.g., 22..6 means 22..24 or 0..6
            hour_utc >= start || hour_utc < end
        }
    }

    /// Return the next off-peak window as (start, end) DateTimes.
    ///
    /// If currently in an off-peak window, returns the current window's
    /// boundaries. Otherwise returns the next upcoming window.
    pub fn next_offpeak_window() -> (DateTime<Utc>, DateTime<Utc>) {
        let now = Utc::now();
        let current_hour = now.hour();
        let start_hour = SCHEDULER_OFFPEAK_START_HOUR;
        let end_hour = SCHEDULER_OFFPEAK_END_HOUR;

        let today = now.date_naive();

        if Self::is_offpeak_at(current_hour) {
            // We are currently in an off-peak window. Determine its bounds.
            if start_hour > end_hour {
                // Wraps midnight
                if current_hour >= start_hour {
                    // We're in the pre-midnight portion
                    let start_dt = today.and_hms_opt(start_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    let end_dt = (today + ChronoDuration::days(1))
                        .and_hms_opt(end_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    (start_dt, end_dt)
                } else {
                    // We're in the post-midnight portion
                    let start_dt = (today - ChronoDuration::days(1))
                        .and_hms_opt(start_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    let end_dt = today.and_hms_opt(end_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    (start_dt, end_dt)
                }
            } else {
                // Does not wrap
                let start_dt = today.and_hms_opt(start_hour, 0, 0)
                    .unwrap()
                    .and_utc();
                let end_dt = today.and_hms_opt(end_hour, 0, 0)
                    .unwrap()
                    .and_utc();
                (start_dt, end_dt)
            }
        } else {
            // We are not in an off-peak window. Find the next one.
            if start_hour > end_hour {
                // Wraps midnight: next window starts today at start_hour
                // if we haven't passed it yet, otherwise tomorrow
                if current_hour < start_hour {
                    let start_dt = today.and_hms_opt(start_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    let end_dt = (today + ChronoDuration::days(1))
                        .and_hms_opt(end_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    (start_dt, end_dt)
                } else {
                    // Shouldn't happen since if hour >= start and wraps, we'd be offpeak
                    // But handle gracefully: next window is tomorrow
                    let tomorrow = today + ChronoDuration::days(1);
                    let start_dt = tomorrow.and_hms_opt(start_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    let end_dt = (tomorrow + ChronoDuration::days(1))
                        .and_hms_opt(end_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    (start_dt, end_dt)
                }
            } else {
                // Simple range
                if current_hour < start_hour {
                    let start_dt = today.and_hms_opt(start_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    let end_dt = today.and_hms_opt(end_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    (start_dt, end_dt)
                } else {
                    // Next window is tomorrow
                    let tomorrow = today + ChronoDuration::days(1);
                    let start_dt = tomorrow.and_hms_opt(start_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    let end_dt = tomorrow.and_hms_opt(end_hour, 0, 0)
                        .unwrap()
                        .and_utc();
                    (start_dt, end_dt)
                }
            }
        }
    }

    /// Return the off-peak hour boundaries as (start_hour, end_hour).
    pub fn offpeak_hours() -> (u32, u32) {
        (SCHEDULER_OFFPEAK_START_HOUR, SCHEDULER_OFFPEAK_END_HOUR)
    }

    /// Duration of the off-peak window in hours.
    pub fn offpeak_duration_hours() -> u32 {
        let start = SCHEDULER_OFFPEAK_START_HOUR;
        let end = SCHEDULER_OFFPEAK_END_HOUR;
        if start <= end {
            end - start
        } else {
            (24 - start) + end
        }
    }

    /// Fraction of the day that is off-peak.
    pub fn offpeak_fraction() -> f64 {
        Self::offpeak_duration_hours() as f64 / 24.0
    }

    /// Hours until the next off-peak window begins.
    /// Returns 0 if currently off-peak.
    pub fn hours_until_offpeak() -> u32 {
        let now = Utc::now();
        let current_hour = now.hour();
        if Self::is_offpeak_at(current_hour) {
            return 0;
        }
        let start = SCHEDULER_OFFPEAK_START_HOUR;
        if current_hour < start {
            start - current_hour
        } else {
            (24 - current_hour) + start
        }
    }
}

// ============================================================================
// PricingEngine
// ============================================================================

/// Main cost estimation and billing engine for the Marabunta Swarm.
///
/// Provides pre-submission cost estimates, real-time billing tracking,
/// receipt generation, and cloud-comparison analysis.
///
/// # Example
///
/// ```ignore
/// let engine = PricingEngine::new();
/// let request = CostEstimateRequest::new(10_000)
///     .with_priority(PriorityLevel::Standard)
///     .with_verification(VerificationStrategy::SpotCheck { check_rate: 0.05 });
/// let estimate = engine.estimate_cost(&request);
/// let billing = engine.start_billing(JobId::new(), &estimate);
/// ```
pub struct PricingEngine {
    /// Optional reference to the swarm's knowledge store for live state queries.
    knowledge: Option<Arc<KnowledgeStore>>,
    /// The active rate card.
    rate_card: RateCard,
    /// Active billing records, keyed by job ID.
    active_jobs: DashMap<JobId, JobBilling>,
    /// Aggregate statistics.
    stats: parking_lot::RwLock<PricingStats>,
    /// Cached module pricing profiles.
    module_profiles: HashMap<String, ModulePricingProfile>,
}

impl PricingEngine {
    /// Create a new pricing engine with default settings.
    pub fn new() -> Self {
        Self {
            knowledge: None,
            rate_card: RateCard::default_card(),
            active_jobs: DashMap::new(),
            stats: parking_lot::RwLock::new(PricingStats::default()),
            module_profiles: ModulePricingProfile::default_profiles(),
        }
    }

    /// Builder: attach a knowledge store for live swarm queries.
    pub fn with_knowledge(mut self, knowledge: Arc<KnowledgeStore>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// Builder: set a custom rate card.
    pub fn with_rate_card(mut self, rate_card: RateCard) -> Self {
        self.rate_card = rate_card;
        self
    }

    /// Return a reference to the active rate card.
    pub fn rate_card(&self) -> &RateCard {
        &self.rate_card
    }

    /// Return the number of active billing records.
    pub fn active_job_count(&self) -> usize {
        self.active_jobs.len()
    }

    /// Computes the dynamic Automated Market Maker (AMM) multiplier based on realtime swarm saturation.
    /// Fulfills the "spot market" economic fantasy by scaling prices exponentially as available compute drops.
    pub fn calculate_spot_multiplier(&self) -> f64 {
        if let Some(ref knowledge) = self.knowledge {
            let live_nodes = knowledge.get_live_nodes();
            if live_nodes.is_empty() { return 1.0; }
            
            let mut total_load = 0.0;
            for node in &live_nodes {
                total_load += node.load as f64;
            }
            let saturation = total_load / live_nodes.len() as f64;
            
        // Mathematical AMM Surge Curve: e^(5 * (saturation - 0.7)) 
        // If saturation is 0.0 (empty), price multiplier is ~0.03x
        // If saturation is 0.7 (normal), price multiplier is 1.0x
        // If saturation is 1.0 (overloaded), price multiplier is 4.48x
        let surge = (5.0 * (saturation - 0.7)).exp();
        
        let mut final_multiplier = surge.clamp(0.1, 100.0);

        // Pillar 15.4: Quantum Scarcity Premium
        // If the swarm is heavily saturated, specialized quantum gates 
        // become exponentially more expensive to prevent queue flooding.
        if saturation > 0.8 {
            let q_premium = crate::swarm::quantum_accelerator::calculate_quantum_premium(saturation);
            final_multiplier *= q_premium;
            tracing::info!(
                premium = q_premium,
                "Applying Quantum Scarcity Premium to compute bid."
            );
        }
        
        final_multiplier
        } else {
            1.0 // Unchanged for unit tests and fallback
        }
    }

    // ========================================================================
    // Cost Estimation
    // ========================================================================

    /// Compute a cost estimate for a job before submission.
    ///
    /// Takes into account:
    /// - Total items and per-item duration
    /// - Priority multiplier (Rush 3x, Standard 1x, Economy 0.5x)
    /// - Verification overhead (Redundant, SpotCheck, Statistical, None)
    /// - Retry overhead (default 20%)
    /// - Transfer costs based on item size
    /// - Effective node count from target tier or live knowledge store
    pub fn estimate_cost(&self, request: &CostEstimateRequest) -> CostEstimate {
        // Determine effective nodes
        let effective_nodes = self.resolve_effective_nodes(request.target_tier);

        // If there's a module profile, use module-aware estimation
        if let Some(ref module_id) = request.module_id {
            if let Some(profile) = self.module_profiles.get(module_id) {
                return self.estimate_from_profile(profile, request, effective_nodes);
            }
        }

        // Generic estimation path
        let total_compute_hours = request.total_compute_hours();
        let transfer_gb = request.total_transfer_bytes() as f64 / (1024.0 * 1024.0 * 1024.0);

        // Base cost using blended rate and Dynamic AMM multiplier
        let rate = self.rate_card.blended_hourly_rate();
        let base_cost = (rate * total_compute_hours + transfer_gb * 0.0002) * self.calculate_spot_multiplier();

        // Priority multiplier
        let priority_multiplier = request.priority.cost_multiplier();
        let priority_cost = base_cost * priority_multiplier;

        // Verification overhead
        let verification_pct = self.verification_overhead_pct(&request.verification);
        let verification_cost = priority_cost * verification_pct;

        // Retry overhead
        let retry_pct = PRICING_RETRY_OVERHEAD_PCT;
        let retry_cost = priority_cost * retry_pct;

        // Total before fees
        let subtotal = priority_cost + verification_cost + retry_cost;

        // Transaction fee
        let total = self.rate_card.apply_transaction_fee(subtotal);
        let total = self.rate_card.enforce_minimum(total);

        // Estimated chunks (1 chunk per item for simplicity, capped at effective_nodes)
        let estimated_chunks = request.total_items.max(1);

        // Duration estimate: total compute time / effective_nodes
        let total_compute_secs = request.total_compute_ms() / 1000;
        let estimated_duration = if effective_nodes > 0 {
            total_compute_secs / effective_nodes
        } else {
            total_compute_secs
        }.max(1);

        // Cost range: +-30% for generic estimation
        let cost_low = total * 0.7;
        let cost_high = total * 1.3;

        // Duration range: +-40%
        let dur_low = (estimated_duration as f64 * 0.6) as u64;
        let dur_high = (estimated_duration as f64 * 1.4) as u64;

        // Cloud comparison
        let cloud_cost = self.estimate_cloud_cost_generic(request);

        CostEstimate {
            estimated_cost_usd: total,
            cost_range_usd: (cost_low, cost_high),
            estimated_duration_secs: estimated_duration,
            duration_range_secs: (dur_low.max(1), dur_high.max(1)),
            estimated_chunks,
            retry_overhead_pct: retry_pct * 100.0,
            verification_overhead_pct: verification_pct * 100.0,
            cloud_comparison_usd: Some(cloud_cost),
            priority: request.priority,
            effective_nodes,
        }
    }

    /// Estimate cost for a specific module given item count and priority.
    ///
    /// This is a convenience wrapper around `estimate_cost` that constructs
    /// the request from the module's default parameters.
    pub fn estimate_for_module(
        &self,
        module_id: &str,
        total_items: u64,
        priority: PriorityLevel,
    ) -> CostEstimate {
        let request = if let Some(profile) = self.module_profiles.get(module_id) {
            CostEstimateRequest {
                total_items,
                item_size_bytes: profile.memory_per_item_mb * 1024 * 1024,
                per_item_duration_ms: profile.typical_item_duration_ms,
                priority,
                verification: VerificationStrategy::SpotCheck { check_rate: VERIFICATION_SPOT_CHECK_RATE },
                target_tier: None,
                module_id: Some(module_id.to_string()),
            }
        } else {
            CostEstimateRequest {
                total_items,
                per_item_duration_ms: 100,
                priority,
                module_id: Some(module_id.to_string()),
                ..Default::default()
            }
        };

        self.estimate_cost(&request)
    }

    // ========================================================================
    // Billing
    // ========================================================================

    /// Begin billing for a job. Returns the initial billing record.
    pub fn start_billing(&self, job_id: JobId, estimate: &CostEstimate) -> JobBilling {
        let billing = JobBilling::new(job_id, estimate.clone());
        self.active_jobs.insert(job_id, billing.clone());
        info!(job = %job_id, estimate_usd = estimate.estimated_cost_usd, "pricing: billing started");
        billing
    }

    /// Record the cost of a completed chunk.
    ///
    /// If `was_retry` is true, the cost is absorbed (not added to the bill
    /// but tracked for transparency).
    pub fn record_chunk_cost(
        &self,
        job_id: &JobId,
        node_class: NodeClass,
        duration_ms: u64,
        was_retry: bool,
    ) {
        if let Some(mut billing) = self.active_jobs.get_mut(job_id) {
            let rate = self.rate_card.rate_for(node_class);
            let duration_hours = duration_ms as f64 / 3_600_000.0;
            let chunk_cost = (rate.usd_per_hour * duration_hours + rate.usd_per_chunk) * self.calculate_spot_multiplier();

            if was_retry {
                billing.retries_absorbed += 1;
                debug!(
                    job = %job_id,
                    class = %node_class,
                    cost = chunk_cost,
                    "pricing: retry absorbed (not charged)"
                );
            } else {
                billing.chunks_billed += 1;
                billing.total_cost_usd += chunk_cost;
                *billing.cost_by_class.entry(node_class).or_insert(0.0) += chunk_cost;
                debug!(
                    job = %job_id,
                    class = %node_class,
                    cost = chunk_cost,
                    total = billing.total_cost_usd,
                    "pricing: chunk cost recorded"
                );
            }
        } else {
            warn!(job = %job_id, "pricing: record_chunk_cost called for unknown job");
        }
    }

    /// Finalize billing for a completed job and generate a receipt.
    ///
    /// Removes the billing record from the active set, updates aggregate
    /// stats, and returns the final receipt.
    pub fn finalize_billing(&self, job_id: &JobId) -> Option<JobReceipt> {
        let (_, billing) = self.active_jobs.remove(job_id)?;

        // Apply transaction fee and minimum
        let final_cost = self.rate_card.enforce_minimum(
            self.rate_card.apply_transaction_fee(billing.total_cost_usd),
        );

        // Cloud comparison for savings
        let cloud_cost = billing.estimate.cloud_comparison_usd.unwrap_or(0.0);
        let savings = if cloud_cost > final_cost {
            cloud_cost - final_cost
        } else {
            0.0
        };

        let receipt = JobReceipt {
            receipt_id: Uuid::new_v4().to_string(),
            job_id: *job_id,
            started_at: billing.started_at,
            completed_at: Utc::now(),
            total_cost_usd: final_cost,
            estimated_cost_usd: billing.estimate.estimated_cost_usd,
            chunks_processed: billing.chunks_billed,
            retries_absorbed: billing.retries_absorbed,
            verification_cost_usd: billing.verification_cost_usd,
            cost_by_class: billing.cost_by_class,
            savings_vs_cloud_usd: savings,
            priority: billing.estimate.priority,
        };

        // Update aggregate stats
        {
            let mut stats = self.stats.write();
            stats.total_jobs_billed += 1;
            stats.total_revenue_usd += final_cost;
            stats.total_retries_absorbed += billing.retries_absorbed;

            let elapsed_hours = receipt.duration_secs() as f64 / 3600.0;
            stats.total_node_hours += elapsed_hours;

            // Rolling average savings
            let savings_pct = receipt.savings_pct();
            let prev_jobs = stats.total_jobs_billed - 1;
            if stats.total_jobs_billed > 0 {
                stats.avg_savings_pct = (stats.avg_savings_pct * prev_jobs as f64 + savings_pct)
                    / stats.total_jobs_billed as f64;
            }
        }

        info!(
            job = %job_id,
            cost_usd = final_cost,
            chunks = receipt.chunks_processed,
            retries = receipt.retries_absorbed,
            savings_usd = savings,
            "pricing: billing finalized"
        );

        Some(receipt)
    }

    /// Look up the current billing record for an active job.
    pub fn get_billing(&self, job_id: &JobId) -> Option<JobBilling> {
        self.active_jobs.get(job_id).map(|r| r.value().clone())
    }

    // ========================================================================
    // Cloud Comparison
    // ========================================================================

    /// Generate a detailed cloud cost comparison for a module workload.
    pub fn compare_to_cloud(
        &self,
        module_id: &str,
        total_items: u64,
    ) -> CloudComparison {
        let effective_nodes = self.resolve_effective_nodes(None);

        let (cr_cost, cr_time) = if let Some(profile) = self.module_profiles.get(module_id) {
            let cost = profile.estimate_cost(total_items, PriorityLevel::Standard);
            let time = profile.estimated_duration_secs(total_items, effective_nodes);
            (cost, time)
        } else {
            // Fallback for unknown modules
            let rate = self.rate_card.blended_hourly_rate();
            let hours = (total_items as f64 * 0.1) / 3600.0; // 100ms per item default
            let cost = rate * hours;
            let time = if effective_nodes > 0 {
                (total_items * 100 / 1000) / effective_nodes
            } else {
                total_items * 100 / 1000
            }.max(1);
            (cost, time)
        };

        // Cloud cost multipliers (empirically derived)
        let aws_multiplier = 5.0;
        let azure_multiplier = 5.2;
        let gcp_multiplier = 4.8;

        let aws_cost = cr_cost * aws_multiplier;
        let azure_cost = cr_cost * azure_multiplier;
        let gcp_cost = cr_cost * gcp_multiplier;

        // Cloud typically runs on fewer, bigger machines -- slower for embarrassingly parallel
        let cloud_time = (cr_time as f64 * 2.5) as u64;

        let cheapest_cloud = aws_cost.min(azure_cost).min(gcp_cost);
        let savings_pct = if cheapest_cloud > 0.0 {
            ((cheapest_cloud - cr_cost) / cheapest_cloud) * 100.0
        } else {
            0.0
        };

        CloudComparison {
            cr_cost_usd: cr_cost,
            aws_cost_usd: aws_cost,
            azure_cost_usd: azure_cost,
            gcp_cost_usd: gcp_cost,
            savings_pct,
            cr_time_estimate: cr_time,
            cloud_time_estimate: cloud_time,
        }
    }

    // ========================================================================
    // Stats
    // ========================================================================

    /// Return a snapshot of the aggregate pricing statistics.
    pub fn get_stats(&self) -> PricingStats {
        self.stats.read().clone()
    }

    /// Reset aggregate pricing statistics.
    pub fn reset_stats(&self) {
        self.stats.write().reset();
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Look up a module pricing profile by ID.
    pub fn get_module_profile(&self, module_id: &str) -> Option<&ModulePricingProfile> {
        self.module_profiles.get(module_id)
    }

    /// Return all known module IDs.
    pub fn known_modules(&self) -> Vec<String> {
        self.module_profiles.keys().cloned().collect()
    }

    /// Determine the verification overhead as a fraction (0.0 to 1.0+).
    fn verification_overhead_pct(&self, strategy: &VerificationStrategy) -> f64 {
        match strategy {
            VerificationStrategy::Redundant { replicas } => {
                // Each replica costs approximately the same as the original
                (*replicas as f64 - 1.0).max(0.0)
            }
            VerificationStrategy::SpotCheck { check_rate } => {
                // Spot checks are cheap: only a fraction of chunks are re-executed
                *check_rate
            }
            VerificationStrategy::Statistical { .. } => {
                // Statistical validation has minimal compute overhead
                0.05
            }
            VerificationStrategy::None => 0.0,
        }
    }

    /// Resolve the effective node count from the target tier or live knowledge.
    fn resolve_effective_nodes(&self, target_tier: Option<SwarmTier>) -> u64 {
        if let Some(tier) = target_tier {
            return tier.effective_nodes();
        }

        // Try to get live node count from knowledge store
        if let Some(ref knowledge) = self.knowledge {
            let live_count = knowledge.get_live_nodes().len() as u64;
            if live_count > 0 {
                return live_count;
            }
        }

        // Fallback: assume a small swarm
        SwarmTier::Cell.effective_nodes()
    }

    /// Estimate cost using a module profile.
    fn estimate_from_profile(
        &self,
        profile: &ModulePricingProfile,
        request: &CostEstimateRequest,
        effective_nodes: u64,
    ) -> CostEstimate {
        let base_cost = profile.base_cost_per_item_usd * request.total_items as f64 * self.calculate_spot_multiplier();
        let priority_cost = base_cost * request.priority.cost_multiplier();

        let verification_pct = self.verification_overhead_pct(&request.verification);
        let verification_cost = priority_cost * verification_pct;

        let retry_pct = PRICING_RETRY_OVERHEAD_PCT;
        let retry_cost = priority_cost * retry_pct;

        let subtotal = priority_cost + verification_cost + retry_cost;
        let total = self.rate_card.apply_transaction_fee(subtotal);
        let total = self.rate_card.enforce_minimum(total);

        let estimated_chunks = request.total_items.max(1);

        let estimated_duration = profile.estimated_duration_secs(request.total_items, effective_nodes);

        // Tighter cost range for module-aware estimation (+-20%)
        let cost_low = total * 0.8;
        let cost_high = total * 1.2;

        let dur_low = (estimated_duration as f64 * 0.7) as u64;
        let dur_high = (estimated_duration as f64 * 1.3) as u64;

        let cloud_cost = profile.cloud_cost(request.total_items);

        CostEstimate {
            estimated_cost_usd: total,
            cost_range_usd: (cost_low, cost_high),
            estimated_duration_secs: estimated_duration,
            duration_range_secs: (dur_low.max(1), dur_high.max(1)),
            estimated_chunks,
            retry_overhead_pct: retry_pct * 100.0,
            verification_overhead_pct: verification_pct * 100.0,
            cloud_comparison_usd: Some(cloud_cost),
            priority: request.priority,
            effective_nodes,
        }
    }

    /// Estimate cloud cost for a generic (non-module) workload.
    fn estimate_cloud_cost_generic(&self, request: &CostEstimateRequest) -> f64 {
        // Cloud cost heuristic: 5x the base CR cost (no priority multiplier)
        let total_compute_hours = request.total_compute_hours();
        let rate = self.rate_card.blended_hourly_rate();
        let base = rate * total_compute_hours;
        base * 5.0
    }

    /// Estimate the total cost for a batch of items at a specific node class.
    pub fn estimate_class_cost(
        &self,
        node_class: NodeClass,
        total_items: u64,
        per_item_duration_ms: u64,
        priority: PriorityLevel,
    ) -> f64 {
        let rate = self.rate_card.rate_for(node_class);
        let total_hours = (total_items as f64 * per_item_duration_ms as f64) / 3_600_000.0;
        let base = (rate.usd_per_hour * total_hours + rate.usd_per_chunk * total_items as f64) * self.calculate_spot_multiplier();
        let with_priority = base * priority.cost_multiplier();
        self.rate_card.apply_transaction_fee(with_priority)
    }

    /// Check if a job's current cost has exceeded its estimate by the given threshold.
    pub fn is_over_budget(&self, job_id: &JobId, threshold_pct: f64) -> bool {
        if let Some(billing) = self.active_jobs.get(job_id) {
            let ratio = billing.budget_ratio();
            ratio > (1.0 + threshold_pct / 100.0)
        } else {
            false
        }
    }

    /// Return all active billing records.
    pub fn all_active_billings(&self) -> Vec<JobBilling> {
        self.active_jobs.iter().map(|r| r.value().clone()).collect()
    }

    /// Cancel billing for a job without generating a receipt.
    pub fn cancel_billing(&self, job_id: &JobId) -> Option<JobBilling> {
        self.active_jobs.remove(job_id).map(|(_, b)| b)
    }

    /// Compute a quick cost estimate for a single chunk at the given class.
    pub fn single_chunk_cost(&self, node_class: NodeClass, duration_ms: u64) -> f64 {
        let rate = self.rate_card.rate_for(node_class);
        let hours = duration_ms as f64 / 3_600_000.0;
        (rate.usd_per_hour * hours + rate.usd_per_chunk) * self.calculate_spot_multiplier()
    }

    /// Estimate the hourly burn rate for a job based on its current billing rate.
    pub fn hourly_burn_rate(&self, job_id: &JobId) -> f64 {
        if let Some(billing) = self.active_jobs.get(job_id) {
            let elapsed_secs = billing.elapsed_secs();
            if elapsed_secs <= 0 {
                return 0.0;
            }
            billing.total_cost_usd / (elapsed_secs as f64 / 3600.0)
        } else {
            0.0
        }
    }

    /// Estimate the remaining cost for a job based on current burn rate.
    pub fn estimated_remaining_cost(&self, job_id: &JobId) -> f64 {
        if let Some(billing) = self.active_jobs.get(job_id) {
            let remaining_chunks = billing.estimate.estimated_chunks
                .saturating_sub(billing.chunks_billed);
            billing.avg_cost_per_chunk() * remaining_chunks as f64
        } else {
            0.0
        }
    }

    /// Return the top N most expensive active jobs by current cost.
    pub fn top_active_by_cost(&self, n: usize) -> Vec<JobBilling> {
        let mut billings: Vec<JobBilling> = self.active_jobs
            .iter()
            .map(|r| r.value().clone())
            .collect();
        billings.sort_by(|a, b| {
            b.total_cost_usd.partial_cmp(&a.total_cost_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        billings.truncate(n);
        billings
    }
}

impl Default for PricingEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Utility functions
// ============================================================================

/// Format a USD amount for display, with appropriate precision.
pub fn format_usd(amount: f64) -> String {
    if amount < 0.001 {
        format!("${:.8}", amount)
    } else if amount < 1.0 {
        format!("${:.6}", amount)
    } else if amount < 100.0 {
        format!("${:.4}", amount)
    } else {
        format!("${:.2}", amount)
    }
}

/// Format a duration in seconds for display.
pub fn format_duration_secs(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m {}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    }
}

/// Convert a byte count to a human-readable GB string.
pub fn bytes_to_gb_str(bytes: u64) -> String {
    let gb = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    if gb < 0.001 {
        format!("{:.6} GB", gb)
    } else {
        format!("{:.3} GB", gb)
    }
}

/// Compute the percentage difference between two values.
pub fn pct_diff(a: f64, b: f64) -> f64 {
    if b == 0.0 {
        return 0.0;
    }
    ((a - b) / b) * 100.0
}

/// Clamp a value to a range.
pub fn clamp_f64(val: f64, min: f64, max: f64) -> f64 {
    val.max(min).min(max)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ====================================================================
    // RateCard tests
    // ====================================================================

    #[test]
    fn test_default_rate_card_has_all_classes() {
        let card = RateCard::default_card();
        for class in NodeClass::all() {
            assert!(card.rates.contains_key(class), "missing rate for {:?}", class);
        }
    }

    #[test]
    fn test_default_rate_card_dust_cheapest() {
        let card = RateCard::default_card();
        let edge = card.rate_for(NodeClass::Edge).usd_per_hour;
        let enterprise = card.rate_for(NodeClass::Enterprise).usd_per_hour;
        assert!(edge < enterprise, "Edge should be cheaper than Enterprise");
    }

    #[test]
    fn test_rate_card_custom() {
        let mut rates = HashMap::new();
        rates.insert(NodeClass::Standard, ClassRate::new(0.01, 0.001, 0.0001));
        let card = RateCard::custom(rates);
        assert_eq!(card.rate_for(NodeClass::Standard).usd_per_hour, 0.01);
    }

    #[test]
    fn test_rate_card_blended_rate() {
        let card = RateCard::default_card();
        let blended = card.blended_hourly_rate();
        let cheapest = card.cheapest_hourly_rate();
        let most_expensive = card.most_expensive_hourly_rate();
        assert!(blended >= cheapest);
        assert!(blended <= most_expensive);
    }

    #[test]
    fn test_rate_card_transaction_fee() {
        let card = RateCard::default_card();
        let raw = 1.0;
        let with_fee = card.apply_transaction_fee(raw);
        assert!(with_fee > raw);
        assert!((with_fee - 1.02).abs() < 0.001, "expected ~1.02, got {}", with_fee);
    }

    #[test]
    fn test_rate_card_minimum_charge() {
        let card = RateCard::default_card();
        let tiny = 0.0000001;
        let enforced = card.enforce_minimum(tiny);
        assert_eq!(enforced, card.minimum_charge_usd);
    }

    #[test]
    fn test_rate_card_with_transaction_fee() {
        let card = RateCard::default_card().with_transaction_fee(0.10);
        assert_eq!(card.transaction_fee_pct, 0.10);
        let with_fee = card.apply_transaction_fee(1.0);
        assert!((with_fee - 1.10).abs() < 0.001);
    }

    #[test]
    fn test_rate_card_with_minimum_charge() {
        let card = RateCard::default_card().with_minimum_charge(5.0);
        assert_eq!(card.minimum_charge_usd, 5.0);
        assert_eq!(card.enforce_minimum(1.0), 5.0);
    }

    #[test]
    fn test_class_rate_from_hourly() {
        let rate = ClassRate::from_hourly(1.0);
        assert_eq!(rate.usd_per_hour, 1.0);
        assert!((rate.usd_per_gb_transfer - 0.1).abs() < 0.001);
        assert!((rate.usd_per_chunk - 1.0 / 3600.0).abs() < 0.0001);
    }

    #[test]
    fn test_class_rate_compute_cost() {
        let rate = ClassRate::new(1.0, 0.10, 0.001);
        let cost = rate.compute_cost(2.0, 5.0, 100);
        // 2*1.0 + 5*0.10 + 100*0.001 = 2.0 + 0.5 + 0.1 = 2.6
        assert!((cost - 2.6).abs() < 0.001, "expected ~2.6, got {}", cost);
    }

    #[test]
    fn test_class_rate_scaled() {
        let rate = ClassRate::new(1.0, 0.10, 0.001);
        let scaled = rate.scaled(3.0);
        assert!((scaled.usd_per_hour - 3.0).abs() < 0.001);
        assert!((scaled.usd_per_gb_transfer - 0.30).abs() < 0.001);
        assert!((scaled.usd_per_chunk - 0.003).abs() < 0.001);
    }

    // ====================================================================
    // ModulePricingProfile tests
    // ====================================================================

    #[test]
    fn test_all_22_modules_have_profiles() {
        let profiles = ModulePricingProfile::default_profiles();
        let expected_modules = vec![
            "cr.montecarlo", "cr.brute", "cr.evolve", "cr.crunch",
            "cr.classify", "cr.validate",
            "cr.render", "cr.transcode", "cr.ocr",
            "cr.dock", "cr.sweep", "cr.replicate", "cr.bootstrap",
            "cr.inference", "cr.embed", "cr.tune", "cr.eval",
            "cr.risk", "cr.price",
            "cr.match", "cr.route", "cr.scan", "cr.grade",
        ];
        assert_eq!(profiles.len(), 23, "expected 23 modules (22 + cr.grade)");
        for module in &expected_modules {
            assert!(profiles.contains_key(*module), "missing profile for {}", module);
        }
    }

    #[test]
    fn test_module_profiles_have_positive_costs() {
        let profiles = ModulePricingProfile::default_profiles();
        for (id, profile) in &profiles {
            assert!(profile.base_cost_per_item_usd > 0.0, "{} has zero base cost", id);
            assert!(profile.cloud_cost_per_item_usd > 0.0, "{} has zero cloud cost", id);
            assert!(profile.typical_item_duration_ms > 0, "{} has zero duration", id);
        }
    }

    #[test]
    fn test_module_profiles_cr_cheaper_than_cloud() {
        let profiles = ModulePricingProfile::default_profiles();
        for (id, profile) in &profiles {
            assert!(
                profile.base_cost_per_item_usd < profile.cloud_cost_per_item_usd,
                "{}: CR cost ({}) should be less than cloud cost ({})",
                id, profile.base_cost_per_item_usd, profile.cloud_cost_per_item_usd,
            );
        }
    }

    #[test]
    fn test_module_estimate_cost() {
        let profile = ModulePricingProfile::new(
            "test.module", 0.001, 100, 64, 0.005, true,
        );
        let cost = profile.estimate_cost(1000, PriorityLevel::Standard);
        assert!((cost - 1.0).abs() < 0.001, "expected ~1.0, got {}", cost);

        let rush_cost = profile.estimate_cost(1000, PriorityLevel::Rush);
        assert!((rush_cost - 3.0).abs() < 0.001, "expected ~3.0, got {}", rush_cost);

        let economy_cost = profile.estimate_cost(1000, PriorityLevel::Economy);
        assert!((economy_cost - 0.5).abs() < 0.001, "expected ~0.5, got {}", economy_cost);
    }

    #[test]
    fn test_module_savings_pct() {
        let profile = ModulePricingProfile::new(
            "test.module", 0.001, 100, 64, 0.005, true,
        );
        let savings = profile.savings_pct(1000, PriorityLevel::Standard);
        assert!((savings - 80.0).abs() < 0.1, "expected ~80%, got {:.1}%", savings);
    }

    #[test]
    fn test_module_estimated_duration() {
        let profile = ModulePricingProfile::new(
            "test.module", 0.001, 100, 64, 0.005, true,
        );
        // 1000 items * 100ms = 100s total, / 10 nodes = 10s
        let dur = profile.estimated_duration_secs(1000, 10);
        assert_eq!(dur, 10);
    }

    #[test]
    fn test_module_lookup_known() {
        let profile = ModulePricingProfile::lookup("cr.montecarlo");
        assert!(profile.is_some());
        assert_eq!(profile.unwrap().module_id, "cr.montecarlo");
    }

    #[test]
    fn test_module_lookup_unknown() {
        let profile = ModulePricingProfile::lookup("cr.nonexistent");
        assert!(profile.is_none());
    }

    #[test]
    fn test_module_all_ids() {
        let ids = ModulePricingProfile::all_module_ids();
        assert!(ids.len() >= 22);
        assert!(ids.contains(&"cr.montecarlo".to_string()));
    }

    // ====================================================================
    // CostEstimateRequest tests
    // ====================================================================

    #[test]
    fn test_request_defaults() {
        let req = CostEstimateRequest::default();
        assert_eq!(req.total_items, 1);
        assert_eq!(req.priority, PriorityLevel::Standard);
    }

    #[test]
    fn test_request_builders() {
        let req = CostEstimateRequest::new(5000)
            .with_item_size(1024)
            .with_duration_ms(200)
            .with_priority(PriorityLevel::Rush)
            .with_tier(SwarmTier::Legion)
            .with_module("cr.montecarlo".to_string());

        assert_eq!(req.total_items, 5000);
        assert_eq!(req.item_size_bytes, 1024);
        assert_eq!(req.per_item_duration_ms, 200);
        assert_eq!(req.priority, PriorityLevel::Rush);
        assert_eq!(req.target_tier, Some(SwarmTier::Legion));
        assert_eq!(req.module_id, Some("cr.montecarlo".to_string()));
    }

    #[test]
    fn test_request_total_transfer() {
        let req = CostEstimateRequest::new(1000).with_item_size(1024);
        assert_eq!(req.total_transfer_bytes(), 1_024_000);
    }

    #[test]
    fn test_request_total_compute_hours() {
        let req = CostEstimateRequest::new(36000).with_duration_ms(100);
        // 36000 * 100ms = 3,600,000ms = 1 hour
        assert!((req.total_compute_hours() - 1.0).abs() < 0.001);
    }

    // ====================================================================
    // CloudComparison tests
    // ====================================================================

    #[test]
    fn test_cloud_comparison_best_savings() {
        let comp = CloudComparison {
            cr_cost_usd: 1.0,
            aws_cost_usd: 5.0,
            azure_cost_usd: 5.5,
            gcp_cost_usd: 4.8,
            savings_pct: 0.0,
            cr_time_estimate: 60,
            cloud_time_estimate: 150,
        };
        let savings = comp.best_savings_pct();
        // cheapest cloud = GCP at 4.8, savings = (4.8 - 1.0) / 4.8 * 100 = 79.17%
        assert!((savings - 79.17).abs() < 0.1, "expected ~79.17%, got {:.2}%", savings);
    }

    #[test]
    fn test_cloud_comparison_cheapest() {
        let comp = CloudComparison {
            cr_cost_usd: 1.0,
            aws_cost_usd: 5.0,
            azure_cost_usd: 5.5,
            gcp_cost_usd: 4.8,
            savings_pct: 0.0,
            cr_time_estimate: 60,
            cloud_time_estimate: 150,
        };
        assert_eq!(comp.cheapest_cloud_cost(), 4.8);
        assert_eq!(comp.cheapest_cloud_name(), "GCP");
        assert_eq!(comp.most_expensive_cloud_cost(), 5.5);
    }

    #[test]
    fn test_cloud_comparison_savings_usd() {
        let comp = CloudComparison {
            cr_cost_usd: 1.0,
            aws_cost_usd: 5.0,
            azure_cost_usd: 5.5,
            gcp_cost_usd: 4.8,
            savings_pct: 0.0,
            cr_time_estimate: 60,
            cloud_time_estimate: 150,
        };
        assert!((comp.savings_usd() - 3.8).abs() < 0.001);
    }

    #[test]
    fn test_cloud_comparison_time_savings() {
        let comp = CloudComparison {
            cr_cost_usd: 1.0,
            aws_cost_usd: 5.0,
            azure_cost_usd: 5.5,
            gcp_cost_usd: 4.8,
            savings_pct: 0.0,
            cr_time_estimate: 60,
            cloud_time_estimate: 150,
        };
        assert_eq!(comp.time_savings_secs(), 90);
        assert!((comp.speedup_factor() - 2.5).abs() < 0.01);
    }

    #[test]
    fn test_cloud_comparison_summary_text() {
        let comp = CloudComparison {
            cr_cost_usd: 1.0,
            aws_cost_usd: 5.0,
            azure_cost_usd: 5.5,
            gcp_cost_usd: 4.8,
            savings_pct: 0.0,
            cr_time_estimate: 60,
            cloud_time_estimate: 150,
        };
        let summary = comp.summary_text();
        assert!(summary.contains("CR:"));
        assert!(summary.contains("AWS:"));
        assert!(summary.contains("Azure:"));
        assert!(summary.contains("GCP:"));
        assert!(summary.contains("Savings:"));
    }

    #[test]
    fn test_cloud_comparison_zero_cr_cost() {
        let comp = CloudComparison {
            cr_cost_usd: 0.0,
            aws_cost_usd: 5.0,
            azure_cost_usd: 5.5,
            gcp_cost_usd: 4.8,
            savings_pct: 0.0,
            cr_time_estimate: 0,
            cloud_time_estimate: 150,
        };
        assert_eq!(comp.best_savings_pct(), 100.0);
        assert_eq!(comp.speedup_factor(), 0.0);
    }

    // ====================================================================
    // EconomyScheduler tests
    // ====================================================================

    #[test]
    fn test_offpeak_hours_returned() {
        let (start, end) = EconomyScheduler::offpeak_hours();
        assert_eq!(start, SCHEDULER_OFFPEAK_START_HOUR);
        assert_eq!(end, SCHEDULER_OFFPEAK_END_HOUR);
    }

    #[test]
    fn test_offpeak_at_midnight() {
        // Midnight (0) is off-peak when window is 22..6
        assert!(EconomyScheduler::is_offpeak_at(0));
    }

    #[test]
    fn test_offpeak_at_3am() {
        assert!(EconomyScheduler::is_offpeak_at(3));
    }

    #[test]
    fn test_offpeak_at_5am() {
        assert!(EconomyScheduler::is_offpeak_at(5));
    }

    #[test]
    fn test_not_offpeak_at_noon() {
        assert!(!EconomyScheduler::is_offpeak_at(12));
    }

    #[test]
    fn test_not_offpeak_at_6am() {
        // 6 is the end boundary, so 6 is NOT off-peak (half-open: [22, 6))
        assert!(!EconomyScheduler::is_offpeak_at(6));
    }

    #[test]
    fn test_offpeak_at_22() {
        assert!(EconomyScheduler::is_offpeak_at(22));
    }

    #[test]
    fn test_offpeak_at_23() {
        assert!(EconomyScheduler::is_offpeak_at(23));
    }

    #[test]
    fn test_not_offpeak_at_10am() {
        assert!(!EconomyScheduler::is_offpeak_at(10));
    }

    #[test]
    fn test_offpeak_duration() {
        // 22 to 6 = 8 hours
        let dur = EconomyScheduler::offpeak_duration_hours();
        assert_eq!(dur, 8);
    }

    #[test]
    fn test_offpeak_fraction() {
        let frac = EconomyScheduler::offpeak_fraction();
        assert!((frac - 8.0 / 24.0).abs() < 0.001);
    }

    #[test]
    fn test_next_offpeak_window_returns_valid_range() {
        let (start, end) = EconomyScheduler::next_offpeak_window();
        assert!(end > start, "end should be after start");
        let duration = end - start;
        let hours = duration.num_hours();
        assert_eq!(hours, 8, "off-peak window should be 8 hours, got {}", hours);
    }

    // ====================================================================
    // PricingStats tests
    // ====================================================================

    #[test]
    fn test_pricing_stats_default() {
        let stats = PricingStats::default();
        assert_eq!(stats.total_jobs_billed, 0);
        assert_eq!(stats.total_revenue_usd, 0.0);
    }

    #[test]
    fn test_pricing_stats_avg_revenue() {
        let stats = PricingStats {
            total_jobs_billed: 10,
            total_revenue_usd: 5.0,
            ..Default::default()
        };
        assert!((stats.avg_revenue_per_job() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_pricing_stats_avg_retries() {
        let stats = PricingStats {
            total_jobs_billed: 10,
            total_retries_absorbed: 20,
            ..Default::default()
        };
        assert!((stats.avg_retries_per_job() - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_pricing_stats_revenue_per_node_hour() {
        let stats = PricingStats {
            total_revenue_usd: 10.0,
            total_node_hours: 5.0,
            ..Default::default()
        };
        assert!((stats.revenue_per_node_hour() - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_pricing_stats_merge() {
        let mut a = PricingStats {
            total_jobs_billed: 5,
            total_revenue_usd: 2.5,
            total_retries_absorbed: 3,
            avg_savings_pct: 70.0,
            total_node_hours: 10.0,
        };
        let b = PricingStats {
            total_jobs_billed: 5,
            total_revenue_usd: 2.5,
            total_retries_absorbed: 7,
            avg_savings_pct: 80.0,
            total_node_hours: 15.0,
        };
        a.merge(&b);
        assert_eq!(a.total_jobs_billed, 10);
        assert!((a.total_revenue_usd - 5.0).abs() < 0.001);
        assert_eq!(a.total_retries_absorbed, 10);
        assert_eq!(a.total_node_hours, 25.0);
        // Weighted average: (70*5 + 80*5) / 10 = 75
        assert!((a.avg_savings_pct - 75.0).abs() < 0.001);
    }

    #[test]
    fn test_pricing_stats_reset() {
        let mut stats = PricingStats {
            total_jobs_billed: 100,
            total_revenue_usd: 500.0,
            total_retries_absorbed: 50,
            avg_savings_pct: 75.0,
            total_node_hours: 1000.0,
        };
        stats.reset();
        assert_eq!(stats.total_jobs_billed, 0);
        assert_eq!(stats.total_revenue_usd, 0.0);
    }

    #[test]
    fn test_pricing_stats_zero_division_safety() {
        let stats = PricingStats::default();
        assert_eq!(stats.avg_revenue_per_job(), 0.0);
        assert_eq!(stats.avg_retries_per_job(), 0.0);
        assert_eq!(stats.revenue_per_node_hour(), 0.0);
    }

    // ====================================================================
    // JobBilling tests
    // ====================================================================

    #[test]
    fn test_job_billing_new() {
        let estimate = make_test_estimate();
        let billing = JobBilling::new(JobId::new(), estimate.clone());
        assert_eq!(billing.chunks_billed, 0);
        assert_eq!(billing.retries_absorbed, 0);
        assert_eq!(billing.total_cost_usd, 0.0);
        assert!(billing.cost_by_class.is_empty());
    }

    #[test]
    fn test_job_billing_avg_cost_per_chunk() {
        let mut billing = JobBilling::new(JobId::new(), make_test_estimate());
        billing.chunks_billed = 10;
        billing.total_cost_usd = 1.0;
        assert!((billing.avg_cost_per_chunk() - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_job_billing_zero_chunks_avg() {
        let billing = JobBilling::new(JobId::new(), make_test_estimate());
        assert_eq!(billing.avg_cost_per_chunk(), 0.0);
    }

    #[test]
    fn test_job_billing_budget_ratio() {
        let mut billing = JobBilling::new(JobId::new(), make_test_estimate());
        billing.total_cost_usd = 0.5;
        // estimate.estimated_cost_usd = 1.0
        assert!((billing.budget_ratio() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_job_billing_over_estimate() {
        let mut billing = JobBilling::new(JobId::new(), make_test_estimate());
        billing.total_cost_usd = 2.0;
        assert!(billing.is_over_estimate());

        billing.total_cost_usd = 0.5;
        assert!(!billing.is_over_estimate());
    }

    #[test]
    fn test_job_billing_dominant_class() {
        let mut billing = JobBilling::new(JobId::new(), make_test_estimate());
        billing.cost_by_class.insert(NodeClass::Edge, 0.1);
        billing.cost_by_class.insert(NodeClass::Enterprise, 0.9);
        assert_eq!(billing.dominant_class(), Some(NodeClass::Enterprise));
    }

    #[test]
    fn test_job_billing_class_count() {
        let mut billing = JobBilling::new(JobId::new(), make_test_estimate());
        assert_eq!(billing.class_count(), 0);
        billing.cost_by_class.insert(NodeClass::Standard, 0.5);
        billing.cost_by_class.insert(NodeClass::Light, 0.3);
        assert_eq!(billing.class_count(), 2);
    }

    // ====================================================================
    // JobReceipt tests
    // ====================================================================

    #[test]
    fn test_job_receipt_all_fields() {
        let receipt = make_test_receipt();
        assert!(!receipt.receipt_id.is_empty());
        assert!(receipt.total_cost_usd > 0.0);
        assert!(receipt.estimated_cost_usd > 0.0);
        assert!(receipt.chunks_processed > 0);
    }

    #[test]
    fn test_job_receipt_estimate_accuracy() {
        let receipt = make_test_receipt();
        // actual=0.5, estimate=1.0 -> accuracy=0.5
        let accuracy = receipt.estimate_accuracy();
        assert!((accuracy - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_job_receipt_was_under_budget() {
        let receipt = make_test_receipt();
        assert!(receipt.was_under_budget());
    }

    #[test]
    fn test_job_receipt_savings_pct() {
        let mut receipt = make_test_receipt();
        receipt.total_cost_usd = 1.0;
        receipt.savings_vs_cloud_usd = 4.0;
        // cloud_cost = 1.0 + 4.0 = 5.0, savings_pct = 4.0/5.0*100 = 80%
        assert!((receipt.savings_pct() - 80.0).abs() < 0.1);
    }

    #[test]
    fn test_job_receipt_summary() {
        let receipt = make_test_receipt();
        let summary = receipt.summary();
        assert!(summary.contains("Receipt"));
        assert!(summary.contains("cost="));
        assert!(summary.contains("chunks="));
    }

    #[test]
    fn test_job_receipt_verification_pct() {
        let mut receipt = make_test_receipt();
        receipt.total_cost_usd = 1.0;
        receipt.verification_cost_usd = 0.2;
        assert!((receipt.verification_pct() - 20.0).abs() < 0.1);
    }

    #[test]
    fn test_job_receipt_cost_per_chunk() {
        let mut receipt = make_test_receipt();
        receipt.total_cost_usd = 1.0;
        receipt.chunks_processed = 100;
        assert!((receipt.cost_per_chunk() - 0.01).abs() < 0.001);
    }

    // ====================================================================
    // PricingEngine: basic estimation tests
    // ====================================================================

    #[test]
    fn test_engine_estimate_basic() {
        let engine = PricingEngine::new();
        let request = CostEstimateRequest::new(1000).with_duration_ms(100);
        let estimate = engine.estimate_cost(&request);

        assert!(estimate.estimated_cost_usd > 0.0);
        assert!(estimate.estimated_chunks == 1000);
        assert!(estimate.estimated_duration_secs > 0);
        assert!(estimate.effective_nodes > 0);
    }

    #[test]
    fn test_engine_estimate_rush_more_expensive() {
        let engine = PricingEngine::new();
        let standard = engine.estimate_cost(
            &CostEstimateRequest::new(1_000_000).with_duration_ms(100),
        );
        let rush = engine.estimate_cost(
            &CostEstimateRequest::new(1_000_000)
                .with_duration_ms(100)
                .with_priority(PriorityLevel::Rush),
        );
        assert!(rush.estimated_cost_usd > standard.estimated_cost_usd,
            "rush ({}) should cost more than standard ({})",
            rush.estimated_cost_usd, standard.estimated_cost_usd);
    }

    #[test]
    fn test_engine_estimate_economy_cheaper() {
        let engine = PricingEngine::new();
        let standard = engine.estimate_cost(
            &CostEstimateRequest::new(1_000_000).with_duration_ms(100),
        );
        let economy = engine.estimate_cost(
            &CostEstimateRequest::new(1_000_000)
                .with_duration_ms(100)
                .with_priority(PriorityLevel::Economy),
        );
        assert!(economy.estimated_cost_usd < standard.estimated_cost_usd,
            "economy ({}) should cost less than standard ({})",
            economy.estimated_cost_usd, standard.estimated_cost_usd);
    }

    #[test]
    fn test_engine_estimate_verification_overhead() {
        let engine = PricingEngine::new();
        let no_verify = engine.estimate_cost(
            &CostEstimateRequest::new(1_000_000)
                .with_duration_ms(100)
                .with_verification(VerificationStrategy::None),
        );
        let redundant = engine.estimate_cost(
            &CostEstimateRequest::new(1_000_000)
                .with_duration_ms(100)
                .with_verification(VerificationStrategy::Redundant { replicas: 3 }),
        );
        assert!(redundant.estimated_cost_usd > no_verify.estimated_cost_usd,
            "redundant ({}) should cost more than none ({})",
            redundant.estimated_cost_usd, no_verify.estimated_cost_usd);
    }

    #[test]
    fn test_engine_estimate_with_tier() {
        let engine = PricingEngine::new();
        let small = engine.estimate_cost(
            &CostEstimateRequest::new(1000)
                .with_duration_ms(1000)
                .with_tier(SwarmTier::Cell),
        );
        let large = engine.estimate_cost(
            &CostEstimateRequest::new(1000)
                .with_duration_ms(1000)
                .with_tier(SwarmTier::Titan),
        );
        // Larger tier should have faster estimated duration
        assert!(large.estimated_duration_secs <= small.estimated_duration_secs,
            "Titan ({}) should be faster than Cell ({})",
            large.estimated_duration_secs, small.estimated_duration_secs);
    }

    #[test]
    fn test_engine_estimate_module_aware() {
        let engine = PricingEngine::new();
        let estimate = engine.estimate_for_module("cr.montecarlo", 10_000, PriorityLevel::Standard);
        assert!(estimate.estimated_cost_usd > 0.0);
        assert!(estimate.cloud_comparison_usd.is_some());
        let cloud = estimate.cloud_comparison_usd.unwrap();
        assert!(cloud > estimate.estimated_cost_usd,
            "cloud ({}) should be more than CR ({})", cloud, estimate.estimated_cost_usd);
    }

    #[test]
    fn test_engine_estimate_zero_items() {
        let engine = PricingEngine::new();
        let request = CostEstimateRequest::new(0);
        let estimate = engine.estimate_cost(&request);
        // Minimum charge should apply
        assert!(estimate.estimated_cost_usd >= engine.rate_card.minimum_charge_usd);
    }

    #[test]
    fn test_engine_estimate_single_item() {
        let engine = PricingEngine::new();
        let request = CostEstimateRequest::new(1).with_duration_ms(1000);
        let estimate = engine.estimate_cost(&request);
        assert!(estimate.estimated_cost_usd > 0.0);
        assert_eq!(estimate.estimated_chunks, 1);
    }

    #[test]
    fn test_engine_estimate_huge_job() {
        let engine = PricingEngine::new();
        let request = CostEstimateRequest::new(10_000_000)
            .with_duration_ms(100)
            .with_item_size(1024);
        let estimate = engine.estimate_cost(&request);
        assert!(estimate.estimated_cost_usd > 0.0);
        assert_eq!(estimate.estimated_chunks, 10_000_000);
    }

    // ====================================================================
    // PricingEngine: billing flow tests
    // ====================================================================

    #[test]
    fn test_engine_billing_flow() {
        let engine = PricingEngine::new();
        let job_id = JobId::new();

        // Estimate
        let request = CostEstimateRequest::new(100).with_duration_ms(100);
        let estimate = engine.estimate_cost(&request);

        // Start billing
        let _billing = engine.start_billing(job_id, &estimate);
        assert_eq!(engine.active_job_count(), 1);

        // Record some chunks
        engine.record_chunk_cost(&job_id, NodeClass::Standard, 100, false);
        engine.record_chunk_cost(&job_id, NodeClass::Standard, 150, false);
        engine.record_chunk_cost(&job_id, NodeClass::Light, 200, false);
        engine.record_chunk_cost(&job_id, NodeClass::Standard, 100, true); // retry

        // Check billing
        let billing = engine.get_billing(&job_id).unwrap();
        assert_eq!(billing.chunks_billed, 3);
        assert_eq!(billing.retries_absorbed, 1);
        assert!(billing.total_cost_usd > 0.0);
        assert!(billing.cost_by_class.contains_key(&NodeClass::Standard));
        assert!(billing.cost_by_class.contains_key(&NodeClass::Light));

        // Finalize
        let receipt = engine.finalize_billing(&job_id).unwrap();
        assert_eq!(receipt.chunks_processed, 3);
        assert_eq!(receipt.retries_absorbed, 1);
        assert!(receipt.total_cost_usd > 0.0);
        assert!(!receipt.receipt_id.is_empty());

        // Billing should be removed
        assert_eq!(engine.active_job_count(), 0);
        assert!(engine.get_billing(&job_id).is_none());

        // Stats should be updated
        let stats = engine.get_stats();
        assert_eq!(stats.total_jobs_billed, 1);
        assert!(stats.total_revenue_usd > 0.0);
        assert_eq!(stats.total_retries_absorbed, 1);
    }

    #[test]
    fn test_engine_finalize_unknown_job() {
        let engine = PricingEngine::new();
        let result = engine.finalize_billing(&JobId::new());
        assert!(result.is_none());
    }

    #[test]
    fn test_engine_record_chunk_unknown_job() {
        let engine = PricingEngine::new();
        // Should not panic
        engine.record_chunk_cost(&JobId::new(), NodeClass::Standard, 100, false);
    }

    #[test]
    fn test_engine_cancel_billing() {
        let engine = PricingEngine::new();
        let job_id = JobId::new();
        let estimate = make_test_estimate();
        engine.start_billing(job_id, &estimate);

        let cancelled = engine.cancel_billing(&job_id);
        assert!(cancelled.is_some());
        assert_eq!(engine.active_job_count(), 0);
    }

    #[test]
    fn test_engine_single_chunk_cost() {
        let engine = PricingEngine::new();
        let cost = engine.single_chunk_cost(NodeClass::Standard, 3_600_000); // 1 hour
        let rate = engine.rate_card().rate_for(NodeClass::Standard);
        let expected = rate.usd_per_hour + rate.usd_per_chunk;
        assert!((cost - expected).abs() < 0.0001,
            "expected {}, got {}", expected, cost);
    }

    #[test]
    fn test_engine_estimate_class_cost() {
        let engine = PricingEngine::new();
        let cost = engine.estimate_class_cost(
            NodeClass::Enterprise, 1000, 100, PriorityLevel::Standard,
        );
        assert!(cost > 0.0);
    }

    #[test]
    fn test_engine_over_budget_detection() {
        let engine = PricingEngine::new();
        let job_id = JobId::new();
        let estimate = make_test_estimate();
        engine.start_billing(job_id, &estimate);

        // At 0 cost, not over budget
        assert!(!engine.is_over_budget(&job_id, 10.0));

        // Simulate exceeding estimate
        if let Some(mut billing) = engine.active_jobs.get_mut(&job_id) {
            billing.total_cost_usd = 2.0; // estimate is 1.0
        }
        assert!(engine.is_over_budget(&job_id, 50.0)); // 200% > 150%
    }

    #[test]
    fn test_engine_top_active_by_cost() {
        let engine = PricingEngine::new();
        let estimate = make_test_estimate();

        for i in 0..5 {
            let job_id = JobId::new();
            engine.start_billing(job_id, &estimate);
            if let Some(mut billing) = engine.active_jobs.get_mut(&job_id) {
                billing.total_cost_usd = (i + 1) as f64;
            }
        }

        let top = engine.top_active_by_cost(3);
        assert_eq!(top.len(), 3);
        assert!(top[0].total_cost_usd >= top[1].total_cost_usd);
        assert!(top[1].total_cost_usd >= top[2].total_cost_usd);
    }

    #[test]
    fn test_engine_compare_to_cloud() {
        let engine = PricingEngine::new();
        let comp = engine.compare_to_cloud("cr.montecarlo", 10_000);

        assert!(comp.cr_cost_usd > 0.0);
        assert!(comp.aws_cost_usd > comp.cr_cost_usd);
        assert!(comp.azure_cost_usd > comp.cr_cost_usd);
        assert!(comp.gcp_cost_usd > comp.cr_cost_usd);
        assert!(comp.savings_pct > 0.0);
    }

    #[test]
    fn test_engine_compare_to_cloud_unknown_module() {
        let engine = PricingEngine::new();
        let comp = engine.compare_to_cloud("unknown.module", 1000);
        // Should still work with fallback pricing
        assert!(comp.cr_cost_usd >= 0.0);
    }

    #[test]
    fn test_engine_known_modules() {
        let engine = PricingEngine::new();
        let modules = engine.known_modules();
        assert!(modules.len() >= 22);
    }

    #[test]
    fn test_engine_reset_stats() {
        let engine = PricingEngine::new();
        let job_id = JobId::new();
        let estimate = make_test_estimate();
        engine.start_billing(job_id, &estimate);
        engine.record_chunk_cost(&job_id, NodeClass::Standard, 100, false);
        engine.finalize_billing(&job_id);

        let stats = engine.get_stats();
        assert!(stats.total_jobs_billed > 0);

        engine.reset_stats();
        let stats = engine.get_stats();
        assert_eq!(stats.total_jobs_billed, 0);
    }

    // ====================================================================
    // PricingEngine: multiple jobs
    // ====================================================================

    #[test]
    fn test_engine_multiple_concurrent_jobs() {
        let engine = PricingEngine::new();
        let estimate = make_test_estimate();

        let job1 = JobId::new();
        let job2 = JobId::new();
        let job3 = JobId::new();

        engine.start_billing(job1, &estimate);
        engine.start_billing(job2, &estimate);
        engine.start_billing(job3, &estimate);
        assert_eq!(engine.active_job_count(), 3);

        engine.record_chunk_cost(&job1, NodeClass::Standard, 100, false);
        engine.record_chunk_cost(&job2, NodeClass::Enterprise, 200, false);
        engine.record_chunk_cost(&job3, NodeClass::Edge, 50, false);

        let receipt1 = engine.finalize_billing(&job1).unwrap();
        let receipt2 = engine.finalize_billing(&job2).unwrap();
        let receipt3 = engine.finalize_billing(&job3).unwrap();

        assert_ne!(receipt1.receipt_id, receipt2.receipt_id);
        assert_ne!(receipt2.receipt_id, receipt3.receipt_id);

        let stats = engine.get_stats();
        assert_eq!(stats.total_jobs_billed, 3);
    }

    // ====================================================================
    // Utility function tests
    // ====================================================================

    #[test]
    fn test_format_usd() {
        assert!(format_usd(0.0000001).starts_with('$'));
        assert!(format_usd(0.01).starts_with('$'));
        assert!(format_usd(50.0).starts_with('$'));
        assert!(format_usd(1000.0).starts_with('$'));
    }

    #[test]
    fn test_format_duration_secs() {
        assert_eq!(format_duration_secs(30), "30s");
        assert_eq!(format_duration_secs(90), "1m 30s");
        assert_eq!(format_duration_secs(3661), "1h 1m 1s");
    }

    #[test]
    fn test_bytes_to_gb_str() {
        let s = bytes_to_gb_str(1024 * 1024 * 1024);
        assert!(s.contains("1.000"));
    }

    #[test]
    fn test_pct_diff() {
        assert!((pct_diff(150.0, 100.0) - 50.0).abs() < 0.001);
        assert!((pct_diff(50.0, 100.0) - (-50.0)).abs() < 0.001);
        assert_eq!(pct_diff(100.0, 0.0), 0.0);
    }

    #[test]
    fn test_clamp_f64() {
        assert_eq!(clamp_f64(5.0, 0.0, 10.0), 5.0);
        assert_eq!(clamp_f64(-1.0, 0.0, 10.0), 0.0);
        assert_eq!(clamp_f64(15.0, 0.0, 10.0), 10.0);
    }

    // ====================================================================
    // Test helpers
    // ====================================================================

    fn make_test_estimate() -> CostEstimate {
        CostEstimate {
            estimated_cost_usd: 1.0,
            cost_range_usd: (0.7, 1.3),
            estimated_duration_secs: 60,
            duration_range_secs: (40, 80),
            estimated_chunks: 100,
            retry_overhead_pct: 20.0,
            verification_overhead_pct: 5.0,
            cloud_comparison_usd: Some(5.0),
            priority: PriorityLevel::Standard,
            effective_nodes: 35,
        }
    }

    fn make_test_receipt() -> JobReceipt {
        let mut cost_by_class = HashMap::new();
        cost_by_class.insert(NodeClass::Standard, 0.3);
        cost_by_class.insert(NodeClass::Light, 0.2);

        JobReceipt {
            receipt_id: Uuid::new_v4().to_string(),
            job_id: JobId::new(),
            started_at: Utc::now() - ChronoDuration::minutes(5),
            completed_at: Utc::now(),
            total_cost_usd: 0.5,
            estimated_cost_usd: 1.0,
            chunks_processed: 100,
            retries_absorbed: 3,
            verification_cost_usd: 0.05,
            cost_by_class,
            savings_vs_cloud_usd: 4.5,
            priority: PriorityLevel::Standard,
        }
    }
}
