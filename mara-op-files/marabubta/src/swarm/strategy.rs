// Marabunta - Licensed under the MIT License.
//! User-defined resource strategy presets (Phase 6).
//!
//! Instead of hardcoded scheduling weights, operators create named
//! strategies with custom scoring weights and constraints. Jobs
//! reference a strategy by name (e.g. `--strategy ram-heavy`), and
//! the marketplace scorer uses those weights to rank candidate nodes.
//!
//! Key types:
//! - [`ResourceStrategy`] — named preset with weights + constraints + shape
//! - [`StrategyWeights`] — 8 scoring dimensions that sum to 1.0
//! - [`StrategyConstraints`] — hard filters that exclude nodes outright
//! - [`SchedulingShape`] — how to distribute chunks across nodes
//! - [`StrategyStore`] — concurrent storage for strategies
//! - [`StrategyScorer`] — scores a node against a strategy

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use super::profile::{GpuInfo, NodeProfile, NodeType};
use super::reputation::{Badge, ReputationRecord};
use super::types::{InstalledSoftware, NodeId, SwarmError, SwarmResult};

// ============================================================================
// StrategyWeights
// ============================================================================

/// Scoring weights for ranking candidate nodes. Each weight is 0.0..1.0.
/// They are normalized to sum to 1.0 at scoring time if they don't already.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyWeights {
    /// Weight for available RAM.
    #[serde(default)]
    pub ram: f64,
    /// Weight for CPU speed × core count.
    #[serde(default)]
    pub cpu: f64,
    /// Weight for GPU capability (VRAM, compute units).
    #[serde(default)]
    pub gpu: f64,
    /// Weight for reputation score (badges, success rate).
    #[serde(default = "default_reputation_weight")]
    pub reputation: f64,
    /// Weight for estimated energy cost (lower = better).
    #[serde(default)]
    pub energy_cost: f64,
    /// Weight for data locality (blobs already present on node).
    #[serde(default)]
    pub data_locality: f64,
    /// Weight for network bandwidth.
    #[serde(default)]
    pub bandwidth: f64,
    /// Weight for software match (required binaries present).
    #[serde(default)]
    pub software_match: f64,
}

fn default_reputation_weight() -> f64 {
    0.25
}

impl Default for StrategyWeights {
    fn default() -> Self {
        Self {
            ram: 0.15,
            cpu: 0.15,
            gpu: 0.0,
            reputation: 0.25,
            energy_cost: 0.10,
            data_locality: 0.15,
            bandwidth: 0.10,
            software_match: 0.10,
        }
    }
}

impl StrategyWeights {
    /// Returns a normalized copy where weights sum to 1.0.
    /// If all weights are zero, returns equal weights.
    pub fn normalized(&self) -> Self {
        let sum = self.ram
            + self.cpu
            + self.gpu
            + self.reputation
            + self.energy_cost
            + self.data_locality
            + self.bandwidth
            + self.software_match;

        if sum <= 0.0 {
            // All zero — return equal weights
            let w = 1.0 / 8.0;
            return Self {
                ram: w,
                cpu: w,
                gpu: w,
                reputation: w,
                energy_cost: w,
                data_locality: w,
                bandwidth: w,
                software_match: w,
            };
        }

        Self {
            ram: self.ram / sum,
            cpu: self.cpu / sum,
            gpu: self.gpu / sum,
            reputation: self.reputation / sum,
            energy_cost: self.energy_cost / sum,
            data_locality: self.data_locality / sum,
            bandwidth: self.bandwidth / sum,
            software_match: self.software_match / sum,
        }
    }

    /// Returns the sum of all weights.
    pub fn sum(&self) -> f64 {
        self.ram
            + self.cpu
            + self.gpu
            + self.reputation
            + self.energy_cost
            + self.data_locality
            + self.bandwidth
            + self.software_match
    }
}

// ============================================================================
// StrategyConstraints
// ============================================================================

/// Hard constraints that filter out nodes before scoring.
/// Any constraint set to `None` means "no filter on this dimension".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StrategyConstraints {
    /// Node must have at least this much RAM (MB).
    #[serde(default)]
    pub min_ram_mb: Option<u64>,
    /// Node must have at least this many CPU cores.
    #[serde(default)]
    pub min_cpu_cores: Option<u32>,
    /// Node must have at least this much disk (MB).
    #[serde(default)]
    pub min_disk_mb: Option<u64>,
    /// Node must have a GPU with at least this much VRAM (MB).
    #[serde(default)]
    pub requires_gpu: bool,
    /// Node must have a GPU with at least this much VRAM (MB).
    #[serde(default)]
    pub min_gpu_vram_mb: Option<u64>,
    /// Node must be one of these types. Empty = any type allowed.
    #[serde(default)]
    pub allowed_node_types: Vec<NodeType>,
    /// Node must NOT be any of these types.
    #[serde(default)]
    pub denied_node_types: Vec<NodeType>,
    /// Required software (name and optional minimum version).
    /// Format: "python3>=3.10" or just "ffmpeg".
    #[serde(default)]
    pub required_software: Vec<String>,
    /// Node must have at least one of these badges.
    #[serde(default)]
    pub required_badges: Vec<Badge>,
    /// Minimum success rate (0.0..1.0) from reputation.
    #[serde(default)]
    pub min_success_rate: Option<f32>,
    /// Node must be in one of these geo regions. Empty = any region.
    #[serde(default)]
    pub allowed_regions: Vec<String>,
    /// Maximum energy cost per kWh (USD). Nodes with higher local
    /// energy prices are excluded.
    #[serde(default)]
    pub max_energy_price_usd: Option<f64>,
}

impl StrategyConstraints {
    /// Returns true if the given node profile satisfies all hard constraints.
    pub fn satisfied_by(
        &self,
        profile: &NodeProfile,
        reputation: Option<&ReputationRecord>,
        badges: &[Badge],
    ) -> bool {
        // RAM check
        if let Some(min) = self.min_ram_mb {
            if profile.ram_total_mb < min {
                return false;
            }
        }

        // CPU check
        if let Some(min) = self.min_cpu_cores {
            if profile.cpu_cores < min {
                return false;
            }
        }

        // Disk check
        if let Some(min) = self.min_disk_mb {
            if profile.disk_available_mb < min {
                return false;
            }
        }

        // GPU check
        if self.requires_gpu && profile.gpu.is_none() {
            return false;
        }

        // GPU VRAM check
        if let Some(min_vram) = self.min_gpu_vram_mb {
            match &profile.gpu {
                Some(gpu) if gpu.vram_mb >= min_vram => {}
                _ => return false,
            }
        }

        // Node type allow-list
        if !self.allowed_node_types.is_empty()
            && !self.allowed_node_types.contains(&profile.node_type)
        {
            return false;
        }

        // Node type deny-list
        if self.denied_node_types.contains(&profile.node_type) {
            return false;
        }

        // Required software
        for req_str in &self.required_software {
            if !software_satisfies(req_str, &profile.installed_software) {
                return false;
            }
        }

        // Required badges
        if !self.required_badges.is_empty() {
            let has_any = self.required_badges.iter().any(|b| badges.contains(b));
            if !has_any {
                return false;
            }
        }

        // Minimum success rate
        if let Some(min_rate) = self.min_success_rate {
            if let Some(rep) = reputation {
                if rep.recent_success.success_rate() < min_rate {
                    return false;
                }
            }
            // No reputation record yet — treat as passing (optimistic for new nodes)
        }

        // Geo region
        if !self.allowed_regions.is_empty() {
            match &profile.geo_region {
                Some(region) => {
                    let region_lower = region.to_lowercase();
                    if !self.allowed_regions.iter().any(|r| r.to_lowercase() == region_lower) {
                        return false;
                    }
                }
                None => return false,
            }
        }

        true
    }
}

/// Check if a node's installed software satisfies a requirement string.
/// Format: "name>=version" or just "name".
fn software_satisfies(requirement: &str, installed: &[InstalledSoftware]) -> bool {
    let (name, min_version) = if let Some(idx) = requirement.find(">=") {
        (&requirement[..idx], Some(&requirement[idx + 2..]))
    } else {
        (requirement, None)
    };

    let name_lower = name.to_lowercase();

    for sw in installed {
        if sw.name.to_lowercase() != name_lower {
            continue;
        }
        // Name matches — check version if required
        if let Some(min_ver_str) = min_version {
            if let Some(installed_ver) = &sw.version {
                if let (Some(min_v), Some(inst_v)) = (
                    parse_loose_version(min_ver_str),
                    parse_loose_version(installed_ver),
                ) {
                    if inst_v >= min_v {
                        return true;
                    }
                }
                // Can't parse either version — skip this entry
            }
            // No installed version to compare — skip
        } else {
            // No version constraint — name match is sufficient
            return true;
        }
    }

    false
}

/// Loosely parse a version string like "3.10", "3.10.2", "21", "v1.21.5-beta".
/// Returns (major, minor, patch) or None.
fn parse_loose_version(s: &str) -> Option<(u64, u64, u64)> {
    let s = s.trim().trim_start_matches('v');
    // Strip anything after a dash or plus (pre-release markers)
    let s = s.split(['-', '+']).next().unwrap_or(s);
    let parts: Vec<&str> = s.split('.').collect();
    let major = parts.first()?.parse::<u64>().ok()?;
    let minor = parts.get(1).and_then(|p| p.parse::<u64>().ok()).unwrap_or(0);
    let patch = parts.get(2).and_then(|p| p.parse::<u64>().ok()).unwrap_or(0);
    Some((major, minor, patch))
}

// ============================================================================
// SchedulingShape
// ============================================================================

/// How to distribute chunks across the selected nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
#[derive(Default)]
pub enum SchedulingShape {
    /// Spread chunks evenly across as many nodes as possible (default).
    #[default]
    SpreadWide,
    /// Prefer fewer, more powerful nodes.
    FewBigNodes {
        /// Maximum nodes to use (None = no limit, just prefer big).
        max_nodes: Option<u32>,
        /// Which metric to prefer for "big".
        prefer_by: PreferMetric,
    },
    /// All chunks on a single node (useful for sequential dependencies).
    SingleNode,
    /// Distribute proportional to each node's capacity.
    Proportional {
        /// Capacity metric to base proportions on.
        metric: PreferMetric,
    },
}


/// Which metric drives "prefer" decisions in scheduling shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreferMetric {
    Ram,
    Cpu,
    Gpu,
    Bandwidth,
    DiskIo,
}

// ============================================================================
// ResourceStrategy
// ============================================================================

/// A named resource strategy preset.
///
/// Users create strategies with custom weights and constraints, then
/// reference them by name when submitting jobs. Built-in presets are
/// loaded at startup and can be overridden.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceStrategy {
    /// Unique identifier (e.g. "ram-heavy", "gpu-burst", "my-custom-preset").
    pub id: String,
    /// Human-readable description.
    pub description: String,
    /// Who created this strategy. `None` = built-in.
    pub created_by: Option<String>,
    /// When this strategy was created or last updated.
    pub updated_at: DateTime<Utc>,

    /// Scoring weights — how much each dimension matters when ranking nodes.
    pub weights: StrategyWeights,
    /// Hard constraints — nodes that don't meet these are excluded entirely.
    pub constraints: StrategyConstraints,
    /// Scheduling shape — how to distribute chunks across qualifying nodes.
    #[serde(default)]
    pub shape: SchedulingShape,
    /// Whether this is a built-in preset (cannot be deleted, but can be overridden).
    #[serde(default)]
    pub builtin: bool,
}

// ============================================================================
// Built-in presets
// ============================================================================

/// Returns the default set of built-in strategy presets.
pub fn builtin_strategies() -> Vec<ResourceStrategy> {
    let now = Utc::now();

    vec![
        ResourceStrategy {
            id: "balanced".into(),
            description: "Default balanced scheduling — equal weight across all dimensions".into(),
            created_by: None,
            updated_at: now,
            weights: StrategyWeights {
                ram: 0.15,
                cpu: 0.15,
                gpu: 0.0,
                reputation: 0.25,
                energy_cost: 0.10,
                data_locality: 0.15,
                bandwidth: 0.10,
                software_match: 0.10,
            },
            constraints: StrategyConstraints::default(),
            shape: SchedulingShape::SpreadWide,
            builtin: true,
        },
        ResourceStrategy {
            id: "ram-heavy".into(),
            description: "Prioritize nodes with the most available RAM".into(),
            created_by: None,
            updated_at: now,
            weights: StrategyWeights {
                ram: 0.50,
                cpu: 0.10,
                gpu: 0.0,
                reputation: 0.15,
                energy_cost: 0.05,
                data_locality: 0.10,
                bandwidth: 0.05,
                software_match: 0.05,
            },
            constraints: StrategyConstraints {
                min_ram_mb: Some(4096),
                ..Default::default()
            },
            shape: SchedulingShape::FewBigNodes {
                max_nodes: None,
                prefer_by: PreferMetric::Ram,
            },
            builtin: true,
        },
        ResourceStrategy {
            id: "parallel-max".into(),
            description: "Maximum parallelism — spread across as many nodes as possible".into(),
            created_by: None,
            updated_at: now,
            weights: StrategyWeights {
                ram: 0.05,
                cpu: 0.10,
                gpu: 0.0,
                reputation: 0.20,
                energy_cost: 0.10,
                data_locality: 0.05,
                bandwidth: 0.20,
                software_match: 0.30,
            },
            constraints: StrategyConstraints::default(),
            shape: SchedulingShape::SpreadWide,
            builtin: true,
        },
        ResourceStrategy {
            id: "gpu-burst".into(),
            description: "Target GPU nodes for compute-heavy work".into(),
            created_by: None,
            updated_at: now,
            weights: StrategyWeights {
                ram: 0.15,
                cpu: 0.10,
                gpu: 0.50,
                reputation: 0.10,
                energy_cost: 0.05,
                data_locality: 0.05,
                bandwidth: 0.0,
                software_match: 0.05,
            },
            constraints: StrategyConstraints {
                requires_gpu: true,
                ..Default::default()
            },
            shape: SchedulingShape::FewBigNodes {
                max_nodes: None,
                prefer_by: PreferMetric::Gpu,
            },
            builtin: true,
        },
        ResourceStrategy {
            id: "cheapest".into(),
            description: "Minimize USD energy cost — schedule on cheapest power".into(),
            created_by: None,
            updated_at: now,
            weights: StrategyWeights {
                ram: 0.05,
                cpu: 0.10,
                gpu: 0.0,
                reputation: 0.15,
                energy_cost: 0.60,
                data_locality: 0.05,
                bandwidth: 0.05,
                software_match: 0.0,
            },
            constraints: StrategyConstraints::default(),
            shape: SchedulingShape::SpreadWide,
            builtin: true,
        },
    ]
}

// ============================================================================
// StrategyStore
// ============================================================================

/// Thread-safe concurrent storage for resource strategies.
pub struct StrategyStore {
    strategies: DashMap<String, ResourceStrategy>,
}

impl StrategyStore {
    /// Create a new store pre-loaded with built-in presets.
    pub fn new() -> Self {
        let store = Self {
            strategies: DashMap::new(),
        };
        for s in builtin_strategies() {
            store.strategies.insert(s.id.clone(), s);
        }
        store
    }

    /// Create an empty store (no built-in presets).
    pub fn empty() -> Self {
        Self {
            strategies: DashMap::new(),
        }
    }

    /// Get a strategy by ID.
    pub fn get(&self, id: &str) -> Option<ResourceStrategy> {
        self.strategies.get(id).map(|r| r.value().clone())
    }

    /// Insert or update a strategy. Returns the previous value if it existed.
    pub fn upsert(&self, strategy: ResourceStrategy) -> Option<ResourceStrategy> {
        self.strategies.insert(strategy.id.clone(), strategy)
    }

    /// Delete a strategy by ID. Returns error if it's a built-in preset.
    pub fn delete(&self, id: &str) -> SwarmResult<Option<ResourceStrategy>> {
        if let Some(entry) = self.strategies.get(id) {
            if entry.builtin {
                return Err(SwarmError::StrategyNotFound(format!(
                    "cannot delete built-in strategy '{}'",
                    id
                )));
            }
        }
        Ok(self.strategies.remove(id).map(|(_, v)| v))
    }

    /// List all strategies, sorted by ID.
    pub fn list(&self) -> Vec<ResourceStrategy> {
        let mut result: Vec<_> = self
            .strategies
            .iter()
            .map(|r| r.value().clone())
            .collect();
        result.sort_by(|a, b| a.id.cmp(&b.id));
        result
    }

    /// Returns the number of stored strategies.
    pub fn count(&self) -> usize {
        self.strategies.len()
    }

    /// Returns the "balanced" strategy (always exists in a properly-initialized store).
    pub fn default_strategy(&self) -> ResourceStrategy {
        self.get("balanced")
            .unwrap_or_else(|| builtin_strategies().into_iter().next().unwrap())
    }
}

impl Default for StrategyStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// StrategyScorer
// ============================================================================

/// Context provided for scoring a node against a strategy.
pub struct NodeScoringContext<'a> {
    pub profile: &'a NodeProfile,
    /// Reputation record for this node (if available).
    pub reputation: Option<&'a ReputationRecord>,
    /// Current badges held by this node.
    pub badges: Vec<Badge>,
    /// Fraction of required input blobs already present on this node (0.0..1.0).
    pub data_locality_score: f64,
    /// Estimated energy price at this node in USD/kWh (lower = better for cheapest).
    pub energy_price_usd: f64,
    /// Software requirements for the job (for computing software match %).
    pub required_software: &'a [String],
}

/// Scores candidate nodes against a strategy and returns ranked results.
pub struct StrategyScorer;

/// A scored candidate with breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredNode {
    pub node_id: NodeId,
    pub total_score: f64,
    pub ram_score: f64,
    pub cpu_score: f64,
    pub gpu_score: f64,
    pub reputation_score: f64,
    pub energy_score: f64,
    pub data_locality_score: f64,
    pub bandwidth_score: f64,
    pub software_match_score: f64,
}

impl StrategyScorer {
    /// Score a single node. Returns None if the node doesn't meet hard constraints.
    pub fn score_node(
        strategy: &ResourceStrategy,
        ctx: &NodeScoringContext<'_>,
    ) -> Option<ScoredNode> {
        // Check hard constraints first
        if !strategy
            .constraints
            .satisfied_by(ctx.profile, ctx.reputation, &ctx.badges)
        {
            return None;
        }

        let w = strategy.weights.normalized();

        // RAM score: normalized to 0..1 using a reference of 64 GB
        let ram_score = (ctx.profile.ram_total_mb as f64 / 65536.0).min(1.0);

        // CPU score: cores × freq normalized to a reference of 128 cores × 5 GHz
        let cpu_raw = ctx.profile.cpu_cores as f64 * ctx.profile.cpu_freq_mhz as f64;
        let cpu_score = (cpu_raw / (128.0 * 5000.0)).min(1.0);

        // GPU score
        let gpu_score = match &ctx.profile.gpu {
            Some(gpu) => gpu_capability_score(gpu),
            None => 0.0,
        };

        // Reputation score
        let reputation_score = compute_reputation_score(ctx.reputation, &ctx.badges);

        // Energy cost score: inverted — lower price = higher score
        // Reference: $0.30/kWh is the worst (score 0), $0.0 is perfect (score 1)
        let energy_score = (1.0 - (ctx.energy_price_usd / 0.30)).max(0.0).min(1.0);

        // Data locality (already 0..1 from caller)
        let data_locality_score = ctx.data_locality_score.max(0.0).min(1.0);

        // Bandwidth score: based on declared strengths
        let bandwidth_score = if ctx
            .profile
            .strengths
            .iter()
            .any(|s| matches!(s, super::profile::Strength::HighBandwidth))
        {
            1.0
        } else if ctx
            .profile
            .weaknesses
            .iter()
            .any(|w| matches!(w, super::profile::Weakness::LowBandwidth))
        {
            0.2
        } else {
            0.5
        };

        // Software match score: fraction of required software present
        let software_match_score = if ctx.required_software.is_empty() {
            1.0
        } else {
            let matched = ctx
                .required_software
                .iter()
                .filter(|req| software_satisfies(req, &ctx.profile.installed_software))
                .count();
            matched as f64 / ctx.required_software.len() as f64
        };

        // Weighted sum
        let total_score = w.ram * ram_score
            + w.cpu * cpu_score
            + w.gpu * gpu_score
            + w.reputation * reputation_score
            + w.energy_cost * energy_score
            + w.data_locality * data_locality_score
            + w.bandwidth * bandwidth_score
            + w.software_match * software_match_score;

        Some(ScoredNode {
            node_id: ctx.profile.node_id,
            total_score,
            ram_score,
            cpu_score,
            gpu_score,
            reputation_score,
            energy_score,
            data_locality_score,
            bandwidth_score,
            software_match_score,
        })
    }

    /// Score and rank a set of candidates. Nodes that fail constraints are excluded.
    /// Returns nodes sorted by score descending (best first).
    pub fn rank_nodes(
        strategy: &ResourceStrategy,
        contexts: &[NodeScoringContext<'_>],
    ) -> Vec<ScoredNode> {
        let mut scored: Vec<ScoredNode> = contexts
            .iter()
            .filter_map(|ctx| Self::score_node(strategy, ctx))
            .collect();

        // Sort descending by score, then by node_id for determinism
        scored.sort_by(|a, b| {
            b.total_score
                .partial_cmp(&a.total_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.node_id.0.cmp(&b.node_id.0))
        });

        scored
    }

    /// Select nodes according to the strategy's scheduling shape.
    /// Returns the node IDs in preferred assignment order.
    pub fn select_nodes(
        strategy: &ResourceStrategy,
        ranked: &[ScoredNode],
        chunk_count: usize,
    ) -> Vec<NodeId> {
        if ranked.is_empty() {
            return Vec::new();
        }

        match &strategy.shape {
            SchedulingShape::SpreadWide => {
                // Use all available nodes, up to chunk_count
                ranked
                    .iter()
                    .take(chunk_count)
                    .map(|s| s.node_id)
                    .collect()
            }
            SchedulingShape::FewBigNodes { max_nodes, .. } => {
                let limit = max_nodes
                    .map(|n| n as usize)
                    .unwrap_or(ranked.len())
                    .min(ranked.len())
                    .max(1);
                ranked.iter().take(limit).map(|s| s.node_id).collect()
            }
            SchedulingShape::SingleNode => {
                // Just the top-scoring node
                vec![ranked[0].node_id]
            }
            SchedulingShape::Proportional { metric } => {
                // Select all qualifying nodes, but the caller should
                // distribute chunks proportionally based on the metric.
                // For now, return all ranked nodes.
                let mut selected: Vec<_> = ranked.iter().map(|s| s.node_id).collect();
                // Re-sort by the chosen metric if different from total score
                if let Some(reranked) = rerank_by_metric(ranked, *metric) {
                    selected = reranked.iter().map(|s| s.node_id).collect();
                }
                selected
            }
        }
    }

    /// Compute proportional chunk distribution for Proportional shape.
    /// Returns (node_id, chunk_count) pairs.
    pub fn proportional_distribution(
        ranked: &[ScoredNode],
        metric: PreferMetric,
        total_chunks: usize,
        profiles: &[&NodeProfile],
    ) -> Vec<(NodeId, usize)> {
        if ranked.is_empty() || total_chunks == 0 {
            return Vec::new();
        }

        // Compute capacity metric for each node
        let capacities: Vec<(NodeId, f64)> = ranked
            .iter()
            .filter_map(|scored| {
                let profile = profiles.iter().find(|p| p.node_id == scored.node_id)?;
                let cap = match metric {
                    PreferMetric::Ram => profile.ram_total_mb as f64,
                    PreferMetric::Cpu => {
                        profile.cpu_cores as f64 * profile.cpu_freq_mhz as f64
                    }
                    PreferMetric::Gpu => profile
                        .gpu
                        .as_ref()
                        .map(|g| g.vram_mb as f64)
                        .unwrap_or(0.0),
                    PreferMetric::Bandwidth | PreferMetric::DiskIo => {
                        // No direct metric — use a proxy (disk for DiskIo)
                        profile.disk_available_mb as f64
                    }
                };
                Some((scored.node_id, cap.max(1.0)))
            })
            .collect();

        let total_cap: f64 = capacities.iter().map(|(_, c)| c).sum();
        if total_cap <= 0.0 {
            // Fallback: equal distribution
            let per_node = total_chunks / capacities.len().max(1);
            let mut result: Vec<(NodeId, usize)> =
                capacities.iter().map(|(id, _)| (*id, per_node)).collect();
            // Distribute remainder to first nodes
            let assigned: usize = result.iter().map(|(_, c)| c).sum();
            for i in 0..(total_chunks - assigned) {
                result[i].1 += 1;
            }
            return result;
        }

        // Proportional allocation with remainder distribution
        let mut result: Vec<(NodeId, usize)> = capacities
            .iter()
            .map(|(id, cap)| {
                let fraction = cap / total_cap;
                let chunks = (fraction * total_chunks as f64).floor() as usize;
                (*id, chunks)
            })
            .collect();

        let assigned: usize = result.iter().map(|(_, c)| c).sum();
        let mut remaining = total_chunks.saturating_sub(assigned);

        // Distribute remainder by highest fractional part
        let mut fractionals: Vec<(usize, f64)> = capacities
            .iter()
            .enumerate()
            .map(|(i, (_, cap))| {
                let fraction = cap / total_cap;
                let exact = fraction * total_chunks as f64;
                let floored = exact.floor();
                (i, exact - floored)
            })
            .collect();
        fractionals.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        for (idx, _) in fractionals {
            if remaining == 0 {
                break;
            }
            result[idx].1 += 1;
            remaining -= 1;
        }

        result
    }
}

/// Re-rank scored nodes by a specific metric.
fn rerank_by_metric(ranked: &[ScoredNode], metric: PreferMetric) -> Option<Vec<ScoredNode>> {
    let mut reranked = ranked.to_vec();
    let extract = |s: &ScoredNode| -> f64 {
        match metric {
            PreferMetric::Ram => s.ram_score,
            PreferMetric::Cpu => s.cpu_score,
            PreferMetric::Gpu => s.gpu_score,
            PreferMetric::Bandwidth => s.bandwidth_score,
            PreferMetric::DiskIo => 0.5, // No direct metric
        }
    };
    reranked.sort_by(|a, b| {
        extract(b)
            .partial_cmp(&extract(a))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.node_id.0.cmp(&b.node_id.0))
    });
    Some(reranked)
}

// ============================================================================
// Scoring helpers
// ============================================================================

/// Score a GPU's capability on a 0..1 scale.
/// Reference: 24 GB VRAM, 16384 CUDA cores = 1.0.
fn gpu_capability_score(gpu: &GpuInfo) -> f64 {
    let vram_score = (gpu.vram_mb as f64 / 24576.0).min(1.0);
    let cuda_score = gpu
        .cuda_cores
        .map(|c| (c as f64 / 16384.0).min(1.0))
        .unwrap_or(0.3); // Non-NVIDIA GPUs get baseline score
    (vram_score * 0.6 + cuda_score * 0.4).min(1.0)
}

/// Compute a reputation score (0..1) from a reputation record and badges.
fn compute_reputation_score(reputation: Option<&ReputationRecord>, badges: &[Badge]) -> f64 {
    let base = match reputation {
        Some(rep) => rep.recent_success.success_rate() as f64,
        None => 0.5, // Unknown — neutral
    };

    // Badge bonuses
    let badge_bonus: f64 = badges
        .iter()
        .map(|b| match b {
            Badge::Lightning => 0.05,
            Badge::Reliable => 0.10,
            Badge::Trusted => 0.15,
            Badge::HeavyLifter => 0.05,
            Badge::Veteran => 0.10,
            Badge::Newcomer => -0.05,
            Badge::Malicious => -1.0,
        })
        .sum();

    (base + badge_bonus).max(0.0).min(1.0)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::profile::{NodeType, Strength, Weakness};
    use crate::swarm::reputation::{EntityId, RollingWindow};
    use crate::swarm::types::InstalledSoftware;
    use crate::swarm::types::SoftwareCategory;

    fn make_profile(node_id: NodeId) -> NodeProfile {
        NodeProfile {
            node_id,
            node_type: NodeType::BareMetal,
            version: 1,
            cpu_cores: 8,
            cpu_freq_mhz: 3500,
            ram_total_mb: 16384,
            disk_available_mb: 500_000,
            gpu: None,
            strengths: vec![],
            runtimes: vec![],
            specializations: vec![],
            weaknesses: vec![],
            max_task_duration: None,
            availability_window: Default::default(),
            preferred_work: vec![],
            avoided_work: vec![],
            max_concurrent: 4,
            installed_software: vec![],
            custom_capabilities: vec![],
            ..Default::default()
            
        }
    }

    fn make_gpu_profile(node_id: NodeId) -> NodeProfile {
        let mut p = make_profile(node_id);
        p.gpu = Some(GpuInfo {
            name: "NVIDIA RTX 4090".into(),
            vram_mb: 24576,
            cuda_cores: Some(16384),
            compute_capability: Some("8.9".into()),
        });
        p.strengths.push(Strength::GPUCompute);
        p
    }

    fn make_scoring_context<'a>(
        profile: &'a NodeProfile,
    ) -> NodeScoringContext<'a> {
        NodeScoringContext {
            profile,
            reputation: None,
            badges: vec![],
            data_locality_score: 0.0,
            energy_price_usd: 0.12,
            required_software: &[],
        }
    }

    // ---- StrategyWeights ----

    #[test]
    fn test_weights_default_sum_to_one() {
        let w = StrategyWeights::default();
        let sum = w.sum();
        assert!((sum - 1.0).abs() < 0.01, "Default weights should sum to ~1.0, got {}", sum);
    }

    #[test]
    fn test_weights_normalized() {
        let w = StrategyWeights {
            ram: 2.0,
            cpu: 2.0,
            gpu: 0.0,
            reputation: 0.0,
            energy_cost: 0.0,
            data_locality: 0.0,
            bandwidth: 0.0,
            software_match: 0.0,
        };
        let n = w.normalized();
        assert!((n.sum() - 1.0).abs() < 1e-10);
        assert!((n.ram - 0.5).abs() < 1e-10);
        assert!((n.cpu - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_weights_all_zero_normalized() {
        let w = StrategyWeights {
            ram: 0.0,
            cpu: 0.0,
            gpu: 0.0,
            reputation: 0.0,
            energy_cost: 0.0,
            data_locality: 0.0,
            bandwidth: 0.0,
            software_match: 0.0,
        };
        let n = w.normalized();
        assert!((n.sum() - 1.0).abs() < 1e-10);
        assert!((n.ram - 0.125).abs() < 1e-10);
    }

    // ---- Software matching ----

    #[test]
    fn test_software_satisfies_name_only() {
        let installed = vec![InstalledSoftware {
            name: "python3".into(),
            version: Some("3.11.6".into()),
            path: "/usr/bin/python3".into(),
            category: SoftwareCategory::Runtime,
        }];
        assert!(software_satisfies("python3", &installed));
        assert!(!software_satisfies("ruby", &installed));
    }

    #[test]
    fn test_software_satisfies_with_version() {
        let installed = vec![InstalledSoftware {
            name: "python3".into(),
            version: Some("3.11.6".into()),
            path: "/usr/bin/python3".into(),
            category: SoftwareCategory::Runtime,
        }];
        assert!(software_satisfies("python3>=3.10", &installed));
        assert!(software_satisfies("python3>=3.11.6", &installed));
        assert!(!software_satisfies("python3>=3.12", &installed));
    }

    #[test]
    fn test_software_satisfies_case_insensitive() {
        let installed = vec![InstalledSoftware {
            name: "FFmpeg".into(),
            version: Some("6.0".into()),
            path: "/usr/bin/ffmpeg".into(),
            category: SoftwareCategory::MediaTool,
        }];
        assert!(software_satisfies("ffmpeg", &installed));
        assert!(software_satisfies("FFmpeg>=5.0", &installed));
    }

    #[test]
    fn test_software_satisfies_no_installed_version() {
        let installed = vec![InstalledSoftware {
            name: "gcc".into(),
            version: None,
            path: "/usr/bin/gcc".into(),
            category: SoftwareCategory::DevTool,
        }];
        // Name-only match works
        assert!(software_satisfies("gcc", &installed));
        // Version constraint can't be satisfied without a version
        assert!(!software_satisfies("gcc>=12", &installed));
    }

    // ---- Version parsing ----

    #[test]
    fn test_parse_loose_version() {
        assert_eq!(parse_loose_version("3.11.6"), Some((3, 11, 6)));
        assert_eq!(parse_loose_version("3.10"), Some((3, 10, 0)));
        assert_eq!(parse_loose_version("21"), Some((21, 0, 0)));
        assert_eq!(parse_loose_version("v1.21.5"), Some((1, 21, 5)));
        assert_eq!(parse_loose_version("3.10.2-beta"), Some((3, 10, 2)));
        assert_eq!(parse_loose_version(""), None);
        assert_eq!(parse_loose_version("abc"), None);
    }

    // ---- Constraints ----

    #[test]
    fn test_constraints_default_passes_all() {
        let constraints = StrategyConstraints::default();
        let profile = make_profile(NodeId::new());
        assert!(constraints.satisfied_by(&profile, None, &[]));
    }

    #[test]
    fn test_constraints_min_ram() {
        let constraints = StrategyConstraints {
            min_ram_mb: Some(32768),
            ..Default::default()
        };
        let mut profile = make_profile(NodeId::new());

        profile.ram_total_mb = 16384;
        assert!(!constraints.satisfied_by(&profile, None, &[]));

        profile.ram_total_mb = 32768;
        assert!(constraints.satisfied_by(&profile, None, &[]));
    }

    #[test]
    fn test_constraints_min_cpu_cores() {
        let constraints = StrategyConstraints {
            min_cpu_cores: Some(16),
            ..Default::default()
        };
        let mut profile = make_profile(NodeId::new());

        profile.cpu_cores = 8;
        assert!(!constraints.satisfied_by(&profile, None, &[]));

        profile.cpu_cores = 16;
        assert!(constraints.satisfied_by(&profile, None, &[]));
    }

    #[test]
    fn test_constraints_requires_gpu() {
        let constraints = StrategyConstraints {
            requires_gpu: true,
            ..Default::default()
        };

        let no_gpu = make_profile(NodeId::new());
        assert!(!constraints.satisfied_by(&no_gpu, None, &[]));

        let with_gpu = make_gpu_profile(NodeId::new());
        assert!(constraints.satisfied_by(&with_gpu, None, &[]));
    }

    #[test]
    fn test_constraints_min_gpu_vram() {
        let constraints = StrategyConstraints {
            min_gpu_vram_mb: Some(16000),
            ..Default::default()
        };

        let no_gpu = make_profile(NodeId::new());
        assert!(!constraints.satisfied_by(&no_gpu, None, &[]));

        let mut small_gpu = make_profile(NodeId::new());
        small_gpu.gpu = Some(GpuInfo {
            name: "T4".into(),
            vram_mb: 8192,
            cuda_cores: Some(2560),
            compute_capability: Some("7.5".into()),
        });
        assert!(!constraints.satisfied_by(&small_gpu, None, &[]));

        let big_gpu = make_gpu_profile(NodeId::new());
        assert!(constraints.satisfied_by(&big_gpu, None, &[]));
    }

    #[test]
    fn test_constraints_allowed_node_types() {
        let constraints = StrategyConstraints {
            allowed_node_types: vec![NodeType::BareMetal, NodeType::CloudVM],
            ..Default::default()
        };

        let mut profile = make_profile(NodeId::new());
        assert!(constraints.satisfied_by(&profile, None, &[]));

        profile.node_type = NodeType::Android;
        assert!(!constraints.satisfied_by(&profile, None, &[]));
    }

    #[test]
    fn test_constraints_denied_node_types() {
        let constraints = StrategyConstraints {
            denied_node_types: vec![NodeType::Browser, NodeType::Lambda],
            ..Default::default()
        };

        let mut profile = make_profile(NodeId::new());
        assert!(constraints.satisfied_by(&profile, None, &[])); // BareMetal

        profile.node_type = NodeType::Browser;
        assert!(!constraints.satisfied_by(&profile, None, &[]));
    }

    #[test]
    fn test_constraints_required_software() {
        let constraints = StrategyConstraints {
            required_software: vec!["python3>=3.10".into()],
            ..Default::default()
        };

        let mut profile = make_profile(NodeId::new());
        assert!(!constraints.satisfied_by(&profile, None, &[]));

        profile.installed_software.push(InstalledSoftware {
            name: "python3".into(),
            version: Some("3.11.6".into()),
            path: "/usr/bin/python3".into(),
            category: SoftwareCategory::Runtime,
        });
        assert!(constraints.satisfied_by(&profile, None, &[]));
    }

    #[test]
    fn test_constraints_required_badges() {
        let constraints = StrategyConstraints {
            required_badges: vec![Badge::Reliable, Badge::Trusted],
            ..Default::default()
        };

        let profile = make_profile(NodeId::new());
        assert!(!constraints.satisfied_by(&profile, None, &[]));
        assert!(!constraints.satisfied_by(&profile, None, &[Badge::Lightning]));
        assert!(constraints.satisfied_by(&profile, None, &[Badge::Reliable]));
        assert!(constraints.satisfied_by(&profile, None, &[Badge::Trusted]));
    }

    #[test]
    fn test_constraints_min_success_rate() {
        let constraints = StrategyConstraints {
            min_success_rate: Some(0.95),
            ..Default::default()
        };

        let profile = make_profile(NodeId::new());

        // No reputation — passes (optimistic)
        assert!(constraints.satisfied_by(&profile, None, &[]));

        // Good reputation
        let mut window = RollingWindow::new(10);
        for _ in 0..10 {
            window.push(1.0);
        }
        let now = Utc::now();
        let rep = ReputationRecord {
            entity_id: EntityId::Node(NodeId::new()),
            badges: Default::default(),
            score: 1.0,
            total_jobs: 10,
            successful_jobs: 10,
            failed_jobs: 0,
            policy_violations: 0,
            recent_success: window,
            recent_speed: RollingWindow::new(10),
            first_seen: now,
            last_active: now,
            updated_at: now,
            version: 1,
            large_jobs_completed: 0,
            large_jobs_failed: 0,
        };
        assert!(constraints.satisfied_by(&profile, Some(&rep), &[]));

        // Bad reputation
        let mut bad_window = RollingWindow::new(10);
        for _ in 0..5 {
            bad_window.push(1.0);
        }
        for _ in 0..5 {
            bad_window.push(0.0);
        }
        let bad_rep = ReputationRecord {
            entity_id: EntityId::Node(NodeId::new()),
            badges: Default::default(),
            score: 0.5,
            total_jobs: 10,
            successful_jobs: 5,
            failed_jobs: 5,
            policy_violations: 0,
            recent_success: bad_window,
            recent_speed: RollingWindow::new(10),
            first_seen: now,
            last_active: now,
            updated_at: now,
            version: 1,
            large_jobs_completed: 0,
            large_jobs_failed: 0,
        };
        assert!(!constraints.satisfied_by(&profile, Some(&bad_rep), &[]));
    }

    #[test]
    fn test_constraints_geo_region() {
        let constraints = StrategyConstraints {
            allowed_regions: vec!["us-east".into(), "eu-west".into()],
            ..Default::default()
        };

        let mut profile = make_profile(NodeId::new());
        // No region set — fails
        assert!(!constraints.satisfied_by(&profile, None, &[]));

        profile.geo_region = Some("us-east".into());
        assert!(constraints.satisfied_by(&profile, None, &[]));

        profile.geo_region = Some("US-EAST".into());
        assert!(constraints.satisfied_by(&profile, None, &[]));

        profile.geo_region = Some("ap-south".into());
        assert!(!constraints.satisfied_by(&profile, None, &[]));
    }

    // ---- Built-in strategies ----

    #[test]
    fn test_builtin_strategies_exist() {
        let strategies = builtin_strategies();
        assert!(strategies.len() >= 5);

        let ids: Vec<&str> = strategies.iter().map(|s| s.id.as_str()).collect();
        assert!(ids.contains(&"balanced"));
        assert!(ids.contains(&"ram-heavy"));
        assert!(ids.contains(&"parallel-max"));
        assert!(ids.contains(&"gpu-burst"));
        assert!(ids.contains(&"cheapest"));
    }

    #[test]
    fn test_builtin_strategies_weights_reasonable() {
        for s in builtin_strategies() {
            let sum = s.weights.sum();
            assert!(
                (sum - 1.0).abs() < 0.01,
                "Strategy '{}' weights sum to {} (expected ~1.0)",
                s.id,
                sum
            );
            assert!(s.builtin, "Strategy '{}' should be marked builtin", s.id);
        }
    }

    #[test]
    fn test_builtin_strategy_serialization() {
        for s in builtin_strategies() {
            let json = serde_json::to_string(&s).unwrap();
            let deserialized: ResourceStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized.id, s.id);
            assert!((deserialized.weights.sum() - s.weights.sum()).abs() < 1e-10);
        }
    }

    // ---- StrategyStore ----

    #[test]
    fn test_store_new_has_builtins() {
        let store = StrategyStore::new();
        assert!(store.count() >= 5);
        assert!(store.get("balanced").is_some());
        assert!(store.get("ram-heavy").is_some());
    }

    #[test]
    fn test_store_empty() {
        let store = StrategyStore::empty();
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn test_store_upsert_and_get() {
        let store = StrategyStore::new();
        let custom = ResourceStrategy {
            id: "my-custom".into(),
            description: "Custom test strategy".into(),
            created_by: Some("test-user".into()),
            
            weights: StrategyWeights::default(),
            constraints: StrategyConstraints::default(),
            shape: SchedulingShape::SpreadWide,
            builtin: false,
        };
        assert!(store.upsert(custom.clone()).is_none());
        let retrieved = store.get("my-custom").unwrap();
        assert_eq!(retrieved.description, "Custom test strategy");
    }

    #[test]
    fn test_store_delete_custom() {
        let store = StrategyStore::new();
        let custom = ResourceStrategy {
            id: "deletable".into(),
            description: "Delete me".into(),
            created_by: None,
            
            weights: StrategyWeights::default(),
            constraints: StrategyConstraints::default(),
            shape: SchedulingShape::SpreadWide,
            builtin: false,
        };
        store.upsert(custom);
        assert!(store.get("deletable").is_some());
        store.delete("deletable").unwrap();
        assert!(store.get("deletable").is_none());
    }

    #[test]
    fn test_store_cannot_delete_builtin() {
        let store = StrategyStore::new();
        let result = store.delete("balanced");
        assert!(result.is_err());
    }

    #[test]
    fn test_store_list_sorted() {
        let store = StrategyStore::new();
        let list = store.list();
        for i in 1..list.len() {
            assert!(list[i - 1].id <= list[i].id);
        }
    }

    #[test]
    fn test_store_default_strategy() {
        let store = StrategyStore::new();
        let default = store.default_strategy();
        assert_eq!(default.id, "balanced");
    }

    // ---- Scoring ----

    #[test]
    fn test_score_node_basic() {
        let strategy = StrategyStore::new().default_strategy();
        let profile = make_profile(NodeId::new());
        let ctx = make_scoring_context(&profile);
        let scored = StrategyScorer::score_node(&strategy, &ctx).unwrap();
        assert!(scored.total_score > 0.0);
        assert!(scored.total_score <= 1.0);
    }

    #[test]
    fn test_score_node_filtered_by_constraints() {
        let strategy = ResourceStrategy {
            id: "test".into(),
            description: "".into(),
            created_by: None,
            
            weights: StrategyWeights::default(),
            constraints: StrategyConstraints {
                min_ram_mb: Some(999_999),
                ..Default::default()
            },
            shape: SchedulingShape::SpreadWide,
            builtin: false,
        };
        let profile = make_profile(NodeId::new());
        let ctx = make_scoring_context(&profile);
        assert!(StrategyScorer::score_node(&strategy, &ctx).is_none());
    }

    #[test]
    fn test_ram_heavy_prefers_more_ram() {
        let strategy = StrategyStore::new().get("ram-heavy").unwrap();

        let id_a = NodeId::new();
        let id_b = NodeId::new();

        let mut profile_a = make_profile(id_a);
        profile_a.ram_total_mb = 4096;

        let mut profile_b = make_profile(id_b);
        profile_b.ram_total_mb = 65536;

        let ctx_a = make_scoring_context(&profile_a);
        let ctx_b = make_scoring_context(&profile_b);

        let score_a = StrategyScorer::score_node(&strategy, &ctx_a).unwrap();
        let score_b = StrategyScorer::score_node(&strategy, &ctx_b).unwrap();

        assert!(
            score_b.total_score > score_a.total_score,
            "ram-heavy should prefer more RAM: {} vs {}",
            score_b.total_score,
            score_a.total_score
        );
    }

    #[test]
    fn test_gpu_burst_prefers_gpu() {
        let strategy = StrategyStore::new().get("gpu-burst").unwrap();

        let id_a = NodeId::new();
        let id_b = NodeId::new();

        let profile_no_gpu = make_profile(id_a);
        let profile_gpu = make_gpu_profile(id_b);

        let ctx_no = make_scoring_context(&profile_no_gpu);
        let ctx_gpu = make_scoring_context(&profile_gpu);

        // No-GPU node should be filtered out by constraints
        assert!(StrategyScorer::score_node(&strategy, &ctx_no).is_none());
        assert!(StrategyScorer::score_node(&strategy, &ctx_gpu).is_some());
    }

    #[test]
    fn test_cheapest_prefers_low_energy() {
        let strategy = StrategyStore::new().get("cheapest").unwrap();

        let id_a = NodeId::new();
        let id_b = NodeId::new();

        let profile_a = make_profile(id_a);
        let profile_b = make_profile(id_b);

        let mut ctx_expensive = make_scoring_context(&profile_a);
        ctx_expensive.energy_price_usd = 0.25;

        let mut ctx_cheap = make_scoring_context(&profile_b);
        ctx_cheap.energy_price_usd = 0.05;

        let score_exp = StrategyScorer::score_node(&strategy, &ctx_expensive).unwrap();
        let score_cheap = StrategyScorer::score_node(&strategy, &ctx_cheap).unwrap();

        assert!(
            score_cheap.total_score > score_exp.total_score,
            "cheapest should prefer lower energy cost"
        );
    }

    #[test]
    fn test_rank_nodes_sorted_descending() {
        let strategy = StrategyStore::new().default_strategy();

        let ids: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();
        let profiles: Vec<NodeProfile> = ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                let mut p = make_profile(id);
                p.ram_total_mb = (i as u64 + 1) * 8192;
                p
            })
            .collect();

        let contexts: Vec<NodeScoringContext> = profiles
            .iter()
            .map(|p| make_scoring_context(p))
            .collect();

        let ranked = StrategyScorer::rank_nodes(&strategy, &contexts);
        assert_eq!(ranked.len(), 5);
        for i in 1..ranked.len() {
            assert!(ranked[i - 1].total_score >= ranked[i].total_score);
        }
    }

    #[test]
    fn test_rank_nodes_filters_constraints() {
        let strategy = ResourceStrategy {
            id: "test".into(),
            description: "".into(),
            created_by: None,
            
            weights: StrategyWeights::default(),
            constraints: StrategyConstraints {
                min_ram_mb: Some(16384),
                ..Default::default()
            },
            shape: SchedulingShape::SpreadWide,
            builtin: false,
        };

        let small_id = NodeId::new();
        let big_id = NodeId::new();

        let mut small = make_profile(small_id);
        small.ram_total_mb = 8192;

        let mut big = make_profile(big_id);
        big.ram_total_mb = 32768;

        let contexts = vec![make_scoring_context(&small), make_scoring_context(&big)];
        let ranked = StrategyScorer::rank_nodes(&strategy, &contexts);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].node_id, big_id);
    }

    // ---- Scheduling shape ----

    #[test]
    fn test_select_nodes_spread_wide() {
        let strategy = StrategyStore::new().default_strategy();
        let scored: Vec<ScoredNode> = (0..5)
            .map(|i| ScoredNode {
                node_id: NodeId::new(),
                total_score: 1.0 - i as f64 * 0.1,
                ram_score: 0.5,
                cpu_score: 0.5,
                gpu_score: 0.0,
                reputation_score: 0.5,
                energy_score: 0.5,
                data_locality_score: 0.0,
                bandwidth_score: 0.5,
                software_match_score: 1.0,
            })
            .collect();

        let selected = StrategyScorer::select_nodes(&strategy, &scored, 3);
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn test_select_nodes_few_big() {
        let strategy = ResourceStrategy {
            id: "test".into(),
            description: "".into(),
            created_by: None,
            
            weights: StrategyWeights::default(),
            constraints: StrategyConstraints::default(),
            shape: SchedulingShape::FewBigNodes {
                max_nodes: Some(2),
                prefer_by: PreferMetric::Ram,
            },
            builtin: false,
        };

        let scored: Vec<ScoredNode> = (0..5)
            .map(|i| ScoredNode {
                node_id: NodeId::new(),
                total_score: 1.0 - i as f64 * 0.1,
                ram_score: 0.5,
                cpu_score: 0.5,
                gpu_score: 0.0,
                reputation_score: 0.5,
                energy_score: 0.5,
                data_locality_score: 0.0,
                bandwidth_score: 0.5,
                software_match_score: 1.0,
            })
            .collect();

        let selected = StrategyScorer::select_nodes(&strategy, &scored, 10);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_select_nodes_single_node() {
        let strategy = ResourceStrategy {
            id: "test".into(),
            description: "".into(),
            created_by: None,
            
            weights: StrategyWeights::default(),
            constraints: StrategyConstraints::default(),
            shape: SchedulingShape::SingleNode,
            builtin: false,
        };

        let scored: Vec<ScoredNode> = (0..5)
            .map(|i| ScoredNode {
                node_id: NodeId::new(),
                total_score: 1.0 - i as f64 * 0.1,
                ram_score: 0.5,
                cpu_score: 0.5,
                gpu_score: 0.0,
                reputation_score: 0.5,
                energy_score: 0.5,
                data_locality_score: 0.0,
                bandwidth_score: 0.5,
                software_match_score: 1.0,
            })
            .collect();

        let selected = StrategyScorer::select_nodes(&strategy, &scored, 10);
        assert_eq!(selected.len(), 1);
    }

    // ---- Proportional distribution ----

    #[test]
    fn test_proportional_distribution_by_ram() {
        let id_a = NodeId::new();
        let id_b = NodeId::new();

        let scored = vec![
            ScoredNode {
                node_id: id_a,
                total_score: 0.8,
                ram_score: 0.75,
                cpu_score: 0.5,
                gpu_score: 0.0,
                reputation_score: 0.5,
                energy_score: 0.5,
                data_locality_score: 0.0,
                bandwidth_score: 0.5,
                software_match_score: 1.0,
            },
            ScoredNode {
                node_id: id_b,
                total_score: 0.6,
                ram_score: 0.25,
                cpu_score: 0.5,
                gpu_score: 0.0,
                reputation_score: 0.5,
                energy_score: 0.5,
                data_locality_score: 0.0,
                bandwidth_score: 0.5,
                software_match_score: 1.0,
            },
        ];

        let profile_a = {
            let mut p = make_profile(id_a);
            p.ram_total_mb = 32768;
            p
        };
        let profile_b = {
            let mut p = make_profile(id_b);
            p.ram_total_mb = 8192;
            p
        };

        let dist = StrategyScorer::proportional_distribution(
            &scored,
            PreferMetric::Ram,
            10,
            &[&profile_a, &profile_b],
        );

        assert_eq!(dist.len(), 2);
        let total: usize = dist.iter().map(|(_, c)| c).sum();
        assert_eq!(total, 10);
        // Node A (32GB) should get more chunks than node B (8GB)
        let a_chunks = dist.iter().find(|(id, _)| *id == id_a).unwrap().1;
        let b_chunks = dist.iter().find(|(id, _)| *id == id_b).unwrap().1;
        assert!(a_chunks > b_chunks, "32GB node should get more chunks: {} vs {}", a_chunks, b_chunks);
    }

    #[test]
    fn test_proportional_distribution_empty() {
        let dist = StrategyScorer::proportional_distribution(&[], PreferMetric::Cpu, 10, &[]);
        assert!(dist.is_empty());
    }

    // ---- GPU scoring ----

    #[test]
    fn test_gpu_capability_score_max() {
        let gpu = GpuInfo {
            name: "RTX 4090".into(),
            vram_mb: 24576,
            cuda_cores: Some(16384),
            compute_capability: Some("8.9".into()),
        };
        let score = gpu_capability_score(&gpu);
        assert!((score - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_gpu_capability_score_small() {
        let gpu = GpuInfo {
            name: "T4".into(),
            vram_mb: 8192,
            cuda_cores: Some(2560),
            compute_capability: Some("7.5".into()),
        };
        let score = gpu_capability_score(&gpu);
        assert!(score > 0.0 && score < 0.5);
    }

    #[test]
    fn test_gpu_capability_score_no_cuda() {
        let gpu = GpuInfo {
            name: "AMD RX 7900".into(),
            vram_mb: 20480,
            cuda_cores: None,
            compute_capability: None,
        };
        let score = gpu_capability_score(&gpu);
        // Should still get a decent score from VRAM alone
        assert!(score > 0.3);
    }

    // ---- Reputation scoring ----

    #[test]
    fn test_reputation_score_no_record() {
        let score = compute_reputation_score(None, &[]);
        assert!((score - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_reputation_score_perfect() {
        let mut window = RollingWindow::new(10);
        for _ in 0..10 {
            window.push(1.0);
        }
        let now = Utc::now();
        let rep = ReputationRecord {
            entity_id: EntityId::Node(NodeId::new()),
            badges: Default::default(),
            score: 1.0,
            total_jobs: 10,
            successful_jobs: 10,
            failed_jobs: 0,
            policy_violations: 0,
            recent_success: window,
            recent_speed: RollingWindow::new(10),
            first_seen: now,
            last_active: now,
            updated_at: now,
            version: 1,
            large_jobs_completed: 0,
            large_jobs_failed: 0,
        };
        let score = compute_reputation_score(
            Some(&rep),
            &[Badge::Reliable, Badge::Trusted],
        );
        assert!(score > 0.9);
    }

    #[test]
    fn test_reputation_score_newcomer_penalty() {
        let score_without = compute_reputation_score(None, &[]);
        let score_with = compute_reputation_score(None, &[Badge::Newcomer]);
        assert!(score_with < score_without);
    }

    // ---- Bandwidth scoring ----

    #[test]
    fn test_bandwidth_high_strength() {
        let strategy = StrategyStore::new().default_strategy();
        let mut profile = make_profile(NodeId::new());
        profile.strengths.push(Strength::HighBandwidth);
        let ctx = make_scoring_context(&profile);
        let scored = StrategyScorer::score_node(&strategy, &ctx).unwrap();
        assert!((scored.bandwidth_score - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_bandwidth_low_weakness() {
        let strategy = StrategyStore::new().default_strategy();
        let mut profile = make_profile(NodeId::new());
        profile.weaknesses.push(Weakness::LowBandwidth);
        let ctx = make_scoring_context(&profile);
        let scored = StrategyScorer::score_node(&strategy, &ctx).unwrap();
        assert!((scored.bandwidth_score - 0.2).abs() < 1e-10);
    }

    // ---- Software match scoring ----

    #[test]
    fn test_software_match_full() {
        let strategy = StrategyStore::new().default_strategy();
        let mut profile = make_profile(NodeId::new());
        profile.installed_software.push(InstalledSoftware {
            name: "python3".into(),
            version: Some("3.11".into()),
            path: "/usr/bin/python3".into(),
            category: SoftwareCategory::Runtime,
        });
        let required = vec!["python3".to_string()];
        let ctx = NodeScoringContext {
            profile: &profile,
            reputation: None,
            badges: vec![],
            data_locality_score: 0.0,
            energy_price_usd: 0.12,
            required_software: &required,
        };
        let scored = StrategyScorer::score_node(&strategy, &ctx).unwrap();
        assert!((scored.software_match_score - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_software_match_partial() {
        let strategy = StrategyStore::new().default_strategy();
        let mut profile = make_profile(NodeId::new());
        profile.installed_software.push(InstalledSoftware {
            name: "python3".into(),
            version: Some("3.11".into()),
            path: "/usr/bin/python3".into(),
            category: SoftwareCategory::Runtime,
        });
        let required = vec!["python3".to_string(), "ffmpeg".to_string()];
        let ctx = NodeScoringContext {
            profile: &profile,
            reputation: None,
            badges: vec![],
            data_locality_score: 0.0,
            energy_price_usd: 0.12,
            required_software: &required,
        };
        let scored = StrategyScorer::score_node(&strategy, &ctx).unwrap();
        assert!((scored.software_match_score - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_software_match_empty_requirements() {
        let strategy = StrategyStore::new().default_strategy();
        let profile = make_profile(NodeId::new());
        let ctx = make_scoring_context(&profile);
        let scored = StrategyScorer::score_node(&strategy, &ctx).unwrap();
        assert!((scored.software_match_score - 1.0).abs() < 1e-10);
    }

    // ---- Serialization round-trips ----

    #[test]
    fn test_strategy_serialization_roundtrip() {
        let strategy = ResourceStrategy {
            id: "test-roundtrip".into(),
            description: "Test".into(),
            created_by: Some("user".into()),
            
            weights: StrategyWeights {
                ram: 0.3,
                cpu: 0.2,
                gpu: 0.1,
                reputation: 0.1,
                energy_cost: 0.1,
                data_locality: 0.1,
                bandwidth: 0.05,
                software_match: 0.05,
            },
            constraints: StrategyConstraints {
                min_ram_mb: Some(8192),
                requires_gpu: true,
                allowed_regions: vec!["us-east".into()],
                ..Default::default()
            },
            shape: SchedulingShape::FewBigNodes {
                max_nodes: Some(4),
                prefer_by: PreferMetric::Gpu,
            },
            builtin: false,
        };

        let json = serde_json::to_string_pretty(&strategy).unwrap();
        let deserialized: ResourceStrategy = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, "test-roundtrip");
        assert_eq!(deserialized.constraints.min_ram_mb, Some(8192));
        assert!(deserialized.constraints.requires_gpu);
        assert_eq!(deserialized.constraints.allowed_regions, vec!["us-east"]);
    }

    #[test]
    fn test_weights_serialization() {
        let w = StrategyWeights::default();
        let json = serde_json::to_string(&w).unwrap();
        let deserialized: StrategyWeights = serde_json::from_str(&json).unwrap();
        assert!((deserialized.sum() - w.sum()).abs() < 1e-10);
    }

    #[test]
    fn test_scheduling_shape_serialization() {
        let shapes = vec![
            SchedulingShape::SpreadWide,
            SchedulingShape::FewBigNodes {
                max_nodes: Some(3),
                prefer_by: PreferMetric::Ram,
            },
            SchedulingShape::SingleNode,
            SchedulingShape::Proportional {
                metric: PreferMetric::Cpu,
            },
        ];

        for shape in shapes {
            let json = serde_json::to_string(&shape).unwrap();
            let deserialized: SchedulingShape = serde_json::from_str(&json).unwrap();
            let json2 = serde_json::to_string(&deserialized).unwrap();
            assert_eq!(json, json2);
        }
    }

    // ---- Concurrent store access ----

    #[test]
    fn test_store_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(StrategyStore::new());
        let mut handles = vec![];

        for i in 0..8 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let strategy = ResourceStrategy {
                    id: format!("concurrent-{}", i),
                    description: format!("Thread {}", i),
                    created_by: None,
                    
                    weights: StrategyWeights::default(),
                    constraints: StrategyConstraints::default(),
                    shape: SchedulingShape::SpreadWide,
                    builtin: false,
                };
                store.upsert(strategy);
                // Read back
                let s = store.get(&format!("concurrent-{}", i));
                assert!(s.is_some());
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All 8 + builtins should exist
        assert!(store.count() >= 13);
    }
}
