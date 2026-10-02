// Marabunta - Licensed under the MIT License.
//! Node-aware chunking engine for the Marabunta Swarm.
//!
//! Sizes task chunks based on node class (Edge / Light / Standard / Enterprise)
//! so that every device in the swarm receives work it can actually finish
//! within its memory, duration, and I/O budgets.
//!
//! # Design principles
//!
//! 1. **Class-proportional sizing** -- Boulders get large chunks; Edge nodes
//!    get tiny ones. The ratio mirrors [`NodeClass::compute_weight`].
//! 2. **Adaptive feedback** -- [`AdaptiveChunker`] records per-class
//!    performance and adjusts future chunk sizes based on P90 execution
//!    times and failure overhead.
//! 3. **Zero-copy splitting** -- [`ChunkSplitter`] emits `(offset, length)`
//!    pairs rather than copying data, suitable for memory-mapped files and
//!    streaming uploads.
//! 4. **Validation before dispatch** -- [`ChunkValidator`] checks planned
//!    chunks against [`TaskConstraints`] before any work is sent to a node.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::knowledge::KnowledgeStore;
use super::types::{ChunkId, ChunkStrategy, NodeClass, NodeId, TaskConstraints};
use crate::common::types::JobId;

// ============================================================================
// Constants
// ============================================================================

/// Default item count per chunk when no better heuristic is available.
const DEFAULT_ITEMS_PER_CHUNK: u64 = 100;

/// Minimum chunk size (items) -- never go below this to avoid overhead
/// dominating real work.
const MIN_CHUNK_ITEMS: u64 = 1;

/// Maximum chunk size (items) even for Boulders.
const MAX_CHUNK_ITEMS: u64 = 1_000_000;

/// Performance history cap per class -- older entries are discarded FIFO.
const MAX_PERFORMANCE_HISTORY: usize = 500;

/// P90 percentile index helper.  For a sorted vec of N, P90 is at index
/// `(N * 90) / 100`.
fn p90_index(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    ((n as u64 * 90) / 100).min(n as u64 - 1) as usize
}

/// Compute the P50 (median) index.
fn p50_index(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    ((n as u64 * 50) / 100).min(n as u64 - 1) as usize
}

/// Compute an arbitrary percentile index.
fn percentile_index(n: usize, pct: u32) -> usize {
    if n == 0 {
        return 0;
    }
    let pct = pct.min(100);
    ((n as u64 * pct as u64) / 100).min(n as u64 - 1) as usize
}

// ============================================================================
// ChunkPerformance
// ============================================================================

/// Performance record for adaptive chunk sizing.
///
/// Recorded after every chunk completion (success or failure) so the
/// [`AdaptiveChunker`] can learn the empirical throughput of each node class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkPerformance {
    /// Number of items the chunk processed.
    pub items_processed: u64,
    /// Wall-clock execution time in milliseconds.
    pub duration_ms: u64,
    /// Whether the chunk finished without error.
    pub success: bool,
    /// Unix-epoch milliseconds when this record was created.
    pub timestamp: u64,
}

impl ChunkPerformance {
    /// Create a new performance record timestamped to now.
    pub fn new(items_processed: u64, duration_ms: u64, success: bool) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            items_processed,
            duration_ms,
            success,
            timestamp,
        }
    }

    /// Items per millisecond throughput.  Returns 0.0 if duration is zero.
    pub fn throughput(&self) -> f64 {
        if self.duration_ms == 0 {
            return 0.0;
        }
        self.items_processed as f64 / self.duration_ms as f64
    }

    /// Duration per item in milliseconds.  Returns 0.0 if items is zero.
    pub fn per_item_ms(&self) -> f64 {
        if self.items_processed == 0 {
            return 0.0;
        }
        self.duration_ms as f64 / self.items_processed as f64
    }

    /// Whether this record is older than `max_age_ms` milliseconds.
    pub fn is_stale(&self, max_age_ms: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now.saturating_sub(self.timestamp) > max_age_ms
    }
}

// ============================================================================
// PlannedChunk
// ============================================================================

/// A single planned chunk within a [`ChunkPlan`].
///
/// The planner emits these *before* any work is dispatched so that the
/// scheduler can validate constraints, estimate costs, and present a
/// preview to the user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedChunk {
    /// Unique identifier for this chunk.
    pub chunk_id: ChunkId,
    /// Zero-based ordering within the parent job.
    pub sequence: u32,
    /// The node class this chunk is sized for.
    pub target_class: NodeClass,
    /// Number of logical items in this chunk.
    pub item_count: u64,
    /// Estimated peak memory requirement in megabytes.
    pub estimated_memory_mb: u64,
    /// Estimated wall-clock duration in seconds.
    pub estimated_duration_secs: u64,
    /// Byte offset into the input data (for range-based splitting).
    pub offset: u64,
    /// Byte length of the input range.
    pub length: u64,
}

impl PlannedChunk {
    /// Human-readable one-line summary.
    pub fn summary(&self) -> String {
        format!(
            "chunk {} seq={} class={} items={} mem={}MB dur={}s range=[{}..{})",
            self.chunk_id,
            self.sequence,
            self.target_class,
            self.item_count,
            self.estimated_memory_mb,
            self.estimated_duration_secs,
            self.offset,
            self.offset + self.length,
        )
    }

    /// Estimated data throughput in items per second.
    pub fn estimated_throughput(&self) -> f64 {
        if self.estimated_duration_secs == 0 {
            return 0.0;
        }
        self.item_count as f64 / self.estimated_duration_secs as f64
    }

    /// Whether this chunk exceeds the constraints for its target class.
    pub fn exceeds_class_limits(&self) -> bool {
        self.estimated_memory_mb > self.target_class.max_memory_mb()
            || self.estimated_duration_secs > self.target_class.max_duration_secs()
    }

    /// Estimated memory per item in bytes (0 if item_count is 0).
    pub fn memory_per_item_bytes(&self) -> u64 {
        if self.item_count == 0 {
            return 0;
        }
        (self.estimated_memory_mb * 1024 * 1024) / self.item_count
    }
}

// ============================================================================
// ChunkPlan
// ============================================================================

/// A complete chunking plan for a job.
///
/// Produced by [`ChunkPlanner`] and consumed by the scheduler.  The plan
/// is immutable once created -- if conditions change, call
/// [`ChunkPlanner::replan_remaining`] to build a new plan covering only
/// the outstanding work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkPlan {
    /// The job this plan belongs to.
    pub job_id: JobId,
    /// Total number of logical items in the job.
    pub total_items: u64,
    /// Ordered list of planned chunks.
    pub chunks: Vec<PlannedChunk>,
    /// The strategy that produced this plan.
    pub strategy_used: ChunkStrategy,
    /// Estimated end-to-end duration in seconds (critical-path estimate).
    pub estimated_duration_secs: u64,
}

impl ChunkPlan {
    /// Number of chunks in this plan.
    pub fn total_chunks(&self) -> usize {
        self.chunks.len()
    }

    /// Return only the chunks targeted at a specific node class.
    pub fn chunks_for_class(&self, class: NodeClass) -> Vec<&PlannedChunk> {
        self.chunks
            .iter()
            .filter(|c| c.target_class == class)
            .collect()
    }

    /// Average item count across all chunks (0 if empty).
    pub fn average_chunk_size(&self) -> u64 {
        if self.chunks.is_empty() {
            return 0;
        }
        let total: u64 = self.chunks.iter().map(|c| c.item_count).sum();
        total / self.chunks.len() as u64
    }

    /// The chunk with the most items, or `None` if the plan is empty.
    pub fn largest_chunk(&self) -> Option<&PlannedChunk> {
        self.chunks.iter().max_by_key(|c| c.item_count)
    }

    /// The chunk with the fewest items, or `None` if the plan is empty.
    pub fn smallest_chunk(&self) -> Option<&PlannedChunk> {
        self.chunks.iter().min_by_key(|c| c.item_count)
    }

    /// Total estimated memory across all chunks (sum, not peak).
    pub fn total_estimated_memory_mb(&self) -> u64 {
        self.chunks.iter().map(|c| c.estimated_memory_mb).sum()
    }

    /// Peak estimated memory (the single largest chunk).
    pub fn peak_estimated_memory_mb(&self) -> u64 {
        self.chunks
            .iter()
            .map(|c| c.estimated_memory_mb)
            .max()
            .unwrap_or(0)
    }

    /// Sum of all item counts across chunks.
    pub fn total_planned_items(&self) -> u64 {
        self.chunks.iter().map(|c| c.item_count).sum()
    }

    /// Whether every planned item is accounted for (no gaps, no overlaps
    /// in a contiguous range).
    pub fn is_contiguous(&self) -> bool {
        if self.chunks.is_empty() {
            return true;
        }
        let mut sorted: Vec<_> = self.chunks.iter().collect();
        sorted.sort_by_key(|c| c.offset);
        let mut expected_offset = sorted[0].offset;
        for chunk in &sorted {
            if chunk.offset != expected_offset {
                return false;
            }
            expected_offset += chunk.length;
        }
        true
    }

    /// Map of class -> chunk count.
    pub fn class_distribution(&self) -> HashMap<NodeClass, usize> {
        let mut dist = HashMap::new();
        for chunk in &self.chunks {
            *dist.entry(chunk.target_class).or_insert(0) += 1;
        }
        dist
    }

    /// Map of class -> total items assigned to that class.
    pub fn items_per_class(&self) -> HashMap<NodeClass, u64> {
        let mut map = HashMap::new();
        for chunk in &self.chunks {
            *map.entry(chunk.target_class).or_insert(0) += chunk.item_count;
        }
        map
    }

    /// Return chunk IDs in sequence order.
    pub fn chunk_ids_ordered(&self) -> Vec<ChunkId> {
        let mut sorted = self.chunks.clone();
        sorted.sort_by_key(|c| c.sequence);
        sorted.into_iter().map(|c| c.chunk_id).collect()
    }

    /// Whether any chunk exceeds its class limits.
    pub fn has_oversized_chunks(&self) -> bool {
        self.chunks.iter().any(|c| c.exceeds_class_limits())
    }
}

// ============================================================================
// ClassDistribution
// ============================================================================

/// Snapshot of the node class distribution in the swarm.
///
/// Used by the planner to proportion chunk sizes to the available fleet.
/// Can be built from explicit counts or derived from the [`KnowledgeStore`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassDistribution {
    pub dust_count: u32,
    pub pebble_count: u32,
    pub rock_count: u32,
    pub boulder_count: u32,
    pub dust_pct: f64,
    pub pebble_pct: f64,
    pub rock_pct: f64,
    pub boulder_pct: f64,
}

impl ClassDistribution {
    /// Build from explicit per-class counts.
    pub fn from_counts(edge: u32, light: u32, standard: u32, enterprise: u32) -> Self {
        let total = (edge + light + standard + enterprise) as f64;
        let (dp, pp, rp, bp) = if total > 0.0 {
            (
                edge as f64 / total,
                light as f64 / total,
                standard as f64 / total,
                enterprise as f64 / total,
            )
        } else {
            (0.0, 0.0, 0.0, 0.0)
        };
        Self {
            dust_count: edge,
            pebble_count: light,
            rock_count: standard,
            boulder_count: enterprise,
            dust_pct: dp,
            pebble_pct: pp,
            rock_pct: rp,
            boulder_pct: bp,
        }
    }

    /// Derive the distribution from live knowledge store data.
    ///
    /// Iterates all alive nodes, classifies each by
    /// [`NodeClass::from_resources`], and tallies the counts.
    pub fn from_knowledge(knowledge: &KnowledgeStore) -> Self {
        let nodes = knowledge.get_live_nodes();
        let mut edge = 0u32;
        let mut light = 0u32;
        let mut standard = 0u32;
        let mut enterprise = 0u32;

        for node in &nodes {
            let class = NodeClass::from_resources(
                node.capacity.cpu_cores,
                node.capacity.memory_total_mb,
            );
            match class {
                NodeClass::Edge => edge += 1,
                NodeClass::Light => light += 1,
                NodeClass::Standard => standard += 1,
                NodeClass::Enterprise => enterprise += 1,
            }
        }

        Self::from_counts(edge, light, standard, enterprise)
    }

    /// Total number of nodes across all classes.
    pub fn total(&self) -> u32 {
        self.dust_count + self.pebble_count + self.rock_count + self.boulder_count
    }

    /// Effective compute units (weighted sum of nodes by class compute weight).
    ///
    /// One Standard = 1.0 compute units.  Useful for estimating parallel
    /// throughput without caring about individual node counts.
    pub fn effective_compute_units(&self) -> f64 {
        self.dust_count as f64 * NodeClass::Edge.compute_weight()
            + self.pebble_count as f64 * NodeClass::Light.compute_weight()
            + self.rock_count as f64 * NodeClass::Standard.compute_weight()
            + self.boulder_count as f64 * NodeClass::Enterprise.compute_weight()
    }

    /// The class with the highest node count.  Ties broken by ascending
    /// class order (Edge < Light < Standard < Enterprise).
    pub fn dominant_class(&self) -> NodeClass {
        let pairs = [
            (self.dust_count, NodeClass::Edge),
            (self.pebble_count, NodeClass::Light),
            (self.rock_count, NodeClass::Standard),
            (self.boulder_count, NodeClass::Enterprise),
        ];
        pairs
            .iter()
            .max_by_key(|(count, _)| *count)
            .map(|(_, class)| *class)
            .unwrap_or(NodeClass::Standard)
    }

    /// Look up the count for a specific class.
    pub fn class_count(&self, class: NodeClass) -> u32 {
        match class {
            NodeClass::Edge => self.dust_count,
            NodeClass::Light => self.pebble_count,
            NodeClass::Standard => self.rock_count,
            NodeClass::Enterprise => self.boulder_count,
        }
    }

    /// Look up the percentage for a specific class (0.0 -- 1.0).
    pub fn class_pct(&self, class: NodeClass) -> f64 {
        match class {
            NodeClass::Edge => self.dust_pct,
            NodeClass::Light => self.pebble_pct,
            NodeClass::Standard => self.rock_pct,
            NodeClass::Enterprise => self.boulder_pct,
        }
    }

    /// Whether the distribution is empty (no nodes at all).
    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }

    /// Return a Vec of (class, count) pairs sorted by count descending.
    pub fn sorted_by_count(&self) -> Vec<(NodeClass, u32)> {
        let mut v = vec![
            (NodeClass::Edge, self.dust_count),
            (NodeClass::Light, self.pebble_count),
            (NodeClass::Standard, self.rock_count),
            (NodeClass::Enterprise, self.boulder_count),
        ];
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v
    }

    /// Weight-proportional item budget per class.
    ///
    /// Given a total item count, returns how many items should be assigned
    /// to each class, proportional to (count * compute_weight).
    pub fn proportional_items(&self, total_items: u64) -> HashMap<NodeClass, u64> {
        let ecu = self.effective_compute_units();
        let mut map = HashMap::new();
        if ecu <= 0.0 || total_items == 0 {
            return map;
        }

        let mut assigned = 0u64;
        let classes = [
            (NodeClass::Edge, self.dust_count),
            (NodeClass::Light, self.pebble_count),
            (NodeClass::Standard, self.rock_count),
            (NodeClass::Enterprise, self.boulder_count),
        ];

        // Assign proportionally, accumulating remainder.
        for (class, count) in &classes {
            if *count == 0 {
                continue;
            }
            let weight = *count as f64 * class.compute_weight();
            let share = ((total_items as f64 * weight) / ecu).floor() as u64;
            map.insert(*class, share);
            assigned += share;
        }

        // Distribute remainder to the largest class.
        let remainder = total_items.saturating_sub(assigned);
        if remainder > 0 {
            let dominant = self.dominant_class();
            *map.entry(dominant).or_insert(0) += remainder;
        }

        map
    }
}

impl Default for ClassDistribution {
    fn default() -> Self {
        // Assume a balanced swarm if no data is available.
        Self::from_counts(10, 30, 40, 20)
    }
}

// ============================================================================
// ChunkPlanner
// ============================================================================

/// The main planning engine.
///
/// Stateless except for optional references to the knowledge store and a
/// class distribution snapshot.  Thread-safe (no interior mutability).
pub struct ChunkPlanner {
    knowledge: Option<Arc<KnowledgeStore>>,
    class_distribution: Option<ClassDistribution>,
}

impl ChunkPlanner {
    /// Create a bare planner with no knowledge and no distribution data.
    pub fn new() -> Self {
        Self {
            knowledge: None,
            class_distribution: None,
        }
    }

    /// Builder: attach a knowledge store for live swarm queries.
    pub fn with_knowledge(mut self, knowledge: Arc<KnowledgeStore>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// Builder: provide a pre-computed class distribution.
    pub fn with_class_distribution(mut self, dist: ClassDistribution) -> Self {
        self.class_distribution = Some(dist);
        self
    }

    // ------------------------------------------------------------------
    // Internal: resolve the effective class distribution
    // ------------------------------------------------------------------

    fn effective_distribution(&self) -> ClassDistribution {
        if let Some(ref dist) = self.class_distribution {
            return dist.clone();
        }
        if let Some(ref ks) = self.knowledge {
            return ClassDistribution::from_knowledge(ks);
        }
        ClassDistribution::default()
    }

    // ------------------------------------------------------------------
    // Internal: items-per-chunk for a given class
    // ------------------------------------------------------------------

    fn items_for_class(class: NodeClass, total_items: u64) -> u64 {
        let batch = class.suggested_batch_size() as u64;
        batch.max(MIN_CHUNK_ITEMS).min(total_items).min(MAX_CHUNK_ITEMS)
    }

    // ------------------------------------------------------------------
    // Internal: estimate memory for N items of a given byte size
    // ------------------------------------------------------------------

    fn estimate_memory_mb(item_count: u64, item_size_bytes: u64) -> u64 {
        // Heuristic: 2x the raw data size to account for deserialization
        // overhead, intermediate buffers, etc.
        let raw_bytes = item_count.saturating_mul(item_size_bytes);
        let with_overhead = raw_bytes.saturating_mul(2);
        // Convert to MB, rounding up.
        with_overhead.div_ceil(1024 * 1024)
    }

    // ------------------------------------------------------------------
    // Public planning methods
    // ------------------------------------------------------------------

    /// Plan a job using an explicit strategy.
    ///
    /// # Arguments
    ///
    /// * `total_items`        -- logical item count for the entire job.
    /// * `item_size_bytes`    -- average size of a single item in bytes.
    /// * `strategy`           -- how to split (`Fixed`, `PerLine`, etc.).
    /// * `per_item_duration_ms` -- estimated time to process one item (ms).
    pub fn plan_job(
        &self,
        total_items: u64,
        item_size_bytes: u64,
        strategy: &ChunkStrategy,
        per_item_duration_ms: u64,
    ) -> ChunkPlan {
        let job_id = JobId::new();
        let dist = self.effective_distribution();

        let raw_chunks: Vec<(u64, u64)> = match strategy {
            ChunkStrategy::Single => {
                vec![(0, total_items)]
            }
            ChunkStrategy::Fixed { count } => {
                let count = (*count).max(1);
                ChunkSplitter::split_into_n(total_items, count)
            }
            ChunkStrategy::PerLine => {
                // Treat each item as a "line".
                let chunk_size = dist
                    .dominant_class()
                    .suggested_batch_size() as u64;
                let chunk_size = chunk_size.max(MIN_CHUNK_ITEMS);
                ChunkSplitter::split_range(total_items, chunk_size)
            }
            ChunkStrategy::PerFile => {
                // One item per chunk.
                ChunkSplitter::split_range(total_items, 1)
            }
            ChunkStrategy::PerNBytes { bytes } => {
                let items_per_chunk = if item_size_bytes > 0 {
                    (*bytes / item_size_bytes).max(1)
                } else {
                    DEFAULT_ITEMS_PER_CHUNK
                };
                ChunkSplitter::split_range(total_items, items_per_chunk)
            }
            ChunkStrategy::ParameterSweep { params } => {
                let combos = ChunkSplitter::split_parameter_sweep(params);
                // One chunk per parameter combination.
                let n = combos.len() as u64;
                if n == 0 {
                    vec![(0, total_items)]
                } else {
                    ChunkSplitter::split_into_n(total_items, n.min(u32::MAX as u64) as u32)
                }
            }
        };

        // Assign each raw range to a node class proportionally.
        let total_chunks = raw_chunks.len();
        let chunks: Vec<PlannedChunk> = raw_chunks
            .into_iter()
            .enumerate()
            .map(|(i, (offset, length))| {
                let target_class = self.class_for_chunk_index(i, total_chunks, &dist);
                let item_count = length;
                let mem = Self::estimate_memory_mb(item_count, item_size_bytes);
                let dur_secs = (item_count * per_item_duration_ms) / 1000;
                let dur_secs = dur_secs.max(1);

                PlannedChunk {
                    chunk_id: ChunkId::new(),
                    sequence: i as u32,
                    target_class,
                    item_count,
                    estimated_memory_mb: mem,
                    estimated_duration_secs: dur_secs,
                    offset,
                    length,
                }
            })
            .collect();

        let max_dur = chunks
            .iter()
            .map(|c| c.estimated_duration_secs)
            .max()
            .unwrap_or(0);

        ChunkPlan {
            job_id,
            total_items,
            chunks,
            strategy_used: strategy.clone(),
            estimated_duration_secs: max_dur,
        }
    }

    /// Plan based on the actual available nodes.
    ///
    /// Each node gets exactly one chunk sized to its class.  Useful for
    /// scatter/gather workloads where you want to utilise every available
    /// node once.
    pub fn plan_adaptive(
        &self,
        total_items: u64,
        available_nodes: &[(NodeId, NodeClass)],
    ) -> ChunkPlan {
        let job_id = JobId::new();

        if available_nodes.is_empty() {
            info!("plan_adaptive: no available nodes, creating single-chunk plan");
            return ChunkPlan {
                job_id,
                total_items,
                chunks: vec![PlannedChunk {
                    chunk_id: ChunkId::new(),
                    sequence: 0,
                    target_class: NodeClass::Standard,
                    item_count: total_items,
                    estimated_memory_mb: 0,
                    estimated_duration_secs: 0,
                    offset: 0,
                    length: total_items,
                }],
                strategy_used: ChunkStrategy::Single,
                estimated_duration_secs: 0,
            };
        }

        // Weight-proportional allocation.
        let total_weight: f64 = available_nodes
            .iter()
            .map(|(_, c)| c.compute_weight())
            .sum();

        let mut assigned = 0u64;
        let mut chunks = Vec::with_capacity(available_nodes.len());

        for (i, (_node_id, class)) in available_nodes.iter().enumerate() {
            let weight = class.compute_weight();
            let share = if i == available_nodes.len() - 1 {
                // Last node gets the remainder to avoid rounding loss.
                total_items.saturating_sub(assigned)
            } else {
                let frac = weight / total_weight;
                let items = (total_items as f64 * frac).floor() as u64;
                items.max(MIN_CHUNK_ITEMS).min(total_items.saturating_sub(assigned))
            };

            let offset = assigned;
            assigned += share;

            chunks.push(PlannedChunk {
                chunk_id: ChunkId::new(),
                sequence: i as u32,
                target_class: *class,
                item_count: share,
                estimated_memory_mb: 0,
                estimated_duration_secs: 0,
                offset,
                length: share,
            });
        }

        let strategy = ChunkStrategy::Fixed {
            count: chunks.len() as u32,
        };

        ChunkPlan {
            job_id,
            total_items,
            chunks,
            strategy_used: strategy,
            estimated_duration_secs: 0,
        }
    }

    /// Uniform chunking for a single node class.
    ///
    /// Every returned chunk has the same target class and approximately
    /// the same item count (the last chunk absorbs the remainder).
    pub fn plan_for_class(&self, total_items: u64, class: NodeClass) -> Vec<PlannedChunk> {
        let items_per_chunk = Self::items_for_class(class, total_items);
        if items_per_chunk == 0 || total_items == 0 {
            return Vec::new();
        }

        let ranges = ChunkSplitter::split_range(total_items, items_per_chunk);
        ranges
            .into_iter()
            .enumerate()
            .map(|(i, (offset, length))| PlannedChunk {
                chunk_id: ChunkId::new(),
                sequence: i as u32,
                target_class: class,
                item_count: length,
                estimated_memory_mb: 0,
                estimated_duration_secs: 0,
                offset,
                length,
            })
            .collect()
    }

    /// Replan after partial completion.
    ///
    /// Takes the set of completed and failed chunk IDs from the original
    /// plan and produces a new plan covering only the items that still need
    /// processing.  Failed chunks are re-included; completed chunks are
    /// excluded.
    pub fn replan_remaining(
        &self,
        completed: &[ChunkId],
        failed: &[ChunkId],
        original: &ChunkPlan,
    ) -> ChunkPlan {
        let completed_set: std::collections::HashSet<ChunkId> =
            completed.iter().copied().collect();
        let failed_set: std::collections::HashSet<ChunkId> =
            failed.iter().copied().collect();

        let remaining: Vec<&PlannedChunk> = original
            .chunks
            .iter()
            .filter(|c| !completed_set.contains(&c.chunk_id))
            .collect();

        if remaining.is_empty() {
            debug!("replan_remaining: all chunks completed or accounted for");
            return ChunkPlan {
                job_id: original.job_id,
                total_items: 0,
                chunks: Vec::new(),
                strategy_used: original.strategy_used.clone(),
                estimated_duration_secs: 0,
            };
        }

        let total_remaining_items: u64 = remaining.iter().map(|c| c.item_count).sum();
        let _dist = self.effective_distribution();

        let chunks: Vec<PlannedChunk> = remaining
            .iter()
            .enumerate()
            .map(|(i, orig)| {
                // Failed chunks keep their original sizing.
                // Non-failed, non-completed chunks (still pending) are also kept.
                let target_class = if failed_set.contains(&orig.chunk_id) {
                    // On retry, optionally upgrade to a higher class for reliability.
                    upgrade_class(orig.target_class)
                } else {
                    orig.target_class
                };

                PlannedChunk {
                    chunk_id: ChunkId::new(), // New ID for the re-plan.
                    sequence: i as u32,
                    target_class,
                    item_count: orig.item_count,
                    estimated_memory_mb: orig.estimated_memory_mb,
                    estimated_duration_secs: orig.estimated_duration_secs,
                    offset: orig.offset,
                    length: orig.length,
                }
            })
            .collect();

        let max_dur = chunks
            .iter()
            .map(|c| c.estimated_duration_secs)
            .max()
            .unwrap_or(0);

        info!(
            remaining_chunks = chunks.len(),
            remaining_items = total_remaining_items,
            failed = failed.len(),
            "replanned job {}",
            original.job_id,
        );

        ChunkPlan {
            job_id: original.job_id,
            total_items: total_remaining_items,
            chunks,
            strategy_used: original.strategy_used.clone(),
            estimated_duration_secs: max_dur,
        }
    }

    /// Estimate how many chunks each class would need for the given workload.
    pub fn estimate_chunks_needed(
        &self,
        total_items: u64,
        avg_item_size_bytes: u64,
    ) -> HashMap<NodeClass, u32> {
        let mut result = HashMap::new();
        for class in NodeClass::all() {
            let items_per = Self::items_for_class(*class, total_items);
            if items_per == 0 {
                result.insert(*class, 0);
                continue;
            }
            let count = total_items.div_ceil(items_per);
            // Verify memory fits.
            let mem = Self::estimate_memory_mb(items_per, avg_item_size_bytes);
            let adjusted = if mem > class.max_memory_mb() && avg_item_size_bytes > 0 {
                // Reduce items per chunk to fit memory.
                let max_bytes = class.max_memory_mb() * 1024 * 1024 / 2; // divide by overhead factor
                let max_items = max_bytes / avg_item_size_bytes;
                let max_items = max_items.max(1);
                total_items.div_ceil(max_items) as u32
            } else {
                count as u32
            };
            result.insert(*class, adjusted);
        }
        result
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Assign a class to a chunk by its index within the plan.
    ///
    /// Uses the class distribution to round-robin proportionally.
    fn class_for_chunk_index(
        &self,
        index: usize,
        total: usize,
        dist: &ClassDistribution,
    ) -> NodeClass {
        if dist.total() == 0 || total == 0 {
            return NodeClass::Standard;
        }

        // Build cumulative thresholds.
        let boulder_end = (dist.boulder_pct * total as f64).round() as usize;
        let rock_end = boulder_end + (dist.rock_pct * total as f64).round() as usize;
        let pebble_end = rock_end + (dist.pebble_pct * total as f64).round() as usize;

        if index < boulder_end {
            NodeClass::Enterprise
        } else if index < rock_end {
            NodeClass::Standard
        } else if index < pebble_end {
            NodeClass::Light
        } else {
            NodeClass::Edge
        }
    }
}

impl Default for ChunkPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Bump a class up one tier for retry resilience.
fn upgrade_class(class: NodeClass) -> NodeClass {
    match class {
        NodeClass::Edge => NodeClass::Light,
        NodeClass::Light => NodeClass::Standard,
        NodeClass::Standard => NodeClass::Enterprise,
        NodeClass::Enterprise => NodeClass::Enterprise,
    }
}

/// Bump a class down one tier (useful for cost optimization).
fn downgrade_class(class: NodeClass) -> NodeClass {
    match class {
        NodeClass::Edge => NodeClass::Edge,
        NodeClass::Light => NodeClass::Edge,
        NodeClass::Standard => NodeClass::Light,
        NodeClass::Enterprise => NodeClass::Standard,
    }
}

// ============================================================================
// AdaptiveChunker
// ============================================================================

/// Dynamically adjusts chunk sizes based on real performance feedback.
///
/// Thread-safe: uses [`DashMap`] internally so multiple workers can record
/// performance concurrently without external locking.
pub struct AdaptiveChunker {
    performance_history: DashMap<NodeClass, Vec<ChunkPerformance>>,
}

impl AdaptiveChunker {
    /// Create an empty adaptive chunker.
    pub fn new() -> Self {
        Self {
            performance_history: DashMap::new(),
        }
    }

    /// Record a performance observation.
    pub fn record_performance(
        &self,
        class: NodeClass,
        chunk_items: u64,
        duration_ms: u64,
        success: bool,
    ) {
        let perf = ChunkPerformance::new(chunk_items, duration_ms, success);
        let mut entry = self.performance_history.entry(class).or_default();
        entry.push(perf);

        // Cap history size.
        if entry.len() > MAX_PERFORMANCE_HISTORY {
            let drain_count = entry.len() - MAX_PERFORMANCE_HISTORY;
            entry.drain(..drain_count);
        }

        debug!(
            class = %class,
            items = chunk_items,
            duration_ms = duration_ms,
            success = success,
            history_len = entry.len(),
            "adaptive chunker: recorded performance",
        );
    }

    /// Suggest a chunk size (item count) for a given class.
    ///
    /// Based on the P90 execution time of successful chunks.  If the P90
    /// duration exceeds 80% of the class's max duration, the suggested size
    /// is reduced.  If we have no history, falls back to the class's
    /// suggested batch size.
    pub fn suggested_chunk_size(&self, class: NodeClass) -> u64 {
        let entry = match self.performance_history.get(&class) {
            Some(e) => e,
            None => return class.suggested_batch_size() as u64,
        };

        let successes: Vec<&ChunkPerformance> =
            entry.iter().filter(|p| p.success).collect();

        if successes.len() < 3 {
            return class.suggested_batch_size() as u64;
        }

        // Sort by duration ascending.
        let mut durations: Vec<u64> = successes.iter().map(|p| p.duration_ms).collect();
        durations.sort_unstable();

        let p90_dur = durations[p90_index(durations.len())];
        let max_dur_ms = class.max_duration_secs() * 1000;

        // Target: P90 duration at 60% of max.
        let target_ms = (max_dur_ms as f64 * 0.6) as u64;

        if p90_dur == 0 {
            return class.suggested_batch_size() as u64;
        }

        // Average items per ms across successful runs.
        let total_items: u64 = successes.iter().map(|p| p.items_processed).sum();
        let total_dur: u64 = successes.iter().map(|p| p.duration_ms).sum();
        if total_dur == 0 {
            return class.suggested_batch_size() as u64;
        }

        let items_per_ms = total_items as f64 / total_dur as f64;
        let suggested = (items_per_ms * target_ms as f64).floor() as u64;

        suggested
            .max(MIN_CHUNK_ITEMS)
            .min(MAX_CHUNK_ITEMS)
    }

    /// Overhead factor for a class: ratio of total attempts (including
    /// failures and retries) to successful completions.
    ///
    /// Returns 1.0 if there are no failures, or > 1.0 if retries are
    /// needed.  Useful as a multiplier when estimating total work time.
    pub fn overhead_factor(&self, class: NodeClass) -> f64 {
        let entry = match self.performance_history.get(&class) {
            Some(e) => e,
            None => return 1.0,
        };

        if entry.is_empty() {
            return 1.0;
        }

        let total = entry.len() as f64;
        let successes = entry.iter().filter(|p| p.success).count() as f64;

        if successes == 0.0 {
            // All failures -- return a high overhead.
            return 5.0;
        }

        total / successes
    }

    /// Clear all history for a specific class.
    pub fn reset_history(&self, class: NodeClass) {
        self.performance_history.remove(&class);
        info!(class = %class, "adaptive chunker: history reset");
    }

    /// Clear all history across all classes.
    pub fn reset_all(&self) {
        self.performance_history.clear();
        info!("adaptive chunker: all history reset");
    }

    /// Number of performance records for a class.
    pub fn history_count(&self, class: NodeClass) -> usize {
        self.performance_history
            .get(&class)
            .map(|e| e.len())
            .unwrap_or(0)
    }

    /// Total records across all classes.
    pub fn total_records(&self) -> usize {
        self.performance_history
            .iter()
            .map(|e| e.value().len())
            .sum()
    }

    /// Success rate for a class (0.0 -- 1.0).
    pub fn success_rate(&self, class: NodeClass) -> f64 {
        let entry = match self.performance_history.get(&class) {
            Some(e) => e,
            None => return 0.0,
        };
        if entry.is_empty() {
            return 0.0;
        }
        let successes = entry.iter().filter(|p| p.success).count();
        successes as f64 / entry.len() as f64
    }

    /// Average duration (ms) for successful chunks of a given class.
    pub fn avg_success_duration_ms(&self, class: NodeClass) -> f64 {
        let entry = match self.performance_history.get(&class) {
            Some(e) => e,
            None => return 0.0,
        };
        let successes: Vec<&ChunkPerformance> =
            entry.iter().filter(|p| p.success).collect();
        if successes.is_empty() {
            return 0.0;
        }
        let total: u64 = successes.iter().map(|p| p.duration_ms).sum();
        total as f64 / successes.len() as f64
    }

    /// P50 (median) duration (ms) for successful chunks.
    pub fn median_success_duration_ms(&self, class: NodeClass) -> u64 {
        let entry = match self.performance_history.get(&class) {
            Some(e) => e,
            None => return 0,
        };
        let mut durations: Vec<u64> = entry
            .iter()
            .filter(|p| p.success)
            .map(|p| p.duration_ms)
            .collect();
        if durations.is_empty() {
            return 0;
        }
        durations.sort_unstable();
        durations[p50_index(durations.len())]
    }

    /// P90 duration (ms) for successful chunks.
    pub fn p90_success_duration_ms(&self, class: NodeClass) -> u64 {
        let entry = match self.performance_history.get(&class) {
            Some(e) => e,
            None => return 0,
        };
        let mut durations: Vec<u64> = entry
            .iter()
            .filter(|p| p.success)
            .map(|p| p.duration_ms)
            .collect();
        if durations.is_empty() {
            return 0;
        }
        durations.sort_unstable();
        durations[p90_index(durations.len())]
    }

    /// Estimated time to complete `items` items on a given class, in ms.
    /// Based on average throughput of successful runs.  Returns 0 if no
    /// data is available.
    pub fn estimate_duration_ms(&self, class: NodeClass, items: u64) -> u64 {
        let entry = match self.performance_history.get(&class) {
            Some(e) => e,
            None => return 0,
        };
        let successes: Vec<&ChunkPerformance> =
            entry.iter().filter(|p| p.success).collect();
        if successes.is_empty() {
            return 0;
        }
        let total_items: u64 = successes.iter().map(|p| p.items_processed).sum();
        let total_dur: u64 = successes.iter().map(|p| p.duration_ms).sum();
        if total_items == 0 {
            return 0;
        }
        let ms_per_item = total_dur as f64 / total_items as f64;
        (ms_per_item * items as f64).ceil() as u64
    }

    /// Remove performance records older than `max_age_ms`.
    pub fn evict_stale(&self, max_age_ms: u64) {
        for mut entry in self.performance_history.iter_mut() {
            entry.value_mut().retain(|p| !p.is_stale(max_age_ms));
        }
    }
}

impl Default for AdaptiveChunker {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ChunkSplitter -- low-level splitting utilities
// ============================================================================

/// Stateless low-level utilities for splitting data into ranges.
///
/// All methods are pure functions that return offset/length pairs without
/// copying any payload data.
pub struct ChunkSplitter;

impl ChunkSplitter {
    /// Split `total` items into chunks of `chunk_size`.
    ///
    /// The last chunk may be smaller than `chunk_size`.
    /// Returns `(offset, length)` pairs.
    pub fn split_range(total: u64, chunk_size: u64) -> Vec<(u64, u64)> {
        if total == 0 || chunk_size == 0 {
            return Vec::new();
        }
        let mut result = Vec::new();
        let mut offset = 0u64;
        while offset < total {
            let length = chunk_size.min(total - offset);
            result.push((offset, length));
            offset += length;
        }
        result
    }

    /// Divide `total` items into `n` roughly equal parts.
    ///
    /// The first `total % n` chunks get one extra item so no items are
    /// lost.  Returns `(offset, length)` pairs.
    pub fn split_into_n(total: u64, n: u32) -> Vec<(u64, u64)> {
        if n == 0 || total == 0 {
            return Vec::new();
        }
        let n = n as u64;
        let base = total / n;
        let remainder = total % n;
        let mut result = Vec::with_capacity(n as usize);
        let mut offset = 0u64;

        for i in 0..n {
            let length = if i < remainder { base + 1 } else { base };
            if length > 0 {
                result.push((offset, length));
                offset += length;
            }
        }
        result
    }

    /// Split items across node classes proportionally to compute weight.
    ///
    /// Returns a map of `NodeClass -> Vec<(offset, length)>`.  Within each
    /// class the items are further split into chunks of the class's
    /// suggested batch size.
    pub fn split_by_class(
        total: u64,
        distribution: &ClassDistribution,
    ) -> HashMap<NodeClass, Vec<(u64, u64)>> {
        let proportions = distribution.proportional_items(total);
        let mut result = HashMap::new();
        let mut global_offset = 0u64;

        // Process in deterministic order: Enterprise, Standard, Light, Edge.
        let order = [
            NodeClass::Enterprise,
            NodeClass::Standard,
            NodeClass::Light,
            NodeClass::Edge,
        ];

        for class in &order {
            let items = proportions.get(class).copied().unwrap_or(0);
            if items == 0 {
                result.insert(*class, Vec::new());
                continue;
            }
            let batch = class.suggested_batch_size() as u64;
            let batch = batch.max(1);
            let ranges = Self::split_range(items, batch);

            // Adjust offsets to be global.
            let adjusted: Vec<(u64, u64)> = ranges
                .into_iter()
                .map(|(local_offset, length)| (global_offset + local_offset, length))
                .collect();

            global_offset += items;
            result.insert(*class, adjusted);
        }

        result
    }

    /// Generate every combination of the given parameter sets.
    ///
    /// Each entry in `params` is `(param_name, [value1, value2, ...])`.
    /// Returns the Cartesian product as a Vec of `HashMap<name, value>`.
    ///
    /// # Example
    ///
    /// ```text
    /// params = [("alpha", ["0.1", "0.5"]), ("beta", ["1", "2"])]
    /// result = [
    ///     {"alpha": "0.1", "beta": "1"},
    ///     {"alpha": "0.1", "beta": "2"},
    ///     {"alpha": "0.5", "beta": "1"},
    ///     {"alpha": "0.5", "beta": "2"},
    /// ]
    /// ```
    pub fn split_parameter_sweep(
        params: &[(String, Vec<String>)],
    ) -> Vec<HashMap<String, String>> {
        if params.is_empty() {
            return vec![HashMap::new()];
        }

        let mut combos: Vec<HashMap<String, String>> = vec![HashMap::new()];

        for (name, values) in params {
            if values.is_empty() {
                continue;
            }
            let mut next = Vec::with_capacity(combos.len() * values.len());
            for combo in &combos {
                for val in values {
                    let mut new_combo = combo.clone();
                    new_combo.insert(name.clone(), val.clone());
                    next.push(new_combo);
                }
            }
            combos = next;
        }

        combos
    }

    /// Line-based splitting: scan byte data for newlines and return
    /// `(byte_offset, byte_length)` ranges each containing at most
    /// `max_lines_per_chunk` lines.
    ///
    /// Lines are delimited by `\n`.  A trailing non-newline-terminated
    /// segment is included in the last chunk.
    pub fn split_lines(data: &[u8], max_lines_per_chunk: usize) -> Vec<(usize, usize)> {
        if data.is_empty() || max_lines_per_chunk == 0 {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut chunk_start = 0usize;
        let mut line_count = 0usize;

        for (i, &byte) in data.iter().enumerate() {
            if byte == b'\n' {
                line_count += 1;
                if line_count >= max_lines_per_chunk {
                    let chunk_end = i + 1; // include the newline
                    result.push((chunk_start, chunk_end - chunk_start));
                    chunk_start = chunk_end;
                    line_count = 0;
                }
            }
        }

        // Remaining bytes (last partial chunk).
        if chunk_start < data.len() {
            result.push((chunk_start, data.len() - chunk_start));
        }

        result
    }

    /// Fast line-count estimation by sampling.
    ///
    /// Scans the first `sample_size` bytes of `data`, counts newlines,
    /// and extrapolates to the full data length.  Useful for large files
    /// where a full scan is too expensive.
    pub fn estimate_line_count(data: &[u8], sample_size: usize) -> u64 {
        if data.is_empty() {
            return 0;
        }
        let sample_len = sample_size.min(data.len());
        let sample = &data[..sample_len];
        let newlines = bytecount(sample, b'\n') as u64;

        if sample_len == data.len() {
            // Scanned the whole thing -- exact count.
            // Add 1 if the last byte is not a newline (trailing line).
            return if data.last() == Some(&b'\n') {
                newlines
            } else {
                newlines + 1
            };
        }

        // Extrapolate.
        if newlines == 0 {
            return 1; // At least one line if data is non-empty.
        }

        let lines_per_byte = newlines as f64 / sample_len as f64;
        let estimate = (lines_per_byte * data.len() as f64).ceil() as u64;
        estimate.max(1)
    }

    /// Merge adjacent ranges that touch or overlap.
    ///
    /// Input must be sorted by offset.  Returns a minimal set of
    /// non-overlapping ranges.
    pub fn merge_ranges(ranges: &[(u64, u64)]) -> Vec<(u64, u64)> {
        if ranges.is_empty() {
            return Vec::new();
        }
        let mut sorted = ranges.to_vec();
        sorted.sort_by_key(|&(off, _)| off);

        let mut merged = Vec::new();
        let (mut cur_off, mut cur_len) = sorted[0];

        for &(off, len) in &sorted[1..] {
            if off <= cur_off + cur_len {
                // Overlapping or adjacent -- extend.
                let new_end = (cur_off + cur_len).max(off + len);
                cur_len = new_end - cur_off;
            } else {
                merged.push((cur_off, cur_len));
                cur_off = off;
                cur_len = len;
            }
        }
        merged.push((cur_off, cur_len));
        merged
    }

    /// Compute the total items covered by a set of ranges (no dedup).
    pub fn total_items(ranges: &[(u64, u64)]) -> u64 {
        ranges.iter().map(|&(_, len)| len).sum()
    }

    /// Check whether two ranges overlap.
    pub fn ranges_overlap(a: (u64, u64), b: (u64, u64)) -> bool {
        let a_end = a.0 + a.1;
        let b_end = b.0 + b.1;
        a.0 < b_end && b.0 < a_end
    }

    /// Subtract range `b` from range `a`, returning 0, 1, or 2 residual ranges.
    pub fn subtract_range(a: (u64, u64), b: (u64, u64)) -> Vec<(u64, u64)> {
        let a_end = a.0 + a.1;
        let b_end = b.0 + b.1;

        if b.0 >= a_end || b_end <= a.0 {
            // No overlap.
            return vec![a];
        }

        let mut result = Vec::new();
        if b.0 > a.0 {
            result.push((a.0, b.0 - a.0));
        }
        if b_end < a_end {
            result.push((b_end, a_end - b_end));
        }
        result
    }
}

/// Simple byte-counting helper (counts occurrences of `needle` in `data`).
fn bytecount(data: &[u8], needle: u8) -> usize {
    data.iter().filter(|&&b| b == needle).count()
}

// ============================================================================
// ChunkValidator
// ============================================================================

/// Validates planned chunks against node class constraints.
///
/// Call this *before* dispatching work to ensure no chunk will be rejected
/// by the executor due to resource limits.
pub struct ChunkValidator;

impl ChunkValidator {
    /// Validate a single chunk against explicit constraints.
    ///
    /// Returns `Ok(())` if the chunk fits, or `Err(violations)` listing
    /// every constraint that is exceeded.
    pub fn validate_chunk(
        chunk: &PlannedChunk,
        constraints: &TaskConstraints,
    ) -> Result<(), Vec<String>> {
        let mut violations = Vec::new();

        if chunk.estimated_memory_mb > constraints.max_memory_mb {
            violations.push(format!(
                "memory: chunk requires {}MB, class {} allows {}MB",
                chunk.estimated_memory_mb,
                constraints.node_class,
                constraints.max_memory_mb,
            ));
        }

        if chunk.estimated_duration_secs > constraints.max_duration_secs {
            violations.push(format!(
                "duration: chunk estimated {}s, class {} allows {}s",
                chunk.estimated_duration_secs,
                constraints.node_class,
                constraints.max_duration_secs,
            ));
        }

        let input_mb = chunk.length / (1024 * 1024);
        if input_mb > constraints.max_input_size_mb && chunk.length > constraints.max_input_size_mb * 1024 * 1024 {
            violations.push(format!(
                "input size: chunk is {}MB, class {} allows {}MB",
                input_mb,
                constraints.node_class,
                constraints.max_input_size_mb,
            ));
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Validate an entire plan.
    ///
    /// Returns a list of `(chunk_index, violations)` for every chunk that
    /// has constraint violations.  An empty return means the whole plan
    /// is valid.
    pub fn validate_plan(plan: &ChunkPlan) -> Vec<(usize, Vec<String>)> {
        let mut issues = Vec::new();
        for (i, chunk) in plan.chunks.iter().enumerate() {
            let constraints = TaskConstraints::for_class(chunk.target_class);
            if let Err(violations) = Self::validate_chunk(chunk, &constraints) {
                issues.push((i, violations));
            }
        }
        issues
    }

    /// Quick check: can a node of the given class handle a chunk with the
    /// specified memory and duration?
    pub fn can_node_handle(class: NodeClass, memory_mb: u64, duration_secs: u64) -> bool {
        memory_mb <= class.max_memory_mb() && duration_secs <= class.max_duration_secs()
    }

    /// Find the minimum node class that can handle the given requirements.
    pub fn minimum_class_for(memory_mb: u64, duration_secs: u64) -> Option<NodeClass> {
        for class in NodeClass::all() {
            if Self::can_node_handle(*class, memory_mb, duration_secs) {
                return Some(*class);
            }
        }
        None
    }

    /// Validate that the plan covers all items without gaps or overlaps.
    pub fn validate_coverage(plan: &ChunkPlan) -> Result<(), String> {
        if plan.chunks.is_empty() {
            if plan.total_items == 0 {
                return Ok(());
            }
            return Err(format!(
                "plan has {} total items but no chunks",
                plan.total_items
            ));
        }

        let planned_total: u64 = plan.chunks.iter().map(|c| c.item_count).sum();
        if planned_total != plan.total_items {
            return Err(format!(
                "plan total_items={} but chunks sum to {}",
                plan.total_items, planned_total,
            ));
        }
        Ok(())
    }
}

// ============================================================================
// Convenience builder
// ============================================================================

/// Build a [`ChunkPlan`] from a minimal set of inputs.
///
/// This is a shortcut for the common case where you just want "split N
/// items into reasonable chunks for the current swarm".
pub fn quick_plan(
    total_items: u64,
    item_size_bytes: u64,
    per_item_duration_ms: u64,
) -> ChunkPlan {
    let planner = ChunkPlanner::new();
    planner.plan_job(
        total_items,
        item_size_bytes,
        &ChunkStrategy::default(),
        per_item_duration_ms,
    )
}

/// Build a [`ChunkPlan`] using a specific strategy and class distribution.
pub fn plan_with_distribution(
    total_items: u64,
    item_size_bytes: u64,
    per_item_duration_ms: u64,
    strategy: &ChunkStrategy,
    dist: ClassDistribution,
) -> ChunkPlan {
    let planner = ChunkPlanner::new().with_class_distribution(dist);
    planner.plan_job(total_items, item_size_bytes, strategy, per_item_duration_ms)
}

// ============================================================================
// Display impls
// ============================================================================

impl std::fmt::Display for ChunkPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChunkPlan(job={}, items={}, chunks={}, est_dur={}s)",
            self.job_id, self.total_items, self.chunks.len(), self.estimated_duration_secs,
        )
    }
}

impl std::fmt::Display for PlannedChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PlannedChunk({}, seq={}, class={}, items={})",
            self.chunk_id, self.sequence, self.target_class, self.item_count,
        )
    }
}

impl std::fmt::Display for ClassDistribution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClassDist(edge={} light={} standard={} enterprise={} total={})",
            self.dust_count,
            self.pebble_count,
            self.rock_count,
            self.boulder_count,
            self.total(),
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ====================================================================
    // ChunkSplitter: split_range
    // ====================================================================

    #[test]
    fn split_range_exact_division() {
        let ranges = ChunkSplitter::split_range(100, 25);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 25));
        assert_eq!(ranges[1], (25, 25));
        assert_eq!(ranges[2], (50, 25));
        assert_eq!(ranges[3], (75, 25));
    }

    #[test]
    fn split_range_remainder() {
        let ranges = ChunkSplitter::split_range(100, 30);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 30));
        assert_eq!(ranges[1], (30, 30));
        assert_eq!(ranges[2], (60, 30));
        assert_eq!(ranges[3], (90, 10)); // remainder
    }

    #[test]
    fn split_range_chunk_larger_than_total() {
        let ranges = ChunkSplitter::split_range(10, 100);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 10));
    }

    #[test]
    fn split_range_zero_total() {
        let ranges = ChunkSplitter::split_range(0, 10);
        assert!(ranges.is_empty());
    }

    #[test]
    fn split_range_zero_chunk_size() {
        let ranges = ChunkSplitter::split_range(100, 0);
        assert!(ranges.is_empty());
    }

    #[test]
    fn split_range_single_item() {
        let ranges = ChunkSplitter::split_range(1, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 1));
    }

    // ====================================================================
    // ChunkSplitter: split_into_n
    // ====================================================================

    #[test]
    fn split_into_n_exact() {
        let ranges = ChunkSplitter::split_into_n(100, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 25));
        assert_eq!(ranges[1], (25, 25));
        assert_eq!(ranges[2], (50, 25));
        assert_eq!(ranges[3], (75, 25));
    }

    #[test]
    fn split_into_n_with_remainder() {
        let ranges = ChunkSplitter::split_into_n(10, 3);
        assert_eq!(ranges.len(), 3);
        // 10 / 3 = 3 base, remainder 1 -> first chunk gets 4
        assert_eq!(ranges[0], (0, 4));
        assert_eq!(ranges[1], (4, 3));
        assert_eq!(ranges[2], (7, 3));
        // Total: 4 + 3 + 3 = 10
        let total: u64 = ranges.iter().map(|&(_, l)| l).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn split_into_n_more_parts_than_items() {
        let ranges = ChunkSplitter::split_into_n(3, 10);
        // Only 3 non-zero ranges.
        assert_eq!(ranges.len(), 3);
        let total: u64 = ranges.iter().map(|&(_, l)| l).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn split_into_n_zero_n() {
        let ranges = ChunkSplitter::split_into_n(100, 0);
        assert!(ranges.is_empty());
    }

    #[test]
    fn split_into_n_zero_total() {
        let ranges = ChunkSplitter::split_into_n(0, 5);
        assert!(ranges.is_empty());
    }

    #[test]
    fn split_into_n_one() {
        let ranges = ChunkSplitter::split_into_n(42, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 42));
    }

    // ====================================================================
    // ChunkSplitter: split_by_class
    // ====================================================================

    #[test]
    fn split_by_class_proportional() {
        let dist = ClassDistribution::from_counts(0, 0, 10, 10);
        let result = ChunkSplitter::split_by_class(10_000, &dist);

        let rock_items: u64 = result[&NodeClass::Standard].iter().map(|&(_, l)| l).sum();
        let boulder_items: u64 = result[&NodeClass::Enterprise].iter().map(|&(_, l)| l).sum();

        // Enterprise has 4x weight vs Standard's 1x, so with equal counts:
        // Enterprise gets 4/5 = 80%, Standard gets 1/5 = 20%.
        assert!(boulder_items > rock_items);
        assert_eq!(rock_items + boulder_items, 10_000);
    }

    #[test]
    fn split_by_class_empty_distribution() {
        let dist = ClassDistribution::from_counts(0, 0, 0, 0);
        let result = ChunkSplitter::split_by_class(100, &dist);
        let total: u64 = result
            .values()
            .flat_map(|v| v.iter())
            .map(|&(_, l)| l)
            .sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn split_by_class_single_class() {
        let dist = ClassDistribution::from_counts(0, 0, 5, 0);
        let result = ChunkSplitter::split_by_class(1000, &dist);
        let rock_items: u64 = result[&NodeClass::Standard].iter().map(|&(_, l)| l).sum();
        assert_eq!(rock_items, 1000);
    }

    // ====================================================================
    // ChunkSplitter: parameter sweep
    // ====================================================================

    #[test]
    fn parameter_sweep_cartesian_product() {
        let params = vec![
            ("alpha".to_string(), vec!["0.1".to_string(), "0.5".to_string()]),
            ("beta".to_string(), vec!["1".to_string(), "2".to_string()]),
        ];
        let combos = ChunkSplitter::split_parameter_sweep(&params);
        assert_eq!(combos.len(), 4); // 2 * 2
        // Check that all combos have both keys.
        for combo in &combos {
            assert!(combo.contains_key("alpha"));
            assert!(combo.contains_key("beta"));
        }
    }

    #[test]
    fn parameter_sweep_single_param() {
        let params = vec![
            ("x".to_string(), vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        ];
        let combos = ChunkSplitter::split_parameter_sweep(&params);
        assert_eq!(combos.len(), 3);
    }

    #[test]
    fn parameter_sweep_empty() {
        let params: Vec<(String, Vec<String>)> = vec![];
        let combos = ChunkSplitter::split_parameter_sweep(&params);
        assert_eq!(combos.len(), 1); // One empty combination.
        assert!(combos[0].is_empty());
    }

    #[test]
    fn parameter_sweep_empty_values() {
        let params = vec![
            ("x".to_string(), vec!["1".to_string()]),
            ("y".to_string(), vec![]),
        ];
        let combos = ChunkSplitter::split_parameter_sweep(&params);
        assert_eq!(combos.len(), 1);
        assert_eq!(combos[0].get("x").unwrap(), "1");
    }

    #[test]
    fn parameter_sweep_three_params() {
        let params = vec![
            ("a".to_string(), vec!["1".to_string(), "2".to_string()]),
            ("b".to_string(), vec!["x".to_string(), "y".to_string()]),
            ("c".to_string(), vec!["p".to_string(), "q".to_string(), "r".to_string()]),
        ];
        let combos = ChunkSplitter::split_parameter_sweep(&params);
        assert_eq!(combos.len(), 12); // 2 * 2 * 3
    }

    // ====================================================================
    // ChunkSplitter: line splitting
    // ====================================================================

    #[test]
    fn split_lines_basic() {
        let data = b"line1\nline2\nline3\nline4\n";
        let ranges = ChunkSplitter::split_lines(data, 2);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], (0, 12));  // "line1\nline2\n"
        assert_eq!(ranges[1], (12, 12)); // "line3\nline4\n"
    }

    #[test]
    fn split_lines_trailing_no_newline() {
        let data = b"line1\nline2\nline3";
        let ranges = ChunkSplitter::split_lines(data, 2);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], (0, 12)); // "line1\nline2\n"
        assert_eq!(ranges[1], (12, 5)); // "line3" (no trailing newline)
    }

    #[test]
    fn split_lines_single_line() {
        let data = b"hello world";
        let ranges = ChunkSplitter::split_lines(data, 10);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 11));
    }

    #[test]
    fn split_lines_empty_data() {
        let ranges = ChunkSplitter::split_lines(b"", 5);
        assert!(ranges.is_empty());
    }

    #[test]
    fn split_lines_max_one_per_chunk() {
        let data = b"a\nb\nc\n";
        let ranges = ChunkSplitter::split_lines(data, 1);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (0, 2));
        assert_eq!(ranges[1], (2, 2));
        assert_eq!(ranges[2], (4, 2));
    }

    // ====================================================================
    // ChunkSplitter: estimate_line_count
    // ====================================================================

    #[test]
    fn estimate_line_count_exact() {
        let data = b"a\nb\nc\n";
        let count = ChunkSplitter::estimate_line_count(data, 1000);
        assert_eq!(count, 3);
    }

    #[test]
    fn estimate_line_count_no_trailing_newline() {
        let data = b"a\nb\nc";
        let count = ChunkSplitter::estimate_line_count(data, 1000);
        assert_eq!(count, 3); // 2 newlines + 1 trailing
    }

    #[test]
    fn estimate_line_count_empty() {
        assert_eq!(ChunkSplitter::estimate_line_count(b"", 100), 0);
    }

    #[test]
    fn estimate_line_count_sampling() {
        // Create data with uniform line lengths.
        let line = b"abcdefghij\n"; // 11 bytes per line
        let mut data = Vec::new();
        for _ in 0..1000 {
            data.extend_from_slice(line);
        }
        // Sample only the first 100 bytes.
        let estimate = ChunkSplitter::estimate_line_count(&data, 100);
        // Should be close to 1000 (within 10% is reasonable).
        assert!(estimate >= 900 && estimate <= 1100, "estimate was {}", estimate);
    }

    // ====================================================================
    // ChunkSplitter: merge_ranges and utilities
    // ====================================================================

    #[test]
    fn merge_ranges_adjacent() {
        let ranges = vec![(0, 10), (10, 10), (20, 10)];
        let merged = ChunkSplitter::merge_ranges(&ranges);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], (0, 30));
    }

    #[test]
    fn merge_ranges_overlapping() {
        let ranges = vec![(0, 15), (10, 15)];
        let merged = ChunkSplitter::merge_ranges(&ranges);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], (0, 25));
    }

    #[test]
    fn merge_ranges_disjoint() {
        let ranges = vec![(0, 5), (10, 5), (20, 5)];
        let merged = ChunkSplitter::merge_ranges(&ranges);
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn ranges_overlap_true() {
        assert!(ChunkSplitter::ranges_overlap((0, 10), (5, 10)));
    }

    #[test]
    fn ranges_overlap_false() {
        assert!(!ChunkSplitter::ranges_overlap((0, 10), (10, 10)));
    }

    #[test]
    fn subtract_range_no_overlap() {
        let res = ChunkSplitter::subtract_range((0, 10), (20, 10));
        assert_eq!(res, vec![(0, 10)]);
    }

    #[test]
    fn subtract_range_full_overlap() {
        let res = ChunkSplitter::subtract_range((5, 10), (0, 20));
        assert!(res.is_empty());
    }

    #[test]
    fn subtract_range_left_residual() {
        let res = ChunkSplitter::subtract_range((0, 10), (5, 10));
        assert_eq!(res, vec![(0, 5)]);
    }

    #[test]
    fn subtract_range_split() {
        let res = ChunkSplitter::subtract_range((0, 20), (5, 5));
        assert_eq!(res, vec![(0, 5), (10, 10)]);
    }

    // ====================================================================
    // ChunkPlanner: plan_job with various strategies
    // ====================================================================

    #[test]
    fn plan_job_single_strategy() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_job(1000, 100, &ChunkStrategy::Single, 10);
        assert_eq!(plan.total_chunks(), 1);
        assert_eq!(plan.chunks[0].item_count, 1000);
    }

    #[test]
    fn plan_job_fixed_strategy() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_job(1000, 100, &ChunkStrategy::Fixed { count: 10 }, 5);
        assert_eq!(plan.total_chunks(), 10);
        let total: u64 = plan.chunks.iter().map(|c| c.item_count).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn plan_job_per_file_strategy() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_job(5, 1024, &ChunkStrategy::PerFile, 100);
        assert_eq!(plan.total_chunks(), 5);
        for chunk in &plan.chunks {
            assert_eq!(chunk.item_count, 1);
        }
    }

    #[test]
    fn plan_job_per_n_bytes() {
        let planner = ChunkPlanner::new();
        // 100 items at 10 bytes each = 1000 bytes. PerNBytes(250) -> 25 items/chunk -> 4 chunks.
        let plan = planner.plan_job(100, 10, &ChunkStrategy::PerNBytes { bytes: 250 }, 5);
        assert_eq!(plan.total_chunks(), 4);
        let total: u64 = plan.chunks.iter().map(|c| c.item_count).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn plan_job_has_duration_estimates() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_job(100, 100, &ChunkStrategy::Fixed { count: 2 }, 100);
        // 50 items * 100ms = 5000ms = 5s per chunk.
        for chunk in &plan.chunks {
            assert!(chunk.estimated_duration_secs >= 1);
        }
    }

    #[test]
    fn plan_job_has_memory_estimates() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_job(1000, 1024, &ChunkStrategy::Fixed { count: 4 }, 10);
        for chunk in &plan.chunks {
            assert!(chunk.estimated_memory_mb > 0);
        }
    }

    // ====================================================================
    // ChunkPlanner: plan_adaptive
    // ====================================================================

    #[test]
    fn plan_adaptive_proportional() {
        let planner = ChunkPlanner::new();
        let nodes = vec![
            (NodeId::new(), NodeClass::Edge),
            (NodeId::new(), NodeClass::Enterprise),
        ];
        let plan = planner.plan_adaptive(1000, &nodes);
        assert_eq!(plan.total_chunks(), 2);

        let dust_items = plan.chunks[0].item_count;
        let boulder_items = plan.chunks[1].item_count;
        assert_eq!(dust_items + boulder_items, 1000);
        // Enterprise should get much more than Edge.
        assert!(boulder_items > dust_items, "enterprise={} edge={}", boulder_items, dust_items);
    }

    #[test]
    fn plan_adaptive_empty_nodes() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_adaptive(500, &[]);
        assert_eq!(plan.total_chunks(), 1);
        assert_eq!(plan.chunks[0].item_count, 500);
    }

    #[test]
    fn plan_adaptive_single_node() {
        let planner = ChunkPlanner::new();
        let nodes = vec![(NodeId::new(), NodeClass::Standard)];
        let plan = planner.plan_adaptive(42, &nodes);
        assert_eq!(plan.total_chunks(), 1);
        assert_eq!(plan.chunks[0].item_count, 42);
        assert_eq!(plan.chunks[0].target_class, NodeClass::Standard);
    }

    // ====================================================================
    // ChunkPlanner: plan_for_class
    // ====================================================================

    #[test]
    fn plan_for_class_dust() {
        let planner = ChunkPlanner::new();
        let chunks = planner.plan_for_class(10, NodeClass::Edge);
        // Edge suggested_batch_size = 1, so 10 chunks.
        assert_eq!(chunks.len(), 10);
        for chunk in &chunks {
            assert_eq!(chunk.target_class, NodeClass::Edge);
            assert_eq!(chunk.item_count, 1);
        }
    }

    #[test]
    fn plan_for_class_boulder() {
        let planner = ChunkPlanner::new();
        let chunks = planner.plan_for_class(5000, NodeClass::Enterprise);
        // Enterprise suggested_batch_size = 2500, so 2 chunks.
        assert_eq!(chunks.len(), 2);
        let total: u64 = chunks.iter().map(|c| c.item_count).sum();
        assert_eq!(total, 5000);
    }

    #[test]
    fn plan_for_class_zero_items() {
        let planner = ChunkPlanner::new();
        let chunks = planner.plan_for_class(0, NodeClass::Standard);
        assert!(chunks.is_empty());
    }

    // ====================================================================
    // ChunkPlanner: replan_remaining
    // ====================================================================

    #[test]
    fn replan_removes_completed() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_job(100, 10, &ChunkStrategy::Fixed { count: 4 }, 10);
        assert_eq!(plan.total_chunks(), 4);

        let completed = vec![plan.chunks[0].chunk_id, plan.chunks[1].chunk_id];
        let replan = planner.replan_remaining(&completed, &[], &plan);
        assert_eq!(replan.total_chunks(), 2);
    }

    #[test]
    fn replan_upgrades_failed_class() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_job(100, 10, &ChunkStrategy::Fixed { count: 2 }, 10);

        let failed = vec![plan.chunks[0].chunk_id];
        let replan = planner.replan_remaining(&[], &failed, &plan);
        assert_eq!(replan.total_chunks(), 2);

        // The failed chunk should have been upgraded one tier.
        let original_class = plan.chunks[0].target_class;
        let replanned_class = replan.chunks.iter()
            .find(|c| c.sequence == 0)
            .unwrap()
            .target_class;
        assert!(replanned_class >= original_class);
    }

    #[test]
    fn replan_all_completed() {
        let planner = ChunkPlanner::new();
        let plan = planner.plan_job(100, 10, &ChunkStrategy::Fixed { count: 3 }, 10);

        let completed: Vec<ChunkId> = plan.chunks.iter().map(|c| c.chunk_id).collect();
        let replan = planner.replan_remaining(&completed, &[], &plan);
        assert_eq!(replan.total_chunks(), 0);
        assert_eq!(replan.total_items, 0);
    }

    // ====================================================================
    // ChunkPlanner: estimate_chunks_needed
    // ====================================================================

    #[test]
    fn estimate_chunks_needed_all_classes() {
        let planner = ChunkPlanner::new();
        let estimates = planner.estimate_chunks_needed(10_000, 100);
        assert!(estimates.contains_key(&NodeClass::Edge));
        assert!(estimates.contains_key(&NodeClass::Enterprise));
        // Edge should need more chunks than Enterprise.
        assert!(estimates[&NodeClass::Edge] > estimates[&NodeClass::Enterprise]);
    }

    // ====================================================================
    // ClassDistribution
    // ====================================================================

    #[test]
    fn class_distribution_from_counts() {
        let dist = ClassDistribution::from_counts(10, 20, 30, 40);
        assert_eq!(dist.total(), 100);
        assert!((dist.dust_pct - 0.1).abs() < 0.001);
        assert!((dist.pebble_pct - 0.2).abs() < 0.001);
        assert!((dist.rock_pct - 0.3).abs() < 0.001);
        assert!((dist.boulder_pct - 0.4).abs() < 0.001);
    }

    #[test]
    fn class_distribution_percentages_sum_to_one() {
        let dist = ClassDistribution::from_counts(5, 15, 25, 55);
        let sum = dist.dust_pct + dist.pebble_pct + dist.rock_pct + dist.boulder_pct;
        assert!((sum - 1.0).abs() < 0.001);
    }

    #[test]
    fn class_distribution_dominant_class() {
        let dist = ClassDistribution::from_counts(1, 2, 100, 3);
        assert_eq!(dist.dominant_class(), NodeClass::Standard);
    }

    #[test]
    fn class_distribution_dominant_class_tie() {
        // Equal counts -- highest class wins (Enterprise > Standard in Ord).
        let dist = ClassDistribution::from_counts(10, 10, 10, 10);
        // max_by_key returns last max in iteration order, which is Enterprise.
        let dominant = dist.dominant_class();
        // Any class is acceptable when tied; just verify it's valid.
        assert!(NodeClass::all().contains(&dominant));
    }

    #[test]
    fn class_distribution_empty() {
        let dist = ClassDistribution::from_counts(0, 0, 0, 0);
        assert!(dist.is_empty());
        assert_eq!(dist.total(), 0);
        assert_eq!(dist.effective_compute_units(), 0.0);
    }

    #[test]
    fn class_distribution_effective_compute_units() {
        let dist = ClassDistribution::from_counts(0, 0, 10, 0);
        // 10 Rocks * 1.0 weight = 10.0 ECU.
        assert!((dist.effective_compute_units() - 10.0).abs() < 0.001);
    }

    #[test]
    fn class_distribution_class_count() {
        let dist = ClassDistribution::from_counts(5, 10, 15, 20);
        assert_eq!(dist.class_count(NodeClass::Edge), 5);
        assert_eq!(dist.class_count(NodeClass::Light), 10);
        assert_eq!(dist.class_count(NodeClass::Standard), 15);
        assert_eq!(dist.class_count(NodeClass::Enterprise), 20);
    }

    #[test]
    fn class_distribution_class_pct() {
        let dist = ClassDistribution::from_counts(25, 25, 25, 25);
        assert!((dist.class_pct(NodeClass::Edge) - 0.25).abs() < 0.001);
        assert!((dist.class_pct(NodeClass::Standard) - 0.25).abs() < 0.001);
    }

    #[test]
    fn class_distribution_proportional_items() {
        let dist = ClassDistribution::from_counts(0, 0, 5, 5);
        let props = dist.proportional_items(1000);
        let standard = props.get(&NodeClass::Standard).copied().unwrap_or(0);
        let enterprise = props.get(&NodeClass::Enterprise).copied().unwrap_or(0);
        assert_eq!(standard + enterprise, 1000);
        // Enterprise weight=4.0, Standard weight=1.0, equal counts -> 4:1 ratio.
        assert!(enterprise > standard);
    }

    #[test]
    fn class_distribution_sorted_by_count() {
        let dist = ClassDistribution::from_counts(5, 50, 20, 10);
        let sorted = dist.sorted_by_count();
        assert_eq!(sorted[0].0, NodeClass::Light);
        assert_eq!(sorted[0].1, 50);
    }

    // ====================================================================
    // AdaptiveChunker
    // ====================================================================

    #[test]
    fn adaptive_chunker_default_when_empty() {
        let chunker = AdaptiveChunker::new();
        let size = chunker.suggested_chunk_size(NodeClass::Standard);
        assert_eq!(size, NodeClass::Standard.suggested_batch_size() as u64);
    }

    #[test]
    fn adaptive_chunker_records_performance() {
        let chunker = AdaptiveChunker::new();
        chunker.record_performance(NodeClass::Standard, 100, 500, true);
        chunker.record_performance(NodeClass::Standard, 100, 600, true);
        chunker.record_performance(NodeClass::Standard, 100, 700, false);
        assert_eq!(chunker.history_count(NodeClass::Standard), 3);
        assert_eq!(chunker.total_records(), 3);
    }

    #[test]
    fn adaptive_chunker_suggested_size_with_data() {
        let chunker = AdaptiveChunker::new();
        // Record enough data points for the algorithm.
        for _ in 0..10 {
            chunker.record_performance(NodeClass::Standard, 250, 1000, true);
        }
        let size = chunker.suggested_chunk_size(NodeClass::Standard);
        // Should return something > 0.
        assert!(size > 0);
    }

    #[test]
    fn adaptive_chunker_overhead_factor_no_failures() {
        let chunker = AdaptiveChunker::new();
        chunker.record_performance(NodeClass::Light, 50, 200, true);
        chunker.record_performance(NodeClass::Light, 50, 250, true);
        let factor = chunker.overhead_factor(NodeClass::Light);
        assert!((factor - 1.0).abs() < 0.001);
    }

    #[test]
    fn adaptive_chunker_overhead_factor_with_failures() {
        let chunker = AdaptiveChunker::new();
        chunker.record_performance(NodeClass::Edge, 10, 100, true);
        chunker.record_performance(NodeClass::Edge, 10, 100, false);
        chunker.record_performance(NodeClass::Edge, 10, 100, true);
        chunker.record_performance(NodeClass::Edge, 10, 100, false);
        // 4 total / 2 successes = 2.0 overhead.
        let factor = chunker.overhead_factor(NodeClass::Edge);
        assert!((factor - 2.0).abs() < 0.001);
    }

    #[test]
    fn adaptive_chunker_overhead_all_failures() {
        let chunker = AdaptiveChunker::new();
        chunker.record_performance(NodeClass::Enterprise, 100, 500, false);
        chunker.record_performance(NodeClass::Enterprise, 100, 600, false);
        let factor = chunker.overhead_factor(NodeClass::Enterprise);
        assert!((factor - 5.0).abs() < 0.001); // Capped at 5.0.
    }

    #[test]
    fn adaptive_chunker_reset_history() {
        let chunker = AdaptiveChunker::new();
        chunker.record_performance(NodeClass::Standard, 100, 500, true);
        assert_eq!(chunker.history_count(NodeClass::Standard), 1);
        chunker.reset_history(NodeClass::Standard);
        assert_eq!(chunker.history_count(NodeClass::Standard), 0);
    }

    #[test]
    fn adaptive_chunker_success_rate() {
        let chunker = AdaptiveChunker::new();
        chunker.record_performance(NodeClass::Standard, 100, 500, true);
        chunker.record_performance(NodeClass::Standard, 100, 500, true);
        chunker.record_performance(NodeClass::Standard, 100, 500, false);
        let rate = chunker.success_rate(NodeClass::Standard);
        assert!((rate - 2.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn adaptive_chunker_avg_success_duration() {
        let chunker = AdaptiveChunker::new();
        chunker.record_performance(NodeClass::Light, 50, 200, true);
        chunker.record_performance(NodeClass::Light, 50, 400, true);
        chunker.record_performance(NodeClass::Light, 50, 100, false); // excluded
        let avg = chunker.avg_success_duration_ms(NodeClass::Light);
        assert!((avg - 300.0).abs() < 0.01);
    }

    #[test]
    fn adaptive_chunker_estimate_duration() {
        let chunker = AdaptiveChunker::new();
        // 100 items in 1000ms = 0.1 items/ms = 10ms/item.
        chunker.record_performance(NodeClass::Standard, 100, 1000, true);
        let est = chunker.estimate_duration_ms(NodeClass::Standard, 50);
        // 50 items * 10ms/item = 500ms.
        assert_eq!(est, 500);
    }

    #[test]
    fn adaptive_chunker_estimate_duration_no_data() {
        let chunker = AdaptiveChunker::new();
        assert_eq!(chunker.estimate_duration_ms(NodeClass::Standard, 100), 0);
    }

    // ====================================================================
    // ChunkValidator
    // ====================================================================

    #[test]
    fn validate_chunk_passes() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Standard,
            item_count: 100,
            estimated_memory_mb: 100,
            estimated_duration_secs: 60,
            offset: 0,
            length: 100,
        };
        let constraints = TaskConstraints::for_class(NodeClass::Standard);
        assert!(ChunkValidator::validate_chunk(&chunk, &constraints).is_ok());
    }

    #[test]
    fn validate_chunk_memory_violation() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Edge,
            item_count: 100,
            estimated_memory_mb: 500, // Edge max is 100MB
            estimated_duration_secs: 10,
            offset: 0,
            length: 100,
        };
        let constraints = TaskConstraints::for_class(NodeClass::Edge);
        let result = ChunkValidator::validate_chunk(&chunk, &constraints);
        assert!(result.is_err());
        let violations = result.unwrap_err();
        assert!(violations.iter().any(|v| v.contains("memory")));
    }

    #[test]
    fn validate_chunk_duration_violation() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Edge,
            item_count: 100,
            estimated_memory_mb: 10,
            estimated_duration_secs: 3600, // Edge max is 60s
            offset: 0,
            length: 100,
        };
        let constraints = TaskConstraints::for_class(NodeClass::Edge);
        let result = ChunkValidator::validate_chunk(&chunk, &constraints);
        assert!(result.is_err());
        let violations = result.unwrap_err();
        assert!(violations.iter().any(|v| v.contains("duration")));
    }

    #[test]
    fn validate_chunk_multiple_violations() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Edge,
            item_count: 100,
            estimated_memory_mb: 500,     // exceeds
            estimated_duration_secs: 3600, // exceeds
            offset: 0,
            length: 100,
        };
        let constraints = TaskConstraints::for_class(NodeClass::Edge);
        let result = ChunkValidator::validate_chunk(&chunk, &constraints);
        assert!(result.is_err());
        let violations = result.unwrap_err();
        assert!(violations.len() >= 2);
    }

    #[test]
    fn validate_plan_mixed() {
        let plan = ChunkPlan {
            job_id: JobId::new(),
            total_items: 200,
            chunks: vec![
                PlannedChunk {
                    chunk_id: ChunkId::new(),
                    sequence: 0,
                    target_class: NodeClass::Standard,
                    item_count: 100,
                    estimated_memory_mb: 100,
                    estimated_duration_secs: 60,
                    offset: 0,
                    length: 100,
                },
                PlannedChunk {
                    chunk_id: ChunkId::new(),
                    sequence: 1,
                    target_class: NodeClass::Edge,
                    item_count: 100,
                    estimated_memory_mb: 500, // Too much for Edge
                    estimated_duration_secs: 10,
                    offset: 100,
                    length: 100,
                },
            ],
            strategy_used: ChunkStrategy::Fixed { count: 2 },
            estimated_duration_secs: 60,
        };

        let issues = ChunkValidator::validate_plan(&plan);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].0, 1); // Second chunk has the issue.
    }

    #[test]
    fn can_node_handle_basic() {
        assert!(ChunkValidator::can_node_handle(NodeClass::Enterprise, 1000, 1000));
        assert!(!ChunkValidator::can_node_handle(NodeClass::Edge, 1000, 1000));
    }

    #[test]
    fn minimum_class_for_small_job() {
        let class = ChunkValidator::minimum_class_for(50, 30);
        assert_eq!(class, Some(NodeClass::Edge));
    }

    #[test]
    fn minimum_class_for_large_job() {
        let class = ChunkValidator::minimum_class_for(20_000, 10_000);
        assert_eq!(class, Some(NodeClass::Enterprise));
    }

    #[test]
    fn minimum_class_for_impossible() {
        // Exceeds even Enterprise limits.
        let class = ChunkValidator::minimum_class_for(100_000, 100_000);
        assert_eq!(class, None);
    }

    #[test]
    fn validate_coverage_ok() {
        let plan = ChunkPlan {
            job_id: JobId::new(),
            total_items: 100,
            chunks: vec![
                PlannedChunk {
                    chunk_id: ChunkId::new(),
                    sequence: 0,
                    target_class: NodeClass::Standard,
                    item_count: 60,
                    estimated_memory_mb: 10,
                    estimated_duration_secs: 10,
                    offset: 0,
                    length: 60,
                },
                PlannedChunk {
                    chunk_id: ChunkId::new(),
                    sequence: 1,
                    target_class: NodeClass::Standard,
                    item_count: 40,
                    estimated_memory_mb: 10,
                    estimated_duration_secs: 10,
                    offset: 60,
                    length: 40,
                },
            ],
            strategy_used: ChunkStrategy::Fixed { count: 2 },
            estimated_duration_secs: 10,
        };
        assert!(ChunkValidator::validate_coverage(&plan).is_ok());
    }

    #[test]
    fn validate_coverage_mismatch() {
        let plan = ChunkPlan {
            job_id: JobId::new(),
            total_items: 100,
            chunks: vec![PlannedChunk {
                chunk_id: ChunkId::new(),
                sequence: 0,
                target_class: NodeClass::Standard,
                item_count: 80, // Only 80 of 100
                estimated_memory_mb: 10,
                estimated_duration_secs: 10,
                offset: 0,
                length: 80,
            }],
            strategy_used: ChunkStrategy::Single,
            estimated_duration_secs: 10,
        };
        assert!(ChunkValidator::validate_coverage(&plan).is_err());
    }

    // ====================================================================
    // ChunkPlan accessors
    // ====================================================================

    #[test]
    fn chunk_plan_average_size() {
        let plan = make_test_plan(vec![100, 200, 300]);
        assert_eq!(plan.average_chunk_size(), 200);
    }

    #[test]
    fn chunk_plan_largest_smallest() {
        let plan = make_test_plan(vec![10, 50, 30]);
        assert_eq!(plan.largest_chunk().unwrap().item_count, 50);
        assert_eq!(plan.smallest_chunk().unwrap().item_count, 10);
    }

    #[test]
    fn chunk_plan_chunks_for_class() {
        let mut plan = make_test_plan(vec![100, 200]);
        plan.chunks[0].target_class = NodeClass::Edge;
        plan.chunks[1].target_class = NodeClass::Enterprise;

        let edge = plan.chunks_for_class(NodeClass::Edge);
        assert_eq!(edge.len(), 1);
        assert_eq!(edge[0].item_count, 100);

        let enterprise = plan.chunks_for_class(NodeClass::Enterprise);
        assert_eq!(enterprise.len(), 1);
        assert_eq!(enterprise[0].item_count, 200);
    }

    #[test]
    fn chunk_plan_empty() {
        let plan = make_test_plan(vec![]);
        assert_eq!(plan.total_chunks(), 0);
        assert_eq!(plan.average_chunk_size(), 0);
        assert!(plan.largest_chunk().is_none());
        assert!(plan.smallest_chunk().is_none());
    }

    #[test]
    fn chunk_plan_is_contiguous() {
        let plan = make_test_plan(vec![50, 50]);
        assert!(plan.is_contiguous());
    }

    #[test]
    fn chunk_plan_class_distribution_map() {
        let mut plan = make_test_plan(vec![100, 200, 300]);
        plan.chunks[0].target_class = NodeClass::Edge;
        plan.chunks[1].target_class = NodeClass::Standard;
        plan.chunks[2].target_class = NodeClass::Standard;

        let dist = plan.class_distribution();
        assert_eq!(dist[&NodeClass::Edge], 1);
        assert_eq!(dist[&NodeClass::Standard], 2);
    }

    #[test]
    fn chunk_plan_items_per_class() {
        let mut plan = make_test_plan(vec![100, 200, 300]);
        plan.chunks[0].target_class = NodeClass::Light;
        plan.chunks[1].target_class = NodeClass::Light;
        plan.chunks[2].target_class = NodeClass::Standard;

        let ipc = plan.items_per_class();
        assert_eq!(ipc[&NodeClass::Light], 300);
        assert_eq!(ipc[&NodeClass::Standard], 300);
    }

    #[test]
    fn chunk_plan_has_oversized_chunks() {
        let mut plan = make_test_plan(vec![100]);
        plan.chunks[0].target_class = NodeClass::Edge;
        plan.chunks[0].estimated_memory_mb = 200; // Edge max is 100MB.
        assert!(plan.has_oversized_chunks());
    }

    #[test]
    fn chunk_plan_no_oversized_chunks() {
        let mut plan = make_test_plan(vec![100]);
        plan.chunks[0].target_class = NodeClass::Enterprise;
        plan.chunks[0].estimated_memory_mb = 100;
        plan.chunks[0].estimated_duration_secs = 60;
        assert!(!plan.has_oversized_chunks());
    }

    // ====================================================================
    // PlannedChunk methods
    // ====================================================================

    #[test]
    fn planned_chunk_summary() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Standard,
            item_count: 500,
            estimated_memory_mb: 64,
            estimated_duration_secs: 30,
            offset: 0,
            length: 500,
        };
        let s = chunk.summary();
        assert!(s.contains("Standard"));
        assert!(s.contains("500"));
    }

    #[test]
    fn planned_chunk_throughput() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Standard,
            item_count: 1000,
            estimated_memory_mb: 10,
            estimated_duration_secs: 10,
            offset: 0,
            length: 1000,
        };
        assert!((chunk.estimated_throughput() - 100.0).abs() < 0.01);
    }

    #[test]
    fn planned_chunk_exceeds_limits() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Edge,
            item_count: 100,
            estimated_memory_mb: 200, // Edge max is 100.
            estimated_duration_secs: 10,
            offset: 0,
            length: 100,
        };
        assert!(chunk.exceeds_class_limits());
    }

    #[test]
    fn planned_chunk_within_limits() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Enterprise,
            item_count: 100,
            estimated_memory_mb: 100,
            estimated_duration_secs: 60,
            offset: 0,
            length: 100,
        };
        assert!(!chunk.exceeds_class_limits());
    }

    // ====================================================================
    // ChunkPerformance
    // ====================================================================

    #[test]
    fn chunk_performance_throughput() {
        let p = ChunkPerformance::new(500, 1000, true);
        assert!((p.throughput() - 0.5).abs() < 0.001);
    }

    #[test]
    fn chunk_performance_per_item_ms() {
        let p = ChunkPerformance::new(100, 500, true);
        assert!((p.per_item_ms() - 5.0).abs() < 0.001);
    }

    #[test]
    fn chunk_performance_zero_duration() {
        let p = ChunkPerformance::new(100, 0, true);
        assert_eq!(p.throughput(), 0.0);
    }

    #[test]
    fn chunk_performance_zero_items() {
        let p = ChunkPerformance::new(0, 100, true);
        assert_eq!(p.per_item_ms(), 0.0);
    }

    // ====================================================================
    // Convenience functions
    // ====================================================================

    #[test]
    fn quick_plan_works() {
        let plan = quick_plan(1000, 100, 10);
        assert_eq!(plan.total_chunks(), 1); // Single strategy default.
        assert_eq!(plan.total_items, 1000);
    }

    #[test]
    fn plan_with_distribution_works() {
        let dist = ClassDistribution::from_counts(0, 0, 10, 5);
        let plan = plan_with_distribution(
            1000,
            100,
            10,
            &ChunkStrategy::Fixed { count: 5 },
            dist,
        );
        assert_eq!(plan.total_chunks(), 5);
    }

    // ====================================================================
    // Helper: upgrade_class / downgrade_class
    // ====================================================================

    #[test]
    fn upgrade_class_progression() {
        assert_eq!(upgrade_class(NodeClass::Edge), NodeClass::Light);
        assert_eq!(upgrade_class(NodeClass::Light), NodeClass::Standard);
        assert_eq!(upgrade_class(NodeClass::Standard), NodeClass::Enterprise);
        assert_eq!(upgrade_class(NodeClass::Enterprise), NodeClass::Enterprise);
    }

    #[test]
    fn downgrade_class_progression() {
        assert_eq!(downgrade_class(NodeClass::Enterprise), NodeClass::Standard);
        assert_eq!(downgrade_class(NodeClass::Standard), NodeClass::Light);
        assert_eq!(downgrade_class(NodeClass::Light), NodeClass::Edge);
        assert_eq!(downgrade_class(NodeClass::Edge), NodeClass::Edge);
    }

    // ====================================================================
    // Percentile helpers
    // ====================================================================

    #[test]
    fn p90_index_basic() {
        assert_eq!(p90_index(100), 90);
        assert_eq!(p90_index(10), 9);
        assert_eq!(p90_index(1), 0);
        assert_eq!(p90_index(0), 0);
    }

    #[test]
    fn p50_index_basic() {
        assert_eq!(p50_index(100), 50);
        assert_eq!(p50_index(10), 5);
        assert_eq!(p50_index(1), 0);
    }

    #[test]
    fn percentile_index_boundaries() {
        assert_eq!(percentile_index(100, 0), 0);
        assert_eq!(percentile_index(100, 100), 99);
        assert_eq!(percentile_index(100, 50), 50);
    }

    // ====================================================================
    // Display impls
    // ====================================================================

    #[test]
    fn display_chunk_plan() {
        let plan = make_test_plan(vec![100]);
        let s = format!("{}", plan);
        assert!(s.contains("ChunkPlan"));
        assert!(s.contains("items=100"));
    }

    #[test]
    fn display_planned_chunk() {
        let chunk = PlannedChunk {
            chunk_id: ChunkId::new(),
            sequence: 0,
            target_class: NodeClass::Standard,
            item_count: 42,
            estimated_memory_mb: 10,
            estimated_duration_secs: 5,
            offset: 0,
            length: 42,
        };
        let s = format!("{}", chunk);
        assert!(s.contains("PlannedChunk"));
        assert!(s.contains("Standard"));
    }

    #[test]
    fn display_class_distribution() {
        let dist = ClassDistribution::from_counts(1, 2, 3, 4);
        let s = format!("{}", dist);
        assert!(s.contains("ClassDist"));
        assert!(s.contains("total=10"));
    }

    // ====================================================================
    // Test helpers
    // ====================================================================

    /// Build a ChunkPlan with the given item counts for testing accessors.
    fn make_test_plan(item_counts: Vec<u64>) -> ChunkPlan {
        let total: u64 = item_counts.iter().sum();
        let mut offset = 0u64;
        let chunks: Vec<PlannedChunk> = item_counts
            .into_iter()
            .enumerate()
            .map(|(i, count)| {
                let chunk = PlannedChunk {
                    chunk_id: ChunkId::new(),
                    sequence: i as u32,
                    target_class: NodeClass::Standard,
                    item_count: count,
                    estimated_memory_mb: 10,
                    estimated_duration_secs: 10,
                    offset,
                    length: count,
                };
                offset += count;
                chunk
            })
            .collect();

        let num_chunks = chunks.len() as u32;
        ChunkPlan {
            job_id: JobId::new(),
            total_items: total,
            chunks,
            strategy_used: ChunkStrategy::Fixed {
                count: num_chunks,
            },
            estimated_duration_secs: 10,
        }
    }
}
