// Marabunta - Licensed under the MIT License.
//! Node class management, classification engine, constraint enforcement, and analytics.
//!
//! The [`NodeClass`] enum itself lives in `types.rs`. This module provides the
//! operational layer: automatic classification from resource snapshots, manual
//! overrides, constraint validation before task assignment, distribution
//! analytics, historical trending, and class-based job recommendations.
//!
//! # Architecture
//!
//! ```text
//!  +-----------------+      +---------------------+
//!  | KnowledgeStore  |<-----| NodeClassifier      |
//!  | (live node data)|      | (classify + cache)  |
//!  +-----------------+      +-----+---------------+
//!                                 |
//!              +------------------+------------------+
//!              |                  |                   |
//!   ClassDistribution   ConstraintEnforcer   ClassAnalytics
//!   (snapshot counts)   (validate tasks)     (throughput etc)
//!              |
//!   ClassificationHistory
//!   (trend over time)
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::knowledge::KnowledgeStore;
use super::types::{NodeClass, NodeId, NodeStatus, ResourceSnapshot, TaskConstraints};

// ============================================================================
// ClassTrend
// ============================================================================

/// Directional trend for a node class over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClassTrend {
    /// The proportion of this class is increasing.
    Rising,
    /// The proportion of this class is decreasing.
    Falling,
    /// The proportion is roughly stable.
    Stable,
}

impl std::fmt::Display for ClassTrend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClassTrend::Rising => write!(f, "Rising"),
            ClassTrend::Falling => write!(f, "Falling"),
            ClassTrend::Stable => write!(f, "Stable"),
        }
    }
}

// ============================================================================
// ConstraintViolation
// ============================================================================

/// Describes a single task constraint that a node class cannot satisfy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintViolation {
    /// Which constraint was violated (e.g. "memory", "duration", "input_size", "output_size").
    pub constraint_type: String,
    /// The class-imposed upper limit.
    pub limit: u64,
    /// The actual (requested) value.
    pub actual: u64,
    /// The node class that was checked.
    pub node_class: NodeClass,
    /// Human-readable explanation.
    pub message: String,
}

impl std::fmt::Display for ConstraintViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

// ============================================================================
// ClassDistribution
// ============================================================================

/// Point-in-time snapshot of the swarm's node class composition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassDistribution {
    pub edge: u32,
    pub light: u32,
    pub standard: u32,
    pub enterprise: u32,
    pub total: u32,
    pub dust_pct: f64,
    pub pebble_pct: f64,
    pub rock_pct: f64,
    pub boulder_pct: f64,
    pub computed_at: DateTime<Utc>,
}

impl ClassDistribution {
    /// Build a distribution from raw counts.
    pub fn from_counts(edge: u32, light: u32, standard: u32, enterprise: u32) -> Self {
        let total = edge + light + standard + enterprise;
        let denom = if total == 0 { 1.0 } else { total as f64 };
        Self {
            edge,
            light,
            standard,
            enterprise,
            total,
            dust_pct: edge as f64 / denom * 100.0,
            pebble_pct: light as f64 / denom * 100.0,
            rock_pct: standard as f64 / denom * 100.0,
            boulder_pct: enterprise as f64 / denom * 100.0,
            computed_at: Utc::now(),
        }
    }

    /// Build a distribution by scanning every live node in the knowledge store.
    pub fn from_knowledge(knowledge: &KnowledgeStore) -> Self {
        let mut edge: u32 = 0;
        let mut light: u32 = 0;
        let mut standard: u32 = 0;
        let mut enterprise: u32 = 0;

        for node in knowledge.get_all_nodes() {
            if node.status == NodeStatus::Dead {
                continue;
            }
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

    /// Return the count for a specific class.
    pub fn count_for(&self, class: NodeClass) -> u32 {
        match class {
            NodeClass::Edge => self.edge,
            NodeClass::Light => self.light,
            NodeClass::Standard => self.standard,
            NodeClass::Enterprise => self.enterprise,
        }
    }

    /// Return the percentage for a specific class.
    pub fn pct_for(&self, class: NodeClass) -> f64 {
        match class {
            NodeClass::Edge => self.dust_pct,
            NodeClass::Light => self.pebble_pct,
            NodeClass::Standard => self.rock_pct,
            NodeClass::Enterprise => self.boulder_pct,
        }
    }

    /// Weighted compute units across all classes.
    ///
    /// Each class contributes `count * compute_weight()`. This gives a single
    /// scalar measure of total swarm compute capacity.
    pub fn effective_compute_units(&self) -> f64 {
        self.edge as f64 * NodeClass::Edge.compute_weight()
            + self.light as f64 * NodeClass::Light.compute_weight()
            + self.standard as f64 * NodeClass::Standard.compute_weight()
            + self.enterprise as f64 * NodeClass::Enterprise.compute_weight()
    }

    /// The class with the highest count. Ties broken by higher class.
    pub fn dominant_class(&self) -> NodeClass {
        let counts = [
            (self.edge, NodeClass::Edge),
            (self.light, NodeClass::Light),
            (self.standard, NodeClass::Standard),
            (self.enterprise, NodeClass::Enterprise),
        ];
        counts
            .iter()
            .max_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)))
            .map(|&(_, c)| c)
            .unwrap_or(NodeClass::Edge)
    }

    /// The class with the lowest count. Ties broken by lower class.
    pub fn weakest_class(&self) -> NodeClass {
        let counts = [
            (self.edge, NodeClass::Edge),
            (self.light, NodeClass::Light),
            (self.standard, NodeClass::Standard),
            (self.enterprise, NodeClass::Enterprise),
        ];
        counts
            .iter()
            .min_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)))
            .map(|&(_, c)| c)
            .unwrap_or(NodeClass::Edge)
    }

    /// Returns true when no single class exceeds 60% of the total.
    pub fn is_balanced(&self) -> bool {
        if self.total == 0 {
            return true;
        }
        let threshold = 60.0;
        self.dust_pct <= threshold
            && self.pebble_pct <= threshold
            && self.rock_pct <= threshold
            && self.boulder_pct <= threshold
    }

    /// Render a horizontal ASCII bar chart.
    ///
    /// `width` is the number of characters available for the bar portion
    /// (not counting the label column).
    pub fn to_bar_chart(&self, width: usize) -> String {
        let max_count = *[self.edge, self.light, self.standard, self.enterprise]
            .iter()
            .max()
            .unwrap_or(&1)
            .max(&1);
        let scale = |count: u32| -> usize {
            if max_count == 0 {
                return 0;
            }
            ((count as f64 / max_count as f64) * width as f64).round() as usize
        };

        let entries = [
            ("Edge   ", self.edge),
            ("Light ", self.light),
            ("Standard   ", self.standard),
            ("Enterprise", self.enterprise),
        ];

        let mut lines = Vec::with_capacity(4);
        for (label, count) in &entries {
            let bar_len = scale(*count);
            let bar: String = "#".repeat(bar_len);
            let padding: String = " ".repeat(width.saturating_sub(bar_len));
            lines.push(format!("{} |{}{}| {:>5} ({:5.1}%)", label, bar, padding, count, {
                if self.total == 0 {
                    0.0
                } else {
                    *count as f64 / self.total as f64 * 100.0
                }
            }));
        }
        lines.join("\n")
    }

    /// Returns a HashMap of class to count.
    pub fn as_count_map(&self) -> HashMap<NodeClass, u32> {
        let mut m = HashMap::new();
        m.insert(NodeClass::Edge, self.edge);
        m.insert(NodeClass::Light, self.light);
        m.insert(NodeClass::Standard, self.standard);
        m.insert(NodeClass::Enterprise, self.enterprise);
        m
    }

    /// True if the distribution has zero nodes.
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Ratio of heavy nodes (Standard + Enterprise) to total.
    pub fn heavy_ratio(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.standard + self.enterprise) as f64 / self.total as f64
    }

    /// Ratio of light nodes (Edge + Light) to total.
    pub fn light_ratio(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.edge + self.light) as f64 / self.total as f64
    }

    /// Merge two distributions (for combining sub-swarm counts).
    pub fn merge(&self, other: &ClassDistribution) -> ClassDistribution {
        ClassDistribution::from_counts(
            self.edge + other.edge,
            self.light + other.light,
            self.standard + other.standard,
            self.enterprise + other.enterprise,
        )
    }

    /// Subtract another distribution (clamped at zero per class).
    pub fn subtract(&self, other: &ClassDistribution) -> ClassDistribution {
        ClassDistribution::from_counts(
            self.edge.saturating_sub(other.edge),
            self.light.saturating_sub(other.light),
            self.standard.saturating_sub(other.standard),
            self.enterprise.saturating_sub(other.enterprise),
        )
    }
}

impl Default for ClassDistribution {
    fn default() -> Self {
        Self::from_counts(0, 0, 0, 0)
    }
}

// ============================================================================
// ClassificationHistory
// ============================================================================

/// Tracks class distribution snapshots over time for trend analysis.
#[derive(Debug)]
pub struct ClassificationHistory {
    snapshots: Vec<(DateTime<Utc>, ClassDistribution)>,
    max_snapshots: usize,
}

impl ClassificationHistory {
    /// Create a new history buffer with the given capacity.
    pub fn new(max_snapshots: usize) -> Self {
        Self {
            snapshots: Vec::new(),
            max_snapshots: max_snapshots.max(2),
        }
    }

    /// Record a new distribution snapshot. Old entries are evicted FIFO when
    /// the buffer is full.
    pub fn record(&mut self, distribution: ClassDistribution) {
        let ts = distribution.computed_at;
        self.snapshots.push((ts, distribution));
        if self.snapshots.len() > self.max_snapshots {
            self.snapshots.remove(0);
        }
    }

    /// Return the most recently recorded distribution.
    pub fn latest(&self) -> Option<&ClassDistribution> {
        self.snapshots.last().map(|(_, d)| d)
    }

    /// How many snapshots are stored.
    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    /// Whether the history is empty.
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    /// Determine the trend for a given class by comparing the first and second
    /// halves of the history window.
    ///
    /// Returns `Stable` if there are fewer than 2 snapshots.
    pub fn trend(&self, class: NodeClass) -> ClassTrend {
        if self.snapshots.len() < 2 {
            return ClassTrend::Stable;
        }
        let mid = self.snapshots.len() / 2;
        let first_half = &self.snapshots[..mid];
        let second_half = &self.snapshots[mid..];

        let avg = |slice: &[(DateTime<Utc>, ClassDistribution)]| -> f64 {
            if slice.is_empty() {
                return 0.0;
            }
            let sum: f64 = slice.iter().map(|(_, d)| d.pct_for(class)).sum();
            sum / slice.len() as f64
        };

        let first_avg = avg(first_half);
        let second_avg = avg(second_half);
        let delta = second_avg - first_avg;

        // Use a 2% threshold to avoid noise-driven trend flips.
        if delta > 2.0 {
            ClassTrend::Rising
        } else if delta < -2.0 {
            ClassTrend::Falling
        } else {
            ClassTrend::Stable
        }
    }

    /// Average distribution over the last `n` snapshots. Returns `None` if
    /// there are no snapshots.
    pub fn avg_over_last_n(&self, n: usize) -> Option<ClassDistribution> {
        if self.snapshots.is_empty() {
            return None;
        }
        let start = self.snapshots.len().saturating_sub(n);
        let window = &self.snapshots[start..];
        let count = window.len() as f64;

        let dust_avg = window.iter().map(|(_, d)| d.edge as f64).sum::<f64>() / count;
        let pebble_avg = window.iter().map(|(_, d)| d.light as f64).sum::<f64>() / count;
        let rock_avg = window.iter().map(|(_, d)| d.standard as f64).sum::<f64>() / count;
        let boulder_avg = window.iter().map(|(_, d)| d.enterprise as f64).sum::<f64>() / count;

        Some(ClassDistribution::from_counts(
            dust_avg.round() as u32,
            pebble_avg.round() as u32,
            rock_avg.round() as u32,
            boulder_avg.round() as u32,
        ))
    }

    /// Return all snapshots as a slice.
    pub fn snapshots(&self) -> &[(DateTime<Utc>, ClassDistribution)] {
        &self.snapshots
    }

    /// Clear all stored snapshots.
    pub fn clear(&mut self) {
        self.snapshots.clear();
    }

    /// Return the oldest snapshot timestamp, if any.
    pub fn oldest_timestamp(&self) -> Option<DateTime<Utc>> {
        self.snapshots.first().map(|(ts, _)| *ts)
    }

    /// Return the newest snapshot timestamp, if any.
    pub fn newest_timestamp(&self) -> Option<DateTime<Utc>> {
        self.snapshots.last().map(|(ts, _)| *ts)
    }
}

impl Default for ClassificationHistory {
    fn default() -> Self {
        Self::new(256)
    }
}

// ============================================================================
// NodeClassifier
// ============================================================================

/// Classifies nodes into hardware classes and maintains the class distribution.
///
/// The classifier uses resource snapshots from the knowledge store as the
/// primary data source. Manual overrides take precedence over computed
/// classifications. A history of distributions is kept for trend analysis.
pub struct NodeClassifier {
    knowledge: Option<Arc<KnowledgeStore>>,
    overrides: DashMap<NodeId, NodeClass>,
    history: parking_lot::RwLock<ClassificationHistory>,
}

impl NodeClassifier {
    /// Create a new classifier without a knowledge store.
    pub fn new() -> Self {
        Self {
            knowledge: None,
            overrides: DashMap::new(),
            history: parking_lot::RwLock::new(ClassificationHistory::default()),
        }
    }

    /// Attach a knowledge store for live classification. Returns self for chaining.
    pub fn with_knowledge(mut self, knowledge: Arc<KnowledgeStore>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// Set the knowledge store after construction.
    pub fn set_knowledge(&mut self, knowledge: Arc<KnowledgeStore>) {
        self.knowledge = Some(knowledge);
    }

    /// Classify a node by looking up its resources in the knowledge store.
    ///
    /// Falls back to `NodeClass::Edge` if the node is not found.
    pub fn classify_node(&self, node_id: &NodeId) -> NodeClass {
        if let Some(class) = self.overrides.get(node_id) {
            return *class;
        }
        match &self.knowledge {
            Some(ks) => match ks.get_node(node_id) {
                Some(info) => Self::classify_from_snapshot(&info.capacity),
                None => {
                    debug!(node = %node_id, "classify_node: node not found, defaulting to Edge");
                    NodeClass::Edge
                }
            },
            None => {
                debug!("classify_node: no knowledge store, defaulting to Edge");
                NodeClass::Edge
            }
        }
    }

    /// Classify from an explicit resource snapshot.
    pub fn classify_from_snapshot(snapshot: &ResourceSnapshot) -> NodeClass {
        NodeClass::from_resources(snapshot.cpu_cores, snapshot.memory_total_mb)
    }

    /// Set a manual class override for a node. This takes priority over
    /// computed classification.
    pub fn override_class(&self, node_id: NodeId, class: NodeClass) {
        info!(node = %node_id, class = %class, "manual class override set");
        self.overrides.insert(node_id, class);
    }

    /// Remove a manual class override, reverting to computed classification.
    pub fn remove_override(&self, node_id: &NodeId) {
        if self.overrides.remove(node_id).is_some() {
            info!(node = %node_id, "manual class override removed");
        }
    }

    /// Check if a node has a manual override.
    pub fn has_override(&self, node_id: &NodeId) -> bool {
        self.overrides.contains_key(node_id)
    }

    /// Return the effective class for a node: override if set, otherwise computed.
    pub fn get_class(&self, node_id: &NodeId) -> NodeClass {
        if let Some(class) = self.overrides.get(node_id) {
            return *class;
        }
        self.classify_node(node_id)
    }

    /// Get the current class distribution (cached from last call to `get_distribution_live`,
    /// or computed fresh if history is empty).
    pub fn get_distribution(&self) -> ClassDistribution {
        let history = self.history.read();
        match history.latest() {
            Some(d) => d.clone(),
            None => {
                drop(history);
                self.get_distribution_live()
            }
        }
    }

    /// Recompute the class distribution from the live knowledge store.
    ///
    /// Also records the snapshot in the classification history.
    pub fn get_distribution_live(&self) -> ClassDistribution {
        let dist = match &self.knowledge {
            Some(ks) => {
                let mut edge: u32 = 0;
                let mut light: u32 = 0;
                let mut standard: u32 = 0;
                let mut enterprise: u32 = 0;

                for node in ks.get_all_nodes() {
                    if node.status == NodeStatus::Dead {
                        continue;
                    }
                    let class = match self.overrides.get(&node.node_id) {
                        Some(c) => *c,
                        None => NodeClass::from_resources(
                            node.capacity.cpu_cores,
                            node.capacity.memory_total_mb,
                        ),
                    };
                    match class {
                        NodeClass::Edge => edge += 1,
                        NodeClass::Light => light += 1,
                        NodeClass::Standard => standard += 1,
                        NodeClass::Enterprise => enterprise += 1,
                    }
                }
                ClassDistribution::from_counts(edge, light, standard, enterprise)
            }
            None => ClassDistribution::from_counts(0, 0, 0, 0),
        };

        let mut history = self.history.write();
        history.record(dist.clone());
        debug!(
            total = dist.total,
            edge = dist.edge,
            light = dist.light,
            standard = dist.standard,
            enterprise = dist.enterprise,
            "class distribution recomputed"
        );
        dist
    }

    /// Static helper: classify from raw core count and memory.
    pub fn class_for_cores_and_memory(cores: u32, memory_mb: u64) -> NodeClass {
        NodeClass::from_resources(cores, memory_mb)
    }

    /// Return a count of nodes per class (live computation, no overrides).
    pub fn node_count_by_class(&self) -> HashMap<NodeClass, usize> {
        let mut counts: HashMap<NodeClass, usize> = HashMap::new();
        for class in NodeClass::all() {
            counts.insert(*class, 0);
        }

        if let Some(ks) = &self.knowledge {
            for node in ks.get_all_nodes() {
                if node.status == NodeStatus::Dead {
                    continue;
                }
                let class = self.get_class(&node.node_id);
                *counts.entry(class).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Weighted sum of all non-dead nodes, where each node contributes its
    /// class's compute_weight().
    pub fn effective_compute_units(&self) -> f64 {
        let dist = self.get_distribution();
        dist.effective_compute_units()
    }

    /// Return a read-locked reference to the classification history.
    pub fn history(&self) -> parking_lot::RwLockReadGuard<'_, ClassificationHistory> {
        self.history.read()
    }

    /// Clear all overrides.
    pub fn clear_overrides(&self) {
        self.overrides.clear();
    }

    /// Number of active overrides.
    pub fn override_count(&self) -> usize {
        self.overrides.len()
    }

    /// List all overridden node IDs and their forced classes.
    pub fn list_overrides(&self) -> Vec<(NodeId, NodeClass)> {
        self.overrides
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }
}

impl Default for NodeClassifier {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ConstraintEnforcer
// ============================================================================

/// Validates task resource constraints against node class limits.
///
/// Each node class defines hard limits for memory, duration, input size, and
/// output size. The enforcer checks whether a specific workload fits within
/// those limits and reports detailed violations when it does not.
pub struct ConstraintEnforcer;

impl ConstraintEnforcer {
    /// Check whether the class can satisfy the memory requirement.
    pub fn check_memory(class: NodeClass, required_mb: u64) -> Result<(), ConstraintViolation> {
        let limit = class.max_memory_mb();
        if required_mb > limit {
            return Err(ConstraintViolation {
                constraint_type: "memory".into(),
                limit,
                actual: required_mb,
                node_class: class,
                message: format!(
                    "{} class allows {}MB memory, but task requires {}MB",
                    class.label(),
                    limit,
                    required_mb
                ),
            });
        }
        Ok(())
    }

    /// Check whether the class can satisfy the duration requirement.
    pub fn check_duration(
        class: NodeClass,
        required_secs: u64,
    ) -> Result<(), ConstraintViolation> {
        let limit = class.max_duration_secs();
        if required_secs > limit {
            return Err(ConstraintViolation {
                constraint_type: "duration".into(),
                limit,
                actual: required_secs,
                node_class: class,
                message: format!(
                    "{} class allows {}s max duration, but task requires {}s",
                    class.label(),
                    limit,
                    required_secs
                ),
            });
        }
        Ok(())
    }

    /// Check whether the class can satisfy the input size requirement.
    pub fn check_input_size(
        class: NodeClass,
        size_mb: u64,
    ) -> Result<(), ConstraintViolation> {
        let limit = class.max_input_size_mb();
        if size_mb > limit {
            return Err(ConstraintViolation {
                constraint_type: "input_size".into(),
                limit,
                actual: size_mb,
                node_class: class,
                message: format!(
                    "{} class allows {}MB input, but task has {}MB input",
                    class.label(),
                    limit,
                    size_mb
                ),
            });
        }
        Ok(())
    }

    /// Check whether the class can satisfy the output size requirement.
    pub fn check_output_size(
        class: NodeClass,
        size_mb: u64,
    ) -> Result<(), ConstraintViolation> {
        let limit = class.max_output_size_mb();
        if size_mb > limit {
            return Err(ConstraintViolation {
                constraint_type: "output_size".into(),
                limit,
                actual: size_mb,
                node_class: class,
                message: format!(
                    "{} class allows {}MB output, but task produces {}MB output",
                    class.label(),
                    limit,
                    size_mb
                ),
            });
        }
        Ok(())
    }

    /// Run all constraint checks and return every violation found.
    ///
    /// An empty vector means the class satisfies all constraints.
    pub fn check_all(
        class: NodeClass,
        memory_mb: u64,
        duration_secs: u64,
        input_mb: u64,
        output_mb: u64,
    ) -> Vec<ConstraintViolation> {
        let mut violations = Vec::new();
        if let Err(v) = Self::check_memory(class, memory_mb) {
            violations.push(v);
        }
        if let Err(v) = Self::check_duration(class, duration_secs) {
            violations.push(v);
        }
        if let Err(v) = Self::check_input_size(class, input_mb) {
            violations.push(v);
        }
        if let Err(v) = Self::check_output_size(class, output_mb) {
            violations.push(v);
        }
        violations
    }

    /// Returns true if `class` is at least as capable as `required_class`
    /// in the class hierarchy (Edge < Light < Standard < Enterprise).
    pub fn can_handle(class: NodeClass, required_class: NodeClass) -> bool {
        class >= required_class
    }

    /// Determine the minimum node class that can satisfy the given memory
    /// and duration requirements.
    pub fn minimum_class_for(memory_mb: u64, duration_secs: u64) -> NodeClass {
        for &class in NodeClass::all() {
            if class.max_memory_mb() >= memory_mb && class.max_duration_secs() >= duration_secs {
                return class;
            }
        }
        // If nothing fits, return the largest class anyway.
        NodeClass::Enterprise
    }

    /// Determine the minimum node class that can satisfy all four constraints.
    pub fn minimum_class_for_all(
        memory_mb: u64,
        duration_secs: u64,
        input_mb: u64,
        output_mb: u64,
    ) -> NodeClass {
        for &class in NodeClass::all() {
            let violations = Self::check_all(class, memory_mb, duration_secs, input_mb, output_mb);
            if violations.is_empty() {
                return class;
            }
        }
        NodeClass::Enterprise
    }

    /// Build a `TaskConstraints` from a class.
    pub fn constraints_for_class(class: NodeClass) -> TaskConstraints {
        TaskConstraints::for_class(class)
    }

    /// Check a task against its target class constraints, returning a
    /// human-readable summary.
    pub fn summary(
        class: NodeClass,
        memory_mb: u64,
        duration_secs: u64,
        input_mb: u64,
        output_mb: u64,
    ) -> String {
        let violations = Self::check_all(class, memory_mb, duration_secs, input_mb, output_mb);
        if violations.is_empty() {
            format!("{} class can handle this task (all constraints satisfied)", class.label())
        } else {
            let details: Vec<String> = violations.iter().map(|v| v.message.clone()).collect();
            format!(
                "{} class CANNOT handle this task: {}",
                class.label(),
                details.join("; ")
            )
        }
    }
}

// ============================================================================
// ClassAnalytics
// ============================================================================

/// Analytical functions over node class distributions.
///
/// These are stateless utility methods that operate on distributions and
/// parameters to produce estimates and recommendations.
pub struct ClassAnalytics;

impl ClassAnalytics {
    /// Compute the effective compute power contributed by each class.
    ///
    /// Returns a map of class to weighted compute units.
    pub fn compute_power_by_class(
        distribution: &ClassDistribution,
    ) -> HashMap<NodeClass, f64> {
        let mut power = HashMap::new();
        for &class in NodeClass::all() {
            let count = distribution.count_for(class) as f64;
            power.insert(class, count * class.compute_weight());
        }
        power
    }

    /// Estimate the overall throughput of the swarm in chunks per second.
    ///
    /// `chunk_duration_ms` is the expected average execution time of a single
    /// chunk. We assume each node can run one chunk at a time (simplified).
    pub fn estimated_throughput(
        distribution: &ClassDistribution,
        chunk_duration_ms: u64,
    ) -> f64 {
        if chunk_duration_ms == 0 {
            return 0.0;
        }
        let ecu = distribution.effective_compute_units();
        // Each compute unit can process one chunk per `chunk_duration_ms`.
        ecu / (chunk_duration_ms as f64 / 1000.0)
    }

    /// Distribute `total_items` across classes proportional to their compute
    /// weight, rounding so the sum equals `total_items`.
    pub fn optimal_chunk_allocation(
        total_items: u64,
        distribution: &ClassDistribution,
    ) -> HashMap<NodeClass, u64> {
        let ecu = distribution.effective_compute_units();
        let mut allocation = HashMap::new();

        if ecu <= 0.0 || total_items == 0 {
            for &class in NodeClass::all() {
                allocation.insert(class, 0);
            }
            return allocation;
        }

        let classes = NodeClass::all();

        // Allocate proportionally, tracking rounding error.
        let mut raw_fractions: Vec<(NodeClass, f64)> = Vec::new();
        for &class in classes {
            let class_ecu = distribution.count_for(class) as f64 * class.compute_weight();
            let fraction = class_ecu / ecu;
            let raw = fraction * total_items as f64;
            raw_fractions.push((class, raw));
        }

        // Floor each allocation.
        let mut floored: Vec<(NodeClass, u64, f64)> = raw_fractions
            .iter()
            .map(|&(class, raw)| {
                let floor = raw.floor() as u64;
                let remainder = raw - floor as f64;
                (class, floor, remainder)
            })
            .collect();

        let assigned: u64 = floored.iter().map(|(_, f, _)| f).sum();

        // Distribute leftover items to classes with the largest remainder.
        let leftover = total_items.saturating_sub(assigned);
        floored.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        for (i, (class, count, _)) in floored.iter_mut().enumerate() {
            let bonus = if (i as u64) < leftover { 1 } else { 0 };
            allocation.insert(*class, *count + bonus);
        }

        allocation
    }

    /// Estimated task success rate for a given class.
    ///
    /// Higher classes have more resources and are less likely to OOM or timeout.
    pub fn class_reliability_estimate(class: NodeClass) -> f64 {
        match class {
            NodeClass::Edge => 0.85,
            NodeClass::Light => 0.92,
            NodeClass::Standard => 0.97,
            NodeClass::Enterprise => 0.99,
        }
    }

    /// Produce a multi-line report summarizing the distribution.
    pub fn format_distribution_report(distribution: &ClassDistribution) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "=== Node Class Distribution (total: {}) ===",
            distribution.total
        ));
        lines.push(String::new());
        lines.push(distribution.to_bar_chart(30));
        lines.push(String::new());
        lines.push(format!(
            "Effective compute units: {:.2}",
            distribution.effective_compute_units()
        ));
        lines.push(format!("Dominant class: {}", distribution.dominant_class()));
        lines.push(format!("Weakest class:  {}", distribution.weakest_class()));
        lines.push(format!(
            "Balanced:       {}",
            if distribution.is_balanced() {
                "yes"
            } else {
                "no"
            }
        ));
        lines.push(format!(
            "Heavy ratio:    {:.1}%",
            distribution.heavy_ratio() * 100.0
        ));
        lines.push(format!(
            "Light ratio:    {:.1}%",
            distribution.light_ratio() * 100.0
        ));
        lines.join("\n")
    }

    /// Estimate total swarm memory capacity in MB from a distribution.
    pub fn estimated_total_memory_mb(distribution: &ClassDistribution) -> u64 {
        // Use midpoint estimates for each class.
        let dust_avg_mb: u64 = 750;
        let pebble_avg_mb: u64 = 4_000;
        let rock_avg_mb: u64 = 12_000;
        let boulder_avg_mb: u64 = 40_000;

        distribution.edge as u64 * dust_avg_mb
            + distribution.light as u64 * pebble_avg_mb
            + distribution.standard as u64 * rock_avg_mb
            + distribution.enterprise as u64 * boulder_avg_mb
    }

    /// Estimate total swarm CPU cores from a distribution.
    pub fn estimated_total_cores(distribution: &ClassDistribution) -> u64 {
        // Midpoint estimates.
        let dust_avg: u64 = 1;
        let pebble_avg: u64 = 3;
        let rock_avg: u64 = 6;
        let boulder_avg: u64 = 16;

        distribution.edge as u64 * dust_avg
            + distribution.light as u64 * pebble_avg
            + distribution.standard as u64 * rock_avg
            + distribution.enterprise as u64 * boulder_avg
    }

    /// Suggest the ideal class mix for a given workload size.
    ///
    /// Returns a HashMap of class -> recommended count.
    pub fn ideal_mix_for_workload(
        total_chunks: u64,
        avg_memory_mb: u64,
        avg_duration_secs: u64,
    ) -> HashMap<NodeClass, u64> {
        let min_class = ConstraintEnforcer::minimum_class_for(avg_memory_mb, avg_duration_secs);
        let mut mix = HashMap::new();
        for &class in NodeClass::all() {
            if class >= min_class {
                mix.insert(class, 0);
            }
        }

        if mix.is_empty() {
            mix.insert(NodeClass::Enterprise, total_chunks);
            return mix;
        }

        // Distribute proportionally to compute weight among eligible classes.
        let total_weight: f64 = mix.keys().map(|c| c.compute_weight()).sum();
        let mut assigned = 0u64;
        let eligible: Vec<NodeClass> = mix.keys().copied().collect();

        for (i, &class) in eligible.iter().enumerate() {
            if i == eligible.len() - 1 {
                // Last class gets the remainder.
                let remaining = total_chunks.saturating_sub(assigned);
                mix.insert(class, remaining);
            } else {
                let share =
                    (class.compute_weight() / total_weight * total_chunks as f64).round() as u64;
                mix.insert(class, share);
                assigned += share;
            }
        }

        mix
    }
}

// ============================================================================
// ClassRecommender
// ============================================================================

/// Recommends which node classes to target for a given job.
pub struct ClassRecommender;

impl ClassRecommender {
    /// Recommend a sorted list of node classes suitable for the given resource
    /// requirements and priority level.
    ///
    /// Higher priority levels prefer larger classes for reliability.
    pub fn recommend_classes(
        memory_mb: u64,
        duration_secs: u64,
        priority_level: &str,
    ) -> Vec<NodeClass> {
        let min_class = ConstraintEnforcer::minimum_class_for(memory_mb, duration_secs);

        let mut candidates: Vec<NodeClass> = NodeClass::all()
            .iter()
            .copied()
            .filter(|c| *c >= min_class)
            .collect();

        match priority_level {
            "rush" => {
                // Rush jobs prefer the biggest classes first.
                candidates.sort_by(|a, b| b.cmp(a));
            }
            "economy" => {
                // Economy jobs prefer the smallest sufficient class.
                candidates.sort();
            }
            _ => {
                // Standard: prefer Standard if available, then up, then down.
                candidates.sort_by_key(|c| {
                    
                    (*c as i32 - NodeClass::Standard as i32).unsigned_abs()
                });
            }
        }

        candidates
    }

    /// Suggest the minimum class for a job described by a generic requirements map.
    ///
    /// Recognized keys: "memory_mb" (u64), "duration_secs" (u64),
    /// "input_size_mb" (u64), "output_size_mb" (u64).
    pub fn suggest_min_class(
        job_requirements: &HashMap<String, serde_json::Value>,
    ) -> NodeClass {
        let extract_u64 = |key: &str| -> u64 {
            job_requirements
                .get(key)
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
        };

        let memory_mb = extract_u64("memory_mb");
        let duration_secs = extract_u64("duration_secs");
        let input_mb = extract_u64("input_size_mb");
        let output_mb = extract_u64("output_size_mb");

        ConstraintEnforcer::minimum_class_for_all(memory_mb, duration_secs, input_mb, output_mb)
    }

    /// Return the recommended batch size (items per chunk) for a class.
    pub fn recommended_batch_size(class: NodeClass) -> u32 {
        class.suggested_batch_size()
    }

    /// Given a target completion time and distribution, recommend whether the
    /// swarm has sufficient capacity. Returns (can_meet, estimated_time_secs).
    pub fn can_meet_deadline(
        total_chunks: u64,
        chunk_duration_ms: u64,
        deadline_secs: u64,
        distribution: &ClassDistribution,
    ) -> (bool, f64) {
        let throughput = ClassAnalytics::estimated_throughput(distribution, chunk_duration_ms);
        if throughput <= 0.0 {
            return (false, f64::INFINITY);
        }
        let estimated_secs = total_chunks as f64 / throughput;
        (estimated_secs <= deadline_secs as f64, estimated_secs)
    }

    /// Suggest a priority level based on the deadline pressure.
    ///
    /// Returns "rush" if the deadline is very tight, "economy" if very relaxed,
    /// "standard" otherwise.
    pub fn suggest_priority(
        total_chunks: u64,
        chunk_duration_ms: u64,
        deadline_secs: u64,
        distribution: &ClassDistribution,
    ) -> &'static str {
        let throughput = ClassAnalytics::estimated_throughput(distribution, chunk_duration_ms);
        if throughput <= 0.0 {
            return "rush";
        }
        let estimated_secs = total_chunks as f64 / throughput;
        let ratio = estimated_secs / deadline_secs as f64;

        if ratio > 0.8 {
            "rush"
        } else if ratio < 0.3 {
            "economy"
        } else {
            "standard"
        }
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::{NodeInfo, NodeStatus, ResourceSnapshot, Trait};
    use chrono::Utc;
    use std::collections::HashSet;

    // ---- helpers ----

    fn make_snapshot(cores: u32, memory_mb: u64) -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_cores: cores,
            cpu_available: 0.5,
            memory_total_mb: memory_mb,
            memory_available_mb: memory_mb / 2,
            disk_total_mb: 50_000,
            disk_available_mb: 25_000,
            network_bandwidth_mbps: 100.0,
            ..Default::default()
        }
    }

    fn make_node_info(node_id: NodeId, cores: u32, memory_mb: u64) -> NodeInfo {
        NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.1,
            capacity: make_snapshot(cores, memory_mb),
            address: None,
            via: node_id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
            ..Default::default()
        }
    }

    fn make_knowledge_store(nodes: Vec<(u32, u64)>) -> Arc<KnowledgeStore> {
        let self_id = NodeId::new();
        let ks = Arc::new(KnowledgeStore::new(self_id));
        for (cores, mem) in nodes {
            let nid = NodeId::new();
            ks.merge_node(make_node_info(nid, cores, mem));
        }
        ks
    }

    // ====================================================================
    // NodeClassifier tests
    // ====================================================================

    #[test]
    fn classify_from_snapshot_dust() {
        let snap = make_snapshot(1, 512);
        assert_eq!(NodeClassifier::classify_from_snapshot(&snap), NodeClass::Edge);
    }

    #[test]
    fn classify_from_snapshot_pebble() {
        let snap = make_snapshot(4, 4_000);
        assert_eq!(NodeClassifier::classify_from_snapshot(&snap), NodeClass::Light);
    }

    #[test]
    fn classify_from_snapshot_rock() {
        let snap = make_snapshot(8, 16_000);
        assert_eq!(NodeClassifier::classify_from_snapshot(&snap), NodeClass::Standard);
    }

    #[test]
    fn classify_from_snapshot_boulder() {
        let snap = make_snapshot(32, 64_000);
        assert_eq!(NodeClassifier::classify_from_snapshot(&snap), NodeClass::Enterprise);
    }

    #[test]
    fn classifier_with_knowledge_store() {
        let ks = make_knowledge_store(vec![(1, 500), (4, 4000), (8, 16000)]);
        let classifier = NodeClassifier::new().with_knowledge(ks.clone());

        let nodes = ks.get_all_nodes();
        assert_eq!(nodes.len(), 3);

        // Verify each node is classified correctly.
        for node in &nodes {
            let class = classifier.classify_node(&node.node_id);
            let expected = NodeClass::from_resources(
                node.capacity.cpu_cores,
                node.capacity.memory_total_mb,
            );
            assert_eq!(class, expected);
        }
    }

    #[test]
    fn classifier_override_takes_priority() {
        let ks = make_knowledge_store(vec![(1, 500)]); // Edge node
        let classifier = NodeClassifier::new().with_knowledge(ks.clone());
        let node_id = ks.get_all_nodes()[0].node_id;

        assert_eq!(classifier.get_class(&node_id), NodeClass::Edge);

        classifier.override_class(node_id, NodeClass::Enterprise);
        assert_eq!(classifier.get_class(&node_id), NodeClass::Enterprise);

        classifier.remove_override(&node_id);
        assert_eq!(classifier.get_class(&node_id), NodeClass::Edge);
    }

    #[test]
    fn classifier_no_knowledge_defaults_dust() {
        let classifier = NodeClassifier::new();
        let random_id = NodeId::new();
        assert_eq!(classifier.classify_node(&random_id), NodeClass::Edge);
    }

    #[test]
    fn classifier_get_distribution_live() {
        let ks = make_knowledge_store(vec![
            (1, 500),    // Edge
            (1, 800),    // Edge
            (4, 4000),   // Light
            (8, 16000),  // Standard
            (32, 64000), // Enterprise
        ]);
        let classifier = NodeClassifier::new().with_knowledge(ks);
        let dist = classifier.get_distribution_live();

        assert_eq!(dist.edge, 2);
        assert_eq!(dist.light, 1);
        assert_eq!(dist.standard, 1);
        assert_eq!(dist.enterprise, 1);
        assert_eq!(dist.total, 5);
    }

    #[test]
    fn classifier_effective_compute_units() {
        let ks = make_knowledge_store(vec![
            (1, 500),   // Edge: 0.05
            (4, 4000),  // Light: 0.25
            (8, 16000), // Standard: 1.0
        ]);
        let classifier = NodeClassifier::new().with_knowledge(ks);
        let ecu = classifier.effective_compute_units();
        let expected = 0.05 + 0.25 + 1.0;
        assert!((ecu - expected).abs() < 0.001);
    }

    #[test]
    fn classifier_class_for_cores_and_memory() {
        assert_eq!(NodeClassifier::class_for_cores_and_memory(1, 500), NodeClass::Edge);
        assert_eq!(NodeClassifier::class_for_cores_and_memory(4, 4000), NodeClass::Light);
        assert_eq!(NodeClassifier::class_for_cores_and_memory(8, 16000), NodeClass::Standard);
        assert_eq!(NodeClassifier::class_for_cores_and_memory(32, 64000), NodeClass::Enterprise);
    }

    #[test]
    fn classifier_node_count_by_class() {
        let ks = make_knowledge_store(vec![
            (1, 500),
            (1, 800),
            (4, 4000),
        ]);
        let classifier = NodeClassifier::new().with_knowledge(ks);
        let counts = classifier.node_count_by_class();
        assert_eq!(*counts.get(&NodeClass::Edge).unwrap(), 2);
        assert_eq!(*counts.get(&NodeClass::Light).unwrap(), 1);
        assert_eq!(*counts.get(&NodeClass::Standard).unwrap(), 0);
        assert_eq!(*counts.get(&NodeClass::Enterprise).unwrap(), 0);
    }

    #[test]
    fn classifier_has_override() {
        let classifier = NodeClassifier::new();
        let nid = NodeId::new();
        assert!(!classifier.has_override(&nid));
        classifier.override_class(nid, NodeClass::Standard);
        assert!(classifier.has_override(&nid));
    }

    #[test]
    fn classifier_clear_overrides() {
        let classifier = NodeClassifier::new();
        classifier.override_class(NodeId::new(), NodeClass::Standard);
        classifier.override_class(NodeId::new(), NodeClass::Enterprise);
        assert_eq!(classifier.override_count(), 2);
        classifier.clear_overrides();
        assert_eq!(classifier.override_count(), 0);
    }

    #[test]
    fn classifier_list_overrides() {
        let classifier = NodeClassifier::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        classifier.override_class(n1, NodeClass::Standard);
        classifier.override_class(n2, NodeClass::Enterprise);
        let overrides = classifier.list_overrides();
        assert_eq!(overrides.len(), 2);
    }

    // ====================================================================
    // ClassDistribution tests
    // ====================================================================

    #[test]
    fn distribution_from_counts_basic() {
        let dist = ClassDistribution::from_counts(10, 20, 30, 40);
        assert_eq!(dist.total, 100);
        assert!((dist.dust_pct - 10.0).abs() < 0.01);
        assert!((dist.pebble_pct - 20.0).abs() < 0.01);
        assert!((dist.rock_pct - 30.0).abs() < 0.01);
        assert!((dist.boulder_pct - 40.0).abs() < 0.01);
    }

    #[test]
    fn distribution_zero_nodes() {
        let dist = ClassDistribution::from_counts(0, 0, 0, 0);
        assert_eq!(dist.total, 0);
        assert!((dist.dust_pct - 0.0).abs() < 0.01);
        assert!(dist.is_empty());
        assert!(dist.is_balanced());
    }

    #[test]
    fn distribution_count_for_and_pct_for() {
        let dist = ClassDistribution::from_counts(5, 10, 15, 20);
        assert_eq!(dist.count_for(NodeClass::Edge), 5);
        assert_eq!(dist.count_for(NodeClass::Light), 10);
        assert_eq!(dist.count_for(NodeClass::Standard), 15);
        assert_eq!(dist.count_for(NodeClass::Enterprise), 20);
        assert!((dist.pct_for(NodeClass::Edge) - 10.0).abs() < 0.01);
    }

    #[test]
    fn distribution_effective_compute_units() {
        let dist = ClassDistribution::from_counts(10, 10, 10, 10);
        let expected = 10.0 * 0.05 + 10.0 * 0.25 + 10.0 * 1.0 + 10.0 * 4.0;
        assert!((dist.effective_compute_units() - expected).abs() < 0.001);
    }

    #[test]
    fn distribution_dominant_class() {
        let dist = ClassDistribution::from_counts(5, 10, 30, 20);
        assert_eq!(dist.dominant_class(), NodeClass::Standard);
    }

    #[test]
    fn distribution_dominant_class_tie_favours_higher() {
        let dist = ClassDistribution::from_counts(10, 10, 10, 10);
        // All equal -- tie broken by higher class.
        assert_eq!(dist.dominant_class(), NodeClass::Enterprise);
    }

    #[test]
    fn distribution_weakest_class() {
        let dist = ClassDistribution::from_counts(5, 10, 30, 20);
        assert_eq!(dist.weakest_class(), NodeClass::Edge);
    }

    #[test]
    fn distribution_is_balanced_true() {
        let dist = ClassDistribution::from_counts(25, 25, 25, 25);
        assert!(dist.is_balanced());
    }

    #[test]
    fn distribution_is_balanced_false() {
        let dist = ClassDistribution::from_counts(0, 0, 0, 100);
        assert!(!dist.is_balanced());
    }

    #[test]
    fn distribution_all_one_class() {
        let dist = ClassDistribution::from_counts(0, 0, 0, 50);
        assert_eq!(dist.total, 50);
        assert!((dist.boulder_pct - 100.0).abs() < 0.01);
        assert_eq!(dist.dominant_class(), NodeClass::Enterprise);
        assert_eq!(dist.weakest_class(), NodeClass::Edge);
        assert!(!dist.is_balanced());
    }

    #[test]
    fn distribution_bar_chart_not_empty() {
        let dist = ClassDistribution::from_counts(10, 20, 30, 40);
        let chart = dist.to_bar_chart(40);
        assert!(chart.contains("Edge"));
        assert!(chart.contains("Light"));
        assert!(chart.contains("Standard"));
        assert!(chart.contains("Enterprise"));
        assert!(chart.contains("#"));
    }

    #[test]
    fn distribution_bar_chart_zero_nodes() {
        let dist = ClassDistribution::from_counts(0, 0, 0, 0);
        let chart = dist.to_bar_chart(30);
        // Should not panic; all bars are empty.
        assert!(chart.contains("Edge"));
        assert!(chart.contains("0"));
    }

    #[test]
    fn distribution_heavy_and_light_ratios() {
        let dist = ClassDistribution::from_counts(10, 10, 40, 40);
        assert!((dist.heavy_ratio() - 0.8).abs() < 0.01);
        assert!((dist.light_ratio() - 0.2).abs() < 0.01);
    }

    #[test]
    fn distribution_merge() {
        let a = ClassDistribution::from_counts(1, 2, 3, 4);
        let b = ClassDistribution::from_counts(4, 3, 2, 1);
        let merged = a.merge(&b);
        assert_eq!(merged.edge, 5);
        assert_eq!(merged.light, 5);
        assert_eq!(merged.standard, 5);
        assert_eq!(merged.enterprise, 5);
        assert_eq!(merged.total, 20);
    }

    #[test]
    fn distribution_subtract_clamped() {
        let a = ClassDistribution::from_counts(5, 5, 5, 5);
        let b = ClassDistribution::from_counts(10, 3, 0, 2);
        let result = a.subtract(&b);
        assert_eq!(result.edge, 0); // clamped from -5
        assert_eq!(result.light, 2);
        assert_eq!(result.standard, 5);
        assert_eq!(result.enterprise, 3);
    }

    #[test]
    fn distribution_as_count_map() {
        let dist = ClassDistribution::from_counts(1, 2, 3, 4);
        let map = dist.as_count_map();
        assert_eq!(map.len(), 4);
        assert_eq!(*map.get(&NodeClass::Standard).unwrap(), 3);
    }

    // ====================================================================
    // ConstraintEnforcer tests
    // ====================================================================

    #[test]
    fn constraint_check_memory_pass() {
        assert!(ConstraintEnforcer::check_memory(NodeClass::Standard, 4000).is_ok());
    }

    #[test]
    fn constraint_check_memory_fail() {
        let result = ConstraintEnforcer::check_memory(NodeClass::Edge, 200);
        assert!(result.is_err());
        let violation = result.unwrap_err();
        assert_eq!(violation.constraint_type, "memory");
        assert_eq!(violation.limit, 100);
        assert_eq!(violation.actual, 200);
        assert_eq!(violation.node_class, NodeClass::Edge);
        assert!(violation.message.contains("Edge"));
    }

    #[test]
    fn constraint_check_duration_pass() {
        assert!(ConstraintEnforcer::check_duration(NodeClass::Enterprise, 10000).is_ok());
    }

    #[test]
    fn constraint_check_duration_fail() {
        let result = ConstraintEnforcer::check_duration(NodeClass::Light, 1000);
        assert!(result.is_err());
        let v = result.unwrap_err();
        assert_eq!(v.constraint_type, "duration");
    }

    #[test]
    fn constraint_check_input_size_pass() {
        assert!(ConstraintEnforcer::check_input_size(NodeClass::Standard, 100).is_ok());
    }

    #[test]
    fn constraint_check_input_size_fail() {
        let result = ConstraintEnforcer::check_input_size(NodeClass::Edge, 10);
        assert!(result.is_err());
    }

    #[test]
    fn constraint_check_output_size_pass() {
        assert!(ConstraintEnforcer::check_output_size(NodeClass::Enterprise, 500).is_ok());
    }

    #[test]
    fn constraint_check_output_size_fail() {
        let result = ConstraintEnforcer::check_output_size(NodeClass::Edge, 5);
        assert!(result.is_err());
    }

    #[test]
    fn constraint_check_all_no_violations() {
        let violations = ConstraintEnforcer::check_all(
            NodeClass::Enterprise,
            1000,   // memory
            3600,   // duration
            100,    // input
            100,    // output
        );
        assert!(violations.is_empty());
    }

    #[test]
    fn constraint_check_all_multiple_violations() {
        let violations = ConstraintEnforcer::check_all(
            NodeClass::Edge,
            200,   // memory: exceeds 100
            120,   // duration: exceeds 60
            10,    // input: exceeds 5
            5,     // output: exceeds 1
        );
        assert_eq!(violations.len(), 4);
    }

    #[test]
    fn constraint_can_handle_hierarchy() {
        assert!(ConstraintEnforcer::can_handle(NodeClass::Enterprise, NodeClass::Edge));
        assert!(ConstraintEnforcer::can_handle(NodeClass::Standard, NodeClass::Standard));
        assert!(ConstraintEnforcer::can_handle(NodeClass::Standard, NodeClass::Light));
        assert!(!ConstraintEnforcer::can_handle(NodeClass::Edge, NodeClass::Light));
        assert!(!ConstraintEnforcer::can_handle(NodeClass::Light, NodeClass::Standard));
    }

    #[test]
    fn constraint_minimum_class_for_dust() {
        assert_eq!(
            ConstraintEnforcer::minimum_class_for(50, 30),
            NodeClass::Edge
        );
    }

    #[test]
    fn constraint_minimum_class_for_rock() {
        assert_eq!(
            ConstraintEnforcer::minimum_class_for(2000, 2000),
            NodeClass::Standard
        );
    }

    #[test]
    fn constraint_minimum_class_for_boulder() {
        assert_eq!(
            ConstraintEnforcer::minimum_class_for(30000, 10000),
            NodeClass::Enterprise
        );
    }

    #[test]
    fn constraint_minimum_class_for_exceeds_all() {
        // Nothing can satisfy this, so Enterprise is returned as the best option.
        assert_eq!(
            ConstraintEnforcer::minimum_class_for(100_000, 100_000),
            NodeClass::Enterprise
        );
    }

    #[test]
    fn constraint_minimum_class_for_all_dimensions() {
        // Edge can handle 100MB memory and 60s, but only 5MB input.
        // Need Light for 10MB input.
        assert_eq!(
            ConstraintEnforcer::minimum_class_for_all(50, 30, 10, 1),
            NodeClass::Light
        );
    }

    #[test]
    fn constraint_summary_pass() {
        let s = ConstraintEnforcer::summary(NodeClass::Enterprise, 100, 100, 10, 10);
        assert!(s.contains("can handle"));
    }

    #[test]
    fn constraint_summary_fail() {
        let s = ConstraintEnforcer::summary(NodeClass::Edge, 200, 120, 10, 5);
        assert!(s.contains("CANNOT"));
    }

    #[test]
    fn constraint_violation_display() {
        let v = ConstraintViolation {
            constraint_type: "memory".into(),
            limit: 100,
            actual: 200,
            node_class: NodeClass::Edge,
            message: "Edge class allows 100MB memory, but task requires 200MB".into(),
        };
        let displayed = format!("{}", v);
        assert!(displayed.contains("100MB"));
        assert!(displayed.contains("200MB"));
    }

    // ====================================================================
    // ClassAnalytics tests
    // ====================================================================

    #[test]
    fn analytics_compute_power_by_class() {
        let dist = ClassDistribution::from_counts(10, 5, 3, 2);
        let power = ClassAnalytics::compute_power_by_class(&dist);
        assert!((power[&NodeClass::Edge] - 10.0 * 0.05).abs() < 0.001);
        assert!((power[&NodeClass::Light] - 5.0 * 0.25).abs() < 0.001);
        assert!((power[&NodeClass::Standard] - 3.0 * 1.0).abs() < 0.001);
        assert!((power[&NodeClass::Enterprise] - 2.0 * 4.0).abs() < 0.001);
    }

    #[test]
    fn analytics_estimated_throughput() {
        let dist = ClassDistribution::from_counts(0, 0, 10, 0);
        // 10 Standard nodes, each with weight 1.0 = 10 ECU.
        // At 1000ms per chunk = 10 chunks/sec.
        let throughput = ClassAnalytics::estimated_throughput(&dist, 1000);
        assert!((throughput - 10.0).abs() < 0.01);
    }

    #[test]
    fn analytics_estimated_throughput_zero_duration() {
        let dist = ClassDistribution::from_counts(10, 10, 10, 10);
        assert_eq!(ClassAnalytics::estimated_throughput(&dist, 0), 0.0);
    }

    #[test]
    fn analytics_optimal_chunk_allocation_sums_correctly() {
        let dist = ClassDistribution::from_counts(10, 10, 10, 10);
        let alloc = ClassAnalytics::optimal_chunk_allocation(100, &dist);
        let total: u64 = alloc.values().sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn analytics_optimal_chunk_allocation_zero_items() {
        let dist = ClassDistribution::from_counts(10, 10, 10, 10);
        let alloc = ClassAnalytics::optimal_chunk_allocation(0, &dist);
        let total: u64 = alloc.values().sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn analytics_optimal_chunk_allocation_zero_nodes() {
        let dist = ClassDistribution::from_counts(0, 0, 0, 0);
        let alloc = ClassAnalytics::optimal_chunk_allocation(100, &dist);
        let total: u64 = alloc.values().sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn analytics_optimal_chunk_allocation_weighted() {
        // Enterprise (weight 4.0) has 10 nodes = 40 ECU.
        // Standard (weight 1.0) has 10 nodes = 10 ECU.
        // Total ECU = 50.
        // Enterprise should get ~80% of 1000 = 800, Standard ~200.
        let dist = ClassDistribution::from_counts(0, 0, 10, 10);
        let alloc = ClassAnalytics::optimal_chunk_allocation(1000, &dist);
        assert!(*alloc.get(&NodeClass::Enterprise).unwrap() > *alloc.get(&NodeClass::Standard).unwrap());
        assert_eq!(alloc.values().sum::<u64>(), 1000);
    }

    #[test]
    fn analytics_class_reliability_estimate() {
        assert!(ClassAnalytics::class_reliability_estimate(NodeClass::Enterprise) >
                ClassAnalytics::class_reliability_estimate(NodeClass::Edge));
        assert!(ClassAnalytics::class_reliability_estimate(NodeClass::Standard) > 0.95);
    }

    #[test]
    fn analytics_format_distribution_report() {
        let dist = ClassDistribution::from_counts(10, 20, 30, 40);
        let report = ClassAnalytics::format_distribution_report(&dist);
        assert!(report.contains("Node Class Distribution"));
        assert!(report.contains("Dominant class"));
        assert!(report.contains("Balanced"));
        assert!(report.contains("Effective compute units"));
    }

    #[test]
    fn analytics_estimated_total_memory() {
        let dist = ClassDistribution::from_counts(0, 0, 0, 10);
        let mem = ClassAnalytics::estimated_total_memory_mb(&dist);
        assert_eq!(mem, 10 * 40_000);
    }

    #[test]
    fn analytics_estimated_total_cores() {
        let dist = ClassDistribution::from_counts(0, 0, 10, 0);
        let cores = ClassAnalytics::estimated_total_cores(&dist);
        assert_eq!(cores, 10 * 6);
    }

    // ====================================================================
    // ClassificationHistory tests
    // ====================================================================

    #[test]
    fn history_record_and_latest() {
        let mut history = ClassificationHistory::new(10);
        assert!(history.is_empty());
        assert!(history.latest().is_none());

        let dist = ClassDistribution::from_counts(1, 2, 3, 4);
        history.record(dist);

        assert_eq!(history.len(), 1);
        assert!(!history.is_empty());
        assert_eq!(history.latest().unwrap().total, 10);
    }

    #[test]
    fn history_evicts_old_snapshots() {
        let mut history = ClassificationHistory::new(3);
        for i in 0..5 {
            let dist = ClassDistribution::from_counts(i, 0, 0, 0);
            history.record(dist);
        }
        assert_eq!(history.len(), 3);
        // The oldest two (i=0 and i=1) should have been evicted.
        assert_eq!(history.snapshots()[0].1.edge, 2);
    }

    #[test]
    fn history_trend_rising() {
        let mut history = ClassificationHistory::new(100);
        // First half: Standard at ~30%.
        for _ in 0..5 {
            history.record(ClassDistribution::from_counts(10, 20, 30, 40));
        }
        // Second half: Standard at ~50%.
        for _ in 0..5 {
            history.record(ClassDistribution::from_counts(10, 10, 50, 30));
        }
        assert_eq!(history.trend(NodeClass::Standard), ClassTrend::Rising);
    }

    #[test]
    fn history_trend_falling() {
        let mut history = ClassificationHistory::new(100);
        for _ in 0..5 {
            history.record(ClassDistribution::from_counts(10, 20, 50, 20));
        }
        for _ in 0..5 {
            history.record(ClassDistribution::from_counts(10, 20, 20, 50));
        }
        assert_eq!(history.trend(NodeClass::Standard), ClassTrend::Falling);
    }

    #[test]
    fn history_trend_stable() {
        let mut history = ClassificationHistory::new(100);
        for _ in 0..10 {
            history.record(ClassDistribution::from_counts(25, 25, 25, 25));
        }
        assert_eq!(history.trend(NodeClass::Standard), ClassTrend::Stable);
    }

    #[test]
    fn history_trend_too_few_snapshots() {
        let mut history = ClassificationHistory::new(100);
        history.record(ClassDistribution::from_counts(10, 20, 30, 40));
        assert_eq!(history.trend(NodeClass::Standard), ClassTrend::Stable);
    }

    #[test]
    fn history_avg_over_last_n() {
        let mut history = ClassificationHistory::new(100);
        history.record(ClassDistribution::from_counts(10, 20, 30, 40));
        history.record(ClassDistribution::from_counts(20, 20, 30, 30));

        let avg = history.avg_over_last_n(2).unwrap();
        // (10+20)/2 = 15, (20+20)/2 = 20, (30+30)/2 = 30, (40+30)/2 = 35
        assert_eq!(avg.edge, 15);
        assert_eq!(avg.light, 20);
        assert_eq!(avg.standard, 30);
        assert_eq!(avg.enterprise, 35);
    }

    #[test]
    fn history_avg_over_last_n_empty() {
        let history = ClassificationHistory::new(100);
        assert!(history.avg_over_last_n(5).is_none());
    }

    #[test]
    fn history_clear() {
        let mut history = ClassificationHistory::new(100);
        history.record(ClassDistribution::from_counts(1, 2, 3, 4));
        history.clear();
        assert!(history.is_empty());
    }

    #[test]
    fn history_timestamps() {
        let mut history = ClassificationHistory::new(100);
        assert!(history.oldest_timestamp().is_none());
        assert!(history.newest_timestamp().is_none());

        history.record(ClassDistribution::from_counts(1, 2, 3, 4));
        assert!(history.oldest_timestamp().is_some());
        assert!(history.newest_timestamp().is_some());
    }

    // ====================================================================
    // ClassRecommender tests
    // ====================================================================

    #[test]
    fn recommender_standard_priority() {
        let classes = ClassRecommender::recommend_classes(50, 30, "standard");
        assert!(!classes.is_empty());
        // Edge can handle 100MB/60s, so Edge should be in the list.
        assert!(classes.contains(&NodeClass::Edge));
    }

    #[test]
    fn recommender_rush_prefers_big() {
        let classes = ClassRecommender::recommend_classes(50, 30, "rush");
        assert!(!classes.is_empty());
        // Rush should put Enterprise first.
        assert_eq!(classes[0], NodeClass::Enterprise);
    }

    #[test]
    fn recommender_economy_prefers_small() {
        let classes = ClassRecommender::recommend_classes(50, 30, "economy");
        assert!(!classes.is_empty());
        // Economy should put the smallest sufficient class first.
        assert_eq!(classes[0], NodeClass::Edge);
    }

    #[test]
    fn recommender_high_requirements_filters() {
        let classes = ClassRecommender::recommend_classes(30000, 10000, "standard");
        // Only Enterprise can satisfy 30GB memory and 10000s.
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0], NodeClass::Enterprise);
    }

    #[test]
    fn recommender_suggest_min_class_from_map() {
        let mut reqs = HashMap::new();
        reqs.insert("memory_mb".into(), serde_json::json!(2000));
        reqs.insert("duration_secs".into(), serde_json::json!(2000));
        reqs.insert("input_size_mb".into(), serde_json::json!(10));
        reqs.insert("output_size_mb".into(), serde_json::json!(5));

        let class = ClassRecommender::suggest_min_class(&reqs);
        // 2000MB mem needs Standard, 2000s needs Standard, 10MB input needs Light, 5MB output needs Light.
        assert!(class >= NodeClass::Standard);
    }

    #[test]
    fn recommender_suggest_min_class_empty_map() {
        let reqs: HashMap<String, serde_json::Value> = HashMap::new();
        let class = ClassRecommender::suggest_min_class(&reqs);
        // All zeros -> Edge can handle it.
        assert_eq!(class, NodeClass::Edge);
    }

    #[test]
    fn recommender_can_meet_deadline() {
        let dist = ClassDistribution::from_counts(0, 0, 10, 0);
        // 10 Standard nodes = 10 ECU. 1000ms chunks -> 10 chunks/s.
        // 100 chunks -> 10 seconds.
        let (can_meet, est) = ClassRecommender::can_meet_deadline(100, 1000, 30, &dist);
        assert!(can_meet);
        assert!((est - 10.0).abs() < 0.5);
    }

    #[test]
    fn recommender_cannot_meet_deadline() {
        let dist = ClassDistribution::from_counts(1, 0, 0, 0);
        // 1 Edge node = 0.05 ECU. 10000ms chunks -> 0.005 chunks/s.
        // 1000 chunks -> 200000 seconds.
        let (can_meet, _est) = ClassRecommender::can_meet_deadline(1000, 10000, 60, &dist);
        assert!(!can_meet);
    }

    #[test]
    fn recommender_suggest_priority() {
        let dist = ClassDistribution::from_counts(0, 0, 10, 0);
        // 10 ECU, 1000ms chunks -> 10 chunks/s. 100 chunks -> 10s.
        // Deadline 100s -> ratio = 0.1 -> economy.
        let priority = ClassRecommender::suggest_priority(100, 1000, 100, &dist);
        assert_eq!(priority, "economy");
    }

    #[test]
    fn recommender_suggest_priority_tight() {
        let dist = ClassDistribution::from_counts(0, 0, 10, 0);
        // 10 ECU, 1000ms chunks -> 10 chunks/s. 100 chunks -> 10s.
        // Deadline 12s -> ratio = 0.833 -> rush.
        let priority = ClassRecommender::suggest_priority(100, 1000, 12, &dist);
        assert_eq!(priority, "rush");
    }

    #[test]
    fn recommender_recommended_batch_size() {
        assert_eq!(ClassRecommender::recommended_batch_size(NodeClass::Edge), 1);
        assert_eq!(ClassRecommender::recommended_batch_size(NodeClass::Light), 25);
        assert_eq!(ClassRecommender::recommended_batch_size(NodeClass::Standard), 250);
        assert_eq!(ClassRecommender::recommended_batch_size(NodeClass::Enterprise), 2500);
    }

    // ====================================================================
    // ClassTrend display
    // ====================================================================

    #[test]
    fn class_trend_display() {
        assert_eq!(format!("{}", ClassTrend::Rising), "Rising");
        assert_eq!(format!("{}", ClassTrend::Falling), "Falling");
        assert_eq!(format!("{}", ClassTrend::Stable), "Stable");
    }

    // ====================================================================
    // Edge cases
    // ====================================================================

    #[test]
    fn distribution_from_knowledge_skips_dead_nodes() {
        let self_id = NodeId::new();
        let ks = Arc::new(KnowledgeStore::new(self_id));

        let alive_id = NodeId::new();
        ks.merge_node(make_node_info(alive_id, 8, 16000));

        let dead_id = NodeId::new();
        ks.merge_node(make_node_info(dead_id, 32, 64000));
        ks.mark_node_dead(&dead_id);

        let dist = ClassDistribution::from_knowledge(&ks);
        assert_eq!(dist.total, 1);
        assert_eq!(dist.standard, 1);
        assert_eq!(dist.enterprise, 0);
    }

    #[test]
    fn analytics_ideal_mix_for_workload() {
        let mix = ClassAnalytics::ideal_mix_for_workload(100, 50, 30);
        // Edge can handle 100MB/60s, so all classes should be eligible.
        let total: u64 = mix.values().sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn analytics_ideal_mix_high_requirements() {
        let mix = ClassAnalytics::ideal_mix_for_workload(100, 30000, 10000);
        // Only Enterprise can satisfy this.
        assert_eq!(*mix.get(&NodeClass::Enterprise).unwrap_or(&0), 100);
    }

    #[test]
    fn constraint_enforcer_constraints_for_class() {
        let tc = ConstraintEnforcer::constraints_for_class(NodeClass::Standard);
        assert_eq!(tc.node_class, NodeClass::Standard);
        assert_eq!(tc.max_memory_mb, NodeClass::Standard.max_memory_mb());
        assert_eq!(tc.max_duration_secs, NodeClass::Standard.max_duration_secs());
    }

    #[test]
    fn constraint_check_memory_exact_boundary() {
        // Exactly at the limit should pass.
        assert!(ConstraintEnforcer::check_memory(NodeClass::Edge, 100).is_ok());
        // One over should fail.
        assert!(ConstraintEnforcer::check_memory(NodeClass::Edge, 101).is_err());
    }

    #[test]
    fn constraint_check_duration_exact_boundary() {
        assert!(ConstraintEnforcer::check_duration(NodeClass::Edge, 60).is_ok());
        assert!(ConstraintEnforcer::check_duration(NodeClass::Edge, 61).is_err());
    }
}
