// Marabunta - Licensed under the MIT License.
//! Priority-aware job scheduler for the Marabunta Swarm.
//!
//! Manages three priority queues (Rush, Standard, Economy), performs
//! node-class-aware assignment, and defers Economy jobs to off-peak hours.
//! The scheduler is fully concurrent: multiple worker threads can call
//! [`JobScheduler::dequeue_for_node`] simultaneously without contention
//! beyond the per-queue mutex.
//!
//! # Queue hierarchy
//!
//! ```text
//!   Rush (weight 100)  ──> always dequeued first
//!   Standard (weight 50) ──> dequeued when no Rush work available
//!   Economy (weight 10) ──> only during off-peak hours (configurable)
//! ```
//!
//! # Node class matching
//!
//! Chunks declare a minimum [`NodeClass`]; a node can handle any chunk
//! whose `min_class` is at or below its own class (Enterprise handles
//! everything, Edge handles only Edge-level work).

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Timelike, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::common::types::JobId;
use super::config::*;
use super::knowledge::KnowledgeStore;
use super::types::{ChunkId, NodeClass, NodeId, PriorityLevel, TaskConstraints};

// ============================================================================
// SchedulerConfig
// ============================================================================

/// Runtime configuration for the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    /// How often the scheduler loop ticks (milliseconds).
    pub tick_interval_ms: u64,
    /// Start of off-peak window (UTC hour, 0-23). Economy jobs run here.
    pub offpeak_start_hour: u32,
    /// End of off-peak window (UTC hour, 0-23).
    pub offpeak_end_hour: u32,
    /// Maximum total queue depth before rejecting new enqueues.
    pub max_queue_depth: usize,
    /// Fraction of capacity reserved for Rush jobs (0.0 - 1.0).
    pub rush_reserve_fraction: f64,
    /// Maximum retry attempts per chunk before permanent failure.
    pub max_attempts_per_chunk: u32,
    /// Seconds before an active assignment is considered timed out.
    pub assignment_timeout_secs: u64,
    /// Whether to prefer data-local nodes when dequeuing.
    pub enable_data_locality: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: SCHEDULER_TICK_MS,
            offpeak_start_hour: SCHEDULER_OFFPEAK_START_HOUR,
            offpeak_end_hour: SCHEDULER_OFFPEAK_END_HOUR,
            max_queue_depth: SCHEDULER_MAX_QUEUE_DEPTH,
            rush_reserve_fraction: SCHEDULER_RUSH_RESERVE_FRACTION,
            max_attempts_per_chunk: MAX_CHUNK_ATTEMPTS,
            assignment_timeout_secs: 300,
            enable_data_locality: true,
        }
    }
}

// ============================================================================
// SchedulableChunk
// ============================================================================

/// A chunk waiting to be assigned to a node for execution.
///
/// Implements [`Ord`] so it can live in a [`BinaryHeap`]: higher priority
/// and earlier enqueue time are dequeued first (max-heap on a composite key).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulableChunk {
    /// Unique identifier for this chunk.
    pub chunk_id: ChunkId,
    /// The job this chunk belongs to.
    pub job_id: JobId,
    /// Priority level (Rush / Standard / Economy).
    pub priority: PriorityLevel,
    /// Minimum node class required to execute this chunk.
    pub min_class: NodeClass,
    /// Memory requirement in megabytes.
    pub memory_mb: u64,
    /// Estimated execution duration in seconds.
    pub duration_secs: u64,
    /// When this chunk was enqueued.
    pub enqueued_at: DateTime<Utc>,
    /// Current attempt number (starts at 0).
    pub attempt: u32,
    /// Maximum number of attempts before permanent failure.
    pub max_attempts: u32,
    /// If set, prefer assigning to this node (data locality hint).
    pub data_locality: Option<NodeId>,
}

impl SchedulableChunk {
    /// Create a new schedulable chunk with default attempt tracking.
    pub fn new(
        chunk_id: ChunkId,
        job_id: JobId,
        priority: PriorityLevel,
        min_class: NodeClass,
    ) -> Self {
        Self {
            chunk_id,
            job_id,
            priority,
            min_class,
            memory_mb: 0,
            duration_secs: 0,
            enqueued_at: Utc::now(),
            attempt: 0,
            max_attempts: MAX_CHUNK_ATTEMPTS,
            data_locality: None,
        }
    }

    /// Builder: set memory requirement.
    pub fn with_memory_mb(mut self, mb: u64) -> Self {
        self.memory_mb = mb;
        self
    }

    /// Builder: set estimated duration.
    pub fn with_duration_secs(mut self, secs: u64) -> Self {
        self.duration_secs = secs;
        self
    }

    /// Builder: set data locality hint.
    pub fn with_data_locality(mut self, node: NodeId) -> Self {
        self.data_locality = Some(node);
        self
    }

    /// Builder: set max attempts.
    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = max;
        self
    }

    /// Whether this chunk has exhausted its retry budget.
    pub fn is_exhausted(&self) -> bool {
        self.attempt >= self.max_attempts
    }

    /// How long this chunk has been waiting in the queue.
    pub fn wait_duration_ms(&self) -> i64 {
        (Utc::now() - self.enqueued_at).num_milliseconds()
    }

    /// Composite sort key: (priority_weight, negative_enqueue_timestamp).
    /// Higher is better for the max-heap.
    fn sort_key(&self) -> (u32, i64) {
        // Negate the timestamp so earlier times sort higher in a max-heap.
        let ts = -self.enqueued_at.timestamp_millis();
        (self.priority.queue_weight(), ts)
    }
}

impl PartialEq for SchedulableChunk {
    fn eq(&self, other: &Self) -> bool {
        self.chunk_id == other.chunk_id
    }
}

impl Eq for SchedulableChunk {}

impl PartialOrd for SchedulableChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SchedulableChunk {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sort_key().cmp(&other.sort_key())
    }
}

// ============================================================================
// Assignment (scheduler-local)
// ============================================================================

/// An active chunk-to-node assignment tracked by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerAssignment {
    /// The chunk being executed.
    pub chunk_id: ChunkId,
    /// The job this chunk belongs to.
    pub job_id: JobId,
    /// The node executing this chunk.
    pub node_id: NodeId,
    /// The class of the executing node.
    pub node_class: NodeClass,
    /// When the assignment was made.
    pub assigned_at: DateTime<Utc>,
    /// Deadline after which the assignment is considered timed out.
    pub deadline: DateTime<Utc>,
    /// Priority level of the assigned chunk.
    pub priority: PriorityLevel,
}

impl SchedulerAssignment {
    /// Whether this assignment has passed its deadline.
    pub fn is_overdue(&self) -> bool {
        Utc::now() > self.deadline
    }

    /// Remaining time before deadline in seconds (negative if overdue).
    pub fn remaining_secs(&self) -> i64 {
        (self.deadline - Utc::now()).num_seconds()
    }
}

// ============================================================================
// QueueDepth
// ============================================================================

/// Snapshot of queue depths across all priority levels.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct QueueDepth {
    /// Number of chunks in the Rush queue.
    pub rush: usize,
    /// Number of chunks in the Standard queue.
    pub standard: usize,
    /// Number of chunks in the Economy queue.
    pub economy: usize,
    /// Total across all queues.
    pub total: usize,
    /// Number of active (in-flight) assignments.
    pub active_assignments: usize,
}

impl QueueDepth {
    /// Whether all queues are empty.
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Fraction of max queue depth currently used.
    pub fn utilization(&self, max_depth: usize) -> f64 {
        if max_depth == 0 {
            return 0.0;
        }
        self.total as f64 / max_depth as f64
    }
}

impl std::fmt::Display for QueueDepth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rush={} standard={} economy={} total={} active={}",
            self.rush, self.standard, self.economy, self.total, self.active_assignments
        )
    }
}

// ============================================================================
// SchedulerStats
// ============================================================================

/// Aggregate statistics for the scheduler.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchedulerStats {
    /// Total chunks enqueued since startup.
    pub total_enqueued: u64,
    /// Total chunks dequeued (assigned to nodes).
    pub total_dequeued: u64,
    /// Total chunks completed successfully.
    pub total_completed: u64,
    /// Total chunks that failed execution.
    pub total_failed: u64,
    /// Total chunks reassigned after failure or timeout.
    pub total_reassigned: u64,
    /// Total chunks cancelled via cancel_job.
    pub total_cancelled: u64,
    /// Total assignments that timed out.
    pub total_timeouts: u64,
    /// Economy chunks deferred because it was not off-peak.
    pub economy_deferred: u64,
    /// Average wait time in milliseconds (exponential moving average).
    pub avg_wait_time_ms: f64,
}

impl SchedulerStats {
    /// Update the moving average wait time with a new sample.
    fn record_wait_time(&mut self, wait_ms: f64) {
        const ALPHA: f64 = 0.1;
        if self.total_dequeued <= 1 {
            self.avg_wait_time_ms = wait_ms;
        } else {
            self.avg_wait_time_ms = self.avg_wait_time_ms * (1.0 - ALPHA) + wait_ms * ALPHA;
        }
    }

    /// Success rate as a fraction (0.0 - 1.0). Returns 1.0 if no completions.
    pub fn success_rate(&self) -> f64 {
        let total = self.total_completed + self.total_failed;
        if total == 0 {
            return 1.0;
        }
        self.total_completed as f64 / total as f64
    }

    /// Throughput: completions per second over the given duration.
    pub fn throughput(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.total_completed as f64 / elapsed_secs
    }
}

// ============================================================================
// NodeClassMatcher
// ============================================================================

/// Determines whether a node can handle a chunk based on the class hierarchy.
///
/// The hierarchy is: `Edge < Light < Standard < Enterprise`. A node of class X
/// can handle any chunk whose `min_class` is <= X.
pub struct NodeClassMatcher;

impl NodeClassMatcher {
    /// Check if a node of `node_class` can handle work requiring `required_class`.
    pub fn can_handle(node_class: NodeClass, required_class: NodeClass) -> bool {
        Self::class_rank(node_class) >= Self::class_rank(required_class)
    }

    /// Find the best (lowest sufficient) class from `available` that can handle `required`.
    ///
    /// Returns the class that meets the requirement with the least excess capacity,
    /// to avoid wasting powerful nodes on trivial work.
    pub fn best_match(available: &[NodeClass], required: NodeClass) -> Option<NodeClass> {
        let required_rank = Self::class_rank(required);
        let mut candidates: Vec<NodeClass> = available
            .iter()
            .copied()
            .filter(|c| Self::class_rank(*c) >= required_rank)
            .collect();
        candidates.sort_by_key(|c| Self::class_rank(*c));
        candidates.first().copied()
    }

    /// The full class hierarchy in ascending order.
    pub fn class_hierarchy() -> Vec<NodeClass> {
        vec![
            NodeClass::Edge,
            NodeClass::Light,
            NodeClass::Standard,
            NodeClass::Enterprise,
        ]
    }

    /// Numeric rank for a class (higher = more powerful).
    pub fn class_rank(class: NodeClass) -> u32 {
        match class {
            NodeClass::Edge => 0,
            NodeClass::Light => 1,
            NodeClass::Standard => 2,
            NodeClass::Enterprise => 3,
        }
    }

    /// Whether `a` is strictly more powerful than `b`.
    pub fn is_strictly_higher(a: NodeClass, b: NodeClass) -> bool {
        Self::class_rank(a) > Self::class_rank(b)
    }

    /// Number of class levels between two classes (absolute distance).
    pub fn class_distance(a: NodeClass, b: NodeClass) -> u32 {
        let ra = Self::class_rank(a);
        let rb = Self::class_rank(b);
        ra.abs_diff(rb)
    }

    /// Memory capacity check: can this node class handle the given memory requirement?
    pub fn can_handle_memory(node_class: NodeClass, required_mb: u64) -> bool {
        let constraints = TaskConstraints::for_class(node_class);
        constraints.max_memory_mb >= required_mb
    }

    /// Duration check: can this node class handle the given duration?
    pub fn can_handle_duration(node_class: NodeClass, required_secs: u64) -> bool {
        let constraints = TaskConstraints::for_class(node_class);
        constraints.max_duration_secs >= required_secs
    }

    /// Full eligibility check: class, memory, and duration.
    pub fn is_eligible(
        node_class: NodeClass,
        required_class: NodeClass,
        required_memory_mb: u64,
        required_duration_secs: u64,
    ) -> bool {
        Self::can_handle(node_class, required_class)
            && Self::can_handle_memory(node_class, required_memory_mb)
            && Self::can_handle_duration(node_class, required_duration_secs)
    }
}

// ============================================================================
// AssignmentTracker
// ============================================================================

/// Utility for detecting timed-out assignments.
pub struct AssignmentTracker;

impl AssignmentTracker {
    /// Scan all active assignments and return chunk IDs that have exceeded
    /// their deadline.
    pub fn check_timeouts(assignments: &DashMap<ChunkId, SchedulerAssignment>) -> Vec<ChunkId> {
        let now = Utc::now();
        assignments
            .iter()
            .filter(|entry| now > entry.value().deadline)
            .map(|entry| *entry.key())
            .collect()
    }

    /// Check if a single assignment is overdue.
    pub fn is_overdue(assignment: &SchedulerAssignment) -> bool {
        assignment.is_overdue()
    }

    /// Return all assignments for a given node.
    pub fn assignments_for_node(
        assignments: &DashMap<ChunkId, SchedulerAssignment>,
        node_id: &NodeId,
    ) -> Vec<SchedulerAssignment> {
        assignments
            .iter()
            .filter(|entry| entry.value().node_id == *node_id)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Return all assignments for a given job.
    pub fn assignments_for_job(
        assignments: &DashMap<ChunkId, SchedulerAssignment>,
        job_id: &JobId,
    ) -> Vec<SchedulerAssignment> {
        assignments
            .iter()
            .filter(|entry| entry.value().job_id == *job_id)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Count assignments by priority level.
    pub fn count_by_priority(
        assignments: &DashMap<ChunkId, SchedulerAssignment>,
    ) -> HashMap<PriorityLevel, usize> {
        let mut counts = HashMap::new();
        for entry in assignments.iter() {
            *counts.entry(entry.value().priority).or_insert(0) += 1;
        }
        counts
    }
}

// ============================================================================
// FairnessTracker
// ============================================================================

/// Tracks per-job allocation counts to ensure fair distribution.
///
/// When multiple jobs compete at the same priority level, the fairness
/// tracker biases dequeue toward the job with the fewest allocations,
/// preventing starvation.
#[derive(Debug, Clone, Default)]
pub struct FairnessTracker {
    /// Number of chunks allocated to each job.
    pub job_allocations: HashMap<JobId, u64>,
}

impl FairnessTracker {
    /// Create a new, empty fairness tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a chunk was allocated for the given job.
    pub fn record_allocation(&mut self, job_id: &JobId) {
        *self.job_allocations.entry(*job_id).or_insert(0) += 1;
    }

    /// Among the candidate jobs, suggest which one should be served next.
    ///
    /// Picks the job with the fewest allocations. Ties are broken by priority
    /// (higher priority wins), then by JobId for determinism.
    pub fn suggested_next_job(
        &self,
        candidates: &[(JobId, PriorityLevel)],
    ) -> Option<JobId> {
        if candidates.is_empty() {
            return None;
        }

        let mut best: Option<(JobId, u64, u32)> = None;

        for &(job_id, priority) in candidates {
            let allocs = self.job_allocations.get(&job_id).copied().unwrap_or(0);
            let weight = priority.queue_weight();

            let is_better = match best {
                None => true,
                Some((_, best_allocs, best_weight)) => {
                    if allocs < best_allocs {
                        true
                    } else if allocs == best_allocs {
                        weight > best_weight
                    } else {
                        false
                    }
                }
            };

            if is_better {
                best = Some((job_id, allocs, weight));
            }
        }

        best.map(|(id, _, _)| id)
    }

    /// Reset all allocation counters.
    pub fn reset(&mut self) {
        self.job_allocations.clear();
    }

    /// Remove tracking data for a specific job.
    pub fn remove_job(&mut self, job_id: &JobId) {
        self.job_allocations.remove(job_id);
    }

    /// Get the allocation count for a specific job.
    pub fn allocations_for(&self, job_id: &JobId) -> u64 {
        self.job_allocations.get(job_id).copied().unwrap_or(0)
    }

    /// Total allocations across all jobs.
    pub fn total_allocations(&self) -> u64 {
        self.job_allocations.values().sum()
    }
}

// ============================================================================
// LocalityAwareAssigner
// ============================================================================

/// Scores nodes based on data locality to prefer nodes that already
/// have relevant data cached.
pub struct LocalityAwareAssigner;

impl LocalityAwareAssigner {
    /// Score a node for a given chunk based on data locality.
    ///
    /// Returns a score between 0.0 (no locality benefit) and 1.0 (perfect match).
    /// The `cache_map` maps node IDs to lists of cached data identifiers.
    pub fn score_node(
        node_id: &NodeId,
        chunk: &SchedulableChunk,
        cache_map: &HashMap<NodeId, Vec<String>>,
    ) -> f64 {
        // If the chunk has a data locality hint and it matches, full score.
        if let Some(ref preferred) = chunk.data_locality {
            if node_id == preferred {
                return 1.0;
            }
        }

        // Check if the node has cached data relevant to this chunk's job.
        let job_key = format!("job-{}", chunk.job_id);
        if let Some(cached) = cache_map.get(node_id) {
            if cached.iter().any(|s| s.contains(&job_key)) {
                return 0.7;
            }
            // Partial cache presence.
            if !cached.is_empty() {
                return 0.2;
            }
        }

        0.0
    }

    /// Among candidates, pick the best node considering both class fitness
    /// and data locality.
    ///
    /// Returns `None` if no candidate meets the chunk's minimum class.
    pub fn best_node(
        candidates: &[(NodeId, NodeClass)],
        chunk: &SchedulableChunk,
    ) -> Option<NodeId> {
        let mut best: Option<(NodeId, u32, bool)> = None;

        for &(node_id, node_class) in candidates {
            if !NodeClassMatcher::can_handle(node_class, chunk.min_class) {
                continue;
            }

            let rank = NodeClassMatcher::class_rank(node_class);
            let is_local = chunk.data_locality.as_ref() == Some(&node_id);

            let is_better = match best {
                None => true,
                Some((_, best_rank, best_local)) => {
                    // Locality-preferred node wins outright.
                    if is_local && !best_local {
                        true
                    } else if !is_local && best_local {
                        false
                    } else {
                        // Among equal locality, prefer the lowest sufficient class
                        // (to avoid wasting powerful nodes).
                        rank < best_rank
                    }
                }
            };

            if is_better {
                best = Some((node_id, rank, is_local));
            }
        }

        best.map(|(id, _, _)| id)
    }

    /// Score all candidates and return them sorted by score (descending).
    pub fn rank_candidates(
        candidates: &[(NodeId, NodeClass)],
        chunk: &SchedulableChunk,
        cache_map: &HashMap<NodeId, Vec<String>>,
    ) -> Vec<(NodeId, f64)> {
        let mut scored: Vec<(NodeId, f64)> = candidates
            .iter()
            .filter(|(_, nc)| NodeClassMatcher::can_handle(*nc, chunk.min_class))
            .map(|(nid, _)| {
                let score = Self::score_node(nid, chunk, cache_map);
                (*nid, score)
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        scored
    }
}

// ============================================================================
// PriorityQueue (inner helper)
// ============================================================================

/// A priority queue backed by a BinaryHeap, with job-level pause support.
#[derive(Debug)]
struct PriorityQueue {
    heap: BinaryHeap<SchedulableChunk>,
    paused_jobs: std::collections::HashSet<JobId>,
}

impl PriorityQueue {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            paused_jobs: std::collections::HashSet::new(),
        }
    }

    fn push(&mut self, chunk: SchedulableChunk) {
        self.heap.push(chunk);
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Pop the highest-priority chunk that matches the node's class
    /// and is not from a paused job.
    ///
    /// Chunks that don't match are temporarily held aside and re-inserted.
    fn pop_for_node(&mut self, node_class: NodeClass) -> Option<SchedulableChunk> {
        let mut skipped = Vec::new();
        let mut result = None;

        while let Some(chunk) = self.heap.pop() {
            if self.paused_jobs.contains(&chunk.job_id) {
                skipped.push(chunk);
                continue;
            }
            if NodeClassMatcher::can_handle(node_class, chunk.min_class) {
                result = Some(chunk);
                break;
            }
            skipped.push(chunk);
        }

        // Re-insert skipped chunks.
        for chunk in skipped {
            self.heap.push(chunk);
        }

        result
    }

    /// Remove all chunks belonging to a specific job. Returns count removed.
    fn remove_job(&mut self, job_id: &JobId) -> u32 {
        let before = self.heap.len();
        let remaining: Vec<SchedulableChunk> = self
            .heap
            .drain()
            .filter(|c| c.job_id != *job_id)
            .collect();
        self.heap = BinaryHeap::from(remaining);
        (before - self.heap.len()) as u32
    }

    /// Pause a job (its chunks stay in the queue but won't be dequeued).
    fn pause_job(&mut self, job_id: &JobId) {
        self.paused_jobs.insert(*job_id);
    }

    /// Resume a paused job.
    fn resume_job(&mut self, job_id: &JobId) {
        self.paused_jobs.remove(job_id);
    }

    /// Check if a job is paused.
    fn is_job_paused(&self, job_id: &JobId) -> bool {
        self.paused_jobs.contains(job_id)
    }

    /// Collect all distinct (job_id, priority) pairs in this queue.
    fn job_summaries(&self) -> Vec<(JobId, PriorityLevel, usize)> {
        let mut map: HashMap<JobId, (PriorityLevel, usize)> = HashMap::new();
        for chunk in self.heap.iter() {
            let entry = map.entry(chunk.job_id).or_insert((chunk.priority, 0));
            entry.1 += 1;
        }
        map.into_iter()
            .map(|(jid, (prio, count))| (jid, prio, count))
            .collect()
    }

    /// Drain all chunks (consuming the queue).
    fn drain_all(&mut self) -> Vec<SchedulableChunk> {
        self.heap.drain().collect()
    }

    /// Peek at the highest-priority chunk without removing it.
    fn peek(&self) -> Option<&SchedulableChunk> {
        self.heap.peek()
    }
}

// ============================================================================
// JobScheduler
// ============================================================================

/// The main scheduler managing multiple priority queues.
///
/// Thread-safe: the inner queues are protected by a `RwLock`, and active
/// assignments use a `DashMap` for concurrent access.
pub struct JobScheduler {
    /// Queue for Rush-priority chunks.
    rush_queue: RwLock<PriorityQueue>,
    /// Queue for Standard-priority chunks.
    standard_queue: RwLock<PriorityQueue>,
    /// Queue for Economy-priority chunks.
    economy_queue: RwLock<PriorityQueue>,
    /// Active chunk-to-node assignments.
    active_assignments: DashMap<ChunkId, SchedulerAssignment>,
    /// Optional knowledge store for swarm awareness.
    knowledge: Option<Arc<KnowledgeStore>>,
    /// Runtime configuration.
    config: SchedulerConfig,
    /// Aggregate statistics.
    stats: RwLock<SchedulerStats>,
    /// Whether the scheduler is globally paused.
    paused: std::sync::atomic::AtomicBool,
    /// Fairness tracker for inter-job fairness.
    fairness: RwLock<FairnessTracker>,
}

impl JobScheduler {
    /// Create a new scheduler with default configuration.
    pub fn new() -> Self {
        Self {
            rush_queue: RwLock::new(PriorityQueue::new()),
            standard_queue: RwLock::new(PriorityQueue::new()),
            economy_queue: RwLock::new(PriorityQueue::new()),
            active_assignments: DashMap::new(),
            knowledge: None,
            config: SchedulerConfig::default(),
            stats: RwLock::new(SchedulerStats::default()),
            paused: std::sync::atomic::AtomicBool::new(false),
            fairness: RwLock::new(FairnessTracker::new()),
        }
    }

    /// Builder: attach a knowledge store.
    pub fn with_knowledge(mut self, knowledge: Arc<KnowledgeStore>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// Builder: set custom configuration.
    pub fn with_config(mut self, config: SchedulerConfig) -> Self {
        self.config = config;
        self
    }

    // ====================================================================
    // Enqueue / Dequeue
    // ====================================================================

    /// Add a job's chunks to the appropriate priority queue.
    ///
    /// All chunks inherit the job's priority level. Returns the number of
    /// chunks actually enqueued (may be less than `chunks.len()` if the
    /// queue depth limit is reached).
    pub fn enqueue_job(
        &self,
        job_id: JobId,
        priority: PriorityLevel,
        chunks: Vec<SchedulableChunk>,
    ) -> usize {
        if self.is_paused() {
            warn!(job = %job_id, "scheduler is paused, rejecting enqueue");
            return 0;
        }

        let current_depth = self.queue_depth().total;
        let available = if current_depth >= self.config.max_queue_depth {
            warn!(
                job = %job_id,
                depth = current_depth,
                max = self.config.max_queue_depth,
                "queue depth limit reached, rejecting enqueue"
            );
            return 0;
        } else {
            self.config.max_queue_depth - current_depth
        };

        let to_enqueue = chunks.len().min(available);
        let mut stats = self.stats.write();

        match priority {
            PriorityLevel::Rush => {
                let mut queue = self.rush_queue.write();
                for chunk in chunks.into_iter().take(to_enqueue) {
                    queue.push(chunk);
                    stats.total_enqueued += 1;
                }
            }
            PriorityLevel::Standard => {
                let mut queue = self.standard_queue.write();
                for chunk in chunks.into_iter().take(to_enqueue) {
                    queue.push(chunk);
                    stats.total_enqueued += 1;
                }
            }
            PriorityLevel::Economy => {
                let mut queue = self.economy_queue.write();
                for chunk in chunks.into_iter().take(to_enqueue) {
                    queue.push(chunk);
                    stats.total_enqueued += 1;
                }
            }
        }

        debug!(
            job = %job_id,
            priority = %priority,
            enqueued = to_enqueue,
            "scheduler: job enqueued"
        );

        to_enqueue
    }

    /// Dequeue the next chunk suitable for the given node.
    ///
    /// Priority order: Rush > Standard > Economy (if off-peak).
    /// Within each queue, higher-priority and earlier-enqueued chunks
    /// are served first. Economy chunks are only served during off-peak
    /// hours.
    pub fn dequeue_for_node(
        &self,
        node_id: NodeId,
        node_class: NodeClass,
    ) -> Option<SchedulableChunk> {
        if self.is_paused() {
            return None;
        }

        // Try Rush queue first.
        {
            let mut queue = self.rush_queue.write();
            if let Some(chunk) = queue.pop_for_node(node_class) {
                self.record_dequeue(&chunk, node_id, node_class);
                return Some(chunk);
            }
        }

        // Try Standard queue.
        {
            let mut queue = self.standard_queue.write();
            if let Some(chunk) = queue.pop_for_node(node_class) {
                self.record_dequeue(&chunk, node_id, node_class);
                return Some(chunk);
            }
        }

        // Try Economy queue (only during off-peak hours).
        if self.is_economy_allowed() {
            let mut queue = self.economy_queue.write();
            if let Some(chunk) = queue.pop_for_node(node_class) {
                self.record_dequeue(&chunk, node_id, node_class);
                return Some(chunk);
            }
        } else {
            let economy_len = self.economy_queue.read().len();
            if economy_len > 0 {
                let mut stats = self.stats.write();
                stats.economy_deferred += 1;
                debug!(
                    pending_economy = economy_len,
                    "scheduler: economy chunks deferred (not off-peak)"
                );
            }
        }

        None
    }

    /// Dequeue up to `max` chunks suitable for the given node.
    pub fn dequeue_batch(
        &self,
        node_id: NodeId,
        node_class: NodeClass,
        max: usize,
    ) -> Vec<SchedulableChunk> {
        let mut batch = Vec::with_capacity(max);
        for _ in 0..max {
            match self.dequeue_for_node(node_id, node_class) {
                Some(chunk) => batch.push(chunk),
                None => break,
            }
        }
        batch
    }

    /// Record a successful dequeue: create an assignment and update stats.
    fn record_dequeue(
        &self,
        chunk: &SchedulableChunk,
        node_id: NodeId,
        node_class: NodeClass,
    ) {
        let now = Utc::now();
        let deadline = now
            + chrono::Duration::seconds(self.config.assignment_timeout_secs as i64);

        let assignment = SchedulerAssignment {
            chunk_id: chunk.chunk_id,
            job_id: chunk.job_id,
            node_id,
            node_class,
            assigned_at: now,
            deadline,
            priority: chunk.priority,
        };

        self.active_assignments
            .insert(chunk.chunk_id, assignment);

        let mut stats = self.stats.write();
        stats.total_dequeued += 1;
        let wait_ms = chunk.wait_duration_ms() as f64;
        stats.record_wait_time(wait_ms);

        let mut fairness = self.fairness.write();
        fairness.record_allocation(&chunk.job_id);

        debug!(
            chunk = %chunk.chunk_id,
            job = %chunk.job_id,
            node = %node_id,
            class = %node_class,
            wait_ms = wait_ms as u64,
            "scheduler: chunk dequeued"
        );
    }

    // ====================================================================
    // Completion / Failure
    // ====================================================================

    /// Mark a chunk as completed.
    pub fn complete_chunk(&self, chunk_id: &ChunkId, success: bool) {
        if let Some((_, assignment)) = self.active_assignments.remove(chunk_id) {
            let mut stats = self.stats.write();
            if success {
                stats.total_completed += 1;
                debug!(
                    chunk = %chunk_id,
                    job = %assignment.job_id,
                    node = %assignment.node_id,
                    "scheduler: chunk completed"
                );
            } else {
                stats.total_failed += 1;
                warn!(
                    chunk = %chunk_id,
                    job = %assignment.job_id,
                    node = %assignment.node_id,
                    "scheduler: chunk failed"
                );
            }
        } else {
            debug!(
                chunk = %chunk_id,
                "scheduler: complete_chunk called for unknown assignment"
            );
        }
    }

    /// Reassign a failed or timed-out chunk back into its queue.
    ///
    /// Increments the attempt counter. If max attempts are reached, the
    /// chunk is dropped and counted as permanently failed.
    pub fn reassign_chunk(&self, chunk_id: &ChunkId) {
        let assignment = self.active_assignments.remove(chunk_id);
        let (_, assignment) = match assignment {
            Some(a) => a,
            None => {
                debug!(chunk = %chunk_id, "scheduler: reassign called for unknown assignment");
                return;
            }
        };

        // Build a new SchedulableChunk for re-enqueue.
        let mut chunk = SchedulableChunk::new(
            assignment.chunk_id,
            assignment.job_id,
            assignment.priority,
            NodeClass::Edge, // Will be overridden below or by the caller.
        );
        chunk.attempt += 1;
        chunk.enqueued_at = Utc::now();
        chunk.max_attempts = self.config.max_attempts_per_chunk;

        // Check if exhausted.
        if chunk.attempt >= chunk.max_attempts {
            let mut stats = self.stats.write();
            stats.total_failed += 1;
            error!(
                chunk = %chunk_id,
                job = %assignment.job_id,
                attempts = chunk.attempt,
                "scheduler: chunk exhausted max attempts, permanently failed"
            );
            return;
        }

        let mut stats = self.stats.write();
        stats.total_reassigned += 1;

        info!(
            chunk = %chunk_id,
            job = %assignment.job_id,
            attempt = chunk.attempt,
            "scheduler: chunk reassigned"
        );

        // Re-enqueue into the appropriate queue.
        drop(stats);
        match chunk.priority {
            PriorityLevel::Rush => self.rush_queue.write().push(chunk),
            PriorityLevel::Standard => self.standard_queue.write().push(chunk),
            PriorityLevel::Economy => self.economy_queue.write().push(chunk),
        }
    }

    // ====================================================================
    // Job-level operations
    // ====================================================================

    /// Cancel all pending chunks for a job. Returns the number of chunks cancelled.
    ///
    /// Active assignments are NOT cancelled (they will complete or time out).
    pub fn cancel_job(&self, job_id: &JobId) -> u32 {
        let mut total = 0u32;

        total += self.rush_queue.write().remove_job(job_id);
        total += self.standard_queue.write().remove_job(job_id);
        total += self.economy_queue.write().remove_job(job_id);

        if total > 0 {
            let mut stats = self.stats.write();
            stats.total_cancelled += total as u64;
            info!(
                job = %job_id,
                cancelled = total,
                "scheduler: job cancelled"
            );
        }

        // Clean up fairness tracker.
        self.fairness.write().remove_job(job_id);

        total
    }

    /// Pause a job: its chunks remain queued but won't be dequeued.
    pub fn pause_job(&self, job_id: &JobId) {
        self.rush_queue.write().pause_job(job_id);
        self.standard_queue.write().pause_job(job_id);
        self.economy_queue.write().pause_job(job_id);
        debug!(job = %job_id, "scheduler: job paused");
    }

    /// Resume a previously paused job.
    pub fn resume_job(&self, job_id: &JobId) {
        self.rush_queue.write().resume_job(job_id);
        self.standard_queue.write().resume_job(job_id);
        self.economy_queue.write().resume_job(job_id);
        debug!(job = %job_id, "scheduler: job resumed");
    }

    // ====================================================================
    // Query methods
    // ====================================================================

    /// Snapshot of current queue depths.
    pub fn queue_depth(&self) -> QueueDepth {
        let rush = self.rush_queue.read().len();
        let standard = self.standard_queue.read().len();
        let economy = self.economy_queue.read().len();
        QueueDepth {
            rush,
            standard,
            economy,
            total: rush + standard + economy,
            active_assignments: self.active_assignments.len(),
        }
    }

    /// Queue depth for a specific priority level.
    pub fn queue_depth_for_priority(&self, priority: PriorityLevel) -> usize {
        match priority {
            PriorityLevel::Rush => self.rush_queue.read().len(),
            PriorityLevel::Standard => self.standard_queue.read().len(),
            PriorityLevel::Economy => self.economy_queue.read().len(),
        }
    }

    /// Whether the current time falls within the off-peak window for Economy jobs.
    pub fn is_economy_allowed(&self) -> bool {
        Self::is_offpeak_hour(
            Utc::now().hour(),
            self.config.offpeak_start_hour,
            self.config.offpeak_end_hour,
        )
    }

    /// Pure function: check if a given hour is within the off-peak window.
    pub fn is_offpeak_hour(hour: u32, start: u32, end: u32) -> bool {
        if start <= end {
            // Window does not cross midnight (e.g., 02:00 - 06:00).
            hour >= start && hour < end
        } else {
            // Window crosses midnight (e.g., 22:00 - 06:00).
            hour >= start || hour < end
        }
    }

    /// Number of active (in-flight) assignments.
    pub fn active_assignments_count(&self) -> usize {
        self.active_assignments.len()
    }

    /// List all pending jobs with their priority and chunk count.
    pub fn pending_jobs(&self) -> Vec<(JobId, PriorityLevel, usize)> {
        let mut jobs = Vec::new();
        jobs.extend(self.rush_queue.read().job_summaries());
        jobs.extend(self.standard_queue.read().job_summaries());
        jobs.extend(self.economy_queue.read().job_summaries());
        jobs
    }

    /// Get a copy of the scheduler statistics.
    pub fn get_stats(&self) -> SchedulerStats {
        self.stats.read().clone()
    }

    /// Get the current configuration.
    pub fn get_config(&self) -> SchedulerConfig {
        self.config.clone()
    }

    /// Get a reference to the active assignments map.
    pub fn get_active_assignments(&self) -> &DashMap<ChunkId, SchedulerAssignment> {
        &self.active_assignments
    }

    /// Get the fairness tracker state.
    pub fn get_fairness(&self) -> FairnessTracker {
        self.fairness.read().clone()
    }

    // ====================================================================
    // Pause / Resume (global)
    // ====================================================================

    /// Globally pause the scheduler. No chunks will be dequeued.
    pub fn pause(&self) {
        self.paused
            .store(true, std::sync::atomic::Ordering::SeqCst);
        info!("scheduler: globally paused");
    }

    /// Resume the scheduler after a global pause.
    pub fn resume(&self) {
        self.paused
            .store(false, std::sync::atomic::Ordering::SeqCst);
        info!("scheduler: globally resumed");
    }

    /// Whether the scheduler is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(std::sync::atomic::Ordering::SeqCst)
    }

    // ====================================================================
    // Timeout sweep
    // ====================================================================

    /// Sweep active assignments for timeouts and reassign them.
    ///
    /// Returns the number of timed-out assignments that were reassigned.
    pub fn sweep_timeouts(&self) -> usize {
        let timed_out = AssignmentTracker::check_timeouts(&self.active_assignments);
        let count = timed_out.len();

        for chunk_id in &timed_out {
            self.stats.write().total_timeouts += 1;
            warn!(chunk = %chunk_id, "scheduler: assignment timed out, reassigning");
            self.reassign_chunk(chunk_id);
        }

        if count > 0 {
            info!(
                count = count,
                "scheduler: swept timed-out assignments"
            );
        }

        count
    }

    // ====================================================================
    // Scheduler loop
    // ====================================================================

    /// Spawn the background scheduler loop that sweeps for timeouts.
    ///
    /// The loop runs until the shutdown signal is received.
    pub fn spawn_scheduler_loop(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let tick = std::time::Duration::from_millis(self.config.tick_interval_ms);

        tokio::spawn(async move {
            info!(
                tick_ms = self.config.tick_interval_ms,
                "scheduler: loop started"
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tick) => {
                        if !self.is_paused() {
                            let timeouts = self.sweep_timeouts();
                            if timeouts > 0 {
                                debug!(timeouts = timeouts, "scheduler: tick processed timeouts");
                            }
                        }
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            info!("scheduler: shutting down");
                            break;
                        }
                    }
                }
            }
        });
    }

    // ====================================================================
    // Utility / introspection
    // ====================================================================

    /// Remove an active assignment without recording it as completed or failed.
    /// Used for manual intervention.
    pub fn remove_assignment(&self, chunk_id: &ChunkId) -> Option<SchedulerAssignment> {
        self.active_assignments.remove(chunk_id).map(|(_, a)| a)
    }

    /// Check if a specific chunk is currently assigned.
    pub fn is_chunk_assigned(&self, chunk_id: &ChunkId) -> bool {
        self.active_assignments.contains_key(chunk_id)
    }

    /// Get the assignment for a specific chunk.
    pub fn get_assignment(&self, chunk_id: &ChunkId) -> Option<SchedulerAssignment> {
        self.active_assignments
            .get(chunk_id)
            .map(|entry| entry.value().clone())
    }

    /// Get all assignments for a specific node.
    pub fn assignments_for_node(&self, node_id: &NodeId) -> Vec<SchedulerAssignment> {
        AssignmentTracker::assignments_for_node(&self.active_assignments, node_id)
    }

    /// Get all assignments for a specific job.
    pub fn assignments_for_job(&self, job_id: &JobId) -> Vec<SchedulerAssignment> {
        AssignmentTracker::assignments_for_job(&self.active_assignments, job_id)
    }

    /// Release all assignments held by a given node (e.g., because the node died).
    /// Reassigns them back to the queue.
    pub fn release_node_assignments(&self, node_id: &NodeId) -> usize {
        let assignments: Vec<ChunkId> = self
            .active_assignments
            .iter()
            .filter(|entry| entry.value().node_id == *node_id)
            .map(|entry| *entry.key())
            .collect();

        let count = assignments.len();
        for chunk_id in &assignments {
            self.reassign_chunk(chunk_id);
        }

        if count > 0 {
            info!(
                node = %node_id,
                released = count,
                "scheduler: released assignments from node"
            );
        }

        count
    }

    /// Drain all queues and cancel everything. Returns total chunks drained.
    pub fn drain_all(&self) -> usize {
        let mut total = 0;
        total += self.rush_queue.write().drain_all().len();
        total += self.standard_queue.write().drain_all().len();
        total += self.economy_queue.write().drain_all().len();

        let assignment_count = self.active_assignments.len();
        self.active_assignments.clear();
        total += assignment_count;

        if total > 0 {
            info!(total = total, "scheduler: drained all queues and assignments");
        }

        total
    }

    /// Reset all statistics.
    pub fn reset_stats(&self) {
        *self.stats.write() = SchedulerStats::default();
    }

    /// Reset the fairness tracker.
    pub fn reset_fairness(&self) {
        self.fairness.write().reset();
    }

    /// Whether a specific job is paused.
    pub fn is_job_paused(&self, job_id: &JobId) -> bool {
        self.rush_queue.read().is_job_paused(job_id)
            || self.standard_queue.read().is_job_paused(job_id)
            || self.economy_queue.read().is_job_paused(job_id)
    }

    /// Peek at the next chunk that would be dequeued for a given node class,
    /// without actually dequeuing it.
    pub fn peek_next(&self, node_class: NodeClass) -> Option<PriorityLevel> {
        // Check Rush.
        {
            let queue = self.rush_queue.read();
            if let Some(chunk) = queue.peek() {
                if NodeClassMatcher::can_handle(node_class, chunk.min_class) {
                    return Some(PriorityLevel::Rush);
                }
            }
        }
        // Check Standard.
        {
            let queue = self.standard_queue.read();
            if let Some(chunk) = queue.peek() {
                if NodeClassMatcher::can_handle(node_class, chunk.min_class) {
                    return Some(PriorityLevel::Standard);
                }
            }
        }
        // Check Economy.
        if self.is_economy_allowed() {
            let queue = self.economy_queue.read();
            if let Some(chunk) = queue.peek() {
                if NodeClassMatcher::can_handle(node_class, chunk.min_class) {
                    return Some(PriorityLevel::Economy);
                }
            }
        }
        None
    }

    /// Compute the estimated wait time for a new chunk at the given priority.
    /// Based on historical average and current queue depth.
    pub fn estimated_wait_ms(&self, priority: PriorityLevel) -> f64 {
        let stats = self.stats.read();
        let depth = self.queue_depth_for_priority(priority) as f64;
        if stats.total_dequeued == 0 {
            return 0.0;
        }
        // Rough estimate: avg_wait * (current_depth / average_depth_at_dequeue_time).
        // Simplified to: avg_wait * depth_factor.
        let depth_factor = (depth + 1.0).ln().max(1.0);
        stats.avg_wait_time_ms * depth_factor
    }
}

impl Default for JobScheduler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    // Helper: create a schedulable chunk with defaults.
    fn make_chunk(
        job_id: JobId,
        priority: PriorityLevel,
        min_class: NodeClass,
    ) -> SchedulableChunk {
        SchedulableChunk::new(ChunkId::new(), job_id, priority, min_class)
    }

    fn make_chunk_with_time(
        job_id: JobId,
        priority: PriorityLevel,
        min_class: NodeClass,
        offset_ms: i64,
    ) -> SchedulableChunk {
        let mut chunk = make_chunk(job_id, priority, min_class);
        chunk.enqueued_at = Utc::now() + ChronoDuration::milliseconds(offset_ms);
        chunk
    }

    // ================================================================
    // SchedulableChunk ordering tests
    // ================================================================

    #[test]
    fn chunk_ordering_rush_before_standard() {
        let job = JobId::new();
        let rush = make_chunk(job, PriorityLevel::Rush, NodeClass::Edge);
        let standard = make_chunk(job, PriorityLevel::Standard, NodeClass::Edge);
        assert!(rush > standard, "Rush should sort higher than Standard");
    }

    #[test]
    fn chunk_ordering_standard_before_economy() {
        let job = JobId::new();
        let standard = make_chunk(job, PriorityLevel::Standard, NodeClass::Edge);
        let economy = make_chunk(job, PriorityLevel::Economy, NodeClass::Edge);
        assert!(
            standard > economy,
            "Standard should sort higher than Economy"
        );
    }

    #[test]
    fn chunk_ordering_rush_before_economy() {
        let job = JobId::new();
        let rush = make_chunk(job, PriorityLevel::Rush, NodeClass::Edge);
        let economy = make_chunk(job, PriorityLevel::Economy, NodeClass::Edge);
        assert!(rush > economy, "Rush should sort higher than Economy");
    }

    #[test]
    fn chunk_ordering_fifo_within_same_priority() {
        let job = JobId::new();
        // Earlier chunk should sort higher (dequeued first).
        let earlier = make_chunk_with_time(job, PriorityLevel::Standard, NodeClass::Edge, -1000);
        let later = make_chunk_with_time(job, PriorityLevel::Standard, NodeClass::Edge, 0);
        assert!(
            earlier > later,
            "Earlier enqueued chunk should sort higher within same priority"
        );
    }

    #[test]
    fn chunk_ordering_priority_beats_fifo() {
        let job = JobId::new();
        // Rush chunk enqueued later should still beat Standard enqueued earlier.
        let standard_early =
            make_chunk_with_time(job, PriorityLevel::Standard, NodeClass::Edge, -5000);
        let rush_late = make_chunk_with_time(job, PriorityLevel::Rush, NodeClass::Edge, 0);
        assert!(
            rush_late > standard_early,
            "Rush should beat Standard even if enqueued later"
        );
    }

    #[test]
    fn chunk_is_exhausted() {
        let mut chunk = make_chunk(JobId::new(), PriorityLevel::Standard, NodeClass::Edge);
        chunk.max_attempts = 3;
        chunk.attempt = 2;
        assert!(!chunk.is_exhausted());
        chunk.attempt = 3;
        assert!(chunk.is_exhausted());
    }

    #[test]
    fn chunk_builder_methods() {
        let chunk = SchedulableChunk::new(
            ChunkId::new(),
            JobId::new(),
            PriorityLevel::Rush,
            NodeClass::Standard,
        )
        .with_memory_mb(4096)
        .with_duration_secs(3600)
        .with_max_attempts(5);

        assert_eq!(chunk.memory_mb, 4096);
        assert_eq!(chunk.duration_secs, 3600);
        assert_eq!(chunk.max_attempts, 5);
        assert_eq!(chunk.priority, PriorityLevel::Rush);
        assert_eq!(chunk.min_class, NodeClass::Standard);
    }

    // ================================================================
    // NodeClassMatcher tests
    // ================================================================

    #[test]
    fn class_matcher_boulder_handles_all() {
        assert!(NodeClassMatcher::can_handle(NodeClass::Enterprise, NodeClass::Edge));
        assert!(NodeClassMatcher::can_handle(NodeClass::Enterprise, NodeClass::Light));
        assert!(NodeClassMatcher::can_handle(NodeClass::Enterprise, NodeClass::Standard));
        assert!(NodeClassMatcher::can_handle(NodeClass::Enterprise, NodeClass::Enterprise));
    }

    #[test]
    fn class_matcher_dust_handles_only_dust() {
        assert!(NodeClassMatcher::can_handle(NodeClass::Edge, NodeClass::Edge));
        assert!(!NodeClassMatcher::can_handle(NodeClass::Edge, NodeClass::Light));
        assert!(!NodeClassMatcher::can_handle(NodeClass::Edge, NodeClass::Standard));
        assert!(!NodeClassMatcher::can_handle(NodeClass::Edge, NodeClass::Enterprise));
    }

    #[test]
    fn class_matcher_pebble_handles_dust_and_pebble() {
        assert!(NodeClassMatcher::can_handle(NodeClass::Light, NodeClass::Edge));
        assert!(NodeClassMatcher::can_handle(NodeClass::Light, NodeClass::Light));
        assert!(!NodeClassMatcher::can_handle(NodeClass::Light, NodeClass::Standard));
    }

    #[test]
    fn class_matcher_rock_handles_up_to_rock() {
        assert!(NodeClassMatcher::can_handle(NodeClass::Standard, NodeClass::Edge));
        assert!(NodeClassMatcher::can_handle(NodeClass::Standard, NodeClass::Light));
        assert!(NodeClassMatcher::can_handle(NodeClass::Standard, NodeClass::Standard));
        assert!(!NodeClassMatcher::can_handle(NodeClass::Standard, NodeClass::Enterprise));
    }

    #[test]
    fn class_matcher_best_match_picks_lowest_sufficient() {
        let available = vec![NodeClass::Enterprise, NodeClass::Light, NodeClass::Standard];
        assert_eq!(
            NodeClassMatcher::best_match(&available, NodeClass::Light),
            Some(NodeClass::Light)
        );
        assert_eq!(
            NodeClassMatcher::best_match(&available, NodeClass::Standard),
            Some(NodeClass::Standard)
        );
        assert_eq!(
            NodeClassMatcher::best_match(&available, NodeClass::Edge),
            Some(NodeClass::Light) // lowest available
        );
    }

    #[test]
    fn class_matcher_best_match_none_if_insufficient() {
        let available = vec![NodeClass::Edge, NodeClass::Light];
        assert_eq!(
            NodeClassMatcher::best_match(&available, NodeClass::Enterprise),
            None
        );
    }

    #[test]
    fn class_matcher_hierarchy() {
        let h = NodeClassMatcher::class_hierarchy();
        assert_eq!(h, vec![NodeClass::Edge, NodeClass::Light, NodeClass::Standard, NodeClass::Enterprise]);
    }

    #[test]
    fn class_matcher_distance() {
        assert_eq!(NodeClassMatcher::class_distance(NodeClass::Edge, NodeClass::Enterprise), 3);
        assert_eq!(NodeClassMatcher::class_distance(NodeClass::Enterprise, NodeClass::Edge), 3);
        assert_eq!(NodeClassMatcher::class_distance(NodeClass::Standard, NodeClass::Light), 1);
        assert_eq!(NodeClassMatcher::class_distance(NodeClass::Edge, NodeClass::Edge), 0);
    }

    #[test]
    fn class_matcher_strictly_higher() {
        assert!(NodeClassMatcher::is_strictly_higher(NodeClass::Enterprise, NodeClass::Standard));
        assert!(!NodeClassMatcher::is_strictly_higher(NodeClass::Standard, NodeClass::Standard));
        assert!(!NodeClassMatcher::is_strictly_higher(NodeClass::Edge, NodeClass::Light));
    }

    #[test]
    fn class_matcher_is_eligible_checks_all() {
        // Enterprise with generous limits should handle anything.
        assert!(NodeClassMatcher::is_eligible(
            NodeClass::Enterprise,
            NodeClass::Standard,
            8192,
            3600,
        ));
        // Edge can't handle Standard-level work.
        assert!(!NodeClassMatcher::is_eligible(
            NodeClass::Edge,
            NodeClass::Standard,
            100,
            60,
        ));
        // Edge can handle Edge-level but not if memory is too high.
        assert!(!NodeClassMatcher::is_eligible(
            NodeClass::Edge,
            NodeClass::Edge,
            200, // Edge max is 100 MB
            30,
        ));
    }

    // ================================================================
    // JobScheduler: enqueue / dequeue tests
    // ================================================================

    #[test]
    fn enqueue_and_dequeue_basic() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        let chunks = vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)];
        let enqueued = scheduler.enqueue_job(job, PriorityLevel::Standard, chunks);
        assert_eq!(enqueued, 1);

        let depth = scheduler.queue_depth();
        assert_eq!(depth.standard, 1);
        assert_eq!(depth.total, 1);

        let dequeued = scheduler.dequeue_for_node(node, NodeClass::Standard);
        assert!(dequeued.is_some());
        let chunk = dequeued.unwrap();
        assert_eq!(chunk.job_id, job);

        let depth = scheduler.queue_depth();
        assert_eq!(depth.total, 0);
        assert_eq!(depth.active_assignments, 1);
    }

    #[test]
    fn rush_dequeued_before_standard() {
        let scheduler = JobScheduler::new();
        let node = NodeId::new();

        let std_job = JobId::new();
        let rush_job = JobId::new();

        // Enqueue standard first, then rush.
        scheduler.enqueue_job(
            std_job,
            PriorityLevel::Standard,
            vec![make_chunk(std_job, PriorityLevel::Standard, NodeClass::Edge)],
        );
        scheduler.enqueue_job(
            rush_job,
            PriorityLevel::Rush,
            vec![make_chunk(rush_job, PriorityLevel::Rush, NodeClass::Edge)],
        );

        // Dequeue should return Rush first.
        let first = scheduler.dequeue_for_node(node, NodeClass::Enterprise).unwrap();
        assert_eq!(first.priority, PriorityLevel::Rush);
        assert_eq!(first.job_id, rush_job);

        let second = scheduler.dequeue_for_node(node, NodeClass::Enterprise).unwrap();
        assert_eq!(second.priority, PriorityLevel::Standard);
        assert_eq!(second.job_id, std_job);
    }

    #[test]
    fn node_class_filtering_on_dequeue() {
        let scheduler = JobScheduler::new();
        let node = NodeId::new();
        let job = JobId::new();

        // Enqueue a chunk requiring Standard.
        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Standard)],
        );

        // Light node can't handle Standard work.
        let result = scheduler.dequeue_for_node(node, NodeClass::Light);
        assert!(result.is_none());

        // Standard node can handle it.
        let result = scheduler.dequeue_for_node(node, NodeClass::Standard);
        assert!(result.is_some());
    }

    #[test]
    fn dequeue_batch_respects_max() {
        let scheduler = JobScheduler::new();
        let node = NodeId::new();
        let job = JobId::new();

        let chunks: Vec<SchedulableChunk> = (0..10)
            .map(|_| make_chunk(job, PriorityLevel::Standard, NodeClass::Edge))
            .collect();
        scheduler.enqueue_job(job, PriorityLevel::Standard, chunks);

        let batch = scheduler.dequeue_batch(node, NodeClass::Enterprise, 3);
        assert_eq!(batch.len(), 3);
        assert_eq!(scheduler.queue_depth().standard, 7);
    }

    #[test]
    fn dequeue_batch_returns_fewer_when_queue_small() {
        let scheduler = JobScheduler::new();
        let node = NodeId::new();
        let job = JobId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );

        let batch = scheduler.dequeue_batch(node, NodeClass::Enterprise, 5);
        assert_eq!(batch.len(), 1);
    }

    // ================================================================
    // Cancel / Pause / Resume tests
    // ================================================================

    #[test]
    fn cancel_job_removes_chunks() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();

        let chunks: Vec<SchedulableChunk> = (0..5)
            .map(|_| make_chunk(job, PriorityLevel::Standard, NodeClass::Edge))
            .collect();
        scheduler.enqueue_job(job, PriorityLevel::Standard, chunks);

        let cancelled = scheduler.cancel_job(&job);
        assert_eq!(cancelled, 5);
        assert_eq!(scheduler.queue_depth().total, 0);

        let stats = scheduler.get_stats();
        assert_eq!(stats.total_cancelled, 5);
    }

    #[test]
    fn cancel_does_not_affect_other_jobs() {
        let scheduler = JobScheduler::new();
        let job_a = JobId::new();
        let job_b = JobId::new();

        scheduler.enqueue_job(
            job_a,
            PriorityLevel::Standard,
            vec![make_chunk(job_a, PriorityLevel::Standard, NodeClass::Edge)],
        );
        scheduler.enqueue_job(
            job_b,
            PriorityLevel::Standard,
            vec![make_chunk(job_b, PriorityLevel::Standard, NodeClass::Edge)],
        );

        scheduler.cancel_job(&job_a);
        assert_eq!(scheduler.queue_depth().total, 1);

        // Should still be able to dequeue job_b.
        let dequeued = scheduler
            .dequeue_for_node(NodeId::new(), NodeClass::Enterprise)
            .unwrap();
        assert_eq!(dequeued.job_id, job_b);
    }

    #[test]
    fn pause_job_prevents_dequeue() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );

        scheduler.pause_job(&job);
        assert!(scheduler.is_job_paused(&job));

        // Should not dequeue paused job's chunks.
        let result = scheduler.dequeue_for_node(node, NodeClass::Enterprise);
        assert!(result.is_none());
    }

    #[test]
    fn resume_job_allows_dequeue() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );

        scheduler.pause_job(&job);
        scheduler.resume_job(&job);
        assert!(!scheduler.is_job_paused(&job));

        let result = scheduler.dequeue_for_node(node, NodeClass::Enterprise);
        assert!(result.is_some());
    }

    // ================================================================
    // Global pause / resume
    // ================================================================

    #[test]
    fn global_pause_prevents_dequeue() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );

        scheduler.pause();
        assert!(scheduler.is_paused());

        let result = scheduler.dequeue_for_node(NodeId::new(), NodeClass::Enterprise);
        assert!(result.is_none());
    }

    #[test]
    fn global_resume_allows_dequeue() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );

        scheduler.pause();
        scheduler.resume();
        assert!(!scheduler.is_paused());

        let result = scheduler.dequeue_for_node(NodeId::new(), NodeClass::Enterprise);
        assert!(result.is_some());
    }

    #[test]
    fn global_pause_prevents_enqueue() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();

        scheduler.pause();
        let enqueued = scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );
        assert_eq!(enqueued, 0);
    }

    // ================================================================
    // Economy scheduling / off-peak detection
    // ================================================================

    #[test]
    fn offpeak_crossing_midnight() {
        // 22:00 - 06:00 window.
        assert!(JobScheduler::is_offpeak_hour(22, 22, 6));
        assert!(JobScheduler::is_offpeak_hour(23, 22, 6));
        assert!(JobScheduler::is_offpeak_hour(0, 22, 6));
        assert!(JobScheduler::is_offpeak_hour(3, 22, 6));
        assert!(JobScheduler::is_offpeak_hour(5, 22, 6));
        assert!(!JobScheduler::is_offpeak_hour(6, 22, 6));
        assert!(!JobScheduler::is_offpeak_hour(12, 22, 6));
        assert!(!JobScheduler::is_offpeak_hour(21, 22, 6));
    }

    #[test]
    fn offpeak_not_crossing_midnight() {
        // 02:00 - 06:00 window.
        assert!(JobScheduler::is_offpeak_hour(2, 2, 6));
        assert!(JobScheduler::is_offpeak_hour(4, 2, 6));
        assert!(JobScheduler::is_offpeak_hour(5, 2, 6));
        assert!(!JobScheduler::is_offpeak_hour(6, 2, 6));
        assert!(!JobScheduler::is_offpeak_hour(1, 2, 6));
        assert!(!JobScheduler::is_offpeak_hour(12, 2, 6));
        assert!(!JobScheduler::is_offpeak_hour(22, 2, 6));
    }

    #[test]
    fn economy_deferred_stats() {
        // Create a scheduler with off-peak window that is definitely NOT now.
        // We pick a 1-hour window 12 hours from now.
        let current_hour = Utc::now().hour();
        let far_start = (current_hour + 12) % 24;
        let far_end = (far_start + 1) % 24;

        let config = SchedulerConfig {
            offpeak_start_hour: far_start,
            offpeak_end_hour: far_end,
            ..Default::default()
        };

        let scheduler = JobScheduler::new().with_config(config);
        let job = JobId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Economy,
            vec![make_chunk(job, PriorityLevel::Economy, NodeClass::Edge)],
        );

        // Economy should be deferred.
        let result = scheduler.dequeue_for_node(NodeId::new(), NodeClass::Enterprise);
        assert!(result.is_none());

        let stats = scheduler.get_stats();
        assert!(stats.economy_deferred > 0);
    }

    // ================================================================
    // Assignment timeout tests
    // ================================================================

    #[test]
    fn assignment_timeout_detection() {
        let assignments: DashMap<ChunkId, SchedulerAssignment> = DashMap::new();
        let chunk_id = ChunkId::new();

        let assignment = SchedulerAssignment {
            chunk_id,
            job_id: JobId::new(),
            node_id: NodeId::new(),
            node_class: NodeClass::Standard,
            assigned_at: Utc::now() - ChronoDuration::seconds(600),
            deadline: Utc::now() - ChronoDuration::seconds(300),
            priority: PriorityLevel::Standard,
        };
        assignments.insert(chunk_id, assignment);

        let timed_out = AssignmentTracker::check_timeouts(&assignments);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], chunk_id);
    }

    #[test]
    fn assignment_not_overdue_before_deadline() {
        let assignment = SchedulerAssignment {
            chunk_id: ChunkId::new(),
            job_id: JobId::new(),
            node_id: NodeId::new(),
            node_class: NodeClass::Standard,
            assigned_at: Utc::now(),
            deadline: Utc::now() + ChronoDuration::seconds(300),
            priority: PriorityLevel::Standard,
        };
        assert!(!AssignmentTracker::is_overdue(&assignment));
        assert!(!assignment.is_overdue());
    }

    #[test]
    fn assignment_overdue_after_deadline() {
        let assignment = SchedulerAssignment {
            chunk_id: ChunkId::new(),
            job_id: JobId::new(),
            node_id: NodeId::new(),
            node_class: NodeClass::Standard,
            assigned_at: Utc::now() - ChronoDuration::seconds(600),
            deadline: Utc::now() - ChronoDuration::seconds(1),
            priority: PriorityLevel::Standard,
        };
        assert!(AssignmentTracker::is_overdue(&assignment));
    }

    // ================================================================
    // FairnessTracker tests
    // ================================================================

    #[test]
    fn fairness_suggests_least_allocated_job() {
        let mut tracker = FairnessTracker::new();
        let job_a = JobId::new();
        let job_b = JobId::new();

        // Give job_a more allocations.
        tracker.record_allocation(&job_a);
        tracker.record_allocation(&job_a);
        tracker.record_allocation(&job_b);

        let candidates = vec![
            (job_a, PriorityLevel::Standard),
            (job_b, PriorityLevel::Standard),
        ];

        // Should suggest job_b (fewer allocations).
        let suggestion = tracker.suggested_next_job(&candidates);
        assert_eq!(suggestion, Some(job_b));
    }

    #[test]
    fn fairness_breaks_ties_by_priority() {
        let tracker = FairnessTracker::new();
        let rush_job = JobId::new();
        let std_job = JobId::new();

        // Both have zero allocations.
        let candidates = vec![
            (std_job, PriorityLevel::Standard),
            (rush_job, PriorityLevel::Rush),
        ];

        // Should suggest rush_job (higher priority as tiebreaker).
        let suggestion = tracker.suggested_next_job(&candidates);
        assert_eq!(suggestion, Some(rush_job));
    }

    #[test]
    fn fairness_reset_clears_all() {
        let mut tracker = FairnessTracker::new();
        let job = JobId::new();
        tracker.record_allocation(&job);
        assert_eq!(tracker.allocations_for(&job), 1);

        tracker.reset();
        assert_eq!(tracker.allocations_for(&job), 0);
        assert_eq!(tracker.total_allocations(), 0);
    }

    #[test]
    fn fairness_remove_job() {
        let mut tracker = FairnessTracker::new();
        let job_a = JobId::new();
        let job_b = JobId::new();
        tracker.record_allocation(&job_a);
        tracker.record_allocation(&job_b);

        tracker.remove_job(&job_a);
        assert_eq!(tracker.allocations_for(&job_a), 0);
        assert_eq!(tracker.allocations_for(&job_b), 1);
    }

    #[test]
    fn fairness_empty_candidates_returns_none() {
        let tracker = FairnessTracker::new();
        assert_eq!(tracker.suggested_next_job(&[]), None);
    }

    // ================================================================
    // QueueDepth tests
    // ================================================================

    #[test]
    fn queue_depth_correct_counts() {
        let scheduler = JobScheduler::new();

        let rush_job = JobId::new();
        let std_job = JobId::new();
        let eco_job = JobId::new();

        scheduler.enqueue_job(
            rush_job,
            PriorityLevel::Rush,
            (0..3)
                .map(|_| make_chunk(rush_job, PriorityLevel::Rush, NodeClass::Edge))
                .collect(),
        );
        scheduler.enqueue_job(
            std_job,
            PriorityLevel::Standard,
            (0..5)
                .map(|_| make_chunk(std_job, PriorityLevel::Standard, NodeClass::Edge))
                .collect(),
        );
        scheduler.enqueue_job(
            eco_job,
            PriorityLevel::Economy,
            (0..2)
                .map(|_| make_chunk(eco_job, PriorityLevel::Economy, NodeClass::Edge))
                .collect(),
        );

        let depth = scheduler.queue_depth();
        assert_eq!(depth.rush, 3);
        assert_eq!(depth.standard, 5);
        assert_eq!(depth.economy, 2);
        assert_eq!(depth.total, 10);
        assert_eq!(depth.active_assignments, 0);
    }

    #[test]
    fn queue_depth_after_dequeue() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            (0..3)
                .map(|_| make_chunk(job, PriorityLevel::Standard, NodeClass::Edge))
                .collect(),
        );

        scheduler.dequeue_for_node(node, NodeClass::Enterprise);

        let depth = scheduler.queue_depth();
        assert_eq!(depth.standard, 2);
        assert_eq!(depth.total, 2);
        assert_eq!(depth.active_assignments, 1);
    }

    #[test]
    fn queue_depth_is_empty() {
        let depth = QueueDepth::default();
        assert!(depth.is_empty());
    }

    #[test]
    fn queue_depth_utilization() {
        let depth = QueueDepth {
            rush: 50,
            standard: 30,
            economy: 20,
            total: 100,
            active_assignments: 10,
        };
        assert!((depth.utilization(1000) - 0.1).abs() < 0.001);
        assert!((depth.utilization(100) - 1.0).abs() < 0.001);
    }

    // ================================================================
    // Reassignment / failure tests
    // ================================================================

    #[test]
    fn reassign_chunk_puts_back_in_queue() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );

        let chunk = scheduler.dequeue_for_node(node, NodeClass::Enterprise).unwrap();
        assert_eq!(scheduler.queue_depth().total, 0);
        assert_eq!(scheduler.active_assignments_count(), 1);

        scheduler.reassign_chunk(&chunk.chunk_id);

        assert_eq!(scheduler.queue_depth().total, 1);
        assert_eq!(scheduler.active_assignments_count(), 0);

        let stats = scheduler.get_stats();
        assert_eq!(stats.total_reassigned, 1);
    }

    #[test]
    fn reassign_exhausted_chunk_is_dropped() {
        let config = SchedulerConfig {
            max_attempts_per_chunk: 1,
            ..Default::default()
        };
        let scheduler = JobScheduler::new().with_config(config);
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );

        let chunk = scheduler.dequeue_for_node(node, NodeClass::Enterprise).unwrap();
        scheduler.reassign_chunk(&chunk.chunk_id);

        // Chunk should NOT be back in queue (max_attempts = 1, attempt becomes 1).
        assert_eq!(scheduler.queue_depth().total, 0);
        let stats = scheduler.get_stats();
        assert_eq!(stats.total_failed, 1);
    }

    #[test]
    fn complete_chunk_updates_stats() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![
                make_chunk(job, PriorityLevel::Standard, NodeClass::Edge),
                make_chunk(job, PriorityLevel::Standard, NodeClass::Edge),
            ],
        );

        let c1 = scheduler.dequeue_for_node(node, NodeClass::Enterprise).unwrap();
        let c2 = scheduler.dequeue_for_node(node, NodeClass::Enterprise).unwrap();

        scheduler.complete_chunk(&c1.chunk_id, true);
        scheduler.complete_chunk(&c2.chunk_id, false);

        let stats = scheduler.get_stats();
        assert_eq!(stats.total_completed, 1);
        assert_eq!(stats.total_failed, 1);
        assert_eq!(scheduler.active_assignments_count(), 0);
    }

    // ================================================================
    // Release node assignments
    // ================================================================

    #[test]
    fn release_node_assignments_reassigns_all() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            (0..3)
                .map(|_| make_chunk(job, PriorityLevel::Standard, NodeClass::Edge))
                .collect(),
        );

        // Dequeue all 3 to same node.
        for _ in 0..3 {
            scheduler.dequeue_for_node(node, NodeClass::Enterprise);
        }
        assert_eq!(scheduler.active_assignments_count(), 3);

        let released = scheduler.release_node_assignments(&node);
        assert_eq!(released, 3);
        // Note: reassign increments attempt, but since max_attempts defaults to 3,
        // chunks should be re-enqueued.
        assert_eq!(scheduler.active_assignments_count(), 0);
    }

    // ================================================================
    // Drain all
    // ================================================================

    #[test]
    fn drain_all_empties_everything() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Rush,
            (0..2)
                .map(|_| make_chunk(job, PriorityLevel::Rush, NodeClass::Edge))
                .collect(),
        );
        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            (0..3)
                .map(|_| make_chunk(job, PriorityLevel::Standard, NodeClass::Edge))
                .collect(),
        );

        scheduler.dequeue_for_node(node, NodeClass::Enterprise);

        let drained = scheduler.drain_all();
        assert!(drained > 0);
        assert_eq!(scheduler.queue_depth().total, 0);
        assert_eq!(scheduler.active_assignments_count(), 0);
    }

    // ================================================================
    // Stats
    // ================================================================

    #[test]
    fn stats_success_rate() {
        let mut stats = SchedulerStats::default();
        assert_eq!(stats.success_rate(), 1.0); // no data = 1.0

        stats.total_completed = 9;
        stats.total_failed = 1;
        assert!((stats.success_rate() - 0.9).abs() < 0.001);
    }

    #[test]
    fn stats_throughput() {
        let mut stats = SchedulerStats::default();
        stats.total_completed = 100;
        assert!((stats.throughput(10.0) - 10.0).abs() < 0.001);
        assert_eq!(stats.throughput(0.0), 0.0);
    }

    #[test]
    fn stats_wait_time_ema() {
        let mut stats = SchedulerStats::default();
        stats.total_dequeued = 1;
        stats.record_wait_time(100.0);
        assert!((stats.avg_wait_time_ms - 100.0).abs() < 0.001);

        stats.total_dequeued = 10;
        stats.record_wait_time(200.0);
        // EMA: 100 * 0.9 + 200 * 0.1 = 110
        assert!((stats.avg_wait_time_ms - 110.0).abs() < 0.001);
    }

    // ================================================================
    // LocalityAwareAssigner tests
    // ================================================================

    #[test]
    fn locality_score_preferred_node() {
        let preferred = NodeId::new();
        let other = NodeId::new();
        let chunk = SchedulableChunk::new(
            ChunkId::new(),
            JobId::new(),
            PriorityLevel::Standard,
            NodeClass::Edge,
        )
        .with_data_locality(preferred);

        let cache_map = HashMap::new();
        let score = LocalityAwareAssigner::score_node(&preferred, &chunk, &cache_map);
        assert!((score - 1.0).abs() < 0.001);

        let score_other = LocalityAwareAssigner::score_node(&other, &chunk, &cache_map);
        assert!(score_other < 1.0);
    }

    #[test]
    fn locality_best_node_prefers_local() {
        let local = NodeId::new();
        let remote = NodeId::new();
        let chunk = SchedulableChunk::new(
            ChunkId::new(),
            JobId::new(),
            PriorityLevel::Standard,
            NodeClass::Edge,
        )
        .with_data_locality(local);

        let candidates = vec![
            (remote, NodeClass::Enterprise),
            (local, NodeClass::Light),
        ];

        let best = LocalityAwareAssigner::best_node(&candidates, &chunk);
        assert_eq!(best, Some(local));
    }

    #[test]
    fn locality_best_node_filters_ineligible() {
        let node = NodeId::new();
        let chunk = SchedulableChunk::new(
            ChunkId::new(),
            JobId::new(),
            PriorityLevel::Standard,
            NodeClass::Standard,
        );

        // Only Edge-class nodes available, but chunk requires Standard.
        let candidates = vec![(node, NodeClass::Edge)];
        let best = LocalityAwareAssigner::best_node(&candidates, &chunk);
        assert!(best.is_none());
    }

    #[test]
    fn locality_rank_candidates_sorted() {
        let preferred = NodeId::new();
        let other = NodeId::new();
        let chunk = SchedulableChunk::new(
            ChunkId::new(),
            JobId::new(),
            PriorityLevel::Standard,
            NodeClass::Edge,
        )
        .with_data_locality(preferred);

        let candidates = vec![
            (other, NodeClass::Standard),
            (preferred, NodeClass::Light),
        ];

        let ranked = LocalityAwareAssigner::rank_candidates(
            &candidates,
            &chunk,
            &HashMap::new(),
        );

        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].0, preferred);
        assert!((ranked[0].1 - 1.0).abs() < 0.001);
    }

    // ================================================================
    // Pending jobs
    // ================================================================

    #[test]
    fn pending_jobs_lists_all_queues() {
        let scheduler = JobScheduler::new();
        let rush_job = JobId::new();
        let std_job = JobId::new();

        scheduler.enqueue_job(
            rush_job,
            PriorityLevel::Rush,
            vec![make_chunk(rush_job, PriorityLevel::Rush, NodeClass::Edge)],
        );
        scheduler.enqueue_job(
            std_job,
            PriorityLevel::Standard,
            (0..3)
                .map(|_| make_chunk(std_job, PriorityLevel::Standard, NodeClass::Edge))
                .collect(),
        );

        let pending = scheduler.pending_jobs();
        assert_eq!(pending.len(), 2);

        let rush_entry = pending.iter().find(|(jid, _, _)| *jid == rush_job);
        assert!(rush_entry.is_some());
        assert_eq!(rush_entry.unwrap().2, 1); // 1 chunk

        let std_entry = pending.iter().find(|(jid, _, _)| *jid == std_job);
        assert!(std_entry.is_some());
        assert_eq!(std_entry.unwrap().2, 3); // 3 chunks
    }

    // ================================================================
    // Queue depth limit
    // ================================================================

    #[test]
    fn enqueue_respects_max_depth() {
        let config = SchedulerConfig {
            max_queue_depth: 5,
            ..Default::default()
        };
        let scheduler = JobScheduler::new().with_config(config);
        let job = JobId::new();

        let chunks: Vec<SchedulableChunk> = (0..10)
            .map(|_| make_chunk(job, PriorityLevel::Standard, NodeClass::Edge))
            .collect();

        let enqueued = scheduler.enqueue_job(job, PriorityLevel::Standard, chunks);
        assert_eq!(enqueued, 5);
        assert_eq!(scheduler.queue_depth().total, 5);
    }

    #[test]
    fn enqueue_rejects_when_full() {
        let config = SchedulerConfig {
            max_queue_depth: 2,
            ..Default::default()
        };
        let scheduler = JobScheduler::new().with_config(config);
        let job = JobId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            (0..2)
                .map(|_| make_chunk(job, PriorityLevel::Standard, NodeClass::Edge))
                .collect(),
        );

        // Now full.
        let enqueued = scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );
        assert_eq!(enqueued, 0);
    }

    // ================================================================
    // Assignment introspection
    // ================================================================

    #[test]
    fn is_chunk_assigned_and_get_assignment() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );

        let chunk = scheduler.dequeue_for_node(node, NodeClass::Enterprise).unwrap();
        assert!(scheduler.is_chunk_assigned(&chunk.chunk_id));

        let assignment = scheduler.get_assignment(&chunk.chunk_id).unwrap();
        assert_eq!(assignment.node_id, node);
        assert_eq!(assignment.job_id, job);
    }

    #[test]
    fn assignments_for_node_query() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            (0..4)
                .map(|_| make_chunk(job, PriorityLevel::Standard, NodeClass::Edge))
                .collect(),
        );

        scheduler.dequeue_for_node(node_a, NodeClass::Enterprise);
        scheduler.dequeue_for_node(node_a, NodeClass::Enterprise);
        scheduler.dequeue_for_node(node_b, NodeClass::Enterprise);

        assert_eq!(scheduler.assignments_for_node(&node_a).len(), 2);
        assert_eq!(scheduler.assignments_for_node(&node_b).len(), 1);
    }

    // ================================================================
    // Peek
    // ================================================================

    #[test]
    fn peek_next_shows_priority_without_consuming() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Rush,
            vec![make_chunk(job, PriorityLevel::Rush, NodeClass::Edge)],
        );

        assert_eq!(
            scheduler.peek_next(NodeClass::Enterprise),
            Some(PriorityLevel::Rush)
        );
        // Should not have consumed it.
        assert_eq!(scheduler.queue_depth().rush, 1);
    }

    #[test]
    fn peek_next_returns_none_when_empty() {
        let scheduler = JobScheduler::new();
        assert_eq!(scheduler.peek_next(NodeClass::Enterprise), None);
    }

    // ================================================================
    // Reset stats
    // ================================================================

    #[test]
    fn reset_stats_clears_all() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Standard,
            vec![make_chunk(job, PriorityLevel::Standard, NodeClass::Edge)],
        );
        let c = scheduler
            .dequeue_for_node(NodeId::new(), NodeClass::Enterprise)
            .unwrap();
        scheduler.complete_chunk(&c.chunk_id, true);

        let stats = scheduler.get_stats();
        assert!(stats.total_enqueued > 0);

        scheduler.reset_stats();
        let stats = scheduler.get_stats();
        assert_eq!(stats.total_enqueued, 0);
        assert_eq!(stats.total_dequeued, 0);
        assert_eq!(stats.total_completed, 0);
    }

    // ================================================================
    // AssignmentTracker utility tests
    // ================================================================

    #[test]
    fn assignment_tracker_count_by_priority() {
        let assignments: DashMap<ChunkId, SchedulerAssignment> = DashMap::new();
        let now = Utc::now();
        let deadline = now + ChronoDuration::seconds(300);

        for _ in 0..3 {
            let a = SchedulerAssignment {
                chunk_id: ChunkId::new(),
                job_id: JobId::new(),
                node_id: NodeId::new(),
                node_class: NodeClass::Standard,
                assigned_at: now,
                deadline,
                priority: PriorityLevel::Rush,
            };
            assignments.insert(a.chunk_id, a);
        }
        for _ in 0..2 {
            let a = SchedulerAssignment {
                chunk_id: ChunkId::new(),
                job_id: JobId::new(),
                node_id: NodeId::new(),
                node_class: NodeClass::Standard,
                assigned_at: now,
                deadline,
                priority: PriorityLevel::Standard,
            };
            assignments.insert(a.chunk_id, a);
        }

        let counts = AssignmentTracker::count_by_priority(&assignments);
        assert_eq!(counts.get(&PriorityLevel::Rush).copied().unwrap_or(0), 3);
        assert_eq!(
            counts
                .get(&PriorityLevel::Standard)
                .copied()
                .unwrap_or(0),
            2
        );
    }

    #[test]
    fn assignment_remaining_secs() {
        let assignment = SchedulerAssignment {
            chunk_id: ChunkId::new(),
            job_id: JobId::new(),
            node_id: NodeId::new(),
            node_class: NodeClass::Standard,
            assigned_at: Utc::now(),
            deadline: Utc::now() + ChronoDuration::seconds(100),
            priority: PriorityLevel::Standard,
        };
        assert!(assignment.remaining_secs() > 90);
        assert!(assignment.remaining_secs() <= 100);
    }

    // ================================================================
    // SchedulerConfig defaults
    // ================================================================

    #[test]
    fn scheduler_config_defaults_match_constants() {
        let config = SchedulerConfig::default();
        assert_eq!(config.tick_interval_ms, SCHEDULER_TICK_MS);
        assert_eq!(config.offpeak_start_hour, SCHEDULER_OFFPEAK_START_HOUR);
        assert_eq!(config.offpeak_end_hour, SCHEDULER_OFFPEAK_END_HOUR);
        assert_eq!(config.max_queue_depth, SCHEDULER_MAX_QUEUE_DEPTH);
    }

    // ================================================================
    // Queue depth for priority
    // ================================================================

    #[test]
    fn queue_depth_for_priority_individual() {
        let scheduler = JobScheduler::new();
        let job = JobId::new();

        scheduler.enqueue_job(
            job,
            PriorityLevel::Rush,
            (0..4)
                .map(|_| make_chunk(job, PriorityLevel::Rush, NodeClass::Edge))
                .collect(),
        );

        assert_eq!(scheduler.queue_depth_for_priority(PriorityLevel::Rush), 4);
        assert_eq!(
            scheduler.queue_depth_for_priority(PriorityLevel::Standard),
            0
        );
        assert_eq!(
            scheduler.queue_depth_for_priority(PriorityLevel::Economy),
            0
        );
    }

    // ================================================================
    // Estimated wait time
    // ================================================================

    #[test]
    fn estimated_wait_when_empty() {
        let scheduler = JobScheduler::new();
        assert_eq!(scheduler.estimated_wait_ms(PriorityLevel::Standard), 0.0);
    }
}
