// Marabunta - Licensed under the MIT License.
//! SSE (Server-Sent Events) streaming engine for job progress, chunk
//! completions, and partial results.
//!
//! The engine provides real-time event delivery for job lifecycle events
//! over SSE connections. Subscribers can watch a specific job or listen
//! on a global firehose channel that receives every event across all jobs.
//!
//! # Architecture
//!
//! ```text
//!     emit_chunk_*()          +------------------+
//!     emit_progress()  -----> | StreamingEngine  |
//!     emit_milestone() -----> |                  |
//!                             | job_channels:    |---> per-job broadcast
//!                             | global_channel:  |---> global broadcast
//!                             | stats:           |
//!                             +------------------+
//!                                     |
//!                             ProgressTracker
//!                             (per-job state)
//! ```
//!
//! # Wire Format
//!
//! Events are formatted according to the SSE specification (RFC 8895):
//!
//! ```text
//! id: <event-id>
//! event: <event-type>
//! data: <json-payload>
//!
//! ```

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn, error};

use crate::common::types::JobId;
use super::types::{ChunkId, NodeId, NodeClass};
use super::config::*;

// ============================================================================
// Streaming configuration
// ============================================================================

/// Configuration for the SSE streaming engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    /// How often to send keepalive comments to prevent proxy timeouts.
    pub keepalive_interval: Duration,

    /// Maximum concurrent SSE connections per job channel.
    pub max_connections_per_job: usize,

    /// Per-job broadcast channel buffer size. When the buffer is full,
    /// lagging subscribers are dropped.
    pub buffer_size: usize,

    /// Global broadcast channel buffer size.
    pub global_buffer_size: usize,

    /// How many seconds after job completion before a job channel is
    /// eligible for cleanup. Gives late subscribers time to drain.
    pub cleanup_after_complete_secs: u64,

    /// Maximum number of concurrent job channels before rejecting new
    /// subscriptions. Prevents unbounded memory growth.
    pub max_job_channels: usize,

    /// Whether to include a retry directive in the SSE stream header.
    pub include_retry_directive: bool,

    /// Default retry interval in milliseconds sent to the SSE client.
    pub default_retry_ms: u64,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            keepalive_interval: Duration::from_secs(SSE_KEEPALIVE_SECS),
            max_connections_per_job: SSE_MAX_CONNECTIONS_PER_JOB,
            buffer_size: SSE_BUFFER_SIZE,
            global_buffer_size: SSE_BUFFER_SIZE * 4,
            cleanup_after_complete_secs: 120,
            max_job_channels: 10_000,
            include_retry_directive: true,
            default_retry_ms: 3000,
        }
    }
}

impl StreamingConfig {
    /// Creates a config suitable for testing with small buffers and short
    /// timeouts.
    pub fn for_testing() -> Self {
        Self {
            keepalive_interval: Duration::from_millis(500),
            max_connections_per_job: 5,
            buffer_size: 16,
            global_buffer_size: 32,
            cleanup_after_complete_secs: 5,
            max_job_channels: 100,
            include_retry_directive: false,
            default_retry_ms: 500,
        }
    }

    /// Returns true if the config is valid.
    pub fn validate(&self) -> Result<(), String> {
        if self.buffer_size == 0 {
            return Err("buffer_size must be > 0".to_string());
        }
        if self.global_buffer_size == 0 {
            return Err("global_buffer_size must be > 0".to_string());
        }
        if self.max_connections_per_job == 0 {
            return Err("max_connections_per_job must be > 0".to_string());
        }
        if self.max_job_channels == 0 {
            return Err("max_job_channels must be > 0".to_string());
        }
        Ok(())
    }
}

// ============================================================================
// Stream event
// ============================================================================

/// An event emitted to SSE subscribers.
///
/// Each event carries a unique ID, an event type string (used in the SSE
/// `event:` field), and a JSON data payload. The `job_id` is always set
/// so subscribers can filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    /// Unique event identifier (monotonically increasing within a job).
    pub id: String,

    /// SSE event type (e.g., "chunk.started", "progress", "job.completed").
    pub event_type: String,

    /// The job this event belongs to.
    pub job_id: JobId,

    /// JSON payload containing event-specific data.
    pub data: serde_json::Value,

    /// When this event was created.
    pub timestamp: DateTime<Utc>,
}

impl StreamEvent {
    /// Creates a new StreamEvent with the given parameters.
    pub fn new(
        id: String,
        event_type: String,
        job_id: JobId,
        data: serde_json::Value,
    ) -> Self {
        Self {
            id,
            event_type,
            job_id,
            data,
            timestamp: Utc::now(),
        }
    }

    /// Formats this event as an SSE wire-protocol string.
    ///
    /// The output follows RFC 8895:
    /// ```text
    /// id: <id>
    /// event: <event_type>
    /// data: <json>
    ///
    /// ```
    pub fn to_sse_string(&self) -> String {
        let json = serde_json::to_string(&self.data).unwrap_or_else(|_| "{}".to_string());
        // SSE data lines: if the JSON contains newlines, each line must
        // be prefixed with "data: "
        let data_lines: Vec<String> = json
            .lines()
            .map(|line| format!("data: {}", line))
            .collect();
        format!(
            "id: {}\nevent: {}\n{}\n\n",
            self.id,
            self.event_type,
            data_lines.join("\n"),
        )
    }

    /// Serializes the entire event (including metadata) to JSON.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "event_type": self.event_type,
            "job_id": self.job_id,
            "data": self.data,
            "timestamp": self.timestamp.to_rfc3339(),
        })
    }

    /// Returns the SSE event name (alias for `event_type`).
    pub fn event_name(&self) -> &str {
        &self.event_type
    }

    /// Returns the size in bytes of the SSE-formatted representation.
    pub fn sse_byte_size(&self) -> usize {
        self.to_sse_string().len()
    }

    /// Returns true if this is a terminal event (job completed or failed).
    pub fn is_terminal(&self) -> bool {
        self.event_type == "job.completed" || self.event_type == "job.failed"
    }

    /// Returns true if this is a progress event.
    pub fn is_progress(&self) -> bool {
        self.event_type == "progress"
    }

    /// Returns true if this is a milestone event.
    pub fn is_milestone(&self) -> bool {
        self.event_type == "milestone"
    }

    /// Extracts the chunk_id from the data payload, if present.
    pub fn chunk_id(&self) -> Option<ChunkId> {
        self.data.get("chunk_id").and_then(|v| {
            serde_json::from_value(v.clone()).ok()
        })
    }

    /// Extracts the node_id from the data payload, if present.
    pub fn node_id(&self) -> Option<NodeId> {
        self.data.get("node_id").and_then(|v| {
            serde_json::from_value(v.clone()).ok()
        })
    }
}

// ============================================================================
// Job progress
// ============================================================================

/// A point-in-time snapshot of job progress, suitable for SSE delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgress {
    pub job_id: JobId,
    pub total_chunks: u32,
    pub completed_chunks: u32,
    pub failed_chunks: u32,
    pub running_chunks: u32,
    pub queued_chunks: u32,
    pub retry_count: u32,
    /// Completion percentage (0.0 - 100.0).
    pub completion_pct: f64,
    /// Estimated seconds remaining (None if insufficient data).
    pub estimated_remaining_secs: Option<f64>,
    /// Number of distinct nodes currently executing chunks.
    pub active_nodes: u32,
    /// Breakdown of active nodes by hardware class.
    pub nodes_by_class: HashMap<NodeClass, u32>,
    /// Accumulated cost so far in USD.
    pub current_cost_usd: f64,
    /// Projected total cost in USD when the job completes.
    pub estimated_total_cost_usd: f64,
    /// Current throughput in completed chunks per second.
    pub throughput_chunks_per_sec: f64,
}

impl JobProgress {
    /// Returns true if all chunks have reached a terminal state.
    pub fn is_finished(&self) -> bool {
        self.completed_chunks + self.failed_chunks >= self.total_chunks
    }

    /// Returns the fraction of chunks that succeeded.
    pub fn success_rate(&self) -> f64 {
        if self.total_chunks == 0 {
            return 0.0;
        }
        self.completed_chunks as f64 / self.total_chunks as f64
    }

    /// Returns the fraction of chunks that failed.
    pub fn failure_rate(&self) -> f64 {
        if self.total_chunks == 0 {
            return 0.0;
        }
        self.failed_chunks as f64 / self.total_chunks as f64
    }

    /// Returns total active (non-terminal) chunks.
    pub fn active_chunks(&self) -> u32 {
        self.running_chunks + self.queued_chunks
    }

    /// Returns the dominant node class by count.
    pub fn dominant_node_class(&self) -> Option<NodeClass> {
        self.nodes_by_class
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(class, _)| *class)
    }

    /// Converts this progress into a JSON value for SSE data payload.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}

impl Default for JobProgress {
    fn default() -> Self {
        Self {
            job_id: JobId(uuid::Uuid::nil()),
            total_chunks: 0,
            completed_chunks: 0,
            failed_chunks: 0,
            running_chunks: 0,
            queued_chunks: 0,
            retry_count: 0,
            completion_pct: 0.0,
            estimated_remaining_secs: None,
            active_nodes: 0,
            nodes_by_class: HashMap::new(),
            current_cost_usd: 0.0,
            estimated_total_cost_usd: 0.0,
            throughput_chunks_per_sec: 0.0,
        }
    }
}

// ============================================================================
// Job completion summary
// ============================================================================

/// Final summary emitted when a job reaches a terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCompletionSummary {
    pub job_id: JobId,
    pub total_chunks: u32,
    pub completed: u32,
    pub failed: u32,
    pub retried: u32,
    /// Wall-clock seconds from first chunk start to last completion.
    pub duration_secs: f64,
    /// Total cost in USD.
    pub total_cost_usd: f64,
    /// Number of distinct nodes that executed at least one chunk.
    pub nodes_used: u32,
    /// Verification statistics as a JSON blob.
    pub verification_stats: serde_json::Value,
    /// Total result data size in bytes.
    pub result_size_bytes: u64,
}

impl JobCompletionSummary {
    /// Returns the overall success rate.
    pub fn success_rate(&self) -> f64 {
        if self.total_chunks == 0 {
            return 0.0;
        }
        self.completed as f64 / self.total_chunks as f64
    }

    /// Returns true if every chunk completed successfully.
    pub fn is_fully_successful(&self) -> bool {
        self.failed == 0 && self.completed == self.total_chunks
    }

    /// Returns cost per chunk in USD.
    pub fn cost_per_chunk(&self) -> f64 {
        if self.total_chunks == 0 {
            return 0.0;
        }
        self.total_cost_usd / self.total_chunks as f64
    }

    /// Returns average chunk duration in seconds.
    pub fn avg_chunk_duration_secs(&self) -> f64 {
        if self.completed == 0 {
            return 0.0;
        }
        self.duration_secs / self.completed as f64
    }

    /// Converts to JSON for SSE payload.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}

// ============================================================================
// Chunk state (for progress tracking)
// ============================================================================

/// Per-chunk lifecycle state tracked by the ProgressTracker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChunkState {
    /// Chunk is waiting to be assigned.
    Queued,

    /// Chunk is actively being executed.
    Running {
        node_id: NodeId,
        started_at: DateTime<Utc>,
    },

    /// Chunk completed successfully.
    Completed {
        node_id: NodeId,
        duration_ms: u64,
    },

    /// Chunk failed (may be retried).
    Failed {
        error: String,
        attempts: u32,
    },
}

impl ChunkState {
    /// Returns true if this chunk is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, ChunkState::Completed { .. } | ChunkState::Failed { .. })
    }

    /// Returns true if the chunk is currently running.
    pub fn is_running(&self) -> bool {
        matches!(self, ChunkState::Running { .. })
    }

    /// Returns true if the chunk is queued.
    pub fn is_queued(&self) -> bool {
        matches!(self, ChunkState::Queued)
    }

    /// Returns the node_id if the chunk is running or completed.
    pub fn node_id(&self) -> Option<NodeId> {
        match self {
            ChunkState::Running { node_id, .. } => Some(*node_id),
            ChunkState::Completed { node_id, .. } => Some(*node_id),
            _ => None,
        }
    }

    /// Returns the duration_ms if the chunk is completed.
    pub fn duration_ms(&self) -> Option<u64> {
        match self {
            ChunkState::Completed { duration_ms, .. } => Some(*duration_ms),
            _ => None,
        }
    }

    /// Returns the error string if the chunk has failed.
    pub fn error(&self) -> Option<&str> {
        match self {
            ChunkState::Failed { error, .. } => Some(error),
            _ => None,
        }
    }

    /// Returns the attempt count if the chunk has failed.
    pub fn attempts(&self) -> Option<u32> {
        match self {
            ChunkState::Failed { attempts, .. } => Some(*attempts),
            _ => None,
        }
    }

    /// Returns a human-readable label for this state.
    pub fn label(&self) -> &'static str {
        match self {
            ChunkState::Queued => "queued",
            ChunkState::Running { .. } => "running",
            ChunkState::Completed { .. } => "completed",
            ChunkState::Failed { .. } => "failed",
        }
    }
}

// ============================================================================
// Progress tracker
// ============================================================================

/// Tracks per-chunk state for a single job and computes derived progress
/// metrics (throughput, ETA, milestone crossings).
///
/// This is the internal bookkeeping struct that feeds `JobProgress` snapshots
/// to the streaming engine.
#[derive(Debug)]
pub struct ProgressTracker {
    pub job_id: JobId,
    pub total_chunks: u32,
    pub chunk_states: HashMap<ChunkId, ChunkState>,
    pub started_at: Option<DateTime<Utc>>,
    pub last_completion_at: Option<DateTime<Utc>>,
    completions_count: u32,
    total_completion_duration_ms: u64,
    retry_count: u32,
    milestones_reached: Vec<u8>,
    active_nodes: HashMap<NodeId, u32>,
    nodes_by_class: HashMap<NodeClass, u32>,
    current_cost_usd: f64,
    failed_count: u32,
}

impl ProgressTracker {
    /// Creates a new tracker for a job with `total_chunks` chunks.
    pub fn new(job_id: JobId, total_chunks: u32) -> Self {
        Self {
            job_id,
            total_chunks,
            chunk_states: HashMap::new(),
            started_at: None,
            last_completion_at: None,
            completions_count: 0,
            total_completion_duration_ms: 0,
            retry_count: 0,
            milestones_reached: Vec::new(),
            active_nodes: HashMap::new(),
            nodes_by_class: HashMap::new(),
            current_cost_usd: 0.0,
            failed_count: 0,
        }
    }

    /// Records that a chunk has started execution on the given node.
    pub fn record_started(&mut self, chunk_id: ChunkId, node_id: NodeId) {
        let now = Utc::now();
        if self.started_at.is_none() {
            self.started_at = Some(now);
        }

        // If the chunk was previously failed, this is a retry
        if let Some(ChunkState::Failed { .. }) = self.chunk_states.get(&chunk_id) {
            self.retry_count += 1;
        }

        self.chunk_states.insert(chunk_id, ChunkState::Running {
            node_id,
            started_at: now,
        });

        *self.active_nodes.entry(node_id).or_insert(0) += 1;
    }

    /// Records that a chunk has started execution on the given node with a
    /// node class annotation. This updates the per-class node count.
    pub fn record_started_with_class(
        &mut self,
        chunk_id: ChunkId,
        node_id: NodeId,
        node_class: NodeClass,
    ) {
        self.record_started(chunk_id, node_id);
        *self.nodes_by_class.entry(node_class).or_insert(0) += 1;
    }

    /// Records that a chunk completed successfully.
    pub fn record_completed(&mut self, chunk_id: ChunkId, duration_ms: u64) {
        let now = Utc::now();
        self.last_completion_at = Some(now);

        // Retrieve the node_id before replacing the state
        let node_id = self
            .chunk_states
            .get(&chunk_id)
            .and_then(|s| s.node_id())
            .unwrap_or_default();

        // Decrement active node count
        if let Some(count) = self.active_nodes.get_mut(&node_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.active_nodes.remove(&node_id);
            }
        }

        self.chunk_states.insert(chunk_id, ChunkState::Completed {
            node_id,
            duration_ms,
        });

        self.completions_count += 1;
        self.total_completion_duration_ms += duration_ms;
    }

    /// Records that a chunk failed.
    pub fn record_failed(&mut self, chunk_id: ChunkId, error: String) {
        // Retrieve the node_id before replacing the state
        let node_id = self
            .chunk_states
            .get(&chunk_id)
            .and_then(|s| s.node_id());

        // Decrement active node count
        if let Some(nid) = node_id {
            if let Some(count) = self.active_nodes.get_mut(&nid) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.active_nodes.remove(&nid);
                }
            }
        }

        let attempts = match self.chunk_states.get(&chunk_id) {
            Some(ChunkState::Failed { attempts, .. }) => attempts + 1,
            _ => 1,
        };

        self.chunk_states.insert(chunk_id, ChunkState::Failed {
            error,
            attempts,
        });

        self.failed_count += 1;
    }

    /// Adds cost to the running total.
    pub fn add_cost(&mut self, cost_usd: f64) {
        self.current_cost_usd += cost_usd;
    }

    /// Computes a full progress snapshot for SSE delivery.
    pub fn compute_progress(&self) -> JobProgress {
        let mut running = 0u32;
        let mut completed = 0u32;
        let mut failed = 0u32;

        for state in self.chunk_states.values() {
            match state {
                ChunkState::Queued => {}
                ChunkState::Running { .. } => running += 1,
                ChunkState::Completed { .. } => completed += 1,
                ChunkState::Failed { .. } => failed += 1,
            }
        }

        let tracked = self.chunk_states.len() as u32;
        let queued = if self.total_chunks > tracked {
            self.total_chunks - tracked
        } else {
            let explicit_queued: u32 = self.chunk_states.values()
                .filter(|s| s.is_queued())
                .count() as u32;
            explicit_queued
        };

        let completion_pct = if self.total_chunks == 0 {
            100.0
        } else {
            (completed as f64 / self.total_chunks as f64) * 100.0
        };

        let throughput = self.throughput();
        let estimated_remaining = self.estimated_remaining();

        let estimated_remaining_secs = estimated_remaining.map(|d| d.as_secs_f64());

        let estimated_total_cost = if completion_pct > 0.0 {
            self.current_cost_usd * (100.0 / completion_pct)
        } else {
            0.0
        };

        JobProgress {
            job_id: self.job_id,
            total_chunks: self.total_chunks,
            completed_chunks: completed,
            failed_chunks: failed,
            running_chunks: running,
            queued_chunks: queued,
            retry_count: self.retry_count,
            completion_pct,
            estimated_remaining_secs,
            active_nodes: self.active_nodes.len() as u32,
            nodes_by_class: self.nodes_by_class.clone(),
            current_cost_usd: self.current_cost_usd,
            estimated_total_cost_usd: estimated_total_cost,
            throughput_chunks_per_sec: throughput,
        }
    }

    /// Returns the current throughput in chunks per second.
    ///
    /// Calculated as total completions divided by elapsed wall-clock time
    /// since the first chunk started.
    pub fn throughput(&self) -> f64 {
        let started = match self.started_at {
            Some(s) => s,
            None => return 0.0,
        };

        if self.completions_count == 0 {
            return 0.0;
        }

        let end = self.last_completion_at.unwrap_or_else(Utc::now);
        let elapsed_secs = (end - started).num_milliseconds() as f64 / 1000.0;

        if elapsed_secs <= 0.0 {
            return 0.0;
        }

        self.completions_count as f64 / elapsed_secs
    }

    /// Estimates the remaining time to completion based on current throughput.
    pub fn estimated_remaining(&self) -> Option<Duration> {
        let throughput = self.throughput();
        if throughput <= 0.0 {
            return None;
        }

        let remaining = self.total_chunks.saturating_sub(self.completions_count + self.failed_count);
        if remaining == 0 {
            return Some(Duration::ZERO);
        }

        let remaining_secs = remaining as f64 / throughput;
        Some(Duration::from_secs_f64(remaining_secs))
    }

    /// Returns true the first time the given percentage milestone is
    /// crossed. Subsequent calls with the same `pct` return false.
    pub fn reached_milestone(&mut self, pct: u8) -> bool {
        if self.milestones_reached.contains(&pct) {
            return false;
        }

        let current_pct = self.completion_pct();
        if current_pct >= pct as f64 {
            self.milestones_reached.push(pct);
            true
        } else {
            false
        }
    }

    /// Returns true if all chunks have reached a terminal state.
    pub fn is_complete(&self) -> bool {
        if self.total_chunks == 0 {
            return true;
        }

        let terminal_count: u32 = self.chunk_states.values()
            .filter(|s| s.is_terminal())
            .count() as u32;

        terminal_count >= self.total_chunks
    }

    /// Returns the completion percentage (0.0 - 100.0).
    pub fn completion_pct(&self) -> f64 {
        if self.total_chunks == 0 {
            return 100.0;
        }
        (self.completions_count as f64 / self.total_chunks as f64) * 100.0
    }

    /// Returns the number of chunks that have completed successfully.
    pub fn completed_count(&self) -> u32 {
        self.completions_count
    }

    /// Returns the number of chunks that have failed.
    pub fn failed_count(&self) -> u32 {
        self.failed_count
    }

    /// Returns the total retry count across all chunks.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Returns the average chunk duration in milliseconds, or None if no
    /// chunks have completed.
    pub fn avg_duration_ms(&self) -> Option<u64> {
        if self.completions_count == 0 {
            return None;
        }
        Some(self.total_completion_duration_ms / self.completions_count as u64)
    }

    /// Returns the set of milestones already reached.
    pub fn milestones_reached(&self) -> &[u8] {
        &self.milestones_reached
    }

    /// Returns the number of distinct nodes currently running chunks.
    pub fn active_node_count(&self) -> usize {
        self.active_nodes.len()
    }

    /// Returns a set of all node IDs that have ever been assigned a chunk.
    pub fn all_participating_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self
            .chunk_states
            .values()
            .filter_map(|s| s.node_id())
            .collect();
        nodes.sort();
        nodes.dedup();
        nodes
    }

    /// Returns the state of a specific chunk.
    pub fn chunk_state(&self, chunk_id: &ChunkId) -> Option<&ChunkState> {
        self.chunk_states.get(chunk_id)
    }

    /// Returns the number of chunks in each state category.
    pub fn state_counts(&self) -> (u32, u32, u32, u32) {
        let mut queued = 0u32;
        let mut running = 0u32;
        let mut completed = 0u32;
        let mut failed = 0u32;

        for state in self.chunk_states.values() {
            match state {
                ChunkState::Queued => queued += 1,
                ChunkState::Running { .. } => running += 1,
                ChunkState::Completed { .. } => completed += 1,
                ChunkState::Failed { .. } => failed += 1,
            }
        }

        (queued, running, completed, failed)
    }

    /// Resets the tracker to its initial state (same job_id and total_chunks).
    pub fn reset(&mut self) {
        self.chunk_states.clear();
        self.started_at = None;
        self.last_completion_at = None;
        self.completions_count = 0;
        self.total_completion_duration_ms = 0;
        self.retry_count = 0;
        self.milestones_reached.clear();
        self.active_nodes.clear();
        self.nodes_by_class.clear();
        self.current_cost_usd = 0.0;
        self.failed_count = 0;
    }
}

// ============================================================================
// SSE formatter
// ============================================================================

/// Stateless helper for formatting SSE wire-protocol strings.
pub struct SseFormatter;

impl SseFormatter {
    /// Formats a StreamEvent as an SSE message block.
    ///
    /// ```text
    /// id: <id>
    /// event: <event_type>
    /// data: <json>
    ///
    /// ```
    pub fn format_event(event: &StreamEvent) -> String {
        event.to_sse_string()
    }

    /// Formats an SSE keepalive comment.
    ///
    /// ```text
    /// : keepalive
    ///
    /// ```
    pub fn format_keepalive() -> String {
        ": keepalive\n\n".to_string()
    }

    /// Formats an SSE retry directive.
    ///
    /// Tells the client to wait `ms` milliseconds before reconnecting.
    ///
    /// ```text
    /// retry: <ms>
    ///
    /// ```
    pub fn format_retry(ms: u64) -> String {
        format!("retry: {}\n\n", ms)
    }

    /// Formats an SSE comment line.
    ///
    /// ```text
    /// : <text>
    ///
    /// ```
    pub fn format_comment(text: &str) -> String {
        let lines: Vec<String> = text.lines().map(|l| format!(": {}", l)).collect();
        format!("{}\n\n", lines.join("\n"))
    }

    /// Formats a raw data-only SSE message (no id or event fields).
    pub fn format_data_only(data: &serde_json::Value) -> String {
        let json = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());
        format!("data: {}\n\n", json)
    }

    /// Formats a named event without an ID field.
    pub fn format_named_event(event_type: &str, data: &serde_json::Value) -> String {
        let json = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());
        format!("event: {}\ndata: {}\n\n", event_type, json)
    }

    /// Formats a stream header with optional retry directive and an
    /// initial comment identifying the stream.
    pub fn format_stream_header(job_id: &JobId, retry_ms: Option<u64>) -> String {
        let mut header = format!(": connected to job stream {}\n\n", job_id);
        if let Some(ms) = retry_ms {
            header.push_str(&Self::format_retry(ms));
        }
        header
    }

    /// Validates that a string is a well-formed SSE message block
    /// (ends with double newline, no bare CR).
    pub fn is_valid_sse(message: &str) -> bool {
        if !message.ends_with("\n\n") {
            return false;
        }
        // SSE must not contain bare CR (only LF or CRLF)
        !message.contains('\r') || message.contains("\r\n")
    }
}

// ============================================================================
// Streaming stats
// ============================================================================

/// Aggregate statistics for the streaming engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamingStats {
    /// Total number of events emitted across all channels.
    pub total_events_emitted: u64,

    /// Current number of active subscriber receivers.
    pub active_subscriptions: u64,

    /// Total number of subscriptions ever created (including dropped).
    pub total_subscriptions_created: u64,

    /// Number of events that were dropped due to lagging subscribers.
    pub events_dropped: u64,

    /// Number of job channels currently open.
    pub active_job_channels: u64,

    /// Total number of job channels ever created.
    pub total_job_channels_created: u64,

    /// Total bytes emitted (SSE formatted).
    pub total_bytes_emitted: u64,
}

impl StreamingStats {
    /// Returns the drop rate as a fraction (0.0 - 1.0).
    pub fn drop_rate(&self) -> f64 {
        if self.total_events_emitted == 0 {
            return 0.0;
        }
        self.events_dropped as f64 / self.total_events_emitted as f64
    }

    /// Returns true if the drop rate exceeds the given threshold.
    pub fn is_dropping_above(&self, threshold: f64) -> bool {
        self.drop_rate() > threshold
    }

    /// Returns the average event size in bytes.
    pub fn avg_event_size_bytes(&self) -> u64 {
        if self.total_events_emitted == 0 {
            return 0;
        }
        self.total_bytes_emitted / self.total_events_emitted
    }

    /// Resets all counters to zero.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Converts to JSON for API responses.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}

// ============================================================================
// Streaming engine
// ============================================================================

/// The main SSE streaming engine.
///
/// Manages per-job and global broadcast channels, emits typed events,
/// and tracks aggregate statistics.
///
/// # Thread Safety
///
/// All fields are either `DashMap` (lock-free concurrent map) or wrapped
/// in `parking_lot::RwLock`, so the engine is safe to share across tasks
/// via `Arc<StreamingEngine>`.
pub struct StreamingEngine {
    /// Per-job broadcast senders. Each job gets its own channel when the
    /// first subscriber or emit call arrives.
    job_channels: DashMap<JobId, broadcast::Sender<StreamEvent>>,

    /// Global broadcast sender that receives a copy of every event.
    global_channel: broadcast::Sender<StreamEvent>,

    /// Aggregate stats protected by a parking_lot RwLock.
    stats: parking_lot::RwLock<StreamingStats>,

    /// Engine configuration.
    config: StreamingConfig,

    /// Monotonic event counter for generating unique IDs.
    event_counter: std::sync::atomic::AtomicU64,
}

impl StreamingEngine {
    /// Creates a new streaming engine with default configuration.
    pub fn new() -> Self {
        Self::with_config(StreamingConfig::default())
    }

    /// Creates a new streaming engine with the given configuration.
    pub fn with_config(config: StreamingConfig) -> Self {
        let (global_tx, _) = broadcast::channel(config.global_buffer_size);
        Self {
            job_channels: DashMap::new(),
            global_channel: global_tx,
            stats: parking_lot::RwLock::new(StreamingStats::default()),
            config,
            event_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Generates a unique event ID.
    fn next_event_id(&self) -> String {
        let counter = self.event_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("evt-{}", counter)
    }

    /// Gets or creates a broadcast sender for the given job.
    fn get_or_create_channel(&self, job_id: &JobId) -> broadcast::Sender<StreamEvent> {
        if let Some(sender) = self.job_channels.get(job_id) {
            return sender.clone();
        }

        let (tx, _) = broadcast::channel(self.config.buffer_size);
        self.job_channels.insert(*job_id, tx.clone());

        {
            let mut stats = self.stats.write();
            stats.active_job_channels = self.job_channels.len() as u64;
            stats.total_job_channels_created += 1;
        }

        debug!(job_id = %job_id, "Created new SSE channel for job");
        tx
    }

    /// Emits an event to both the job-specific and global channels.
    fn emit(&self, event: StreamEvent) {
        let job_id = event.job_id;
        let sse_size = event.sse_byte_size() as u64;

        // Send to job channel
        let job_tx = self.get_or_create_channel(&job_id);
        let job_result = job_tx.send(event.clone());

        // Send to global channel
        let global_result = self.global_channel.send(event);

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_events_emitted += 1;
            stats.total_bytes_emitted += sse_size;

            if job_result.is_err() && global_result.is_err() {
                // Both channels had no receivers; this is not a drop, just
                // no one is listening
            }
        }
    }

    /// Subscribes to events for a specific job.
    ///
    /// Returns a `broadcast::Receiver` that will receive all events
    /// emitted for this job from now on. If the job channel does not
    /// exist yet, it is created.
    pub fn subscribe_job(&self, job_id: &JobId) -> broadcast::Receiver<StreamEvent> {
        let tx = self.get_or_create_channel(job_id);
        let rx = tx.subscribe();

        {
            let mut stats = self.stats.write();
            stats.active_subscriptions += 1;
            stats.total_subscriptions_created += 1;
        }

        info!(job_id = %job_id, "New SSE subscriber for job");
        rx
    }

    /// Subscribes to the global event firehose.
    ///
    /// Returns a `broadcast::Receiver` that receives every event from
    /// every job. Useful for dashboards and monitoring.
    pub fn subscribe_global(&self) -> broadcast::Receiver<StreamEvent> {
        let rx = self.global_channel.subscribe();

        {
            let mut stats = self.stats.write();
            stats.active_subscriptions += 1;
            stats.total_subscriptions_created += 1;
        }

        info!("New global SSE subscriber");
        rx
    }

    /// Emits a "chunk.started" event.
    pub fn emit_chunk_started(
        &self,
        job_id: &JobId,
        chunk_id: ChunkId,
        node_id: NodeId,
        node_class: NodeClass,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "chunk.started".to_string(),
            *job_id,
            serde_json::json!({
                "chunk_id": chunk_id,
                "node_id": node_id,
                "node_class": node_class,
                "started_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            chunk_id = %chunk_id,
            node_id = %node_id,
            "Emitting chunk.started"
        );
        self.emit(event);
    }

    /// Emits a "chunk.completed" event.
    pub fn emit_chunk_completed(
        &self,
        job_id: &JobId,
        chunk_id: ChunkId,
        node_id: NodeId,
        duration_ms: u64,
        result_size: u64,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "chunk.completed".to_string(),
            *job_id,
            serde_json::json!({
                "chunk_id": chunk_id,
                "node_id": node_id,
                "duration_ms": duration_ms,
                "result_size": result_size,
                "completed_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            chunk_id = %chunk_id,
            duration_ms = duration_ms,
            "Emitting chunk.completed"
        );
        self.emit(event);
    }

    /// Emits a "chunk.failed" event.
    pub fn emit_chunk_failed(
        &self,
        job_id: &JobId,
        chunk_id: ChunkId,
        node_id: NodeId,
        error: String,
        will_retry: bool,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "chunk.failed".to_string(),
            *job_id,
            serde_json::json!({
                "chunk_id": chunk_id,
                "node_id": node_id,
                "error": error,
                "will_retry": will_retry,
                "failed_at": Utc::now().to_rfc3339(),
            }),
        );
        warn!(
            job_id = %job_id,
            chunk_id = %chunk_id,
            error = %error,
            will_retry = will_retry,
            "Emitting chunk.failed"
        );
        self.emit(event);
    }

    /// Emits a "progress" event with a full progress snapshot.
    pub fn emit_progress(&self, job_id: &JobId, progress: JobProgress) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "progress".to_string(),
            *job_id,
            progress.to_json(),
        );
        debug!(
            job_id = %job_id,
            completion_pct = progress.completion_pct,
            "Emitting progress"
        );
        self.emit(event);
    }

    /// Emits a "milestone" event when a percentage threshold is crossed.
    pub fn emit_milestone(&self, job_id: &JobId, milestone_pct: u8) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "milestone".to_string(),
            *job_id,
            serde_json::json!({
                "milestone_pct": milestone_pct,
                "reached_at": Utc::now().to_rfc3339(),
            }),
        );
        info!(
            job_id = %job_id,
            milestone_pct = milestone_pct,
            "Emitting milestone"
        );
        self.emit(event);
    }

    /// Emits a "job.completed" event with the final summary.
    pub fn emit_job_completed(&self, job_id: &JobId, summary: JobCompletionSummary) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "job.completed".to_string(),
            *job_id,
            summary.to_json(),
        );
        info!(
            job_id = %job_id,
            total_chunks = summary.total_chunks,
            completed = summary.completed,
            failed = summary.failed,
            duration_secs = summary.duration_secs,
            "Emitting job.completed"
        );
        self.emit(event);
    }

    /// Emits a "job.failed" event.
    pub fn emit_job_failed(&self, job_id: &JobId, error: String) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "job.failed".to_string(),
            *job_id,
            serde_json::json!({
                "error": error,
                "failed_at": Utc::now().to_rfc3339(),
            }),
        );
        error!(
            job_id = %job_id,
            error = %error,
            "Emitting job.failed"
        );
        self.emit(event);
    }

    /// Emits a "partial_result" event with a preview of chunk output.
    pub fn emit_partial_result(
        &self,
        job_id: &JobId,
        chunk_id: ChunkId,
        result_preview: serde_json::Value,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "partial_result".to_string(),
            *job_id,
            serde_json::json!({
                "chunk_id": chunk_id,
                "result_preview": result_preview,
                "emitted_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            chunk_id = %chunk_id,
            "Emitting partial_result"
        );
        self.emit(event);
    }

    /// Emits a "verification" event for chunk output verification.
    pub fn emit_verification_event(
        &self,
        job_id: &JobId,
        chunk_id: ChunkId,
        outcome: String,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "verification".to_string(),
            *job_id,
            serde_json::json!({
                "chunk_id": chunk_id,
                "outcome": outcome,
                "verified_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            chunk_id = %chunk_id,
            outcome = %outcome,
            "Emitting verification"
        );
        self.emit(event);
    }

    /// Emits a "cost_update" event with current and projected costs.
    pub fn emit_cost_update(
        &self,
        job_id: &JobId,
        current_cost_usd: f64,
        estimated_remaining_usd: f64,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "cost_update".to_string(),
            *job_id,
            serde_json::json!({
                "current_cost_usd": current_cost_usd,
                "estimated_remaining_usd": estimated_remaining_usd,
                "estimated_total_usd": current_cost_usd + estimated_remaining_usd,
                "updated_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            current = current_cost_usd,
            remaining = estimated_remaining_usd,
            "Emitting cost_update"
        );
        self.emit(event);
    }

    /// Returns the number of active subscribers for a specific job.
    pub fn subscriber_count(&self, job_id: &JobId) -> usize {
        match self.job_channels.get(job_id) {
            Some(tx) => tx.receiver_count(),
            None => 0,
        }
    }

    /// Returns the number of active job streams.
    pub fn active_streams(&self) -> usize {
        self.job_channels.len()
    }

    /// Removes the channel for a completed/failed job, freeing resources.
    pub fn cleanup_job(&self, job_id: &JobId) {
        if self.job_channels.remove(job_id).is_some() {
            let mut stats = self.stats.write();
            stats.active_job_channels = self.job_channels.len() as u64;
            info!(job_id = %job_id, "Cleaned up SSE channel for job");
        }
    }

    /// Returns a snapshot of the aggregate streaming statistics.
    pub fn get_stats(&self) -> StreamingStats {
        self.stats.read().clone()
    }

    /// Returns the engine configuration.
    pub fn config(&self) -> &StreamingConfig {
        &self.config
    }

    /// Returns the number of subscribers on the global channel.
    pub fn global_subscriber_count(&self) -> usize {
        self.global_channel.receiver_count()
    }

    /// Cleans up all job channels that have zero subscribers.
    pub fn cleanup_idle_channels(&self) -> usize {
        let mut removed = 0;
        let keys: Vec<JobId> = self.job_channels.iter()
            .filter(|entry| entry.value().receiver_count() == 0)
            .map(|entry| *entry.key())
            .collect();

        for job_id in keys {
            self.job_channels.remove(&job_id);
            removed += 1;
        }

        if removed > 0 {
            let mut stats = self.stats.write();
            stats.active_job_channels = self.job_channels.len() as u64;
            debug!(removed = removed, "Cleaned up idle SSE channels");
        }

        removed
    }

    /// Returns a list of all job IDs that currently have open channels.
    pub fn active_job_ids(&self) -> Vec<JobId> {
        self.job_channels.iter().map(|entry| *entry.key()).collect()
    }

    /// Emits a custom event with an arbitrary type and payload.
    pub fn emit_custom(
        &self,
        job_id: &JobId,
        event_type: &str,
        data: serde_json::Value,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            event_type.to_string(),
            *job_id,
            data,
        );
        debug!(
            job_id = %job_id,
            event_type = event_type,
            "Emitting custom event"
        );
        self.emit(event);
    }

    /// Records that an event was dropped due to a lagging subscriber.
    pub fn record_drop(&self) {
        let mut stats = self.stats.write();
        stats.events_dropped += 1;
    }

    /// Records that a subscription was closed (subscriber disconnected).
    pub fn record_unsubscribe(&self) {
        let mut stats = self.stats.write();
        stats.active_subscriptions = stats.active_subscriptions.saturating_sub(1);
    }

    /// Returns true if the engine has any active subscribers (job or global).
    pub fn has_subscribers(&self) -> bool {
        if self.global_channel.receiver_count() > 0 {
            return true;
        }
        self.job_channels.iter().any(|entry| entry.value().receiver_count() > 0)
    }

    /// Emits a "chunk.reassigned" event when a chunk moves to a new node.
    pub fn emit_chunk_reassigned(
        &self,
        job_id: &JobId,
        chunk_id: ChunkId,
        old_node: NodeId,
        new_node: NodeId,
        reason: &str,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "chunk.reassigned".to_string(),
            *job_id,
            serde_json::json!({
                "chunk_id": chunk_id,
                "old_node_id": old_node,
                "new_node_id": new_node,
                "reason": reason,
                "reassigned_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            chunk_id = %chunk_id,
            "Emitting chunk.reassigned"
        );
        self.emit(event);
    }

    /// Emits a "job.started" event when a job begins processing.
    pub fn emit_job_started(&self, job_id: &JobId, total_chunks: u32) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "job.started".to_string(),
            *job_id,
            serde_json::json!({
                "total_chunks": total_chunks,
                "started_at": Utc::now().to_rfc3339(),
            }),
        );
        info!(
            job_id = %job_id,
            total_chunks = total_chunks,
            "Emitting job.started"
        );
        self.emit(event);
    }

    /// Emits a "node.joined" event when a new node begins work on a job.
    pub fn emit_node_joined(
        &self,
        job_id: &JobId,
        node_id: NodeId,
        node_class: NodeClass,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "node.joined".to_string(),
            *job_id,
            serde_json::json!({
                "node_id": node_id,
                "node_class": node_class,
                "joined_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            node_id = %node_id,
            "Emitting node.joined"
        );
        self.emit(event);
    }

    /// Emits a "node.left" event when a node stops working on a job.
    pub fn emit_node_left(
        &self,
        job_id: &JobId,
        node_id: NodeId,
        reason: &str,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "node.left".to_string(),
            *job_id,
            serde_json::json!({
                "node_id": node_id,
                "reason": reason,
                "left_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            node_id = %node_id,
            "Emitting node.left"
        );
        self.emit(event);
    }

    /// Emits a "checkpoint" event when a chunk is checkpointed.
    pub fn emit_checkpoint(
        &self,
        job_id: &JobId,
        chunk_id: ChunkId,
        progress_pct: f32,
    ) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "checkpoint".to_string(),
            *job_id,
            serde_json::json!({
                "chunk_id": chunk_id,
                "progress_pct": progress_pct,
                "checkpointed_at": Utc::now().to_rfc3339(),
            }),
        );
        debug!(
            job_id = %job_id,
            chunk_id = %chunk_id,
            progress_pct = progress_pct,
            "Emitting checkpoint"
        );
        self.emit(event);
    }

    /// Emits a "throttle_warning" event when event throughput is high.
    pub fn emit_throttle_warning(&self, job_id: &JobId, events_per_sec: f64) {
        let event = StreamEvent::new(
            self.next_event_id(),
            "throttle_warning".to_string(),
            *job_id,
            serde_json::json!({
                "events_per_sec": events_per_sec,
                "warning": "Event rate is high; some events may be dropped for lagging subscribers.",
            }),
        );
        warn!(
            job_id = %job_id,
            events_per_sec = events_per_sec,
            "Emitting throttle_warning"
        );
        self.emit(event);
    }
}

impl Default for StreamingEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for StreamingEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingEngine")
            .field("active_streams", &self.active_streams())
            .field("global_subscribers", &self.global_subscriber_count())
            .field("stats", &self.get_stats())
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn test_job_id() -> JobId {
        JobId(Uuid::new_v4())
    }

    fn test_chunk_id() -> ChunkId {
        ChunkId(Uuid::new_v4())
    }

    fn test_node_id() -> NodeId {
        NodeId(Uuid::new_v4())
    }

    // ---- StreamingConfig tests ----

    #[test]
    fn test_config_defaults() {
        let config = StreamingConfig::default();
        assert_eq!(config.keepalive_interval, Duration::from_secs(SSE_KEEPALIVE_SECS));
        assert_eq!(config.max_connections_per_job, SSE_MAX_CONNECTIONS_PER_JOB);
        assert_eq!(config.buffer_size, SSE_BUFFER_SIZE);
        assert_eq!(config.global_buffer_size, SSE_BUFFER_SIZE * 4);
        assert_eq!(config.cleanup_after_complete_secs, 120);
        assert!(config.include_retry_directive);
    }

    #[test]
    fn test_config_for_testing() {
        let config = StreamingConfig::for_testing();
        assert_eq!(config.buffer_size, 16);
        assert_eq!(config.keepalive_interval, Duration::from_millis(500));
        assert!(!config.include_retry_directive);
    }

    #[test]
    fn test_config_validate_ok() {
        let config = StreamingConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_zero_buffer() {
        let mut config = StreamingConfig::default();
        config.buffer_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_zero_global_buffer() {
        let mut config = StreamingConfig::default();
        config.global_buffer_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_zero_connections() {
        let mut config = StreamingConfig::default();
        config.max_connections_per_job = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_zero_channels() {
        let mut config = StreamingConfig::default();
        config.max_job_channels = 0;
        assert!(config.validate().is_err());
    }

    // ---- StreamEvent tests ----

    #[test]
    fn test_stream_event_new() {
        let job_id = test_job_id();
        let event = StreamEvent::new(
            "evt-1".to_string(),
            "chunk.started".to_string(),
            job_id,
            serde_json::json!({"hello": "world"}),
        );
        assert_eq!(event.id, "evt-1");
        assert_eq!(event.event_type, "chunk.started");
        assert_eq!(event.job_id, job_id);
    }

    #[test]
    fn test_stream_event_to_sse_string() {
        let job_id = test_job_id();
        let event = StreamEvent::new(
            "evt-42".to_string(),
            "progress".to_string(),
            job_id,
            serde_json::json!({"pct": 50}),
        );
        let sse = event.to_sse_string();
        assert!(sse.starts_with("id: evt-42\n"));
        assert!(sse.contains("event: progress\n"));
        assert!(sse.contains("data: "));
        assert!(sse.ends_with("\n\n"));
    }

    #[test]
    fn test_stream_event_to_json() {
        let job_id = test_job_id();
        let event = StreamEvent::new(
            "evt-1".to_string(),
            "test".to_string(),
            job_id,
            serde_json::json!({"key": "value"}),
        );
        let json = event.to_json();
        assert_eq!(json["id"], "evt-1");
        assert_eq!(json["event_type"], "test");
        assert!(json["timestamp"].is_string());
    }

    #[test]
    fn test_stream_event_event_name() {
        let event = StreamEvent::new(
            "e".to_string(),
            "chunk.failed".to_string(),
            test_job_id(),
            serde_json::json!({}),
        );
        assert_eq!(event.event_name(), "chunk.failed");
    }

    #[test]
    fn test_stream_event_is_terminal() {
        let job_id = test_job_id();

        let completed = StreamEvent::new(
            "e".to_string(),
            "job.completed".to_string(),
            job_id,
            serde_json::json!({}),
        );
        assert!(completed.is_terminal());

        let failed = StreamEvent::new(
            "e".to_string(),
            "job.failed".to_string(),
            job_id,
            serde_json::json!({}),
        );
        assert!(failed.is_terminal());

        let progress = StreamEvent::new(
            "e".to_string(),
            "progress".to_string(),
            job_id,
            serde_json::json!({}),
        );
        assert!(!progress.is_terminal());
    }

    #[test]
    fn test_stream_event_is_progress() {
        let event = StreamEvent::new(
            "e".to_string(),
            "progress".to_string(),
            test_job_id(),
            serde_json::json!({}),
        );
        assert!(event.is_progress());
    }

    #[test]
    fn test_stream_event_is_milestone() {
        let event = StreamEvent::new(
            "e".to_string(),
            "milestone".to_string(),
            test_job_id(),
            serde_json::json!({}),
        );
        assert!(event.is_milestone());
    }

    #[test]
    fn test_stream_event_sse_byte_size() {
        let event = StreamEvent::new(
            "e".to_string(),
            "test".to_string(),
            test_job_id(),
            serde_json::json!({"x": 1}),
        );
        let size = event.sse_byte_size();
        assert!(size > 0);
        assert_eq!(size, event.to_sse_string().len());
    }

    // ---- ChunkState tests ----

    #[test]
    fn test_chunk_state_queued() {
        let state = ChunkState::Queued;
        assert!(state.is_queued());
        assert!(!state.is_running());
        assert!(!state.is_terminal());
        assert_eq!(state.label(), "queued");
        assert!(state.node_id().is_none());
        assert!(state.duration_ms().is_none());
        assert!(state.error().is_none());
    }

    #[test]
    fn test_chunk_state_running() {
        let node_id = test_node_id();
        let state = ChunkState::Running {
            node_id,
            started_at: Utc::now(),
        };
        assert!(state.is_running());
        assert!(!state.is_queued());
        assert!(!state.is_terminal());
        assert_eq!(state.label(), "running");
        assert_eq!(state.node_id(), Some(node_id));
    }

    #[test]
    fn test_chunk_state_completed() {
        let node_id = test_node_id();
        let state = ChunkState::Completed {
            node_id,
            duration_ms: 1234,
        };
        assert!(state.is_terminal());
        assert!(!state.is_running());
        assert_eq!(state.label(), "completed");
        assert_eq!(state.node_id(), Some(node_id));
        assert_eq!(state.duration_ms(), Some(1234));
    }

    #[test]
    fn test_chunk_state_failed() {
        let state = ChunkState::Failed {
            error: "timeout".to_string(),
            attempts: 3,
        };
        assert!(state.is_terminal());
        assert!(!state.is_running());
        assert_eq!(state.label(), "failed");
        assert!(state.node_id().is_none());
        assert_eq!(state.error(), Some("timeout"));
        assert_eq!(state.attempts(), Some(3));
    }

    // ---- ProgressTracker tests ----

    #[test]
    fn test_tracker_new() {
        let job_id = test_job_id();
        let tracker = ProgressTracker::new(job_id, 10);
        assert_eq!(tracker.job_id, job_id);
        assert_eq!(tracker.total_chunks, 10);
        assert_eq!(tracker.completed_count(), 0);
        assert_eq!(tracker.failed_count(), 0);
        assert!(!tracker.is_complete());
    }

    #[test]
    fn test_tracker_empty_job_is_complete() {
        let tracker = ProgressTracker::new(test_job_id(), 0);
        assert!(tracker.is_complete());
        assert_eq!(tracker.completion_pct(), 100.0);
    }

    #[test]
    fn test_tracker_record_started() {
        let mut tracker = ProgressTracker::new(test_job_id(), 5);
        let chunk_id = test_chunk_id();
        let node_id = test_node_id();

        tracker.record_started(chunk_id, node_id);

        assert!(tracker.started_at.is_some());
        assert_eq!(tracker.active_node_count(), 1);
        assert!(tracker.chunk_state(&chunk_id).unwrap().is_running());
    }

    #[test]
    fn test_tracker_record_completed() {
        let mut tracker = ProgressTracker::new(test_job_id(), 5);
        let chunk_id = test_chunk_id();
        let node_id = test_node_id();

        tracker.record_started(chunk_id, node_id);
        tracker.record_completed(chunk_id, 500);

        assert_eq!(tracker.completed_count(), 1);
        assert!(tracker.last_completion_at.is_some());
        assert!(tracker.chunk_state(&chunk_id).unwrap().is_terminal());
        assert_eq!(tracker.completion_pct(), 20.0);
    }

    #[test]
    fn test_tracker_record_failed() {
        let mut tracker = ProgressTracker::new(test_job_id(), 5);
        let chunk_id = test_chunk_id();
        let node_id = test_node_id();

        tracker.record_started(chunk_id, node_id);
        tracker.record_failed(chunk_id, "OOM".to_string());

        assert_eq!(tracker.failed_count(), 1);
        assert_eq!(
            tracker.chunk_state(&chunk_id).unwrap().error(),
            Some("OOM")
        );
    }

    #[test]
    fn test_tracker_retry_increments() {
        let mut tracker = ProgressTracker::new(test_job_id(), 5);
        let chunk_id = test_chunk_id();
        let node_id = test_node_id();

        tracker.record_started(chunk_id, node_id);
        tracker.record_failed(chunk_id, "err".to_string());
        assert_eq!(tracker.retry_count(), 0);

        // Starting a previously failed chunk counts as a retry
        tracker.record_started(chunk_id, node_id);
        assert_eq!(tracker.retry_count(), 1);
    }

    #[test]
    fn test_tracker_is_complete_all_done() {
        let mut tracker = ProgressTracker::new(test_job_id(), 2);
        let c1 = test_chunk_id();
        let c2 = test_chunk_id();
        let n = test_node_id();

        tracker.record_started(c1, n);
        tracker.record_completed(c1, 100);

        tracker.record_started(c2, n);
        tracker.record_completed(c2, 200);

        assert!(tracker.is_complete());
        assert_eq!(tracker.completion_pct(), 100.0);
    }

    #[test]
    fn test_tracker_is_complete_with_failures() {
        let mut tracker = ProgressTracker::new(test_job_id(), 2);
        let c1 = test_chunk_id();
        let c2 = test_chunk_id();
        let n = test_node_id();

        tracker.record_started(c1, n);
        tracker.record_completed(c1, 100);

        tracker.record_started(c2, n);
        tracker.record_failed(c2, "err".to_string());

        assert!(tracker.is_complete());
    }

    #[test]
    fn test_tracker_throughput_no_completions() {
        let tracker = ProgressTracker::new(test_job_id(), 5);
        assert_eq!(tracker.throughput(), 0.0);
    }

    #[test]
    fn test_tracker_throughput_with_completions() {
        let mut tracker = ProgressTracker::new(test_job_id(), 10);
        let n = test_node_id();

        // Manually set started_at to compute realistic throughput
        tracker.started_at = Some(Utc::now() - chrono::Duration::seconds(10));

        for _ in 0..5 {
            let c = test_chunk_id();
            tracker.record_started(c, n);
            tracker.record_completed(c, 100);
        }
        // Override last_completion_at to get deterministic throughput
        tracker.last_completion_at = tracker.started_at.map(|s| s + chrono::Duration::seconds(10));

        let throughput = tracker.throughput();
        // 5 chunks / 10 seconds = 0.5
        assert!((throughput - 0.5).abs() < 0.01, "throughput was {}", throughput);
    }

    #[test]
    fn test_tracker_estimated_remaining() {
        let mut tracker = ProgressTracker::new(test_job_id(), 10);
        let n = test_node_id();

        // No completions -> None
        assert!(tracker.estimated_remaining().is_none());

        tracker.started_at = Some(Utc::now() - chrono::Duration::seconds(5));

        for _ in 0..5 {
            let c = test_chunk_id();
            tracker.record_started(c, n);
            tracker.record_completed(c, 100);
        }
        tracker.last_completion_at = tracker.started_at.map(|s| s + chrono::Duration::seconds(5));

        // 5 completed in 5 seconds = 1/sec, 5 remaining = ~5 seconds
        let remaining = tracker.estimated_remaining().unwrap();
        assert!(remaining.as_secs_f64() > 4.0 && remaining.as_secs_f64() < 6.0,
            "remaining was {:?}", remaining);
    }

    #[test]
    fn test_tracker_milestone_25_50_75_100() {
        let mut tracker = ProgressTracker::new(test_job_id(), 4);
        let n = test_node_id();

        // 0% -- no milestones
        assert!(!tracker.reached_milestone(25));
        assert!(!tracker.reached_milestone(50));
        assert!(!tracker.reached_milestone(75));
        assert!(!tracker.reached_milestone(100));

        // Complete 1/4 = 25%
        let c1 = test_chunk_id();
        tracker.record_started(c1, n);
        tracker.record_completed(c1, 100);
        assert!(tracker.reached_milestone(25));
        // Second call returns false (already reached)
        assert!(!tracker.reached_milestone(25));
        assert!(!tracker.reached_milestone(50));

        // Complete 2/4 = 50%
        let c2 = test_chunk_id();
        tracker.record_started(c2, n);
        tracker.record_completed(c2, 100);
        assert!(tracker.reached_milestone(50));
        assert!(!tracker.reached_milestone(50));

        // Complete 3/4 = 75%
        let c3 = test_chunk_id();
        tracker.record_started(c3, n);
        tracker.record_completed(c3, 100);
        assert!(tracker.reached_milestone(75));

        // Complete 4/4 = 100%
        let c4 = test_chunk_id();
        tracker.record_started(c4, n);
        tracker.record_completed(c4, 100);
        assert!(tracker.reached_milestone(100));
    }

    #[test]
    fn test_tracker_milestone_not_yet_crossed() {
        let mut tracker = ProgressTracker::new(test_job_id(), 10);
        let n = test_node_id();

        // Complete 2/10 = 20%, not enough for 25%
        for _ in 0..2 {
            let c = test_chunk_id();
            tracker.record_started(c, n);
            tracker.record_completed(c, 100);
        }
        assert!(!tracker.reached_milestone(25));
    }

    #[test]
    fn test_tracker_milestones_reached_vec() {
        let mut tracker = ProgressTracker::new(test_job_id(), 2);
        let n = test_node_id();

        let c1 = test_chunk_id();
        tracker.record_started(c1, n);
        tracker.record_completed(c1, 100);
        tracker.reached_milestone(50);

        assert_eq!(tracker.milestones_reached(), &[50]);
    }

    #[test]
    fn test_tracker_compute_progress() {
        let mut tracker = ProgressTracker::new(test_job_id(), 4);
        let n = test_node_id();

        let c1 = test_chunk_id();
        let c2 = test_chunk_id();
        let c3 = test_chunk_id();

        tracker.record_started_with_class(c1, n, NodeClass::Standard);
        tracker.record_completed(c1, 100);

        tracker.record_started_with_class(c2, n, NodeClass::Standard);
        tracker.record_started_with_class(c3, n, NodeClass::Light);

        let progress = tracker.compute_progress();
        assert_eq!(progress.total_chunks, 4);
        assert_eq!(progress.completed_chunks, 1);
        assert_eq!(progress.running_chunks, 2);
        assert_eq!(progress.completion_pct, 25.0);
    }

    #[test]
    fn test_tracker_avg_duration() {
        let mut tracker = ProgressTracker::new(test_job_id(), 3);
        let n = test_node_id();

        assert!(tracker.avg_duration_ms().is_none());

        let c1 = test_chunk_id();
        let c2 = test_chunk_id();
        tracker.record_started(c1, n);
        tracker.record_completed(c1, 100);
        tracker.record_started(c2, n);
        tracker.record_completed(c2, 300);

        assert_eq!(tracker.avg_duration_ms(), Some(200));
    }

    #[test]
    fn test_tracker_all_participating_nodes() {
        let mut tracker = ProgressTracker::new(test_job_id(), 3);
        let n1 = test_node_id();
        let n2 = test_node_id();

        let c1 = test_chunk_id();
        let c2 = test_chunk_id();
        tracker.record_started(c1, n1);
        tracker.record_started(c2, n2);
        tracker.record_completed(c1, 100);

        let nodes = tracker.all_participating_nodes();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_tracker_state_counts() {
        let mut tracker = ProgressTracker::new(test_job_id(), 5);
        let n = test_node_id();

        let c1 = test_chunk_id();
        let c2 = test_chunk_id();
        let c3 = test_chunk_id();

        tracker.chunk_states.insert(c1, ChunkState::Queued);
        tracker.record_started(c2, n);
        tracker.record_started(c3, n);
        tracker.record_completed(c3, 100);

        let (queued, running, completed, failed) = tracker.state_counts();
        assert_eq!(queued, 1);
        assert_eq!(running, 1);
        assert_eq!(completed, 1);
        assert_eq!(failed, 0);
    }

    #[test]
    fn test_tracker_reset() {
        let mut tracker = ProgressTracker::new(test_job_id(), 5);
        let n = test_node_id();

        let c1 = test_chunk_id();
        tracker.record_started(c1, n);
        tracker.record_completed(c1, 100);
        tracker.add_cost(1.0);
        tracker.reached_milestone(20);

        tracker.reset();

        assert_eq!(tracker.completed_count(), 0);
        assert_eq!(tracker.failed_count(), 0);
        assert!(tracker.started_at.is_none());
        assert!(tracker.milestones_reached().is_empty());
        assert_eq!(tracker.chunk_states.len(), 0);
        assert!(!tracker.is_complete());
    }

    // ---- SseFormatter tests ----

    #[test]
    fn test_sse_format_event() {
        let event = StreamEvent::new(
            "evt-1".to_string(),
            "test".to_string(),
            test_job_id(),
            serde_json::json!({"x": 1}),
        );
        let formatted = SseFormatter::format_event(&event);
        assert!(formatted.starts_with("id: evt-1\n"));
        assert!(formatted.contains("event: test\n"));
        assert!(formatted.contains("data: "));
        assert!(formatted.ends_with("\n\n"));
    }

    #[test]
    fn test_sse_format_keepalive() {
        let ka = SseFormatter::format_keepalive();
        assert_eq!(ka, ": keepalive\n\n");
    }

    #[test]
    fn test_sse_format_retry() {
        let retry = SseFormatter::format_retry(3000);
        assert_eq!(retry, "retry: 3000\n\n");
    }

    #[test]
    fn test_sse_format_comment_single_line() {
        let comment = SseFormatter::format_comment("hello world");
        assert_eq!(comment, ": hello world\n\n");
    }

    #[test]
    fn test_sse_format_comment_multi_line() {
        let comment = SseFormatter::format_comment("line one\nline two");
        assert_eq!(comment, ": line one\n: line two\n\n");
    }

    #[test]
    fn test_sse_format_data_only() {
        let data = SseFormatter::format_data_only(&serde_json::json!({"key": "val"}));
        assert!(data.starts_with("data: "));
        assert!(data.ends_with("\n\n"));
        assert!(data.contains("\"key\""));
    }

    #[test]
    fn test_sse_format_named_event() {
        let msg = SseFormatter::format_named_event("ping", &serde_json::json!({}));
        assert!(msg.starts_with("event: ping\n"));
        assert!(msg.contains("data: "));
        assert!(msg.ends_with("\n\n"));
    }

    #[test]
    fn test_sse_format_stream_header() {
        let job_id = test_job_id();
        let header = SseFormatter::format_stream_header(&job_id, Some(5000));
        assert!(header.contains("connected to job stream"));
        assert!(header.contains("retry: 5000"));
    }

    #[test]
    fn test_sse_format_stream_header_no_retry() {
        let job_id = test_job_id();
        let header = SseFormatter::format_stream_header(&job_id, None);
        assert!(header.contains("connected to job stream"));
        assert!(!header.contains("retry:"));
    }

    #[test]
    fn test_sse_is_valid_sse() {
        assert!(SseFormatter::is_valid_sse(": keepalive\n\n"));
        assert!(SseFormatter::is_valid_sse("data: hello\n\n"));
        assert!(!SseFormatter::is_valid_sse("data: hello\n")); // missing double newline
    }

    // ---- JobProgress tests ----

    #[test]
    fn test_job_progress_is_finished() {
        let mut p = JobProgress::default();
        p.total_chunks = 10;
        p.completed_chunks = 7;
        p.failed_chunks = 3;
        assert!(p.is_finished());
    }

    #[test]
    fn test_job_progress_not_finished() {
        let mut p = JobProgress::default();
        p.total_chunks = 10;
        p.completed_chunks = 5;
        p.failed_chunks = 2;
        assert!(!p.is_finished());
    }

    #[test]
    fn test_job_progress_success_rate() {
        let mut p = JobProgress::default();
        p.total_chunks = 10;
        p.completed_chunks = 8;
        assert!((p.success_rate() - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_job_progress_failure_rate() {
        let mut p = JobProgress::default();
        p.total_chunks = 10;
        p.failed_chunks = 3;
        assert!((p.failure_rate() - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_job_progress_zero_chunks() {
        let p = JobProgress::default();
        assert_eq!(p.success_rate(), 0.0);
        assert_eq!(p.failure_rate(), 0.0);
    }

    #[test]
    fn test_job_progress_active_chunks() {
        let mut p = JobProgress::default();
        p.running_chunks = 3;
        p.queued_chunks = 7;
        assert_eq!(p.active_chunks(), 10);
    }

    #[test]
    fn test_job_progress_dominant_class() {
        let mut p = JobProgress::default();
        p.nodes_by_class.insert(NodeClass::Edge, 2);
        p.nodes_by_class.insert(NodeClass::Standard, 5);
        p.nodes_by_class.insert(NodeClass::Enterprise, 1);
        assert_eq!(p.dominant_node_class(), Some(NodeClass::Standard));
    }

    #[test]
    fn test_job_progress_dominant_class_empty() {
        let p = JobProgress::default();
        assert_eq!(p.dominant_node_class(), None);
    }

    #[test]
    fn test_job_progress_to_json() {
        let mut p = JobProgress::default();
        p.total_chunks = 10;
        p.completion_pct = 50.0;
        let json = p.to_json();
        assert_eq!(json["total_chunks"], 10);
        assert_eq!(json["completion_pct"], 50.0);
    }

    // ---- JobCompletionSummary tests ----

    #[test]
    fn test_completion_summary_success_rate() {
        let summary = JobCompletionSummary {
            job_id: test_job_id(),
            total_chunks: 10,
            completed: 9,
            failed: 1,
            retried: 2,
            duration_secs: 30.0,
            total_cost_usd: 0.50,
            nodes_used: 3,
            verification_stats: serde_json::json!({}),
            result_size_bytes: 1024,
        };
        assert!((summary.success_rate() - 0.9).abs() < f64::EPSILON);
        assert!(!summary.is_fully_successful());
    }

    #[test]
    fn test_completion_summary_fully_successful() {
        let summary = JobCompletionSummary {
            job_id: test_job_id(),
            total_chunks: 5,
            completed: 5,
            failed: 0,
            retried: 0,
            duration_secs: 10.0,
            total_cost_usd: 0.10,
            nodes_used: 2,
            verification_stats: serde_json::json!({}),
            result_size_bytes: 512,
        };
        assert!(summary.is_fully_successful());
    }

    #[test]
    fn test_completion_summary_cost_per_chunk() {
        let summary = JobCompletionSummary {
            job_id: test_job_id(),
            total_chunks: 10,
            completed: 10,
            failed: 0,
            retried: 0,
            duration_secs: 20.0,
            total_cost_usd: 1.00,
            nodes_used: 5,
            verification_stats: serde_json::json!({}),
            result_size_bytes: 2048,
        };
        assert!((summary.cost_per_chunk() - 0.10).abs() < f64::EPSILON);
    }

    #[test]
    fn test_completion_summary_avg_chunk_duration() {
        let summary = JobCompletionSummary {
            job_id: test_job_id(),
            total_chunks: 4,
            completed: 4,
            failed: 0,
            retried: 0,
            duration_secs: 20.0,
            total_cost_usd: 0.10,
            nodes_used: 2,
            verification_stats: serde_json::json!({}),
            result_size_bytes: 512,
        };
        assert!((summary.avg_chunk_duration_secs() - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_completion_summary_zero_chunks() {
        let summary = JobCompletionSummary {
            job_id: test_job_id(),
            total_chunks: 0,
            completed: 0,
            failed: 0,
            retried: 0,
            duration_secs: 0.0,
            total_cost_usd: 0.0,
            nodes_used: 0,
            verification_stats: serde_json::json!({}),
            result_size_bytes: 0,
        };
        assert_eq!(summary.success_rate(), 0.0);
        assert_eq!(summary.cost_per_chunk(), 0.0);
        assert_eq!(summary.avg_chunk_duration_secs(), 0.0);
    }

    // ---- StreamingStats tests ----

    #[test]
    fn test_stats_default() {
        let stats = StreamingStats::default();
        assert_eq!(stats.total_events_emitted, 0);
        assert_eq!(stats.active_subscriptions, 0);
        assert_eq!(stats.events_dropped, 0);
        assert_eq!(stats.drop_rate(), 0.0);
    }

    #[test]
    fn test_stats_drop_rate() {
        let stats = StreamingStats {
            total_events_emitted: 100,
            events_dropped: 10,
            ..Default::default()
        };
        assert!((stats.drop_rate() - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stats_is_dropping_above() {
        let stats = StreamingStats {
            total_events_emitted: 100,
            events_dropped: 20,
            ..Default::default()
        };
        assert!(stats.is_dropping_above(0.1));
        assert!(!stats.is_dropping_above(0.3));
    }

    #[test]
    fn test_stats_avg_event_size() {
        let stats = StreamingStats {
            total_events_emitted: 10,
            total_bytes_emitted: 1000,
            ..Default::default()
        };
        assert_eq!(stats.avg_event_size_bytes(), 100);
    }

    #[test]
    fn test_stats_avg_event_size_zero() {
        let stats = StreamingStats::default();
        assert_eq!(stats.avg_event_size_bytes(), 0);
    }

    #[test]
    fn test_stats_reset() {
        let mut stats = StreamingStats {
            total_events_emitted: 100,
            events_dropped: 5,
            total_bytes_emitted: 50000,
            ..Default::default()
        };
        stats.reset();
        assert_eq!(stats.total_events_emitted, 0);
        assert_eq!(stats.events_dropped, 0);
    }

    #[test]
    fn test_stats_to_json() {
        let stats = StreamingStats {
            total_events_emitted: 42,
            ..Default::default()
        };
        let json = stats.to_json();
        assert_eq!(json["total_events_emitted"], 42);
    }

    // ---- StreamingEngine tests ----

    #[test]
    fn test_engine_new() {
        let engine = StreamingEngine::new();
        assert_eq!(engine.active_streams(), 0);
        assert_eq!(engine.global_subscriber_count(), 0);
    }

    #[test]
    fn test_engine_with_config() {
        let config = StreamingConfig::for_testing();
        let engine = StreamingEngine::with_config(config.clone());
        assert_eq!(engine.config().buffer_size, 16);
    }

    #[test]
    fn test_engine_subscribe_job() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();

        let _rx = engine.subscribe_job(&job_id);
        assert_eq!(engine.active_streams(), 1);
        assert_eq!(engine.subscriber_count(&job_id), 1);
    }

    #[test]
    fn test_engine_subscribe_global() {
        let engine = StreamingEngine::new();

        let _rx = engine.subscribe_global();
        assert_eq!(engine.global_subscriber_count(), 1);
    }

    #[test]
    fn test_engine_emit_chunk_started() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_chunk_started(&job_id, test_chunk_id(), test_node_id(), NodeClass::Standard);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "chunk.started");
        assert_eq!(event.job_id, job_id);
    }

    #[test]
    fn test_engine_emit_chunk_completed() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_chunk_completed(&job_id, test_chunk_id(), test_node_id(), 1234, 5678);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "chunk.completed");
        assert_eq!(event.data["duration_ms"], 1234);
        assert_eq!(event.data["result_size"], 5678);
    }

    #[test]
    fn test_engine_emit_chunk_failed() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_chunk_failed(&job_id, test_chunk_id(), test_node_id(), "OOM".into(), true);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "chunk.failed");
        assert_eq!(event.data["error"], "OOM");
        assert_eq!(event.data["will_retry"], true);
    }

    #[test]
    fn test_engine_emit_progress() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        let progress = JobProgress {
            job_id,
            total_chunks: 10,
            completed_chunks: 5,
            completion_pct: 50.0,
            ..Default::default()
        };
        engine.emit_progress(&job_id, progress);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "progress");
        assert_eq!(event.data["total_chunks"], 10);
    }

    #[test]
    fn test_engine_emit_milestone() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_milestone(&job_id, 50);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "milestone");
        assert_eq!(event.data["milestone_pct"], 50);
    }

    #[test]
    fn test_engine_emit_job_completed() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        let summary = JobCompletionSummary {
            job_id,
            total_chunks: 10,
            completed: 10,
            failed: 0,
            retried: 1,
            duration_secs: 60.0,
            total_cost_usd: 0.20,
            nodes_used: 3,
            verification_stats: serde_json::json!({}),
            result_size_bytes: 4096,
        };
        engine.emit_job_completed(&job_id, summary);

        let event = rx.try_recv().unwrap();
        assert!(event.is_terminal());
        assert_eq!(event.event_type, "job.completed");
    }

    #[test]
    fn test_engine_emit_job_failed() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_job_failed(&job_id, "Too many failures".into());

        let event = rx.try_recv().unwrap();
        assert!(event.is_terminal());
        assert_eq!(event.event_type, "job.failed");
        assert_eq!(event.data["error"], "Too many failures");
    }

    #[test]
    fn test_engine_emit_partial_result() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_partial_result(&job_id, test_chunk_id(), serde_json::json!({"result": 42}));

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "partial_result");
        assert_eq!(event.data["result_preview"]["result"], 42);
    }

    #[test]
    fn test_engine_emit_verification_event() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_verification_event(&job_id, test_chunk_id(), "verified".into());

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "verification");
        assert_eq!(event.data["outcome"], "verified");
    }

    #[test]
    fn test_engine_emit_cost_update() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_cost_update(&job_id, 0.15, 0.05);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "cost_update");
        assert_eq!(event.data["current_cost_usd"], 0.15);
        assert_eq!(event.data["estimated_remaining_usd"], 0.05);
        assert_eq!(event.data["estimated_total_usd"], 0.20);
    }

    #[test]
    fn test_engine_cleanup_job() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();

        let _rx = engine.subscribe_job(&job_id);
        assert_eq!(engine.active_streams(), 1);

        engine.cleanup_job(&job_id);
        assert_eq!(engine.active_streams(), 0);
        assert_eq!(engine.subscriber_count(&job_id), 0);
    }

    #[test]
    fn test_engine_cleanup_nonexistent_job() {
        let engine = StreamingEngine::new();
        engine.cleanup_job(&test_job_id()); // should not panic
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn test_engine_subscriber_count_no_channel() {
        let engine = StreamingEngine::new();
        assert_eq!(engine.subscriber_count(&test_job_id()), 0);
    }

    #[test]
    fn test_engine_stats() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let _rx = engine.subscribe_job(&job_id);

        engine.emit_chunk_started(&job_id, test_chunk_id(), test_node_id(), NodeClass::Edge);

        let stats = engine.get_stats();
        assert_eq!(stats.total_events_emitted, 1);
        assert_eq!(stats.total_subscriptions_created, 1);
    }

    #[test]
    fn test_engine_global_receives_job_events() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut global_rx = engine.subscribe_global();

        engine.emit_milestone(&job_id, 75);

        let event = global_rx.try_recv().unwrap();
        assert_eq!(event.event_type, "milestone");
        assert_eq!(event.job_id, job_id);
    }

    #[test]
    fn test_engine_multiple_jobs() {
        let engine = StreamingEngine::new();
        let job1 = test_job_id();
        let job2 = test_job_id();

        let mut rx1 = engine.subscribe_job(&job1);
        let mut rx2 = engine.subscribe_job(&job2);

        engine.emit_milestone(&job1, 50);
        engine.emit_milestone(&job2, 75);

        let e1 = rx1.try_recv().unwrap();
        assert_eq!(e1.data["milestone_pct"], 50);

        let e2 = rx2.try_recv().unwrap();
        assert_eq!(e2.data["milestone_pct"], 75);

        // rx1 should NOT have received job2's event
        assert!(rx1.try_recv().is_err());
    }

    #[test]
    fn test_engine_emit_custom() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_custom(&job_id, "my.event", serde_json::json!({"custom": true}));

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "my.event");
        assert_eq!(event.data["custom"], true);
    }

    #[test]
    fn test_engine_active_job_ids() {
        let engine = StreamingEngine::new();
        let job1 = test_job_id();
        let job2 = test_job_id();

        let _rx1 = engine.subscribe_job(&job1);
        let _rx2 = engine.subscribe_job(&job2);

        let ids = engine.active_job_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&job1));
        assert!(ids.contains(&job2));
    }

    #[test]
    fn test_engine_has_subscribers() {
        let engine = StreamingEngine::new();
        assert!(!engine.has_subscribers());

        let _rx = engine.subscribe_global();
        assert!(engine.has_subscribers());
    }

    #[test]
    fn test_engine_record_unsubscribe() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let _rx = engine.subscribe_job(&job_id);

        let stats_before = engine.get_stats();
        assert_eq!(stats_before.active_subscriptions, 1);

        engine.record_unsubscribe();
        let stats_after = engine.get_stats();
        assert_eq!(stats_after.active_subscriptions, 0);
    }

    #[test]
    fn test_engine_record_drop() {
        let engine = StreamingEngine::new();
        engine.record_drop();
        engine.record_drop();

        let stats = engine.get_stats();
        assert_eq!(stats.events_dropped, 2);
    }

    #[test]
    fn test_engine_cleanup_idle_channels() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();

        // Create a channel but drop the receiver
        {
            let _rx = engine.subscribe_job(&job_id);
        }
        // Receiver is dropped, so subscriber_count should be 0
        assert_eq!(engine.subscriber_count(&job_id), 0);

        let removed = engine.cleanup_idle_channels();
        assert_eq!(removed, 1);
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn test_engine_emit_job_started() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_job_started(&job_id, 42);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "job.started");
        assert_eq!(event.data["total_chunks"], 42);
    }

    #[test]
    fn test_engine_emit_chunk_reassigned() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);
        let old = test_node_id();
        let new = test_node_id();

        engine.emit_chunk_reassigned(&job_id, test_chunk_id(), old, new, "node_dead");

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "chunk.reassigned");
        assert_eq!(event.data["reason"], "node_dead");
    }

    #[test]
    fn test_engine_emit_checkpoint() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_checkpoint(&job_id, test_chunk_id(), 33.3);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, "checkpoint");
    }

    #[test]
    fn test_engine_debug_format() {
        let engine = StreamingEngine::new();
        let debug_str = format!("{:?}", engine);
        assert!(debug_str.contains("StreamingEngine"));
        assert!(debug_str.contains("active_streams"));
    }

    #[test]
    fn test_engine_default_impl() {
        let engine = StreamingEngine::default();
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn test_engine_event_ids_are_unique() {
        let engine = StreamingEngine::new();
        let job_id = test_job_id();
        let mut rx = engine.subscribe_job(&job_id);

        engine.emit_milestone(&job_id, 25);
        engine.emit_milestone(&job_id, 50);

        let e1 = rx.try_recv().unwrap();
        let e2 = rx.try_recv().unwrap();
        assert_ne!(e1.id, e2.id);
    }
}
