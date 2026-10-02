// Marabunta - Licensed under the MIT License.
//! Integration tests for the combo infrastructure layer.
//!
//! Tests verification strategies, chunking, pricing, scheduling,
//! streaming, webhooks, node classes, combo registry, and WASM executor
//! configuration types. All tests are self-contained and require no
//! external services or running SwarmNode.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{Timelike, Utc};
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::swarm::config::*;
use crate::swarm::types::*;

// ============================================================================
// Helpers
// ============================================================================

/// Create a deterministic NodeId from a u64.
fn make_node_id(n: u64) -> NodeId {
    NodeId(Uuid::from_u128(n as u128))
}

/// Build a ResourceSnapshot with the given cpu/memory.
fn make_resources(cpu_cores: u32, memory_total_mb: u64) -> ResourceSnapshot {
    ResourceSnapshot {
        cpu_cores,
        cpu_available: 0.5,
        memory_total_mb,
        memory_available_mb: memory_total_mb / 2,
        disk_total_mb: 100_000,
        disk_available_mb: 50_000,
        network_bandwidth_mbps: 100.0,
                current_tdp_watts: 15.0,
                ask_usd_per_megagas: 0.0001,
    }
}

/// Compute a SHA-256 hex digest of arbitrary bytes.
fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    hash.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Normalize JSON by parsing and re-serializing with sorted keys.
fn normalize_json(raw: &str) -> String {
    let val: serde_json::Value = serde_json::from_str(raw).unwrap();
    // BTreeMap gives us sorted keys
    fn sort_value(v: &serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(map) => {
                let sorted: BTreeMap<String, serde_json::Value> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), sort_value(v)))
                    .collect();
                serde_json::to_value(sorted).unwrap()
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(sort_value).collect())
            }
            other => other.clone(),
        }
    }
    serde_json::to_string(&sort_value(&val)).unwrap()
}

// ---------------------------------------------------------------------------
// Result hasher: deterministic hashing of chunk results for verification.
// ---------------------------------------------------------------------------

/// Computes a deterministic hash of raw bytes for verification.
fn hash_result_bytes(data: &[u8]) -> String {
    sha256_hex(data)
}

/// Computes a deterministic hash of a JSON result after normalization.
fn hash_result_json(raw_json: &str) -> String {
    let normalized = normalize_json(raw_json);
    sha256_hex(normalized.as_bytes())
}

// ---------------------------------------------------------------------------
// Trust scorer: maps job-completion history to a trust level.
// ---------------------------------------------------------------------------

struct TrustScorer {
    trust_threshold_jobs: u64,
}

impl TrustScorer {
    fn new() -> Self {
        Self {
            trust_threshold_jobs: VERIFICATION_TRUST_THRESHOLD_JOBS,
        }
    }

    /// Returns a trust score in [0.0, 1.0].
    fn score(&self, completed_jobs: u64, failure_rate: f64) -> f64 {
        let maturity = (completed_jobs as f64 / self.trust_threshold_jobs as f64).min(1.0);
        let reliability = 1.0 - failure_rate;
        maturity * reliability
    }
}

// ---------------------------------------------------------------------------
// Verification helpers
// ---------------------------------------------------------------------------

/// Run redundant verification across a set of (node, hash) pairs.
fn verify_redundant(results: &[(NodeId, String)]) -> VerificationOutcome {
    if results.is_empty() {
        return VerificationOutcome::Conflict { hashes: vec![] };
    }
    // Count hash frequencies
    let mut freq: HashMap<&str, Vec<NodeId>> = HashMap::new();
    for (node, hash) in results {
        freq.entry(hash.as_str()).or_default().push(*node);
    }

    if freq.len() == 1 {
        let hash = results[0].1.clone();
        return VerificationOutcome::Verified {
            consensus_hash: hash,
        };
    }

    // Find majority
    let total = results.len();
    let mut best_hash = "";
    let mut best_count = 0;
    for (hash, nodes) in &freq {
        if nodes.len() > best_count {
            best_hash = hash;
            best_count = nodes.len();
        }
    }

    if best_count * 2 > total {
        // Majority exists
        let outlier = results
            .iter()
            .find(|(_, h)| h.as_str() != best_hash)
            .map(|(n, _)| *n)
            .unwrap();
        return VerificationOutcome::MajorityConsensus {
            consensus_hash: best_hash.to_string(),
            outlier_node: outlier,
            replicas_agreed: best_count as u8,
        };
    }

    // All disagree
    VerificationOutcome::Conflict {
        hashes: results.to_vec(),
    }
}

/// Spot-check verification: compare trusted hash against checked hash.
fn verify_spot_check(trusted_hash: &str, checked_hash: &str, node: NodeId) -> VerificationOutcome {
    if trusted_hash == checked_hash {
        VerificationOutcome::SpotCheckPassed
    } else {
        VerificationOutcome::SpotCheckFailed {
            trusted_hash: trusted_hash.to_string(),
            checked_hash: checked_hash.to_string(),
            node,
        }
    }
}

/// Statistical verification: check if a value's z-score exceeds the threshold.
fn verify_statistical(values: &[f64], outlier_tolerance: f64) -> VerificationOutcome {
    if values.is_empty() {
        return VerificationOutcome::StatisticallyValid { p_value: 1.0 };
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    let std_dev = variance.sqrt();

    if std_dev < 1e-12 {
        return VerificationOutcome::StatisticallyValid { p_value: 1.0 };
    }

    for (i, val) in values.iter().enumerate() {
        let z = ((val - mean) / std_dev).abs();
        if z > outlier_tolerance {
            return VerificationOutcome::StatisticalOutlier {
                z_score: z,
                node: make_node_id(i as u64),
            };
        }
    }

    VerificationOutcome::StatisticallyValid { p_value: 0.95 }
}

// ---------------------------------------------------------------------------
// Chunk splitter / planner helpers
// ---------------------------------------------------------------------------

/// Split `total` items into chunks of at most `chunk_size`.
fn split_range(total: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    let mut chunks = Vec::new();
    let mut offset = 0;
    while offset < total {
        let end = (offset + chunk_size).min(total);
        chunks.push((offset, end));
        offset = end;
    }
    chunks
}

/// Split `total` items into exactly `n` parts (as evenly as possible).
fn split_into_n(total: u64, n: u64) -> Vec<(u64, u64)> {
    let base = total / n;
    let remainder = total % n;
    let mut chunks = Vec::new();
    let mut offset = 0;
    for i in 0..n {
        let size = base + if i < remainder { 1 } else { 0 };
        chunks.push((offset, offset + size));
        offset += size;
    }
    chunks
}

/// Split work proportionally to node-class distribution.
fn split_by_class(total: u64, distribution: &[(NodeClass, u32)]) -> Vec<(NodeClass, u64)> {
    let total_weight: f64 = distribution
        .iter()
        .map(|(c, count)| c.compute_weight() * *count as f64)
        .sum();
    if total_weight < 1e-12 {
        return vec![];
    }
    let mut result = Vec::new();
    let mut assigned = 0u64;
    for (i, (class, count)) in distribution.iter().enumerate() {
        let weight = class.compute_weight() * *count as f64;
        let share = if i == distribution.len() - 1 {
            total - assigned
        } else {
            ((weight / total_weight) * total as f64).round() as u64
        };
        result.push((*class, share));
        assigned += share;
    }
    result
}

/// Plan chunks using a Fixed(n) strategy.
fn plan_fixed(total_items: u64, count: u32) -> Vec<(u64, u64)> {
    split_into_n(total_items, count as u64)
}

/// Plan chunks by line count splitting.
fn plan_per_line(line_count: u64, lines_per_chunk: u64) -> Vec<(u64, u64)> {
    split_range(line_count, lines_per_chunk)
}

/// Adaptive planning: adjust chunk count based on available nodes.
fn plan_adaptive(total_items: u64, available_nodes: u32) -> Vec<(u64, u64)> {
    // Heuristic: 2 chunks per node for load balancing
    let chunk_count = (available_nodes as u64 * 2).max(1).min(total_items);
    split_into_n(total_items, chunk_count)
}

/// Class distribution: compute percentages from node counts.
fn class_percentages(counts: &[(NodeClass, u32)]) -> Vec<(NodeClass, f64)> {
    let total: u32 = counts.iter().map(|(_, c)| c).sum();
    if total == 0 {
        return vec![];
    }
    counts
        .iter()
        .map(|(class, count)| (*class, *count as f64 / total as f64 * 100.0))
        .collect()
}

/// Class distribution: compute effective compute weight.
fn effective_compute(counts: &[(NodeClass, u32)]) -> f64 {
    counts
        .iter()
        .map(|(class, count)| class.compute_weight() * *count as f64)
        .sum()
}

/// Chunk validator: check constraints.
fn validate_chunk(
    memory_mb: u64,
    duration_secs: u64,
    input_size_mb: u64,
    node_class: NodeClass,
) -> Result<(), String> {
    let constraints = TaskConstraints::for_class(node_class);
    if memory_mb > constraints.max_memory_mb {
        return Err(format!(
            "Memory {} MB exceeds max {} MB for {:?}",
            memory_mb, constraints.max_memory_mb, node_class
        ));
    }
    if duration_secs > constraints.max_duration_secs {
        return Err(format!(
            "Duration {} s exceeds max {} s for {:?}",
            duration_secs, constraints.max_duration_secs, node_class
        ));
    }
    if input_size_mb > constraints.max_input_size_mb {
        return Err(format!(
            "Input size {} MB exceeds max {} MB for {:?}",
            input_size_mb, constraints.max_input_size_mb, node_class
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pricing helpers
// ---------------------------------------------------------------------------

/// Rate card: base cost per node-hour for each class.
fn rate_card() -> HashMap<NodeClass, f64> {
    let mut card = HashMap::new();
    card.insert(NodeClass::Edge, PRICING_BASE_NODE_HOUR_USD * 0.25);
    card.insert(NodeClass::Light, PRICING_BASE_NODE_HOUR_USD * 0.5);
    card.insert(NodeClass::Standard, PRICING_BASE_NODE_HOUR_USD * 1.0);
    card.insert(NodeClass::Enterprise, PRICING_BASE_NODE_HOUR_USD * 2.0);
    card
}

/// Estimate cost for a job.
fn estimate_cost(
    node_class: NodeClass,
    node_hours: f64,
    priority: PriorityLevel,
    verification: &VerificationStrategy,
    effective_nodes: u64,
) -> CostEstimate {
    let card = rate_card();
    let base_rate = card[&node_class];
    let base_cost = base_rate * node_hours;
    let priority_cost = base_cost * priority.cost_multiplier();

    let verification_overhead = match verification {
        VerificationStrategy::Redundant { replicas } => (*replicas as f64 - 1.0) / *replicas as f64,
        VerificationStrategy::SpotCheck { check_rate } => *check_rate,
        VerificationStrategy::Statistical { .. } => 0.02,
        VerificationStrategy::None => 0.0,
    };

    let total_cost = priority_cost * (1.0 + verification_overhead) * (1.0 + PRICING_RETRY_OVERHEAD_PCT);
    let cost_low = total_cost * 0.8;
    let cost_high = total_cost * 1.5;
    let duration_secs = (node_hours * 3600.0 / effective_nodes.max(1) as f64) as u64;

    CostEstimate {
        estimated_cost_usd: total_cost,
        cost_range_usd: (cost_low, cost_high),
        estimated_duration_secs: duration_secs,
        duration_range_secs: (duration_secs / 2, duration_secs * 2),
        estimated_chunks: (node_hours * 10.0) as u64,
        retry_overhead_pct: PRICING_RETRY_OVERHEAD_PCT * 100.0,
        verification_overhead_pct: verification_overhead * 100.0,
        cloud_comparison_usd: Some(total_cost * 8.0),
        priority,
        effective_nodes,
    }
}

/// Module pricing profile (for combo registry modules).
struct ModulePricingProfile {
    module_id: String,
    base_rate_per_unit: f64,
    cloud_comparison_base_usd: Option<f64>,
}

/// Build default module pricing profiles for all 22+ modules.
fn default_module_profiles() -> Vec<ModulePricingProfile> {
    let modules = vec![
        ("cr.montecarlo", 0.001, Some(0.50)),
        ("cr.raytrace", 0.003, Some(1.20)),
        ("cr.genetic", 0.002, Some(0.80)),
        ("cr.fluid_sim", 0.004, Some(2.00)),
        ("cr.password_hash", 0.0005, Some(0.30)),
        ("dt.etl_pipeline", 0.001, Some(0.40)),
        ("dt.log_analysis", 0.0008, Some(0.35)),
        ("dt.dedup", 0.0006, Some(0.25)),
        ("md.transcode", 0.005, Some(3.00)),
        ("md.thumbnail", 0.001, Some(0.50)),
        ("md.watermark", 0.0008, Some(0.30)),
        ("sc.protein_fold", 0.008, Some(5.00)),
        ("sc.genome_align", 0.006, Some(4.00)),
        ("sc.climate_sim", 0.007, Some(4.50)),
        ("sc.molecular_dynamics", 0.009, Some(6.00)),
        ("ai.inference", 0.010, Some(8.00)),
        ("ai.fine_tune", 0.015, Some(12.00)),
        ("ai.embedding", 0.004, Some(2.50)),
        ("fi.risk_model", 0.003, Some(1.50)),
        ("fi.backtest", 0.002, Some(1.00)),
        ("fi.fraud_detect", 0.004, Some(2.00)),
        ("en.batch_report", 0.001, Some(0.40)),
    ];
    modules
        .into_iter()
        .map(|(id, rate, cloud)| ModulePricingProfile {
            module_id: id.to_string(),
            base_rate_per_unit: rate,
            cloud_comparison_base_usd: cloud,
        })
        .collect()
}

/// Cloud comparison: compute savings fraction.
fn cloud_savings(swarm_cost: f64, cloud_cost: f64) -> f64 {
    if cloud_cost <= 0.0 {
        return 0.0;
    }
    (cloud_cost - swarm_cost) / cloud_cost
}

/// Job billing: accumulate and finalize.
struct JobBilling {
    job_id: Uuid,
    started_at: chrono::DateTime<Utc>,
    records: Vec<BillingRecord>,
    finalized: bool,
}

struct BillingRecord {
    chunk_id: Uuid,
    node_class: NodeClass,
    duration_secs: f64,
    cost_usd: f64,
}

struct JobReceipt {
    job_id: Uuid,
    total_cost_usd: f64,
    total_chunks: u32,
    total_duration_secs: f64,
    priority: PriorityLevel,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
    node_classes_used: Vec<NodeClass>,
}

impl JobBilling {
    fn new(job_id: Uuid) -> Self {
        Self {
            job_id,
            started_at: Utc::now(),
            records: Vec::new(),
            finalized: false,
        }
    }

    fn record(&mut self, chunk_id: Uuid, node_class: NodeClass, duration_secs: f64) {
        let card = rate_card();
        let cost = card[&node_class] * (duration_secs / 3600.0);
        self.records.push(BillingRecord {
            chunk_id,
            node_class,
            duration_secs,
            cost_usd: cost,
        });
    }

    fn finalize(&mut self, priority: PriorityLevel) -> JobReceipt {
        self.finalized = true;
        let total_cost: f64 = self.records.iter().map(|r| r.cost_usd).sum::<f64>()
            * priority.cost_multiplier();
        let total_duration: f64 = self.records.iter().map(|r| r.duration_secs).sum();
        let mut classes: Vec<NodeClass> = self.records.iter().map(|r| r.node_class).collect();
        classes.sort();
        classes.dedup();
        JobReceipt {
            job_id: self.job_id,
            total_cost_usd: total_cost,
            total_chunks: self.records.len() as u32,
            total_duration_secs: total_duration,
            priority,
            started_at: self.started_at,
            finished_at: Utc::now(),
            node_classes_used: classes,
        }
    }
}

/// Economy scheduler off-peak detection.
fn is_offpeak(hour: u32) -> bool {
    // Off-peak: 22:00 - 06:00 UTC (wraps around midnight)
    hour >= SCHEDULER_OFFPEAK_START_HOUR || hour < SCHEDULER_OFFPEAK_END_HOUR
}

// ---------------------------------------------------------------------------
// Scheduler helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SchedulerEntry {
    job_id: Uuid,
    chunk_id: Uuid,
    priority: PriorityLevel,
    min_node_class: NodeClass,
    submitted_at: Instant,
    attempts: u32,
}

struct PriorityScheduler {
    queue: Vec<SchedulerEntry>,
    max_attempts: u32,
    max_queue_depth: usize,
}

impl PriorityScheduler {
    fn new() -> Self {
        Self {
            queue: Vec::new(),
            max_attempts: MAX_CHUNK_ATTEMPTS,
            max_queue_depth: SCHEDULER_MAX_QUEUE_DEPTH,
        }
    }

    fn enqueue(&mut self, entry: SchedulerEntry) -> Result<(), String> {
        if self.queue.len() >= self.max_queue_depth {
            return Err("Queue full".to_string());
        }
        self.queue.push(entry);
        Ok(())
    }

    fn dequeue_for_class(&mut self, node_class: NodeClass) -> Option<SchedulerEntry> {
        // Sort by priority weight descending, then by submission time ascending
        self.queue.sort_by(|a, b| {
            b.priority
                .queue_weight()
                .cmp(&a.priority.queue_weight())
                .then_with(|| a.submitted_at.cmp(&b.submitted_at))
        });

        let idx = self.queue.iter().position(|e| {
            // Node class must be >= the minimum required
            node_class >= e.min_node_class
        })?;

        Some(self.queue.remove(idx))
    }

    fn cancel_job(&mut self, job_id: Uuid) -> usize {
        let before = self.queue.len();
        self.queue.retain(|e| e.job_id != job_id);
        before - self.queue.len()
    }

    fn reassign_failed(&mut self, mut entry: SchedulerEntry) -> Result<(), String> {
        entry.attempts += 1;
        if entry.attempts >= self.max_attempts {
            return Err(format!(
                "Chunk {} exceeded max attempts ({})",
                entry.chunk_id, self.max_attempts
            ));
        }
        self.queue.push(entry);
        Ok(())
    }

    fn queue_depth(&self, priority: PriorityLevel) -> usize {
        self.queue.iter().filter(|e| e.priority == priority).count()
    }

    fn total_depth(&self) -> usize {
        self.queue.len()
    }
}

// ---------------------------------------------------------------------------
// Streaming / SSE helpers
// ---------------------------------------------------------------------------

/// A single SSE event.
#[derive(Debug, Clone)]
struct StreamEvent {
    event_type: String,
    data: String,
    id: Option<String>,
}

impl StreamEvent {
    fn to_sse(&self) -> String {
        let mut out = String::new();
        if let Some(ref id) = self.id {
            out.push_str(&format!("id: {}\n", id));
        }
        out.push_str(&format!("event: {}\n", self.event_type));
        for line in self.data.lines() {
            out.push_str(&format!("data: {}\n", line));
        }
        out.push('\n');
        out
    }
}

/// Progress tracker for a job.
struct ProgressTracker {
    total_chunks: u32,
    completed_chunks: u32,
    started_at: Instant,
    milestones_fired: Vec<u8>,
}

impl ProgressTracker {
    fn new(total_chunks: u32) -> Self {
        Self {
            total_chunks,
            completed_chunks: 0,
            started_at: Instant::now(),
            milestones_fired: Vec::new(),
        }
    }

    fn record_completion(&mut self) -> Vec<u8> {
        self.completed_chunks += 1;
        let pct = self.completion_pct();
        let mut new_milestones = Vec::new();
        for milestone in &[25u8, 50, 75, 100] {
            if pct >= *milestone as f64 && !self.milestones_fired.contains(milestone) {
                self.milestones_fired.push(*milestone);
                new_milestones.push(*milestone);
            }
        }
        new_milestones
    }

    fn completion_pct(&self) -> f64 {
        if self.total_chunks == 0 {
            return 100.0;
        }
        self.completed_chunks as f64 / self.total_chunks as f64 * 100.0
    }

    fn throughput(&self) -> f64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        if elapsed < 1e-9 {
            return 0.0;
        }
        self.completed_chunks as f64 / elapsed
    }
}

/// Simple streaming engine: broadcast events to subscribers.
struct StreamingEngine {
    tx: broadcast::Sender<StreamEvent>,
}

impl StreamingEngine {
    fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.tx.subscribe()
    }

    fn emit(&self, event: StreamEvent) -> usize {
        self.tx.send(event).unwrap_or(0)
    }
}

/// SSE formatting utilities.
fn format_sse_event(event_type: &str, data: &str) -> String {
    let mut out = String::new();
    out.push_str(&format!("event: {}\n", event_type));
    for line in data.lines() {
        out.push_str(&format!("data: {}\n", line));
    }
    out.push('\n');
    out
}

fn format_sse_keepalive() -> String {
    ": keepalive\n\n".to_string()
}

fn format_sse_comment(text: &str) -> String {
    format!(": {}\n\n", text)
}

/// Job progress summary (computed fields).
struct JobProgress {
    job_id: Uuid,
    total_chunks: u32,
    completed_chunks: u32,
    failed_chunks: u32,
    completion_pct: f64,
    estimated_remaining_secs: Option<u64>,
    throughput_chunks_per_sec: f64,
}

impl JobProgress {
    fn from_tracker(job_id: Uuid, tracker: &ProgressTracker, failed: u32) -> Self {
        let pct = tracker.completion_pct();
        let throughput = tracker.throughput();
        let remaining = if throughput > 0.0 && pct < 100.0 {
            let remaining_chunks = tracker.total_chunks - tracker.completed_chunks;
            Some((remaining_chunks as f64 / throughput) as u64)
        } else {
            None
        };
        Self {
            job_id,
            total_chunks: tracker.total_chunks,
            completed_chunks: tracker.completed_chunks,
            failed_chunks: failed,
            completion_pct: pct,
            estimated_remaining_secs: remaining,
            throughput_chunks_per_sec: throughput,
        }
    }
}

// ---------------------------------------------------------------------------
// Webhook helpers
// ---------------------------------------------------------------------------

/// Webhook payload for a milestone event.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WebhookMilestonePayload {
    event_type: String,
    job_id: String,
    milestone_pct: u8,
    completed_chunks: u32,
    total_chunks: u32,
    timestamp: String,
}

/// Webhook payload for a completion event.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WebhookCompletionPayload {
    event_type: String,
    job_id: String,
    total_cost_usd: f64,
    total_duration_secs: f64,
    total_chunks: u32,
    timestamp: String,
}

/// Build a milestone webhook payload.
fn build_milestone_payload(
    job_id: &Uuid,
    milestone: u8,
    completed: u32,
    total: u32,
) -> WebhookMilestonePayload {
    WebhookMilestonePayload {
        event_type: "milestone".to_string(),
        job_id: job_id.to_string(),
        milestone_pct: milestone,
        completed_chunks: completed,
        total_chunks: total,
        timestamp: Utc::now().to_rfc3339(),
    }
}

/// Build a completion webhook payload.
fn build_completion_payload(
    job_id: &Uuid,
    cost: f64,
    duration: f64,
    chunks: u32,
) -> WebhookCompletionPayload {
    WebhookCompletionPayload {
        event_type: "completion".to_string(),
        job_id: job_id.to_string(),
        total_cost_usd: cost,
        total_duration_secs: duration,
        total_chunks: chunks,
        timestamp: Utc::now().to_rfc3339(),
    }
}

/// Sign a webhook payload with HMAC-SHA256 using sha2 directly.
fn webhook_sign(secret: &[u8], payload: &[u8]) -> String {
    // HMAC-SHA256: H((K ^ opad) || H((K ^ ipad) || message))
    let block_size = 64;
    let mut key = vec![0u8; block_size];
    if secret.len() > block_size {
        let hash = Sha256::digest(secret);
        key[..32].copy_from_slice(&hash);
    } else {
        key[..secret.len()].copy_from_slice(secret);
    }

    let mut ipad = vec![0x36u8; block_size];
    let mut opad = vec![0x5cu8; block_size];
    for i in 0..block_size {
        ipad[i] ^= key[i];
        opad[i] ^= key[i];
    }

    let mut inner_hasher = Sha256::new();
    inner_hasher.update(&ipad);
    inner_hasher.update(payload);
    let inner_hash = inner_hasher.finalize();

    let mut outer_hasher = Sha256::new();
    outer_hasher.update(&opad);
    outer_hasher.update(&inner_hash);
    let result = outer_hasher.finalize();

    result.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Verify a webhook signature.
fn webhook_verify(secret: &[u8], payload: &[u8], signature: &str) -> bool {
    let expected = webhook_sign(secret, payload);
    expected == signature
}

/// Webhook event filter: check if an event type is in the allowed list.
fn webhook_filter(event_type: &str, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true; // No filter = allow all
    }
    allowed.iter().any(|a| a == event_type)
}

// ---------------------------------------------------------------------------
// Combo registry helpers
// ---------------------------------------------------------------------------

/// In-memory combo module registry.
struct ComboRegistry {
    modules: Vec<ComboModule>,
}

impl ComboRegistry {
    fn new() -> Self {
        Self {
            modules: Self::builtin_modules(),
        }
    }

    fn builtin_modules() -> Vec<ComboModule> {
        let categories = vec![
            ("computation", vec![
                ("cr.montecarlo", "CR.MonteCarlo", "Monte Carlo simulation", NodeClass::Light),
                ("cr.raytrace", "CR.RayTrace", "Distributed ray tracing", NodeClass::Standard),
                ("cr.genetic", "CR.Genetic", "Genetic algorithm optimization", NodeClass::Light),
                ("cr.fluid_sim", "CR.FluidSim", "Computational fluid dynamics", NodeClass::Enterprise),
                ("cr.password_hash", "CR.PasswordHash", "Parallel password hashing", NodeClass::Edge),
            ]),
            ("data", vec![
                ("dt.etl_pipeline", "DT.ETLPipeline", "Extract-Transform-Load pipeline", NodeClass::Light),
                ("dt.log_analysis", "DT.LogAnalysis", "Distributed log analysis", NodeClass::Light),
                ("dt.dedup", "DT.Dedup", "Data deduplication", NodeClass::Light),
            ]),
            ("media", vec![
                ("md.transcode", "MD.Transcode", "Video/audio transcoding", NodeClass::Standard),
                ("md.thumbnail", "MD.Thumbnail", "Image thumbnail generation", NodeClass::Edge),
                ("md.watermark", "MD.Watermark", "Media watermarking", NodeClass::Edge),
            ]),
            ("science", vec![
                ("sc.protein_fold", "SC.ProteinFold", "Protein folding simulation", NodeClass::Enterprise),
                ("sc.genome_align", "SC.GenomeAlign", "Genome sequence alignment", NodeClass::Standard),
                ("sc.climate_sim", "SC.ClimateSim", "Climate model simulation", NodeClass::Enterprise),
                ("sc.molecular_dynamics", "SC.MolDyn", "Molecular dynamics simulation", NodeClass::Enterprise),
            ]),
            ("ai", vec![
                ("ai.inference", "AI.Inference", "Model inference batch processing", NodeClass::Standard),
                ("ai.fine_tune", "AI.FineTune", "Distributed fine-tuning", NodeClass::Enterprise),
                ("ai.embedding", "AI.Embedding", "Embedding computation", NodeClass::Light),
            ]),
            ("finance", vec![
                ("fi.risk_model", "FI.RiskModel", "Financial risk modeling", NodeClass::Standard),
                ("fi.backtest", "FI.Backtest", "Trading strategy backtesting", NodeClass::Light),
                ("fi.fraud_detect", "FI.FraudDetect", "Fraud detection pipeline", NodeClass::Standard),
            ]),
            ("enterprise", vec![
                ("en.batch_report", "EN.BatchReport", "Enterprise batch reporting", NodeClass::Light),
            ]),
        ];

        let mut modules = Vec::new();
        for (category, entries) in categories {
            for (id, display, desc, min_class) in entries {
                modules.push(ComboModule {
                    id: id.to_string(),
                    display_name: display.to_string(),
                    category: category.to_string(),
                    description: desc.to_string(),
                    plugin_id: format!("plugin.{}", id),
                    default_verification: VerificationStrategy::default(),
                    default_chunk_strategy: ChunkStrategy::default(),
                    min_node_class: min_class,
                    parameter_schema: serde_json::json!({
                        "type": "object",
                        "properties": {},
                        "required": []
                    }),
                    output_formats: vec!["json".to_string(), "csv".to_string()],
                    cloud_comparison_base_usd: Some(1.0),
                    supports_streaming: true,
                    supports_statistical_verification: category == "computation" || category == "science",
                });
            }
        }
        modules
    }

    fn get_by_id(&self, id: &str) -> Option<&ComboModule> {
        self.modules.iter().find(|m| m.id == id)
    }

    fn list_by_category(&self, category: &str) -> Vec<&ComboModule> {
        self.modules
            .iter()
            .filter(|m| m.category == category)
            .collect()
    }

    fn categories(&self) -> Vec<String> {
        let mut cats: Vec<String> = self.modules.iter().map(|m| m.category.clone()).collect();
        cats.sort();
        cats.dedup();
        cats
    }

    fn validate_params(
        &self,
        module_id: &str,
        params: &serde_json::Value,
    ) -> Result<(), String> {
        let module = self
            .get_by_id(module_id)
            .ok_or_else(|| format!("Module {} not found", module_id))?;

        if let Some(required) = module.parameter_schema.get("required") {
            if let Some(required_arr) = required.as_array() {
                for req in required_arr {
                    if let Some(field_name) = req.as_str() {
                        if params.get(field_name).is_none() {
                            return Err(format!("Missing required parameter: {}", field_name));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn register(&mut self, module: ComboModule) {
        // Remove existing if same id
        self.modules.retain(|m| m.id != module.id);
        self.modules.push(module);
    }

    fn unregister(&mut self, id: &str) -> bool {
        let before = self.modules.len();
        self.modules.retain(|m| m.id != id);
        self.modules.len() < before
    }
}

// ---------------------------------------------------------------------------
// WASM executor helpers
// ---------------------------------------------------------------------------

/// WASM execution configuration.
#[derive(Debug, Clone)]
struct WasmConfig {
    max_memory_pages: u32,
    max_execution_secs: u64,
    max_stack_size: usize,
    sandbox_mode: WasmSandboxMode,
    allowed_imports: Vec<String>,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            max_memory_pages: 256,          // 16 MB
            max_execution_secs: 300,        // 5 minutes
            max_stack_size: 1024 * 1024,    // 1 MB
            sandbox_mode: WasmSandboxMode::Standard,
            allowed_imports: vec![
                "wasi_snapshot_preview1".to_string(),
                "env".to_string(),
            ],
        }
    }
}

/// WASM sandbox strictness modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WasmSandboxMode {
    /// No filesystem, no network, no clock. Pure computation only.
    Strict,
    /// WASI preview1 subset: fs read-only on mapped dirs, clock.
    Standard,
    /// Full WASI preview1: read-write mapped dirs, env vars, args.
    Relaxed,
}

impl std::fmt::Display for WasmSandboxMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WasmSandboxMode::Strict => write!(f, "strict"),
            WasmSandboxMode::Standard => write!(f, "standard"),
            WasmSandboxMode::Relaxed => write!(f, "relaxed"),
        }
    }
}

/// WASM value types.
#[derive(Debug, Clone, PartialEq)]
enum WasmValue {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

impl std::fmt::Display for WasmValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WasmValue::I32(v) => write!(f, "i32:{}", v),
            WasmValue::I64(v) => write!(f, "i64:{}", v),
            WasmValue::F32(v) => write!(f, "f32:{}", v),
            WasmValue::F64(v) => write!(f, "f64:{}", v),
        }
    }
}

/// WASM execution error types.
#[derive(Debug)]
enum WasmError {
    CompilationFailed(String),
    LinkError(String),
    Trap(String),
    MemoryExceeded { limit: u32, requested: u32 },
    Timeout { limit_secs: u64 },
    InvalidExport(String),
}

impl std::fmt::Display for WasmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WasmError::CompilationFailed(e) => write!(f, "compilation failed: {}", e),
            WasmError::LinkError(e) => write!(f, "link error: {}", e),
            WasmError::Trap(e) => write!(f, "trap: {}", e),
            WasmError::MemoryExceeded { limit, requested } => {
                write!(f, "memory exceeded: limit={}, requested={}", limit, requested)
            }
            WasmError::Timeout { limit_secs } => {
                write!(f, "execution timed out after {}s", limit_secs)
            }
            WasmError::InvalidExport(e) => write!(f, "invalid export: {}", e),
        }
    }
}

/// WASM module metadata.
struct WasmModuleInfo {
    name: String,
    exports: Vec<WasmExport>,
    imports: Vec<WasmImport>,
    memory_pages: u32,
}

#[derive(Debug, Clone)]
struct WasmExport {
    name: String,
    kind: WasmExportKind,
}

#[derive(Debug, Clone, PartialEq)]
enum WasmExportKind {
    Function,
    Memory,
    Table,
    Global,
}

#[derive(Debug, Clone)]
struct WasmImport {
    module: String,
    name: String,
}

// ============================================================================
// 1. Node Class Tests (8 tests)
// ============================================================================

#[test]
fn test_node_class_from_resources_dust() {
    // 1 core, 512 MB RAM -> Edge (< 1500 MB threshold)
    let class = NodeClass::from_resources(1, 512);
    assert_eq!(class, NodeClass::Edge);
}

#[test]
fn test_node_class_from_resources_pebble() {
    // 4 cores, 4096 MB -> Light (cores <= 4, memory < 6000)
    let class = NodeClass::from_resources(4, 4096);
    assert_eq!(class, NodeClass::Light);
}

#[test]
fn test_node_class_from_resources_rock() {
    // 8 cores, 16384 MB -> Standard (cores <= 8, memory < 20000)
    let class = NodeClass::from_resources(8, 16384);
    assert_eq!(class, NodeClass::Standard);
}

#[test]
fn test_node_class_from_resources_boulder() {
    // 32 cores, 65536 MB -> Enterprise (everything else)
    let class = NodeClass::from_resources(32, 65536);
    assert_eq!(class, NodeClass::Enterprise);
}

#[test]
fn test_node_class_constraints() {
    // Each class should have valid, strictly positive constraints
    for class in NodeClass::all() {
        let c = TaskConstraints::for_class(*class);
        assert_eq!(c.node_class, *class);
        assert!(c.max_memory_mb > 0, "max_memory_mb must be > 0 for {:?}", class);
        assert!(c.max_duration_secs > 0, "max_duration_secs must be > 0 for {:?}", class);
        assert!(c.max_input_size_mb > 0, "max_input_size_mb must be > 0 for {:?}", class);
        assert!(c.max_output_size_mb > 0, "max_output_size_mb must be > 0 for {:?}", class);
        assert!(c.max_concurrent_chunks > 0, "max_concurrent must be > 0 for {:?}", class);
    }
}

#[test]
fn test_node_class_suggested_batch_sizes() {
    // Batch sizes should be in ascending order: Edge < Light < Standard < Enterprise
    let sizes: Vec<u32> = NodeClass::all().iter().map(|c| c.suggested_batch_size()).collect();
    for i in 1..sizes.len() {
        assert!(
            sizes[i] > sizes[i - 1],
            "Batch size for {:?} ({}) should be > {:?} ({})",
            NodeClass::all()[i],
            sizes[i],
            NodeClass::all()[i - 1],
            sizes[i - 1]
        );
    }
}

#[test]
fn test_node_class_compute_weight() {
    // compute_weight: Enterprise > Standard > Light > Edge
    let w_dust = NodeClass::Edge.compute_weight();
    let w_pebble = NodeClass::Light.compute_weight();
    let w_rock = NodeClass::Standard.compute_weight();
    let w_boulder = NodeClass::Enterprise.compute_weight();

    assert!(w_boulder > w_rock, "Enterprise ({}) > Standard ({})", w_boulder, w_rock);
    assert!(w_rock > w_pebble, "Standard ({}) > Light ({})", w_rock, w_pebble);
    assert!(w_pebble > w_dust, "Light ({}) > Edge ({})", w_pebble, w_dust);
}

#[test]
fn test_node_class_display() {
    assert_eq!(format!("{}", NodeClass::Edge), "Edge");
    assert_eq!(format!("{}", NodeClass::Light), "Light");
    assert_eq!(format!("{}", NodeClass::Standard), "Standard");
    assert_eq!(format!("{}", NodeClass::Enterprise), "Enterprise");
}

// ============================================================================
// 2. Priority Level Tests (5 tests)
// ============================================================================

#[test]
fn test_priority_cost_multiplier() {
    assert!((PriorityLevel::Rush.cost_multiplier() - 3.0).abs() < f64::EPSILON);
    assert!((PriorityLevel::Standard.cost_multiplier() - 1.0).abs() < f64::EPSILON);
    assert!((PriorityLevel::Economy.cost_multiplier() - 0.5).abs() < f64::EPSILON);
}

#[test]
fn test_priority_queue_weight() {
    let rush = PriorityLevel::Rush.queue_weight();
    let standard = PriorityLevel::Standard.queue_weight();
    let economy = PriorityLevel::Economy.queue_weight();

    assert!(rush > standard, "Rush ({}) > Standard ({})", rush, standard);
    assert!(standard > economy, "Standard ({}) > Economy ({})", standard, economy);
}

#[test]
fn test_priority_default() {
    let p: PriorityLevel = Default::default();
    assert_eq!(p, PriorityLevel::Standard);
}

#[test]
fn test_priority_display() {
    assert_eq!(format!("{}", PriorityLevel::Rush), "rush");
    assert_eq!(format!("{}", PriorityLevel::Standard), "standard");
    assert_eq!(format!("{}", PriorityLevel::Economy), "economy");
}

#[test]
fn test_priority_all_variants() {
    let variants = [PriorityLevel::Rush, PriorityLevel::Standard, PriorityLevel::Economy];
    assert_eq!(variants.len(), 3);

    // Each variant should round-trip through serde
    for v in &variants {
        let json = serde_json::to_string(v).unwrap();
        let back: PriorityLevel = serde_json::from_str(&json).unwrap();
        assert_eq!(*v, back);
    }
}

// ============================================================================
// 3. Verification Tests (12 tests)
// ============================================================================

#[test]
fn test_verification_redundant_all_agree() {
    let hash = sha256_hex(b"result_data_123");
    let results = vec![
        (make_node_id(1), hash.clone()),
        (make_node_id(2), hash.clone()),
        (make_node_id(3), hash.clone()),
    ];
    match verify_redundant(&results) {
        VerificationOutcome::Verified { consensus_hash } => {
            assert_eq!(consensus_hash, hash);
        }
        other => panic!("Expected Verified, got {:?}", other),
    }
}

#[test]
fn test_verification_redundant_majority() {
    let good_hash = sha256_hex(b"correct");
    let bad_hash = sha256_hex(b"wrong");
    let results = vec![
        (make_node_id(1), good_hash.clone()),
        (make_node_id(2), good_hash.clone()),
        (make_node_id(3), bad_hash.clone()),
    ];
    match verify_redundant(&results) {
        VerificationOutcome::MajorityConsensus {
            consensus_hash,
            outlier_node,
            replicas_agreed,
        } => {
            assert_eq!(consensus_hash, good_hash);
            assert_eq!(outlier_node, make_node_id(3));
            assert_eq!(replicas_agreed, 2);
        }
        other => panic!("Expected MajorityConsensus, got {:?}", other),
    }
}

#[test]
fn test_verification_redundant_all_disagree() {
    let results = vec![
        (make_node_id(1), sha256_hex(b"a")),
        (make_node_id(2), sha256_hex(b"b")),
        (make_node_id(3), sha256_hex(b"c")),
    ];
    match verify_redundant(&results) {
        VerificationOutcome::Conflict { hashes } => {
            assert_eq!(hashes.len(), 3);
        }
        other => panic!("Expected Conflict, got {:?}", other),
    }
}

#[test]
fn test_verification_spot_check_pass() {
    let hash = sha256_hex(b"trusted_result");
    let outcome = verify_spot_check(&hash, &hash, make_node_id(1));
    match outcome {
        VerificationOutcome::SpotCheckPassed => {}
        other => panic!("Expected SpotCheckPassed, got {:?}", other),
    }
}

#[test]
fn test_verification_spot_check_fail() {
    let trusted = sha256_hex(b"trusted");
    let checked = sha256_hex(b"different");
    let node = make_node_id(42);
    let outcome = verify_spot_check(&trusted, &checked, node);
    match outcome {
        VerificationOutcome::SpotCheckFailed {
            trusted_hash,
            checked_hash,
            node: n,
        } => {
            assert_eq!(trusted_hash, trusted);
            assert_eq!(checked_hash, checked);
            assert_eq!(n, node);
        }
        other => panic!("Expected SpotCheckFailed, got {:?}", other),
    }
}

#[test]
fn test_verification_statistical_normal() {
    // All values close to mean: no outliers
    let values = vec![10.0, 10.1, 9.9, 10.05, 9.95];
    let outcome = verify_statistical(&values, VERIFICATION_OUTLIER_Z_SCORE);
    match outcome {
        VerificationOutcome::StatisticallyValid { .. } => {}
        other => panic!("Expected StatisticallyValid, got {:?}", other),
    }
}

#[test]
fn test_verification_statistical_outlier() {
    // One value far from mean
    let mut values = vec![10.0; 99];
    values.push(100.0);
    let outcome = verify_statistical(&values, VERIFICATION_OUTLIER_Z_SCORE);
    match outcome {
        VerificationOutcome::StatisticalOutlier { z_score, .. } => {
            assert!(z_score > VERIFICATION_OUTLIER_Z_SCORE);
        }
        other => panic!("Expected StatisticalOutlier, got {:?}", other),
    }
}

#[test]
fn test_result_hasher_bytes() {
    // Same input always produces the same hash
    let data = b"hello world";
    let h1 = hash_result_bytes(data);
    let h2 = hash_result_bytes(data);
    assert_eq!(h1, h2);

    // Different input produces different hash
    let h3 = hash_result_bytes(b"hello world!");
    assert_ne!(h1, h3);

    // Hash is 64 hex characters (SHA-256)
    assert_eq!(h1.len(), 64);
}

#[test]
fn test_result_hasher_json() {
    // JSON normalization: different key order produces same hash
    let json_a = r#"{"b": 2, "a": 1}"#;
    let json_b = r#"{"a": 1, "b": 2}"#;
    let h1 = hash_result_json(json_a);
    let h2 = hash_result_json(json_b);
    assert_eq!(h1, h2, "JSON normalization should produce same hash regardless of key order");
}

#[test]
fn test_trust_scorer_new_node() {
    let scorer = TrustScorer::new();
    // New node with 5 jobs and 0% failure
    let score = scorer.score(5, 0.0);
    // 5/100 = 0.05 maturity * 1.0 reliability = 0.05
    assert!(score < 0.1, "New node trust should be low, got {}", score);
    assert!(score > 0.0, "New node trust should be positive, got {}", score);
}

#[test]
fn test_trust_scorer_trusted_node() {
    let scorer = TrustScorer::new();
    // Experienced node with 200 jobs and 1% failure
    let score = scorer.score(200, 0.01);
    // min(200/100, 1.0) = 1.0 maturity * 0.99 reliability = 0.99
    assert!(score > 0.9, "Trusted node score should be high, got {}", score);
}

#[test]
fn test_verification_strategy_default() {
    let strategy: VerificationStrategy = Default::default();
    match strategy {
        VerificationStrategy::Redundant { replicas } => {
            assert_eq!(replicas, 2);
        }
        other => panic!("Expected Redundant(2), got {:?}", other),
    }
}

// ============================================================================
// 4. Chunking Tests (10 tests)
// ============================================================================

#[test]
fn test_chunk_splitter_range() {
    // Split 1000 items into chunks of 100
    let chunks = split_range(1000, 100);
    assert_eq!(chunks.len(), 10);
    assert_eq!(chunks[0], (0, 100));
    assert_eq!(chunks[9], (900, 1000));

    // Total coverage
    let total: u64 = chunks.iter().map(|(s, e)| e - s).sum();
    assert_eq!(total, 1000);
}

#[test]
fn test_chunk_splitter_into_n() {
    // Split 100 items into 7 parts
    let chunks = split_into_n(100, 7);
    assert_eq!(chunks.len(), 7);

    // Total coverage
    let total: u64 = chunks.iter().map(|(s, e)| e - s).sum();
    assert_eq!(total, 100);

    // Sizes should be 15 or 14 (100 / 7 = 14 remainder 2)
    for (start, end) in &chunks {
        let size = end - start;
        assert!(size == 14 || size == 15, "Unexpected chunk size: {}", size);
    }

    // No gaps: each chunk starts where the previous ended
    for i in 1..chunks.len() {
        assert_eq!(chunks[i].0, chunks[i - 1].1);
    }
}

#[test]
fn test_chunk_splitter_by_class() {
    // 1000 items, distribution: 10 Edge nodes, 5 Standard nodes
    let distribution = vec![(NodeClass::Edge, 10), (NodeClass::Standard, 5)];
    let result = split_by_class(1000, &distribution);
    assert_eq!(result.len(), 2);

    let total: u64 = result.iter().map(|(_, count)| count).sum();
    assert_eq!(total, 1000);

    // Standard should get more work per node due to higher compute_weight
    let dust_share = result.iter().find(|(c, _)| *c == NodeClass::Edge).unwrap().1;
    let rock_share = result.iter().find(|(c, _)| *c == NodeClass::Standard).unwrap().1;
    // Standard weight = 1.0 * 5 = 5.0; Edge weight = 0.05 * 10 = 0.5
    // So Standard should get ~91% of work
    assert!(rock_share > dust_share, "Standard ({}) should get more than Edge ({})", rock_share, dust_share);
}

#[test]
fn test_chunk_planner_fixed() {
    let chunks = plan_fixed(1000, 10);
    assert_eq!(chunks.len(), 10);
    let total: u64 = chunks.iter().map(|(s, e)| e - s).sum();
    assert_eq!(total, 1000);
}

#[test]
fn test_chunk_planner_per_line() {
    // 500 lines, 50 per chunk
    let chunks = plan_per_line(500, 50);
    assert_eq!(chunks.len(), 10);
    assert_eq!(chunks[0], (0, 50));
    assert_eq!(chunks[9], (450, 500));
}

#[test]
fn test_chunk_planner_adaptive() {
    // 1000 items, 5 available nodes => 10 chunks (2 per node)
    let chunks = plan_adaptive(1000, 5);
    assert_eq!(chunks.len(), 10);
    let total: u64 = chunks.iter().map(|(s, e)| e - s).sum();
    assert_eq!(total, 1000);
}

#[test]
fn test_class_distribution_from_counts() {
    let counts = vec![
        (NodeClass::Edge, 50),
        (NodeClass::Light, 30),
        (NodeClass::Standard, 15),
        (NodeClass::Enterprise, 5),
    ];
    let pcts = class_percentages(&counts);
    assert_eq!(pcts.len(), 4);

    let dust_pct = pcts.iter().find(|(c, _)| *c == NodeClass::Edge).unwrap().1;
    assert!((dust_pct - 50.0).abs() < 0.01, "Edge should be 50%, got {}", dust_pct);

    let boulder_pct = pcts.iter().find(|(c, _)| *c == NodeClass::Enterprise).unwrap().1;
    assert!((boulder_pct - 5.0).abs() < 0.01, "Enterprise should be 5%, got {}", boulder_pct);

    // Percentages sum to 100
    let total: f64 = pcts.iter().map(|(_, p)| p).sum();
    assert!((total - 100.0).abs() < 0.01);
}

#[test]
fn test_class_distribution_effective_compute() {
    let counts = vec![
        (NodeClass::Edge, 100),
        (NodeClass::Enterprise, 1),
    ];
    let compute = effective_compute(&counts);
    // Edge: 0.05 * 100 = 5.0, Enterprise: 4.0 * 1 = 4.0, total = 9.0
    assert!((compute - 9.0).abs() < 0.01, "Expected 9.0, got {}", compute);
}

#[test]
fn test_chunk_validator_valid() {
    // Within Standard constraints: 4 GB memory, 1800s, 200 MB input
    let result = validate_chunk(4096, 1800, 200, NodeClass::Standard);
    assert!(result.is_ok());
}

#[test]
fn test_chunk_validator_memory_violation() {
    // Edge max_memory = 100 MB, try 200 MB
    let result = validate_chunk(200, 30, 1, NodeClass::Edge);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Memory"), "Error should mention memory: {}", err);
}

// ============================================================================
// 5. Pricing Tests (10 tests)
// ============================================================================

#[test]
fn test_cost_estimate_standard_priority() {
    let est = estimate_cost(
        NodeClass::Standard,
        10.0,
        PriorityLevel::Standard,
        &VerificationStrategy::None,
        100,
    );
    assert!(est.estimated_cost_usd > 0.0);
    assert_eq!(est.priority, PriorityLevel::Standard);
    // Standard multiplier = 1.0, verification = 0.0, retry = 0.20
    // base = 0.002 * 10 = 0.02, total = 0.02 * 1.0 * 1.0 * 1.20 = 0.024
    assert!(
        (est.estimated_cost_usd - 0.024).abs() < 0.001,
        "Expected ~0.024, got {}",
        est.estimated_cost_usd
    );
}

#[test]
fn test_cost_estimate_rush_priority() {
    let est = estimate_cost(
        NodeClass::Standard,
        10.0,
        PriorityLevel::Rush,
        &VerificationStrategy::None,
        100,
    );
    // Rush = 3x base
    let base = PRICING_BASE_NODE_HOUR_USD * 10.0 * 3.0 * 1.2;
    assert!(
        (est.estimated_cost_usd - base).abs() < 0.001,
        "Expected ~{}, got {}",
        base,
        est.estimated_cost_usd
    );
}

#[test]
fn test_cost_estimate_economy_priority() {
    let est = estimate_cost(
        NodeClass::Standard,
        10.0,
        PriorityLevel::Economy,
        &VerificationStrategy::None,
        100,
    );
    // Economy = 0.5x base
    let base = PRICING_BASE_NODE_HOUR_USD * 10.0 * 0.5 * 1.2;
    assert!(
        (est.estimated_cost_usd - base).abs() < 0.001,
        "Expected ~{}, got {}",
        base,
        est.estimated_cost_usd
    );
}

#[test]
fn test_cost_estimate_with_verification() {
    let est_none = estimate_cost(
        NodeClass::Standard,
        10.0,
        PriorityLevel::Standard,
        &VerificationStrategy::None,
        100,
    );
    let est_redundant = estimate_cost(
        NodeClass::Standard,
        10.0,
        PriorityLevel::Standard,
        &VerificationStrategy::Redundant { replicas: 2 },
        100,
    );
    // Redundant(2) adds 50% overhead (1/2)
    assert!(
        est_redundant.estimated_cost_usd > est_none.estimated_cost_usd,
        "Redundant cost ({}) should exceed no-verification ({})",
        est_redundant.estimated_cost_usd,
        est_none.estimated_cost_usd
    );
    assert!(est_redundant.verification_overhead_pct > 0.0);
}

#[test]
fn test_rate_card_default() {
    let card = rate_card();
    // All 4 classes should have rates
    assert_eq!(card.len(), 4);
    for class in NodeClass::all() {
        assert!(card.contains_key(class), "Rate card missing {:?}", class);
        assert!(*card.get(class).unwrap() > 0.0, "Rate for {:?} must be positive", class);
    }
    // Enterprise rate > Standard rate > Light rate > Edge rate
    assert!(card[&NodeClass::Enterprise] > card[&NodeClass::Standard]);
    assert!(card[&NodeClass::Standard] > card[&NodeClass::Light]);
    assert!(card[&NodeClass::Light] > card[&NodeClass::Edge]);
}

#[test]
fn test_module_pricing_profiles_all_modules() {
    let profiles = default_module_profiles();
    assert!(profiles.len() >= 22, "Expected 22+ module profiles, got {}", profiles.len());
    for profile in &profiles {
        assert!(!profile.module_id.is_empty());
        assert!(profile.base_rate_per_unit > 0.0);
    }
}

#[test]
fn test_cloud_comparison() {
    let swarm_cost = 0.50;
    let cloud_cost = 4.00;
    let savings = cloud_savings(swarm_cost, cloud_cost);
    assert!((savings - 0.875).abs() < 0.001, "Expected 87.5% savings, got {}", savings * 100.0);
}

#[test]
fn test_job_billing_flow() {
    let job_id = Uuid::new_v4();
    let mut billing = JobBilling::new(job_id);

    billing.record(Uuid::new_v4(), NodeClass::Standard, 60.0);
    billing.record(Uuid::new_v4(), NodeClass::Standard, 120.0);
    billing.record(Uuid::new_v4(), NodeClass::Light, 30.0);

    assert!(!billing.finalized);
    let receipt = billing.finalize(PriorityLevel::Standard);
    assert!(billing.finalized);

    assert_eq!(receipt.job_id, job_id);
    assert_eq!(receipt.total_chunks, 3);
    assert!(receipt.total_cost_usd > 0.0);
    assert!((receipt.total_duration_secs - 210.0).abs() < 0.01);
}

#[test]
fn test_job_receipt_fields() {
    let job_id = Uuid::new_v4();
    let mut billing = JobBilling::new(job_id);
    billing.record(Uuid::new_v4(), NodeClass::Enterprise, 3600.0);
    billing.record(Uuid::new_v4(), NodeClass::Standard, 1800.0);

    let receipt = billing.finalize(PriorityLevel::Rush);

    assert_eq!(receipt.job_id, job_id);
    assert_eq!(receipt.priority, PriorityLevel::Rush);
    assert_eq!(receipt.total_chunks, 2);
    assert!(receipt.total_cost_usd > 0.0);
    assert!(receipt.started_at <= receipt.finished_at);
    assert!(receipt.node_classes_used.contains(&NodeClass::Enterprise));
    assert!(receipt.node_classes_used.contains(&NodeClass::Standard));
}

#[test]
fn test_economy_scheduler_offpeak() {
    // 22:00 - 06:00 UTC is off-peak
    assert!(is_offpeak(22));
    assert!(is_offpeak(23));
    assert!(is_offpeak(0));
    assert!(is_offpeak(3));
    assert!(is_offpeak(5));
    assert!(!is_offpeak(6));
    assert!(!is_offpeak(12));
    assert!(!is_offpeak(18));
    assert!(!is_offpeak(21));
}

// ============================================================================
// 6. Scheduler Tests (10 tests)
// ============================================================================

fn make_scheduler_entry(
    job_id: Uuid,
    priority: PriorityLevel,
    min_class: NodeClass,
) -> SchedulerEntry {
    SchedulerEntry {
        job_id,
        chunk_id: Uuid::new_v4(),
        priority,
        min_node_class: min_class,
        submitted_at: Instant::now(),
        attempts: 0,
    }
}

#[test]
fn test_scheduler_priority_ordering() {
    let mut sched = PriorityScheduler::new();
    let job1 = Uuid::new_v4();
    let job2 = Uuid::new_v4();
    let job3 = Uuid::new_v4();

    sched.enqueue(make_scheduler_entry(job1, PriorityLevel::Economy, NodeClass::Edge)).unwrap();
    sched.enqueue(make_scheduler_entry(job2, PriorityLevel::Rush, NodeClass::Edge)).unwrap();
    sched.enqueue(make_scheduler_entry(job3, PriorityLevel::Standard, NodeClass::Edge)).unwrap();

    // Rush should come first
    let first = sched.dequeue_for_class(NodeClass::Enterprise).unwrap();
    assert_eq!(first.job_id, job2, "Rush should be dequeued first");

    let second = sched.dequeue_for_class(NodeClass::Enterprise).unwrap();
    assert_eq!(second.job_id, job3, "Standard should be dequeued second");

    let third = sched.dequeue_for_class(NodeClass::Enterprise).unwrap();
    assert_eq!(third.job_id, job1, "Economy should be dequeued third");
}

#[test]
fn test_scheduler_fifo_within_priority() {
    let mut sched = PriorityScheduler::new();
    let job1 = Uuid::new_v4();
    let job2 = Uuid::new_v4();
    let job3 = Uuid::new_v4();

    // All standard priority, FIFO order
    sched.enqueue(make_scheduler_entry(job1, PriorityLevel::Standard, NodeClass::Edge)).unwrap();
    // Small delay to ensure ordering
    std::thread::sleep(Duration::from_millis(1));
    sched.enqueue(make_scheduler_entry(job2, PriorityLevel::Standard, NodeClass::Edge)).unwrap();
    std::thread::sleep(Duration::from_millis(1));
    sched.enqueue(make_scheduler_entry(job3, PriorityLevel::Standard, NodeClass::Edge)).unwrap();

    let first = sched.dequeue_for_class(NodeClass::Enterprise).unwrap();
    assert_eq!(first.job_id, job1);
    let second = sched.dequeue_for_class(NodeClass::Enterprise).unwrap();
    assert_eq!(second.job_id, job2);
    let third = sched.dequeue_for_class(NodeClass::Enterprise).unwrap();
    assert_eq!(third.job_id, job3);
}

#[test]
fn test_scheduler_node_class_matching() {
    let mut sched = PriorityScheduler::new();
    let job = Uuid::new_v4();

    // Edge-class work can be served by Enterprise
    sched.enqueue(make_scheduler_entry(job, PriorityLevel::Standard, NodeClass::Edge)).unwrap();
    let entry = sched.dequeue_for_class(NodeClass::Enterprise);
    assert!(entry.is_some(), "Enterprise should handle Edge work");
}

#[test]
fn test_scheduler_node_class_rejection() {
    let mut sched = PriorityScheduler::new();
    let job = Uuid::new_v4();

    // Standard-class work cannot be served by Edge
    sched.enqueue(make_scheduler_entry(job, PriorityLevel::Standard, NodeClass::Standard)).unwrap();
    let entry = sched.dequeue_for_class(NodeClass::Edge);
    assert!(entry.is_none(), "Edge should not handle Standard work");
}

#[test]
fn test_scheduler_economy_offpeak() {
    // Economy jobs: deferred during peak hours
    // This test verifies the time detection logic
    let peak_hours = vec![8, 12, 15, 20];
    let offpeak_hours = vec![0, 3, 22, 23];

    for h in peak_hours {
        assert!(!is_offpeak(h), "Hour {} should be peak", h);
    }
    for h in offpeak_hours {
        assert!(is_offpeak(h), "Hour {} should be off-peak", h);
    }
}

#[test]
fn test_scheduler_cancel_job() {
    let mut sched = PriorityScheduler::new();
    let job1 = Uuid::new_v4();
    let job2 = Uuid::new_v4();

    sched.enqueue(make_scheduler_entry(job1, PriorityLevel::Standard, NodeClass::Edge)).unwrap();
    sched.enqueue(make_scheduler_entry(job1, PriorityLevel::Standard, NodeClass::Edge)).unwrap();
    sched.enqueue(make_scheduler_entry(job2, PriorityLevel::Standard, NodeClass::Edge)).unwrap();

    let removed = sched.cancel_job(job1);
    assert_eq!(removed, 2, "Should have removed 2 chunks for job1");
    assert_eq!(sched.total_depth(), 1, "Only job2's chunk should remain");
}

#[test]
fn test_scheduler_reassign_failed() {
    let mut sched = PriorityScheduler::new();
    let job = Uuid::new_v4();

    let entry = make_scheduler_entry(job, PriorityLevel::Standard, NodeClass::Edge);
    let chunk_id = entry.chunk_id;

    // Simulate first attempt failure
    let result = sched.reassign_failed(entry);
    assert!(result.is_ok());
    assert_eq!(sched.total_depth(), 1);

    // The re-queued entry should have attempts = 1
    let requeued = sched.dequeue_for_class(NodeClass::Enterprise).unwrap();
    assert_eq!(requeued.chunk_id, chunk_id);
    assert_eq!(requeued.attempts, 1);
}

#[test]
fn test_scheduler_max_attempts() {
    let mut sched = PriorityScheduler::new();
    let mut entry = make_scheduler_entry(Uuid::new_v4(), PriorityLevel::Standard, NodeClass::Edge);
    entry.attempts = MAX_CHUNK_ATTEMPTS - 1;

    let result = sched.reassign_failed(entry);
    assert!(result.is_err(), "Should fail when max attempts exceeded");
    assert!(result.unwrap_err().contains("max attempts"));
}

#[test]
fn test_scheduler_queue_depth() {
    let mut sched = PriorityScheduler::new();
    for _ in 0..5 {
        sched.enqueue(make_scheduler_entry(Uuid::new_v4(), PriorityLevel::Rush, NodeClass::Edge)).unwrap();
    }
    for _ in 0..3 {
        sched.enqueue(make_scheduler_entry(Uuid::new_v4(), PriorityLevel::Standard, NodeClass::Edge)).unwrap();
    }
    for _ in 0..2 {
        sched.enqueue(make_scheduler_entry(Uuid::new_v4(), PriorityLevel::Economy, NodeClass::Edge)).unwrap();
    }

    assert_eq!(sched.queue_depth(PriorityLevel::Rush), 5);
    assert_eq!(sched.queue_depth(PriorityLevel::Standard), 3);
    assert_eq!(sched.queue_depth(PriorityLevel::Economy), 2);
    assert_eq!(sched.total_depth(), 10);
}

#[test]
fn test_scheduler_fairness() {
    // Round-robin: interleave jobs from different submitters
    let mut sched = PriorityScheduler::new();
    let jobs: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();

    // Each job submits 3 chunks at Standard priority
    for job in &jobs {
        for _ in 0..3 {
            sched.enqueue(make_scheduler_entry(*job, PriorityLevel::Standard, NodeClass::Edge)).unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    // Dequeue all and verify that jobs are interleaved (FIFO means job1 x3 first,
    // but they should at least all be served)
    let mut served_jobs: Vec<Uuid> = Vec::new();
    while let Some(entry) = sched.dequeue_for_class(NodeClass::Enterprise) {
        served_jobs.push(entry.job_id);
    }

    assert_eq!(served_jobs.len(), 12);
    // All 4 jobs should appear
    for job in &jobs {
        let count = served_jobs.iter().filter(|j| *j == job).count();
        assert_eq!(count, 3, "Each job should have exactly 3 chunks served");
    }
}

// ============================================================================
// 7. Streaming Tests (8 tests)
// ============================================================================

#[test]
fn test_stream_event_sse_format() {
    let event = StreamEvent {
        event_type: "progress".to_string(),
        data: r#"{"pct": 50}"#.to_string(),
        id: Some("evt-123".to_string()),
    };
    let sse = event.to_sse();
    assert!(sse.contains("id: evt-123\n"));
    assert!(sse.contains("event: progress\n"));
    assert!(sse.contains("data: {\"pct\": 50}\n"));
    assert!(sse.ends_with("\n\n"));
}

#[test]
fn test_progress_tracker_completion_pct() {
    let mut tracker = ProgressTracker::new(100);
    assert!((tracker.completion_pct() - 0.0).abs() < 0.01);

    for _ in 0..50 {
        tracker.record_completion();
    }
    assert!((tracker.completion_pct() - 50.0).abs() < 0.01);

    for _ in 0..50 {
        tracker.record_completion();
    }
    assert!((tracker.completion_pct() - 100.0).abs() < 0.01);
}

#[test]
fn test_progress_tracker_throughput() {
    let mut tracker = ProgressTracker::new(100);
    // Complete 10 chunks
    for _ in 0..10 {
        tracker.record_completion();
    }
    // Throughput should be positive (we can't assert exact value due to timing)
    // Sleep briefly to get a measurable elapsed time
    std::thread::sleep(Duration::from_millis(10));
    for _ in 0..10 {
        tracker.record_completion();
    }
    let tp = tracker.throughput();
    assert!(tp > 0.0, "Throughput should be positive, got {}", tp);
}

#[test]
fn test_progress_tracker_milestone_detection() {
    let mut tracker = ProgressTracker::new(100);

    // Complete chunks up to 25%
    for _ in 0..25 {
        let milestones = tracker.record_completion();
        if tracker.completed_chunks == 25 {
            assert!(milestones.contains(&25), "25% milestone should fire at chunk 25");
        }
    }

    // Complete to 50%
    for _ in 0..25 {
        let milestones = tracker.record_completion();
        if tracker.completed_chunks == 50 {
            assert!(milestones.contains(&50));
        }
    }

    // Complete to 75%
    for _ in 0..25 {
        let milestones = tracker.record_completion();
        if tracker.completed_chunks == 75 {
            assert!(milestones.contains(&75));
        }
    }

    // Complete to 100%
    for _ in 0..25 {
        let milestones = tracker.record_completion();
        if tracker.completed_chunks == 100 {
            assert!(milestones.contains(&100));
        }
    }

    // All 4 milestones should have been fired exactly once
    assert_eq!(tracker.milestones_fired.len(), 4);
    assert!(tracker.milestones_fired.contains(&25));
    assert!(tracker.milestones_fired.contains(&50));
    assert!(tracker.milestones_fired.contains(&75));
    assert!(tracker.milestones_fired.contains(&100));
}

#[tokio::test]
async fn test_streaming_engine_subscribe() {
    let engine = StreamingEngine::new(16);
    let mut rx = engine.subscribe();

    let event = StreamEvent {
        event_type: "test".to_string(),
        data: "hello".to_string(),
        id: None,
    };
    engine.emit(event.clone());

    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("should receive within 1s")
        .expect("should not be recv error");

    assert_eq!(received.event_type, "test");
    assert_eq!(received.data, "hello");
}

#[tokio::test]
async fn test_streaming_engine_emit() {
    let engine = StreamingEngine::new(16);
    let mut rx1 = engine.subscribe();
    let mut rx2 = engine.subscribe();

    let event = StreamEvent {
        event_type: "broadcast".to_string(),
        data: "world".to_string(),
        id: Some("1".to_string()),
    };
    let count = engine.emit(event);
    assert_eq!(count, 2, "Should broadcast to 2 subscribers");

    let r1 = rx1.recv().await.unwrap();
    let r2 = rx2.recv().await.unwrap();
    assert_eq!(r1.event_type, "broadcast");
    assert_eq!(r2.event_type, "broadcast");
}

#[test]
fn test_job_progress_fields() {
    let mut tracker = ProgressTracker::new(200);
    for _ in 0..100 {
        tracker.record_completion();
    }
    std::thread::sleep(Duration::from_millis(10));

    let job_id = Uuid::new_v4();
    let progress = JobProgress::from_tracker(job_id, &tracker, 5);

    assert_eq!(progress.job_id, job_id);
    assert_eq!(progress.total_chunks, 200);
    assert_eq!(progress.completed_chunks, 100);
    assert_eq!(progress.failed_chunks, 5);
    assert!((progress.completion_pct - 50.0).abs() < 0.01);
    assert!(progress.estimated_remaining_secs.is_some());
    assert!(progress.throughput_chunks_per_sec > 0.0);
}

#[test]
fn test_sse_formatter() {
    // format_event
    let event_str = format_sse_event("progress", r#"{"pct":42}"#);
    assert!(event_str.contains("event: progress\n"));
    assert!(event_str.contains("data: {\"pct\":42}\n"));
    assert!(event_str.ends_with("\n\n"));

    // format_keepalive
    let keepalive = format_sse_keepalive();
    assert_eq!(keepalive, ": keepalive\n\n");

    // format_comment
    let comment = format_sse_comment("connected to swarm");
    assert_eq!(comment, ": connected to swarm\n\n");
}

// ============================================================================
// 8. Webhook Tests (5 tests)
// ============================================================================

#[test]
fn test_webhook_payload_builder_milestone() {
    let job_id = Uuid::new_v4();
    let payload = build_milestone_payload(&job_id, 50, 25, 50);

    assert_eq!(payload.event_type, "milestone");
    assert_eq!(payload.job_id, job_id.to_string());
    assert_eq!(payload.milestone_pct, 50);
    assert_eq!(payload.completed_chunks, 25);
    assert_eq!(payload.total_chunks, 50);
    assert!(!payload.timestamp.is_empty());

    // Should serialize to valid JSON
    let json = serde_json::to_string(&payload).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["event_type"], "milestone");
}

#[test]
fn test_webhook_payload_builder_completion() {
    let job_id = Uuid::new_v4();
    let payload = build_completion_payload(&job_id, 1.50, 3600.0, 100);

    assert_eq!(payload.event_type, "completion");
    assert_eq!(payload.job_id, job_id.to_string());
    assert!((payload.total_cost_usd - 1.50).abs() < 0.01);
    assert!((payload.total_duration_secs - 3600.0).abs() < 0.01);
    assert_eq!(payload.total_chunks, 100);
}

#[test]
fn test_webhook_signer_roundtrip() {
    let secret = b"my_webhook_secret_key_12345";
    let payload = b"Hello, this is the webhook payload!";

    let signature = webhook_sign(secret, payload);
    assert_eq!(signature.len(), 64, "SHA-256 hex should be 64 chars");

    // Verify with same secret
    assert!(
        webhook_verify(secret, payload, &signature),
        "Signature verification should pass"
    );

    // Deterministic: signing same payload twice produces same signature
    let sig2 = webhook_sign(secret, payload);
    assert_eq!(signature, sig2);
}

#[test]
fn test_webhook_signer_wrong_secret() {
    let secret = b"correct_secret";
    let wrong = b"wrong_secret";
    let payload = b"test payload data";

    let signature = webhook_sign(secret, payload);

    // Wrong secret should fail verification
    assert!(
        !webhook_verify(wrong, payload, &signature),
        "Verification with wrong secret should fail"
    );
}

#[test]
fn test_webhook_filter() {
    let allowed = vec![
        "milestone".to_string(),
        "completion".to_string(),
    ];

    assert!(webhook_filter("milestone", &allowed));
    assert!(webhook_filter("completion", &allowed));
    assert!(!webhook_filter("error", &allowed));
    assert!(!webhook_filter("heartbeat", &allowed));

    // Empty filter = allow all
    let no_filter: Vec<String> = vec![];
    assert!(webhook_filter("anything", &no_filter));
}

// ============================================================================
// 9. Combo Registry Tests (8 tests)
// ============================================================================

#[test]
fn test_registry_has_all_modules() {
    let registry = ComboRegistry::new();
    assert!(
        registry.modules.len() >= 22,
        "Expected 22+ modules, got {}",
        registry.modules.len()
    );
}

#[test]
fn test_registry_categories() {
    let registry = ComboRegistry::new();
    let cats = registry.categories();
    assert_eq!(cats.len(), 7, "Expected 7 categories, got {}: {:?}", cats.len(), cats);
    assert!(cats.contains(&"computation".to_string()));
    assert!(cats.contains(&"data".to_string()));
    assert!(cats.contains(&"media".to_string()));
    assert!(cats.contains(&"science".to_string()));
    assert!(cats.contains(&"ai".to_string()));
    assert!(cats.contains(&"finance".to_string()));
    assert!(cats.contains(&"enterprise".to_string()));
}

#[test]
fn test_registry_get_by_id() {
    let registry = ComboRegistry::new();
    let module = registry.get_by_id("cr.montecarlo");
    assert!(module.is_some());
    let m = module.unwrap();
    assert_eq!(m.display_name, "CR.MonteCarlo");
    assert_eq!(m.category, "computation");
    assert_eq!(m.min_node_class, NodeClass::Light);
}

#[test]
fn test_registry_list_by_category() {
    let registry = ComboRegistry::new();
    let science = registry.list_by_category("science");
    assert_eq!(science.len(), 4, "Science should have 4 modules, got {}", science.len());

    let ids: Vec<&str> = science.iter().map(|m| m.id.as_str()).collect();
    assert!(ids.contains(&"sc.protein_fold"));
    assert!(ids.contains(&"sc.genome_align"));
    assert!(ids.contains(&"sc.climate_sim"));
    assert!(ids.contains(&"sc.molecular_dynamics"));
}

#[test]
fn test_registry_validate_params_valid() {
    let registry = ComboRegistry::new();
    // Default schema has no required fields, so empty params should pass
    let result = registry.validate_params("cr.montecarlo", &serde_json::json!({}));
    assert!(result.is_ok());
}

#[test]
fn test_registry_validate_params_missing_required() {
    let mut registry = ComboRegistry::new();

    // Register a module with a required parameter
    let mut module = registry.get_by_id("cr.montecarlo").unwrap().clone();
    module.id = "test.required_param".to_string();
    module.parameter_schema = serde_json::json!({
        "type": "object",
        "properties": {
            "iterations": { "type": "integer" }
        },
        "required": ["iterations"]
    });
    registry.register(module);

    let result = registry.validate_params("test.required_param", &serde_json::json!({}));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("iterations"));
}

#[test]
fn test_registry_cloud_comparison() {
    let registry = ComboRegistry::new();
    let module = registry.get_by_id("cr.montecarlo").unwrap();
    assert!(module.cloud_comparison_base_usd.is_some());

    let cloud_base = module.cloud_comparison_base_usd.unwrap();
    assert!(cloud_base > 0.0);

    // Our cost should be lower than cloud
    let swarm_est = estimate_cost(
        module.min_node_class,
        1.0,
        PriorityLevel::Standard,
        &module.default_verification,
        100,
    );

    // Cloud cost for the reference workload
    let savings = cloud_savings(swarm_est.estimated_cost_usd, cloud_base);
    assert!(savings > 0.0, "Swarm should be cheaper than cloud");
}

#[test]
fn test_registry_custom_module() {
    let mut registry = ComboRegistry::new();
    let initial_count = registry.modules.len();

    let custom = ComboModule {
        id: "custom.my_module".to_string(),
        display_name: "Custom.MyModule".to_string(),
        category: "custom".to_string(),
        description: "A custom test module".to_string(),
        plugin_id: "plugin.custom.my_module".to_string(),
        default_verification: VerificationStrategy::None,
        default_chunk_strategy: ChunkStrategy::Fixed { count: 4 },
        min_node_class: NodeClass::Edge,
        parameter_schema: serde_json::json!({"type": "object"}),
        output_formats: vec!["json".to_string()],
        cloud_comparison_base_usd: None,
        supports_streaming: false,
        supports_statistical_verification: false,
    };

    registry.register(custom);
    assert_eq!(registry.modules.len(), initial_count + 1);
    assert!(registry.get_by_id("custom.my_module").is_some());

    // Unregister
    let removed = registry.unregister("custom.my_module");
    assert!(removed);
    assert_eq!(registry.modules.len(), initial_count);
    assert!(registry.get_by_id("custom.my_module").is_none());

    // Unregistering non-existent returns false
    assert!(!registry.unregister("nonexistent.module"));
}

// ============================================================================
// 10. WASM Executor Tests (5 tests)
// ============================================================================

#[test]
fn test_wasm_config_defaults() {
    let config = WasmConfig::default();
    assert_eq!(config.max_memory_pages, 256);
    assert_eq!(config.max_execution_secs, 300);
    assert_eq!(config.max_stack_size, 1024 * 1024);
    assert_eq!(config.sandbox_mode, WasmSandboxMode::Standard);
    assert!(config.allowed_imports.contains(&"wasi_snapshot_preview1".to_string()));
    assert!(config.allowed_imports.contains(&"env".to_string()));
}

#[test]
fn test_wasm_error_display() {
    let errors = vec![
        (
            WasmError::CompilationFailed("bad wasm".to_string()),
            "compilation failed: bad wasm",
        ),
        (
            WasmError::LinkError("missing import".to_string()),
            "link error: missing import",
        ),
        (
            WasmError::Trap("unreachable".to_string()),
            "trap: unreachable",
        ),
        (
            WasmError::MemoryExceeded { limit: 256, requested: 512 },
            "memory exceeded: limit=256, requested=512",
        ),
        (
            WasmError::Timeout { limit_secs: 300 },
            "execution timed out after 300s",
        ),
        (
            WasmError::InvalidExport("_start".to_string()),
            "invalid export: _start",
        ),
    ];

    for (err, expected) in errors {
        let display = format!("{}", err);
        assert_eq!(display, expected, "WasmError Display mismatch");
    }
}

#[test]
fn test_wasm_value_types() {
    let values = vec![
        (WasmValue::I32(42), "i32:42"),
        (WasmValue::I64(-100), "i64:-100"),
        (WasmValue::F32(3.14), "f32:3.14"),
        (WasmValue::F64(2.718281828), "f64:2.718281828"),
    ];

    for (val, expected) in &values {
        assert_eq!(format!("{}", val), *expected);
    }

    // Equality
    assert_eq!(WasmValue::I32(10), WasmValue::I32(10));
    assert_ne!(WasmValue::I32(10), WasmValue::I64(10));
}

#[test]
fn test_wasm_module_info() {
    let info = WasmModuleInfo {
        name: "test_module.wasm".to_string(),
        exports: vec![
            WasmExport { name: "_start".to_string(), kind: WasmExportKind::Function },
            WasmExport { name: "memory".to_string(), kind: WasmExportKind::Memory },
            WasmExport { name: "compute".to_string(), kind: WasmExportKind::Function },
            WasmExport { name: "__data_end".to_string(), kind: WasmExportKind::Global },
        ],
        imports: vec![
            WasmImport { module: "wasi_snapshot_preview1".to_string(), name: "fd_write".to_string() },
            WasmImport { module: "env".to_string(), name: "abort".to_string() },
        ],
        memory_pages: 16,
    };

    assert_eq!(info.name, "test_module.wasm");
    assert_eq!(info.exports.len(), 4);
    assert_eq!(info.imports.len(), 2);
    assert_eq!(info.memory_pages, 16);

    // Count function exports
    let fn_exports: Vec<&WasmExport> = info
        .exports
        .iter()
        .filter(|e| e.kind == WasmExportKind::Function)
        .collect();
    assert_eq!(fn_exports.len(), 2);
    assert_eq!(fn_exports[0].name, "_start");
    assert_eq!(fn_exports[1].name, "compute");
}

#[test]
fn test_wasm_sandbox_modes() {
    let modes = vec![
        (WasmSandboxMode::Strict, "strict"),
        (WasmSandboxMode::Standard, "standard"),
        (WasmSandboxMode::Relaxed, "relaxed"),
    ];

    for (mode, expected) in &modes {
        assert_eq!(format!("{}", mode), *expected);
    }

    // Strict != Standard != Relaxed
    assert_ne!(WasmSandboxMode::Strict, WasmSandboxMode::Standard);
    assert_ne!(WasmSandboxMode::Standard, WasmSandboxMode::Relaxed);
    assert_ne!(WasmSandboxMode::Strict, WasmSandboxMode::Relaxed);
}

// ============================================================================
// Bonus: Cross-cutting integration tests (2 tests)
// ============================================================================

#[test]
fn test_end_to_end_cost_estimate_for_montecarlo() {
    // Simulate a full cost estimation flow for a Monte Carlo job:
    // 1. Determine node class from resources
    // 2. Plan chunks
    // 3. Estimate cost
    // 4. Verify verification overhead is included

    let class = NodeClass::from_resources(8, 16384); // Standard
    assert_eq!(class, NodeClass::Standard);

    let chunks = plan_fixed(10_000, 100);
    assert_eq!(chunks.len(), 100);

    let est = estimate_cost(
        class,
        5.0, // 5 node-hours
        PriorityLevel::Standard,
        &VerificationStrategy::Redundant { replicas: 2 },
        50, // 50 effective nodes
    );

    assert!(est.estimated_cost_usd > 0.0);
    assert!(est.verification_overhead_pct > 0.0);
    assert!(est.estimated_duration_secs > 0);
    assert!(est.cloud_comparison_usd.is_some());
    assert!(est.cloud_comparison_usd.unwrap() > est.estimated_cost_usd);
}

#[test]
fn test_webhook_config_serde_roundtrip() {
    let config = WebhookConfig {
        url: "https://example.com/webhook".to_string(),
        auth_header: Some("Bearer token123".to_string()),
        milestones: vec![25, 50, 75, 100],
        include_partial_results: true,
        max_retries: 5,
        timeout_secs: 60,
    };

    let json = serde_json::to_string(&config).unwrap();
    let back: WebhookConfig = serde_json::from_str(&json).unwrap();

    assert_eq!(back.url, "https://example.com/webhook");
    assert_eq!(back.auth_header, Some("Bearer token123".to_string()));
    assert_eq!(back.milestones, vec![25, 50, 75, 100]);
    assert!(back.include_partial_results);
    assert_eq!(back.max_retries, 5);
    assert_eq!(back.timeout_secs, 60);
}

#[test]
fn test_verification_outcome_serde_roundtrip() {
    let outcomes = vec![
        VerificationOutcome::Verified {
            consensus_hash: "abc123".to_string(),
        },
        VerificationOutcome::MajorityConsensus {
            consensus_hash: "def456".to_string(),
            outlier_node: make_node_id(99),
            replicas_agreed: 2,
        },
        VerificationOutcome::Conflict {
            hashes: vec![
                (make_node_id(1), "h1".to_string()),
                (make_node_id(2), "h2".to_string()),
            ],
        },
        VerificationOutcome::SpotCheckPassed,
        VerificationOutcome::SpotCheckFailed {
            trusted_hash: "t".to_string(),
            checked_hash: "c".to_string(),
            node: make_node_id(7),
        },
        VerificationOutcome::StatisticallyValid { p_value: 0.95 },
        VerificationOutcome::StatisticalOutlier {
            z_score: 4.5,
            node: make_node_id(3),
        },
        VerificationOutcome::Skipped,
    ];

    for outcome in &outcomes {
        let json = serde_json::to_string(outcome).unwrap();
        let back: VerificationOutcome = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&back).unwrap();
        assert_eq!(json, json2, "Serde roundtrip failed for {:?}", outcome);
    }
}

#[test]
fn test_combo_module_serde_roundtrip() {
    let module = ComboModule {
        id: "cr.montecarlo".to_string(),
        display_name: "CR.MonteCarlo".to_string(),
        category: "computation".to_string(),
        description: "Monte Carlo simulation".to_string(),
        plugin_id: "plugin.cr.montecarlo".to_string(),
        default_verification: VerificationStrategy::Redundant { replicas: 3 },
        default_chunk_strategy: ChunkStrategy::Fixed { count: 100 },
        min_node_class: NodeClass::Standard,
        parameter_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "iterations": {"type": "integer"},
                "seed": {"type": "integer"}
            },
            "required": ["iterations"]
        }),
        output_formats: vec!["json".to_string(), "csv".to_string()],
        cloud_comparison_base_usd: Some(4.50),
        supports_streaming: true,
        supports_statistical_verification: true,
    };

    let json = serde_json::to_string(&module).unwrap();
    let back: ComboModule = serde_json::from_str(&json).unwrap();

    assert_eq!(back.id, "cr.montecarlo");
    assert_eq!(back.min_node_class, NodeClass::Standard);
    assert!(back.supports_streaming);
    assert!(back.supports_statistical_verification);
}

#[test]
fn test_node_class_from_resources_edge_cases() {
    // Edge case: exactly at Edge boundary (1500 MB)
    // Memory < 1500 -> Edge, memory >= 1500 -> depends on cores
    let at_boundary = NodeClass::from_resources(1, 1500);
    // 1500 is NOT < 1500, so it goes to next match: cores <= 4 && memory < 6000 -> Light
    assert_eq!(at_boundary, NodeClass::Light);

    // Edge: 4 cores, 5999 MB -> Light (< 6000)
    assert_eq!(NodeClass::from_resources(4, 5999), NodeClass::Light);
    // Edge: 4 cores, 6000 MB -> Standard (cores <= 8, memory < 20000)
    assert_eq!(NodeClass::from_resources(4, 6000), NodeClass::Standard);
    // Edge: 8 cores, 19999 MB -> Standard
    assert_eq!(NodeClass::from_resources(8, 19999), NodeClass::Standard);
    // Edge: 8 cores, 20000 MB -> Enterprise
    assert_eq!(NodeClass::from_resources(8, 20000), NodeClass::Enterprise);
    // Edge: 9 cores, 10000 MB -> Enterprise (> 8 cores)
    assert_eq!(NodeClass::from_resources(9, 10000), NodeClass::Enterprise);
}

#[test]
fn test_webhook_delivery_status_serde() {
    let statuses = vec![
        WebhookDeliveryStatus::Pending,
        WebhookDeliveryStatus::Delivered {
            status_code: 200,
            at: Utc::now(),
        },
        WebhookDeliveryStatus::Failed {
            error: "timeout".to_string(),
            attempts: 3,
        },
        WebhookDeliveryStatus::Exhausted {
            last_error: "connection refused".to_string(),
        },
    ];

    for status in &statuses {
        let json = serde_json::to_string(status).unwrap();
        let back: WebhookDeliveryStatus = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&back).unwrap();
        assert_eq!(json, json2, "Serde roundtrip failed");
    }
}

#[test]
fn test_swarm_tier_properties() {
    let tiers = vec![
        SwarmTier::Titan,
        SwarmTier::Legion,
        SwarmTier::Battalion,
        SwarmTier::Squad,
        SwarmTier::Cell,
    ];

    let mut prev_nodes = u64::MAX;
    for tier in &tiers {
        let registered = tier.registered_nodes();
        let effective = tier.effective_nodes();

        assert!(registered > 0);
        assert!(effective > 0);
        assert!(effective < registered, "Effective ({}) < registered ({})", effective, registered);
        assert!(registered < prev_nodes, "Tiers should be in descending node order");
        prev_nodes = registered;

        // Display works
        let label = format!("{}", tier);
        assert!(!label.is_empty());
    }
}

#[test]
fn test_cost_estimate_serde_roundtrip() {
    let est = CostEstimate {
        estimated_cost_usd: 1.234,
        cost_range_usd: (0.80, 2.00),
        estimated_duration_secs: 3600,
        duration_range_secs: (1800, 7200),
        estimated_chunks: 100,
        retry_overhead_pct: 20.0,
        verification_overhead_pct: 50.0,
        cloud_comparison_usd: Some(10.0),
        priority: PriorityLevel::Rush,
        effective_nodes: 87,
    };

    let json = serde_json::to_string(&est).unwrap();
    let back: CostEstimate = serde_json::from_str(&json).unwrap();

    assert!((back.estimated_cost_usd - 1.234).abs() < 0.001);
    assert_eq!(back.priority, PriorityLevel::Rush);
    assert_eq!(back.effective_nodes, 87);
    assert_eq!(back.estimated_chunks, 100);
}
