// Marabunta - Licensed under the MIT License.
//! Verification engine for the Marabunta Swarm.
//!
//! Implements the three verification strategies described in the failure model:
//!
//! 1. **Redundant Execution** — Run the same chunk on N independent nodes,
//!    compare output hashes.  If all agree → verified.  If majority agree →
//!    outlier discarded, node penalised.  If no majority → conflict, requeue.
//!
//! 2. **Spot Checking** — Trusted nodes (high reputation, many completed jobs)
//!    get single-execution privileges.  A random fraction of their results are
//!    independently re-executed and compared.
//!
//! 3. **Statistical Validation** — For aggregate workloads (Monte Carlo,
//!    bootstrapping) where individual results don't matter, we validate the
//!    distribution shape rather than per-chunk correctness.
//!
//! # Architecture
//!
//! ```text
//!   VerificationEngine
//!   ├── ReplicaTracker      — tracks pending replicas per chunk
//!   ├── ResultCollector      — collects and compares hashes
//!   ├── TrustEvaluator       — decides if node is trusted enough for spot-check
//!   ├── StatisticalValidator — z-score / distribution checks
//!   ├── VerificationStore    — persisted verification records
//!   └── VerificationMetrics  — counters for dashboards
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use super::config::{
    VERIFICATION_DEFAULT_REPLICAS,
    VERIFICATION_REPLICA_TIMEOUT_SECS, VERIFICATION_SPOT_CHECK_RATE,
    VERIFICATION_TRUST_THRESHOLD_JOBS,
};
use super::types::{ChunkId, NodeId, VerificationOutcome, VerificationStrategy};
use crate::common::types::JobId;

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of verification records retained per job.
const MAX_RECORDS_PER_JOB: usize = 50_000;

/// Maximum number of statistical samples retained for distribution checks.
const MAX_STATISTICAL_SAMPLES: usize = 100_000;

/// Window size for rolling trust evaluation (recent jobs).
const TRUST_WINDOW_SIZE: usize = 200;

/// Minimum agreement fraction for majority consensus.
const MAJORITY_FRACTION: f64 = 0.5;

/// Default timeout for replica completion (seconds).
const DEFAULT_REPLICA_TIMEOUT_SECS: u64 = 300;

/// Maximum age for hash cache entries (seconds).
const HASH_CACHE_MAX_AGE_SECS: u64 = 3_600;

// ============================================================================
// ReplicaResult
// ============================================================================

/// Result submitted by a single node for a chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaResult {
    /// Which node produced this result.
    pub node_id: NodeId,
    /// Hash of the result payload (SHA-256 hex).
    pub result_hash: String,
    /// Size of the result payload in bytes.
    pub result_size_bytes: u64,
    /// How long the node took to compute (milliseconds).
    pub compute_duration_ms: u64,
    /// When the result was received.
    pub received_at: DateTime<Utc>,
    /// Optional raw result bytes (for spot-check comparison).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_payload: Option<Vec<u8>>,
    /// Optional hash of the heavy result data stored in the DHT BlobStore.
    #[serde(default)]
    pub output_blob_hash: Option<crate::swarm::types::BlobHash>,
}

impl ReplicaResult {
    /// Create a new replica result, computing the hash from payload.
    pub fn from_payload(node_id: NodeId, payload: &[u8], compute_duration_ms: u64, output_blob_hash: Option<crate::swarm::types::BlobHash>) -> Self {
        let result_hash = if let Some(hash) = output_blob_hash {
            crate::swarm::blobstore::hash_hex(&hash)
        } else {
            let mut hasher = Sha256::new();
            hasher.update(payload);
            format!("{:x}", hasher.finalize())
        };

        Self {
            node_id,
            result_hash,
            result_size_bytes: payload.len() as u64,
            compute_duration_ms,
            received_at: Utc::now(),
            raw_payload: None,
            output_blob_hash,
        }
    }

    /// Create with a pre-computed hash.
    pub fn with_hash(node_id: NodeId, result_hash: String, result_size_bytes: u64, compute_duration_ms: u64) -> Self {
        Self {
            node_id,
            result_hash,
            result_size_bytes,
            compute_duration_ms,
            received_at: Utc::now(),
            raw_payload: None,
            output_blob_hash: None,
        }
    }

    /// Attach the raw payload for deep comparison.
    pub fn with_raw_payload(mut self, payload: Vec<u8>) -> Self {
        self.raw_payload = Some(payload);
        self
    }
}

// ============================================================================
// ReplicaSet
// ============================================================================

/// Tracks all replicas for a single chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaSet {
    /// The chunk being verified.
    pub chunk_id: ChunkId,
    /// Parent job.
    pub job_id: JobId,
    /// Which verification strategy is active.
    pub strategy: VerificationStrategy,
    /// How many replicas are required.
    pub required_replicas: u8,
    /// Nodes assigned to execute this chunk.
    pub assigned_nodes: Vec<NodeId>,
    /// Results received so far.
    pub results: Vec<ReplicaResult>,
    /// When this replica set was created.
    pub created_at: DateTime<Utc>,
    /// Deadline for all replicas to report.
    pub deadline: DateTime<Utc>,
    /// Whether verification is complete.
    pub resolved: bool,
    /// Final outcome, if resolved.
    pub outcome: Option<VerificationOutcome>,
}

impl ReplicaSet {
    /// Create a new replica set for redundant execution.
    pub fn new_redundant(chunk_id: ChunkId, job_id: JobId, replicas: u8, assigned_nodes: Vec<NodeId>) -> Self {
        let now = Utc::now();
        let deadline = now + chrono::Duration::seconds(VERIFICATION_REPLICA_TIMEOUT_SECS as i64);
        Self {
            chunk_id,
            job_id,
            strategy: VerificationStrategy::Redundant { replicas },
            required_replicas: replicas,
            assigned_nodes,
            results: Vec::with_capacity(replicas as usize),
            created_at: now,
            deadline,
            resolved: false,
            outcome: None,
        }
    }

    /// Create a replica set for spot checking (single execution + random re-check).
    pub fn new_spot_check(chunk_id: ChunkId, job_id: JobId, primary_node: NodeId, check_rate: f64) -> Self {
        let now = Utc::now();
        let deadline = now + chrono::Duration::seconds(VERIFICATION_REPLICA_TIMEOUT_SECS as i64);
        Self {
            chunk_id,
            job_id,
            strategy: VerificationStrategy::SpotCheck { check_rate },
            required_replicas: 1,
            assigned_nodes: vec![primary_node],
            results: Vec::with_capacity(2),
            created_at: now,
            deadline,
            resolved: false,
            outcome: None,
        }
    }

    /// Create a replica set for statistical validation.
    pub fn new_statistical(chunk_id: ChunkId, job_id: JobId, node: NodeId, tolerance: f64) -> Self {
        let now = Utc::now();
        let deadline = now + chrono::Duration::seconds(VERIFICATION_REPLICA_TIMEOUT_SECS as i64);
        Self {
            chunk_id,
            job_id,
            strategy: VerificationStrategy::Statistical { outlier_tolerance: tolerance },
            required_replicas: 1,
            assigned_nodes: vec![node],
            results: Vec::with_capacity(1),
            created_at: now,
            deadline,
            resolved: false,
            outcome: None,
        }
    }

    /// Create a replica set with no verification.
    pub fn new_unverified(chunk_id: ChunkId, job_id: JobId, node: NodeId) -> Self {
        let now = Utc::now();
        Self {
            chunk_id,
            job_id,
            strategy: VerificationStrategy::None,
            required_replicas: 1,
            assigned_nodes: vec![node],
            results: Vec::new(),
            created_at: now,
            deadline: now + chrono::Duration::seconds(VERIFICATION_REPLICA_TIMEOUT_SECS as i64),
            resolved: false,
            outcome: None,
        }
    }

    /// Number of results still needed.
    pub fn pending_count(&self) -> u8 {
        self.required_replicas.saturating_sub(self.results.len() as u8)
    }

    /// Whether all required results have been received.
    pub fn is_complete(&self) -> bool {
        self.results.len() >= self.required_replicas as usize
    }

    /// Whether the deadline has passed.
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.deadline
    }

    /// Add a result. Returns true if the set is now complete.
    pub fn add_result(&mut self, result: ReplicaResult) -> bool {
        if self.resolved {
            return false;
        }
        self.results.push(result);
        self.is_complete()
    }
}

// ============================================================================
// VerificationRecord
// ============================================================================

/// Permanent record of a completed verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationRecord {
    pub chunk_id: ChunkId,
    pub job_id: JobId,
    pub strategy: VerificationStrategy,
    pub outcome: VerificationOutcome,
    pub participating_nodes: Vec<NodeId>,
    pub consensus_hash: Option<String>,
    pub verified_at: DateTime<Utc>,
    pub verification_duration_ms: u64,
    pub replicas_submitted: u8,
    pub replicas_required: u8,
}

// ============================================================================
// TrustProfile
// ============================================================================

/// Rolling trust profile for a node, used to decide spot-check eligibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustProfile {
    pub node_id: NodeId,
    /// Total verified results submitted by this node.
    pub total_verified: u64,
    /// Total failed verifications.
    pub total_failed: u64,
    /// Recent outcomes (true = success, false = failure).
    pub recent_outcomes: VecDeque<bool>,
    /// Current trust score (0.0 = untrusted, 1.0 = fully trusted).
    pub trust_score: f64,
    /// Whether this node qualifies for spot-check-only verification.
    pub spot_check_eligible: bool,
    /// Last time trust was recalculated.
    pub last_evaluated: DateTime<Utc>,
}

impl TrustProfile {
    /// Create a new profile for a fresh node.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            total_verified: 0,
            total_failed: 0,
            recent_outcomes: VecDeque::with_capacity(TRUST_WINDOW_SIZE),
            trust_score: 0.0,
            spot_check_eligible: false,
            last_evaluated: Utc::now(),
        }
    }

    /// Record a verification outcome.
    pub fn record_outcome(&mut self, success: bool) {
        if success {
            self.total_verified += 1;
        } else {
            self.total_failed += 1;
        }
        if self.recent_outcomes.len() >= TRUST_WINDOW_SIZE {
            self.recent_outcomes.pop_front();
        }
        self.recent_outcomes.push_back(success);
        self.recalculate();
    }

    /// Recalculate trust score from recent outcomes.
    fn recalculate(&mut self) {
        if self.recent_outcomes.is_empty() {
            self.trust_score = 0.0;
            self.spot_check_eligible = false;
            self.last_evaluated = Utc::now();
            return;
        }

        let successes = self.recent_outcomes.iter().filter(|&&s| s).count();
        let total = self.recent_outcomes.len();
        self.trust_score = successes as f64 / total as f64;

        // Spot check eligible: high trust score AND enough total jobs
        self.spot_check_eligible = self.trust_score >= 0.95
            && self.total_verified >= VERIFICATION_TRUST_THRESHOLD_JOBS;

        self.last_evaluated = Utc::now();
    }

    /// Success rate over the recent window.
    pub fn recent_success_rate(&self) -> f64 {
        self.trust_score
    }

    /// Total jobs completed.
    pub fn total_jobs(&self) -> u64 {
        self.total_verified + self.total_failed
    }

    /// Reset trust (e.g., after catching malicious behaviour).
    pub fn reset_trust(&mut self) {
        self.trust_score = 0.0;
        self.spot_check_eligible = false;
        self.recent_outcomes.clear();
        self.last_evaluated = Utc::now();
    }
}

// ============================================================================
// StatisticalSample
// ============================================================================

/// A single numerical sample for statistical validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalSample {
    pub chunk_id: ChunkId,
    pub node_id: NodeId,
    pub value: f64,
    pub received_at: DateTime<Utc>,
}

// ============================================================================
// VerificationStore
// ============================================================================

/// Persistent storage for verification state.
pub struct VerificationStore {
    /// Active (unresolved) replica sets.
    active_sets: DashMap<ChunkId, ReplicaSet>,
    /// Completed verification records, keyed by job.
    records: DashMap<JobId, Vec<VerificationRecord>>,
    /// Trust profiles per node.
    trust_profiles: DashMap<NodeId, TrustProfile>,
    /// Statistical samples per job (for statistical validation).
    statistical_samples: DashMap<JobId, Vec<StatisticalSample>>,
    /// Hash cache for deduplication (hash → chunk_ids with that hash).
    hash_cache: DashMap<String, Vec<(ChunkId, DateTime<Utc>)>>,
}

impl VerificationStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self {
            active_sets: DashMap::new(),
            records: DashMap::new(),
            trust_profiles: DashMap::new(),
            statistical_samples: DashMap::new(),
            hash_cache: DashMap::new(),
        }
    }

    /// Register a new replica set.
    pub fn register_set(&self, set: ReplicaSet) {
        self.active_sets.insert(set.chunk_id, set);
    }

    /// Get an active replica set.
    pub fn get_active_set(&self, chunk_id: &ChunkId) -> Option<ReplicaSet> {
        self.active_sets.get(chunk_id).map(|r| r.value().clone())
    }

    /// Get a mutable reference via closure.
    pub fn with_active_set_mut<F, R>(&self, chunk_id: &ChunkId, f: F) -> Option<R>
    where
        F: FnOnce(&mut ReplicaSet) -> R,
    {
        self.active_sets.get_mut(chunk_id).map(|mut r| f(r.value_mut()))
    }

    /// Remove a completed replica set from active tracking.
    pub fn remove_active_set(&self, chunk_id: &ChunkId) -> Option<ReplicaSet> {
        self.active_sets.remove(chunk_id).map(|(_, v)| v)
    }

    /// Store a completed verification record.
    pub fn add_record(&self, record: VerificationRecord) {
        let job_id = record.job_id;
        let mut entry = self.records.entry(job_id).or_default();
        if entry.len() < MAX_RECORDS_PER_JOB {
            entry.push(record);
        }
    }

    /// Get all records for a job.
    pub fn get_records(&self, job_id: &JobId) -> Vec<VerificationRecord> {
        self.records.get(job_id).map(|r| r.value().clone()).unwrap_or_default()
    }

    /// Get or create a trust profile.
    pub fn get_trust_profile(&self, node_id: &NodeId) -> TrustProfile {
        self.trust_profiles
            .entry(*node_id)
            .or_insert_with(|| TrustProfile::new(*node_id))
            .value()
            .clone()
    }

    /// Update trust profile.
    pub fn update_trust(&self, node_id: &NodeId, success: bool) {
        let mut entry = self.trust_profiles
            .entry(*node_id)
            .or_insert_with(|| TrustProfile::new(*node_id));
        entry.value_mut().record_outcome(success);
    }

    /// Reset a node's trust (after catching bad behaviour).
    pub fn reset_trust(&self, node_id: &NodeId) {
        if let Some(mut profile) = self.trust_profiles.get_mut(node_id) {
            profile.value_mut().reset_trust();
        }
    }

    /// Check if a node is eligible for spot-check-only verification.
    pub fn is_spot_check_eligible(&self, node_id: &NodeId) -> bool {
        self.trust_profiles
            .get(node_id)
            .map(|p| p.value().spot_check_eligible)
            .unwrap_or(false)
    }

    /// Add a statistical sample.
    pub fn add_statistical_sample(&self, job_id: JobId, sample: StatisticalSample) {
        let mut entry = self.statistical_samples.entry(job_id).or_default();
        if entry.len() < MAX_STATISTICAL_SAMPLES {
            entry.push(sample);
        }
    }

    /// Get all statistical samples for a job.
    pub fn get_statistical_samples(&self, job_id: &JobId) -> Vec<StatisticalSample> {
        self.statistical_samples.get(job_id).map(|r| r.value().clone()).unwrap_or_default()
    }

    /// Cache a result hash for deduplication.
    pub fn cache_hash(&self, hash: &str, chunk_id: ChunkId) {
        let mut entry = self.hash_cache.entry(hash.to_string()).or_default();
        entry.push((chunk_id, Utc::now()));
    }

    /// Look up chunks with a matching hash (for dedup).
    pub fn lookup_hash(&self, hash: &str) -> Vec<ChunkId> {
        let cutoff = Utc::now() - chrono::Duration::seconds(HASH_CACHE_MAX_AGE_SECS as i64);
        self.hash_cache
            .get(hash)
            .map(|r| {
                r.value()
                    .iter()
                    .filter(|(_, ts)| *ts > cutoff)
                    .map(|(id, _)| *id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Count active (unresolved) replica sets.
    pub fn active_count(&self) -> usize {
        self.active_sets.len()
    }

    /// Count all completed records.
    pub fn total_records(&self) -> usize {
        self.records.iter().map(|r| r.value().len()).sum()
    }

    /// Count tracked nodes.
    pub fn tracked_nodes(&self) -> usize {
        self.trust_profiles.len()
    }

    /// Get all expired replica sets (deadline passed, not resolved).
    pub fn get_expired_sets(&self) -> Vec<ReplicaSet> {
        let now = Utc::now();
        self.active_sets
            .iter()
            .filter(|r| !r.value().resolved && now > r.value().deadline)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Get all active sets for a job.
    pub fn get_sets_for_job(&self, job_id: &JobId) -> Vec<ReplicaSet> {
        self.active_sets
            .iter()
            .filter(|r| r.value().job_id == *job_id)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Purge old hash cache entries.
    pub fn purge_hash_cache(&self) {
        let cutoff = Utc::now() - chrono::Duration::seconds(HASH_CACHE_MAX_AGE_SECS as i64);
        self.hash_cache.retain(|_, entries| {
            entries.retain(|(_, ts)| *ts > cutoff);
            !entries.is_empty()
        });
    }

    /// Purge records for a completed job.
    pub fn purge_job(&self, job_id: &JobId) {
        self.records.remove(job_id);
        self.statistical_samples.remove(job_id);
        self.active_sets.retain(|_, set| set.job_id != *job_id);
    }

    /// Get summary statistics.
    pub fn summary(&self) -> VerificationSummary {
        let mut total_verified = 0u64;
        let mut total_failed = 0u64;
        let mut total_conflicts = 0u64;
        let mut total_spot_passed = 0u64;
        let mut total_spot_failed = 0u64;
        let mut total_statistical = 0u64;

        for entry in self.records.iter() {
            for record in entry.value() {
                match &record.outcome {
                    VerificationOutcome::Verified { .. } => total_verified += 1,
                    VerificationOutcome::MajorityConsensus { .. } => total_verified += 1,
                    VerificationOutcome::Conflict { .. } => total_conflicts += 1,
                    VerificationOutcome::SpotCheckPassed => total_spot_passed += 1,
                    VerificationOutcome::SpotCheckFailed { .. } => total_spot_failed += 1,
                    VerificationOutcome::StatisticallyValid { .. } => total_statistical += 1,
                    VerificationOutcome::StatisticalOutlier { .. } => total_failed += 1,
                    VerificationOutcome::Skipped => {}
                }
            }
        }

        let trusted_nodes = self.trust_profiles.iter()
            .filter(|r| r.value().spot_check_eligible)
            .count();

        VerificationSummary {
            active_sets: self.active_sets.len() as u64,
            total_records: self.total_records() as u64,
            total_verified,
            total_failed,
            total_conflicts,
            total_spot_passed,
            total_spot_failed,
            total_statistical,
            tracked_nodes: self.trust_profiles.len() as u64,
            trusted_nodes: trusted_nodes as u64,
        }
    }
}

impl Default for VerificationStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// VerificationSummary
// ============================================================================

/// Summary statistics for the verification system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationSummary {
    pub active_sets: u64,
    pub total_records: u64,
    pub total_verified: u64,
    pub total_failed: u64,
    pub total_conflicts: u64,
    pub total_spot_passed: u64,
    pub total_spot_failed: u64,
    pub total_statistical: u64,
    pub tracked_nodes: u64,
    pub trusted_nodes: u64,
}

impl VerificationSummary {
    /// Overall success rate.
    pub fn success_rate(&self) -> f64 {
        let total = self.total_verified + self.total_failed + self.total_conflicts;
        if total == 0 {
            return 1.0;
        }
        self.total_verified as f64 / total as f64
    }

    /// Conflict rate.
    pub fn conflict_rate(&self) -> f64 {
        let total = self.total_verified + self.total_failed + self.total_conflicts;
        if total == 0 {
            return 0.0;
        }
        self.total_conflicts as f64 / total as f64
    }

    /// Spot check failure rate.
    pub fn spot_check_failure_rate(&self) -> f64 {
        let total = self.total_spot_passed + self.total_spot_failed;
        if total == 0 {
            return 0.0;
        }
        self.total_spot_failed as f64 / total as f64
    }
}

// ============================================================================
// ResultComparator
// ============================================================================

/// Compares replica results and produces a verification outcome.
pub struct ResultComparator;

impl ResultComparator {
    /// Compare results from a completed redundant replica set.
    pub fn compare_redundant(results: &[ReplicaResult]) -> VerificationOutcome {
        if results.is_empty() {
            return VerificationOutcome::Conflict {
                hashes: Vec::new(),
            };
        }

        // Group by hash
        let mut hash_groups: HashMap<&str, Vec<&ReplicaResult>> = HashMap::new();
        for r in results {
            hash_groups.entry(&r.result_hash).or_default().push(r);
        }

        // All agree
        if hash_groups.len() == 1 {
            let hash = results[0].result_hash.clone();
            return VerificationOutcome::Verified {
                consensus_hash: hash,
            };
        }

        // Find majority
        let total = results.len();
        let mut best_hash = "";
        let mut best_count = 0usize;
        let mut outliers: Vec<&ReplicaResult> = Vec::new();

        for (hash, group) in &hash_groups {
            if group.len() > best_count {
                best_hash = hash;
                best_count = group.len();
            }
        }

        if best_count as f64 / total as f64 > MAJORITY_FRACTION {
            // Majority consensus — identify outlier(s)
            for (hash, group) in &hash_groups {
                if *hash != best_hash {
                    outliers.extend(group.iter());
                }
            }
            let outlier_node = outliers.first().map(|r| r.node_id).unwrap_or_default();
            return VerificationOutcome::MajorityConsensus {
                consensus_hash: best_hash.to_string(),
                outlier_node,
                replicas_agreed: best_count as u8,
            };
        }

        // No majority — full conflict
        let hashes: Vec<(NodeId, String)> = results
            .iter()
            .map(|r| (r.node_id, r.result_hash.clone()))
            .collect();
        VerificationOutcome::Conflict { hashes }
    }

    /// Compare a spot-check result against the trusted result.
    pub fn compare_spot_check(
        trusted_result: &ReplicaResult,
        check_result: &ReplicaResult,
    ) -> VerificationOutcome {
        if trusted_result.result_hash == check_result.result_hash {
            VerificationOutcome::SpotCheckPassed
        } else {
            VerificationOutcome::SpotCheckFailed {
                trusted_hash: trusted_result.result_hash.clone(),
                checked_hash: check_result.result_hash.clone(),
                node: trusted_result.node_id,
            }
        }
    }
}

// ============================================================================
// StatisticalValidator
// ============================================================================

/// Validates results using statistical methods for aggregate workloads.
pub struct StatisticalValidator;

impl StatisticalValidator {
    /// Validate a single sample against the distribution of all samples.
    /// Returns the z-score. If |z| > tolerance → outlier.
    pub fn validate_sample(samples: &[f64], new_value: f64, tolerance: f64) -> (f64, bool) {
        if samples.len() < 10 {
            // Not enough data to validate statistically
            return (0.0, true);
        }

        let mean = Self::mean(samples);
        let std_dev = Self::std_dev(samples, mean);

        if std_dev < f64::EPSILON {
            // All values identical — any deviation is suspicious
            let is_match = (new_value - mean).abs() < f64::EPSILON;
            return (0.0, is_match);
        }

        let z_score = (new_value - mean) / std_dev;
        let is_valid = z_score.abs() <= tolerance;
        (z_score, is_valid)
    }

    /// Validate an entire batch of samples using Grubbs' test simplified.
    pub fn validate_batch(samples: &[f64], tolerance: f64) -> Vec<(usize, f64)> {
        if samples.len() < 10 {
            return Vec::new();
        }

        let mean = Self::mean(samples);
        let std_dev = Self::std_dev(samples, mean);

        if std_dev < f64::EPSILON {
            return Vec::new();
        }

        let mut outliers = Vec::new();
        for (i, &val) in samples.iter().enumerate() {
            let z = (val - mean) / std_dev;
            if z.abs() > tolerance {
                outliers.push((i, z));
            }
        }
        outliers
    }

    /// Compute the mean of a sample set.
    pub fn mean(samples: &[f64]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        samples.iter().sum::<f64>() / samples.len() as f64
    }

    /// Compute the standard deviation.
    pub fn std_dev(samples: &[f64], mean: f64) -> f64 {
        if samples.len() < 2 {
            return 0.0;
        }
        let variance = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
            / (samples.len() - 1) as f64;
        variance.sqrt()
    }

    /// Compute the median.
    pub fn median(samples: &mut [f64]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = samples.len() / 2;
        if samples.len() % 2 == 0 {
            (samples[mid - 1] + samples[mid]) / 2.0
        } else {
            samples[mid]
        }
    }

    /// Interquartile range.
    pub fn iqr(samples: &mut [f64]) -> f64 {
        if samples.len() < 4 {
            return 0.0;
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let q1_idx = samples.len() / 4;
        let q3_idx = 3 * samples.len() / 4;
        samples[q3_idx] - samples[q1_idx]
    }

    /// Coefficient of variation (std_dev / mean).
    pub fn coefficient_of_variation(samples: &[f64]) -> f64 {
        let m = Self::mean(samples);
        if m.abs() < f64::EPSILON {
            return 0.0;
        }
        let sd = Self::std_dev(samples, m);
        sd / m.abs()
    }

    /// Check if a distribution is approximately normal using a simplified
    /// skewness/kurtosis check.
    pub fn is_approximately_normal(samples: &[f64]) -> bool {
        if samples.len() < 30 {
            return false;
        }
        let m = Self::mean(samples);
        let n = samples.len() as f64;
        let sd = Self::std_dev(samples, m);
        if sd < f64::EPSILON {
            return true;
        }

        // Skewness
        let skewness = samples.iter()
            .map(|x| ((x - m) / sd).powi(3))
            .sum::<f64>() / n;

        // Kurtosis (excess)
        let kurtosis = samples.iter()
            .map(|x| ((x - m) / sd).powi(4))
            .sum::<f64>() / n - 3.0;

        skewness.abs() < 2.0 && kurtosis.abs() < 7.0
    }
}

// ============================================================================
// API-facing types
// ============================================================================

/// Aggregate verification statistics returned to the API layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationStats {
    pub total_verified: u64,
    pub total_failed: u64,
    pub pending_count: u64,
}

impl VerificationStats {
    pub fn success_rate(&self) -> f64 {
        let total = self.total_verified + self.total_failed;
        if total == 0 { 1.0 } else { self.total_verified as f64 / total as f64 }
    }
}

/// Information about a pending verification, returned to the API layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingVerificationInfo {
    pub job_id: JobId,
    pub chunk_id: ChunkId,
    pub status: String,
    pub submitted_at: DateTime<Utc>,
}

// ============================================================================
// VerificationEngine
// ============================================================================

/// The main verification engine that orchestrates all verification strategies.
pub struct VerificationEngine {
    store: Arc<VerificationStore>,
    knowledge: Arc<super::knowledge::KnowledgeStore>,
    reputation: Arc<super::reputation::ReputationStore>,
    /// Counter for total verifications initiated.
    total_initiated: AtomicU64,
    /// Counter for total verifications completed.
    total_completed: AtomicU64,
    /// Counter for total conflicts detected.
    total_conflicts: AtomicU64,
    /// Counter for total trust resets.
    total_trust_resets: AtomicU64,
}

impl VerificationEngine {
    /// Create a new engine with knowledge and reputation dependencies.
    ///
    /// The engine creates its own internal [`VerificationStore`].
    pub fn new(
        knowledge: Arc<super::knowledge::KnowledgeStore>,
        reputation: Arc<super::reputation::ReputationStore>,
    ) -> Self {
        Self {
            store: Arc::new(VerificationStore::new()),
            knowledge,
            reputation,
            total_initiated: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_conflicts: AtomicU64::new(0),
            total_trust_resets: AtomicU64::new(0),
        }
    }

    /// Create a new engine from a pre-existing store (for testing).
    pub fn from_store(store: Arc<VerificationStore>, knowledge: Arc<super::knowledge::KnowledgeStore>, reputation: Arc<super::reputation::ReputationStore>) -> Self {
        Self {
            store,
            knowledge,
            reputation,
            total_initiated: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_conflicts: AtomicU64::new(0),
            total_trust_resets: AtomicU64::new(0),
        }
    }

    /// Get the underlying store.
    pub fn store(&self) -> &Arc<VerificationStore> {
        &self.store
    }

    /// Get aggregate verification statistics (used by the API layer).
    pub fn get_stats(&self) -> VerificationStats {
        let total_verified = self.total_completed.load(Ordering::Relaxed);
        let total_failed = self.total_conflicts.load(Ordering::Relaxed);
        let pending_count = self.store.active_sets.len() as u64;
        VerificationStats {
            total_verified,
            total_failed,
            pending_count,
        }
    }

    /// List pending (unresolved) verification sets (used by the API layer).
    pub fn pending_verifications(&self) -> Vec<PendingVerificationInfo> {
        self.store
            .active_sets
            .iter()
            .filter(|entry| !entry.value().resolved)
            .map(|entry| {
                let set = entry.value();
                PendingVerificationInfo {
                    job_id: set.job_id,
                    chunk_id: set.chunk_id,
                    status: format!("{}/{} replicas", set.results.len(), set.required_replicas),
                    submitted_at: set.deadline - chrono::Duration::seconds(VERIFICATION_REPLICA_TIMEOUT_SECS as i64),
                }
            })
            .collect()
    }

    // ----------------------------------------------------------------
    // Strategy selection
    // ----------------------------------------------------------------

    /// Decide which verification strategy to use for a chunk, considering
    /// the job's configured strategy and the assigned node's trust level.
    pub fn select_strategy(
        &self,
        job_strategy: &VerificationStrategy,
        node_id: &NodeId,
    ) -> VerificationStrategy {
        match job_strategy {
            // If the job requests no verification, respect it
            VerificationStrategy::None => VerificationStrategy::None,

            // If the job requests statistical validation, use it
            VerificationStrategy::Statistical { outlier_tolerance } => {
                VerificationStrategy::Statistical {
                    outlier_tolerance: *outlier_tolerance,
                }
            }

            // If the job requests spot-check, use it (node must be eligible)
            VerificationStrategy::SpotCheck { check_rate } => {
                if self.store.is_spot_check_eligible(node_id) {
                    VerificationStrategy::SpotCheck {
                        check_rate: *check_rate,
                    }
                } else {
                    // Node not trusted enough — fall back to redundant
                    VerificationStrategy::Redundant {
                        replicas: VERIFICATION_DEFAULT_REPLICAS,
                    }
                }
            }

            // Redundant: check if node qualifies for spot-check upgrade
            VerificationStrategy::Redundant { replicas } => {
                if self.store.is_spot_check_eligible(node_id) {
                    VerificationStrategy::SpotCheck {
                        check_rate: VERIFICATION_SPOT_CHECK_RATE,
                    }
                } else {
                    VerificationStrategy::Redundant {
                        replicas: *replicas,
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------
    // Initiation
    // ----------------------------------------------------------------

    /// Initiate verification for a chunk.
    pub fn initiate(
        &self,
        chunk_id: ChunkId,
        job_id: JobId,
        strategy: &VerificationStrategy,
        assigned_nodes: Vec<NodeId>,
    ) -> ReplicaSet {
        self.total_initiated.fetch_add(1, Ordering::Relaxed);

        let set = match strategy {
            VerificationStrategy::Redundant { replicas } => {
                ReplicaSet::new_redundant(chunk_id, job_id, *replicas, assigned_nodes)
            }
            VerificationStrategy::SpotCheck { check_rate } => {
                let primary = assigned_nodes.into_iter().next().unwrap_or_default();
                ReplicaSet::new_spot_check(chunk_id, job_id, primary, *check_rate)
            }
            VerificationStrategy::Statistical { outlier_tolerance } => {
                let node = assigned_nodes.into_iter().next().unwrap_or_default();
                ReplicaSet::new_statistical(chunk_id, job_id, node, *outlier_tolerance)
            }
            VerificationStrategy::None => {
                let node = assigned_nodes.into_iter().next().unwrap_or_default();
                ReplicaSet::new_unverified(chunk_id, job_id, node)
            }
        };

        debug!(
            chunk_id = %set.chunk_id,
            strategy = ?set.strategy,
            "Verification initiated"
        );

        self.store.register_set(set.clone());
        set
    }

    // ----------------------------------------------------------------
    // Cleanup
    // ----------------------------------------------------------------

    /// Purge verification records for a completed job to prevent RAM leaks.
    pub fn purge_job(&self, job_id: &JobId) {
        self.store.purge_job(job_id);
    }

    /// Submit a result for a chunk. Returns the verification outcome if the
    /// replica set is now complete, or None if more replicas are needed.
    pub fn submit_result(
        &self,
        chunk_id: &ChunkId,
        result: ReplicaResult,
    ) -> Option<VerificationOutcome> {
        let _node_id = result.node_id;

        // Add result to the set
        let is_complete = self.store.with_active_set_mut(chunk_id, |set| {
            set.add_result(result)
        });

        let is_complete = match is_complete {
            Some(complete) => complete,
            None => {
                warn!(chunk_id = %chunk_id, "Result submitted for unknown chunk");
                return None;
            }
        };

        if !is_complete {
            return None;
        }

        // All replicas received — resolve
        let set = self.store.get_active_set(chunk_id)?;

        let outcome = self.resolve(&set);

        // Update trust profiles
        self.update_trust_from_outcome(&set, &outcome);

        // Store record
        let record = VerificationRecord {
            chunk_id: *chunk_id,
            job_id: set.job_id,
            strategy: set.strategy.clone(),
            outcome: outcome.clone(),
            participating_nodes: set.results.iter().map(|r| r.node_id).collect(),
            consensus_hash: self.extract_consensus_hash(&outcome),
            verified_at: Utc::now(),
            verification_duration_ms: (Utc::now() - set.created_at).num_milliseconds().max(0) as u64,
            replicas_submitted: set.results.len() as u8,
            replicas_required: set.required_replicas,
        };

        self.store.add_record(record);

        // Cache the consensus hash
        if let Some(hash) = self.extract_consensus_hash(&outcome) {
            self.store.cache_hash(&hash, *chunk_id);
        }

        // Mark resolved and remove from active
        self.store.with_active_set_mut(chunk_id, |set| {
            set.resolved = true;
            set.outcome = Some(outcome.clone());
        });
        
        // 🛑 MEMORY LEAK FIX: We MUST actually remove the active set after it has been fully
        // resolved and logged, otherwise a 10-million-node swarm running a 1,000,000-chunk
        // parameter sweep will OOM crash the Orchestrator's verification engine entirely.
        self.store.remove_active_set(chunk_id);

        self.total_completed.fetch_add(1, Ordering::Relaxed);

        debug!(
            chunk_id = %chunk_id,
            outcome = ?outcome,
            "Verification resolved"
        );

        Some(outcome)
    }

    // ----------------------------------------------------------------
    // Resolution
    // ----------------------------------------------------------------

    /// Resolve a complete replica set into a verification outcome.
    fn resolve(&self, set: &ReplicaSet) -> VerificationOutcome {
        match &set.strategy {
            VerificationStrategy::None => VerificationOutcome::Skipped,

            VerificationStrategy::Redundant { .. } => {
                ResultComparator::compare_redundant(&set.results)
            }

            VerificationStrategy::SpotCheck { .. } => {
                if set.results.len() >= 2 {
                    ResultComparator::compare_spot_check(&set.results[0], &set.results[1])
                } else if set.results.len() == 1 {
                    // No checker ran — accept as passed
                    VerificationOutcome::SpotCheckPassed
                } else {
                    VerificationOutcome::Conflict {
                        hashes: Vec::new(),
                    }
                }
            }

            VerificationStrategy::Statistical { outlier_tolerance } => {
                // For statistical, we validate against accumulated samples
                if let Some(result) = set.results.first() {
                    // Try to parse the result as a numeric value
                    if let Ok(value) = result.result_hash.parse::<f64>() {
                        let samples = self.store.get_statistical_samples(&set.job_id);
                        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

                        let (z_score, is_valid) =
                            StatisticalValidator::validate_sample(&values, value, *outlier_tolerance);

                        if is_valid {
                            VerificationOutcome::StatisticallyValid {
                                p_value: Self::z_to_p(z_score),
                            }
                        } else {
                            VerificationOutcome::StatisticalOutlier {
                                z_score,
                                node: result.node_id,
                            }
                        }
                    } else {
                        // Non-numeric result — treat as hash comparison
                        VerificationOutcome::StatisticallyValid { p_value: 1.0 }
                    }
                } else {
                    VerificationOutcome::Conflict {
                        hashes: Vec::new(),
                    }
                }
            }
        }
    }

    /// Update trust profiles based on verification outcome.
    /// Physically slash a node's reputation across all local stores (Local Slashing).
    /// Drops trust to 0.0 and blacklists the node from future work.
    pub fn slash_node(&self, node_id: &NodeId) {
        tracing::error!("🪓 LOCAL SLASHING: Permanently dropping trust for node {} due to cryptographic mismatch!", node_id);
        
        // 1. Reset trust in the verification store
        self.store.reset_trust(node_id);
        
        // 2. Reset reputation in the global reputation store
        self.reputation.slash_score(&(*node_id).into());
        
        // 3. Blacklist in knowledge store (set status to Dead)
        self.knowledge.update_node_status(node_id, crate::swarm::types::NodeStatus::Dead);
        
        self.total_trust_resets.fetch_add(1, Ordering::Relaxed);
    }

    fn update_trust_from_outcome(&self, set: &ReplicaSet, outcome: &VerificationOutcome) {
        match outcome {
            VerificationOutcome::Verified { .. } => {
                for r in &set.results {
                    self.store.update_trust(&r.node_id, true);
                }
            }
            VerificationOutcome::MajorityConsensus { outlier_node, .. } => {
                for r in &set.results {
                    if r.node_id == *outlier_node {
                        self.slash_node(&r.node_id);
                    } else {
                        self.store.update_trust(&r.node_id, true);
                    }
                }
            }
            VerificationOutcome::Conflict { .. } => {
                self.total_conflicts.fetch_add(1, Ordering::Relaxed);
                // Don't penalise anyone — we can't tell who's wrong
            }
            VerificationOutcome::SpotCheckPassed => {
                for r in &set.results {
                    self.store.update_trust(&r.node_id, true);
                }
            }
            VerificationOutcome::SpotCheckFailed { node, .. } => {
                self.store.update_trust(node, false);
                // If trust was previously high, reset it entirely
                let profile = self.store.get_trust_profile(node);
                if profile.spot_check_eligible {
                    warn!(node = %node, "Spot-check failure from trusted node — resetting trust");
                    self.store.reset_trust(node);
                    self.total_trust_resets.fetch_add(1, Ordering::Relaxed);
                }
            }
            VerificationOutcome::StatisticallyValid { .. } => {
                for r in &set.results {
                    self.store.update_trust(&r.node_id, true);
                }
            }
            VerificationOutcome::StatisticalOutlier { node, .. } => {
                self.store.update_trust(node, false);
            }
            VerificationOutcome::Skipped => {}
        }
    }

    // ----------------------------------------------------------------
    // Timeout handling
    // ----------------------------------------------------------------

    /// Process expired replica sets. Returns chunk IDs that need re-queuing.
    pub fn process_expired(&self) -> Vec<(ChunkId, JobId)> {
        let expired = self.store.get_expired_sets();
        let mut requeue = Vec::new();

        for set in expired {
            warn!(
                chunk_id = %set.chunk_id,
                job_id = %set.job_id,
                received = set.results.len(),
                required = set.required_replicas,
                "Replica set expired — requeuing"
            );

            // Penalise nodes that didn't respond
            let responded: HashSet<NodeId> = set.results.iter().map(|r| r.node_id).collect();
            for node in &set.assigned_nodes {
                if !responded.contains(node) {
                    self.store.update_trust(node, false);
                }
            }

            requeue.push((set.chunk_id, set.job_id));
            self.store.remove_active_set(&set.chunk_id);
        }

        requeue
    }

    // ----------------------------------------------------------------
    // Statistical helpers
    // ----------------------------------------------------------------

    /// Add a numerical sample for statistical validation.
    pub fn add_statistical_sample(
        &self,
        job_id: JobId,
        chunk_id: ChunkId,
        node_id: NodeId,
        value: f64,
    ) {
        let sample = StatisticalSample {
            chunk_id,
            node_id,
            value,
            received_at: Utc::now(),
        };
        self.store.add_statistical_sample(job_id, sample);
    }

    /// Validate the overall distribution for a job.
    pub fn validate_job_distribution(
        &self,
        job_id: &JobId,
        tolerance: f64,
    ) -> (f64, Vec<(ChunkId, f64)>) {
        let samples = self.store.get_statistical_samples(job_id);
        if samples.is_empty() {
            return (0.0, Vec::new());
        }

        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
        let outliers = StatisticalValidator::validate_batch(&values, tolerance);
        let cv = StatisticalValidator::coefficient_of_variation(&values);

        let flagged: Vec<(ChunkId, f64)> = outliers
            .iter()
            .filter_map(|(idx, z)| samples.get(*idx).map(|s| (s.chunk_id, *z)))
            .collect();

        (cv, flagged)
    }

    // ----------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------

    /// Extract the consensus hash from a verification outcome.
    fn extract_consensus_hash(&self, outcome: &VerificationOutcome) -> Option<String> {
        match outcome {
            VerificationOutcome::Verified { consensus_hash } => Some(consensus_hash.clone()),
            VerificationOutcome::MajorityConsensus { consensus_hash, .. } => {
                Some(consensus_hash.clone())
            }
            _ => None,
        }
    }

    /// Approximate z-score to p-value (two-tailed, using error function approx).
    fn z_to_p(z: f64) -> f64 {
        // Abramowitz and Stegun approximation
        let t = 1.0 / (1.0 + 0.2316419 * z.abs());
        let d = 0.3989422804014327; // 1/sqrt(2*pi)
        let p = d * (-z * z / 2.0).exp()
            * (0.319381530 * t
                - 0.356563782 * t.powi(2)
                + 1.781477937 * t.powi(3)
                - 1.821255978 * t.powi(4)
                + 1.330274429 * t.powi(5));
        // Two-tailed
        (2.0 * p).min(1.0).max(0.0)
    }

    /// Get engine metrics.
    pub fn metrics(&self) -> VerificationEngineMetrics {
        VerificationEngineMetrics {
            total_initiated: self.total_initiated.load(Ordering::Relaxed),
            total_completed: self.total_completed.load(Ordering::Relaxed),
            total_conflicts: self.total_conflicts.load(Ordering::Relaxed),
            total_trust_resets: self.total_trust_resets.load(Ordering::Relaxed),
            active_sets: self.store.active_count() as u64,
            tracked_nodes: self.store.tracked_nodes() as u64,
        }
    }

    /// Get the summary from the store.
    pub fn summary(&self) -> VerificationSummary {
        self.store.summary()
    }
}

// ============================================================================
// VerificationEngineMetrics
// ============================================================================

/// Operational metrics for the verification engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationEngineMetrics {
    pub total_initiated: u64,
    pub total_completed: u64,
    pub total_conflicts: u64,
    pub total_trust_resets: u64,
    pub active_sets: u64,
    pub tracked_nodes: u64,
}

// ============================================================================
// SpotCheckDecider
// ============================================================================

/// Decides whether to perform a spot check on a trusted node's result.
pub struct SpotCheckDecider;

impl SpotCheckDecider {
    /// Decide whether this chunk should be spot-checked.
    /// Uses a deterministic hash-based approach so the decision is reproducible.
    pub fn should_check(chunk_id: &ChunkId, check_rate: f64) -> bool {
        if check_rate >= 1.0 {
            return true;
        }
        if check_rate <= 0.0 {
            return false;
        }

        // Use chunk ID hash for deterministic decision
        let mut hasher = Sha256::new();
        hasher.update(chunk_id.to_string().as_bytes());
        hasher.update(b"spot_check_salt");
        let hash = hasher.finalize();
        let value = u32::from_be_bytes([hash[0], hash[1], hash[2], hash[3]]) as f64
            / u32::MAX as f64;
        value < check_rate
    }

    /// Compute how many spot checks to expect for a given number of chunks.
    pub fn expected_checks(total_chunks: u64, check_rate: f64) -> u64 {
        (total_chunks as f64 * check_rate).ceil() as u64
    }
}

// ============================================================================
// VerificationCostEstimator
// ============================================================================

/// Estimates the compute overhead of different verification strategies.
pub struct VerificationCostEstimator;

impl VerificationCostEstimator {
    /// Compute the overhead multiplier for a strategy.
    pub fn overhead_multiplier(strategy: &VerificationStrategy) -> f64 {
        match strategy {
            VerificationStrategy::Redundant { replicas } => *replicas as f64,
            VerificationStrategy::SpotCheck { check_rate } => 1.0 + check_rate,
            VerificationStrategy::Statistical { .. } => 1.05,
            VerificationStrategy::None => 1.0,
        }
    }

    /// Estimate total compute units needed given strategy and base chunks.
    pub fn estimate_total_compute(
        base_chunks: u64,
        strategy: &VerificationStrategy,
        retry_rate: f64,
    ) -> f64 {
        let multiplier = Self::overhead_multiplier(strategy);
        let retry_overhead = 1.0 + retry_rate;
        base_chunks as f64 * multiplier * retry_overhead
    }

    /// Estimate effective throughput as a fraction of raw compute.
    pub fn effective_throughput(strategy: &VerificationStrategy) -> f64 {
        1.0 / Self::overhead_multiplier(strategy)
    }

    /// Human-readable label for a strategy's overhead.
    pub fn overhead_label(strategy: &VerificationStrategy) -> &'static str {
        match strategy {
            VerificationStrategy::Redundant { replicas } => match replicas {
                2 => "+100% (2x redundant)",
                3 => "+200% (3x redundant)",
                _ => "+Nx redundant",
            },
            VerificationStrategy::SpotCheck { .. } => "+5-10% (spot check)",
            VerificationStrategy::Statistical { .. } => "+3-5% (statistical)",
            VerificationStrategy::None => "0% (no verification)",
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn test_node_id() -> NodeId {
        NodeId(Uuid::new_v4())
    }

    fn test_chunk_id() -> ChunkId {
        ChunkId(Uuid::new_v4())
    }

    fn test_job_id() -> JobId {
        JobId(Uuid::new_v4())
    }

    // ---- ReplicaResult ----

    #[test]
    fn test_replica_result_from_payload() {
        let node = test_node_id();
        let payload = b"hello world";
        let result = ReplicaResult::from_payload(node, payload, 100);
        assert_eq!(result.node_id, node);
        assert!(!result.result_hash.is_empty());
        assert_eq!(result.result_size_bytes, 11);
        assert_eq!(result.compute_duration_ms, 100);
        assert!(result.raw_payload.is_none());
    }

    #[test]
    fn test_replica_result_with_hash() {
        let node = test_node_id();
        let result = ReplicaResult::with_hash(node, "abc123".into(), 500, 200);
        assert_eq!(result.result_hash, "abc123");
        assert_eq!(result.result_size_bytes, 500);
    }

    #[test]
    fn test_replica_result_deterministic_hash() {
        let node = test_node_id();
        let payload = b"deterministic test";
        let r1 = ReplicaResult::from_payload(node, payload, 0);
        let r2 = ReplicaResult::from_payload(node, payload, 0);
        assert_eq!(r1.result_hash, r2.result_hash);
    }

    // ---- ReplicaSet ----

    #[test]
    fn test_replica_set_redundant() {
        let chunk = test_chunk_id();
        let job = test_job_id();
        let nodes = vec![test_node_id(), test_node_id()];
        let set = ReplicaSet::new_redundant(chunk, job, 2, nodes.clone());
        assert_eq!(set.required_replicas, 2);
        assert_eq!(set.assigned_nodes.len(), 2);
        assert!(!set.is_complete());
        assert_eq!(set.pending_count(), 2);
    }

    #[test]
    fn test_replica_set_add_result() {
        let chunk = test_chunk_id();
        let job = test_job_id();
        let n1 = test_node_id();
        let n2 = test_node_id();
        let mut set = ReplicaSet::new_redundant(chunk, job, 2, vec![n1, n2]);

        let r1 = ReplicaResult::with_hash(n1, "hash1".into(), 100, 50);
        assert!(!set.add_result(r1));
        assert_eq!(set.pending_count(), 1);

        let r2 = ReplicaResult::with_hash(n2, "hash1".into(), 100, 60);
        assert!(set.add_result(r2));
        assert!(set.is_complete());
    }

    #[test]
    fn test_replica_set_spot_check() {
        let chunk = test_chunk_id();
        let job = test_job_id();
        let node = test_node_id();
        let set = ReplicaSet::new_spot_check(chunk, job, node, 0.05);
        assert_eq!(set.required_replicas, 1);
    }

    #[test]
    fn test_replica_set_statistical() {
        let chunk = test_chunk_id();
        let job = test_job_id();
        let node = test_node_id();
        let set = ReplicaSet::new_statistical(chunk, job, node, 3.0);
        assert_eq!(set.required_replicas, 1);
        assert!(matches!(set.strategy, VerificationStrategy::Statistical { .. }));
    }

    #[test]
    fn test_replica_set_unverified() {
        let chunk = test_chunk_id();
        let job = test_job_id();
        let node = test_node_id();
        let set = ReplicaSet::new_unverified(chunk, job, node);
        assert!(matches!(set.strategy, VerificationStrategy::None));
    }

    // ---- TrustProfile ----

    #[test]
    fn test_trust_profile_new() {
        let node = test_node_id();
        let profile = TrustProfile::new(node);
        assert_eq!(profile.trust_score, 0.0);
        assert!(!profile.spot_check_eligible);
        assert_eq!(profile.total_jobs(), 0);
    }

    #[test]
    fn test_trust_profile_builds_trust() {
        let node = test_node_id();
        let mut profile = TrustProfile::new(node);

        for _ in 0..150 {
            profile.record_outcome(true);
        }

        assert!(profile.trust_score >= 0.95);
        assert!(profile.spot_check_eligible);
        assert_eq!(profile.total_verified, 150);
    }

    #[test]
    fn test_trust_profile_loses_trust() {
        let node = test_node_id();
        let mut profile = TrustProfile::new(node);

        for _ in 0..100 {
            profile.record_outcome(true);
        }
        assert!(profile.spot_check_eligible);

        // Record 50 failures — should drop trust
        for _ in 0..50 {
            profile.record_outcome(false);
        }
        assert!(profile.trust_score < 0.95);
        assert!(!profile.spot_check_eligible);
    }

    #[test]
    fn test_trust_profile_reset() {
        let node = test_node_id();
        let mut profile = TrustProfile::new(node);
        for _ in 0..100 {
            profile.record_outcome(true);
        }
        assert!(profile.trust_score > 0.0);

        profile.reset_trust();
        assert_eq!(profile.trust_score, 0.0);
        assert!(!profile.spot_check_eligible);
        assert!(profile.recent_outcomes.is_empty());
    }

    // ---- ResultComparator ----

    #[test]
    fn test_comparator_all_agree() {
        let n1 = test_node_id();
        let n2 = test_node_id();
        let n3 = test_node_id();
        let results = vec![
            ReplicaResult::with_hash(n1, "aaa".into(), 100, 50),
            ReplicaResult::with_hash(n2, "aaa".into(), 100, 60),
            ReplicaResult::with_hash(n3, "aaa".into(), 100, 55),
        ];
        let outcome = ResultComparator::compare_redundant(&results);
        assert!(matches!(outcome, VerificationOutcome::Verified { .. }));
    }

    #[test]
    fn test_comparator_majority_consensus() {
        let n1 = test_node_id();
        let n2 = test_node_id();
        let n3 = test_node_id();
        let results = vec![
            ReplicaResult::with_hash(n1, "aaa".into(), 100, 50),
            ReplicaResult::with_hash(n2, "aaa".into(), 100, 60),
            ReplicaResult::with_hash(n3, "bbb".into(), 100, 55),
        ];
        let outcome = ResultComparator::compare_redundant(&results);
        match outcome {
            VerificationOutcome::MajorityConsensus {
                consensus_hash,
                outlier_node,
                replicas_agreed,
            } => {
                assert_eq!(consensus_hash, "aaa");
                assert_eq!(outlier_node, n3);
                assert_eq!(replicas_agreed, 2);
            }
            _ => panic!("Expected MajorityConsensus"),
        }
    }

    #[test]
    fn test_comparator_full_conflict() {
        let n1 = test_node_id();
        let n2 = test_node_id();
        let results = vec![
            ReplicaResult::with_hash(n1, "aaa".into(), 100, 50),
            ReplicaResult::with_hash(n2, "bbb".into(), 100, 60),
        ];
        let outcome = ResultComparator::compare_redundant(&results);
        assert!(matches!(outcome, VerificationOutcome::Conflict { .. }));
    }

    #[test]
    fn test_comparator_spot_check_pass() {
        let n1 = test_node_id();
        let n2 = test_node_id();
        let trusted = ReplicaResult::with_hash(n1, "aaa".into(), 100, 50);
        let checker = ReplicaResult::with_hash(n2, "aaa".into(), 100, 60);
        let outcome = ResultComparator::compare_spot_check(&trusted, &checker);
        assert!(matches!(outcome, VerificationOutcome::SpotCheckPassed));
    }

    #[test]
    fn test_comparator_spot_check_fail() {
        let n1 = test_node_id();
        let n2 = test_node_id();
        let trusted = ReplicaResult::with_hash(n1, "aaa".into(), 100, 50);
        let checker = ReplicaResult::with_hash(n2, "bbb".into(), 100, 60);
        let outcome = ResultComparator::compare_spot_check(&trusted, &checker);
        assert!(matches!(outcome, VerificationOutcome::SpotCheckFailed { .. }));
    }

    // ---- StatisticalValidator ----

    #[test]
    fn test_statistical_mean() {
        let samples = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((StatisticalValidator::mean(&samples) - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_statistical_std_dev() {
        let samples = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let mean = StatisticalValidator::mean(&samples);
        let sd = StatisticalValidator::std_dev(&samples, mean);
        assert!((sd - 2.13809).abs() < 0.0001);
    }

    #[test]
    fn test_statistical_median() {
        let mut samples = vec![3.0, 1.0, 4.0, 1.0, 5.0];
        let median = StatisticalValidator::median(&mut samples);
        assert!((median - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_statistical_validate_inlier() {
        let samples: Vec<f64> = (0..100).map(|i| 50.0 + (i as f64 * 0.1)).collect();
        let (z, valid) = StatisticalValidator::validate_sample(&samples, 55.0, 3.0);
        assert!(valid);
    }

    #[test]
    fn test_statistical_validate_outlier() {
        let samples: Vec<f64> = (0..100).map(|i| 50.0 + (i as f64 * 0.01)).collect();
        let (z, valid) = StatisticalValidator::validate_sample(&samples, 99999.0, 3.0);
        assert!(!valid);
    }

    #[test]
    fn test_statistical_batch_outliers() {
        let mut samples: Vec<f64> = (0..100).map(|_| 50.0).collect();
        samples.push(99999.0); // extreme outlier
        let outliers = StatisticalValidator::validate_batch(&samples, 3.0);
        assert!(!outliers.is_empty());
    }

    #[test]
    fn test_statistical_coefficient_of_variation() {
        let samples = vec![10.0, 10.0, 10.0, 10.0];
        let cv = StatisticalValidator::coefficient_of_variation(&samples);
        assert!(cv < f64::EPSILON);
    }

    #[test]
    fn test_statistical_iqr() {
        let mut samples = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let iqr = StatisticalValidator::iqr(&mut samples);
        assert!(iqr > 0.0);
    }

    // ---- VerificationStore ----

    #[test]
    fn test_store_register_and_get() {
        let store = VerificationStore::new();
        let chunk = test_chunk_id();
        let job = test_job_id();
        let set = ReplicaSet::new_redundant(chunk, job, 2, vec![test_node_id(), test_node_id()]);
        store.register_set(set);
        assert!(store.get_active_set(&chunk).is_some());
        assert_eq!(store.active_count(), 1);
    }

    #[test]
    fn test_store_trust_profiles() {
        let store = VerificationStore::new();
        let node = test_node_id();

        let profile = store.get_trust_profile(&node);
        assert_eq!(profile.trust_score, 0.0);

        for _ in 0..100 {
            store.update_trust(&node, true);
        }

        let profile = store.get_trust_profile(&node);
        assert!(profile.trust_score >= 0.95);
        assert!(store.is_spot_check_eligible(&node));
    }

    #[test]
    fn test_store_reset_trust() {
        let store = VerificationStore::new();
        let node = test_node_id();
        for _ in 0..100 {
            store.update_trust(&node, true);
        }
        assert!(store.is_spot_check_eligible(&node));

        store.reset_trust(&node);
        assert!(!store.is_spot_check_eligible(&node));
    }

    #[test]
    fn test_store_hash_cache() {
        let store = VerificationStore::new();
        let chunk = test_chunk_id();
        store.cache_hash("abc123", chunk);
        let found = store.lookup_hash("abc123");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0], chunk);
    }

    #[test]
    fn test_store_summary() {
        let store = VerificationStore::new();
        let summary = store.summary();
        assert_eq!(summary.active_sets, 0);
        assert_eq!(summary.total_records, 0);
    }

    // ---- VerificationEngine ----

    #[test]
    fn test_engine_initiate_redundant() {
        let store = Arc::new(VerificationStore::new());
        let engine = VerificationEngine::from_store(store, Arc::new(super::knowledge::KnowledgeStore::new(NodeId::new())), Arc::new(super::reputation::ReputationStore::new()));
        let chunk = test_chunk_id();
        let job = test_job_id();
        let nodes = vec![test_node_id(), test_node_id()];
        let strategy = VerificationStrategy::Redundant { replicas: 2 };

        let set = engine.initiate(chunk, job, &strategy, nodes);
        assert_eq!(set.required_replicas, 2);
    }

    #[test]
    fn test_engine_submit_and_resolve_verified() {
        let store = Arc::new(VerificationStore::new());
        let engine = VerificationEngine::from_store(store, Arc::new(super::knowledge::KnowledgeStore::new(NodeId::new())), Arc::new(super::reputation::ReputationStore::new()));
        let chunk = test_chunk_id();
        let job = test_job_id();
        let n1 = test_node_id();
        let n2 = test_node_id();
        let strategy = VerificationStrategy::Redundant { replicas: 2 };

        engine.initiate(chunk, job, &strategy, vec![n1, n2]);

        let r1 = ReplicaResult::with_hash(n1, "hash_ok".into(), 100, 50);
        assert!(engine.submit_result(&chunk, r1).is_none()); // needs 2nd

        let r2 = ReplicaResult::with_hash(n2, "hash_ok".into(), 100, 60);
        let outcome = engine.submit_result(&chunk, r2);
        assert!(outcome.is_some());
        assert!(matches!(outcome.unwrap(), VerificationOutcome::Verified { .. }));
    }

    #[test]
    fn test_engine_submit_conflict() {
        let store = Arc::new(VerificationStore::new());
        let engine = VerificationEngine::from_store(store, Arc::new(super::knowledge::KnowledgeStore::new(NodeId::new())), Arc::new(super::reputation::ReputationStore::new()));
        let chunk = test_chunk_id();
        let job = test_job_id();
        let n1 = test_node_id();
        let n2 = test_node_id();
        let strategy = VerificationStrategy::Redundant { replicas: 2 };

        engine.initiate(chunk, job, &strategy, vec![n1, n2]);

        engine.submit_result(&chunk, ReplicaResult::with_hash(n1, "aaa".into(), 100, 50));
        let outcome = engine.submit_result(&chunk, ReplicaResult::with_hash(n2, "bbb".into(), 100, 60));
        assert!(matches!(outcome.unwrap(), VerificationOutcome::Conflict { .. }));
    }

    #[test]
    fn test_engine_select_strategy_trusted() {
        let store = Arc::new(VerificationStore::new());
        let engine = VerificationEngine::from_store(store.clone(), Arc::new(super::knowledge::KnowledgeStore::new(NodeId::new())), Arc::new(super::reputation::ReputationStore::new()));
        let node = test_node_id();

        // Build trust
        for _ in 0..150 {
            store.update_trust(&node, true);
        }

        let strategy = VerificationStrategy::Redundant { replicas: 2 };
        let selected = engine.select_strategy(&strategy, &node);
        assert!(matches!(selected, VerificationStrategy::SpotCheck { .. }));
    }

    #[test]
    fn test_engine_select_strategy_untrusted() {
        let store = Arc::new(VerificationStore::new());
        let engine = VerificationEngine::from_store(store, Arc::new(super::knowledge::KnowledgeStore::new(NodeId::new())), Arc::new(super::reputation::ReputationStore::new()));
        let node = test_node_id();

        let strategy = VerificationStrategy::SpotCheck { check_rate: 0.05 };
        let selected = engine.select_strategy(&strategy, &node);
        // Untrusted node requesting spot-check → falls back to redundant
        assert!(matches!(selected, VerificationStrategy::Redundant { .. }));
    }

    #[test]
    fn test_engine_metrics() {
        let store = Arc::new(VerificationStore::new());
        let engine = VerificationEngine::from_store(store, Arc::new(super::knowledge::KnowledgeStore::new(NodeId::new())), Arc::new(super::reputation::ReputationStore::new()));
        let metrics = engine.metrics();
        assert_eq!(metrics.total_initiated, 0);
        assert_eq!(metrics.total_completed, 0);
    }

    // ---- SpotCheckDecider ----

    #[test]
    fn test_spot_check_deterministic() {
        let chunk = test_chunk_id();
        let result1 = SpotCheckDecider::should_check(&chunk, 0.5);
        let result2 = SpotCheckDecider::should_check(&chunk, 0.5);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_spot_check_rate_zero() {
        let chunk = test_chunk_id();
        assert!(!SpotCheckDecider::should_check(&chunk, 0.0));
    }

    #[test]
    fn test_spot_check_rate_one() {
        let chunk = test_chunk_id();
        assert!(SpotCheckDecider::should_check(&chunk, 1.0));
    }

    #[test]
    fn test_expected_checks() {
        assert_eq!(SpotCheckDecider::expected_checks(100, 0.05), 5);
        assert_eq!(SpotCheckDecider::expected_checks(1000, 0.1), 100);
    }

    // ---- VerificationCostEstimator ----

    #[test]
    fn test_cost_overhead_redundant() {
        let strategy = VerificationStrategy::Redundant { replicas: 2 };
        assert!((VerificationCostEstimator::overhead_multiplier(&strategy) - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cost_overhead_spot_check() {
        let strategy = VerificationStrategy::SpotCheck { check_rate: 0.05 };
        assert!((VerificationCostEstimator::overhead_multiplier(&strategy) - 1.05).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cost_overhead_statistical() {
        let strategy = VerificationStrategy::Statistical { outlier_tolerance: 3.0 };
        assert!((VerificationCostEstimator::overhead_multiplier(&strategy) - 1.05).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cost_overhead_none() {
        let strategy = VerificationStrategy::None;
        assert!((VerificationCostEstimator::overhead_multiplier(&strategy) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_effective_throughput() {
        let strategy = VerificationStrategy::Redundant { replicas: 3 };
        let throughput = VerificationCostEstimator::effective_throughput(&strategy);
        assert!((throughput - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn test_estimate_total_compute() {
        let strategy = VerificationStrategy::Redundant { replicas: 2 };
        let total = VerificationCostEstimator::estimate_total_compute(100, &strategy, 0.2);
        // 100 chunks * 2x redundant * 1.2 retry = 240
        assert!((total - 240.0).abs() < f64::EPSILON);
    }
}
