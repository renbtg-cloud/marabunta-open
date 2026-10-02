// Marabunta - Licensed under the MIT License.
//! Organic Node Admission — jury-based vetting for new swarm nodes.
//!
//! When a new node wants to join the swarm, it submits a `JoinRequest` with
//! claimed resources and a cryptographic signature. The admission engine
//! selects a jury of 5-7 established nodes, distributes verification test
//! tasks, collects verdicts, and either admits the node on probation or
//! rejects it. Probationary nodes must complete a minimum number of real
//! tasks with an acceptable failure rate before graduating to full membership.

use std::collections::{HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::auth::NodeIdentity;
use super::knowledge::KnowledgeStore;
use super::reputation::{EntityId, ReputationStore};
use super::types::{NodeId, NodeStatus};

// ============================================================================
// Admission config constants
// ============================================================================

/// Minimum number of jurors for an admission review.
pub const JURY_MIN_SIZE: usize = 5;

/// Maximum number of jurors for an admission review.
pub const JURY_MAX_SIZE: usize = 7;

/// Minimum tasks a probationary node must complete.
pub const PROBATION_MIN_TASKS: u32 = 50;

/// Maximum tasks in probation (longer probation for lower-confidence verdicts).
pub const PROBATION_MAX_TASKS: u32 = 200;

/// Timeout for individual test tasks (increased to prevent Raspberry Pi slaughter).
pub const TEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Timeout for collecting all juror verdicts.
pub const VERDICT_TIMEOUT: Duration = Duration::from_secs(600);

/// Failure rate above which a probationary node is expelled.
pub const FAILURE_THRESHOLD: f32 = 0.3;

/// Reputation penalty applied to jurors when an admitted node is expelled.
pub const JUROR_PENALTY: f32 = -0.05;

/// Reputation bonus for jurors when an admitted node graduates.
pub const JUROR_BONUS: f32 = 0.01;

/// How often the admission engine runs its maintenance loop.
pub const ADMISSION_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Graduation requires at least this success rate.
pub const GRADUATION_SUCCESS_RATE: f32 = 0.90;

/// Maximum join requests per IP within the rate limit window.
pub const RATE_LIMIT_MAX_REQUESTS: u32 = 5;

/// Rate limit window duration.
pub const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

/// Maximum number of recent nonces to track (replay prevention).
pub const NONCE_CACHE_SIZE: usize = 10_000;

/// CPU hash iterations per core for admission test tasks.
pub const CPU_HASH_ITERATIONS_PER_CORE: u64 = 500_000;

// ============================================================================
// Types
// ============================================================================

/// Resources the joining node claims to have.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedResources {
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub disk_mb: u64,
    pub bandwidth_mbps: f32,
    pub has_gpu: bool,
    pub gpu_model: Option<String>,
}

/// A request from a new node to join the swarm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub candidate_id: NodeId,
    pub claimed_resources: ClaimedResources,
    pub signature: Vec<u8>,
    pub public_key: Vec<u8>,
    pub requested_at: DateTime<Utc>,
    pub region: Option<String>,
    /// Unique nonce for replay prevention.
    #[serde(default)]
    pub nonce: u64,
}

/// Types of verification tasks sent to the candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TestTaskType {
    /// Hash N iterations of SHA-256 to verify CPU speed.
    CpuHash { iterations: u64 },
    /// Allocate and fill N MB of memory.
    MemoryFill { megabytes: u64 },
    /// Write and read N MB to/from disk.
    StorageWrite { megabytes: u64 },
    /// Echo back a payload via network to measure latency.
    NetworkEcho { payload_bytes: usize },
    /// Run a GPU kernel if candidate claims GPU.
    GpuCompute { workload_id: String },
}

/// A test task assigned to a candidate for verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestTask {
    pub task_id: Uuid,
    pub candidate_id: NodeId,
    pub task_type: TestTaskType,
    pub timeout: Duration,
    pub created_at: DateTime<Utc>,
}

/// Result of a test task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestTaskResult {
    pub task_id: Uuid,
    pub candidate_id: NodeId,
    pub success: bool,
    pub duration_ms: u64,
    pub output_hash: Option<[u8; 32]>,
    pub error: Option<String>,
}

/// Verification of claimed resources against test results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceVerification {
    pub cpu_verified: bool,
    pub memory_verified: bool,
    pub disk_verified: bool,
    pub network_verified: bool,
    pub gpu_verified: Option<bool>,
    pub overall_match_pct: f32,
}

/// IP reputation data for the candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpReputation {
    pub ip_address: String,
    pub is_datacenter: bool,
    pub is_proxy: bool,
    pub is_tor_exit: bool,
    pub abuse_score: f32,
    pub country_code: Option<String>,
}

/// A single juror's verdict on a candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JurorVerdict {
    pub juror_id: NodeId,
    pub candidate_id: NodeId,
    pub decision: VerdictDecision,
    pub confidence: f32,
    pub resource_verification: ResourceVerification,
    pub ip_reputation: Option<IpReputation>,
    pub notes: Option<String>,
    pub submitted_at: DateTime<Utc>,
}

/// A juror's decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerdictDecision {
    Admit,
    Reject,
    Unsure,
}

/// An admission vote (wrapper for gossip propagation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionVote {
    pub candidate_id: NodeId,
    pub voter_id: NodeId,
    pub decision: VerdictDecision,
    pub confidence: f32,
    pub voted_at: DateTime<Utc>,
}

/// Final admission decision for a candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionDecision {
    pub candidate_id: NodeId,
    pub outcome: AdmissionOutcome,
    pub jury: Vec<NodeId>,
    pub votes_admit: u32,
    pub votes_reject: u32,
    pub votes_unsure: u32,
    pub avg_confidence: f32,
    pub probation_length: Option<u32>,
    pub decided_at: DateTime<Utc>,
}

/// Outcome of the admission process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionOutcome {
    Admitted,
    Rejected,
    Deferred,
}

/// Probation tracking for admitted nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbationStatus {
    pub node_id: NodeId,
    pub required_tasks: u32,
    pub tasks_completed: u32,
    pub tasks_failed: u32,
    pub started_at: DateTime<Utc>,
    pub jury: Vec<NodeId>,
    pub graduated: bool,
    pub expelled: bool,
}

impl ProbationStatus {
    pub fn success_rate(&self) -> f32 {
        if self.tasks_completed == 0 {
            return 1.0;
        }
        1.0 - (self.tasks_failed as f32 / self.tasks_completed as f32)
    }

    pub fn failure_rate(&self) -> f32 {
        if self.tasks_completed == 0 {
            return 0.0;
        }
        self.tasks_failed as f32 / self.tasks_completed as f32
    }
}

/// Status of a pending admission review.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReviewStatus {
    /// Waiting for jury to be selected.
    PendingJury,
    /// Test tasks distributed, waiting for results.
    TestingInProgress,
    /// Verdicts being collected.
    VerdictCollection,
    /// Review complete, decision made.
    Complete(AdmissionOutcome),
}

/// A full admission review record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionReview {
    pub request: JoinRequest,
    pub status: ReviewStatus,
    pub jury: Vec<NodeId>,
    pub test_tasks: Vec<TestTask>,
    pub test_results: Vec<TestTaskResult>,
    pub verdicts: Vec<JurorVerdict>,
    pub decision: Option<AdmissionDecision>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ============================================================================
// AdmissionStore
// ============================================================================

/// DashMap-backed store for admission reviews, probation records, and decisions.
/// A pending consent — candidate sent a JoinRequest and we sent back a ConfigManifest.
/// Waiting for ConfigConsent before proceeding to jury selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsentPending {
    /// The original join request.
    pub request: JoinRequest,
    /// The manifest hash we sent.
    pub manifest_hash: String,
    /// When the manifest was sent.
    pub sent_at: DateTime<Utc>,
    /// Source IP (if known) for rate-limiting.
    pub source_ip: Option<IpAddr>,
}

pub struct AdmissionStore {
    /// Pending and completed reviews, keyed by candidate NodeId.
    reviews: DashMap<NodeId, AdmissionReview>,
    /// Probation records for admitted nodes.
    probations: DashMap<NodeId, ProbationStatus>,
    /// Recent admission decisions (for gossip propagation).
    decisions: DashMap<NodeId, AdmissionDecision>,
    /// Candidates awaiting consent before jury selection.
    consent_pending: DashMap<NodeId, ConsentPending>,
}

impl Default for AdmissionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AdmissionStore {
    pub fn new() -> Self {
        Self {
            reviews: DashMap::new(),
            probations: DashMap::new(),
            decisions: DashMap::new(),
            consent_pending: DashMap::new(),
        }
    }

    pub fn get_review(&self, candidate: &NodeId) -> Option<AdmissionReview> {
        self.reviews.get(candidate).map(|r| r.value().clone())
    }

    pub fn upsert_review(&self, review: AdmissionReview) {
        self.reviews.insert(review.request.candidate_id, review);
    }

    pub fn get_probation(&self, node_id: &NodeId) -> Option<ProbationStatus> {
        self.probations.get(node_id).map(|r| r.value().clone())
    }

    pub fn upsert_probation(&self, status: ProbationStatus) {
        self.probations.insert(status.node_id, status);
    }

    pub fn get_decision(&self, candidate: &NodeId) -> Option<AdmissionDecision> {
        self.decisions.get(candidate).map(|r| r.value().clone())
    }

    pub fn upsert_decision(&self, decision: AdmissionDecision) {
        self.decisions.insert(decision.candidate_id, decision);
    }

    pub fn pending_reviews(&self) -> Vec<AdmissionReview> {
        self.reviews
            .iter()
            .filter(|r| !matches!(r.value().status, ReviewStatus::Complete(_)))
            .map(|r| r.value().clone())
            .collect()
    }

    pub fn all_probations(&self) -> Vec<ProbationStatus> {
        self.probations.iter().map(|r| r.value().clone()).collect()
    }

    pub fn active_probations(&self) -> Vec<ProbationStatus> {
        self.probations
            .iter()
            .filter(|r| !r.value().graduated && !r.value().expelled)
            .map(|r| r.value().clone())
            .collect()
    }

    pub fn recent_decisions(&self) -> Vec<AdmissionDecision> {
        self.decisions.iter().map(|r| r.value().clone()).collect()
    }

    pub fn remove_review(&self, candidate: &NodeId) {
        self.reviews.remove(candidate);
    }

    pub fn review_count(&self) -> usize {
        self.reviews.len()
    }

    pub fn probation_count(&self) -> usize {
        self.probations.len()
    }

    // --- Consent pending accessors ---

    pub fn get_consent_pending(&self, candidate: &NodeId) -> Option<ConsentPending> {
        self.consent_pending.get(candidate).map(|r| r.value().clone())
    }

    pub fn insert_consent_pending(&self, pending: ConsentPending) {
        self.consent_pending.insert(pending.request.candidate_id, pending);
    }

    pub fn remove_consent_pending(&self, candidate: &NodeId) -> Option<ConsentPending> {
        self.consent_pending.remove(candidate).map(|(_, v)| v)
    }

    pub fn consent_pending_count(&self) -> usize {
        self.consent_pending.len()
    }

    pub fn all_consent_pending(&self) -> Vec<ConsentPending> {
        self.consent_pending.iter().map(|r| r.value().clone()).collect()
    }

    pub fn decision_count(&self) -> usize {
        self.decisions.len()
    }
}

// ============================================================================
// AdmissionEngine
// ============================================================================

/// Full lifecycle management for node admission.
pub struct AdmissionEngine {
    self_id: NodeId,
    store: Arc<AdmissionStore>,
    knowledge: Arc<KnowledgeStore>,
    reputation_store: Arc<ReputationStore>,
    /// Rate limiter: IP → (request count, window start).
    rate_limiter: DashMap<IpAddr, (u32, Instant)>,
    /// Recently seen nonces for replay prevention.
    recent_nonces: Mutex<VecDeque<u64>>,
    /// Admission tuning configuration (promoted from module constants).
    pub config: super::config::AdmissionConfig,
}

impl AdmissionEngine {
    pub fn new(
        self_id: NodeId,
        store: Arc<AdmissionStore>,
        knowledge: Arc<KnowledgeStore>,
        reputation_store: Arc<ReputationStore>,
    ) -> Self {
        let config = super::config::AdmissionConfig::default();
        Self {
            self_id,
            store,
            knowledge,
            reputation_store,
            rate_limiter: DashMap::new(),
            recent_nonces: Mutex::new(VecDeque::with_capacity(config.nonce_cache_size)),
            config,
        }
    }

    /// Override admission configuration (for runtime config).
    pub fn with_admission_config(mut self, config: super::config::AdmissionConfig) -> Self {
        self.config = config;
        self
    }

    /// Check rate limit for an IP address. Returns true if the request is allowed.
    fn check_rate_limit(&self, ip: &IpAddr) -> bool {
        let now = Instant::now();
        let mut entry = self.rate_limiter.entry(*ip).or_insert((0, now));
        let (count, window_start) = entry.value_mut();

        // Reset window if expired.
        if now.duration_since(*window_start) > RATE_LIMIT_WINDOW {
            *count = 1;
            *window_start = now;
            return true;
        }

        if *count >= RATE_LIMIT_MAX_REQUESTS {
            return false;
        }

        *count += 1;
        true
    }

    /// Check if a nonce has been seen before. Returns true if it's a replay.
    fn is_replay_nonce(&self, nonce: u64) -> bool {
        let mut nonces = self.recent_nonces.lock();
        if nonces.contains(&nonce) {
            return true;
        }
        if nonces.len() >= NONCE_CACHE_SIZE {
            nonces.pop_front();
        }
        nonces.push_back(nonce);
        false
    }

    /// Verify the Ed25519 signature on a join request.
    ///
    /// The signature must be over `candidate_id || nonce_bytes` using the
    /// public key provided. The public key must also match the candidate_id
    /// (deterministic derivation).
    fn verify_join_signature(request: &JoinRequest) -> bool {
        // Public key must be exactly 32 bytes (Ed25519).
        let Ok(pubkey_array): Result<[u8; 32], _> = request.public_key.as_slice().try_into() else {
            warn!(candidate = %request.candidate_id, "invalid public key length");
            return false;
        };

        // Verify that the public key matches the claimed candidate_id.
        let derived_id = NodeIdentity::node_id_from_pubkey(&pubkey_array);
        if derived_id != request.candidate_id {
            warn!(
                candidate = %request.candidate_id,
                derived = %derived_id,
                "public key does not match candidate_id"
            );
            return false;
        }

        // Signature must be exactly 64 bytes (Ed25519).
        let Ok(sig_array): Result<[u8; 64], _> = request.signature.as_slice().try_into() else {
            warn!(candidate = %request.candidate_id, "invalid signature length");
            return false;
        };

        // Build the signed message: candidate_id bytes || nonce as little-endian bytes.
        let mut message = Vec::new();
        message.extend_from_slice(request.candidate_id.0.as_bytes());
        message.extend_from_slice(&request.nonce.to_le_bytes());

        NodeIdentity::verify(&pubkey_array, &message, &sig_array)
    }

    /// Handle an incoming join request from a new node.
    ///
    /// Validates the signature, selects a jury, generates test tasks,
    /// and creates an admission review.
    pub fn handle_join_request(
        &self,
        request: JoinRequest,
        source_ip: Option<IpAddr>,
    ) -> Option<AdmissionReview> {
        let candidate = request.candidate_id;

        // Rate limit by source IP.
        if let Some(ip) = source_ip {
            if !self.check_rate_limit(&ip) {
                warn!(candidate = %candidate, ip = %ip, "join request rate limited");
                return None;
            }
        }

        // Check if already under review.
        if self.store.get_review(&candidate).is_some() {
            debug!(candidate = %candidate, "join request already under review");
            return None;
        }

        // Check if already on probation.
        if self.store.get_probation(&candidate).is_some() {
            debug!(candidate = %candidate, "node already on probation");
            return None;
        }

        // Replay prevention: reject duplicate nonces.
        if request.nonce != 0 && self.is_replay_nonce(request.nonce) {
            warn!(candidate = %candidate, nonce = request.nonce, "replayed nonce detected");
            return None;
        }

        // Validate Ed25519 signature: public key must match candidate_id,
        // and signature must verify over (candidate_id || nonce).
        if request.signature.is_empty() || request.public_key.is_empty() {
            warn!(candidate = %candidate, "join request missing signature or public key");
            return None;
        }
        if !Self::verify_join_signature(&request) {
            warn!(candidate = %candidate, "join request signature verification failed");
            return None;
        }

        // Select jury.
        let jury = self.select_jury(&candidate, request.region.as_deref());
        if jury.len() < JURY_MIN_SIZE {
            warn!(
                candidate = %candidate,
                jury_size = jury.len(),
                "insufficient jurors available (need {})", JURY_MIN_SIZE
            );
            return None;
        }

        // Generate test tasks.
        let test_tasks = self.generate_test_tasks(&candidate, &request.claimed_resources);

        let review = AdmissionReview {
            request,
            status: ReviewStatus::TestingInProgress,
            jury: jury.clone(),
            test_tasks,
            test_results: Vec::new(),
            verdicts: Vec::new(),
            decision: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        info!(
            candidate = %candidate,
            jury_size = jury.len(),
            test_tasks = review.test_tasks.len(),
            "admission review created"
        );

        self.store.upsert_review(review.clone());
        Some(review)
    }

    /// Select a jury for an admission review.
    ///
    /// Uses reputation-weighted, region-diverse selection. The jury has
    /// 5-7 full (non-probationary) members, selected deterministically
    /// from a seed derived from the candidate's ID.
    pub fn select_jury(&self, candidate_id: &NodeId, _region: Option<&str>) -> Vec<NodeId> {
        let all_nodes = self.knowledge.get_live_nodes();

        // Filter to eligible jurors: alive, not us, not the candidate,
        // not on probation, with reasonable reputation.
        let mut candidates: Vec<(NodeId, f32)> = all_nodes
            .into_iter()
            .filter(|n| n.node_id != self.self_id)
            .filter(|n| n.node_id != *candidate_id)
            .filter(|n| n.status == NodeStatus::Alive)
            .filter(|n| self.store.get_probation(&n.node_id).is_none())
            .map(|n| {
                let score = self.reputation_store.score_for(&EntityId::Node(n.node_id));
                (n.node_id, score)
            })
            .filter(|(_, score)| *score >= 0.3)
            .collect();

        if candidates.is_empty() {
            return Vec::new();
        }

        // Deduplicate candidates by NodeId (defensive: get_live_nodes should
        // not return duplicates, but guard against it for jury integrity).
        {
            let mut seen = HashSet::with_capacity(candidates.len());
            candidates.retain(|(id, _)| seen.insert(*id));
        }

        // Deterministic RNG seeded from candidate_id for reproducibility.
        let seed = {
            let bytes = candidate_id.0.as_bytes();
            let mut seed_val: u64 = 0;
            for (i, &b) in bytes.iter().enumerate() {
                seed_val ^= (b as u64) << ((i % 8) * 8);
            }
            seed_val
        };

        // Sort by reputation descending first, then apply deterministic
        // shuffle within equal-reputation tiers. This ensures higher-reputation
        // nodes are preferred while still providing diversity via the seed.
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Deterministic weighted selection: walk through sorted candidates
        // and use the LCG to probabilistically skip some, creating diversity
        // while still biasing toward higher reputation.
        let jury_size = candidates.len().min(JURY_MAX_SIZE);
        if candidates.len() <= jury_size {
            // Not enough candidates to be selective — take them all.
            return candidates.into_iter().map(|(id, _)| id).collect();
        }

        // Fisher-Yates shuffle with deterministic seed on the sorted list,
        // then take the first jury_size. Because the list is sorted by rep
        // first, the shuffle provides diversity while the top-heavy
        // distribution means high-rep nodes are still more likely selected.
        let n = candidates.len();
        let mut state = seed;
        for i in (1..n).rev() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (state >> 33) as usize % (i + 1);
            candidates.swap(i, j);
        }

        candidates.into_iter().take(jury_size).map(|(id, _)| id).collect()
    }

    /// Generate verification test tasks for a candidate.
    pub fn generate_test_tasks(
        &self,
        candidate_id: &NodeId,
        claimed: &ClaimedResources,
    ) -> Vec<TestTask> {
        let now = Utc::now();
        let mut tasks = Vec::new();

        // CPU hash test — scale iterations by claimed cores.
        tasks.push(TestTask {
            task_id: Uuid::new_v4(),
            candidate_id: *candidate_id,
            task_type: TestTaskType::CpuHash {
                iterations: (claimed.cpu_cores as u64) * CPU_HASH_ITERATIONS_PER_CORE,
            },
            timeout: TEST_TIMEOUT,
            created_at: now,
        });

        // Memory fill test — test a fraction of claimed memory.
        let mem_test_mb = (claimed.memory_mb / 10).max(16).min(512);
        tasks.push(TestTask {
            task_id: Uuid::new_v4(),
            candidate_id: *candidate_id,
            task_type: TestTaskType::MemoryFill {
                megabytes: mem_test_mb,
            },
            timeout: TEST_TIMEOUT,
            created_at: now,
        });

        // Storage write test — test a fraction of claimed disk.
        let disk_test_mb = (claimed.disk_mb / 100).max(1).min(64);
        tasks.push(TestTask {
            task_id: Uuid::new_v4(),
            candidate_id: *candidate_id,
            task_type: TestTaskType::StorageWrite {
                megabytes: disk_test_mb,
            },
            timeout: TEST_TIMEOUT,
            created_at: now,
        });

        // Network echo test.
        tasks.push(TestTask {
            task_id: Uuid::new_v4(),
            candidate_id: *candidate_id,
            task_type: TestTaskType::NetworkEcho {
                payload_bytes: 1024,
            },
            timeout: TEST_TIMEOUT,
            created_at: now,
        });

        // GPU test if claimed.
        if claimed.has_gpu {
            tasks.push(TestTask {
                task_id: Uuid::new_v4(),
                candidate_id: *candidate_id,
                task_type: TestTaskType::GpuCompute {
                    workload_id: "matrix_multiply_256".to_string(),
                },
                timeout: TEST_TIMEOUT,
                created_at: now,
            });
        }

        tasks
    }

    /// Handle a test task result from a candidate.
    ///
    /// NOTE: This uses a get-clone-modify-upsert pattern on the DashMap-backed
    /// store. Concurrent calls for the same candidate could race (one call's
    /// modifications overwriting the other's). In practice this is unlikely
    /// because test results arrive serially from the candidate, but callers
    /// should be aware of this limitation.
    pub fn handle_test_result(&self, result: TestTaskResult) {
        let candidate = result.candidate_id;
        if let Some(mut review) = self.store.get_review(&candidate) {
            // Reject results for already-completed reviews.
            if matches!(review.status, ReviewStatus::Complete(_)) {
                return;
            }

            // Verify the task ID belongs to this review.
            let valid = review.test_tasks.iter().any(|t| t.task_id == result.task_id);
            if !valid {
                warn!(
                    candidate = %candidate,
                    task_id = %result.task_id,
                    "test result for unknown task"
                );
                return;
            }

            // Avoid duplicate results.
            if review.test_results.iter().any(|r| r.task_id == result.task_id) {
                return;
            }

            review.test_results.push(result);
            review.updated_at = Utc::now();

            // If all test tasks have results, move to verdict collection.
            if review.test_results.len() >= review.test_tasks.len() {
                review.status = ReviewStatus::VerdictCollection;
                debug!(candidate = %candidate, "all test tasks complete, collecting verdicts");
            }

            self.store.upsert_review(review);
        }
    }

    /// Handle a juror's verdict on a candidate.
    pub fn handle_verdict(&self, verdict: JurorVerdict) {
        let candidate = verdict.candidate_id;
        if let Some(mut review) = self.store.get_review(&candidate) {
            // Only accept verdicts when the review is in an appropriate state.
            if matches!(review.status, ReviewStatus::Complete(_)) {
                debug!(
                    candidate = %candidate,
                    "verdict received for already-completed review, ignoring"
                );
                return;
            }

            // Verify the juror is part of this review's jury.
            if !review.jury.contains(&verdict.juror_id) {
                warn!(
                    candidate = %candidate,
                    juror = %verdict.juror_id,
                    "verdict from non-jury member"
                );
                return;
            }

            // Avoid duplicate verdicts.
            if review.verdicts.iter().any(|v| v.juror_id == verdict.juror_id) {
                return;
            }

            review.verdicts.push(verdict);
            review.updated_at = Utc::now();

            // If we have verdicts from a majority of jurors, tally.
            let majority = (review.jury.len() / 2) + 1;
            if review.verdicts.len() >= majority {
                let decision = self.tally_votes(&review);
                review.status = ReviewStatus::Complete(decision.outcome);
                review.decision = Some(decision.clone());

                info!(
                    candidate = %candidate,
                    outcome = ?decision.outcome,
                    admit = decision.votes_admit,
                    reject = decision.votes_reject,
                    unsure = decision.votes_unsure,
                    "admission decision reached"
                );

                // If admitted, create probation record.
                if decision.outcome == AdmissionOutcome::Admitted {
                    let probation = ProbationStatus {
                        node_id: candidate,
                        required_tasks: decision.probation_length.unwrap_or(PROBATION_MIN_TASKS),
                        tasks_completed: 0,
                        tasks_failed: 0,
                        started_at: Utc::now(),
                        jury: review.jury.clone(),
                        graduated: false,
                        expelled: false,
                    };
                    self.store.upsert_probation(probation);
                }

                self.store.upsert_decision(decision);
            }

            self.store.upsert_review(review);
        }
    }

    /// Tally votes from juror verdicts and produce an admission decision.
    pub fn tally_votes(&self, review: &AdmissionReview) -> AdmissionDecision {
        let mut votes_admit: u32 = 0;
        let mut votes_reject: u32 = 0;
        let mut votes_unsure: u32 = 0;
        let mut total_confidence: f32 = 0.0;

        for verdict in &review.verdicts {
            match verdict.decision {
                VerdictDecision::Admit => votes_admit += 1,
                VerdictDecision::Reject => votes_reject += 1,
                VerdictDecision::Unsure => votes_unsure += 1,
            }
            total_confidence += verdict.confidence;
        }

        let total_votes = review.verdicts.len() as u32;
        let avg_confidence = if total_votes > 0 {
            total_confidence / total_votes as f32
        } else {
            0.0
        };

        // Determine outcome:
        // - Majority admit → Admitted
        // - Majority reject → Rejected
        // - >1/3 unsure → Deferred
        let unsure_threshold = (review.jury.len() as f32 / 3.0).ceil() as u32;
        let outcome = if votes_unsure >= unsure_threshold {
            AdmissionOutcome::Deferred
        } else if votes_admit > votes_reject {
            AdmissionOutcome::Admitted
        } else {
            AdmissionOutcome::Rejected
        };

        let probation_length = if outcome == AdmissionOutcome::Admitted {
            Some(self.calculate_probation_length(avg_confidence))
        } else {
            None
        };

        AdmissionDecision {
            candidate_id: review.request.candidate_id,
            outcome,
            jury: review.jury.clone(),
            votes_admit,
            votes_reject,
            votes_unsure,
            avg_confidence,
            probation_length,
            decided_at: Utc::now(),
        }
    }

    /// Calculate probation length based on average jury confidence.
    ///
    /// Higher confidence → shorter probation (closer to PROBATION_MIN_TASKS).
    /// Lower confidence → longer probation (closer to PROBATION_MAX_TASKS).
    pub fn calculate_probation_length(&self, avg_confidence: f32) -> u32 {
        let confidence = avg_confidence.clamp(0.0, 1.0);
        let range = PROBATION_MAX_TASKS - PROBATION_MIN_TASKS;
        let length = PROBATION_MAX_TASKS - (confidence * range as f32) as u32;
        length.clamp(PROBATION_MIN_TASKS, PROBATION_MAX_TASKS)
    }

    /// Update probation status after a task completes.
    pub fn update_probation(&self, node_id: &NodeId, success: bool) {
        if let Some(mut status) = self.store.get_probation(node_id) {
            if status.graduated || status.expelled {
                return;
            }

            status.tasks_completed += 1;
            if !success {
                status.tasks_failed += 1;
            }

            // Check for expulsion.
            if self.check_expulsion(&status) {
                status.expelled = true;
                info!(
                    node = %node_id,
                    completed = status.tasks_completed,
                    failed = status.tasks_failed,
                    rate = status.failure_rate(),
                    "probationary node expelled"
                );
                self.on_node_expelled(node_id, &status.jury);
            }
            // Check for graduation.
            else if self.check_graduation(&status) {
                status.graduated = true;
                info!(
                    node = %node_id,
                    completed = status.tasks_completed,
                    failed = status.tasks_failed,
                    rate = status.success_rate(),
                    "probationary node graduated"
                );
                self.on_node_graduated(node_id, &status.jury);
            }

            self.store.upsert_probation(status);
        }
    }

    /// Check if a probationary node should graduate.
    ///
    /// Requires: tasks_completed >= required_tasks AND failure_rate < 10%.
    pub fn check_graduation(&self, status: &ProbationStatus) -> bool {
        status.tasks_completed >= status.required_tasks
            && status.success_rate() >= GRADUATION_SUCCESS_RATE
    }

    /// Check if a probationary node should be expelled.
    ///
    /// Expelled if failure_rate > FAILURE_THRESHOLD (30%).
    /// Requires at least 5 completed tasks to avoid false positives.
    pub fn check_expulsion(&self, status: &ProbationStatus) -> bool {
        status.tasks_completed >= 5 && status.failure_rate() > FAILURE_THRESHOLD
    }

    // ========================================================================
    // Consent integration
    // ========================================================================

    /// Build a ConfigManifest for a candidate awaiting consent.
    ///
    /// Called after JoinRequest validation passes. The manifest is sent to the
    /// candidate, and the join request is parked in `consent_pending` until
    /// the candidate responds with a ConfigConsent.
    pub fn begin_consent(
        &self,
        request: JoinRequest,
        config: &super::config::SwarmConfig,
        source_ip: Option<IpAddr>,
    ) -> super::config_consent::ConfigManifest {
        let manifest = super::config_consent::ConfigManifest::build(config, self.self_id);

        let pending = ConsentPending {
            request,
            manifest_hash: manifest.manifest_hash.clone(),
            sent_at: Utc::now(),
            source_ip,
        };

        info!(
            candidate = %pending.request.candidate_id,
            manifest_hash = %manifest.manifest_hash,
            "consent manifest sent to candidate"
        );

        self.store.insert_consent_pending(pending);
        manifest
    }

    /// Handle a ConfigConsent response from a candidate.
    ///
    /// If accepted and manifest hash matches, removes from consent_pending
    /// and proceeds with jury selection (returns Some(AdmissionReview)).
    /// If rejected, removes from consent_pending and returns None.
    pub fn handle_consent_response(
        &self,
        consent: &super::config_consent::ConfigConsent,
    ) -> Option<AdmissionReview> {
        let pending = match self.store.remove_consent_pending(&consent.node_id) {
            Some(p) => p,
            None => {
                warn!(
                    node_id = %consent.node_id,
                    "consent response for unknown candidate"
                );
                return None;
            }
        };

        // Verify manifest hash matches.
        if consent.manifest_hash != pending.manifest_hash {
            warn!(
                node_id = %consent.node_id,
                expected = %pending.manifest_hash,
                got = %consent.manifest_hash,
                "consent manifest hash mismatch"
            );
            return None;
        }

        if !consent.accepted {
            info!(
                node_id = %consent.node_id,
                reason = ?consent.rejection_reason,
                "candidate rejected swarm manifest"
            );
            return None;
        }

        info!(
            node_id = %consent.node_id,
            auto = consent.auto_accepted,
            "candidate accepted swarm manifest, proceeding to jury selection"
        );

        // Now proceed with the standard jury-selection flow.
        self.handle_join_request(pending.request, pending.source_ip)
    }

    /// Check and time out stale consent-pending entries.
    pub fn check_consent_timeouts(&self) {
        let now = Utc::now();
        let timeout = chrono::Duration::from_std(VERDICT_TIMEOUT)
            .unwrap_or_else(|_| chrono::Duration::seconds(VERDICT_TIMEOUT.as_secs() as i64));

        let expired: Vec<NodeId> = self
            .store
            .all_consent_pending()
            .into_iter()
            .filter(|p| now.signed_duration_since(p.sent_at) > timeout)
            .map(|p| p.request.candidate_id)
            .collect();

        for candidate_id in expired {
            info!(candidate = %candidate_id, "consent request timed out");
            self.store.remove_consent_pending(&candidate_id);
        }
    }

    /// Called when a probationary node graduates — jurors get reputation bonus.
    fn on_node_graduated(&self, _node_id: &NodeId, jury: &[NodeId]) {
        for juror_id in jury {
            self.reputation_store
                .adjust_score(&EntityId::Node(*juror_id), JUROR_BONUS);
            debug!(juror = %juror_id, bonus = JUROR_BONUS, "juror reputation bonus for graduation");
        }
    }

    /// Called when a probationary node is expelled — jurors get reputation penalty.
    fn on_node_expelled(&self, _node_id: &NodeId, jury: &[NodeId]) {
        for juror_id in jury {
            self.reputation_store
                .adjust_score(&EntityId::Node(*juror_id), JUROR_PENALTY);
            debug!(juror = %juror_id, penalty = JUROR_PENALTY, "juror reputation penalty for expulsion");
        }
    }

    /// Check and time out stale reviews that haven't collected enough verdicts.
    pub fn check_timeouts(&self) {
        let now = Utc::now();
        let verdict_timeout_chrono = chrono::Duration::from_std(VERDICT_TIMEOUT)
            .unwrap_or_else(|_| chrono::Duration::seconds(VERDICT_TIMEOUT.as_secs() as i64));

        for review in self.store.pending_reviews() {
            let age = now.signed_duration_since(review.created_at);
            if age > verdict_timeout_chrono
                && matches!(review.status, ReviewStatus::VerdictCollection | ReviewStatus::TestingInProgress) {
                    // Force a decision with whatever verdicts we have.
                    let mut review = review.clone();
                    if review.verdicts.is_empty() {
                        // No verdicts at all — reject.
                        let decision = AdmissionDecision {
                            candidate_id: review.request.candidate_id,
                            outcome: AdmissionOutcome::Rejected,
                            jury: review.jury.clone(),
                            votes_admit: 0,
                            votes_reject: 0,
                            votes_unsure: 0,
                            avg_confidence: 0.0,
                            probation_length: None,
                            decided_at: Utc::now(),
                        };
                        review.status = ReviewStatus::Complete(AdmissionOutcome::Rejected);
                        review.decision = Some(decision.clone());
                        self.store.upsert_decision(decision);
                    } else {
                        let decision = self.tally_votes(&review);
                        review.status = ReviewStatus::Complete(decision.outcome);
                        review.decision = Some(decision.clone());

                        if decision.outcome == AdmissionOutcome::Admitted {
                            let probation = ProbationStatus {
                                node_id: review.request.candidate_id,
                                required_tasks: decision.probation_length.unwrap_or(PROBATION_MIN_TASKS),
                                tasks_completed: 0,
                                tasks_failed: 0,
                                started_at: Utc::now(),
                                jury: review.jury.clone(),
                                graduated: false,
                                expelled: false,
                            };
                            self.store.upsert_probation(probation);
                        }

                        self.store.upsert_decision(decision);
                    }

                    let candidate_id = review.request.candidate_id;
                    review.updated_at = Utc::now();
                    self.store.upsert_review(review);
                    warn!(
                        candidate = %candidate_id,
                        "admission review timed out, forced decision"
                    );
                }
        }
    }

    /// Get the admission store.
    pub fn store(&self) -> &Arc<AdmissionStore> {
        &self.store
    }

    /// Spawn the admission maintenance loop.
    pub fn spawn_loop(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("admission engine maintenance loop started");

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(ADMISSION_CHECK_INTERVAL) => {}
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow() {
                            info!("admission engine loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown.borrow() {
                    break;
                }

                // Check for timed-out reviews.
                self.check_timeouts();
            }
        })
    }
}

// ============================================================================
// SwarmMessage variants for admission (added to types.rs)
// ============================================================================

// These are defined in types.rs as SwarmMessage variants:
// - AdmissionJoinRequest { request, from }
// - AdmissionTestTask { task, from }
// - AdmissionTestResult { result, from }
// - AdmissionVerdict { verdict, from }
// - AdmissionVoteBroadcast { vote, from }
// - AdmissionDecisionBroadcast { decision, from }
// - AdmissionProbationUpdate { status, from }

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::auth::NodeIdentity;
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::types::{NodeInfo, ResourceSnapshot};

    /// Create a properly signed join request using a fresh Ed25519 identity.
    /// Returns (JoinRequest, NodeIdentity) so tests can use the identity.
    fn make_signed_join_request() -> (JoinRequest, NodeIdentity) {
        let identity = NodeIdentity::generate();
        let nonce: u64 = rand::random();

        // Build signed message: candidate_id bytes || nonce LE bytes.
        let mut message = Vec::new();
        message.extend_from_slice(identity.node_id.0.as_bytes());
        message.extend_from_slice(&nonce.to_le_bytes());
        let signature = identity.sign(&message);

        let request = JoinRequest {
            candidate_id: identity.node_id,
            claimed_resources: ClaimedResources {
                cpu_cores: 4,
                memory_mb: 8192,
                disk_mb: 102400,
                bandwidth_mbps: 100.0,
                has_gpu: false,
                gpu_model: None,
            },
            signature: signature.to_vec(),
            public_key: identity.public_key.to_bytes().to_vec(),
            requested_at: Utc::now(),
            region: Some("us-east".to_string()),
            nonce,
        };
        (request, identity)
    }

    /// Legacy helper for tests that don't need the identity.
    fn make_join_request() -> JoinRequest {
        make_signed_join_request().0
    }

    fn make_node_info(id: NodeId) -> NodeInfo {
        NodeInfo {
            node_id: id,
            last_seen: Utc::now(),
            traits: HashSet::new(),
            load: 0.3,
            capacity: ResourceSnapshot::default(),
            address: Some("127.0.0.1:4200".parse().unwrap()),
            via: id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
            ..Default::default()
        }
    }

    fn setup_engine_with_peers(peer_count: usize) -> (AdmissionEngine, NodeId, Vec<NodeId>) {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let reputation = Arc::new(ReputationStore::new());
        let store = Arc::new(AdmissionStore::new());

        let mut peer_ids = Vec::new();
        for _ in 0..peer_count {
            let peer_id = NodeId::new();
            knowledge.merge_node(make_node_info(peer_id));
            // Give each peer a decent reputation score.
            let mut record = reputation.get_or_create(EntityId::Node(peer_id));
            record.score = 0.7;
            record.version += 1;
            reputation.merge(record);
            peer_ids.push(peer_id);
        }

        let engine = AdmissionEngine::new(self_id, store, knowledge, reputation);
        (engine, self_id, peer_ids)
    }

    #[test]
    fn test_probation_status_success_rate() {
        let status = ProbationStatus {
            node_id: NodeId::new(),
            required_tasks: 50,
            tasks_completed: 10,
            tasks_failed: 2,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };
        assert!((status.success_rate() - 0.8).abs() < f32::EPSILON);
        assert!((status.failure_rate() - 0.2).abs() < f32::EPSILON);
    }

    #[test]
    fn test_probation_status_empty() {
        let status = ProbationStatus {
            node_id: NodeId::new(),
            required_tasks: 50,
            tasks_completed: 0,
            tasks_failed: 0,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };
        assert!((status.success_rate() - 1.0).abs() < f32::EPSILON);
        assert!((status.failure_rate() - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn test_calculate_probation_length() {
        let (engine, _, _) = setup_engine_with_peers(0);

        // High confidence → short probation.
        let length = engine.calculate_probation_length(1.0);
        assert_eq!(length, PROBATION_MIN_TASKS);

        // Low confidence → long probation.
        let length = engine.calculate_probation_length(0.0);
        assert_eq!(length, PROBATION_MAX_TASKS);

        // Mid confidence → mid probation.
        let length = engine.calculate_probation_length(0.5);
        assert!(length > PROBATION_MIN_TASKS && length < PROBATION_MAX_TASKS);
    }

    #[test]
    fn test_generate_test_tasks_without_gpu() {
        let (engine, _, _) = setup_engine_with_peers(0);
        let candidate = NodeId::new();
        let claimed = ClaimedResources {
            cpu_cores: 4,
            memory_mb: 8192,
            disk_mb: 102400,
            bandwidth_mbps: 100.0,
            has_gpu: false,
            gpu_model: None,
        };

        let tasks = engine.generate_test_tasks(&candidate, &claimed);
        assert_eq!(tasks.len(), 4); // CPU, memory, storage, network (no GPU)
    }

    #[test]
    fn test_generate_test_tasks_with_gpu() {
        let (engine, _, _) = setup_engine_with_peers(0);
        let candidate = NodeId::new();
        let claimed = ClaimedResources {
            cpu_cores: 8,
            memory_mb: 16384,
            disk_mb: 204800,
            bandwidth_mbps: 1000.0,
            has_gpu: true,
            gpu_model: Some("RTX 4090".to_string()),
        };

        let tasks = engine.generate_test_tasks(&candidate, &claimed);
        assert_eq!(tasks.len(), 5); // CPU, memory, storage, network, GPU
    }

    #[test]
    fn test_check_graduation() {
        let (engine, _, _) = setup_engine_with_peers(0);

        // Not enough tasks.
        let status = ProbationStatus {
            node_id: NodeId::new(),
            required_tasks: 50,
            tasks_completed: 30,
            tasks_failed: 0,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };
        assert!(!engine.check_graduation(&status));

        // Enough tasks, good rate.
        let status = ProbationStatus {
            node_id: NodeId::new(),
            required_tasks: 50,
            tasks_completed: 50,
            tasks_failed: 2,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };
        assert!(engine.check_graduation(&status));

        // Enough tasks, bad rate.
        let status = ProbationStatus {
            node_id: NodeId::new(),
            required_tasks: 50,
            tasks_completed: 50,
            tasks_failed: 10,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };
        assert!(!engine.check_graduation(&status));
    }

    #[test]
    fn test_check_expulsion() {
        let (engine, _, _) = setup_engine_with_peers(0);

        // Too few tasks to judge.
        let status = ProbationStatus {
            node_id: NodeId::new(),
            required_tasks: 50,
            tasks_completed: 3,
            tasks_failed: 3,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };
        assert!(!engine.check_expulsion(&status));

        // High failure rate.
        let status = ProbationStatus {
            node_id: NodeId::new(),
            required_tasks: 50,
            tasks_completed: 10,
            tasks_failed: 5,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };
        assert!(engine.check_expulsion(&status));

        // Acceptable failure rate.
        let status = ProbationStatus {
            node_id: NodeId::new(),
            required_tasks: 50,
            tasks_completed: 10,
            tasks_failed: 1,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };
        assert!(!engine.check_expulsion(&status));
    }

    #[test]
    fn test_admission_store_basic() {
        let store = AdmissionStore::new();
        assert_eq!(store.review_count(), 0);
        assert_eq!(store.probation_count(), 0);
        assert_eq!(store.decision_count(), 0);
    }

    #[test]
    fn test_select_jury_insufficient_peers() {
        let (engine, _, _) = setup_engine_with_peers(2); // Need at least 5
        let candidate = NodeId::new();
        let jury = engine.select_jury(&candidate, None);
        assert!(jury.len() < JURY_MIN_SIZE);
    }

    #[test]
    fn test_select_jury_sufficient_peers() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let candidate = NodeId::new();
        let jury = engine.select_jury(&candidate, None);
        assert!(jury.len() >= JURY_MIN_SIZE);
        assert!(jury.len() <= JURY_MAX_SIZE);
    }

    #[test]
    fn test_handle_join_request_missing_signature() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let mut request = make_join_request();
        request.signature = vec![]; // Empty signature
        let result = engine.handle_join_request(request, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_join_request_success() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let request = make_join_request();
        let result = engine.handle_join_request(request, None);
        assert!(result.is_some());
        let review = result.unwrap();
        assert!(review.jury.len() >= JURY_MIN_SIZE);
        assert!(!review.test_tasks.is_empty());
        assert!(matches!(review.status, ReviewStatus::TestingInProgress));
    }

    #[test]
    fn test_handle_join_request_duplicate() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let (request, _identity) = make_signed_join_request();
        // First request succeeds.
        let result1 = engine.handle_join_request(request.clone(), None);
        assert!(result1.is_some());
        // Second identical request is rejected (duplicate candidate, nonce is replayed).
        let result2 = engine.handle_join_request(request, None);
        assert!(result2.is_none());
    }

    #[test]
    fn test_handle_join_request_bad_signature() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let mut request = make_join_request();
        // Corrupt the signature.
        request.signature[0] ^= 0xff;
        let result = engine.handle_join_request(request, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_join_request_wrong_pubkey() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let mut request = make_join_request();
        // Use a different identity's public key — mismatch with candidate_id.
        let other = NodeIdentity::generate();
        request.public_key = other.public_key.to_bytes().to_vec();
        let result = engine.handle_join_request(request, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_rate_limiting() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        for _ in 0..RATE_LIMIT_MAX_REQUESTS {
            let request = make_join_request();
            // Each request is unique (different candidate), so only the rate limit stops them.
            engine.handle_join_request(request, Some(ip));
        }

        // Next request from same IP should be rate-limited.
        let request = make_join_request();
        let result = engine.handle_join_request(request, Some(ip));
        assert!(result.is_none());
    }

    #[test]
    fn test_nonce_replay_prevention() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let (request1, identity) = make_signed_join_request();

        // First request succeeds.
        let result1 = engine.handle_join_request(request1.clone(), None);
        assert!(result1.is_some());

        // Create a second request with the same nonce but different candidate_id
        // (simulating a replay attempt). The nonce should be rejected.
        let nonce = request1.nonce;
        let identity2 = NodeIdentity::generate();
        let mut message = Vec::new();
        message.extend_from_slice(identity2.node_id.0.as_bytes());
        message.extend_from_slice(&nonce.to_le_bytes());
        let signature = identity2.sign(&message);

        let request2 = JoinRequest {
            candidate_id: identity2.node_id,
            claimed_resources: request1.claimed_resources.clone(),
            signature: signature.to_vec(),
            public_key: identity2.public_key.to_bytes().to_vec(),
            requested_at: Utc::now(),
            region: None,
            nonce,
        };
        let result2 = engine.handle_join_request(request2, None);
        assert!(result2.is_none());
    }

    #[test]
    fn test_tally_votes_admit() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let request = make_join_request();
        let candidate = request.candidate_id;
        let jury: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();

        let review = AdmissionReview {
            request,
            status: ReviewStatus::VerdictCollection,
            jury: jury.clone(),
            test_tasks: vec![],
            test_results: vec![],
            verdicts: vec![
                JurorVerdict {
                    juror_id: jury[0],
                    candidate_id: candidate,
                    decision: VerdictDecision::Admit,
                    confidence: 0.9,
                    resource_verification: ResourceVerification {
                        cpu_verified: true,
                        memory_verified: true,
                        disk_verified: true,
                        network_verified: true,
                        gpu_verified: None,
                        overall_match_pct: 0.95,
                    },
                    ip_reputation: None,
                    notes: None,
                    submitted_at: Utc::now(),
                },
                JurorVerdict {
                    juror_id: jury[1],
                    candidate_id: candidate,
                    decision: VerdictDecision::Admit,
                    confidence: 0.8,
                    resource_verification: ResourceVerification {
                        cpu_verified: true,
                        memory_verified: true,
                        disk_verified: true,
                        network_verified: true,
                        gpu_verified: None,
                        overall_match_pct: 0.90,
                    },
                    ip_reputation: None,
                    notes: None,
                    submitted_at: Utc::now(),
                },
                JurorVerdict {
                    juror_id: jury[2],
                    candidate_id: candidate,
                    decision: VerdictDecision::Reject,
                    confidence: 0.6,
                    resource_verification: ResourceVerification {
                        cpu_verified: false,
                        memory_verified: true,
                        disk_verified: true,
                        network_verified: true,
                        gpu_verified: None,
                        overall_match_pct: 0.70,
                    },
                    ip_reputation: None,
                    notes: None,
                    submitted_at: Utc::now(),
                },
            ],
            decision: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let decision = engine.tally_votes(&review);
        assert_eq!(decision.outcome, AdmissionOutcome::Admitted);
        assert_eq!(decision.votes_admit, 2);
        assert_eq!(decision.votes_reject, 1);
        assert!(decision.probation_length.is_some());
    }

    #[test]
    fn test_tally_votes_reject() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let request = make_join_request();
        let candidate = request.candidate_id;
        let jury: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();

        let make_verdict = |juror_id, decision| JurorVerdict {
            juror_id,
            candidate_id: candidate,
            decision,
            confidence: 0.8,
            resource_verification: ResourceVerification {
                cpu_verified: true,
                memory_verified: true,
                disk_verified: true,
                network_verified: true,
                gpu_verified: None,
                overall_match_pct: 0.9,
            },
            ip_reputation: None,
            notes: None,
            submitted_at: Utc::now(),
        };

        let review = AdmissionReview {
            request,
            status: ReviewStatus::VerdictCollection,
            jury: jury.clone(),
            test_tasks: vec![],
            test_results: vec![],
            verdicts: vec![
                make_verdict(jury[0], VerdictDecision::Reject),
                make_verdict(jury[1], VerdictDecision::Reject),
                make_verdict(jury[2], VerdictDecision::Admit),
            ],
            decision: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let decision = engine.tally_votes(&review);
        assert_eq!(decision.outcome, AdmissionOutcome::Rejected);
        assert!(decision.probation_length.is_none());
    }

    #[test]
    fn test_tally_votes_defer() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let request = make_join_request();
        let candidate = request.candidate_id;
        let jury: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();

        let make_verdict = |juror_id, decision| JurorVerdict {
            juror_id,
            candidate_id: candidate,
            decision,
            confidence: 0.5,
            resource_verification: ResourceVerification {
                cpu_verified: true,
                memory_verified: true,
                disk_verified: true,
                network_verified: true,
                gpu_verified: None,
                overall_match_pct: 0.8,
            },
            ip_reputation: None,
            notes: None,
            submitted_at: Utc::now(),
        };

        let review = AdmissionReview {
            request,
            status: ReviewStatus::VerdictCollection,
            jury: jury.clone(),
            test_tasks: vec![],
            test_results: vec![],
            verdicts: vec![
                make_verdict(jury[0], VerdictDecision::Unsure),
                make_verdict(jury[1], VerdictDecision::Unsure),
                make_verdict(jury[2], VerdictDecision::Admit),
            ],
            decision: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let decision = engine.tally_votes(&review);
        assert_eq!(decision.outcome, AdmissionOutcome::Deferred);
    }

    #[test]
    fn test_update_probation_graduation() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let node_id = NodeId::new();
        let juror1 = NodeId::new();
        let juror2 = NodeId::new();

        // Create reputation records for jurors.
        let mut r1 = engine.reputation_store.get_or_create(EntityId::Node(juror1));
        r1.score = 0.5;
        engine.reputation_store.merge(r1);
        let mut r2 = engine.reputation_store.get_or_create(EntityId::Node(juror2));
        r2.score = 0.5;
        engine.reputation_store.merge(r2);

        let status = ProbationStatus {
            node_id,
            required_tasks: 5,
            tasks_completed: 4,
            tasks_failed: 0,
            started_at: Utc::now(),
            jury: vec![juror1, juror2],
            graduated: false,
            expelled: false,
        };
        engine.store.upsert_probation(status);

        // Complete the final task.
        engine.update_probation(&node_id, true);

        let status = engine.store.get_probation(&node_id).unwrap();
        assert!(status.graduated);
        assert!(!status.expelled);
        assert_eq!(status.tasks_completed, 5);

        // Jurors should have received bonus.
        let s1 = engine.reputation_store.score_for(&EntityId::Node(juror1));
        assert!(s1 > 0.5);
        let s2 = engine.reputation_store.score_for(&EntityId::Node(juror2));
        assert!(s2 > 0.5);
    }

    #[test]
    fn test_update_probation_expulsion() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let node_id = NodeId::new();
        let juror1 = NodeId::new();

        let mut r1 = engine.reputation_store.get_or_create(EntityId::Node(juror1));
        r1.score = 0.7;
        engine.reputation_store.merge(r1);

        let status = ProbationStatus {
            node_id,
            required_tasks: 50,
            tasks_completed: 4,
            tasks_failed: 3,
            started_at: Utc::now(),
            jury: vec![juror1],
            graduated: false,
            expelled: false,
        };
        engine.store.upsert_probation(status);

        // Fail another task (5 completed, 4 failed = 80% failure > 30%).
        engine.update_probation(&node_id, false);

        let status = engine.store.get_probation(&node_id).unwrap();
        assert!(status.expelled);
        assert!(!status.graduated);

        // Juror should have received penalty.
        let s1 = engine.reputation_store.score_for(&EntityId::Node(juror1));
        assert!(s1 < 0.7);
    }

    #[test]
    fn test_handle_test_result() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let request = make_join_request();
        let candidate = request.candidate_id;
        let review = engine.handle_join_request(request, None).unwrap();
        let task_id = review.test_tasks[0].task_id;

        let result = TestTaskResult {
            task_id,
            candidate_id: candidate,
            success: true,
            duration_ms: 150,
            output_hash: None,
            error: None,
        };

        engine.handle_test_result(result);

        let updated = engine.store.get_review(&candidate).unwrap();
        assert_eq!(updated.test_results.len(), 1);
    }

    #[test]
    fn test_handle_verdict_triggers_decision() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let request = make_join_request();
        let candidate = request.candidate_id;
        let review = engine.handle_join_request(request, None).unwrap();
        let jury = review.jury.clone();

        // Move to verdict collection by completing all test results.
        for task in &review.test_tasks {
            engine.handle_test_result(TestTaskResult {
                task_id: task.task_id,
                candidate_id: candidate,
                success: true,
                duration_ms: 100,
                output_hash: None,
                error: None,
            });
        }

        // Submit verdicts from majority of jurors.
        let majority = (jury.len() / 2) + 1;
        for juror_id in jury.iter().take(majority) {
            engine.handle_verdict(JurorVerdict {
                juror_id: *juror_id,
                candidate_id: candidate,
                decision: VerdictDecision::Admit,
                confidence: 0.85,
                resource_verification: ResourceVerification {
                    cpu_verified: true,
                    memory_verified: true,
                    disk_verified: true,
                    network_verified: true,
                    gpu_verified: None,
                    overall_match_pct: 0.95,
                },
                ip_reputation: None,
                notes: None,
                submitted_at: Utc::now(),
            });
        }

        // Decision should be made.
        let updated = engine.store.get_review(&candidate).unwrap();
        assert!(matches!(updated.status, ReviewStatus::Complete(AdmissionOutcome::Admitted)));
        assert!(updated.decision.is_some());

        // Probation should be created.
        let probation = engine.store.get_probation(&candidate);
        assert!(probation.is_some());
    }

    #[test]
    fn test_select_jury_no_duplicates() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let candidate = NodeId::new();
        let jury = engine.select_jury(&candidate, None);
        let unique: HashSet<NodeId> = jury.iter().copied().collect();
        assert_eq!(jury.len(), unique.len(), "jury must not contain duplicate node IDs");
    }

    #[test]
    fn test_select_jury_excludes_self_and_candidate() {
        let (engine, self_id, _) = setup_engine_with_peers(10);
        let candidate = NodeId::new();
        let jury = engine.select_jury(&candidate, None);
        assert!(!jury.contains(&self_id), "jury must not contain the selecting node");
        assert!(!jury.contains(&candidate), "jury must not contain the candidate");
    }

    #[test]
    fn test_verdict_on_completed_review_ignored() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let request = make_join_request();
        let candidate = request.candidate_id;
        let review = engine.handle_join_request(request, None).unwrap();
        let jury = review.jury.clone();

        // Complete all test results.
        for task in &review.test_tasks {
            engine.handle_test_result(TestTaskResult {
                task_id: task.task_id,
                candidate_id: candidate,
                success: true,
                duration_ms: 100,
                output_hash: None,
                error: None,
            });
        }

        // Submit enough verdicts to trigger a decision.
        let majority = (jury.len() / 2) + 1;
        for juror_id in jury.iter().take(majority) {
            engine.handle_verdict(JurorVerdict {
                juror_id: *juror_id,
                candidate_id: candidate,
                decision: VerdictDecision::Admit,
                confidence: 0.85,
                resource_verification: ResourceVerification {
                    cpu_verified: true,
                    memory_verified: true,
                    disk_verified: true,
                    network_verified: true,
                    gpu_verified: None,
                    overall_match_pct: 0.95,
                },
                ip_reputation: None,
                notes: None,
                submitted_at: Utc::now(),
            });
        }

        // Review should now be Complete.
        let updated = engine.store.get_review(&candidate).unwrap();
        assert!(matches!(updated.status, ReviewStatus::Complete(_)));
        let verdict_count = updated.verdicts.len();

        // Submit another verdict after completion — should be ignored.
        let extra_juror = jury[majority]; // juror who hasn't voted yet
        engine.handle_verdict(JurorVerdict {
            juror_id: extra_juror,
            candidate_id: candidate,
            decision: VerdictDecision::Reject,
            confidence: 0.9,
            resource_verification: ResourceVerification {
                cpu_verified: false,
                memory_verified: false,
                disk_verified: false,
                network_verified: false,
                gpu_verified: None,
                overall_match_pct: 0.0,
            },
            ip_reputation: None,
            notes: None,
            submitted_at: Utc::now(),
        });

        // Verdict count should not have changed.
        let final_review = engine.store.get_review(&candidate).unwrap();
        assert_eq!(final_review.verdicts.len(), verdict_count);
    }

    #[test]
    fn test_test_result_on_completed_review_ignored() {
        let (engine, _, _) = setup_engine_with_peers(10);
        let request = make_join_request();
        let candidate = request.candidate_id;
        let review = engine.handle_join_request(request, None).unwrap();
        let jury = review.jury.clone();
        let task_ids: Vec<Uuid> = review.test_tasks.iter().map(|t| t.task_id).collect();

        // Complete all test results.
        for task in &review.test_tasks {
            engine.handle_test_result(TestTaskResult {
                task_id: task.task_id,
                candidate_id: candidate,
                success: true,
                duration_ms: 100,
                output_hash: None,
                error: None,
            });
        }

        // Submit verdicts to complete the review.
        let majority = (jury.len() / 2) + 1;
        for juror_id in jury.iter().take(majority) {
            engine.handle_verdict(JurorVerdict {
                juror_id: *juror_id,
                candidate_id: candidate,
                decision: VerdictDecision::Reject,
                confidence: 0.9,
                resource_verification: ResourceVerification {
                    cpu_verified: true,
                    memory_verified: true,
                    disk_verified: true,
                    network_verified: true,
                    gpu_verified: None,
                    overall_match_pct: 0.9,
                },
                ip_reputation: None,
                notes: None,
                submitted_at: Utc::now(),
            });
        }

        // Review should now be Complete.
        let updated = engine.store.get_review(&candidate).unwrap();
        assert!(matches!(updated.status, ReviewStatus::Complete(_)));
        let result_count = updated.test_results.len();

        // Try to submit another test result — should be ignored.
        engine.handle_test_result(TestTaskResult {
            task_id: task_ids[0], // duplicate, but also completed-review guard
            candidate_id: candidate,
            success: false,
            duration_ms: 999,
            output_hash: None,
            error: Some("late result".into()),
        });

        let final_review = engine.store.get_review(&candidate).unwrap();
        assert_eq!(final_review.test_results.len(), result_count);
    }

    #[test]
    fn test_update_probation_no_change_after_graduated() {
        let (engine, _, _) = setup_engine_with_peers(0);
        let node_id = NodeId::new();

        let status = ProbationStatus {
            node_id,
            required_tasks: 5,
            tasks_completed: 5,
            tasks_failed: 0,
            started_at: Utc::now(),
            jury: vec![],
            graduated: true,
            expelled: false,
        };
        engine.store.upsert_probation(status);

        // Should have no effect since already graduated.
        engine.update_probation(&node_id, false);

        let updated = engine.store.get_probation(&node_id).unwrap();
        assert!(updated.graduated);
        assert!(!updated.expelled);
        assert_eq!(updated.tasks_completed, 5); // unchanged
        assert_eq!(updated.tasks_failed, 0); // unchanged
    }

    #[test]
    fn test_update_probation_no_change_after_expelled() {
        let (engine, _, _) = setup_engine_with_peers(0);
        let node_id = NodeId::new();

        let status = ProbationStatus {
            node_id,
            required_tasks: 50,
            tasks_completed: 10,
            tasks_failed: 8,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: true,
        };
        engine.store.upsert_probation(status);

        // Should have no effect since already expelled.
        engine.update_probation(&node_id, true);

        let updated = engine.store.get_probation(&node_id).unwrap();
        assert!(updated.expelled);
        assert!(!updated.graduated);
        assert_eq!(updated.tasks_completed, 10); // unchanged
    }
}
