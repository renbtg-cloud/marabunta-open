// Marabunta - Licensed under the MIT License.
//! Pillar 12.1: The Immune System (Reputation Engine)
//!
//! Decentralized Elo/PageRank scoring for the organic swarm.
//! Nodes and collectives earn badges through consistent behavior over time.
//! Behavioral quarantining is enforced for Byzantine or Parasitic nodes.
//!
//! Reputation records propagate via gossip. Conflict resolution uses a
//! monotonic version counter (newer wins) with total-jobs as a tiebreaker.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};
use super::collective::CollectiveId;
use super::types::NodeId;

// ============================================================================
// EntityId
// ============================================================================

/// An entity that can earn reputation — either a single node or a collective.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityId {
    Node(NodeId),
    Collective(CollectiveId),
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityId::Node(id) => write!(f, "{}", id),
            EntityId::Collective(id) => write!(f, "{}", id),
        }
    }
}

impl From<NodeId> for EntityId {
    fn from(id: NodeId) -> Self {
        EntityId::Node(id)
    }
}

impl From<CollectiveId> for EntityId {
    fn from(id: CollectiveId) -> Self {
        EntityId::Collective(id)
    }
}

// ============================================================================
// Badge
// ============================================================================

/// Reputation badges that entities earn through consistent behavior.
///
/// Each badge has clear earn/shed criteria. The system is designed so
/// that badges are *lost* when performance degrades — there is no
/// permanent badge grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Badge {
    /// First 50 jobs. Limited to small tasks. Automatically removed
    /// once the entity completes 50 jobs.
    Newcomer,
    /// Consistently completes work under the estimated time. Requires
    /// a recent speed ratio average below the lightning threshold.
    Lightning,
    /// Less than 2% failure rate over 100+ lifetime jobs. Shed if
    /// recent success rate drops below the reliable-shed threshold.
    Reliable,
    /// 200+ jobs with zero policy violations and geo-verification.
    /// Immediately shed on any policy violation.
    Trusted,
    /// Handles large multi-chunk jobs (>1 GB). Requires 20+ large
    /// jobs completed with a 95%+ success rate.
    HeavyLifter,
    /// Caught engaging in verifiable cryptographic mismatch or data poisoning.
    Malicious,
    /// 6+ months active, 500+ jobs, and 3+ other qualifying badges.
    /// Shed if the entity loses enough other badges.
    Veteran,
}

impl Badge {
    /// All badge variants, for iteration.
    pub const ALL: &'static [Badge] = &[
        Badge::Newcomer,
        Badge::Lightning,
        Badge::Reliable,
        Badge::Trusted,
        Badge::HeavyLifter,
        Badge::Malicious,
        Badge::Veteran,
    ];

    /// Human-readable description of what this badge represents.
    pub fn description(&self) -> &'static str {
        match self {
            Badge::Newcomer => "New to the swarm, limited to small tasks (first 50 jobs)",
            Badge::Lightning => "Consistently completes work under estimated time",
            Badge::Reliable => "Less than 2% failure rate over 100+ jobs",
            Badge::Trusted => "200+ jobs with zero policy violations",
            Badge::HeavyLifter => "Handles large multi-chunk jobs (>1 GB) reliably",
            Badge::Malicious => "Caught engaging in verifiable cryptographic mismatch or data poisoning",
            Badge::Veteran => "6+ months active, 500+ jobs, 3+ other badges earned",
        }
    }

    /// The minimum number of lifetime jobs required before this badge
    /// can be earned.
    pub fn min_jobs(&self) -> u64 {
        match self {
            Badge::Newcomer => 0,
            Badge::Lightning => 50,
            Badge::Reliable => 100,
            Badge::Trusted => 200,
            Badge::HeavyLifter => 0, // gated by large_jobs_completed, not total_jobs
            Badge::Malicious => 0,
            Badge::Veteran => 500,
        }
    }
}

impl fmt::Display for Badge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Badge::Newcomer => write!(f, "newcomer"),
            Badge::Lightning => write!(f, "lightning"),
            Badge::Reliable => write!(f, "reliable"),
            Badge::Trusted => write!(f, "trusted"),
            Badge::HeavyLifter => write!(f, "heavy_lifter"),
            Badge::Malicious => write!(f, "malicious"),
            Badge::Veteran => write!(f, "veteran"),
        }
    }
}

// ============================================================================
// RollingWindow
// ============================================================================

/// Fixed-capacity ring buffer for tracking recent metric values.
///
/// Used for sliding-window calculations like recent success rate and
/// speed ratio. When full, the oldest value is evicted on push.
///
/// Convention:
/// - For success tracking: push `1.0` for success, `0.0` for failure
/// - For speed tracking: push `actual_time / estimated_time` ratio
#[derive(Debug, Clone)]
pub struct RollingWindow {
    values: VecDeque<f32>,
    capacity: usize,
}

impl RollingWindow {
    /// Create a new rolling window with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "RollingWindow capacity must be > 0");
        Self {
            values: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Push a value into the window, evicting the oldest if full.
    pub fn push(&mut self, value: f32) {
        if self.values.len() >= self.capacity {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    /// Arithmetic mean of all values in the window. Returns `0.0` if empty.
    pub fn average(&self) -> f32 {
        if self.values.is_empty() {
            return 0.0;
        }
        let sum: f32 = self.values.iter().sum();
        sum / self.values.len() as f32
    }

    /// Number of values currently in the window.
    pub fn count(&self) -> usize {
        self.values.len()
    }

    /// Whether the window has reached its capacity.
    pub fn is_full(&self) -> bool {
        self.values.len() >= self.capacity
    }

    /// Fraction of values that are >= 1.0 (i.e., successes).
    ///
    /// Returns `1.0` if empty (optimistic default for new entities).
    pub fn success_rate(&self) -> f32 {
        if self.values.is_empty() {
            return 1.0;
        }
        let successes = self.values.iter().filter(|&&v| v >= 1.0).count();
        successes as f32 / self.values.len() as f32
    }
}

impl Serialize for RollingWindow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as a struct with capacity and values
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("RollingWindow", 2)?;
        let values_vec: Vec<f32> = self.values.iter().copied().collect();
        state.serialize_field("capacity", &self.capacity)?;
        state.serialize_field("values", &values_vec)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RollingWindow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RollingWindowHelper {
            capacity: usize,
            values: Vec<f32>,
        }

        let helper = RollingWindowHelper::deserialize(deserializer)?;
        if helper.capacity == 0 {
            return Err(serde::de::Error::custom(
                "RollingWindow capacity must be > 0",
            ));
        }
        let mut window = RollingWindow::new(helper.capacity);
        for v in helper.values {
            window.push(v);
        }
        Ok(window)
    }
}

// ============================================================================
// ReputationConfig
// ============================================================================

/// Tunable thresholds for badge earn/shed decisions.
///
/// All defaults match the specification. Override via config file or
/// programmatically for testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReputationConfig {
    /// Number of recent jobs to track in rolling windows.
    pub rolling_window_size: usize,
    /// Jobs threshold below which Newcomer badge is active.
    pub newcomer_threshold: u64,
    /// Speed ratio must be below this to earn Lightning.
    pub lightning_speed_earn: f32,
    /// Speed ratio above this causes Lightning to be shed.
    pub lightning_speed_shed: f32,
    /// Minimum lifetime jobs to earn Reliable.
    pub reliable_min_jobs: u64,
    /// Maximum lifetime failure rate to earn Reliable.
    pub reliable_max_failure_rate: f32,
    /// Recent success rate below this causes Reliable to be shed.
    pub reliable_shed_rate: f32,
    /// Minimum lifetime jobs to earn Trusted.
    pub trusted_min_jobs: u64,
    /// Minimum large jobs completed to earn HeavyLifter.
    pub heavy_lifter_min_large: u64,
    /// Minimum large-job success rate to earn HeavyLifter.
    pub heavy_lifter_success_rate: f32,
    /// Minimum months active to earn Veteran.
    pub veteran_min_months: f32,
    /// Minimum lifetime jobs to earn Veteran.
    pub veteran_min_jobs: u64,
    /// Minimum number of qualifying badges (excluding Newcomer and Veteran)
    /// required to earn Veteran.
    pub veteran_min_badges: usize,
}

impl Default for ReputationConfig {
    fn default() -> Self {
        Self {
            rolling_window_size: 50,
            newcomer_threshold: 50,
            lightning_speed_earn: 0.9,
            lightning_speed_shed: 1.2,
            reliable_min_jobs: 100,
            reliable_max_failure_rate: 0.02,
            reliable_shed_rate: 0.95,
            trusted_min_jobs: 200,
            heavy_lifter_min_large: 20,
            heavy_lifter_success_rate: 0.95,
            veteran_min_months: 6.0,
            veteran_min_jobs: 500,
            veteran_min_badges: 3,
        }
    }
}

// ============================================================================
// ReputationRecord
// ============================================================================

/// Complete reputation state for a single entity (node or collective).
///
/// Every field is designed for gossip propagation and deterministic
/// merge semantics. The `version` counter is bumped on every mutation;
/// during gossip merge the higher version wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReputationRecord {
    pub entity_id: EntityId,
    pub badges: HashSet<Badge>,
    /// Overall reputation score in the range `[0.0, 1.0]`.
    pub score: f32,

    // -- Lifetime counters --
    pub total_jobs: u64,
    pub successful_jobs: u64,
    pub failed_jobs: u64,
    pub policy_violations: u32,

    // -- Rolling windows (last N jobs) --
    pub recent_success: RollingWindow,
    pub recent_speed: RollingWindow,

    // -- Timing --
    pub first_seen: DateTime<Utc>,
    pub last_active: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Monotonic version counter, bumped on every mutation.
    pub version: u64,

    // -- Large job tracking (for HeavyLifter badge) --
    pub large_jobs_completed: u64,
    pub large_jobs_failed: u64,
}

impl ReputationRecord {
    /// Create a new reputation record for the given entity.
    ///
    /// Starts with the `Newcomer` badge, a default score of `0.5`,
    /// and empty rolling windows.
    pub fn new(entity_id: EntityId) -> Self {
        Self::with_config(entity_id, &ReputationConfig::default())
    }

    /// Create a new reputation record with custom config for window sizes.
    pub fn with_config(entity_id: EntityId, config: &ReputationConfig) -> Self {
        let now = Utc::now();
        let mut badges = HashSet::new();
        badges.insert(Badge::Newcomer);

        Self {
            entity_id,
            badges,
            score: 0.5,
            total_jobs: 0,
            successful_jobs: 0,
            failed_jobs: 0,
            policy_violations: 0,
            recent_success: RollingWindow::new(config.rolling_window_size),
            recent_speed: RollingWindow::new(config.rolling_window_size),
            first_seen: now,
            last_active: now,
            updated_at: now,
            version: 1,
            large_jobs_completed: 0,
            large_jobs_failed: 0,
        }
    }

    /// Record a successful job completion.
    ///
    /// - `speed_ratio`: `actual_time / estimated_time` (lower is faster)
    /// - `is_large_job`: whether the job data exceeded 1 GB
    pub fn record_success(&mut self, speed_ratio: f32, is_large_job: bool) {
        self.total_jobs += 1;
        self.successful_jobs += 1;
        self.recent_success.push(1.0);
        self.recent_speed.push(speed_ratio);
        self.last_active = Utc::now();

        if is_large_job {
            self.large_jobs_completed += 1;
        }

        self.evaluate_badges_with_config(&ReputationConfig::default());
        self.recalculate_score();
        self.touch();
    }

    /// Record a successful job with custom config.
    pub fn record_success_with_config(
        &mut self,
        speed_ratio: f32,
        is_large_job: bool,
        config: &ReputationConfig,
    ) {
        self.total_jobs += 1;
        self.successful_jobs += 1;
        self.recent_success.push(1.0);
        self.recent_speed.push(speed_ratio);
        self.last_active = Utc::now();

        if is_large_job {
            self.large_jobs_completed += 1;
        }

        self.evaluate_badges_with_config(config);
        self.recalculate_score();
        self.touch();
    }

    /// Record a failed job.
    pub fn record_failure(&mut self, is_large_job: bool) {
        self.total_jobs += 1;
        self.failed_jobs += 1;
        self.recent_success.push(0.0);
        self.last_active = Utc::now();

        if is_large_job {
            self.large_jobs_failed += 1;
        }

        self.evaluate_badges_with_config(&ReputationConfig::default());
        self.recalculate_score();
        self.touch();
    }

    /// Record a failed job with custom config.
    pub fn record_failure_with_config(&mut self, is_large_job: bool, config: &ReputationConfig) {
        self.total_jobs += 1;
        self.failed_jobs += 1;
        self.recent_success.push(0.0);
        self.last_active = Utc::now();

        if is_large_job {
            self.large_jobs_failed += 1;
        }

        self.evaluate_badges_with_config(config);
        self.recalculate_score();
        self.touch();
    }

    /// Record a policy violation. Immediately triggers badge re-evaluation.
    pub fn record_violation(&mut self) {
        self.policy_violations += 1;
        self.last_active = Utc::now();

        self.evaluate_badges_with_config(&ReputationConfig::default());
        self.recalculate_score();
        self.touch();
    }

    /// Record a policy violation with custom config.
    pub fn record_violation_with_config(&mut self, config: &ReputationConfig) {
        self.policy_violations += 1;
        self.last_active = Utc::now();

        self.evaluate_badges_with_config(config);
        self.recalculate_score();
        self.touch();
    }

    /// Recalculate the overall reputation score from all metrics.
    ///
    /// Formula:
    /// ```text
    /// base = lifetime_success_rate * 0.3
    ///      + recent_success_rate   * 0.4
    ///      + speed_factor          * 0.15
    ///      + longevity             * 0.15
    /// ```
    ///
    /// Where:
    /// - `speed_factor = 1.0 - (recent_speed_avg - 1.0).max(0.0).min(1.0)`
    /// - `longevity = min(months_active / 12.0, 1.0)`
    /// Recalculate the overall reputation score from all metrics.
    ///
    /// Formula (Chronos Decay):
    /// ```text
    /// base = lifetime_success_rate * 0.2
    ///      + recent_success_rate   * 0.3
    ///      + speed_factor          * 0.4  <-- Latency is now the dominant factor
    ///      + longevity             * 0.1
    /// ```
    ///
    /// Where:
    /// - `speed_factor`: 1.0 if (actual < estimated), drops exponentially 
    ///   as actual time approaches timeout_ms.
    pub fn recalculate_score(&mut self) {
        let lifetime_success = self.success_rate();
        let recent_success = self.recent_success.success_rate();

        // speed_avg is actual_time / estimated_time. 
        // 1.0 means exactly on time. < 1.0 is fast. > 1.0 is slow.
        let speed_avg = self.recent_speed.average();
        
        // Chronos Decay: aggressively punish anything above 1.2x estimated time.
        // 1.0 -> 1.0
        // 1.5 -> 0.5
        // 2.0 -> 0.0
        let speed_factor = (2.0 - speed_avg).clamp(0.0, 1.0);

        let longevity = (self.months_active() / 12.0).min(1.0);

        self.score =
            lifetime_success * 0.2 + recent_success * 0.3 + speed_factor * 0.4 + longevity * 0.1;

        // Clamp to [0.0, 1.0] for safety
        self.score = self.score.clamp(0.0, 1.0);
    }

    /// Evaluate which badges should be earned or shed using default config.
    pub fn evaluate_badges(&mut self) {
        self.evaluate_badges_with_config(&ReputationConfig::default());
    }

    /// Evaluate which badges should be earned or shed.
    ///
    /// This is the core self-policing mechanism. Badges are granted when
    /// criteria are met and *removed* when criteria are no longer met.
    pub fn evaluate_badges_with_config(&mut self, config: &ReputationConfig) {
        // -- Newcomer --
        // Naturally removed when total_jobs >= threshold
        if self.total_jobs >= config.newcomer_threshold {
            self.badges.remove(&Badge::Newcomer);
        } else {
            self.badges.insert(Badge::Newcomer);
        }

        // -- Lightning --
        // Earn: total_jobs >= 50 AND recent speed average < earn threshold
        // Shed: recent speed average > shed threshold
        if self.total_jobs >= config.newcomer_threshold
            && self.recent_speed.count() > 0
            && self.recent_speed.average() < config.lightning_speed_earn
        {
            self.badges.insert(Badge::Lightning);
        }
        if self.recent_speed.count() > 0 && self.recent_speed.average() > config.lightning_speed_shed
        {
            self.badges.remove(&Badge::Lightning);
        }

        // -- Reliable --
        // Earn: total_jobs >= min AND lifetime failure_rate < max
        // Shed: recent success rate < shed threshold
        if self.total_jobs >= config.reliable_min_jobs
            && self.failure_rate() < config.reliable_max_failure_rate
        {
            self.badges.insert(Badge::Reliable);
        }
        if self.recent_success.count() > 0
            && self.recent_success.success_rate() < config.reliable_shed_rate
        {
            self.badges.remove(&Badge::Reliable);
        }

        // -- Trusted --
        // Earn: total_jobs >= min AND policy_violations == 0
        // Shed: policy_violations > 0
        if self.total_jobs >= config.trusted_min_jobs && self.policy_violations == 0 {
            self.badges.insert(Badge::Trusted);
        }
        if self.policy_violations > 0 {
            self.badges.remove(&Badge::Trusted);
        }

        // -- HeavyLifter --
        // Earn: large_jobs_completed >= min AND large success rate >= threshold
        // Shed: recent large job failure rate > 10%
        let total_large = self.large_jobs_completed + self.large_jobs_failed;
        let large_success_rate = if total_large > 0 {
            self.large_jobs_completed as f32 / total_large as f32
        } else {
            0.0
        };

        if self.large_jobs_completed >= config.heavy_lifter_min_large
            && large_success_rate >= config.heavy_lifter_success_rate
        {
            self.badges.insert(Badge::HeavyLifter);
        }
        if total_large > 0 && large_success_rate < (1.0 - 0.1) {
            // failure rate > 10%
            self.badges.remove(&Badge::HeavyLifter);
        }

        // -- Veteran --
        // Earn: months_active >= min AND total_jobs >= min AND qualifying badges >= min
        // Shed: lost enough other badges (qualifying badges < min)
        // Qualifying badges: everything except Newcomer and Veteran itself
        let qualifying_badge_count = self
            .badges
            .iter()
            .filter(|b| **b != Badge::Newcomer && **b != Badge::Veteran)
            .count();

        if self.months_active() >= config.veteran_min_months
            && self.total_jobs >= config.veteran_min_jobs
            && qualifying_badge_count >= config.veteran_min_badges
        {
            self.badges.insert(Badge::Veteran);
        }
        // Shed veteran if qualifying badges dropped below threshold
        if self.badges.contains(&Badge::Veteran) && qualifying_badge_count < config.veteran_min_badges
        {
            self.badges.remove(&Badge::Veteran);
        }
    }

    /// Lifetime success rate as a fraction in `[0.0, 1.0]`.
    /// Returns `1.0` if no jobs have been recorded (optimistic default).
    pub fn success_rate(&self) -> f32 {
        if self.total_jobs == 0 {
            return 1.0;
        }
        self.successful_jobs as f32 / self.total_jobs as f32
    }

    /// Lifetime failure rate as a fraction in `[0.0, 1.0]`.
    /// Returns `0.0` if no jobs have been recorded.
    pub fn failure_rate(&self) -> f32 {
        if self.total_jobs == 0 {
            return 0.0;
        }
        self.failed_jobs as f32 / self.total_jobs as f32
    }

    /// How many months this entity has been active (first_seen to now).
    pub fn months_active(&self) -> f32 {
        let duration = Utc::now() - self.first_seen;
        let days = duration.num_days() as f32;
        days / 30.44 // average days per month
    }

    /// Check whether this entity currently holds a specific badge.
    pub fn has_badge(&self, badge: Badge) -> bool {
        self.badges.contains(&badge)
    }

    /// Bump the version counter and update timestamp.
    pub fn touch(&mut self) {
        self.version += 1;
        self.updated_at = Utc::now();
    }
}

// ============================================================================
// ReputationStore
// ============================================================================

/// Thread-safe, concurrent store for all entity reputation records.
///
/// Backed by `DashMap` for lock-free reads and fine-grained write locking.
/// Designed for gossip propagation: records can be merged from remote
/// peers using version-based conflict resolution.
pub struct ReputationStore {
    pub(crate) records: DashMap<EntityId, ReputationRecord>,
    config: ReputationConfig,
}

impl ReputationStore {
    /// Create a new empty reputation store with default config.
    pub fn new() -> Self {
        Self {
            records: DashMap::new(),
            config: ReputationConfig::default(),
        }
    }

    /// Create a new reputation store with custom config.
    pub fn with_config(config: ReputationConfig) -> Self {
        Self {
            records: DashMap::new(),
            config,
        }
    }

    /// Get the config reference.
    pub fn config(&self) -> &ReputationConfig {
        &self.config
    }

    /// Get or create a reputation record for the given entity.
    ///
    /// If the entity has no record yet, a fresh one is created with
    /// the `Newcomer` badge and default score.
    pub fn get_or_create(&self, entity_id: EntityId) -> ReputationRecord {
        self.records
            .entry(entity_id.clone())
            .or_insert_with(|| ReputationRecord::with_config(entity_id, &self.config))
            .value()
            .clone()
    }

    /// Get a record if it exists.
    pub fn get(&self, entity_id: &EntityId) -> Option<ReputationRecord> {
        self.records.get(entity_id).map(|r| r.value().clone())
    }

    /// Get just the score for an entity. Returns `0.5` (neutral) if unknown.
    pub fn score_for(&self, entity_id: &EntityId) -> f32 {
        self.records
            .get(entity_id)
            .map(|r| r.score)
            .unwrap_or(0.5)
    }

    /// Adjust an entity's score by a fractional delta, clamping to [0.0, 1.0].
    ///
    /// Used by the admission system: jurors get +1% bonus on successful
    /// admission, or -5% penalty when an admitted node is expelled.
    pub fn adjust_score(&self, entity_id: &EntityId, delta: f32) {
        if let Some(mut record) = self.records.get_mut(entity_id) {
            record.score = (record.score + delta).clamp(0.0, 1.0);
            record.version += 1;
            record.updated_at = Utc::now();
        }
    }

    /// Get the badges for an entity. Returns empty set if unknown.
    pub fn badges_for(&self, entity_id: &EntityId) -> HashSet<Badge> {
        self.records
            .get(entity_id)
            .map(|r| r.badges.clone())
            .unwrap_or_default()
    }

    /// Record a job result and re-evaluate badges.
    ///
    /// Creates a new record if the entity is not yet known.
    pub fn record_job_result(
        &self,
        entity_id: EntityId,
        success: bool,
        speed_ratio: f32,
        is_large_job: bool,
    ) {
        let mut entry = self
            .records
            .entry(entity_id.clone())
            .or_insert_with(|| ReputationRecord::with_config(entity_id, &self.config));

        let record = entry.value_mut();
        if success {
            record.record_success_with_config(speed_ratio, is_large_job, &self.config);
        } else {
            record.record_failure_with_config(is_large_job, &self.config);
        }

        trace!(
            entity = %record.entity_id,
            total_jobs = record.total_jobs,
            score = record.score,
            badges = ?record.badges,
            "job result recorded"
        );
    }

    /// Permanently slash an entity's score to 0.0 (Local Slashing).
    pub fn slash_score(&self, entity_id: &EntityId) {
        if let Some(mut record) = self.records.get_mut(entity_id) {
            record.score = 0.0;
            record.policy_violations += 1;
            record.version += 1;
            record.updated_at = Utc::now();
            record.badges.insert(Badge::Malicious);
        }
    }

    /// Record a policy violation for an entity.
    pub fn record_violation(&self, entity_id: EntityId) {
        let mut entry = self
            .records
            .entry(entity_id.clone())
            .or_insert_with(|| ReputationRecord::with_config(entity_id, &self.config));

        entry.value_mut().record_violation_with_config(&self.config);

        debug!(
            entity = %entry.value().entity_id,
            violations = entry.value().policy_violations,
            "policy violation recorded"
        );
    }

    /// Merge a reputation record received from gossip.
    ///
    /// Conflict resolution:
    /// - Higher `version` wins (more recent data).
    /// - If versions are equal, the record with higher `total_jobs` wins
    ///   (more observations = more authoritative).
    ///
    /// Returns `true` if the incoming record was accepted (replaced the
    /// existing one or was inserted fresh).
    pub fn merge(&self, record: ReputationRecord) -> bool {
        let entity_id = record.entity_id.clone();

        match self.records.entry(entity_id) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(record);
                true
            }
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let existing = occupied.get();

                // Newer version wins
                if record.version > existing.version {
                    occupied.insert(record);
                    return true;
                }

                // Same version: more data wins
                if record.version == existing.version && record.total_jobs > existing.total_jobs {
                    occupied.insert(record);
                    return true;
                }

                false
            }
        }
    }

    /// Merge a reputation record, rejecting self-authored records.
    ///
    /// Hardening A.9: A node must not be able to gossip its own reputation
    /// record. If the record's entity is `EntityId::Node(nid)` and `nid`
    /// matches the `sender_id`, the record is silently rejected.
    pub fn merge_with_sender(&self, record: ReputationRecord, sender_id: NodeId) -> bool {
        if let EntityId::Node(ref nid) = record.entity_id {
            if *nid == sender_id {
                return false; // Reject: node cannot author its own reputation
            }
        }
        self.merge(record)
    }

    /// Sample up to `max` records for gossip propagation.
    ///
    /// Returns a random sample of records, prioritizing recently-updated ones.
    pub fn sample(&self, max: usize) -> Vec<ReputationRecord> {
        use rand::seq::SliceRandom;

        let mut all: Vec<ReputationRecord> = self.records.iter().map(|r| r.value().clone()).collect();

        if all.len() <= max {
            return all;
        }

        // Shuffle and take first `max` entries
        let mut rng = {
            // ThreadRng is !Send, scope it before returning
            rand::thread_rng()
        };
        all.shuffle(&mut rng);
        all.truncate(max);
        all
    }

    /// Get the top N entities by score, sorted descending.
    pub fn top_by_score(&self, n: usize) -> Vec<ReputationRecord> {
        let mut all: Vec<ReputationRecord> = self.records.iter().map(|r| r.value().clone()).collect();
        all.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        all.truncate(n);
        all
    }

    /// Get all entity IDs that currently hold a specific badge.
    pub fn with_badge(&self, badge: Badge) -> Vec<EntityId> {
        self.records
            .iter()
            .filter(|r| r.value().badges.contains(&badge))
            .map(|r| r.key().clone())
            .collect()
    }

    /// Total number of tracked entities.
    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// Remove records for entities not seen within `max_age`.
    ///
    /// Returns the number of records pruned.
    pub fn prune_stale(&self, max_age: chrono::Duration) -> usize {
        let cutoff = Utc::now() - max_age;
        let stale_keys: Vec<EntityId> = self
            .records
            .iter()
            .filter(|r| r.value().last_active < cutoff)
            .map(|r| r.key().clone())
            .collect();

        let count = stale_keys.len();
        for key in stale_keys {
            self.records.remove(&key);
        }

        if count > 0 {
            debug!(pruned = count, "pruned stale reputation records");
        }

        count
    }
}

impl Default for ReputationStore {
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
    use chrono::Duration;

    // Helper: create a node entity ID
    fn node_entity() -> EntityId {
        EntityId::Node(NodeId::new())
    }

    // Helper: create a collective entity ID
    fn collective_entity() -> EntityId {
        EntityId::Collective(CollectiveId::new())
    }

    // Helper: default config
    fn config() -> ReputationConfig {
        ReputationConfig::default()
    }

    // ====================================================================
    // RollingWindow tests
    // ====================================================================

    #[test]
    fn rolling_window_new() {
        let w = RollingWindow::new(10);
        assert_eq!(w.count(), 0);
        assert!(!w.is_full());
        assert_eq!(w.average(), 0.0);
        assert_eq!(w.success_rate(), 1.0); // optimistic default
    }

    #[test]
    fn rolling_window_push_and_average() {
        let mut w = RollingWindow::new(3);
        w.push(2.0);
        w.push(4.0);
        w.push(6.0);
        assert_eq!(w.count(), 3);
        assert!(w.is_full());
        assert!((w.average() - 4.0).abs() < f32::EPSILON);
    }

    #[test]
    fn rolling_window_eviction() {
        let mut w = RollingWindow::new(3);
        w.push(1.0);
        w.push(2.0);
        w.push(3.0);
        w.push(10.0); // evicts 1.0
        assert_eq!(w.count(), 3);
        assert!((w.average() - 5.0).abs() < f32::EPSILON); // (2+3+10)/3 = 5.0
    }

    #[test]
    fn rolling_window_success_rate() {
        let mut w = RollingWindow::new(10);
        // 8 successes, 2 failures
        for _ in 0..8 {
            w.push(1.0);
        }
        for _ in 0..2 {
            w.push(0.0);
        }
        assert!((w.success_rate() - 0.8).abs() < f32::EPSILON);
    }

    #[test]
    fn rolling_window_success_rate_all_success() {
        let mut w = RollingWindow::new(5);
        for _ in 0..5 {
            w.push(1.0);
        }
        assert!((w.success_rate() - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn rolling_window_success_rate_all_failure() {
        let mut w = RollingWindow::new(5);
        for _ in 0..5 {
            w.push(0.0);
        }
        assert!((w.success_rate() - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn rolling_window_zero_capacity_panics() {
        let _ = RollingWindow::new(0);
    }

    #[test]
    fn rolling_window_serialization_roundtrip() {
        let mut w = RollingWindow::new(5);
        w.push(1.0);
        w.push(0.5);
        w.push(0.75);

        let json = serde_json::to_string(&w).unwrap();
        let deserialized: RollingWindow = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.count(), 3);
        assert_eq!(deserialized.capacity, 5);
        assert!((deserialized.average() - w.average()).abs() < f32::EPSILON);
    }

    // ====================================================================
    // Badge tests
    // ====================================================================

    #[test]
    fn badge_all_variants() {
        assert_eq!(Badge::ALL.len(), 6);
    }

    #[test]
    fn badge_descriptions_non_empty() {
        for badge in Badge::ALL {
            assert!(!badge.description().is_empty(), "badge {:?} has empty description", badge);
        }
    }

    #[test]
    fn badge_min_jobs() {
        assert_eq!(Badge::Newcomer.min_jobs(), 0);
        assert_eq!(Badge::Lightning.min_jobs(), 50);
        assert_eq!(Badge::Reliable.min_jobs(), 100);
        assert_eq!(Badge::Trusted.min_jobs(), 200);
        assert_eq!(Badge::Veteran.min_jobs(), 500);
    }

    #[test]
    fn badge_display() {
        assert_eq!(format!("{}", Badge::Newcomer), "newcomer");
        assert_eq!(format!("{}", Badge::Lightning), "lightning");
        assert_eq!(format!("{}", Badge::HeavyLifter), "heavy_lifter");
    }

    // ====================================================================
    // EntityId tests
    // ====================================================================

    #[test]
    fn entity_id_from_node() {
        let node_id = NodeId::new();
        let entity: EntityId = node_id.into();
        assert!(matches!(entity, EntityId::Node(_)));
    }

    #[test]
    fn entity_id_from_collective() {
        let coll_id = CollectiveId::new();
        let entity: EntityId = coll_id.into();
        assert!(matches!(entity, EntityId::Collective(_)));
    }

    #[test]
    fn entity_id_display() {
        use uuid::Uuid;
        let node_entity = EntityId::Node(NodeId::new());
        let display = format!("{}", node_entity);
        assert!(Uuid::parse_str(&display).is_ok());

        let coll_entity = EntityId::Collective(CollectiveId::new());
        let display = format!("{}", coll_entity);
        // CollectiveId's Display uses short hex format
        assert!(!display.is_empty());
    }

    #[test]
    fn entity_id_equality() {
        let id = NodeId::new();
        let e1 = EntityId::Node(id);
        let e2 = EntityId::Node(id);
        assert_eq!(e1, e2);

        let e3 = EntityId::Node(NodeId::new());
        assert_ne!(e1, e3);
    }

    // ====================================================================
    // ReputationRecord tests
    // ====================================================================

    #[test]
    fn new_entity_starts_with_newcomer() {
        let record = ReputationRecord::new(node_entity());
        assert!(record.has_badge(Badge::Newcomer));
        assert_eq!(record.badges.len(), 1);
        assert_eq!(record.total_jobs, 0);
        assert!((record.score - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn record_success_increments_counters() {
        let mut record = ReputationRecord::new(node_entity());
        record.record_success(0.8, false);
        assert_eq!(record.total_jobs, 1);
        assert_eq!(record.successful_jobs, 1);
        assert_eq!(record.failed_jobs, 0);
        assert_eq!(record.recent_success.count(), 1);
        assert_eq!(record.recent_speed.count(), 1);
    }

    #[test]
    fn record_failure_increments_counters() {
        let mut record = ReputationRecord::new(node_entity());
        record.record_failure(false);
        assert_eq!(record.total_jobs, 1);
        assert_eq!(record.successful_jobs, 0);
        assert_eq!(record.failed_jobs, 1);
    }

    #[test]
    fn record_large_job_tracking() {
        let mut record = ReputationRecord::new(node_entity());
        record.record_success(0.8, true);
        assert_eq!(record.large_jobs_completed, 1);
        assert_eq!(record.large_jobs_failed, 0);

        record.record_failure(true);
        assert_eq!(record.large_jobs_completed, 1);
        assert_eq!(record.large_jobs_failed, 1);
    }

    #[test]
    fn newcomer_badge_removed_after_threshold() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // Complete 49 jobs: still Newcomer
        for _ in 0..49 {
            record.record_success_with_config(0.8, false, &cfg);
        }
        assert!(record.has_badge(Badge::Newcomer));

        // Job 50: Newcomer removed
        record.record_success_with_config(0.8, false, &cfg);
        assert!(!record.has_badge(Badge::Newcomer));
    }

    #[test]
    fn lightning_badge_earned() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // Complete 50 fast jobs (speed_ratio < 0.9)
        for _ in 0..50 {
            record.record_success_with_config(0.7, false, &cfg);
        }

        assert!(record.has_badge(Badge::Lightning));
    }

    #[test]
    fn lightning_badge_shed_when_slow() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // Earn lightning first
        for _ in 0..50 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        assert!(record.has_badge(Badge::Lightning));

        // Now submit many slow jobs to push average above shed threshold
        // The window is 50, so we need to fill it with slow jobs
        for _ in 0..50 {
            record.record_success_with_config(1.5, false, &cfg);
        }
        assert!(!record.has_badge(Badge::Lightning));
    }

    #[test]
    fn reliable_badge_earned() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // 100 successful jobs, 0 failures => failure_rate = 0.0 < 0.02
        for _ in 0..100 {
            record.record_success_with_config(1.0, false, &cfg);
        }

        assert!(record.has_badge(Badge::Reliable));
    }

    #[test]
    fn reliable_badge_shed_on_recent_failures() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // Earn reliable: 100 successes
        for _ in 0..100 {
            record.record_success_with_config(1.0, false, &cfg);
        }
        assert!(record.has_badge(Badge::Reliable));

        // Recent window is 50. Fill with 50 failures to drop recent success below 0.95
        // (all 50 failures = success_rate 0.0 < 0.95)
        for _ in 0..50 {
            record.record_failure_with_config(false, &cfg);
        }
        assert!(!record.has_badge(Badge::Reliable));
    }

    #[test]
    fn trusted_badge_earned() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // 200 successful jobs, 0 violations
        for _ in 0..200 {
            record.record_success_with_config(1.0, false, &cfg);
        }
        assert_eq!(record.policy_violations, 0);
        assert!(record.has_badge(Badge::Trusted));
    }

    #[test]
    fn trusted_badge_shed_on_violation() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // Earn trusted
        for _ in 0..200 {
            record.record_success_with_config(1.0, false, &cfg);
        }
        assert!(record.has_badge(Badge::Trusted));

        // Single violation => shed
        record.record_violation_with_config(&cfg);
        assert!(!record.has_badge(Badge::Trusted));
    }

    #[test]
    fn heavy_lifter_badge_earned() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // 20 large jobs completed successfully, 0 failed
        for _ in 0..20 {
            record.record_success_with_config(1.0, true, &cfg);
        }

        assert!(record.has_badge(Badge::HeavyLifter));
    }

    #[test]
    fn heavy_lifter_badge_shed_on_failures() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // Earn heavy lifter
        for _ in 0..20 {
            record.record_success_with_config(1.0, true, &cfg);
        }
        assert!(record.has_badge(Badge::HeavyLifter));

        // Add enough large failures to push failure rate above 10%
        // Currently: 20 completed, 0 failed. Need > 10% failure rate.
        // 3 failures out of 23 total = 13% failure rate > 10%
        for _ in 0..3 {
            record.record_failure_with_config(true, &cfg);
        }
        assert!(!record.has_badge(Badge::HeavyLifter));
    }

    #[test]
    fn veteran_badge_earned() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // Set first_seen to 7 months ago
        record.first_seen = Utc::now() - Duration::days(210);

        // Need 3+ qualifying badges (not Newcomer, not Veteran)
        // Earn Lightning, Reliable, Trusted, HeavyLifter
        // First get past newcomer phase with fast jobs
        for _ in 0..200 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        // Earn HeavyLifter
        for _ in 0..20 {
            record.record_success_with_config(0.7, true, &cfg);
        }
        // Now get to 500 jobs
        for _ in 0..280 {
            record.record_success_with_config(0.7, false, &cfg);
        }

        // Should have: Lightning, Reliable, Trusted, HeavyLifter (4 qualifying)
        // Plus Veteran conditions met: 7 months, 500 jobs, 4 badges
        assert!(record.has_badge(Badge::Veteran), "badges: {:?}", record.badges);
    }

    #[test]
    fn veteran_badge_shed_when_badges_lost() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        // Set first_seen to 7 months ago
        record.first_seen = Utc::now() - Duration::days(210);

        // Earn multiple badges quickly
        for _ in 0..200 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        for _ in 0..20 {
            record.record_success_with_config(0.7, true, &cfg);
        }
        for _ in 0..280 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        assert!(record.has_badge(Badge::Veteran));

        // Lose Trusted via violation
        record.record_violation_with_config(&cfg);

        // Lose Lightning by being slow
        for _ in 0..50 {
            record.record_success_with_config(1.5, false, &cfg);
        }

        // Now qualifying badges: Reliable, HeavyLifter = 2 (< 3 required)
        // Veteran should be shed
        assert!(!record.has_badge(Badge::Veteran), "badges: {:?}", record.badges);
    }

    #[test]
    fn score_calculation_perfect_entity() {
        let mut record = ReputationRecord::new(node_entity());
        // Set first_seen to 12 months ago for full longevity
        record.first_seen = Utc::now() - Duration::days(365);

        // All successes, fast speed
        for _ in 0..100 {
            record.record_success(0.5, false);
        }

        // lifetime_success = 1.0
        // recent_success = 1.0
        // speed_factor = 1.0 - (0.5 - 1.0).max(0.0) = 1.0
        // longevity = min(12/12, 1.0) = 1.0
        // score = 1.0*0.3 + 1.0*0.4 + 1.0*0.15 + 1.0*0.15 = 1.0
        assert!(
            (record.score - 1.0).abs() < 0.05,
            "score should be near 1.0, got {}",
            record.score
        );
    }

    #[test]
    fn score_calculation_poor_entity() {
        let mut record = ReputationRecord::new(node_entity());

        // 50% failure rate, slow speed
        for _ in 0..25 {
            record.record_success(2.0, false);
        }
        for _ in 0..25 {
            record.record_failure(false);
        }

        // Should be well below 0.5
        assert!(
            record.score < 0.5,
            "score should be below 0.5, got {}",
            record.score
        );
    }

    #[test]
    fn success_rate_zero_jobs() {
        let record = ReputationRecord::new(node_entity());
        assert!((record.success_rate() - 1.0).abs() < f32::EPSILON);
        assert!((record.failure_rate() - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn version_increments_on_mutations() {
        let mut record = ReputationRecord::new(node_entity());
        let v0 = record.version;

        record.record_success(1.0, false);
        assert!(record.version > v0);

        let v1 = record.version;
        record.record_failure(false);
        assert!(record.version > v1);

        let v2 = record.version;
        record.record_violation();
        assert!(record.version > v2);
    }

    // ====================================================================
    // ReputationRecord serialization
    // ====================================================================

    #[test]
    fn reputation_record_serialization_roundtrip() {
        let mut record = ReputationRecord::new(node_entity());
        record.record_success(0.8, false);
        record.record_success(0.9, true);
        record.record_failure(false);

        let json = serde_json::to_string(&record).unwrap();
        let deserialized: ReputationRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.entity_id, record.entity_id);
        assert_eq!(deserialized.total_jobs, record.total_jobs);
        assert_eq!(deserialized.successful_jobs, record.successful_jobs);
        assert_eq!(deserialized.failed_jobs, record.failed_jobs);
        assert_eq!(deserialized.policy_violations, record.policy_violations);
        assert_eq!(deserialized.large_jobs_completed, record.large_jobs_completed);
        assert_eq!(deserialized.badges, record.badges);
        assert!((deserialized.score - record.score).abs() < f32::EPSILON);
    }

    // ====================================================================
    // ReputationStore tests
    // ====================================================================

    #[test]
    fn store_get_or_create() {
        let store = ReputationStore::new();
        let entity = node_entity();

        let record = store.get_or_create(entity.clone());
        assert!(record.has_badge(Badge::Newcomer));
        assert_eq!(store.count(), 1);

        // Second call returns the same record (not a new one)
        let record2 = store.get_or_create(entity);
        assert_eq!(record.entity_id, record2.entity_id);
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn store_get_nonexistent() {
        let store = ReputationStore::new();
        assert!(store.get(&node_entity()).is_none());
    }

    #[test]
    fn store_score_for_unknown() {
        let store = ReputationStore::new();
        assert!((store.score_for(&node_entity()) - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn store_badges_for_unknown() {
        let store = ReputationStore::new();
        assert!(store.badges_for(&node_entity()).is_empty());
    }

    #[test]
    fn store_record_job_result() {
        let store = ReputationStore::new();
        let entity = node_entity();

        store.record_job_result(entity.clone(), true, 0.8, false);

        let record = store.get(&entity).unwrap();
        assert_eq!(record.total_jobs, 1);
        assert_eq!(record.successful_jobs, 1);
    }

    #[test]
    fn store_record_violation() {
        let store = ReputationStore::new();
        let entity = node_entity();

        store.record_violation(entity.clone());

        let record = store.get(&entity).unwrap();
        assert_eq!(record.policy_violations, 1);
    }

    #[test]
    fn store_merge_newer_version_wins() {
        let store = ReputationStore::new();
        let entity = node_entity();

        // Insert initial record
        let mut record1 = ReputationRecord::new(entity.clone());
        record1.version = 5;
        record1.total_jobs = 10;
        store.merge(record1);

        // Merge with newer version
        let mut record2 = ReputationRecord::new(entity.clone());
        record2.version = 10;
        record2.total_jobs = 20;
        assert!(store.merge(record2));

        let stored = store.get(&entity).unwrap();
        assert_eq!(stored.version, 10);
        assert_eq!(stored.total_jobs, 20);
    }

    #[test]
    fn store_merge_older_version_rejected() {
        let store = ReputationStore::new();
        let entity = node_entity();

        let mut record1 = ReputationRecord::new(entity.clone());
        record1.version = 10;
        record1.total_jobs = 20;
        store.merge(record1);

        let mut record2 = ReputationRecord::new(entity.clone());
        record2.version = 5;
        record2.total_jobs = 10;
        assert!(!store.merge(record2));

        let stored = store.get(&entity).unwrap();
        assert_eq!(stored.version, 10);
    }

    #[test]
    fn store_merge_same_version_more_jobs_wins() {
        let store = ReputationStore::new();
        let entity = node_entity();

        let mut record1 = ReputationRecord::new(entity.clone());
        record1.version = 5;
        record1.total_jobs = 10;
        store.merge(record1);

        let mut record2 = ReputationRecord::new(entity.clone());
        record2.version = 5;
        record2.total_jobs = 20;
        assert!(store.merge(record2));

        let stored = store.get(&entity).unwrap();
        assert_eq!(stored.total_jobs, 20);
    }

    #[test]
    fn store_merge_same_version_fewer_jobs_rejected() {
        let store = ReputationStore::new();
        let entity = node_entity();

        let mut record1 = ReputationRecord::new(entity.clone());
        record1.version = 5;
        record1.total_jobs = 20;
        store.merge(record1);

        let mut record2 = ReputationRecord::new(entity.clone());
        record2.version = 5;
        record2.total_jobs = 10;
        assert!(!store.merge(record2));

        let stored = store.get(&entity).unwrap();
        assert_eq!(stored.total_jobs, 20);
    }

    #[test]
    fn store_sample() {
        let store = ReputationStore::new();

        for _ in 0..10 {
            store.get_or_create(node_entity());
        }

        let sample = store.sample(5);
        assert_eq!(sample.len(), 5);

        let full_sample = store.sample(100);
        assert_eq!(full_sample.len(), 10);
    }

    #[test]
    fn store_top_by_score() {
        let store = ReputationStore::new();

        let e1 = node_entity();
        let e2 = node_entity();
        let e3 = node_entity();

        // Give different scores by recording different outcomes
        for _ in 0..10 {
            store.record_job_result(e1.clone(), true, 0.5, false);
        }
        for _ in 0..10 {
            store.record_job_result(e2.clone(), true, 1.5, false);
        }
        // e3: mix of success and failure
        for _ in 0..5 {
            store.record_job_result(e3.clone(), true, 1.0, false);
        }
        for _ in 0..5 {
            store.record_job_result(e3.clone(), false, 1.0, false);
        }

        let top = store.top_by_score(2);
        assert_eq!(top.len(), 2);
        // First entry should have the highest score
        assert!(top[0].score >= top[1].score);
    }

    #[test]
    fn store_with_badge() {
        let store = ReputationStore::new();

        let e1 = node_entity();
        let e2 = node_entity();

        store.get_or_create(e1.clone());
        store.get_or_create(e2.clone());

        // Both should be Newcomers
        let newcomers = store.with_badge(Badge::Newcomer);
        assert_eq!(newcomers.len(), 2);

        // No one should be Lightning yet
        let lightning = store.with_badge(Badge::Lightning);
        assert!(lightning.is_empty());
    }

    #[test]
    fn store_prune_stale() {
        let store = ReputationStore::new();

        let e1 = node_entity();
        let e2 = node_entity();

        // Create both
        store.get_or_create(e1.clone());
        store.get_or_create(e2.clone());

        // Manually set e1 to be very old
        if let Some(mut record) = store.records.get_mut(&e1) {
            record.last_active = Utc::now() - Duration::days(100);
        }

        // Prune anything older than 30 days
        let pruned = store.prune_stale(Duration::days(30));
        assert_eq!(pruned, 1);
        assert_eq!(store.count(), 1);

        // e1 should be gone, e2 should remain
        assert!(store.get(&e1).is_none());
        assert!(store.get(&e2).is_some());
    }

    #[test]
    fn store_count() {
        let store = ReputationStore::new();
        assert_eq!(store.count(), 0);

        store.get_or_create(node_entity());
        assert_eq!(store.count(), 1);

        store.get_or_create(node_entity());
        assert_eq!(store.count(), 2);
    }

    // ====================================================================
    // Config tests
    // ====================================================================

    #[test]
    fn reputation_config_default() {
        let cfg = ReputationConfig::default();
        assert_eq!(cfg.rolling_window_size, 50);
        assert_eq!(cfg.newcomer_threshold, 50);
        assert!((cfg.lightning_speed_earn - 0.9).abs() < f32::EPSILON);
        assert!((cfg.lightning_speed_shed - 1.2).abs() < f32::EPSILON);
        assert_eq!(cfg.reliable_min_jobs, 100);
        assert!((cfg.reliable_max_failure_rate - 0.02).abs() < f32::EPSILON);
        assert!((cfg.reliable_shed_rate - 0.95).abs() < f32::EPSILON);
        assert_eq!(cfg.trusted_min_jobs, 200);
        assert_eq!(cfg.heavy_lifter_min_large, 20);
        assert!((cfg.heavy_lifter_success_rate - 0.95).abs() < f32::EPSILON);
        assert!((cfg.veteran_min_months - 6.0).abs() < f32::EPSILON);
        assert_eq!(cfg.veteran_min_jobs, 500);
        assert_eq!(cfg.veteran_min_badges, 3);
    }

    #[test]
    fn reputation_config_serialization_roundtrip() {
        let cfg = ReputationConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let deserialized: ReputationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.rolling_window_size, cfg.rolling_window_size);
        assert_eq!(deserialized.newcomer_threshold, cfg.newcomer_threshold);
    }

    // ====================================================================
    // Edge cases
    // ====================================================================

    #[test]
    fn collective_entity_reputation() {
        let store = ReputationStore::new();
        let entity = collective_entity();

        store.record_job_result(entity.clone(), true, 0.8, false);
        let record = store.get(&entity).unwrap();
        assert_eq!(record.total_jobs, 1);
        assert!(matches!(record.entity_id, EntityId::Collective(_)));
    }

    #[test]
    fn multiple_violations_keep_trusted_shed() {
        let cfg = config();
        let mut record = ReputationRecord::with_config(node_entity(), &cfg);

        for _ in 0..200 {
            record.record_success_with_config(1.0, false, &cfg);
        }
        assert!(record.has_badge(Badge::Trusted));

        record.record_violation_with_config(&cfg);
        assert!(!record.has_badge(Badge::Trusted));

        // More jobs won't re-earn Trusted with violations > 0
        for _ in 0..100 {
            record.record_success_with_config(1.0, false, &cfg);
        }
        assert!(!record.has_badge(Badge::Trusted));
    }

    #[test]
    fn empty_store_operations() {
        let store = ReputationStore::new();
        assert_eq!(store.count(), 0);
        assert!(store.top_by_score(10).is_empty());
        assert!(store.sample(10).is_empty());
        assert!(store.with_badge(Badge::Newcomer).is_empty());
        assert_eq!(store.prune_stale(Duration::days(1)), 0);
    }

    #[test]
    fn score_clamped_to_valid_range() {
        let mut record = ReputationRecord::new(node_entity());
        // Even with extreme values, score should be in [0.0, 1.0]
        for _ in 0..100 {
            record.record_failure(false);
        }
        assert!(record.score >= 0.0);
        assert!(record.score <= 1.0);

        let mut record2 = ReputationRecord::new(node_entity());
        record2.first_seen = Utc::now() - Duration::days(3650); // 10 years
        for _ in 0..100 {
            record2.record_success(0.1, false);
        }
        assert!(record2.score >= 0.0);
        assert!(record2.score <= 1.0);
    }

    #[test]
    fn months_active_zero_for_new_entity() {
        let record = ReputationRecord::new(node_entity());
        // Just created, months_active should be very close to 0
        assert!(record.months_active() < 0.01);
    }

    #[test]
    fn store_merge_fresh_entity() {
        let store = ReputationStore::new();
        let entity = node_entity();

        let record = ReputationRecord::new(entity.clone());
        assert!(store.merge(record));
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn has_badge_returns_false_for_unearned() {
        let record = ReputationRecord::new(node_entity());
        assert!(!record.has_badge(Badge::Lightning));
        assert!(!record.has_badge(Badge::Reliable));
        assert!(!record.has_badge(Badge::Trusted));
        assert!(!record.has_badge(Badge::HeavyLifter));
        assert!(!record.has_badge(Badge::Veteran));
    }

    #[test]
    fn store_with_custom_config() {
        let mut cfg = ReputationConfig::default();
        cfg.newcomer_threshold = 10; // lower threshold for testing

        let store = ReputationStore::with_config(cfg);
        let entity = node_entity();

        for _ in 0..10 {
            store.record_job_result(entity.clone(), true, 0.8, false);
        }

        let record = store.get(&entity).unwrap();
        assert!(!record.has_badge(Badge::Newcomer));
    }
}
