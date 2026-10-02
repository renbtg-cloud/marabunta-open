// Marabunta - Licensed under the MIT License.
//! Policy guardrails for the organic swarm.
//!
//! Each swarm node carries a [`PolicySet`] that defines behavioral rules:
//! geo-fencing, reputation gates, rate limits, node-type restrictions,
//! collective constraints, and node blacklists. Policies are propagated
//! via gossip and merged using version-based conflict resolution (higher
//! version wins, preventing rollback attacks).
//!
//! The [`PolicyEngine`] evaluates policies against incoming work and
//! outbound actions, acting as a local gatekeeper. It combines the active
//! policy set with a [`RateLimitTracker`] for sliding-window enforcement.
//!
//! The [`PolicyStore`] provides concurrent, gossip-mergeable storage for
//! policies across the swarm, backed by [`DashMap`].

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Mutex;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::common::types::JobId;

use super::config::{RESIDENCY_AUDIT_MAX_ENTRIES, RESIDENCY_AUDIT_ENABLED};
use super::types::{GeoRegion, SwarmJobInfo};
use super::profile::NodeType;
use super::reputation::Badge;
use super::types::NodeId;

// ============================================================================
// Constants
// ============================================================================

/// Default maximum tasks per node per hour.
const DEFAULT_MAX_TASKS_PER_HOUR: u32 = 100;

/// Default maximum concurrent tasks per node.
const DEFAULT_MAX_CONCURRENT_TASKS: u32 = 16;

/// Default minimum reputation score for accepting work.
const DEFAULT_MIN_REPUTATION: f32 = 0.0;

/// Default maximum collective size.
const DEFAULT_MAX_COLLECTIVE_SIZE: u32 = 20;

/// Default minimum collective size.
const DEFAULT_MIN_COLLECTIVE_SIZE: u32 = 2;

// ============================================================================
// GeoFence
// ============================================================================

/// Geographic restriction for job execution.
///
/// When a policy includes a geo-fence, only nodes whose declared region
/// matches one of the `allowed_regions` may execute the job. If
/// `allowed_regions` is empty, the fence is considered open (no restriction).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeoFence {
    /// Regions where execution is permitted.
    pub allowed_regions: Vec<GeoRegion>,
    /// If true, the geo-fence is enforced; if false, it is advisory only.
    pub enforced: bool,
}

impl GeoFence {
    /// Create a new enforced geo-fence with the given allowed regions.
    pub fn new(allowed_regions: Vec<GeoRegion>) -> Self {
        Self {
            allowed_regions,
            enforced: true,
        }
    }

    /// An open fence that permits all regions.
    pub fn open() -> Self {
        Self {
            allowed_regions: Vec::new(),
            enforced: false,
        }
    }

    /// Check if a given region is permitted by this fence.
    ///
    /// Returns `true` if:
    /// - The fence is not enforced, OR
    /// - `allowed_regions` is empty (no restriction), OR
    /// - The region is in `allowed_regions`.
    pub fn permits_region(&self, region: &GeoRegion) -> bool {
        if !self.enforced || self.allowed_regions.is_empty() {
            return true;
        }
        self.allowed_regions.contains(region)
    }

    /// Check if a posting's data_residency is compatible with this fence.
    ///
    /// If the posting has no data_residency, it is always compatible.
    /// Otherwise the posting's region must be in the allowed set.
    pub fn permits_posting(&self, posting: &SwarmJobInfo) -> bool {
        if !self.enforced || self.allowed_regions.is_empty() {
            return true;
        }
        match &posting.data_residency {
            Some(region) => self.allowed_regions.contains(region),
            None => true,
        }
    }
}

impl Default for GeoFence {
    fn default() -> Self {
        Self::open()
    }
}

// ============================================================================
// ReputationGate
// ============================================================================

/// Minimum reputation requirements for participation.
///
/// Gates can require a minimum reputation score, specific badges,
/// or both. A node that fails to meet either condition is denied.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReputationGate {
    /// Minimum overall reputation score (0.0..1.0).
    pub min_score: f32,
    /// Badges that MUST all be held by the entity.
    pub required_badges: HashSet<Badge>,
    /// If true, the gate is enforced; if false, it is advisory only.
    pub enforced: bool,
}

impl ReputationGate {
    /// Create a new enforced reputation gate.
    pub fn new(min_score: f32, required_badges: HashSet<Badge>) -> Self {
        Self {
            min_score,
            required_badges,
            enforced: true,
        }
    }

    /// An open gate that permits all entities.
    pub fn open() -> Self {
        Self {
            min_score: 0.0,
            required_badges: HashSet::new(),
            enforced: false,
        }
    }

    /// Check if the given score and badges satisfy this gate.
    ///
    /// Returns `true` if:
    /// - The gate is not enforced, OR
    /// - `score >= min_score` AND all `required_badges` are present.
    pub fn permits(&self, score: f32, badges: &HashSet<Badge>) -> bool {
        if !self.enforced {
            return true;
        }
        if score < self.min_score {
            return false;
        }
        self.required_badges.is_subset(badges)
    }
}

impl Default for ReputationGate {
    fn default() -> Self {
        Self::open()
    }
}

// ============================================================================
// RateLimit
// ============================================================================

/// Rate limiting configuration for task submission and execution.
///
/// Limits are specified as maximums per hour. The [`RateLimitTracker`]
/// enforces these using a sliding window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimit {
    /// Maximum number of tasks a node may execute per hour.
    pub max_tasks_per_hour: u32,
    /// Maximum number of tasks executing concurrently.
    pub max_concurrent_tasks: u32,
    /// If true, the rate limit is enforced; if false, it is advisory only.
    pub enforced: bool,
}

impl RateLimit {
    /// Create a new enforced rate limit.
    pub fn new(max_tasks_per_hour: u32, max_concurrent_tasks: u32) -> Self {
        Self {
            max_tasks_per_hour,
            max_concurrent_tasks,
            enforced: true,
        }
    }
}

impl Default for RateLimit {
    fn default() -> Self {
        Self {
            max_tasks_per_hour: DEFAULT_MAX_TASKS_PER_HOUR,
            max_concurrent_tasks: DEFAULT_MAX_CONCURRENT_TASKS,
            enforced: true,
        }
    }
}

// ============================================================================
// NodeTypeRestriction
// ============================================================================

/// Restricts which node types may execute certain kinds of work.
///
/// Contains an allow-list of node types. If the list is empty, all
/// node types are permitted. If non-empty, only listed types may execute.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeTypeRestriction {
    /// Node types that are permitted. Empty = all types allowed.
    pub allowed_types: HashSet<NodeType>,
    /// Node types that are explicitly denied. Takes precedence over allowed.
    pub denied_types: HashSet<NodeType>,
    /// If true, the restriction is enforced.
    pub enforced: bool,
}

impl NodeTypeRestriction {
    /// Create a new enforced restriction with allowed types.
    pub fn allow(types: HashSet<NodeType>) -> Self {
        Self {
            allowed_types: types,
            denied_types: HashSet::new(),
            enforced: true,
        }
    }

    /// Create a new enforced restriction with denied types.
    pub fn deny(types: HashSet<NodeType>) -> Self {
        Self {
            allowed_types: HashSet::new(),
            denied_types: types,
            enforced: true,
        }
    }

    /// An open restriction that permits all node types.
    pub fn open() -> Self {
        Self {
            allowed_types: HashSet::new(),
            denied_types: HashSet::new(),
            enforced: false,
        }
    }

    /// Check if a given node type is permitted.
    ///
    /// Denied types take precedence over allowed types.
    pub fn permits(&self, node_type: &NodeType) -> bool {
        if !self.enforced {
            return true;
        }
        if self.denied_types.contains(node_type) {
            return false;
        }
        if self.allowed_types.is_empty() {
            return true;
        }
        self.allowed_types.contains(node_type)
    }
}

impl Default for NodeTypeRestriction {
    fn default() -> Self {
        Self::open()
    }
}

// ============================================================================
// CollectiveConstraint
// ============================================================================

/// Constraints on collective formation and operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectiveConstraint {
    /// Maximum number of members in a collective.
    pub max_size: u32,
    /// Minimum number of members for a collective to be active.
    pub min_size: u32,
    /// Minimum reputation score for collective membership.
    pub min_member_reputation: f32,
    /// If true, the constraint is enforced.
    pub enforced: bool,
}

impl CollectiveConstraint {
    /// Create a new enforced collective constraint.
    pub fn new(max_size: u32, min_size: u32, min_member_reputation: f32) -> Self {
        Self {
            max_size,
            min_size,
            min_member_reputation,
            enforced: true,
        }
    }

    /// Check if a collective size is within bounds.
    pub fn permits_size(&self, size: u32) -> bool {
        if !self.enforced {
            return true;
        }
        size >= self.min_size && size <= self.max_size
    }

    /// Check if a member reputation score meets the threshold.
    pub fn permits_member(&self, reputation: f32) -> bool {
        if !self.enforced {
            return true;
        }
        reputation >= self.min_member_reputation
    }
}

impl Default for CollectiveConstraint {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_MAX_COLLECTIVE_SIZE,
            min_size: DEFAULT_MIN_COLLECTIVE_SIZE,
            min_member_reputation: 0.3,
            enforced: true,
        }
    }
}

// ============================================================================
// DataResidencyPolicy
// ============================================================================

/// Data residency enforcement for regulatory compliance (LGPD, GDPR, CCPA).
/// When applied, data tagged with a residency region can only be stored,
/// processed, and replicated on nodes within the allowed regions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataResidencyPolicy {
    /// Human-readable name for this policy (e.g., "Brazil LGPD")
    pub name: String,
    /// The region where data originates
    pub data_region: GeoRegion,
    /// Regions where processing and storage are allowed
    /// (typically includes data_region plus any approved transfer destinations)
    pub allowed_regions: Vec<GeoRegion>,
    /// Whether to generate audit log entries for all access checks
    pub audit_enabled: bool,
    /// Whether to hard-enforce (reject) or soft-enforce (warn only)
    pub enforcement_mode: EnforcementMode,
}

impl DataResidencyPolicy {
    /// Check if a node with the given geo_region is permitted by this policy.
    ///
    /// The `node_geo_region` is matched against the policy's allowed_regions
    /// using case-insensitive comparison. If the node has no declared region,
    /// access is denied (we cannot confirm residency compliance).
    pub fn permits_node(&self, node_geo_region: &Option<String>) -> bool {
        match node_geo_region {
            None => false, // Cannot confirm compliance without a declared region
            Some(region) => {
                let region_lower = region.to_lowercase();
                self.allowed_regions.iter().any(|allowed| {
                    match allowed {
                        GeoRegion::US => region_lower == "us" || region_lower.starts_with("us-"),
                        GeoRegion::EU => region_lower == "eu" || region_lower.starts_with("eu-"),
                        GeoRegion::Asia => region_lower == "asia" || region_lower.starts_with("asia-") || region_lower.starts_with("ap-"),
                        GeoRegion::Custom(s) => s.to_lowercase() == region_lower,
                    }
                })
            }
        }
    }

    /// Check if a requester in the given region may access data in this
    /// policy's data_region, returning a detailed verdict.
    pub fn check_access(
        &self,
        requester_region: &Option<String>,
        data_region: &GeoRegion,
    ) -> ResidencyVerdict {
        // Only applies if the data_region matches this policy's data_region
        if *data_region != self.data_region {
            return ResidencyVerdict {
                allowed: true,
                reason: "data region does not match this policy".to_string(),
                policy_name: self.name.clone(),
            };
        }

        let permitted = self.permits_node(requester_region);

        match self.enforcement_mode {
            EnforcementMode::Enforce => {
                if permitted {
                    ResidencyVerdict {
                        allowed: true,
                        reason: format!(
                            "requester region {:?} is in allowed regions for policy '{}'",
                            requester_region, self.name
                        ),
                        policy_name: self.name.clone(),
                    }
                } else {
                    ResidencyVerdict {
                        allowed: false,
                        reason: format!(
                            "requester region {:?} is NOT in allowed regions for policy '{}' (allowed: {:?})",
                            requester_region, self.name, self.allowed_regions
                        ),
                        policy_name: self.name.clone(),
                    }
                }
            }
            EnforcementMode::WarnOnly => {
                if !permitted {
                    warn!(
                        policy = %self.name,
                        requester_region = ?requester_region,
                        "data residency violation (warn-only mode)"
                    );
                }
                ResidencyVerdict {
                    allowed: true,
                    reason: if permitted {
                        format!(
                            "requester region {:?} is in allowed regions for policy '{}'",
                            requester_region, self.name
                        )
                    } else {
                        format!(
                            "requester region {:?} violates policy '{}' (warn-only, not enforced)",
                            requester_region, self.name
                        )
                    },
                    policy_name: self.name.clone(),
                }
            }
        }
    }
}

/// Enforcement mode for data residency policies.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EnforcementMode {
    /// Reject operations that violate residency
    Enforce,
    /// Allow but log warnings
    WarnOnly,
}

/// Result of a residency check — includes the verdict, reason, and
/// which policy produced it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResidencyVerdict {
    pub allowed: bool,
    pub reason: String,
    pub policy_name: String,
}

// ============================================================================
// ResidencyAuditLog
// ============================================================================

/// An action checked against residency policies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResidencyAction {
    BlobFetch,
    BlobStore,
    JobExecution,
    ShardPlacement,
    ShardReplication,
}

impl fmt::Display for ResidencyAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResidencyAction::BlobFetch => write!(f, "blob_fetch"),
            ResidencyAction::BlobStore => write!(f, "blob_store"),
            ResidencyAction::JobExecution => write!(f, "job_execution"),
            ResidencyAction::ShardPlacement => write!(f, "shard_placement"),
            ResidencyAction::ShardReplication => write!(f, "shard_replication"),
        }
    }
}

/// A single residency audit log entry recording an access check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResidencyAuditEntry {
    pub timestamp: DateTime<Utc>,
    pub action: ResidencyAction,
    pub data_id: String,
    pub data_region: GeoRegion,
    pub requester_node: NodeId,
    pub requester_region: Option<String>,
    pub verdict: ResidencyVerdict,
}

/// Thread-safe, bounded audit log for data residency checks.
///
/// Backed by a `VecDeque` with FIFO eviction when the maximum capacity
/// is reached. All operations are protected by a `Mutex`.
pub struct ResidencyAuditLog {
    entries: Mutex<VecDeque<ResidencyAuditEntry>>,
    max_entries: usize,
}

impl ResidencyAuditLog {
    /// Create a new audit log with the given maximum capacity.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(
                max_entries.min(1024), // pre-allocate up to 1024 to avoid huge initial alloc
            )),
            max_entries,
        }
    }

    /// Log a residency audit entry. If the log is at capacity, the oldest
    /// entry is evicted (FIFO).
    pub fn log_entry(&self, entry: ResidencyAuditEntry) {
        let mut entries = self.entries.lock().expect("audit log mutex poisoned");
        if entries.len() >= self.max_entries {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Get audit entries, optionally filtered by timestamp and limited
    /// to a maximum number of results.
    pub fn get_entries(
        &self,
        since: Option<DateTime<Utc>>,
        limit: usize,
    ) -> Vec<ResidencyAuditEntry> {
        let entries = self.entries.lock().expect("audit log mutex poisoned");
        entries
            .iter()
            .filter(|e| {
                if let Some(since_ts) = since {
                    e.timestamp >= since_ts
                } else {
                    true
                }
            })
            .rev() // newest first
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get only entries where the verdict was denied (violations).
    pub fn get_violations(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Vec<ResidencyAuditEntry> {
        let entries = self.entries.lock().expect("audit log mutex poisoned");
        entries
            .iter()
            .filter(|e| {
                let time_ok = if let Some(since_ts) = since {
                    e.timestamp >= since_ts
                } else {
                    true
                };
                time_ok && !e.verdict.allowed
            })
            .rev()
            .cloned()
            .collect()
    }

    /// Current number of entries in the log.
    pub fn len(&self) -> usize {
        self.entries.lock().expect("audit log mutex poisoned").len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ResidencyAuditLog {
    fn default() -> Self {
        Self::new(RESIDENCY_AUDIT_MAX_ENTRIES)
    }
}

// ============================================================================
// PolicyViolation
// ============================================================================

/// A recorded policy violation.
///
/// Violations are stored locally and can be used to update reputation
/// scores. Each violation captures what rule was broken, by whom,
/// and when.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyViolation {
    /// Which rule was violated.
    pub rule: String,
    /// The entity that committed the violation.
    pub violator: NodeId,
    /// Human-readable description of the violation.
    pub description: String,
    /// When the violation was detected.
    pub detected_at: DateTime<Utc>,
    /// The job that triggered the violation, if any.
    pub job_id: Option<JobId>,
    /// Severity: higher = worse.
    pub severity: ViolationSeverity,
}

/// Severity level of a policy violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ViolationSeverity {
    /// Informational, no action required.
    Info,
    /// Warning: may affect reputation if repeated.
    Warning,
    /// Serious: immediate reputation impact.
    Serious,
    /// Critical: immediate blacklisting recommended.
    Critical,
}

impl fmt::Display for ViolationSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViolationSeverity::Info => write!(f, "info"),
            ViolationSeverity::Warning => write!(f, "warning"),
            ViolationSeverity::Serious => write!(f, "serious"),
            ViolationSeverity::Critical => write!(f, "critical"),
        }
    }
}

// ============================================================================
// PolicyCheckResult
// ============================================================================

/// The result of evaluating a policy check.
#[derive(Debug, Clone)]
pub struct PolicyCheckResult {
    /// Whether the action is allowed.
    pub allowed: bool,
    /// Reasons why the action was denied (empty if allowed).
    pub denial_reasons: Vec<String>,
    /// Violations detected during the check (may be non-empty even if allowed,
    /// for advisory-only rules).
    pub violations: Vec<PolicyViolation>,
}

impl PolicyCheckResult {
    /// Create an allowed result with no violations.
    pub fn allowed() -> Self {
        Self {
            allowed: true,
            denial_reasons: Vec::new(),
            violations: Vec::new(),
        }
    }

    /// Create a denied result with a single reason.
    pub fn denied(reason: String) -> Self {
        Self {
            allowed: false,
            denial_reasons: vec![reason],
            violations: Vec::new(),
        }
    }

    /// Create a denied result with a violation attached.
    pub fn denied_with_violation(reason: String, violation: PolicyViolation) -> Self {
        Self {
            allowed: false,
            denial_reasons: vec![reason.clone()],
            violations: vec![violation],
        }
    }

    /// Merge another check result into this one.
    ///
    /// If the other result is denied, this result becomes denied too.
    /// Violations and denial reasons are appended.
    pub fn merge(&mut self, other: PolicyCheckResult) {
        if !other.allowed {
            self.allowed = false;
        }
        self.denial_reasons.extend(other.denial_reasons);
        self.violations.extend(other.violations);
    }
}

// ============================================================================
// PolicySet
// ============================================================================

/// A versioned collection of policy rules for a node or the swarm.
///
/// Policy sets are propagated via gossip. Merging uses version-based
/// conflict resolution: a newer version always replaces an older one.
/// The monotonically increasing version counter prevents rollback attacks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicySet {
    /// The node that authored this policy set.
    pub author: NodeId,
    /// Monotonically increasing version counter.
    pub version: u64,
    /// When this policy set was last updated.
    pub updated_at: DateTime<Utc>,

    // -- Policy rules --
    /// Geographic execution restrictions.
    pub geo_fence: GeoFence,
    /// Reputation requirements for participation.
    pub reputation_gate: ReputationGate,
    /// Rate limiting configuration.
    pub rate_limit: RateLimit,
    /// Node type restrictions.
    pub node_type_restriction: NodeTypeRestriction,
    /// Collective formation constraints.
    pub collective_constraint: CollectiveConstraint,
    /// Explicitly blacklisted nodes (denied all participation).
    pub blacklisted_nodes: HashSet<NodeId>,
    /// Free-form key-value parameters for custom rules.
    pub custom_params: HashMap<String, String>,
    /// Data residency policies for regulatory compliance (LGPD, GDPR, etc.).
    #[serde(default)]
    pub data_residency_policies: Vec<DataResidencyPolicy>,
}

impl PolicySet {
    /// Create an empty policy set for the given node (all rules open/default).
    pub fn empty(author: NodeId) -> Self {
        Self {
            author,
            version: 1,
            updated_at: Utc::now(),
            geo_fence: GeoFence::default(),
            reputation_gate: ReputationGate::default(),
            rate_limit: RateLimit::default(),
            node_type_restriction: NodeTypeRestriction::default(),
            collective_constraint: CollectiveConstraint::default(),
            blacklisted_nodes: HashSet::new(),
            custom_params: HashMap::new(),
            data_residency_policies: Vec::new(),
        }
    }

    /// Create a default policy set with sensible baseline rules.
    pub fn default_for(author: NodeId) -> Self {
        Self {
            author,
            version: 1,
            updated_at: Utc::now(),
            geo_fence: GeoFence::default(),
            reputation_gate: ReputationGate {
                min_score: DEFAULT_MIN_REPUTATION,
                required_badges: HashSet::new(),
                enforced: true,
            },
            rate_limit: RateLimit::default(),
            node_type_restriction: NodeTypeRestriction::default(),
            collective_constraint: CollectiveConstraint::default(),
            blacklisted_nodes: HashSet::new(),
            custom_params: HashMap::new(),
            data_residency_policies: Vec::new(),
        }
    }

    /// Bump the version and update the timestamp.
    pub fn touch(&mut self) {
        self.version += 1;
        self.updated_at = Utc::now();
    }

    /// Check if a node is blacklisted.
    pub fn is_blacklisted(&self, node_id: &NodeId) -> bool {
        self.blacklisted_nodes.contains(node_id)
    }

    /// Add a node to the blacklist and bump the version.
    pub fn blacklist(&mut self, node_id: NodeId) {
        self.blacklisted_nodes.insert(node_id);
        self.touch();
    }

    /// Remove a node from the blacklist and bump the version.
    pub fn unblacklist(&mut self, node_id: &NodeId) {
        self.blacklisted_nodes.remove(node_id);
        self.touch();
    }

    /// Get a custom parameter value.
    pub fn get_param(&self, key: &str) -> Option<&String> {
        self.custom_params.get(key)
    }

    /// Set a custom parameter and bump the version.
    pub fn set_param(&mut self, key: String, value: String) {
        self.custom_params.insert(key, value);
        self.touch();
    }

    /// Number of blacklisted nodes.
    pub fn blacklist_count(&self) -> usize {
        self.blacklisted_nodes.len()
    }
}

impl Default for PolicySet {
    fn default() -> Self {
        Self::empty(NodeId::new())
    }
}

// ============================================================================
// RateLimitTracker
// ============================================================================

/// Sliding window rate limit tracker.
///
/// Tracks task execution timestamps per node using a sliding window of
/// one hour. Provides both per-hour and concurrency checks against the
/// configured [`RateLimit`].
pub struct RateLimitTracker {
    /// Per-node task timestamps within the sliding window.
    windows: DashMap<NodeId, Vec<DateTime<Utc>>>,
    /// Per-node current concurrent task count.
    concurrent: DashMap<NodeId, u32>,
}

impl RateLimitTracker {
    /// Create a new rate limit tracker.
    pub fn new() -> Self {
        Self {
            windows: DashMap::new(),
            concurrent: DashMap::new(),
        }
    }

    /// Record a task start for the given node.
    pub fn record_task_start(&self, node_id: &NodeId) {
        self.windows
            .entry(*node_id)
            .or_default()
            .push(Utc::now());
        *self.concurrent.entry(*node_id).or_insert(0) += 1;
    }

    /// Record a task completion for the given node (decrements concurrent count).
    pub fn record_task_end(&self, node_id: &NodeId) {
        if let Some(mut count) = self.concurrent.get_mut(node_id) {
            if *count > 0 {
                *count -= 1;
            }
        }
    }

    /// Check if a node is within the rate limit.
    ///
    /// Returns `true` if the node is allowed to start a new task.
    pub fn check(&self, node_id: &NodeId, limit: &RateLimit) -> bool {
        if !limit.enforced {
            return true;
        }

        // Check concurrent limit
        let current_concurrent = self
            .concurrent
            .get(node_id)
            .map(|v| *v)
            .unwrap_or(0);
        if current_concurrent >= limit.max_concurrent_tasks {
            return false;
        }

        // Check hourly limit
        let tasks_in_window = self.tasks_in_window(node_id);
        tasks_in_window < limit.max_tasks_per_hour
    }

    /// Count tasks within the sliding window (last hour) for a node.
    pub fn tasks_in_window(&self, node_id: &NodeId) -> u32 {
        let cutoff = Utc::now() - ChronoDuration::hours(1);
        self.windows
            .get(node_id)
            .map(|timestamps| {
                timestamps
                    .iter()
                    .filter(|ts| **ts >= cutoff)
                    .count() as u32
            })
            .unwrap_or(0)
    }

    /// Get the current concurrent task count for a node.
    pub fn concurrent_count(&self, node_id: &NodeId) -> u32 {
        self.concurrent
            .get(node_id)
            .map(|v| *v)
            .unwrap_or(0)
    }

    /// Prune expired entries from all sliding windows.
    ///
    /// Removes timestamps older than one hour. Returns the number of
    /// entries pruned.
    pub fn prune_expired(&self) -> usize {
        let cutoff = Utc::now() - ChronoDuration::hours(1);
        let mut total_pruned = 0usize;

        let keys: Vec<NodeId> = self.windows.iter().map(|r| *r.key()).collect();
        for key in keys {
            if let Some(mut timestamps) = self.windows.get_mut(&key) {
                let before = timestamps.len();
                timestamps.retain(|ts| *ts >= cutoff);
                total_pruned += before - timestamps.len();
            }
        }

        total_pruned
    }

    /// Number of tracked nodes.
    pub fn tracked_node_count(&self) -> usize {
        self.windows.len()
    }
}

impl Default for RateLimitTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// PolicyEngine
// ============================================================================

/// Local policy enforcement engine.
///
/// Holds the active policy set and provides methods to evaluate whether
/// actions are permitted. The policy set can be updated via gossip merge
/// or local mutation. Combines static policy rules with runtime rate
/// limit tracking.
pub struct PolicyEngine {
    /// The currently active policy set.
    active: RwLock<PolicySet>,
    /// Runtime rate limit tracker.
    rate_tracker: RateLimitTracker,
    /// Recorded violations (bounded, newest first).
    violations: RwLock<Vec<PolicyViolation>>,
    /// Maximum number of violations to retain.
    max_violations: usize,
    /// Audit log for data residency checks.
    residency_audit_log: ResidencyAuditLog,
}

impl PolicyEngine {
    /// Create a new policy engine with the given initial policy set.
    pub fn new(initial: PolicySet) -> Self {
        Self {
            active: RwLock::new(initial),
            rate_tracker: RateLimitTracker::new(),
            violations: RwLock::new(Vec::new()),
            max_violations: 1000,
            residency_audit_log: ResidencyAuditLog::default(),
        }
    }

    /// Create a policy engine with a custom violation retention limit.
    pub fn with_max_violations(mut self, max: usize) -> Self {
        self.max_violations = max;
        self
    }

    /// Get a clone of the active policy set.
    pub fn active_policy(&self) -> PolicySet {
        self.active.read().clone()
    }

    /// Get the current policy version.
    pub fn version(&self) -> u64 {
        self.active.read().version
    }

    /// Merge an incoming policy set (version-based conflict resolution).
    ///
    /// Returns `true` if the incoming policy was accepted (newer version).
    pub fn merge_policy(&self, incoming: PolicySet) -> bool {
        let mut current = self.active.write();
        if incoming.version > current.version {
            debug!(
                old_version = current.version,
                new_version = incoming.version,
                author = %incoming.author,
                "policy: merged newer policy set"
            );
            *current = incoming;
            true
        } else {
            false
        }
    }

    /// Update the active policy set directly (for local policy changes).
    pub fn update(&self, policy: PolicySet) {
        let mut current = self.active.write();
        *current = policy;
    }

    // ====================================================================
    // Policy checks
    // ====================================================================

    /// Check if a node is allowed to participate (not blacklisted).
    pub fn check_node_allowed(&self, node_id: &NodeId) -> PolicyCheckResult {
        let policy = self.active.read();
        if policy.is_blacklisted(node_id) {
            return PolicyCheckResult::denied(format!(
                "node {} is blacklisted",
                node_id
            ));
        }
        PolicyCheckResult::allowed()
    }

    /// Check if a node type is permitted by the current policy.
    pub fn check_node_type(&self, node_type: &NodeType) -> PolicyCheckResult {
        let policy = self.active.read();
        if policy.node_type_restriction.permits(node_type) {
            PolicyCheckResult::allowed()
        } else {
            PolicyCheckResult::denied(format!(
                "node type {} is not permitted by policy",
                node_type
            ))
        }
    }

    /// Check if the given reputation score and badges satisfy the gate.
    pub fn check_reputation(
        &self,
        score: f32,
        badges: &HashSet<Badge>,
    ) -> PolicyCheckResult {
        let policy = self.active.read();
        if policy.reputation_gate.permits(score, badges) {
            PolicyCheckResult::allowed()
        } else {
            let mut reasons = Vec::new();
            if score < policy.reputation_gate.min_score {
                reasons.push(format!(
                    "reputation score {:.2} below minimum {:.2}",
                    score, policy.reputation_gate.min_score
                ));
            }
            if !policy.reputation_gate.required_badges.is_subset(badges) {
                let missing: Vec<_> = policy
                    .reputation_gate
                    .required_badges
                    .difference(badges)
                    .map(|b| format!("{}", b))
                    .collect();
                reasons.push(format!("missing required badges: {}", missing.join(", ")));
            }
            PolicyCheckResult {
                allowed: false,
                denial_reasons: reasons,
                violations: Vec::new(),
            }
        }
    }

    /// Check if a node is within its rate limit and may start a new task.
    pub fn check_rate_limit(&self, node_id: &NodeId) -> PolicyCheckResult {
        let policy = self.active.read();
        if self.rate_tracker.check(node_id, &policy.rate_limit) {
            PolicyCheckResult::allowed()
        } else {
            let current = self.rate_tracker.tasks_in_window(node_id);
            let concurrent = self.rate_tracker.concurrent_count(node_id);
            PolicyCheckResult::denied(format!(
                "rate limit exceeded: {}/{} per hour, {}/{} concurrent",
                current,
                policy.rate_limit.max_tasks_per_hour,
                concurrent,
                policy.rate_limit.max_concurrent_tasks,
            ))
        }
    }

    /// Check if a posting's geo constraint is compatible with our fence.
    pub fn check_geo_fence(&self, posting: &SwarmJobInfo) -> PolicyCheckResult {
        let policy = self.active.read();
        if policy.geo_fence.permits_posting(posting) {
            PolicyCheckResult::allowed()
        } else {
            let region_str = posting
                .data_residency
                .as_ref()
                .map(|r| format!("{:?}", r))
                .unwrap_or_else(|| "none".to_string());
            PolicyCheckResult::denied(format!(
                "posting geo constraint {:?} not permitted by policy (allowed: {:?})",
                region_str, policy.geo_fence.allowed_regions
            ))
        }
    }

    /// Check if a collective size is within policy bounds.
    pub fn check_collective_size(&self, size: u32) -> PolicyCheckResult {
        let policy = self.active.read();
        if policy.collective_constraint.permits_size(size) {
            PolicyCheckResult::allowed()
        } else {
            PolicyCheckResult::denied(format!(
                "collective size {} outside allowed range [{}, {}]",
                size,
                policy.collective_constraint.min_size,
                policy.collective_constraint.max_size,
            ))
        }
    }

    /// Check if a collective member meets the reputation threshold.
    pub fn check_collective_member(&self, reputation: f32) -> PolicyCheckResult {
        let policy = self.active.read();
        if policy.collective_constraint.permits_member(reputation) {
            PolicyCheckResult::allowed()
        } else {
            PolicyCheckResult::denied(format!(
                "member reputation {:.2} below collective minimum {:.2}",
                reputation, policy.collective_constraint.min_member_reputation,
            ))
        }
    }

    /// Comprehensive check: can a node with the given attributes execute
    /// a posting? Combines blacklist, node type, reputation, rate limit,
    /// and geo-fence checks.
    pub fn check_can_execute(
        &self,
        node_id: &NodeId,
        node_type: &NodeType,
        reputation_score: f32,
        badges: &HashSet<Badge>,
        posting: &SwarmJobInfo,
    ) -> PolicyCheckResult {
        let mut result = PolicyCheckResult::allowed();

        result.merge(self.check_node_allowed(node_id));
        if !result.allowed {
            return result;
        }

        result.merge(self.check_node_type(node_type));
        result.merge(self.check_reputation(reputation_score, badges));
        result.merge(self.check_rate_limit(node_id));
        result.merge(self.check_geo_fence(posting));

        result
    }

    // ====================================================================
    // Rate limit delegation
    // ====================================================================

    /// Record a task start for rate limiting purposes.
    pub fn record_task_start(&self, node_id: &NodeId) {
        self.rate_tracker.record_task_start(node_id);
    }

    /// Record a task completion for rate limiting purposes.
    pub fn record_task_end(&self, node_id: &NodeId) {
        self.rate_tracker.record_task_end(node_id);
    }

    /// Get a reference to the rate limit tracker.
    pub fn rate_tracker(&self) -> &RateLimitTracker {
        &self.rate_tracker
    }

    // ====================================================================
    // Data residency checks
    // ====================================================================

    /// Check if a node in the given region may access data tagged with
    /// `data_region`. Evaluates all applicable data residency policies
    /// in the active policy set. If any enforcing policy denies access,
    /// the overall result is denied.
    pub fn check_data_residency(
        &self,
        data_region: &GeoRegion,
        node_region: &Option<String>,
    ) -> ResidencyVerdict {
        let policy = self.active.read();
        let applicable = self.get_residency_policies_for_region_inner(&policy, data_region);

        if applicable.is_empty() {
            return ResidencyVerdict {
                allowed: true,
                reason: "no residency policy applies to this data region".to_string(),
                policy_name: String::new(),
            };
        }

        for drp in &applicable {
            let verdict = drp.check_access(node_region, data_region);
            if !verdict.allowed {
                return verdict;
            }
        }

        ResidencyVerdict {
            allowed: true,
            reason: format!(
                "node region {:?} permitted by all {} applicable residency policies",
                node_region,
                applicable.len()
            ),
            policy_name: applicable
                .first()
                .map(|p| p.name.clone())
                .unwrap_or_default(),
        }
    }

    /// Return all data residency policies that apply to the given data region.
    pub fn get_residency_policies_for_region(
        &self,
        region: &GeoRegion,
    ) -> Vec<DataResidencyPolicy> {
        let policy = self.active.read();
        self.get_residency_policies_for_region_inner(&policy, region)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Internal helper that returns references to applicable policies.
    fn get_residency_policies_for_region_inner<'a>(
        &self,
        policy: &'a PolicySet,
        region: &GeoRegion,
    ) -> Vec<&'a DataResidencyPolicy> {
        policy
            .data_residency_policies
            .iter()
            .filter(|drp| drp.data_region == *region)
            .collect()
    }

    /// Log a residency audit entry and return a reference to the audit log.
    pub fn log_residency_audit(&self, entry: ResidencyAuditEntry) {
        if RESIDENCY_AUDIT_ENABLED {
            info!(
                action = %entry.action,
                data_id = %entry.data_id,
                data_region = ?entry.data_region,
                requester = %entry.requester_node,
                allowed = entry.verdict.allowed,
                "residency audit"
            );
        }
        self.residency_audit_log.log_entry(entry);
    }

    /// Get a reference to the residency audit log.
    pub fn residency_audit_log(&self) -> &ResidencyAuditLog {
        &self.residency_audit_log
    }

    /// Add a data residency policy to the active policy set. Bumps version.
    pub fn add_residency_policy(&self, policy: DataResidencyPolicy) {
        let mut active = self.active.write();
        active.data_residency_policies.push(policy);
        active.touch();
    }

    /// Remove a data residency policy by name. Bumps version if found.
    /// Returns `true` if a policy was removed.
    pub fn remove_residency_policy(&self, name: &str) -> bool {
        let mut active = self.active.write();
        let before = active.data_residency_policies.len();
        active
            .data_residency_policies
            .retain(|p| p.name != name);
        let removed = active.data_residency_policies.len() < before;
        if removed {
            active.touch();
        }
        removed
    }

    /// List all active data residency policies.
    pub fn residency_policies(&self) -> Vec<DataResidencyPolicy> {
        self.active.read().data_residency_policies.clone()
    }

    // ====================================================================
    // Violation tracking
    // ====================================================================

    /// Record a policy violation.
    pub fn record_violation(&self, violation: PolicyViolation) {
        warn!(
            rule = %violation.rule,
            violator = %violation.violator,
            severity = %violation.severity,
            "policy violation recorded"
        );

        let mut violations = self.violations.write();
        violations.push(violation);

        // Trim to max_violations (keep newest)
        if violations.len() > self.max_violations {
            let excess = violations.len() - self.max_violations;
            violations.drain(0..excess);
        }
    }

    /// Get all recorded violations.
    pub fn violations(&self) -> Vec<PolicyViolation> {
        self.violations.read().clone()
    }

    /// Get violations for a specific node.
    pub fn violations_for(&self, node_id: &NodeId) -> Vec<PolicyViolation> {
        self.violations
            .read()
            .iter()
            .filter(|v| v.violator == *node_id)
            .cloned()
            .collect()
    }

    /// Count of violations for a specific node.
    pub fn violation_count_for(&self, node_id: &NodeId) -> usize {
        self.violations
            .read()
            .iter()
            .filter(|v| v.violator == *node_id)
            .count()
    }

    /// Total number of recorded violations.
    pub fn total_violations(&self) -> usize {
        self.violations.read().len()
    }

    /// Clear all recorded violations.
    pub fn clear_violations(&self) {
        self.violations.write().clear();
    }

    // ====================================================================
    // Maintenance
    // ====================================================================

    /// Run periodic maintenance: prune rate limit windows and old violations.
    pub fn maintenance(&self) {
        let pruned = self.rate_tracker.prune_expired();
        if pruned > 0 {
            debug!(pruned, "policy: pruned expired rate limit entries");
        }
    }
}

// ============================================================================
// PolicyStore
// ============================================================================

/// Thread-safe, gossip-mergeable storage for policies across the swarm.
///
/// Stores per-node policies and supports version-based conflict resolution
/// for gossip propagation.
pub struct PolicyStore {
    /// Per-node policy sets, keyed by the authoring node.
    policies: DashMap<NodeId, PolicySet>,
}

impl PolicyStore {
    /// Create a new empty policy store.
    pub fn new() -> Self {
        Self {
            policies: DashMap::new(),
        }
    }

    /// Insert or update a policy set. Newer version wins.
    ///
    /// Returns `true` if the policy was accepted (new entry or newer version).
    pub fn upsert(&self, policy: PolicySet) -> bool {
        let author = policy.author;

        match self.policies.entry(author) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                debug!(
                    author = %author,
                    version = policy.version,
                    "policy store: new policy stored"
                );
                vacant.insert(policy);
                true
            }
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                if policy.version > occupied.get().version {
                    debug!(
                        author = %author,
                        old_version = occupied.get().version,
                        new_version = policy.version,
                        "policy store: updated to newer version"
                    );
                    occupied.insert(policy);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Retrieve a policy set by author node ID.
    pub fn get(&self, author: &NodeId) -> Option<PolicySet> {
        self.policies.get(author).map(|r| r.value().clone())
    }

    /// Remove a policy set.
    pub fn remove(&self, author: &NodeId) {
        self.policies.remove(author);
    }

    /// Return a snapshot of all stored policies.
    pub fn all_policies(&self) -> Vec<PolicySet> {
        self.policies.iter().map(|r| r.value().clone()).collect()
    }

    /// Number of stored policies.
    pub fn count(&self) -> usize {
        self.policies.len()
    }

    /// Get the policy with the highest version among all stored policies.
    ///
    /// Useful for selecting the "winning" policy when multiple nodes
    /// have published overlapping policies.
    pub fn highest_version(&self) -> Option<PolicySet> {
        self.policies
            .iter()
            .max_by_key(|r| r.value().version)
            .map(|r| r.value().clone())
    }

    /// Remove policies whose `updated_at` is older than `max_age`.
    ///
    /// Returns the number of policies removed.
    pub fn prune_stale(&self, max_age: ChronoDuration) -> usize {
        let cutoff = Utc::now() - max_age;

        let stale_authors: Vec<NodeId> = self
            .policies
            .iter()
            .filter(|r| r.value().updated_at < cutoff)
            .map(|r| *r.key())
            .collect();

        let count = stale_authors.len();
        for author in stale_authors {
            self.policies.remove(&author);
        }

        if count > 0 {
            debug!(pruned = count, "policy store: pruned stale policies");
        }

        count
    }
}

impl Default for PolicyStore {
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
    use crate::common::types::JobId;
    use std::time::Duration;

    // -- Helpers --

    fn test_node() -> NodeId {
        NodeId::new()
    }

    fn make_posting() -> SwarmJobInfo {
        use super::super::profile::JobRequirements;
        
        let now = Utc::now();
        SwarmJobInfo {
            job_id: JobId::new(),
            status: crate::swarm::types::SwarmJobStatus::Pending,
            chunks_total: 10,
            chunks_completed: 0,
            chunks_failed: 0,
            submitter: test_node(),
            first_seen: now,
            updated_at: now,
            payload_type: "shell".to_string(),
            total_fuel_consumed: 0,
            final_result: None,
            priority: 1,
            max_duration_ms: Some(60_000),
            verify_mode: None,
            orchestration: Default::default(),
            requirements: JobRequirements::minimal(),
            verification_strategy: Default::default(),
            associated_topology: None,
            max_mmx_per_instruction: None,
            data_residency: None,
        }
    }

    fn make_posting_with_geo(region: GeoRegion) -> SwarmJobInfo {
        let mut posting = make_posting();
        posting.data_residency = Some(region);
        posting
    }

    // ====================================================================
    // GeoFence tests
    // ====================================================================

    #[test]
    fn geo_fence_open_permits_all() {
        let fence = GeoFence::open();
        assert!(fence.permits_region(&GeoRegion::US));
        assert!(fence.permits_region(&GeoRegion::EU));
        assert!(fence.permits_region(&GeoRegion::Asia));
        assert!(fence.permits_region(&GeoRegion::Custom("brazil".into())));
    }

    #[test]
    fn geo_fence_enforced_restricts() {
        let fence = GeoFence::new(vec![GeoRegion::US, GeoRegion::EU]);
        assert!(fence.permits_region(&GeoRegion::US));
        assert!(fence.permits_region(&GeoRegion::EU));
        assert!(!fence.permits_region(&GeoRegion::Asia));
        assert!(!fence.permits_region(&GeoRegion::Custom("brazil".into())));
    }

    #[test]
    fn geo_fence_empty_allowed_permits_all() {
        let fence = GeoFence {
            allowed_regions: Vec::new(),
            enforced: true,
        };
        assert!(fence.permits_region(&GeoRegion::Asia));
    }

    #[test]
    fn geo_fence_permits_posting_no_constraint() {
        let fence = GeoFence::new(vec![GeoRegion::US]);
        let posting = make_posting(); // no data_residency
        assert!(fence.permits_posting(&posting));
    }

    #[test]
    fn geo_fence_permits_posting_matching() {
        let fence = GeoFence::new(vec![GeoRegion::US, GeoRegion::EU]);
        let posting = make_posting_with_geo(GeoRegion::US);
        assert!(fence.permits_posting(&posting));
    }

    #[test]
    fn geo_fence_denies_posting_non_matching() {
        let fence = GeoFence::new(vec![GeoRegion::US]);
        let posting = make_posting_with_geo(GeoRegion::Asia);
        assert!(!fence.permits_posting(&posting));
    }

    #[test]
    fn geo_fence_default_is_open() {
        let fence = GeoFence::default();
        assert!(!fence.enforced);
        assert!(fence.allowed_regions.is_empty());
    }

    // ====================================================================
    // ReputationGate tests
    // ====================================================================

    #[test]
    fn reputation_gate_open_permits_all() {
        let gate = ReputationGate::open();
        assert!(gate.permits(0.0, &HashSet::new()));
        assert!(gate.permits(1.0, &HashSet::new()));
    }

    #[test]
    fn reputation_gate_min_score() {
        let gate = ReputationGate::new(0.5, HashSet::new());
        assert!(gate.permits(0.5, &HashSet::new()));
        assert!(gate.permits(0.8, &HashSet::new()));
        assert!(!gate.permits(0.3, &HashSet::new()));
    }

    #[test]
    fn reputation_gate_required_badges() {
        let mut required = HashSet::new();
        required.insert(Badge::Reliable);
        required.insert(Badge::Trusted);

        let gate = ReputationGate::new(0.0, required);

        let mut has_both = HashSet::new();
        has_both.insert(Badge::Reliable);
        has_both.insert(Badge::Trusted);
        has_both.insert(Badge::Lightning);
        assert!(gate.permits(0.5, &has_both));

        let mut missing_one = HashSet::new();
        missing_one.insert(Badge::Reliable);
        assert!(!gate.permits(0.5, &missing_one));

        assert!(!gate.permits(0.5, &HashSet::new()));
    }

    #[test]
    fn reputation_gate_combined() {
        let mut required = HashSet::new();
        required.insert(Badge::Reliable);
        let gate = ReputationGate::new(0.7, required);

        let mut badges = HashSet::new();
        badges.insert(Badge::Reliable);

        // High enough score + has badge = pass
        assert!(gate.permits(0.8, &badges));
        // Low score + has badge = fail
        assert!(!gate.permits(0.3, &badges));
        // High score + missing badge = fail
        assert!(!gate.permits(0.9, &HashSet::new()));
    }

    // ====================================================================
    // RateLimit tests
    // ====================================================================

    #[test]
    fn rate_limit_default() {
        let rl = RateLimit::default();
        assert_eq!(rl.max_tasks_per_hour, DEFAULT_MAX_TASKS_PER_HOUR);
        assert_eq!(rl.max_concurrent_tasks, DEFAULT_MAX_CONCURRENT_TASKS);
        assert!(rl.enforced);
    }

    #[test]
    fn rate_limit_custom() {
        let rl = RateLimit::new(50, 4);
        assert_eq!(rl.max_tasks_per_hour, 50);
        assert_eq!(rl.max_concurrent_tasks, 4);
    }

    // ====================================================================
    // NodeTypeRestriction tests
    // ====================================================================

    #[test]
    fn node_type_restriction_open() {
        let r = NodeTypeRestriction::open();
        assert!(r.permits(&NodeType::BareMetal));
        assert!(r.permits(&NodeType::Android));
        assert!(r.permits(&NodeType::Browser));
    }

    #[test]
    fn node_type_restriction_allow() {
        let mut allowed = HashSet::new();
        allowed.insert(NodeType::BareMetal);
        allowed.insert(NodeType::CloudVM);
        let r = NodeTypeRestriction::allow(allowed);

        assert!(r.permits(&NodeType::BareMetal));
        assert!(r.permits(&NodeType::CloudVM));
        assert!(!r.permits(&NodeType::Android));
        assert!(!r.permits(&NodeType::Browser));
    }

    #[test]
    fn node_type_restriction_deny() {
        let mut denied = HashSet::new();
        denied.insert(NodeType::Browser);
        denied.insert(NodeType::Android);
        let r = NodeTypeRestriction::deny(denied);

        assert!(r.permits(&NodeType::BareMetal));
        assert!(r.permits(&NodeType::CloudVM));
        assert!(!r.permits(&NodeType::Browser));
        assert!(!r.permits(&NodeType::Android));
    }

    #[test]
    fn node_type_restriction_deny_overrides_allow() {
        let mut allowed = HashSet::new();
        allowed.insert(NodeType::BareMetal);
        allowed.insert(NodeType::Browser);
        let mut denied = HashSet::new();
        denied.insert(NodeType::Browser);

        let r = NodeTypeRestriction {
            allowed_types: allowed,
            denied_types: denied,
            enforced: true,
        };

        assert!(r.permits(&NodeType::BareMetal));
        assert!(!r.permits(&NodeType::Browser)); // denied takes precedence
    }

    // ====================================================================
    // CollectiveConstraint tests
    // ====================================================================

    #[test]
    fn collective_constraint_default() {
        let cc = CollectiveConstraint::default();
        assert_eq!(cc.max_size, DEFAULT_MAX_COLLECTIVE_SIZE);
        assert_eq!(cc.min_size, DEFAULT_MIN_COLLECTIVE_SIZE);
        assert!(cc.enforced);
    }

    #[test]
    fn collective_constraint_permits_size() {
        let cc = CollectiveConstraint::new(10, 2, 0.5);
        assert!(cc.permits_size(2));
        assert!(cc.permits_size(5));
        assert!(cc.permits_size(10));
        assert!(!cc.permits_size(1));
        assert!(!cc.permits_size(11));
    }

    #[test]
    fn collective_constraint_permits_member() {
        let cc = CollectiveConstraint::new(10, 2, 0.5);
        assert!(cc.permits_member(0.5));
        assert!(cc.permits_member(0.9));
        assert!(!cc.permits_member(0.3));
    }

    // ====================================================================
    // PolicyViolation tests
    // ====================================================================

    #[test]
    fn policy_violation_creation() {
        let v = PolicyViolation {
            rule: "rate_limit".to_string(),
            violator: test_node(),
            description: "exceeded hourly limit".to_string(),
            detected_at: Utc::now(),
            job_id: Some(JobId::new()),
            severity: ViolationSeverity::Warning,
        };
        assert_eq!(v.rule, "rate_limit");
        assert_eq!(v.severity, ViolationSeverity::Warning);
    }

    #[test]
    fn violation_severity_display() {
        assert_eq!(ViolationSeverity::Info.to_string(), "info");
        assert_eq!(ViolationSeverity::Warning.to_string(), "warning");
        assert_eq!(ViolationSeverity::Serious.to_string(), "serious");
        assert_eq!(ViolationSeverity::Critical.to_string(), "critical");
    }

    // ====================================================================
    // PolicyCheckResult tests
    // ====================================================================

    #[test]
    fn policy_check_result_allowed() {
        let r = PolicyCheckResult::allowed();
        assert!(r.allowed);
        assert!(r.denial_reasons.is_empty());
        assert!(r.violations.is_empty());
    }

    #[test]
    fn policy_check_result_denied() {
        let r = PolicyCheckResult::denied("test reason".to_string());
        assert!(!r.allowed);
        assert_eq!(r.denial_reasons.len(), 1);
        assert_eq!(r.denial_reasons[0], "test reason");
    }

    #[test]
    fn policy_check_result_merge() {
        let mut r1 = PolicyCheckResult::allowed();
        let r2 = PolicyCheckResult::denied("reason1".to_string());
        let r3 = PolicyCheckResult::denied("reason2".to_string());

        r1.merge(r2);
        assert!(!r1.allowed);
        assert_eq!(r1.denial_reasons.len(), 1);

        r1.merge(r3);
        assert_eq!(r1.denial_reasons.len(), 2);
    }

    #[test]
    fn policy_check_result_merge_allowed() {
        let mut r1 = PolicyCheckResult::allowed();
        let r2 = PolicyCheckResult::allowed();
        r1.merge(r2);
        assert!(r1.allowed);
        assert!(r1.denial_reasons.is_empty());
    }

    // ====================================================================
    // PolicySet tests
    // ====================================================================

    #[test]
    fn policy_set_empty() {
        let node = test_node();
        let ps = PolicySet::empty(node);
        assert_eq!(ps.version, 1);
        assert_eq!(ps.author, node);
        assert!(ps.blacklisted_nodes.is_empty());
        assert!(!ps.geo_fence.enforced);
        assert!(!ps.reputation_gate.enforced);
    }

    #[test]
    fn policy_set_default_for() {
        let node = test_node();
        let ps = PolicySet::default_for(node);
        assert_eq!(ps.version, 1);
        assert!(ps.reputation_gate.enforced);
        assert!(ps.rate_limit.enforced);
    }

    #[test]
    fn policy_set_touch() {
        let mut ps = PolicySet::empty(test_node());
        assert_eq!(ps.version, 1);
        ps.touch();
        assert_eq!(ps.version, 2);
    }

    #[test]
    fn policy_set_blacklist() {
        let mut ps = PolicySet::empty(test_node());
        let bad_node = test_node();

        assert!(!ps.is_blacklisted(&bad_node));
        ps.blacklist(bad_node);
        assert!(ps.is_blacklisted(&bad_node));
        assert_eq!(ps.blacklist_count(), 1);

        ps.unblacklist(&bad_node);
        assert!(!ps.is_blacklisted(&bad_node));
        assert_eq!(ps.blacklist_count(), 0);
    }

    #[test]
    fn policy_set_custom_params() {
        let mut ps = PolicySet::empty(test_node());
        assert!(ps.get_param("key1").is_none());

        ps.set_param("key1".to_string(), "value1".to_string());
        assert_eq!(ps.get_param("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn policy_set_serialization_roundtrip() {
        let mut ps = PolicySet::default_for(test_node());
        ps.geo_fence = GeoFence::new(vec![GeoRegion::US, GeoRegion::EU]);
        ps.blacklist(test_node());
        ps.set_param("max_data_mb".to_string(), "1024".to_string());

        let json = serde_json::to_string(&ps).expect("serialize");
        let deserialized: PolicySet = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.version, ps.version);
        assert_eq!(deserialized.author, ps.author);
        assert_eq!(deserialized.geo_fence.allowed_regions.len(), 2);
        assert_eq!(deserialized.blacklist_count(), 1);
        assert_eq!(
            deserialized.get_param("max_data_mb"),
            Some(&"1024".to_string())
        );
    }

    // ====================================================================
    // RateLimitTracker tests
    // ====================================================================

    #[test]
    fn rate_limit_tracker_empty() {
        let tracker = RateLimitTracker::new();
        let node = test_node();
        let limit = RateLimit::default();

        assert!(tracker.check(&node, &limit));
        assert_eq!(tracker.tasks_in_window(&node), 0);
        assert_eq!(tracker.concurrent_count(&node), 0);
    }

    #[test]
    fn rate_limit_tracker_records_tasks() {
        let tracker = RateLimitTracker::new();
        let node = test_node();

        tracker.record_task_start(&node);
        assert_eq!(tracker.tasks_in_window(&node), 1);
        assert_eq!(tracker.concurrent_count(&node), 1);

        tracker.record_task_start(&node);
        assert_eq!(tracker.tasks_in_window(&node), 2);
        assert_eq!(tracker.concurrent_count(&node), 2);

        tracker.record_task_end(&node);
        assert_eq!(tracker.concurrent_count(&node), 1);
        // Window count stays at 2 (task still within the hour)
        assert_eq!(tracker.tasks_in_window(&node), 2);
    }

    #[test]
    fn rate_limit_tracker_concurrent_limit() {
        let tracker = RateLimitTracker::new();
        let node = test_node();
        let limit = RateLimit::new(1000, 2); // high hourly, low concurrent

        tracker.record_task_start(&node);
        tracker.record_task_start(&node);
        assert!(!tracker.check(&node, &limit)); // at concurrent limit

        tracker.record_task_end(&node);
        assert!(tracker.check(&node, &limit)); // back under limit
    }

    #[test]
    fn rate_limit_tracker_hourly_limit() {
        let tracker = RateLimitTracker::new();
        let node = test_node();
        let limit = RateLimit::new(3, 100); // low hourly, high concurrent

        for _ in 0..3 {
            tracker.record_task_start(&node);
            tracker.record_task_end(&node);
        }
        // 3 tasks in window, concurrent is 0 now
        assert!(!tracker.check(&node, &limit)); // at hourly limit
    }

    #[test]
    fn rate_limit_tracker_unenforced() {
        let tracker = RateLimitTracker::new();
        let node = test_node();
        let limit = RateLimit {
            max_tasks_per_hour: 1,
            max_concurrent_tasks: 1,
            enforced: false,
        };

        tracker.record_task_start(&node);
        tracker.record_task_start(&node);
        // Not enforced, so always passes
        assert!(tracker.check(&node, &limit));
    }

    #[test]
    fn rate_limit_tracker_task_end_no_underflow() {
        let tracker = RateLimitTracker::new();
        let node = test_node();

        // End without start should not underflow
        tracker.record_task_end(&node);
        assert_eq!(tracker.concurrent_count(&node), 0);
    }

    #[test]
    fn rate_limit_tracker_tracked_count() {
        let tracker = RateLimitTracker::new();
        let n1 = test_node();
        let n2 = test_node();

        tracker.record_task_start(&n1);
        tracker.record_task_start(&n2);

        assert_eq!(tracker.tracked_node_count(), 2);
    }

    // ====================================================================
    // PolicyEngine tests
    // ====================================================================

    #[test]
    fn policy_engine_creation() {
        let engine = PolicyEngine::new(PolicySet::empty(test_node()));
        assert_eq!(engine.version(), 1);
        assert_eq!(engine.total_violations(), 0);
    }

    #[test]
    fn policy_engine_merge_newer() {
        let node = test_node();
        let engine = PolicyEngine::new(PolicySet::empty(node));
        assert_eq!(engine.version(), 1);

        let mut newer = PolicySet::default_for(node);
        newer.version = 5;
        assert!(engine.merge_policy(newer));
        assert_eq!(engine.version(), 5);
    }

    #[test]
    fn policy_engine_reject_stale() {
        let node = test_node();
        let mut initial = PolicySet::default_for(node);
        initial.version = 10;
        let engine = PolicyEngine::new(initial);

        let mut stale = PolicySet::empty(node);
        stale.version = 3;
        assert!(!engine.merge_policy(stale));
        assert_eq!(engine.version(), 10);
    }

    #[test]
    fn policy_engine_check_node_allowed() {
        let mut policy = PolicySet::empty(test_node());
        let bad_node = test_node();
        policy.blacklist(bad_node);

        let engine = PolicyEngine::new(policy);

        let result = engine.check_node_allowed(&bad_node);
        assert!(!result.allowed);

        let good_node = test_node();
        let result = engine.check_node_allowed(&good_node);
        assert!(result.allowed);
    }

    #[test]
    fn policy_engine_check_node_type() {
        let mut policy = PolicySet::empty(test_node());
        let mut allowed = HashSet::new();
        allowed.insert(NodeType::BareMetal);
        allowed.insert(NodeType::CloudVM);
        policy.node_type_restriction = NodeTypeRestriction::allow(allowed);

        let engine = PolicyEngine::new(policy);

        assert!(engine.check_node_type(&NodeType::BareMetal).allowed);
        assert!(!engine.check_node_type(&NodeType::Browser).allowed);
    }

    #[test]
    fn policy_engine_check_reputation() {
        let mut policy = PolicySet::empty(test_node());
        let mut required = HashSet::new();
        required.insert(Badge::Reliable);
        policy.reputation_gate = ReputationGate::new(0.5, required);

        let engine = PolicyEngine::new(policy);

        let mut good_badges = HashSet::new();
        good_badges.insert(Badge::Reliable);
        assert!(engine.check_reputation(0.7, &good_badges).allowed);
        assert!(!engine.check_reputation(0.3, &good_badges).allowed);
        assert!(!engine.check_reputation(0.7, &HashSet::new()).allowed);
    }

    #[test]
    fn policy_engine_check_rate_limit() {
        let mut policy = PolicySet::empty(test_node());
        policy.rate_limit = RateLimit::new(2, 1);

        let engine = PolicyEngine::new(policy);
        let node = test_node();

        assert!(engine.check_rate_limit(&node).allowed);

        engine.record_task_start(&node);
        // concurrent = 1, max = 1, should fail
        assert!(!engine.check_rate_limit(&node).allowed);

        engine.record_task_end(&node);
        assert!(engine.check_rate_limit(&node).allowed);
    }

    #[test]
    fn policy_engine_check_geo_fence() {
        let mut policy = PolicySet::empty(test_node());
        policy.geo_fence = GeoFence::new(vec![GeoRegion::US]);

        let engine = PolicyEngine::new(policy);

        let us_posting = make_posting_with_geo(GeoRegion::US);
        assert!(engine.check_geo_fence(&us_posting).allowed);

        let asia_posting = make_posting_with_geo(GeoRegion::Asia);
        assert!(!engine.check_geo_fence(&asia_posting).allowed);

        // No geo constraint = always passes
        let plain_posting = make_posting();
        assert!(engine.check_geo_fence(&plain_posting).allowed);
    }

    #[test]
    fn policy_engine_check_collective_size() {
        let mut policy = PolicySet::empty(test_node());
        policy.collective_constraint = CollectiveConstraint::new(5, 2, 0.5);

        let engine = PolicyEngine::new(policy);

        assert!(engine.check_collective_size(2).allowed);
        assert!(engine.check_collective_size(5).allowed);
        assert!(!engine.check_collective_size(1).allowed);
        assert!(!engine.check_collective_size(6).allowed);
    }

    #[test]
    fn policy_engine_check_collective_member() {
        let mut policy = PolicySet::empty(test_node());
        policy.collective_constraint = CollectiveConstraint::new(10, 2, 0.6);

        let engine = PolicyEngine::new(policy);

        assert!(engine.check_collective_member(0.6).allowed);
        assert!(engine.check_collective_member(0.9).allowed);
        assert!(!engine.check_collective_member(0.5).allowed);
    }

    #[test]
    fn policy_engine_check_can_execute_allowed() {
        let policy = PolicySet::empty(test_node());
        let engine = PolicyEngine::new(policy);

        let node = test_node();
        let posting = make_posting();
        let badges = HashSet::new();

        let result = engine.check_can_execute(
            &node,
            &NodeType::Desktop,
            0.8,
            &badges,
            &posting,
        );
        assert!(result.allowed);
    }

    #[test]
    fn policy_engine_check_can_execute_blacklisted() {
        let mut policy = PolicySet::empty(test_node());
        let bad_node = test_node();
        policy.blacklist(bad_node);

        let engine = PolicyEngine::new(policy);
        let posting = make_posting();

        let result = engine.check_can_execute(
            &bad_node,
            &NodeType::Desktop,
            0.8,
            &HashSet::new(),
            &posting,
        );
        assert!(!result.allowed);
        assert!(result.denial_reasons[0].contains("blacklisted"));
    }

    #[test]
    fn policy_engine_check_can_execute_posting_blacklist() {
        let policy = PolicySet::empty(test_node());
        let engine = PolicyEngine::new(policy);

        let node = test_node();
        let posting = make_posting_with_blacklist(vec![node]);

        let result = engine.check_can_execute(
            &node,
            &NodeType::Desktop,
            0.8,
            &HashSet::new(),
            &posting,
        );
        assert!(!result.allowed);
    }

    #[test]
    fn policy_engine_check_can_execute_posting_min_rep() {
        let policy = PolicySet::empty(test_node());
        let engine = PolicyEngine::new(policy);

        let node = test_node();
        let posting = make_posting_with_min_rep(0.7);

        let result = engine.check_can_execute(
            &node,
            &NodeType::Desktop,
            0.5, // below posting's min
            &HashSet::new(),
            &posting,
        );
        assert!(!result.allowed);
    }

    #[test]
    fn policy_engine_violations() {
        let engine = PolicyEngine::new(PolicySet::empty(test_node()));
        let violator = test_node();

        let v = PolicyViolation {
            rule: "rate_limit".to_string(),
            violator,
            description: "test".to_string(),
            detected_at: Utc::now(),
            job_id: None,
            severity: ViolationSeverity::Warning,
        };

        engine.record_violation(v);
        assert_eq!(engine.total_violations(), 1);
        assert_eq!(engine.violation_count_for(&violator), 1);
        assert_eq!(engine.violations_for(&violator).len(), 1);

        engine.clear_violations();
        assert_eq!(engine.total_violations(), 0);
    }

    #[test]
    fn policy_engine_violation_trimming() {
        let engine = PolicyEngine::new(PolicySet::empty(test_node()))
            .with_max_violations(5);

        let violator = test_node();
        for i in 0..10 {
            engine.record_violation(PolicyViolation {
                rule: format!("rule_{}", i),
                violator,
                description: "test".to_string(),
                detected_at: Utc::now(),
                job_id: None,
                severity: ViolationSeverity::Info,
            });
        }

        assert_eq!(engine.total_violations(), 5);
        // Should keep the newest 5 (rule_5 through rule_9)
        let violations = engine.violations();
        assert_eq!(violations[0].rule, "rule_5");
        assert_eq!(violations[4].rule, "rule_9");
    }

    #[test]
    fn policy_engine_update() {
        let node = test_node();
        let engine = PolicyEngine::new(PolicySet::empty(node));
        assert!(!engine.active_policy().reputation_gate.enforced);

        let mut updated = PolicySet::default_for(node);
        updated.reputation_gate = ReputationGate::new(0.5, HashSet::new());
        engine.update(updated);

        assert!(engine.active_policy().reputation_gate.enforced);
    }

    #[test]
    fn policy_engine_maintenance() {
        let engine = PolicyEngine::new(PolicySet::empty(test_node()));
        let node = test_node();

        // Record some tasks
        engine.record_task_start(&node);
        engine.record_task_end(&node);

        // Maintenance should not panic
        engine.maintenance();
    }

    // ====================================================================
    // PolicyStore tests
    // ====================================================================

    #[test]
    fn policy_store_empty() {
        let store = PolicyStore::new();
        assert_eq!(store.count(), 0);
        assert!(store.all_policies().is_empty());
        assert!(store.highest_version().is_none());
    }

    #[test]
    fn policy_store_upsert_new() {
        let store = PolicyStore::new();
        let policy = PolicySet::empty(test_node());
        assert!(store.upsert(policy));
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn policy_store_upsert_newer_version() {
        let store = PolicyStore::new();
        let node = test_node();

        let p1 = PolicySet::empty(node);
        assert!(store.upsert(p1));

        let mut p2 = PolicySet::empty(node);
        p2.set_param("key".to_string(), "value".to_string());
        p2.version = 5;
        assert!(store.upsert(p2));

        let stored = store.get(&node).unwrap();
        assert_eq!(stored.version, 5);
        assert_eq!(stored.get_param("key"), Some(&"value".to_string()));
    }

    #[test]
    fn policy_store_upsert_stale_rejected() {
        let store = PolicyStore::new();
        let node = test_node();

        let mut p1 = PolicySet::empty(node);
        p1.version = 10;
        store.upsert(p1);

        let mut p2 = PolicySet::empty(node);
        p2.version = 5;
        assert!(!store.upsert(p2));

        assert_eq!(store.get(&node).unwrap().version, 10);
    }

    #[test]
    fn policy_store_remove() {
        let store = PolicyStore::new();
        let node = test_node();

        store.upsert(PolicySet::empty(node));
        assert_eq!(store.count(), 1);

        store.remove(&node);
        assert_eq!(store.count(), 0);
        assert!(store.get(&node).is_none());
    }

    #[test]
    fn policy_store_all_policies() {
        let store = PolicyStore::new();
        for _ in 0..5 {
            store.upsert(PolicySet::empty(test_node()));
        }
        assert_eq!(store.all_policies().len(), 5);
    }

    #[test]
    fn policy_store_highest_version() {
        let store = PolicyStore::new();

        let mut p1 = PolicySet::empty(test_node());
        p1.version = 3;
        store.upsert(p1);

        let mut p2 = PolicySet::empty(test_node());
        p2.version = 7;
        store.upsert(p2);

        let mut p3 = PolicySet::empty(test_node());
        p3.version = 5;
        store.upsert(p3);

        let highest = store.highest_version().unwrap();
        assert_eq!(highest.version, 7);
    }

    #[test]
    fn policy_store_prune_stale() {
        let store = PolicyStore::new();

        // Fresh policy
        store.upsert(PolicySet::empty(test_node()));

        // Stale policy
        let mut stale = PolicySet::empty(test_node());
        stale.updated_at = Utc::now() - ChronoDuration::hours(2);
        store.upsert(stale);

        let pruned = store.prune_stale(ChronoDuration::hours(1));
        assert_eq!(pruned, 1);
        assert_eq!(store.count(), 1);
    }

    // ====================================================================
    // Serialization round-trips
    // ====================================================================

    #[test]
    fn geo_fence_serialization_roundtrip() {
        let fence = GeoFence::new(vec![GeoRegion::US, GeoRegion::Custom("brazil".into())]);
        let json = serde_json::to_string(&fence).unwrap();
        let deserialized: GeoFence = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.allowed_regions.len(), 2);
        assert!(deserialized.enforced);
    }

    #[test]
    fn reputation_gate_serialization_roundtrip() {
        let mut badges = HashSet::new();
        badges.insert(Badge::Reliable);
        badges.insert(Badge::Trusted);
        let gate = ReputationGate::new(0.7, badges);

        let json = serde_json::to_string(&gate).unwrap();
        let deserialized: ReputationGate = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.required_badges.len(), 2);
        assert!((deserialized.min_score - 0.7).abs() < f32::EPSILON);
    }

    #[test]
    fn rate_limit_serialization_roundtrip() {
        let rl = RateLimit::new(50, 4);
        let json = serde_json::to_string(&rl).unwrap();
        let deserialized: RateLimit = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_tasks_per_hour, 50);
        assert_eq!(deserialized.max_concurrent_tasks, 4);
    }

    #[test]
    fn node_type_restriction_serialization_roundtrip() {
        let mut allowed = HashSet::new();
        allowed.insert(NodeType::BareMetal);
        let r = NodeTypeRestriction::allow(allowed);

        let json = serde_json::to_string(&r).unwrap();
        let deserialized: NodeTypeRestriction = serde_json::from_str(&json).unwrap();
        assert!(deserialized.allowed_types.contains(&NodeType::BareMetal));
        assert!(deserialized.enforced);
    }

    #[test]
    fn collective_constraint_serialization_roundtrip() {
        let cc = CollectiveConstraint::new(10, 3, 0.6);
        let json = serde_json::to_string(&cc).unwrap();
        let deserialized: CollectiveConstraint = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_size, 10);
        assert_eq!(deserialized.min_size, 3);
        assert!((deserialized.min_member_reputation - 0.6).abs() < f32::EPSILON);
    }

    #[test]
    fn violation_severity_serialization_roundtrip() {
        for severity in &[
            ViolationSeverity::Info,
            ViolationSeverity::Warning,
            ViolationSeverity::Serious,
            ViolationSeverity::Critical,
        ] {
            let json = serde_json::to_string(severity).unwrap();
            let deserialized: ViolationSeverity = serde_json::from_str(&json).unwrap();
            assert_eq!(*severity, deserialized);
        }
    }
}
