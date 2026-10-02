// Marabunta - Licensed under the MIT License.
//! Role Election System for the Marabunta Swarm.
//!
//! Nodes in the swarm can hold one or more **roles** that determine their
//! responsibilities.  Roles are either pinned by a human operator, preferred
//! by an operator (but subject to autonomous failover), or autonomously
//! elected based on a scoring function that considers hardware, uptime,
//! network centrality, and existing role load.
//!
//! # Design
//!
//! * **SwarmRole** -- six well-known roles (Aggregator, Witness, Gateway,
//!   Relay, Storage, Ephemeral).
//! * **RoleManager** -- owns the `DashMap`-backed assignment and holder
//!   registries.  Provides `assign_role`, `release_role`, conflict checking,
//!   and scoring.
//! * **score_candidate** -- pure function that produces a signed integer
//!   score for a (candidate, role) pair.  Higher is better.
//! * **can_assign** -- conflict checker.  Returns `(allowed, warnings)`.
//!
//! The module is fully self-contained: all types, constants, and tests live
//! here so that concurrent edits to `config.rs` / `types.rs` / `gossip.rs`
//! are not required.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

// ============================================================================
// Config constants (module-local to avoid cross-file conflicts)
// ============================================================================

const ELECTION_CYCLE_INTERVAL_SECS: u64 = 30;
const SCORE_HARDWARE_MATCH: i32 = 10;
const SCORE_DETECTED_SOFTWARE: i32 = 5;
const SCORE_NETWORK_CENTRALITY: i32 = 3;
const SCORE_UPTIME_BONUS: i32 = 2;
const PENALTY_HEAVY_ROLE: i32 = -5;
const PENALTY_UNRELIABLE: i32 = -10;
const MAX_ROLE_WEIGHT_PER_NODE: u32 = 20;
const MIN_ELECTION_SCORE: i32 = 0;

// ============================================================================
// ConflictLevel
// ============================================================================

/// Severity of a role-combination conflict on the same node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConflictLevel {
    /// The two roles are compatible.
    None,
    /// The combination is suboptimal but allowed.
    Warn,
    /// The combination is forbidden.
    Block,
}

impl fmt::Display for ConflictLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConflictLevel::None => write!(f, "None"),
            ConflictLevel::Warn => write!(f, "Warn"),
            ConflictLevel::Block => write!(f, "Block"),
        }
    }
}

// ============================================================================
// RoleRequirements
// ============================================================================

/// Minimum hardware / software requirements for a role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleRequirements {
    pub min_cpu_cores: Option<usize>,
    pub min_ram_mb: Option<u64>,
    pub min_disk_mb: Option<u64>,
    pub needs_public_ip: bool,
    pub preferred_software: Vec<String>,
}

// ============================================================================
// SwarmRole
// ============================================================================

/// The six well-known roles a node can hold within the swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum SwarmRole {
    /// Collects and reassembles chunk results for a job.
    Aggregator,
    /// Attests to audit events and provides corroboration.
    Witness,
    /// Accepts external client connections and routes work into the swarm.
    Gateway,
    /// Bridges nodes behind restrictive NATs.
    Relay,
    /// Persists blobs, checkpoints, and swarm metadata.
    Storage,
    /// Transient node with no special responsibilities.
    #[default]
    Ephemeral,
    /// An authenticated IDE acting as a Super-Peer for topology control and overrides.
    ControlPlaneIde,
}


impl fmt::Display for SwarmRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SwarmRole::Aggregator => write!(f, "Aggregator"),
            SwarmRole::Witness => write!(f, "Witness"),
            SwarmRole::Gateway => write!(f, "Gateway"),
            SwarmRole::Relay => write!(f, "Relay"),
            SwarmRole::Storage => write!(f, "Storage"),
            SwarmRole::Ephemeral => write!(f, "Ephemeral"),
            SwarmRole::ControlPlaneIde => write!(f, "ControlPlaneIde"),
        }
    }
}

impl SwarmRole {
    /// All roles that are subject to election (everything except Ephemeral).
    pub fn electable() -> &'static [SwarmRole] {
        &[
            SwarmRole::Aggregator,
            SwarmRole::Witness,
            SwarmRole::Gateway,
            SwarmRole::Relay,
            SwarmRole::Storage,
            // Note: ControlPlaneIde is not electable autonomously; it requires PKI.
        ]
    }

    /// Numeric weight representing how resource-intensive this role is.
    ///
    /// Used for overcommit detection: the sum of weights across all roles
    /// held by a single node should not exceed [`MAX_ROLE_WEIGHT_PER_NODE`].
    pub fn weight(&self) -> u32 {
        match self {
            SwarmRole::Aggregator => 8,
            SwarmRole::Storage => 6,
            SwarmRole::Gateway => 4,
            SwarmRole::Relay => 3,
            SwarmRole::Witness => 2,
            SwarmRole::Ephemeral => 1,
            SwarmRole::ControlPlaneIde => 0, // Control planes don't consume standard worker capacity
        }
    }

    /// Hardware and software requirements for this role.
    pub fn requirements(&self) -> RoleRequirements {
        match self {
            SwarmRole::Aggregator => RoleRequirements {
                min_cpu_cores: Some(4),
                min_ram_mb: Some(8_192),
                min_disk_mb: Some(10_000),
                needs_public_ip: false,
                preferred_software: vec!["python3".into()],
            },
            SwarmRole::Storage => RoleRequirements {
                min_cpu_cores: Some(2),
                min_ram_mb: Some(4_096),
                min_disk_mb: Some(100_000),
                needs_public_ip: false,
                preferred_software: vec!["sqlite3".into()],
            },
            SwarmRole::Gateway => RoleRequirements {
                min_cpu_cores: Some(2),
                min_ram_mb: Some(2_048),
                min_disk_mb: None,
                needs_public_ip: true,
                preferred_software: vec![],
            },
            SwarmRole::Relay => RoleRequirements {
                min_cpu_cores: Some(2),
                min_ram_mb: Some(1_024),
                min_disk_mb: None,
                needs_public_ip: true,
                preferred_software: vec![],
            },
            SwarmRole::Witness => RoleRequirements {
                min_cpu_cores: None,
                min_ram_mb: Some(512),
                min_disk_mb: None,
                needs_public_ip: false,
                preferred_software: vec![],
            },
            SwarmRole::Ephemeral => RoleRequirements {
                min_cpu_cores: None,
                min_ram_mb: None,
                min_disk_mb: None,
                needs_public_ip: false,
                preferred_software: vec![],
            },
            SwarmRole::ControlPlaneIde => RoleRequirements {
                min_cpu_cores: None,
                min_ram_mb: None,
                min_disk_mb: None,
                needs_public_ip: false,
                preferred_software: vec!["ide-plugin".into()],
            },
        }
    }

    /// Determine the conflict level when `self` and `other` are held by the
    /// same node simultaneously.
    pub fn conflicts_with(&self, other: &SwarmRole) -> ConflictLevel {
        // Ephemeral + any non-Witness = Block
        if (*self == SwarmRole::Ephemeral && *other != SwarmRole::Witness)
            || (*other == SwarmRole::Ephemeral && *self != SwarmRole::Witness)
        {
            // Ephemeral + Ephemeral is technically fine (both are Ephemeral)
            if *self == SwarmRole::Ephemeral && *other == SwarmRole::Ephemeral {
                return ConflictLevel::None;
            }
            return ConflictLevel::Block;
        }

        // Aggregator + Storage = Warn (both are heavy)
        if (*self == SwarmRole::Aggregator && *other == SwarmRole::Storage)
            || (*self == SwarmRole::Storage && *other == SwarmRole::Aggregator)
        {
            return ConflictLevel::Warn;
        }

        ConflictLevel::None
    }
}

// ============================================================================
// AssignmentMode
// ============================================================================

/// How a role was assigned to a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AssignmentMode {
    /// Operator pinned this role; autonomous election will not override it.
    HumanPinned,
    /// Operator preferred this role, but the system may fail over to another
    /// node if this one becomes unavailable.
    HumanPreferred,
    /// Fully autonomous election based on scoring.
    Autonomous,
}

impl fmt::Display for AssignmentMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssignmentMode::HumanPinned => write!(f, "HumanPinned"),
            AssignmentMode::HumanPreferred => write!(f, "HumanPreferred"),
            AssignmentMode::Autonomous => write!(f, "Autonomous"),
        }
    }
}

// ============================================================================
// RoleAssignment
// ============================================================================

/// A single role assigned to a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleAssignment {
    pub node_id: String,
    pub role: SwarmRole,
    pub mode: AssignmentMode,
    pub assigned_at: u64,
    pub score: i32,
    pub overcommit_warning: bool,
}

// ============================================================================
// DashboardRecommendation
// ============================================================================

/// A recommendation surfaced to operators via the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardRecommendation {
    pub role: SwarmRole,
    pub reason: String,
    pub suggested_action: String,
    pub timestamp: u64,
}

// ============================================================================
// Scoring function (pure)
// ============================================================================

/// Score a candidate node for a specific role.
///
/// Higher is better.  The function is deterministic given the same inputs.
///
/// # Scoring breakdown
///
/// | Factor              | Points |
/// |---------------------|--------|
/// | Hardware match      | +10    |
/// | Detected software   | +5     |
/// | Network centrality  | +3     |
/// | Uptime > 1h         | +2     |
/// | Per heavy role held | -5     |
/// | Ephemeral-only flag | -10    |
#[allow(clippy::too_many_arguments)]
pub fn score_candidate(
    node_cpu: usize,
    node_ram_mb: u64,
    node_disk_mb: u64,
    uptime_secs: u64,
    peer_count: usize,
    median_peers: usize,
    current_roles: &[SwarmRole],
    has_preferred_software: bool,
    ephemeral_only: bool,
    role: &SwarmRole,
) -> i32 {
    let mut score: i32 = 0;

    // --- Hardware match ---
    let reqs = role.requirements();
    let cpu_ok = reqs.min_cpu_cores.map_or(true, |min| node_cpu >= min);
    let ram_ok = reqs.min_ram_mb.map_or(true, |min| node_ram_mb >= min);
    let disk_ok = reqs.min_disk_mb.map_or(true, |min| node_disk_mb >= min);
    if cpu_ok && ram_ok && disk_ok {
        score += SCORE_HARDWARE_MATCH;
    }

    // --- Detected software ---
    if has_preferred_software {
        score += SCORE_DETECTED_SOFTWARE;
    }

    // --- Network centrality ---
    if peer_count > median_peers {
        score += SCORE_NETWORK_CENTRALITY;
    }

    // --- Uptime bonus ---
    if uptime_secs > 3600 {
        score += SCORE_UPTIME_BONUS;
    }

    // --- Heavy-role penalty ---
    for r in current_roles {
        if r.weight() >= 6 {
            score += PENALTY_HEAVY_ROLE;
        }
    }

    // --- Unreliable penalty (ephemeral-only nodes) ---
    if ephemeral_only {
        score += PENALTY_UNRELIABLE;
    }

    score
}

// ============================================================================
// Conflict checker
// ============================================================================

/// Check whether `role` can be assigned to `node_id` given current state.
///
/// Returns `(allowed, warnings)`.  When `allowed` is `false` the caller must
/// not proceed; the warnings list describes every issue found (including
/// non-blocking ones).
pub fn can_assign(
    node_id: &str,
    role: &SwarmRole,
    current_assignments: &DashMap<String, Vec<RoleAssignment>>,
) -> (bool, Vec<String>) {
    let mut warnings: Vec<String> = Vec::new();
    let mut blocked = false;

    if let Some(assignments) = current_assignments.get(node_id) {
        let existing_roles: Vec<SwarmRole> = assignments.iter().map(|a| a.role).collect();

        // Check pairwise conflicts
        for existing in &existing_roles {
            let conflict = role.conflicts_with(existing);
            match conflict {
                ConflictLevel::Block => {
                    warnings.push(format!(
                        "role {} conflicts with existing role {} (Block)",
                        role, existing
                    ));
                    blocked = true;
                }
                ConflictLevel::Warn => {
                    warnings.push(format!(
                        "role {} combined with {} may degrade performance (Warn)",
                        role, existing
                    ));
                }
                ConflictLevel::None => {}
            }
        }

        // Duplicate check
        if existing_roles.contains(role) {
            warnings.push(format!("node {} already holds role {}", node_id, role));
            blocked = true;
        }

        // Overcommit check
        let current_weight: u32 = existing_roles.iter().map(|r| r.weight()).sum();
        if current_weight + role.weight() > MAX_ROLE_WEIGHT_PER_NODE {
            warnings.push(format!(
                "total weight {} + {} = {} exceeds max {}",
                current_weight,
                role.weight(),
                current_weight + role.weight(),
                MAX_ROLE_WEIGHT_PER_NODE
            ));
            blocked = true;
        }
    }

    (!blocked, warnings)
}

// ============================================================================
// RoleManager
// ============================================================================

/// Manages role assignments for the local swarm node and the broader swarm.
///
/// Thread-safe via `DashMap` internals. Designed to work with gossip:
/// assignments propagate via the standard gossip message extensions.
pub struct RoleManager {
    /// node_id -> list of role assignments for that node
    pub assignments: Arc<DashMap<String, Vec<RoleAssignment>>>,
    /// role -> the node_id currently holding it (singleton roles)
    pub role_holders: Arc<DashMap<SwarmRole, String>>,
    /// role -> dashboard recommendation for that role
    pub recommendations: Arc<DashMap<SwarmRole, DashboardRecommendation>>,
    /// How frequently autonomous elections run.
    pub election_interval: Duration,
    /// The local node's identifier.
    pub node_id: String,
}

impl RoleManager {
    /// Create a new `RoleManager` for the given local node.
    pub fn new(node_id: String) -> Self {
        Self {
            assignments: Arc::new(DashMap::new()),
            role_holders: Arc::new(DashMap::new()),
            recommendations: Arc::new(DashMap::new()),
            election_interval: Duration::from_secs(ELECTION_CYCLE_INTERVAL_SECS),
            node_id,
        }
    }

    /// Assign a role to a node.
    ///
    /// Runs the conflict checker first.  On success the assignment is stored
    /// in both `assignments` and `role_holders`.  Returns the new
    /// [`RoleAssignment`] or an error describing why the assignment was
    /// refused.
    pub fn assign_role(
        &self,
        node_id: &str,
        role: SwarmRole,
        mode: AssignmentMode,
    ) -> Result<RoleAssignment, String> {
        let (allowed, warnings) = can_assign(node_id, &role, &self.assignments);

        if !allowed {
            return Err(warnings.join("; "));
        }

        for w in &warnings {
            warn!(node = node_id, role = %role, warning = %w, "role assignment warning");
        }

        // Compute score (lightweight — we don't have full hardware info here,
        // so we produce a nominal score of 0 that the election cycle can
        // override later).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Compute overcommit warning
        let current_weight: u32 = self
            .assignments
            .get(node_id)
            .map(|a| a.iter().map(|r| r.role.weight()).sum())
            .unwrap_or(0);
        let overcommit_warning = current_weight + role.weight() > MAX_ROLE_WEIGHT_PER_NODE / 2;

        let assignment = RoleAssignment {
            node_id: node_id.to_string(),
            role,
            mode,
            assigned_at: now,
            score: 0,
            overcommit_warning,
        };

        // Store in assignments
        self.assignments
            .entry(node_id.to_string())
            .or_default()
            .push(assignment.clone());

        // Update role holder (last writer wins for singleton roles)
        self.role_holders
            .insert(role, node_id.to_string());

        debug!(
            node = node_id,
            role = %role,
            mode = %mode,
            overcommit = overcommit_warning,
            "role assigned"
        );

        Ok(assignment)
    }

    /// Release a role from a node.
    ///
    /// Removes the assignment from `assignments` and clears the `role_holders`
    /// entry if the released node was the current holder.  Returns the removed
    /// [`RoleAssignment`] if found, or `None` if the node did not hold this
    /// role.
    pub fn release_role(&self, node_id: &str, role: &SwarmRole) -> Option<RoleAssignment> {
        let mut released: Option<RoleAssignment> = None;

        if let Some(mut assignments) = self.assignments.get_mut(node_id) {
            if let Some(pos) = assignments.iter().position(|a| a.role == *role) {
                released = Some(assignments.remove(pos));
            }
        }

        // Clear role holder if this node was the holder
        if let Some(holder) = self.role_holders.get(role) {
            if holder.value() == node_id {
                drop(holder);
                self.role_holders.remove(role);
            }
        }

        if let Some(ref a) = released {
            debug!(
                node = node_id,
                role = %a.role,
                "role released"
            );
        }

        released
    }

    /// Get all role assignments across all nodes, flattened.
    pub fn get_assignments(&self) -> Vec<RoleAssignment> {
        self.assignments
            .iter()
            .flat_map(|entry| entry.value().clone())
            .collect()
    }

    /// Get the node currently holding a specific role (if any).
    pub fn get_role_holder(&self, role: &SwarmRole) -> Option<String> {
        self.role_holders.get(role).map(|r| r.value().clone())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Scoring tests -------------------------------------------------------

    #[test]
    fn test_score_hardware_match() {
        // Node meets Aggregator requirements: 4+ cores, 8192+ MB RAM, 10000+ disk
        let score = score_candidate(
            8,       // cpu
            16_384,  // ram
            50_000,  // disk
            7200,    // uptime
            5,       // peers
            3,       // median
            &[],     // current_roles
            false,   // software
            false,   // ephemeral_only
            &SwarmRole::Aggregator,
        );
        // Should include: +10 hardware, +3 centrality, +2 uptime = 15
        assert!(
            score >= SCORE_HARDWARE_MATCH,
            "score {} should include hardware bonus {}",
            score,
            SCORE_HARDWARE_MATCH
        );
    }

    #[test]
    fn test_score_detected_software() {
        let with_sw = score_candidate(
            4, 8_192, 50_000, 7200, 5, 3, &[], true, false, &SwarmRole::Aggregator,
        );
        let without_sw = score_candidate(
            4, 8_192, 50_000, 7200, 5, 3, &[], false, false, &SwarmRole::Aggregator,
        );
        assert_eq!(
            with_sw - without_sw,
            SCORE_DETECTED_SOFTWARE,
            "software bonus should be exactly {}",
            SCORE_DETECTED_SOFTWARE
        );
    }

    #[test]
    fn test_score_heavy_role_penalty() {
        // Already holds Aggregator (weight=8 >=6) and Storage (weight=6 >=6)
        let score = score_candidate(
            4,
            8_192,
            50_000,
            7200,
            5,
            3,
            &[SwarmRole::Aggregator, SwarmRole::Storage],
            false,
            false,
            &SwarmRole::Witness,
        );
        // Witness has no hardware req for CPU, ram >=512 ok, no disk req
        // +10 hw + 3 centrality + 2 uptime - 5 (Aggregator) - 5 (Storage) = 5
        let base = score_candidate(
            4, 8_192, 50_000, 7200, 5, 3, &[], false, false, &SwarmRole::Witness,
        );
        assert_eq!(
            score,
            base + 2 * PENALTY_HEAVY_ROLE,
            "two heavy roles should apply penalty twice"
        );
    }

    #[test]
    fn test_score_unreliable_penalty() {
        let reliable = score_candidate(
            4, 8_192, 50_000, 7200, 5, 3, &[], false, false, &SwarmRole::Gateway,
        );
        let unreliable = score_candidate(
            4, 8_192, 50_000, 7200, 5, 3, &[], false, true, &SwarmRole::Gateway,
        );
        assert_eq!(
            reliable - unreliable,
            -PENALTY_UNRELIABLE,
            "ephemeral_only penalty should be {}",
            PENALTY_UNRELIABLE
        );
    }

    // -- Conflict tests ------------------------------------------------------

    #[test]
    fn test_conflict_block() {
        // Ephemeral + Aggregator = Block
        assert_eq!(
            SwarmRole::Ephemeral.conflicts_with(&SwarmRole::Aggregator),
            ConflictLevel::Block
        );
        // Ephemeral + Storage = Block
        assert_eq!(
            SwarmRole::Ephemeral.conflicts_with(&SwarmRole::Storage),
            ConflictLevel::Block
        );
        // Ephemeral + Gateway = Block
        assert_eq!(
            SwarmRole::Ephemeral.conflicts_with(&SwarmRole::Gateway),
            ConflictLevel::Block
        );
        // Ephemeral + Relay = Block
        assert_eq!(
            SwarmRole::Ephemeral.conflicts_with(&SwarmRole::Relay),
            ConflictLevel::Block
        );
        // Ephemeral + Ephemeral = None (same role, not a conflict)
        assert_eq!(
            SwarmRole::Ephemeral.conflicts_with(&SwarmRole::Ephemeral),
            ConflictLevel::None
        );
    }

    #[test]
    fn test_conflict_warn() {
        assert_eq!(
            SwarmRole::Aggregator.conflicts_with(&SwarmRole::Storage),
            ConflictLevel::Warn
        );
        assert_eq!(
            SwarmRole::Storage.conflicts_with(&SwarmRole::Aggregator),
            ConflictLevel::Warn
        );
        // Non-conflicting pair
        assert_eq!(
            SwarmRole::Aggregator.conflicts_with(&SwarmRole::Witness),
            ConflictLevel::None
        );
    }

    // -- Election tests ------------------------------------------------------

    #[test]
    fn test_election_selects_highest_scorer() {
        // Simulate scoring three candidates for the Gateway role
        let score_a = score_candidate(
            8, 16_384, 100_000, 7200, 10, 5, &[], true, false, &SwarmRole::Gateway,
        );
        let score_b = score_candidate(
            2, 2_048, 10_000, 3600, 3, 5, &[], false, false, &SwarmRole::Gateway,
        );
        let score_c = score_candidate(
            1, 512, 1_000, 100, 1, 5, &[], false, true, &SwarmRole::Gateway,
        );

        let mut candidates = vec![
            ("node-a", score_a),
            ("node-b", score_b),
            ("node-c", score_c),
        ];
        candidates.sort_by(|a, b| b.1.cmp(&a.1));

        assert_eq!(
            candidates[0].0, "node-a",
            "highest-scored candidate should win"
        );
        assert!(
            score_a > score_b && score_b > score_c,
            "scores: a={}, b={}, c={}",
            score_a,
            score_b,
            score_c
        );
    }

    #[test]
    fn test_election_tiebreak_by_node_id() {
        // Two identical candidates -- tiebreak by lexicographic node_id
        let score_a = score_candidate(
            4, 8_192, 50_000, 7200, 5, 3, &[], false, false, &SwarmRole::Witness,
        );
        let score_b = score_candidate(
            4, 8_192, 50_000, 7200, 5, 3, &[], false, false, &SwarmRole::Witness,
        );
        assert_eq!(score_a, score_b, "tied scores");

        let mut candidates = vec![("node-beta", score_a), ("node-alpha", score_b)];
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

        assert_eq!(
            candidates[0].0, "node-alpha",
            "alphabetically earlier node wins the tiebreak"
        );
    }

    // -- Assignment mode tests -----------------------------------------------

    #[test]
    fn test_human_pinned_override() {
        let mgr = RoleManager::new("local".to_string());

        // Autonomous assignment
        let auto = mgr
            .assign_role("node-a", SwarmRole::Gateway, AssignmentMode::Autonomous)
            .unwrap();
        assert_eq!(auto.mode, AssignmentMode::Autonomous);

        // Human pinned to a different node -- should succeed
        let pinned = mgr
            .assign_role("node-b", SwarmRole::Gateway, AssignmentMode::HumanPinned)
            .unwrap();
        assert_eq!(pinned.mode, AssignmentMode::HumanPinned);

        // Both are now recorded; role_holder points at the last writer
        assert_eq!(
            mgr.get_role_holder(&SwarmRole::Gateway).unwrap(),
            "node-b"
        );
    }

    #[test]
    fn test_human_preferred_failover() {
        let mgr = RoleManager::new("local".to_string());

        // Human-preferred assignment
        let preferred = mgr
            .assign_role("node-a", SwarmRole::Relay, AssignmentMode::HumanPreferred)
            .unwrap();
        assert_eq!(preferred.mode, AssignmentMode::HumanPreferred);

        // Simulate failover: release node-a, auto-assign to node-b
        let released = mgr.release_role("node-a", &SwarmRole::Relay);
        assert!(released.is_some());
        assert!(mgr.get_role_holder(&SwarmRole::Relay).is_none());

        let failover = mgr
            .assign_role("node-b", SwarmRole::Relay, AssignmentMode::Autonomous)
            .unwrap();
        assert_eq!(failover.mode, AssignmentMode::Autonomous);
        assert_eq!(
            mgr.get_role_holder(&SwarmRole::Relay).unwrap(),
            "node-b"
        );
    }

    // -- Multi-role / overcommit tests ---------------------------------------

    #[test]
    fn test_multi_role_assignment() {
        let mgr = RoleManager::new("local".to_string());

        // Assign Witness + Relay to same node -- compatible, light weight
        mgr.assign_role("node-a", SwarmRole::Witness, AssignmentMode::Autonomous)
            .unwrap();
        mgr.assign_role("node-a", SwarmRole::Relay, AssignmentMode::Autonomous)
            .unwrap();

        let assignments = mgr.get_assignments();
        let node_a_roles: Vec<SwarmRole> = assignments
            .iter()
            .filter(|a| a.node_id == "node-a")
            .map(|a| a.role)
            .collect();
        assert_eq!(node_a_roles.len(), 2);
        assert!(node_a_roles.contains(&SwarmRole::Witness));
        assert!(node_a_roles.contains(&SwarmRole::Relay));
    }

    #[test]
    fn test_dashboard_recommendation() {
        let mgr = RoleManager::new("local".to_string());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let rec = DashboardRecommendation {
            role: SwarmRole::Storage,
            reason: "No node currently holds the Storage role".to_string(),
            suggested_action: "Assign a node with >100 GB disk".to_string(),
            timestamp: now,
        };

        mgr.recommendations.insert(SwarmRole::Storage, rec.clone());

        let stored = mgr.recommendations.get(&SwarmRole::Storage).unwrap();
        assert_eq!(stored.role, SwarmRole::Storage);
        assert!(stored.reason.contains("No node"));
        assert!(stored.suggested_action.contains("100 GB"));
    }

    #[test]
    fn test_role_release_gossip() {
        let mgr = RoleManager::new("local".to_string());

        // Assign and then release
        mgr.assign_role("node-a", SwarmRole::Aggregator, AssignmentMode::Autonomous)
            .unwrap();
        assert!(mgr.get_role_holder(&SwarmRole::Aggregator).is_some());

        let released = mgr.release_role("node-a", &SwarmRole::Aggregator);
        assert!(released.is_some());
        assert_eq!(released.unwrap().role, SwarmRole::Aggregator);

        // Holder should be cleared
        assert!(mgr.get_role_holder(&SwarmRole::Aggregator).is_none());

        // Assignments list should be empty for node-a
        let node_a_count = mgr
            .assignments
            .get("node-a")
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(node_a_count, 0);
    }

    #[test]
    fn test_overcommit_warning() {
        let mgr = RoleManager::new("local".to_string());

        // Aggregator (8) + Storage (6) = 14 which is > MAX/2 (10)
        // so the second assignment should get an overcommit warning.
        mgr.assign_role("node-a", SwarmRole::Aggregator, AssignmentMode::Autonomous)
            .unwrap();
        let storage = mgr
            .assign_role("node-a", SwarmRole::Storage, AssignmentMode::Autonomous);

        // Aggregator+Storage triggers a Warn conflict, not a Block, so it should succeed.
        // But verify overcommit_warning is set.
        match storage {
            Ok(a) => {
                assert!(
                    a.overcommit_warning,
                    "overcommit_warning should be true: weight={} + {}={}",
                    SwarmRole::Aggregator.weight(),
                    SwarmRole::Storage.weight(),
                    SwarmRole::Aggregator.weight() + SwarmRole::Storage.weight()
                );
            }
            Err(e) => {
                // If it was blocked due to weight, that's also acceptable
                assert!(
                    e.contains("weight") || e.contains("exceeds"),
                    "unexpected error: {}",
                    e
                );
            }
        }
    }
}
