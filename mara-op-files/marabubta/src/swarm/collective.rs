// Marabunta - Licensed under the MIT License.
//! Self-organizing collectives for the organic swarm.
//!
//! A **collective** is a self-formed group of swarm nodes whose combined
//! capabilities exceed what any single member can provide. Nodes with
//! complementary strengths (e.g. one has a GPU, another has large RAM)
//! discover each other through gossip, negotiate formation via proposals
//! and acknowledgements, and then advertise themselves as a single
//! composite entity that can accept jobs no individual member could handle.
//!
//! Collectives are fully decentralized: any node can propose one, members
//! vote on ejections, and the group dissolves automatically when it drops
//! below two members or its success rate collapses.
//!
//! # Lifecycle
//!
//! ```text
//!   Forming  -->  Active  -->  Degraded  -->  Dissolving  -->  Dissolved
//!                  ^  |            |
//!                  |  +--- (member replaced) --+
//!                  +---------------------------+
//! ```

use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info};
use uuid::Uuid;

use super::knowledge::KnowledgeStore;
use super::profile::{JobRequirements, NodeProfile, ProfileStore, Runtime};
use super::types::{NodeId, SwarmMessage, Trait};

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of collectives a single node may belong to simultaneously.
const MAX_COLLECTIVES_PER_NODE: usize = 3;

/// Interval for the collective management background loop.
const COLLECTIVE_LOOP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Maximum age for a pending proposal before it is pruned.
const PROPOSAL_MAX_AGE_SECS: i64 = 60;

/// Minimum members required to keep a collective alive.
const MIN_COLLECTIVE_SIZE: usize = 2;

/// Success rate below which a collective should dissolve.
const MIN_SUCCESS_RATE: f32 = 0.3;

/// Minimum number of jobs before success rate is evaluated.
const MIN_JOBS_FOR_RATE: u64 = 5;

// ============================================================================
// CollectiveId
// ============================================================================

/// Unique identifier for a collective.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CollectiveId(pub Uuid);

impl CollectiveId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for CollectiveId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CollectiveId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = format!("{:x}", self.0.as_u128());
        let short = if hex.len() >= 4 { &hex[..4] } else { &hex };
        write!(f, "{}", short)
    }
}

// ============================================================================
// CompositeProfile
// ============================================================================

/// Aggregate capability profile of a collective's combined members.
///
/// Resources are summed (CPU cores, RAM, disk), runtimes and specializations
/// are unioned, and availability takes the minimum across all members
/// (weakest-link model).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeProfile {
    pub total_cpu_cores: u32,
    pub total_ram_mb: u64,
    pub total_disk_mb: u64,
    pub gpu_cores: Option<u32>,
    pub runtimes: HashSet<Runtime>,
    pub specializations: HashSet<String>,
    /// Weakest-link availability across all members (0.0..1.0).
    pub min_availability: f32,
    pub combined_traits: HashSet<Trait>,
}

impl Default for CompositeProfile {
    fn default() -> Self {
        Self {
            total_cpu_cores: 0,
            total_ram_mb: 0,
            total_disk_mb: 0,
            gpu_cores: None,
            runtimes: HashSet::new(),
            specializations: HashSet::new(),
            min_availability: 1.0,
            combined_traits: HashSet::new(),
        }
    }
}

impl CompositeProfile {
    /// Build a composite profile from a slice of member profiles.
    pub fn from_members(profiles: &[NodeProfile]) -> Self {
        let mut composite = CompositeProfile::default();

        if profiles.is_empty() {
            return composite;
        }

        for profile in profiles {
            composite.total_cpu_cores += profile.cpu_cores;
            composite.total_ram_mb += profile.ram_total_mb;
            composite.total_disk_mb += profile.disk_available_mb;

            // Merge GPU cores: sum if both have GPUs.
            if let Some(gpu) = &profile.gpu {
                let existing = composite.gpu_cores.unwrap_or(0);
                composite.gpu_cores = Some(existing + gpu.cuda_cores.unwrap_or(0));
            }

            composite.runtimes.extend(profile.runtimes.iter().cloned());
            composite
                .specializations
                .extend(profile.specializations.iter().cloned());

            // Availability window: use is_available_now() as a proxy for
            // the weakest-link model. If a member is not currently available,
            // lower the composite availability.
            if !profile.availability_window.is_available_now() {
                composite.min_availability = 0.0;
            }

            // Merge traits from the profile's strengths into the combined
            // trait set. Map each Strength variant to a Trait if applicable.
            for _strength in &profile.strengths {
                // Strengths don't map 1:1 to Trait, but we keep CanExecute
                // as the baseline trait contributed by every member.
                composite.combined_traits.insert(Trait::CanExecute);
            }
        }

        composite
    }

    /// Check if this composite profile can satisfy the given job requirements.
    pub fn satisfies(&self, requirements: &JobRequirements) -> bool {
        // Check CPU
        if let Some(min_cpu) = requirements.min_cpu_cores {
            if self.total_cpu_cores < min_cpu {
                return false;
            }
        }

        // Check RAM
        if let Some(min_ram) = requirements.min_memory_mb {
            if self.total_ram_mb < min_ram {
                return false;
            }
        }

        // Check disk
        if let Some(min_disk) = requirements.min_disk_mb {
            if self.total_disk_mb < min_disk {
                return false;
            }
        }

        // Check GPU
        if requirements.needs_gpu
            && (self.gpu_cores.is_none() || self.gpu_cores == Some(0)) {
                return false;
            }

        // Check runtimes: all required runtimes must be present.
        for rt in &requirements.required_runtimes {
            if !self.runtimes.contains(rt) {
                return false;
            }
        }

        true
    }

    /// Compute a capability match score (0.0 - 1.0) against requirements.
    ///
    /// Higher scores indicate a better match. The score is the average of
    /// individual dimension scores (CPU, RAM, disk, runtime coverage, GPU).
    pub fn capability_match(&self, requirements: &JobRequirements) -> f32 {
        let mut scores: Vec<f32> = Vec::new();

        // CPU score
        if let Some(min_cpu) = requirements.min_cpu_cores {
            if min_cpu > 0 {
                let ratio = self.total_cpu_cores as f32 / min_cpu as f32;
                scores.push(ratio.min(1.0));
            } else {
                scores.push(1.0);
            }
        }

        // RAM score
        if let Some(min_ram) = requirements.min_memory_mb {
            if min_ram > 0 {
                let ratio = self.total_ram_mb as f32 / min_ram as f32;
                scores.push(ratio.min(1.0));
            } else {
                scores.push(1.0);
            }
        }

        // Disk score
        if let Some(min_disk) = requirements.min_disk_mb {
            if min_disk > 0 {
                let ratio = self.total_disk_mb as f32 / min_disk as f32;
                scores.push(ratio.min(1.0));
            } else {
                scores.push(1.0);
            }
        }

        // Runtime score: fraction of required runtimes that are available.
        if !requirements.required_runtimes.is_empty() {
            let matched = requirements
                .required_runtimes
                .iter()
                .filter(|rt| self.runtimes.contains(rt))
                .count();
            scores.push(matched as f32 / requirements.required_runtimes.len() as f32);
        }

        // GPU score
        if requirements.needs_gpu {
            if let Some(cores) = self.gpu_cores {
                if cores > 0 {
                    scores.push(1.0);
                } else {
                    scores.push(0.0);
                }
            } else {
                scores.push(0.0);
            }
        }

        if scores.is_empty() {
            // No requirements specified; everything matches perfectly.
            return 1.0;
        }

        scores.iter().sum::<f32>() / scores.len() as f32
    }
}

// ============================================================================
// MemberRole
// ============================================================================

/// Role of a node within a collective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberRole {
    /// Proposed the collective and is coordinating formation.
    Initiator,
    /// Accepted member, fully participating.
    Member,
    /// Proposed but not yet acknowledged.
    Pending,
}

// ============================================================================
// CollectiveMember
// ============================================================================

/// A single member of a collective, with its role and contribution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectiveMember {
    pub node_id: NodeId,
    pub role: MemberRole,
    pub joined_at: DateTime<Utc>,
    /// What traits this member brings to the collective.
    pub contribution: HashSet<Trait>,
}

// ============================================================================
// CollectiveState
// ============================================================================

/// Lifecycle state of a collective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectiveState {
    /// Proposal sent, waiting for acknowledgements from invited nodes.
    Forming,
    /// Fully formed, accepting work.
    Active,
    /// Lost a member, seeking replacement.
    Degraded,
    /// Disbanding.
    Dissolving,
    /// Final state: the collective no longer exists.
    Dissolved,
}

// ============================================================================
// Collective
// ============================================================================

/// A self-organized group of swarm nodes acting as a single composite entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collective {
    pub id: CollectiveId,
    pub members: Vec<CollectiveMember>,
    pub composite: CompositeProfile,
    pub state: CollectiveState,
    pub formed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: u64,

    /// Descriptors of what the group can handle (e.g. "gpu_compute", "ml_training").
    pub can_handle: Vec<String>,
    pub max_parallel_jobs: u32,
    /// Estimated throughput in jobs per hour, updated from track record.
    pub estimated_throughput: f64,

    // Performance tracking
    pub jobs_completed: u64,
    pub jobs_failed: u64,

    /// Minimum reputation score required for membership.
    pub min_member_reputation: f32,
}

impl Collective {
    /// Create a new collective with the given initiator.
    pub fn new(initiator: NodeId, initiator_profile: &NodeProfile) -> Self {
        let id = CollectiveId::new();
        let now = Utc::now();

        let mut contribution = HashSet::new();
        contribution.insert(Trait::CanExecute);
        let member = CollectiveMember {
            node_id: initiator,
            role: MemberRole::Initiator,
            joined_at: now,
            contribution,
        };

        let composite = CompositeProfile::from_members(&[initiator_profile.clone()]);
        let can_handle: Vec<String> = initiator_profile.specializations.to_vec();

        Self {
            id,
            members: vec![member],
            composite,
            state: CollectiveState::Forming,
            formed_at: now,
            updated_at: now,
            version: 1,
            can_handle,
            max_parallel_jobs: initiator_profile.max_concurrent,
            estimated_throughput: 0.0,
            jobs_completed: 0,
            jobs_failed: 0,
            min_member_reputation: 0.5,
        }
    }

    /// Add a member to the collective and recalculate the composite profile.
    ///
    /// Returns `true` if the member was added, `false` if they are already present.
    pub fn add_member(&mut self, node_id: NodeId, profile: &NodeProfile) -> bool {
        if self.is_member(&node_id) {
            return false;
        }

        let mut contribution = HashSet::new();
        contribution.insert(Trait::CanExecute);
        let member = CollectiveMember {
            node_id,
            role: MemberRole::Member,
            joined_at: Utc::now(),
            contribution,
        };

        self.members.push(member);
        self.max_parallel_jobs += profile.max_concurrent;

        // Extend can_handle with the new member's specializations.
        for spec in &profile.specializations {
            if !self.can_handle.contains(spec) {
                self.can_handle.push(spec.clone());
            }
        }

        // Rebuild composite from scratch using all member profiles is ideal,
        // but we only have the new profile here. We incrementally update.
        self.composite.total_cpu_cores += profile.cpu_cores;
        self.composite.total_ram_mb += profile.ram_total_mb;
        self.composite.total_disk_mb += profile.disk_available_mb;

        if let Some(gpu) = &profile.gpu {
            let existing = self.composite.gpu_cores.unwrap_or(0);
            self.composite.gpu_cores = Some(existing + gpu.cuda_cores.unwrap_or(0));
        }

        self.composite
            .runtimes
            .extend(profile.runtimes.iter().cloned());
        self.composite
            .specializations
            .extend(profile.specializations.iter().cloned());
        self.composite.combined_traits.insert(Trait::CanExecute);

        if !profile.availability_window.is_available_now() {
            self.composite.min_availability = 0.0;
        }

        self.touch();
        true
    }

    /// Remove a member from the collective and recalculate the composite.
    ///
    /// Returns `true` if the member was found and removed.
    pub fn remove_member(&mut self, node_id: &NodeId) -> bool {
        let original_len = self.members.len();
        self.members.retain(|m| m.node_id != *node_id);

        if self.members.len() == original_len {
            return false;
        }

        // After removal, we need to rebuild the composite from remaining members.
        // We cannot rebuild fully without profiles, so we zero the composite and
        // rebuild from member contributions (traits). For numeric fields, we set
        // a flag to require a full recalculation from outside.
        //
        // For now, do a conservative rebuild from member contributions.
        self.composite.combined_traits.clear();
        for member in &self.members {
            self.composite
                .combined_traits
                .extend(member.contribution.iter());
        }

        if self.members.len() < MIN_COLLECTIVE_SIZE {
            self.state = CollectiveState::Dissolving;
        } else if self.state == CollectiveState::Active {
            self.state = CollectiveState::Degraded;
        }

        self.touch();
        true
    }

    /// Check if a node is a member of this collective.
    pub fn is_member(&self, node_id: &NodeId) -> bool {
        self.members.iter().any(|m| m.node_id == *node_id)
    }

    /// Number of active members (non-Pending).
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Recalculate the composite profile from a full set of member profiles.
    ///
    /// This should be called after membership changes when profiles are available.
    pub fn recalculate_composite(&mut self, profiles: &[NodeProfile]) {
        self.composite = CompositeProfile::from_members(profiles);
        self.touch();
    }

    /// Record a successfully completed job.
    pub fn record_job_success(&mut self) {
        self.jobs_completed += 1;
        self.update_throughput();
        self.touch();
    }

    /// Record a failed job.
    pub fn record_job_failure(&mut self) {
        self.jobs_failed += 1;
        self.touch();
    }

    /// Success rate as a fraction (0.0..1.0). Returns 1.0 if no jobs completed yet.
    pub fn success_rate(&self) -> f32 {
        let total = self.jobs_completed + self.jobs_failed;
        if total == 0 {
            return 1.0;
        }
        self.jobs_completed as f32 / total as f32
    }

    /// Check if this collective should dissolve.
    ///
    /// A collective should dissolve if:
    /// - It has fewer than 2 members, OR
    /// - It has completed at least `MIN_JOBS_FOR_RATE` jobs and its success
    ///   rate has dropped below `MIN_SUCCESS_RATE`.
    pub fn should_dissolve(&self) -> bool {
        if self.members.len() < MIN_COLLECTIVE_SIZE {
            return true;
        }

        let total = self.jobs_completed + self.jobs_failed;
        if total >= MIN_JOBS_FOR_RATE && self.success_rate() < MIN_SUCCESS_RATE {
            return true;
        }

        false
    }

    /// Bump the version counter and update timestamp.
    pub fn touch(&mut self) {
        self.version += 1;
        self.updated_at = Utc::now();
    }

    /// Update estimated throughput from job history.
    fn update_throughput(&mut self) {
        let elapsed_hours = (Utc::now() - self.formed_at).num_seconds() as f64 / 3600.0;
        if elapsed_hours > 0.0 {
            self.estimated_throughput = self.jobs_completed as f64 / elapsed_hours;
        }
    }
}

// ============================================================================
// CollectiveProposal
// ============================================================================

/// A proposal to form a new collective, sent by the initiator to invited nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectiveProposal {
    pub collective_id: CollectiveId,
    pub initiator: NodeId,
    pub invited: Vec<NodeId>,
    /// Why these nodes complement each other.
    pub reason: String,
    pub proposed_at: DateTime<Utc>,
}

// ============================================================================
// CollectiveAck
// ============================================================================

/// Acknowledgement (accept/reject) of a collective proposal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectiveAck {
    pub collective_id: CollectiveId,
    pub node_id: NodeId,
    pub accepted: bool,
    pub timestamp: DateTime<Utc>,
}

// ============================================================================
// EjectionProposal
// ============================================================================

/// A proposal to eject a member from a collective.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EjectionProposal {
    pub collective_id: CollectiveId,
    pub proposer: NodeId,
    pub target: NodeId,
    pub reason: String,
    pub proposed_at: DateTime<Utc>,
}

// ============================================================================
// EjectionVote
// ============================================================================

/// A vote on an ejection proposal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EjectionVote {
    pub collective_id: CollectiveId,
    pub voter: NodeId,
    pub target: NodeId,
    pub approve: bool,
    pub timestamp: DateTime<Utc>,
}

// ============================================================================
// AffinityScorer
// ============================================================================

/// Scores how well nodes complement each other for collective formation.
///
/// The scoring philosophy is that nodes with DIFFERENT strengths are more
/// complementary. Two GPU nodes have low complementarity. A GPU node paired
/// with a high-RAM node has high complementarity.
pub struct AffinityScorer;

impl AffinityScorer {
    /// Score how well two nodes complement each other.
    ///
    /// Returns a value in `0.0..1.0` where:
    /// - `0.0` means the nodes are identical (no complementarity)
    /// - `1.0` means they are perfectly complementary
    pub fn complementarity(a: &NodeProfile, b: &NodeProfile) -> f32 {
        let mut dimensions: Vec<f32> = Vec::new();

        // Runtime diversity: fraction of runtimes in the union that are NOT in
        // the intersection (i.e. unique to one node).
        let a_rt: HashSet<_> = a.runtimes.iter().collect();
        let b_rt: HashSet<_> = b.runtimes.iter().collect();
        let union_size = a_rt.union(&b_rt).count();
        let intersection_size = a_rt.intersection(&b_rt).count();
        if union_size > 0 {
            let diversity = (union_size - intersection_size) as f32 / union_size as f32;
            dimensions.push(diversity);
        }

        // Specialization diversity.
        let a_spec: HashSet<&String> = a.specializations.iter().collect();
        let b_spec: HashSet<&String> = b.specializations.iter().collect();
        let spec_union_count = a_spec.union(&b_spec).count();
        let spec_intersection_count = a_spec.intersection(&b_spec).count();
        if spec_union_count > 0 {
            let diversity =
                (spec_union_count - spec_intersection_count) as f32 / spec_union_count as f32;
            dimensions.push(diversity);
        }

        // Strength diversity: different strengths are complementary.
        let a_str: HashSet<_> = a.strengths.iter().collect();
        let b_str: HashSet<_> = b.strengths.iter().collect();
        let str_union: HashSet<_> = a_str.union(&b_str).cloned().collect();
        let str_intersection: HashSet<_> =
            a_str.intersection(&b_str).cloned().collect();
        if !str_union.is_empty() {
            let diversity = (str_union.len() - str_intersection.len()) as f32
                / str_union.len() as f32;
            dimensions.push(diversity);
        }

        // GPU complementarity: one has GPU, other doesn't = high complementarity.
        let a_has_gpu = a.gpu.is_some();
        let b_has_gpu = b.gpu.is_some();
        if a_has_gpu != b_has_gpu {
            dimensions.push(1.0);
        } else {
            dimensions.push(0.0);
        }

        // CPU asymmetry: how different their CPU counts are (normalized).
        let max_cores = a.cpu_cores.max(b.cpu_cores);
        if max_cores > 0 {
            let diff = (a.cpu_cores as f32 - b.cpu_cores as f32).abs() / max_cores as f32;
            dimensions.push(diff * 0.5); // weight CPU asymmetry lower
        }

        // RAM asymmetry.
        let max_ram = a.ram_total_mb.max(b.ram_total_mb);
        if max_ram > 0 {
            let diff = (a.ram_total_mb as f64 - b.ram_total_mb as f64).abs() / max_ram as f64;
            dimensions.push(diff as f32 * 0.5);
        }

        // NodeType diversity: different types are more complementary.
        if a.node_type != b.node_type {
            dimensions.push(0.8);
        } else {
            dimensions.push(0.0);
        }

        if dimensions.is_empty() {
            return 0.0;
        }

        dimensions.iter().sum::<f32>() / dimensions.len() as f32
    }

    /// Find the best N candidates to form a collective with the given node.
    ///
    /// Returns a vector of `(candidate_id, complementarity_score)` sorted by
    /// descending complementarity.
    pub fn find_candidates(
        node: &NodeProfile,
        all_profiles: &[NodeProfile],
        max_candidates: usize,
    ) -> Vec<(NodeId, f32)> {
        let mut candidates: Vec<(NodeId, f32)> = all_profiles
            .iter()
            .filter(|p| p.node_id != node.node_id)
            .map(|p| (p.node_id, Self::complementarity(node, p)))
            .collect();

        // Sort by descending score.
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(max_candidates);
        candidates
    }

    /// Check if adding a candidate would improve the collective's composite.
    ///
    /// A node improves the collective if it brings at least one new runtime,
    /// specialization, or trait that the collective doesn't already have.
    pub fn improves_collective(collective: &Collective, candidate: &NodeProfile) -> bool {
        // Check for new runtimes.
        for rt in &candidate.runtimes {
            if !collective.composite.runtimes.contains(rt) {
                return true;
            }
        }

        // Check for new specializations.
        for spec in &candidate.specializations {
            if !collective.composite.specializations.contains(spec) {
                return true;
            }
        }

        // Check if candidate brings GPU when collective has none.
        if (collective.composite.gpu_cores.is_none() || collective.composite.gpu_cores == Some(0))
            && candidate.gpu.is_some() {
                return true;
            }

        false
    }
}

// ============================================================================
// CollectiveStore
// ============================================================================

/// Thread-safe store for all collective-related state.
///
/// Backed by [`DashMap`] for concurrent access from gossip receivers,
/// the collective engine, and the work routing layer.
pub struct CollectiveStore {
    collectives: DashMap<CollectiveId, Collective>,
    /// Index: node -> which collectives it belongs to.
    node_memberships: DashMap<NodeId, HashSet<CollectiveId>>,
    /// Pending formation proposals.
    pending_proposals: DashMap<CollectiveId, CollectiveProposal>,
    /// Pending ejection votes, keyed by (collective_id, target_node_id).
    ejection_votes: DashMap<(CollectiveId, NodeId), Vec<EjectionVote>>,
}

impl CollectiveStore {
    pub fn new() -> Self {
        Self {
            collectives: DashMap::new(),
            node_memberships: DashMap::new(),
            pending_proposals: DashMap::new(),
            ejection_votes: DashMap::new(),
        }
    }

    // ========================================================================
    // Collective CRUD
    // ========================================================================

    /// Insert or update a collective. Newer version wins.
    ///
    /// Returns `true` if the collective was inserted or updated.
    pub fn upsert(&self, collective: Collective) -> bool {
        let id = collective.id;

        match self.collectives.entry(id) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                // Update the membership index.
                for member in &collective.members {
                    self.node_memberships
                        .entry(member.node_id)
                        .or_default()
                        .insert(id);
                }
                vacant.insert(collective);
                true
            }
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                if collective.version > occupied.get().version {
                    // Clear old membership index entries.
                    for member in &occupied.get().members {
                        if let Some(mut set) = self.node_memberships.get_mut(&member.node_id) {
                            set.remove(&id);
                        }
                    }
                    // Set new membership index entries.
                    for member in &collective.members {
                        self.node_memberships
                            .entry(member.node_id)
                            .or_default()
                            .insert(id);
                    }
                    occupied.insert(collective);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Get a collective by its ID.
    pub fn get(&self, id: &CollectiveId) -> Option<Collective> {
        self.collectives.get(id).map(|r| r.value().clone())
    }

    /// Remove a collective entirely.
    pub fn remove(&self, id: &CollectiveId) {
        if let Some((_, collective)) = self.collectives.remove(id) {
            for member in &collective.members {
                if let Some(mut set) = self.node_memberships.get_mut(&member.node_id) {
                    set.remove(id);
                }
            }
        }
        self.pending_proposals.remove(id);
    }

    /// Return all collectives.
    pub fn all_collectives(&self) -> Vec<Collective> {
        self.collectives
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return only active collectives (state == Active or Degraded).
    pub fn active_collectives(&self) -> Vec<Collective> {
        self.collectives
            .iter()
            .filter(|r| {
                matches!(
                    r.value().state,
                    CollectiveState::Active | CollectiveState::Degraded
                )
            })
            .map(|r| r.value().clone())
            .collect()
    }

    /// Total number of tracked collectives.
    pub fn count(&self) -> usize {
        self.collectives.len()
    }

    // ========================================================================
    // Membership queries
    // ========================================================================

    /// Get all collective IDs that a node belongs to.
    pub fn collectives_for_node(&self, node_id: &NodeId) -> Vec<CollectiveId> {
        self.node_memberships
            .get(node_id)
            .map(|r| r.value().iter().copied().collect())
            .unwrap_or_default()
    }

    /// Find collectives whose composite profile satisfies the given requirements.
    pub fn find_matching(&self, requirements: &JobRequirements) -> Vec<Collective> {
        self.collectives
            .iter()
            .filter(|r| {
                matches!(
                    r.value().state,
                    CollectiveState::Active | CollectiveState::Degraded
                ) && r.value().composite.satisfies(requirements)
            })
            .map(|r| r.value().clone())
            .collect()
    }

    // ========================================================================
    // Proposal management
    // ========================================================================

    /// Add a formation proposal.
    pub fn add_proposal(&self, proposal: CollectiveProposal) {
        self.pending_proposals
            .insert(proposal.collective_id, proposal);
    }

    /// Get a pending proposal by collective ID.
    pub fn get_proposal(&self, id: &CollectiveId) -> Option<CollectiveProposal> {
        self.pending_proposals.get(id).map(|r| r.value().clone())
    }

    /// Remove a pending proposal.
    pub fn remove_proposal(&self, id: &CollectiveId) {
        self.pending_proposals.remove(id);
    }

    // ========================================================================
    // Ejection management
    // ========================================================================

    /// Add an ejection vote.
    pub fn add_ejection_vote(&self, vote: EjectionVote) {
        self.ejection_votes
            .entry((vote.collective_id, vote.target))
            .or_default()
            .push(vote);
    }

    /// Get all ejection votes for a specific target in a collective.
    pub fn get_ejection_votes(
        &self,
        collective_id: &CollectiveId,
        target: &NodeId,
    ) -> Vec<EjectionVote> {
        self.ejection_votes
            .get(&(*collective_id, *target))
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    /// Check if enough votes have been cast to eject the target.
    ///
    /// Ejection requires a strict majority of approval votes among members.
    pub fn should_eject(
        &self,
        collective_id: &CollectiveId,
        target: &NodeId,
        total_members: usize,
    ) -> bool {
        let votes = self.get_ejection_votes(collective_id, target);
        let approve_count = votes.iter().filter(|v| v.approve).count();
        let majority = total_members / 2 + 1;
        approve_count >= majority
    }

    // ========================================================================
    // Maintenance
    // ========================================================================

    /// Remove all collectives in the `Dissolved` state. Returns the count removed.
    pub fn prune_dissolved(&self) -> usize {
        let dissolved: Vec<CollectiveId> = self
            .collectives
            .iter()
            .filter(|r| r.value().state == CollectiveState::Dissolved)
            .map(|r| *r.key())
            .collect();

        let count = dissolved.len();
        for id in dissolved {
            self.remove(&id);
        }
        count
    }

    /// Remove proposals older than `max_age`. Returns the count removed.
    pub fn prune_expired_proposals(&self, max_age: ChronoDuration) -> usize {
        let now = Utc::now();
        let expired: Vec<CollectiveId> = self
            .pending_proposals
            .iter()
            .filter(|r| now - r.value().proposed_at > max_age)
            .map(|r| *r.key())
            .collect();

        let count = expired.len();
        for id in expired {
            self.pending_proposals.remove(&id);
        }
        count
    }
}

impl Default for CollectiveStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// CollectiveEngine
// ============================================================================

/// Orchestrator for collective lifecycle management.
///
/// Evaluates formation opportunities, handles proposals and acknowledgements,
/// manages ejections, and runs periodic maintenance. Communicates with other
/// nodes via the outbound message channel.
pub struct CollectiveEngine {
    self_id: NodeId,
    collective_store: Arc<CollectiveStore>,
    profile_store: Arc<ProfileStore>,
    knowledge: Arc<KnowledgeStore>,
    outbound_tx: mpsc::Sender<(SocketAddr, SwarmMessage)>,
}

impl CollectiveEngine {
    pub fn new(
        self_id: NodeId,
        collective_store: Arc<CollectiveStore>,
        profile_store: Arc<ProfileStore>,
        knowledge: Arc<KnowledgeStore>,
        outbound_tx: mpsc::Sender<(SocketAddr, SwarmMessage)>,
    ) -> Self {
        Self {
            self_id,
            collective_store,
            profile_store,
            knowledge,
            outbound_tx,
        }
    }

    /// Evaluate whether to propose a new collective based on our profile and known peers.
    ///
    /// Returns `Some(proposal)` if a good formation opportunity exists:
    /// - We are not already in `MAX_COLLECTIVES_PER_NODE` collectives
    /// - We can find complementary peers
    /// - The resulting composite would be meaningfully stronger
    pub fn evaluate_formation(&self, my_profile: &NodeProfile) -> Option<CollectiveProposal> {
        // Check if we're already in too many collectives.
        let my_collectives = self.collective_store.collectives_for_node(&self.self_id);
        if my_collectives.len() >= MAX_COLLECTIVES_PER_NODE {
            debug!(
                count = my_collectives.len(),
                max = MAX_COLLECTIVES_PER_NODE,
                "skipping collective formation: at max collectives"
            );
            return None;
        }

        // Gather all known peer profiles.
        let all_profiles: Vec<NodeProfile> = self
            .profile_store
            .all_profiles()
            .into_iter()
            .filter(|p| p.node_id != self.self_id)
            .collect();

        if all_profiles.is_empty() {
            return None;
        }

        // Find the best candidates.
        let candidates = AffinityScorer::find_candidates(my_profile, &all_profiles, 5);

        // Filter out candidates already in a collective with us.
        let candidates: Vec<(NodeId, f32)> = candidates
            .into_iter()
            .filter(|(cid, score)| {
                // Require a minimum complementarity threshold.
                if *score < 0.2 {
                    return false;
                }
                // Don't invite nodes already in a collective with us.
                let their_collectives = self.collective_store.collectives_for_node(cid);
                !their_collectives
                    .iter()
                    .any(|col_id| my_collectives.contains(col_id))
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        // Propose with the top 2-4 candidates.
        let invited: Vec<NodeId> = candidates
            .iter()
            .take(4)
            .map(|(nid, _)| *nid)
            .collect();

        let avg_score: f32 =
            candidates.iter().take(4).map(|(_, s)| s).sum::<f32>() / invited.len() as f32;

        let collective_id = CollectiveId::new();
        let proposal = CollectiveProposal {
            collective_id,
            initiator: self.self_id,
            invited: invited.clone(),
            reason: format!(
                "complementarity avg={:.2}, {} candidates",
                avg_score,
                invited.len()
            ),
            proposed_at: Utc::now(),
        };

        // Create the forming collective locally.
        let mut collective = Collective::new(self.self_id, my_profile);
        collective.id = collective_id;

        // Add invited members as Pending.
        for invited_id in &invited {
            let pending_member = CollectiveMember {
                node_id: *invited_id,
                role: MemberRole::Pending,
                joined_at: Utc::now(),
                contribution: HashSet::new(),
            };
            collective.members.push(pending_member);
        }

        self.collective_store.upsert(collective);
        self.collective_store.add_proposal(proposal.clone());

        info!(
            collective_id = %collective_id,
            invited = invited.len(),
            avg_complementarity = format!("{:.2}", avg_score),
            "proposed new collective"
        );

        Some(proposal)
    }

    /// Handle an incoming formation proposal.
    ///
    /// Returns an ack if we decide to join (or reject).
    pub fn handle_proposal(&self, proposal: CollectiveProposal) -> Option<CollectiveAck> {
        // Am I invited?
        if !proposal.invited.contains(&self.self_id) {
            debug!(
                collective_id = %proposal.collective_id,
                "ignoring proposal: not invited"
            );
            return None;
        }

        // Check if we're already in too many collectives.
        let my_collectives = self.collective_store.collectives_for_node(&self.self_id);
        let accepted = my_collectives.len() < MAX_COLLECTIVES_PER_NODE;

        let ack = CollectiveAck {
            collective_id: proposal.collective_id,
            node_id: self.self_id,
            accepted,
            timestamp: Utc::now(),
        };

        if accepted {
            // Store the proposal so we track this forming collective.
            self.collective_store.add_proposal(proposal);
            info!(
                collective_id = %ack.collective_id,
                "accepted collective proposal"
            );
        } else {
            debug!(
                collective_id = %ack.collective_id,
                "rejected collective proposal: at max collectives"
            );
        }

        Some(ack)
    }

    /// Handle an incoming acknowledgement.
    ///
    /// If enough acks have been received, finalize the collective and return it.
    pub fn handle_ack(&self, ack: CollectiveAck) -> Option<Collective> {
        let mut collective = self.collective_store.get(&ack.collective_id)?;

        // Only the initiator processes acks.
        if collective.members.first().map(|m| m.node_id) != Some(self.self_id) {
            return None;
        }

        if ack.accepted {
            // Upgrade the member from Pending to Member.
            for member in &mut collective.members {
                if member.node_id == ack.node_id && member.role == MemberRole::Pending {
                    member.role = MemberRole::Member;
                    member.joined_at = Utc::now();

                    // If we have their profile, set their contribution.
                    if let Some(_profile) = self.profile_store.get(&ack.node_id) {
                        let mut contribution = HashSet::new();
                        contribution.insert(Trait::CanExecute);
                        member.contribution = contribution;
                    }
                }
            }
        } else {
            // Remove the rejecting node.
            collective
                .members
                .retain(|m| m.node_id != ack.node_id);
        }

        // Count how many non-pending members we have.
        let accepted_count = collective
            .members
            .iter()
            .filter(|m| m.role != MemberRole::Pending)
            .count();

        let pending_count = collective
            .members
            .iter()
            .filter(|m| m.role == MemberRole::Pending)
            .count();

        // If no more pending members and at least 2 accepted, finalize.
        if pending_count == 0 && accepted_count >= MIN_COLLECTIVE_SIZE {
            collective.state = CollectiveState::Active;

            // Rebuild composite from all member profiles.
            let profiles: Vec<NodeProfile> = collective
                .members
                .iter()
                .filter_map(|m| self.profile_store.get(&m.node_id))
                .collect();
            collective.recalculate_composite(&profiles);

            self.collective_store.remove_proposal(&ack.collective_id);
            collective.touch();
            self.collective_store.upsert(collective.clone());

            info!(
                collective_id = %ack.collective_id,
                members = accepted_count,
                "collective finalized"
            );

            return Some(collective);
        }

        // If no pending members but < 2 accepted, dissolve.
        if pending_count == 0 && accepted_count < MIN_COLLECTIVE_SIZE {
            collective.state = CollectiveState::Dissolved;
            self.collective_store.remove_proposal(&ack.collective_id);
            collective.touch();
            self.collective_store.upsert(collective);
            return None;
        }

        // Still waiting for more acks.
        collective.touch();
        self.collective_store.upsert(collective);
        None
    }

    /// Handle an ejection proposal: cast our vote.
    pub fn handle_ejection_proposal(&self, proposal: EjectionProposal) -> Option<EjectionVote> {
        let collective = self.collective_store.get(&proposal.collective_id)?;

        // Must be a member to vote.
        if !collective.is_member(&self.self_id) {
            return None;
        }

        // Don't vote to eject ourselves.
        if proposal.target == self.self_id {
            return Some(EjectionVote {
                collective_id: proposal.collective_id,
                voter: self.self_id,
                target: proposal.target,
                approve: false,
                timestamp: Utc::now(),
            });
        }

        // Simple heuristic: approve ejection if the proposer is not the target.
        // In a real system, you'd check the target's reputation/performance.
        let vote = EjectionVote {
            collective_id: proposal.collective_id,
            voter: self.self_id,
            target: proposal.target,
            approve: true,
            timestamp: Utc::now(),
        };

        self.collective_store.add_ejection_vote(vote.clone());

        Some(vote)
    }

    /// Handle an ejection vote. If majority reached, execute the ejection.
    ///
    /// Returns `true` if the ejection was executed.
    pub fn handle_ejection_vote(&self, vote: EjectionVote) -> bool {
        self.collective_store.add_ejection_vote(vote.clone());

        let collective = match self.collective_store.get(&vote.collective_id) {
            Some(c) => c,
            None => return false,
        };

        let total_members = collective.member_count();

        if self
            .collective_store
            .should_eject(&vote.collective_id, &vote.target, total_members)
        {
            let mut updated = collective;
            updated.remove_member(&vote.target);
            updated.touch();
            self.collective_store.upsert(updated);

            info!(
                collective_id = %vote.collective_id,
                target = %vote.target,
                "member ejected from collective"
            );

            true
        } else {
            false
        }
    }

    /// Self-police: check all our collectives for underperforming members.
    ///
    /// Uses the provided reputation function to look up each member's score.
    /// Returns ejection proposals for members below the collective's minimum
    /// reputation threshold.
    pub fn self_police(
        &self,
        reputation_fn: &dyn Fn(&NodeId) -> Option<f32>,
    ) -> Vec<EjectionProposal> {
        let mut proposals = Vec::new();

        let my_collectives = self.collective_store.collectives_for_node(&self.self_id);

        for col_id in my_collectives {
            let collective = match self.collective_store.get(&col_id) {
                Some(c) => c,
                None => continue,
            };

            if collective.state != CollectiveState::Active {
                continue;
            }

            for member in &collective.members {
                // Don't propose ejecting ourselves.
                if member.node_id == self.self_id {
                    continue;
                }

                if let Some(rep_score) = reputation_fn(&member.node_id) {
                    if rep_score < collective.min_member_reputation {
                        let proposal = EjectionProposal {
                            collective_id: col_id,
                            proposer: self.self_id,
                            target: member.node_id,
                            reason: format!(
                                "reputation {:.2} below minimum {:.2}",
                                rep_score, collective.min_member_reputation
                            ),
                            proposed_at: Utc::now(),
                        };
                        proposals.push(proposal);
                    }
                }
            }
        }

        proposals
    }

    /// Handle a member going offline. Removes them from all collectives they
    /// belonged to and returns the list of affected collective IDs.
    pub fn handle_member_death(&self, dead_node: &NodeId) -> Vec<CollectiveId> {
        let affected_collectives = self.collective_store.collectives_for_node(dead_node);
        let mut result = Vec::new();

        for col_id in affected_collectives {
            if let Some(mut collective) = self.collective_store.get(&col_id) {
                collective.remove_member(dead_node);
                collective.touch();
                self.collective_store.upsert(collective);
                result.push(col_id);

                info!(
                    collective_id = %col_id,
                    dead_node = %dead_node,
                    "removed dead member from collective"
                );
            }
        }

        result
    }

    /// Periodic maintenance: dissolve dead collectives, prune expired proposals.
    pub fn maintenance(&self) {
        // Dissolve collectives that should dissolve.
        let all = self.collective_store.all_collectives();
        for mut collective in all {
            if collective.state == CollectiveState::Dissolved
                || collective.state == CollectiveState::Dissolving
            {
                continue;
            }

            if collective.should_dissolve() {
                info!(
                    collective_id = %collective.id,
                    members = collective.member_count(),
                    success_rate = format!("{:.2}", collective.success_rate()),
                    "dissolving collective"
                );
                collective.state = CollectiveState::Dissolved;
                collective.touch();
                self.collective_store.upsert(collective);
            }
        }

        // Prune dissolved collectives.
        let pruned = self.collective_store.prune_dissolved();
        if pruned > 0 {
            debug!(count = pruned, "pruned dissolved collectives");
        }

        // Prune expired proposals.
        let expired = self
            .collective_store
            .prune_expired_proposals(ChronoDuration::seconds(PROPOSAL_MAX_AGE_SECS));
        if expired > 0 {
            debug!(count = expired, "pruned expired proposals");
        }
    }

    /// Spawn the background collective management loop.
    ///
    /// Runs every `COLLECTIVE_LOOP_INTERVAL` (10 seconds):
    /// 1. Evaluate formation (if not in too many collectives already)
    /// 2. Run self_police
    /// 3. Run maintenance
    pub fn spawn_loop(
        self: Arc<Self>,
        my_profile: Arc<RwLock<NodeProfile>>,
        mut shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("collective management loop started");

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(COLLECTIVE_LOOP_INTERVAL) => {}
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow() {
                            info!("collective management loop shutting down");
                            return;
                        }
                    }
                }

                if *shutdown.borrow() {
                    info!("collective management loop shutting down");
                    return;
                }

                // 1. Evaluate formation.
                let profile = my_profile.read().clone();
                if let Some(proposal) = self.evaluate_formation(&profile) {
                    debug!(
                        collective_id = %proposal.collective_id,
                        invited = proposal.invited.len(),
                        "formation proposal created"
                    );
                    // In a full implementation, we'd send the proposal to invited
                    // nodes via the outbound channel. The SwarmMessage enum will
                    // be extended to carry collective-specific messages.
                }

                // 2. Self-police.
                // Use a no-op reputation function for now. In production, this
                // would query the ReputationStore.
                let ejection_proposals = self.self_police(&|_| None);
                for proposal in ejection_proposals {
                    debug!(
                        collective_id = %proposal.collective_id,
                        target = %proposal.target,
                        reason = %proposal.reason,
                        "ejection proposal created"
                    );
                }

                // 3. Maintenance.
                self.maintenance();
            }
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // -- Test helpers --

    /// Create a minimal NodeProfile for testing.
    fn make_profile(
        node_id: NodeId,
        cpu_cores: u32,
        ram_mb: u64,
        runtimes: Vec<Runtime>,
        _strengths: Vec<Trait>,
        specs: Vec<&str>,
    ) -> NodeProfile {
        use super::super::profile::{Strength, TimeWindow, Weakness};
        NodeProfile {
            node_id,
            node_type: super::super::profile::NodeType::BareMetal,
            version: 1,
            cpu_cores,
            cpu_freq_mhz: 3000,
            ram_total_mb: ram_mb,
            disk_available_mb: 10000,
            gpu: None,
            strengths: vec![Strength::HighCoreCount],
            runtimes,
            specializations: specs.iter().map(|s| s.to_string()).collect(),
            weaknesses: vec![],
            max_task_duration: None,
            availability_window: TimeWindow::always(),
            preferred_work: vec![],
            avoided_work: vec![],
            max_concurrent: 4,
            installed_software: vec![],
            custom_capabilities: vec![],
            ..Default::default()
        }
    }

    /// Create a profile with GPU.
    fn make_gpu_profile(node_id: NodeId) -> NodeProfile {
        let mut profile = make_profile(
            node_id,
            8,
            16384,
            vec![Runtime::Python3, Runtime::Docker],
            vec![Trait::CanExecute],
            vec!["ml_training"],
        );
        profile.gpu = Some(super::super::profile::GpuInfo {
            name: "RTX 4090".to_string(),
            vram_mb: 24576,
            cuda_cores: Some(16384),
            compute_capability: Some("8.9".to_string()),
        });
        profile
    }

    fn make_requirements(
        min_cpu: Option<u32>,
        min_ram: Option<u64>,
        needs_gpu: bool,
        runtimes: Vec<Runtime>,
    ) -> JobRequirements {
        JobRequirements {
            min_cpu_cores: min_cpu,
            min_memory_mb: min_ram,
            min_disk_mb: None,
            needs_gpu,
            required_runtimes: runtimes,
            max_duration: None,
            required_capabilities: vec![],
            preferred_node_types: vec![],
            excluded_node_types: vec![],
            ..Default::default()
        }
    }

    // ========================================================================
    // CollectiveId tests
    // ========================================================================

    #[test]
    fn collective_id_display_short_hex() {
        let id = CollectiveId::new();
        let display = format!("{}", id);
        assert!(display.len() >= 1 && display.len() <= 4);
    }

    #[test]
    fn collective_id_equality() {
        let id1 = CollectiveId::new();
        let id2 = CollectiveId::new();
        assert_ne!(id1, id2);
        assert_eq!(id1, id1);
    }

    // ========================================================================
    // CompositeProfile tests
    // ========================================================================

    #[test]
    fn composite_from_empty_members() {
        let composite = CompositeProfile::from_members(&[]);
        assert_eq!(composite.total_cpu_cores, 0);
        assert_eq!(composite.total_ram_mb, 0);
        assert!(composite.runtimes.is_empty());
    }

    #[test]
    fn composite_sums_resources() {
        let p1 = make_profile(
            NodeId::new(),
            4,
            8192,
            vec![Runtime::Shell, Runtime::Python3],
            vec![Trait::CanExecute],
            vec!["data_processing"],
        );
        let p2 = make_profile(
            NodeId::new(),
            8,
            16384,
            vec![Runtime::Python3, Runtime::Docker],
            vec![Trait::CanExecute, Trait::CanAggregate],
            vec!["ml_training"],
        );

        let composite = CompositeProfile::from_members(&[p1, p2]);

        assert_eq!(composite.total_cpu_cores, 12);
        assert_eq!(composite.total_ram_mb, 8192 + 16384);
        assert!(composite.runtimes.contains(&Runtime::Shell));
        assert!(composite.runtimes.contains(&Runtime::Python3));
        assert!(composite.runtimes.contains(&Runtime::Docker));
        assert_eq!(composite.runtimes.len(), 3);
        assert!(composite.specializations.contains("data_processing"));
        assert!(composite.specializations.contains("ml_training"));
        assert!(composite.combined_traits.contains(&Trait::CanExecute));
    }

    #[test]
    fn composite_gpu_aggregation() {
        let p1 = make_gpu_profile(NodeId::new());
        let mut p2 = make_gpu_profile(NodeId::new());
        p2.gpu = Some(super::super::profile::GpuInfo {
            name: "RTX 3090".to_string(),
            vram_mb: 24576,
            cuda_cores: Some(10496),
            compute_capability: Some("8.6".to_string()),
        });

        let composite = CompositeProfile::from_members(&[p1, p2]);
        assert_eq!(composite.gpu_cores, Some(16384 + 10496));
    }

    #[test]
    fn composite_satisfies_requirements() {
        let p1 = make_profile(
            NodeId::new(),
            4,
            8192,
            vec![Runtime::Shell, Runtime::Python3],
            vec![Trait::CanExecute],
            vec![],
        );
        let p2 = make_gpu_profile(NodeId::new());

        let composite = CompositeProfile::from_members(&[p1, p2]);

        // Should satisfy basic requirements.
        let reqs = make_requirements(Some(10), Some(20000), false, vec![Runtime::Python3]);
        assert!(composite.satisfies(&reqs));

        // Should satisfy GPU requirement.
        let gpu_reqs = make_requirements(None, None, true, vec![]);
        assert!(composite.satisfies(&gpu_reqs));

        // Should NOT satisfy extreme CPU requirement.
        let extreme_reqs = make_requirements(Some(100), None, false, vec![]);
        assert!(!composite.satisfies(&extreme_reqs));
    }

    #[test]
    fn composite_capability_match_scoring() {
        let p1 = make_profile(
            NodeId::new(),
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec![],
        );
        let composite = CompositeProfile::from_members(&[p1]);

        // Exact match for CPU.
        let reqs = make_requirements(Some(4), None, false, vec![]);
        let score = composite.capability_match(&reqs);
        assert!((score - 1.0).abs() < f32::EPSILON);

        // Half CPU.
        let reqs = make_requirements(Some(8), None, false, vec![]);
        let score = composite.capability_match(&reqs);
        assert!((score - 0.5).abs() < f32::EPSILON);

        // No requirements = perfect match.
        let reqs = make_requirements(None, None, false, vec![]);
        let score = composite.capability_match(&reqs);
        assert!((score - 1.0).abs() < f32::EPSILON);
    }

    // ========================================================================
    // Collective lifecycle tests
    // ========================================================================

    #[test]
    fn collective_creation_and_membership() {
        let initiator = NodeId::new();
        let profile = make_profile(
            initiator,
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec![],
        );

        let collective = Collective::new(initiator, &profile);

        assert_eq!(collective.state, CollectiveState::Forming);
        assert_eq!(collective.member_count(), 1);
        assert!(collective.is_member(&initiator));
        assert!(!collective.is_member(&NodeId::new()));
        assert_eq!(collective.version, 1);
    }

    #[test]
    fn collective_add_remove_member() {
        let initiator = NodeId::new();
        let member_id = NodeId::new();

        let init_profile = make_profile(
            initiator,
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec![],
        );
        let member_profile = make_profile(
            member_id,
            8,
            16384,
            vec![Runtime::Python3],
            vec![Trait::CanAggregate],
            vec!["ml"],
        );

        let mut collective = Collective::new(initiator, &init_profile);

        // Add member.
        assert!(collective.add_member(member_id, &member_profile));
        assert_eq!(collective.member_count(), 2);
        assert!(collective.is_member(&member_id));

        // Cannot add same member twice.
        assert!(!collective.add_member(member_id, &member_profile));

        // Remove member.
        assert!(collective.remove_member(&member_id));
        assert_eq!(collective.member_count(), 1);
        assert!(!collective.is_member(&member_id));

        // Cannot remove non-member.
        assert!(!collective.remove_member(&member_id));
    }

    #[test]
    fn collective_recalculate_composite() {
        let initiator = NodeId::new();
        let init_profile = make_profile(
            initiator,
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec![],
        );
        let member_id = NodeId::new();
        let member_profile = make_profile(
            member_id,
            8,
            16384,
            vec![Runtime::Python3],
            vec![Trait::CanAggregate],
            vec![],
        );

        let mut collective = Collective::new(initiator, &init_profile);
        collective.add_member(member_id, &member_profile);

        // Recalculate from full profiles.
        collective.recalculate_composite(&[init_profile, member_profile]);

        assert_eq!(collective.composite.total_cpu_cores, 12);
        assert_eq!(collective.composite.total_ram_mb, 8192 + 16384);
    }

    #[test]
    fn collective_job_tracking() {
        let initiator = NodeId::new();
        let profile = make_profile(
            initiator,
            4,
            8192,
            vec![],
            vec![],
            vec![],
        );

        let mut collective = Collective::new(initiator, &profile);

        // No jobs yet: rate is 1.0.
        assert!((collective.success_rate() - 1.0).abs() < f32::EPSILON);

        collective.record_job_success();
        collective.record_job_success();
        collective.record_job_failure();

        assert_eq!(collective.jobs_completed, 2);
        assert_eq!(collective.jobs_failed, 1);

        let rate = collective.success_rate();
        assert!((rate - 2.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn collective_should_dissolve() {
        let initiator = NodeId::new();
        let profile = make_profile(initiator, 4, 8192, vec![], vec![], vec![]);

        // Collective with 1 member should dissolve.
        let collective = Collective::new(initiator, &profile);
        assert!(collective.should_dissolve());

        // Collective with 2 members should not dissolve (yet).
        let mut collective = Collective::new(initiator, &profile);
        let member_id = NodeId::new();
        let member_profile = make_profile(member_id, 4, 8192, vec![], vec![], vec![]);
        collective.add_member(member_id, &member_profile);
        assert!(!collective.should_dissolve());

        // With low success rate after enough jobs.
        for _ in 0..MIN_JOBS_FOR_RATE {
            collective.record_job_failure();
        }
        assert!(collective.should_dissolve());
    }

    // ========================================================================
    // AffinityScorer tests
    // ========================================================================

    #[test]
    fn affinity_identical_nodes_low_score() {
        let p1 = make_profile(
            NodeId::new(),
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec!["web"],
        );
        let p2 = make_profile(
            NodeId::new(),
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec!["web"],
        );

        let score = AffinityScorer::complementarity(&p1, &p2);
        assert!(score < 0.3, "identical nodes should have low complementarity: {}", score);
    }

    #[test]
    fn affinity_different_nodes_high_score() {
        let p1 = make_profile(
            NodeId::new(),
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec!["web"],
        );
        let p2 = make_gpu_profile(NodeId::new());

        let score = AffinityScorer::complementarity(&p1, &p2);
        assert!(score > 0.3, "different nodes should have higher complementarity: {}", score);
    }

    #[test]
    fn affinity_find_candidates() {
        let me = make_profile(
            NodeId::new(),
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec![],
        );

        let others = vec![
            make_gpu_profile(NodeId::new()),
            make_profile(
                NodeId::new(),
                4,
                8192,
                vec![Runtime::Shell],
                vec![Trait::CanExecute],
                vec![],
            ),
            make_profile(
                NodeId::new(),
                16,
                65536,
                vec![Runtime::Docker, Runtime::Go],
                vec![Trait::CanAggregate, Trait::CanStoreState],
                vec!["big_data"],
            ),
        ];

        let candidates = AffinityScorer::find_candidates(&me, &others, 2);

        assert_eq!(candidates.len(), 2);
        // Should be sorted by descending complementarity.
        assert!(candidates[0].1 >= candidates[1].1);
    }

    #[test]
    fn affinity_improves_collective() {
        let initiator = NodeId::new();
        let init_profile = make_profile(
            initiator,
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec![],
        );

        let collective = Collective::new(initiator, &init_profile);

        // A GPU node should improve the collective.
        let gpu_profile = make_gpu_profile(NodeId::new());
        assert!(AffinityScorer::improves_collective(&collective, &gpu_profile));

        // An identical node should not improve it.
        let same_profile = make_profile(
            NodeId::new(),
            4,
            8192,
            vec![Runtime::Shell],
            vec![Trait::CanExecute],
            vec![],
        );
        assert!(!AffinityScorer::improves_collective(&collective, &same_profile));
    }

    // ========================================================================
    // CollectiveStore tests
    // ========================================================================

    #[test]
    fn store_upsert_and_get() {
        let store = CollectiveStore::new();
        let initiator = NodeId::new();
        let profile = make_profile(initiator, 4, 8192, vec![], vec![], vec![]);
        let collective = Collective::new(initiator, &profile);
        let id = collective.id;

        assert!(store.upsert(collective.clone()));
        assert_eq!(store.count(), 1);

        let retrieved = store.get(&id).unwrap();
        assert_eq!(retrieved.id, id);
    }

    #[test]
    fn store_version_wins() {
        let store = CollectiveStore::new();
        let initiator = NodeId::new();
        let profile = make_profile(initiator, 4, 8192, vec![], vec![], vec![]);

        let mut c1 = Collective::new(initiator, &profile);
        let id = c1.id;
        c1.version = 5;
        store.upsert(c1);

        // Older version should not replace.
        let mut c2 = store.get(&id).unwrap();
        c2.version = 3;
        c2.jobs_completed = 999;
        assert!(!store.upsert(c2));

        let stored = store.get(&id).unwrap();
        assert_eq!(stored.jobs_completed, 0);

        // Newer version should replace.
        let mut c3 = store.get(&id).unwrap();
        c3.version = 10;
        c3.jobs_completed = 42;
        assert!(store.upsert(c3));

        let stored = store.get(&id).unwrap();
        assert_eq!(stored.jobs_completed, 42);
    }

    #[test]
    fn store_node_membership_index() {
        let store = CollectiveStore::new();
        let node1 = NodeId::new();
        let node2 = NodeId::new();

        let p1 = make_profile(node1, 4, 8192, vec![], vec![], vec![]);
        let p2 = make_profile(node2, 4, 8192, vec![], vec![], vec![]);

        let mut collective = Collective::new(node1, &p1);
        collective.add_member(node2, &p2);
        collective.state = CollectiveState::Active;
        let col_id = collective.id;

        store.upsert(collective);

        assert_eq!(store.collectives_for_node(&node1).len(), 1);
        assert_eq!(store.collectives_for_node(&node1)[0], col_id);
        assert_eq!(store.collectives_for_node(&node2).len(), 1);
    }

    #[test]
    fn store_active_collectives() {
        let store = CollectiveStore::new();
        let n1 = NodeId::new();
        let p1 = make_profile(n1, 4, 8192, vec![], vec![], vec![]);

        let mut active = Collective::new(n1, &p1);
        active.state = CollectiveState::Active;
        store.upsert(active);

        let mut forming = Collective::new(n1, &p1);
        forming.state = CollectiveState::Forming;
        store.upsert(forming);

        let mut dissolved = Collective::new(n1, &p1);
        dissolved.state = CollectiveState::Dissolved;
        store.upsert(dissolved);

        assert_eq!(store.count(), 3);
        assert_eq!(store.active_collectives().len(), 1);
    }

    #[test]
    fn store_ejection_voting() {
        let store = CollectiveStore::new();
        let col_id = CollectiveId::new();
        let target = NodeId::new();

        // 3 voters: 2 approve, 1 disapproves.
        for i in 0..3 {
            let vote = EjectionVote {
                collective_id: col_id,
                voter: NodeId::new(),
                target,
                approve: i < 2,
                timestamp: Utc::now(),
            };
            store.add_ejection_vote(vote);
        }

        let votes = store.get_ejection_votes(&col_id, &target);
        assert_eq!(votes.len(), 3);

        // With 5 total members, majority = 3, so 2 approvals is not enough.
        assert!(!store.should_eject(&col_id, &target, 5));

        // With 3 total members, majority = 2, so 2 approvals is enough.
        assert!(store.should_eject(&col_id, &target, 3));
    }

    #[test]
    fn store_prune_dissolved() {
        let store = CollectiveStore::new();
        let n1 = NodeId::new();
        let p1 = make_profile(n1, 4, 8192, vec![], vec![], vec![]);

        let mut c1 = Collective::new(n1, &p1);
        c1.state = CollectiveState::Dissolved;
        store.upsert(c1);

        let mut c2 = Collective::new(n1, &p1);
        c2.state = CollectiveState::Active;
        store.upsert(c2);

        assert_eq!(store.count(), 2);
        let pruned = store.prune_dissolved();
        assert_eq!(pruned, 1);
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn store_prune_expired_proposals() {
        let store = CollectiveStore::new();

        let old_proposal = CollectiveProposal {
            collective_id: CollectiveId::new(),
            initiator: NodeId::new(),
            invited: vec![NodeId::new()],
            reason: "test".to_string(),
            proposed_at: Utc::now() - ChronoDuration::seconds(120),
        };

        let fresh_proposal = CollectiveProposal {
            collective_id: CollectiveId::new(),
            initiator: NodeId::new(),
            invited: vec![NodeId::new()],
            reason: "test".to_string(),
            proposed_at: Utc::now(),
        };

        store.add_proposal(old_proposal);
        store.add_proposal(fresh_proposal.clone());

        let pruned = store.prune_expired_proposals(ChronoDuration::seconds(60));
        assert_eq!(pruned, 1);

        // Fresh proposal should still be there.
        assert!(store.get_proposal(&fresh_proposal.collective_id).is_some());
    }

    #[test]
    fn store_find_matching() {
        let store = CollectiveStore::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();

        let p1 = make_profile(
            n1,
            4,
            8192,
            vec![Runtime::Shell, Runtime::Python3],
            vec![Trait::CanExecute],
            vec![],
        );
        let p2 = make_gpu_profile(n2);

        let mut collective = Collective::new(n1, &p1);
        collective.add_member(n2, &p2);
        collective.state = CollectiveState::Active;
        collective.recalculate_composite(&[p1.clone(), p2]);
        store.upsert(collective);

        // Should find the collective for GPU work.
        let gpu_reqs = make_requirements(None, None, true, vec![]);
        let matching = store.find_matching(&gpu_reqs);
        assert_eq!(matching.len(), 1);

        // Should not find if requiring a runtime nobody has.
        let wasm_reqs = make_requirements(None, None, false, vec![Runtime::Wasm]);
        let matching = store.find_matching(&wasm_reqs);
        assert_eq!(matching.len(), 0);
    }

    // ========================================================================
    // Serialization round-trip
    // ========================================================================

    #[test]
    fn serialization_round_trip() {
        let initiator = NodeId::new();
        let profile = make_profile(
            initiator,
            4,
            8192,
            vec![Runtime::Shell, Runtime::Python3],
            vec![Trait::CanExecute, Trait::CanAggregate],
            vec!["ml"],
        );

        let collective = Collective::new(initiator, &profile);

        let serialized = serde_json::to_string(&collective).unwrap();
        let deserialized: Collective = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.id, collective.id);
        assert_eq!(deserialized.member_count(), collective.member_count());
        assert_eq!(deserialized.state, collective.state);
        assert_eq!(deserialized.version, collective.version);
    }

    #[test]
    fn proposal_serialization_round_trip() {
        let proposal = CollectiveProposal {
            collective_id: CollectiveId::new(),
            initiator: NodeId::new(),
            invited: vec![NodeId::new(), NodeId::new()],
            reason: "complementary nodes".to_string(),
            proposed_at: Utc::now(),
        };

        let json = serde_json::to_string(&proposal).unwrap();
        let decoded: CollectiveProposal = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.collective_id, proposal.collective_id);
        assert_eq!(decoded.invited.len(), 2);
    }

    #[test]
    fn ejection_vote_serialization_round_trip() {
        let vote = EjectionVote {
            collective_id: CollectiveId::new(),
            voter: NodeId::new(),
            target: NodeId::new(),
            approve: true,
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string(&vote).unwrap();
        let decoded: EjectionVote = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.collective_id, vote.collective_id);
        assert!(decoded.approve);
    }

    // ========================================================================
    // Dissolution logic
    // ========================================================================

    #[test]
    fn collective_dissolves_on_member_removal() {
        let initiator = NodeId::new();
        let member_id = NodeId::new();

        let p1 = make_profile(initiator, 4, 8192, vec![], vec![], vec![]);
        let p2 = make_profile(member_id, 4, 8192, vec![], vec![], vec![]);

        let mut collective = Collective::new(initiator, &p1);
        collective.add_member(member_id, &p2);
        collective.state = CollectiveState::Active;

        assert_eq!(collective.member_count(), 2);

        // Remove the member.
        collective.remove_member(&member_id);
        assert_eq!(collective.member_count(), 1);
        assert_eq!(collective.state, CollectiveState::Dissolving);
    }

    #[test]
    fn collective_degrades_when_still_viable() {
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();

        let p1 = make_profile(n1, 4, 8192, vec![], vec![], vec![]);
        let p2 = make_profile(n2, 4, 8192, vec![], vec![], vec![]);
        let p3 = make_profile(n3, 4, 8192, vec![], vec![], vec![]);

        let mut collective = Collective::new(n1, &p1);
        collective.add_member(n2, &p2);
        collective.add_member(n3, &p3);
        collective.state = CollectiveState::Active;

        // Remove one of three members.
        collective.remove_member(&n3);
        assert_eq!(collective.member_count(), 2);
        assert_eq!(collective.state, CollectiveState::Degraded);
    }
}
