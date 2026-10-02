// Marabunta - Licensed under the MIT License.
//! Failure detection engine for the Marabunta Swarm.
//!
//! Detects node failures purely through gossip absence — no explicit heartbeat
//! protocol is required. The detector periodically sweeps all known nodes and
//! transitions them through `Alive -> Suspect -> Dead` based on how long it
//! has been since they were last heard from (directly or via gossip).
//!
//! When a node is declared dead, its in-progress chunks are released back to
//! `Pending` status so the swarm can reassign them to healthy nodes.
//!
//! ## Split-brain protection
//!
//! The detector includes quorum-based death declaration and partition detection:
//!
//! - **WitnessStore**: collects witness reports from peers (piggybacked on gossip)
//!   attesting whether they have recently seen a given node.
//! - **Quorum check**: before declaring a node dead, the detector verifies that
//!   at least `QUORUM_FRACTION` of active peers also consider the node unreachable.
//! - **PartitionDetector**: monitors what fraction of previously-known peers
//!   are still reachable, transitioning through `Normal -> Degraded -> Isolated`
//!   states and suppressing new work claims during partitions.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use tracing;

use super::collective::{CollectiveId, CollectiveStore};
use super::config::{
    DEAD_THRESHOLD, FAILURE_CHECK_INTERVAL, PARTITION_RECOVERY_ROUNDS, QUORUM_FRACTION,
    SUSPECT_THRESHOLD, WITNESS_DECAY_SECS,
};
use super::knowledge::KnowledgeStore;
use super::types::{ChunkId, NodeId, NodeStatus, PartitionStatus, WitnessReport};

// ============================================================================
// Failure report
// ============================================================================

/// Result of a single failure detection sweep.
///
/// Contains everything that changed during the sweep: which nodes were
/// newly suspected or declared dead, which chunks were reassigned, and
/// a snapshot of current node counts by status.
#[derive(Debug, Default, Clone)]
pub struct FailureReport {
    /// Nodes that transitioned from `Alive` to `Suspect` in this sweep.
    pub nodes_suspected: Vec<NodeId>,
    /// Nodes that transitioned to `Dead` in this sweep (from either `Alive` or `Suspect`).
    pub nodes_declared_dead: Vec<NodeId>,
    /// Chunks that were released from dead nodes and set back to `Pending`.
    pub chunks_reassigned: Vec<ChunkId>,
    /// Number of nodes currently in `Alive` status (after this sweep).
    pub total_alive: usize,
    /// Number of nodes currently in `Suspect` status (after this sweep).
    pub total_suspect: usize,
    /// Number of nodes currently in `Dead` status (after this sweep).
    pub total_dead: usize,
    /// Collectives affected by member deaths in this sweep.
    pub collectives_affected: Vec<CollectiveId>,
}

impl FailureReport {
    /// Returns `true` if the sweep caused any state transitions.
    pub fn has_changes(&self) -> bool {
        !self.nodes_suspected.is_empty()
            || !self.nodes_declared_dead.is_empty()
            || !self.chunks_reassigned.is_empty()
            || !self.collectives_affected.is_empty()
    }
}

// ============================================================================
// Callback type
// ============================================================================

/// Callback invoked after each sweep that produces state changes.
///
/// The callback receives a reference to the [`FailureReport`] and runs
/// synchronously on the detector's task. Implementations should be lightweight
/// or spawn their own async work to avoid blocking the detection loop.
pub type FailureCallback = Arc<dyn Fn(&FailureReport) + Send + Sync>;

// ============================================================================
// WitnessStore
// ============================================================================

/// Stores recent witness reports from peers about node liveness.
///
/// For each subject node, we record which reporters have recently confirmed
/// seeing it (or not). The failure detector uses this to establish quorum
/// before declaring a node dead.
pub struct WitnessStore {
    /// subject_node_id -> Vec<(reporter_node_id, last_seen_timestamp)>
    witnesses: DashMap<NodeId, Vec<(NodeId, DateTime<Utc>)>>,
}

impl WitnessStore {
    /// Create a new, empty witness store.
    pub fn new() -> Self {
        Self {
            witnesses: DashMap::new(),
        }
    }

    /// Record a witness report: `reporter` claims to have last seen `subject`
    /// at `last_seen`.
    pub fn record(&self, report: &WitnessReport) {
        let mut entry = self.witnesses.entry(report.subject).or_default();
        // Update or insert the reporter's observation.
        if let Some(existing) = entry.iter_mut().find(|(r, _)| *r == report.reporter) {
            if report.last_seen > existing.1 {
                existing.1 = report.last_seen;
            }
        } else {
            entry.push((report.reporter, report.last_seen));
        }
    }

    /// Record a batch of witness reports (typically from a single gossip message).
    pub fn record_batch(&self, reports: &[WitnessReport]) {
        for report in reports {
            self.record(report);
        }
    }

    /// Count how many reporters have a recent witness report for `subject`
    /// where their `last_seen` is within `decay_secs` of `now`.
    ///
    /// "Recent" means the reporter's last_seen timestamp for this subject
    /// is within the decay window -- i.e., the reporter recently saw the
    /// subject alive.
    pub fn recent_witness_count(
        &self,
        subject: &NodeId,
        now: DateTime<Utc>,
        decay_secs: u64,
    ) -> usize {
        let decay = chrono::Duration::seconds(decay_secs as i64);
        match self.witnesses.get(subject) {
            Some(entry) => entry
                .iter()
                .filter(|(_, last_seen)| now.signed_duration_since(*last_seen) < decay)
                .count(),
            None => 0,
        }
    }

    /// Check whether quorum agrees a node is unreachable.
    ///
    /// Returns `true` if less than `quorum_fraction` of `active_peer_count`
    /// peers have recently seen the subject alive. In other words, if enough
    /// peers have NOT seen the subject recently, the quorum agrees the node
    /// is likely dead.
    pub fn quorum_agrees_unreachable(
        &self,
        subject: &NodeId,
        now: DateTime<Utc>,
        decay_secs: u64,
        active_peer_count: usize,
        quorum_fraction: f32,
    ) -> bool {
        if active_peer_count == 0 {
            // Hardening A.7: An isolated node (zero peers) must NOT declare deaths.
            // With no corroboration, we cannot form quorum. Previously this returned
            // `true`, which caused isolated nodes to falsely declare others dead.
            return false;
        }
        let recent_witnesses = self.recent_witness_count(subject, now, decay_secs);
        let recently_seen_fraction = recent_witnesses as f32 / active_peer_count as f32;
        // If few peers have recently seen the subject, the quorum agrees it is unreachable.
        recently_seen_fraction < (1.0 - quorum_fraction)
    }

    /// Prune witness entries for reporters that haven't reported in a while.
    pub fn prune_stale(&self, now: DateTime<Utc>, decay_secs: u64) {
        let decay = chrono::Duration::seconds(decay_secs as i64);
        self.witnesses.alter_all(|_, mut entries| {
            entries.retain(|(_, last_seen)| now.signed_duration_since(*last_seen) < decay);
            entries
        });
        // Remove subjects with no remaining witnesses.
        self.witnesses.retain(|_, entries| !entries.is_empty());
    }

    /// Returns the number of subjects being tracked.
    pub fn subject_count(&self) -> usize {
        self.witnesses.len()
    }
}

// ============================================================================
// PartitionDetector
// ============================================================================

/// Detects network partitions by tracking what fraction of previously-known
/// peers remain reachable.
///
/// The detector is updated during each failure detection sweep and exposes
/// the current [`PartitionStatus`] for use by the work engine.
pub struct PartitionDetector {
    /// Current partition status.
    status: RwLock<PartitionStatus>,
    /// Number of consecutive healthy rounds observed (for healing).
    consecutive_healthy_rounds: RwLock<u32>,
    /// Total known peers (set during sweep).
    total_known: RwLock<usize>,
    /// Reachable peers (set during sweep).
    reachable_count: RwLock<usize>,
}

impl PartitionDetector {
    /// Create a new partition detector in the Normal state.
    pub fn new() -> Self {
        Self {
            status: RwLock::new(PartitionStatus::Normal),
            consecutive_healthy_rounds: RwLock::new(0),
            total_known: RwLock::new(0),
            reachable_count: RwLock::new(0),
        }
    }

    /// Returns the current partition status.
    pub fn status(&self) -> PartitionStatus {
        *self.status.read()
    }

    /// Compute the fraction of known peers that are currently reachable.
    pub fn reachable_fraction(&self) -> f32 {
        let total = *self.total_known.read();
        if total == 0 {
            return 1.0;
        }
        *self.reachable_count.read() as f32 / total as f32
    }

    /// Update partition state based on current reachability observations.
    ///
    /// Called once per failure detection sweep with the counts of total known
    /// peers and currently reachable peers.
    pub fn update(
        &self,
        total_known: usize,
        reachable: usize,
        quorum_fraction: f32,
        recovery_rounds: u32,
    ) {
        *self.total_known.write() = total_known;
        *self.reachable_count.write() = reachable;

        // Don't trigger partition detection if we don't know enough peers.
        // A single-node swarm or a just-started node shouldn't be considered
        // partitioned.
        if total_known <= 1 {
            *self.status.write() = PartitionStatus::Normal;
            *self.consecutive_healthy_rounds.write() = 0;
            return;
        }

        let fraction = reachable as f32 / total_known as f32;

        if fraction >= quorum_fraction {
            // Healthy round.
            let mut rounds = self.consecutive_healthy_rounds.write();
            *rounds += 1;

            let current_status = *self.status.read();
            if current_status != PartitionStatus::Normal
                && *rounds >= recovery_rounds {
                    *self.status.write() = PartitionStatus::Normal;
                    *rounds = 0;
                    tracing::info!(
                        reachable = reachable,
                        total = total_known,
                        fraction = format!("{:.2}", fraction),
                        "Partition healed: {}/{} peers reachable",
                        reachable,
                        total_known,
                    );
                }
        } else {
            // Unhealthy round -- reset recovery counter.
            *self.consecutive_healthy_rounds.write() = 0;

            if fraction < 0.1 && total_known >= 100 {
                let was = *self.status.read();
                *self.status.write() = PartitionStatus::DeepWinter;
                if was != PartitionStatus::DeepWinter {
                    tracing::error!(
                        reachable = reachable,
                        total = total_known,
                        fraction = format!("{:.2}", fraction),
                        "DEEP WINTER EXTINCTION EVENT: Critical mass lost ({}%). Halting compute.",
                        (1.0 - fraction) * 100.0,
                    );
                }
            } else if fraction < 0.2 {
                let was = *self.status.read();
                *self.status.write() = PartitionStatus::Isolated;
                if was != PartitionStatus::Isolated {
                    tracing::error!(
                        reachable = reachable,
                        total = total_known,
                        fraction = format!("{:.2}", fraction),
                        "Network isolation detected: only {}/{} peers reachable",
                        reachable,
                        total_known,
                    );
                }
            } else {
                let was = *self.status.read();
                *self.status.write() = PartitionStatus::Degraded;
                if was == PartitionStatus::Normal {
                    tracing::warn!(
                        reachable = reachable,
                        total = total_known,
                        fraction = format!("{:.2}", fraction),
                        "Possible network partition: only {}/{} peers reachable",
                        reachable,
                        total_known,
                    );
                }
            }
        }
    }
}

// ============================================================================
// Detector statistics
// ============================================================================

/// Cumulative statistics tracked by the failure detector across all sweeps.
#[derive(Debug, Default, Clone)]
pub struct FailureDetectorStats {
    /// Total number of sweeps completed since the detector was created.
    pub sweeps_completed: u64,
    /// Total number of nodes that have been marked `Suspect` across all sweeps.
    pub total_suspects: u64,
    /// Total number of nodes that have been declared `Dead` across all sweeps.
    pub total_deaths: u64,
    /// Total number of chunks reassigned from dead nodes across all sweeps.
    pub total_chunks_reassigned: u64,
    /// Wall-clock duration of the most recent sweep, in microseconds.
    pub last_sweep_duration_us: u64,
}

// ============================================================================
// Failure detector
// ============================================================================

/// Gossip-absence-based failure detector for swarm nodes.
///
/// The detector does not send or receive any messages itself. It reads the
/// knowledge store (which is populated by the gossip protocol) and makes
/// decisions based on how recently each node was last seen.
///
/// # Usage
///
/// ```rust,ignore
/// let detector = Arc::new(FailureDetector::new(knowledge.clone(), my_node_id));
///
/// // One-shot sweep:
/// let report = detector.sweep();
///
/// // Or run continuously:
/// let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
/// let handle = detector.clone().spawn_loop(None, shutdown_rx);
///
/// // Later, to stop:
/// shutdown_tx.send(true).unwrap();
/// handle.await.unwrap();
/// ```
pub struct FailureDetector {
    /// Shared knowledge store containing all known node and chunk state.
    knowledge: Arc<KnowledgeStore>,
    /// This node's own identity (excluded from failure checks).
    self_id: NodeId,
    /// Duration of silence before a node is marked `Suspect`.
    suspect_threshold: Duration,
    /// Duration of silence before a node is declared `Dead`.
    dead_threshold: Duration,
    /// Interval between consecutive sweeps in the periodic loop.
    check_interval: Duration,
    /// Cumulative statistics, updated after each sweep.
    stats: Arc<RwLock<FailureDetectorStats>>,
    /// Optional collective store for collective-aware failure handling.
    collective_store: Option<Arc<CollectiveStore>>,
    /// Witness store for quorum-based death declarations.
    witness_store: Arc<WitnessStore>,
    /// Partition detector for split-brain protection.
    partition_detector: Arc<PartitionDetector>,

    // -- Observability & fleet integration (optional, wired via builders) --
    /// Event bus for emitting failure-related events (suspect, dead, recovery, partitions).
    event_bus: Option<Arc<super::events::EventBus>>,
    /// Prometheus metrics collector.
    metrics: Option<Arc<super::metrics::SwarmMetrics>>,
    /// Fleet manager for notifying about dead nodes and checking fleet state.
    fleet_manager: Option<Arc<super::fleet::FleetManager>>,
}

impl FailureDetector {
    /// Creates a new failure detector with default thresholds from [`super::config`].
    ///
    /// # Arguments
    ///
    /// * `knowledge` - Shared reference to the swarm knowledge store.
    /// * `self_id` - This node's identity. The detector will never suspect itself.
    pub fn new(knowledge: Arc<KnowledgeStore>, self_id: NodeId) -> Self {
        Self {
            knowledge,
            self_id,
            suspect_threshold: SUSPECT_THRESHOLD,
            dead_threshold: DEAD_THRESHOLD,
            check_interval: FAILURE_CHECK_INTERVAL,
            stats: Arc::new(RwLock::new(FailureDetectorStats::default())),
            collective_store: None,
            witness_store: Arc::new(WitnessStore::new()),
            partition_detector: Arc::new(PartitionDetector::new()),
            event_bus: None,
            metrics: None,
            fleet_manager: None,
        }
    }

    /// Attaches a [`CollectiveStore`] for collective-aware failure handling.
    ///
    /// When set, the detector will automatically remove dead nodes from their
    /// collectives during each sweep, dissolving collectives that drop below
    /// the minimum member threshold.
    pub fn with_collective_store(mut self, store: Arc<CollectiveStore>) -> Self {
        self.collective_store = Some(store);
        self
    }

    /// Overrides the default suspect and dead thresholds.
    ///
    /// # Panics
    ///
    /// Panics if `suspect >= dead` (the suspect threshold must be strictly
    /// less than the dead threshold for the state machine to make sense).
    pub fn with_thresholds(mut self, suspect: Duration, dead: Duration) -> Self {
        assert!(
            suspect < dead,
            "suspect threshold ({:?}) must be less than dead threshold ({:?})",
            suspect,
            dead,
        );
        self.suspect_threshold = suspect;
        self.dead_threshold = dead;
        self
    }

    /// Overrides the default check interval for the periodic loop.
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Wire in the event bus for emitting failure-related events (builder pattern).
    ///
    /// When set, the failure detector emits events on node state transitions
    /// (Alive->Suspect, Suspect->Dead, Dead->Alive recovery) and on
    /// partition detection/healing.
    pub fn with_event_bus(mut self, bus: Arc<super::events::EventBus>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    /// Wire in the Prometheus metrics collector (builder pattern).
    ///
    /// When set, each sweep updates node count gauges, partition metrics,
    /// and records sweep duration.
    pub fn with_metrics(mut self, metrics: Arc<super::metrics::SwarmMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Wire in the fleet manager for dead-node notifications (builder pattern).
    ///
    /// When set, the failure detector notifies the fleet manager when nodes
    /// are declared dead, allowing fleet operations to react (e.g., removing
    /// dead nodes from update plans).
    pub fn with_fleet_manager(mut self, fm: Arc<super::fleet::FleetManager>) -> Self {
        self.fleet_manager = Some(fm);
        self
    }

    /// Returns a reference to the witness store.
    ///
    /// The gossip engine uses this to feed incoming witness reports into
    /// the failure detector.
    pub fn witness_store(&self) -> &Arc<WitnessStore> {
        &self.witness_store
    }

    /// Returns a reference to the partition detector.
    ///
    /// The work engine uses this to check whether work claiming should
    /// be suppressed during a partition.
    pub fn partition_detector(&self) -> &Arc<PartitionDetector> {
        &self.partition_detector
    }

    /// Executes a single failure detection sweep across all known nodes.
    ///
    /// For each node (excluding self):
    /// - If the node has been silent for longer than `dead_threshold` and is
    ///   not already `Dead`:
    ///   - Check quorum: if enough peers agree the node is unreachable,
    ///     declare it dead and reassign its chunks.
    ///   - If quorum is not met, keep it as Suspect and log a message.
    /// - If the node has been silent for longer than `suspect_threshold` and
    ///   is currently `Alive`, it is marked as `Suspect`.
    ///
    /// Also updates the partition detector with current reachability data.
    ///
    /// Returns a [`FailureReport`] describing everything that changed.
    pub fn sweep(&self) -> FailureReport {
        let sweep_start = Instant::now();
        let now = Utc::now();
        let mut report = FailureReport::default();

        let nodes = self.knowledge.get_all_nodes();

        // Prune stale witness entries before evaluating.
        self.witness_store.prune_stale(now, WITNESS_DECAY_SECS);

        // Count active peers (non-self, non-dead) for quorum calculation.
        let active_peer_count = nodes
            .iter()
            .filter(|n| n.node_id != self.self_id && n.status != NodeStatus::Dead)
            .count();

        // Track reachable peers for partition detection.
        let dead_chrono = chrono::Duration::from_std(self.dead_threshold)
            .unwrap_or_else(|_| chrono::Duration::seconds(self.dead_threshold.as_secs() as i64));
        let suspect_chrono = chrono::Duration::from_std(self.suspect_threshold)
            .unwrap_or_else(|_| chrono::Duration::seconds(self.suspect_threshold.as_secs() as i64));

        let mut reachable_count: usize = 0;
        let mut total_non_self: usize = 0;

        for node in &nodes {
            // Never suspect ourselves.
            if node.node_id == self.self_id {
                continue;
            }

            total_non_self += 1;

            let silence = now.signed_duration_since(node.last_seen);

            if silence > dead_chrono && node.status != NodeStatus::Dead {
                // Node exceeds the dead threshold. Check quorum before declaring dead.
                let quorum_met = self.witness_store.quorum_agrees_unreachable(
                    &node.node_id,
                    now,
                    WITNESS_DECAY_SECS,
                    active_peer_count,
                    QUORUM_FRACTION,
                );

                if quorum_met {
                    // Quorum agrees -- declare dead.
                    self.knowledge.mark_node_dead(&node.node_id);
                    report.nodes_declared_dead.push(node.node_id);

                    let released = self.knowledge.release_chunks_from_node(&node.node_id);
                    let released_count = released.len();
                    report.chunks_reassigned.extend(released);

                    tracing::warn!(
                        node = %node.node_id,
                        silence_secs = silence.num_seconds(),
                        chunks_released = released_count,
                        active_peers = active_peer_count,
                        "Node declared dead (quorum agreed), reassigned {} chunks",
                        released_count,
                    );

                    // Emit event: node declared dead.
                    if let Some(ref bus) = self.event_bus {
                        bus.emit_with_entities(
                            super::complexity::ConcernDomain::Health,
                            super::complexity::EventSeverity::Error,
                            format!(
                                "Node {} declared dead after {}s silence, {} chunks reassigned",
                                node.node_id, silence.num_seconds(), released_count,
                            ),
                            vec![super::events::EntityRef::node(&node.node_id)],
                            serde_json::json!({
                                "node_id": node.node_id.to_string(),
                                "silence_secs": silence.num_seconds(),
                                "chunks_released": released_count,
                                "active_peers": active_peer_count,
                            }),
                        );
                    }

                    // Notify fleet manager about the dead node.
                    if let Some(ref fm) = self.fleet_manager {
                        let _ = fm.quarantine_node(
                            node.node_id,
                            format!("declared dead after {}s silence", silence.num_seconds()),
                            Some("failure_detector".to_string()),
                        );
                    }
                } else {
                    // Insufficient quorum -- keep as Suspect.
                    if node.status == NodeStatus::Alive {
                        self.knowledge.mark_node_suspect(&node.node_id);
                        report.nodes_suspected.push(node.node_id);

                        // Emit event: Alive -> Suspect transition.
                        if let Some(ref bus) = self.event_bus {
                            bus.emit_with_entities(
                                super::complexity::ConcernDomain::Health,
                                super::complexity::EventSeverity::Warning,
                                format!(
                                    "Node {} suspected ({}s silence, quorum not met for death)",
                                    node.node_id, silence.num_seconds(),
                                ),
                                vec![super::events::EntityRef::node(&node.node_id)],
                                serde_json::json!({
                                    "node_id": node.node_id.to_string(),
                                    "silence_secs": silence.num_seconds(),
                                    "active_peers": active_peer_count,
                                    "transition": "alive_to_suspect",
                                }),
                            );
                        }
                    }

                    tracing::info!(
                        node = %node.node_id,
                        silence_secs = silence.num_seconds(),
                        active_peers = active_peer_count,
                        "insufficient quorum for death declaration of {}",
                        node.node_id,
                    );
                }
            } else if silence > suspect_chrono && node.status == NodeStatus::Alive {
                // Node is suspect: mark it but do not reassign work yet.
                self.knowledge.mark_node_suspect(&node.node_id);
                report.nodes_suspected.push(node.node_id);

                tracing::info!(
                    node = %node.node_id,
                    silence_secs = silence.num_seconds(),
                    "Node suspected",
                );

                // Emit event: Alive -> Suspect transition.
                if let Some(ref bus) = self.event_bus {
                    bus.emit_with_entities(
                        super::complexity::ConcernDomain::Health,
                        super::complexity::EventSeverity::Warning,
                        format!(
                            "Node {} suspected after {}s silence",
                            node.node_id, silence.num_seconds(),
                        ),
                        vec![super::events::EntityRef::node(&node.node_id)],
                        serde_json::json!({
                            "node_id": node.node_id.to_string(),
                            "silence_secs": silence.num_seconds(),
                            "transition": "alive_to_suspect",
                        }),
                    );
                }
            }

            // Track reachability: a node is "reachable" if it has been heard
            // from within the dead threshold.
            if silence <= dead_chrono {
                reachable_count += 1;
            }

            // Count totals using the *effective* status after this sweep.
            // If we just transitioned the node, use the new status.
            let effective_status = if report.nodes_declared_dead.contains(&node.node_id) {
                NodeStatus::Dead
            } else if report.nodes_suspected.contains(&node.node_id) {
                NodeStatus::Suspect
            } else {
                node.status
            };

            match effective_status {
                NodeStatus::Alive => report.total_alive += 1,
                NodeStatus::Suspect => report.total_suspect += 1,
                NodeStatus::Dead => report.total_dead += 1,
                // Fleet-managed states are counted as alive for failure
                // detection purposes (they are not failed nodes).
                NodeStatus::Draining
                | NodeStatus::Cordoned
                | NodeStatus::Quarantined
                | NodeStatus::Updating => report.total_alive += 1,
            }
        }

        // Capture the partition status before the update for transition detection.
        let partition_status_before = self.partition_detector.status();

        // Update partition detector.
        self.partition_detector.update(
            total_non_self,
            reachable_count,
            QUORUM_FRACTION,
            PARTITION_RECOVERY_ROUNDS,
        );

        // Detect partition status transitions and emit events.
        let partition_status_after = self.partition_detector.status();
        if partition_status_before != partition_status_after {
            if let Some(ref bus) = self.event_bus {
                if partition_status_after == PartitionStatus::Normal
                    && partition_status_before != PartitionStatus::Normal
                {
                    // Partition healed.
                    bus.emit_simple(
                        super::complexity::ConcernDomain::Health,
                        super::complexity::EventSeverity::Info,
                        format!(
                            "Partition healed: {}/{} peers reachable (was {:?})",
                            reachable_count, total_non_self, partition_status_before,
                        ),
                    );
                } else if partition_status_before == PartitionStatus::Normal {
                    // Partition detected (Normal -> Degraded or Normal -> Isolated).
                    bus.emit_simple(
                        super::complexity::ConcernDomain::Health,
                        super::complexity::EventSeverity::Error,
                        format!(
                            "Partition detected: {}/{} peers reachable, status={:?}",
                            reachable_count, total_non_self, partition_status_after,
                        ),
                    );
                } else {
                    // Partition status changed (Degraded -> Isolated or vice versa).
                    bus.emit_simple(
                        super::complexity::ConcernDomain::Health,
                        super::complexity::EventSeverity::Warning,
                        format!(
                            "Partition status changed: {:?} -> {:?} ({}/{} peers reachable)",
                            partition_status_before, partition_status_after,
                            reachable_count, total_non_self,
                        ),
                    );
                }
            }
        }

        // Update partition metrics.
        if let Some(ref m) = self.metrics {
            let active_partitions = match partition_status_after {
                PartitionStatus::Normal => 0,
                PartitionStatus::Degraded | PartitionStatus::Isolated | PartitionStatus::DeepWinter => 1,
            };
            m.partitions_active.set(active_partitions);
            m.partition_reachable_fraction
                .set(self.partition_detector.reachable_fraction() as f64);
        }

        // Handle collective member deaths
        if let Some(ref cs) = self.collective_store {
            for dead_id in &report.nodes_declared_dead {
                // Find all collectives this dead node belongs to
                let affected = cs.collectives_for_node(dead_id);
                for cid in affected {
                    if let Some(mut collective) = cs.get(&cid) {
                        collective.remove_member(dead_id);
                        if collective.should_dissolve() {
                            cs.remove(&cid);
                            tracing::info!(
                                collective_id = %cid,
                                dead_node = %dead_id,
                                "Collective {} dissolved due to member death: {}", cid, dead_id,
                            );
                        } else {
                            collective.touch();
                            cs.upsert(collective);
                            tracing::info!(
                                collective_id = %cid,
                                dead_node = %dead_id,
                                "Collective {} degraded, member {} declared dead", cid, dead_id,
                            );
                        }
                        report.collectives_affected.push(cid);
                    }
                }
            }
        }

        // Update cumulative stats.
        let sweep_duration = sweep_start.elapsed();
        {
            let mut stats = self.stats.write();
            stats.sweeps_completed += 1;
            stats.total_suspects += report.nodes_suspected.len() as u64;
            stats.total_deaths += report.nodes_declared_dead.len() as u64;
            stats.total_chunks_reassigned += report.chunks_reassigned.len() as u64;
            stats.last_sweep_duration_us = sweep_duration.as_micros() as u64;
        }

        // Update node count metrics if metrics collector is available.
        if let Some(ref m) = self.metrics {
            m.update_node_counts(
                report.total_alive as i64,
                report.total_suspect as i64,
                report.total_dead as i64,
                0, // draining -- not tracked by failure detector
                0, // cordoned
                0, // quarantined
                0, // updating
            );
        }

        report
    }

    /// Spawns the periodic failure detection loop as a Tokio task.
    ///
    /// The loop runs until the `shutdown` receiver signals `true`, executing
    /// a sweep every `check_interval`. If a `callback` is provided, it is
    /// invoked after every sweep that produces at least one state change.
    ///
    /// # Arguments
    ///
    /// * `callback` - Optional callback invoked when a sweep has changes.
    /// * `shutdown` - Watch receiver; the loop exits when this becomes `true`.
    ///
    /// # Returns
    ///
    /// A [`JoinHandle`](tokio::task::JoinHandle) for the spawned task.
    pub fn spawn_loop(
        self: Arc<Self>,
        callback: Option<FailureCallback>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let check_interval = self.check_interval;

        tokio::spawn(async move {
            let mut sweep_count: u64 = 0;

            tracing::info!(
                self_id = %self.self_id,
                suspect_threshold_ms = self.suspect_threshold.as_millis() as u64,
                dead_threshold_ms = self.dead_threshold.as_millis() as u64,
                check_interval_ms = check_interval.as_millis() as u64,
                "Failure detection loop started",
            );

            loop {
                // Check for shutdown before sleeping.
                tokio::select! {
                    _ = tokio::time::sleep(check_interval) => {}
                    result = shutdown.changed() => {
                        // Channel closed or value changed.
                        if result.is_err() || *shutdown.borrow() {
                            tracing::info!("Failure detection loop shutting down");
                            break;
                        }
                    }
                }

                // Double-check shutdown after waking (in case both branches were ready).
                if *shutdown.borrow() {
                    tracing::info!("Failure detection loop shutting down");
                    break;
                }

                // Run the sweep.
                let report = self.sweep();
                sweep_count += 1;

                // Invoke the callback if the sweep produced changes.
                if report.has_changes() {
                    if let Some(ref cb) = callback {
                        cb(&report);
                    }
                }

                // Log a summary every 10 sweeps at debug level.
                if sweep_count % 10 == 0 {
                    let stats = self.stats.read().clone();
                    tracing::debug!(
                        sweep_count,
                        alive = report.total_alive,
                        suspect = report.total_suspect,
                        dead = report.total_dead,
                        cumulative_suspects = stats.total_suspects,
                        cumulative_deaths = stats.total_deaths,
                        cumulative_chunks_reassigned = stats.total_chunks_reassigned,
                        last_sweep_us = stats.last_sweep_duration_us,
                        "Failure detector periodic summary",
                    );
                }
            }

            tracing::info!(
                sweeps_completed = sweep_count,
                "Failure detection loop exited",
            );
        })
    }

    /// Returns a snapshot of the cumulative failure detector statistics.
    pub fn stats(&self) -> FailureDetectorStats {
        self.stats.read().clone()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::time::Duration;

    use super::super::types::{NodeInfo, NodeStatus, ResourceSnapshot};

    /// Minimal stub of `KnowledgeStore` for testing the failure detector
    /// in isolation. This is *not* the real knowledge store — it provides
    /// just enough to exercise the detector's logic.
    mod mock_knowledge {
        use super::*;
        use parking_lot::RwLock;
        use std::collections::HashMap;

        pub struct MockKnowledgeStore {
            nodes: RwLock<Vec<NodeInfo>>,
            released_chunks: RwLock<HashMap<NodeId, Vec<ChunkId>>>,
        }

        impl MockKnowledgeStore {
            pub fn new(nodes: Vec<NodeInfo>) -> Self {
                Self {
                    nodes: RwLock::new(nodes),
                    released_chunks: RwLock::new(HashMap::new()),
                }
            }

            pub fn set_release_chunks(&self, node_id: NodeId, chunks: Vec<ChunkId>) {
                self.released_chunks.write().insert(node_id, chunks);
            }

            pub fn get_node_status(&self, node_id: &NodeId) -> Option<NodeStatus> {
                self.nodes
                    .read()
                    .iter()
                    .find(|n| n.node_id == *node_id)
                    .map(|n| n.status)
            }
        }

        // We cannot implement methods on the real KnowledgeStore here since
        // it does not exist yet. Instead, the tests below will only compile
        // when the full swarm module is available. For now, we document the
        // expected test behavior.
    }

    fn make_node(id: NodeId, last_seen: chrono::DateTime<Utc>, status: NodeStatus) -> NodeInfo {
        NodeInfo {
            node_id: id,
            last_seen,
            traits: Default::default(),
            load: 0.0,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: id,
            status,
            generation: 1,
            trust_level: Default::default(),
        }
    }

    // NOTE: Full integration tests require the `KnowledgeStore` implementation
    // from `super::knowledge`. These unit tests validate the report structure
    // and threshold logic once that module is available.

    #[test]
    fn failure_report_has_changes_empty() {
        let report = FailureReport::default();
        assert!(!report.has_changes());
    }

    #[test]
    fn failure_report_has_changes_with_suspects() {
        let mut report = FailureReport::default();
        report.nodes_suspected.push(NodeId::new());
        assert!(report.has_changes());
    }

    #[test]
    fn failure_report_has_changes_with_dead() {
        let mut report = FailureReport::default();
        report.nodes_declared_dead.push(NodeId::new());
        assert!(report.has_changes());
    }

    #[test]
    fn failure_report_has_changes_with_chunks() {
        let mut report = FailureReport::default();
        report.chunks_reassigned.push(ChunkId::new());
        assert!(report.has_changes());
    }

    #[test]
    fn stats_default_is_zero() {
        let stats = FailureDetectorStats::default();
        assert_eq!(stats.sweeps_completed, 0);
        assert_eq!(stats.total_suspects, 0);
        assert_eq!(stats.total_deaths, 0);
        assert_eq!(stats.total_chunks_reassigned, 0);
        assert_eq!(stats.last_sweep_duration_us, 0);
    }

    #[test]
    #[should_panic(expected = "suspect threshold")]
    fn with_thresholds_panics_if_suspect_ge_dead() {
        let knowledge = Arc::new(KnowledgeStore::new(NodeId::new()));
        let detector = FailureDetector::new(knowledge, NodeId::new());
        // suspect == dead should panic
        let _ = detector.with_thresholds(Duration::from_secs(10), Duration::from_secs(10));
    }

    #[test]
    fn with_thresholds_accepts_valid_values() {
        let knowledge = Arc::new(KnowledgeStore::new(NodeId::new()));
        let detector = FailureDetector::new(knowledge, NodeId::new())
            .with_thresholds(Duration::from_secs(5), Duration::from_secs(15));
        assert_eq!(detector.suspect_threshold, Duration::from_secs(5));
        assert_eq!(detector.dead_threshold, Duration::from_secs(15));
    }

    #[test]
    fn with_check_interval_overrides_default() {
        let knowledge = Arc::new(KnowledgeStore::new(NodeId::new()));
        let detector = FailureDetector::new(knowledge, NodeId::new())
            .with_check_interval(Duration::from_millis(500));
        assert_eq!(detector.check_interval, Duration::from_millis(500));
    }

    // ------------------------------------------------------------------------
    // WitnessStore tests
    // ------------------------------------------------------------------------

    #[test]
    fn witness_store_record_and_count() {
        let store = WitnessStore::new();
        let subject = NodeId::new();
        let reporter1 = NodeId::new();
        let reporter2 = NodeId::new();
        let now = Utc::now();

        store.record(&WitnessReport {
            reporter: reporter1,
            subject,
            last_seen: now,
            signature: vec![],
        });
        store.record(&WitnessReport {
            reporter: reporter2,
            subject,
            last_seen: now,
            signature: vec![],
        });

        assert_eq!(store.recent_witness_count(&subject, now, 60), 2);
        assert_eq!(store.subject_count(), 1);
    }

    #[test]
    fn witness_store_decay() {
        let store = WitnessStore::new();
        let subject = NodeId::new();
        let reporter = NodeId::new();
        let old_time = Utc::now() - chrono::Duration::seconds(120);

        store.record(&WitnessReport {
            reporter,
            subject,
            last_seen: old_time,
            signature: vec![],
        });

        // With a 60-second decay, the old report should not count.
        let now = Utc::now();
        assert_eq!(store.recent_witness_count(&subject, now, 60), 0);
    }

    #[test]
    fn witness_store_prune_stale() {
        let store = WitnessStore::new();
        let subject = NodeId::new();
        let reporter = NodeId::new();
        let old_time = Utc::now() - chrono::Duration::seconds(120);

        store.record(&WitnessReport {
            reporter,
            subject,
            last_seen: old_time,
            signature: vec![],
        });

        assert_eq!(store.subject_count(), 1);
        store.prune_stale(Utc::now(), 60);
        assert_eq!(store.subject_count(), 0);
    }

    #[test]
    fn witness_store_update_existing_reporter() {
        let store = WitnessStore::new();
        let subject = NodeId::new();
        let reporter = NodeId::new();
        let old_time = Utc::now() - chrono::Duration::seconds(30);
        let new_time = Utc::now();

        store.record(&WitnessReport {
            reporter,
            subject,
            last_seen: old_time,
            signature: vec![],
        });
        store.record(&WitnessReport {
            reporter,
            subject,
            last_seen: new_time,
            signature: vec![],
        });

        // Should have only 1 entry, not 2 (updated, not duplicated).
        assert_eq!(store.recent_witness_count(&subject, new_time, 60), 1);
    }

    #[test]
    fn witness_store_quorum_agrees_unreachable() {
        let store = WitnessStore::new();
        let subject = NodeId::new();
        let now = Utc::now();

        // No witnesses at all -- quorum should agree the node is unreachable.
        assert!(store.quorum_agrees_unreachable(&subject, now, 60, 10, 0.5));

        // Add enough recent witnesses that prove the node IS reachable.
        // If 6 out of 10 peers recently saw the subject, then
        // recently_seen_fraction = 0.6, which is >= (1 - 0.5) = 0.5,
        // so quorum does NOT agree the node is unreachable.
        for _ in 0..6 {
            let reporter = NodeId::new();
            store.record(&WitnessReport {
                reporter,
                subject,
                last_seen: now,
                signature: vec![],
            });
        }
        assert!(!store.quorum_agrees_unreachable(&subject, now, 60, 10, 0.5));

        // With only 3 out of 10 recent witnesses, recently_seen_fraction = 0.3,
        // which is < (1 - 0.5) = 0.5, so quorum DOES agree unreachable.
        let store2 = WitnessStore::new();
        let subject2 = NodeId::new();
        for _ in 0..3 {
            let reporter = NodeId::new();
            store2.record(&WitnessReport {
                reporter,
                subject: subject2,
                last_seen: now,
                signature: vec![],
            });
        }
        assert!(store2.quorum_agrees_unreachable(&subject2, now, 60, 10, 0.5));
    }

    #[test]
    fn witness_store_empty_active_peers() {
        let store = WitnessStore::new();
        let subject = NodeId::new();
        let now = Utc::now();
        // Hardening A.7: With zero active peers, should return false.
        // An isolated node must NOT declare deaths — no quorum is possible.
        assert!(!store.quorum_agrees_unreachable(&subject, now, 60, 0, 0.5));
    }

    // ------------------------------------------------------------------------
    // PartitionDetector tests
    // ------------------------------------------------------------------------

    #[test]
    fn partition_detector_starts_normal() {
        let pd = PartitionDetector::new();
        assert_eq!(pd.status(), PartitionStatus::Normal);
        assert!((pd.reachable_fraction() - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn partition_detector_triggers_degraded() {
        let pd = PartitionDetector::new();
        // 3 out of 10 reachable = 0.3, which is < 0.5 but >= 0.2
        pd.update(10, 3, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Degraded);
    }

    #[test]
    fn partition_detector_triggers_isolated() {
        let pd = PartitionDetector::new();
        // 1 out of 10 reachable = 0.1, which is < 0.2
        pd.update(10, 1, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Isolated);
    }

    #[test]
    fn partition_detector_healing() {
        let pd = PartitionDetector::new();
        // Go degraded first.
        pd.update(10, 3, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Degraded);

        // One healthy round -- not enough for recovery (needs 2).
        pd.update(10, 8, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Degraded);

        // Second healthy round -- should heal.
        pd.update(10, 8, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Normal);
    }

    #[test]
    fn partition_detector_healing_interrupted() {
        let pd = PartitionDetector::new();
        // Go degraded.
        pd.update(10, 3, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Degraded);

        // One healthy round.
        pd.update(10, 8, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Degraded);

        // Unhealthy round interrupts recovery.
        pd.update(10, 3, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Degraded);

        // Need two consecutive healthy rounds from scratch now.
        pd.update(10, 8, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Degraded);
        pd.update(10, 8, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Normal);
    }

    #[test]
    fn partition_detector_single_node_stays_normal() {
        let pd = PartitionDetector::new();
        // total_known <= 1 should always be Normal.
        pd.update(1, 0, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Normal);
        pd.update(0, 0, 0.5, 2);
        assert_eq!(pd.status(), PartitionStatus::Normal);
    }

    #[test]
    fn partition_detector_reachable_fraction() {
        let pd = PartitionDetector::new();
        pd.update(10, 7, 0.5, 2);
        let frac = pd.reachable_fraction();
        assert!((frac - 0.7).abs() < f32::EPSILON);
    }

    // ------------------------------------------------------------------------
    // Quorum prevents premature death declaration (integration-level)
    // ------------------------------------------------------------------------

    #[test]
    fn quorum_prevents_premature_death_declaration() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let detector = FailureDetector::new(knowledge.clone(), self_id)
            .with_thresholds(Duration::from_secs(5), Duration::from_secs(10));

        // Add a node that has been silent long enough to be declared dead.
        let dead_candidate = NodeId::new();
        let old_time = Utc::now() - chrono::Duration::seconds(15);
        knowledge.merge_node(NodeInfo {
            node_id: dead_candidate,
            last_seen: old_time,
            traits: Default::default(),
            load: 0.0,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: dead_candidate,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        });

        // Add several alive nodes that recently witnessed the dead_candidate.
        // This means the majority has recently seen the candidate, so quorum
        // should NOT agree the node is unreachable.
        for i in 0..5 {
            let peer_id = NodeId::new();
            knowledge.merge_node(NodeInfo {
                node_id: peer_id,
                last_seen: Utc::now(),
                traits: Default::default(),
                load: 0.0,
                capacity: ResourceSnapshot::default(),
                address: Some(format!("10.0.0.{}:4200", i + 1).parse().unwrap()),
                via: peer_id,
                status: NodeStatus::Alive,
                generation: 1,
                trust_level: Default::default(),
            });

            // These peers recently saw the dead_candidate.
            detector.witness_store().record(&WitnessReport {
                reporter: peer_id,
                subject: dead_candidate,
                last_seen: Utc::now(),
                signature: vec![],
            });
        }

        // Sweep: the node exceeds the dead threshold, but quorum says
        // it's still reachable (5 out of 6 active peers saw it recently).
        let report = detector.sweep();
        // The node should NOT be declared dead -- quorum prevents it.
        assert!(
            !report.nodes_declared_dead.contains(&dead_candidate),
            "node should not be declared dead when quorum witnesses say it's reachable"
        );
        // It should be moved to Suspect instead.
        assert!(
            report.nodes_suspected.contains(&dead_candidate),
            "node should be suspected when quorum prevents death declaration"
        );
    }

    #[test]
    fn quorum_allows_death_declaration_when_majority_agrees() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let detector = FailureDetector::new(knowledge.clone(), self_id)
            .with_thresholds(Duration::from_secs(5), Duration::from_secs(10));

        // Add a node that has been silent long enough to be declared dead.
        let dead_candidate = NodeId::new();
        let old_time = Utc::now() - chrono::Duration::seconds(15);
        knowledge.merge_node(NodeInfo {
            node_id: dead_candidate,
            last_seen: old_time,
            traits: Default::default(),
            load: 0.0,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: dead_candidate,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        });

        // Add several alive nodes but DO NOT add witness reports for
        // the dead_candidate. This means no one has recently seen it.
        for i in 0..5 {
            let peer_id = NodeId::new();
            knowledge.merge_node(NodeInfo {
                node_id: peer_id,
                last_seen: Utc::now(),
                traits: Default::default(),
                load: 0.0,
                capacity: ResourceSnapshot::default(),
                address: Some(format!("10.0.0.{}:4200", i + 1).parse().unwrap()),
                via: peer_id,
                status: NodeStatus::Alive,
                generation: 1,
                trust_level: Default::default(),
            });
        }

        // Sweep: quorum agrees the node is unreachable (no witnesses).
        let report = detector.sweep();
        assert!(
            report.nodes_declared_dead.contains(&dead_candidate),
            "node should be declared dead when quorum agrees it is unreachable"
        );
    }

    #[test]
    fn witness_report_propagation_via_record_batch() {
        let store = WitnessStore::new();
        let subject1 = NodeId::new();
        let subject2 = NodeId::new();
        let reporter1 = NodeId::new();
        let reporter2 = NodeId::new();
        let now = Utc::now();

        let reports = vec![
            WitnessReport { reporter: reporter1, subject: subject1, last_seen: now, signature: vec![] },
            WitnessReport { reporter: reporter2, subject: subject1, last_seen: now, signature: vec![] },
            WitnessReport { reporter: reporter1, subject: subject2, last_seen: now, signature: vec![] },
        ];

        store.record_batch(&reports);
        assert_eq!(store.subject_count(), 2);
        assert_eq!(store.recent_witness_count(&subject1, now, 60), 2);
        assert_eq!(store.recent_witness_count(&subject2, now, 60), 1);
    }

    #[test]
    fn partition_status_display() {
        assert_eq!(format!("{}", PartitionStatus::Normal), "Normal");
        assert_eq!(format!("{}", PartitionStatus::Degraded), "Degraded");
        assert_eq!(format!("{}", PartitionStatus::Isolated), "Isolated");
    }

    #[test]
    fn partition_status_default_is_normal() {
        assert_eq!(PartitionStatus::default(), PartitionStatus::Normal);
    }
}
