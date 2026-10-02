// Marabunta - Licensed under the MIT License.
//! Prometheus metrics for the Marabunta Swarm.
//!
//! This module defines [`SwarmMetrics`], a self-contained Prometheus metrics
//! collector that covers every operational concern domain of the swarm:
//! health, work, data, fleet, security, cost, psyche, operational, and
//! multi-swarm. All metrics use the `swarm_` prefix.
//!
//! Metrics are registered against a **custom** [`prometheus::Registry`]
//! (not the global default) so that multiple swarm nodes or tests can
//! coexist in the same process without conflicts.
//!
//! The companion [`MetricsSnapshot`] and [`MetricsSnapshotter`] types
//! capture point-in-time snapshots for dashboards and diffing, while
//! [`PercentileCalculator`] extracts p50/p90/p95/p99 from histogram data.

use std::collections::HashMap;
use std::sync::Arc;

use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
    core::Metric,
};
use serde::{Deserialize, Serialize};

// ============================================================================
// Histogram bucket definitions
// ============================================================================

/// Histogram buckets for gossip round-trip time (seconds).
const GOSSIP_RTT_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

/// Histogram buckets for chunk execution duration (seconds).
const CHUNK_DURATION_BUCKETS: &[f64] = &[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0];

/// Histogram buckets for API request duration (seconds).
const API_DURATION_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0];

/// Histogram buckets for psyche computation duration (seconds).
const PSYCHE_DURATION_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0];

/// Histogram buckets for blind pipeline durations (seconds) — powers of 2.
/// Anti-timing-leakage: quantized to reduce information leaked via observation.
const BLIND_DURATION_BUCKETS: &[f64] = &[
    0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024,
];

/// Histogram buckets for blind output sizes (bytes) — powers of 2.
const BLIND_OUTPUT_BYTES_BUCKETS: &[f64] = &[
    256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0, 131072.0,
];

/// Histogram buckets for blind verification replica counts — powers of 2.
const BLIND_REPLICA_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0];

// ============================================================================
// SwarmMetrics
// ============================================================================

/// Prometheus metrics for every concern domain of the swarm.
///
/// Each `SwarmMetrics` instance owns a private [`Registry`] so metrics
/// from different nodes or tests do not collide. Use [`encode`](Self::encode)
/// to produce Prometheus text exposition format.
///
/// # Example
///
/// ```rust,no_run
/// use marabunta_compute::swarm::metrics::SwarmMetrics;
///
/// let m = SwarmMetrics::new();
/// m.update_node_counts(5, 1, 0, 0, 0, 0, 0);
/// let text = m.encode();
/// assert!(text.contains("swarm_nodes_total"));
/// ```
pub struct SwarmMetrics {
    /// Custom registry (not the process-global one).
    registry: Registry,

    // ---- Health domain ----
    /// Gauge of nodes by status: alive, suspect, dead, draining, cordoned, quarantined, updating.
    pub nodes_total: IntGaugeVec,
    /// Counter of gossip messages by direction: sent, received.
    pub gossip_messages_total: IntCounterVec,
    /// Histogram of gossip round-trip time by peer_region.
    pub gossip_rtt_seconds: HistogramVec,
    /// Number of peers selected in the most recent gossip round.
    pub gossip_peers_selected: IntGauge,
    /// Number of currently active network partitions.
    pub partitions_active: IntGauge,
    /// Fraction of known peers that are reachable (0.0 to 1.0).
    pub partition_reachable_fraction: Gauge,
    /// Monotonically increasing generation counter for this node.
    pub node_generation: IntGauge,

    // ---- Work domain ----
    /// Gauge of jobs by status: pending, running, completed, failed, cancelled.
    pub jobs_total: IntGaugeVec,
    /// Gauge of chunks by status: pending, executing, completed, failed.
    pub chunks_total: IntGaugeVec,
    /// Counter of chunk executions by result and type.
    pub chunks_executed_total: IntCounterVec,
    /// Histogram of chunk execution duration by type.
    pub chunk_duration_seconds: HistogramVec,
    /// Number of chunks currently waiting for execution.
    pub pending_chunks: IntGauge,
    /// Number of chunks currently being executed.
    pub active_chunks: IntGauge,
    /// Total number of jobs submitted since startup.
    pub job_submission_total: IntCounter,
    /// Current job completion rate (jobs completed / total jobs, 0.0 to 1.0).
    pub job_completion_rate: Gauge,

    // ---- Data domain ----
    /// Total bytes stored in the blob store.
    pub blob_store_bytes: IntGauge,
    /// Total number of blobs in the store.
    pub blob_store_count: IntGauge,
    /// Configured maximum capacity of the blob store in bytes.
    pub blob_store_capacity_bytes: IntGauge,
    /// Counter of data residency violations by policy and region.
    pub residency_violations_total: IntCounterVec,
    /// Counter of blob transfers by direction: inbound, outbound.
    pub blob_transfers_total: IntCounterVec,
    /// Counter of bytes transferred by direction.
    pub blob_transfer_bytes_total: IntCounterVec,

    // ---- Fleet domain ----
    /// Number of nodes in draining state.
    pub fleet_nodes_draining: IntGauge,
    /// Number of nodes in cordoned state.
    pub fleet_nodes_cordoned: IntGauge,
    /// Number of nodes in quarantined state.
    pub fleet_nodes_quarantined: IntGauge,
    /// Number of nodes currently performing a rolling update.
    pub fleet_nodes_updating: IntGauge,
    /// Progress of the current fleet update (0.0 to 1.0).
    pub fleet_update_progress: Gauge,
    /// Whether a fleet update is currently active (1 = yes, 0 = no).
    pub fleet_update_active: IntGauge,
    /// Number of nodes waiting for admission.
    pub admission_pending: IntGauge,
    /// Number of nodes currently in probation.
    pub admission_probation: IntGauge,
    /// Counter of admission decisions by result: admitted, rejected, timeout.
    pub admission_total: IntCounterVec,
    /// Gauge of nodes by hardware class.
    pub hardware_class_nodes: IntGaugeVec,

    // ---- Security domain ----
    /// Counter of auth requests by result: allowed, denied.
    pub auth_requests_total: IntCounterVec,
    /// Number of currently active API tokens.
    pub active_tokens: IntGauge,
    /// Counter of policy violations by policy type.
    pub policy_violations_total: IntCounterVec,
    /// Counter of policy evaluations by result: pass, fail.
    pub policy_evaluations_total: IntCounterVec,

    // ---- Cost domain ----
    /// Total bids placed since startup.
    pub marketplace_bids_total: IntCounter,
    /// Total awards made since startup.
    pub marketplace_awards_total: IntCounter,
    /// Ratio of idle nodes (0.0 = all busy, 1.0 = all idle).
    pub idle_node_ratio: Gauge,
    /// Estimated energy cost in USD for the current billing period.
    pub energy_cost_estimate_usd: Gauge,

    // ---- Psyche domain ----
    /// Psyche facet intensities (0 to 4) by facet name.
    pub psyche_facet: IntGaugeVec,
    /// Active psyche archetype (1 if active, 0 otherwise) by name.
    pub psyche_archetype: IntGaugeVec,
    /// Duration of psyche computation cycles.
    pub psyche_computation_duration_seconds: Histogram,

    // ---- Operational domain ----
    /// Uptime of this node in seconds.
    pub uptime_seconds: Gauge,
    /// Total events emitted to the event bus since startup.
    pub event_bus_emitted_total: IntCounter,
    /// Total events dropped by the event bus since startup.
    pub event_bus_dropped_total: IntCounter,
    /// Counter of API requests by method, path, and status code.
    pub api_request_total: IntCounterVec,
    /// Histogram of API request duration by method and path.
    pub api_request_duration_seconds: HistogramVec,

    // ---- Blind computation domain ----
    /// Counter of blind computations by outcome (success, failure).
    pub blind_executions_total: IntCounterVec,
    /// Histogram of blind decryption duration in seconds (power-of-2 buckets).
    pub blind_decryption_seconds: Histogram,
    /// Histogram of blind sandbox setup duration in seconds (power-of-2 buckets).
    pub blind_sandbox_setup_seconds: Histogram,
    /// Histogram of blind memory scrub duration in seconds (power-of-2 buckets).
    pub blind_scrub_seconds: Histogram,
    /// Histogram of blind verification replica counts (power-of-2 buckets).
    pub blind_verification_replicas: Histogram,
    /// Histogram of blind padded output sizes in bytes (power-of-2 buckets).
    pub blind_padded_output_bytes: Histogram,

    // ---- Multi-swarm domain ----
    /// Counter of membrane crossings by membrane_id, direction, and category.
    pub membrane_crossings_total: IntCounterVec,
    /// Counter of bytes crossing membranes by membrane_id and direction.
    pub membrane_crossing_bytes_total: IntCounterVec,
    /// Status of membranes by membrane_id (1 = open, 0 = closed).
    pub membrane_status: IntGaugeVec,
    /// Number of active inter-swarm treaties.
    pub active_treaties: IntGauge,
    /// Percentage of local capacity being lent to other swarms (0.0 to 1.0).
    pub capacity_lending_pct: Gauge,
}

impl SwarmMetrics {
    /// Create a new `SwarmMetrics` instance with a custom registry and all
    /// metrics registered.
    ///
    /// All metric names use the `swarm_` prefix via the custom registry
    /// prefix mechanism.
    pub fn new() -> Self {
        let registry = Registry::new_custom(Some("swarm".to_string()), None)
            .expect("custom registry creation must succeed");

        // ---- Health domain ----
        let nodes_total = IntGaugeVec::new(
            Opts::new("nodes_total", "Number of nodes by status"),
            &["status"],
        )
        .expect("nodes_total metric creation must succeed");

        let gossip_messages_total = IntCounterVec::new(
            Opts::new("gossip_messages_total", "Gossip messages by direction"),
            &["direction"],
        )
        .expect("gossip_messages_total metric creation must succeed");

        let gossip_rtt_seconds = HistogramVec::new(
            HistogramOpts::new("gossip_rtt_seconds", "Gossip round-trip time in seconds")
                .buckets(GOSSIP_RTT_BUCKETS.to_vec()),
            &["peer_region"],
        )
        .expect("gossip_rtt_seconds metric creation must succeed");

        let gossip_peers_selected = IntGauge::new(
            "gossip_peers_selected",
            "Number of peers selected in last gossip round",
        )
        .expect("gossip_peers_selected metric creation must succeed");

        let partitions_active =
            IntGauge::new("partitions_active", "Number of active network partitions")
                .expect("partitions_active metric creation must succeed");

        let partition_reachable_fraction = Gauge::new(
            "partition_reachable_fraction",
            "Fraction of known peers that are reachable",
        )
        .expect("partition_reachable_fraction metric creation must succeed");

        let node_generation =
            IntGauge::new("node_generation", "Node generation counter")
                .expect("node_generation metric creation must succeed");

        // ---- Work domain ----
        let jobs_total = IntGaugeVec::new(
            Opts::new("jobs_total", "Number of jobs by status"),
            &["status"],
        )
        .expect("jobs_total metric creation must succeed");

        let chunks_total = IntGaugeVec::new(
            Opts::new("chunks_total", "Number of chunks by status"),
            &["status"],
        )
        .expect("chunks_total metric creation must succeed");

        let chunks_executed_total = IntCounterVec::new(
            Opts::new("chunks_executed_total", "Chunk executions by result and type"),
            &["result", "type"],
        )
        .expect("chunks_executed_total metric creation must succeed");

        let chunk_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "chunk_duration_seconds",
                "Chunk execution duration in seconds",
            )
            .buckets(CHUNK_DURATION_BUCKETS.to_vec()),
            &["type"],
        )
        .expect("chunk_duration_seconds metric creation must succeed");

        let pending_chunks = IntGauge::new(
            "pending_chunks",
            "Number of chunks waiting for execution",
        )
        .expect("pending_chunks metric creation must succeed");

        let active_chunks =
            IntGauge::new("active_chunks", "Number of chunks being executed")
                .expect("active_chunks metric creation must succeed");

        let job_submission_total = IntCounter::new(
            "job_submission_total",
            "Total jobs submitted since startup",
        )
        .expect("job_submission_total metric creation must succeed");

        let job_completion_rate = Gauge::new(
            "job_completion_rate",
            "Current job completion rate (0.0 to 1.0)",
        )
        .expect("job_completion_rate metric creation must succeed");

        // ---- Data domain ----
        let blob_store_bytes = IntGauge::new(
            "blob_store_bytes",
            "Total bytes stored in the blob store",
        )
        .expect("blob_store_bytes metric creation must succeed");

        let blob_store_count =
            IntGauge::new("blob_store_count", "Total number of blobs in the store")
                .expect("blob_store_count metric creation must succeed");

        let blob_store_capacity_bytes = IntGauge::new(
            "blob_store_capacity_bytes",
            "Maximum capacity of the blob store in bytes",
        )
        .expect("blob_store_capacity_bytes metric creation must succeed");

        let residency_violations_total = IntCounterVec::new(
            Opts::new(
                "residency_violations_total",
                "Data residency violations by policy and region",
            ),
            &["policy", "region"],
        )
        .expect("residency_violations_total metric creation must succeed");

        let blob_transfers_total = IntCounterVec::new(
            Opts::new("blob_transfers_total", "Blob transfers by direction"),
            &["direction"],
        )
        .expect("blob_transfers_total metric creation must succeed");

        let blob_transfer_bytes_total = IntCounterVec::new(
            Opts::new(
                "blob_transfer_bytes_total",
                "Bytes transferred in blob transfers by direction",
            ),
            &["direction"],
        )
        .expect("blob_transfer_bytes_total metric creation must succeed");

        // ---- Fleet domain ----
        let fleet_nodes_draining =
            IntGauge::new("fleet_nodes_draining", "Nodes in draining state")
                .expect("fleet_nodes_draining metric creation must succeed");

        let fleet_nodes_cordoned =
            IntGauge::new("fleet_nodes_cordoned", "Nodes in cordoned state")
                .expect("fleet_nodes_cordoned metric creation must succeed");

        let fleet_nodes_quarantined =
            IntGauge::new("fleet_nodes_quarantined", "Nodes in quarantined state")
                .expect("fleet_nodes_quarantined metric creation must succeed");

        let fleet_nodes_updating =
            IntGauge::new("fleet_nodes_updating", "Nodes performing rolling update")
                .expect("fleet_nodes_updating metric creation must succeed");

        let fleet_update_progress = Gauge::new(
            "fleet_update_progress",
            "Current fleet update progress (0.0 to 1.0)",
        )
        .expect("fleet_update_progress metric creation must succeed");

        let fleet_update_active =
            IntGauge::new("fleet_update_active", "Whether a fleet update is active")
                .expect("fleet_update_active metric creation must succeed");

        let admission_pending =
            IntGauge::new("admission_pending", "Nodes waiting for admission")
                .expect("admission_pending metric creation must succeed");

        let admission_probation =
            IntGauge::new("admission_probation", "Nodes currently in probation")
                .expect("admission_probation metric creation must succeed");

        let admission_total = IntCounterVec::new(
            Opts::new("admission_total", "Admission decisions by result"),
            &["result"],
        )
        .expect("admission_total metric creation must succeed");

        let hardware_class_nodes = IntGaugeVec::new(
            Opts::new("hardware_class_nodes", "Nodes by hardware class"),
            &["class"],
        )
        .expect("hardware_class_nodes metric creation must succeed");

        // ---- Security domain ----
        let auth_requests_total = IntCounterVec::new(
            Opts::new("auth_requests_total", "Auth requests by result"),
            &["result"],
        )
        .expect("auth_requests_total metric creation must succeed");

        let active_tokens = IntGauge::new("active_tokens", "Number of active API tokens")
            .expect("active_tokens metric creation must succeed");

        let policy_violations_total = IntCounterVec::new(
            Opts::new("policy_violations_total", "Policy violations by type"),
            &["policy_type"],
        )
        .expect("policy_violations_total metric creation must succeed");

        let policy_evaluations_total = IntCounterVec::new(
            Opts::new("policy_evaluations_total", "Policy evaluations by result"),
            &["result"],
        )
        .expect("policy_evaluations_total metric creation must succeed");

        // ---- Cost domain ----
        let marketplace_bids_total =
            IntCounter::new("marketplace_bids_total", "Total bids placed since startup")
                .expect("marketplace_bids_total metric creation must succeed");

        let marketplace_awards_total =
            IntCounter::new("marketplace_awards_total", "Total awards made since startup")
                .expect("marketplace_awards_total metric creation must succeed");

        let idle_node_ratio = Gauge::new("idle_node_ratio", "Ratio of idle nodes (0.0 to 1.0)")
            .expect("idle_node_ratio metric creation must succeed");

        let energy_cost_estimate_usd = Gauge::new(
            "energy_cost_estimate_usd",
            "Estimated energy cost in USD for current billing period",
        )
        .expect("energy_cost_estimate_usd metric creation must succeed");

        // ---- Psyche domain ----
        let psyche_facet = IntGaugeVec::new(
            Opts::new("psyche_facet", "Psyche facet intensity (0-4)"),
            &["facet"],
        )
        .expect("psyche_facet metric creation must succeed");

        let psyche_archetype = IntGaugeVec::new(
            Opts::new("psyche_archetype", "Active psyche archetype (1=active, 0=inactive)"),
            &["name"],
        )
        .expect("psyche_archetype metric creation must succeed");

        let psyche_computation_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "psyche_computation_duration_seconds",
                "Duration of psyche computation cycles",
            )
            .buckets(PSYCHE_DURATION_BUCKETS.to_vec()),
        )
        .expect("psyche_computation_duration_seconds metric creation must succeed");

        // ---- Operational domain ----
        let uptime_seconds =
            Gauge::new("uptime_seconds", "Uptime of this node in seconds")
                .expect("uptime_seconds metric creation must succeed");

        let event_bus_emitted_total = IntCounter::new(
            "event_bus_emitted_total",
            "Total events emitted to the event bus",
        )
        .expect("event_bus_emitted_total metric creation must succeed");

        let event_bus_dropped_total = IntCounter::new(
            "event_bus_dropped_total",
            "Total events dropped by the event bus",
        )
        .expect("event_bus_dropped_total metric creation must succeed");

        let api_request_total = IntCounterVec::new(
            Opts::new("api_request_total", "API requests by method, path, and status"),
            &["method", "path", "status"],
        )
        .expect("api_request_total metric creation must succeed");

        let api_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "api_request_duration_seconds",
                "API request duration in seconds",
            )
            .buckets(API_DURATION_BUCKETS.to_vec()),
            &["method", "path"],
        )
        .expect("api_request_duration_seconds metric creation must succeed");

        // ---- Blind computation domain ----
        let blind_executions_total = IntCounterVec::new(
            Opts::new("blind_executions_total", "Blind computation executions by outcome"),
            &["outcome"],
        )
        .expect("blind_executions_total metric creation must succeed");

        let blind_decryption_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "blind_decryption_seconds",
                "Blind envelope decryption duration in seconds",
            )
            .buckets(BLIND_DURATION_BUCKETS.to_vec()),
        )
        .expect("blind_decryption_seconds metric creation must succeed");

        let blind_sandbox_setup_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "blind_sandbox_setup_seconds",
                "Blind sandbox setup duration in seconds",
            )
            .buckets(BLIND_DURATION_BUCKETS.to_vec()),
        )
        .expect("blind_sandbox_setup_seconds metric creation must succeed");

        let blind_scrub_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "blind_scrub_seconds",
                "Blind memory scrub duration in seconds",
            )
            .buckets(BLIND_DURATION_BUCKETS.to_vec()),
        )
        .expect("blind_scrub_seconds metric creation must succeed");

        let blind_verification_replicas = Histogram::with_opts(
            HistogramOpts::new(
                "blind_verification_replicas",
                "Blind verification replica count per job",
            )
            .buckets(BLIND_REPLICA_BUCKETS.to_vec()),
        )
        .expect("blind_verification_replicas metric creation must succeed");

        let blind_padded_output_bytes = Histogram::with_opts(
            HistogramOpts::new(
                "blind_padded_output_bytes",
                "Blind padded output size in bytes",
            )
            .buckets(BLIND_OUTPUT_BYTES_BUCKETS.to_vec()),
        )
        .expect("blind_padded_output_bytes metric creation must succeed");

        // ---- Multi-swarm domain ----
        let membrane_crossings_total = IntCounterVec::new(
            Opts::new(
                "membrane_crossings_total",
                "Membrane crossings by membrane_id, direction, and category",
            ),
            &["membrane_id", "direction", "category"],
        )
        .expect("membrane_crossings_total metric creation must succeed");

        let membrane_crossing_bytes_total = IntCounterVec::new(
            Opts::new(
                "membrane_crossing_bytes_total",
                "Bytes crossing membranes by membrane_id and direction",
            ),
            &["membrane_id", "direction"],
        )
        .expect("membrane_crossing_bytes_total metric creation must succeed");

        let membrane_status = IntGaugeVec::new(
            Opts::new("membrane_status", "Membrane status by membrane_id (1=open, 0=closed)"),
            &["membrane_id"],
        )
        .expect("membrane_status metric creation must succeed");

        let active_treaties =
            IntGauge::new("active_treaties", "Number of active inter-swarm treaties")
                .expect("active_treaties metric creation must succeed");

        let capacity_lending_pct = Gauge::new(
            "capacity_lending_pct",
            "Percentage of local capacity lent to other swarms",
        )
        .expect("capacity_lending_pct metric creation must succeed");

        // ---- Register all metrics ----
        registry.register(Box::new(nodes_total.clone())).expect("register nodes_total");
        registry.register(Box::new(gossip_messages_total.clone())).expect("register gossip_messages_total");
        registry.register(Box::new(gossip_rtt_seconds.clone())).expect("register gossip_rtt_seconds");
        registry.register(Box::new(gossip_peers_selected.clone())).expect("register gossip_peers_selected");
        registry.register(Box::new(partitions_active.clone())).expect("register partitions_active");
        registry.register(Box::new(partition_reachable_fraction.clone())).expect("register partition_reachable_fraction");
        registry.register(Box::new(node_generation.clone())).expect("register node_generation");

        registry.register(Box::new(jobs_total.clone())).expect("register jobs_total");
        registry.register(Box::new(chunks_total.clone())).expect("register chunks_total");
        registry.register(Box::new(chunks_executed_total.clone())).expect("register chunks_executed_total");
        registry.register(Box::new(chunk_duration_seconds.clone())).expect("register chunk_duration_seconds");
        registry.register(Box::new(pending_chunks.clone())).expect("register pending_chunks");
        registry.register(Box::new(active_chunks.clone())).expect("register active_chunks");
        registry.register(Box::new(job_submission_total.clone())).expect("register job_submission_total");
        registry.register(Box::new(job_completion_rate.clone())).expect("register job_completion_rate");

        registry.register(Box::new(blob_store_bytes.clone())).expect("register blob_store_bytes");
        registry.register(Box::new(blob_store_count.clone())).expect("register blob_store_count");
        registry.register(Box::new(blob_store_capacity_bytes.clone())).expect("register blob_store_capacity_bytes");
        registry.register(Box::new(residency_violations_total.clone())).expect("register residency_violations_total");
        registry.register(Box::new(blob_transfers_total.clone())).expect("register blob_transfers_total");
        registry.register(Box::new(blob_transfer_bytes_total.clone())).expect("register blob_transfer_bytes_total");

        registry.register(Box::new(fleet_nodes_draining.clone())).expect("register fleet_nodes_draining");
        registry.register(Box::new(fleet_nodes_cordoned.clone())).expect("register fleet_nodes_cordoned");
        registry.register(Box::new(fleet_nodes_quarantined.clone())).expect("register fleet_nodes_quarantined");
        registry.register(Box::new(fleet_nodes_updating.clone())).expect("register fleet_nodes_updating");
        registry.register(Box::new(fleet_update_progress.clone())).expect("register fleet_update_progress");
        registry.register(Box::new(fleet_update_active.clone())).expect("register fleet_update_active");
        registry.register(Box::new(admission_pending.clone())).expect("register admission_pending");
        registry.register(Box::new(admission_probation.clone())).expect("register admission_probation");
        registry.register(Box::new(admission_total.clone())).expect("register admission_total");
        registry.register(Box::new(hardware_class_nodes.clone())).expect("register hardware_class_nodes");

        registry.register(Box::new(auth_requests_total.clone())).expect("register auth_requests_total");
        registry.register(Box::new(active_tokens.clone())).expect("register active_tokens");
        registry.register(Box::new(policy_violations_total.clone())).expect("register policy_violations_total");
        registry.register(Box::new(policy_evaluations_total.clone())).expect("register policy_evaluations_total");

        registry.register(Box::new(marketplace_bids_total.clone())).expect("register marketplace_bids_total");
        registry.register(Box::new(marketplace_awards_total.clone())).expect("register marketplace_awards_total");
        registry.register(Box::new(idle_node_ratio.clone())).expect("register idle_node_ratio");
        registry.register(Box::new(energy_cost_estimate_usd.clone())).expect("register energy_cost_estimate_usd");

        registry.register(Box::new(psyche_facet.clone())).expect("register psyche_facet");
        registry.register(Box::new(psyche_archetype.clone())).expect("register psyche_archetype");
        registry.register(Box::new(psyche_computation_duration_seconds.clone())).expect("register psyche_computation_duration_seconds");

        registry.register(Box::new(uptime_seconds.clone())).expect("register uptime_seconds");
        registry.register(Box::new(event_bus_emitted_total.clone())).expect("register event_bus_emitted_total");
        registry.register(Box::new(event_bus_dropped_total.clone())).expect("register event_bus_dropped_total");
        registry.register(Box::new(api_request_total.clone())).expect("register api_request_total");
        registry.register(Box::new(api_request_duration_seconds.clone())).expect("register api_request_duration_seconds");

        registry.register(Box::new(blind_executions_total.clone())).expect("register blind_executions_total");
        registry.register(Box::new(blind_decryption_seconds.clone())).expect("register blind_decryption_seconds");
        registry.register(Box::new(blind_sandbox_setup_seconds.clone())).expect("register blind_sandbox_setup_seconds");
        registry.register(Box::new(blind_scrub_seconds.clone())).expect("register blind_scrub_seconds");
        registry.register(Box::new(blind_verification_replicas.clone())).expect("register blind_verification_replicas");
        registry.register(Box::new(blind_padded_output_bytes.clone())).expect("register blind_padded_output_bytes");

        registry.register(Box::new(membrane_crossings_total.clone())).expect("register membrane_crossings_total");
        registry.register(Box::new(membrane_crossing_bytes_total.clone())).expect("register membrane_crossing_bytes_total");
        registry.register(Box::new(membrane_status.clone())).expect("register membrane_status");
        registry.register(Box::new(active_treaties.clone())).expect("register active_treaties");
        registry.register(Box::new(capacity_lending_pct.clone())).expect("register capacity_lending_pct");

        Self {
            registry,
            nodes_total,
            gossip_messages_total,
            gossip_rtt_seconds,
            gossip_peers_selected,
            partitions_active,
            partition_reachable_fraction,
            node_generation,
            jobs_total,
            chunks_total,
            chunks_executed_total,
            chunk_duration_seconds,
            pending_chunks,
            active_chunks,
            job_submission_total,
            job_completion_rate,
            blob_store_bytes,
            blob_store_count,
            blob_store_capacity_bytes,
            residency_violations_total,
            blob_transfers_total,
            blob_transfer_bytes_total,
            fleet_nodes_draining,
            fleet_nodes_cordoned,
            fleet_nodes_quarantined,
            fleet_nodes_updating,
            fleet_update_progress,
            fleet_update_active,
            admission_pending,
            admission_probation,
            admission_total,
            hardware_class_nodes,
            auth_requests_total,
            active_tokens,
            policy_violations_total,
            policy_evaluations_total,
            marketplace_bids_total,
            marketplace_awards_total,
            idle_node_ratio,
            energy_cost_estimate_usd,
            psyche_facet,
            psyche_archetype,
            psyche_computation_duration_seconds,
            uptime_seconds,
            event_bus_emitted_total,
            event_bus_dropped_total,
            api_request_total,
            api_request_duration_seconds,
            blind_executions_total,
            blind_decryption_seconds,
            blind_sandbox_setup_seconds,
            blind_scrub_seconds,
            blind_verification_replicas,
            blind_padded_output_bytes,
            membrane_crossings_total,
            membrane_crossing_bytes_total,
            membrane_status,
            active_treaties,
            capacity_lending_pct,
        }
    }

    /// Encode all registered metrics in Prometheus text exposition format.
    ///
    /// Returns an empty string if encoding fails (should never happen in
    /// practice, since all metric families are well-formed).
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        if encoder.encode(&metric_families, &mut buffer).is_err() {
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }

    /// Get a reference to the custom registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    // ================================================================
    // Convenience update methods
    // ================================================================

    /// Update node count gauges for all statuses.
    ///
    /// Each parameter is the current count for that status category.
    pub fn update_node_counts(
        &self,
        alive: i64,
        suspect: i64,
        dead: i64,
        draining: i64,
        cordoned: i64,
        quarantined: i64,
        updating: i64,
    ) {
        self.nodes_total.with_label_values(&["alive"]).set(alive);
        self.nodes_total.with_label_values(&["suspect"]).set(suspect);
        self.nodes_total.with_label_values(&["dead"]).set(dead);
        self.nodes_total.with_label_values(&["draining"]).set(draining);
        self.nodes_total.with_label_values(&["cordoned"]).set(cordoned);
        self.nodes_total.with_label_values(&["quarantined"]).set(quarantined);
        self.nodes_total.with_label_values(&["updating"]).set(updating);
    }

    /// Update job count gauges for all statuses.
    pub fn update_job_counts(
        &self,
        pending: i64,
        running: i64,
        completed: i64,
        failed: i64,
        cancelled: i64,
    ) {
        self.jobs_total.with_label_values(&["pending"]).set(pending);
        self.jobs_total.with_label_values(&["running"]).set(running);
        self.jobs_total.with_label_values(&["completed"]).set(completed);
        self.jobs_total.with_label_values(&["failed"]).set(failed);
        self.jobs_total.with_label_values(&["cancelled"]).set(cancelled);
    }

    /// Update chunk count gauges for all statuses.
    pub fn update_chunk_counts(
        &self,
        pending: i64,
        executing: i64,
        completed: i64,
        failed: i64,
    ) {
        self.chunks_total.with_label_values(&["pending"]).set(pending);
        self.chunks_total.with_label_values(&["executing"]).set(executing);
        self.chunks_total.with_label_values(&["completed"]).set(completed);
        self.chunks_total.with_label_values(&["failed"]).set(failed);
        self.pending_chunks.set(pending);
        self.active_chunks.set(executing);
    }

    /// Record a single chunk execution with its result, type, and duration.
    ///
    /// `success` indicates whether the chunk completed successfully.
    /// `chunk_type` is one of "shell", "python", "function".
    /// `duration_secs` is the execution wall-clock time in seconds.
    pub fn record_chunk_execution(
        &self,
        success: bool,
        chunk_type: &str,
        duration_secs: f64,
    ) {
        let result_label = if success { "success" } else { "failure" };
        self.chunks_executed_total
            .with_label_values(&[result_label, chunk_type])
            .inc();
        self.chunk_duration_seconds
            .with_label_values(&[chunk_type])
            .observe(duration_secs);
    }

    /// Record a gossip message sent.
    pub fn record_gossip_sent(&self) {
        self.gossip_messages_total
            .with_label_values(&["sent"])
            .inc();
    }

    /// Record a gossip message received.
    pub fn record_gossip_received(&self) {
        self.gossip_messages_total
            .with_label_values(&["received"])
            .inc();
    }

    /// Record a gossip round-trip time measurement.
    ///
    /// `peer_region` identifies the geographic region of the peer.
    /// `rtt_secs` is the round-trip time in seconds.
    pub fn record_gossip_rtt(&self, peer_region: &str, rtt_secs: f64) {
        self.gossip_rtt_seconds
            .with_label_values(&[peer_region])
            .observe(rtt_secs);
    }

    /// Update psyche facet intensities and active archetype.
    ///
    /// `facets` maps facet names to intensity values (0-4).
    /// `active_archetype` is the name of the currently active archetype,
    /// or `None` if no archetype is active.
    pub fn update_psyche(
        &self,
        facets: &HashMap<String, i64>,
        active_archetype: Option<&str>,
    ) {
        for (facet_name, &intensity) in facets {
            self.psyche_facet
                .with_label_values(&[facet_name])
                .set(intensity);
        }
        // Reset all archetypes to 0, then set the active one to 1.
        // Since we cannot enumerate all existing label values from a GaugeVec,
        // we only set the active one. Consumers should treat any facet not
        // present in a scrape as inactive.
        if let Some(archetype_name) = active_archetype {
            self.psyche_archetype
                .with_label_values(&[archetype_name])
                .set(1);
        }
    }

    /// Update blob store statistics.
    ///
    /// `bytes_stored` is the total bytes currently on disk.
    /// `blob_count` is the number of distinct blobs.
    /// `capacity_bytes` is the configured maximum storage.
    pub fn update_blob_stats(
        &self,
        bytes_stored: i64,
        blob_count: i64,
        capacity_bytes: i64,
    ) {
        self.blob_store_bytes.set(bytes_stored);
        self.blob_store_count.set(blob_count);
        self.blob_store_capacity_bytes.set(capacity_bytes);
    }

    /// Update fleet node counts for operational states.
    pub fn update_fleet_counts(
        &self,
        draining: i64,
        cordoned: i64,
        quarantined: i64,
        updating: i64,
    ) {
        self.fleet_nodes_draining.set(draining);
        self.fleet_nodes_cordoned.set(cordoned);
        self.fleet_nodes_quarantined.set(quarantined);
        self.fleet_nodes_updating.set(updating);
    }

    /// Record an API request with its method, path, status code, and duration.
    pub fn record_api_request(
        &self,
        method: &str,
        path: &str,
        status: &str,
        duration_secs: f64,
    ) {
        self.api_request_total
            .with_label_values(&[method, path, status])
            .inc();
        self.api_request_duration_seconds
            .with_label_values(&[method, path])
            .observe(duration_secs);
    }

    /// Record a membrane crossing event.
    ///
    /// `membrane_id` identifies the membrane boundary.
    /// `direction` is "inbound" or "outbound".
    /// `category` classifies the crossing (e.g., "job", "data", "gossip").
    /// `bytes` is the number of bytes transferred in this crossing.
    pub fn record_membrane_crossing(
        &self,
        membrane_id: &str,
        direction: &str,
        category: &str,
        bytes: u64,
    ) {
        self.membrane_crossings_total
            .with_label_values(&[membrane_id, direction, category])
            .inc();
        self.membrane_crossing_bytes_total
            .with_label_values(&[membrane_id, direction])
            .inc_by(bytes);
    }

    /// Record an authentication request result.
    ///
    /// `allowed` indicates whether the request was authorized.
    pub fn record_auth_request(&self, allowed: bool) {
        let result_label = if allowed { "allowed" } else { "denied" };
        self.auth_requests_total
            .with_label_values(&[result_label])
            .inc();
    }

    /// Set the uptime gauge to the given number of seconds.
    pub fn set_uptime(&self, seconds: f64) {
        self.uptime_seconds.set(seconds);
    }

    /// Record a blob transfer event.
    ///
    /// `direction` is "inbound" or "outbound".
    /// `bytes` is the number of bytes transferred.
    pub fn record_blob_transfer(&self, direction: &str, bytes: u64) {
        self.blob_transfers_total
            .with_label_values(&[direction])
            .inc();
        self.blob_transfer_bytes_total
            .with_label_values(&[direction])
            .inc_by(bytes);
    }

    /// Record a data residency violation.
    ///
    /// `policy` is the name of the policy that was violated.
    /// `region` is the geographic region where the violation occurred.
    pub fn record_residency_violation(&self, policy: &str, region: &str) {
        self.residency_violations_total
            .with_label_values(&[policy, region])
            .inc();
    }

    /// Record a policy evaluation result.
    ///
    /// `passed` indicates whether the policy check passed.
    pub fn record_policy_evaluation(&self, passed: bool) {
        let label = if passed { "pass" } else { "fail" };
        self.policy_evaluations_total
            .with_label_values(&[label])
            .inc();
    }

    /// Record a policy violation by type.
    pub fn record_policy_violation(&self, policy_type: &str) {
        self.policy_violations_total
            .with_label_values(&[policy_type])
            .inc();
    }

    /// Record an admission decision.
    ///
    /// `result` is one of "admitted", "rejected", "timeout".
    pub fn record_admission_decision(&self, result: &str) {
        self.admission_total.with_label_values(&[result]).inc();
    }

    /// Set the hardware class node counts.
    pub fn set_hardware_class_count(&self, class: &str, count: i64) {
        self.hardware_class_nodes
            .with_label_values(&[class])
            .set(count);
    }

    /// Set membrane status for a given membrane.
    ///
    /// `open` is true if the membrane is open, false if closed.
    pub fn set_membrane_status(&self, membrane_id: &str, open: bool) {
        let val = if open { 1 } else { 0 };
        self.membrane_status
            .with_label_values(&[membrane_id])
            .set(val);
    }

    /// Record a psyche computation duration.
    pub fn record_psyche_computation(&self, duration_secs: f64) {
        self.psyche_computation_duration_seconds.observe(duration_secs);
    }

    /// Record an event bus emission.
    pub fn record_event_emitted(&self) {
        self.event_bus_emitted_total.inc();
    }

    /// Record an event bus drop.
    pub fn record_event_dropped(&self) {
        self.event_bus_dropped_total.inc();
    }
}

// ============================================================================
// MetricsSnapshot
// ============================================================================

/// A point-in-time capture of key metric values.
///
/// This is a lightweight, serializable summary suitable for dashboards,
/// diffs, and historical storage. It does not carry the full histogram
/// distribution -- only summary counters and gauges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// When the snapshot was taken (milliseconds since Unix epoch).
    pub timestamp_ms: i64,
    /// Node counts by status.
    pub nodes: HashMap<String, i64>,
    /// Job counts by status.
    pub jobs: HashMap<String, i64>,
    /// Chunk counts by status.
    pub chunks: HashMap<String, i64>,
    /// Total gossip messages sent.
    pub gossip_sent: u64,
    /// Total gossip messages received.
    pub gossip_received: u64,
    /// Blob store bytes used.
    pub blob_bytes: i64,
    /// Blob count.
    pub blob_count: i64,
    /// Uptime in seconds.
    pub uptime_secs: f64,
    /// Total chunks executed successfully.
    pub chunks_executed_success: u64,
    /// Total chunks that failed.
    pub chunks_executed_failure: u64,
    /// Job submission count.
    pub job_submissions: u64,
    /// Marketplace bids placed.
    pub marketplace_bids: u64,
    /// Marketplace awards made.
    pub marketplace_awards: u64,
    /// Active partitions.
    pub partitions_active: i64,
    /// Reachable fraction.
    pub reachable_fraction: f64,
    /// API requests total (sum across all labels).
    pub api_requests_total: u64,
    /// Event bus emitted total.
    pub events_emitted: u64,
    /// Event bus dropped total.
    pub events_dropped: u64,
}

impl MetricsSnapshot {
    /// Capture a snapshot from the given `SwarmMetrics`.
    pub fn capture(metrics: &SwarmMetrics) -> Self {
        let node_statuses = ["alive", "suspect", "dead", "draining", "cordoned", "quarantined", "updating"];
        let mut nodes = HashMap::new();
        for &status in &node_statuses {
            nodes.insert(
                status.to_string(),
                metrics.nodes_total.with_label_values(&[status]).get(),
            );
        }

        let job_statuses = ["pending", "running", "completed", "failed", "cancelled"];
        let mut jobs = HashMap::new();
        for &status in &job_statuses {
            jobs.insert(
                status.to_string(),
                metrics.jobs_total.with_label_values(&[status]).get(),
            );
        }

        let chunk_statuses = ["pending", "executing", "completed", "failed"];
        let mut chunks = HashMap::new();
        for &status in &chunk_statuses {
            chunks.insert(
                status.to_string(),
                metrics.chunks_total.with_label_values(&[status]).get(),
            );
        }

        let gossip_sent = metrics
            .gossip_messages_total
            .with_label_values(&["sent"])
            .get();
        let gossip_received = metrics
            .gossip_messages_total
            .with_label_values(&["received"])
            .get();

        // Sum chunks_executed_total across all type labels for success/failure.
        let chunk_types = ["shell", "python", "function"];
        let mut chunks_executed_success = 0u64;
        let mut chunks_executed_failure = 0u64;
        for &t in &chunk_types {
            chunks_executed_success += metrics
                .chunks_executed_total
                .with_label_values(&["success", t])
                .get();
            chunks_executed_failure += metrics
                .chunks_executed_total
                .with_label_values(&["failure", t])
                .get();
        }

        Self {
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            nodes,
            jobs,
            chunks,
            gossip_sent,
            gossip_received,
            blob_bytes: metrics.blob_store_bytes.get(),
            blob_count: metrics.blob_store_count.get(),
            uptime_secs: metrics.uptime_seconds.get(),
            chunks_executed_success,
            chunks_executed_failure,
            job_submissions: metrics.job_submission_total.get(),
            marketplace_bids: metrics.marketplace_bids_total.get(),
            marketplace_awards: metrics.marketplace_awards_total.get(),
            partitions_active: metrics.partitions_active.get(),
            reachable_fraction: metrics.partition_reachable_fraction.get(),
            api_requests_total: 0, // Aggregated on demand below
            events_emitted: metrics.event_bus_emitted_total.get(),
            events_dropped: metrics.event_bus_dropped_total.get(),
        }
    }

    /// Compute the diff between this snapshot and an older snapshot.
    ///
    /// Returns a new snapshot where counter-type values represent the
    /// delta (this - older). Gauge-type values reflect the current
    /// (this) snapshot since deltas of gauges are not meaningful.
    pub fn diff(&self, older: &MetricsSnapshot) -> MetricsSnapshotDiff {
        MetricsSnapshotDiff {
            elapsed_ms: self.timestamp_ms.saturating_sub(older.timestamp_ms),
            gossip_sent_delta: self.gossip_sent.saturating_sub(older.gossip_sent),
            gossip_received_delta: self.gossip_received.saturating_sub(older.gossip_received),
            chunks_executed_success_delta: self
                .chunks_executed_success
                .saturating_sub(older.chunks_executed_success),
            chunks_executed_failure_delta: self
                .chunks_executed_failure
                .saturating_sub(older.chunks_executed_failure),
            job_submissions_delta: self
                .job_submissions
                .saturating_sub(older.job_submissions),
            marketplace_bids_delta: self
                .marketplace_bids
                .saturating_sub(older.marketplace_bids),
            marketplace_awards_delta: self
                .marketplace_awards
                .saturating_sub(older.marketplace_awards),
            events_emitted_delta: self
                .events_emitted
                .saturating_sub(older.events_emitted),
            events_dropped_delta: self
                .events_dropped
                .saturating_sub(older.events_dropped),
            current_nodes: self.nodes.clone(),
            current_jobs: self.jobs.clone(),
            current_chunks: self.chunks.clone(),
            current_blob_bytes: self.blob_bytes,
            current_blob_count: self.blob_count,
            current_uptime_secs: self.uptime_secs,
            current_partitions_active: self.partitions_active,
            current_reachable_fraction: self.reachable_fraction,
        }
    }
}

// ============================================================================
// MetricsSnapshotDiff
// ============================================================================

/// The difference between two [`MetricsSnapshot`] instances.
///
/// Counter fields represent deltas; gauge fields represent the current value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshotDiff {
    /// Elapsed time in milliseconds between the two snapshots.
    pub elapsed_ms: i64,
    /// Gossip messages sent in the interval.
    pub gossip_sent_delta: u64,
    /// Gossip messages received in the interval.
    pub gossip_received_delta: u64,
    /// Successful chunk executions in the interval.
    pub chunks_executed_success_delta: u64,
    /// Failed chunk executions in the interval.
    pub chunks_executed_failure_delta: u64,
    /// Jobs submitted in the interval.
    pub job_submissions_delta: u64,
    /// Bids placed in the interval.
    pub marketplace_bids_delta: u64,
    /// Awards made in the interval.
    pub marketplace_awards_delta: u64,
    /// Events emitted in the interval.
    pub events_emitted_delta: u64,
    /// Events dropped in the interval.
    pub events_dropped_delta: u64,
    /// Current node counts by status (gauge -- not a delta).
    pub current_nodes: HashMap<String, i64>,
    /// Current job counts by status (gauge).
    pub current_jobs: HashMap<String, i64>,
    /// Current chunk counts by status (gauge).
    pub current_chunks: HashMap<String, i64>,
    /// Current blob store bytes (gauge).
    pub current_blob_bytes: i64,
    /// Current blob count (gauge).
    pub current_blob_count: i64,
    /// Current uptime in seconds (gauge).
    pub current_uptime_secs: f64,
    /// Current number of active partitions (gauge).
    pub current_partitions_active: i64,
    /// Current reachable fraction (gauge).
    pub current_reachable_fraction: f64,
}

impl MetricsSnapshotDiff {
    /// Compute the per-second rate for gossip messages sent during the interval.
    ///
    /// Returns 0.0 if the elapsed time is zero.
    pub fn gossip_sent_rate_per_sec(&self) -> f64 {
        if self.elapsed_ms <= 0 {
            return 0.0;
        }
        self.gossip_sent_delta as f64 / (self.elapsed_ms as f64 / 1000.0)
    }

    /// Compute the per-second rate for gossip messages received during the interval.
    pub fn gossip_received_rate_per_sec(&self) -> f64 {
        if self.elapsed_ms <= 0 {
            return 0.0;
        }
        self.gossip_received_delta as f64 / (self.elapsed_ms as f64 / 1000.0)
    }

    /// Compute the per-second rate of successful chunk executions.
    pub fn chunk_success_rate_per_sec(&self) -> f64 {
        if self.elapsed_ms <= 0 {
            return 0.0;
        }
        self.chunks_executed_success_delta as f64 / (self.elapsed_ms as f64 / 1000.0)
    }

    /// Compute the chunk failure ratio over the interval (0.0 = no failures, 1.0 = all failures).
    ///
    /// Returns 0.0 if no chunks were executed.
    pub fn chunk_failure_ratio(&self) -> f64 {
        let total = self.chunks_executed_success_delta + self.chunks_executed_failure_delta;
        if total == 0 {
            return 0.0;
        }
        self.chunks_executed_failure_delta as f64 / total as f64
    }
}

// ============================================================================
// MetricsSnapshotter
// ============================================================================

/// Periodically captures [`MetricsSnapshot`] instances and maintains a
/// bounded history for trend analysis and dashboards.
///
/// The snapshotter is designed to be driven externally (call
/// [`take_snapshot`](Self::take_snapshot) from a timer loop) rather than
/// spawning its own background task, giving callers full control over
/// the snapshot cadence.
pub struct MetricsSnapshotter {
    /// Reference to the metrics being snapshotted.
    metrics: Arc<SwarmMetrics>,
    /// Bounded history of snapshots (newest at the back).
    history: parking_lot::RwLock<std::collections::VecDeque<MetricsSnapshot>>,
    /// Maximum number of snapshots to retain.
    max_history: usize,
}

impl MetricsSnapshotter {
    /// Create a new snapshotter targeting the given metrics.
    ///
    /// `max_history` bounds how many snapshots are retained.
    pub fn new(metrics: Arc<SwarmMetrics>, max_history: usize) -> Self {
        Self {
            metrics,
            history: parking_lot::RwLock::new(std::collections::VecDeque::with_capacity(
                max_history.min(10_000),
            )),
            max_history,
        }
    }

    /// Take a snapshot now and store it in the history ring buffer.
    ///
    /// Returns the snapshot for immediate use.
    pub fn take_snapshot(&self) -> MetricsSnapshot {
        let snap = MetricsSnapshot::capture(&self.metrics);
        let mut history = self.history.write();
        if history.len() >= self.max_history {
            history.pop_front();
        }
        history.push_back(snap.clone());
        snap
    }

    /// Get a clone of the most recent snapshot, if any.
    pub fn latest(&self) -> Option<MetricsSnapshot> {
        self.history.read().back().cloned()
    }

    /// Get all stored snapshots (oldest first).
    pub fn history(&self) -> Vec<MetricsSnapshot> {
        self.history.read().iter().cloned().collect()
    }

    /// Get the number of snapshots currently stored.
    pub fn len(&self) -> usize {
        self.history.read().len()
    }

    /// Returns true if no snapshots have been taken yet.
    pub fn is_empty(&self) -> bool {
        self.history.read().is_empty()
    }

    /// Compute a diff between the latest snapshot and the one `n` snapshots
    /// ago. Returns `None` if fewer than `n + 1` snapshots are available.
    pub fn diff_last_n(&self, n: usize) -> Option<MetricsSnapshotDiff> {
        let history = self.history.read();
        if history.len() < n + 1 {
            return None;
        }
        let latest = history.back()?;
        let older_idx = history.len() - 1 - n;
        let older = history.get(older_idx)?;
        Some(latest.diff(older))
    }

    /// Clear all stored snapshots.
    pub fn clear(&self) {
        self.history.write().clear();
    }
}

// ============================================================================
// PercentileCalculator
// ============================================================================

/// Utility for extracting approximate percentiles from Prometheus histogram data.
///
/// Prometheus histograms use cumulative buckets, so percentile extraction
/// requires linear interpolation between bucket boundaries. This calculator
/// works with the raw `(upper_bound, cumulative_count)` pairs that the
/// prometheus crate exposes.
pub struct PercentileCalculator;

impl PercentileCalculator {
    /// Calculate a percentile from cumulative histogram buckets.
    ///
    /// `buckets` is a sorted slice of `(upper_bound, cumulative_count)` pairs.
    /// `percentile` is the desired percentile as a fraction (e.g., 0.95 for p95).
    ///
    /// Returns `None` if the buckets are empty or the total count is zero.
    pub fn percentile(buckets: &[(f64, u64)], percentile: f64) -> Option<f64> {
        if buckets.is_empty() {
            return None;
        }

        let total = match buckets.last() {
            Some(&(_, count)) => count,
            None => return None,
        };

        if total == 0 {
            return None;
        }

        let target = (percentile * total as f64).ceil() as u64;

        // Find the first bucket whose cumulative count >= target.
        let mut prev_bound = 0.0_f64;
        let mut prev_count = 0_u64;

        for &(bound, count) in buckets {
            if count >= target {
                // Linear interpolation within this bucket.
                let bucket_count = count.saturating_sub(prev_count);
                if bucket_count == 0 {
                    return Some(prev_bound);
                }
                let fraction_into_bucket =
                    (target.saturating_sub(prev_count)) as f64 / bucket_count as f64;
                let value = prev_bound + fraction_into_bucket * (bound - prev_bound);
                return Some(value);
            }
            prev_bound = bound;
            prev_count = count;
        }

        // Target exceeds all buckets; return the last finite bound.
        buckets
            .iter()
            .rev()
            .find(|&&(b, _)| b.is_finite())
            .map(|&(b, _)| b)
    }

    /// Convenience: calculate p50 from cumulative histogram buckets.
    pub fn p50(buckets: &[(f64, u64)]) -> Option<f64> {
        Self::percentile(buckets, 0.50)
    }

    /// Convenience: calculate p90 from cumulative histogram buckets.
    pub fn p90(buckets: &[(f64, u64)]) -> Option<f64> {
        Self::percentile(buckets, 0.90)
    }

    /// Convenience: calculate p95 from cumulative histogram buckets.
    pub fn p95(buckets: &[(f64, u64)]) -> Option<f64> {
        Self::percentile(buckets, 0.95)
    }

    /// Convenience: calculate p99 from cumulative histogram buckets.
    pub fn p99(buckets: &[(f64, u64)]) -> Option<f64> {
        Self::percentile(buckets, 0.99)
    }

    /// Extract cumulative bucket data from a Prometheus [`HistogramVec`] for
    /// the given label values.
    ///
    /// Returns a sorted vector of `(upper_bound, cumulative_count)` pairs,
    /// or an empty vector if the metric has not been observed.
    pub fn extract_buckets(histogram: &HistogramVec, label_values: &[&str]) -> Vec<(f64, u64)> {
        let metric = histogram.with_label_values(label_values);
        let proto = metric.metric();
        let h = proto.get_histogram();
        let mut buckets: Vec<(f64, u64)> = h
            .get_bucket()
            .iter()
            .map(|b| (b.get_upper_bound(), b.get_cumulative_count()))
            .collect();
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        buckets
    }

    /// Extract cumulative bucket data from a plain [`Histogram`].
    pub fn extract_histogram_buckets(histogram: &Histogram) -> Vec<(f64, u64)> {
        let proto = histogram.metric();
        let h = proto.get_histogram();
        let mut buckets: Vec<(f64, u64)> = h
            .get_bucket()
            .iter()
            .map(|b| (b.get_upper_bound(), b.get_cumulative_count()))
            .collect();
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        buckets
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -- SwarmMetrics construction and encoding --

    #[test]
    fn metrics_new_creates_valid_instance() {
        let m = SwarmMetrics::new();
        let text = m.encode();
        assert!(!text.is_empty());
    }

    #[test]
    fn encode_contains_swarm_prefix() {
        let m = SwarmMetrics::new();
        m.update_node_counts(3, 1, 0, 0, 0, 0, 0);
        let text = m.encode();
        assert!(text.contains("swarm_nodes_total"));
    }

    #[test]
    fn encode_empty_metrics_still_valid() {
        let m = SwarmMetrics::new();
        let text = m.encode();
        // Should contain at least HELP/TYPE lines for registered metrics
        assert!(text.contains("HELP"));
        assert!(text.contains("TYPE"));
    }

    // -- Health domain --

    #[test]
    fn update_node_counts_sets_all_labels() {
        let m = SwarmMetrics::new();
        m.update_node_counts(10, 2, 1, 3, 0, 1, 2);
        assert_eq!(m.nodes_total.with_label_values(&["alive"]).get(), 10);
        assert_eq!(m.nodes_total.with_label_values(&["suspect"]).get(), 2);
        assert_eq!(m.nodes_total.with_label_values(&["dead"]).get(), 1);
        assert_eq!(m.nodes_total.with_label_values(&["draining"]).get(), 3);
        assert_eq!(m.nodes_total.with_label_values(&["cordoned"]).get(), 0);
        assert_eq!(m.nodes_total.with_label_values(&["quarantined"]).get(), 1);
        assert_eq!(m.nodes_total.with_label_values(&["updating"]).get(), 2);
    }

    #[test]
    fn gossip_sent_received_increments() {
        let m = SwarmMetrics::new();
        m.record_gossip_sent();
        m.record_gossip_sent();
        m.record_gossip_received();
        assert_eq!(m.gossip_messages_total.with_label_values(&["sent"]).get(), 2);
        assert_eq!(m.gossip_messages_total.with_label_values(&["received"]).get(), 1);
    }

    #[test]
    fn gossip_rtt_records_observation() {
        let m = SwarmMetrics::new();
        m.record_gossip_rtt("us-east", 0.015);
        m.record_gossip_rtt("us-east", 0.025);
        let text = m.encode();
        assert!(text.contains("swarm_gossip_rtt_seconds"));
    }

    #[test]
    fn node_generation_gauge() {
        let m = SwarmMetrics::new();
        m.node_generation.set(42);
        assert_eq!(m.node_generation.get(), 42);
    }

    #[test]
    fn partitions_active_and_reachable_fraction() {
        let m = SwarmMetrics::new();
        m.partitions_active.set(1);
        m.partition_reachable_fraction.set(0.75);
        assert_eq!(m.partitions_active.get(), 1);
        assert!((m.partition_reachable_fraction.get() - 0.75).abs() < f64::EPSILON);
    }

    // -- Work domain --

    #[test]
    fn update_job_counts_all_statuses() {
        let m = SwarmMetrics::new();
        m.update_job_counts(5, 3, 10, 2, 1);
        assert_eq!(m.jobs_total.with_label_values(&["pending"]).get(), 5);
        assert_eq!(m.jobs_total.with_label_values(&["running"]).get(), 3);
        assert_eq!(m.jobs_total.with_label_values(&["completed"]).get(), 10);
        assert_eq!(m.jobs_total.with_label_values(&["failed"]).get(), 2);
        assert_eq!(m.jobs_total.with_label_values(&["cancelled"]).get(), 1);
    }

    #[test]
    fn update_chunk_counts_also_sets_pending_active() {
        let m = SwarmMetrics::new();
        m.update_chunk_counts(8, 4, 20, 3);
        assert_eq!(m.chunks_total.with_label_values(&["pending"]).get(), 8);
        assert_eq!(m.chunks_total.with_label_values(&["executing"]).get(), 4);
        assert_eq!(m.pending_chunks.get(), 8);
        assert_eq!(m.active_chunks.get(), 4);
    }

    #[test]
    fn record_chunk_execution_success() {
        let m = SwarmMetrics::new();
        m.record_chunk_execution(true, "shell", 1.5);
        assert_eq!(
            m.chunks_executed_total
                .with_label_values(&["success", "shell"])
                .get(),
            1
        );
    }

    #[test]
    fn record_chunk_execution_failure() {
        let m = SwarmMetrics::new();
        m.record_chunk_execution(false, "python", 0.3);
        assert_eq!(
            m.chunks_executed_total
                .with_label_values(&["failure", "python"])
                .get(),
            1
        );
    }

    #[test]
    fn job_submission_counter() {
        let m = SwarmMetrics::new();
        m.job_submission_total.inc();
        m.job_submission_total.inc();
        assert_eq!(m.job_submission_total.get(), 2);
    }

    #[test]
    fn job_completion_rate_gauge() {
        let m = SwarmMetrics::new();
        m.job_completion_rate.set(0.85);
        assert!((m.job_completion_rate.get() - 0.85).abs() < f64::EPSILON);
    }

    // -- Data domain --

    #[test]
    fn update_blob_stats() {
        let m = SwarmMetrics::new();
        m.update_blob_stats(1024 * 1024, 42, 10 * 1024 * 1024);
        assert_eq!(m.blob_store_bytes.get(), 1024 * 1024);
        assert_eq!(m.blob_store_count.get(), 42);
        assert_eq!(m.blob_store_capacity_bytes.get(), 10 * 1024 * 1024);
    }

    #[test]
    fn record_residency_violation() {
        let m = SwarmMetrics::new();
        m.record_residency_violation("gdpr", "eu");
        m.record_residency_violation("gdpr", "eu");
        assert_eq!(
            m.residency_violations_total
                .with_label_values(&["gdpr", "eu"])
                .get(),
            2
        );
    }

    #[test]
    fn record_blob_transfer() {
        let m = SwarmMetrics::new();
        m.record_blob_transfer("inbound", 500);
        m.record_blob_transfer("outbound", 1000);
        assert_eq!(m.blob_transfers_total.with_label_values(&["inbound"]).get(), 1);
        assert_eq!(m.blob_transfer_bytes_total.with_label_values(&["outbound"]).get(), 1000);
    }

    // -- Fleet domain --

    #[test]
    fn update_fleet_counts() {
        let m = SwarmMetrics::new();
        m.update_fleet_counts(2, 1, 0, 3);
        assert_eq!(m.fleet_nodes_draining.get(), 2);
        assert_eq!(m.fleet_nodes_cordoned.get(), 1);
        assert_eq!(m.fleet_nodes_quarantined.get(), 0);
        assert_eq!(m.fleet_nodes_updating.get(), 3);
    }

    #[test]
    fn fleet_update_progress_and_active() {
        let m = SwarmMetrics::new();
        m.fleet_update_progress.set(0.5);
        m.fleet_update_active.set(1);
        assert!((m.fleet_update_progress.get() - 0.5).abs() < f64::EPSILON);
        assert_eq!(m.fleet_update_active.get(), 1);
    }

    #[test]
    fn admission_metrics() {
        let m = SwarmMetrics::new();
        m.admission_pending.set(5);
        m.admission_probation.set(2);
        m.record_admission_decision("admitted");
        m.record_admission_decision("rejected");
        assert_eq!(m.admission_pending.get(), 5);
        assert_eq!(m.admission_total.with_label_values(&["admitted"]).get(), 1);
        assert_eq!(m.admission_total.with_label_values(&["rejected"]).get(), 1);
    }

    #[test]
    fn hardware_class_nodes() {
        let m = SwarmMetrics::new();
        m.set_hardware_class_count("standard", 10);
        m.set_hardware_class_count("heavy", 3);
        assert_eq!(m.hardware_class_nodes.with_label_values(&["standard"]).get(), 10);
        assert_eq!(m.hardware_class_nodes.with_label_values(&["heavy"]).get(), 3);
    }

    // -- Security domain --

    #[test]
    fn auth_request_metrics() {
        let m = SwarmMetrics::new();
        m.record_auth_request(true);
        m.record_auth_request(true);
        m.record_auth_request(false);
        assert_eq!(m.auth_requests_total.with_label_values(&["allowed"]).get(), 2);
        assert_eq!(m.auth_requests_total.with_label_values(&["denied"]).get(), 1);
    }

    #[test]
    fn active_tokens_gauge() {
        let m = SwarmMetrics::new();
        m.active_tokens.set(15);
        assert_eq!(m.active_tokens.get(), 15);
    }

    #[test]
    fn policy_evaluations_and_violations() {
        let m = SwarmMetrics::new();
        m.record_policy_evaluation(true);
        m.record_policy_evaluation(false);
        m.record_policy_violation("geo_fence");
        assert_eq!(m.policy_evaluations_total.with_label_values(&["pass"]).get(), 1);
        assert_eq!(m.policy_evaluations_total.with_label_values(&["fail"]).get(), 1);
        assert_eq!(m.policy_violations_total.with_label_values(&["geo_fence"]).get(), 1);
    }

    // -- Cost domain --

    #[test]
    fn marketplace_counters() {
        let m = SwarmMetrics::new();
        m.marketplace_bids_total.inc();
        m.marketplace_awards_total.inc();
        m.marketplace_awards_total.inc();
        assert_eq!(m.marketplace_bids_total.get(), 1);
        assert_eq!(m.marketplace_awards_total.get(), 2);
    }

    #[test]
    fn idle_node_ratio_and_energy() {
        let m = SwarmMetrics::new();
        m.idle_node_ratio.set(0.3);
        m.energy_cost_estimate_usd.set(12.50);
        assert!((m.idle_node_ratio.get() - 0.3).abs() < f64::EPSILON);
        assert!((m.energy_cost_estimate_usd.get() - 12.50).abs() < f64::EPSILON);
    }

    // -- Psyche domain --

    #[test]
    fn update_psyche_facets() {
        let m = SwarmMetrics::new();
        let mut facets = HashMap::new();
        facets.insert("openness".to_string(), 3i64);
        facets.insert("resilience".to_string(), 2i64);
        m.update_psyche(&facets, Some("explorer"));
        assert_eq!(m.psyche_facet.with_label_values(&["openness"]).get(), 3);
        assert_eq!(m.psyche_facet.with_label_values(&["resilience"]).get(), 2);
        assert_eq!(m.psyche_archetype.with_label_values(&["explorer"]).get(), 1);
    }

    #[test]
    fn psyche_computation_records() {
        let m = SwarmMetrics::new();
        m.record_psyche_computation(0.025);
        let text = m.encode();
        assert!(text.contains("swarm_psyche_computation_duration_seconds"));
    }

    // -- Operational domain --

    #[test]
    fn set_uptime() {
        let m = SwarmMetrics::new();
        m.set_uptime(3600.0);
        assert!((m.uptime_seconds.get() - 3600.0).abs() < f64::EPSILON);
    }

    #[test]
    fn event_bus_counters() {
        let m = SwarmMetrics::new();
        m.record_event_emitted();
        m.record_event_emitted();
        m.record_event_dropped();
        assert_eq!(m.event_bus_emitted_total.get(), 2);
        assert_eq!(m.event_bus_dropped_total.get(), 1);
    }

    #[test]
    fn record_api_request() {
        let m = SwarmMetrics::new();
        m.record_api_request("GET", "/api/v1/nodes", "200", 0.015);
        m.record_api_request("POST", "/api/v1/jobs", "201", 0.1);
        assert_eq!(
            m.api_request_total
                .with_label_values(&["GET", "/api/v1/nodes", "200"])
                .get(),
            1
        );
        assert_eq!(
            m.api_request_total
                .with_label_values(&["POST", "/api/v1/jobs", "201"])
                .get(),
            1
        );
    }

    // -- Multi-swarm domain --

    #[test]
    fn record_membrane_crossing() {
        let m = SwarmMetrics::new();
        m.record_membrane_crossing("membrane-a", "inbound", "job", 1024);
        m.record_membrane_crossing("membrane-a", "outbound", "data", 2048);
        assert_eq!(
            m.membrane_crossings_total
                .with_label_values(&["membrane-a", "inbound", "job"])
                .get(),
            1
        );
        assert_eq!(
            m.membrane_crossing_bytes_total
                .with_label_values(&["membrane-a", "outbound"])
                .get(),
            2048
        );
    }

    #[test]
    fn membrane_status_toggle() {
        let m = SwarmMetrics::new();
        m.set_membrane_status("membrane-b", true);
        assert_eq!(m.membrane_status.with_label_values(&["membrane-b"]).get(), 1);
        m.set_membrane_status("membrane-b", false);
        assert_eq!(m.membrane_status.with_label_values(&["membrane-b"]).get(), 0);
    }

    #[test]
    fn active_treaties_and_capacity_lending() {
        let m = SwarmMetrics::new();
        m.active_treaties.set(3);
        m.capacity_lending_pct.set(0.15);
        assert_eq!(m.active_treaties.get(), 3);
        assert!((m.capacity_lending_pct.get() - 0.15).abs() < f64::EPSILON);
    }

    // -- MetricsSnapshot tests --

    #[test]
    fn snapshot_capture_basic() {
        let m = SwarmMetrics::new();
        m.update_node_counts(5, 1, 0, 0, 0, 0, 0);
        m.record_gossip_sent();
        let snap = MetricsSnapshot::capture(&m);
        assert_eq!(snap.nodes.get("alive"), Some(&5));
        assert_eq!(snap.gossip_sent, 1);
    }

    #[test]
    fn snapshot_diff_counters() {
        let m = SwarmMetrics::new();
        m.record_gossip_sent();
        m.record_gossip_sent();
        let older = MetricsSnapshot::capture(&m);

        m.record_gossip_sent();
        m.record_gossip_sent();
        m.record_gossip_sent();
        let newer = MetricsSnapshot::capture(&m);

        let diff = newer.diff(&older);
        assert_eq!(diff.gossip_sent_delta, 3);
    }

    #[test]
    fn snapshot_diff_rates() {
        let mut older = MetricsSnapshot::capture(&SwarmMetrics::new());
        older.timestamp_ms = 1000;
        older.gossip_sent = 10;

        let mut newer = older.clone();
        newer.timestamp_ms = 2000; // 1 second later
        newer.gossip_sent = 20;

        let diff = newer.diff(&older);
        assert!((diff.gossip_sent_rate_per_sec() - 10.0).abs() < 0.001);
    }

    #[test]
    fn snapshot_diff_zero_elapsed() {
        let snap = MetricsSnapshot::capture(&SwarmMetrics::new());
        let diff = snap.diff(&snap);
        assert_eq!(diff.gossip_sent_rate_per_sec(), 0.0);
    }

    #[test]
    fn snapshot_diff_chunk_failure_ratio() {
        let m = SwarmMetrics::new();
        m.record_chunk_execution(true, "shell", 1.0);
        m.record_chunk_execution(true, "shell", 1.0);
        m.record_chunk_execution(false, "shell", 1.0);
        let snap1 = MetricsSnapshot::capture(&m);

        // No new executions
        let snap2 = MetricsSnapshot::capture(&m);
        let diff = snap2.diff(&snap1);
        assert_eq!(diff.chunk_failure_ratio(), 0.0);
    }

    #[test]
    fn snapshot_diff_with_chunk_failures() {
        let m = SwarmMetrics::new();
        let snap1 = MetricsSnapshot::capture(&m);

        m.record_chunk_execution(true, "shell", 1.0);
        m.record_chunk_execution(false, "shell", 1.0);
        let snap2 = MetricsSnapshot::capture(&m);

        let diff = snap2.diff(&snap1);
        assert!((diff.chunk_failure_ratio() - 0.5).abs() < 0.001);
    }

    // -- MetricsSnapshotter tests --

    #[test]
    fn snapshotter_basic_lifecycle() {
        let m = Arc::new(SwarmMetrics::new());
        let snapshotter = MetricsSnapshotter::new(m, 10);
        assert!(snapshotter.is_empty());

        let snap = snapshotter.take_snapshot();
        assert!(!snapshotter.is_empty());
        assert_eq!(snapshotter.len(), 1);
        assert!(snapshotter.latest().is_some());
        assert_eq!(
            snapshotter.latest().map(|s| s.timestamp_ms),
            Some(snap.timestamp_ms)
        );
    }

    #[test]
    fn snapshotter_respects_max_history() {
        let m = Arc::new(SwarmMetrics::new());
        let snapshotter = MetricsSnapshotter::new(m, 3);

        for _ in 0..5 {
            snapshotter.take_snapshot();
        }

        assert_eq!(snapshotter.len(), 3);
    }

    #[test]
    fn snapshotter_diff_last_n() {
        let m = Arc::new(SwarmMetrics::new());
        let snapshotter = MetricsSnapshotter::new(m.clone(), 10);

        m.record_gossip_sent();
        snapshotter.take_snapshot();

        m.record_gossip_sent();
        m.record_gossip_sent();
        snapshotter.take_snapshot();

        let diff = snapshotter.diff_last_n(1);
        assert!(diff.is_some());
        let diff = diff.expect("diff should exist");
        assert_eq!(diff.gossip_sent_delta, 2);
    }

    #[test]
    fn snapshotter_diff_last_n_not_enough_history() {
        let m = Arc::new(SwarmMetrics::new());
        let snapshotter = MetricsSnapshotter::new(m, 10);
        snapshotter.take_snapshot();
        assert!(snapshotter.diff_last_n(2).is_none());
    }

    #[test]
    fn snapshotter_clear() {
        let m = Arc::new(SwarmMetrics::new());
        let snapshotter = MetricsSnapshotter::new(m, 10);
        snapshotter.take_snapshot();
        snapshotter.take_snapshot();
        assert_eq!(snapshotter.len(), 2);
        snapshotter.clear();
        assert!(snapshotter.is_empty());
    }

    #[test]
    fn snapshotter_history_order() {
        let m = Arc::new(SwarmMetrics::new());
        let snapshotter = MetricsSnapshotter::new(m.clone(), 10);

        m.update_node_counts(1, 0, 0, 0, 0, 0, 0);
        snapshotter.take_snapshot();

        m.update_node_counts(5, 0, 0, 0, 0, 0, 0);
        snapshotter.take_snapshot();

        let history = snapshotter.history();
        assert_eq!(history.len(), 2);
        // Oldest first
        assert_eq!(history[0].nodes.get("alive"), Some(&1));
        assert_eq!(history[1].nodes.get("alive"), Some(&5));
    }

    // -- PercentileCalculator tests --

    #[test]
    fn percentile_empty_buckets() {
        assert!(PercentileCalculator::p50(&[]).is_none());
    }

    #[test]
    fn percentile_zero_count() {
        let buckets = vec![(1.0, 0), (5.0, 0), (10.0, 0)];
        assert!(PercentileCalculator::p50(&buckets).is_none());
    }

    #[test]
    fn percentile_single_bucket() {
        // All observations in the first bucket
        let buckets = vec![(1.0, 100)];
        let p50 = PercentileCalculator::p50(&buckets);
        assert!(p50.is_some());
        let val = p50.expect("should have value");
        assert!(val >= 0.0 && val <= 1.0);
    }

    #[test]
    fn percentile_uniform_distribution() {
        // 10 observations per bucket, 5 buckets
        let buckets = vec![
            (1.0, 10),
            (2.0, 20),
            (3.0, 30),
            (4.0, 40),
            (5.0, 50),
        ];
        let p50 = PercentileCalculator::p50(&buckets).expect("p50 should exist");
        // Median should be around 2.5 (middle of the range)
        assert!(p50 >= 2.0 && p50 <= 3.5, "p50={} out of expected range", p50);

        let p99 = PercentileCalculator::p99(&buckets).expect("p99 should exist");
        assert!(p99 >= 4.0, "p99={} should be >= 4.0", p99);
    }

    #[test]
    fn percentile_p90_p95_p99() {
        let buckets = vec![
            (0.01, 50),
            (0.05, 90),
            (0.1, 95),
            (0.5, 99),
            (1.0, 100),
        ];
        let p90 = PercentileCalculator::p90(&buckets).expect("p90");
        let p95 = PercentileCalculator::p95(&buckets).expect("p95");
        let p99 = PercentileCalculator::p99(&buckets).expect("p99");

        // p90 <= p95 <= p99
        assert!(p90 <= p95, "p90={} <= p95={}", p90, p95);
        assert!(p95 <= p99, "p95={} <= p99={}", p95, p99);
    }

    #[test]
    fn percentile_all_in_first_bucket() {
        let buckets = vec![(0.1, 100), (0.5, 100), (1.0, 100)];
        let p50 = PercentileCalculator::p50(&buckets).expect("p50");
        assert!(p50 <= 0.1, "all observations in first bucket, p50={}", p50);
    }

    #[test]
    fn percentile_all_in_last_bucket() {
        let buckets = vec![(0.1, 0), (0.5, 0), (1.0, 100)];
        let p50 = PercentileCalculator::p50(&buckets).expect("p50");
        assert!(p50 >= 0.5 && p50 <= 1.0, "p50={}", p50);
    }

    #[test]
    fn extract_buckets_from_histogram_vec() {
        let m = SwarmMetrics::new();
        m.record_chunk_execution(true, "shell", 0.3);
        m.record_chunk_execution(true, "shell", 2.5);
        let buckets = PercentileCalculator::extract_buckets(
            &m.chunk_duration_seconds,
            &["shell"],
        );
        assert!(!buckets.is_empty());
        // Last bucket should have count = 2
        let last_count = buckets.last().map(|&(_, c)| c).unwrap_or(0);
        assert_eq!(last_count, 2);
    }

    #[test]
    fn extract_buckets_from_histogram() {
        let m = SwarmMetrics::new();
        m.record_psyche_computation(0.002);
        m.record_psyche_computation(0.01);
        let buckets = PercentileCalculator::extract_histogram_buckets(
            &m.psyche_computation_duration_seconds,
        );
        assert!(!buckets.is_empty());
        let last_count = buckets.last().map(|&(_, c)| c).unwrap_or(0);
        assert_eq!(last_count, 2);
    }

    // -- Multiple registries can coexist --

    #[test]
    fn two_metrics_instances_independent() {
        let m1 = SwarmMetrics::new();
        let m2 = SwarmMetrics::new();
        m1.update_node_counts(10, 0, 0, 0, 0, 0, 0);
        m2.update_node_counts(20, 0, 0, 0, 0, 0, 0);
        assert_eq!(m1.nodes_total.with_label_values(&["alive"]).get(), 10);
        assert_eq!(m2.nodes_total.with_label_values(&["alive"]).get(), 20);
    }

    // -- Encode contains specific metric families --

    #[test]
    fn encode_contains_all_domains() {
        let m = SwarmMetrics::new();
        // Trigger at least one label in each domain
        m.update_node_counts(1, 0, 0, 0, 0, 0, 0);
        m.update_job_counts(1, 0, 0, 0, 0);
        m.update_chunk_counts(1, 0, 0, 0);
        m.update_blob_stats(0, 0, 0);
        m.update_fleet_counts(0, 0, 0, 0);
        m.record_auth_request(true);
        m.marketplace_bids_total.inc();
        m.set_uptime(1.0);
        m.record_membrane_crossing("test", "inbound", "job", 0);
        let text = m.encode();

        assert!(text.contains("swarm_nodes_total"), "missing health metric");
        assert!(text.contains("swarm_jobs_total"), "missing work metric");
        assert!(text.contains("swarm_blob_store_bytes"), "missing data metric");
        assert!(text.contains("swarm_fleet_nodes_draining"), "missing fleet metric");
        assert!(text.contains("swarm_auth_requests_total"), "missing security metric");
        assert!(text.contains("swarm_marketplace_bids_total"), "missing cost metric");
        assert!(text.contains("swarm_uptime_seconds"), "missing operational metric");
        assert!(text.contains("swarm_membrane_crossings_total"), "missing multi-swarm metric");
    }

    #[test]
    fn gossip_peers_selected_gauge() {
        let m = SwarmMetrics::new();
        m.gossip_peers_selected.set(5);
        assert_eq!(m.gossip_peers_selected.get(), 5);
    }

    #[test]
    fn record_chunk_execution_multiple_types() {
        let m = SwarmMetrics::new();
        m.record_chunk_execution(true, "shell", 1.0);
        m.record_chunk_execution(true, "python", 2.0);
        m.record_chunk_execution(true, "function", 0.5);
        m.record_chunk_execution(false, "function", 0.1);
        assert_eq!(m.chunks_executed_total.with_label_values(&["success", "shell"]).get(), 1);
        assert_eq!(m.chunks_executed_total.with_label_values(&["success", "python"]).get(), 1);
        assert_eq!(m.chunks_executed_total.with_label_values(&["success", "function"]).get(), 1);
        assert_eq!(m.chunks_executed_total.with_label_values(&["failure", "function"]).get(), 1);
    }

    #[test]
    fn snapshot_capture_includes_chunk_executions() {
        let m = SwarmMetrics::new();
        m.record_chunk_execution(true, "shell", 1.0);
        m.record_chunk_execution(false, "python", 2.0);
        let snap = MetricsSnapshot::capture(&m);
        assert_eq!(snap.chunks_executed_success, 1);
        assert_eq!(snap.chunks_executed_failure, 1);
    }

    #[test]
    fn snapshot_diff_events() {
        let m = SwarmMetrics::new();
        m.record_event_emitted();
        let snap1 = MetricsSnapshot::capture(&m);

        m.record_event_emitted();
        m.record_event_emitted();
        m.record_event_dropped();
        let snap2 = MetricsSnapshot::capture(&m);

        let diff = snap2.diff(&snap1);
        assert_eq!(diff.events_emitted_delta, 2);
        assert_eq!(diff.events_dropped_delta, 1);
    }
}
