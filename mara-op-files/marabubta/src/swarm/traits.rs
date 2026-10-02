// Marabunta - Licensed under the MIT License.
//! Trait evaluation engine for the Marabunta Swarm architecture.
//!
//! Every swarm node periodically evaluates its own hardware resources, load,
//! and connectivity to decide which [`Trait`]s (capabilities) it should claim.
//! Traits are dynamic: they can be acquired when resources free up and shed
//! when the node is overloaded.
//!
//! The main entry points are:
//! - [`measure_resources`] -- snapshot current hardware via `sysinfo`
//! - [`measure_load`] -- compute a 0.0..1.0 load fraction
//! - [`evaluate_traits`] -- pure function: resources + load + connectivity -> trait set
//! - [`TraitEvaluator`] -- stateful wrapper that runs the above on a timer

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, RefreshKind, System};
use tracing::{debug, info};

use super::config::{
    thresholds_for_class, THRESHOLD_EXECUTE_CPU, THRESHOLD_FORWARD_LOAD,
    THRESHOLD_RELAY_BANDWIDTH_MBPS, TRAIT_EVAL_INTERVAL,
};
use super::types::{ConnectivityInfo, NatType, ResourceSnapshot, Trait};

// ============================================================================
// Hardware classification
// ============================================================================

/// Hardware classification based on available resources.
/// Used to adjust trait thresholds so weak nodes can still contribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HardwareClass {
    /// < 2 cores, < 2 GB RAM (old phones, Raspberry Pi Zero, etc.)
    Minimal,
    /// 2-4 cores, 2-4 GB RAM (old laptops, budget phones)
    Light,
    /// 4-8 cores, 4-16 GB RAM (typical desktops/laptops)
    Standard,
    /// 8+ cores, 16+ GB RAM (workstations, servers)
    Heavy,
}

impl HardwareClass {
    /// Detect hardware class from CPU core count and total RAM.
    pub fn detect(cpu_cores: usize, ram_total_mb: u64) -> Self {
        match (cpu_cores, ram_total_mb) {
            (c, r) if c < 2 || r < 2048 => HardwareClass::Minimal,
            (c, r) if c <= 4 && r <= 4096 => HardwareClass::Light,
            (c, r) if c <= 8 && r <= 16384 => HardwareClass::Standard,
            _ => HardwareClass::Heavy,
        }
    }
}

// ============================================================================
// Constants
// ============================================================================

/// Maximum load at which a node may still claim `CanRelay`.
const MAX_RELAY_LOAD: f32 = 0.5;

/// Jitter percentage applied to the evaluation interval (+-20%).
const EVAL_JITTER_PERCENT: u32 = 20;

// ============================================================================
// Resource measurement
// ============================================================================

/// Measures current hardware resources using `sysinfo`.
///
/// Creates a short-lived [`System`] with only the subsystems we need
/// (CPU, memory) to minimise overhead. Uses [`Disks`] separately for
/// filesystem information. CPU usage requires two samples with a small
/// sleep in between so sysinfo can compute a delta.
///
/// Network bandwidth is not directly measurable through sysinfo, so it
/// defaults to `100.0` Mbps unless a future probe overrides it.
pub fn measure_resources() -> ResourceSnapshot {
    let mut sys = System::new_with_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::everything()),
    );

    // sysinfo needs two refreshes separated by a short interval to compute
    // meaningful CPU usage percentages. The first refresh seeds the counters.
    sys.refresh_cpu_usage();
    std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
    sys.refresh_cpu_usage();
    sys.refresh_memory();

    // --- CPU ---
    let cpu_cores = sys.cpus().len() as u32;
    let cpu_usage_fraction = if cpu_cores > 0 {
        let total_usage: f32 = sys.cpus().iter().map(|c| c.cpu_usage()).sum();
        (total_usage / (cpu_cores as f32)) / 100.0
    } else {
        1.0 // assume fully loaded if we cannot read CPUs
    };
    let cpu_available = (1.0 - cpu_usage_fraction).clamp(0.0, 1.0);

    // --- Memory ---
    let memory_total_mb = sys.total_memory() / (1024 * 1024);
    let memory_available_mb = sys.available_memory() / (1024 * 1024);

    // --- Disk ---
    let disks = Disks::new_with_refreshed_list();
    let (disk_total_mb, disk_available_mb) = aggregate_disk_space(&disks);

    // --- Network ---
    // Not directly measurable via sysinfo; use a sensible default.
    let network_bandwidth_mbps = 100.0_f32;

    let snapshot = ResourceSnapshot {
        cpu_cores,
        cpu_available,
        memory_total_mb,
        memory_available_mb,
        disk_total_mb,
        disk_available_mb,
        network_bandwidth_mbps,
        current_tdp_watts: 15.0,
        ask_usd_per_megagas: 0.0001,
    };

    debug!(
        cpu_cores,
        cpu_available = format!("{:.2}", snapshot.cpu_available),
        memory_total_mb,
        memory_available_mb,
        disk_total_mb,
        disk_available_mb,
        "resource snapshot captured"
    );

    snapshot
}

/// Aggregates disk space across all mounted filesystems.
///
/// De-duplicates by mount point to avoid double-counting bind mounts and
/// overlay filesystems that appear multiple times.
fn aggregate_disk_space(disks: &Disks) -> (u64, u64) {
    let mut seen_mount_points = HashSet::new();
    let mut total_mb: u64 = 0;
    let mut available_mb: u64 = 0;

    for disk in disks.list() {
        let mount = disk.mount_point().to_path_buf();
        if seen_mount_points.insert(mount) {
            total_mb = total_mb.saturating_add(disk.total_space() / (1024 * 1024));
            available_mb = available_mb.saturating_add(disk.available_space() / (1024 * 1024));
        }
    }

    (total_mb, available_mb)
}

// ============================================================================
// Load measurement
// ============================================================================

/// Computes current system load as a `0.0..1.0` fraction.
///
/// The load is the average of:
/// - CPU usage fraction (0.0 = idle, 1.0 = fully loaded)
/// - Memory usage fraction (used / total)
///
/// This gives a balanced view: a node with idle CPUs but full memory (or
/// vice-versa) will report moderate load rather than appearing idle.
pub fn measure_load() -> f32 {
    let mut sys = System::new_with_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::everything()),
    );

    sys.refresh_cpu_usage();
    std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
    sys.refresh_cpu_usage();
    sys.refresh_memory();

    let cpu_count = sys.cpus().len() as f32;
    let cpu_fraction = if cpu_count > 0.0 {
        let total_usage: f32 = sys.cpus().iter().map(|c| c.cpu_usage()).sum();
        (total_usage / cpu_count) / 100.0
    } else {
        1.0
    };

    let total_mem = sys.total_memory();
    let used_mem = total_mem.saturating_sub(sys.available_memory());
    let mem_fraction = if total_mem > 0 {
        used_mem as f32 / total_mem as f32
    } else {
        1.0
    };

    let load = ((cpu_fraction + mem_fraction) / 2.0).clamp(0.0, 1.0);

    debug!(
        cpu_fraction = format!("{:.3}", cpu_fraction),
        mem_fraction = format!("{:.3}", mem_fraction),
        load = format!("{:.3}", load),
        "load measurement"
    );

    load
}

// ============================================================================
// Trait evaluation (pure function)
// ============================================================================

/// Evaluates which traits this node should claim given its current state.
///
/// The evaluation is deterministic and side-effect-free: given the same
/// inputs it always returns the same output. This makes it easy to test
/// and reason about.
///
/// After the resource-based evaluation, two overrides are applied:
/// - `force_traits`: always included regardless of resource checks
/// - `deny_traits`: always excluded regardless of resource checks
///
/// `force_traits` takes precedence if a trait appears in both sets.
pub fn evaluate_traits(
    resources: &ResourceSnapshot,
    load: f32,
    connectivity: &ConnectivityInfo,
    force_traits: &HashSet<Trait>,
    deny_traits: &HashSet<Trait>,
) -> HashSet<Trait> {
    let mut traits = HashSet::new();

    // Detect hardware class and look up per-class thresholds so that
    // weak nodes (old phones, Raspberry Pis) get more lenient limits
    // and can still contribute to the swarm.
    let hw_class = HardwareClass::detect(
        resources.cpu_cores as usize,
        resources.memory_total_mb,
    );
    let thresholds = thresholds_for_class(hw_class);

    // --- CanExecute ---
    // The baseline trait. Nearly every node that has spare CPU and is not
    // overloaded should be willing to execute work.
    if resources.cpu_available > THRESHOLD_EXECUTE_CPU && load < thresholds.max_execute_load {
        traits.insert(Trait::CanExecute);
    }

    // --- CanForward ---
    // Only publicly reachable nodes with low load can accept external jobs
    // and route them into the swarm.
    if connectivity.is_publicly_reachable && load < THRESHOLD_FORWARD_LOAD {
        traits.insert(Trait::CanForward);
    }

    // --- CanAggregate ---
    // Needs enough RAM to buffer partial results from many chunks,
    // and moderate load headroom.
    if resources.memory_available_mb > thresholds.min_aggregate_ram_mb
        && load < thresholds.max_aggregate_load
    {
        traits.insert(Trait::CanAggregate);
    }

    // --- CanStoreState ---
    // Needs enough disk to persist job queues and metadata, with plenty
    // of load headroom (disk I/O is expensive).
    if resources.disk_available_mb > thresholds.min_store_state_disk_mb
        && load < thresholds.max_store_state_load
    {
        traits.insert(Trait::CanStoreState);
    }

    // --- CanDiscover ---
    // Must be reachable (directly or via relay) and have completed NAT
    // detection. Nodes with Unknown NAT type cannot reliably help others
    // discover peers.
    if (connectivity.is_publicly_reachable || connectivity.has_relay_access)
        && connectivity.nat_type != NatType::Unknown
    {
        traits.insert(Trait::CanDiscover);
    }

    // --- CanRelay ---
    // The most demanding trait: must be publicly reachable with high
    // bandwidth and very low load.
    if connectivity.is_publicly_reachable
        && resources.network_bandwidth_mbps > THRESHOLD_RELAY_BANDWIDTH_MBPS
        && load < MAX_RELAY_LOAD
    {
        traits.insert(Trait::CanRelay);
    }

    // --- Overrides ---
    // force_traits are always added (operator knows best).
    for t in force_traits {
        traits.insert(*t);
    }
    // deny_traits are always removed. However, force_traits wins when a
    // trait appears in both sets (operator explicitly forced it).
    for t in deny_traits {
        if !force_traits.contains(t) {
            traits.remove(t);
        }
    }

    traits
}

// ============================================================================
// TraitEvaluator
// ============================================================================

/// The trait evaluator: runs periodic evaluation cycles and reports changes.
///
/// Holds the `force_traits` / `deny_traits` overrides and the previous
/// trait set so it can log meaningful diffs when traits are gained or lost.
pub struct TraitEvaluator {
    force_traits: HashSet<Trait>,
    deny_traits: HashSet<Trait>,
    previous_traits: HashSet<Trait>,
    /// Whether we have already logged the hardware class (only log once).
    hw_class_logged: bool,
    /// Trait evaluation tuning configuration (promoted from constants).
    pub config: super::config::TraitEvalConfig,
}

impl TraitEvaluator {
    /// Create a new evaluator with the given override sets.
    pub fn new(force_traits: HashSet<Trait>, deny_traits: HashSet<Trait>) -> Self {
        Self {
            force_traits,
            deny_traits,
            previous_traits: HashSet::new(),
            hw_class_logged: false,
            config: super::config::TraitEvalConfig::default(),
        }
    }

    /// Override trait evaluation configuration (for runtime config).
    pub fn with_trait_config(mut self, config: super::config::TraitEvalConfig) -> Self {
        self.config = config;
        self
    }

    /// Run one evaluation cycle.
    ///
    /// Measures resources and load, evaluates the trait set, logs any
    /// changes compared to the previous cycle, and returns the new trait
    /// set along with the current load and resource snapshot.
    pub fn evaluate(
        &mut self,
        connectivity: &ConnectivityInfo,
    ) -> (HashSet<Trait>, f32, ResourceSnapshot) {
        let resources = measure_resources();
        let load = measure_load();

        // Log the detected hardware class on first evaluation.
        if !self.hw_class_logged {
            let hw_class = HardwareClass::detect(
                resources.cpu_cores as usize,
                resources.memory_total_mb,
            );
            info!(
                class = ?hw_class,
                cores = resources.cpu_cores,
                ram_mb = resources.memory_total_mb,
                "Hardware class detected: {:?} ({} cores, {} MB RAM)",
                hw_class, resources.cpu_cores, resources.memory_total_mb,
            );
            self.hw_class_logged = true;
        }

        let new_traits = evaluate_traits(
            &resources,
            load,
            connectivity,
            &self.force_traits,
            &self.deny_traits,
        );

        // Log trait changes at INFO level so operators can see the swarm adapting.
        let gained: Vec<_> = new_traits.difference(&self.previous_traits).collect();
        let lost: Vec<_> = self.previous_traits.difference(&new_traits).collect();

        if !gained.is_empty() || !lost.is_empty() {
            let gained_names: Vec<String> = gained.iter().map(|t| t.to_string()).collect();
            let lost_names: Vec<String> = lost.iter().map(|t| t.to_string()).collect();
            let current_names: Vec<String> = new_traits.iter().map(|t| t.to_string()).collect();

            info!(
                gained = ?gained_names,
                lost = ?lost_names,
                current = ?current_names,
                load = format!("{:.3}", load),
                "trait set changed"
            );
        } else {
            debug!(
                traits = new_traits.len(),
                load = format!("{:.3}", load),
                "trait set unchanged"
            );
        }

        self.previous_traits = new_traits.clone();
        (new_traits, load, resources)
    }

    /// Spawn the periodic evaluation loop as a tokio task.
    ///
    /// The loop runs [`evaluate`](Self::evaluate) every
    /// [`TRAIT_EVAL_INTERVAL`] with +-20% random jitter to avoid
    /// synchronised evaluations across the swarm. The `callback` is
    /// invoked after each cycle with the new trait set, load, and
    /// resource snapshot.
    ///
    /// Returns a [`JoinHandle`](tokio::task::JoinHandle) that can be
    /// used to abort the loop (via `.abort()`) or await its completion.
    /// The loop runs until the task is aborted or the runtime shuts down.
    pub fn spawn_loop<F>(
        self,
        connectivity: Arc<RwLock<ConnectivityInfo>>,
        callback: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn(HashSet<Trait>, f32, ResourceSnapshot) + Send + Sync + 'static,
    {
        let callback = Arc::new(callback);

        tokio::spawn(async move {
            // We wrap the evaluator in a parking_lot::Mutex so that we can
            // share it with spawn_blocking closures and get it back each
            // iteration. An alternative is to move it in/out, but Mutex is
            // cleaner and the critical section is tiny (just the swap).
            let evaluator = Arc::new(parking_lot::Mutex::new(self));
            let base_interval = TRAIT_EVAL_INTERVAL;

            info!(
                interval_ms = base_interval.as_millis() as u64,
                jitter_percent = EVAL_JITTER_PERCENT,
                "trait evaluation loop started"
            );

            loop {
                // Compute jittered sleep duration: base +- 20%.
                let jitter_range =
                    base_interval.as_millis() as f64 * (EVAL_JITTER_PERCENT as f64 / 100.0);
                let jitter_offset = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(-jitter_range..=jitter_range)
                };
                let sleep_ms =
                    (base_interval.as_millis() as f64 + jitter_offset).max(100.0) as u64;

                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;

                // Read connectivity info under a short-lived read lock.
                let conn = {
                    let guard = connectivity.read();
                    guard.clone()
                };

                // Run the evaluation on the blocking thread pool because
                // measure_resources() and measure_load() use sysinfo which
                // performs synchronous system calls (including a short sleep
                // for CPU measurement). We must not block the tokio runtime.
                let eval_handle = evaluator.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let mut guard = eval_handle.lock();
                    guard.evaluate(&conn)
                });

                match result.await {
                    Ok((traits, load, resources)) => {
                        callback(traits, load, resources);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "trait evaluation task panicked");
                    }
                }
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
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    /// Helper: build a resource snapshot with the given parameters.
    fn make_resources(
        cpu_available: f32,
        memory_available_mb: u64,
        disk_available_mb: u64,
        bandwidth_mbps: f32,
    ) -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_cores: 4,
            cpu_available,
            memory_total_mb: 8192,
            memory_available_mb,
            disk_total_mb: 102400,
            disk_available_mb,
            network_bandwidth_mbps: bandwidth_mbps,
                current_tdp_watts: 15.0,
                ask_usd_per_megagas: 0.0001,
        }
    }

    /// Helper: build connectivity info for a public node.
    fn public_connectivity() -> ConnectivityInfo {
        ConnectivityInfo {
            is_publicly_reachable: true,
            nat_type: NatType::Public,
            has_relay_access: false,
            listen_address: Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
                4200,
            )),
            avg_rtt_ms: Some(5.0),
        }
    }

    /// Helper: build connectivity info for a NATed node with relay.
    fn natted_with_relay() -> ConnectivityInfo {
        ConnectivityInfo {
            is_publicly_reachable: false,
            nat_type: NatType::ConeNat,
            has_relay_access: true,
            listen_address: None,
            avg_rtt_ms: Some(25.0),
        }
    }

    /// Helper: default empty override sets.
    fn no_overrides() -> (HashSet<Trait>, HashSet<Trait>) {
        (HashSet::new(), HashSet::new())
    }

    #[test]
    fn idle_public_node_gets_all_traits() {
        let resources = make_resources(0.9, 4096, 51200, 100.0);
        let conn = public_connectivity();
        let (force, deny) = no_overrides();

        let traits = evaluate_traits(&resources, 0.1, &conn, &force, &deny);

        assert!(traits.contains(&Trait::CanExecute));
        assert!(traits.contains(&Trait::CanForward));
        assert!(traits.contains(&Trait::CanAggregate));
        assert!(traits.contains(&Trait::CanStoreState));
        assert!(traits.contains(&Trait::CanDiscover));
        assert!(traits.contains(&Trait::CanRelay));
    }

    #[test]
    fn overloaded_node_sheds_traits() {
        let resources = make_resources(0.9, 4096, 51200, 100.0);
        let conn = public_connectivity();
        let (force, deny) = no_overrides();

        // load = 0.85 exceeds MAX_EXECUTE_LOAD (0.8)
        let traits = evaluate_traits(&resources, 0.85, &conn, &force, &deny);

        assert!(!traits.contains(&Trait::CanExecute));
        assert!(!traits.contains(&Trait::CanForward));
        assert!(!traits.contains(&Trait::CanAggregate));
        assert!(!traits.contains(&Trait::CanStoreState));
        assert!(!traits.contains(&Trait::CanRelay));
        // CanDiscover only depends on connectivity, not load
        assert!(traits.contains(&Trait::CanDiscover));
    }

    #[test]
    fn natted_node_cannot_forward_or_relay() {
        let resources = make_resources(0.9, 4096, 51200, 100.0);
        let conn = natted_with_relay();
        let (force, deny) = no_overrides();

        let traits = evaluate_traits(&resources, 0.1, &conn, &force, &deny);

        assert!(traits.contains(&Trait::CanExecute));
        assert!(!traits.contains(&Trait::CanForward));
        assert!(traits.contains(&Trait::CanAggregate));
        assert!(traits.contains(&Trait::CanStoreState));
        assert!(traits.contains(&Trait::CanDiscover)); // has relay access
        assert!(!traits.contains(&Trait::CanRelay));
    }

    #[test]
    fn unknown_nat_prevents_discover() {
        let conn = ConnectivityInfo {
            is_publicly_reachable: false,
            nat_type: NatType::Unknown,
            has_relay_access: true,
            listen_address: None,
            avg_rtt_ms: None,
        };
        let resources = make_resources(0.9, 4096, 51200, 100.0);
        let (force, deny) = no_overrides();

        let traits = evaluate_traits(&resources, 0.1, &conn, &force, &deny);

        assert!(!traits.contains(&Trait::CanDiscover));
    }

    #[test]
    fn low_resources_shed_specific_traits() {
        let resources = make_resources(
            0.05, // below THRESHOLD_EXECUTE_CPU (0.1)
            256,  // below THRESHOLD_AGGREGATE_RAM_MB (512)
            500,  // below THRESHOLD_STORE_DISK_MB (1024)
            5.0,  // below THRESHOLD_RELAY_BANDWIDTH_MBPS (10.0)
        );
        let conn = public_connectivity();
        let (force, deny) = no_overrides();

        let traits = evaluate_traits(&resources, 0.1, &conn, &force, &deny);

        assert!(!traits.contains(&Trait::CanExecute));
        assert!(!traits.contains(&Trait::CanAggregate));
        assert!(!traits.contains(&Trait::CanStoreState));
        assert!(!traits.contains(&Trait::CanRelay));
        // These depend on connectivity/load, not resources
        assert!(traits.contains(&Trait::CanForward));
        assert!(traits.contains(&Trait::CanDiscover));
    }

    #[test]
    fn force_traits_override_resource_checks() {
        let resources = make_resources(0.0, 0, 0, 0.0);
        let conn = ConnectivityInfo::default();

        let mut force = HashSet::new();
        force.insert(Trait::CanExecute);
        force.insert(Trait::CanStoreState);
        let deny = HashSet::new();

        let traits = evaluate_traits(&resources, 1.0, &conn, &force, &deny);

        assert!(traits.contains(&Trait::CanExecute));
        assert!(traits.contains(&Trait::CanStoreState));
        assert!(!traits.contains(&Trait::CanForward));
    }

    #[test]
    fn deny_traits_override_resource_checks() {
        let resources = make_resources(0.9, 4096, 51200, 100.0);
        let conn = public_connectivity();

        let force = HashSet::new();
        let mut deny = HashSet::new();
        deny.insert(Trait::CanExecute);
        deny.insert(Trait::CanRelay);

        let traits = evaluate_traits(&resources, 0.1, &conn, &force, &deny);

        assert!(!traits.contains(&Trait::CanExecute));
        assert!(!traits.contains(&Trait::CanRelay));
        // Others are unaffected
        assert!(traits.contains(&Trait::CanForward));
        assert!(traits.contains(&Trait::CanAggregate));
    }

    #[test]
    fn force_wins_over_deny_for_same_trait() {
        let resources = make_resources(0.0, 0, 0, 0.0);
        let conn = ConnectivityInfo::default();

        let mut force = HashSet::new();
        force.insert(Trait::CanExecute);
        let mut deny = HashSet::new();
        deny.insert(Trait::CanExecute);

        let traits = evaluate_traits(&resources, 1.0, &conn, &force, &deny);

        // force_traits wins: the trait is present
        assert!(traits.contains(&Trait::CanExecute));
    }

    #[test]
    fn moderate_load_sheds_heavy_traits_only() {
        let resources = make_resources(0.9, 4096, 51200, 100.0);
        let conn = public_connectivity();
        let (force, deny) = no_overrides();

        // load = 0.55 is above CanRelay (0.5) and CanForward (0.5)
        // but below CanStoreState (0.6), CanAggregate (0.7), CanExecute (0.8)
        let traits = evaluate_traits(&resources, 0.55, &conn, &force, &deny);

        assert!(traits.contains(&Trait::CanExecute));
        assert!(!traits.contains(&Trait::CanForward));
        assert!(traits.contains(&Trait::CanAggregate));
        assert!(traits.contains(&Trait::CanStoreState));
        assert!(traits.contains(&Trait::CanDiscover));
        assert!(!traits.contains(&Trait::CanRelay));
    }

    #[test]
    fn evaluator_tracks_previous_traits() {
        let conn = public_connectivity();
        let mut evaluator = TraitEvaluator::new(HashSet::new(), HashSet::new());

        // The first call measures real system resources, so we just verify
        // that it returns a valid triple and does not panic.
        let (traits, load, resources) = evaluator.evaluate(&conn);

        assert!(load >= 0.0 && load <= 1.0);
        assert!(resources.cpu_cores > 0);
        // After the first evaluation, previous_traits should be set.
        assert_eq!(evaluator.previous_traits, traits);
    }

    #[test]
    fn measure_resources_returns_sane_values() {
        let snap = measure_resources();

        assert!(snap.cpu_cores > 0, "must detect at least one CPU core");
        assert!(
            snap.cpu_available >= 0.0 && snap.cpu_available <= 1.0,
            "cpu_available out of range: {}",
            snap.cpu_available
        );
        assert!(snap.memory_total_mb > 0, "must detect some memory");
        assert!(
            snap.memory_available_mb <= snap.memory_total_mb,
            "available memory exceeds total"
        );
        assert!(
            snap.network_bandwidth_mbps > 0.0,
            "bandwidth should default to > 0"
        );
    }

    #[test]
    fn measure_load_returns_valid_fraction() {
        let load = measure_load();
        assert!(
            load >= 0.0 && load <= 1.0,
            "load out of [0, 1] range: {}",
            load
        );
    }

    // ====================================================================
    // Hardware class and tiered threshold tests
    // ====================================================================

    /// Helper: build a resource snapshot with explicit hardware parameters.
    fn make_resources_full(
        cpu_cores: u32,
        cpu_available: f32,
        memory_total_mb: u64,
        memory_available_mb: u64,
        disk_total_mb: u64,
        disk_available_mb: u64,
        bandwidth_mbps: f32,
    ) -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_cores,
            cpu_available,
            memory_total_mb,
            memory_available_mb,
            disk_total_mb,
            disk_available_mb,
            network_bandwidth_mbps: bandwidth_mbps,
                current_tdp_watts: 15.0,
                ask_usd_per_megagas: 0.0001,
        }
    }

    #[test]
    fn test_hardware_class_detection() {
        // Minimal: < 2 cores or < 2 GB RAM
        assert_eq!(HardwareClass::detect(1, 1024), HardwareClass::Minimal);
        assert_eq!(HardwareClass::detect(1, 2048), HardwareClass::Minimal); // 1 core < 2
        assert_eq!(HardwareClass::detect(4, 1024), HardwareClass::Minimal); // 1024 < 2048

        // Light: 2-4 cores, 2-4 GB RAM
        assert_eq!(HardwareClass::detect(2, 2048), HardwareClass::Light);
        assert_eq!(HardwareClass::detect(4, 4096), HardwareClass::Light);

        // Standard: 4-8 cores, 4-16 GB RAM (must exceed Light thresholds)
        assert_eq!(HardwareClass::detect(5, 8192), HardwareClass::Standard);
        assert_eq!(HardwareClass::detect(8, 16384), HardwareClass::Standard);

        // Heavy: 8+ cores, 16+ GB RAM (must exceed Standard thresholds)
        assert_eq!(HardwareClass::detect(16, 32768), HardwareClass::Heavy);
        assert_eq!(HardwareClass::detect(9, 16385), HardwareClass::Heavy);
    }

    #[test]
    fn test_minimal_node_keeps_execute_trait() {
        // 1 core, 1 GB RAM = Minimal class.
        // Load 0.92 is under Minimal's max_execute_load of 0.95.
        let resources = make_resources_full(1, 0.5, 1024, 200, 10000, 2000, 100.0);
        let conn = public_connectivity();
        let (force, deny) = no_overrides();

        let traits = evaluate_traits(&resources, 0.92, &conn, &force, &deny);

        assert!(
            traits.contains(&Trait::CanExecute),
            "Minimal node at load 0.92 should still have CanExecute (threshold 0.95)"
        );
    }

    #[test]
    fn test_standard_node_loses_execute_at_80() {
        // 4 cores, 8 GB RAM = Standard class.
        // Load 0.85 exceeds Standard's max_execute_load of 0.80.
        let resources = make_resources_full(4, 0.5, 8192, 4000, 102400, 51200, 100.0);
        let conn = public_connectivity();
        let (force, deny) = no_overrides();

        let traits = evaluate_traits(&resources, 0.85, &conn, &force, &deny);

        assert!(
            !traits.contains(&Trait::CanExecute),
            "Standard node at load 0.85 should lose CanExecute (threshold 0.80)"
        );
    }

    #[test]
    fn test_per_class_concurrent_chunks() {
        use super::super::config::thresholds_for_class;

        assert_eq!(thresholds_for_class(HardwareClass::Minimal).max_concurrent_chunks, 1);
        assert_eq!(thresholds_for_class(HardwareClass::Light).max_concurrent_chunks, 2);
        assert_eq!(thresholds_for_class(HardwareClass::Standard).max_concurrent_chunks, 4);
        assert_eq!(thresholds_for_class(HardwareClass::Heavy).max_concurrent_chunks, 8);
    }

    #[test]
    fn test_minimal_node_can_aggregate() {
        // 1 core, 1.5 GB RAM = Minimal class.
        // 200 MB available RAM > Minimal's min_aggregate_ram_mb of 128.
        // Load 0.5 < Minimal's max_aggregate_load of 0.9.
        let resources = make_resources_full(1, 0.5, 1536, 200, 10000, 2000, 100.0);
        let conn = public_connectivity();
        let (force, deny) = no_overrides();

        let traits = evaluate_traits(&resources, 0.5, &conn, &force, &deny);

        assert!(
            traits.contains(&Trait::CanAggregate),
            "Minimal node with 200 MB free RAM should have CanAggregate (threshold 128 MB)"
        );
    }
}
