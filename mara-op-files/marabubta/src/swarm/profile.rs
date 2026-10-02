// Marabunta - Licensed under the MIT License.
//! Self-aware node profiles for the organic swarm.
//!
//! Each node in the Marabunta Swarm maintains a [`NodeProfile`] that describes
//! its hardware, runtimes, strengths, weaknesses, and scheduling preferences.
//! Profiles are auto-detected from the local environment and propagated via
//! gossip so the swarm can make informed placement decisions.
//!
//! Key types:
//! - [`NodeProfile`] -- the core self-description of a node
//! - [`JobRequirements`] -- what a job needs from a node
//! - [`ProfileStore`] -- concurrent storage for profiles across the swarm
//! - [`NodeType`], [`Strength`], [`Weakness`], [`Runtime`] -- classification enums

use chrono::{DateTime, Datelike, Timelike, Utc, Weekday};
use dashmap::DashMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::time::Duration;
use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, RefreshKind, System};
use tracing::debug;

use super::types::{InstalledSoftware, NodeId, ResourceSnapshot};

// ============================================================================
// Duration serialization helpers (millis wire format)
// ============================================================================

fn ser_opt_duration_ms<S>(val: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match val {
        Some(d) => serializer.serialize_some(&d.as_millis()),
        None => serializer.serialize_none(),
    }
}

fn de_opt_duration_ms<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<u128> = Option::deserialize(deserializer)?;
    Ok(opt.map(|ms| Duration::from_millis(ms as u64)))
}

// ============================================================================
// NodeType
// ============================================================================

/// Classification of the physical or virtual environment a node runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    #[default]
    BareMetal,
    CloudVM,
    Lambda,
    Container,
    Android,
    Browser,
    Desktop,
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeType::BareMetal => write!(f, "bare_metal"),
            NodeType::CloudVM => write!(f, "cloud_vm"),
            NodeType::Lambda => write!(f, "lambda"),
            NodeType::Container => write!(f, "container"),
            NodeType::Android => write!(f, "android"),
            NodeType::Browser => write!(f, "browser"),
            NodeType::Desktop => write!(f, "desktop"),
        }
    }
}

// ============================================================================
// Strength
// ============================================================================

/// A self-declared strength of a node, detected or manually declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strength {
    FastIO,
    LargeMemory,
    GPUCompute,
    LowLatencyNetwork,
    HighBandwidth,
    StableUptime,
    MultiRuntime,
    NVMeStorage,
    HighCoreCount,
}

impl fmt::Display for Strength {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Strength::FastIO => write!(f, "fast_io"),
            Strength::LargeMemory => write!(f, "large_memory"),
            Strength::GPUCompute => write!(f, "gpu_compute"),
            Strength::LowLatencyNetwork => write!(f, "low_latency_network"),
            Strength::HighBandwidth => write!(f, "high_bandwidth"),
            Strength::StableUptime => write!(f, "stable_uptime"),
            Strength::MultiRuntime => write!(f, "multi_runtime"),
            Strength::NVMeStorage => write!(f, "nvme_storage"),
            Strength::HighCoreCount => write!(f, "high_core_count"),
        }
    }
}

// ============================================================================
// Weakness
// ============================================================================

/// A self-admitted weakness, allowing the swarm to route work around known
/// limitations rather than discovering them through failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Weakness {
    /// May go offline unpredictably
    Intermittent,
    /// Low network bandwidth
    LowBandwidth,
    /// Subject to thermal throttling
    ThermalThrottled,
    /// Constrained by battery
    BatteryPowered,
    /// Short-lived (Lambda, browser tab)
    Ephemeral,
    /// Cannot persist anything locally
    NoLocalState,
    /// Restricted execution environment
    Sandboxed,
    /// Very constrained RAM
    LimitedMemory,
    /// Far from most peers
    HighLatency,
}

impl fmt::Display for Weakness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Weakness::Intermittent => write!(f, "intermittent"),
            Weakness::LowBandwidth => write!(f, "low_bandwidth"),
            Weakness::ThermalThrottled => write!(f, "thermal_throttled"),
            Weakness::BatteryPowered => write!(f, "battery_powered"),
            Weakness::Ephemeral => write!(f, "ephemeral"),
            Weakness::NoLocalState => write!(f, "no_local_state"),
            Weakness::Sandboxed => write!(f, "sandboxed"),
            Weakness::LimitedMemory => write!(f, "limited_memory"),
            Weakness::HighLatency => write!(f, "high_latency"),
        }
    }
}

// ============================================================================
// Runtime
// ============================================================================

/// A runtime environment that the node can execute tasks in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Runtime {
    Shell,
    Python3,
    NodeJS,
    Wasm,
    Docker,
    Java,
    DotNet,
    Go,
    Rust,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum HardwareEfficiencyTier {
    Performance,
    #[default]
    Balanced,
    Budget,
}


impl fmt::Display for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Runtime::Shell => write!(f, "shell"),
            Runtime::Python3 => write!(f, "python3"),
            Runtime::NodeJS => write!(f, "nodejs"),
            Runtime::Wasm => write!(f, "wasm"),
            Runtime::Docker => write!(f, "docker"),
            Runtime::Java => write!(f, "java"),
            Runtime::DotNet => write!(f, "dotnet"),
            Runtime::Go => write!(f, "go"),
            Runtime::Rust => write!(f, "rust"),
        }
    }
}

// ============================================================================
// AvailabilityCondition
// ============================================================================

/// A special condition that further constrains a time window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityCondition {
    ChargingOnly,
    WifiOnly,
    ChargingAndWifi,
    AlwaysOn,
}

// ============================================================================
// TimeWindow
// ============================================================================

/// A recurring time window during which a node is available for work.
///
/// The window is defined in UTC to avoid timezone ambiguity across the swarm.
/// If `start_hour_utc > end_hour_utc`, the window wraps around midnight
/// (e.g. 22:00 - 06:00).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindow {
    /// Start hour in UTC (0-23)
    pub start_hour_utc: u8,
    /// End hour in UTC (0-23)
    pub end_hour_utc: u8,
    /// Days of the week this window applies to (empty = all days)
    pub days: Vec<Weekday>,
    /// Optional additional constraint (e.g. charging only)
    pub special: Option<AvailabilityCondition>,
}

impl TimeWindow {
    /// A window that is always available, every day, all hours.
    pub fn always() -> Self {
        Self {
            start_hour_utc: 0,
            end_hour_utc: 0,
            days: Vec::new(),
            special: Some(AvailabilityCondition::AlwaysOn),
        }
    }

    /// Business hours: Monday-Friday, 9:00 - 17:00 UTC.
    pub fn business_hours() -> Self {
        Self {
            start_hour_utc: 9,
            end_hour_utc: 17,
            days: vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ],
            special: None,
        }
    }

    /// Charging and WiFi required -- available all hours, all days, but
    /// only when plugged in and on WiFi. Typical for mobile devices that
    /// contribute compute only when idle on a charger.
    pub fn charging_and_wifi() -> Self {
        Self {
            start_hour_utc: 0,
            end_hour_utc: 0,
            days: Vec::new(),
            special: Some(AvailabilityCondition::ChargingAndWifi),
        }
    }

    /// Check whether the current UTC time falls within this window.
    ///
    /// Note: this does **not** check the `special` condition (charging, wifi)
    /// because those require platform-specific APIs. The caller must verify
    /// `special` constraints separately.
    pub fn is_available_now(&self) -> bool {
        // AlwaysOn short-circuit
        if self.special == Some(AvailabilityCondition::AlwaysOn) {
            return true;
        }

        let now = Utc::now();
        let current_hour = now.hour() as u8;
        let current_day = now.weekday();

        // Check day-of-week constraint (empty = all days allowed)
        if !self.days.is_empty() && !self.days.contains(&current_day) {
            return false;
        }

        // Check hour constraint
        // Special case: start == end means "all hours" (full day)
        if self.start_hour_utc == self.end_hour_utc {
            return true;
        }

        if self.start_hour_utc < self.end_hour_utc {
            // Normal range: e.g. 9-17
            current_hour >= self.start_hour_utc && current_hour < self.end_hour_utc
        } else {
            // Wrapping range: e.g. 22-06 means 22,23,0,1,2,3,4,5
            current_hour >= self.start_hour_utc || current_hour < self.end_hour_utc
        }
    }
}

impl Default for TimeWindow {
    fn default() -> Self {
        Self::always()
    }
}

// ============================================================================
// GpuInfo
// ============================================================================

/// Information about a GPU available on the node.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GpuInfo {
    /// GPU model name (e.g. "NVIDIA RTX 4090")
    pub name: String,
    /// Video RAM in megabytes
    pub vram_mb: u64,
    /// CUDA core count (None for non-NVIDIA GPUs)
    pub cuda_cores: Option<u32>,
    /// Compute capability string (e.g. "8.9")
    pub compute_capability: Option<String>,
}

// ============================================================================
// NodeProfile
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PricingStrategy {
    /// Fixed asking price per instruction.
    Fixed { mmx_per_instruction: u64 },
    /// Dynamic pricing scaling with local CPU load.
    Dynamic { base_mmx: u64, load_multiplier: f32 },
    /// Turing-Complete Pricing Oracle (Rhai script).
    OracleScript { 
        /// The Rhai script source code that calculates the Ask price.
        script: String,
    },
}

impl Default for PricingStrategy {
    fn default() -> Self {
        PricingStrategy::Fixed { mmx_per_instruction: 1 } // Base rate of 1 MMX
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecurityPolicy {
    /// If true, the node will refuse to execute jobs with FHE/obfuscated payloads.
    pub require_cleartext_payloads: bool,
    /// If false, the node will refuse to execute jobs that require external network egress (HttpPush / Webhooks).
    pub allow_network_egress: bool,
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self {
            require_cleartext_payloads: false,
            allow_network_egress: true,
        }
    }
}

/// The core self-description of a swarm node.
///
/// A profile contains both auto-detected hardware information and
/// operator-declared metadata about the node's strengths, weaknesses,
/// and preferences. Profiles are versioned: each mutation increments
/// `version` so gossip can determine which copy is newer.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeProfile {
    pub node_id: NodeId,
    pub node_type: NodeType,
    /// Monotonically increasing version counter, bumped on every profile update.
    pub version: u64,
    
    // -- Economic Spot Market --
    /// The node's autonomous pricing algorithm for incoming job bids.
    #[serde(default)]
    pub pricing: PricingStrategy,
    
    /// The node's autonomous local triage scheduler for selecting the most desirable jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduler_script: Option<String>,
    
    // -- Strict Boundaries (The Enclave) --
    /// Non-negotiable security boundaries that override pricing.
    #[serde(default)]
    pub security_policy: SecurityPolicy,

    // -- Detected (auto-measured) --
    pub cpu_cores: u32,
    pub cpu_freq_mhz: u32,
    pub ram_total_mb: u64,
    pub disk_available_mb: u64,
    pub gpu: Option<GpuInfo>,
    pub hardware_attestations: Vec<String>,
    pub target_os: String,
    pub target_arch: String,
    pub hardware_tier: HardwareEfficiencyTier,
    /// Cryptographic quote (e.g. SGX / TPM 2.0) proving hardware architecture
    pub hardware_quote: Option<Vec<u8>>,
    
    /// Tier 2 Sovereign Identity Delegation (Optional)
    /// If present, this node acts on behalf of a FleetId/UserId for reputation and settlement.
    #[serde(default)]
    pub delegation_certificate: Option<crate::marabunta::identity::DelegationCertificate>,
    
    /// Tier 3 Financial Directive (Optional)
    /// If present, routing instructions signed by the FleetId for where to send MMX tokens.
    #[serde(default)]
    pub financial_directive: Option<crate::marabunta::identity::FinancialDirective>,

    // -- Economic & Security Reputation --
    /// Cryptographic Trust Score. Determines access to high-paying Enterprise jobs.
    #[serde(default)]
    pub trust_score: i32,
    /// Digital Purgatory Flag. If true, the node was caught faking compute and is banned
    /// until it pays a thermodynamic PoW fine via 'marabunta-cli apologize'.
    #[serde(default)]
    pub is_in_purgatory: bool,
    /// Thermodynamic Civic Duty Score. (Verifications Performed - Chunks Claimed).
    /// If this drops too low, the Orchestrator will force the node to audit ZKPs before granting paid work.
    #[serde(default)]
    pub civic_duty_score: i64,
    /// Number of pro-bono chunks completed while serving a thermodynamic sentence in Purgatory.
    #[serde(default)]
    pub purgatory_chunks_served: u32,

    // -- Fleet Identity --
    /// If this node operates under a Fleet, this is the Fleet's public key.
    #[serde(default)]
    pub fleet_public_key: Option<Vec<u8>>,
    /// If the Fleet accumulates 3 strikes (from malicious delegated nodes), 
    /// the entire Fleet (and all its nodes) is economically quarantined.
    #[serde(default)]
    pub fleet_strikes: u32,

    // -- Declared strengths --
    pub strengths: Vec<Strength>,
    pub runtimes: Vec<Runtime>,
    /// Free-form specialization tags (e.g. "ml-inference", "video-transcode")
    pub specializations: Vec<String>,

    // -- Admitted weaknesses --
    pub weaknesses: Vec<Weakness>,
    /// Maximum duration this node can sustain a single task
    #[serde(
        serialize_with = "ser_opt_duration_ms",
        deserialize_with = "de_opt_duration_ms"
    )]
    pub max_task_duration: Option<Duration>,
    pub availability_window: TimeWindow,

    // -- Preferences --
    /// Work types this node prefers (e.g. "ml-inference", "data-pipeline")
    pub preferred_work: Vec<String>,
    /// Work types this node wants to avoid
    pub avoided_work: Vec<String>,
    /// Maximum number of concurrent tasks
    pub max_concurrent: u32,

    // -- Discovered software (Phase 5) --
    /// Software automatically discovered on this node.
    #[serde(default)]
    pub installed_software: Vec<InstalledSoftware>,

    // -- Custom capabilities (Phase 5) --
    /// User-declared custom capabilities (e.g. "wolfram-api-key", "near-s3-us-east-1")
    #[serde(default)]
    pub custom_capabilities: Vec<String>,

    // -- Geo region (Phase 7 — energy) --
    /// ISO-3166 region code or custom string for energy price matching.
    #[serde(default)]
    pub geo_region: Option<String>,

    // -- Metadata --
    pub updated_at: DateTime<Utc>,
}

impl NodeProfile {
    /// Auto-detect hardware and create a profile for the current machine.
    ///
    /// Uses `sysinfo` to measure CPU cores, frequency, RAM, and disk.
    /// GPU detection is not performed (set to `None`); callers should
    /// populate the `gpu` field via platform-specific APIs if available.
    pub fn detect(node_id: NodeId, node_type: NodeType) -> Self {
        let sys = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::everything())
                .with_memory(MemoryRefreshKind::everything()),
        );

        let cpu_cores = sys.cpus().len() as u32;
        let cpu_freq_mhz = if !sys.cpus().is_empty() {
            // Average frequency across all cores
            let total_freq: u64 = sys.cpus().iter().map(|c| c.frequency()).sum();
            (total_freq / sys.cpus().len() as u64) as u32
        } else {
            0
        };
        let ram_total_mb = sys.total_memory() / (1024 * 1024);

        let disks = Disks::new_with_refreshed_list();
        let disk_available_mb = aggregate_disk_available(&disks);

        let gpu: Option<GpuInfo> = None;

        let strengths = Self::detect_strengths(cpu_cores, ram_total_mb, disk_available_mb, &gpu);

        debug!(
            cpu_cores,
            cpu_freq_mhz,
            ram_total_mb,
            disk_available_mb,
            strengths = strengths.len(),
            node_type = %node_type,
            "node profile detected"
        );

        Self {
            node_id,
            node_type,
            version: 1,
            cpu_cores,
            cpu_freq_mhz,
            ram_total_mb,
            disk_available_mb,
            gpu,
            hardware_attestations: Vec::new(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            hardware_tier: if cpu_freq_mhz < 2400 { HardwareEfficiencyTier::Budget } else if cpu_freq_mhz > 3200 { HardwareEfficiencyTier::Performance } else { HardwareEfficiencyTier::Balanced },
            hardware_quote: None, // Node operator must provide this manually via configuration
            delegation_certificate: None,
            financial_directive: None,
            trust_score: 0,
            is_in_purgatory: false,
            civic_duty_score: 0,
            purgatory_chunks_served: 0,
            fleet_public_key: None,
            fleet_strikes: 0,
            pricing: PricingStrategy::default(),
            scheduler_script: None,
            security_policy: SecurityPolicy::default(),
            strengths,
            runtimes: vec![Runtime::Shell],
            specializations: Vec::new(),
            weaknesses: Vec::new(),
            max_task_duration: None,
            availability_window: TimeWindow::always(),
            preferred_work: Vec::new(),
            avoided_work: Vec::new(),
            max_concurrent: cpu_cores.max(1),
            installed_software: Vec::new(),
            custom_capabilities: Vec::new(),
            geo_region: None,
            updated_at: Utc::now(),
        }
    }

    /// Create a profile for a bare-metal server with auto-detection.
    pub fn for_bare_metal(node_id: NodeId) -> Self {
        let mut profile = Self::detect(node_id, NodeType::BareMetal);
        profile.strengths.push(Strength::StableUptime);
        profile.runtimes = vec![Runtime::Shell, Runtime::Python3, Runtime::Docker];
        profile.strengths.dedup();
        profile
    }

    /// Create a profile for a cloud VM with auto-detection.
    pub fn for_cloud_vm(node_id: NodeId) -> Self {
        let mut profile = Self::detect(node_id, NodeType::CloudVM);
        profile.strengths.push(Strength::StableUptime);
        profile.runtimes = vec![Runtime::Shell, Runtime::Python3, Runtime::Docker];
        profile.strengths.dedup();
        profile
    }

    /// Create a profile for a Lambda/serverless function.
    pub fn for_lambda(node_id: NodeId) -> Self {
        let mut profile = Self::detect(node_id, NodeType::Lambda);
        profile.weaknesses = vec![Weakness::Ephemeral, Weakness::NoLocalState];
        profile.max_task_duration = Some(Duration::from_secs(900)); // 15 min
        profile.runtimes = vec![Runtime::Shell, Runtime::Python3, Runtime::NodeJS];
        profile.max_concurrent = 1;
        profile
    }

    /// Create a profile for a container.
    pub fn for_container(node_id: NodeId) -> Self {
        let mut profile = Self::detect(node_id, NodeType::Container);
        profile.weaknesses = vec![Weakness::Sandboxed];
        profile.runtimes = vec![Runtime::Shell, Runtime::Python3];
        profile
    }

    /// Create a profile for an Android device.
    pub fn for_android(node_id: NodeId) -> Self {
        let mut profile = Self::detect(node_id, NodeType::Android);
        profile.weaknesses = vec![
            Weakness::Intermittent,
            Weakness::BatteryPowered,
            Weakness::ThermalThrottled,
            Weakness::LimitedMemory,
        ];
        profile.availability_window = TimeWindow::charging_and_wifi();
        profile.runtimes = vec![Runtime::Shell, Runtime::Python3];
        profile.max_concurrent = 2;
        profile
    }

    /// Create a profile for a browser tab (WebAssembly).
    pub fn for_browser(node_id: NodeId) -> Self {
        let mut profile = Self::detect(node_id, NodeType::Browser);
        profile.weaknesses = vec![
            Weakness::Ephemeral,
            Weakness::NoLocalState,
            Weakness::Sandboxed,
            Weakness::LimitedMemory,
        ];
        profile.runtimes = vec![Runtime::Wasm];
        profile.max_task_duration = Some(Duration::from_secs(300)); // 5 min
        profile.max_concurrent = 1;
        profile
    }

    /// Create a profile for a desktop machine.
    pub fn for_desktop(node_id: NodeId) -> Self {
        let mut profile = Self::detect(node_id, NodeType::Desktop);
        profile.runtimes = vec![Runtime::Shell, Runtime::Python3, Runtime::Docker];
        profile
    }

    /// Compute a compatibility score (0.0 - 1.0) between this profile and
    /// a set of job requirements.
    ///
    /// Returns 0.0 if any hard constraint fails (use [`can_handle`] for a
    /// pure boolean check). The score considers:
    /// - Runtime availability (hard requirement, 0.0 if missing)
    /// - Resource headroom (memory, disk, CPU)
    /// - GPU match
    /// - Node type preference
    /// - Weakness penalties
    pub fn capability_match(&self, requirements: &JobRequirements) -> f32 {
        // Hard constraint: if can_handle fails, score is 0.
        if !self.can_handle(requirements) {
            return 0.0;
        }

        let mut score: f32 = 0.0;
        let mut weight_total: f32 = 0.0;

        // --- Runtime match (weighted 0.3) ---
        let runtime_weight = 0.3;
        weight_total += runtime_weight;
        if requirements.required_runtimes.is_empty() {
            score += runtime_weight; // no requirement = full score
        } else {
            let matched = requirements
                .required_runtimes
                .iter()
                .filter(|r| self.runtimes.contains(r))
                .count();
            let fraction = matched as f32 / requirements.required_runtimes.len() as f32;
            score += runtime_weight * fraction;
        }

        // --- Memory headroom (weighted 0.2) ---
        let mem_weight = 0.2;
        weight_total += mem_weight;
        if let Some(min_mem) = requirements.min_memory_mb {
            if min_mem > 0 {
                // Score based on how much headroom we have (capped at 2x)
                let ratio = (self.ram_total_mb as f32 / min_mem as f32).min(2.0) / 2.0;
                score += mem_weight * ratio;
            } else {
                score += mem_weight;
            }
        } else {
            score += mem_weight;
        }

        // --- Disk headroom (weighted 0.1) ---
        let disk_weight = 0.1;
        weight_total += disk_weight;
        if let Some(min_disk) = requirements.min_disk_mb {
            if min_disk > 0 {
                let ratio = (self.disk_available_mb as f32 / min_disk as f32).min(2.0) / 2.0;
                score += disk_weight * ratio;
            } else {
                score += disk_weight;
            }
        } else {
            score += disk_weight;
        }

        // --- CPU headroom (weighted 0.15) ---
        let cpu_weight = 0.15;
        weight_total += cpu_weight;
        if let Some(min_cpu) = requirements.min_cpu_cores {
            if min_cpu > 0 {
                let ratio = (self.cpu_cores as f32 / min_cpu as f32).min(2.0) / 2.0;
                score += cpu_weight * ratio;
            } else {
                score += cpu_weight;
            }
        } else {
            score += cpu_weight;
        }

        // --- GPU match (weighted 0.1) ---
        let gpu_weight = 0.1;
        weight_total += gpu_weight;
        if requirements.needs_gpu {
            if self.gpu.is_some() {
                score += gpu_weight;
            }
            // else: 0 (hard constraint already checked by can_handle)
        } else {
            score += gpu_weight;
        }

        // --- Node type preference (weighted 0.1) ---
        let type_weight = 0.1;
        weight_total += type_weight;
        if !requirements.preferred_node_types.is_empty() {
            if requirements.preferred_node_types.contains(&self.node_type) {
                score += type_weight;
            } else {
                score += type_weight * 0.5; // not preferred but not excluded
            }
        } else {
            score += type_weight;
        }

        // --- Weakness penalty (weighted 0.05) ---
        let weakness_weight = 0.05;
        weight_total += weakness_weight;
        if self.weaknesses.is_empty() {
            score += weakness_weight;
        } else {
            // Fewer weaknesses = higher score
            let penalty = (self.weaknesses.len() as f32 * 0.1).min(1.0);
            score += weakness_weight * (1.0 - penalty);
        }

        if weight_total > 0.0 {
            (score / weight_total).clamp(0.0, 1.0)
        } else {
            1.0
        }
    }

    /// Check whether this node can handle the given requirements at all
    /// (hard constraints only).
    ///
    /// Returns `false` if any of the following fail:
    /// - Required runtimes not available
    /// - Insufficient memory
    /// - Insufficient disk
    /// - Insufficient CPU cores
    /// - GPU required but not available
    /// - Duration exceeds max_task_duration
    /// - Node type is excluded
    pub fn can_handle(&self, requirements: &JobRequirements) -> bool {
        // Check excluded node types
        if requirements.excluded_node_types.contains(&self.node_type) {
            return false;
        }

        // Check required runtimes
        for rt in &requirements.required_runtimes {
            if !self.runtimes.contains(rt) {
                return false;
            }
        }

        // Check minimum memory
        if let Some(min_mem) = requirements.min_memory_mb {
            if self.ram_total_mb < min_mem {
                return false;
            }
        }

        // Check minimum disk
        if let Some(min_disk) = requirements.min_disk_mb {
            if self.disk_available_mb < min_disk {
                return false;
            }
        }

        // Check minimum CPU cores
        if let Some(min_cpu) = requirements.min_cpu_cores {
            if self.cpu_cores < min_cpu {
                return false;
            }
        }

        // Check GPU requirement
        if requirements.needs_gpu && self.gpu.is_none() {
            return false;
        }

        // Check duration constraint
        if let (Some(max_dur), Some(req_dur)) = (self.max_task_duration, requirements.max_duration)
        {
            if req_dur > max_dur {
                return false;
            }
        }

        // Check required capabilities against specializations and runtime names
        for cap in &requirements.required_capabilities {
            let cap_lower = cap.to_lowercase();
            let found = self
                .specializations
                .iter()
                .any(|s| s.to_lowercase() == cap_lower)
                || self.runtimes.iter().any(|r| r.to_string() == cap_lower)
                || self
                    .strengths
                    .iter()
                    .any(|s| s.to_string() == cap_lower);
            if !found {
                return false;
            }
        }

        true
    }

    /// Convert this profile to a compact [`ResourceSnapshot`] for gossip.
    ///
    /// The snapshot contains only the hardware metrics that the gossip
    /// layer needs for load-balancing decisions. The full profile can be
    /// requested separately when detailed matching is needed.
    pub fn to_resource_snapshot(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_cores: self.cpu_cores,
            cpu_available: 1.0, // snapshot does not carry live utilization
            memory_total_mb: self.ram_total_mb,
            memory_available_mb: self.ram_total_mb, // best-effort estimate
            disk_total_mb: self.disk_available_mb,
            disk_available_mb: self.disk_available_mb,
            network_bandwidth_mbps: 100.0,
                current_tdp_watts: 15.0,
                ask_usd_per_megagas: 0.0001, // default; callers override
        }
    }

    /// Auto-detect strengths from measured hardware characteristics.
    fn detect_strengths(
        cpu_cores: u32,
        ram_total_mb: u64,
        disk_mb: u64,
        gpu: &Option<GpuInfo>,
    ) -> Vec<Strength> {
        let mut strengths = Vec::new();

        if cpu_cores >= 16 {
            strengths.push(Strength::HighCoreCount);
        }

        if ram_total_mb >= 32_768 {
            // 32 GB
            strengths.push(Strength::LargeMemory);
        }

        if disk_mb >= 500_000 {
            // 500 GB available
            strengths.push(Strength::FastIO);
        }

        if gpu.is_some() {
            strengths.push(Strength::GPUCompute);
        }

        strengths
    }

    /// Bump the version counter and update the timestamp.
    ///
    /// Call this after any mutation to the profile so gossip propagation
    /// can determine which copy is newer.
    pub fn touch(&mut self) {
        self.version += 1;
        self.updated_at = Utc::now();
    }
}

// ============================================================================
// JobRequirements
// ============================================================================

/// Requirements that a job declares to constrain which nodes can execute it.
///
/// Used by [`NodeProfile::can_handle`] for hard constraints and
/// [`NodeProfile::capability_match`] for scored matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRequirements {
    /// Free-form capability strings (e.g. "gpu-compute", "python3")
    pub required_capabilities: Vec<String>,
    /// Specific runtimes needed to execute the job
    pub required_runtimes: Vec<Runtime>,
    /// Minimum RAM in MB
    pub min_memory_mb: Option<u64>,
    /// Minimum available disk in MB
    pub min_disk_mb: Option<u64>,
    /// Minimum CPU cores
    pub min_cpu_cores: Option<u32>,
    /// Whether the job requires a GPU
    pub needs_gpu: bool,
    /// Expected maximum duration of the job
    #[serde(
        serialize_with = "ser_opt_duration_ms",
        deserialize_with = "de_opt_duration_ms"
    )]
    pub max_duration: Option<Duration>,
    /// Node types preferred for this job (empty = no preference)
    pub preferred_node_types: Vec<NodeType>,
    /// Node types that must not run this job
    pub excluded_node_types: Vec<NodeType>,
    pub hardware_tier_preference: Option<HardwareEfficiencyTier>,
    /// Optional CEL/EvalExpr DAG for declarative, attribute-based placement rules.
    /// E.g. "node.os == 'windows' && node.has_blob('hash')"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advanced_placement_dag: Option<String>,
    /// Physical execution SLA boundary (The Reality Anchor)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub associated_topology: Option<crate::common::types::TopologyId>,
    
    // -- Economic Spot Market --
    /// Maximum budget per instruction. If a node's ask price exceeds this, it cannot claim the chunk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_mmx_per_instruction: Option<u64>,
}

impl JobRequirements {
    /// Minimal requirements -- just needs a node that can execute.
    pub fn minimal() -> Self {
        Self {
            required_capabilities: Vec::new(),
            required_runtimes: Vec::new(),
            min_memory_mb: None,
            min_disk_mb: None,
            min_cpu_cores: None,
            needs_gpu: false,
            max_duration: None,
            preferred_node_types: Vec::new(),
            excluded_node_types: Vec::new(),
            hardware_tier_preference: None,
            advanced_placement_dag: None,
            associated_topology: None,
            max_mmx_per_instruction: None,
        }
    }

    /// Requirements for GPU compute workloads.
    pub fn gpu_compute() -> Self {
        Self {
            required_capabilities: vec!["gpu_compute".to_string()],
            required_runtimes: Vec::new(),
            min_memory_mb: Some(4096),
            min_disk_mb: None,
            min_cpu_cores: None,
            needs_gpu: true,
            max_duration: None,
            preferred_node_types: vec![NodeType::BareMetal, NodeType::CloudVM],
            excluded_node_types: vec![NodeType::Browser, NodeType::Android],
            hardware_tier_preference: None,
            advanced_placement_dag: None,
            associated_topology: None,
            max_mmx_per_instruction: None,
        }
    }

    /// Requirements for Python workloads.
    pub fn python() -> Self {
        Self {
            required_capabilities: Vec::new(),
            required_runtimes: vec![Runtime::Python3],
            min_memory_mb: Some(512),
            min_disk_mb: None,
            min_cpu_cores: None,
            needs_gpu: false,
            max_duration: None,
            preferred_node_types: Vec::new(),
            excluded_node_types: Vec::new(),
            hardware_tier_preference: None,
            advanced_placement_dag: None,
            associated_topology: None,
            max_mmx_per_instruction: None,
        }
    }
}

impl Default for JobRequirements {
    fn default() -> Self {
        Self::minimal()
    }
}

// ============================================================================
// ProfileStore
// ============================================================================

/// Concurrent store for node profiles, backed by [`DashMap`].
///
/// Profiles are keyed by [`NodeId`] and use version-based conflict resolution:
/// a newer version always overwrites an older one.
pub struct ProfileStore {
    profiles: DashMap<NodeId, NodeProfile>,
}

impl ProfileStore {
    /// Create a new, empty profile store.
    pub fn new() -> Self {
        Self {
            profiles: DashMap::new(),
        }
    }

    /// Insert or update a profile.
    ///
    /// Returns `true` if the profile was accepted (new entry or newer version).
    /// Returns `false` if the incoming profile has a version <= the existing one.
    pub fn upsert(&self, profile: NodeProfile) -> bool {
        let node_id = profile.node_id;

        match self.profiles.entry(node_id) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                debug!(node = %node_id, version = profile.version, "profile: new profile stored");
                vacant.insert(profile);
                true
            }
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                if profile.version > occupied.get().version {
                    debug!(
                        node = %node_id,
                        old_version = occupied.get().version,
                        new_version = profile.version,
                        "profile: updated to newer version"
                    );
                    occupied.insert(profile);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Retrieve a profile by node ID.
    pub fn get(&self, node_id: &NodeId) -> Option<NodeProfile> {
        self.profiles.get(node_id).map(|r| r.value().clone())
    }

    /// Remove a profile.
    pub fn remove(&self, node_id: &NodeId) {
        self.profiles.remove(node_id);
    }

    /// Return a snapshot of all stored profiles.
    pub fn all_profiles(&self) -> Vec<NodeProfile> {
        self.profiles.iter().map(|r| r.value().clone()).collect()
    }

    /// Return all profiles that can handle the given requirements.
    ///
    /// Results are sorted by capability_match score in descending order
    /// (best match first).
    pub fn matching_profiles(&self, requirements: &JobRequirements) -> Vec<NodeProfile> {
        let mut matches: Vec<(f32, NodeProfile)> = self
            .profiles
            .iter()
            .filter_map(|r| {
                let profile = r.value().clone();
                if profile.can_handle(requirements) {
                    let score = profile.capability_match(requirements);
                    Some((score, profile))
                } else {
                    None
                }
            })
            .collect();

        // Sort by score descending
        matches.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        matches.into_iter().map(|(_, p)| p).collect()
    }

    /// Number of stored profiles.
    pub fn count(&self) -> usize {
        self.profiles.len()
    }

    /// Remove profiles whose `updated_at` is older than `max_age`.
    ///
    /// Returns the number of profiles removed.
    pub fn prune_stale(&self, max_age: chrono::Duration) -> usize {
        let cutoff = Utc::now() - max_age;

        let stale_ids: Vec<NodeId> = self
            .profiles
            .iter()
            .filter(|r| r.value().updated_at < cutoff)
            .map(|r| *r.key())
            .collect();

        let count = stale_ids.len();
        for id in stale_ids {
            self.profiles.remove(&id);
        }

        if count > 0 {
            debug!(pruned = count, "profile store: pruned stale profiles");
        }

        count
    }
}

impl Default for ProfileStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Disk measurement helper
// ============================================================================

/// Aggregate available disk space across all filesystems, deduplicating
/// by mount point (same pattern as `traits.rs`).
fn aggregate_disk_available(disks: &Disks) -> u64 {
    let mut seen = std::collections::HashSet::new();
    let mut available_mb: u64 = 0;

    for disk in disks.list() {
        let mount = disk.mount_point().to_path_buf();
        if seen.insert(mount) {
            available_mb = available_mb.saturating_add(disk.available_space() / (1024 * 1024));
        }
    }

    available_mb
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Helpers ----

    fn test_node_id() -> NodeId {
        NodeId::new()
    }

    /// Build a minimal profile for testing without hitting sysinfo.
    fn make_test_profile(node_id: NodeId, node_type: NodeType) -> NodeProfile {
        NodeProfile {
            node_id,
            node_type,
            version: 1,
            cpu_cores: 8,
            cpu_freq_mhz: 3200,
            ram_total_mb: 16384,
            disk_available_mb: 200_000,
            gpu: None,
            hardware_attestations: Vec::new(),
            strengths: vec![],
            runtimes: vec![Runtime::Shell, Runtime::Python3],
            specializations: vec!["ml-inference".to_string()],
            weaknesses: vec![],
            max_task_duration: None,
            availability_window: TimeWindow::always(),
            preferred_work: vec![],
            avoided_work: vec![],
            max_concurrent: 8,
            installed_software: vec![],
            custom_capabilities: vec![],
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            hardware_tier: HardwareEfficiencyTier::Balanced,
            hardware_quote: None,
            delegation_certificate: None,
            financial_directive: None,
            trust_score: 0,
            is_in_purgatory: false,
            civic_duty_score: 0,
            purgatory_chunks_served: 0,
            fleet_public_key: None,
            fleet_strikes: 0,
            pricing: PricingStrategy::default(),
            scheduler_script: None,
            security_policy: SecurityPolicy::default(),
            ..Default::default()
        }
    }

    // ---- NodeType Display ----

    #[test]
    fn node_type_display() {
        assert_eq!(NodeType::BareMetal.to_string(), "bare_metal");
        assert_eq!(NodeType::CloudVM.to_string(), "cloud_vm");
        assert_eq!(NodeType::Lambda.to_string(), "lambda");
        assert_eq!(NodeType::Container.to_string(), "container");
        assert_eq!(NodeType::Android.to_string(), "android");
        assert_eq!(NodeType::Browser.to_string(), "browser");
        assert_eq!(NodeType::Desktop.to_string(), "desktop");
    }

    // ---- Strength Display ----

    #[test]
    fn strength_display() {
        assert_eq!(Strength::FastIO.to_string(), "fast_io");
        assert_eq!(Strength::GPUCompute.to_string(), "gpu_compute");
        assert_eq!(Strength::HighCoreCount.to_string(), "high_core_count");
    }

    // ---- Weakness Display ----

    #[test]
    fn weakness_display() {
        assert_eq!(Weakness::Intermittent.to_string(), "intermittent");
        assert_eq!(Weakness::Ephemeral.to_string(), "ephemeral");
        assert_eq!(Weakness::Sandboxed.to_string(), "sandboxed");
    }

    // ---- Runtime Display ----

    #[test]
    fn runtime_display() {
        assert_eq!(Runtime::Shell.to_string(), "shell");
        assert_eq!(Runtime::Python3.to_string(), "python3");
        assert_eq!(Runtime::Wasm.to_string(), "wasm");
        assert_eq!(Runtime::Docker.to_string(), "docker");
    }

    // ---- TimeWindow ----

    #[test]
    fn time_window_always_is_available() {
        let w = TimeWindow::always();
        assert!(w.is_available_now());
    }

    #[test]
    fn time_window_default_is_always() {
        let w = TimeWindow::default();
        assert!(w.is_available_now());
    }

    #[test]
    fn time_window_business_hours_day_check() {
        let w = TimeWindow::business_hours();
        // We can't control the current time in tests, but we can verify the
        // structure is correct.
        assert_eq!(w.start_hour_utc, 9);
        assert_eq!(w.end_hour_utc, 17);
        assert_eq!(w.days.len(), 5);
        assert!(w.days.contains(&Weekday::Mon));
        assert!(w.days.contains(&Weekday::Fri));
        assert!(!w.days.contains(&Weekday::Sat));
    }

    #[test]
    fn time_window_wrap_around_midnight() {
        // A window from 22:00 to 06:00 should include hour 23 and hour 3
        let w = TimeWindow {
            start_hour_utc: 22,
            end_hour_utc: 6,
            days: Vec::new(),
            special: None,
        };
        // We cannot fully test runtime-dependent behavior, but we verify
        // the logic path by constructing and checking the struct.
        assert_eq!(w.start_hour_utc, 22);
        assert_eq!(w.end_hour_utc, 6);
        // The actual is_available_now() depends on current time, so we
        // just ensure it does not panic.
        let _ = w.is_available_now();
    }

    #[test]
    fn time_window_charging_and_wifi() {
        let w = TimeWindow::charging_and_wifi();
        assert_eq!(w.special, Some(AvailabilityCondition::ChargingAndWifi));
        // All hours, all days (start == end means all hours)
        assert_eq!(w.start_hour_utc, 0);
        assert_eq!(w.end_hour_utc, 0);
        assert!(w.days.is_empty());
    }

    // ---- detect_strengths ----

    #[test]
    fn detect_strengths_high_core_count() {
        let strengths = NodeProfile::detect_strengths(32, 8192, 100_000, &None);
        assert!(strengths.contains(&Strength::HighCoreCount));
        assert!(!strengths.contains(&Strength::LargeMemory));
    }

    #[test]
    fn detect_strengths_large_memory() {
        let strengths = NodeProfile::detect_strengths(4, 65536, 100_000, &None);
        assert!(strengths.contains(&Strength::LargeMemory));
        assert!(!strengths.contains(&Strength::HighCoreCount));
    }

    #[test]
    fn detect_strengths_gpu() {
        let gpu = Some(GpuInfo {
            name: "Test GPU".to_string(),
            vram_mb: 8192,
            cuda_cores: Some(5120),
            compute_capability: Some("8.6".to_string()),
        });
        let strengths = NodeProfile::detect_strengths(4, 8192, 100_000, &gpu);
        assert!(strengths.contains(&Strength::GPUCompute));
    }

    #[test]
    fn detect_strengths_fast_io() {
        let strengths = NodeProfile::detect_strengths(4, 8192, 600_000, &None);
        assert!(strengths.contains(&Strength::FastIO));
    }

    #[test]
    fn detect_strengths_nothing_special() {
        let strengths = NodeProfile::detect_strengths(4, 8192, 100_000, &None);
        assert!(strengths.is_empty());
    }

    // ---- can_handle ----

    #[test]
    fn can_handle_minimal_requirements() {
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        let reqs = JobRequirements::minimal();
        assert!(profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_python_requirements() {
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        let reqs = JobRequirements::python();
        assert!(profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_gpu_requirements_without_gpu() {
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        let reqs = JobRequirements::gpu_compute();
        assert!(!profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_gpu_requirements_with_gpu() {
        let mut profile = make_test_profile(test_node_id(), NodeType::BareMetal);
        profile.gpu = Some(GpuInfo {
            name: "RTX 4090".to_string(),
            vram_mb: 24576,
            cuda_cores: Some(16384),
            compute_capability: Some("8.9".to_string()),
        });
        profile.strengths.push(Strength::GPUCompute);
        let reqs = JobRequirements::gpu_compute();
        assert!(profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_excluded_node_type() {
        let profile = make_test_profile(test_node_id(), NodeType::Browser);
        let reqs = JobRequirements {
            excluded_node_types: vec![NodeType::Browser],
            hardware_tier_preference: None,
            ..JobRequirements::minimal()
        };
        assert!(!profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_insufficient_memory() {
        let mut profile = make_test_profile(test_node_id(), NodeType::Desktop);
        profile.ram_total_mb = 256;
        let reqs = JobRequirements {
            min_memory_mb: Some(1024),
            ..JobRequirements::minimal()
        };
        assert!(!profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_insufficient_cpu() {
        let mut profile = make_test_profile(test_node_id(), NodeType::Desktop);
        profile.cpu_cores = 2;
        let reqs = JobRequirements {
            min_cpu_cores: Some(8),
            ..JobRequirements::minimal()
        };
        assert!(!profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_missing_runtime() {
        let mut profile = make_test_profile(test_node_id(), NodeType::Desktop);
        profile.runtimes = vec![Runtime::Shell];
        let reqs = JobRequirements {
            required_runtimes: vec![Runtime::Wasm],
            ..JobRequirements::minimal()
        };
        assert!(!profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_duration_constraint() {
        let mut profile = make_test_profile(test_node_id(), NodeType::Lambda);
        profile.max_task_duration = Some(Duration::from_secs(300));
        let reqs = JobRequirements {
            max_duration: Some(Duration::from_secs(600)),
            ..JobRequirements::minimal()
        };
        assert!(!profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_duration_within_limit() {
        let mut profile = make_test_profile(test_node_id(), NodeType::Lambda);
        profile.max_task_duration = Some(Duration::from_secs(900));
        let reqs = JobRequirements {
            max_duration: Some(Duration::from_secs(600)),
            ..JobRequirements::minimal()
        };
        assert!(profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_required_capability() {
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        let reqs = JobRequirements {
            required_capabilities: vec!["ml-inference".to_string()],
            ..JobRequirements::minimal()
        };
        assert!(profile.can_handle(&reqs));
    }

    #[test]
    fn can_handle_missing_capability() {
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        let reqs = JobRequirements {
            required_capabilities: vec!["video-transcode".to_string()],
            ..JobRequirements::minimal()
        };
        assert!(!profile.can_handle(&reqs));
    }

    // ---- capability_match ----

    #[test]
    fn capability_match_zero_when_cannot_handle() {
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        let reqs = JobRequirements::gpu_compute();
        assert_eq!(profile.capability_match(&reqs), 0.0);
    }

    #[test]
    fn capability_match_high_for_well_matched() {
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        let reqs = JobRequirements::minimal();
        let score = profile.capability_match(&reqs);
        assert!(
            score > 0.8,
            "well-matched profile should score > 0.8, got {}",
            score
        );
    }

    #[test]
    fn capability_match_preferred_node_type_bonus() {
        let profile = make_test_profile(test_node_id(), NodeType::BareMetal);
        let reqs_preferred = JobRequirements {
            preferred_node_types: vec![NodeType::BareMetal],
            ..JobRequirements::minimal()
        };
        let reqs_not_preferred = JobRequirements {
            preferred_node_types: vec![NodeType::CloudVM],
            ..JobRequirements::minimal()
        };

        let score_preferred = profile.capability_match(&reqs_preferred);
        let score_not_preferred = profile.capability_match(&reqs_not_preferred);
        assert!(
            score_preferred > score_not_preferred,
            "preferred: {} should be > not preferred: {}",
            score_preferred,
            score_not_preferred
        );
    }

    // ---- to_resource_snapshot ----

    #[test]
    fn to_resource_snapshot_maps_correctly() {
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        let snap = profile.to_resource_snapshot();
        assert_eq!(snap.cpu_cores, 8);
        assert_eq!(snap.memory_total_mb, 16384);
        assert_eq!(snap.disk_available_mb, 200_000);
    }

    // ---- touch ----

    #[test]
    fn touch_increments_version() {
        let mut profile = make_test_profile(test_node_id(), NodeType::Desktop);
        assert_eq!(profile.version, 1);
        let old_ts = profile.updated_at;

        // Small sleep to ensure timestamp advances
        std::thread::sleep(Duration::from_millis(2));
        profile.touch();

        assert_eq!(profile.version, 2);
        assert!(profile.updated_at >= old_ts);
    }

    // ---- ProfileStore ----

    #[test]
    fn profile_store_upsert_new() {
        let store = ProfileStore::new();
        let profile = make_test_profile(test_node_id(), NodeType::Desktop);
        assert!(store.upsert(profile));
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn profile_store_upsert_newer_version() {
        let store = ProfileStore::new();
        let id = test_node_id();

        let p1 = make_test_profile(id, NodeType::Desktop);
        assert!(store.upsert(p1));

        let mut p2 = make_test_profile(id, NodeType::Desktop);
        p2.version = 2;
        p2.cpu_cores = 16;
        assert!(store.upsert(p2));

        let stored = store.get(&id).expect("profile should exist");
        assert_eq!(stored.version, 2);
        assert_eq!(stored.cpu_cores, 16);
    }

    #[test]
    fn profile_store_upsert_stale_rejected() {
        let store = ProfileStore::new();
        let id = test_node_id();

        let mut p1 = make_test_profile(id, NodeType::Desktop);
        p1.version = 5;
        assert!(store.upsert(p1));

        let mut p2 = make_test_profile(id, NodeType::Desktop);
        p2.version = 3; // older
        assert!(!store.upsert(p2));

        assert_eq!(store.get(&id).expect("exists").version, 5);
    }

    #[test]
    fn profile_store_remove() {
        let store = ProfileStore::new();
        let id = test_node_id();
        store.upsert(make_test_profile(id, NodeType::Desktop));
        assert_eq!(store.count(), 1);

        store.remove(&id);
        assert_eq!(store.count(), 0);
        assert!(store.get(&id).is_none());
    }

    #[test]
    fn profile_store_all_profiles() {
        let store = ProfileStore::new();
        for _ in 0..5 {
            store.upsert(make_test_profile(test_node_id(), NodeType::Desktop));
        }
        assert_eq!(store.all_profiles().len(), 5);
    }

    #[test]
    fn profile_store_matching_profiles() {
        let store = ProfileStore::new();

        // A desktop with Python
        let mut p1 = make_test_profile(test_node_id(), NodeType::Desktop);
        p1.runtimes = vec![Runtime::Shell, Runtime::Python3];
        store.upsert(p1);

        // A browser with only Wasm
        let mut p2 = make_test_profile(test_node_id(), NodeType::Browser);
        p2.runtimes = vec![Runtime::Wasm];
        store.upsert(p2);

        let reqs = JobRequirements::python();
        let matches = store.matching_profiles(&reqs);

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].node_type, NodeType::Desktop);
    }

    #[test]
    fn profile_store_matching_profiles_sorted_by_score() {
        let store = ProfileStore::new();

        // Lower-spec node
        let mut p1 = make_test_profile(test_node_id(), NodeType::Desktop);
        p1.ram_total_mb = 1024;
        p1.cpu_cores = 2;
        store.upsert(p1);

        // Higher-spec node
        let mut p2 = make_test_profile(test_node_id(), NodeType::BareMetal);
        p2.ram_total_mb = 65536;
        p2.cpu_cores = 32;
        p2.version = 1;
        store.upsert(p2);

        let reqs = JobRequirements {
            min_memory_mb: Some(512),
            min_cpu_cores: Some(2),
            ..JobRequirements::minimal()
        };
        let matches = store.matching_profiles(&reqs);

        assert_eq!(matches.len(), 2);
        // Higher-spec should be first (higher score)
        assert_eq!(matches[0].node_type, NodeType::BareMetal);
    }

    #[test]
    fn profile_store_prune_stale() {
        let store = ProfileStore::new();

        // Fresh profile
        store.upsert(make_test_profile(test_node_id(), NodeType::Desktop));

        // Stale profile
        let mut stale = make_test_profile(test_node_id(), NodeType::CloudVM);
        stale.updated_at = Utc::now() - chrono::Duration::hours(2);
        store.upsert(stale);

        let pruned = store.prune_stale(chrono::Duration::hours(1));
        assert_eq!(pruned, 1);
        assert_eq!(store.count(), 1);
    }

    // ---- Serialization round-trip ----

    #[test]
    fn node_profile_serialization_roundtrip() {
        let mut profile = make_test_profile(test_node_id(), NodeType::Lambda);
        profile.max_task_duration = Some(Duration::from_secs(900));
        profile.weaknesses = vec![Weakness::Ephemeral, Weakness::NoLocalState];
        profile.strengths = vec![Strength::FastIO];

        let json = serde_json::to_string(&profile).expect("serialize");
        let deserialized: NodeProfile = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.node_id, profile.node_id);
        assert_eq!(deserialized.node_type, NodeType::Lambda);
        assert_eq!(deserialized.max_task_duration, Some(Duration::from_secs(900)));
        assert_eq!(deserialized.weaknesses.len(), 2);
        assert_eq!(deserialized.strengths.len(), 1);
    }

    #[test]
    fn job_requirements_serialization_roundtrip() {
        let reqs = JobRequirements {
            required_capabilities: vec!["gpu_compute".to_string()],
            required_runtimes: vec![Runtime::Python3],
            min_memory_mb: Some(4096),
            min_disk_mb: Some(10240),
            min_cpu_cores: Some(4),
            needs_gpu: true,
            max_duration: Some(Duration::from_secs(3600)),
            preferred_node_types: vec![NodeType::BareMetal],
            excluded_node_types: vec![NodeType::Browser],
            hardware_tier_preference: None,
            advanced_placement_dag: None,
            associated_topology: None,
            max_mmx_per_instruction: None,
        };

        let json = serde_json::to_string(&reqs).expect("serialize");
        let deserialized: JobRequirements = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.required_runtimes, vec![Runtime::Python3]);
        assert_eq!(deserialized.min_memory_mb, Some(4096));
        assert!(deserialized.needs_gpu);
        assert_eq!(deserialized.max_duration, Some(Duration::from_secs(3600)));
        assert_eq!(deserialized.preferred_node_types, vec![NodeType::BareMetal]);
    }

    // ---- Constructor variants ----

    #[test]
    fn for_lambda_has_ephemeral_weakness() {
        let profile = NodeProfile::for_lambda(test_node_id());
        assert_eq!(profile.node_type, NodeType::Lambda);
        assert!(profile.weaknesses.contains(&Weakness::Ephemeral));
        assert!(profile.weaknesses.contains(&Weakness::NoLocalState));
        assert_eq!(profile.max_task_duration, Some(Duration::from_secs(900)));
        assert_eq!(profile.max_concurrent, 1);
    }

    #[test]
    fn for_android_has_battery_weakness() {
        let profile = NodeProfile::for_android(test_node_id());
        assert_eq!(profile.node_type, NodeType::Android);
        assert!(profile.weaknesses.contains(&Weakness::BatteryPowered));
        assert!(profile.weaknesses.contains(&Weakness::ThermalThrottled));
        assert!(profile.weaknesses.contains(&Weakness::Intermittent));
        assert_eq!(
            profile.availability_window.special,
            Some(AvailabilityCondition::ChargingAndWifi)
        );
    }

    #[test]
    fn for_browser_has_wasm_runtime() {
        let profile = NodeProfile::for_browser(test_node_id());
        assert_eq!(profile.node_type, NodeType::Browser);
        assert_eq!(profile.runtimes, vec![Runtime::Wasm]);
        assert!(profile.weaknesses.contains(&Weakness::Sandboxed));
        assert_eq!(profile.max_concurrent, 1);
    }

    #[test]
    fn for_container_has_sandboxed_weakness() {
        let profile = NodeProfile::for_container(test_node_id());
        assert_eq!(profile.node_type, NodeType::Container);
        assert!(profile.weaknesses.contains(&Weakness::Sandboxed));
    }

    #[test]
    fn for_bare_metal_has_stable_uptime() {
        let profile = NodeProfile::for_bare_metal(test_node_id());
        assert_eq!(profile.node_type, NodeType::BareMetal);
        assert!(profile.strengths.contains(&Strength::StableUptime));
        assert!(profile.runtimes.contains(&Runtime::Docker));
    }

    #[test]
    fn for_cloud_vm_has_stable_uptime() {
        let profile = NodeProfile::for_cloud_vm(test_node_id());
        assert_eq!(profile.node_type, NodeType::CloudVM);
        assert!(profile.strengths.contains(&Strength::StableUptime));
    }

    #[test]
    fn for_desktop_has_shell_and_python() {
        let profile = NodeProfile::for_desktop(test_node_id());
        assert_eq!(profile.node_type, NodeType::Desktop);
        assert!(profile.runtimes.contains(&Runtime::Shell));
        assert!(profile.runtimes.contains(&Runtime::Python3));
    }
}
