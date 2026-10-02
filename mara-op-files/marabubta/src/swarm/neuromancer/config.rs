// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct WildDogsConfig {
    pub quorum_threshold: f32,
    pub correlation_window: Duration,
    pub min_cluster_size: usize,
    pub similarity_threshold: f32,
    pub max_concurrent_hunts: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct ViperConfig {
    pub trapdoor_frequency: f32,
    pub max_quarantined: usize,
    pub grace_period: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct ElektraConfig {
    pub voltage_limit: f32,
    pub blacklist_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct SpiderConfig {
    pub mesh_density: f32,
    pub ema_alpha: f64,
    pub warmup_samples: usize,
    pub preemptive_checkpoint_threshold: f64,
    pub anomaly_threshold: f64,
    pub collection_interval: Duration,
    pub anomaly_sustained_duration: Duration,
    pub stale_node_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct CrocodileConfig {
    pub ambush_threshold: f32,
    pub max_active_honeypots: usize,
    pub honeypot_timeout: Duration,
    pub fast_track_timeout: Duration,
    pub honeypot_ratio: u32,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct LazarusConfig {
    pub recovery_delay_ms: u64,
    pub data_fragments: usize,
    pub redundancy_fragments: usize,
    pub max_checkpoint_size: usize,
    pub retention_per_task: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct CrowConfig {
    pub observation_window_secs: u64,
    pub ring_buffer_capacity: usize,
    pub dedup_set_capacity: usize,
    pub data_dir: PathBuf,
    pub compact_after_days: u32,
    pub retention_days: u32,
    pub max_log_size_mb: u64,
    pub rotation: String,
    pub gossip_share_window_minutes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct EngramConfig {
    pub memory_retention_secs: u64,
    pub data_dir: PathBuf,
    pub max_storage_mb: u64,
    pub eviction_trigger_percent: u64,
    pub eviction_target_percent: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct MantisConfig {
    pub camouflage_level: f32,
    pub enable_intel_gathering: bool,
    pub enable_immigration: bool,
    pub max_deception_duration: Duration,
    pub immigration_honeypot_count: usize,
    pub immigration_pass_rate: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct GhostConfig {
    pub trace_obfuscation: bool,
    pub enable_cache: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct ChopShopConfig {
    pub recycling_rate: f32,
    pub enable_hotplug: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct SandmanConfig {
    pub sleep_mode_threshold: f32,
    pub enabled: bool,
    pub min_sequence_length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq)]
#[derive(Default)]
pub enum AggressionLevel {
    #[default]
    Low,
    Medium,
    High,
    Paranoid,
    Balanced,
    Permissive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct OrcaConfig {
    pub aggression: AggressionLevel,
    pub auto_escalate_window: Duration,
    pub auto_escalate_threshold: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct WintermuteConfig {
    pub logic_gate_complexity: u32,
    pub max_pending: usize,
    pub task_timeout: Duration,
    pub max_running: usize,
    pub queue_check_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NeuromancerConfig {
    pub wild_dogs: Option<WildDogsConfig>,
    pub viper: Option<ViperConfig>,
    pub crow: Option<CrowConfig>,
    pub engram: Option<EngramConfig>,
    pub spider: Option<SpiderConfig>,
    pub lazarus: Option<LazarusConfig>,
    pub crocodile: Option<CrocodileConfig>,
    pub elektra: Option<ElektraConfig>,
    pub mantis: Option<MantisConfig>,
    pub ghost: Option<GhostConfig>,
    pub chop_shop: Option<ChopShopConfig>,
    pub sandman: Option<SandmanConfig>,
    pub orca: Option<OrcaConfig>,
    pub wintermute: Option<WintermuteConfig>,
    pub darwin: Option<DarwinConfig>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct DarwinConfig {
    pub enabled: bool,
    pub pipeline: Vec<String>,
    pub llm_endpoint: String,
    pub prompt_template: String,
    pub jira_url: String,
    pub jira_user: String,
    pub jira_token: String,
    pub jira_project: String,
    pub github_token: Option<String>,
    pub gitlab_token: Option<String>,
    pub bitbucket_token: Option<String>,
    pub artifact_storage_path: String,
    pub webhook_url: Option<String>,
}
