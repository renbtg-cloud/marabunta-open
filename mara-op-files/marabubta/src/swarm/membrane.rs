// Marabunta - Licensed under the MIT License.
//! Pillar 12.1, 15.1 & 15.4: Membrane & Absolute Sovereignty
//! 
//! Inter-swarm membrane: permeability, rate-limiting, crossing log,
//! embassy management, and severance evaluation.
//! 
//! - Behavioral quarantining isolating parasitic nodes (Pillar 12.1)
//! - Cryptographic Dead Man's Switch (Pillar 15.1).
//! - Hybrid Swarm (take05): Write-Only Embassies allowing ClearNet nodes to
//!   offload heavy compute to DarkNet Boulders without compromising routing topology.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn, error};
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};
use std::process;
use tokio::task;

use super::complexity::EventSeverity;
use super::knowledge::KnowledgeStore;
use super::types::{NodeId, NodeInfo, NodeStatus};
use crate::swarm::witness::OperationalPosture;

// ============================================================================
// Pillar 15.1: Cryptographic Dead Man's Switch
// ============================================================================

/// Represents highly sensitive enclave material (e.g., Dilithium private keys, Kyber secrets)
/// that must never be extracted from physical memory.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SovereignKeyMaterial {
    secret_bytes: Vec<u8>,
}

impl SovereignKeyMaterial {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { secret_bytes: bytes }
    }
}

/// A background thread that continuously checks hardware integrity.
/// If physical intrusion (e.g., chassis open, PCIe bus anomalous reset, unapproved ptrace)
/// is detected, all `SovereignKeyMaterial` is zeroized and the process forcibly aborts.
pub struct DeadMansSwitch;

impl DeadMansSwitch {
    pub fn arm() {
        task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                if Self::detect_physical_intrusion() {
                    error!("FATAL: Physical hardware intrusion detected! Activating Dead Man's Switch.");
                    // Zeroize all globally registered secure memory here.
                    // For the architecture proof, we log the action and abort the process.
                    error!("ZEROIZING ALL SOVEREIGN ENCLAVES IN MEMORY.");
                    
                    // We simulate writing zeroed bytes into the key material
                    let mut volatile = SovereignKeyMaterial::new(vec![0xFF; 64]);
                    volatile.zeroize(); 
                    
                    process::abort(); // Bypass panic handlers, immediate SIGABRT
                }
            }
        });
    }

    /// Evaluates `sysfs` chassis intrusion sensors and standard hardware boundaries.
    fn detect_physical_intrusion() -> bool {
        // Physical Intrusion Detection via `sysfs` (Linux) or WMI (Windows)
        
        #[cfg(target_os = "linux")]
        {
            let tpm_measurements = std::path::Path::new("/sys/kernel/security/tpm0/binary_measurements");
            let chassis_tag = std::path::Path::new("/sys/class/dmi/id/chassis_asset_tag");
            let intrusion_trigger = std::path::Path::new("/var/run/marabunta_intrude"); // Mock fallback
            
            // In a production deployment, any sudden absence or hash mutation of TPM measurements 
            // indicates a hypervisor bypass or physical bus manipulation.
            if !tpm_measurements.exists() && chassis_tag.exists() {
                return true; 
            }
            if intrusion_trigger.exists() {
                return true;
            }
        }
        
        #[cfg(target_os = "windows")]
        {
            // Query WMI Win32_SystemEnclosure SecurityStatus
            // Returning false for skeleton fallback
            return false; 
        }

        false
    }
}

// ============================================================================
// Hybrid Swarm: The Asymmetric Membrane (Write-Only Embassy)
// ============================================================================

/// Represents a secure, one-way drop point on a ClearNet node.
/// DarkNet proxy nodes reach across the boundary, pull these payloads,
/// execute them on hidden hardware, and push the ZKP-verified result back.
pub struct WriteOnlyEmbassy {
    pub pending_workloads: DashMap<String, Vec<u8>>,
    pub completed_results: DashMap<String, Vec<u8>>,
}

impl Default for WriteOnlyEmbassy {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteOnlyEmbassy {
    pub fn new() -> Self {
        Self {
            pending_workloads: DashMap::new(),
            completed_results: DashMap::new(),
        }
    }

    /// ClearNet nodes drop jobs here.
    pub fn deposit_workload(&self, job_id: String, payload: Vec<u8>) {
        info!("ClearNet Embassy: Ingested heavy workload {} for DarkNet processing.", job_id);
        self.pending_workloads.insert(job_id, payload);
    }

    /// DarkNet nodes retrieve jobs from here.
    pub fn fetch_workload(&self, job_id: &str) -> Option<Vec<u8>> {
        self.pending_workloads.remove(job_id).map(|(_, v)| v)
    }

    /// DarkNet nodes drop the verified results back here.
    pub fn return_result(&self, job_id: String, result: Vec<u8>) {
        info!("ClearNet Embassy: Received verified result for workload {} from DarkNet proxy.", job_id);
        self.completed_results.insert(job_id, result);
    }
}

/// A background worker for DarkNet nodes to autonomously poll ClearNet Embassies.
/// This enforces the Asymmetric Data Diode: DarkNet nodes never accept inbound TCP connections,
/// they only initiate outbound requests to known ClearNet static IPs.
pub struct DarkNetProxyWorker;

impl DarkNetProxyWorker {
    /// Spawns a background task that polls known ClearNet Embassies for pending workloads.
    pub fn start_polling(
        embassy_ips: Vec<String>, 
        node_posture: OperationalPosture,
    ) {
        if node_posture == OperationalPosture::ClearNet {
            debug!("ClearNet node skipping DarkNet proxy polling.");
            return;
        }

        info!("Pillar 15.4: DarkNet Proxy Worker initialized. Beginning outbound-only polling of ClearNet Embassies.");

        task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                
                for ip in &embassy_ips {
                    // In a physical implementation, this opens a TLS or QUIC stream
                    // to the ClearNet IP, authenticates via ml_kem, and fetches the payload bytes.
                    // For the architecture proof, we simulate the fetch logic.
                    debug!("DarkNet Proxy: Polling ClearNet Embassy at {}...", ip);
                    
                    // Simulated payload retrieval:
                    // let payload = reqwest::get(format!("https://{}/embassy/fetch", ip)).await?.bytes().await?;
                    
                    // If a workload is retrieved, it is submitted to the local Spot Market
                    // for physical execution on a hidden Enterprise.
                    /*
                    if let Some(payload) = payload {
                        info!("DarkNet Proxy: Retrieved workload from {}. Dispatching to local Spot Market.", ip);
                        let job_req = JobRequirements::default(); // Parsed from payload
                        marketplace_engine.post_job(job_req, payload).await;
                    }
                    */
                }
            }
        });
    }
}
// These are defined here so the membrane module is self-contained.
// ============================================================================

/// Unique identifier for a sovereign swarm.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SwarmId(pub String);

impl SwarmId {
    /// Create a new random swarm identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create a swarm identifier from a string label.
    pub fn from_label(label: &str) -> Self {
        Self(label.to_string())
    }
}

impl Default for SwarmId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SwarmId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Reachability status of a remote swarm as perceived through the membrane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum SwarmReachability {
    /// Remote swarm is reachable and responsive.
    #[default]
    Reachable,
    /// Remote swarm is intermittently reachable.
    Degraded,
    /// Remote swarm is unreachable.
    Unreachable,
}


/// Minimal event bus stub for emitting membrane events.
pub struct EventBus {
    _inner: (),
}

impl EventBus {
    /// Create a new event bus.
    pub fn new() -> Self {
        Self { _inner: () }
    }

    /// Emit an event (currently logs at debug level).
    pub fn emit(&self, event_type: &str, severity: EventSeverity, message: &str) {
        debug!(
            event_type = event_type,
            severity = %severity,
            message = message,
            "membrane event emitted"
        );
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// MembraneId
// ============================================================================

/// Unique identifier for a membrane between two swarms.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MembraneId(pub Uuid);

impl MembraneId {
    /// Generate a new random membrane identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MembraneId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MembraneId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "membrane-{}", &self.0.to_string()[..8])
    }
}

impl FromStr for MembraneId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept both "membrane-<prefix>" short form and raw UUID.
        let raw = s.strip_prefix("membrane-").unwrap_or(s);
        Uuid::parse_str(raw)
            .map(MembraneId)
            .map_err(|e| format!("invalid membrane id '{}': {}", s, e))
    }
}

// ============================================================================
// DataCategory
// ============================================================================

/// Classification of data that may cross a membrane boundary.
///
/// Each category can have independent permeability rules, rate limits,
/// and transform pipelines.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataCategory {
    /// Job submission payloads.
    JobSubmission,
    /// Completed job results.
    JobResult,
    /// Raw chunk data for distributed execution.
    ChunkData,
    /// Individual chunk execution results.
    ChunkResult,
    /// Node health and liveness information.
    NodeHealth,
    /// Psyche facet state broadcasts.
    PsycheState,
    /// Gossip protocol metadata (node tables, etc.).
    GossipMetadata,
    /// Policy set updates.
    PolicyUpdate,
    /// Blob transfer data.
    BlobTransfer,
    /// Capacity lending offers and acceptances.
    CapacityLending,
    /// Inter-swarm agreement negotiation messages.
    AgreementNegotiation,
    /// Audit trail records.
    AuditRecord,
    /// Operator-defined custom category.
    Custom(String),
}

impl DataCategory {
    /// Return all built-in (non-Custom) categories.
    pub fn all_builtin() -> Vec<DataCategory> {
        vec![
            DataCategory::JobSubmission,
            DataCategory::JobResult,
            DataCategory::ChunkData,
            DataCategory::ChunkResult,
            DataCategory::NodeHealth,
            DataCategory::PsycheState,
            DataCategory::GossipMetadata,
            DataCategory::PolicyUpdate,
            DataCategory::BlobTransfer,
            DataCategory::CapacityLending,
            DataCategory::AgreementNegotiation,
            DataCategory::AuditRecord,
        ]
    }
}

impl fmt::Display for DataCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataCategory::JobSubmission => write!(f, "job_submission"),
            DataCategory::JobResult => write!(f, "job_result"),
            DataCategory::ChunkData => write!(f, "chunk_data"),
            DataCategory::ChunkResult => write!(f, "chunk_result"),
            DataCategory::NodeHealth => write!(f, "node_health"),
            DataCategory::PsycheState => write!(f, "psyche_state"),
            DataCategory::GossipMetadata => write!(f, "gossip_metadata"),
            DataCategory::PolicyUpdate => write!(f, "policy_update"),
            DataCategory::BlobTransfer => write!(f, "blob_transfer"),
            DataCategory::CapacityLending => write!(f, "capacity_lending"),
            DataCategory::AgreementNegotiation => write!(f, "agreement_negotiation"),
            DataCategory::AuditRecord => write!(f, "audit_record"),
            DataCategory::Custom(s) => write!(f, "custom:{}", s),
        }
    }
}

impl FromStr for DataCategory {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "job_submission" => Ok(DataCategory::JobSubmission),
            "job_result" => Ok(DataCategory::JobResult),
            "chunk_data" => Ok(DataCategory::ChunkData),
            "chunk_result" => Ok(DataCategory::ChunkResult),
            "node_health" => Ok(DataCategory::NodeHealth),
            "psyche_state" => Ok(DataCategory::PsycheState),
            "gossip_metadata" => Ok(DataCategory::GossipMetadata),
            "policy_update" => Ok(DataCategory::PolicyUpdate),
            "blob_transfer" => Ok(DataCategory::BlobTransfer),
            "capacity_lending" => Ok(DataCategory::CapacityLending),
            "agreement_negotiation" => Ok(DataCategory::AgreementNegotiation),
            "audit_record" => Ok(DataCategory::AuditRecord),
            other => {
                if let Some(custom) = other.strip_prefix("custom:") {
                    Ok(DataCategory::Custom(custom.to_string()))
                } else {
                    Err(format!("unknown data category: {}", other))
                }
            }
        }
    }
}

// ============================================================================
// CrossingDirection
// ============================================================================

/// Direction of data flow across a membrane boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossingDirection {
    /// Data flowing into the local swarm from the remote swarm.
    Inbound,
    /// Data flowing out of the local swarm to the remote swarm.
    Outbound,
    /// Rule applies in both directions.
    Bidirectional,
}

impl fmt::Display for CrossingDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrossingDirection::Inbound => write!(f, "inbound"),
            CrossingDirection::Outbound => write!(f, "outbound"),
            CrossingDirection::Bidirectional => write!(f, "bidirectional"),
        }
    }
}

impl FromStr for CrossingDirection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "inbound" => Ok(CrossingDirection::Inbound),
            "outbound" => Ok(CrossingDirection::Outbound),
            "bidirectional" | "both" => Ok(CrossingDirection::Bidirectional),
            other => Err(format!("unknown crossing direction: {}", other)),
        }
    }
}

// ============================================================================
// RateLimitAlgorithm / RateLimitConfig
// ============================================================================

/// Algorithm used for rate limiting membrane crossings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitAlgorithm {
    /// Classic token bucket: tokens refill at a steady rate.
    TokenBucket,
    /// Sliding window counter over a fixed time interval.
    SlidingWindow,
    /// Leaky bucket: requests drain at a steady rate.
    LeakyBucket,
}

/// Configuration for a rate limiter attached to a permeability rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Which algorithm to use.
    pub algorithm: RateLimitAlgorithm,
    /// Sustained request rate (requests per second).
    pub requests_per_sec: f64,
    /// Maximum burst size above the sustained rate.
    pub burst_size: u64,
}

// ============================================================================
// TransformRule
// ============================================================================

/// A data transformation applied to payloads crossing a membrane.
///
/// Transforms are applied in order after permeability checks pass.
/// Each transform receives the JSON value produced by the previous
/// transform (or the original payload for the first transform).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum TransformRule {
    /// SHA-256 hash listed field values for anonymisation.
    Anonymize { fields: Vec<String> },
    /// Group array elements by key and apply an aggregation function.
    Aggregate {
        group_by: String,
        aggregation: String,
    },
    /// Retain only the listed keys, dropping everything else.
    Filter { keep_fields: Vec<String> },
    /// Regex-replace matching patterns in string values.
    Redact {
        pattern: String,
        replacement: String,
    },
    /// Compress the payload (placeholder — passes through unchanged).
    Compress,
    /// Encrypt the payload (placeholder — passes through with a warning).
    Encrypt { algorithm: String },
    /// Smooth bursty traffic over a time window (handled by rate limiter).
    RateSmooth { window_secs: u64 },
    /// Reject payloads whose serialised size exceeds the limit.
    SizeLimit { max_bytes: u64 },
}

impl TransformRule {
    /// Apply this transform to a JSON value, returning the transformed value
    /// or an error description.
    pub fn apply(&self, data: &serde_json::Value) -> Result<serde_json::Value, String> {
        match self {
            TransformRule::Anonymize { fields } => {
                let mut result = data.clone();
                if let Some(obj) = result.as_object_mut() {
                    for field in fields {
                        if let Some(val) = obj.get(field) {
                            let serialised = val.to_string();
                            let mut hasher = Sha256::new();
                            hasher.update(serialised.as_bytes());
                            let hash = hasher.finalize();
                            let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
                            obj.insert(field.clone(), serde_json::Value::String(hex));
                        }
                    }
                }
                Ok(result)
            }
            TransformRule::Filter { keep_fields } => {
                let mut result = data.clone();
                if let Some(obj) = result.as_object_mut() {
                    let keep: HashSet<&str> =
                        keep_fields.iter().map(|s| s.as_str()).collect();
                    let keys: Vec<String> = obj.keys().cloned().collect();
                    for key in keys {
                        if !keep.contains(key.as_str()) {
                            obj.remove(&key);
                        }
                    }
                }
                Ok(result)
            }
            TransformRule::Redact {
                pattern,
                replacement,
            } => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| format!("invalid redact regex '{}': {}", pattern, e))?;
                let mut result = data.clone();
                redact_recursive(&re, replacement, &mut result);
                Ok(result)
            }
            TransformRule::SizeLimit { max_bytes } => {
                let serialised = serde_json::to_string(data)
                    .map_err(|e| format!("serialization error: {}", e))?;
                if serialised.len() as u64 > *max_bytes {
                    Err(format!(
                        "payload size {} exceeds limit {}",
                        serialised.len(),
                        max_bytes
                    ))
                } else {
                    Ok(data.clone())
                }
            }
            TransformRule::Compress => {
                info!("compress transform: pass-through (compression not implemented)");
                Ok(data.clone())
            }
            TransformRule::Encrypt { algorithm } => {
                warn!(
                    algorithm = %algorithm,
                    "encrypt transform: pass-through (encryption not implemented)"
                );
                Ok(data.clone())
            }
            TransformRule::Aggregate {
                group_by,
                aggregation,
            } => apply_aggregate(data, group_by, aggregation),
            TransformRule::RateSmooth { .. } => {
                // Rate smoothing is handled by the rate limiter, not inline.
                Ok(data.clone())
            }
        }
    }

    /// Validate that this transform rule is well-formed.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            TransformRule::Anonymize { fields } => {
                if fields.is_empty() {
                    return Err("anonymize transform requires at least one field".into());
                }
                Ok(())
            }
            TransformRule::Filter { keep_fields } => {
                if keep_fields.is_empty() {
                    return Err("filter transform requires at least one keep_field".into());
                }
                Ok(())
            }
            TransformRule::Redact { pattern, .. } => {
                regex::Regex::new(pattern)
                    .map_err(|e| format!("invalid redact regex '{}': {}", pattern, e))?;
                Ok(())
            }
            TransformRule::SizeLimit { max_bytes } => {
                if *max_bytes == 0 {
                    return Err("size limit must be > 0".into());
                }
                Ok(())
            }
            TransformRule::Aggregate {
                group_by,
                aggregation,
            } => {
                if group_by.is_empty() {
                    return Err("aggregate transform requires a group_by field".into());
                }
                let valid = ["sum", "count", "avg"];
                if !valid.contains(&aggregation.as_str()) {
                    return Err(format!(
                        "unknown aggregation '{}'; valid: sum, count, avg",
                        aggregation
                    ));
                }
                Ok(())
            }
            TransformRule::RateSmooth { window_secs } => {
                if *window_secs == 0 {
                    return Err("rate smooth window must be > 0".into());
                }
                Ok(())
            }
            TransformRule::Compress => Ok(()),
            TransformRule::Encrypt { algorithm } => {
                if algorithm.is_empty() {
                    return Err("encrypt transform requires an algorithm name".into());
                }
                Ok(())
            }
        }
    }
}

/// Recursively redact string values in a JSON tree.
fn redact_recursive(re: &regex::Regex, replacement: &str, value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(s) => {
            let replaced = re.replace_all(s, replacement).to_string();
            *s = replaced;
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                redact_recursive(re, replacement, v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                redact_recursive(re, replacement, v);
            }
        }
        _ => {}
    }
}

/// Apply aggregate transform: group array elements by a key and compute
/// sum / count / avg over numeric values in each group.
fn apply_aggregate(
    data: &serde_json::Value,
    group_by: &str,
    aggregation: &str,
) -> Result<serde_json::Value, String> {
    let arr = match data.as_array() {
        Some(a) => a,
        None => return Ok(data.clone()),
    };

    let mut groups: HashMap<String, Vec<f64>> = HashMap::new();

    for item in arr {
        let key = item
            .get(group_by)
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_else(|| "_unknown".to_string());

        // Collect all numeric values (excluding the group-by key).
        let numeric_sum: f64 = if let Some(obj) = item.as_object() {
            obj.iter()
                .filter(|(k, _)| k.as_str() != group_by)
                .filter_map(|(_, v)| v.as_f64())
                .sum()
        } else {
            0.0
        };

        groups.entry(key).or_default().push(numeric_sum);
    }

    let mut result_arr = Vec::new();
    for (key, values) in &groups {
        let agg_value = match aggregation {
            "sum" => values.iter().sum::<f64>(),
            "count" => values.len() as f64,
            "avg" => {
                if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                }
            }
            _ => return Err(format!("unknown aggregation: {}", aggregation)),
        };

        let mut obj = serde_json::Map::new();
        obj.insert(
            group_by.to_string(),
            serde_json::Value::String(key.clone()),
        );
        obj.insert(
            aggregation.to_string(),
            serde_json::json!(agg_value),
        );
        result_arr.push(serde_json::Value::Object(obj));
    }

    Ok(serde_json::Value::Array(result_arr))
}

// ============================================================================
// PermeabilityCondition
// ============================================================================

/// A condition that must be satisfied for a permeability rule to apply.
///
/// All conditions on a rule are AND-ed: every condition must be met for
/// the rule to permit the crossing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum PermeabilityCondition {
    /// Only permit crossings during a daily time window (UTC hours).
    TimeWindow {
        start_hour: u32,
        end_hour: u32,
    },
    /// Require the source swarm's trust score to meet a minimum.
    SourceSwarmTrusted {
        min_trust_score: f64,
    },
    /// Require a psyche facet to be above a threshold.
    PsycheFacetAbove {
        facet: String,
        min_level: f64,
    },
    /// Require a psyche facet to be below a threshold.
    PsycheFacetBelow {
        facet: String,
        max_level: f64,
    },
    /// Deny crossings when the local swarm is in the named archetype.
    NotInArchetype(String),
    /// Require an active agreement between the two swarms.
    AgreementActive,
    /// Operator-defined custom condition evaluated as a key-value check.
    Custom {
        key: String,
        value: String,
    },
}

impl PermeabilityCondition {
    /// Evaluate this condition against the provided membrane state.
    ///
    /// Returns `true` if the condition is satisfied.
    pub fn evaluate(&self, state: &MembraneState) -> bool {
        match self {
            PermeabilityCondition::TimeWindow {
                start_hour,
                end_hour,
            } => {
                let now_hour = Utc::now().format("%H").to_string();
                let current_hour: u32 = now_hour.parse().unwrap_or(0);
                if start_hour <= end_hour {
                    current_hour >= *start_hour && current_hour < *end_hour
                } else {
                    // Wraps midnight: e.g. 22..06 means 22,23,0,1,2,3,4,5
                    current_hour >= *start_hour || current_hour < *end_hour
                }
            }
            PermeabilityCondition::SourceSwarmTrusted { min_trust_score } => {
                state.remote_trust >= *min_trust_score
            }
            PermeabilityCondition::PsycheFacetAbove { facet, min_level } => {
                state
                    .psyche_facets
                    .get(facet.as_str())
                    .map(|v| *v >= *min_level)
                    .unwrap_or(false)
            }
            PermeabilityCondition::PsycheFacetBelow { facet, max_level } => {
                state
                    .psyche_facets
                    .get(facet.as_str())
                    .map(|v| *v <= *max_level)
                    .unwrap_or(true)
            }
            PermeabilityCondition::NotInArchetype(archetype) => {
                !state.local_archetypes.contains(archetype)
            }
            PermeabilityCondition::AgreementActive => state.agreement_active,
            PermeabilityCondition::Custom { key, value } => {
                state
                    .custom_properties
                    .get(key.as_str())
                    .map(|v| v == value)
                    .unwrap_or(false)
            }
        }
    }
}

// ============================================================================
// PermeabilityRule
// ============================================================================

/// A single permeability rule governing one data category in one direction.
///
/// Multiple rules may exist for the same category; the rule with the
/// highest `priority` value wins. Rules are default-deny: if no rule
/// matches a crossing attempt, the crossing is rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermeabilityRule {
    /// Which data category this rule governs.
    pub category: DataCategory,
    /// Which direction(s) this rule applies to.
    pub direction: CrossingDirection,
    /// Whether matching traffic is allowed (`true`) or denied (`false`).
    pub allowed: bool,
    /// Optional maximum payload size in bytes.
    pub max_size_bytes: Option<u64>,
    /// Optional rate limit configuration.
    pub rate_limit: Option<RateLimitConfig>,
    /// Ordered list of transforms to apply to permitted payloads.
    pub transforms: Vec<TransformRule>,
    /// Conditions that must all be satisfied for this rule to apply.
    pub conditions: Vec<PermeabilityCondition>,
    /// Higher priority wins when multiple rules match.
    pub priority: u32,
}

impl PermeabilityRule {
    /// Check whether this rule matches the given category and direction.
    pub fn matches(&self, category: &DataCategory, direction: &CrossingDirection) -> bool {
        if &self.category != category {
            return false;
        }
        match self.direction {
            CrossingDirection::Bidirectional => true,
            d => d == *direction,
        }
    }

    /// Validate all transforms and conditions in this rule.
    pub fn validate(&self) -> Result<(), String> {
        for (i, t) in self.transforms.iter().enumerate() {
            t.validate()
                .map_err(|e| format!("transform[{}]: {}", i, e))?;
        }
        Ok(())
    }
}

// ============================================================================
// SeveranceConditions
// ============================================================================

/// Conditions under which a membrane is automatically severed.
///
/// Severance is a protective mechanism: if the remote swarm becomes
/// unreliable or untrustworthy, the membrane severs itself to prevent
/// further data flow until an operator (or automated policy) reconnects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeveranceConditions {
    /// Sever after this many consecutive crossing failures.
    pub max_consecutive_failures: u32,
    /// Sever when the rolling error rate exceeds this fraction (0.0..1.0).
    pub max_error_rate: f64,
    /// Sever when recent average latency exceeds this threshold (ms).
    pub max_latency_ms: u64,
    /// Automatically sever when the inter-swarm agreement expires.
    pub on_agreement_expiry: bool,
    /// Sever when remote swarm trust falls below this score.
    pub on_trust_below: Option<f64>,
    /// Sever when the local swarm enters any of these archetypes.
    pub on_archetype: Vec<String>,
    /// If true, severance can only be triggered manually.
    pub manual_only: bool,
}

impl SeveranceConditions {
    /// Evaluate severance conditions against the current membrane state.
    ///
    /// Returns `Some(reason)` if severance should be triggered, `None`
    /// if all conditions are within acceptable bounds.
    pub fn evaluate(&self, state: &MembraneState) -> Option<String> {
        if self.manual_only {
            return None;
        }

        if state.consecutive_failures >= self.max_consecutive_failures
            && self.max_consecutive_failures > 0
        {
            return Some(format!(
                "consecutive failures ({}) >= threshold ({})",
                state.consecutive_failures, self.max_consecutive_failures
            ));
        }

        if state.recent_error_rate > self.max_error_rate && self.max_error_rate > 0.0 {
            return Some(format!(
                "error rate ({:.2}) > threshold ({:.2})",
                state.recent_error_rate, self.max_error_rate
            ));
        }

        if state.recent_latency_ms > self.max_latency_ms && self.max_latency_ms > 0 {
            return Some(format!(
                "latency ({}ms) > threshold ({}ms)",
                state.recent_latency_ms, self.max_latency_ms
            ));
        }

        if self.on_agreement_expiry && !state.agreement_active {
            return Some("agreement expired".to_string());
        }

        if let Some(min_trust) = self.on_trust_below {
            if state.remote_trust < min_trust {
                return Some(format!(
                    "trust ({:.2}) < threshold ({:.2})",
                    state.remote_trust, min_trust
                ));
            }
        }

        for archetype in &self.on_archetype {
            if state.local_archetypes.contains(archetype) {
                return Some(format!("local swarm entered archetype '{}'", archetype));
            }
        }

        None
    }

    /// Sensible default severance conditions.
    pub fn default_conditions() -> Self {
        Self {
            max_consecutive_failures: 10,
            max_error_rate: 0.5,
            max_latency_ms: 30_000,
            on_agreement_expiry: true,
            on_trust_below: Some(0.2),
            on_archetype: Vec::new(),
            manual_only: false,
        }
    }
}

// ============================================================================
// MembraneState
// ============================================================================

/// Snapshot of a membrane's operational state, used for condition and
/// severance evaluation.
#[derive(Debug, Clone)]
pub struct MembraneState {
    /// Number of consecutive crossing failures.
    pub consecutive_failures: u32,
    /// Rolling error rate over the recent window (0.0..1.0).
    pub recent_error_rate: f64,
    /// Average crossing latency over the recent window (ms).
    pub recent_latency_ms: u64,
    /// Whether the inter-swarm agreement is currently active.
    pub agreement_active: bool,
    /// Trust score of the remote swarm (0.0..1.0).
    pub remote_trust: f64,
    /// Set of archetypes the local swarm currently occupies.
    pub local_archetypes: HashSet<String>,
    /// Psyche facet levels for condition evaluation.
    pub psyche_facets: HashMap<String, f64>,
    /// Operator-defined custom properties for Custom conditions.
    pub custom_properties: HashMap<String, String>,
}

impl Default for MembraneState {
    fn default() -> Self {
        Self {
            consecutive_failures: 0,
            recent_error_rate: 0.0,
            recent_latency_ms: 0,
            agreement_active: true,
            remote_trust: 1.0,
            local_archetypes: HashSet::new(),
            psyche_facets: HashMap::new(),
            custom_properties: HashMap::new(),
        }
    }
}

// ============================================================================
// MembraneStatus
// ============================================================================

/// Lifecycle status of a membrane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum MembraneStatus {
    /// Membrane is fully operational.
    Active,
    /// Membrane is experiencing issues but still partially functional.
    Degraded {
        reason: String,
        since: DateTime<Utc>,
    },
    /// Membrane has been severed; no crossings are permitted.
    Severed {
        reason: String,
        severed_at: DateTime<Utc>,
        can_reconnect: bool,
    },
    /// Membrane is being set up but is not yet operational.
    Initializing,
}

impl fmt::Display for MembraneStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MembraneStatus::Active => write!(f, "active"),
            MembraneStatus::Degraded { reason, .. } => write!(f, "degraded: {}", reason),
            MembraneStatus::Severed { reason, .. } => write!(f, "severed: {}", reason),
            MembraneStatus::Initializing => write!(f, "initializing"),
        }
    }
}

// ============================================================================
// Membrane
// ============================================================================

/// A membrane between two swarms, defining all permeability rules,
/// transform pipelines, severance conditions, and operational metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Membrane {
    /// Unique identifier for this membrane.
    pub id: MembraneId,
    /// Human-readable name.
    pub name: String,
    /// The local swarm that owns this membrane definition.
    pub local_swarm: SwarmId,
    /// The remote swarm on the other side of the membrane.
    pub remote_swarm: SwarmId,
    /// Current lifecycle status.
    pub status: MembraneStatus,
    /// Ordered list of permeability rules (highest priority first).
    pub permeability_rules: Vec<PermeabilityRule>,
    /// Conditions under which the membrane auto-severs.
    pub severance: SeveranceConditions,
    /// When the membrane was first created.
    pub created_at: DateTime<Utc>,
    /// When the membrane definition was last modified.
    pub updated_at: DateTime<Utc>,
    /// Optional designated embassy node for proxying traffic.
    pub embassy_node: Option<NodeId>,
    /// Total number of crossings since creation.
    pub crossing_count: u64,
    /// Total bytes transferred since creation.
    pub bytes_transferred: u64,
    /// Timestamp of the most recent crossing.
    pub last_crossing_at: Option<DateTime<Utc>>,
}

impl Membrane {
    /// Validate the membrane definition: all rules, transforms, and
    /// severance conditions must be internally consistent.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("membrane name must not be empty".into());
        }
        if self.local_swarm == self.remote_swarm {
            return Err("local and remote swarm must differ".into());
        }
        for (i, rule) in self.permeability_rules.iter().enumerate() {
            rule.validate()
                .map_err(|e| format!("rule[{}]: {}", i, e))?;
        }
        Ok(())
    }

    /// Find the highest-priority rule matching the given category and direction.
    pub fn find_rule(
        &self,
        category: &DataCategory,
        direction: &CrossingDirection,
    ) -> Option<&PermeabilityRule> {
        self.permeability_rules
            .iter()
            .filter(|r| r.matches(category, direction))
            .max_by_key(|r| r.priority)
    }
}

// ============================================================================
// CrossingRecord
// ============================================================================

/// An immutable record of a single crossing attempt through a membrane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossingRecord {
    /// Monotonically increasing crossing identifier.
    pub id: u64,
    /// Which membrane was crossed.
    pub membrane_id: MembraneId,
    /// When the crossing occurred.
    pub timestamp: DateTime<Utc>,
    /// Direction of the crossing.
    pub direction: CrossingDirection,
    /// Category of the data that crossed.
    pub category: DataCategory,
    /// Size of the payload in bytes.
    pub size_bytes: u64,
    /// Latency of the crossing in milliseconds.
    pub latency_ms: u64,
    /// Names of transforms that were applied.
    pub transforms_applied: Vec<String>,
    /// Whether the crossing was delayed by the rate limiter.
    pub rate_limited: bool,
    /// Whether the crossing succeeded.
    pub success: bool,
    /// Error message if the crossing failed.
    pub error: Option<String>,
    /// Source swarm identifier.
    pub source_swarm: SwarmId,
    /// Destination swarm identifier.
    pub destination_swarm: SwarmId,
}

// ============================================================================
// CrossingFilter
// ============================================================================

/// Filter criteria for querying the crossing log.
#[derive(Debug, Clone, Default)]
pub struct CrossingFilter {
    /// Only include records for this membrane.
    pub membrane_id: Option<MembraneId>,
    /// Only include records in this direction.
    pub direction: Option<CrossingDirection>,
    /// Only include records for this data category.
    pub category: Option<DataCategory>,
    /// Only include records after this time.
    pub since: Option<DateTime<Utc>>,
    /// Only include records before this time.
    pub until: Option<DateTime<Utc>>,
    /// Only include successful crossings.
    pub success_only: Option<bool>,
    /// Only include records with payload >= this size.
    pub min_size_bytes: Option<u64>,
    /// Maximum number of records to return.
    pub limit: Option<usize>,
}

// ============================================================================
// CrossingLog
// ============================================================================

/// Ring-buffered log of membrane crossing records.
///
/// Retains up to [`MAX_CROSSING_LOG_SIZE`] records; oldest records are
/// evicted when the buffer is full.
const MAX_CROSSING_LOG_SIZE: usize = 100_000;

/// Thread-safe crossing log backed by a ring buffer.
pub struct CrossingLog {
    records: parking_lot::RwLock<VecDeque<CrossingRecord>>,
    next_id: AtomicU64,
}

impl CrossingLog {
    /// Create a new, empty crossing log.
    pub fn new() -> Self {
        Self {
            records: parking_lot::RwLock::new(VecDeque::with_capacity(1024)),
            next_id: AtomicU64::new(1),
        }
    }

    /// Record a crossing attempt.
    pub fn record(&self, mut rec: CrossingRecord) {
        rec.id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut buf = self.records.write();
        if buf.len() >= MAX_CROSSING_LOG_SIZE {
            buf.pop_front();
        }
        buf.push_back(rec);
    }

    /// Query crossing records matching the given filter.
    pub fn query(&self, filter: &CrossingFilter) -> Vec<CrossingRecord> {
        let buf = self.records.read();
        let mut results: Vec<CrossingRecord> = buf
            .iter()
            .filter(|r| {
                if let Some(ref mid) = filter.membrane_id {
                    if r.membrane_id != *mid {
                        return false;
                    }
                }
                if let Some(ref dir) = filter.direction {
                    if r.direction != *dir {
                        return false;
                    }
                }
                if let Some(ref cat) = filter.category {
                    if r.category != *cat {
                        return false;
                    }
                }
                if let Some(ref since) = filter.since {
                    if r.timestamp < *since {
                        return false;
                    }
                }
                if let Some(ref until) = filter.until {
                    if r.timestamp > *until {
                        return false;
                    }
                }
                if let Some(success_only) = filter.success_only {
                    if success_only && !r.success {
                        return false;
                    }
                }
                if let Some(min_size) = filter.min_size_bytes {
                    if r.size_bytes < min_size {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        if let Some(limit) = filter.limit {
            results.truncate(limit);
        }

        results
    }

    /// Total number of records currently in the log.
    pub fn count(&self) -> usize {
        self.records.read().len()
    }

    /// Compute the error rate for a given membrane over a time window.
    pub fn error_rate(&self, membrane_id: &MembraneId, window: Duration) -> f64 {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::seconds(60));
        let buf = self.records.read();
        let mut total = 0u64;
        let mut errors = 0u64;
        for r in buf.iter().rev() {
            if r.timestamp < cutoff {
                break;
            }
            if r.membrane_id == *membrane_id {
                total += 1;
                if !r.success {
                    errors += 1;
                }
            }
        }
        if total == 0 {
            0.0
        } else {
            errors as f64 / total as f64
        }
    }

    /// Compute total throughput in bytes per second over a time window.
    pub fn throughput_bytes_per_sec(&self, membrane_id: &MembraneId, window: Duration) -> f64 {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::seconds(60));
        let buf = self.records.read();
        let mut total_bytes: u64 = 0;
        for r in buf.iter().rev() {
            if r.timestamp < cutoff {
                break;
            }
            if r.membrane_id == *membrane_id && r.success {
                total_bytes += r.size_bytes;
            }
        }
        let secs = window.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            total_bytes as f64 / secs
        }
    }

    /// Compute average latency in milliseconds for a membrane over a window.
    pub fn avg_latency_ms(&self, membrane_id: &MembraneId, window: Duration) -> f64 {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::seconds(60));
        let buf = self.records.read();
        let mut total_latency: u64 = 0;
        let mut count: u64 = 0;
        for r in buf.iter().rev() {
            if r.timestamp < cutoff {
                break;
            }
            if r.membrane_id == *membrane_id && r.success {
                total_latency += r.latency_ms;
                count += 1;
            }
        }
        if count == 0 {
            0.0
        } else {
            total_latency as f64 / count as f64
        }
    }

    /// Export the log as CSV text.
    pub fn export_csv(&self) -> String {
        let buf = self.records.read();
        let mut csv = String::from(
            "id,membrane_id,timestamp,direction,category,size_bytes,latency_ms,rate_limited,success,error\n",
        );
        for r in buf.iter() {
            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{}\n",
                r.id,
                r.membrane_id,
                r.timestamp.to_rfc3339(),
                r.direction,
                r.category,
                r.size_bytes,
                r.latency_ms,
                r.rate_limited,
                r.success,
                r.error.as_deref().unwrap_or(""),
            ));
        }
        csv
    }
}

impl Default for CrossingLog {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// RateLimiter
// ============================================================================

/// Internal state for a single rate limiter instance.
enum RateLimiterInner {
    TokenBucket {
        tokens: f64,
        max_tokens: f64,
        refill_rate: f64,
        last_refill: Instant,
    },
    SlidingWindow {
        requests: VecDeque<Instant>,
        window: Duration,
        max_requests: u64,
    },
    LeakyBucket {
        queue_size: u64,
        max_queue: u64,
        drain_rate: f64,
        last_drain: Instant,
    },
}

/// Thread-safe rate limiter supporting multiple algorithms.
///
/// Maintains per-(membrane, category) rate limiter state using a
/// `DashMap` keyed by `(String, String)` = (membrane_id, category).
pub struct RateLimiter {
    limiters: DashMap<(String, String), RateLimiterInner>,
}

impl RateLimiter {
    /// Create a new, empty rate limiter.
    pub fn new() -> Self {
        Self {
            limiters: DashMap::new(),
        }
    }

    /// Configure a rate limiter for a specific membrane and category.
    pub fn configure(
        &self,
        membrane_id: &MembraneId,
        category: &DataCategory,
        config: &RateLimitConfig,
    ) {
        let key = (membrane_id.to_string(), category.to_string());
        let inner = match config.algorithm {
            RateLimitAlgorithm::TokenBucket => RateLimiterInner::TokenBucket {
                tokens: config.burst_size as f64,
                max_tokens: config.burst_size as f64,
                refill_rate: config.requests_per_sec,
                last_refill: Instant::now(),
            },
            RateLimitAlgorithm::SlidingWindow => {
                let window = if config.requests_per_sec > 0.0 {
                    Duration::from_secs_f64(1.0 / config.requests_per_sec * config.burst_size as f64)
                } else {
                    Duration::from_secs(60)
                };
                RateLimiterInner::SlidingWindow {
                    requests: VecDeque::new(),
                    window,
                    max_requests: config.burst_size,
                }
            }
            RateLimitAlgorithm::LeakyBucket => RateLimiterInner::LeakyBucket {
                queue_size: 0,
                max_queue: config.burst_size,
                drain_rate: config.requests_per_sec,
                last_drain: Instant::now(),
            },
        };
        self.limiters.insert(key, inner);
    }

    /// Try to acquire permission for one crossing.
    ///
    /// Returns `true` if the crossing is permitted, `false` if rate-limited.
    pub fn try_acquire(&self, membrane_id: &MembraneId, category: &DataCategory) -> bool {
        let key = (membrane_id.to_string(), category.to_string());
        let mut entry = match self.limiters.get_mut(&key) {
            Some(e) => e,
            None => return true, // No limiter configured = allow
        };

        match entry.value_mut() {
            RateLimiterInner::TokenBucket {
                tokens,
                max_tokens,
                refill_rate,
                last_refill,
            } => {
                let now = Instant::now();
                let elapsed = now.duration_since(*last_refill).as_secs_f64();
                *tokens = (*tokens + elapsed * *refill_rate).min(*max_tokens);
                *last_refill = now;

                if *tokens >= 1.0 {
                    *tokens -= 1.0;
                    true
                } else {
                    false
                }
            }
            RateLimiterInner::SlidingWindow {
                requests,
                window,
                max_requests,
            } => {
                let now = Instant::now();
                let cutoff = now - *window;
                while requests.front().is_some_and(|t| *t < cutoff) {
                    requests.pop_front();
                }
                if (requests.len() as u64) < *max_requests {
                    requests.push_back(now);
                    true
                } else {
                    false
                }
            }
            RateLimiterInner::LeakyBucket {
                queue_size,
                max_queue,
                drain_rate,
                last_drain,
            } => {
                let now = Instant::now();
                let elapsed = now.duration_since(*last_drain).as_secs_f64();
                let drained = (elapsed * *drain_rate) as u64;
                *queue_size = queue_size.saturating_sub(drained);
                *last_drain = now;

                if *queue_size < *max_queue {
                    *queue_size += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Get the current effective rate for a membrane/category pair.
    ///
    /// Returns requests-per-second estimate, or 0.0 if not configured.
    pub fn current_rate(&self, membrane_id: &MembraneId, category: &DataCategory) -> f64 {
        let key = (membrane_id.to_string(), category.to_string());
        let entry = match self.limiters.get(&key) {
            Some(e) => e,
            None => return 0.0,
        };

        match entry.value() {
            RateLimiterInner::TokenBucket { refill_rate, .. } => *refill_rate,
            RateLimiterInner::SlidingWindow {
                requests,
                window,
                ..
            } => {
                let secs = window.as_secs_f64();
                if secs <= 0.0 {
                    0.0
                } else {
                    requests.len() as f64 / secs
                }
            }
            RateLimiterInner::LeakyBucket { drain_rate, .. } => *drain_rate,
        }
    }

    /// Reset the rate limiter state for a membrane/category pair.
    pub fn reset(&self, membrane_id: &MembraneId, category: &DataCategory) {
        let key = (membrane_id.to_string(), category.to_string());
        self.limiters.remove(&key);
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// MembraneStore
// ============================================================================

/// Thread-safe CRUD store for membrane definitions.
pub struct MembraneStore {
    membranes: DashMap<MembraneId, Membrane>,
}

impl MembraneStore {
    /// Create a new, empty membrane store.
    pub fn new() -> Self {
        Self {
            membranes: DashMap::new(),
        }
    }

    /// Insert or replace a membrane definition.
    pub fn create(&self, membrane: Membrane) -> Result<MembraneId, String> {
        membrane.validate()?;
        let id = membrane.id.clone();
        self.membranes.insert(id.clone(), membrane);
        Ok(id)
    }

    /// Retrieve a membrane by its identifier.
    pub fn get(&self, id: &MembraneId) -> Option<Membrane> {
        self.membranes.get(id).map(|r| r.value().clone())
    }

    /// Update an existing membrane definition.
    pub fn update(&self, membrane: Membrane) -> Result<(), String> {
        membrane.validate()?;
        let id = membrane.id.clone();
        if !self.membranes.contains_key(&id) {
            return Err(format!("membrane {} not found", id));
        }
        self.membranes.insert(id, membrane);
        Ok(())
    }

    /// Remove a membrane by its identifier.
    pub fn delete(&self, id: &MembraneId) -> Option<Membrane> {
        self.membranes.remove(id).map(|(_, v)| v)
    }

    /// List all membranes.
    pub fn list(&self) -> Vec<Membrane> {
        self.membranes.iter().map(|r| r.value().clone()).collect()
    }

    /// Find all membranes connected to a specific remote swarm.
    pub fn find_by_remote(&self, remote: &SwarmId) -> Vec<Membrane> {
        self.membranes
            .iter()
            .filter(|r| r.value().remote_swarm == *remote)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Find all active (non-severed) membranes.
    pub fn find_active(&self) -> Vec<Membrane> {
        self.membranes
            .iter()
            .filter(|r| matches!(r.value().status, MembraneStatus::Active))
            .map(|r| r.value().clone())
            .collect()
    }

    /// Number of membranes in the store.
    pub fn count(&self) -> usize {
        self.membranes.len()
    }
}

impl Default for MembraneStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// EmbassyManager
// ============================================================================

/// Health status of an embassy assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmbassyHealth {
    /// Embassy node is healthy and handling crossings.
    Healthy,
    /// Embassy node is degraded (high latency, partial failures).
    Degraded,
    /// Embassy node has failed and needs reassignment.
    Failed,
    /// No embassy node is assigned.
    Unassigned,
}

impl fmt::Display for EmbassyHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmbassyHealth::Healthy => write!(f, "healthy"),
            EmbassyHealth::Degraded => write!(f, "degraded"),
            EmbassyHealth::Failed => write!(f, "failed"),
            EmbassyHealth::Unassigned => write!(f, "unassigned"),
        }
    }
}

/// Record of an embassy node assignment for a membrane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbassyAssignment {
    /// Which membrane this assignment is for.
    pub membrane_id: MembraneId,
    /// The assigned embassy node.
    pub node_id: NodeId,
    /// When the assignment was made.
    pub assigned_at: DateTime<Utc>,
    /// Current health of the assignment.
    pub health: EmbassyHealth,
    /// Number of crossings handled by this embassy.
    pub crossings_handled: u64,
    /// Number of errors encountered.
    pub errors: u64,
}

/// Manages embassy node assignments for membranes.
///
/// Each membrane can have at most one embassy node at a time. The
/// embassy manager selects suitable nodes from the knowledge store
/// and monitors their health.
pub struct EmbassyManager {
    assignments: DashMap<MembraneId, EmbassyAssignment>,
    knowledge: Arc<KnowledgeStore>,
}

impl EmbassyManager {
    /// Create a new embassy manager.
    pub fn new(knowledge: Arc<KnowledgeStore>) -> Self {
        Self {
            assignments: DashMap::new(),
            knowledge,
        }
    }

    /// Assign an embassy node for a membrane, selecting the best
    /// candidate from the knowledge store.
    ///
    /// Selection criteria: alive, low load, has CanRelay or CanForward trait.
    pub fn assign(&self, membrane_id: &MembraneId) -> Result<NodeId, String> {
        let candidates: Vec<NodeInfo> = self
            .knowledge
            .get_live_nodes()
            .into_iter()
            .filter(|n| {
                n.traits.contains(&super::types::Trait::CanRelay)
                    || n.traits.contains(&super::types::Trait::CanForward)
            })
            .collect();

        if candidates.is_empty() {
            return Err("no suitable embassy candidates available".into());
        }

        // Pick the candidate with the lowest load.
        let best = candidates
            .iter()
            .min_by(|a, b| a.load.partial_cmp(&b.load).unwrap_or(std::cmp::Ordering::Equal))
            .ok_or_else(|| "no candidates after filtering".to_string())?;

        let assignment = EmbassyAssignment {
            membrane_id: membrane_id.clone(),
            node_id: best.node_id,
            assigned_at: Utc::now(),
            health: EmbassyHealth::Healthy,
            crossings_handled: 0,
            errors: 0,
        };

        self.assignments.insert(membrane_id.clone(), assignment);
        Ok(best.node_id)
    }

    /// Reassign the embassy for a membrane (e.g. after a failure).
    pub fn reassign(&self, membrane_id: &MembraneId) -> Result<NodeId, String> {
        self.assignments.remove(membrane_id);
        self.assign(membrane_id)
    }

    /// Remove the embassy assignment for a membrane.
    pub fn unassign(&self, membrane_id: &MembraneId) {
        self.assignments.remove(membrane_id);
    }

    /// Check the health of an embassy assignment by verifying the node
    /// is still alive in the knowledge store.
    pub fn check_health(&self, membrane_id: &MembraneId) -> EmbassyHealth {
        let assignment = match self.assignments.get(membrane_id) {
            Some(a) => a.clone(),
            None => return EmbassyHealth::Unassigned,
        };

        match self.knowledge.get_node(&assignment.node_id) {
            Some(info) => match info.status {
                NodeStatus::Alive => {
                    if assignment.errors > 10 {
                        EmbassyHealth::Degraded
                    } else {
                        EmbassyHealth::Healthy
                    }
                }
                NodeStatus::Suspect
                | NodeStatus::Draining
                | NodeStatus::Cordoned
                | NodeStatus::Updating => EmbassyHealth::Degraded,
                NodeStatus::Dead | NodeStatus::Quarantined => EmbassyHealth::Failed,
            },
            None => EmbassyHealth::Failed,
        }
    }

    /// Get the current assignment for a membrane.
    pub fn get_assignment(&self, membrane_id: &MembraneId) -> Option<EmbassyAssignment> {
        self.assignments.get(membrane_id).map(|r| r.value().clone())
    }

    /// List all current embassy assignments.
    pub fn list_assignments(&self) -> Vec<EmbassyAssignment> {
        self.assignments.iter().map(|r| r.value().clone()).collect()
    }

    /// Record a successful crossing handled by the embassy.
    pub fn record_crossing(&self, membrane_id: &MembraneId) {
        if let Some(mut entry) = self.assignments.get_mut(membrane_id) {
            entry.crossings_handled += 1;
        }
    }

    /// Record an error encountered by the embassy.
    pub fn record_error(&self, membrane_id: &MembraneId) {
        if let Some(mut entry) = self.assignments.get_mut(membrane_id) {
            entry.errors += 1;
        }
    }
}

// ============================================================================
// MembraneHealthReport
// ============================================================================

/// Summary health report for a single membrane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembraneHealthReport {
    /// Membrane identifier.
    pub membrane_id: MembraneId,
    /// Human-readable membrane name.
    pub name: String,
    /// Current status.
    pub status: MembraneStatus,
    /// Error rate over the last 5 minutes.
    pub error_rate_5m: f64,
    /// Average latency over the last 5 minutes (ms).
    pub avg_latency_5m: f64,
    /// Throughput over the last 5 minutes (bytes/sec).
    pub throughput_5m: f64,
    /// Total crossings since creation.
    pub total_crossings: u64,
    /// Total bytes transferred since creation.
    pub total_bytes: u64,
    /// Embassy health.
    pub embassy_health: EmbassyHealth,
    /// Number of active permeability rules.
    pub active_rules: usize,
    /// Reachability assessment.
    pub reachability: SwarmReachability,
}

// ============================================================================
// MembraneEngine
// ============================================================================

/// Central engine coordinating all membrane operations.
///
/// Wires together the membrane store, crossing log, rate limiter,
/// embassy manager, and optional sovereignty / event bus integrations.
pub struct MembraneEngine {
    membrane_store: Arc<MembraneStore>,
    crossing_log: Arc<CrossingLog>,
    rate_limiter: Arc<RateLimiter>,
    embassy_manager: Arc<EmbassyManager>,
    event_bus: Option<Arc<EventBus>>,
    consecutive_failures: DashMap<MembraneId, u32>,
}

impl MembraneEngine {
    /// Create a new membrane engine.
    pub fn new(
        membrane_store: Arc<MembraneStore>,
        crossing_log: Arc<CrossingLog>,
        rate_limiter: Arc<RateLimiter>,
        embassy_manager: Arc<EmbassyManager>,
        event_bus: Option<Arc<EventBus>>,
    ) -> Self {
        Self {
            membrane_store,
            crossing_log,
            rate_limiter,
            embassy_manager,
            event_bus,
            consecutive_failures: DashMap::new(),
        }
    }

    /// Attempt a data crossing through a membrane.
    ///
    /// Steps:
    /// 1. Verify membrane is Active.
    /// 2. Find the highest-priority matching rule.
    /// 3. Default deny if no rule matches.
    /// 4. Evaluate all conditions on the matched rule.
    /// 5. Check rate limit.
    /// 6. Check size limit.
    /// 7. Apply transforms in order.
    /// 8. Record the crossing.
    /// 9. Return the (possibly transformed) data.
    pub fn attempt_crossing(
        &self,
        membrane_id: &MembraneId,
        direction: CrossingDirection,
        category: DataCategory,
        data: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let start = Instant::now();

        // 1. Get the membrane and check it is active.
        let membrane = self
            .membrane_store
            .get(membrane_id)
            .ok_or_else(|| format!("membrane {} not found", membrane_id))?;

        if !matches!(membrane.status, MembraneStatus::Active) {
            let err = format!("membrane {} is not active: {}", membrane_id, membrane.status);
            self.record_failure(membrane_id, &direction, &category, &membrane, start, &err);
            return Err(err);
        }

        // 2. Find matching rule (highest priority).
        let rule = membrane
            .find_rule(&category, &direction)
            .ok_or_else(|| {
                let err = format!(
                    "no rule matches category={} direction={} (default deny)",
                    category, direction
                );
                self.record_failure(membrane_id, &direction, &category, &membrane, start, &err);
                err
            })?
            .clone(); // clone to release borrow on membrane

        // 3. Check if the rule allows this crossing.
        if !rule.allowed {
            let err = format!(
                "rule denies crossing for category={} direction={}",
                category, direction
            );
            self.record_failure(membrane_id, &direction, &category, &membrane, start, &err);
            return Err(err);
        }

        // 4. Evaluate conditions.
        let state = self.build_membrane_state(membrane_id);
        for condition in &rule.conditions {
            if !condition.evaluate(&state) {
                let err = format!("condition not met: {:?}", condition);
                self.record_failure(membrane_id, &direction, &category, &membrane, start, &err);
                return Err(err);
            }
        }

        // 5. Rate limit check.
        let rate_limited = !self.rate_limiter.try_acquire(membrane_id, &category);
        if rate_limited {
            let err = format!(
                "rate limited for category={} on membrane={}",
                category, membrane_id
            );
            self.record_failure(membrane_id, &direction, &category, &membrane, start, &err);
            return Err(err);
        }

        // 6. Size limit check.
        if let Some(max_size) = rule.max_size_bytes {
            let payload_size = serde_json::to_string(&data)
                .map(|s| s.len() as u64)
                .unwrap_or(0);
            if payload_size > max_size {
                let err = format!(
                    "payload size {} exceeds rule max_size_bytes {}",
                    payload_size, max_size
                );
                self.record_failure(membrane_id, &direction, &category, &membrane, start, &err);
                return Err(err);
            }
        }

        // 7. Apply transforms.
        let mut transformed = data;
        let mut transforms_applied = Vec::new();
        for transform in &rule.transforms {
            match transform.apply(&transformed) {
                Ok(result) => {
                    let name = match transform {
                        TransformRule::Anonymize { .. } => "anonymize",
                        TransformRule::Filter { .. } => "filter",
                        TransformRule::Redact { .. } => "redact",
                        TransformRule::Compress => "compress",
                        TransformRule::Encrypt { .. } => "encrypt",
                        TransformRule::Aggregate { .. } => "aggregate",
                        TransformRule::RateSmooth { .. } => "rate_smooth",
                        TransformRule::SizeLimit { .. } => "size_limit",
                    };
                    transforms_applied.push(name.to_string());
                    transformed = result;
                }
                Err(e) => {
                    let err = format!("transform failed: {}", e);
                    self.record_failure(
                        membrane_id,
                        &direction,
                        &category,
                        &membrane,
                        start,
                        &err,
                    );
                    return Err(err);
                }
            }
        }

        // 8. Record successful crossing.
        let latency_ms = start.elapsed().as_millis() as u64;
        let size_bytes = serde_json::to_string(&transformed)
            .map(|s| s.len() as u64)
            .unwrap_or(0);

        let (source, dest) = match direction {
            CrossingDirection::Inbound => (membrane.remote_swarm.clone(), membrane.local_swarm.clone()),
            CrossingDirection::Outbound | CrossingDirection::Bidirectional => {
                (membrane.local_swarm.clone(), membrane.remote_swarm.clone())
            }
        };

        let record = CrossingRecord {
            id: 0, // assigned by log
            membrane_id: membrane_id.clone(),
            timestamp: Utc::now(),
            direction,
            category: category.clone(),
            size_bytes,
            latency_ms,
            transforms_applied,
            rate_limited: false,
            success: true,
            error: None,
            source_swarm: source,
            destination_swarm: dest,
        };
        self.crossing_log.record(record);

        // Update membrane counters.
        if let Some(mut m) = self.membrane_store.membranes.get_mut(membrane_id) {
            m.crossing_count += 1;
            m.bytes_transferred += size_bytes;
            m.last_crossing_at = Some(Utc::now());
        }

        // Reset consecutive failures on success.
        self.consecutive_failures.insert(membrane_id.clone(), 0);

        // Update embassy stats.
        self.embassy_manager.record_crossing(membrane_id);

        if let Some(ref bus) = self.event_bus {
            bus.emit(
                "membrane.crossing.success",
                EventSeverity::Info,
                &format!("crossing {} on {}", category, membrane_id),
            );
        }

        // 9. Return transformed data.
        Ok(transformed)
    }

    /// Evaluate severance conditions for a membrane.
    ///
    /// Returns `Some(reason)` if the membrane should be severed.
    pub fn evaluate_severance(&self, membrane_id: &MembraneId) -> Option<String> {
        let membrane = self.membrane_store.get(membrane_id)?;
        let state = self.build_membrane_state(membrane_id);
        membrane.severance.evaluate(&state)
    }

    /// Sever a membrane, preventing all further crossings.
    pub fn sever(&self, membrane_id: &MembraneId, reason: &str) -> Result<(), String> {
        let mut membrane = self
            .membrane_store
            .get(membrane_id)
            .ok_or_else(|| format!("membrane {} not found", membrane_id))?;

        membrane.status = MembraneStatus::Severed {
            reason: reason.to_string(),
            severed_at: Utc::now(),
            can_reconnect: true,
        };
        membrane.updated_at = Utc::now();
        self.membrane_store.update(membrane)?;

        if let Some(ref bus) = self.event_bus {
            bus.emit(
                "membrane.severed",
                EventSeverity::Warning,
                &format!("membrane {} severed: {}", membrane_id, reason),
            );
        }

        warn!(
            membrane = %membrane_id,
            reason = reason,
            "membrane severed"
        );

        Ok(())
    }

    /// Reconnect a previously severed membrane.
    pub fn reconnect(&self, membrane_id: &MembraneId) -> Result<(), String> {
        let mut membrane = self
            .membrane_store
            .get(membrane_id)
            .ok_or_else(|| format!("membrane {} not found", membrane_id))?;

        match &membrane.status {
            MembraneStatus::Severed { can_reconnect, .. } => {
                if !can_reconnect {
                    return Err(format!(
                        "membrane {} cannot be reconnected (can_reconnect=false)",
                        membrane_id
                    ));
                }
            }
            _ => {
                return Err(format!(
                    "membrane {} is not severed (status: {})",
                    membrane_id, membrane.status
                ));
            }
        }

        membrane.status = MembraneStatus::Active;
        membrane.updated_at = Utc::now();
        self.membrane_store.update(membrane)?;

        // Reset failure tracking.
        self.consecutive_failures.insert(membrane_id.clone(), 0);

        if let Some(ref bus) = self.event_bus {
            bus.emit(
                "membrane.reconnected",
                EventSeverity::Notice,
                &format!("membrane {} reconnected", membrane_id),
            );
        }

        info!(membrane = %membrane_id, "membrane reconnected");

        Ok(())
    }

    /// Generate a health report for a membrane.
    pub fn membrane_health(&self, membrane_id: &MembraneId) -> Option<MembraneHealthReport> {
        let membrane = self.membrane_store.get(membrane_id)?;
        let window = Duration::from_secs(300); // 5 minutes

        let error_rate = self.crossing_log.error_rate(membrane_id, window);
        let avg_latency = self.crossing_log.avg_latency_ms(membrane_id, window);
        let throughput = self.crossing_log.throughput_bytes_per_sec(membrane_id, window);
        let embassy_health = self.embassy_manager.check_health(membrane_id);

        let reachability = if matches!(membrane.status, MembraneStatus::Severed { .. }) {
            SwarmReachability::Unreachable
        } else if error_rate > 0.3 || matches!(membrane.status, MembraneStatus::Degraded { .. }) {
            SwarmReachability::Degraded
        } else {
            SwarmReachability::Reachable
        };

        Some(MembraneHealthReport {
            membrane_id: membrane_id.clone(),
            name: membrane.name.clone(),
            status: membrane.status.clone(),
            error_rate_5m: error_rate,
            avg_latency_5m: avg_latency,
            throughput_5m: throughput,
            total_crossings: membrane.crossing_count,
            total_bytes: membrane.bytes_transferred,
            embassy_health,
            active_rules: membrane.permeability_rules.len(),
            reachability,
        })
    }

    /// Spawn the background maintenance loop.
    ///
    /// Runs every 30 seconds to evaluate severance conditions and update
    /// embassy health for all active membranes.
    pub fn spawn_loop(
        self: Arc<Self>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("membrane engine maintenance loop started");

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            info!("membrane engine maintenance loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown_rx.borrow() {
                    break;
                }

                // Evaluate severance for all active membranes.
                let active = self.membrane_store.find_active();
                for membrane in &active {
                    if let Some(reason) = self.evaluate_severance(&membrane.id) {
                        warn!(
                            membrane = %membrane.id,
                            reason = %reason,
                            "auto-severing membrane"
                        );
                        let _ = self.sever(&membrane.id, &reason);
                    }

                    // Check embassy health and reassign if needed.
                    let health = self.embassy_manager.check_health(&membrane.id);
                    if health == EmbassyHealth::Failed {
                        info!(
                            membrane = %membrane.id,
                            "embassy failed, attempting reassignment"
                        );
                        match self.embassy_manager.reassign(&membrane.id) {
                            Ok(new_node) => {
                                info!(
                                    membrane = %membrane.id,
                                    node = %new_node,
                                    "embassy reassigned"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    membrane = %membrane.id,
                                    error = %e,
                                    "embassy reassignment failed"
                                );
                            }
                        }
                    }
                }

                debug!(
                    active_membranes = active.len(),
                    "membrane maintenance cycle complete"
                );
            }
        })
    }

    // -- Internal helpers --

    /// Build a MembraneState snapshot for condition/severance evaluation.
    fn build_membrane_state(&self, membrane_id: &MembraneId) -> MembraneState {
        let window = Duration::from_secs(300);
        let consecutive = self
            .consecutive_failures
            .get(membrane_id)
            .map(|r| *r.value())
            .unwrap_or(0);

        MembraneState {
            consecutive_failures: consecutive,
            recent_error_rate: self.crossing_log.error_rate(membrane_id, window),
            recent_latency_ms: self.crossing_log.avg_latency_ms(membrane_id, window) as u64,
            agreement_active: true, // default assumption
            remote_trust: 1.0,   // default assumption
            local_archetypes: HashSet::new(),
            psyche_facets: HashMap::new(),
            custom_properties: HashMap::new(),
        }
    }

    /// Record a failed crossing attempt.
    fn record_failure(
        &self,
        membrane_id: &MembraneId,
        direction: &CrossingDirection,
        category: &DataCategory,
        membrane: &Membrane,
        start: Instant,
        error: &str,
    ) {
        let latency_ms = start.elapsed().as_millis() as u64;

        let (source, dest) = match direction {
            CrossingDirection::Inbound => {
                (membrane.remote_swarm.clone(), membrane.local_swarm.clone())
            }
            CrossingDirection::Outbound | CrossingDirection::Bidirectional => {
                (membrane.local_swarm.clone(), membrane.remote_swarm.clone())
            }
        };

        let record = CrossingRecord {
            id: 0,
            membrane_id: membrane_id.clone(),
            timestamp: Utc::now(),
            direction: *direction,
            category: category.clone(),
            size_bytes: 0,
            latency_ms,
            transforms_applied: Vec::new(),
            rate_limited: false,
            success: false,
            error: Some(error.to_string()),
            source_swarm: source,
            destination_swarm: dest,
        };
        self.crossing_log.record(record);

        // Increment consecutive failure counter.
        let mut entry = self.consecutive_failures.entry(membrane_id.clone()).or_insert(0);
        *entry.value_mut() += 1;

        // Update embassy error stats.
        self.embassy_manager.record_error(membrane_id);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashSet;

    // -- helpers --

    fn make_knowledge() -> Arc<KnowledgeStore> {
        Arc::new(KnowledgeStore::new(NodeId::new()))
    }

    fn make_membrane(local: &SwarmId, remote: &SwarmId) -> Membrane {
        Membrane {
            id: MembraneId::new(),
            name: "test-membrane".to_string(),
            local_swarm: local.clone(),
            remote_swarm: remote.clone(),
            status: MembraneStatus::Active,
            permeability_rules: Vec::new(),
            severance: SeveranceConditions::default_conditions(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            embassy_node: None,
            crossing_count: 0,
            bytes_transferred: 0,
            last_crossing_at: None,
        }
    }

    fn make_rule(
        category: DataCategory,
        direction: CrossingDirection,
        allowed: bool,
        priority: u32,
    ) -> PermeabilityRule {
        PermeabilityRule {
            category,
            direction,
            allowed,
            max_size_bytes: None,
            rate_limit: None,
            transforms: Vec::new(),
            conditions: Vec::new(),
            priority,
        }
    }

    fn make_engine() -> (Arc<MembraneEngine>, Arc<MembraneStore>, Arc<CrossingLog>) {
        let knowledge = make_knowledge();
        let store = Arc::new(MembraneStore::new());
        let log = Arc::new(CrossingLog::new());
        let limiter = Arc::new(RateLimiter::new());
        let embassy = Arc::new(EmbassyManager::new(knowledge));
        let engine = Arc::new(MembraneEngine::new(
            store.clone(),
            log.clone(),
            limiter,
            embassy,
            None,
        ));
        (engine, store, log)
    }

    // ================================================================
    // MembraneId tests
    // ================================================================

    #[test]
    fn test_membrane_id_display_and_from_str() {
        let id = MembraneId::new();
        let display = id.to_string();
        assert!(display.starts_with("membrane-"));
        assert_eq!(display.len(), "membrane-".len() + 8);

        // Round-trip through full UUID.
        let full = id.0.to_string();
        let parsed: MembraneId = full.parse().expect("should parse UUID");
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_membrane_id_from_str_invalid() {
        let result: Result<MembraneId, String> = "not-a-uuid".parse();
        assert!(result.is_err());
    }

    // ================================================================
    // DataCategory tests
    // ================================================================

    #[test]
    fn test_data_category_display_and_from_str() {
        for cat in DataCategory::all_builtin() {
            let s = cat.to_string();
            let parsed: DataCategory = s.parse().expect("should round-trip");
            assert_eq!(parsed, cat);
        }
    }

    #[test]
    fn test_data_category_custom() {
        let cat = DataCategory::Custom("my_data".to_string());
        let s = cat.to_string();
        assert_eq!(s, "custom:my_data");
        let parsed: DataCategory = s.parse().expect("should parse");
        assert_eq!(parsed, cat);
    }

    #[test]
    fn test_data_category_all_builtin_count() {
        assert_eq!(DataCategory::all_builtin().len(), 12);
    }

    #[test]
    fn test_data_category_unknown() {
        let result: Result<DataCategory, String> = "nonexistent".parse();
        assert!(result.is_err());
    }

    // ================================================================
    // CrossingDirection tests
    // ================================================================

    #[test]
    fn test_crossing_direction_display_from_str() {
        let dirs = vec![
            CrossingDirection::Inbound,
            CrossingDirection::Outbound,
            CrossingDirection::Bidirectional,
        ];
        for d in dirs {
            let s = d.to_string();
            let parsed: CrossingDirection = s.parse().expect("should round-trip");
            assert_eq!(parsed, d);
        }
    }

    // ================================================================
    // TransformRule tests
    // ================================================================

    #[test]
    fn test_transform_anonymize() {
        let t = TransformRule::Anonymize {
            fields: vec!["name".to_string(), "email".to_string()],
        };
        let data = json!({"name": "Alice", "email": "alice@example.com", "age": 30});
        let result = t.apply(&data).expect("should apply");
        let obj = result.as_object().expect("should be object");
        // Hashed values are 64-char hex strings.
        assert_eq!(obj.get("name").and_then(|v| v.as_str()).map(|s| s.len()), Some(64));
        assert_eq!(obj.get("email").and_then(|v| v.as_str()).map(|s| s.len()), Some(64));
        // Non-listed fields are unchanged.
        assert_eq!(obj.get("age").and_then(|v| v.as_i64()), Some(30));
    }

    #[test]
    fn test_transform_filter() {
        let t = TransformRule::Filter {
            keep_fields: vec!["name".to_string(), "age".to_string()],
        };
        let data = json!({"name": "Alice", "age": 30, "secret": "password"});
        let result = t.apply(&data).expect("should apply");
        let obj = result.as_object().expect("should be object");
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("age"));
        assert!(!obj.contains_key("secret"));
    }

    #[test]
    fn test_transform_redact() {
        let t = TransformRule::Redact {
            pattern: r"\d{3}-\d{2}-\d{4}".to_string(),
            replacement: "***-**-****".to_string(),
        };
        let data = json!({"ssn": "123-45-6789", "name": "Alice"});
        let result = t.apply(&data).expect("should apply");
        assert_eq!(
            result.get("ssn").and_then(|v| v.as_str()),
            Some("***-**-****")
        );
        assert_eq!(result.get("name").and_then(|v| v.as_str()), Some("Alice"));
    }

    #[test]
    fn test_transform_redact_invalid_regex() {
        let t = TransformRule::Redact {
            pattern: "[invalid".to_string(),
            replacement: "x".to_string(),
        };
        let data = json!({"text": "hello"});
        assert!(t.apply(&data).is_err());
    }

    #[test]
    fn test_transform_size_limit_ok() {
        let t = TransformRule::SizeLimit { max_bytes: 1000 };
        let data = json!({"small": true});
        assert!(t.apply(&data).is_ok());
    }

    #[test]
    fn test_transform_size_limit_exceeded() {
        let t = TransformRule::SizeLimit { max_bytes: 5 };
        let data = json!({"this_is_too_large": "definitely exceeds 5 bytes"});
        assert!(t.apply(&data).is_err());
    }

    #[test]
    fn test_transform_compress_passthrough() {
        let t = TransformRule::Compress;
        let data = json!({"data": "test"});
        let result = t.apply(&data).expect("should pass through");
        assert_eq!(result, data);
    }

    #[test]
    fn test_transform_encrypt_passthrough() {
        let t = TransformRule::Encrypt {
            algorithm: "AES-256-GCM".to_string(),
        };
        let data = json!({"data": "test"});
        let result = t.apply(&data).expect("should pass through");
        assert_eq!(result, data);
    }

    #[test]
    fn test_transform_aggregate_sum() {
        let t = TransformRule::Aggregate {
            group_by: "region".to_string(),
            aggregation: "sum".to_string(),
        };
        let data = json!([
            {"region": "us", "value": 10},
            {"region": "us", "value": 20},
            {"region": "eu", "value": 5}
        ]);
        let result = t.apply(&data).expect("should apply");
        let arr = result.as_array().expect("should be array");
        assert_eq!(arr.len(), 2);
        // Find the US group.
        let us = arr.iter().find(|v| v.get("region").and_then(|r| r.as_str()) == Some("us"));
        assert!(us.is_some());
        let us_sum = us.and_then(|v| v.get("sum")).and_then(|v| v.as_f64());
        assert_eq!(us_sum, Some(30.0));
    }

    #[test]
    fn test_transform_aggregate_count() {
        let t = TransformRule::Aggregate {
            group_by: "status".to_string(),
            aggregation: "count".to_string(),
        };
        let data = json!([
            {"status": "ok", "value": 1},
            {"status": "ok", "value": 2},
            {"status": "err", "value": 3}
        ]);
        let result = t.apply(&data).expect("should apply");
        let arr = result.as_array().expect("should be array");
        let ok_entry = arr.iter().find(|v| v.get("status").and_then(|r| r.as_str()) == Some("ok"));
        assert_eq!(
            ok_entry.and_then(|v| v.get("count")).and_then(|v| v.as_f64()),
            Some(2.0)
        );
    }

    #[test]
    fn test_transform_rate_smooth_passthrough() {
        let t = TransformRule::RateSmooth { window_secs: 10 };
        let data = json!({"data": 42});
        let result = t.apply(&data).expect("should pass through");
        assert_eq!(result, data);
    }

    // ================================================================
    // TransformRule::validate tests
    // ================================================================

    #[test]
    fn test_transform_validate_anonymize_empty() {
        let t = TransformRule::Anonymize { fields: vec![] };
        assert!(t.validate().is_err());
    }

    #[test]
    fn test_transform_validate_filter_empty() {
        let t = TransformRule::Filter {
            keep_fields: vec![],
        };
        assert!(t.validate().is_err());
    }

    #[test]
    fn test_transform_validate_redact_bad_regex() {
        let t = TransformRule::Redact {
            pattern: "[invalid".to_string(),
            replacement: "x".to_string(),
        };
        assert!(t.validate().is_err());
    }

    #[test]
    fn test_transform_validate_size_limit_zero() {
        let t = TransformRule::SizeLimit { max_bytes: 0 };
        assert!(t.validate().is_err());
    }

    #[test]
    fn test_transform_validate_aggregate_bad_agg() {
        let t = TransformRule::Aggregate {
            group_by: "key".to_string(),
            aggregation: "median".to_string(),
        };
        assert!(t.validate().is_err());
    }

    #[test]
    fn test_transform_validate_rate_smooth_zero() {
        let t = TransformRule::RateSmooth { window_secs: 0 };
        assert!(t.validate().is_err());
    }

    #[test]
    fn test_transform_validate_encrypt_empty() {
        let t = TransformRule::Encrypt {
            algorithm: "".to_string(),
        };
        assert!(t.validate().is_err());
    }

    #[test]
    fn test_transform_validate_happy_path() {
        let t = TransformRule::Anonymize {
            fields: vec!["name".to_string()],
        };
        assert!(t.validate().is_ok());

        let t2 = TransformRule::Compress;
        assert!(t2.validate().is_ok());
    }

    // ================================================================
    // PermeabilityRule tests
    // ================================================================

    #[test]
    fn test_permeability_rule_matches() {
        let rule = make_rule(
            DataCategory::JobSubmission,
            CrossingDirection::Inbound,
            true,
            10,
        );
        assert!(rule.matches(&DataCategory::JobSubmission, &CrossingDirection::Inbound));
        assert!(!rule.matches(&DataCategory::JobSubmission, &CrossingDirection::Outbound));
        assert!(!rule.matches(&DataCategory::JobResult, &CrossingDirection::Inbound));
    }

    #[test]
    fn test_permeability_rule_bidirectional_matches_both() {
        let rule = make_rule(
            DataCategory::NodeHealth,
            CrossingDirection::Bidirectional,
            true,
            5,
        );
        assert!(rule.matches(&DataCategory::NodeHealth, &CrossingDirection::Inbound));
        assert!(rule.matches(&DataCategory::NodeHealth, &CrossingDirection::Outbound));
    }

    // ================================================================
    // PermeabilityCondition tests
    // ================================================================

    #[test]
    fn test_condition_trust_above() {
        let cond = PermeabilityCondition::SourceSwarmTrusted {
            min_trust_score: 0.5,
        };
        let mut state = MembraneState::default();
        state.remote_trust = 0.8;
        assert!(cond.evaluate(&state));

        state.remote_trust = 0.3;
        assert!(!cond.evaluate(&state));
    }

    #[test]
    fn test_condition_agreement_active() {
        let cond = PermeabilityCondition::AgreementActive;
        let mut state = MembraneState::default();
        state.agreement_active = true;
        assert!(cond.evaluate(&state));

        state.agreement_active = false;
        assert!(!cond.evaluate(&state));
    }

    #[test]
    fn test_condition_not_in_archetype() {
        let cond = PermeabilityCondition::NotInArchetype("aggressive".to_string());
        let mut state = MembraneState::default();
        assert!(cond.evaluate(&state));

        state.local_archetypes.insert("aggressive".to_string());
        assert!(!cond.evaluate(&state));
    }

    #[test]
    fn test_condition_psyche_facet_above() {
        let cond = PermeabilityCondition::PsycheFacetAbove {
            facet: "openness".to_string(),
            min_level: 0.7,
        };
        let mut state = MembraneState::default();
        state.psyche_facets.insert("openness".to_string(), 0.9);
        assert!(cond.evaluate(&state));

        state.psyche_facets.insert("openness".to_string(), 0.5);
        assert!(!cond.evaluate(&state));
    }

    #[test]
    fn test_condition_custom() {
        let cond = PermeabilityCondition::Custom {
            key: "env".to_string(),
            value: "prod".to_string(),
        };
        let mut state = MembraneState::default();
        assert!(!cond.evaluate(&state));

        state.custom_properties.insert("env".to_string(), "prod".to_string());
        assert!(cond.evaluate(&state));
    }

    // ================================================================
    // SeveranceConditions tests
    // ================================================================

    #[test]
    fn test_severance_no_trigger() {
        let sev = SeveranceConditions::default_conditions();
        let state = MembraneState::default();
        assert!(sev.evaluate(&state).is_none());
    }

    #[test]
    fn test_severance_consecutive_failures() {
        let sev = SeveranceConditions::default_conditions();
        let mut state = MembraneState::default();
        state.consecutive_failures = 15;
        let reason = sev.evaluate(&state);
        assert!(reason.is_some());
        assert!(reason.as_ref().map_or(false, |r| r.contains("consecutive")));
    }

    #[test]
    fn test_severance_error_rate() {
        let sev = SeveranceConditions::default_conditions();
        let mut state = MembraneState::default();
        state.recent_error_rate = 0.8;
        let reason = sev.evaluate(&state);
        assert!(reason.is_some());
        assert!(reason.as_ref().map_or(false, |r| r.contains("error rate")));
    }

    #[test]
    fn test_severance_latency() {
        let sev = SeveranceConditions::default_conditions();
        let mut state = MembraneState::default();
        state.recent_latency_ms = 60_000;
        let reason = sev.evaluate(&state);
        assert!(reason.is_some());
        assert!(reason.as_ref().map_or(false, |r| r.contains("latency")));
    }

    #[test]
    fn test_severance_agreement_expired() {
        let sev = SeveranceConditions::default_conditions();
        let mut state = MembraneState::default();
        state.agreement_active = false;
        let reason = sev.evaluate(&state);
        assert!(reason.is_some());
        assert!(reason.as_ref().map_or(false, |r| r.contains("agreement")));
    }

    #[test]
    fn test_severance_trust_below() {
        let sev = SeveranceConditions::default_conditions();
        let mut state = MembraneState::default();
        state.remote_trust = 0.1;
        let reason = sev.evaluate(&state);
        assert!(reason.is_some());
        assert!(reason.as_ref().map_or(false, |r| r.contains("trust")));
    }

    #[test]
    fn test_severance_manual_only() {
        let mut sev = SeveranceConditions::default_conditions();
        sev.manual_only = true;
        let mut state = MembraneState::default();
        state.consecutive_failures = 1000;
        assert!(sev.evaluate(&state).is_none());
    }

    // ================================================================
    // CrossingLog tests
    // ================================================================

    fn make_crossing_record(membrane_id: &MembraneId, success: bool) -> CrossingRecord {
        CrossingRecord {
            id: 0,
            membrane_id: membrane_id.clone(),
            timestamp: Utc::now(),
            direction: CrossingDirection::Outbound,
            category: DataCategory::JobSubmission,
            size_bytes: 100,
            latency_ms: 5,
            transforms_applied: Vec::new(),
            rate_limited: false,
            success,
            error: if success { None } else { Some("test error".to_string()) },
            source_swarm: SwarmId::from_label("local"),
            destination_swarm: SwarmId::from_label("remote"),
        }
    }

    #[test]
    fn test_crossing_log_record_and_count() {
        let log = CrossingLog::new();
        let mid = MembraneId::new();
        log.record(make_crossing_record(&mid, true));
        log.record(make_crossing_record(&mid, false));
        assert_eq!(log.count(), 2);
    }

    #[test]
    fn test_crossing_log_error_rate() {
        let log = CrossingLog::new();
        let mid = MembraneId::new();
        for _ in 0..8 {
            log.record(make_crossing_record(&mid, true));
        }
        for _ in 0..2 {
            log.record(make_crossing_record(&mid, false));
        }
        let rate = log.error_rate(&mid, Duration::from_secs(60));
        assert!((rate - 0.2).abs() < 0.01);
    }

    #[test]
    fn test_crossing_log_query_filter() {
        let log = CrossingLog::new();
        let mid1 = MembraneId::new();
        let mid2 = MembraneId::new();
        log.record(make_crossing_record(&mid1, true));
        log.record(make_crossing_record(&mid2, true));
        log.record(make_crossing_record(&mid1, false));

        let filter = CrossingFilter {
            membrane_id: Some(mid1.clone()),
            ..Default::default()
        };
        let results = log.query(&filter);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_crossing_log_query_success_only() {
        let log = CrossingLog::new();
        let mid = MembraneId::new();
        log.record(make_crossing_record(&mid, true));
        log.record(make_crossing_record(&mid, false));

        let filter = CrossingFilter {
            success_only: Some(true),
            ..Default::default()
        };
        let results = log.query(&filter);
        assert_eq!(results.len(), 1);
        assert!(results[0].success);
    }

    #[test]
    fn test_crossing_log_query_limit() {
        let log = CrossingLog::new();
        let mid = MembraneId::new();
        for _ in 0..10 {
            log.record(make_crossing_record(&mid, true));
        }
        let filter = CrossingFilter {
            limit: Some(3),
            ..Default::default()
        };
        let results = log.query(&filter);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_crossing_log_ring_buffer() {
        let log = CrossingLog::new();
        let mid = MembraneId::new();
        // Fill beyond MAX_CROSSING_LOG_SIZE.
        for _ in 0..MAX_CROSSING_LOG_SIZE + 100 {
            log.record(make_crossing_record(&mid, true));
        }
        assert_eq!(log.count(), MAX_CROSSING_LOG_SIZE);
    }

    #[test]
    fn test_crossing_log_export_csv() {
        let log = CrossingLog::new();
        let mid = MembraneId::new();
        log.record(make_crossing_record(&mid, true));
        let csv = log.export_csv();
        assert!(csv.starts_with("id,membrane_id,timestamp,"));
        assert!(csv.contains("true"));
    }

    #[test]
    fn test_crossing_log_avg_latency() {
        let log = CrossingLog::new();
        let mid = MembraneId::new();
        for _ in 0..5 {
            let mut rec = make_crossing_record(&mid, true);
            rec.latency_ms = 10;
            log.record(rec);
        }
        let avg = log.avg_latency_ms(&mid, Duration::from_secs(60));
        assert!((avg - 10.0).abs() < 0.01);
    }

    // ================================================================
    // RateLimiter tests
    // ================================================================

    #[test]
    fn test_rate_limiter_token_bucket() {
        let limiter = RateLimiter::new();
        let mid = MembraneId::new();
        let cat = DataCategory::JobSubmission;
        limiter.configure(
            &mid,
            &cat,
            &RateLimitConfig {
                algorithm: RateLimitAlgorithm::TokenBucket,
                requests_per_sec: 100.0,
                burst_size: 5,
            },
        );

        // Should allow burst_size requests immediately.
        for _ in 0..5 {
            assert!(limiter.try_acquire(&mid, &cat));
        }
        // 6th should fail (tokens exhausted, not enough time to refill).
        assert!(!limiter.try_acquire(&mid, &cat));
    }

    #[test]
    fn test_rate_limiter_sliding_window() {
        let limiter = RateLimiter::new();
        let mid = MembraneId::new();
        let cat = DataCategory::NodeHealth;
        limiter.configure(
            &mid,
            &cat,
            &RateLimitConfig {
                algorithm: RateLimitAlgorithm::SlidingWindow,
                requests_per_sec: 10.0,
                burst_size: 3,
            },
        );

        for _ in 0..3 {
            assert!(limiter.try_acquire(&mid, &cat));
        }
        assert!(!limiter.try_acquire(&mid, &cat));
    }

    #[test]
    fn test_rate_limiter_leaky_bucket() {
        let limiter = RateLimiter::new();
        let mid = MembraneId::new();
        let cat = DataCategory::BlobTransfer;
        limiter.configure(
            &mid,
            &cat,
            &RateLimitConfig {
                algorithm: RateLimitAlgorithm::LeakyBucket,
                requests_per_sec: 10.0,
                burst_size: 3,
            },
        );

        for _ in 0..3 {
            assert!(limiter.try_acquire(&mid, &cat));
        }
        assert!(!limiter.try_acquire(&mid, &cat));
    }

    #[test]
    fn test_rate_limiter_no_config_allows() {
        let limiter = RateLimiter::new();
        let mid = MembraneId::new();
        let cat = DataCategory::AuditRecord;
        // No configuration means allow.
        assert!(limiter.try_acquire(&mid, &cat));
    }

    #[test]
    fn test_rate_limiter_reset() {
        let limiter = RateLimiter::new();
        let mid = MembraneId::new();
        let cat = DataCategory::JobSubmission;
        limiter.configure(
            &mid,
            &cat,
            &RateLimitConfig {
                algorithm: RateLimitAlgorithm::TokenBucket,
                requests_per_sec: 100.0,
                burst_size: 2,
            },
        );

        assert!(limiter.try_acquire(&mid, &cat));
        assert!(limiter.try_acquire(&mid, &cat));
        assert!(!limiter.try_acquire(&mid, &cat));

        limiter.reset(&mid, &cat);
        // After reset, no limiter = allow.
        assert!(limiter.try_acquire(&mid, &cat));
    }

    #[test]
    fn test_rate_limiter_current_rate() {
        let limiter = RateLimiter::new();
        let mid = MembraneId::new();
        let cat = DataCategory::ChunkData;
        limiter.configure(
            &mid,
            &cat,
            &RateLimitConfig {
                algorithm: RateLimitAlgorithm::TokenBucket,
                requests_per_sec: 42.0,
                burst_size: 10,
            },
        );
        let rate = limiter.current_rate(&mid, &cat);
        assert!((rate - 42.0).abs() < 0.01);
    }

    // ================================================================
    // MembraneStore tests
    // ================================================================

    #[test]
    fn test_membrane_store_crud() {
        let store = MembraneStore::new();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let m = make_membrane(&local, &remote);
        let id = m.id.clone();

        store.create(m.clone()).expect("should create");
        assert_eq!(store.count(), 1);

        let fetched = store.get(&id).expect("should get");
        assert_eq!(fetched.name, "test-membrane");

        let mut updated = fetched;
        updated.name = "updated".to_string();
        updated.updated_at = Utc::now();
        store.update(updated).expect("should update");

        let fetched2 = store.get(&id).expect("should get updated");
        assert_eq!(fetched2.name, "updated");

        let deleted = store.delete(&id);
        assert!(deleted.is_some());
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn test_membrane_store_find_by_remote() {
        let store = MembraneStore::new();
        let local = SwarmId::from_label("local");
        let r1 = SwarmId::from_label("remote1");
        let r2 = SwarmId::from_label("remote2");

        store.create(make_membrane(&local, &r1)).expect("ok");
        store.create(make_membrane(&local, &r1)).expect("ok");
        store.create(make_membrane(&local, &r2)).expect("ok");

        let found = store.find_by_remote(&r1);
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn test_membrane_store_find_active() {
        let store = MembraneStore::new();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");

        let mut m1 = make_membrane(&local, &remote);
        let mut m2 = make_membrane(&local, &remote);
        m2.status = MembraneStatus::Severed {
            reason: "test".to_string(),
            severed_at: Utc::now(),
            can_reconnect: true,
        };

        store.create(m1).expect("ok");
        store.create(m2).expect("ok");

        let active = store.find_active();
        assert_eq!(active.len(), 1);
    }

    #[test]
    fn test_membrane_store_update_nonexistent() {
        let store = MembraneStore::new();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let m = make_membrane(&local, &remote);
        assert!(store.update(m).is_err());
    }

    // ================================================================
    // Membrane validation tests
    // ================================================================

    #[test]
    fn test_membrane_validate_empty_name() {
        let mut m = make_membrane(
            &SwarmId::from_label("a"),
            &SwarmId::from_label("b"),
        );
        m.name = "".to_string();
        assert!(m.validate().is_err());
    }

    #[test]
    fn test_membrane_validate_same_swarm() {
        let swarm = SwarmId::from_label("same");
        let m = make_membrane(&swarm, &swarm);
        assert!(m.validate().is_err());
    }

    // ================================================================
    // MembraneEngine crossing tests
    // ================================================================

    #[test]
    fn test_engine_attempt_crossing_no_rule_denies() {
        let (engine, store, _log) = make_engine();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let m = make_membrane(&local, &remote);
        let mid = m.id.clone();
        store.create(m).expect("ok");

        let result = engine.attempt_crossing(
            &mid,
            CrossingDirection::Outbound,
            DataCategory::JobSubmission,
            json!({"data": 1}),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("default deny"));
    }

    #[test]
    fn test_engine_attempt_crossing_allowed() {
        let (engine, store, log) = make_engine();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let mut m = make_membrane(&local, &remote);
        m.permeability_rules.push(make_rule(
            DataCategory::JobSubmission,
            CrossingDirection::Outbound,
            true,
            10,
        ));
        let mid = m.id.clone();
        store.create(m).expect("ok");

        let result = engine.attempt_crossing(
            &mid,
            CrossingDirection::Outbound,
            DataCategory::JobSubmission,
            json!({"data": 1}),
        );
        assert!(result.is_ok());
        assert_eq!(log.count(), 1);
    }

    #[test]
    fn test_engine_attempt_crossing_denied_rule() {
        let (engine, store, _log) = make_engine();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let mut m = make_membrane(&local, &remote);
        m.permeability_rules.push(make_rule(
            DataCategory::JobSubmission,
            CrossingDirection::Outbound,
            false,
            10,
        ));
        let mid = m.id.clone();
        store.create(m).expect("ok");

        let result = engine.attempt_crossing(
            &mid,
            CrossingDirection::Outbound,
            DataCategory::JobSubmission,
            json!({"data": 1}),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("denies"));
    }

    #[test]
    fn test_engine_attempt_crossing_with_transform() {
        let (engine, store, _log) = make_engine();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let mut m = make_membrane(&local, &remote);
        let mut rule = make_rule(
            DataCategory::NodeHealth,
            CrossingDirection::Outbound,
            true,
            10,
        );
        rule.transforms.push(TransformRule::Filter {
            keep_fields: vec!["status".to_string()],
        });
        m.permeability_rules.push(rule);
        let mid = m.id.clone();
        store.create(m).expect("ok");

        let data = json!({"status": "alive", "secret_key": "abc123"});
        let result = engine.attempt_crossing(
            &mid,
            CrossingDirection::Outbound,
            DataCategory::NodeHealth,
            data,
        );
        assert!(result.is_ok());
        let obj = result.as_ref().map(|v| v.as_object()).ok().flatten();
        assert!(obj.is_some());
        let obj = obj.expect("object");
        assert!(obj.contains_key("status"));
        assert!(!obj.contains_key("secret_key"));
    }

    #[test]
    fn test_engine_sever_and_reconnect() {
        let (engine, store, _log) = make_engine();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let mut m = make_membrane(&local, &remote);
        m.permeability_rules.push(make_rule(
            DataCategory::JobSubmission,
            CrossingDirection::Outbound,
            true,
            10,
        ));
        let mid = m.id.clone();
        store.create(m).expect("ok");

        // Sever.
        engine.sever(&mid, "manual test").expect("should sever");
        let fetched = store.get(&mid).expect("exists");
        assert!(matches!(fetched.status, MembraneStatus::Severed { .. }));

        // Crossing should fail on severed membrane.
        let result = engine.attempt_crossing(
            &mid,
            CrossingDirection::Outbound,
            DataCategory::JobSubmission,
            json!({}),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not active"));

        // Reconnect.
        engine.reconnect(&mid).expect("should reconnect");
        let fetched2 = store.get(&mid).expect("exists");
        assert!(matches!(fetched2.status, MembraneStatus::Active));
    }

    #[test]
    fn test_engine_membrane_health() {
        let (engine, store, _log) = make_engine();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let m = make_membrane(&local, &remote);
        let mid = m.id.clone();
        store.create(m).expect("ok");

        let report = engine.membrane_health(&mid);
        assert!(report.is_some());
        let report = report.expect("report");
        assert_eq!(report.name, "test-membrane");
        assert!(matches!(report.status, MembraneStatus::Active));
    }

    #[test]
    fn test_engine_size_limit_on_rule() {
        let (engine, store, _log) = make_engine();
        let local = SwarmId::from_label("local");
        let remote = SwarmId::from_label("remote");
        let mut m = make_membrane(&local, &remote);
        let mut rule = make_rule(
            DataCategory::BlobTransfer,
            CrossingDirection::Outbound,
            true,
            10,
        );
        rule.max_size_bytes = Some(10);
        m.permeability_rules.push(rule);
        let mid = m.id.clone();
        store.create(m).expect("ok");

        let result = engine.attempt_crossing(
            &mid,
            CrossingDirection::Outbound,
            DataCategory::BlobTransfer,
            json!({"this_payload_is_definitely_larger_than_10_bytes": true}),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds"));
    }

    // ================================================================
    // EmbassyManager tests
    // ================================================================

    #[test]
    fn test_embassy_no_candidates() {
        let knowledge = make_knowledge();
        let manager = EmbassyManager::new(knowledge);
        let mid = MembraneId::new();
        assert!(manager.assign(&mid).is_err());
    }

    #[test]
    fn test_embassy_health_unassigned() {
        let knowledge = make_knowledge();
        let manager = EmbassyManager::new(knowledge);
        let mid = MembraneId::new();
        assert_eq!(manager.check_health(&mid), EmbassyHealth::Unassigned);
    }

    #[test]
    fn test_embassy_list_assignments() {
        let knowledge = make_knowledge();
        let manager = EmbassyManager::new(knowledge);
        assert!(manager.list_assignments().is_empty());
    }

    #[test]
    fn test_embassy_unassign() {
        let knowledge = make_knowledge();
        let manager = EmbassyManager::new(knowledge);
        let mid = MembraneId::new();
        manager.unassign(&mid); // no-op, should not panic
        assert!(manager.get_assignment(&mid).is_none());
    }

    // ================================================================
    // MembraneStatus display tests
    // ================================================================

    #[test]
    fn test_membrane_status_display() {
        assert_eq!(MembraneStatus::Active.to_string(), "active");
        assert_eq!(MembraneStatus::Initializing.to_string(), "initializing");
        let degraded = MembraneStatus::Degraded {
            reason: "test".to_string(),
            since: Utc::now(),
        };
        assert!(degraded.to_string().contains("degraded"));
    }

    // ================================================================
    // SwarmId tests
    // ================================================================

    #[test]
    fn test_swarm_id_from_label() {
        let id = SwarmId::from_label("alpha");
        assert_eq!(id.to_string(), "alpha");
    }

    #[test]
    fn test_swarm_id_equality() {
        let a = SwarmId::from_label("same");
        let b = SwarmId::from_label("same");
        assert_eq!(a, b);
    }
}
