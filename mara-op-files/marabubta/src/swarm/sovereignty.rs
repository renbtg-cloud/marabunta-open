// Marabunta - Licensed under the MIT License.
//! Swarm sovereignty -- cross-swarm identity, heartbeat, and federation.
//!
//! This module allows independent swarm clusters to discover each other,
//! exchange heartbeats, and maintain trust scores. Each swarm has a
//! cryptographic identity (Ed25519 keypair) and a set of declared
//! capabilities. The [`SovereigntyManager`] tracks remote swarm liveness
//! and detects staleness, conflicts, and reachability transitions.
//!
//! # Architecture
//!
//! ```text
//!   SwarmKeypair ──┐
//!                  ▼
//!   SwarmIdentity ─── SovereigntyManager ─── remote_swarms (DashMap)
//!                          │
//!                          ├── generate_heartbeat()
//!                          ├── receive_heartbeat()
//!                          ├── detect_staleness()
//!                          └── spawn_loop()
//! ```

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signer, Verifier};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::types::ResourceSnapshot;

// ============================================================================
// EventBus -- lightweight event bus for sovereignty events
// ============================================================================

/// Lightweight event bus for emitting domain events to subscribers.
///
/// The sovereignty module uses this to broadcast reachability transitions,
/// conflict detections, and heartbeat anomalies. Subscribers receive events
/// asynchronously via a broadcast channel.
#[derive(Debug, Clone)]
pub struct EventBus {
    sender: tokio::sync::broadcast::Sender<SwarmEvent>,
}

/// An event emitted by the sovereignty subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmEvent {
    /// Domain of the event (e.g. "sovereignty", "heartbeat").
    pub domain: String,
    /// Severity level: "info", "warn", "error".
    pub severity: String,
    /// Human-readable summary.
    pub summary: String,
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
}

impl EventBus {
    /// Create a new event bus with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self { sender }
    }

    /// Emit a simple event with the given domain, severity, and summary.
    pub fn emit_simple(&self, domain: &str, severity: &str, summary: &str) {
        let event = SwarmEvent {
            domain: domain.to_string(),
            severity: severity.to_string(),
            summary: summary.to_string(),
            timestamp: Utc::now(),
        };
        // Ignore send errors (no active receivers is acceptable).
        let _ = self.sender.send(event);
    }

    /// Subscribe to events. Returns a broadcast receiver.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SwarmEvent> {
        self.sender.subscribe()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(256)
    }
}

// ============================================================================
// SwarmId
// ============================================================================

/// Unique identifier for an entire swarm cluster.
///
/// A newtype around [`Uuid`] that provides compact display (first 8 hex
/// characters) and flexible parsing (full UUID or short hex prefix).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SwarmId(pub Uuid);

impl SwarmId {
    /// Create a new random swarm identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SwarmId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SwarmId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = self.0.to_string();
        // First 8 characters of the UUID string (includes hyphens removed).
        let short: String = hex.chars().filter(|c| *c != '-').take(8).collect();
        write!(f, "swarm-{}", short)
    }
}

impl FromStr for SwarmId {
    type Err = String;

    /// Parse a full UUID string or a short hex prefix (at least 4 chars).
    ///
    /// Full UUID: `"550e8400-e29b-41d4-a716-446655440000"`
    /// Short hex: `"550e8400"` (padded to a UUID with zeros)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try full UUID first.
        if let Ok(uuid) = Uuid::parse_str(s) {
            return Ok(Self(uuid));
        }

        // Try short hex prefix: strip "swarm-" if present.
        let hex_part = s.strip_prefix("swarm-").unwrap_or(s);

        // Validate hex characters.
        if hex_part.len() < 4 {
            return Err(format!(
                "SwarmId hex prefix too short (minimum 4 chars): '{}'",
                hex_part
            ));
        }

        if !hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(format!(
                "SwarmId hex prefix contains non-hex characters: '{}'",
                hex_part
            ));
        }

        // Pad to 32 hex characters (UUID without dashes).
        let padded: String = if hex_part.len() >= 32 {
            hex_part[..32].to_string()
        } else {
            format!("{:0<32}", hex_part)
        };

        // Insert dashes at the UUID positions: 8-4-4-4-12.
        let uuid_str = format!(
            "{}-{}-{}-{}-{}",
            &padded[0..8],
            &padded[8..12],
            &padded[12..16],
            &padded[16..20],
            &padded[20..32]
        );

        Uuid::parse_str(&uuid_str)
            .map(SwarmId)
            .map_err(|e| format!("failed to parse padded hex as UUID: {}", e))
    }
}

// ============================================================================
// SwarmCapability
// ============================================================================

/// A capability that a swarm cluster advertises to the federation.
///
/// Capabilities describe what kinds of workloads or services the swarm
/// can handle. They are included in the [`SwarmIdentity`] and broadcast
/// to other swarms via heartbeats.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwarmCapability {
    /// General-purpose compute workloads.
    Compute {
        /// Maximum number of concurrent jobs the swarm can handle.
        max_concurrent: u32,
        /// Hardware classes available (e.g. "x86_64", "arm64").
        hardware_classes: Vec<String>,
    },
    /// Persistent data storage.
    Storage {
        /// Total storage capacity in gigabytes.
        capacity_gb: u64,
        /// Number of replicas maintained.
        replicas: u32,
    },
    /// Network relay and NAT traversal services.
    Relay {
        /// Available bandwidth in megabits per second.
        bandwidth_mbps: u32,
    },
    /// Python script execution.
    PythonExecution {
        /// Available Python versions (e.g. ["3.10", "3.11"]).
        versions: Vec<String>,
    },
    /// Shell command execution.
    ShellExecution,
    /// GPU-accelerated compute workloads.
    GpuCompute {
        /// Number of GPUs available.
        gpu_count: u32,
        /// GPU model identifier (e.g. "NVIDIA A100").
        gpu_model: String,
    },
    /// Plugin host for external integrations.
    PluginHost {
        /// Names of loaded plugins.
        plugins: Vec<String>,
    },
    /// User-defined capability.
    Custom(String),
}

impl SwarmCapability {
    /// Return the type tag of this capability as a string.
    pub fn type_tag(&self) -> &str {
        match self {
            SwarmCapability::Compute { .. } => "compute",
            SwarmCapability::Storage { .. } => "storage",
            SwarmCapability::Relay { .. } => "relay",
            SwarmCapability::PythonExecution { .. } => "python_execution",
            SwarmCapability::ShellExecution => "shell_execution",
            SwarmCapability::GpuCompute { .. } => "gpu_compute",
            SwarmCapability::PluginHost { .. } => "plugin_host",
            SwarmCapability::Custom(name) => name.as_str(),
        }
    }
}

// ============================================================================
// SwarmIdentity
// ============================================================================

/// The public identity of a swarm cluster.
///
/// Contains the swarm's unique identifier, human-readable name, public
/// key for signature verification, declared capabilities, and metadata.
/// This struct is exchanged during swarm registration and included in
/// heartbeats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmIdentity {
    /// Unique identifier for this swarm.
    pub id: SwarmId,
    /// Human-readable name (must be non-empty, max 128 chars).
    pub name: String,
    /// Free-form description of the swarm's purpose.
    pub description: String,
    /// Ed25519 public key bytes (32 bytes) for signature verification.
    pub public_key: Vec<u8>,
    /// Optional geographic region (e.g. "us-east-1", "eu-west").
    pub region: Option<String>,
    /// Capabilities this swarm advertises.
    pub capabilities: Vec<SwarmCapability>,
    /// Software version string (e.g. "0.1.0").
    pub software_version: String,
    /// When this identity was first created.
    pub created_at: DateTime<Utc>,
    /// Optional contact information (email, URL, etc.).
    pub contact: Option<String>,
    /// Arbitrary key-value metadata tags.
    pub tags: HashMap<String, String>,
}

impl SwarmIdentity {
    /// Validate that this identity is well-formed.
    ///
    /// Returns `Ok(())` if valid, or `Err(reason)` describing the first
    /// validation failure encountered.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("swarm name must not be empty".to_string());
        }
        if self.name.len() > 128 {
            return Err(format!(
                "swarm name too long ({} chars, max 128)",
                self.name.len()
            ));
        }
        if self.public_key.len() != 32 {
            return Err(format!(
                "public key must be 32 bytes, got {}",
                self.public_key.len()
            ));
        }
        if self.software_version.is_empty() {
            return Err("software_version must not be empty".to_string());
        }
        if self.description.len() > 1024 {
            return Err(format!(
                "description too long ({} chars, max 1024)",
                self.description.len()
            ));
        }
        Ok(())
    }

    /// Check whether this swarm advertises a capability matching the
    /// given type tag (e.g. "compute", "storage", "relay").
    pub fn has_capability(&self, cap_type: &str) -> bool {
        self.capabilities.iter().any(|c| c.type_tag() == cap_type)
    }

    /// Compute a short fingerprint of the public key.
    ///
    /// Returns the first 16 hex characters of the SHA-256 hash of the
    /// public key bytes, suitable for visual identification.
    pub fn fingerprint(&self) -> String {
        let hash = Sha256::digest(&self.public_key);
        let hex = hex_encode(&hash);
        hex[..16.min(hex.len())].to_string()
    }
}

// ============================================================================
// SwarmKeypair
// ============================================================================

/// Ed25519 keypair for a swarm cluster's cryptographic identity.
///
/// Used to sign heartbeats and verify remote swarm messages. The keypair
/// can be persisted to and loaded from a JSON file with base64-encoded keys.
pub struct SwarmKeypair {
    signing_key: ed25519_dalek::SigningKey,
    verifying_key: ed25519_dalek::VerifyingKey,
}

impl SwarmKeypair {
    /// Generate a new random Ed25519 keypair.
    pub fn generate() -> Self {
        let mut rng = rand::rngs::OsRng;
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rng);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
        }
    }

    /// Construct a keypair from a 32-byte Ed25519 secret seed.
    ///
    /// Returns `Err` if the bytes do not form a valid key.
    pub fn from_bytes(secret: &[u8; 32]) -> Result<Self, String> {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(secret);
        let verifying_key = signing_key.verifying_key();
        // Validate the public key is on the curve by attempting a
        // round-trip through bytes.
        let pubkey_bytes = verifying_key.to_bytes();
        ed25519_dalek::VerifyingKey::from_bytes(&pubkey_bytes)
            .map_err(|e| format!("invalid key material: {}", e))?;
        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Save the keypair to a JSON file with base64-encoded keys.
    ///
    /// The file format is:
    /// ```json
    /// {
    ///   "secret_key": "<base64 of 32-byte seed>",
    ///   "public_key": "<base64 of 32-byte public key>"
    /// }
    /// ```
    pub fn save(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "failed to create directory {}: {}",
                    parent.display(),
                    e
                )
            })?;
        }

        let data = KeypairFile {
            secret_key: URL_SAFE_NO_PAD.encode(self.signing_key.to_bytes()),
            public_key: URL_SAFE_NO_PAD.encode(self.verifying_key.to_bytes()),
        };

        let json = serde_json::to_string_pretty(&data)
            .map_err(|e| format!("failed to serialize keypair: {}", e))?;

        std::fs::write(path, json.as_bytes())
            .map_err(|e| format!("failed to write keypair to {}: {}", path.display(), e))?;

        // Restrict permissions on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            let _ = std::fs::set_permissions(path, perms);
        }

        Ok(())
    }

    /// Load a keypair from a JSON file.
    ///
    /// Returns `Err` if the file cannot be read, parsed, or contains
    /// invalid key material.
    pub fn load(path: &Path) -> Result<Self, String> {
        let json = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read keypair from {}: {}", path.display(), e))?;

        let data: KeypairFile = serde_json::from_str(&json)
            .map_err(|e| format!("failed to parse keypair JSON: {}", e))?;

        let secret_bytes = URL_SAFE_NO_PAD
            .decode(data.secret_key.as_bytes())
            .map_err(|e| format!("invalid base64 in secret_key: {}", e))?;

        if secret_bytes.len() != 32 {
            return Err(format!(
                "secret_key must be 32 bytes, got {}",
                secret_bytes.len()
            ));
        }

        let mut seed = [0u8; 32];
        seed.copy_from_slice(&secret_bytes);

        let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
        let verifying_key = signing_key.verifying_key();

        // Validate that the stored public key matches the derived one.
        let stored_pub = URL_SAFE_NO_PAD
            .decode(data.public_key.as_bytes())
            .map_err(|e| format!("invalid base64 in public_key: {}", e))?;

        if stored_pub.as_slice() != verifying_key.to_bytes().as_slice() {
            return Err("stored public key does not match derived public key".to_string());
        }

        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Try to load a keypair from the given path; if loading fails for
    /// any reason, generate a new keypair and save it to the path.
    pub fn load_or_generate(path: &Path) -> Self {
        match Self::load(path) {
            Ok(kp) => {
                debug!(path = %path.display(), "loaded swarm keypair from file");
                kp
            }
            Err(e) => {
                debug!(
                    path = %path.display(),
                    error = %e,
                    "failed to load swarm keypair, generating new one"
                );
                let kp = Self::generate();
                if let Err(save_err) = kp.save(path) {
                    warn!(
                        path = %path.display(),
                        error = %save_err,
                        "failed to save newly generated swarm keypair"
                    );
                }
                kp
            }
        }
    }

    /// Sign a message with this keypair's private key.
    ///
    /// Returns the 64-byte Ed25519 signature as a `Vec<u8>`.
    pub fn sign(&self, message: &[u8]) -> Vec<u8> {
        let sig = self.signing_key.sign(message);
        sig.to_bytes().to_vec()
    }

    /// Verify a signature against a public key.
    ///
    /// `public_key` must be 32 bytes (Ed25519 verifying key).
    /// `signature` must be 64 bytes (Ed25519 signature).
    /// Returns `true` if the signature is valid for the given message
    /// and public key.
    pub fn verify(public_key: &[u8], message: &[u8], signature: &[u8]) -> bool {
        if public_key.len() != 32 || signature.len() != 64 {
            return false;
        }
        let mut pk_bytes = [0u8; 32];
        pk_bytes.copy_from_slice(public_key);
        let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(&pk_bytes) else {
            return false;
        };
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(signature);
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        vk.verify(message, &sig).is_ok()
    }

    /// Return the public key bytes (32 bytes).
    pub fn public_key_bytes(&self) -> Vec<u8> {
        self.verifying_key.to_bytes().to_vec()
    }
}

impl fmt::Debug for SwarmKeypair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwarmKeypair")
            .field(
                "public_key",
                &hex_encode(&self.verifying_key.to_bytes()),
            )
            .finish()
    }
}

/// Internal JSON format for persisted keypairs.
#[derive(Serialize, Deserialize)]
struct KeypairFile {
    secret_key: String,
    public_key: String,
}

// ============================================================================
// PsycheSummary
// ============================================================================

/// Summary of a swarm's internal "psyche" state.
///
/// Each dimension is scored 0-4, where 0 means low/absent and 4 means
/// high/excellent. The dominant archetype is an optional label describing
/// the swarm's behavioral profile (e.g. "worker-marabunta", "research-cluster").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PsycheSummary {
    /// How much computational effort the swarm is currently expending (0-4).
    pub exertion: u8,
    /// Overall health and resource availability (0-4).
    pub vitality: u8,
    /// Rate of work completion and throughput trend (0-4).
    pub momentum: u8,
    /// Capacity for predictive scheduling and preemptive action (0-4).
    pub foresight: u8,
    /// How well-connected and synchronized the nodes are (0-4).
    pub cohesion: u8,
    /// Ability to recover from node failures and partitions (0-4).
    pub resilience: u8,
    /// Ratio of useful work to overhead (0-4).
    pub efficiency: u8,
    /// Optional label for the swarm's behavioral archetype.
    pub dominant_archetype: Option<String>,
}

impl PsycheSummary {
    /// Validate that all dimension scores are within the 0-4 range.
    pub fn validate(&self) -> Result<(), String> {
        let fields = [
            ("exertion", self.exertion),
            ("vitality", self.vitality),
            ("momentum", self.momentum),
            ("foresight", self.foresight),
            ("cohesion", self.cohesion),
            ("resilience", self.resilience),
            ("efficiency", self.efficiency),
        ];
        for (name, value) in fields {
            if value > 4 {
                return Err(format!("{} must be 0-4, got {}", name, value));
            }
        }
        Ok(())
    }

    /// Compute a single aggregate score (average of all dimensions, 0.0-4.0).
    pub fn aggregate_score(&self) -> f64 {
        let sum = self.exertion as f64
            + self.vitality as f64
            + self.momentum as f64
            + self.foresight as f64
            + self.cohesion as f64
            + self.resilience as f64
            + self.efficiency as f64;
        sum / 7.0
    }
}

// ============================================================================
// SwarmHeartbeat
// ============================================================================

/// Periodic heartbeat message from a swarm cluster to the federation.
///
/// Contains liveness information, resource availability, and an optional
/// psyche summary. The heartbeat is cryptographically signed by the
/// sending swarm's Ed25519 key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmHeartbeat {
    /// Identity of the swarm sending this heartbeat.
    pub swarm_id: SwarmId,
    /// When this heartbeat was generated.
    pub timestamp: DateTime<Utc>,
    /// Ed25519 signature over the canonical heartbeat payload.
    pub signature: Vec<u8>,
    /// Total number of nodes in the swarm.
    pub node_count: u32,
    /// Number of nodes currently alive (responding to gossip).
    pub alive_count: u32,
    /// Optional psyche summary of the swarm's internal state.
    pub psyche_summary: Option<PsycheSummary>,
    /// Average system load (0.0 = idle, 1.0 = fully loaded).
    pub load_average: f64,
    /// Uptime of the swarm's seed/coordinator node in seconds.
    pub uptime_secs: u64,
    /// Software version string.
    pub software_version: String,
    /// Number of currently active (non-terminal) jobs.
    pub active_jobs: u32,
    /// Aggregate available resources across all nodes.
    pub available_capacity: ResourceSnapshot,
    /// SLA compliance metrics: metric_name -> compliance_percentage (0.0-1.0).
    pub sla_compliance: HashMap<String, f64>,
}

impl SwarmHeartbeat {
    /// Build the canonical byte payload for signing/verification.
    ///
    /// The payload is deterministic: `swarm_id || timestamp_millis || node_count || alive_count`.
    pub fn signing_payload(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(self.swarm_id.0.as_bytes());
        buf.extend_from_slice(&self.timestamp.timestamp_millis().to_be_bytes());
        buf.extend_from_slice(&self.node_count.to_be_bytes());
        buf.extend_from_slice(&self.alive_count.to_be_bytes());
        buf.extend_from_slice(&(self.load_average.to_bits()).to_be_bytes());
        buf.extend_from_slice(&self.uptime_secs.to_be_bytes());
        buf.extend_from_slice(&self.active_jobs.to_be_bytes());
        buf.extend_from_slice(self.software_version.as_bytes());
        buf
    }
}

// ============================================================================
// SwarmReachability
// ============================================================================

/// Reachability state of a remote swarm as perceived by the local manager.
///
/// Transitions: Unknown -> Reachable -> Stale -> Unreachable.
/// A fresh heartbeat resets the state to Reachable regardless of the
/// current state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(Default)]
pub enum SwarmReachability {
    /// Heartbeats are arriving within the expected interval.
    Reachable,
    /// No heartbeat received for longer than `stale_threshold`.
    Stale {
        /// When the stale state was first detected.
        since: DateTime<Utc>,
    },
    /// No heartbeat for longer than `unreachable_threshold`.
    Unreachable {
        /// When the unreachable state was first detected.
        since: DateTime<Utc>,
    },
    /// No heartbeat has ever been received.
    #[default]
    Unknown,
}


// ============================================================================
// SwarmSummary
// ============================================================================

/// Aggregated summary of a known remote swarm.
///
/// Maintained by the [`SovereigntyManager`] and updated on each
/// received heartbeat. Includes reachability state and trust score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmSummary {
    /// The full identity of the remote swarm.
    pub identity: SwarmIdentity,
    /// The most recently received heartbeat, if any.
    pub last_heartbeat: Option<SwarmHeartbeat>,
    /// When this swarm was first registered locally.
    pub first_seen: DateTime<Utc>,
    /// Total number of heartbeats received from this swarm.
    pub heartbeat_count: u64,
    /// Current reachability assessment.
    pub reachability: SwarmReachability,
    /// Trust score (0.0 = untrusted, 1.0 = fully trusted).
    pub trust_score: f64,
    /// Average interval between consecutive heartbeats in seconds.
    pub avg_heartbeat_interval_secs: f64,
}

// ============================================================================
// SovereigntyManager
// ============================================================================

/// Default heartbeat interval (30 seconds).
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Default threshold before a remote swarm is considered stale (2 minutes).
const DEFAULT_STALE_THRESHOLD: Duration = Duration::from_secs(120);

/// Default threshold before a remote swarm is considered unreachable (5 minutes).
const DEFAULT_UNREACHABLE_THRESHOLD: Duration = Duration::from_secs(300);

/// Maximum allowed age for an incoming heartbeat (5 minutes).
const MAX_HEARTBEAT_AGE: Duration = Duration::from_secs(300);

/// Maximum allowed future clock skew for an incoming heartbeat (30 seconds).
const MAX_HEARTBEAT_FUTURE: Duration = Duration::from_secs(30);

/// Manages cross-swarm federation: identity, heartbeats, and trust.
///
/// The `SovereigntyManager` holds the local swarm's identity and keypair,
/// tracks known remote swarms via a concurrent [`DashMap`], and provides
/// methods to generate signed heartbeats, verify incoming heartbeats,
/// detect staleness, and compute trust scores.
///
/// # Thread Safety
///
/// All public methods are safe to call from multiple threads. The local
/// identity is protected by a [`RwLock`], and the remote swarm map uses
/// [`DashMap`] for lock-free concurrent reads.
pub struct SovereigntyManager {
    /// This swarm's identity (mutable for updates to capabilities, tags, etc.).
    local_identity: Arc<RwLock<SwarmIdentity>>,
    /// Signing keypair for this swarm.
    keypair: Arc<SwarmKeypair>,
    /// Known remote swarms, keyed by their SwarmId.
    remote_swarms: DashMap<SwarmId, SwarmSummary>,
    /// Optional event bus for emitting sovereignty events.
    event_bus: Option<Arc<EventBus>>,
    /// How often to emit heartbeats.
    heartbeat_interval: Duration,
    /// How long without a heartbeat before marking a remote as stale.
    stale_threshold: Duration,
    /// How long without a heartbeat before marking a remote as unreachable.
    unreachable_threshold: Duration,
}

impl SovereigntyManager {
    /// Create a new sovereignty manager.
    ///
    /// The `identity` is the local swarm's public identity, `keypair` is
    /// used for signing heartbeats, and `event_bus` (optional) receives
    /// domain events about reachability transitions and conflicts.
    pub fn new(
        identity: SwarmIdentity,
        keypair: Arc<SwarmKeypair>,
        event_bus: Option<Arc<EventBus>>,
    ) -> Self {
        Self {
            local_identity: Arc::new(RwLock::new(identity)),
            keypair,
            remote_swarms: DashMap::new(),
            event_bus,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            stale_threshold: DEFAULT_STALE_THRESHOLD,
            unreachable_threshold: DEFAULT_UNREACHABLE_THRESHOLD,
        }
    }

    /// Return a read-locked reference to the local identity.
    pub fn local_identity(&self) -> SwarmIdentity {
        self.local_identity.read().clone()
    }

    /// Return the local swarm's ID.
    pub fn local_id(&self) -> SwarmId {
        self.local_identity.read().id
    }

    /// Return a reference to the signing keypair.
    pub fn keypair(&self) -> &Arc<SwarmKeypair> {
        &self.keypair
    }

    /// Set custom timing thresholds.
    pub fn with_thresholds(
        mut self,
        heartbeat_interval: Duration,
        stale_threshold: Duration,
        unreachable_threshold: Duration,
    ) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self.stale_threshold = stale_threshold;
        self.unreachable_threshold = unreachable_threshold;
        self
    }

    /// Generate a signed heartbeat for the local swarm.
    ///
    /// The heartbeat includes the provided runtime stats and is signed
    /// with the local keypair. The caller is responsible for broadcasting
    /// the heartbeat to known remote swarms.
    pub fn generate_heartbeat(
        &self,
        node_count: u32,
        alive_count: u32,
        load_average: f64,
        uptime_secs: u64,
    ) -> SwarmHeartbeat {
        let identity = self.local_identity.read();
        let mut heartbeat = SwarmHeartbeat {
            swarm_id: identity.id,
            timestamp: Utc::now(),
            signature: Vec::new(),
            node_count,
            alive_count,
            psyche_summary: None,
            load_average,
            uptime_secs,
            software_version: identity.software_version.clone(),
            active_jobs: 0,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };

        let payload = heartbeat.signing_payload();
        heartbeat.signature = self.keypair.sign(&payload);
        heartbeat
    }

    /// Generate a signed heartbeat with full parameters including psyche and SLA.
    pub fn generate_heartbeat_full(
        &self,
        node_count: u32,
        alive_count: u32,
        load_average: f64,
        uptime_secs: u64,
        psyche: Option<PsycheSummary>,
        active_jobs: u32,
        available_capacity: ResourceSnapshot,
        sla_compliance: HashMap<String, f64>,
    ) -> SwarmHeartbeat {
        let identity = self.local_identity.read();
        let mut heartbeat = SwarmHeartbeat {
            swarm_id: identity.id,
            timestamp: Utc::now(),
            signature: Vec::new(),
            node_count,
            alive_count,
            psyche_summary: psyche,
            load_average,
            uptime_secs,
            software_version: identity.software_version.clone(),
            active_jobs,
            available_capacity,
            sla_compliance,
        };

        let payload = heartbeat.signing_payload();
        heartbeat.signature = self.keypair.sign(&payload);
        heartbeat
    }

    /// Process an incoming heartbeat from a remote swarm.
    ///
    /// Verifies the cryptographic signature, checks timestamp freshness,
    /// updates the remote swarm's summary, detects reachability transitions,
    /// and emits events as appropriate.
    ///
    /// Returns `Err` if the heartbeat fails validation (bad signature,
    /// expired, unknown swarm, etc.).
    pub fn receive_heartbeat(&self, heartbeat: SwarmHeartbeat) -> Result<(), String> {
        let swarm_id = heartbeat.swarm_id;

        // 1. Check that we know this swarm.
        let mut entry = self.remote_swarms.get_mut(&swarm_id).ok_or_else(|| {
            format!(
                "heartbeat from unknown swarm {}; register it first",
                swarm_id
            )
        })?;

        let summary = entry.value_mut();

        // 2. Verify signature against the registered public key.
        let payload = heartbeat.signing_payload();
        if !SwarmKeypair::verify(&summary.identity.public_key, &payload, &heartbeat.signature) {
            return Err(format!(
                "invalid signature on heartbeat from swarm {}",
                swarm_id
            ));
        }

        // 3. Check timestamp freshness.
        let now = Utc::now();
        let age = now.signed_duration_since(heartbeat.timestamp);

        if age.num_milliseconds() > MAX_HEARTBEAT_AGE.as_millis() as i64 {
            return Err(format!(
                "heartbeat from swarm {} is too old ({} seconds)",
                swarm_id,
                age.num_seconds()
            ));
        }

        if age.num_milliseconds() < -(MAX_HEARTBEAT_FUTURE.as_millis() as i64) {
            return Err(format!(
                "heartbeat from swarm {} is from the future ({} seconds ahead)",
                swarm_id,
                -age.num_seconds()
            ));
        }

        // 4. Detect reachability transition.
        let old_reachability = summary.reachability.clone();
        let was_reachable = matches!(old_reachability, SwarmReachability::Reachable);

        summary.reachability = SwarmReachability::Reachable;

        // 5. Update heartbeat interval average.
        if let Some(ref last_hb) = summary.last_heartbeat {
            let interval = heartbeat
                .timestamp
                .signed_duration_since(last_hb.timestamp)
                .num_milliseconds() as f64
                / 1000.0;
            if interval > 0.0 {
                // Exponential moving average with alpha = 0.3.
                let alpha = 0.3;
                if summary.avg_heartbeat_interval_secs <= 0.0 {
                    summary.avg_heartbeat_interval_secs = interval;
                } else {
                    summary.avg_heartbeat_interval_secs = alpha * interval
                        + (1.0 - alpha) * summary.avg_heartbeat_interval_secs;
                }
            }
        }

        // 6. Update summary fields.
        summary.heartbeat_count += 1;
        summary.last_heartbeat = Some(heartbeat);

        // 7. Recompute trust score.
        summary.trust_score = Self::compute_trust(summary);

        // 8. Emit event if transitioning to Reachable.
        if !was_reachable {
            if let Some(ref bus) = self.event_bus {
                bus.emit_simple(
                    "sovereignty",
                    "info",
                    &format!("swarm {} became reachable", swarm_id),
                );
            }
        }

        Ok(())
    }

    /// Register a remote swarm's identity.
    ///
    /// Returns `Err` if the identity is invalid or if there is a name
    /// conflict with an existing registered swarm.
    pub fn register_remote(&self, identity: SwarmIdentity) -> Result<(), String> {
        identity.validate()?;

        // Check for conflicts.
        if let Some(conflict) = self.conflict_check(&identity) {
            return Err(conflict);
        }

        let swarm_id = identity.id;

        let summary = SwarmSummary {
            identity,
            last_heartbeat: None,
            first_seen: Utc::now(),
            heartbeat_count: 0,
            reachability: SwarmReachability::Unknown,
            trust_score: 0.0,
            avg_heartbeat_interval_secs: 0.0,
        };

        self.remote_swarms.insert(swarm_id, summary);

        if let Some(ref bus) = self.event_bus {
            bus.emit_simple(
                "sovereignty",
                "info",
                &format!("registered remote swarm {}", swarm_id),
            );
        }

        info!(swarm_id = %swarm_id, "registered remote swarm");
        Ok(())
    }

    /// Unregister a remote swarm by its ID.
    ///
    /// Returns `Err` if the swarm was not registered.
    pub fn unregister_remote(&self, id: &SwarmId) -> Result<(), String> {
        if self.remote_swarms.remove(id).is_some() {
            if let Some(ref bus) = self.event_bus {
                bus.emit_simple(
                    "sovereignty",
                    "info",
                    &format!("unregistered remote swarm {}", id),
                );
            }
            info!(swarm_id = %id, "unregistered remote swarm");
            Ok(())
        } else {
            Err(format!("swarm {} is not registered", id))
        }
    }

    /// Look up a remote swarm by its ID.
    pub fn get_remote(&self, id: &SwarmId) -> Option<SwarmSummary> {
        self.remote_swarms.get(id).map(|r| r.value().clone())
    }

    /// Return all known remote swarms.
    pub fn list_remotes(&self) -> Vec<SwarmSummary> {
        self.remote_swarms
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return only remote swarms whose reachability is [`SwarmReachability::Reachable`].
    pub fn reachable_remotes(&self) -> Vec<SwarmSummary> {
        self.remote_swarms
            .iter()
            .filter(|r| matches!(r.value().reachability, SwarmReachability::Reachable))
            .map(|r| r.value().clone())
            .collect()
    }

    /// Number of known remote swarms.
    pub fn remote_count(&self) -> usize {
        self.remote_swarms.len()
    }

    /// Scan all remote swarms and transition reachability states based
    /// on time elapsed since the last heartbeat.
    ///
    /// - Reachable -> Stale if last heartbeat is older than `stale_threshold`.
    /// - Stale -> Unreachable if last heartbeat is older than `unreachable_threshold`.
    /// - Unknown remains Unknown until a heartbeat arrives.
    ///
    /// Returns the number of swarms whose reachability changed.
    pub fn detect_staleness(&self) -> usize {
        let now = Utc::now();
        let stale_ms = self.stale_threshold.as_millis() as i64;
        let unreachable_ms = self.unreachable_threshold.as_millis() as i64;
        let mut transitions = 0;

        for mut entry in self.remote_swarms.iter_mut() {
            let summary = entry.value_mut();

            let last_hb_time = summary
                .last_heartbeat
                .as_ref()
                .map(|hb| hb.timestamp);

            let Some(last_time) = last_hb_time else {
                // No heartbeat ever received; leave as Unknown.
                continue;
            };

            let elapsed_ms = now
                .signed_duration_since(last_time)
                .num_milliseconds();

            if elapsed_ms > unreachable_ms {
                if !matches!(summary.reachability, SwarmReachability::Unreachable { .. }) {
                    summary.reachability = SwarmReachability::Unreachable { since: now };
                    transitions += 1;

                    if let Some(ref bus) = self.event_bus {
                        bus.emit_simple(
                            "sovereignty",
                            "warn",
                            &format!(
                                "swarm {} became unreachable (last heartbeat {} sec ago)",
                                summary.identity.id,
                                elapsed_ms / 1000
                            ),
                        );
                    }
                }
            } else if elapsed_ms > stale_ms
                && !matches!(summary.reachability, SwarmReachability::Stale { .. }) {
                    summary.reachability = SwarmReachability::Stale { since: now };
                    transitions += 1;

                    if let Some(ref bus) = self.event_bus {
                        bus.emit_simple(
                            "sovereignty",
                            "info",
                            &format!(
                                "swarm {} became stale (last heartbeat {} sec ago)",
                                summary.identity.id,
                                elapsed_ms / 1000
                            ),
                        );
                    }
                }
            // If elapsed_ms <= stale_ms, the swarm is still Reachable (or
            // remains in its current state if it hasn't received a heartbeat
            // that set it to Reachable).
        }

        transitions
    }

    /// Check for conflicts between a new identity and existing registrations.
    ///
    /// Returns `Some(reason)` if there is a name collision or key reuse,
    /// `None` if no conflicts exist.
    pub fn conflict_check(&self, identity: &SwarmIdentity) -> Option<String> {
        for entry in self.remote_swarms.iter() {
            let existing = &entry.value().identity;

            // Skip same swarm (re-registration is handled separately).
            if existing.id == identity.id {
                continue;
            }

            // Name collision.
            if existing.name == identity.name {
                return Some(format!(
                    "name conflict: swarm '{}' is already registered under ID {}",
                    identity.name, existing.id
                ));
            }

            // Public key reuse.
            if existing.public_key == identity.public_key {
                return Some(format!(
                    "public key conflict: key fingerprint {} is already used by swarm {}",
                    identity.fingerprint(),
                    existing.id
                ));
            }
        }

        // Also check against local identity.
        let local = self.local_identity.read();
        if local.name == identity.name && local.id != identity.id {
            return Some(format!(
                "name conflict with local swarm: '{}'",
                identity.name
            ));
        }
        if local.public_key == identity.public_key && local.id != identity.id {
            return Some(format!(
                "public key conflict with local swarm: fingerprint {}",
                identity.fingerprint()
            ));
        }

        None
    }

    /// Compute the trust score for a remote swarm (0.0 to 1.0).
    ///
    /// The score is based on:
    /// - Age: older registrations are more trusted (up to 0.4).
    /// - Heartbeat consistency: regular heartbeats increase trust (up to 0.4).
    /// - Heartbeat count: more heartbeats increase trust (up to 0.2).
    pub fn trust_score(&self, id: &SwarmId) -> f64 {
        match self.remote_swarms.get(id) {
            Some(entry) => Self::compute_trust(entry.value()),
            None => 0.0,
        }
    }

    /// Spawn a background loop that periodically detects staleness.
    ///
    /// The loop runs every `heartbeat_interval` and calls
    /// [`detect_staleness`](Self::detect_staleness). It exits when the
    /// shutdown signal is received.
    ///
    /// `node_count_fn` is called each iteration to get the current node
    /// count and alive count for heartbeat generation.
    pub fn spawn_loop(
        self: Arc<Self>,
        node_count_fn: Arc<dyn Fn() -> (u32, u32) + Send + Sync>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let interval = self.heartbeat_interval;

        tokio::spawn(async move {
            info!("sovereignty manager loop started");

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            info!("sovereignty manager loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown_rx.borrow() {
                    break;
                }

                // Detect staleness.
                let transitions = self.detect_staleness();
                if transitions > 0 {
                    debug!(
                        transitions = transitions,
                        "sovereignty: reachability transitions detected"
                    );
                }

                // Generate heartbeat (for external consumers to broadcast).
                let (node_count, alive_count) = node_count_fn();
                let _heartbeat = self.generate_heartbeat(
                    node_count,
                    alive_count,
                    0.0,
                    0,
                );
            }
        })
    }

    /// Compute trust score from a SwarmSummary.
    fn compute_trust(summary: &SwarmSummary) -> f64 {
        let now = Utc::now();

        // Age component (0.0 to 0.4): max trust at 24 hours.
        let age_secs = now
            .signed_duration_since(summary.first_seen)
            .num_seconds()
            .max(0) as f64;
        let max_age_secs = 86400.0; // 24 hours
        let age_score = (age_secs / max_age_secs).min(1.0) * 0.4;

        // Heartbeat consistency (0.0 to 0.4): based on how close the
        // average interval is to the expected 30s.
        let consistency_score = if summary.avg_heartbeat_interval_secs > 0.0 {
            let expected = DEFAULT_HEARTBEAT_INTERVAL.as_secs_f64();
            let ratio = (expected / summary.avg_heartbeat_interval_secs).min(1.0);
            ratio * 0.4
        } else {
            0.0
        };

        // Heartbeat count (0.0 to 0.2): max trust at 100 heartbeats.
        let count_score = (summary.heartbeat_count as f64 / 100.0).min(1.0) * 0.2;

        let total = age_score + consistency_score + count_score;
        total.min(1.0).max(0.0)
    }
}

impl fmt::Debug for SovereigntyManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SovereigntyManager")
            .field("local_id", &self.local_id())
            .field("remote_count", &self.remote_swarms.len())
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish()
    }
}

// ============================================================================
// Utility functions
// ============================================================================

/// Encode a byte slice as a lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    // ====================================================================
    // Helper functions
    // ====================================================================

    fn make_identity(name: &str, keypair: &SwarmKeypair) -> SwarmIdentity {
        SwarmIdentity {
            id: SwarmId::new(),
            name: name.to_string(),
            description: "Test swarm".to_string(),
            public_key: keypair.public_key_bytes(),
            region: Some("us-east-1".to_string()),
            capabilities: vec![SwarmCapability::Compute {
                max_concurrent: 100,
                hardware_classes: vec!["x86_64".to_string()],
            }],
            software_version: "0.1.0".to_string(),
            created_at: Utc::now(),
            contact: None,
            tags: HashMap::new(),
        }
    }

    fn make_manager(name: &str) -> (SovereigntyManager, Arc<SwarmKeypair>) {
        let keypair = Arc::new(SwarmKeypair::generate());
        let identity = make_identity(name, &keypair);
        let manager = SovereigntyManager::new(identity, keypair.clone(), None);
        (manager, keypair)
    }

    fn make_manager_with_bus(name: &str) -> (SovereigntyManager, Arc<SwarmKeypair>, Arc<EventBus>) {
        let keypair = Arc::new(SwarmKeypair::generate());
        let identity = make_identity(name, &keypair);
        let bus = Arc::new(EventBus::new(64));
        let manager = SovereigntyManager::new(identity, keypair.clone(), Some(bus.clone()));
        (manager, keypair, bus)
    }

    fn make_remote_identity(name: &str) -> (SwarmIdentity, Arc<SwarmKeypair>) {
        let keypair = Arc::new(SwarmKeypair::generate());
        let identity = make_identity(name, &keypair);
        (identity, keypair)
    }

    // ====================================================================
    // SwarmId tests
    // ====================================================================

    #[test]
    fn swarm_id_display_shows_first_8_hex() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid UUID");
        let id = SwarmId(uuid);
        let display = format!("{}", id);
        assert_eq!(display, "swarm-550e8400");
    }

    #[test]
    fn swarm_id_from_str_full_uuid() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let id: SwarmId = uuid_str.parse().expect("should parse full UUID");
        assert_eq!(id.0.to_string(), uuid_str);
    }

    #[test]
    fn swarm_id_from_str_short_hex() {
        let id: SwarmId = "abcd1234".parse().expect("should parse short hex");
        let hex: String = id.0.to_string().replace('-', "");
        assert!(hex.starts_with("abcd1234"));
    }

    #[test]
    fn swarm_id_from_str_with_prefix() {
        let id: SwarmId = "swarm-abcd1234".parse().expect("should parse with prefix");
        let hex: String = id.0.to_string().replace('-', "");
        assert!(hex.starts_with("abcd1234"));
    }

    #[test]
    fn swarm_id_from_str_too_short() {
        let result: Result<SwarmId, _> = "ab".parse();
        assert!(result.is_err());
    }

    #[test]
    fn swarm_id_from_str_invalid_hex() {
        let result: Result<SwarmId, _> = "zzzzzzzz".parse();
        assert!(result.is_err());
    }

    #[test]
    fn swarm_id_roundtrip_serde() {
        let id = SwarmId::new();
        let json = serde_json::to_string(&id).expect("serialize");
        let parsed: SwarmId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(id, parsed);
    }

    #[test]
    fn swarm_id_display_from_str_roundtrip() {
        let original = SwarmId::new();
        let full_uuid_str = original.0.to_string();
        let parsed: SwarmId = full_uuid_str.parse().expect("should parse");
        assert_eq!(original, parsed);
    }

    // ====================================================================
    // SwarmKeypair tests
    // ====================================================================

    #[test]
    fn keypair_generate_produces_valid_pair() {
        let kp = SwarmKeypair::generate();
        let pubkey = kp.public_key_bytes();
        assert_eq!(pubkey.len(), 32);
    }

    #[test]
    fn keypair_sign_verify() {
        let kp = SwarmKeypair::generate();
        let message = b"hello swarm world";
        let signature = kp.sign(message);
        assert_eq!(signature.len(), 64);
        assert!(SwarmKeypair::verify(
            &kp.public_key_bytes(),
            message,
            &signature
        ));
    }

    #[test]
    fn keypair_reject_tampered_message() {
        let kp = SwarmKeypair::generate();
        let message = b"original message";
        let signature = kp.sign(message);
        let tampered = b"tampered message";
        assert!(!SwarmKeypair::verify(
            &kp.public_key_bytes(),
            tampered,
            &signature
        ));
    }

    #[test]
    fn keypair_reject_wrong_key() {
        let kp1 = SwarmKeypair::generate();
        let kp2 = SwarmKeypair::generate();
        let message = b"test message";
        let signature = kp1.sign(message);
        assert!(!SwarmKeypair::verify(
            &kp2.public_key_bytes(),
            message,
            &signature
        ));
    }

    #[test]
    fn keypair_from_bytes() {
        let kp = SwarmKeypair::generate();
        let secret = kp.signing_key.to_bytes();
        let kp2 = SwarmKeypair::from_bytes(&secret).expect("should construct from bytes");
        assert_eq!(kp.public_key_bytes(), kp2.public_key_bytes());
    }

    #[test]
    fn keypair_save_load_roundtrip() {
        let kp = SwarmKeypair::generate();
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let path = tmp.path().join("test_keypair.json");
        kp.save(&path).expect("save");
        let loaded = SwarmKeypair::load(&path).expect("load");
        assert_eq!(kp.public_key_bytes(), loaded.public_key_bytes());

        // Verify that signatures from loaded key match.
        let msg = b"roundtrip test";
        let sig = kp.sign(msg);
        assert!(SwarmKeypair::verify(
            &loaded.public_key_bytes(),
            msg,
            &sig
        ));
    }

    #[test]
    fn keypair_load_nonexistent_file() {
        let result = SwarmKeypair::load(Path::new("/nonexistent/keypair.json"));
        assert!(result.is_err());
    }

    #[test]
    fn keypair_load_or_generate_creates_new() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let path = tmp.path().join("new_keypair.json");
        let kp = SwarmKeypair::load_or_generate(&path);
        assert!(path.exists());
        assert_eq!(kp.public_key_bytes().len(), 32);
    }

    #[test]
    fn keypair_load_or_generate_loads_existing() {
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let path = tmp.path().join("existing_keypair.json");
        let kp1 = SwarmKeypair::generate();
        kp1.save(&path).expect("save");
        let kp2 = SwarmKeypair::load_or_generate(&path);
        assert_eq!(kp1.public_key_bytes(), kp2.public_key_bytes());
    }

    #[test]
    fn keypair_verify_rejects_bad_lengths() {
        assert!(!SwarmKeypair::verify(&[0u8; 31], b"msg", &[0u8; 64]));
        assert!(!SwarmKeypair::verify(&[0u8; 32], b"msg", &[0u8; 63]));
    }

    // ====================================================================
    // SwarmIdentity tests
    // ====================================================================

    #[test]
    fn identity_validate_ok() {
        let kp = SwarmKeypair::generate();
        let identity = make_identity("test-swarm", &kp);
        assert!(identity.validate().is_ok());
    }

    #[test]
    fn identity_validate_empty_name() {
        let kp = SwarmKeypair::generate();
        let mut identity = make_identity("", &kp);
        identity.name = String::new();
        let result = identity.validate();
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("name"));
    }

    #[test]
    fn identity_validate_long_name() {
        let kp = SwarmKeypair::generate();
        let mut identity = make_identity("x", &kp);
        identity.name = "a".repeat(129);
        let result = identity.validate();
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("too long"));
    }

    #[test]
    fn identity_validate_bad_pubkey_length() {
        let kp = SwarmKeypair::generate();
        let mut identity = make_identity("test", &kp);
        identity.public_key = vec![0u8; 16];
        let result = identity.validate();
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("32 bytes"));
    }

    #[test]
    fn identity_validate_empty_version() {
        let kp = SwarmKeypair::generate();
        let mut identity = make_identity("test", &kp);
        identity.software_version = String::new();
        let result = identity.validate();
        assert!(result.is_err());
    }

    #[test]
    fn identity_fingerprint() {
        let kp = SwarmKeypair::generate();
        let identity = make_identity("test", &kp);
        let fp = identity.fingerprint();
        assert_eq!(fp.len(), 16);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn identity_fingerprint_deterministic() {
        let kp = SwarmKeypair::generate();
        let identity = make_identity("test", &kp);
        assert_eq!(identity.fingerprint(), identity.fingerprint());
    }

    #[test]
    fn identity_has_capability() {
        let kp = SwarmKeypair::generate();
        let mut identity = make_identity("test", &kp);
        identity.capabilities = vec![
            SwarmCapability::Compute {
                max_concurrent: 10,
                hardware_classes: vec!["x86_64".to_string()],
            },
            SwarmCapability::Storage {
                capacity_gb: 500,
                replicas: 3,
            },
        ];
        assert!(identity.has_capability("compute"));
        assert!(identity.has_capability("storage"));
        assert!(!identity.has_capability("relay"));
        assert!(!identity.has_capability("gpu_compute"));
    }

    #[test]
    fn identity_serde_roundtrip() {
        let kp = SwarmKeypair::generate();
        let identity = make_identity("serde-test", &kp);
        let json = serde_json::to_string(&identity).expect("serialize");
        let parsed: SwarmIdentity = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.id, identity.id);
        assert_eq!(parsed.name, identity.name);
        assert_eq!(parsed.public_key, identity.public_key);
    }

    // ====================================================================
    // SwarmCapability tests
    // ====================================================================

    #[test]
    fn capability_type_tags() {
        assert_eq!(
            SwarmCapability::Compute {
                max_concurrent: 1,
                hardware_classes: vec![]
            }
            .type_tag(),
            "compute"
        );
        assert_eq!(
            SwarmCapability::Storage {
                capacity_gb: 1,
                replicas: 1
            }
            .type_tag(),
            "storage"
        );
        assert_eq!(
            SwarmCapability::Relay { bandwidth_mbps: 1 }.type_tag(),
            "relay"
        );
        assert_eq!(
            SwarmCapability::PythonExecution {
                versions: vec![]
            }
            .type_tag(),
            "python_execution"
        );
        assert_eq!(SwarmCapability::ShellExecution.type_tag(), "shell_execution");
        assert_eq!(
            SwarmCapability::GpuCompute {
                gpu_count: 1,
                gpu_model: "test".to_string()
            }
            .type_tag(),
            "gpu_compute"
        );
        assert_eq!(
            SwarmCapability::PluginHost {
                plugins: vec![]
            }
            .type_tag(),
            "plugin_host"
        );
        assert_eq!(
            SwarmCapability::Custom("my_cap".to_string()).type_tag(),
            "my_cap"
        );
    }

    #[test]
    fn capability_serde_roundtrip() {
        let cap = SwarmCapability::GpuCompute {
            gpu_count: 4,
            gpu_model: "NVIDIA A100".to_string(),
        };
        let json = serde_json::to_string(&cap).expect("serialize");
        let parsed: SwarmCapability = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(cap, parsed);
    }

    // ====================================================================
    // PsycheSummary tests
    // ====================================================================

    #[test]
    fn psyche_validate_ok() {
        let ps = PsycheSummary {
            exertion: 3,
            vitality: 4,
            momentum: 2,
            foresight: 1,
            cohesion: 0,
            resilience: 4,
            efficiency: 3,
            dominant_archetype: Some("worker-marabunta".to_string()),
        };
        assert!(ps.validate().is_ok());
    }

    #[test]
    fn psyche_validate_out_of_range() {
        let ps = PsycheSummary {
            exertion: 5,
            vitality: 0,
            momentum: 0,
            foresight: 0,
            cohesion: 0,
            resilience: 0,
            efficiency: 0,
            dominant_archetype: None,
        };
        let result = ps.validate();
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("exertion"));
    }

    #[test]
    fn psyche_aggregate_score() {
        let ps = PsycheSummary {
            exertion: 4,
            vitality: 4,
            momentum: 4,
            foresight: 4,
            cohesion: 4,
            resilience: 4,
            efficiency: 4,
            dominant_archetype: None,
        };
        let score = ps.aggregate_score();
        assert!((score - 4.0).abs() < 0.001);
    }

    #[test]
    fn psyche_aggregate_score_zero() {
        let ps = PsycheSummary {
            exertion: 0,
            vitality: 0,
            momentum: 0,
            foresight: 0,
            cohesion: 0,
            resilience: 0,
            efficiency: 0,
            dominant_archetype: None,
        };
        assert!((ps.aggregate_score() - 0.0).abs() < 0.001);
    }

    #[test]
    fn psyche_serde_roundtrip() {
        let ps = PsycheSummary {
            exertion: 2,
            vitality: 3,
            momentum: 1,
            foresight: 4,
            cohesion: 2,
            resilience: 3,
            efficiency: 1,
            dominant_archetype: Some("research-cluster".to_string()),
        };
        let json = serde_json::to_string(&ps).expect("serialize");
        let parsed: PsycheSummary = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ps, parsed);
    }

    // ====================================================================
    // SwarmHeartbeat tests
    // ====================================================================

    #[test]
    fn heartbeat_sign_and_verify() {
        let kp = SwarmKeypair::generate();
        let mut hb = SwarmHeartbeat {
            swarm_id: SwarmId::new(),
            timestamp: Utc::now(),
            signature: Vec::new(),
            node_count: 10,
            alive_count: 8,
            psyche_summary: None,
            load_average: 0.5,
            uptime_secs: 3600,
            software_version: "0.1.0".to_string(),
            active_jobs: 3,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = kp.sign(&payload);
        assert!(SwarmKeypair::verify(
            &kp.public_key_bytes(),
            &payload,
            &hb.signature
        ));
    }

    #[test]
    fn heartbeat_serde_roundtrip() {
        let hb = SwarmHeartbeat {
            swarm_id: SwarmId::new(),
            timestamp: Utc::now(),
            signature: vec![0u8; 64],
            node_count: 5,
            alive_count: 5,
            psyche_summary: Some(PsycheSummary {
                exertion: 2,
                vitality: 3,
                momentum: 2,
                foresight: 1,
                cohesion: 3,
                resilience: 4,
                efficiency: 2,
                dominant_archetype: None,
            }),
            load_average: 0.3,
            uptime_secs: 7200,
            software_version: "0.2.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::from([("uptime".to_string(), 0.999)]),
        };
        let json = serde_json::to_string(&hb).expect("serialize");
        let parsed: SwarmHeartbeat = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.swarm_id, hb.swarm_id);
        assert_eq!(parsed.node_count, hb.node_count);
    }

    // ====================================================================
    // SwarmReachability tests
    // ====================================================================

    #[test]
    fn reachability_default_is_unknown() {
        let r = SwarmReachability::default();
        assert!(matches!(r, SwarmReachability::Unknown));
    }

    #[test]
    fn reachability_serde_roundtrip() {
        let states = vec![
            SwarmReachability::Reachable,
            SwarmReachability::Stale { since: Utc::now() },
            SwarmReachability::Unreachable { since: Utc::now() },
            SwarmReachability::Unknown,
        ];
        for state in states {
            let json = serde_json::to_string(&state).expect("serialize");
            let parsed: SwarmReachability = serde_json::from_str(&json).expect("deserialize");
            match (&state, &parsed) {
                (SwarmReachability::Reachable, SwarmReachability::Reachable) => {}
                (SwarmReachability::Unknown, SwarmReachability::Unknown) => {}
                (SwarmReachability::Stale { .. }, SwarmReachability::Stale { .. }) => {}
                (SwarmReachability::Unreachable { .. }, SwarmReachability::Unreachable { .. }) => {}
                _ => panic!("reachability mismatch: {:?} vs {:?}", state, parsed),
            }
        }
    }

    // ====================================================================
    // SovereigntyManager tests
    // ====================================================================

    #[test]
    fn manager_local_identity() {
        let (manager, _) = make_manager("test-swarm");
        let identity = manager.local_identity();
        assert_eq!(identity.name, "test-swarm");
    }

    #[test]
    fn manager_local_id() {
        let (manager, _) = make_manager("test-swarm");
        let id = manager.local_id();
        assert_eq!(id, manager.local_identity().id);
    }

    #[test]
    fn manager_register_and_get_remote() {
        let (manager, _) = make_manager("local");
        let (remote_identity, _) = make_remote_identity("remote-1");
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity)
            .expect("register");

        let summary = manager.get_remote(&remote_id).expect("should exist");
        assert_eq!(summary.identity.name, "remote-1");
        assert_eq!(summary.heartbeat_count, 0);
        assert!(matches!(summary.reachability, SwarmReachability::Unknown));
    }

    #[test]
    fn manager_register_invalid_identity() {
        let (manager, _) = make_manager("local");
        let kp = SwarmKeypair::generate();
        let mut identity = make_identity("", &kp);
        identity.name = String::new();
        let result = manager.register_remote(identity);
        assert!(result.is_err());
    }

    #[test]
    fn manager_unregister_remote() {
        let (manager, _) = make_manager("local");
        let (remote_identity, _) = make_remote_identity("remote-1");
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity)
            .expect("register");
        assert_eq!(manager.remote_count(), 1);

        manager.unregister_remote(&remote_id).expect("unregister");
        assert_eq!(manager.remote_count(), 0);
        assert!(manager.get_remote(&remote_id).is_none());
    }

    #[test]
    fn manager_unregister_unknown() {
        let (manager, _) = make_manager("local");
        let result = manager.unregister_remote(&SwarmId::new());
        assert!(result.is_err());
    }

    #[test]
    fn manager_receive_heartbeat_valid() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("remote-1", &remote_kp);
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity.clone())
            .expect("register");

        // Build a signed heartbeat.
        let mut hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now(),
            signature: Vec::new(),
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.2,
            uptime_secs: 100,
            software_version: "0.1.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = remote_kp.sign(&payload);

        manager
            .receive_heartbeat(hb)
            .expect("should accept heartbeat");

        let summary = manager.get_remote(&remote_id).expect("should exist");
        assert_eq!(summary.heartbeat_count, 1);
        assert!(matches!(
            summary.reachability,
            SwarmReachability::Reachable
        ));
    }

    #[test]
    fn manager_receive_heartbeat_invalid_signature() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("remote-1", &remote_kp);
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity)
            .expect("register");

        let hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now(),
            signature: vec![0u8; 64], // Invalid signature.
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.2,
            uptime_secs: 100,
            software_version: "0.1.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };

        let result = manager.receive_heartbeat(hb);
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("invalid signature"));
    }

    #[test]
    fn manager_receive_heartbeat_unknown_swarm() {
        let (manager, _) = make_manager("local");

        let hb = SwarmHeartbeat {
            swarm_id: SwarmId::new(),
            timestamp: Utc::now(),
            signature: vec![0u8; 64],
            node_count: 1,
            alive_count: 1,
            psyche_summary: None,
            load_average: 0.0,
            uptime_secs: 0,
            software_version: "0.1.0".to_string(),
            active_jobs: 0,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };

        let result = manager.receive_heartbeat(hb);
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("unknown swarm"));
    }

    #[test]
    fn manager_receive_heartbeat_expired() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("remote-1", &remote_kp);
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity)
            .expect("register");

        // Heartbeat from 10 minutes ago (> MAX_HEARTBEAT_AGE of 5 min).
        let mut hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now() - ChronoDuration::seconds(600),
            signature: Vec::new(),
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.2,
            uptime_secs: 100,
            software_version: "0.1.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = remote_kp.sign(&payload);

        let result = manager.receive_heartbeat(hb);
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("too old"));
    }

    #[test]
    fn manager_receive_heartbeat_future() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("remote-1", &remote_kp);
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity)
            .expect("register");

        // Heartbeat 60 seconds in the future (> MAX_HEARTBEAT_FUTURE of 30s).
        let mut hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now() + ChronoDuration::seconds(60),
            signature: Vec::new(),
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.2,
            uptime_secs: 100,
            software_version: "0.1.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = remote_kp.sign(&payload);

        let result = manager.receive_heartbeat(hb);
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("future"));
    }

    #[test]
    fn manager_staleness_detection() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("remote-1", &remote_kp);
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity)
            .expect("register");

        // Send a valid heartbeat timestamped 3 minutes ago.
        let mut hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now() - ChronoDuration::seconds(180),
            signature: Vec::new(),
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.2,
            uptime_secs: 100,
            software_version: "0.1.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = remote_kp.sign(&payload);

        // The heartbeat is within MAX_HEARTBEAT_AGE (5 min), so it is accepted.
        manager
            .receive_heartbeat(hb)
            .expect("should accept");

        // Now detect staleness. The heartbeat is 3 min old, which exceeds
        // the default stale threshold of 2 min.
        let transitions = manager.detect_staleness();
        assert_eq!(transitions, 1);

        let summary = manager.get_remote(&remote_id).expect("should exist");
        assert!(matches!(summary.reachability, SwarmReachability::Stale { .. }));
    }

    #[test]
    fn manager_unreachable_detection() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("remote-1", &remote_kp);
        let remote_id = remote_identity.id;

        // Use short thresholds for testing.
        let manager = SovereigntyManager::new(
            manager.local_identity(),
            manager.keypair().clone(),
            None,
        )
        .with_thresholds(
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4),
        );

        manager
            .register_remote(remote_identity)
            .expect("register");

        // Heartbeat from 3 seconds ago (within MAX_HEARTBEAT_AGE).
        let mut hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now() - ChronoDuration::seconds(5),
            signature: Vec::new(),
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.2,
            uptime_secs: 100,
            software_version: "0.1.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = remote_kp.sign(&payload);

        manager
            .receive_heartbeat(hb)
            .expect("should accept");

        let transitions = manager.detect_staleness();
        assert_eq!(transitions, 1);

        let summary = manager.get_remote(&remote_id).expect("should exist");
        assert!(
            matches!(summary.reachability, SwarmReachability::Unreachable { .. }),
            "expected Unreachable, got {:?}",
            summary.reachability
        );
    }

    #[test]
    fn manager_conflict_name_collision() {
        let (manager, _) = make_manager("local");
        let (remote1, _) = make_remote_identity("same-name");
        let (mut remote2, _) = make_remote_identity("same-name");
        // Ensure different IDs.
        remote2.id = SwarmId::new();

        manager.register_remote(remote1).expect("register first");
        let result = manager.register_remote(remote2);
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("name conflict"));
    }

    #[test]
    fn manager_conflict_key_reuse() {
        let (manager, _) = make_manager("local");
        let shared_kp = SwarmKeypair::generate();

        let mut identity1 = make_identity("swarm-a", &shared_kp);
        identity1.id = SwarmId::new();

        let mut identity2 = make_identity("swarm-b", &shared_kp);
        identity2.id = SwarmId::new();

        manager.register_remote(identity1).expect("register first");
        let result = manager.register_remote(identity2);
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("public key conflict"));
    }

    #[test]
    fn manager_conflict_with_local_name() {
        let (manager, _) = make_manager("local-swarm");
        let (mut remote, _) = make_remote_identity("local-swarm");
        remote.id = SwarmId::new(); // Different ID, same name.

        let result = manager.register_remote(remote);
        assert!(result.is_err());
        assert!(result.err().expect("err").contains("name conflict with local"));
    }

    #[test]
    fn manager_trust_score_zero_for_new() {
        let (manager, _) = make_manager("local");
        let (remote_identity, _) = make_remote_identity("remote-1");
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity)
            .expect("register");

        let score = manager.trust_score(&remote_id);
        // New swarm with no heartbeats: very low trust.
        assert!(score < 0.1, "new swarm trust should be near 0, got {}", score);
    }

    #[test]
    fn manager_trust_score_unknown_swarm() {
        let (manager, _) = make_manager("local");
        let score = manager.trust_score(&SwarmId::new());
        assert!((score - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn manager_trust_increases_with_heartbeats() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("remote-1", &remote_kp);
        let remote_id = remote_identity.id;

        manager
            .register_remote(remote_identity)
            .expect("register");

        let score_before = manager.trust_score(&remote_id);

        // Send a valid heartbeat.
        let mut hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now(),
            signature: Vec::new(),
            node_count: 10,
            alive_count: 10,
            psyche_summary: None,
            load_average: 0.1,
            uptime_secs: 500,
            software_version: "0.1.0".to_string(),
            active_jobs: 0,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = remote_kp.sign(&payload);
        manager.receive_heartbeat(hb).expect("heartbeat");

        let score_after = manager.trust_score(&remote_id);
        assert!(
            score_after >= score_before,
            "trust should increase after heartbeat: {} >= {}",
            score_after,
            score_before
        );
    }

    #[test]
    fn manager_list_remotes() {
        let (manager, _) = make_manager("local");
        let (r1, _) = make_remote_identity("remote-1");
        let (r2, _) = make_remote_identity("remote-2");

        manager.register_remote(r1).expect("register");
        manager.register_remote(r2).expect("register");

        let remotes = manager.list_remotes();
        assert_eq!(remotes.len(), 2);

        let names: Vec<String> = remotes.iter().map(|r| r.identity.name.clone()).collect();
        assert!(names.contains(&"remote-1".to_string()));
        assert!(names.contains(&"remote-2".to_string()));
    }

    #[test]
    fn manager_reachable_remotes_filter() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("reachable-remote", &remote_kp);
        let remote_id = remote_identity.id;

        let (unreachable_identity, _) = make_remote_identity("unreachable-remote");

        manager
            .register_remote(remote_identity)
            .expect("register reachable");
        manager
            .register_remote(unreachable_identity)
            .expect("register unreachable");

        // Send heartbeat to one remote only.
        let mut hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now(),
            signature: Vec::new(),
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.1,
            uptime_secs: 100,
            software_version: "0.1.0".to_string(),
            active_jobs: 0,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = remote_kp.sign(&payload);
        manager.receive_heartbeat(hb).expect("heartbeat");

        let reachable = manager.reachable_remotes();
        assert_eq!(reachable.len(), 1);
        assert_eq!(reachable[0].identity.name, "reachable-remote");
    }

    #[test]
    fn manager_generate_heartbeat_signed() {
        let (manager, keypair) = make_manager("local");
        let hb = manager.generate_heartbeat(10, 8, 0.5, 3600);
        assert_eq!(hb.node_count, 10);
        assert_eq!(hb.alive_count, 8);
        assert!(!hb.signature.is_empty());

        // Verify signature.
        let payload = hb.signing_payload();
        assert!(SwarmKeypair::verify(
            &keypair.public_key_bytes(),
            &payload,
            &hb.signature
        ));
    }

    #[test]
    fn manager_generate_heartbeat_full() {
        let (manager, keypair) = make_manager("local");
        let psyche = PsycheSummary {
            exertion: 3,
            vitality: 4,
            momentum: 2,
            foresight: 1,
            cohesion: 3,
            resilience: 4,
            efficiency: 2,
            dominant_archetype: Some("worker-marabunta".to_string()),
        };
        let mut sla = HashMap::new();
        sla.insert("uptime".to_string(), 0.999);

        let hb = manager.generate_heartbeat_full(
            20,
            18,
            0.7,
            7200,
            Some(psyche),
            5,
            ResourceSnapshot::default(),
            sla,
        );

        assert_eq!(hb.node_count, 20);
        assert_eq!(hb.active_jobs, 5);
        assert!(hb.psyche_summary.is_some());

        let payload = hb.signing_payload();
        assert!(SwarmKeypair::verify(
            &keypair.public_key_bytes(),
            &payload,
            &hb.signature
        ));
    }

    #[test]
    fn manager_with_event_bus() {
        let (manager, _, bus) = make_manager_with_bus("local-bus");
        let mut rx = bus.subscribe();

        let (remote_identity, _) = make_remote_identity("remote-bus");
        manager
            .register_remote(remote_identity)
            .expect("register");

        // Should have received a registration event.
        let event = rx.try_recv();
        assert!(event.is_ok(), "should receive event");
        let event = event.expect("event");
        assert_eq!(event.domain, "sovereignty");
        assert!(event.summary.contains("registered"));
    }

    #[test]
    fn manager_staleness_no_heartbeat_stays_unknown() {
        let (manager, _) = make_manager("local");
        let (remote_identity, _) = make_remote_identity("remote-1");

        manager
            .register_remote(remote_identity)
            .expect("register");

        // No heartbeat sent. detect_staleness should not transition
        // Unknown to anything.
        let transitions = manager.detect_staleness();
        assert_eq!(transitions, 0);
    }

    #[test]
    fn manager_heartbeat_resets_stale_to_reachable() {
        let (manager, _) = make_manager("local");
        let remote_kp = Arc::new(SwarmKeypair::generate());
        let remote_identity = make_identity("remote-1", &remote_kp);
        let remote_id = remote_identity.id;

        let manager = SovereigntyManager::new(
            manager.local_identity(),
            manager.keypair().clone(),
            None,
        )
        .with_thresholds(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(5),
        );

        manager
            .register_remote(remote_identity)
            .expect("register");

        // Heartbeat from 2 seconds ago (> stale_threshold of 1s, but
        // within MAX_HEARTBEAT_AGE of 5 min).
        let mut hb = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now() - ChronoDuration::seconds(2),
            signature: Vec::new(),
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.2,
            uptime_secs: 100,
            software_version: "0.1.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload = hb.signing_payload();
        hb.signature = remote_kp.sign(&payload);
        manager.receive_heartbeat(hb).expect("hb");

        // Force stale.
        manager.detect_staleness();
        let summary = manager.get_remote(&remote_id).expect("exists");
        assert!(matches!(summary.reachability, SwarmReachability::Stale { .. }));

        // Now send a fresh heartbeat.
        let mut hb2 = SwarmHeartbeat {
            swarm_id: remote_id,
            timestamp: Utc::now(),
            signature: Vec::new(),
            node_count: 5,
            alive_count: 5,
            psyche_summary: None,
            load_average: 0.2,
            uptime_secs: 102,
            software_version: "0.1.0".to_string(),
            active_jobs: 1,
            available_capacity: ResourceSnapshot::default(),
            sla_compliance: HashMap::new(),
        };
        let payload2 = hb2.signing_payload();
        hb2.signature = remote_kp.sign(&payload2);
        manager.receive_heartbeat(hb2).expect("hb2");

        let summary2 = manager.get_remote(&remote_id).expect("exists");
        assert!(
            matches!(summary2.reachability, SwarmReachability::Reachable),
            "expected Reachable, got {:?}",
            summary2.reachability
        );
    }

    // ====================================================================
    // EventBus tests
    // ====================================================================

    #[test]
    fn event_bus_emit_and_receive() {
        let bus = EventBus::new(16);
        let mut rx = bus.subscribe();

        bus.emit_simple("test-domain", "info", "hello world");

        let event = rx.try_recv().expect("should receive event");
        assert_eq!(event.domain, "test-domain");
        assert_eq!(event.severity, "info");
        assert_eq!(event.summary, "hello world");
    }

    #[test]
    fn event_bus_no_receiver_ok() {
        let bus = EventBus::new(16);
        // No subscriber; emit should not panic.
        bus.emit_simple("test", "info", "no one is listening");
    }

    // ====================================================================
    // SwarmSummary serde roundtrip
    // ====================================================================

    #[test]
    fn swarm_summary_serde_roundtrip() {
        let kp = SwarmKeypair::generate();
        let identity = make_identity("summary-test", &kp);

        let summary = SwarmSummary {
            identity,
            last_heartbeat: None,
            first_seen: Utc::now(),
            heartbeat_count: 42,
            reachability: SwarmReachability::Reachable,
            trust_score: 0.75,
            avg_heartbeat_interval_secs: 30.5,
        };

        let json = serde_json::to_string(&summary).expect("serialize");
        let parsed: SwarmSummary = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.identity.name, "summary-test");
        assert_eq!(parsed.heartbeat_count, 42);
    }

    // ====================================================================
    // Hex encoding utility
    // ====================================================================

    #[test]
    fn hex_encode_empty() {
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn hex_encode_known() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }
}
