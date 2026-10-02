// Marabunta - Licensed under the MIT License.
//! Protocol constants and configuration for the Marabunta Radiation Protocol.
//!
//! All tunable parameters with defaults derived from the protocol specification.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ============================================================================
// Frame (spec 02-TRANSPORT)
// ============================================================================

/// Fixed frame size in bytes. Every frame on the wire is exactly this size.
pub const FRAME_SIZE: usize = 1024;

/// Frame header overhead: type(1) + sequence(4) + payload_len(2) = 7 bytes.
pub const FRAME_HEADER_SIZE: usize = 7;

/// Maximum payload that fits in a single frame.
pub const MAX_PAYLOAD_SIZE: usize = FRAME_SIZE - FRAME_HEADER_SIZE;

// ============================================================================
// Gossip (spec 03-GOSSIP)
// ============================================================================

/// Base gossip interval in milliseconds (normal mode).
pub const GOSSIP_BASE_INTERVAL_MS: u64 = 2000;

/// Gossip jitter range in milliseconds (uniform random ±).
pub const GOSSIP_JITTER_MS: u64 = 500;

/// Number of peers to gossip to per round.
pub const GOSSIP_FANOUT: usize = 3;

/// Default TTL for gossip entries (max hops within neighborhood).
pub const DEFAULT_TTL: u8 = 3;

/// TTL for inter-neighborhood summary messages.
pub const SUMMARY_TTL: u8 = 2;

/// Maximum age of a gossip entry before it is pruned (milliseconds).
pub const MAX_ENTRY_AGE_MS: u64 = 30_000;

/// Gossip interval during high churn (milliseconds).
pub const GOSSIP_CHURN_INTERVAL_MS: u64 = 1000;

/// Gossip interval when cluster is stable (milliseconds).
pub const GOSSIP_STABLE_INTERVAL_MS: u64 = 5000;

/// Duration of stability before switching to stable interval (seconds).
pub const GOSSIP_STABLE_THRESHOLD_S: u64 = 300;

/// Anti-entropy Merkle digest exchange interval (seconds).
pub const ANTI_ENTROPY_INTERVAL_S: u64 = 60;

// ============================================================================
// Neighborhood (spec 03-GOSSIP + spec 07-HIERARCHY)
// ============================================================================

/// Target number of nodes per neighborhood.
pub const NEIGHBORHOOD_TARGET_SIZE: usize = 100;

/// Split neighborhood when membership exceeds this threshold.
pub const NEIGHBORHOOD_SPLIT_THRESHOLD: usize = 150;

/// Merge neighborhoods when both are below this threshold.
pub const NEIGHBORHOOD_MERGE_THRESHOLD: usize = 30;

/// Time to be considered "settled" in a neighborhood (seconds).
pub const NEIGHBORHOOD_SETTLE_TIME_S: u64 = 60;

/// Number of candidate neighborhoods to probe during join.
pub const NEIGHBORHOOD_JOIN_CANDIDATES: usize = 3;

/// Number of nodes to ping per candidate during join procedure.
pub const NEIGHBORHOOD_JOIN_PROBES: usize = 10;

// ============================================================================
// Summary exchange (spec 07-HIERARCHY)
// ============================================================================

/// Interval for neighborhood summary exchange (milliseconds).
pub const SUMMARY_EXCHANGE_INTERVAL_MS: u64 = 5000;

/// Jitter range for summary exchange (milliseconds).
pub const SUMMARY_EXCHANGE_JITTER_MS: u64 = 1000;

/// Number of neighboring neighborhoods to exchange summaries with.
pub const SUMMARY_EXCHANGE_FANOUT: usize = 5;

/// Region summary exchange interval (milliseconds).
pub const REGION_SUMMARY_INTERVAL_MS: u64 = 15_000;

/// Zone summary exchange interval (milliseconds).
pub const ZONE_SUMMARY_INTERVAL_MS: u64 = 45_000;

// ============================================================================
// Hierarchy scale (spec 07-HIERARCHY)
// ============================================================================

/// Expected number of individual nodes at full scale.
pub const LEVEL0_NODES: usize = 5_000_000;

/// Expected number of neighborhoods at full scale.
pub const LEVEL1_NEIGHBORHOODS: usize = 50_000;

/// Expected number of regions at full scale.
pub const LEVEL2_REGIONS: usize = 5_000;

/// Expected number of zones at full scale.
pub const LEVEL3_ZONES: usize = 50;

// ============================================================================
// Connection management (spec 02-TRANSPORT)
// ============================================================================

/// Maximum number of simultaneous relay connections per node.
pub const MAX_RELAY_CONNECTIONS: usize = 5;

/// Maximum peers reachable through a single relay.
pub const MAX_PEERS_PER_RELAY: usize = 50;

/// WebSocket keepalive (ping/pong) interval (seconds).
pub const KEEPALIVE_INTERVAL_S: u64 = 30;

/// Initial reconnect backoff delay (milliseconds).
pub const RECONNECT_BACKOFF_INIT_MS: u64 = 1_000;

/// Maximum reconnect backoff delay (milliseconds).
pub const RECONNECT_BACKOFF_MAX_MS: u64 = 300_000;

/// Backoff multiplier per retry attempt.
pub const RECONNECT_BACKOFF_MULTIPLIER: u32 = 2;

// ============================================================================
// Transport profile transition (spec 06-TRANSITION)
// ============================================================================

/// Relay connectivity below this triggers Adaptive mode.
pub const TRANSITION_RELAY_CONNECTIVITY_LOW: f32 = 0.4;

/// Packet loss above this triggers Adaptive mode.
pub const TRANSITION_PACKET_LOSS_HIGH: f32 = 0.3;

/// Connection resets per minute above this triggers Adaptive mode.
pub const TRANSITION_RESET_RATE_HIGH: f32 = 10.0;

/// DNS resolution rate below this triggers Adaptive mode.
pub const TRANSITION_DNS_RATE_LOW: f32 = 0.5;

/// Throughput ratio below this triggers Adaptive mode.
pub const TRANSITION_THROUGHPUT_LOW: f32 = 0.1;

/// Relay connectivity above this allows return to Open mode.
pub const TRANSITION_RELAY_CONNECTIVITY_OK: f32 = 0.8;

/// Packet loss below this allows return to Open mode.
pub const TRANSITION_PACKET_LOSS_OK: f32 = 0.05;

/// Connection resets per minute below this allows return to Open mode.
pub const TRANSITION_RESET_RATE_OK: f32 = 1.0;

/// DNS resolution rate above this allows return to Open mode.
pub const TRANSITION_DNS_RATE_OK: f32 = 0.95;

/// Throughput ratio above this allows return to Open mode.
pub const TRANSITION_THROUGHPUT_OK: f32 = 0.5;

/// Minimum time between profile transitions (seconds).
pub const HYSTERESIS_DURATION_S: u64 = 300;

/// Number of relay connections in Open profile.
pub const OPEN_RELAY_COUNT: usize = 5;

/// Number of relay connections in Adaptive profile.
pub const ADAPTIVE_RELAY_COUNT: usize = 8;

// ============================================================================
// Dark mode (spec 06-TRANSITION)
// ============================================================================

/// Exponential retry intervals for dark mode (seconds).
pub const DARK_MODE_RETRY_INTERVALS_S: &[u64] = &[60, 300, 1_800, 7_200, 43_200];

// ============================================================================
// Blind computation (spec 05-BLIND)
// ============================================================================

/// Maximum WASM sandbox memory (megabytes).
pub const MAX_WASM_MEMORY_MB: u32 = 512;

/// Maximum WASM execution time (seconds).
pub const MAX_EXECUTION_TIME_S: u64 = 300;

/// Maximum WASM binary size (megabytes).
pub const MAX_WASM_BINARY_MB: u32 = 10;

/// Default fuel limit for WASM execution.
pub const DEFAULT_WASM_FUEL: u64 = 10_000_000_000;

// ============================================================================
// Failure detection (spec 03-GOSSIP)
// ============================================================================

/// Time before a silent node is marked SUSPECT (seconds).
pub const SUSPECT_TIMEOUT_S: u64 = 10;

/// Additional time before a suspect node is marked DEAD (seconds).
pub const DEAD_TIMEOUT_S: u64 = 30;

// ============================================================================
// Traffic shaping (spec 02-TRANSPORT)
// ============================================================================

/// Minimum idle bandwidth target (KB/s) for PAD generation.
pub const IDLE_BANDWIDTH_MIN_KBPS: u32 = 5;

/// Maximum idle bandwidth target (KB/s) for PAD generation.
pub const IDLE_BANDWIDTH_MAX_KBPS: u32 = 15;

/// Target download/upload ratio for traffic shaping.
pub const DOWNLOAD_UPLOAD_RATIO: f32 = 0.8;

// ============================================================================
// Relay selection weights (spec 04-IDENTITY)
// ============================================================================

/// Weight for latency in relay scoring.
pub const RELAY_SCORE_LATENCY_WEIGHT: f64 = 0.30;

/// Weight for available capacity in relay scoring.
pub const RELAY_SCORE_CAPACITY_WEIGHT: f64 = 0.25;

/// Weight for reputation in relay scoring.
pub const RELAY_SCORE_REPUTATION_WEIGHT: f64 = 0.25;

/// Weight for ASN/region diversity in relay scoring.
pub const RELAY_SCORE_DIVERSITY_WEIGHT: f64 = 0.20;

/// Maximum relays from the same region.
pub const RELAY_MAX_SAME_REGION: usize = 2;

// ============================================================================
// MarabuntaConfig — runtime configuration
// ============================================================================

/// Runtime configuration for a Marabunta protocol node.
///
/// All fields have sensible defaults derived from the protocol spec constants.
/// Can be deserialized from TOML for operator customization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarabuntaConfig {
    /// Gossip interval in milliseconds.
    #[serde(default = "default_gossip_interval_ms")]
    pub gossip_interval_ms: u64,

    /// Number of peers to gossip to per round.
    #[serde(default = "default_gossip_fanout")]
    pub gossip_fanout: usize,

    /// Maximum number of simultaneous relay connections.
    #[serde(default = "default_max_relay_connections")]
    pub max_relay_connections: usize,

    /// WebSocket keepalive interval in seconds.
    #[serde(default = "default_keepalive_interval_s")]
    pub keepalive_interval_s: u64,

    /// Maximum WASM sandbox memory in megabytes.
    #[serde(default = "default_max_wasm_memory_mb")]
    pub max_wasm_memory_mb: u32,

    /// Maximum WASM execution time in seconds.
    #[serde(default = "default_max_execution_time_s")]
    pub max_execution_time_s: u64,

    /// Maximum WASM binary size in megabytes.
    #[serde(default = "default_max_wasm_binary_mb")]
    pub max_wasm_binary_mb: u32,

    /// WASM fuel limit for execution metering.
    #[serde(default = "default_wasm_fuel")]
    pub wasm_fuel: u64,

    /// Path to encrypted identity keystore. If None, generates ephemeral identity.
    #[serde(default)]
    pub identity_keystore_path: Option<PathBuf>,

    /// Hardcoded bootstrap relay endpoints.
    #[serde(default = "default_bootstrap_relays")]
    pub bootstrap_relays: Vec<String>,

    /// Default job redundancy (execute on N independent nodes).
    #[serde(default = "default_redundancy")]
    pub default_redundancy: u8,

    /// Neighborhood target size override.
    #[serde(default = "default_neighborhood_target")]
    pub neighborhood_target_size: usize,

    /// Enable Adaptive transport profile switching.
    #[serde(default = "default_true")]
    pub adaptive_transport_enabled: bool,
}

fn default_gossip_interval_ms() -> u64 { GOSSIP_BASE_INTERVAL_MS }
fn default_gossip_fanout() -> usize { GOSSIP_FANOUT }
fn default_max_relay_connections() -> usize { MAX_RELAY_CONNECTIONS }
fn default_keepalive_interval_s() -> u64 { KEEPALIVE_INTERVAL_S }
fn default_max_wasm_memory_mb() -> u32 { MAX_WASM_MEMORY_MB }
fn default_max_execution_time_s() -> u64 { MAX_EXECUTION_TIME_S }
fn default_max_wasm_binary_mb() -> u32 { MAX_WASM_BINARY_MB }
fn default_wasm_fuel() -> u64 { DEFAULT_WASM_FUEL }
fn default_bootstrap_relays() -> Vec<String> {
    vec![
        "wss://relay-us.marabunta.io:443".into(),
        "wss://relay-eu.marabunta.io:443".into(),
        "wss://relay-ap.marabunta.io:443".into(),
        "wss://relay-sa.marabunta.io:443".into(),
        "wss://relay-af.marabunta.io:443".into(),
    ]
}
fn default_redundancy() -> u8 { 1 }
fn default_neighborhood_target() -> usize { NEIGHBORHOOD_TARGET_SIZE }
fn default_true() -> bool { true }

impl Default for MarabuntaConfig {
    fn default() -> Self {
        Self {
            gossip_interval_ms: default_gossip_interval_ms(),
            gossip_fanout: default_gossip_fanout(),
            max_relay_connections: default_max_relay_connections(),
            keepalive_interval_s: default_keepalive_interval_s(),
            max_wasm_memory_mb: default_max_wasm_memory_mb(),
            max_execution_time_s: default_max_execution_time_s(),
            max_wasm_binary_mb: default_max_wasm_binary_mb(),
            wasm_fuel: default_wasm_fuel(),
            identity_keystore_path: None,
            bootstrap_relays: default_bootstrap_relays(),
            default_redundancy: default_redundancy(),
            neighborhood_target_size: default_neighborhood_target(),
            adaptive_transport_enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MarabuntaConfig::default();
        assert_eq!(config.gossip_interval_ms, GOSSIP_BASE_INTERVAL_MS);
        assert_eq!(config.gossip_fanout, GOSSIP_FANOUT);
        assert_eq!(config.max_relay_connections, MAX_RELAY_CONNECTIONS);
        assert_eq!(config.keepalive_interval_s, KEEPALIVE_INTERVAL_S);
        assert_eq!(config.max_wasm_memory_mb, MAX_WASM_MEMORY_MB);
        assert_eq!(config.max_execution_time_s, MAX_EXECUTION_TIME_S);
        assert_eq!(config.max_wasm_binary_mb, MAX_WASM_BINARY_MB);
        assert_eq!(config.wasm_fuel, DEFAULT_WASM_FUEL);
        assert!(config.identity_keystore_path.is_none());
        assert_eq!(config.bootstrap_relays.len(), 5);
        assert_eq!(config.default_redundancy, 1);
        assert_eq!(config.neighborhood_target_size, NEIGHBORHOOD_TARGET_SIZE);
        assert!(config.adaptive_transport_enabled);
    }

    #[test]
    fn test_config_toml_roundtrip() {
        let config = MarabuntaConfig::default();
        let toml_str = toml::to_string(&config).expect("serialize to TOML");
        let parsed: MarabuntaConfig = toml::from_str(&toml_str).expect("deserialize from TOML");
        assert_eq!(parsed.gossip_interval_ms, config.gossip_interval_ms);
        assert_eq!(parsed.gossip_fanout, config.gossip_fanout);
        assert_eq!(parsed.max_relay_connections, config.max_relay_connections);
        assert_eq!(parsed.max_wasm_memory_mb, config.max_wasm_memory_mb);
        assert_eq!(parsed.bootstrap_relays, config.bootstrap_relays);
    }

    #[test]
    fn test_config_partial_toml() {
        let toml_str = r#"
gossip_interval_ms = 1000
gossip_fanout = 5
"#;
        let config: MarabuntaConfig = toml::from_str(toml_str).expect("parse partial TOML");
        assert_eq!(config.gossip_interval_ms, 1000);
        assert_eq!(config.gossip_fanout, 5);
        // Others should be defaults
        assert_eq!(config.max_relay_connections, MAX_RELAY_CONNECTIONS);
        assert_eq!(config.max_wasm_memory_mb, MAX_WASM_MEMORY_MB);
    }

    #[test]
    fn test_frame_size_constants() {
        assert_eq!(FRAME_SIZE, 1024);
        assert_eq!(FRAME_HEADER_SIZE, 7);
        assert_eq!(MAX_PAYLOAD_SIZE, 1017);
    }

    #[test]
    fn test_transition_thresholds_consistent() {
        // "Back to Open" thresholds should be stricter than "To Adaptive" thresholds
        assert!(TRANSITION_RELAY_CONNECTIVITY_OK > TRANSITION_RELAY_CONNECTIVITY_LOW);
        assert!(TRANSITION_PACKET_LOSS_OK < TRANSITION_PACKET_LOSS_HIGH);
        assert!(TRANSITION_RESET_RATE_OK < TRANSITION_RESET_RATE_HIGH);
        assert!(TRANSITION_DNS_RATE_OK > TRANSITION_DNS_RATE_LOW);
        assert!(TRANSITION_THROUGHPUT_OK > TRANSITION_THROUGHPUT_LOW);
    }

    #[test]
    fn test_relay_score_weights_sum_to_one() {
        let sum = RELAY_SCORE_LATENCY_WEIGHT
            + RELAY_SCORE_CAPACITY_WEIGHT
            + RELAY_SCORE_REPUTATION_WEIGHT
            + RELAY_SCORE_DIVERSITY_WEIGHT;
        assert!((sum - 1.0).abs() < f64::EPSILON);
    }
}
