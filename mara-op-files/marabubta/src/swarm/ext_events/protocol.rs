// Marabunta - Licensed under the MIT License.
//! The Ext. Event Bus: Protocol Definitions
//! Defines the cryptographic payloads and routing semantics for the planetary pub/sub mesh.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use crate::swarm::planetary::topology::GeoCell;

/// Defines the physical survival and routing mechanics of an event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// Raw Gossip (The IoT Telemetry Killer).
    /// Lightning-fast, eventually consistent, dropped if TTL expires.
    Ephemeral { ttl_ms: u64 },
    
    /// BFT Hashgraph Ledger (The Financial MQ Killer).
    /// Requires 2/3 multi-signature consensus. Shattered via Reed-Solomon for permanent DHT retention.
    /// Strictly bounds the Reed-Solomon shards to a specific geographic jurisdiction (GDPR compliance).
    Sovereign { 
        required_bft_threshold: usize,
        jurisdiction: GeoCell,
    },
}

/// A causal vector clock resolving concurrent events across network partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DottedVersionVector {
    pub actor_id: [u8; 32],
    pub sequence_number: u64,
}

/// The fundamental unit of execution in the Ext. Event Bus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtEvent {
    /// Blake3 hash of the human-readable topic string (e.g., "global_thermal_alerts").
    pub topic_hash: [u8; 32],
    /// The un-interpretable binary payload destined for a WASM actor.
    pub payload: Vec<u8>,
    pub causal_vector: DottedVersionVector,
    pub retention: RetentionPolicy,
    pub timestamp: u64,
}

impl ExtEvent {
    pub fn new(topic: &str, payload: Vec<u8>, actor_id: [u8; 32], seq: u64, retention: RetentionPolicy) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(topic.as_bytes());
        let topic_hash: [u8; 32] = hasher.finalize().into();

        Self {
            topic_hash,
            payload,
            causal_vector: DottedVersionVector { actor_id, sequence_number: seq },
            retention,
            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64,
        }
    }
}
