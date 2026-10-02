// Marabunta - Licensed under the MIT License.
//! Core types for the Neuromancer subsystem.
//!
//! Defines `MarabuntaEvent` (the universal event enum), type aliases for
//! content-addressable hashes, and all supporting structs used across
//! the predator/intelligence modules.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

// Re-export the swarm's NodeId so all neuromancer modules use one type.
pub use crate::swarm::types::NodeId;

// ============================================================================
// Type aliases — bridge to the spec's [u8; 32] notation
// ============================================================================

/// Identifier for a submitted task (blake3 of task descriptor).
pub type TaskId = [u8; 32];

/// Blake3 content hash used throughout Engram, Crow, Lazarus.
pub type Blake3Hash = [u8; 32];

/// Neuromancer-native node identifier (spec uses [u8; 32]).
/// Use [`NodeId`] (Uuid-based) for internal code; this alias exists only
/// for wire-format compatibility.
pub type NmNodeId = [u8; 32];

/// Identifier for a Lazarus checkpoint.
pub type CheckpointId = [u8; 32];

/// Identifier for a Wild Dogs threat cluster.
pub type ClusterId = [u8; 32];

// ============================================================================
// NodeId ↔ NmNodeId conversion helpers
// ============================================================================

/// Convert the swarm's Uuid-based `NodeId` into a 32-byte Neuromancer id
/// by hashing the UUID bytes with blake3.
pub fn nm_node_id_from_uuid(id: &NodeId) -> NmNodeId {
    let hash = blake3::hash(id.0.as_bytes());
    *hash.as_bytes()
}

/// Convert a 32-byte Neuromancer id back into a swarm `NodeId`.
/// This is lossy (truncates to 16 bytes for a UUID); used only when an
/// approximate reverse mapping is acceptable.
pub fn uuid_from_nm_node_id(nm: &NmNodeId) -> NodeId {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&nm[..16]);
    NodeId(uuid::Uuid::from_bytes(bytes))
}

// ============================================================================
// MarabuntaEvent — the universal event enum
// ============================================================================

/// Every neuromancer module emits and subscribes to these events on the
/// [`NeuromancerBus`](super::bus::NeuromancerBus).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum MarabuntaEvent {
    // -- Task lifecycle --
    TaskSubmitted {
        id: TaskId,
        input_hash: Blake3Hash,
        requirements: ResourceRequirements,
        timestamp: SystemTime,
    },
    TaskCompleted {
        id: TaskId,
        result_hash: Blake3Hash,
        node: NodeId,
        duration_ms: u64,
        timestamp: SystemTime,
    },
    TaskFailed {
        id: TaskId,
        node: NodeId,
        reason: String,
        timestamp: SystemTime,
    },
    TaskResurrected {
        id: TaskId,
        from_checkpoint: Blake3Hash,
        new_node: NodeId,
        timestamp: SystemTime,
    },

    // -- Node lifecycle --
    NodeJoined {
        node: NodeId,
        capabilities: Vec<Capability>,
        timestamp: SystemTime,
    },
    NodeLeft {
        node: NodeId,
        reason: NodeLeaveReason,
        timestamp: SystemTime,
    },
    NodeHealthUpdate {
        node: NodeId,
        metrics: NodeMetrics,
        timestamp: SystemTime,
    },

    // -- Engram (Fossil Record) --
    FossilStored {
        hash: Blake3Hash,
        size: u64,
        provenance: ProvenanceInfo,
        timestamp: SystemTime,
    },
    FossilHit {
        task_id: TaskId,
        fossil_hash: Blake3Hash,
        timestamp: SystemTime,
    },
    FossilEvicted {
        hash: Blake3Hash,
        reason: EvictionReason,
        timestamp: SystemTime,
    },

    // -- Lazarus (Checkpoints) --
    CheckpointCreated {
        task_id: TaskId,
        checkpoint_hash: Blake3Hash,
        fragment_nodes: Vec<NodeId>,
        timestamp: SystemTime,
    },
    CheckpointRestored {
        task_id: TaskId,
        checkpoint_hash: Blake3Hash,
        timestamp: SystemTime,
    },

    // -- Security: Detection --
    AnomalyDetected {
        node: NodeId,
        score: f64,
        details: String,
        timestamp: SystemTime,
    },
    ThreatConfirmed {
        node: NodeId,
        evidence: EvidenceChain,
        timestamp: SystemTime,
    },

    // -- Security: Response --
    NodeQuarantined {
        node: NodeId,
        reason: String,
        timestamp: SystemTime,
    },
    NodeKilled {
        node: NodeId,
        evidence: EvidenceChain,
        timestamp: SystemTime,
    },
    PackHuntInitiated {
        targets: Vec<NodeId>,
        pattern: String,
        timestamp: SystemTime,
    },
    DeceptionStarted {
        node: NodeId,
        timestamp: SystemTime,
    },
    AutopsyCompleted {
        node: NodeId,
        findings: AutopsyReport,
        timestamp: SystemTime,
    },

    // -- Capability --
    CapabilityDetected {
        node: NodeId,
        capability: Capability,
        timestamp: SystemTime,
    },
    CapabilityLost {
        node: NodeId,
        capability: Capability,
        timestamp: SystemTime,
    },

    // -- Ghost (Phantom Assembly) --
    PhantomAssembled {
        phantom_id: TaskId,
        contributing_nodes: Vec<NodeId>,
        total_resources: ResourceRequirements,
        timestamp: SystemTime,
    },
    PhantomDissolved {
        phantom_id: TaskId,
        timestamp: SystemTime,
    },

    // -- Sandman (Speculative Dreaming) --
    DreamStarted {
        task_id: TaskId,
        reason: String,
        timestamp: SystemTime,
    },
    DreamCompleted {
        task_id: TaskId,
        result_hash: Blake3Hash,
        timestamp: SystemTime,
    },
    DreamYielded {
        task_id: TaskId,
        reason: String,
        timestamp: SystemTime,
    },

    // -- Darwin (Auto-Remediation) --
    PanicCaptured {
        node: NodeId,
        snapshot: crate::marabunta::sandbox::CrashSnapshot,
        timestamp: SystemTime,
    },
}

impl MarabuntaEvent {
    /// Return the string name of this event variant (for Crow indexing).
    pub fn type_name(&self) -> &str {
        match self {
            Self::TaskSubmitted { .. } => "TaskSubmitted",
            Self::TaskCompleted { .. } => "TaskCompleted",
            Self::TaskFailed { .. } => "TaskFailed",
            Self::TaskResurrected { .. } => "TaskResurrected",
            Self::NodeJoined { .. } => "NodeJoined",
            Self::NodeLeft { .. } => "NodeLeft",
            Self::NodeHealthUpdate { .. } => "NodeHealthUpdate",
            Self::FossilStored { .. } => "FossilStored",
            Self::FossilHit { .. } => "FossilHit",
            Self::FossilEvicted { .. } => "FossilEvicted",
            Self::CheckpointCreated { .. } => "CheckpointCreated",
            Self::CheckpointRestored { .. } => "CheckpointRestored",
            Self::AnomalyDetected { .. } => "AnomalyDetected",
            Self::ThreatConfirmed { .. } => "ThreatConfirmed",
            Self::NodeQuarantined { .. } => "NodeQuarantined",
            Self::NodeKilled { .. } => "NodeKilled",
            Self::PackHuntInitiated { .. } => "PackHuntInitiated",
            Self::DeceptionStarted { .. } => "DeceptionStarted",
            Self::AutopsyCompleted { .. } => "AutopsyCompleted",
            Self::CapabilityDetected { .. } => "CapabilityDetected",
            Self::CapabilityLost { .. } => "CapabilityLost",
            Self::PhantomAssembled { .. } => "PhantomAssembled",
            Self::PhantomDissolved { .. } => "PhantomDissolved",
            Self::DreamStarted { .. } => "DreamStarted",
            Self::DreamCompleted { .. } => "DreamCompleted",
            Self::DreamYielded { .. } => "DreamYielded",
            Self::PanicCaptured { .. } => "PanicCaptured",
        }
    }

    /// Extract the `TaskId` from variants that carry one.
    pub fn task_id(&self) -> Option<TaskId> {
        match self {
            Self::TaskSubmitted { id, .. }
            | Self::TaskCompleted { id, .. }
            | Self::TaskFailed { id, .. }
            | Self::TaskResurrected { id, .. } => Some(*id),
            Self::FossilHit { task_id, .. }
            | Self::CheckpointCreated { task_id, .. }
            | Self::CheckpointRestored { task_id, .. }
            | Self::DreamStarted { task_id, .. }
            | Self::DreamCompleted { task_id, .. }
            | Self::DreamYielded { task_id, .. } => Some(*task_id),
            Self::PhantomAssembled { phantom_id, .. }
            | Self::PhantomDissolved { phantom_id, .. } => Some(*phantom_id),
            _ => None,
        }
    }

    /// Extract the target node from security/lifecycle events.
    pub fn target_node(&self) -> Option<NodeId> {
        match self {
            Self::AnomalyDetected { node, .. }
            | Self::ThreatConfirmed { node, .. }
            | Self::NodeQuarantined { node, .. }
            | Self::NodeKilled { node, .. }
            | Self::DeceptionStarted { node, .. }
            | Self::AutopsyCompleted { node, .. }
            | Self::NodeJoined { node, .. }
            | Self::NodeLeft { node, .. }
            | Self::NodeHealthUpdate { node, .. }
            | Self::CapabilityDetected { node, .. }
            | Self::CapabilityLost { node, .. }
            | Self::PanicCaptured { node, .. } => Some(*node),
            Self::TaskCompleted { node, .. }
            | Self::TaskFailed { node, .. }
            | Self::TaskResurrected { new_node: node, .. } => Some(*node),
            _ => None,
        }
    }

    /// Extract the timestamp from any event variant.
    pub fn timestamp(&self) -> SystemTime {
        match self {
            Self::TaskSubmitted { timestamp, .. }
            | Self::TaskCompleted { timestamp, .. }
            | Self::TaskFailed { timestamp, .. }
            | Self::TaskResurrected { timestamp, .. }
            | Self::NodeJoined { timestamp, .. }
            | Self::NodeLeft { timestamp, .. }
            | Self::NodeHealthUpdate { timestamp, .. }
            | Self::FossilStored { timestamp, .. }
            | Self::FossilHit { timestamp, .. }
            | Self::FossilEvicted { timestamp, .. }
            | Self::CheckpointCreated { timestamp, .. }
            | Self::CheckpointRestored { timestamp, .. }
            | Self::AnomalyDetected { timestamp, .. }
            | Self::ThreatConfirmed { timestamp, .. }
            | Self::NodeQuarantined { timestamp, .. }
            | Self::NodeKilled { timestamp, .. }
            | Self::PackHuntInitiated { timestamp, .. }
            | Self::DeceptionStarted { timestamp, .. }
            | Self::AutopsyCompleted { timestamp, .. }
            | Self::CapabilityDetected { timestamp, .. }
            | Self::CapabilityLost { timestamp, .. }
            | Self::PhantomAssembled { timestamp, .. }
            | Self::PhantomDissolved { timestamp, .. }
            | Self::DreamStarted { timestamp, .. }
            | Self::DreamCompleted { timestamp, .. }
            | Self::DreamYielded { timestamp, .. }
            | Self::PanicCaptured { timestamp, .. } => *timestamp,
        }
    }
}

// ============================================================================
// Supporting types
// ============================================================================

/// Resource requirements for task scheduling and phantom assembly.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceRequirements {
    pub min_cpu_cores: u32,
    pub min_memory_mb: u64,
    pub min_storage_mb: u64,
    pub gpu_required: bool,
    pub capabilities_required: Vec<Capability>,
}

/// Hardware / software capability a node may possess.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Capability {
    CpuX86_64,
    CpuArm64,
    GpuCuda { vram_mb: u64 },
    GpuOpenCL,
    Tpu,
    Fpga,
    HighMemory { mb: u64 },
    HighStorage { mb: u64 },
    HighBandwidth { mbps: u64 },
    Custom(String),
}

/// Real-time node metrics collected by Spider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub cpu_usage_percent: f64,
    pub memory_usage_percent: f64,
    pub disk_usage_percent: f64,
    pub disk_io_read_bytes_sec: u64,
    pub disk_io_write_bytes_sec: u64,
    pub network_rx_bytes_sec: u64,
    pub network_tx_bytes_sec: u64,
    pub open_file_descriptors: u64,
    pub active_tasks: u32,
    pub temperature_celsius: Option<f64>,
    pub fan_speed_rpm: Option<u64>,
    pub uptime_seconds: u64,
    pub timestamp: SystemTime,
}

/// Chain of evidence for threat confirmation / autopsy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceChain {
    pub events: Vec<EvidenceItem>,
}

impl EvidenceChain {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn push(&mut self, item: EvidenceItem) {
        self.events.push(item);
    }
}

impl Default for EvidenceChain {
    fn default() -> Self {
        Self::new()
    }
}

/// A single piece of evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceItem {
    pub event_type: String,
    pub details: String,
    pub timestamp: SystemTime,
    pub confidence: f64,
}

/// Provenance of a fossil (who computed it, from what, how long).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenanceInfo {
    pub task_id: TaskId,
    pub node: NodeId,
    pub input_hash: Blake3Hash,
    pub computation_type: String,
    pub computation_duration_ms: u64,
}

/// Autopsy findings after a node kill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutopsyReport {
    pub attack_vector: String,
    pub affected_tasks: Vec<TaskId>,
    pub affected_fossils: Vec<Blake3Hash>,
    pub behavioral_signature: Vec<f64>,
    pub recommendations: Vec<String>,
}

/// Why a node left the swarm.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeLeaveReason {
    Graceful,
    HeartbeatTimeout,
    Killed,
    NetworkPartition,
}

/// Why a fossil was evicted from the Engram cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionReason {
    StoragePressure,
    Untrusted { killed_node: NodeId },
    Expired,
}

/// Trust status of a fossil record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TrustStatus {
    Trusted,
    Untrusted { reason: String },
    Purged,
}

impl TrustStatus {
    pub fn is_trusted(&self) -> bool {
        matches!(self, Self::Trusted)
    }
}

impl std::fmt::Display for TrustStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trusted => write!(f, "trusted"),
            Self::Untrusted { reason } => write!(f, "untrusted:{}", reason),
            Self::Purged => write!(f, "purged"),
        }
    }
}

impl std::str::FromStr for TrustStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "trusted" {
            Ok(Self::Trusted)
        } else if s == "purged" {
            Ok(Self::Purged)
        } else if let Some(reason) = s.strip_prefix("untrusted:") {
            Ok(Self::Untrusted {
                reason: reason.to_string(),
            })
        } else {
            Err(format!("invalid trust status: {}", s))
        }
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_marabunta_event_serde_roundtrip() {
        let event = MarabuntaEvent::TaskSubmitted {
            id: [1u8; 32],
            input_hash: [2u8; 32],
            requirements: ResourceRequirements::default(),
            timestamp: SystemTime::now(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: MarabuntaEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.type_name(), "TaskSubmitted");
    }

    #[test]
    fn test_resource_requirements_serde_roundtrip() {
        let rr = ResourceRequirements {
            min_cpu_cores: 4,
            min_memory_mb: 8192,
            min_storage_mb: 1024,
            gpu_required: true,
            capabilities_required: vec![Capability::GpuCuda { vram_mb: 8192 }],
        };
        let json = serde_json::to_string(&rr).unwrap();
        let back: ResourceRequirements = serde_json::from_str(&json).unwrap();
        assert_eq!(back.min_cpu_cores, 4);
        assert!(back.gpu_required);
    }

    #[test]
    fn test_capability_serde_roundtrip() {
        let caps = vec![
            Capability::CpuX86_64,
            Capability::GpuCuda { vram_mb: 16384 },
            Capability::Custom("quantum-sim".into()),
        ];
        let json = serde_json::to_string(&caps).unwrap();
        let back: Vec<Capability> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.len(), 3);
    }

    #[test]
    fn test_type_name_all_variants() {
        let now = SystemTime::now();
        let node = NodeId::new();
        let events = vec![
            MarabuntaEvent::TaskSubmitted {
                id: [0; 32], input_hash: [0; 32],
                requirements: ResourceRequirements::default(), timestamp: now,
            },
            MarabuntaEvent::TaskCompleted {
                id: [0; 32], result_hash: [0; 32], node,
                duration_ms: 100, timestamp: now,
            },
            MarabuntaEvent::AnomalyDetected {
                node, score: 3.5, details: "test".into(), timestamp: now,
            },
            MarabuntaEvent::PhantomAssembled {
                phantom_id: [0; 32], contributing_nodes: vec![node],
                total_resources: ResourceRequirements::default(), timestamp: now,
            },
            MarabuntaEvent::DreamYielded {
                task_id: [0; 32], reason: "idle".into(), timestamp: now,
            },
        ];
        for event in &events {
            assert!(!event.type_name().is_empty());
        }
    }

    #[test]
    fn test_task_id_extraction() {
        let now = SystemTime::now();
        let tid = [42u8; 32];
        let event = MarabuntaEvent::TaskSubmitted {
            id: tid,
            input_hash: [0; 32],
            requirements: ResourceRequirements::default(),
            timestamp: now,
        };
        assert_eq!(event.task_id(), Some(tid));

        let event2 = MarabuntaEvent::NodeJoined {
            node: NodeId::new(),
            capabilities: vec![],
            timestamp: now,
        };
        assert_eq!(event2.task_id(), None);
    }

    #[test]
    fn test_target_node_extraction() {
        let now = SystemTime::now();
        let node = NodeId::new();
        let event = MarabuntaEvent::AnomalyDetected {
            node,
            score: 5.0,
            details: "spike".into(),
            timestamp: now,
        };
        assert_eq!(event.target_node(), Some(node));

        let event2 = MarabuntaEvent::FossilStored {
            hash: [0; 32],
            size: 1024,
            provenance: ProvenanceInfo {
                task_id: [0; 32],
                node,
                input_hash: [0; 32],
                computation_type: "test".into(),
                computation_duration_ms: 100,
            },
            timestamp: now,
        };
        assert_eq!(event2.target_node(), None);
    }

    #[test]
    fn test_nm_node_id_conversion() {
        let node = NodeId::new();
        let nm = nm_node_id_from_uuid(&node);
        assert_ne!(nm, [0u8; 32]);
        let back = uuid_from_nm_node_id(&nm);
        // Not a lossless roundtrip, but should produce a valid NodeId
        assert_ne!(back.0, uuid::Uuid::nil());
    }

    #[test]
    fn test_trust_status_display_parse() {
        let trusted = TrustStatus::Trusted;
        assert_eq!(trusted.to_string(), "trusted");
        assert_eq!("trusted".parse::<TrustStatus>().unwrap(), TrustStatus::Trusted);

        let untrusted = TrustStatus::Untrusted { reason: "killed".into() };
        assert_eq!(untrusted.to_string(), "untrusted:killed");
        assert_eq!(
            "untrusted:killed".parse::<TrustStatus>().unwrap(),
            TrustStatus::Untrusted { reason: "killed".into() }
        );

        let purged = TrustStatus::Purged;
        assert_eq!(purged.to_string(), "purged");
        assert_eq!("purged".parse::<TrustStatus>().unwrap(), TrustStatus::Purged);
    }

    #[test]
    fn test_evidence_chain() {
        let mut chain = EvidenceChain::new();
        assert!(chain.events.is_empty());
        chain.push(EvidenceItem {
            event_type: "AnomalyDetected".into(),
            details: "high cpu".into(),
            timestamp: SystemTime::now(),
            confidence: 0.9,
        });
        chain.push(EvidenceItem {
            event_type: "HoneypotFailed".into(),
            details: "wrong hash".into(),
            timestamp: SystemTime::now(),
            confidence: 1.0,
        });
        assert_eq!(chain.events.len(), 2);
    }
}

#[derive(Debug, Clone)]
pub enum Anomaly {
    Mock,
}

#[async_trait::async_trait]
pub trait Predator: Send + Sync {
    fn name(&self) -> &'static str;
    async fn analyze(&self) -> Result<Option<Anomaly>, NeuromancerError>;
}

#[derive(Debug, thiserror::Error)]
pub enum NeuromancerError {
    #[error("Mock")]
    Mock,
    #[error("Internal error: {0}")]
    Internal(String),
}
