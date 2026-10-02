// Marabunta - Licensed under the MIT License.
use std::sync::Arc;
use tracing::{debug, warn};

use crate::swarm::complexity::{ConcernDomain, ComplexityHint, EventSeverity};
use crate::swarm::events::{EventBus, SwarmEvent};
use crate::swarm::neuromancer::bus::NeuromancerBus;
use crate::swarm::neuromancer::types::MarabuntaEvent;

/// Spawn a background task that reads MarabuntaEvents from the NeuromancerBus
/// and emits corresponding SwarmEvents on the EventBus.
pub fn spawn_neuromancer_bridge(
    neuromancer_bus: Arc<NeuromancerBus>,
    event_bus: Arc<EventBus>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = neuromancer_bus.subscribe();
        loop {
            match rx.recv().await {
                Ok(marabunta_event) => {
                    let swarm_event = convert_marabunta_to_swarm(&marabunta_event);
                    event_bus.emit(swarm_event);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(lagged = n, "neuromancer bridge lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    debug!("neuromancer bus closed, bridge exiting");
                    break;
                }
            }
        }
    })
}

/// Format a [u8; 32] hash as hex string for display.
fn hex_hash(h: &[u8; 32]) -> String {
    hex::encode(h)
}

/// Convert a MarabuntaEvent to a SwarmEvent.
fn convert_marabunta_to_swarm(event: &MarabuntaEvent) -> SwarmEvent {
    let (summary, severity, complexity, details) = match event {
        MarabuntaEvent::TaskSubmitted { id, input_hash, .. } => (
            format!("Task submitted: {}", hex_hash(id)),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"task_id": hex_hash(id), "input_hash": hex_hash(input_hash)}),
        ),
        MarabuntaEvent::TaskCompleted { id, node, duration_ms, .. } => (
            format!("Task completed: {} on {} ({}ms)", hex_hash(id), node, duration_ms),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"task_id": hex_hash(id), "node": node.to_string(), "duration_ms": duration_ms}),
        ),
        MarabuntaEvent::TaskFailed { id, node, reason, .. } => (
            format!("Task failed: {} on {}: {}", hex_hash(id), node, reason),
            EventSeverity::Warning,
            ComplexityHint::Moderate,
            serde_json::json!({"task_id": hex_hash(id), "node": node.to_string(), "reason": reason}),
        ),
        MarabuntaEvent::TaskResurrected { id, new_node, .. } => (
            format!("Task resurrected: {} on {}", hex_hash(id), new_node),
            EventSeverity::Info,
            ComplexityHint::Moderate,
            serde_json::json!({"task_id": hex_hash(id), "new_node": new_node.to_string()}),
        ),
        MarabuntaEvent::NodeJoined { node, capabilities, .. } => (
            format!("Node joined: {} ({} capabilities)", node, capabilities.len()),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"node": node.to_string(), "capability_count": capabilities.len()}),
        ),
        MarabuntaEvent::NodeLeft { node, reason, .. } => (
            format!("Node left: {} ({:?})", node, reason),
            EventSeverity::Notice,
            ComplexityHint::Simple,
            serde_json::json!({"node": node.to_string(), "reason": format!("{:?}", reason)}),
        ),
        MarabuntaEvent::NodeHealthUpdate { node, .. } => (
            format!("Node health update: {}", node),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"node": node.to_string()}),
        ),
        MarabuntaEvent::FossilStored { hash, size, .. } => (
            format!("Fossil stored: {} ({} bytes)", hex_hash(hash), size),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"hash": hex_hash(hash), "size": size}),
        ),
        MarabuntaEvent::FossilHit { task_id, fossil_hash, .. } => (
            format!("Fossil cache hit: {} -> {}", hex_hash(task_id), hex_hash(fossil_hash)),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"task_id": hex_hash(task_id), "fossil_hash": hex_hash(fossil_hash)}),
        ),
        MarabuntaEvent::FossilEvicted { hash, reason, .. } => (
            format!("Fossil evicted: {} ({:?})", hex_hash(hash), reason),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"hash": hex_hash(hash), "reason": format!("{:?}", reason)}),
        ),
        MarabuntaEvent::CheckpointCreated { task_id, checkpoint_hash, fragment_nodes, .. } => (
            format!("Checkpoint created for {} ({} fragments)", hex_hash(task_id), fragment_nodes.len()),
            EventSeverity::Info,
            ComplexityHint::Moderate,
            serde_json::json!({"task_id": hex_hash(task_id), "checkpoint_hash": hex_hash(checkpoint_hash), "fragment_count": fragment_nodes.len()}),
        ),
        MarabuntaEvent::CheckpointRestored { task_id, checkpoint_hash, .. } => (
            format!("Checkpoint restored for {}", hex_hash(task_id)),
            EventSeverity::Info,
            ComplexityHint::Moderate,
            serde_json::json!({"task_id": hex_hash(task_id), "checkpoint_hash": hex_hash(checkpoint_hash)}),
        ),
        MarabuntaEvent::AnomalyDetected { node, score, details: d, .. } => (
            format!("Anomaly detected on {} (score: {:.2})", node, score),
            EventSeverity::Warning,
            ComplexityHint::Detailed,
            serde_json::json!({"node": node.to_string(), "score": score, "details": d}),
        ),
        MarabuntaEvent::ThreatConfirmed { node, .. } => (
            format!("Threat confirmed on {}", node),
            EventSeverity::Critical,
            ComplexityHint::Detailed,
            serde_json::json!({"node": node.to_string()}),
        ),
        MarabuntaEvent::NodeQuarantined { node, reason, .. } => (
            format!("Node quarantined: {} ({})", node, reason),
            EventSeverity::Warning,
            ComplexityHint::Moderate,
            serde_json::json!({"node": node.to_string(), "reason": reason}),
        ),
        MarabuntaEvent::NodeKilled { node, .. } => (
            format!("Node killed: {}", node),
            EventSeverity::Critical,
            ComplexityHint::Detailed,
            serde_json::json!({"node": node.to_string()}),
        ),
        MarabuntaEvent::PackHuntInitiated { targets, pattern, .. } => (
            format!("Pack hunt initiated ({} targets, pattern: {})", targets.len(), pattern),
            EventSeverity::Warning,
            ComplexityHint::Detailed,
            serde_json::json!({"target_count": targets.len(), "pattern": pattern}),
        ),
        MarabuntaEvent::DeceptionStarted { node, .. } => (
            format!("Deception started on {}", node),
            EventSeverity::Warning,
            ComplexityHint::Detailed,
            serde_json::json!({"node": node.to_string()}),
        ),
        MarabuntaEvent::AutopsyCompleted { node, .. } => (
            format!("Autopsy completed for {}", node),
            EventSeverity::Info,
            ComplexityHint::Detailed,
            serde_json::json!({"node": node.to_string()}),
        ),
        MarabuntaEvent::CapabilityDetected { node, capability, .. } => (
            format!("Capability detected on {}: {:?}", node, capability),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"node": node.to_string(), "capability": format!("{:?}", capability)}),
        ),
        MarabuntaEvent::CapabilityLost { node, capability, .. } => (
            format!("Capability lost on {}: {:?}", node, capability),
            EventSeverity::Notice,
            ComplexityHint::Simple,
            serde_json::json!({"node": node.to_string(), "capability": format!("{:?}", capability)}),
        ),
        MarabuntaEvent::PhantomAssembled { phantom_id, contributing_nodes, .. } => (
            format!("Phantom assembled: {} ({} nodes)", hex_hash(phantom_id), contributing_nodes.len()),
            EventSeverity::Info,
            ComplexityHint::Moderate,
            serde_json::json!({"phantom_id": hex_hash(phantom_id), "node_count": contributing_nodes.len()}),
        ),
        MarabuntaEvent::PhantomDissolved { phantom_id, .. } => (
            format!("Phantom dissolved: {}", hex_hash(phantom_id)),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"phantom_id": hex_hash(phantom_id)}),
        ),
        MarabuntaEvent::DreamStarted { task_id, reason, .. } => (
            format!("Dream started for {}: {}", hex_hash(task_id), reason),
            EventSeverity::Info,
            ComplexityHint::Moderate,
            serde_json::json!({"task_id": hex_hash(task_id), "reason": reason}),
        ),
        MarabuntaEvent::DreamCompleted { task_id, result_hash, .. } => (
            format!("Dream completed for {}", hex_hash(task_id)),
            EventSeverity::Info,
            ComplexityHint::Moderate,
            serde_json::json!({"task_id": hex_hash(task_id), "result_hash": hex_hash(result_hash)}),
        ),
        MarabuntaEvent::DreamYielded { task_id, reason, .. } => (
            format!("Dream yielded for {}: {}", hex_hash(task_id), reason),
            EventSeverity::Info,
            ComplexityHint::Simple,
            serde_json::json!({"task_id": hex_hash(task_id), "reason": reason}),
        ),
        MarabuntaEvent::PanicCaptured { node, .. } => (
            format!("Panic captured on node {}", node),
            EventSeverity::Critical,
            ComplexityHint::Detailed,
            serde_json::json!({"node": node.to_string(), "action": "Darwin auto-remediation triggered"}),
        ),
    };

    SwarmEvent {
        id: 0,
        timestamp: chrono::Utc::now(),
        domain: ConcernDomain::Security,
        severity,
        complexity,
        summary,
        details,
        related_entities: Vec::new(),
        suggested_actions: Vec::new(),
        source_node: None,
        correlation_id: None,
        supersedes: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::neuromancer::types as nt;

    fn sample_task_id() -> nt::TaskId {
        [0u8; 32]
    }

    fn sample_hash() -> nt::Blake3Hash {
        [1u8; 32]
    }

    fn sample_node() -> nt::NodeId {
        nt::NodeId::new()
    }

    fn sample_metrics() -> nt::NodeMetrics {
        nt::NodeMetrics {
            cpu_usage_percent: 50.0,
            memory_usage_percent: 40.0,
            disk_usage_percent: 30.0,
            disk_io_read_bytes_sec: 0,
            disk_io_write_bytes_sec: 0,
            network_rx_bytes_sec: 0,
            network_tx_bytes_sec: 0,
            open_file_descriptors: 10,
            active_tasks: 1,
            temperature_celsius: None,
            fan_speed_rpm: None,
            uptime_seconds: 3600,
            timestamp: std::time::SystemTime::now(),
        }
    }

    fn sample_provenance() -> nt::ProvenanceInfo {
        nt::ProvenanceInfo {
            task_id: sample_task_id(),
            node: sample_node(),
            input_hash: sample_hash(),
            computation_type: "test".to_string(),
            computation_duration_ms: 100,
        }
    }

    fn sample_autopsy() -> nt::AutopsyReport {
        nt::AutopsyReport {
            attack_vector: "none".to_string(),
            affected_tasks: vec![],
            affected_fossils: vec![],
            behavioral_signature: vec![],
            recommendations: vec![],
        }
    }

    #[test]
    fn test_convert_marabunta_to_swarm_all_variants() {
        use std::time::SystemTime;
        let node = sample_node();
        let task_id = sample_task_id();
        let hash = sample_hash();
        let now = SystemTime::now();

        let events: Vec<MarabuntaEvent> = vec![
            MarabuntaEvent::TaskSubmitted { id: task_id, input_hash: hash, requirements: nt::ResourceRequirements::default(), timestamp: now },
            MarabuntaEvent::TaskCompleted { id: task_id, result_hash: hash, node: node.clone(), duration_ms: 100, timestamp: now },
            MarabuntaEvent::TaskFailed { id: task_id, node: node.clone(), reason: "timeout".into(), timestamp: now },
            MarabuntaEvent::TaskResurrected { id: task_id, from_checkpoint: hash, new_node: node.clone(), timestamp: now },
            MarabuntaEvent::NodeJoined { node: node.clone(), capabilities: vec![], timestamp: now },
            MarabuntaEvent::NodeLeft { node: node.clone(), reason: nt::NodeLeaveReason::Graceful, timestamp: now },
            MarabuntaEvent::NodeHealthUpdate { node: node.clone(), metrics: sample_metrics(), timestamp: now },
            MarabuntaEvent::FossilStored { hash, size: 1024, provenance: sample_provenance(), timestamp: now },
            MarabuntaEvent::FossilHit { task_id, fossil_hash: hash, timestamp: now },
            MarabuntaEvent::FossilEvicted { hash, reason: nt::EvictionReason::StoragePressure, timestamp: now },
            MarabuntaEvent::CheckpointCreated { task_id, checkpoint_hash: hash, fragment_nodes: vec![node.clone()], timestamp: now },
            MarabuntaEvent::CheckpointRestored { task_id, checkpoint_hash: hash, timestamp: now },
            MarabuntaEvent::AnomalyDetected { node: node.clone(), score: 0.9, details: "suspicious".into(), timestamp: now },
            MarabuntaEvent::ThreatConfirmed { node: node.clone(), evidence: nt::EvidenceChain::new(), timestamp: now },
            MarabuntaEvent::NodeQuarantined { node: node.clone(), reason: "anomaly".into(), timestamp: now },
            MarabuntaEvent::NodeKilled { node: node.clone(), evidence: nt::EvidenceChain::new(), timestamp: now },
            MarabuntaEvent::PackHuntInitiated { targets: vec![node.clone()], pattern: "lateral".into(), timestamp: now },
            MarabuntaEvent::DeceptionStarted { node: node.clone(), timestamp: now },
            MarabuntaEvent::AutopsyCompleted { node: node.clone(), findings: sample_autopsy(), timestamp: now },
            MarabuntaEvent::CapabilityDetected { node: node.clone(), capability: nt::Capability::CpuX86_64, timestamp: now },
            MarabuntaEvent::CapabilityLost { node: node.clone(), capability: nt::Capability::CpuArm64, timestamp: now },
            MarabuntaEvent::PhantomAssembled { phantom_id: task_id, contributing_nodes: vec![node.clone()], total_resources: nt::ResourceRequirements::default(), timestamp: now },
            MarabuntaEvent::PhantomDissolved { phantom_id: task_id, timestamp: now },
            MarabuntaEvent::DreamStarted { task_id, reason: "speculation".into(), timestamp: now },
            MarabuntaEvent::DreamCompleted { task_id, result_hash: hash, timestamp: now },
            MarabuntaEvent::DreamYielded { task_id, reason: "low priority".into(), timestamp: now },
        ];

        for ce in &events {
            let se = convert_marabunta_to_swarm(ce);
            assert_eq!(se.domain, ConcernDomain::Security);
            assert!(!se.summary.is_empty());
        }
    }

    #[test]
    fn test_threat_confirmed_is_critical() {
        let ce = MarabuntaEvent::ThreatConfirmed {
            node: sample_node(),
            evidence: nt::EvidenceChain::new(),
            timestamp: std::time::SystemTime::now(),
        };
        let se = convert_marabunta_to_swarm(&ce);
        assert_eq!(se.severity, EventSeverity::Critical);
    }

    #[test]
    fn test_anomaly_detected_is_warning() {
        let ce = MarabuntaEvent::AnomalyDetected {
            node: sample_node(),
            score: 0.95,
            details: "high score".into(),
            timestamp: std::time::SystemTime::now(),
        };
        let se = convert_marabunta_to_swarm(&ce);
        assert_eq!(se.severity, EventSeverity::Warning);
    }
}
