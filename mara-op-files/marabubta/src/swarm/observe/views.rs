// Marabunta - Licensed under the MIT License.
use serde::Serialize;

use crate::swarm::fleet::FleetManager;
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::NodeStatus;

/// Topology graph: nodes + connections.
#[derive(Debug, Clone, Serialize)]
pub struct TopologyView {
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<TopologyEdge>,
    pub total_nodes: usize,
    pub alive_nodes: usize,
    pub computed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopologyNode {
    pub id: String,
    pub status: NodeStatus,
    pub fleet_state: String,
    pub traits: Vec<String>,
    pub load: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopologyEdge {
    pub from: String,
    pub to: String,
    pub last_gossip_at: chrono::DateTime<chrono::Utc>,
}

/// Build topology view from KnowledgeStore + FleetManager.
pub fn build_topology(
    knowledge: &KnowledgeStore,
    fleet: &FleetManager,
) -> TopologyView {
    let all_nodes = knowledge.get_all_nodes();
    let mut nodes = Vec::new();
    let mut alive = 0usize;

    for node_info in &all_nodes {
        let fleet_state = fleet.fleet_store()
            .get_state(&node_info.node_id)
            .state_name()
            .to_string();

        if node_info.status == NodeStatus::Alive {
            alive += 1;
        }

        nodes.push(TopologyNode {
            id: node_info.node_id.0.to_string(),
            status: node_info.status,
            fleet_state,
            traits: node_info.traits.iter().map(|t| t.to_string()).collect(),
            load: node_info.load,
        });
    }

    let total = nodes.len();

    TopologyView {
        nodes,
        edges: Vec::new(),
        total_nodes: total,
        alive_nodes: alive,
        computed_at: chrono::Utc::now(),
    }
}

/// Resource heatmap: per-node resource utilization snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct ResourceHeatmap {
    pub entries: Vec<ResourceHeatmapEntry>,
    pub computed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceHeatmapEntry {
    pub node_id: String,
    pub cpu_pct: f32,
    pub memory_pct: f32,
    pub disk_pct: f32,
    pub active_chunks: usize,
}

pub fn build_resource_heatmap(knowledge: &KnowledgeStore) -> ResourceHeatmap {
    let all_nodes = knowledge.get_all_nodes();
    let entries: Vec<ResourceHeatmapEntry> = all_nodes.iter()
        .map(|n| {
            let cpu_pct = (1.0 - n.capacity.cpu_available) * 100.0;
            let memory_pct = if n.capacity.memory_total_mb > 0 {
                ((n.capacity.memory_total_mb - n.capacity.memory_available_mb) as f32
                    / n.capacity.memory_total_mb as f32) * 100.0
            } else {
                0.0
            };
            let disk_pct = if n.capacity.disk_total_mb > 0 {
                ((n.capacity.disk_total_mb - n.capacity.disk_available_mb) as f32
                    / n.capacity.disk_total_mb as f32) * 100.0
            } else {
                0.0
            };
            ResourceHeatmapEntry {
                node_id: n.node_id.0.to_string(),
                cpu_pct,
                memory_pct,
                disk_pct,
                active_chunks: 0,
            }
        })
        .collect();

    ResourceHeatmap {
        entries,
        computed_at: chrono::Utc::now(),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AdmissionPipeline {
    pub pending_requests: usize,
    pub probation_nodes: usize,
    pub computed_at: chrono::DateTime<chrono::Utc>,
}

pub fn build_admission_pipeline(store: &crate::swarm::admission::AdmissionStore) -> AdmissionPipeline {
    AdmissionPipeline {
        pending_requests: store.review_count(),
        probation_nodes: store.probation_count(),
        computed_at: chrono::Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_view_serializes() {
        let view = TopologyView {
            nodes: vec![],
            edges: vec![],
            total_nodes: 0,
            alive_nodes: 0,
            computed_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&view).expect("serialize");
        assert!(json.contains("total_nodes"));
    }

    #[test]
    fn test_resource_heatmap_entry() {
        let entry = ResourceHeatmapEntry {
            node_id: "test".to_string(),
            cpu_pct: 50.0,
            memory_pct: 75.0,
            disk_pct: 25.0,
            active_chunks: 3,
        };
        let json = serde_json::to_string(&entry).expect("serialize");
        assert!(json.contains("cpu_pct"));
    }

    #[test]
    fn test_admission_pipeline_serializes() {
        let pipeline = AdmissionPipeline {
            pending_requests: 2,
            probation_nodes: 1,
            computed_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&pipeline).expect("serialize");
        assert!(json.contains("pending_requests"));
    }
}
