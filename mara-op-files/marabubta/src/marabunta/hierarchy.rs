// Marabunta - Licensed under the MIT License.
//! Hierarchical routing for the Marabunta protocol.
//!
//! 5-level routing: node → neighborhood → region → zone → global.
//! Routes jobs based on required capabilities and size hint.

use crate::marabunta::gossip::{CapabilityVector, GossipEntry};
use crate::marabunta::identity::NodeId;
use crate::marabunta::neighborhood::{NeighborhoodSummary, NeighborhoodId};
use serde::{Deserialize, Serialize};

/// Region summary — aggregate of multiple neighborhoods.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionSummary {
    pub region_id: u64,
    pub neighborhoods: Vec<NeighborhoodSummary>,
    pub total_nodes: usize,
    pub total_cores: u64,
    pub total_memory_mb: u64,
}

/// Zone summary — aggregate of multiple regions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneSummary {
    pub zone_id: u64,
    pub regions: Vec<RegionSummary>,
    pub total_nodes: usize,
}

/// Job size hint for routing decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobSizeHint {
    /// Fits on a single node; route locally.
    Small,
    /// Needs a few nodes; route to nearby neighborhoods.
    Medium,
    /// Needs many nodes; route regionally.
    Large,
    /// Massive scale; route across zones.
    Massive,
}

/// Per-node routing table (kept under 20KB total).
pub struct RoutingTable {
    /// Local neighborhood members.
    pub local_members: Vec<GossipEntry>,
    /// Summaries of neighboring neighborhoods.
    pub neighbor_summaries: Vec<NeighborhoodSummary>,
    /// Region-level summaries.
    pub region_summaries: Vec<RegionSummary>,
    /// Zone-level summaries.
    pub zone_summaries: Vec<ZoneSummary>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self {
            local_members: Vec::new(),
            neighbor_summaries: Vec::new(),
            region_summaries: Vec::new(),
            zone_summaries: Vec::new(),
        }
    }

    /// Estimate memory usage of the routing table.
    pub fn estimated_size_bytes(&self) -> usize {
        let local = self.local_members.len() * 128; // ~128B per entry estimate
        let neighbors = self.neighbor_summaries.len() * 64;
        let regions = self.region_summaries.len() * 48;
        let zones = self.zone_summaries.len() * 32;
        local + neighbors + regions + zones
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Hierarchical router that selects nodes for job execution.
pub struct HierarchyRouter {
    routing_table: RoutingTable,
}

impl HierarchyRouter {
    pub fn new(routing_table: RoutingTable) -> Self {
        Self { routing_table }
    }

    /// Route a job to candidate nodes based on required capabilities and size.
    pub fn route_job(
        &self,
        required_caps: &CapabilityVector,
        size_hint: JobSizeHint,
    ) -> Vec<NodeId> {
        match size_hint {
            JobSizeHint::Small => self.route_local(required_caps),
            JobSizeHint::Medium => self.route_nearby(required_caps),
            JobSizeHint::Large => self.route_regional(required_caps),
            JobSizeHint::Massive => self.route_global(required_caps),
        }
    }

    /// Route locally — 0 hops, search within local neighborhood.
    fn route_local(&self, required: &CapabilityVector) -> Vec<NodeId> {
        self.routing_table
            .local_members
            .iter()
            .filter(|e| capabilities_match(&e.capabilities, required))
            .map(|e| e.node_id)
            .collect()
    }

    /// Route to nearby neighborhoods — up to 2 hops.
    fn route_nearby(&self, required: &CapabilityVector) -> Vec<NodeId> {
        let mut results = self.route_local(required);

        // Add representatives from neighborhoods that might have capable nodes
        for summary in &self.routing_table.neighbor_summaries {
            if summary_meets_requirements(summary, required) {
                if let Some(rep) = summary.representative {
                    results.push(rep);
                }
            }
        }

        results
    }

    /// Route regionally — up to 4 hops.
    fn route_regional(&self, required: &CapabilityVector) -> Vec<NodeId> {
        let mut results = self.route_nearby(required);

        for region in &self.routing_table.region_summaries {
            if region.total_cores >= required.cores as u64 {
                for summary in &region.neighborhoods {
                    if let Some(rep) = summary.representative {
                        if !results.contains(&rep) {
                            results.push(rep);
                        }
                    }
                }
            }
        }

        results
    }

    /// Route globally — up to 6 hops.
    fn route_global(&self, required: &CapabilityVector) -> Vec<NodeId> {
        let mut results = self.route_regional(required);

        for zone in &self.routing_table.zone_summaries {
            for region in &zone.regions {
                for summary in &region.neighborhoods {
                    if let Some(rep) = summary.representative {
                        if !results.contains(&rep) {
                            results.push(rep);
                        }
                    }
                }
            }
        }

        results
    }

    /// Get the routing table reference.
    pub fn routing_table(&self) -> &RoutingTable {
        &self.routing_table
    }

    /// Update the routing table.
    pub fn update_routing_table(&mut self, table: RoutingTable) {
        self.routing_table = table;
    }
}

/// Check if a node's capabilities meet the requirements.
fn capabilities_match(node: &CapabilityVector, required: &CapabilityVector) -> bool {
    node.cores >= required.cores
        && node.memory_mb >= required.memory_mb
        && node.bandwidth_kbps >= required.bandwidth_kbps
        && (!required.gpu || node.gpu)
        && (!required.blind_capable || node.blind_capable)
}

/// Check if a neighborhood summary suggests it could have capable nodes.
fn summary_meets_requirements(
    summary: &NeighborhoodSummary,
    required: &CapabilityVector,
) -> bool {
    summary.total_cores >= required.cores as u64
        && summary.total_memory_mb >= required.memory_mb
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marabunta::gossip::LoadMetrics;
    use chrono::Utc;

    fn make_node_id(b: u8) -> NodeId {
        NodeId([b; 32])
    }

    fn make_entry(b: u8, cores: u32, memory: u64) -> GossipEntry {
        let mut caps = CapabilityVector::default();
        caps.cores = cores;
        caps.memory_mb = memory;
        GossipEntry {
            node_id: make_node_id(b),
            capabilities: caps,
            load: LoadMetrics::default(),
            last_seen: Utc::now(),
            reputation: 100,
            version: 1,
            connectivity_score: 100,
        }
    }

    fn required_caps(cores: u32, memory: u64) -> CapabilityVector {
        let mut caps = CapabilityVector::default();
        caps.cores = cores;
        caps.memory_mb = memory;
        caps
    }

    #[test]
    fn test_route_local_small() {
        let mut table = RoutingTable::new();
        table.local_members.push(make_entry(1, 4, 8192));
        table.local_members.push(make_entry(2, 2, 4096));
        table.local_members.push(make_entry(3, 1, 512));

        let router = HierarchyRouter::new(table);
        let result = router.route_job(&required_caps(2, 4096), JobSizeHint::Small);
        assert_eq!(result.len(), 2); // nodes 1 and 2 meet requirements
    }

    #[test]
    fn test_route_medium_includes_neighbors() {
        let mut table = RoutingTable::new();
        table.local_members.push(make_entry(1, 4, 8192));
        table.neighbor_summaries.push(NeighborhoodSummary {
            id: NeighborhoodId(2),
            member_count: 50,
            total_cores: 200,
            total_memory_mb: 400000,
            total_bandwidth_kbps: 100000,
            avg_cpu_percent: 30.0,
            representative: Some(make_node_id(10)),
        });

        let router = HierarchyRouter::new(table);
        let result = router.route_job(&required_caps(2, 4096), JobSizeHint::Medium);
        assert!(result.contains(&make_node_id(1)));
        assert!(result.contains(&make_node_id(10)));
    }

    #[test]
    fn test_route_no_matches() {
        let mut table = RoutingTable::new();
        table.local_members.push(make_entry(1, 1, 512));

        let router = HierarchyRouter::new(table);
        let result = router.route_job(&required_caps(8, 32768), JobSizeHint::Small);
        assert!(result.is_empty());
    }

    #[test]
    fn test_routing_table_size() {
        let mut table = RoutingTable::new();
        for i in 0..100 {
            table.local_members.push(make_entry(i, 4, 8192));
        }
        for i in 0..50 {
            table.neighbor_summaries.push(NeighborhoodSummary {
                id: NeighborhoodId(i as u64),
                member_count: 100,
                total_cores: 400,
                total_memory_mb: 800000,
                total_bandwidth_kbps: 500000,
                avg_cpu_percent: 40.0,
                representative: Some(make_node_id(i as u8)),
            });
        }
        // Should be well under 20KB
        assert!(table.estimated_size_bytes() < 20_000);
    }

    #[test]
    fn test_capabilities_match() {
        let node = CapabilityVector {
            cores: 4,
            memory_mb: 8192,
            bandwidth_kbps: 10000,
            gpu: true,
            blind_capable: true,
            ..Default::default()
        };
        let req = CapabilityVector {
            cores: 2,
            memory_mb: 4096,
            gpu: true,
            ..Default::default()
        };
        assert!(capabilities_match(&node, &req));
    }

    #[test]
    fn test_capabilities_no_gpu_mismatch() {
        let node = CapabilityVector {
            cores: 8,
            memory_mb: 16384,
            gpu: false,
            ..Default::default()
        };
        let req = CapabilityVector {
            gpu: true,
            ..Default::default()
        };
        assert!(!capabilities_match(&node, &req));
    }
}
