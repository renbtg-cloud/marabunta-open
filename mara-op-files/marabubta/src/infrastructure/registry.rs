// Marabunta - Licensed under the MIT License.
//! Node Registration and Discovery
//!
//! This module provides a thread-safe registry for infrastructure nodes with
//! capability-based queries, location-aware selection, and validation.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::node::{
    HardwareSpecs, InfrastructureNode, NodeId, NodeStatus, NodeValidationError, SlaTier,
};

/// Capability requirements for node selection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Capability {
    /// Minimum CPU cores required.
    Cpu { min_cores: u32 },
    /// GPU requirements.
    Gpu { min_count: u32, min_vram: u64 },
    /// High memory requirement.
    HighMemory { min_bytes: u64 },
    /// Low latency network requirement.
    LowLatency { max_ms: u32 },
    /// High storage requirement.
    HighStorage { min_bytes: u64 },
    /// High network bandwidth requirement.
    HighBandwidth { min_mbps: u32 },
}

impl Capability {
    /// Checks if the given hardware specs satisfy this capability.
    pub fn is_satisfied_by(&self, specs: &HardwareSpecs) -> bool {
        match self {
            Capability::Cpu { min_cores } => specs.cpu_cores >= *min_cores,
            Capability::Gpu {
                min_count,
                min_vram,
            } => {
                specs.gpu_count >= *min_count
                    && specs.gpu_vram_bytes.iter().all(|&v| v >= *min_vram)
            }
            Capability::HighMemory { min_bytes } => specs.ram_bytes >= *min_bytes,
            Capability::LowLatency { .. } => true, // Network latency needs runtime measurement
            Capability::HighStorage { min_bytes } => specs.storage_bytes >= *min_bytes,
            Capability::HighBandwidth { min_mbps } => specs.network_bandwidth_mbps >= *min_mbps,
        }
    }
}

/// Query criteria for finding nodes.
#[derive(Debug, Clone, Default)]
pub struct NodeQuery {
    /// Required capabilities.
    pub capabilities: Vec<Capability>,
    /// Required datacenter (exact match).
    pub datacenter: Option<String>,
    /// Required network zone (exact match).
    pub network_zone: Option<String>,
    /// Required SLA tier (minimum).
    pub min_sla_tier: Option<SlaTier>,
    /// Required tags (all must match).
    pub required_tags: Vec<String>,
    /// Exclude nodes with these tags.
    pub excluded_tags: Vec<String>,
    /// Only include nodes that can accept work.
    pub available_only: bool,
    /// Number of results to skip (for pagination).
    pub offset: Option<usize>,
    /// Maximum number of results.
    pub limit: Option<usize>,
}

impl NodeQuery {
    /// Creates a new empty query.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a capability requirement.
    pub fn with_capability(mut self, capability: Capability) -> Self {
        self.capabilities.push(capability);
        self
    }

    /// Requires a specific datacenter.
    pub fn in_datacenter(mut self, datacenter: impl Into<String>) -> Self {
        self.datacenter = Some(datacenter.into());
        self
    }

    /// Requires a specific network zone.
    pub fn in_network_zone(mut self, zone: impl Into<String>) -> Self {
        self.network_zone = Some(zone.into());
        self
    }

    /// Requires a minimum SLA tier.
    pub fn with_min_sla(mut self, tier: SlaTier) -> Self {
        self.min_sla_tier = Some(tier);
        self
    }

    /// Requires all specified tags.
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.required_tags = tags;
        self
    }

    /// Excludes nodes with any of these tags.
    pub fn without_tags(mut self, tags: Vec<String>) -> Self {
        self.excluded_tags = tags;
        self
    }

    /// Only returns nodes that can accept work.
    pub fn available_only(mut self) -> Self {
        self.available_only = true;
        self
    }

    /// Skips the specified number of results.
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Limits the number of results.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Checks if a node matches this query.
    fn matches(&self, node: &InfrastructureNode) -> bool {
        // Check availability
        if self.available_only && !node.can_accept_work() {
            return false;
        }

        // Check capabilities
        for cap in &self.capabilities {
            if !cap.is_satisfied_by(&node.specs) {
                return false;
            }
        }

        // Check datacenter
        if let Some(ref dc) = self.datacenter {
            if &node.location.datacenter != dc {
                return false;
            }
        }

        // Check network zone
        if let Some(ref zone) = self.network_zone {
            if &node.location.network_zone != zone {
                return false;
            }
        }

        // Check SLA tier
        if let Some(min_tier) = self.min_sla_tier {
            if node.sla_tier.priority() < min_tier.priority() {
                return false;
            }
        }

        // Check required tags
        for tag in &self.required_tags {
            if !node.tags.contains(tag) {
                return false;
            }
        }

        // Check excluded tags
        for tag in &self.excluded_tags {
            if node.tags.contains(tag) {
                return false;
            }
        }

        true
    }
}

/// Selection strategy for choosing among matching nodes.
#[derive(Debug, Clone, Copy, Default)]
pub enum NodeSelection {
    /// Return first matching node.
    #[default]
    First,
    /// Return random matching node.
    Random,
    /// Return least loaded node (by CPU).
    LeastLoaded,
    /// Return node with most available resources.
    MostCapacity,
    /// Return node with highest SLA tier.
    HighestSla,
    /// Round-robin selection (requires external state).
    RoundRobin,
}

/// Error types for registry operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RegistryError {
    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),
    #[error("Node already exists: {0}")]
    NodeAlreadyExists(NodeId),
    #[error("Node validation failed: {0}")]
    ValidationFailed(#[from] NodeValidationError),
    #[error("No nodes match the query")]
    NoMatchingNodes,
}

/// Registry for infrastructure nodes.
///
/// Provides thread-safe registration, lookup, and capability-based queries.
pub struct NodeRegistry {
    /// All registered nodes.
    nodes: Arc<RwLock<HashMap<NodeId, InfrastructureNode>>>,
    /// Index by datacenter.
    by_datacenter: Arc<RwLock<HashMap<String, Vec<NodeId>>>>,
    /// Index by network zone.
    by_network_zone: Arc<RwLock<HashMap<String, Vec<NodeId>>>>,
    /// Index by tag.
    by_tag: Arc<RwLock<HashMap<String, Vec<NodeId>>>>,
    /// Round-robin counter.
    round_robin_counter: Arc<RwLock<usize>>,
}

impl NodeRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            by_datacenter: Arc::new(RwLock::new(HashMap::new())),
            by_network_zone: Arc::new(RwLock::new(HashMap::new())),
            by_tag: Arc::new(RwLock::new(HashMap::new())),
            round_robin_counter: Arc::new(RwLock::new(0)),
        }
    }

    /// Registers a new node.
    ///
    /// Returns an error if a node with the same ID already exists or validation fails.
    pub async fn register(&self, node: InfrastructureNode) -> Result<(), RegistryError> {
        // Validate node
        node.validate()?;

        let node_id = node.id;

        // Check for duplicates
        {
            let nodes = self.nodes.read().await;
            if nodes.contains_key(&node_id) {
                return Err(RegistryError::NodeAlreadyExists(node_id));
            }
        }

        // Add to main index
        {
            let mut nodes = self.nodes.write().await;
            nodes.insert(node_id, node.clone());
        }

        // Add to datacenter index
        {
            let mut by_dc = self.by_datacenter.write().await;
            by_dc
                .entry(node.location.datacenter.clone())
                .or_default()
                .push(node_id);
        }

        // Add to network zone index
        {
            let mut by_zone = self.by_network_zone.write().await;
            by_zone
                .entry(node.location.network_zone.clone())
                .or_default()
                .push(node_id);
        }

        // Add to tag indexes
        {
            let mut by_tag = self.by_tag.write().await;
            for tag in &node.tags {
                by_tag.entry(tag.clone()).or_default().push(node_id);
            }
        }

        Ok(())
    }

    /// Unregisters a node.
    pub async fn unregister(&self, node_id: &NodeId) -> Result<InfrastructureNode, RegistryError> {
        // Remove from main index
        let node = {
            let mut nodes = self.nodes.write().await;
            nodes
                .remove(node_id)
                .ok_or(RegistryError::NodeNotFound(*node_id))?
        };

        // Remove from datacenter index
        {
            let mut by_dc = self.by_datacenter.write().await;
            if let Some(ids) = by_dc.get_mut(&node.location.datacenter) {
                ids.retain(|id| id != node_id);
            }
        }

        // Remove from network zone index
        {
            let mut by_zone = self.by_network_zone.write().await;
            if let Some(ids) = by_zone.get_mut(&node.location.network_zone) {
                ids.retain(|id| id != node_id);
            }
        }

        // Remove from tag indexes
        {
            let mut by_tag = self.by_tag.write().await;
            for tag in &node.tags {
                if let Some(ids) = by_tag.get_mut(tag) {
                    ids.retain(|id| id != node_id);
                }
            }
        }

        Ok(node)
    }

    /// Gets a node by ID.
    pub async fn get(&self, node_id: &NodeId) -> Option<InfrastructureNode> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    /// Updates a node's data.
    ///
    /// The update function receives a mutable reference to the node.
    pub async fn update<F>(&self, node_id: &NodeId, update_fn: F) -> Result<(), RegistryError>
    where
        F: FnOnce(&mut InfrastructureNode),
    {
        let mut nodes = self.nodes.write().await;
        let node = nodes
            .get_mut(node_id)
            .ok_or(RegistryError::NodeNotFound(*node_id))?;
        update_fn(node);
        Ok(())
    }

    /// Updates a node's status.
    pub async fn update_status(
        &self,
        node_id: &NodeId,
        status: NodeStatus,
    ) -> Result<(), RegistryError> {
        self.update(node_id, |node| node.status = status).await
    }

    /// Queries nodes matching the given criteria.
    pub async fn query(&self, query: &NodeQuery) -> Vec<InfrastructureNode> {
        let nodes = self.nodes.read().await;

        let mut iter = nodes.values().filter(|n| query.matches(n));

        if let Some(offset) = query.offset {
            if offset > 0 {
                iter.nth(offset - 1);
            }
        }

        if let Some(limit) = query.limit {
            iter.take(limit).cloned().collect()
        } else {
            iter.cloned().collect()
        }
    }

    /// Selects a single node matching the query using the specified strategy.
    pub async fn select(
        &self,
        query: &NodeQuery,
        strategy: NodeSelection,
    ) -> Result<InfrastructureNode, RegistryError> {
        let matches = self.query(query).await;

        if matches.is_empty() {
            return Err(RegistryError::NoMatchingNodes);
        }

        let selected = match strategy {
            NodeSelection::First => matches.into_iter().next(),
            NodeSelection::Random => {
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                matches.choose(&mut rng).cloned()
            }
            NodeSelection::LeastLoaded => matches.into_iter().min_by(|a, b| {
                a.metrics
                    .cpu_usage_percent
                    .partial_cmp(&b.metrics.cpu_usage_percent)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            NodeSelection::MostCapacity => matches.into_iter().max_by(|a, b| {
                let a_score = a.specs.ram_bytes as f64
                    + a.specs.cpu_cores as f64 * 1_000_000_000.0
                    + a.specs.total_gpu_vram() as f64;
                let b_score = b.specs.ram_bytes as f64
                    + b.specs.cpu_cores as f64 * 1_000_000_000.0
                    + b.specs.total_gpu_vram() as f64;
                a_score
                    .partial_cmp(&b_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            NodeSelection::HighestSla => matches.into_iter().max_by_key(|n| n.sla_tier.priority()),
            NodeSelection::RoundRobin => {
                let mut counter = self.round_robin_counter.write().await;
                let idx = *counter % matches.len();
                *counter = counter.wrapping_add(1);
                matches.into_iter().nth(idx)
            }
        };

        selected.ok_or(RegistryError::NoMatchingNodes)
    }

    /// Gets all nodes in a datacenter.
    pub async fn nodes_in_datacenter(&self, datacenter: &str) -> Vec<InfrastructureNode> {
        let by_dc = self.by_datacenter.read().await;
        let nodes = self.nodes.read().await;

        by_dc
            .get(datacenter)
            .map(|ids| ids.iter().filter_map(|id| nodes.get(id).cloned()).collect())
            .unwrap_or_default()
    }

    /// Gets all nodes in a network zone.
    pub async fn nodes_in_network_zone(&self, zone: &str) -> Vec<InfrastructureNode> {
        let by_zone = self.by_network_zone.read().await;
        let nodes = self.nodes.read().await;

        by_zone
            .get(zone)
            .map(|ids| ids.iter().filter_map(|id| nodes.get(id).cloned()).collect())
            .unwrap_or_default()
    }

    /// Gets all nodes with a specific tag.
    pub async fn nodes_with_tag(&self, tag: &str) -> Vec<InfrastructureNode> {
        let by_tag = self.by_tag.read().await;
        let nodes = self.nodes.read().await;

        by_tag
            .get(tag)
            .map(|ids| ids.iter().filter_map(|id| nodes.get(id).cloned()).collect())
            .unwrap_or_default()
    }

    /// Returns the total number of registered nodes.
    pub async fn node_count(&self) -> usize {
        let nodes = self.nodes.read().await;
        nodes.len()
    }

    /// Returns the number of online nodes.
    pub async fn online_count(&self) -> usize {
        let nodes = self.nodes.read().await;
        nodes.values().filter(|n| n.can_accept_work()).count()
    }

    /// Returns all datacenters with registered nodes.
    pub async fn list_datacenters(&self) -> Vec<String> {
        let by_dc = self.by_datacenter.read().await;
        by_dc.keys().cloned().collect()
    }

    /// Returns all network zones with registered nodes.
    pub async fn list_network_zones(&self) -> Vec<String> {
        let by_zone = self.by_network_zone.read().await;
        by_zone.keys().cloned().collect()
    }

    /// Returns all tags in use.
    pub async fn list_tags(&self) -> Vec<String> {
        let by_tag = self.by_tag.read().await;
        by_tag.keys().cloned().collect()
    }

    /// Returns aggregate capacity statistics.
    pub async fn aggregate_capacity(&self) -> CapacityStats {
        let nodes = self.nodes.read().await;

        let mut stats = CapacityStats::default();

        for node in nodes.values() {
            stats.total_nodes += 1;
            if node.can_accept_work() {
                stats.online_nodes += 1;
            }
            stats.total_cpu_cores += node.specs.cpu_cores as u64;
            stats.total_ram_bytes += node.specs.ram_bytes;
            stats.total_gpu_count += node.specs.gpu_count as u64;
            stats.total_gpu_vram_bytes += node.specs.total_gpu_vram();
            stats.total_storage_bytes += node.specs.storage_bytes;
        }

        stats
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate capacity statistics.
#[derive(Debug, Clone, Default)]
pub struct CapacityStats {
    /// Total number of registered nodes.
    pub total_nodes: u64,
    /// Number of online nodes.
    pub online_nodes: u64,
    /// Total CPU cores across all nodes.
    pub total_cpu_cores: u64,
    /// Total RAM across all nodes (bytes).
    pub total_ram_bytes: u64,
    /// Total GPU count across all nodes.
    pub total_gpu_count: u64,
    /// Total GPU VRAM across all nodes (bytes).
    pub total_gpu_vram_bytes: u64,
    /// Total storage across all nodes (bytes).
    pub total_storage_bytes: u64,
}

impl CapacityStats {
    /// Returns RAM in terabytes.
    pub fn total_ram_tb(&self) -> f64 {
        self.total_ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 1024.0)
    }

    /// Returns storage in petabytes.
    pub fn total_storage_pb(&self) -> f64 {
        self.total_storage_bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::node::{Department, Location};

    fn create_test_node(hostname: &str, datacenter: &str, cores: u32) -> InfrastructureNode {
        InfrastructureNode::new(
            hostname,
            Location::new(datacenter, "prod"),
            HardwareSpecs::cpu_only(cores, "Intel Xeon", 64 * 1024 * 1024 * 1024),
            Department::new("eng", "Engineering"),
            SlaTier::Production,
        )
    }

    #[tokio::test]
    async fn test_registry_register_and_get() {
        let registry = NodeRegistry::new();
        let node = create_test_node("server-1", "us-east-1", 16);
        let node_id = node.id;

        registry.register(node.clone()).await.unwrap();

        let retrieved = registry.get(&node_id).await.unwrap();
        assert_eq!(retrieved.hostname, "server-1");
    }

    #[tokio::test]
    async fn test_registry_duplicate_registration() {
        let registry = NodeRegistry::new();
        let node = create_test_node("server-1", "us-east-1", 16);

        registry.register(node.clone()).await.unwrap();
        let result = registry.register(node).await;

        assert!(matches!(result, Err(RegistryError::NodeAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_registry_unregister() {
        let registry = NodeRegistry::new();
        let node = create_test_node("server-1", "us-east-1", 16);
        let node_id = node.id;

        registry.register(node).await.unwrap();
        assert_eq!(registry.node_count().await, 1);

        let removed = registry.unregister(&node_id).await.unwrap();
        assert_eq!(removed.hostname, "server-1");
        assert_eq!(registry.node_count().await, 0);
    }

    #[tokio::test]
    async fn test_registry_query_by_capability() {
        let registry = NodeRegistry::new();

        // Register nodes with different specs
        let small = create_test_node("small", "us-east-1", 4);
        let medium = create_test_node("medium", "us-east-1", 16);
        let large = create_test_node("large", "us-east-1", 64);

        registry.register(small).await.unwrap();
        registry.register(medium).await.unwrap();
        registry.register(large).await.unwrap();

        // Query for nodes with at least 8 cores
        let query = NodeQuery::new().with_capability(Capability::Cpu { min_cores: 8 });
        let results = registry.query(&query).await;

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|n| n.specs.cpu_cores >= 8));
    }

    #[tokio::test]
    async fn test_registry_query_by_datacenter() {
        let registry = NodeRegistry::new();

        registry
            .register(create_test_node("us-1", "us-east-1", 16))
            .await
            .unwrap();
        registry
            .register(create_test_node("us-2", "us-east-1", 16))
            .await
            .unwrap();
        registry
            .register(create_test_node("eu-1", "eu-west-1", 16))
            .await
            .unwrap();

        let query = NodeQuery::new().in_datacenter("us-east-1");
        let results = registry.query(&query).await;

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|n| n.location.datacenter == "us-east-1"));
    }

    #[tokio::test]
    async fn test_registry_query_by_tags() {
        let registry = NodeRegistry::new();

        let mut gpu_node = create_test_node("gpu-1", "us-east-1", 16);
        gpu_node.tags = vec!["gpu".to_string(), "cuda".to_string()];

        let mut cpu_node = create_test_node("cpu-1", "us-east-1", 32);
        cpu_node.tags = vec!["high-cpu".to_string()];

        registry.register(gpu_node).await.unwrap();
        registry.register(cpu_node).await.unwrap();

        let query = NodeQuery::new().with_tags(vec!["gpu".to_string()]);
        let results = registry.query(&query).await;

        assert_eq!(results.len(), 1);
        assert!(results[0].tags.contains(&"gpu".to_string()));
    }

    #[tokio::test]
    async fn test_registry_query_available_only() {
        let registry = NodeRegistry::new();

        let mut online = create_test_node("online", "us-east-1", 16);
        online.status = NodeStatus::online();

        let offline = create_test_node("offline", "us-east-1", 16);
        // Default status is offline

        registry.register(online).await.unwrap();
        registry.register(offline).await.unwrap();

        let query = NodeQuery::new().available_only();
        let results = registry.query(&query).await;

        assert_eq!(results.len(), 1);
        assert!(results[0].can_accept_work());
    }

    #[tokio::test]
    async fn test_registry_select_least_loaded() {
        let registry = NodeRegistry::new();

        let mut node1 = create_test_node("node-1", "us-east-1", 16);
        node1.status = NodeStatus::online();
        node1.metrics.cpu_usage_percent = 80.0;

        let mut node2 = create_test_node("node-2", "us-east-1", 16);
        node2.status = NodeStatus::online();
        node2.metrics.cpu_usage_percent = 20.0;

        let mut node3 = create_test_node("node-3", "us-east-1", 16);
        node3.status = NodeStatus::online();
        node3.metrics.cpu_usage_percent = 50.0;

        registry.register(node1).await.unwrap();
        registry.register(node2).await.unwrap();
        registry.register(node3).await.unwrap();

        let query = NodeQuery::new().available_only();
        let selected = registry
            .select(&query, NodeSelection::LeastLoaded)
            .await
            .unwrap();

        assert_eq!(selected.hostname, "node-2");
    }

    #[tokio::test]
    async fn test_registry_select_highest_sla() {
        let registry = NodeRegistry::new();

        let mut standard = InfrastructureNode::new(
            "standard",
            Location::new("us-east-1", "prod"),
            HardwareSpecs::cpu_only(16, "Intel Xeon", 64 * 1024 * 1024 * 1024),
            Department::new("eng", "Engineering"),
            SlaTier::Standard,
        );
        standard.status = NodeStatus::online();

        let mut critical = InfrastructureNode::new(
            "critical",
            Location::new("us-east-1", "prod"),
            HardwareSpecs::cpu_only(16, "Intel Xeon", 64 * 1024 * 1024 * 1024),
            Department::new("eng", "Engineering"),
            SlaTier::Critical,
        );
        critical.status = NodeStatus::online();

        registry.register(standard).await.unwrap();
        registry.register(critical).await.unwrap();

        let query = NodeQuery::new().available_only();
        let selected = registry
            .select(&query, NodeSelection::HighestSla)
            .await
            .unwrap();

        assert_eq!(selected.sla_tier, SlaTier::Critical);
    }

    #[tokio::test]
    async fn test_registry_aggregate_capacity() {
        let registry = NodeRegistry::new();

        let mut node1 = create_test_node("node-1", "us-east-1", 16);
        node1.status = NodeStatus::online();

        let node2 = create_test_node("node-2", "us-east-1", 32);
        // offline

        registry.register(node1).await.unwrap();
        registry.register(node2).await.unwrap();

        let stats = registry.aggregate_capacity().await;

        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.online_nodes, 1);
        assert_eq!(stats.total_cpu_cores, 48);
    }

    #[tokio::test]
    async fn test_capability_satisfaction() {
        let specs = HardwareSpecs::cpu_only(32, "AMD EPYC", 128 * 1024 * 1024 * 1024)
            .with_gpus(
                vec!["A100".to_string(), "A100".to_string()],
                vec![40 * 1024 * 1024 * 1024, 40 * 1024 * 1024 * 1024],
            )
            .with_storage(10 * 1024 * 1024 * 1024 * 1024)
            .with_network(100_000);

        // Should satisfy
        assert!(Capability::Cpu { min_cores: 16 }.is_satisfied_by(&specs));
        assert!(Capability::Gpu {
            min_count: 2,
            min_vram: 30 * 1024 * 1024 * 1024
        }
        .is_satisfied_by(&specs));
        assert!(Capability::HighMemory {
            min_bytes: 64 * 1024 * 1024 * 1024
        }
        .is_satisfied_by(&specs));
        assert!(Capability::HighStorage {
            min_bytes: 5 * 1024 * 1024 * 1024 * 1024
        }
        .is_satisfied_by(&specs));
        assert!(Capability::HighBandwidth { min_mbps: 50_000 }.is_satisfied_by(&specs));

        // Should not satisfy
        assert!(!Capability::Cpu { min_cores: 64 }.is_satisfied_by(&specs));
        assert!(!Capability::Gpu {
            min_count: 4,
            min_vram: 40 * 1024 * 1024 * 1024
        }
        .is_satisfied_by(&specs));
    }

    #[tokio::test]
    async fn test_registry_list_functions() {
        let registry = NodeRegistry::new();

        let mut node1 = create_test_node("node-1", "us-east-1", 16);
        node1.tags = vec!["gpu".to_string()];

        let mut node2 = InfrastructureNode::new(
            "node-2",
            Location::new("eu-west-1", "staging"),
            HardwareSpecs::cpu_only(16, "Intel Xeon", 64 * 1024 * 1024 * 1024),
            Department::new("eng", "Engineering"),
            SlaTier::Production,
        );
        node2.tags = vec!["cpu".to_string()];

        registry.register(node1).await.unwrap();
        registry.register(node2).await.unwrap();

        let datacenters = registry.list_datacenters().await;
        assert_eq!(datacenters.len(), 2);

        let zones = registry.list_network_zones().await;
        assert_eq!(zones.len(), 2);

        let tags = registry.list_tags().await;
        assert_eq!(tags.len(), 2);
    }
}
