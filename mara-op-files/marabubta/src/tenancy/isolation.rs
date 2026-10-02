// Marabunta - Licensed under the MIT License.
//! Tenant Isolation
//!
//! This module provides isolation mechanisms for multi-tenancy including
//! resource isolation, network isolation, and data isolation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::types::TenantId;

/// Resource isolation mode for a tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ResourceIsolationMode {
    /// Shared resources with other tenants (default)
    #[default]
    Shared,
    /// Dedicated resources exclusively for this tenant
    Dedicated,
    /// Hybrid: some dedicated, some shared
    Hybrid,
}


/// Node affinity for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct TenantNodeAffinity {
    /// Preferred node tags
    pub preferred_tags: HashMap<String, String>,
    /// Required node tags (hard requirement)
    pub required_tags: HashMap<String, String>,
    /// Anti-affinity tags (avoid these nodes)
    pub anti_affinity_tags: HashMap<String, String>,
    /// Specific node IDs dedicated to this tenant
    pub dedicated_nodes: HashSet<String>,
    /// Node IDs to exclude
    pub excluded_nodes: HashSet<String>,
}


impl TenantNodeAffinity {
    /// Adds a preferred tag.
    pub fn with_preferred_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.preferred_tags.insert(key.into(), value.into());
        self
    }

    /// Adds a required tag.
    pub fn with_required_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.required_tags.insert(key.into(), value.into());
        self
    }

    /// Adds an anti-affinity tag.
    pub fn with_anti_affinity_tag(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.anti_affinity_tags.insert(key.into(), value.into());
        self
    }

    /// Adds a dedicated node.
    pub fn with_dedicated_node(mut self, node_id: impl Into<String>) -> Self {
        self.dedicated_nodes.insert(node_id.into());
        self
    }

    /// Excludes a node.
    pub fn with_excluded_node(mut self, node_id: impl Into<String>) -> Self {
        self.excluded_nodes.insert(node_id.into());
        self
    }

    /// Checks if a node matches the affinity rules.
    pub fn matches_node(&self, node_id: &str, node_tags: &HashMap<String, String>) -> NodeMatch {
        // Check exclusions first
        if self.excluded_nodes.contains(node_id) {
            return NodeMatch::Excluded("Node explicitly excluded".to_string());
        }

        // Check dedicated nodes
        if !self.dedicated_nodes.is_empty() && !self.dedicated_nodes.contains(node_id) {
            return NodeMatch::Excluded("Node not in dedicated set".to_string());
        }

        // Check anti-affinity
        for (key, value) in &self.anti_affinity_tags {
            if node_tags.get(key) == Some(value) {
                return NodeMatch::Excluded(format!(
                    "Anti-affinity tag matched: {}={}",
                    key, value
                ));
            }
        }

        // Check required tags
        for (key, value) in &self.required_tags {
            match node_tags.get(key) {
                Some(v) if v == value => {}
                _ => {
                    return NodeMatch::Excluded(format!(
                        "Required tag not matched: {}={}",
                        key, value
                    ))
                }
            }
        }

        // Calculate preference score
        let mut score: f64 = 0.5; // Base score
        for (key, value) in &self.preferred_tags {
            if node_tags.get(key) == Some(value) {
                score += 0.1; // Bonus for each matching preferred tag
            }
        }

        NodeMatch::Allowed {
            score: score.min(1.0),
        }
    }
}

/// Result of node matching for tenant affinity.
#[derive(Debug, Clone)]
pub enum NodeMatch {
    /// Node is allowed with a preference score (0.0 - 1.0)
    Allowed { score: f64 },
    /// Node is excluded with a reason
    Excluded(String),
}

impl NodeMatch {
    /// Returns true if the node is allowed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, NodeMatch::Allowed { .. })
    }

    /// Returns the score if allowed, or 0.0 if excluded.
    pub fn score(&self) -> f64 {
        match self {
            NodeMatch::Allowed { score } => *score,
            NodeMatch::Excluded(_) => 0.0,
        }
    }
}

/// Network isolation configuration for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct NetworkIsolation {
    /// Tenant-specific API endpoint prefix
    pub endpoint_prefix: String,
    /// Allowed source IP ranges (CIDR notation)
    pub allowed_ip_ranges: Vec<String>,
    /// VPC/VLAN ID for this tenant (if using network segmentation)
    pub vlan_id: Option<u32>,
    /// Enable mTLS for tenant connections
    pub require_mtls: bool,
    /// Custom DNS entries for this tenant
    pub dns_entries: HashMap<String, String>,
    /// Egress destinations allowed (empty = all)
    pub allowed_egress: Vec<String>,
    /// Egress destinations blocked
    pub blocked_egress: Vec<String>,
}


impl NetworkIsolation {
    /// Creates network isolation for a tenant ID.
    pub fn for_tenant(tenant_id: &TenantId) -> Self {
        Self {
            endpoint_prefix: format!("/tenant/{}", tenant_id.0),
            ..Default::default()
        }
    }

    /// Adds an allowed IP range.
    pub fn with_allowed_ip(mut self, cidr: impl Into<String>) -> Self {
        self.allowed_ip_ranges.push(cidr.into());
        self
    }

    /// Sets the VLAN ID.
    pub fn with_vlan(mut self, vlan_id: u32) -> Self {
        self.vlan_id = Some(vlan_id);
        self
    }

    /// Enables mTLS requirement.
    pub fn with_mtls(mut self) -> Self {
        self.require_mtls = true;
        self
    }

    /// Checks if an IP is allowed.
    pub fn is_ip_allowed(&self, ip: &str) -> bool {
        if self.allowed_ip_ranges.is_empty() {
            return true;
        }
        // Simplified IP check - in production, use proper CIDR matching
        self.allowed_ip_ranges
            .iter()
            .any(|range| ip.starts_with(&range.split('/').next().unwrap_or("").to_string()))
    }

    /// Checks if an egress destination is allowed.
    pub fn is_egress_allowed(&self, destination: &str) -> bool {
        // Check blocked list first
        if self.blocked_egress.iter().any(|b| destination.contains(b)) {
            return false;
        }
        // If allowed list is empty, all non-blocked are allowed
        if self.allowed_egress.is_empty() {
            return true;
        }
        // Check allowed list
        self.allowed_egress.iter().any(|a| destination.contains(a))
    }
}

/// Data isolation configuration for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataIsolation {
    /// Namespace prefix for all tenant data
    pub namespace_prefix: String,
    /// Storage bucket/container for this tenant
    pub storage_bucket: Option<String>,
    /// Encryption key ID for tenant data
    pub encryption_key_id: Option<String>,
    /// Whether to use separate database schema
    pub separate_schema: bool,
    /// Data classification level
    pub data_classification: DataClassification,
    /// Cross-tenant data sharing rules
    pub sharing_rules: Vec<DataSharingRule>,
}

impl Default for DataIsolation {
    fn default() -> Self {
        Self {
            namespace_prefix: String::new(),
            storage_bucket: None,
            encryption_key_id: None,
            separate_schema: false,
            data_classification: DataClassification::Internal,
            sharing_rules: Vec::new(),
        }
    }
}

impl DataIsolation {
    /// Creates data isolation for a tenant.
    pub fn for_tenant(tenant_id: &TenantId) -> Self {
        Self {
            namespace_prefix: format!("tenant:{}", tenant_id.0),
            storage_bucket: Some(format!("marabunta-tenant-{}", tenant_id.0)),
            ..Default::default()
        }
    }

    /// Sets the encryption key.
    pub fn with_encryption_key(mut self, key_id: impl Into<String>) -> Self {
        self.encryption_key_id = Some(key_id.into());
        self
    }

    /// Enables separate schema.
    pub fn with_separate_schema(mut self) -> Self {
        self.separate_schema = true;
        self
    }

    /// Sets the data classification.
    pub fn with_classification(mut self, classification: DataClassification) -> Self {
        self.data_classification = classification;
        self
    }

    /// Adds a sharing rule.
    pub fn with_sharing_rule(mut self, rule: DataSharingRule) -> Self {
        self.sharing_rules.push(rule);
        self
    }

    /// Returns the namespaced key for a given key.
    pub fn namespace_key(&self, key: &str) -> String {
        format!("{}:{}", self.namespace_prefix, key)
    }

    /// Checks if sharing is allowed with another tenant.
    pub fn can_share_with(&self, other_tenant_id: &TenantId, data_type: &str) -> bool {
        self.sharing_rules.iter().any(|rule| {
            rule.target_tenant_id == *other_tenant_id
                && rule.is_active()
                && (rule.data_types.is_empty() || rule.data_types.contains(&data_type.to_string()))
        })
    }
}

/// Data classification level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum DataClassification {
    /// Public data - no restrictions
    Public,
    /// Internal data - tenant only
    #[default]
    Internal,
    /// Confidential data - restricted access
    Confidential,
    /// Highly confidential - strict controls
    HighlyConfidential,
}


/// Rule for cross-tenant data sharing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSharingRule {
    /// Target tenant to share with
    pub target_tenant_id: TenantId,
    /// Data types allowed to share (empty = all)
    pub data_types: Vec<String>,
    /// Whether sharing is bidirectional
    pub bidirectional: bool,
    /// When the sharing rule expires
    pub expires_at: Option<DateTime<Utc>>,
    /// Who approved this sharing
    pub approved_by: String,
    /// When approved
    pub approved_at: DateTime<Utc>,
}

impl DataSharingRule {
    /// Creates a new sharing rule.
    pub fn new(target_tenant_id: TenantId, approved_by: impl Into<String>) -> Self {
        Self {
            target_tenant_id,
            data_types: Vec::new(),
            bidirectional: false,
            expires_at: None,
            approved_by: approved_by.into(),
            approved_at: Utc::now(),
        }
    }

    /// Sets the allowed data types.
    pub fn with_data_types(mut self, types: Vec<String>) -> Self {
        self.data_types = types;
        self
    }

    /// Makes the rule bidirectional.
    pub fn bidirectional(mut self) -> Self {
        self.bidirectional = true;
        self
    }

    /// Sets expiration.
    pub fn expires_at(mut self, when: DateTime<Utc>) -> Self {
        self.expires_at = Some(when);
        self
    }

    /// Checks if the rule is active.
    pub fn is_active(&self) -> bool {
        self.expires_at.map_or(true, |exp| exp > Utc::now())
    }
}

/// Complete isolation configuration for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantIsolation {
    /// Tenant ID this isolation applies to
    pub tenant_id: TenantId,
    /// Resource isolation mode
    pub resource_mode: ResourceIsolationMode,
    /// Node affinity rules
    pub node_affinity: TenantNodeAffinity,
    /// Network isolation
    pub network: NetworkIsolation,
    /// Data isolation
    pub data: DataIsolation,
    /// When this configuration was created
    pub created_at: DateTime<Utc>,
    /// When this configuration was last updated
    pub updated_at: DateTime<Utc>,
}

impl TenantIsolation {
    /// Creates default isolation for a tenant.
    pub fn new(tenant_id: TenantId) -> Self {
        let now = Utc::now();
        Self {
            tenant_id,
            resource_mode: ResourceIsolationMode::Shared,
            node_affinity: TenantNodeAffinity::default(),
            network: NetworkIsolation::for_tenant(&tenant_id),
            data: DataIsolation::for_tenant(&tenant_id),
            created_at: now,
            updated_at: now,
        }
    }

    /// Creates strict isolation (dedicated resources, network controls, encryption).
    pub fn strict(tenant_id: TenantId) -> Self {
        let now = Utc::now();
        Self {
            tenant_id,
            resource_mode: ResourceIsolationMode::Dedicated,
            node_affinity: TenantNodeAffinity::default()
                .with_required_tag("tenant", tenant_id.to_string()),
            network: NetworkIsolation::for_tenant(&tenant_id).with_mtls(),
            data: DataIsolation::for_tenant(&tenant_id)
                .with_separate_schema()
                .with_classification(DataClassification::Confidential),
            created_at: now,
            updated_at: now,
        }
    }

    /// Sets the resource isolation mode.
    pub fn with_resource_mode(mut self, mode: ResourceIsolationMode) -> Self {
        self.resource_mode = mode;
        self.updated_at = Utc::now();
        self
    }

    /// Sets the node affinity.
    pub fn with_node_affinity(mut self, affinity: TenantNodeAffinity) -> Self {
        self.node_affinity = affinity;
        self.updated_at = Utc::now();
        self
    }

    /// Sets the network isolation.
    pub fn with_network(mut self, network: NetworkIsolation) -> Self {
        self.network = network;
        self.updated_at = Utc::now();
        self
    }

    /// Sets the data isolation.
    pub fn with_data(mut self, data: DataIsolation) -> Self {
        self.data = data;
        self.updated_at = Utc::now();
        self
    }
}

/// Manager for tenant isolation configurations.
pub struct IsolationManager {
    /// Isolation configurations by tenant ID
    configurations: Arc<RwLock<HashMap<TenantId, TenantIsolation>>>,
    /// Node to tenant assignments (for dedicated mode)
    node_assignments: Arc<RwLock<HashMap<String, TenantId>>>,
}

impl IsolationManager {
    /// Creates a new isolation manager.
    pub fn new() -> Self {
        Self {
            configurations: Arc::new(RwLock::new(HashMap::new())),
            node_assignments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Gets the isolation configuration for a tenant.
    pub async fn get_isolation(&self, tenant_id: &TenantId) -> Option<TenantIsolation> {
        self.configurations.read().await.get(tenant_id).cloned()
    }

    /// Sets the isolation configuration for a tenant.
    pub async fn set_isolation(&self, isolation: TenantIsolation) {
        let tenant_id = isolation.tenant_id;

        // Update node assignments for dedicated nodes
        if !isolation.node_affinity.dedicated_nodes.is_empty() {
            let mut assignments = self.node_assignments.write().await;
            for node_id in &isolation.node_affinity.dedicated_nodes {
                assignments.insert(node_id.clone(), tenant_id);
            }
        }

        self.configurations
            .write()
            .await
            .insert(tenant_id, isolation);
    }

    /// Removes isolation configuration for a tenant.
    pub async fn remove_isolation(&self, tenant_id: &TenantId) -> Option<TenantIsolation> {
        let isolation = self.configurations.write().await.remove(tenant_id);

        // Remove node assignments
        if let Some(ref iso) = isolation {
            let mut assignments = self.node_assignments.write().await;
            for node_id in &iso.node_affinity.dedicated_nodes {
                assignments.remove(node_id);
            }
        }

        isolation
    }

    /// Assigns a node to a tenant.
    pub async fn assign_node(&self, node_id: impl Into<String>, tenant_id: TenantId) {
        let node_id = node_id.into();
        self.node_assignments
            .write()
            .await
            .insert(node_id.clone(), tenant_id);

        // Update tenant's isolation config
        if let Some(isolation) = self.configurations.write().await.get_mut(&tenant_id) {
            isolation.node_affinity.dedicated_nodes.insert(node_id);
            isolation.updated_at = Utc::now();
        }
    }

    /// Unassigns a node from a tenant.
    pub async fn unassign_node(&self, node_id: &str) -> Option<TenantId> {
        let tenant_id = self.node_assignments.write().await.remove(node_id);

        // Update tenant's isolation config
        if let Some(tid) = tenant_id {
            if let Some(isolation) = self.configurations.write().await.get_mut(&tid) {
                isolation.node_affinity.dedicated_nodes.remove(node_id);
                isolation.updated_at = Utc::now();
            }
        }

        tenant_id
    }

    /// Gets the tenant assigned to a node.
    pub async fn get_node_tenant(&self, node_id: &str) -> Option<TenantId> {
        self.node_assignments.read().await.get(node_id).copied()
    }

    /// Checks if a node is available for a tenant.
    pub async fn is_node_available(
        &self,
        node_id: &str,
        tenant_id: &TenantId,
        node_tags: &HashMap<String, String>,
    ) -> NodeMatch {
        // Check if node is assigned to another tenant
        if let Some(assigned_tenant) = self.node_assignments.read().await.get(node_id) {
            if assigned_tenant != tenant_id {
                return NodeMatch::Excluded("Node assigned to another tenant".to_string());
            }
        }

        // Check tenant's isolation config
        if let Some(isolation) = self.configurations.read().await.get(tenant_id) {
            return isolation.node_affinity.matches_node(node_id, node_tags);
        }

        // No isolation config - allow by default
        NodeMatch::Allowed { score: 0.5 }
    }

    /// Filters nodes available for a tenant.
    pub async fn filter_nodes(
        &self,
        tenant_id: &TenantId,
        nodes: &[(String, HashMap<String, String>)],
    ) -> Vec<(String, f64)> {
        let mut available = Vec::new();

        for (node_id, node_tags) in nodes {
            match self.is_node_available(node_id, tenant_id, node_tags).await {
                NodeMatch::Allowed { score } => {
                    available.push((node_id.clone(), score));
                }
                NodeMatch::Excluded(_) => {}
            }
        }

        // Sort by score descending
        available.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        available
    }

    /// Lists all isolation configurations.
    pub async fn list_all(&self) -> Vec<TenantIsolation> {
        self.configurations.read().await.values().cloned().collect()
    }

    /// Lists all node assignments.
    pub async fn list_node_assignments(&self) -> HashMap<String, TenantId> {
        self.node_assignments.read().await.clone()
    }
}

impl Default for IsolationManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_affinity_matching() {
        let affinity = TenantNodeAffinity::default()
            .with_required_tag("env", "production")
            .with_preferred_tag("region", "us-east-1")
            .with_anti_affinity_tag("dedicated", "other-tenant");

        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "production".to_string());
        tags.insert("region".to_string(), "us-east-1".to_string());

        let result = affinity.matches_node("node-1", &tags);
        assert!(result.is_allowed());
        assert!(result.score() > 0.5); // Has preferred tag match

        // Missing required tag
        let mut tags2 = HashMap::new();
        tags2.insert("env".to_string(), "staging".to_string());
        let result2 = affinity.matches_node("node-2", &tags2);
        assert!(!result2.is_allowed());

        // Anti-affinity match
        let mut tags3 = HashMap::new();
        tags3.insert("env".to_string(), "production".to_string());
        tags3.insert("dedicated".to_string(), "other-tenant".to_string());
        let result3 = affinity.matches_node("node-3", &tags3);
        assert!(!result3.is_allowed());
    }

    #[test]
    fn test_node_affinity_dedicated() {
        let affinity = TenantNodeAffinity::default()
            .with_dedicated_node("node-1")
            .with_dedicated_node("node-2");

        let tags = HashMap::new();

        // Dedicated node is allowed
        let result = affinity.matches_node("node-1", &tags);
        assert!(result.is_allowed());

        // Non-dedicated node is excluded
        let result2 = affinity.matches_node("node-3", &tags);
        assert!(!result2.is_allowed());
    }

    #[test]
    fn test_network_isolation() {
        let network = NetworkIsolation::default()
            .with_allowed_ip("10.0.0")
            .with_mtls();

        assert!(network.is_ip_allowed("10.0.0.5"));
        assert!(!network.is_ip_allowed("192.168.1.1"));
        assert!(network.require_mtls);
    }

    #[test]
    fn test_network_egress() {
        let mut network = NetworkIsolation::default();
        network.blocked_egress.push("malicious.com".to_string());

        assert!(network.is_egress_allowed("api.example.com"));
        assert!(!network.is_egress_allowed("malicious.com/api"));

        network.allowed_egress = vec!["trusted.com".to_string()];
        assert!(network.is_egress_allowed("api.trusted.com"));
        assert!(!network.is_egress_allowed("other.com"));
    }

    #[test]
    fn test_data_isolation() {
        let tenant_id = TenantId::new();
        let data = DataIsolation::for_tenant(&tenant_id).with_encryption_key("key-123");

        assert!(data.namespace_prefix.starts_with("tenant:"));
        assert!(data.storage_bucket.is_some());
        assert_eq!(data.encryption_key_id, Some("key-123".to_string()));
    }

    #[test]
    fn test_data_sharing() {
        let tenant_id = TenantId::new();
        let other_tenant_id = TenantId::new();

        let rule = DataSharingRule::new(other_tenant_id, "admin")
            .with_data_types(vec!["results".to_string()]);

        let data = DataIsolation::for_tenant(&tenant_id).with_sharing_rule(rule);

        assert!(data.can_share_with(&other_tenant_id, "results"));
        assert!(!data.can_share_with(&other_tenant_id, "secrets"));
        assert!(!data.can_share_with(&TenantId::new(), "results"));
    }

    #[test]
    fn test_tenant_isolation() {
        let tenant_id = TenantId::new();
        let isolation =
            TenantIsolation::new(tenant_id).with_resource_mode(ResourceIsolationMode::Dedicated);

        assert_eq!(isolation.tenant_id, tenant_id);
        assert_eq!(isolation.resource_mode, ResourceIsolationMode::Dedicated);
    }

    #[test]
    fn test_strict_isolation() {
        let tenant_id = TenantId::new();
        let isolation = TenantIsolation::strict(tenant_id);

        assert_eq!(isolation.resource_mode, ResourceIsolationMode::Dedicated);
        assert!(isolation.network.require_mtls);
        assert!(isolation.data.separate_schema);
        assert_eq!(
            isolation.data.data_classification,
            DataClassification::Confidential
        );
    }

    #[tokio::test]
    async fn test_isolation_manager() {
        let manager = IsolationManager::new();
        let tenant_id = TenantId::new();

        // Set isolation
        let isolation = TenantIsolation::new(tenant_id);
        manager.set_isolation(isolation.clone()).await;

        // Get isolation
        let retrieved = manager.get_isolation(&tenant_id).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().tenant_id, tenant_id);

        // Remove isolation
        let removed = manager.remove_isolation(&tenant_id).await;
        assert!(removed.is_some());
        assert!(manager.get_isolation(&tenant_id).await.is_none());
    }

    #[tokio::test]
    async fn test_isolation_manager_node_assignment() {
        let manager = IsolationManager::new();
        let tenant_id = TenantId::new();
        let other_tenant_id = TenantId::new();

        manager.assign_node("node-1", tenant_id).await;

        // Node is available for assigned tenant
        let tags = HashMap::new();
        let result = manager.is_node_available("node-1", &tenant_id, &tags).await;
        assert!(result.is_allowed());

        // Node is not available for other tenant
        let result2 = manager
            .is_node_available("node-1", &other_tenant_id, &tags)
            .await;
        assert!(!result2.is_allowed());
    }

    #[tokio::test]
    async fn test_filter_nodes() {
        let manager = IsolationManager::new();
        let tenant_id = TenantId::new();

        let isolation = TenantIsolation::new(tenant_id).with_node_affinity(
            TenantNodeAffinity::default().with_required_tag("env", "production"),
        );
        manager.set_isolation(isolation).await;

        let mut prod_tags = HashMap::new();
        prod_tags.insert("env".to_string(), "production".to_string());

        let mut dev_tags = HashMap::new();
        dev_tags.insert("env".to_string(), "development".to_string());

        let nodes = vec![
            ("node-1".to_string(), prod_tags),
            ("node-2".to_string(), dev_tags),
        ];

        let available = manager.filter_nodes(&tenant_id, &nodes).await;
        assert_eq!(available.len(), 1);
        assert_eq!(available[0].0, "node-1");
    }
}
