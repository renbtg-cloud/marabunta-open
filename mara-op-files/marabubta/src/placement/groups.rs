// Marabunta - Licensed under the MIT License.
//! Groups system for organizing nodes into collections
//!
//! Groups are named collections of nodes defined either explicitly,
//! dynamically via tag expressions, or as composites of other groups.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

use super::tags::{Tag, TagExpr, TagSet};

/// Unique identifier for a node
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    /// Create a new node ID
    pub fn new(id: impl Into<String>) -> Self {
        NodeId(id.into())
    }

    /// Generate a random node ID
    pub fn random() -> Self {
        NodeId(Uuid::new_v4().to_string())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        NodeId(s.to_string())
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        NodeId(s)
    }
}

/// Unique identifier for a group
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupId(pub String);

impl GroupId {
    /// Create a new group ID
    pub fn new(id: impl Into<String>) -> Self {
        GroupId(id.into())
    }

    /// Generate a random group ID
    pub fn random() -> Self {
        GroupId(Uuid::new_v4().to_string())
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for GroupId {
    fn from(s: &str) -> Self {
        GroupId(s.to_string())
    }
}

impl From<String> for GroupId {
    fn from(s: String) -> Self {
        GroupId(s)
    }
}

/// Error types for group operations
#[derive(Error, Debug, Clone)]
pub enum GroupError {
    #[error("Group not found: {0}")]
    NotFound(String),
    #[error("Group already exists: {0}")]
    AlreadyExists(String),
    #[error("Circular dependency detected involving group: {0}")]
    CircularDependency(String),
    #[error("Invalid group membership: {0}")]
    InvalidMembership(String),
    #[error("Referenced group not found: {0}")]
    ReferencedGroupNotFound(String),
    #[error("Cannot modify group: {0}")]
    CannotModify(String),
}

/// Status of a node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum NodeStatus {
    /// Node is online and ready
    Online,
    /// Node is offline
    Offline,
    /// Node is draining (not accepting new work)
    Draining,
    /// Node is in maintenance mode
    Maintenance,
    /// Node status is unknown
    #[default]
    Unknown,
}


/// Information about a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier
    pub id: NodeId,
    /// Tags attached to this node
    pub tags: TagSet,
    /// Last time this node was seen
    pub last_seen: DateTime<Utc>,
    /// Current node status
    pub status: NodeStatus,
}

impl NodeInfo {
    /// Create a new node info
    pub fn new(id: impl Into<NodeId>) -> Self {
        NodeInfo {
            id: id.into(),
            tags: TagSet::new(),
            last_seen: Utc::now(),
            status: NodeStatus::Unknown,
        }
    }

    /// Create a node with tags
    pub fn with_tags(id: impl Into<NodeId>, tags: TagSet) -> Self {
        NodeInfo {
            id: id.into(),
            tags,
            last_seen: Utc::now(),
            status: NodeStatus::Online,
        }
    }

    /// Check if the node matches a tag expression
    pub fn matches(&self, expr: &TagExpr) -> bool {
        self.tags.matches(expr)
    }

    /// Update the last seen timestamp
    pub fn touch(&mut self) {
        self.last_seen = Utc::now();
    }
}

/// Registry for managing nodes
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeRegistry {
    nodes: HashMap<NodeId, NodeInfo>,
}

impl NodeRegistry {
    /// Create a new empty node registry
    pub fn new() -> Self {
        NodeRegistry {
            nodes: HashMap::new(),
        }
    }

    /// Register a new node
    pub fn register(&mut self, node: NodeInfo) {
        self.nodes.insert(node.id.clone(), node);
    }

    /// Unregister a node
    pub fn unregister(&mut self, id: &NodeId) -> Option<NodeInfo> {
        self.nodes.remove(id)
    }

    /// Update a node's tags completely
    pub fn update_tags(&mut self, node_id: &NodeId, tags: TagSet) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.tags = tags;
            node.touch();
        }
    }

    /// Add a single tag to a node
    pub fn add_tag(&mut self, node_id: &NodeId, tag: Tag) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.tags.insert(tag);
            node.touch();
        }
    }

    /// Remove a tag from a node
    pub fn remove_tag(&mut self, node_id: &NodeId, key: &str) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.tags.remove(key);
            node.touch();
        }
    }

    /// Get a node by ID
    pub fn get(&self, id: &NodeId) -> Option<&NodeInfo> {
        self.nodes.get(id)
    }

    /// Get a mutable reference to a node
    pub fn get_mut(&mut self, id: &NodeId) -> Option<&mut NodeInfo> {
        self.nodes.get_mut(id)
    }

    /// Find all nodes matching a tag expression
    pub fn find_matching(&self, expr: &TagExpr) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|node| node.matches(expr))
            .collect()
    }

    /// Get all nodes matching an expression as node IDs
    pub fn find_matching_ids(&self, expr: &TagExpr) -> HashSet<NodeId> {
        self.nodes
            .values()
            .filter(|node| node.matches(expr))
            .map(|node| node.id.clone())
            .collect()
    }

    /// Iterate over all nodes
    pub fn all_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }

    /// Get the number of registered nodes
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get all node IDs
    pub fn all_ids(&self) -> HashSet<NodeId> {
        self.nodes.keys().cloned().collect()
    }

    /// Update the status of a node
    pub fn update_status(&mut self, node_id: &NodeId, status: NodeStatus) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.status = status;
            node.touch();
        }
    }

    /// Get nodes by status
    pub fn nodes_by_status(&self, status: NodeStatus) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|node| node.status == status)
            .collect()
    }
}

/// How group membership is determined
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupMembership {
    /// Static list of node IDs
    Explicit(HashSet<NodeId>),
    /// Dynamic membership based on tag expression
    Dynamic(TagExpr),
    /// Union of multiple groups
    Union(Vec<GroupId>),
    /// Intersection of multiple groups
    Intersection(Vec<GroupId>),
    /// Set difference: base minus subtract
    Difference { base: GroupId, subtract: GroupId },
}

impl GroupMembership {
    /// Create an explicit membership with the given node IDs
    pub fn explicit<I: IntoIterator<Item = NodeId>>(nodes: I) -> Self {
        GroupMembership::Explicit(nodes.into_iter().collect())
    }

    /// Create a dynamic membership with the given tag expression
    pub fn dynamic(expr: TagExpr) -> Self {
        GroupMembership::Dynamic(expr)
    }

    /// Create a union of groups
    pub fn union<I: IntoIterator<Item = GroupId>>(groups: I) -> Self {
        GroupMembership::Union(groups.into_iter().collect())
    }

    /// Create an intersection of groups
    pub fn intersection<I: IntoIterator<Item = GroupId>>(groups: I) -> Self {
        GroupMembership::Intersection(groups.into_iter().collect())
    }

    /// Create a difference (base - subtract)
    pub fn difference(base: GroupId, subtract: GroupId) -> Self {
        GroupMembership::Difference { base, subtract }
    }
}

/// Type of group
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum GroupType {
    /// Manually managed group
    #[default]
    Static,
    /// Auto-detected group that may disappear
    Ephemeral,
    /// Derived from other groups
    Computed,
}


/// Metadata associated with a group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMetadata {
    /// Who created this group
    pub created_by: String,
    /// When this group was created
    pub created_at: DateTime<Utc>,
    /// Type of group
    pub group_type: GroupType,
    /// Governance domain (who can modify)
    pub authority_domain: Option<String>,
}

impl Default for GroupMetadata {
    fn default() -> Self {
        GroupMetadata {
            created_by: "system".to_string(),
            created_at: Utc::now(),
            group_type: GroupType::Static,
            authority_domain: None,
        }
    }
}

impl GroupMetadata {
    /// Create metadata for a static group
    pub fn static_group(created_by: impl Into<String>) -> Self {
        GroupMetadata {
            created_by: created_by.into(),
            created_at: Utc::now(),
            group_type: GroupType::Static,
            authority_domain: None,
        }
    }

    /// Create metadata for an ephemeral group
    pub fn ephemeral(created_by: impl Into<String>) -> Self {
        GroupMetadata {
            created_by: created_by.into(),
            created_at: Utc::now(),
            group_type: GroupType::Ephemeral,
            authority_domain: None,
        }
    }

    /// Create metadata for a computed group
    pub fn computed(created_by: impl Into<String>) -> Self {
        GroupMetadata {
            created_by: created_by.into(),
            created_at: Utc::now(),
            group_type: GroupType::Computed,
            authority_domain: None,
        }
    }

    /// Set the authority domain
    pub fn with_authority(mut self, domain: impl Into<String>) -> Self {
        self.authority_domain = Some(domain.into());
        self
    }
}

/// A named collection of nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    /// Unique group identifier
    pub id: GroupId,
    /// Human-readable name
    pub name: String,
    /// Description of the group's purpose
    pub description: String,
    /// How membership is determined
    pub membership: GroupMembership,
    /// Group metadata
    pub metadata: GroupMetadata,
}

impl Group {
    /// Create a new group with explicit membership
    pub fn explicit(
        name: impl Into<String>,
        description: impl Into<String>,
        nodes: HashSet<NodeId>,
    ) -> Self {
        Group {
            id: GroupId::random(),
            name: name.into(),
            description: description.into(),
            membership: GroupMembership::Explicit(nodes),
            metadata: GroupMetadata::default(),
        }
    }

    /// Create a new group with dynamic membership
    pub fn dynamic(name: impl Into<String>, description: impl Into<String>, expr: TagExpr) -> Self {
        Group {
            id: GroupId::random(),
            name: name.into(),
            description: description.into(),
            membership: GroupMembership::Dynamic(expr),
            metadata: GroupMetadata::default(),
        }
    }

    /// Create a union group
    pub fn union(
        name: impl Into<String>,
        description: impl Into<String>,
        groups: Vec<GroupId>,
    ) -> Self {
        Group {
            id: GroupId::random(),
            name: name.into(),
            description: description.into(),
            membership: GroupMembership::Union(groups),
            metadata: GroupMetadata::computed("system"),
        }
    }

    /// Create an intersection group
    pub fn intersection(
        name: impl Into<String>,
        description: impl Into<String>,
        groups: Vec<GroupId>,
    ) -> Self {
        Group {
            id: GroupId::random(),
            name: name.into(),
            description: description.into(),
            membership: GroupMembership::Intersection(groups),
            metadata: GroupMetadata::computed("system"),
        }
    }

    /// Create a difference group
    pub fn difference(
        name: impl Into<String>,
        description: impl Into<String>,
        base: GroupId,
        subtract: GroupId,
    ) -> Self {
        Group {
            id: GroupId::random(),
            name: name.into(),
            description: description.into(),
            membership: GroupMembership::Difference { base, subtract },
            metadata: GroupMetadata::computed("system"),
        }
    }

    /// Set custom metadata
    pub fn with_metadata(mut self, metadata: GroupMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Set a specific group ID
    pub fn with_id(mut self, id: impl Into<GroupId>) -> Self {
        self.id = id.into();
        self
    }

    /// Check if this is a composite group (depends on other groups)
    pub fn is_composite(&self) -> bool {
        matches!(
            self.membership,
            GroupMembership::Union(_)
                | GroupMembership::Intersection(_)
                | GroupMembership::Difference { .. }
        )
    }

    /// Get the group IDs this group depends on
    pub fn dependencies(&self) -> Vec<&GroupId> {
        match &self.membership {
            GroupMembership::Explicit(_) => vec![],
            GroupMembership::Dynamic(_) => vec![],
            GroupMembership::Union(groups) => groups.iter().collect(),
            GroupMembership::Intersection(groups) => groups.iter().collect(),
            GroupMembership::Difference { base, subtract } => vec![base, subtract],
        }
    }
}

/// Registry for managing groups
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GroupRegistry {
    groups: HashMap<GroupId, Group>,
    /// Index from group name to ID for fast lookup
    name_index: HashMap<String, GroupId>,
    /// Cache of node to groups mapping (for explicit/static groups only)
    node_to_groups: HashMap<NodeId, HashSet<GroupId>>,
}

impl GroupRegistry {
    /// Create a new empty group registry
    pub fn new() -> Self {
        GroupRegistry {
            groups: HashMap::new(),
            name_index: HashMap::new(),
            node_to_groups: HashMap::new(),
        }
    }

    /// Register a new group
    pub fn register(&mut self, group: Group) -> Result<GroupId, GroupError> {
        // Check for duplicate name
        if self.name_index.contains_key(&group.name) {
            return Err(GroupError::AlreadyExists(group.name.clone()));
        }

        // Check for duplicate ID
        if self.groups.contains_key(&group.id) {
            return Err(GroupError::AlreadyExists(group.id.to_string()));
        }

        // Validate dependencies for composite groups
        for dep in group.dependencies() {
            if !self.groups.contains_key(dep) {
                return Err(GroupError::ReferencedGroupNotFound(dep.to_string()));
            }
        }

        // Check for circular dependencies
        if group.is_composite()
            && self.would_create_cycle(&group) {
                return Err(GroupError::CircularDependency(group.id.to_string()));
            }

        let id = group.id.clone();

        // Update node index for explicit groups
        if let GroupMembership::Explicit(ref nodes) = group.membership {
            for node_id in nodes {
                self.node_to_groups
                    .entry(node_id.clone())
                    .or_default()
                    .insert(id.clone());
            }
        }

        self.name_index.insert(group.name.clone(), id.clone());
        self.groups.insert(id.clone(), group);

        Ok(id)
    }

    /// Check if adding a group would create a circular dependency
    fn would_create_cycle(&self, new_group: &Group) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![&new_group.id];

        while let Some(current_id) = stack.pop() {
            if !visited.insert(current_id.clone()) {
                return true;
            }

            // Get dependencies of current group
            let deps = if current_id == &new_group.id {
                new_group.dependencies()
            } else if let Some(group) = self.groups.get(current_id) {
                group.dependencies()
            } else {
                continue;
            };

            for dep in deps {
                if dep == &new_group.id {
                    return true;
                }
                stack.push(dep);
            }
        }

        false
    }

    /// Unregister a group
    pub fn unregister(&mut self, id: &GroupId) -> Option<Group> {
        if let Some(group) = self.groups.remove(id) {
            self.name_index.remove(&group.name);

            // Clean up node index
            if let GroupMembership::Explicit(ref nodes) = group.membership {
                for node_id in nodes {
                    if let Some(groups) = self.node_to_groups.get_mut(node_id) {
                        groups.remove(id);
                    }
                }
            }

            Some(group)
        } else {
            None
        }
    }

    /// Get a group by ID
    pub fn get(&self, id: &GroupId) -> Option<&Group> {
        self.groups.get(id)
    }

    /// Get a mutable reference to a group
    pub fn get_mut(&mut self, id: &GroupId) -> Option<&mut Group> {
        self.groups.get_mut(id)
    }

    /// Get a group by name
    pub fn get_by_name(&self, name: &str) -> Option<&Group> {
        self.name_index.get(name).and_then(|id| self.groups.get(id))
    }

    /// Resolve the members of a group
    pub fn resolve_members(&self, id: &GroupId, nodes: &NodeRegistry) -> HashSet<NodeId> {
        self.resolve_members_internal(id, nodes, &mut HashSet::new())
    }

    fn resolve_members_internal(
        &self,
        id: &GroupId,
        nodes: &NodeRegistry,
        visited: &mut HashSet<GroupId>,
    ) -> HashSet<NodeId> {
        // Prevent infinite recursion
        if !visited.insert(id.clone()) {
            return HashSet::new();
        }

        let Some(group) = self.groups.get(id) else {
            return HashSet::new();
        };

        match &group.membership {
            GroupMembership::Explicit(node_ids) => node_ids.clone(),

            GroupMembership::Dynamic(expr) => nodes.find_matching_ids(expr),

            GroupMembership::Union(group_ids) => {
                let mut result = HashSet::new();
                for gid in group_ids {
                    result.extend(self.resolve_members_internal(gid, nodes, visited));
                }
                result
            }

            GroupMembership::Intersection(group_ids) => {
                let mut iter = group_ids.iter();
                if let Some(first) = iter.next() {
                    let mut result = self.resolve_members_internal(first, nodes, visited);
                    for gid in iter {
                        let other = self.resolve_members_internal(gid, nodes, visited);
                        result.retain(|id| other.contains(id));
                    }
                    result
                } else {
                    HashSet::new()
                }
            }

            GroupMembership::Difference { base, subtract } => {
                let base_members = self.resolve_members_internal(base, nodes, visited);
                let subtract_members = self.resolve_members_internal(subtract, nodes, visited);
                base_members
                    .difference(&subtract_members)
                    .cloned()
                    .collect()
            }
        }
    }

    /// Find all groups a node belongs to (evaluating dynamic groups)
    pub fn groups_for_node(&self, node_id: &NodeId, nodes: &NodeRegistry) -> HashSet<GroupId> {
        let mut result = HashSet::new();

        for (group_id, group) in &self.groups {
            match &group.membership {
                GroupMembership::Explicit(members) => {
                    if members.contains(node_id) {
                        result.insert(group_id.clone());
                    }
                }
                GroupMembership::Dynamic(expr) => {
                    if let Some(node) = nodes.get(node_id) {
                        if node.matches(expr) {
                            result.insert(group_id.clone());
                        }
                    }
                }
                _ => {
                    // For composite groups, check if node is in resolved members
                    let members = self.resolve_members(group_id, nodes);
                    if members.contains(node_id) {
                        result.insert(group_id.clone());
                    }
                }
            }
        }

        result
    }

    /// Rebuild indexes after node tag changes
    pub fn rebuild_index(&mut self, _nodes: &NodeRegistry) {
        // Rebuild node_to_groups for explicit groups
        self.node_to_groups.clear();

        for (group_id, group) in &self.groups {
            if let GroupMembership::Explicit(ref node_ids) = group.membership {
                for node_id in node_ids {
                    self.node_to_groups
                        .entry(node_id.clone())
                        .or_default()
                        .insert(group_id.clone());
                }
            }
        }
    }

    /// Get all groups
    pub fn all_groups(&self) -> impl Iterator<Item = &Group> {
        self.groups.values()
    }

    /// Get the number of groups
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Update a group's explicit membership
    pub fn update_explicit_membership(
        &mut self,
        group_id: &GroupId,
        nodes: HashSet<NodeId>,
    ) -> Result<(), GroupError> {
        let group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| GroupError::NotFound(group_id.to_string()))?;

        if !matches!(group.membership, GroupMembership::Explicit(_)) {
            return Err(GroupError::InvalidMembership(
                "Cannot update non-explicit group membership".to_string(),
            ));
        }

        // Remove old index entries
        if let GroupMembership::Explicit(ref old_nodes) = group.membership {
            for node_id in old_nodes {
                if let Some(groups) = self.node_to_groups.get_mut(node_id) {
                    groups.remove(group_id);
                }
            }
        }

        // Add new index entries
        for node_id in &nodes {
            self.node_to_groups
                .entry(node_id.clone())
                .or_default()
                .insert(group_id.clone());
        }

        group.membership = GroupMembership::Explicit(nodes);
        Ok(())
    }

    /// Add a node to an explicit group
    pub fn add_node_to_group(
        &mut self,
        group_id: &GroupId,
        node_id: NodeId,
    ) -> Result<(), GroupError> {
        let group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| GroupError::NotFound(group_id.to_string()))?;

        match &mut group.membership {
            GroupMembership::Explicit(nodes) => {
                nodes.insert(node_id.clone());
                self.node_to_groups
                    .entry(node_id)
                    .or_default()
                    .insert(group_id.clone());
                Ok(())
            }
            _ => Err(GroupError::InvalidMembership(
                "Can only add nodes to explicit groups".to_string(),
            )),
        }
    }

    /// Remove a node from an explicit group
    pub fn remove_node_from_group(
        &mut self,
        group_id: &GroupId,
        node_id: &NodeId,
    ) -> Result<(), GroupError> {
        let group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| GroupError::NotFound(group_id.to_string()))?;

        match &mut group.membership {
            GroupMembership::Explicit(nodes) => {
                nodes.remove(node_id);
                if let Some(groups) = self.node_to_groups.get_mut(node_id) {
                    groups.remove(group_id);
                }
                Ok(())
            }
            _ => Err(GroupError::InvalidMembership(
                "Can only remove nodes from explicit groups".to_string(),
            )),
        }
    }

    /// Find groups by type
    pub fn groups_by_type(&self, group_type: GroupType) -> Vec<&Group> {
        self.groups
            .values()
            .filter(|g| g.metadata.group_type == group_type)
            .collect()
    }

    /// Check if a group contains a node
    pub fn group_contains_node(
        &self,
        group_id: &GroupId,
        node_id: &NodeId,
        nodes: &NodeRegistry,
    ) -> bool {
        self.resolve_members(group_id, nodes).contains(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::tags::{Tag, TagNamespace, TagValue};

    fn create_test_nodes() -> NodeRegistry {
        let mut registry = NodeRegistry::new();

        // Node 1: GPU node in building A
        let mut tags1 = TagSet::new();
        tags1.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));
        tags1.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(128.0),
        ));
        tags1.insert(Tag::new(
            TagNamespace::Location,
            "building",
            TagValue::String("building-a".to_string()),
        ));
        registry.register(NodeInfo::with_tags("node-1", tags1));

        // Node 2: GPU node in building B
        let mut tags2 = TagSet::new();
        tags2.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));
        tags2.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(64.0),
        ));
        tags2.insert(Tag::new(
            TagNamespace::Location,
            "building",
            TagValue::String("building-b".to_string()),
        ));
        registry.register(NodeInfo::with_tags("node-2", tags2));

        // Node 3: CPU-only node in building A
        let mut tags3 = TagSet::new();
        tags3.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(32.0),
        ));
        tags3.insert(Tag::new(
            TagNamespace::Location,
            "building",
            TagValue::String("building-a".to_string()),
        ));
        registry.register(NodeInfo::with_tags("node-3", tags3));

        // Node 4: High-memory node in building C
        let mut tags4 = TagSet::new();
        tags4.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(256.0),
        ));
        tags4.insert(Tag::new(
            TagNamespace::Location,
            "building",
            TagValue::String("building-c".to_string()),
        ));
        registry.register(NodeInfo::with_tags("node-4", tags4));

        registry
    }

    #[test]
    fn test_node_registry_basic() {
        let mut registry = NodeRegistry::new();
        let node = NodeInfo::new("test-node");
        registry.register(node);

        assert_eq!(registry.len(), 1);
        assert!(registry.get(&NodeId::new("test-node")).is_some());
    }

    #[test]
    fn test_node_registry_find_matching() {
        let nodes = create_test_nodes();

        // Find GPU nodes
        let expr = TagExpr::Has("hardware:gpu".into());
        let matching = nodes.find_matching(&expr);
        assert_eq!(matching.len(), 2);

        // Find nodes with high memory
        let expr = TagExpr::GreaterThan("hardware:memory".into(), 100.0);
        let matching = nodes.find_matching(&expr);
        assert_eq!(matching.len(), 2); // node-1 (128) and node-4 (256)
    }

    #[test]
    fn test_explicit_group() {
        let nodes = create_test_nodes();
        let mut groups = GroupRegistry::new();

        let group = Group::explicit(
            "test-group",
            "A test group",
            vec![NodeId::new("node-1"), NodeId::new("node-2")]
                .into_iter()
                .collect(),
        );
        let group_id = groups.register(group).unwrap();

        let members = groups.resolve_members(&group_id, &nodes);
        assert_eq!(members.len(), 2);
        assert!(members.contains(&NodeId::new("node-1")));
        assert!(members.contains(&NodeId::new("node-2")));
    }

    #[test]
    fn test_dynamic_group() {
        let nodes = create_test_nodes();
        let mut groups = GroupRegistry::new();

        // Create a dynamic group for GPU nodes
        let group = Group::dynamic(
            "gpu-nodes",
            "All nodes with GPUs",
            TagExpr::Has("hardware:gpu".into()),
        );
        let group_id = groups.register(group).unwrap();

        let members = groups.resolve_members(&group_id, &nodes);
        assert_eq!(members.len(), 2);
        assert!(members.contains(&NodeId::new("node-1")));
        assert!(members.contains(&NodeId::new("node-2")));
    }

    #[test]
    fn test_dynamic_group_complex_expr() {
        let nodes = create_test_nodes();
        let mut groups = GroupRegistry::new();

        // GPU nodes with high memory
        let expr = TagExpr::And(
            Box::new(TagExpr::Has("hardware:gpu".into())),
            Box::new(TagExpr::GreaterThan("hardware:memory".into(), 100.0)),
        );
        let group = Group::dynamic("high-mem-gpu", "GPU nodes with >100GB memory", expr);
        let group_id = groups.register(group).unwrap();

        let members = groups.resolve_members(&group_id, &nodes);
        assert_eq!(members.len(), 1);
        assert!(members.contains(&NodeId::new("node-1")));
    }

    #[test]
    fn test_union_group() {
        let nodes = create_test_nodes();
        let mut groups = GroupRegistry::new();

        // Create two explicit groups
        let group1 = Group::explicit(
            "group-1",
            "First group",
            vec![NodeId::new("node-1")].into_iter().collect(),
        )
        .with_id("group-1-id");
        let id1 = groups.register(group1).unwrap();

        let group2 = Group::explicit(
            "group-2",
            "Second group",
            vec![NodeId::new("node-3")].into_iter().collect(),
        )
        .with_id("group-2-id");
        let id2 = groups.register(group2).unwrap();

        // Create union group
        let union = Group::union("union-group", "Union of groups", vec![id1, id2]);
        let union_id = groups.register(union).unwrap();

        let members = groups.resolve_members(&union_id, &nodes);
        assert_eq!(members.len(), 2);
        assert!(members.contains(&NodeId::new("node-1")));
        assert!(members.contains(&NodeId::new("node-3")));
    }

    #[test]
    fn test_intersection_group() {
        let nodes = create_test_nodes();
        let mut groups = GroupRegistry::new();

        // Group for building-a
        let group1 = Group::dynamic(
            "building-a",
            "Building A nodes",
            TagExpr::Equals(
                "location:building".into(),
                TagValue::String("building-a".into()),
            ),
        )
        .with_id("building-a-id");
        let id1 = groups.register(group1).unwrap();

        // Group for GPU nodes
        let group2 = Group::dynamic(
            "gpu-nodes",
            "GPU nodes",
            TagExpr::Has("hardware:gpu".into()),
        )
        .with_id("gpu-nodes-id");
        let id2 = groups.register(group2).unwrap();

        // Intersection: GPU nodes in building A
        let intersection =
            Group::intersection("gpu-building-a", "GPU nodes in building A", vec![id1, id2]);
        let inter_id = groups.register(intersection).unwrap();

        let members = groups.resolve_members(&inter_id, &nodes);
        assert_eq!(members.len(), 1);
        assert!(members.contains(&NodeId::new("node-1")));
    }

    #[test]
    fn test_difference_group() {
        let nodes = create_test_nodes();
        let mut groups = GroupRegistry::new();

        // All nodes
        let all = Group::dynamic("all-nodes", "All nodes", TagExpr::True).with_id("all-id");
        let all_id = groups.register(all).unwrap();

        // GPU nodes
        let gpu = Group::dynamic(
            "gpu-nodes",
            "GPU nodes",
            TagExpr::Has("hardware:gpu".into()),
        )
        .with_id("gpu-id");
        let gpu_id = groups.register(gpu).unwrap();

        // Non-GPU nodes (all - gpu)
        let diff = Group::difference("non-gpu", "Non-GPU nodes", all_id, gpu_id);
        let diff_id = groups.register(diff).unwrap();

        let members = groups.resolve_members(&diff_id, &nodes);
        assert_eq!(members.len(), 2);
        assert!(members.contains(&NodeId::new("node-3")));
        assert!(members.contains(&NodeId::new("node-4")));
    }

    #[test]
    fn test_groups_for_node() {
        let nodes = create_test_nodes();
        let mut groups = GroupRegistry::new();

        // Create some groups
        let gpu_group = Group::dynamic(
            "gpu-nodes",
            "GPU nodes",
            TagExpr::Has("hardware:gpu".into()),
        );
        groups.register(gpu_group).unwrap();

        let building_a = Group::dynamic(
            "building-a",
            "Building A",
            TagExpr::Equals(
                "location:building".into(),
                TagValue::String("building-a".into()),
            ),
        );
        groups.register(building_a).unwrap();

        // Node 1 should be in both groups
        let node1_groups = groups.groups_for_node(&NodeId::new("node-1"), &nodes);
        assert_eq!(node1_groups.len(), 2);

        // Node 3 should only be in building-a
        let node3_groups = groups.groups_for_node(&NodeId::new("node-3"), &nodes);
        assert_eq!(node3_groups.len(), 1);

        // Node 4 should not be in any group
        let node4_groups = groups.groups_for_node(&NodeId::new("node-4"), &nodes);
        assert!(node4_groups.is_empty());
    }

    #[test]
    fn test_circular_dependency_detection() {
        let mut groups = GroupRegistry::new();

        // Create a base group
        let base = Group::explicit("base", "Base group", HashSet::new()).with_id("base-id");
        groups.register(base).unwrap();

        // Try to create a circular dependency
        let group_a = Group::union("group-a", "Group A", vec![GroupId::new("group-b-id")])
            .with_id("group-a-id");
        // This should fail because group-b doesn't exist
        assert!(groups.register(group_a).is_err());
    }

    #[test]
    fn test_group_by_name() {
        let mut groups = GroupRegistry::new();

        let group = Group::explicit("my-group", "My test group", HashSet::new());
        groups.register(group).unwrap();

        let found = groups.get_by_name("my-group");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "my-group");

        assert!(groups.get_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_add_remove_node_from_group() {
        let mut groups = GroupRegistry::new();

        let group = Group::explicit("test-group", "Test", HashSet::new());
        let group_id = groups.register(group).unwrap();

        // Add a node
        groups
            .add_node_to_group(&group_id, NodeId::new("node-1"))
            .unwrap();

        let group = groups.get(&group_id).unwrap();
        if let GroupMembership::Explicit(members) = &group.membership {
            assert!(members.contains(&NodeId::new("node-1")));
        } else {
            panic!("Expected explicit membership");
        }

        // Remove the node
        groups
            .remove_node_from_group(&group_id, &NodeId::new("node-1"))
            .unwrap();

        let group = groups.get(&group_id).unwrap();
        if let GroupMembership::Explicit(members) = &group.membership {
            assert!(members.is_empty());
        }
    }

    #[test]
    fn test_update_explicit_membership() {
        let mut groups = GroupRegistry::new();

        let group = Group::explicit(
            "test-group",
            "Test",
            vec![NodeId::new("node-1")].into_iter().collect(),
        );
        let group_id = groups.register(group).unwrap();

        // Update membership
        groups
            .update_explicit_membership(
                &group_id,
                vec![NodeId::new("node-2"), NodeId::new("node-3")]
                    .into_iter()
                    .collect(),
            )
            .unwrap();

        let group = groups.get(&group_id).unwrap();
        if let GroupMembership::Explicit(members) = &group.membership {
            assert_eq!(members.len(), 2);
            assert!(!members.contains(&NodeId::new("node-1")));
            assert!(members.contains(&NodeId::new("node-2")));
            assert!(members.contains(&NodeId::new("node-3")));
        }
    }

    #[test]
    fn test_groups_by_type() {
        let mut groups = GroupRegistry::new();

        let static_group = Group::explicit("static", "Static group", HashSet::new());
        groups.register(static_group).unwrap();

        let computed_group = Group::explicit("computed", "Computed group", HashSet::new())
            .with_metadata(GroupMetadata::computed("test"));
        groups.register(computed_group).unwrap();

        let static_groups = groups.groups_by_type(GroupType::Static);
        assert_eq!(static_groups.len(), 1);

        let computed_groups = groups.groups_by_type(GroupType::Computed);
        assert_eq!(computed_groups.len(), 1);
    }

    #[test]
    fn test_unregister_group() {
        let mut groups = GroupRegistry::new();

        let group = Group::explicit("test-group", "Test", HashSet::new());
        let group_id = groups.register(group).unwrap();

        assert!(groups.get(&group_id).is_some());

        let removed = groups.unregister(&group_id);
        assert!(removed.is_some());
        assert!(groups.get(&group_id).is_none());
        assert!(groups.get_by_name("test-group").is_none());
    }
}
