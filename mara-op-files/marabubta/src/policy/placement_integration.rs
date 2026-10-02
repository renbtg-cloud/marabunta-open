// Marabunta - Licensed under the MIT License.
//! Integration between policy and placement modules
//!
//! This module provides the bridge between the policy evaluation system and the
//! placement module's tags and groups system. It enables:
//!
//! - Policy `NodeSelector::Group` to resolve using placement's `GroupRegistry`
//! - Conversion between policy's `TagExpr` and placement's `TagExpr`
//! - Policy evaluation using placement's `NodeRegistry` for node information
//!
//! # Feature Flag
//!
//! This integration is optional and enabled via the `placement-integration` feature.
//! When enabled, the `PolicyEngine` can be configured with placement registries.
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::{Arc, RwLock};
//! use marabunta_compute::policy::{PolicyEngine, Policy, PolicyCondition, PolicyEffect, NodeSelector};
//! use marabunta_compute::placement::{NodeRegistry, GroupRegistry, Group, TagExpr as PlacementTagExpr};
//!
//! // Create registries
//! let node_registry = Arc::new(RwLock::new(NodeRegistry::new()));
//! let group_registry = Arc::new(RwLock::new(GroupRegistry::new()));
//!
//! // Create engine with placement integration
//! let mut engine = PolicyEngine::new();
//! engine.with_placement(node_registry.clone(), group_registry.clone());
//!
//! // Now NodeSelector::Group("my-group") will resolve using GroupRegistry
//! ```

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use crate::placement::{
    self,
    groups::{GroupId, GroupRegistry, NodeRegistry},
    tags::{TagExpr as PlacementTagExpr, TagValue as PlacementTagValue},
};

use super::engine::{NodeInfo as PolicyNodeInfo, PolicyEngine};
use super::ir::{NodeSelector, TagExpr as PolicyTagExpr};

/// Configuration for placement integration
#[derive(Clone)]
pub struct PlacementConfig {
    /// Node registry for resolving node information
    pub node_registry: Arc<RwLock<NodeRegistry>>,
    /// Group registry for resolving group membership
    pub group_registry: Arc<RwLock<GroupRegistry>>,
}

impl PlacementConfig {
    /// Create a new placement configuration
    pub fn new(
        node_registry: Arc<RwLock<NodeRegistry>>,
        group_registry: Arc<RwLock<GroupRegistry>>,
    ) -> Self {
        Self {
            node_registry,
            group_registry,
        }
    }
}

impl PolicyEngine {
    /// Set placement registries for policy evaluation
    ///
    /// When set, the engine will use these registries to:
    /// - Resolve `NodeSelector::Group` references using `GroupRegistry`
    /// - Look up node tags from `NodeRegistry`
    ///
    /// # Arguments
    ///
    /// * `node_registry` - Registry containing node information and tags
    /// * `group_registry` - Registry containing group definitions
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut engine = PolicyEngine::new();
    /// engine.with_placement(node_registry, group_registry);
    /// ```
    pub fn with_placement(
        &mut self,
        node_registry: Arc<RwLock<NodeRegistry>>,
        group_registry: Arc<RwLock<GroupRegistry>>,
    ) {
        self.placement_config = Some(PlacementConfig::new(node_registry, group_registry));
    }

    /// Resolve a NodeSelector using placement registries
    ///
    /// This method resolves a `NodeSelector` to a set of node IDs using the
    /// configured placement registries. If no placement config is set, it
    /// falls back to the default behavior.
    ///
    /// # Arguments
    ///
    /// * `selector` - The node selector to resolve
    /// * `available_nodes` - The nodes available for placement (used as fallback)
    ///
    /// # Returns
    ///
    /// A vector of node IDs that match the selector
    pub fn resolve_node_selector(
        &self,
        selector: &NodeSelector,
        available_nodes: &[PolicyNodeInfo],
    ) -> Vec<String> {
        match selector {
            NodeSelector::All => available_nodes.iter().map(|n| n.id.clone()).collect(),

            NodeSelector::NodeIds(ids) => {
                let id_set: HashSet<_> = ids.iter().collect();
                available_nodes
                    .iter()
                    .filter(|n| id_set.contains(&n.id))
                    .map(|n| n.id.clone())
                    .collect()
            }

            NodeSelector::Group(group_name) => {
                self.resolve_group_selector(group_name, available_nodes)
            }

            NodeSelector::Tag(tag_expr) => self.resolve_tag_selector(tag_expr, available_nodes),
        }
    }

    /// Resolve a group selector using the group registry
    fn resolve_group_selector(
        &self,
        group_name: &str,
        available_nodes: &[PolicyNodeInfo],
    ) -> Vec<String> {
        // If we have placement config, use the group registry
        if let Some(config) = &self.placement_config {
            if let (Ok(group_registry), Ok(node_registry)) =
                (config.group_registry.read(), config.node_registry.read())
            {
                // Try to find the group by name first, then by ID
                let group_id = if let Some(group) = group_registry.get_by_name(group_name) {
                    group.id.clone()
                } else {
                    GroupId::new(group_name)
                };

                // Resolve group members
                let members = group_registry.resolve_members(&group_id, &node_registry);

                // Filter to only available nodes
                let available_set: HashSet<_> = available_nodes.iter().map(|n| &n.id).collect();
                return members
                    .into_iter()
                    .filter(|id| available_set.contains(&id.0))
                    .map(|id| id.0)
                    .collect();
            }
        }

        // Fallback: match nodes that have a "group" tag matching the given group
        available_nodes
            .iter()
            .filter(|n| n.tags.get("group").is_some_and(|g| g == group_name))
            .map(|n| n.id.clone())
            .collect()
    }

    /// Resolve a tag selector, optionally using placement's richer tag expression
    fn resolve_tag_selector(
        &self,
        tag_expr: &PolicyTagExpr,
        available_nodes: &[PolicyNodeInfo],
    ) -> Vec<String> {
        // If we have placement config, convert and use placement's tag evaluation
        if let Some(config) = &self.placement_config {
            if let Ok(node_registry) = config.node_registry.read() {
                let placement_expr = PolicyTagExprAdapter::to_placement(tag_expr);

                // Use placement's tag matching on nodes from the registry
                let matching_ids = node_registry.find_matching_ids(&placement_expr);

                // Filter to only available nodes
                let available_set: HashSet<_> = available_nodes.iter().map(|n| &n.id).collect();
                return matching_ids
                    .into_iter()
                    .filter(|id| available_set.contains(&id.0))
                    .map(|id| id.0)
                    .collect();
            }
        }

        // Fallback: use the engine's built-in tag evaluation
        available_nodes
            .iter()
            .filter(|n| self.evaluate_tag_expr_internal(tag_expr, &n.tags))
            .map(|n| n.id.clone())
            .collect()
    }

    /// Internal method to evaluate a policy TagExpr against a policy TagSet
    fn evaluate_tag_expr_internal(&self, expr: &PolicyTagExpr, tags: &super::ir::TagSet) -> bool {
        match expr {
            PolicyTagExpr::HasKey(key) => tags.contains_key(key),
            PolicyTagExpr::Equals { key, value } => tags.get(key) == Some(value),
            PolicyTagExpr::KeyMatches { key, pattern } => {
                tags.contains_key(key) && self.matches_regex_internal(pattern, key)
            }
            PolicyTagExpr::ValueMatches { key, pattern } => tags
                .get(key)
                .is_some_and(|v| self.matches_regex_internal(pattern, v)),
            PolicyTagExpr::And(exprs) => exprs
                .iter()
                .all(|e| self.evaluate_tag_expr_internal(e, tags)),
            PolicyTagExpr::Or(exprs) => exprs
                .iter()
                .any(|e| self.evaluate_tag_expr_internal(e, tags)),
            PolicyTagExpr::Not(inner) => !self.evaluate_tag_expr_internal(inner, tags),
        }
    }

    /// Internal regex matching helper
    fn matches_regex_internal(&self, pattern: &str, text: &str) -> bool {
        regex::Regex::new(pattern)
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }
}

/// Adapter for converting between policy and placement TagExpr types
pub struct PolicyTagExprAdapter;

impl PolicyTagExprAdapter {
    /// Convert a policy TagExpr to a placement TagExpr
    ///
    /// This conversion maps the policy's simpler tag expression types to
    /// the placement module's richer expression language.
    ///
    /// # Mapping
    ///
    /// - `HasKey(key)` -> `Has(key)`
    /// - `Equals { key, value }` -> `Equals(key, TagValue::String(value))`
    /// - `KeyMatches { key, pattern }` -> `And(Has(key), Regex(key, pattern))` (approximation)
    /// - `ValueMatches { key, pattern }` -> `Regex(key, pattern)`
    /// - `And(vec)` -> nested `And(_, And(_, ...))`
    /// - `Or(vec)` -> nested `Or(_, Or(_, ...))`
    /// - `Not(inner)` -> `Not(inner)`
    pub fn to_placement(policy_expr: &PolicyTagExpr) -> PlacementTagExpr {
        match policy_expr {
            PolicyTagExpr::HasKey(key) => PlacementTagExpr::Has(key.clone()),

            PolicyTagExpr::Equals { key, value } => {
                PlacementTagExpr::Equals(key.clone(), PlacementTagValue::String(value.clone()))
            }

            PolicyTagExpr::KeyMatches { key, pattern } => {
                // KeyMatches is a bit unusual - it checks if a key exists AND matches a pattern
                // We approximate this as Has(key) AND Regex(key, pattern)
                PlacementTagExpr::And(
                    Box::new(PlacementTagExpr::Has(key.clone())),
                    Box::new(PlacementTagExpr::Regex(key.clone(), pattern.clone())),
                )
            }

            PolicyTagExpr::ValueMatches { key, pattern } => {
                PlacementTagExpr::Regex(key.clone(), pattern.clone())
            }

            PolicyTagExpr::And(exprs) => Self::fold_to_binary_and(exprs),

            PolicyTagExpr::Or(exprs) => Self::fold_to_binary_or(exprs),

            PolicyTagExpr::Not(inner) => PlacementTagExpr::Not(Box::new(Self::to_placement(inner))),
        }
    }

    /// Convert a placement TagExpr to a policy TagExpr
    ///
    /// This conversion maps placement's richer expression types to policy's
    /// simpler types. Some information may be lost in this conversion.
    ///
    /// # Mapping
    ///
    /// - `Has(key)` -> `HasKey(key)`
    /// - `Equals(key, value)` -> `Equals { key, value.to_string() }`
    /// - `Regex(key, pattern)` -> `ValueMatches { key, pattern }`
    /// - `GreaterThan`, `LessThan`, `Between` -> converted to regex patterns (approximation)
    /// - `StartsWith`, `EndsWith`, `Contains` -> converted to regex patterns
    /// - `And(a, b)` -> `And(vec![a, b])`
    /// - `Or(a, b)` -> `Or(vec![a, b])`
    /// - `Not(inner)` -> `Not(inner)`
    /// - `True` -> `HasKey("")` that always matches (approximation)
    /// - `False` -> `Not(HasKey(""))` that never matches (approximation)
    /// - Other complex types -> approximated or simplified
    pub fn to_policy(placement_expr: &PlacementTagExpr) -> PolicyTagExpr {
        match placement_expr {
            PlacementTagExpr::Has(key) => PolicyTagExpr::HasKey(key.clone()),

            PlacementTagExpr::Equals(key, value) => PolicyTagExpr::Equals {
                key: key.clone(),
                value: value.to_display_string(),
            },

            PlacementTagExpr::Regex(key, pattern) => PolicyTagExpr::ValueMatches {
                key: key.clone(),
                pattern: pattern.clone(),
            },

            PlacementTagExpr::GreaterThan(key, _n) => {
                // Approximate with a regex that could match numbers > n
                // This is lossy but provides some compatibility
                PolicyTagExpr::ValueMatches {
                    key: key.clone(),
                    pattern: ".*".to_string(), // Can't really express numeric comparison in regex
                }
            }

            PlacementTagExpr::LessThan(key, _n) => PolicyTagExpr::ValueMatches {
                key: key.clone(),
                pattern: ".*".to_string(),
            },

            PlacementTagExpr::Between(key, _min, _max) => PolicyTagExpr::ValueMatches {
                key: key.clone(),
                pattern: ".*".to_string(),
            },

            PlacementTagExpr::StartsWith(key, prefix) => PolicyTagExpr::ValueMatches {
                key: key.clone(),
                pattern: format!("^{}", regex::escape(prefix)),
            },

            PlacementTagExpr::EndsWith(key, suffix) => PolicyTagExpr::ValueMatches {
                key: key.clone(),
                pattern: format!("{}$", regex::escape(suffix)),
            },

            PlacementTagExpr::Contains(key, substring) => PolicyTagExpr::ValueMatches {
                key: key.clone(),
                pattern: regex::escape(substring),
            },

            PlacementTagExpr::Under(key, path) => {
                // Convert hierarchical path to a starts-with pattern
                let path_str = path.join(":");
                PolicyTagExpr::ValueMatches {
                    key: key.clone(),
                    pattern: format!("^{}", regex::escape(&path_str)),
                }
            }

            PlacementTagExpr::In(key, values) => {
                // Convert to OR of equals
                let equals: Vec<PolicyTagExpr> = values
                    .iter()
                    .map(|v| PolicyTagExpr::Equals {
                        key: key.clone(),
                        value: v.to_display_string(),
                    })
                    .collect();
                PolicyTagExpr::Or(equals)
            }

            PlacementTagExpr::And(left, right) => {
                PolicyTagExpr::And(vec![Self::to_policy(left), Self::to_policy(right)])
            }

            PlacementTagExpr::Or(left, right) => {
                PolicyTagExpr::Or(vec![Self::to_policy(left), Self::to_policy(right)])
            }

            PlacementTagExpr::Not(inner) => PolicyTagExpr::Not(Box::new(Self::to_policy(inner))),

            PlacementTagExpr::True => {
                // Approximate "always true" - this will match any tag set
                // Using HasKey with an empty expression list that's always true
                PolicyTagExpr::Or(vec![
                    PolicyTagExpr::HasKey("__always_true__".to_string()),
                    PolicyTagExpr::Not(Box::new(PolicyTagExpr::HasKey(
                        "__always_true__".to_string(),
                    ))),
                ])
            }

            PlacementTagExpr::False => {
                // Approximate "always false"
                PolicyTagExpr::And(vec![
                    PolicyTagExpr::HasKey("__never_match__".to_string()),
                    PolicyTagExpr::Not(Box::new(PolicyTagExpr::HasKey(
                        "__never_match__".to_string(),
                    ))),
                ])
            }
        }
    }

    /// Fold a vector of expressions into nested binary And
    fn fold_to_binary_and(exprs: &[PolicyTagExpr]) -> PlacementTagExpr {
        if exprs.is_empty() {
            return PlacementTagExpr::True;
        }
        if exprs.len() == 1 {
            return Self::to_placement(&exprs[0]);
        }

        let mut iter = exprs.iter();
        let first = Self::to_placement(iter.next().unwrap());

        iter.fold(first, |acc, expr| {
            PlacementTagExpr::And(Box::new(acc), Box::new(Self::to_placement(expr)))
        })
    }

    /// Fold a vector of expressions into nested binary Or
    fn fold_to_binary_or(exprs: &[PolicyTagExpr]) -> PlacementTagExpr {
        if exprs.is_empty() {
            return PlacementTagExpr::False;
        }
        if exprs.len() == 1 {
            return Self::to_placement(&exprs[0]);
        }

        let mut iter = exprs.iter();
        let first = Self::to_placement(iter.next().unwrap());

        iter.fold(first, |acc, expr| {
            PlacementTagExpr::Or(Box::new(acc), Box::new(Self::to_placement(expr)))
        })
    }
}

/// Implement From trait for policy TagExpr to placement TagExpr conversion
impl From<&PolicyTagExpr> for PlacementTagExpr {
    fn from(expr: &PolicyTagExpr) -> Self {
        PolicyTagExprAdapter::to_placement(expr)
    }
}

impl From<PolicyTagExpr> for PlacementTagExpr {
    fn from(expr: PolicyTagExpr) -> Self {
        PolicyTagExprAdapter::to_placement(&expr)
    }
}

/// Implement From trait for placement TagExpr to policy TagExpr conversion
impl From<&PlacementTagExpr> for PolicyTagExpr {
    fn from(expr: &PlacementTagExpr) -> Self {
        PolicyTagExprAdapter::to_policy(expr)
    }
}

impl From<PlacementTagExpr> for PolicyTagExpr {
    fn from(expr: PlacementTagExpr) -> Self {
        PolicyTagExprAdapter::to_policy(&expr)
    }
}

/// Helper to convert a placement NodeInfo to a policy NodeInfo
pub fn placement_node_to_policy_node(node: &placement::groups::NodeInfo) -> PolicyNodeInfo {
    use super::engine::NodeStatus as PolicyNodeStatus;
    use super::ir::TagSet as PolicyTagSet;

    let mut policy_tags = PolicyTagSet::new();
    for tag in node.tags.iter() {
        policy_tags.insert(tag.qualified_key(), tag.value.to_display_string());
    }

    let status = match node.status {
        placement::NodeStatus::Online => PolicyNodeStatus::Available,
        placement::NodeStatus::Offline => PolicyNodeStatus::Offline,
        placement::NodeStatus::Draining => PolicyNodeStatus::Draining,
        placement::NodeStatus::Maintenance => PolicyNodeStatus::Offline,
        placement::NodeStatus::Unknown => PolicyNodeStatus::Offline,
    };

    PolicyNodeInfo::new(&node.id.0)
        .with_tags(policy_tags)
        .with_status(status)
}

/// Helper to get available policy nodes from a placement NodeRegistry
pub fn get_available_policy_nodes(node_registry: &NodeRegistry) -> Vec<PolicyNodeInfo> {
    node_registry
        .all_nodes()
        .filter(|n| matches!(n.status, placement::NodeStatus::Online))
        .map(placement_node_to_policy_node)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::groups::{Group, NodeId};
    use crate::placement::tags::{Tag, TagNamespace, TagSet as PlacementTagSet, TagValue};
    use crate::policy::ir::TagSet as PolicyTagSet;

    fn create_test_placement_nodes() -> NodeRegistry {
        let mut registry = NodeRegistry::new();

        // Node 1: GPU node in group "ml-cluster"
        let mut tags1 = PlacementTagSet::new();
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
        registry.register(placement::groups::NodeInfo::with_tags("node-1", tags1));

        // Node 2: CPU node in building B
        let mut tags2 = PlacementTagSet::new();
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
        registry.register(placement::groups::NodeInfo::with_tags("node-2", tags2));

        // Node 3: GPU node in building A
        let mut tags3 = PlacementTagSet::new();
        tags3.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));
        tags3.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(256.0),
        ));
        tags3.insert(Tag::new(
            TagNamespace::Location,
            "building",
            TagValue::String("building-a".to_string()),
        ));
        registry.register(placement::groups::NodeInfo::with_tags("node-3", tags3));

        registry
    }

    fn create_test_groups(_node_registry: &NodeRegistry) -> GroupRegistry {
        let mut registry = GroupRegistry::new();

        // Create a dynamic group for GPU nodes
        let gpu_group = Group::dynamic(
            "gpu-nodes",
            "All nodes with GPUs",
            PlacementTagExpr::Has("hardware:gpu".into()),
        )
        .with_id("gpu-nodes");
        registry.register(gpu_group).unwrap();

        // Create an explicit group
        let ml_cluster = Group::explicit(
            "ml-cluster",
            "ML compute cluster",
            vec![NodeId::new("node-1"), NodeId::new("node-3")]
                .into_iter()
                .collect(),
        )
        .with_id("ml-cluster");
        registry.register(ml_cluster).unwrap();

        // Create a dynamic group for building A
        let building_a = Group::dynamic(
            "building-a-nodes",
            "Nodes in building A",
            PlacementTagExpr::Equals(
                "location:building".into(),
                TagValue::String("building-a".into()),
            ),
        )
        .with_id("building-a-nodes");
        registry.register(building_a).unwrap();

        registry
    }

    #[test]
    fn test_tag_expr_conversion_has_key() {
        let policy_expr = PolicyTagExpr::HasKey("hardware:gpu".to_string());
        let placement_expr: PlacementTagExpr = policy_expr.clone().into();

        assert!(matches!(placement_expr, PlacementTagExpr::Has(ref key) if key == "hardware:gpu"));

        // Round-trip
        let back: PolicyTagExpr = placement_expr.into();
        assert!(matches!(back, PolicyTagExpr::HasKey(ref key) if key == "hardware:gpu"));
    }

    #[test]
    fn test_tag_expr_conversion_equals() {
        let policy_expr = PolicyTagExpr::Equals {
            key: "env".to_string(),
            value: "production".to_string(),
        };
        let placement_expr: PlacementTagExpr = policy_expr.clone().into();

        assert!(matches!(
            placement_expr,
            PlacementTagExpr::Equals(key, PlacementTagValue::String(val))
            if key == "env" && val == "production"
        ));
    }

    #[test]
    fn test_tag_expr_conversion_and() {
        let policy_expr = PolicyTagExpr::And(vec![
            PolicyTagExpr::HasKey("hardware:gpu".to_string()),
            PolicyTagExpr::HasKey("hardware:memory".to_string()),
        ]);
        let placement_expr: PlacementTagExpr = policy_expr.into();

        // Should be And(Has("hardware:gpu"), Has("hardware:memory"))
        match placement_expr {
            PlacementTagExpr::And(left, right) => {
                assert!(matches!(*left, PlacementTagExpr::Has(ref k) if k == "hardware:gpu"));
                assert!(matches!(*right, PlacementTagExpr::Has(ref k) if k == "hardware:memory"));
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_tag_expr_conversion_value_matches() {
        let policy_expr = PolicyTagExpr::ValueMatches {
            key: "name".to_string(),
            pattern: "ml-.*".to_string(),
        };
        let placement_expr: PlacementTagExpr = policy_expr.into();

        assert!(matches!(
            placement_expr,
            PlacementTagExpr::Regex(key, pattern)
            if key == "name" && pattern == "ml-.*"
        ));
    }

    #[test]
    fn test_placement_to_policy_starts_with() {
        let placement_expr = PlacementTagExpr::StartsWith("env".to_string(), "prod".to_string());
        let policy_expr: PolicyTagExpr = placement_expr.into();

        match policy_expr {
            PolicyTagExpr::ValueMatches { key, pattern } => {
                assert_eq!(key, "env");
                assert!(pattern.starts_with("^"));
                assert!(pattern.contains("prod"));
            }
            _ => panic!("Expected ValueMatches expression"),
        }
    }

    #[test]
    fn test_placement_to_policy_in() {
        let placement_expr = PlacementTagExpr::In(
            "env".to_string(),
            vec![
                PlacementTagValue::String("dev".to_string()),
                PlacementTagValue::String("staging".to_string()),
            ],
        );
        let policy_expr: PolicyTagExpr = placement_expr.into();

        match policy_expr {
            PolicyTagExpr::Or(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected Or expression"),
        }
    }

    #[test]
    fn test_engine_with_placement_group_resolution() {
        let node_registry = Arc::new(RwLock::new(create_test_placement_nodes()));
        let group_registry = Arc::new(RwLock::new(create_test_groups(
            &node_registry.read().unwrap(),
        )));

        let mut engine = PolicyEngine::new();
        engine.with_placement(node_registry.clone(), group_registry.clone());

        // Create available nodes for policy engine
        let available_nodes = get_available_policy_nodes(&node_registry.read().unwrap());

        // Test resolving a group selector
        let selector = NodeSelector::Group("ml-cluster".to_string());
        let resolved = engine.resolve_node_selector(&selector, &available_nodes);

        assert_eq!(resolved.len(), 2);
        assert!(resolved.contains(&"node-1".to_string()));
        assert!(resolved.contains(&"node-3".to_string()));
    }

    #[test]
    fn test_engine_with_placement_dynamic_group() {
        let node_registry = Arc::new(RwLock::new(create_test_placement_nodes()));
        let group_registry = Arc::new(RwLock::new(create_test_groups(
            &node_registry.read().unwrap(),
        )));

        let mut engine = PolicyEngine::new();
        engine.with_placement(node_registry.clone(), group_registry.clone());

        let available_nodes = get_available_policy_nodes(&node_registry.read().unwrap());

        // Test resolving a dynamic group selector (gpu-nodes)
        let selector = NodeSelector::Group("gpu-nodes".to_string());
        let resolved = engine.resolve_node_selector(&selector, &available_nodes);

        assert_eq!(resolved.len(), 2);
        assert!(resolved.contains(&"node-1".to_string()));
        assert!(resolved.contains(&"node-3".to_string()));
    }

    #[test]
    fn test_engine_with_placement_tag_selector() {
        let node_registry = Arc::new(RwLock::new(create_test_placement_nodes()));
        let group_registry = Arc::new(RwLock::new(create_test_groups(
            &node_registry.read().unwrap(),
        )));

        let mut engine = PolicyEngine::new();
        engine.with_placement(node_registry.clone(), group_registry.clone());

        let available_nodes = get_available_policy_nodes(&node_registry.read().unwrap());

        // Test resolving a tag selector
        let selector = NodeSelector::Tag(PolicyTagExpr::HasKey("hardware:gpu".to_string()));
        let resolved = engine.resolve_node_selector(&selector, &available_nodes);

        assert_eq!(resolved.len(), 2);
        assert!(resolved.contains(&"node-1".to_string()));
        assert!(resolved.contains(&"node-3".to_string()));
    }

    #[test]
    fn test_placement_node_to_policy_node_conversion() {
        let mut tags = PlacementTagSet::new();
        tags.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));
        tags.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(128.0),
        ));

        let placement_node = placement::groups::NodeInfo::with_tags("test-node", tags);
        let policy_node = placement_node_to_policy_node(&placement_node);

        assert_eq!(policy_node.id, "test-node");
        assert!(policy_node.tags.contains_key("hardware:gpu"));
        assert!(policy_node.tags.contains_key("hardware:memory"));
    }

    #[test]
    fn test_fallback_without_placement_config() {
        // Test that the engine works without placement config
        let engine = PolicyEngine::new();

        let mut tags = PolicyTagSet::new();
        tags.insert("group", "test-group");

        let available_nodes = vec![
            PolicyNodeInfo::new("node-1").with_tags(tags.clone()),
            PolicyNodeInfo::new("node-2"),
        ];

        let selector = NodeSelector::Group("test-group".to_string());
        let resolved = engine.resolve_node_selector(&selector, &available_nodes);

        // Without placement config, falls back to checking "group" tag
        assert_eq!(resolved.len(), 1);
        assert!(resolved.contains(&"node-1".to_string()));
    }
}
