// Marabunta - Licensed under the MIT License.
//! Policy Intermediate Representation (IR)
//!
//! This is the core representation that all policy formats (DSL, visual, Python, etc.)
//! compile to. It provides a unified, type-safe representation of scheduling policies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Unique identifier for a policy
pub type PolicyId = String;

/// A scheduling policy that defines conditions and effects for job placement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// Unique identifier
    pub id: PolicyId,
    /// Human-readable name
    pub name: String,
    /// Detailed description of what this policy does
    pub description: String,
    /// Version number for tracking changes
    pub version: u32,

    /// When does this policy apply?
    pub condition: PolicyCondition,

    /// What does it do when applied?
    pub effects: Vec<PolicyEffect>,

    /// Governance metadata
    pub governance: PolicyGovernance,

    /// Whether this policy is currently active
    pub enabled: bool,
    /// When this policy was created
    pub created_at: DateTime<Utc>,
    /// When this policy was last updated
    pub updated_at: DateTime<Utc>,
    /// Optional expiration time
    pub expires_at: Option<DateTime<Utc>>,
}

impl Policy {
    /// Create a new policy with the given ID and name
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            name: name.into(),
            description: String::new(),
            version: 1,
            condition: PolicyCondition::Always,
            effects: Vec::new(),
            governance: PolicyGovernance::default(),
            enabled: true,
            created_at: now,
            updated_at: now,
            expires_at: None,
        }
    }

    /// Check if this policy is currently valid (enabled and not expired)
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.enabled && self.expires_at.map_or(true, |exp| now < exp)
    }

    /// Add an effect to this policy
    pub fn with_effect(mut self, effect: PolicyEffect) -> Self {
        self.effects.push(effect);
        self
    }

    /// Set the condition for this policy
    pub fn with_condition(mut self, condition: PolicyCondition) -> Self {
        self.condition = condition;
        self
    }

    /// Set the governance metadata
    pub fn with_governance(mut self, governance: PolicyGovernance) -> Self {
        self.governance = governance;
        self
    }
}

/// Conditions for when a policy applies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PolicyCondition {
    /// Always applies
    Always,

    /// Never applies (disabled)
    Never,

    /// Job matches criteria
    JobMatches(JobMatcher),

    /// Submitter matches criteria
    SubmitterMatches(SubmitterMatcher),

    /// Time-based condition using cron expression
    TimeWindow {
        /// Cron expression (e.g., "0 9-17 * * MON-FRI" for business hours)
        cron: String,
    },

    /// Resource request matches
    ResourceMatches(ResourceMatcher),

    /// Logical AND of multiple conditions
    And(Vec<PolicyCondition>),

    /// Logical OR of multiple conditions
    Or(Vec<PolicyCondition>),

    /// Logical NOT of a condition
    Not(Box<PolicyCondition>),
}

impl PolicyCondition {
    /// Create an AND condition
    pub fn and(conditions: Vec<PolicyCondition>) -> Self {
        PolicyCondition::And(conditions)
    }

    /// Create an OR condition
    pub fn or(conditions: Vec<PolicyCondition>) -> Self {
        PolicyCondition::Or(conditions)
    }

    /// Create a NOT condition
    pub fn not(condition: PolicyCondition) -> Self {
        PolicyCondition::Not(Box::new(condition))
    }
}

/// Matcher for job attributes
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct JobMatcher {
    /// Regex pattern for job name
    pub name_pattern: Option<String>,
    /// Tag expression that job tags must match
    pub tags: Option<TagExpr>,
    /// Job types to match (monte_carlo, parameter_sweep, etc.)
    pub job_type: Option<Vec<String>>,
    /// Priority range (min, max) inclusive
    pub priority_range: Option<(u32, u32)>,
}

impl JobMatcher {
    /// Create a new empty job matcher
    pub fn new() -> Self {
        Self::default()
    }

    /// Match by job name pattern
    pub fn with_name_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.name_pattern = Some(pattern.into());
        self
    }

    /// Match by tags
    pub fn with_tags(mut self, tags: TagExpr) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Match by job type
    pub fn with_job_types(mut self, types: Vec<String>) -> Self {
        self.job_type = Some(types);
        self
    }

    /// Match by priority range
    pub fn with_priority_range(mut self, min: u32, max: u32) -> Self {
        self.priority_range = Some((min, max));
        self
    }
}

/// Matcher for submitter attributes
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct SubmitterMatcher {
    /// Match a specific principal ID
    pub principal_id: Option<String>,
    /// Regex pattern for principal ID
    pub principal_pattern: Option<String>,
    /// Submitter must have authority in this domain
    pub in_domain: Option<String>,
    /// Minimum priority level required
    pub min_priority: Option<u32>,
}

impl SubmitterMatcher {
    /// Create a new empty submitter matcher
    pub fn new() -> Self {
        Self::default()
    }

    /// Match a specific principal
    pub fn with_principal(mut self, id: impl Into<String>) -> Self {
        self.principal_id = Some(id.into());
        self
    }

    /// Match principals by pattern
    pub fn with_principal_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.principal_pattern = Some(pattern.into());
        self
    }

    /// Match principals in a domain
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.in_domain = Some(domain.into());
        self
    }

    /// Match principals with minimum priority
    pub fn with_min_priority(mut self, priority: u32) -> Self {
        self.min_priority = Some(priority);
        self
    }
}

/// Matcher for resource requests
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ResourceMatcher {
    /// Minimum CPU cores
    pub min_cpu: Option<u32>,
    /// Maximum CPU cores
    pub max_cpu: Option<u32>,
    /// Minimum memory in GB
    pub min_memory_gb: Option<f64>,
    /// Maximum memory in GB
    pub max_memory_gb: Option<f64>,
    /// Whether GPU is required
    pub requires_gpu: Option<bool>,
    /// Minimum number of GPUs
    pub min_gpu: Option<u32>,
}

impl ResourceMatcher {
    /// Create a new empty resource matcher
    pub fn new() -> Self {
        Self::default()
    }

    /// Match by CPU range
    pub fn with_cpu_range(mut self, min: Option<u32>, max: Option<u32>) -> Self {
        self.min_cpu = min;
        self.max_cpu = max;
        self
    }

    /// Match by memory range
    pub fn with_memory_range(mut self, min: Option<f64>, max: Option<f64>) -> Self {
        self.min_memory_gb = min;
        self.max_memory_gb = max;
        self
    }

    /// Match by GPU requirement
    pub fn with_gpu(mut self, requires: bool, min_count: Option<u32>) -> Self {
        self.requires_gpu = Some(requires);
        self.min_gpu = min_count;
        self
    }
}

/// Tag expression for matching tags
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TagExpr {
    /// Tag must have this key with any value
    HasKey(String),
    /// Tag must have this exact key-value pair
    Equals { key: String, value: String },
    /// Tag key must match regex pattern
    KeyMatches { key: String, pattern: String },
    /// Tag value must match regex pattern for given key
    ValueMatches { key: String, pattern: String },
    /// Logical AND of tag expressions
    And(Vec<TagExpr>),
    /// Logical OR of tag expressions
    Or(Vec<TagExpr>),
    /// Logical NOT of a tag expression
    Not(Box<TagExpr>),
}

impl TagExpr {
    /// Create an expression that checks for a tag key
    pub fn has_key(key: impl Into<String>) -> Self {
        TagExpr::HasKey(key.into())
    }

    /// Create an expression that checks for exact key-value match
    pub fn equals(key: impl Into<String>, value: impl Into<String>) -> Self {
        TagExpr::Equals {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Create an AND expression
    pub fn and(exprs: Vec<TagExpr>) -> Self {
        TagExpr::And(exprs)
    }

    /// Create an OR expression
    pub fn or(exprs: Vec<TagExpr>) -> Self {
        TagExpr::Or(exprs)
    }

    /// Create a NOT expression
    pub fn not(expr: TagExpr) -> Self {
        TagExpr::Not(Box::new(expr))
    }
}

/// A set of tags (key-value pairs)
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TagSet {
    tags: HashMap<String, String>,
}

impl TagSet {
    /// Create a new empty tag set
    pub fn new() -> Self {
        Self {
            tags: HashMap::new(),
        }
    }

    /// Insert a tag
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.tags.insert(key.into(), value.into());
    }

    /// Get a tag value by key
    pub fn get(&self, key: &str) -> Option<&String> {
        self.tags.get(key)
    }

    /// Check if a key exists
    pub fn contains_key(&self, key: &str) -> bool {
        self.tags.contains_key(key)
    }

    /// Get all keys
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.tags.keys()
    }

    /// Get all key-value pairs
    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.tags.iter()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Get the number of tags
    pub fn len(&self) -> usize {
        self.tags.len()
    }
}

impl From<HashMap<String, String>> for TagSet {
    fn from(tags: HashMap<String, String>) -> Self {
        Self { tags }
    }
}

impl FromIterator<(String, String)> for TagSet {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self {
            tags: iter.into_iter().collect(),
        }
    }
}

/// Effects - what a policy does when applied
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PolicyEffect {
    /// Prefer nodes matching selector (soft constraint)
    Prefer {
        selector: NodeSelector,
        /// Weight from 0.0 to 1.0
        weight: f64,
    },

    /// Require nodes matching selector (hard constraint)
    Require { selector: NodeSelector },

    /// Exclude nodes matching selector (hard constraint)
    Exclude { selector: NodeSelector },

    /// Co-locate with other jobs/tasks
    Affinity {
        with: AffinityTarget,
        scope: AffinityScope,
        /// Weight from 0.0 to 1.0
        weight: f64,
    },

    /// Spread away from other jobs/tasks
    AntiAffinity {
        with: AffinityTarget,
        scope: AffinityScope,
        /// Weight from 0.0 to 1.0
        weight: f64,
    },

    /// Set a resource limit
    SetResourceLimit { resource: ResourceType, limit: f64 },

    /// Adjust job priority
    SetPriority {
        /// Priority value (can be negative for deprioritization)
        priority: i32,
        mode: PriorityMode,
    },

    /// Charge against a quota
    ChargeQuota {
        quota_id: String,
        /// Multiplier for charge (1.0 = normal, 2.0 = double charge)
        multiplier: f64,
    },

    /// Allow this job to be preempted
    AllowPreemption {
        /// Minimum priority of jobs that can preempt this one
        by_min_priority: u32,
    },

    /// Disallow preemption of this job
    DisallowPreemption,

    /// Custom scoring function for advanced users
    CustomScore {
        /// Reference to a registered scoring function
        function_id: String,
        /// Parameters for the scoring function
        params: HashMap<String, serde_json::Value>,
    },
}

impl PolicyEffect {
    /// Create a prefer effect with the given selector and weight
    pub fn prefer(selector: NodeSelector, weight: f64) -> Self {
        PolicyEffect::Prefer {
            selector,
            weight: weight.clamp(0.0, 1.0),
        }
    }

    /// Create a require effect
    pub fn require(selector: NodeSelector) -> Self {
        PolicyEffect::Require { selector }
    }

    /// Create an exclude effect
    pub fn exclude(selector: NodeSelector) -> Self {
        PolicyEffect::Exclude { selector }
    }

    /// Create an affinity effect
    pub fn affinity(with: AffinityTarget, scope: AffinityScope, weight: f64) -> Self {
        PolicyEffect::Affinity {
            with,
            scope,
            weight: weight.clamp(0.0, 1.0),
        }
    }

    /// Create an anti-affinity effect
    pub fn anti_affinity(with: AffinityTarget, scope: AffinityScope, weight: f64) -> Self {
        PolicyEffect::AntiAffinity {
            with,
            scope,
            weight: weight.clamp(0.0, 1.0),
        }
    }
}

/// Selector for matching nodes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeSelector {
    /// Nodes matching tag expression
    Tag(TagExpr),
    /// Nodes in a named group
    Group(String),
    /// Specific node IDs
    NodeIds(Vec<String>),
    /// All nodes
    All,
}

impl NodeSelector {
    /// Create a selector that matches all nodes
    pub fn all() -> Self {
        NodeSelector::All
    }

    /// Create a selector for specific node IDs
    pub fn node_ids(ids: Vec<String>) -> Self {
        NodeSelector::NodeIds(ids)
    }

    /// Create a selector for a node group
    pub fn group(name: impl Into<String>) -> Self {
        NodeSelector::Group(name.into())
    }

    /// Create a selector based on tags
    pub fn tag(expr: TagExpr) -> Self {
        NodeSelector::Tag(expr)
    }
}

/// Target for affinity/anti-affinity rules
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AffinityTarget {
    /// Other tasks in the same job
    SameJob,
    /// Jobs matching a name pattern
    JobPattern(String),
    /// Jobs with matching tags
    Tag(TagExpr),
}

/// Scope for affinity rules (topology level)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AffinityScope {
    /// Same physical node
    Node,
    /// Same rack
    Rack,
    /// Same building/data center
    Building,
    /// Same region
    Region,
    /// Custom topology level
    Custom(String),
}

impl AffinityScope {
    /// Get the hierarchy level (lower = more specific)
    pub fn level(&self) -> u32 {
        match self {
            AffinityScope::Node => 0,
            AffinityScope::Rack => 1,
            AffinityScope::Building => 2,
            AffinityScope::Region => 3,
            AffinityScope::Custom(_) => 4,
        }
    }
}

/// Types of resources that can be constrained
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Cpu,
    Memory,
    Gpu,
    Disk,
    Network,
    Custom(String),
}

/// How to apply priority changes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PriorityMode {
    /// Set to this exact value
    Set,
    /// Add to current priority
    Add,
    /// Multiply current priority
    Multiply,
    /// Take max of current and this value
    Max,
    /// Take min of current and this value
    Min,
}

/// Governance metadata for a policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyGovernance {
    /// Who created/owns this policy
    pub author: String,
    /// Domain this policy has authority over
    pub authority_domain: String,
    /// Override behavior
    pub override_policy: OverridePolicyRef,
    /// Priority for conflict resolution (higher = wins)
    pub conflict_priority: i32,
}

impl Default for PolicyGovernance {
    fn default() -> Self {
        Self {
            author: String::from("system"),
            authority_domain: String::from("*"),
            override_policy: OverridePolicyRef::Advisory,
            conflict_priority: 0,
        }
    }
}

impl PolicyGovernance {
    /// Create new governance with the given author
    pub fn new(author: impl Into<String>) -> Self {
        Self {
            author: author.into(),
            ..Default::default()
        }
    }

    /// Set the authority domain
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.authority_domain = domain.into();
        self
    }

    /// Set the override policy
    pub fn with_override_policy(mut self, policy: OverridePolicyRef) -> Self {
        self.override_policy = policy;
        self
    }

    /// Set the conflict priority
    pub fn with_conflict_priority(mut self, priority: i32) -> Self {
        self.conflict_priority = priority;
        self
    }
}

/// Reference to an override policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OverridePolicyRef {
    /// Cannot be overridden
    Mandatory,
    /// Can be overridden by policies with at least this priority
    Blueprint { min_priority: u32 },
    /// Suggestions only, can always be overridden
    Advisory,
    /// Requires approval from specified approver
    RequiresApproval { approver_spec: String },
    /// Custom override policy referenced by ID
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_creation() {
        let policy = Policy::new("test-policy", "Test Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        assert_eq!(policy.id, "test-policy");
        assert_eq!(policy.name, "Test Policy");
        assert!(policy.enabled);
        assert_eq!(policy.effects.len(), 1);
    }

    #[test]
    fn test_policy_is_active() {
        let now = Utc::now();
        let mut policy = Policy::new("test", "Test");

        // Active by default
        assert!(policy.is_active(now));

        // Disabled
        policy.enabled = false;
        assert!(!policy.is_active(now));

        // Enabled but expired
        policy.enabled = true;
        policy.expires_at = Some(now - chrono::Duration::hours(1));
        assert!(!policy.is_active(now));

        // Enabled and not expired
        policy.expires_at = Some(now + chrono::Duration::hours(1));
        assert!(policy.is_active(now));
    }

    #[test]
    fn test_tag_set() {
        let mut tags = TagSet::new();
        tags.insert("env", "production");
        tags.insert("team", "platform");

        assert!(tags.contains_key("env"));
        assert_eq!(tags.get("env"), Some(&String::from("production")));
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn test_tag_expr() {
        let expr = TagExpr::and(vec![
            TagExpr::has_key("env"),
            TagExpr::equals("region", "us-east"),
        ]);

        match expr {
            TagExpr::And(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_job_matcher() {
        let matcher = JobMatcher::new()
            .with_name_pattern("ml-.*")
            .with_job_types(vec!["monte_carlo".to_string()])
            .with_priority_range(0, 100);

        assert_eq!(matcher.name_pattern, Some("ml-.*".to_string()));
        assert_eq!(matcher.priority_range, Some((0, 100)));
    }

    #[test]
    fn test_policy_effect() {
        // Test weight clamping
        let effect = PolicyEffect::prefer(NodeSelector::all(), 1.5);
        match effect {
            PolicyEffect::Prefer { weight, .. } => {
                assert_eq!(weight, 1.0); // Clamped to max
            }
            _ => panic!("Expected Prefer effect"),
        }

        let effect = PolicyEffect::prefer(NodeSelector::all(), -0.5);
        match effect {
            PolicyEffect::Prefer { weight, .. } => {
                assert_eq!(weight, 0.0); // Clamped to min
            }
            _ => panic!("Expected Prefer effect"),
        }
    }

    #[test]
    fn test_affinity_scope_level() {
        assert!(AffinityScope::Node.level() < AffinityScope::Rack.level());
        assert!(AffinityScope::Rack.level() < AffinityScope::Building.level());
        assert!(AffinityScope::Building.level() < AffinityScope::Region.level());
    }

    #[test]
    fn test_condition_combinators() {
        let cond = PolicyCondition::and(vec![
            PolicyCondition::JobMatches(JobMatcher::new().with_priority_range(0, 50)),
            PolicyCondition::not(PolicyCondition::SubmitterMatches(
                SubmitterMatcher::new().with_domain("admin"),
            )),
        ]);

        match cond {
            PolicyCondition::And(conditions) => {
                assert_eq!(conditions.len(), 2);
                match &conditions[1] {
                    PolicyCondition::Not(inner) => {
                        assert!(matches!(**inner, PolicyCondition::SubmitterMatches(_)));
                    }
                    _ => panic!("Expected Not condition"),
                }
            }
            _ => panic!("Expected And condition"),
        }
    }
}
