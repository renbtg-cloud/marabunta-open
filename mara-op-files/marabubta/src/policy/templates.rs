// Marabunta - Licensed under the MIT License.
//! Policy Templates
//!
//! Common policy patterns as pre-built templates with parameterization support.
//! Templates can be instantiated with custom parameters to create policies.

use chrono::Utc;
use std::collections::HashMap;

use super::ir::{
    AffinityScope, AffinityTarget, JobMatcher, NodeSelector, Policy, PolicyCondition, PolicyEffect,
    PolicyGovernance, ResourceMatcher, SubmitterMatcher, TagExpr,
};

/// A policy template that can be instantiated with parameters
#[derive(Debug, Clone)]
pub struct PolicyTemplate {
    /// Template identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Description of what this template does
    pub description: String,
    /// Category for organization
    pub category: TemplateCategory,
    /// Parameter definitions
    pub parameters: Vec<ParameterDef>,
    /// Factory function type
    factory: TemplateFactory,
}

/// Template categories for organization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateCategory {
    /// Priority-based scheduling
    Priority,
    /// Resource requirements
    Resources,
    /// Geographic/region preferences
    Region,
    /// Security and access control
    Security,
    /// Performance optimization
    Performance,
    /// Cost optimization
    Cost,
    /// Compliance and governance
    Compliance,
}

impl TemplateCategory {
    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            TemplateCategory::Priority => "Priority",
            TemplateCategory::Resources => "Resources",
            TemplateCategory::Region => "Region",
            TemplateCategory::Security => "Security",
            TemplateCategory::Performance => "Performance",
            TemplateCategory::Cost => "Cost",
            TemplateCategory::Compliance => "Compliance",
        }
    }
}

/// Parameter definition for a template
#[derive(Debug, Clone)]
pub struct ParameterDef {
    /// Parameter name
    pub name: String,
    /// Description
    pub description: String,
    /// Parameter type
    pub param_type: ParameterType,
    /// Whether this parameter is required
    pub required: bool,
    /// Default value (as JSON)
    pub default: Option<serde_json::Value>,
}

/// Parameter types
#[derive(Debug, Clone, PartialEq)]
pub enum ParameterType {
    String,
    Integer,
    Float,
    Boolean,
    StringList,
}

impl ParameterType {
    /// Get display name
    pub fn name(&self) -> &'static str {
        match self {
            ParameterType::String => "string",
            ParameterType::Integer => "integer",
            ParameterType::Float => "float",
            ParameterType::Boolean => "boolean",
            ParameterType::StringList => "string list",
        }
    }
}

/// Parameter values for template instantiation
pub type TemplateParams = HashMap<String, serde_json::Value>;

/// Factory function type for creating policies
type TemplateFactory = fn(&TemplateParams) -> Result<Policy, TemplateError>;

/// Errors that can occur during template instantiation
#[derive(Debug, Clone)]
pub enum TemplateError {
    /// Missing required parameter
    MissingParameter(String),
    /// Parameter type mismatch
    InvalidParameterType {
        name: String,
        expected: String,
        found: String,
    },
    /// Invalid parameter value
    InvalidParameterValue { name: String, message: String },
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemplateError::MissingParameter(name) => {
                write!(f, "missing required parameter: {}", name)
            }
            TemplateError::InvalidParameterType {
                name,
                expected,
                found,
            } => {
                write!(
                    f,
                    "parameter '{}': expected {}, found {}",
                    name, expected, found
                )
            }
            TemplateError::InvalidParameterValue { name, message } => {
                write!(f, "parameter '{}': {}", name, message)
            }
        }
    }
}

impl std::error::Error for TemplateError {}

impl PolicyTemplate {
    /// Instantiate this template with the given parameters
    pub fn instantiate(&self, params: &TemplateParams) -> Result<Policy, TemplateError> {
        // Validate required parameters
        for param in &self.parameters {
            if param.required && !params.contains_key(&param.name)
                && param.default.is_none() {
                    return Err(TemplateError::MissingParameter(param.name.clone()));
                }
        }

        // Merge with defaults
        let mut merged_params = TemplateParams::new();
        for param in &self.parameters {
            if let Some(value) = params.get(&param.name) {
                merged_params.insert(param.name.clone(), value.clone());
            } else if let Some(default) = &param.default {
                merged_params.insert(param.name.clone(), default.clone());
            }
        }

        // Call factory
        (self.factory)(&merged_params)
    }
}

// ============================================================================
// Template Registry
// ============================================================================

/// Registry of available policy templates
pub struct TemplateRegistry {
    templates: HashMap<String, PolicyTemplate>,
}

impl Default for TemplateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateRegistry {
    /// Create a new registry with built-in templates
    pub fn new() -> Self {
        let mut registry = Self {
            templates: HashMap::new(),
        };

        // Register built-in templates
        registry.register(high_priority_first());
        registry.register(region_affinity());
        registry.register(gpu_preference());
        registry.register(production_only());
        registry.register(business_hours());
        registry.register(spread_across_regions());
        registry.register(cost_optimization());
        registry.register(high_availability());
        registry.register(team_isolation());
        registry.register(resource_quota());

        registry
    }

    /// Register a template
    pub fn register(&mut self, template: PolicyTemplate) {
        self.templates.insert(template.id.clone(), template);
    }

    /// Get a template by ID
    pub fn get(&self, id: &str) -> Option<&PolicyTemplate> {
        self.templates.get(id)
    }

    /// List all templates
    pub fn list(&self) -> Vec<&PolicyTemplate> {
        self.templates.values().collect()
    }

    /// List templates by category
    pub fn list_by_category(&self, category: TemplateCategory) -> Vec<&PolicyTemplate> {
        self.templates
            .values()
            .filter(|t| t.category == category)
            .collect()
    }

    /// Instantiate a template by ID
    pub fn instantiate(
        &self,
        template_id: &str,
        params: &TemplateParams,
    ) -> Result<Policy, TemplateError> {
        let template =
            self.get(template_id)
                .ok_or_else(|| TemplateError::InvalidParameterValue {
                    name: "template_id".to_string(),
                    message: format!("unknown template: {}", template_id),
                })?;

        template.instantiate(params)
    }
}

// ============================================================================
// Built-in Templates
// ============================================================================

/// High Priority First template
/// Places high-priority jobs ahead of lower priority ones
pub fn high_priority_first() -> PolicyTemplate {
    PolicyTemplate {
        id: "high-priority-first".to_string(),
        name: "High Priority First".to_string(),
        description: "Jobs with priority above threshold get preferential node selection"
            .to_string(),
        category: TemplateCategory::Priority,
        parameters: vec![
            ParameterDef {
                name: "min_priority".to_string(),
                description: "Minimum priority threshold".to_string(),
                param_type: ParameterType::Integer,
                required: false,
                default: Some(serde_json::json!(50)),
            },
            ParameterDef {
                name: "weight".to_string(),
                description: "Preference weight (0.0-1.0)".to_string(),
                param_type: ParameterType::Float,
                required: false,
                default: Some(serde_json::json!(0.8)),
            },
        ],
        factory: |params| {
            let min_priority = params
                .get("min_priority")
                .and_then(|v| v.as_i64())
                .unwrap_or(50) as u32;
            let weight = params.get("weight").and_then(|v| v.as_f64()).unwrap_or(0.8);

            let now = Utc::now();
            Ok(Policy {
                id: format!("high-priority-first-{}", min_priority),
                name: format!("High Priority First (>{})", min_priority),
                description: format!(
                    "Jobs with priority > {} get preferred scheduling",
                    min_priority
                ),
                version: 1,
                condition: PolicyCondition::JobMatches(
                    JobMatcher::new().with_priority_range(min_priority + 1, u32::MAX),
                ),
                effects: vec![PolicyEffect::prefer(NodeSelector::All, weight)],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// Region Affinity template
/// Prefers or requires nodes in specific regions
pub fn region_affinity() -> PolicyTemplate {
    PolicyTemplate {
        id: "region-affinity".to_string(),
        name: "Region Affinity".to_string(),
        description: "Route jobs to specific regions".to_string(),
        category: TemplateCategory::Region,
        parameters: vec![
            ParameterDef {
                name: "region".to_string(),
                description: "Target region".to_string(),
                param_type: ParameterType::String,
                required: true,
                default: None,
            },
            ParameterDef {
                name: "require".to_string(),
                description: "Hard requirement (true) or soft preference (false)".to_string(),
                param_type: ParameterType::Boolean,
                required: false,
                default: Some(serde_json::json!(false)),
            },
            ParameterDef {
                name: "weight".to_string(),
                description: "Preference weight (for soft constraint)".to_string(),
                param_type: ParameterType::Float,
                required: false,
                default: Some(serde_json::json!(0.7)),
            },
        ],
        factory: |params| {
            let region = params
                .get("region")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TemplateError::MissingParameter("region".to_string()))?;
            let require = params
                .get("require")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let weight = params.get("weight").and_then(|v| v.as_f64()).unwrap_or(0.7);

            let selector = NodeSelector::Tag(TagExpr::equals("region", region));
            let effect = if require {
                PolicyEffect::require(selector)
            } else {
                PolicyEffect::prefer(selector, weight)
            };

            let now = Utc::now();
            Ok(Policy {
                id: format!("region-affinity-{}", region),
                name: format!("Region Affinity: {}", region),
                description: format!(
                    "{} jobs to run in region {}",
                    if require { "Require" } else { "Prefer" },
                    region
                ),
                version: 1,
                condition: PolicyCondition::Always,
                effects: vec![effect],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// GPU Preference template
/// Routes GPU-requiring jobs to GPU nodes
pub fn gpu_preference() -> PolicyTemplate {
    PolicyTemplate {
        id: "gpu-preference".to_string(),
        name: "GPU Preference".to_string(),
        description: "Jobs requiring GPUs are placed on GPU-enabled nodes".to_string(),
        category: TemplateCategory::Resources,
        parameters: vec![
            ParameterDef {
                name: "min_gpus".to_string(),
                description: "Minimum GPU count to trigger policy".to_string(),
                param_type: ParameterType::Integer,
                required: false,
                default: Some(serde_json::json!(1)),
            },
            ParameterDef {
                name: "require".to_string(),
                description: "Hard requirement (true) or soft preference (false)".to_string(),
                param_type: ParameterType::Boolean,
                required: false,
                default: Some(serde_json::json!(true)),
            },
        ],
        factory: |params| {
            let min_gpus = params.get("min_gpus").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
            let require = params
                .get("require")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            let selector = NodeSelector::Tag(TagExpr::equals("gpu", "true"));
            let effect = if require {
                PolicyEffect::require(selector)
            } else {
                PolicyEffect::prefer(selector, 0.9)
            };

            let now = Utc::now();
            Ok(Policy {
                id: format!("gpu-preference-{}", min_gpus),
                name: "GPU Job Routing".to_string(),
                description: format!(
                    "Jobs requiring {} or more GPUs {} GPU nodes",
                    min_gpus,
                    if require { "require" } else { "prefer" }
                ),
                version: 1,
                condition: PolicyCondition::ResourceMatches(
                    ResourceMatcher::new().with_gpu(true, Some(min_gpus)),
                ),
                effects: vec![effect],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// Production Only template
/// Restricts production workloads to production nodes
pub fn production_only() -> PolicyTemplate {
    PolicyTemplate {
        id: "production-only".to_string(),
        name: "Production Only".to_string(),
        description: "Production workloads run only on production nodes".to_string(),
        category: TemplateCategory::Security,
        parameters: vec![
            ParameterDef {
                name: "domain".to_string(),
                description: "Domain that triggers production policy".to_string(),
                param_type: ParameterType::String,
                required: false,
                default: Some(serde_json::json!("production")),
            },
            ParameterDef {
                name: "env_tag".to_string(),
                description: "Node tag value for production environment".to_string(),
                param_type: ParameterType::String,
                required: false,
                default: Some(serde_json::json!("production")),
            },
        ],
        factory: |params| {
            let domain = params
                .get("domain")
                .and_then(|v| v.as_str())
                .unwrap_or("production");
            let env_tag = params
                .get("env_tag")
                .and_then(|v| v.as_str())
                .unwrap_or("production");

            let now = Utc::now();
            Ok(Policy {
                id: format!("production-only-{}", domain),
                name: "Production Environment Isolation".to_string(),
                description: format!("Jobs from {} domain run only on {} nodes", domain, env_tag),
                version: 1,
                condition: PolicyCondition::SubmitterMatches(
                    SubmitterMatcher::new().with_domain(domain),
                ),
                effects: vec![PolicyEffect::require(NodeSelector::Tag(TagExpr::equals(
                    "env", env_tag,
                )))],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// Business Hours template
/// Time-based scheduling during business hours
pub fn business_hours() -> PolicyTemplate {
    PolicyTemplate {
        id: "business-hours".to_string(),
        name: "Business Hours".to_string(),
        description: "Schedule jobs differently during business hours".to_string(),
        category: TemplateCategory::Cost,
        parameters: vec![
            ParameterDef {
                name: "cron".to_string(),
                description: "Cron expression for business hours".to_string(),
                param_type: ParameterType::String,
                required: false,
                default: Some(serde_json::json!("0 9-17 * * MON-FRI")),
            },
            ParameterDef {
                name: "node_group".to_string(),
                description: "Node group to prefer during business hours".to_string(),
                param_type: ParameterType::String,
                required: false,
                default: Some(serde_json::json!("on-demand")),
            },
        ],
        factory: |params| {
            let cron = params
                .get("cron")
                .and_then(|v| v.as_str())
                .unwrap_or("0 9-17 * * MON-FRI");
            let node_group = params
                .get("node_group")
                .and_then(|v| v.as_str())
                .unwrap_or("on-demand");

            let now = Utc::now();
            Ok(Policy {
                id: "business-hours".to_string(),
                name: "Business Hours Scheduling".to_string(),
                description: format!(
                    "During business hours ({}), prefer {} nodes",
                    cron, node_group
                ),
                version: 1,
                condition: PolicyCondition::TimeWindow {
                    cron: cron.to_string(),
                },
                effects: vec![PolicyEffect::prefer(
                    NodeSelector::Group(node_group.to_string()),
                    0.7,
                )],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// Spread Across Regions template
/// Anti-affinity to spread jobs across regions for HA
pub fn spread_across_regions() -> PolicyTemplate {
    PolicyTemplate {
        id: "spread-across-regions".to_string(),
        name: "Spread Across Regions".to_string(),
        description: "Spread job tasks across different regions for high availability".to_string(),
        category: TemplateCategory::Performance,
        parameters: vec![
            ParameterDef {
                name: "job_pattern".to_string(),
                description: "Job name pattern to apply this policy to".to_string(),
                param_type: ParameterType::String,
                required: false,
                default: Some(serde_json::json!(".*")),
            },
            ParameterDef {
                name: "weight".to_string(),
                description: "Anti-affinity weight".to_string(),
                param_type: ParameterType::Float,
                required: false,
                default: Some(serde_json::json!(0.8)),
            },
        ],
        factory: |params| {
            let job_pattern = params
                .get("job_pattern")
                .and_then(|v| v.as_str())
                .unwrap_or(".*");
            let weight = params.get("weight").and_then(|v| v.as_f64()).unwrap_or(0.8);

            let now = Utc::now();
            Ok(Policy {
                id: "spread-across-regions".to_string(),
                name: "Regional Spread".to_string(),
                description: "Spread job tasks across regions".to_string(),
                version: 1,
                condition: PolicyCondition::JobMatches(
                    JobMatcher::new().with_name_pattern(job_pattern),
                ),
                effects: vec![PolicyEffect::anti_affinity(
                    AffinityTarget::SameJob,
                    AffinityScope::Region,
                    weight,
                )],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// Cost Optimization template
/// Prefer cheaper spot/preemptible nodes for batch jobs
pub fn cost_optimization() -> PolicyTemplate {
    PolicyTemplate {
        id: "cost-optimization".to_string(),
        name: "Cost Optimization".to_string(),
        description: "Route batch jobs to cheaper preemptible nodes".to_string(),
        category: TemplateCategory::Cost,
        parameters: vec![
            ParameterDef {
                name: "job_types".to_string(),
                description: "Job types eligible for cost optimization".to_string(),
                param_type: ParameterType::StringList,
                required: false,
                default: Some(serde_json::json!(["batch", "parameter_sweep"])),
            },
            ParameterDef {
                name: "node_type".to_string(),
                description: "Node type tag value (e.g., spot, preemptible)".to_string(),
                param_type: ParameterType::String,
                required: false,
                default: Some(serde_json::json!("spot")),
            },
        ],
        factory: |params| {
            let job_types: Vec<String> = params
                .get("job_types")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_else(|| vec!["batch".to_string(), "parameter_sweep".to_string()]);
            let node_type = params
                .get("node_type")
                .and_then(|v| v.as_str())
                .unwrap_or("spot");

            let now = Utc::now();
            Ok(Policy {
                id: "cost-optimization".to_string(),
                name: "Cost Optimization".to_string(),
                description: format!(
                    "Route {:?} jobs to {} nodes for cost savings",
                    job_types, node_type
                ),
                version: 1,
                condition: PolicyCondition::JobMatches(JobMatcher::new().with_job_types(job_types)),
                effects: vec![PolicyEffect::prefer(
                    NodeSelector::Tag(TagExpr::equals("instance_type", node_type)),
                    0.8,
                )],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// High Availability template
/// Ensure critical jobs have backup placement
pub fn high_availability() -> PolicyTemplate {
    PolicyTemplate {
        id: "high-availability".to_string(),
        name: "High Availability".to_string(),
        description: "Critical jobs spread across failure domains".to_string(),
        category: TemplateCategory::Performance,
        parameters: vec![
            ParameterDef {
                name: "min_priority".to_string(),
                description: "Minimum priority for HA treatment".to_string(),
                param_type: ParameterType::Integer,
                required: false,
                default: Some(serde_json::json!(80)),
            },
            ParameterDef {
                name: "disallow_preemption".to_string(),
                description: "Prevent preemption of HA jobs".to_string(),
                param_type: ParameterType::Boolean,
                required: false,
                default: Some(serde_json::json!(true)),
            },
        ],
        factory: |params| {
            let min_priority = params
                .get("min_priority")
                .and_then(|v| v.as_i64())
                .unwrap_or(80) as u32;
            let disallow_preemption = params
                .get("disallow_preemption")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            let mut effects = vec![PolicyEffect::anti_affinity(
                AffinityTarget::SameJob,
                AffinityScope::Rack,
                0.9,
            )];

            if disallow_preemption {
                effects.push(PolicyEffect::DisallowPreemption);
            }

            let now = Utc::now();
            Ok(Policy {
                id: format!("high-availability-{}", min_priority),
                name: "High Availability".to_string(),
                description: format!("High priority jobs (>{}) get HA treatment", min_priority),
                version: 1,
                condition: PolicyCondition::JobMatches(
                    JobMatcher::new().with_priority_range(min_priority + 1, u32::MAX),
                ),
                effects,
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// Team Isolation template
/// Keep team workloads on dedicated nodes
pub fn team_isolation() -> PolicyTemplate {
    PolicyTemplate {
        id: "team-isolation".to_string(),
        name: "Team Isolation".to_string(),
        description: "Isolate team workloads on dedicated node groups".to_string(),
        category: TemplateCategory::Security,
        parameters: vec![
            ParameterDef {
                name: "team".to_string(),
                description: "Team domain identifier".to_string(),
                param_type: ParameterType::String,
                required: true,
                default: None,
            },
            ParameterDef {
                name: "node_group".to_string(),
                description: "Dedicated node group for the team".to_string(),
                param_type: ParameterType::String,
                required: true,
                default: None,
            },
            ParameterDef {
                name: "require".to_string(),
                description: "Hard requirement or soft preference".to_string(),
                param_type: ParameterType::Boolean,
                required: false,
                default: Some(serde_json::json!(true)),
            },
        ],
        factory: |params| {
            let team = params
                .get("team")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TemplateError::MissingParameter("team".to_string()))?;
            let node_group = params
                .get("node_group")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TemplateError::MissingParameter("node_group".to_string()))?;
            let require = params
                .get("require")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            let selector = NodeSelector::Group(node_group.to_string());
            let effect = if require {
                PolicyEffect::require(selector)
            } else {
                PolicyEffect::prefer(selector, 0.9)
            };

            let now = Utc::now();
            Ok(Policy {
                id: format!("team-isolation-{}", team),
                name: format!("Team Isolation: {}", team),
                description: format!(
                    "Jobs from {} {} {} nodes",
                    team,
                    if require { "require" } else { "prefer" },
                    node_group
                ),
                version: 1,
                condition: PolicyCondition::SubmitterMatches(
                    SubmitterMatcher::new().with_domain(team),
                ),
                effects: vec![effect],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

/// Resource Quota template
/// Apply quota charges based on resource usage
pub fn resource_quota() -> PolicyTemplate {
    PolicyTemplate {
        id: "resource-quota".to_string(),
        name: "Resource Quota".to_string(),
        description: "Charge resource usage against quotas".to_string(),
        category: TemplateCategory::Cost,
        parameters: vec![
            ParameterDef {
                name: "quota_id".to_string(),
                description: "Quota identifier".to_string(),
                param_type: ParameterType::String,
                required: true,
                default: None,
            },
            ParameterDef {
                name: "domain".to_string(),
                description: "Domain to apply quota to".to_string(),
                param_type: ParameterType::String,
                required: true,
                default: None,
            },
            ParameterDef {
                name: "multiplier".to_string(),
                description: "Charge multiplier".to_string(),
                param_type: ParameterType::Float,
                required: false,
                default: Some(serde_json::json!(1.0)),
            },
        ],
        factory: |params| {
            let quota_id = params
                .get("quota_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TemplateError::MissingParameter("quota_id".to_string()))?;
            let domain = params
                .get("domain")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TemplateError::MissingParameter("domain".to_string()))?;
            let multiplier = params
                .get("multiplier")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);

            let now = Utc::now();
            Ok(Policy {
                id: format!("resource-quota-{}-{}", domain, quota_id),
                name: format!("Resource Quota: {}", quota_id),
                description: format!(
                    "Charge {} jobs against quota {} at {}x rate",
                    domain, quota_id, multiplier
                ),
                version: 1,
                condition: PolicyCondition::SubmitterMatches(
                    SubmitterMatcher::new().with_domain(domain),
                ),
                effects: vec![PolicyEffect::ChargeQuota {
                    quota_id: quota_id.to_string(),
                    multiplier,
                }],
                governance: PolicyGovernance::default(),
                enabled: true,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = TemplateRegistry::new();
        assert!(!registry.templates.is_empty());
    }

    #[test]
    fn test_get_template() {
        let registry = TemplateRegistry::new();
        let template = registry.get("high-priority-first");
        assert!(template.is_some());
    }

    #[test]
    fn test_list_by_category() {
        let registry = TemplateRegistry::new();
        let priority_templates = registry.list_by_category(TemplateCategory::Priority);
        assert!(!priority_templates.is_empty());
    }

    #[test]
    fn test_instantiate_high_priority() {
        let registry = TemplateRegistry::new();
        let mut params = TemplateParams::new();
        params.insert("min_priority".to_string(), serde_json::json!(75));
        params.insert("weight".to_string(), serde_json::json!(0.9));

        let policy = registry
            .instantiate("high-priority-first", &params)
            .unwrap();
        assert!(policy.id.contains("75"));
    }

    #[test]
    fn test_instantiate_region_affinity() {
        let registry = TemplateRegistry::new();
        let mut params = TemplateParams::new();
        params.insert("region".to_string(), serde_json::json!("us-east"));
        params.insert("require".to_string(), serde_json::json!(true));

        let policy = registry.instantiate("region-affinity", &params).unwrap();
        assert!(matches!(policy.effects[0], PolicyEffect::Require { .. }));
    }

    #[test]
    fn test_missing_required_parameter() {
        let registry = TemplateRegistry::new();
        let params = TemplateParams::new();

        let result = registry.instantiate("region-affinity", &params);
        assert!(result.is_err());
    }

    #[test]
    fn test_default_parameters() {
        let registry = TemplateRegistry::new();
        let params = TemplateParams::new();

        let policy = registry
            .instantiate("high-priority-first", &params)
            .unwrap();
        // Should use default min_priority of 50
        assert!(policy.id.contains("50"));
    }

    #[test]
    fn test_gpu_preference() {
        let registry = TemplateRegistry::new();
        let mut params = TemplateParams::new();
        params.insert("min_gpus".to_string(), serde_json::json!(2));

        let policy = registry.instantiate("gpu-preference", &params).unwrap();
        assert!(matches!(
            policy.condition,
            PolicyCondition::ResourceMatches(_)
        ));
    }

    #[test]
    fn test_team_isolation() {
        let registry = TemplateRegistry::new();
        let mut params = TemplateParams::new();
        params.insert("team".to_string(), serde_json::json!("ml-team"));
        params.insert("node_group".to_string(), serde_json::json!("ml-cluster"));

        let policy = registry.instantiate("team-isolation", &params).unwrap();
        assert!(policy.id.contains("ml-team"));
    }
}
