// Marabunta - Licensed under the MIT License.
//! Policy DSL Compiler
//!
//! Compiles the DSL AST into the Policy IR for execution by the policy engine.

use chrono::Utc;

use super::ast::*;
use super::error::{DslError, DslErrorKind, DslResult};
use crate::policy::ir::{
    AffinityScope, AffinityTarget, JobMatcher, NodeSelector, Policy, PolicyCondition,
    PolicyEffect, PolicyGovernance, PriorityMode, ResourceMatcher, SubmitterMatcher, TagExpr,
};

/// Compiles DSL AST to Policy IR
pub struct Compiler {
    /// Counter for generating unique policy IDs
    policy_counter: u32,
    /// Default governance settings
    default_governance: PolicyGovernance,
}

impl Default for Compiler {
    fn default() -> Self {
        Self::new()
    }
}

impl Compiler {
    /// Create a new compiler
    pub fn new() -> Self {
        Self {
            policy_counter: 0,
            default_governance: PolicyGovernance::default(),
        }
    }

    /// Set default governance for compiled policies
    pub fn with_governance(mut self, governance: PolicyGovernance) -> Self {
        self.default_governance = governance;
        self
    }

    /// Compile a policy AST to IR
    pub fn compile(&mut self, ast: &PolicyAst) -> DslResult<Vec<Policy>> {
        let mut policies = Vec::new();

        for rule in &ast.rules {
            let policy = self.compile_rule(&rule.value, ast)?;
            policies.push(policy);
        }

        Ok(policies)
    }

    /// Compile a single rule to a Policy
    fn compile_rule(&mut self, rule: &RuleAst, ast: &PolicyAst) -> DslResult<Policy> {
        self.policy_counter += 1;

        // Generate policy ID and name
        let id = ast
            .id
            .as_ref()
            .map(|s| format!("{}-{}", s.value, self.policy_counter))
            .unwrap_or_else(|| format!("dsl-policy-{}", self.policy_counter));

        let name = ast
            .name
            .as_ref()
            .map(|s| s.value.clone())
            .unwrap_or_else(|| format!("DSL Policy {}", self.policy_counter));

        // Compile condition
        let condition = self.compile_condition(&rule.condition.value)?;

        // Compile effects
        let effects: DslResult<Vec<_>> = rule
            .effects
            .iter()
            .map(|e| self.compile_effect(&e.value))
            .collect();
        let effects = effects?;

        let now = Utc::now();
        Ok(Policy {
            id,
            name,
            description: ast
                .description
                .as_ref()
                .map(|s| s.value.clone())
                .unwrap_or_default(),
            version: 1,
            condition,
            effects,
            governance: self.default_governance.clone(),
            enabled: true,
            created_at: now,
            updated_at: now,
            expires_at: None,
        })
    }

    /// Compile a condition AST to PolicyCondition
    fn compile_condition(&self, cond: &ConditionAst) -> DslResult<PolicyCondition> {
        match cond {
            ConditionAst::Always => Ok(PolicyCondition::Always),
            ConditionAst::Never => Ok(PolicyCondition::Never),

            ConditionAst::Comparison {
                left,
                operator,
                right,
            } => self.compile_comparison(&left.value, &operator.value, &right.value),

            ConditionAst::And(conds) => {
                let compiled: DslResult<Vec<_>> = conds
                    .iter()
                    .map(|c| self.compile_condition(&c.value))
                    .collect();
                Ok(PolicyCondition::And(compiled?))
            }

            ConditionAst::Or(conds) => {
                let compiled: DslResult<Vec<_>> = conds
                    .iter()
                    .map(|c| self.compile_condition(&c.value))
                    .collect();
                Ok(PolicyCondition::Or(compiled?))
            }

            ConditionAst::Not(inner) => {
                let compiled = self.compile_condition(&inner.value)?;
                Ok(PolicyCondition::Not(Box::new(compiled)))
            }

            ConditionAst::Has { object, key } => {
                self.compile_has_condition(&object.value, &key.value)
            }

            ConditionAst::Contains { object, value } => {
                self.compile_contains_condition(&object.value, &value.value)
            }

            ConditionAst::Matches { object, pattern } => {
                self.compile_matches_condition(&object.value, &pattern.value)
            }

            ConditionAst::Between { object, low, high } => {
                self.compile_between_condition(&object.value, &low.value, &high.value)
            }

            ConditionAst::In { object, values } => self.compile_in_condition(&object.value, values),

            ConditionAst::TimeWindow { cron } => Ok(PolicyCondition::TimeWindow {
                cron: cron.value.clone(),
            }),

            ConditionAst::Grouped(inner) => self.compile_condition(&inner.value),
        }
    }

    /// Compile a comparison to PolicyCondition
    fn compile_comparison(
        &self,
        left: &ExprAst,
        op: &ComparisonOp,
        right: &ExprAst,
    ) -> DslResult<PolicyCondition> {
        // Extract object access from left side
        let access = match left {
            ExprAst::Access(acc) => acc,
            _ => {
                return Err(DslError::new(DslErrorKind::InvalidSyntax(
                    "left side of comparison must be an object property".to_string(),
                )));
            }
        };

        // Determine the matcher type based on object type
        match access.object_type {
            ObjectType::Job => self.compile_job_comparison(access, op, right),
            ObjectType::Submitter => self.compile_submitter_comparison(access, op, right),
            ObjectType::Resource => self.compile_resource_comparison(access, op, right),
            ObjectType::Node => {
                // Node comparisons in conditions don't make sense (conditions apply to jobs)
                // This might be a selector used in wrong context
                Err(DslError::new(DslErrorKind::InvalidSyntax(
                    "node properties cannot be used in conditions, only in selectors".to_string(),
                )))
            }
            _ => Err(DslError::new(DslErrorKind::UnsupportedFeature(format!(
                "comparisons on {} objects",
                access.object_type.name()
            )))),
        }
    }

    fn compile_job_comparison(
        &self,
        access: &ObjectAccess,
        op: &ComparisonOp,
        right: &ExprAst,
    ) -> DslResult<PolicyCondition> {
        let property = access
            .properties
            .first()
            .map(|p| p.value.as_str())
            .unwrap_or("");

        let mut matcher = JobMatcher::new();

        match property {
            "priority" => {
                let value = self.expr_to_u32(right)?;
                match op {
                    ComparisonOp::Equals => {
                        matcher.priority_range = Some((value, value));
                    }
                    ComparisonOp::GreaterThan => {
                        matcher.priority_range = Some((value + 1, u32::MAX));
                    }
                    ComparisonOp::GreaterThanOrEqual => {
                        matcher.priority_range = Some((value, u32::MAX));
                    }
                    ComparisonOp::LessThan => {
                        matcher.priority_range = Some((0, value.saturating_sub(1)));
                    }
                    ComparisonOp::LessThanOrEqual => {
                        matcher.priority_range = Some((0, value));
                    }
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: op.symbol().to_string(),
                            left_type: "priority".to_string(),
                            right_type: "number".to_string(),
                        }));
                    }
                }
            }
            "name" => {
                let pattern = self.expr_to_string(right)?;
                match op {
                    ComparisonOp::Equals => {
                        matcher.name_pattern = Some(format!("^{}$", regex::escape(&pattern)));
                    }
                    ComparisonOp::Matches => {
                        matcher.name_pattern = Some(pattern);
                    }
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: op.symbol().to_string(),
                            left_type: "name".to_string(),
                            right_type: "string".to_string(),
                        }));
                    }
                }
            }
            "type" => {
                let type_name = self.expr_to_string(right)?;
                match op {
                    ComparisonOp::Equals => {
                        matcher.job_type = Some(vec![type_name]);
                    }
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: op.symbol().to_string(),
                            left_type: "type".to_string(),
                            right_type: "string".to_string(),
                        }));
                    }
                }
            }
            _ => {
                return Err(DslError::new(DslErrorKind::InvalidProperty {
                    object_type: "job".to_string(),
                    property: property.to_string(),
                }));
            }
        }

        Ok(PolicyCondition::JobMatches(matcher))
    }

    fn compile_submitter_comparison(
        &self,
        access: &ObjectAccess,
        op: &ComparisonOp,
        right: &ExprAst,
    ) -> DslResult<PolicyCondition> {
        let property = access
            .properties
            .first()
            .map(|p| p.value.as_str())
            .unwrap_or("");

        let mut matcher = SubmitterMatcher::new();

        match property {
            "principal" | "principal_id" => {
                let value = self.expr_to_string(right)?;
                match op {
                    ComparisonOp::Equals => {
                        matcher.principal_id = Some(value);
                    }
                    ComparisonOp::Matches => {
                        matcher.principal_pattern = Some(value);
                    }
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: op.symbol().to_string(),
                            left_type: "principal".to_string(),
                            right_type: "string".to_string(),
                        }));
                    }
                }
            }
            "domain" | "in_domain" => {
                let value = self.expr_to_string(right)?;
                match op {
                    ComparisonOp::Equals => {
                        matcher.in_domain = Some(value);
                    }
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: op.symbol().to_string(),
                            left_type: "domain".to_string(),
                            right_type: "string".to_string(),
                        }));
                    }
                }
            }
            "priority" | "min_priority" => {
                let value = self.expr_to_u32(right)?;
                match op {
                    ComparisonOp::GreaterThanOrEqual | ComparisonOp::GreaterThan => {
                        matcher.min_priority = Some(value);
                    }
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: op.symbol().to_string(),
                            left_type: "priority".to_string(),
                            right_type: "number".to_string(),
                        }));
                    }
                }
            }
            _ => {
                return Err(DslError::new(DslErrorKind::InvalidProperty {
                    object_type: "submitter".to_string(),
                    property: property.to_string(),
                }));
            }
        }

        Ok(PolicyCondition::SubmitterMatches(matcher))
    }

    fn compile_resource_comparison(
        &self,
        access: &ObjectAccess,
        op: &ComparisonOp,
        right: &ExprAst,
    ) -> DslResult<PolicyCondition> {
        let property = access
            .properties
            .first()
            .map(|p| p.value.as_str())
            .unwrap_or("");

        let mut matcher = ResourceMatcher::new();

        match property {
            "cpu" | "cores" => {
                let value = self.expr_to_u32(right)?;
                match op {
                    ComparisonOp::Equals => {
                        matcher.min_cpu = Some(value);
                        matcher.max_cpu = Some(value);
                    }
                    ComparisonOp::GreaterThan | ComparisonOp::GreaterThanOrEqual => {
                        matcher.min_cpu = Some(value);
                    }
                    ComparisonOp::LessThan | ComparisonOp::LessThanOrEqual => {
                        matcher.max_cpu = Some(value);
                    }
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: op.symbol().to_string(),
                            left_type: "cpu".to_string(),
                            right_type: "number".to_string(),
                        }));
                    }
                }
            }
            "memory" | "memory_gb" => {
                let value = self.expr_to_f64(right)?;
                match op {
                    ComparisonOp::GreaterThan | ComparisonOp::GreaterThanOrEqual => {
                        matcher.min_memory_gb = Some(value);
                    }
                    ComparisonOp::LessThan | ComparisonOp::LessThanOrEqual => {
                        matcher.max_memory_gb = Some(value);
                    }
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: op.symbol().to_string(),
                            left_type: "memory".to_string(),
                            right_type: "number".to_string(),
                        }));
                    }
                }
            }
            "gpu" | "gpus" => {
                let value = self.expr_to_u32(right)?;
                match op {
                    ComparisonOp::Equals if value == 0 => {
                        matcher.requires_gpu = Some(false);
                    }
                    ComparisonOp::GreaterThan | ComparisonOp::GreaterThanOrEqual => {
                        matcher.requires_gpu = Some(true);
                        matcher.min_gpu = Some(value.max(1));
                    }
                    _ => {
                        matcher.requires_gpu = Some(value > 0);
                        matcher.min_gpu = Some(value);
                    }
                }
            }
            _ => {
                return Err(DslError::new(DslErrorKind::InvalidProperty {
                    object_type: "resource".to_string(),
                    property: property.to_string(),
                }));
            }
        }

        Ok(PolicyCondition::ResourceMatches(matcher))
    }

    fn compile_has_condition(
        &self,
        object: &ObjectAccess,
        key: &str,
    ) -> DslResult<PolicyCondition> {
        match object.object_type {
            ObjectType::Job => {
                let property = object
                    .properties
                    .first()
                    .map(|p| p.value.as_str())
                    .unwrap_or("");

                if property == "tags" {
                    let matcher = JobMatcher::new().with_tags(TagExpr::has_key(key));
                    Ok(PolicyCondition::JobMatches(matcher))
                } else {
                    Err(DslError::new(DslErrorKind::InvalidProperty {
                        object_type: "job".to_string(),
                        property: format!("{}.has", property),
                    }))
                }
            }
            _ => Err(DslError::new(DslErrorKind::UnsupportedFeature(format!(
                "has condition on {} objects",
                object.object_type.name()
            )))),
        }
    }

    fn compile_contains_condition(
        &self,
        object: &ObjectAccess,
        value: &str,
    ) -> DslResult<PolicyCondition> {
        match object.object_type {
            ObjectType::Submitter => {
                let property = object
                    .properties
                    .first()
                    .map(|p| p.value.as_str())
                    .unwrap_or("");

                if property == "domains" {
                    let matcher = SubmitterMatcher::new().with_domain(value);
                    Ok(PolicyCondition::SubmitterMatches(matcher))
                } else {
                    Err(DslError::new(DslErrorKind::InvalidProperty {
                        object_type: "submitter".to_string(),
                        property: format!("{}.contains", property),
                    }))
                }
            }
            _ => Err(DslError::new(DslErrorKind::UnsupportedFeature(format!(
                "contains condition on {} objects",
                object.object_type.name()
            )))),
        }
    }

    fn compile_matches_condition(
        &self,
        object: &ObjectAccess,
        pattern: &str,
    ) -> DslResult<PolicyCondition> {
        match object.object_type {
            ObjectType::Job => {
                let property = object
                    .properties
                    .first()
                    .map(|p| p.value.as_str())
                    .unwrap_or("");

                match property {
                    "name" => {
                        let matcher = JobMatcher::new().with_name_pattern(pattern);
                        Ok(PolicyCondition::JobMatches(matcher))
                    }
                    _ => Err(DslError::new(DslErrorKind::InvalidProperty {
                        object_type: "job".to_string(),
                        property: format!("{}.matches", property),
                    })),
                }
            }
            ObjectType::Submitter => {
                let property = object
                    .properties
                    .first()
                    .map(|p| p.value.as_str())
                    .unwrap_or("");

                match property {
                    "principal" | "principal_id" => {
                        let matcher = SubmitterMatcher::new().with_principal_pattern(pattern);
                        Ok(PolicyCondition::SubmitterMatches(matcher))
                    }
                    _ => Err(DslError::new(DslErrorKind::InvalidProperty {
                        object_type: "submitter".to_string(),
                        property: format!("{}.matches", property),
                    })),
                }
            }
            _ => Err(DslError::new(DslErrorKind::UnsupportedFeature(format!(
                "matches condition on {} objects",
                object.object_type.name()
            )))),
        }
    }

    fn compile_between_condition(
        &self,
        object: &ObjectAccess,
        low: &ExprAst,
        high: &ExprAst,
    ) -> DslResult<PolicyCondition> {
        match object.object_type {
            ObjectType::Job => {
                let property = object
                    .properties
                    .first()
                    .map(|p| p.value.as_str())
                    .unwrap_or("");

                if property == "priority" {
                    let low_val = self.expr_to_u32(low)?;
                    let high_val = self.expr_to_u32(high)?;
                    let matcher = JobMatcher::new().with_priority_range(low_val, high_val);
                    Ok(PolicyCondition::JobMatches(matcher))
                } else {
                    Err(DslError::new(DslErrorKind::InvalidProperty {
                        object_type: "job".to_string(),
                        property: format!("{}.between", property),
                    }))
                }
            }
            _ => Err(DslError::new(DslErrorKind::UnsupportedFeature(format!(
                "between condition on {} objects",
                object.object_type.name()
            )))),
        }
    }

    fn compile_in_condition(
        &self,
        object: &ObjectAccess,
        values: &[Spanned<LiteralAst>],
    ) -> DslResult<PolicyCondition> {
        match object.object_type {
            ObjectType::Job => {
                let property = object
                    .properties
                    .first()
                    .map(|p| p.value.as_str())
                    .unwrap_or("");

                if property == "type" {
                    let types: DslResult<Vec<String>> = values
                        .iter()
                        .map(|v| self.literal_to_string(&v.value))
                        .collect();
                    let matcher = JobMatcher::new().with_job_types(types?);
                    Ok(PolicyCondition::JobMatches(matcher))
                } else {
                    Err(DslError::new(DslErrorKind::InvalidProperty {
                        object_type: "job".to_string(),
                        property: format!("{}.in", property),
                    }))
                }
            }
            _ => Err(DslError::new(DslErrorKind::UnsupportedFeature(format!(
                "in condition on {} objects",
                object.object_type.name()
            )))),
        }
    }

    /// Compile an effect AST to PolicyEffect
    fn compile_effect(&self, effect: &EffectAst) -> DslResult<PolicyEffect> {
        match effect {
            EffectAst::Prefer { selector, weight } => {
                let node_selector = self.compile_selector(&selector.value)?;
                let w = weight.as_ref().map(|w| w.value).unwrap_or(0.5);
                Ok(PolicyEffect::prefer(node_selector, w))
            }
            EffectAst::Require { selector } => {
                let node_selector = self.compile_selector(&selector.value)?;
                Ok(PolicyEffect::require(node_selector))
            }
            EffectAst::Exclude { selector } => {
                let node_selector = self.compile_selector(&selector.value)?;
                Ok(PolicyEffect::exclude(node_selector))
            }
            EffectAst::Affinity {
                target,
                scope,
                weight,
            } => {
                let aff_target = self.compile_affinity_target(&target.value)?;
                let aff_scope = scope
                    .as_ref()
                    .map(|s| self.compile_affinity_scope(&s.value))
                    .unwrap_or(AffinityScope::Node);
                let w = weight.as_ref().map(|w| w.value).unwrap_or(0.5);
                Ok(PolicyEffect::affinity(aff_target, aff_scope, w))
            }
            EffectAst::AntiAffinity {
                target,
                scope,
                weight,
            } => {
                let aff_target = self.compile_affinity_target(&target.value)?;
                let aff_scope = scope
                    .as_ref()
                    .map(|s| self.compile_affinity_scope(&s.value))
                    .unwrap_or(AffinityScope::Node);
                let w = weight.as_ref().map(|w| w.value).unwrap_or(0.5);
                Ok(PolicyEffect::anti_affinity(aff_target, aff_scope, w))
            }
            EffectAst::SetResourceLimit { resource, limit } => {
                let res_type = self.compile_resource_type(&resource.value)?;
                Ok(PolicyEffect::SetResourceLimit {
                    resource: res_type,
                    limit: limit.value,
                })
            }
            EffectAst::SetPriority { value, mode } => {
                let priority_mode = mode
                    .as_ref()
                    .map(|m| self.compile_priority_mode(&m.value))
                    .unwrap_or(PriorityMode::Set);
                Ok(PolicyEffect::SetPriority {
                    priority: value.value as i32,
                    mode: priority_mode,
                })
            }
            EffectAst::ChargeQuota {
                quota_id,
                multiplier,
            } => Ok(PolicyEffect::ChargeQuota {
                quota_id: quota_id.value.clone(),
                multiplier: multiplier.as_ref().map(|m| m.value).unwrap_or(1.0),
            }),
            EffectAst::AllowPreemption { min_priority } => Ok(PolicyEffect::AllowPreemption {
                by_min_priority: min_priority.as_ref().map(|p| p.value).unwrap_or(0),
            }),
            EffectAst::DisallowPreemption => Ok(PolicyEffect::DisallowPreemption),
        }
    }

    /// Compile a selector AST to NodeSelector
    fn compile_selector(&self, selector: &SelectorAst) -> DslResult<NodeSelector> {
        match selector {
            SelectorAst::All => Ok(NodeSelector::All),
            SelectorAst::NodeIds(ids) => {
                let id_strings: Vec<String> = ids.iter().map(|id| id.value.clone()).collect();
                Ok(NodeSelector::NodeIds(id_strings))
            }
            SelectorAst::Group(name) => Ok(NodeSelector::Group(name.value.clone())),
            SelectorAst::Tag(tag_sel) => {
                let tag_expr = self.compile_tag_selector(&tag_sel.value)?;
                Ok(NodeSelector::Tag(tag_expr))
            }
            SelectorAst::Property {
                object,
                operator,
                value,
            } => {
                // Convert property comparison to tag expression
                if object.value.object_type != ObjectType::Node {
                    return Err(DslError::new(DslErrorKind::InvalidSyntax(
                        "property selector must use node object".to_string(),
                    )));
                }

                let property = object
                    .value
                    .properties
                    .first()
                    .map(|p| p.value.as_str())
                    .unwrap_or("");

                let value_str = self.literal_to_string(&value.value)?;

                let tag_expr = match operator.value {
                    ComparisonOp::Equals => TagExpr::equals(property, value_str),
                    ComparisonOp::Matches => TagExpr::ValueMatches {
                        key: property.to_string(),
                        pattern: value_str,
                    },
                    _ => {
                        return Err(DslError::new(DslErrorKind::InvalidOperator {
                            operator: operator.value.symbol().to_string(),
                            left_type: "node property".to_string(),
                            right_type: "value".to_string(),
                        }));
                    }
                };

                Ok(NodeSelector::Tag(tag_expr))
            }
        }
    }

    /// Compile a tag selector AST to TagExpr
    fn compile_tag_selector(&self, selector: &TagSelectorAst) -> DslResult<TagExpr> {
        match selector {
            TagSelectorAst::HasKey(key) => Ok(TagExpr::has_key(&key.value)),
            TagSelectorAst::Equals { key, value } => Ok(TagExpr::equals(&key.value, &value.value)),
            TagSelectorAst::KeyMatches { key, pattern } => Ok(TagExpr::KeyMatches {
                key: key.value.clone(),
                pattern: pattern.value.clone(),
            }),
            TagSelectorAst::ValueMatches { key, pattern } => Ok(TagExpr::ValueMatches {
                key: key.value.clone(),
                pattern: pattern.value.clone(),
            }),
            TagSelectorAst::And(sels) => {
                let compiled: DslResult<Vec<_>> = sels
                    .iter()
                    .map(|s| self.compile_tag_selector(&s.value))
                    .collect();
                Ok(TagExpr::and(compiled?))
            }
            TagSelectorAst::Or(sels) => {
                let compiled: DslResult<Vec<_>> = sels
                    .iter()
                    .map(|s| self.compile_tag_selector(&s.value))
                    .collect();
                Ok(TagExpr::or(compiled?))
            }
            TagSelectorAst::Not(inner) => {
                let compiled = self.compile_tag_selector(&inner.value)?;
                Ok(TagExpr::not(compiled))
            }
        }
    }

    /// Compile affinity target
    fn compile_affinity_target(&self, target: &AffinityTargetAst) -> DslResult<AffinityTarget> {
        match target {
            AffinityTargetAst::SameJob => Ok(AffinityTarget::SameJob),
            AffinityTargetAst::JobPattern(pattern) => {
                Ok(AffinityTarget::JobPattern(pattern.value.clone()))
            }
            AffinityTargetAst::Tag(tag) => {
                let tag_expr = self.compile_tag_selector(&tag.value)?;
                Ok(AffinityTarget::Tag(tag_expr))
            }
        }
    }

    /// Compile affinity scope
    fn compile_affinity_scope(&self, scope: &str) -> AffinityScope {
        match scope.to_lowercase().as_str() {
            "node" => AffinityScope::Node,
            "rack" => AffinityScope::Rack,
            "building" => AffinityScope::Building,
            "region" => AffinityScope::Region,
            _ => AffinityScope::Custom(scope.to_string()),
        }
    }

    /// Compile resource type
    fn compile_resource_type(&self, resource: &str) -> DslResult<crate::policy::ir::ResourceType> {
        match resource.to_lowercase().as_str() {
            "cpu" | "cores" => Ok(crate::policy::ir::ResourceType::Cpu),
            "memory" | "mem" | "ram" => Ok(crate::policy::ir::ResourceType::Memory),
            "gpu" | "gpus" => Ok(crate::policy::ir::ResourceType::Gpu),
            "disk" | "storage" => Ok(crate::policy::ir::ResourceType::Disk),
            "network" | "bandwidth" => Ok(crate::policy::ir::ResourceType::Network),
            _ => Ok(crate::policy::ir::ResourceType::Custom(
                resource.to_string(),
            )),
        }
    }

    /// Compile priority mode
    fn compile_priority_mode(&self, mode: &PriorityModeAst) -> PriorityMode {
        match mode {
            PriorityModeAst::Set => PriorityMode::Set,
            PriorityModeAst::Add => PriorityMode::Add,
            PriorityModeAst::Multiply => PriorityMode::Multiply,
            PriorityModeAst::Max => PriorityMode::Max,
            PriorityModeAst::Min => PriorityMode::Min,
        }
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    fn expr_to_string(&self, expr: &ExprAst) -> DslResult<String> {
        match expr {
            ExprAst::Literal(lit) => self.literal_to_string(lit),
            _ => Err(DslError::new(DslErrorKind::TypeMismatch {
                expected: "string".to_string(),
                found: "expression".to_string(),
            })),
        }
    }

    fn literal_to_string(&self, lit: &LiteralAst) -> DslResult<String> {
        match lit {
            LiteralAst::String(s) => Ok(s.clone()),
            LiteralAst::Boolean(b) => Ok(b.to_string()),
            _ => Err(DslError::new(DslErrorKind::TypeMismatch {
                expected: "string".to_string(),
                found: lit.type_name().to_string(),
            })),
        }
    }

    fn expr_to_u32(&self, expr: &ExprAst) -> DslResult<u32> {
        match expr {
            ExprAst::Literal(LiteralAst::Integer(i)) => {
                if *i < 0 {
                    Err(DslError::new(DslErrorKind::TypeMismatch {
                        expected: "positive integer".to_string(),
                        found: "negative integer".to_string(),
                    }))
                } else {
                    Ok(*i as u32)
                }
            }
            ExprAst::Literal(lit) => Err(DslError::new(DslErrorKind::TypeMismatch {
                expected: "integer".to_string(),
                found: lit.type_name().to_string(),
            })),
            _ => Err(DslError::new(DslErrorKind::TypeMismatch {
                expected: "integer".to_string(),
                found: "expression".to_string(),
            })),
        }
    }

    fn expr_to_f64(&self, expr: &ExprAst) -> DslResult<f64> {
        match expr {
            ExprAst::Literal(LiteralAst::Float(f)) => Ok(*f),
            ExprAst::Literal(LiteralAst::Integer(i)) => Ok(*i as f64),
            ExprAst::Literal(lit) => Err(DslError::new(DslErrorKind::TypeMismatch {
                expected: "number".to_string(),
                found: lit.type_name().to_string(),
            })),
            _ => Err(DslError::new(DslErrorKind::TypeMismatch {
                expected: "number".to_string(),
                found: "expression".to_string(),
            })),
        }
    }
}

/// Compile DSL source to Policy IR
pub fn compile(source: &str) -> DslResult<Vec<Policy>> {
    let ast = super::parser::parse(source)?;
    Compiler::new().compile(&ast)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_simple_rule() {
        let policies = compile(r#"job.priority > 5 => prefer all"#).unwrap();
        assert_eq!(policies.len(), 1);
        assert!(matches!(
            policies[0].condition,
            PolicyCondition::JobMatches(_)
        ));
    }

    #[test]
    fn test_compile_and_condition() {
        let policies =
            compile(r#"job.priority > 5 AND job.name matches "ml-.*" => prefer all"#).unwrap();
        assert!(matches!(policies[0].condition, PolicyCondition::And(_)));
    }

    #[test]
    fn test_compile_prefer_with_weight() {
        let policies =
            compile(r#"job.priority > 5 => prefer node.gpu = "true" weight 0.8"#).unwrap();
        match &policies[0].effects[0] {
            PolicyEffect::Prefer { weight, .. } => {
                assert!((*weight - 0.8).abs() < f64::EPSILON);
            }
            _ => panic!("Expected Prefer effect"),
        }
    }

    #[test]
    fn test_compile_require() {
        let policies = compile(r#"job.priority > 5 => require node.region = "us-east""#).unwrap();
        assert!(matches!(
            policies[0].effects[0],
            PolicyEffect::Require { .. }
        ));
    }

    #[test]
    fn test_compile_exclude() {
        let policies = compile(r#"job.priority > 5 => exclude node.region = "eu-west""#).unwrap();
        assert!(matches!(
            policies[0].effects[0],
            PolicyEffect::Exclude { .. }
        ));
    }

    #[test]
    fn test_compile_tag_selector() {
        let policies = compile(r#"job.priority > 5 => prefer tag(gpu = "true")"#).unwrap();
        match &policies[0].effects[0] {
            PolicyEffect::Prefer { selector, .. } => {
                assert!(matches!(selector, NodeSelector::Tag(_)));
            }
            _ => panic!("Expected Prefer effect"),
        }
    }

    #[test]
    fn test_compile_group_selector() {
        let policies = compile(r#"job.priority > 5 => prefer group("ml-cluster")"#).unwrap();
        match &policies[0].effects[0] {
            PolicyEffect::Prefer { selector, .. } => {
                assert!(matches!(selector, NodeSelector::Group(_)));
            }
            _ => panic!("Expected Prefer effect"),
        }
    }

    #[test]
    fn test_compile_submitter_condition() {
        let policies = compile(r#"submitter.domains contains "ml" => prefer all"#).unwrap();
        assert!(matches!(
            policies[0].condition,
            PolicyCondition::SubmitterMatches(_)
        ));
    }

    #[test]
    fn test_compile_resource_condition() {
        let policies = compile(r#"resource.gpu > 0 => require node.gpu = "true""#).unwrap();
        assert!(matches!(
            policies[0].condition,
            PolicyCondition::ResourceMatches(_)
        ));
    }

    #[test]
    fn test_compile_time_condition() {
        let policies = compile(r#"time in "0 9-17 * * MON-FRI" => prefer all"#).unwrap();
        assert!(matches!(
            policies[0].condition,
            PolicyCondition::TimeWindow { .. }
        ));
    }

    #[test]
    fn test_compile_between_condition() {
        let policies = compile(r#"job.priority between 0 and 100 => prefer all"#).unwrap();
        match &policies[0].condition {
            PolicyCondition::JobMatches(matcher) => {
                assert_eq!(matcher.priority_range, Some((0, 100)));
            }
            _ => panic!("Expected JobMatches condition"),
        }
    }

    #[test]
    fn test_compile_complex_policy() {
        let source = r#"
            # High priority ML jobs
            job.priority > 50 AND job.name matches "ml-.*" => prefer tag(gpu = "true") weight 0.9

            # Production only
            submitter.domains contains "production" => require node.env = "production"
        "#;

        let policies = compile(source).unwrap();
        assert_eq!(policies.len(), 2);
    }
}
