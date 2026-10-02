// Marabunta - Licensed under the MIT License.
//! DSL Validation
//!
//! Validates DSL policies before compilation, providing early error detection
//! with detailed error messages.

use std::collections::HashSet;

use super::ast::*;
use super::error::{DslError, DslErrorKind, DslResult, Span};

/// Known property names for each object type
static JOB_PROPERTIES: &[&str] = &["priority", "name", "type", "tags", "id"];
static NODE_PROPERTIES: &[&str] = &["region", "gpu", "cpu", "memory", "env", "group", "tags"];
static SUBMITTER_PROPERTIES: &[&str] = &[
    "principal",
    "principal_id",
    "domain",
    "domains",
    "priority",
    "min_priority",
];
static RESOURCE_PROPERTIES: &[&str] = &[
    "cpu",
    "cores",
    "memory",
    "memory_gb",
    "gpu",
    "gpus",
    "disk",
    "network",
];

/// Validation result with warnings
#[derive(Debug)]
pub struct DslValidationResult {
    /// Whether validation passed (no errors)
    pub valid: bool,
    /// Errors found
    pub errors: Vec<DslError>,
    /// Warnings found
    pub warnings: Vec<DslWarning>,
}

impl DslValidationResult {
    /// Create a valid result
    pub fn valid() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Add an error
    pub fn add_error(&mut self, error: DslError) {
        self.valid = false;
        self.errors.push(error);
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: DslWarning) {
        self.warnings.push(warning);
    }

    /// Check if there are warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// A validation warning
#[derive(Debug, Clone)]
pub struct DslWarning {
    /// Warning code
    pub code: String,
    /// Warning message
    pub message: String,
    /// Location in source
    pub span: Option<Span>,
}

impl std::fmt::Display for DslWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(span) = &self.span {
            write!(f, "[{}] {} at {}", self.code, self.message, span)
        } else {
            write!(f, "[{}] {}", self.code, self.message)
        }
    }
}

/// DSL Validator
pub struct DslValidator {
    /// Known node groups
    known_groups: HashSet<String>,
    /// Known quota IDs
    known_quotas: HashSet<String>,
    /// Current validation result
    result: DslValidationResult,
}

impl Default for DslValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl DslValidator {
    /// Create a new validator
    pub fn new() -> Self {
        Self {
            known_groups: HashSet::new(),
            known_quotas: HashSet::new(),
            result: DslValidationResult::valid(),
        }
    }

    /// Add known node groups
    pub fn with_groups(mut self, groups: impl IntoIterator<Item = String>) -> Self {
        self.known_groups.extend(groups);
        self
    }

    /// Add known quota IDs
    pub fn with_quotas(mut self, quotas: impl IntoIterator<Item = String>) -> Self {
        self.known_quotas.extend(quotas);
        self
    }

    /// Validate a policy AST
    pub fn validate(mut self, ast: &PolicyAst) -> DslValidationResult {
        // Check for empty policy
        if ast.rules.is_empty() {
            self.result.add_error(
                DslError::new(DslErrorKind::InvalidPolicyStructure(
                    "policy must contain at least one rule".to_string(),
                ))
                .with_span(ast.span),
            );
            return self.result;
        }

        // Validate each rule
        for rule in &ast.rules {
            self.validate_rule(&rule.value, rule.span);
        }

        self.result
    }

    fn validate_rule(&mut self, rule: &RuleAst, span: Span) {
        // Validate condition
        self.validate_condition(&rule.condition.value, rule.condition.span);

        // Validate effects
        if rule.effects.is_empty() {
            self.result.add_warning(DslWarning {
                code: "W001".to_string(),
                message: "rule has no effects".to_string(),
                span: Some(span),
            });
        }

        for effect in &rule.effects {
            self.validate_effect(&effect.value, effect.span);
        }
    }

    fn validate_condition(&mut self, cond: &ConditionAst, span: Span) {
        match cond {
            ConditionAst::Always => {
                self.result.add_warning(DslWarning {
                    code: "W002".to_string(),
                    message: "condition is always true, consider if this is intentional"
                        .to_string(),
                    span: Some(span),
                });
            }
            ConditionAst::Never => {
                self.result.add_warning(DslWarning {
                    code: "W003".to_string(),
                    message: "condition is never true, rule will never apply".to_string(),
                    span: Some(span),
                });
            }
            ConditionAst::Comparison {
                left,
                operator,
                right,
            } => {
                self.validate_expression(&left.value, left.span);
                self.validate_comparison_types(&left.value, &operator.value, &right.value, span);
            }
            ConditionAst::And(conds) => {
                if conds.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "AND condition has no sub-conditions".to_string(),
                        ))
                        .with_span(span),
                    );
                }
                for c in conds {
                    self.validate_condition(&c.value, c.span);
                }
            }
            ConditionAst::Or(conds) => {
                if conds.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "OR condition has no sub-conditions".to_string(),
                        ))
                        .with_span(span),
                    );
                }
                for c in conds {
                    self.validate_condition(&c.value, c.span);
                }
            }
            ConditionAst::Not(inner) => {
                self.validate_condition(&inner.value, inner.span);
            }
            ConditionAst::Has { object, key } => {
                self.validate_object_access(&object.value, object.span);
                if key.value.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "has condition key cannot be empty".to_string(),
                        ))
                        .with_span(key.span),
                    );
                }
            }
            ConditionAst::Contains { object, value } => {
                self.validate_object_access(&object.value, object.span);
                if value.value.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "contains condition value cannot be empty".to_string(),
                        ))
                        .with_span(value.span),
                    );
                }
            }
            ConditionAst::Matches { object, pattern } => {
                self.validate_object_access(&object.value, object.span);
                self.validate_regex(&pattern.value, pattern.span);
            }
            ConditionAst::Between { object, low, high } => {
                self.validate_object_access(&object.value, object.span);
                self.validate_expression(&low.value, low.span);
                self.validate_expression(&high.value, high.span);
            }
            ConditionAst::In { object, values } => {
                self.validate_object_access(&object.value, object.span);
                if values.is_empty() {
                    self.result.add_warning(DslWarning {
                        code: "W004".to_string(),
                        message: "in condition has empty value list".to_string(),
                        span: Some(span),
                    });
                }
            }
            ConditionAst::TimeWindow { cron } => {
                self.validate_cron(&cron.value, cron.span);
            }
            ConditionAst::Grouped(inner) => {
                self.validate_condition(&inner.value, inner.span);
            }
        }
    }

    fn validate_effect(&mut self, effect: &EffectAst, _span: Span) {
        match effect {
            EffectAst::Prefer { selector, weight } => {
                self.validate_selector(&selector.value, selector.span);
                if let Some(w) = weight {
                    self.validate_weight(w.value, w.span);
                }
            }
            EffectAst::Require { selector } => {
                self.validate_selector(&selector.value, selector.span);
            }
            EffectAst::Exclude { selector } => {
                self.validate_selector(&selector.value, selector.span);
            }
            EffectAst::Affinity {
                target,
                scope,
                weight,
            } => {
                self.validate_affinity_target(&target.value, target.span);
                if let Some(s) = scope {
                    self.validate_affinity_scope(&s.value, s.span);
                }
                if let Some(w) = weight {
                    self.validate_weight(w.value, w.span);
                }
            }
            EffectAst::AntiAffinity {
                target,
                scope,
                weight,
            } => {
                self.validate_affinity_target(&target.value, target.span);
                if let Some(s) = scope {
                    self.validate_affinity_scope(&s.value, s.span);
                }
                if let Some(w) = weight {
                    self.validate_weight(w.value, w.span);
                }
            }
            EffectAst::SetResourceLimit { resource: _, limit } => {
                if limit.value < 0.0 {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "resource limit cannot be negative".to_string(),
                        ))
                        .with_span(limit.span),
                    );
                }
            }
            EffectAst::SetPriority { value: _, mode: _ } => {
                // Priority can be negative
            }
            EffectAst::ChargeQuota {
                quota_id,
                multiplier,
            } => {
                if quota_id.value.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "quota ID cannot be empty".to_string(),
                        ))
                        .with_span(quota_id.span),
                    );
                }
                if !self.known_quotas.is_empty() && !self.known_quotas.contains(&quota_id.value) {
                    self.result.add_warning(DslWarning {
                        code: "W005".to_string(),
                        message: format!("unknown quota ID: {}", quota_id.value),
                        span: Some(quota_id.span),
                    });
                }
                if let Some(m) = multiplier {
                    if m.value < 0.0 {
                        self.result.add_error(
                            DslError::new(DslErrorKind::InvalidSyntax(
                                "quota multiplier cannot be negative".to_string(),
                            ))
                            .with_span(m.span),
                        );
                    }
                }
            }
            EffectAst::AllowPreemption { min_priority: _ } => {
                // Valid
            }
            EffectAst::DisallowPreemption => {
                // Valid
            }
        }
    }

    fn validate_selector(&mut self, selector: &SelectorAst, span: Span) {
        match selector {
            SelectorAst::All => {
                // Valid
            }
            SelectorAst::NodeIds(ids) => {
                if ids.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "node ID list cannot be empty".to_string(),
                        ))
                        .with_span(span),
                    );
                }
            }
            SelectorAst::Group(name) => {
                if name.value.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "group name cannot be empty".to_string(),
                        ))
                        .with_span(name.span),
                    );
                }
                if !self.known_groups.is_empty() && !self.known_groups.contains(&name.value) {
                    self.result.add_warning(DslWarning {
                        code: "W006".to_string(),
                        message: format!("unknown node group: {}", name.value),
                        span: Some(name.span),
                    });
                }
            }
            SelectorAst::Tag(tag) => {
                self.validate_tag_selector(&tag.value, tag.span);
            }
            SelectorAst::Property {
                object,
                operator: _,
                value,
            } => {
                self.validate_object_access(&object.value, object.span);
                // Validate that comparison makes sense
                self.validate_literal_type(&value.value, value.span);
            }
        }
    }

    fn validate_tag_selector(&mut self, selector: &TagSelectorAst, _span: Span) {
        match selector {
            TagSelectorAst::HasKey(key) => {
                if key.value.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "tag key cannot be empty".to_string(),
                        ))
                        .with_span(key.span),
                    );
                }
            }
            TagSelectorAst::Equals { key, value: _ } => {
                if key.value.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "tag key cannot be empty".to_string(),
                        ))
                        .with_span(key.span),
                    );
                }
            }
            TagSelectorAst::KeyMatches { key: _, pattern } => {
                self.validate_regex(&pattern.value, pattern.span);
            }
            TagSelectorAst::ValueMatches { key, pattern } => {
                if key.value.is_empty() {
                    self.result.add_error(
                        DslError::new(DslErrorKind::InvalidSyntax(
                            "tag key cannot be empty".to_string(),
                        ))
                        .with_span(key.span),
                    );
                }
                self.validate_regex(&pattern.value, pattern.span);
            }
            TagSelectorAst::And(sels) => {
                for s in sels {
                    self.validate_tag_selector(&s.value, s.span);
                }
            }
            TagSelectorAst::Or(sels) => {
                for s in sels {
                    self.validate_tag_selector(&s.value, s.span);
                }
            }
            TagSelectorAst::Not(inner) => {
                self.validate_tag_selector(&inner.value, inner.span);
            }
        }
    }

    fn validate_affinity_target(&mut self, target: &AffinityTargetAst, _span: Span) {
        match target {
            AffinityTargetAst::SameJob => {
                // Valid
            }
            AffinityTargetAst::JobPattern(pattern) => {
                self.validate_regex(&pattern.value, pattern.span);
            }
            AffinityTargetAst::Tag(tag) => {
                self.validate_tag_selector(&tag.value, tag.span);
            }
        }
    }

    fn validate_affinity_scope(&mut self, scope: &str, span: Span) {
        let known_scopes = ["node", "rack", "building", "region"];
        if !known_scopes.contains(&scope.to_lowercase().as_str()) {
            self.result.add_warning(DslWarning {
                code: "W007".to_string(),
                message: format!("custom affinity scope '{}', ensure this is defined", scope),
                span: Some(span),
            });
        }
    }

    fn validate_expression(&mut self, expr: &ExprAst, span: Span) {
        match expr {
            ExprAst::Literal(_) => {
                // Literals are always valid
            }
            ExprAst::Access(access) => {
                self.validate_object_access(access, span);
            }
            ExprAst::Binary { left, right, .. } => {
                self.validate_expression(&left.value, left.span);
                self.validate_expression(&right.value, right.span);
            }
            ExprAst::Unary { operand, .. } => {
                self.validate_expression(&operand.value, operand.span);
            }
            ExprAst::Grouped(inner) => {
                self.validate_expression(&inner.value, inner.span);
            }
        }
    }

    fn validate_object_access(&mut self, access: &ObjectAccess, _span: Span) {
        let known_properties: &[&str] = match access.object_type {
            ObjectType::Job => JOB_PROPERTIES,
            ObjectType::Node => NODE_PROPERTIES,
            ObjectType::Submitter => SUBMITTER_PROPERTIES,
            ObjectType::Resource => RESOURCE_PROPERTIES,
            ObjectType::Time => &[],
            ObjectType::Tag => &[],
            ObjectType::Group => &[],
        };

        // Check first property
        if let Some(first_prop) = access.properties.first() {
            if !known_properties.is_empty()
                && !known_properties.contains(&first_prop.value.as_str())
            {
                self.result.add_warning(DslWarning {
                    code: "W008".to_string(),
                    message: format!(
                        "unknown property '{}' on '{}', known properties: {}",
                        first_prop.value,
                        access.object_type.name(),
                        known_properties.join(", ")
                    ),
                    span: Some(first_prop.span),
                });
            }
        }
    }

    fn validate_comparison_types(
        &mut self,
        left: &ExprAst,
        op: &ComparisonOp,
        right: &ExprAst,
        span: Span,
    ) {
        // Check for type mismatches
        let left_type = self.infer_type(left);
        let right_type = self.infer_type(right);

        match (left_type.as_str(), right_type.as_str(), op) {
            // String comparisons
            (
                "string",
                "string",
                ComparisonOp::Equals | ComparisonOp::NotEquals | ComparisonOp::Matches,
            ) => {}
            // Numeric comparisons
            ("number", "number", _) => {}
            ("integer", "integer", _) => {}
            ("float", "float", _) => {}
            ("number", "integer", _) | ("integer", "number", _) => {}
            ("number", "float", _) | ("float", "number", _) => {}
            // Boolean comparisons
            ("boolean", "boolean", ComparisonOp::Equals | ComparisonOp::NotEquals) => {}
            // Unknown types (from object access)
            ("unknown", _, _) | (_, "unknown", _) => {}
            // Type mismatch
            (l, r, _) => {
                self.result.add_error(
                    DslError::new(DslErrorKind::TypeMismatch {
                        expected: l.to_string(),
                        found: r.to_string(),
                    })
                    .with_span(span)
                    .with_hint(format!(
                        "cannot compare {} with {} using '{}'",
                        l,
                        r,
                        op.symbol()
                    )),
                );
            }
        }
    }

    fn infer_type(&self, expr: &ExprAst) -> String {
        match expr {
            ExprAst::Literal(lit) => match lit {
                LiteralAst::String(_) => "string".to_string(),
                LiteralAst::Integer(_) => "integer".to_string(),
                LiteralAst::Float(_) => "float".to_string(),
                LiteralAst::Boolean(_) => "boolean".to_string(),
                LiteralAst::Array(_) => "array".to_string(),
            },
            ExprAst::Access(access) => {
                // Try to infer from known properties
                if let Some(prop) = access.properties.first() {
                    match prop.value.as_str() {
                        "priority" => "number".to_string(),
                        "name" | "type" | "principal" | "domain" => "string".to_string(),
                        _ => "unknown".to_string(),
                    }
                } else {
                    "unknown".to_string()
                }
            }
            _ => "unknown".to_string(),
        }
    }

    fn validate_weight(&mut self, weight: f64, span: Span) {
        if !(0.0..=1.0).contains(&weight) {
            self.result.add_error(
                DslError::new(DslErrorKind::InvalidSyntax(format!(
                    "weight must be between 0.0 and 1.0, got {}",
                    weight
                )))
                .with_span(span),
            );
        }
    }

    fn validate_regex(&mut self, pattern: &str, span: Span) {
        if let Err(e) = regex::Regex::new(pattern) {
            self.result.add_error(
                DslError::new(DslErrorKind::InvalidSyntax(format!(
                    "invalid regex pattern: {}",
                    e
                )))
                .with_span(span)
                .with_hint("check your regex syntax"),
            );
        }
    }

    fn validate_cron(&mut self, cron: &str, span: Span) {
        if cron.is_empty() {
            self.result.add_error(
                DslError::new(DslErrorKind::InvalidSyntax(
                    "cron expression cannot be empty".to_string(),
                ))
                .with_span(span),
            );
            return;
        }

        // Basic cron validation (5 or 6 fields)
        let parts: Vec<&str> = cron.split_whitespace().collect();
        if parts.len() < 5 || parts.len() > 6 {
            self.result.add_error(
                DslError::new(DslErrorKind::InvalidSyntax(format!(
                    "cron expression must have 5 or 6 fields, got {}",
                    parts.len()
                )))
                .with_span(span)
                .with_hint("format: minute hour day-of-month month day-of-week"),
            );
        }
    }

    fn validate_literal_type(&mut self, literal: &LiteralAst, span: Span) {
        // Literals are generally valid, but we can check for common issues
        match literal {
            LiteralAst::String(s) if s.is_empty() => {
                self.result.add_warning(DslWarning {
                    code: "W009".to_string(),
                    message: "empty string literal".to_string(),
                    span: Some(span),
                });
            }
            _ => {}
        }
    }
}

/// Validate DSL source code
pub fn validate(source: &str) -> DslResult<DslValidationResult> {
    let ast = super::parser::parse(source)?;
    Ok(DslValidator::new().validate(&ast))
}

/// Validate DSL source code with known groups and quotas
pub fn validate_with_context(
    source: &str,
    groups: Vec<String>,
    quotas: Vec<String>,
) -> DslResult<DslValidationResult> {
    let ast = super::parser::parse(source)?;
    Ok(DslValidator::new()
        .with_groups(groups)
        .with_quotas(quotas)
        .validate(&ast))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_policy() {
        let result = validate(r#"job.priority > 5 => prefer all"#).unwrap();
        assert!(result.valid);
    }

    #[test]
    fn test_invalid_regex() {
        let result = validate(r#"job.name matches "[invalid" => prefer all"#).unwrap();
        assert!(!result.valid);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e.kind, DslErrorKind::InvalidSyntax(_))));
    }

    #[test]
    fn test_invalid_weight() {
        let result = validate(r#"job.priority > 5 => prefer all weight 1.5"#).unwrap();
        assert!(!result.valid);
    }

    #[test]
    fn test_unknown_group_warning() {
        let result = validate_with_context(
            r#"job.priority > 5 => prefer group("unknown-group")"#,
            vec!["known-group".to_string()],
            vec![],
        )
        .unwrap();

        assert!(result.valid);
        assert!(result.warnings.iter().any(|w| w.code == "W006"));
    }

    #[test]
    fn test_unknown_quota_warning() {
        let result = validate_with_context(
            r#"job.priority > 5 => charge_quota "unknown-quota""#,
            vec![],
            vec!["known-quota".to_string()],
        )
        .unwrap();

        assert!(result.valid);
        assert!(result.warnings.iter().any(|w| w.code == "W005"));
    }

    #[test]
    fn test_type_mismatch() {
        let result = validate(r#"job.priority = "not-a-number" => prefer all"#).unwrap();
        assert!(!result.valid);
        assert!(result
            .errors
            .iter()
            .any(|e| matches!(e.kind, DslErrorKind::TypeMismatch { .. })));
    }

    #[test]
    fn test_empty_cron() {
        let result = validate(r#"time in "" => prefer all"#).unwrap();
        assert!(!result.valid);
    }

    #[test]
    fn test_invalid_cron_fields() {
        let result = validate(r#"time in "* * *" => prefer all"#).unwrap();
        assert!(!result.valid);
    }

    #[test]
    fn test_empty_node_ids() {
        let result = validate(r#"job.priority > 5 => prefer []"#).unwrap();
        assert!(!result.valid);
    }

    #[test]
    fn test_warning_unknown_property() {
        let result = validate(r#"job.unknown_prop = "value" => prefer all"#).unwrap();
        assert!(result.has_warnings());
        assert!(result.warnings.iter().any(|w| w.code == "W008"));
    }
}
