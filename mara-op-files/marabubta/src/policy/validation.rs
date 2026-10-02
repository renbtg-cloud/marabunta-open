// Marabunta - Licensed under the MIT License.
//! Policy Validation
//!
//! This module provides comprehensive validation for policies before they are
//! registered with the engine. It checks for semantic errors, warns about
//! potential issues, and ensures policies are well-formed.

use regex::Regex;
use std::collections::HashSet;

use super::ir::*;

/// Validates policies for correctness and potential issues
pub struct PolicyValidator {
    /// Known node groups (for validating group selectors)
    known_groups: HashSet<String>,
    /// Known quota IDs (for validating quota effects)
    known_quotas: HashSet<String>,
    /// Known custom scoring functions
    known_scoring_functions: HashSet<String>,
}

impl Default for PolicyValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyValidator {
    /// Create a new policy validator
    pub fn new() -> Self {
        Self {
            known_groups: HashSet::new(),
            known_quotas: HashSet::new(),
            known_scoring_functions: HashSet::new(),
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

    /// Add known scoring functions
    pub fn with_scoring_functions(mut self, functions: impl IntoIterator<Item = String>) -> Self {
        self.known_scoring_functions.extend(functions);
        self
    }

    /// Validate a policy and return the result
    pub fn validate(&self, policy: &Policy) -> ValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Validate basic fields
        self.validate_basic_fields(policy, &mut errors, &mut warnings);

        // Validate condition
        self.validate_condition(&policy.condition, "condition", &mut errors, &mut warnings);

        // Validate effects
        for (idx, effect) in policy.effects.iter().enumerate() {
            self.validate_effect(
                effect,
                &format!("effects[{}]", idx),
                &mut errors,
                &mut warnings,
            );
        }

        // Validate governance
        self.validate_governance(&policy.governance, &mut errors, &mut warnings);

        // Check for logical issues
        self.check_logical_issues(policy, &mut warnings);

        ValidationResult {
            valid: errors.is_empty(),
            errors,
            warnings,
        }
    }

    fn validate_basic_fields(
        &self,
        policy: &Policy,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        // Policy ID
        if policy.id.is_empty() {
            errors.push(ValidationError {
                code: "E001".to_string(),
                message: "Policy ID cannot be empty".to_string(),
                location: Some("id".to_string()),
            });
        } else if !Self::is_valid_id(&policy.id) {
            errors.push(ValidationError {
                code: "E002".to_string(),
                message: "Policy ID must be alphanumeric with hyphens/underscores".to_string(),
                location: Some("id".to_string()),
            });
        }

        // Policy name
        if policy.name.is_empty() {
            warnings.push(ValidationWarning {
                code: "W001".to_string(),
                message: "Policy name is empty".to_string(),
                location: Some("name".to_string()),
            });
        }

        // Version
        if policy.version == 0 {
            warnings.push(ValidationWarning {
                code: "W002".to_string(),
                message: "Policy version is 0, consider using 1-based versioning".to_string(),
                location: Some("version".to_string()),
            });
        }

        // Effects
        if policy.effects.is_empty() {
            warnings.push(ValidationWarning {
                code: "W003".to_string(),
                message: "Policy has no effects".to_string(),
                location: Some("effects".to_string()),
            });
        }

        // Expiration
        if let Some(expires_at) = policy.expires_at {
            if expires_at < policy.created_at {
                errors.push(ValidationError {
                    code: "E003".to_string(),
                    message: "Expiration time is before creation time".to_string(),
                    location: Some("expires_at".to_string()),
                });
            }
        }
    }

    fn validate_condition(
        &self,
        condition: &PolicyCondition,
        location: &str,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        match condition {
            PolicyCondition::Always => {
                // Always valid
            }

            PolicyCondition::Never => {
                warnings.push(ValidationWarning {
                    code: "W010".to_string(),
                    message: "Condition is 'Never', policy will never apply".to_string(),
                    location: Some(location.to_string()),
                });
            }

            PolicyCondition::JobMatches(matcher) => {
                self.validate_job_matcher(matcher, location, errors, warnings);
            }

            PolicyCondition::SubmitterMatches(matcher) => {
                self.validate_submitter_matcher(matcher, location, errors, warnings);
            }

            PolicyCondition::TimeWindow { cron } => {
                self.validate_cron(cron, location, errors);
            }

            PolicyCondition::ResourceMatches(matcher) => {
                self.validate_resource_matcher(matcher, location, errors, warnings);
            }

            PolicyCondition::And(conditions) => {
                if conditions.is_empty() {
                    errors.push(ValidationError {
                        code: "E010".to_string(),
                        message: "And condition has no sub-conditions".to_string(),
                        location: Some(location.to_string()),
                    });
                }
                for (idx, cond) in conditions.iter().enumerate() {
                    self.validate_condition(
                        cond,
                        &format!("{}.and[{}]", location, idx),
                        errors,
                        warnings,
                    );
                }
            }

            PolicyCondition::Or(conditions) => {
                if conditions.is_empty() {
                    errors.push(ValidationError {
                        code: "E011".to_string(),
                        message: "Or condition has no sub-conditions".to_string(),
                        location: Some(location.to_string()),
                    });
                }
                for (idx, cond) in conditions.iter().enumerate() {
                    self.validate_condition(
                        cond,
                        &format!("{}.or[{}]", location, idx),
                        errors,
                        warnings,
                    );
                }
            }

            PolicyCondition::Not(inner) => {
                self.validate_condition(inner, &format!("{}.not", location), errors, warnings);
            }
        }
    }

    fn validate_job_matcher(
        &self,
        matcher: &JobMatcher,
        location: &str,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        // Check if matcher is empty
        if matcher.name_pattern.is_none()
            && matcher.tags.is_none()
            && matcher.job_type.is_none()
            && matcher.priority_range.is_none()
        {
            warnings.push(ValidationWarning {
                code: "W011".to_string(),
                message: "JobMatcher has no criteria, will match all jobs".to_string(),
                location: Some(location.to_string()),
            });
        }

        // Validate regex pattern
        if let Some(pattern) = &matcher.name_pattern {
            self.validate_regex(pattern, &format!("{}.name_pattern", location), errors);
        }

        // Validate tags
        if let Some(tags) = &matcher.tags {
            self.validate_tag_expr(tags, &format!("{}.tags", location), errors);
        }

        // Validate priority range
        if let Some((min, max)) = matcher.priority_range {
            if min > max {
                errors.push(ValidationError {
                    code: "E012".to_string(),
                    message: format!("Priority range min ({}) > max ({})", min, max),
                    location: Some(format!("{}.priority_range", location)),
                });
            }
        }
    }

    fn validate_submitter_matcher(
        &self,
        matcher: &SubmitterMatcher,
        location: &str,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        // Check if matcher is empty
        if matcher.principal_id.is_none()
            && matcher.principal_pattern.is_none()
            && matcher.in_domain.is_none()
            && matcher.min_priority.is_none()
        {
            warnings.push(ValidationWarning {
                code: "W012".to_string(),
                message: "SubmitterMatcher has no criteria, will match all submitters".to_string(),
                location: Some(location.to_string()),
            });
        }

        // Validate regex pattern
        if let Some(pattern) = &matcher.principal_pattern {
            self.validate_regex(pattern, &format!("{}.principal_pattern", location), errors);
        }

        // Check for conflicting matchers
        if matcher.principal_id.is_some() && matcher.principal_pattern.is_some() {
            warnings.push(ValidationWarning {
                code: "W013".to_string(),
                message:
                    "Both principal_id and principal_pattern specified, pattern may be redundant"
                        .to_string(),
                location: Some(location.to_string()),
            });
        }
    }

    fn validate_resource_matcher(
        &self,
        matcher: &ResourceMatcher,
        location: &str,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        // Check if matcher is empty
        if matcher.min_cpu.is_none()
            && matcher.max_cpu.is_none()
            && matcher.min_memory_gb.is_none()
            && matcher.max_memory_gb.is_none()
            && matcher.requires_gpu.is_none()
            && matcher.min_gpu.is_none()
        {
            warnings.push(ValidationWarning {
                code: "W014".to_string(),
                message: "ResourceMatcher has no criteria, will match all resource requests"
                    .to_string(),
                location: Some(location.to_string()),
            });
        }

        // Validate CPU range
        if let (Some(min), Some(max)) = (matcher.min_cpu, matcher.max_cpu) {
            if min > max {
                errors.push(ValidationError {
                    code: "E013".to_string(),
                    message: format!("CPU range min ({}) > max ({})", min, max),
                    location: Some(format!("{}.cpu", location)),
                });
            }
        }

        // Validate memory range
        if let (Some(min), Some(max)) = (matcher.min_memory_gb, matcher.max_memory_gb) {
            if min > max {
                errors.push(ValidationError {
                    code: "E014".to_string(),
                    message: format!("Memory range min ({}) > max ({})", min, max),
                    location: Some(format!("{}.memory_gb", location)),
                });
            }
            if min < 0.0 {
                errors.push(ValidationError {
                    code: "E015".to_string(),
                    message: "Memory min cannot be negative".to_string(),
                    location: Some(format!("{}.min_memory_gb", location)),
                });
            }
        }
    }

    fn validate_cron(&self, cron: &str, location: &str, errors: &mut Vec<ValidationError>) {
        if cron.is_empty() {
            errors.push(ValidationError {
                code: "E020".to_string(),
                message: "Cron expression cannot be empty".to_string(),
                location: Some(location.to_string()),
            });
            return;
        }

        // Basic cron validation (5 or 6 fields)
        let parts: Vec<&str> = cron.split_whitespace().collect();
        if parts.len() < 5 || parts.len() > 6 {
            errors.push(ValidationError {
                code: "E021".to_string(),
                message: format!(
                    "Cron expression must have 5 or 6 fields, got {}",
                    parts.len()
                ),
                location: Some(location.to_string()),
            });
        }
    }

    fn validate_tag_expr(&self, expr: &TagExpr, location: &str, errors: &mut Vec<ValidationError>) {
        match expr {
            TagExpr::HasKey(key) => {
                if key.is_empty() {
                    errors.push(ValidationError {
                        code: "E030".to_string(),
                        message: "Tag key cannot be empty".to_string(),
                        location: Some(location.to_string()),
                    });
                }
            }

            TagExpr::Equals { key, value: _ } => {
                if key.is_empty() {
                    errors.push(ValidationError {
                        code: "E031".to_string(),
                        message: "Tag key cannot be empty".to_string(),
                        location: Some(location.to_string()),
                    });
                }
            }

            TagExpr::KeyMatches { key: _, pattern } => {
                self.validate_regex(pattern, &format!("{}.pattern", location), errors);
            }

            TagExpr::ValueMatches { key, pattern } => {
                if key.is_empty() {
                    errors.push(ValidationError {
                        code: "E032".to_string(),
                        message: "Tag key cannot be empty".to_string(),
                        location: Some(location.to_string()),
                    });
                }
                self.validate_regex(pattern, &format!("{}.pattern", location), errors);
            }

            TagExpr::And(exprs) => {
                for (idx, e) in exprs.iter().enumerate() {
                    self.validate_tag_expr(e, &format!("{}.and[{}]", location, idx), errors);
                }
            }

            TagExpr::Or(exprs) => {
                for (idx, e) in exprs.iter().enumerate() {
                    self.validate_tag_expr(e, &format!("{}.or[{}]", location, idx), errors);
                }
            }

            TagExpr::Not(inner) => {
                self.validate_tag_expr(inner, &format!("{}.not", location), errors);
            }
        }
    }

    fn validate_effect(
        &self,
        effect: &PolicyEffect,
        location: &str,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        match effect {
            PolicyEffect::Prefer { selector, weight } => {
                self.validate_node_selector(selector, location, errors, warnings);
                self.validate_weight(*weight, location, errors);
            }

            PolicyEffect::Require { selector } => {
                self.validate_node_selector(selector, location, errors, warnings);
            }

            PolicyEffect::Exclude { selector } => {
                self.validate_node_selector(selector, location, errors, warnings);
            }

            PolicyEffect::Affinity {
                with,
                scope: _,
                weight,
            } => {
                self.validate_affinity_target(with, location, errors);
                self.validate_weight(*weight, location, errors);
            }

            PolicyEffect::AntiAffinity {
                with,
                scope: _,
                weight,
            } => {
                self.validate_affinity_target(with, location, errors);
                self.validate_weight(*weight, location, errors);
            }

            PolicyEffect::SetResourceLimit { resource: _, limit } => {
                if *limit < 0.0 {
                    errors.push(ValidationError {
                        code: "E040".to_string(),
                        message: "Resource limit cannot be negative".to_string(),
                        location: Some(location.to_string()),
                    });
                }
            }

            PolicyEffect::SetPriority { priority, mode } => {
                // Priority can be negative for deprioritization
                if let PriorityMode::Multiply = mode {
                    if *priority < 0 {
                        warnings.push(ValidationWarning {
                            code: "W040".to_string(),
                            message: "Multiplying by negative priority will invert sign"
                                .to_string(),
                            location: Some(location.to_string()),
                        });
                    }
                }
            }

            PolicyEffect::ChargeQuota {
                quota_id,
                multiplier,
            } => {
                if quota_id.is_empty() {
                    errors.push(ValidationError {
                        code: "E041".to_string(),
                        message: "Quota ID cannot be empty".to_string(),
                        location: Some(location.to_string()),
                    });
                }
                if !self.known_quotas.is_empty() && !self.known_quotas.contains(quota_id) {
                    warnings.push(ValidationWarning {
                        code: "W041".to_string(),
                        message: format!("Unknown quota ID: {}", quota_id),
                        location: Some(location.to_string()),
                    });
                }
                if *multiplier < 0.0 {
                    errors.push(ValidationError {
                        code: "E042".to_string(),
                        message: "Quota multiplier cannot be negative".to_string(),
                        location: Some(location.to_string()),
                    });
                }
            }

            PolicyEffect::AllowPreemption { by_min_priority: _ } => {
                // Valid
            }

            PolicyEffect::DisallowPreemption => {
                // Valid
            }

            PolicyEffect::CustomScore {
                function_id,
                params: _,
            } => {
                if function_id.is_empty() {
                    errors.push(ValidationError {
                        code: "E043".to_string(),
                        message: "Custom scoring function ID cannot be empty".to_string(),
                        location: Some(location.to_string()),
                    });
                }
                if !self.known_scoring_functions.is_empty()
                    && !self.known_scoring_functions.contains(function_id)
                {
                    warnings.push(ValidationWarning {
                        code: "W042".to_string(),
                        message: format!("Unknown scoring function: {}", function_id),
                        location: Some(location.to_string()),
                    });
                }
            }
        }
    }

    fn validate_node_selector(
        &self,
        selector: &NodeSelector,
        location: &str,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        match selector {
            NodeSelector::All => {
                // Valid
            }

            NodeSelector::NodeIds(ids) => {
                if ids.is_empty() {
                    errors.push(ValidationError {
                        code: "E050".to_string(),
                        message: "Node ID list cannot be empty".to_string(),
                        location: Some(location.to_string()),
                    });
                }
            }

            NodeSelector::Group(group) => {
                if group.is_empty() {
                    errors.push(ValidationError {
                        code: "E051".to_string(),
                        message: "Node group name cannot be empty".to_string(),
                        location: Some(location.to_string()),
                    });
                }
                if !self.known_groups.is_empty() && !self.known_groups.contains(group) {
                    warnings.push(ValidationWarning {
                        code: "W050".to_string(),
                        message: format!("Unknown node group: {}", group),
                        location: Some(location.to_string()),
                    });
                }
            }

            NodeSelector::Tag(tag_expr) => {
                self.validate_tag_expr(tag_expr, &format!("{}.tag", location), errors);
            }
        }
    }

    fn validate_affinity_target(
        &self,
        target: &AffinityTarget,
        location: &str,
        errors: &mut Vec<ValidationError>,
    ) {
        match target {
            AffinityTarget::SameJob => {
                // Valid
            }

            AffinityTarget::JobPattern(pattern) => {
                self.validate_regex(pattern, &format!("{}.pattern", location), errors);
            }

            AffinityTarget::Tag(tag_expr) => {
                self.validate_tag_expr(tag_expr, &format!("{}.tag", location), errors);
            }
        }
    }

    fn validate_weight(&self, weight: f64, location: &str, errors: &mut Vec<ValidationError>) {
        if !(0.0..=1.0).contains(&weight) {
            errors.push(ValidationError {
                code: "E060".to_string(),
                message: format!("Weight must be between 0.0 and 1.0, got {}", weight),
                location: Some(location.to_string()),
            });
        }
    }

    fn validate_regex(&self, pattern: &str, location: &str, errors: &mut Vec<ValidationError>) {
        if let Err(e) = Regex::new(pattern) {
            errors.push(ValidationError {
                code: "E070".to_string(),
                message: format!("Invalid regex pattern '{}': {}", pattern, e),
                location: Some(location.to_string()),
            });
        }
    }

    fn validate_governance(
        &self,
        governance: &PolicyGovernance,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        if governance.author.is_empty() {
            warnings.push(ValidationWarning {
                code: "W060".to_string(),
                message: "Policy author is empty".to_string(),
                location: Some("governance.author".to_string()),
            });
        }

        if governance.authority_domain.is_empty() {
            warnings.push(ValidationWarning {
                code: "W061".to_string(),
                message: "Authority domain is empty".to_string(),
                location: Some("governance.authority_domain".to_string()),
            });
        }

        // Validate override policy
        match &governance.override_policy {
            OverridePolicyRef::RequiresApproval { approver_spec } => {
                if approver_spec.is_empty() {
                    errors.push(ValidationError {
                        code: "E080".to_string(),
                        message: "Approver spec cannot be empty".to_string(),
                        location: Some("governance.override_policy.approver_spec".to_string()),
                    });
                }
            }
            OverridePolicyRef::Custom(id) => {
                if id.is_empty() {
                    errors.push(ValidationError {
                        code: "E081".to_string(),
                        message: "Custom override policy ID cannot be empty".to_string(),
                        location: Some("governance.override_policy".to_string()),
                    });
                }
            }
            _ => {}
        }
    }

    fn check_logical_issues(&self, policy: &Policy, warnings: &mut Vec<ValidationWarning>) {
        // Check for redundant conditions
        if matches!(policy.condition, PolicyCondition::Always) && policy.effects.is_empty() {
            warnings.push(ValidationWarning {
                code: "W070".to_string(),
                message: "Policy with Always condition but no effects has no purpose".to_string(),
                location: None,
            });
        }

        // Check for potentially conflicting effects
        let mut has_require = false;
        let mut has_exclude = false;
        let mut require_selector = None;
        let mut exclude_selector = None;

        for effect in &policy.effects {
            match effect {
                PolicyEffect::Require { selector } => {
                    has_require = true;
                    require_selector = Some(selector);
                }
                PolicyEffect::Exclude { selector } => {
                    has_exclude = true;
                    exclude_selector = Some(selector);
                }
                _ => {}
            }
        }

        if has_require && has_exclude {
            // Check if they might conflict
            if let (Some(req), Some(exc)) = (require_selector, exclude_selector) {
                if Self::selectors_might_overlap(req, exc) {
                    warnings.push(ValidationWarning {
                        code: "W071".to_string(),
                        message: "Policy has both Require and Exclude effects that might conflict"
                            .to_string(),
                        location: None,
                    });
                }
            }
        }

        // Check for conflicting preemption settings
        let mut has_allow_preemption = false;
        let mut has_disallow_preemption = false;

        for effect in &policy.effects {
            match effect {
                PolicyEffect::AllowPreemption { .. } => has_allow_preemption = true,
                PolicyEffect::DisallowPreemption => has_disallow_preemption = true,
                _ => {}
            }
        }

        if has_allow_preemption && has_disallow_preemption {
            warnings.push(ValidationWarning {
                code: "W072".to_string(),
                message: "Policy has both AllowPreemption and DisallowPreemption effects"
                    .to_string(),
                location: None,
            });
        }
    }

    fn selectors_might_overlap(a: &NodeSelector, b: &NodeSelector) -> bool {
        match (a, b) {
            (NodeSelector::All, _) | (_, NodeSelector::All) => true,
            (NodeSelector::NodeIds(ids_a), NodeSelector::NodeIds(ids_b)) => {
                ids_a.iter().any(|id| ids_b.contains(id))
            }
            (NodeSelector::Group(g1), NodeSelector::Group(g2)) => g1 == g2,
            _ => true, // Assume potential overlap for complex cases
        }
    }

    fn is_valid_id(id: &str) -> bool {
        !id.is_empty()
            && id
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    }
}

/// Result of policy validation
#[derive(Debug)]
pub struct ValidationResult {
    /// Whether the policy is valid (no errors)
    pub valid: bool,
    /// List of errors found
    pub errors: Vec<ValidationError>,
    /// List of warnings found
    pub warnings: Vec<ValidationWarning>,
}

impl ValidationResult {
    /// Create a valid result with no issues
    pub fn valid() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Check if there are any errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Check if there are any warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Get a summary string
    pub fn summary(&self) -> String {
        if self.valid {
            if self.warnings.is_empty() {
                "Valid".to_string()
            } else {
                format!("Valid with {} warning(s)", self.warnings.len())
            }
        } else {
            format!(
                "Invalid: {} error(s), {} warning(s)",
                self.errors.len(),
                self.warnings.len()
            )
        }
    }
}

/// A validation error (prevents policy from being used)
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error code
    pub code: String,
    /// Human-readable message
    pub message: String,
    /// Location in the policy (e.g., "effects[2].weight")
    pub location: Option<String>,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(loc) = &self.location {
            write!(f, "[{}] {} at {}", self.code, self.message, loc)
        } else {
            write!(f, "[{}] {}", self.code, self.message)
        }
    }
}

/// A validation warning (policy can still be used)
#[derive(Debug, Clone)]
pub struct ValidationWarning {
    /// Warning code
    pub code: String,
    /// Human-readable message
    pub message: String,
    /// Location in the policy (e.g., "effects[2].weight")
    pub location: Option<String>,
}

impl std::fmt::Display for ValidationWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(loc) = &self.location {
            write!(f, "[{}] {} at {}", self.code, self.message, loc)
        } else {
            write!(f, "[{}] {}", self.code, self.message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_valid_policy() {
        let policy = Policy::new("valid-policy", "Valid Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(result.valid);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_empty_policy_id() {
        let mut policy = Policy::new("", "Empty ID Policy");
        policy
            .effects
            .push(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "E001"));
    }

    #[test]
    fn test_invalid_policy_id() {
        let policy = Policy::new("invalid policy!", "Invalid ID")
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "E002"));
    }

    #[test]
    fn test_empty_effects_warning() {
        let policy = Policy::new("no-effects", "No Effects");

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(result.valid); // Still valid, just has warning
        assert!(result.warnings.iter().any(|w| w.code == "W003"));
    }

    #[test]
    fn test_invalid_regex_pattern() {
        let policy = Policy::new("bad-regex", "Bad Regex").with_condition(
            PolicyCondition::JobMatches(JobMatcher::new().with_name_pattern("[invalid")),
        );

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "E070"));
    }

    #[test]
    fn test_invalid_priority_range() {
        let policy = Policy::new("bad-priority", "Bad Priority").with_condition(
            PolicyCondition::JobMatches(JobMatcher::new().with_priority_range(100, 50)),
        );

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "E012"));
    }

    #[test]
    fn test_invalid_weight() {
        let policy = Policy::new("bad-weight", "Bad Weight").with_effect(PolicyEffect::Prefer {
            selector: NodeSelector::All,
            weight: 1.5, // > 1.0
        });

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "E060"));
    }

    #[test]
    fn test_empty_cron() {
        let policy =
            Policy::new("empty-cron", "Empty Cron").with_condition(PolicyCondition::TimeWindow {
                cron: "".to_string(),
            });

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "E020"));
    }

    #[test]
    fn test_invalid_cron_fields() {
        let policy =
            Policy::new("bad-cron", "Bad Cron").with_condition(PolicyCondition::TimeWindow {
                cron: "* * *".to_string(), // Only 3 fields
            });

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.code == "E021"));
    }

    #[test]
    fn test_never_condition_warning() {
        let policy = Policy::new("never-policy", "Never Policy")
            .with_condition(PolicyCondition::Never)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(result.valid);
        assert!(result.warnings.iter().any(|w| w.code == "W010"));
    }

    #[test]
    fn test_conflicting_preemption() {
        let policy = Policy::new("conflict-preempt", "Conflicting Preemption")
            .with_effect(PolicyEffect::AllowPreemption {
                by_min_priority: 50,
            })
            .with_effect(PolicyEffect::DisallowPreemption);

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        assert!(result.valid); // Warning, not error
        assert!(result.warnings.iter().any(|w| w.code == "W072"));
    }

    #[test]
    fn test_unknown_group_warning() {
        let policy = Policy::new("unknown-group", "Unknown Group")
            .with_effect(PolicyEffect::require(NodeSelector::group("unknown-group")));

        let validator = PolicyValidator::new().with_groups(vec!["known-group".to_string()]);
        let result = validator.validate(&policy);

        assert!(result.valid);
        assert!(result.warnings.iter().any(|w| w.code == "W050"));
    }

    #[test]
    fn test_unknown_quota_warning() {
        let policy =
            Policy::new("unknown-quota", "Unknown Quota").with_effect(PolicyEffect::ChargeQuota {
                quota_id: "unknown-quota".to_string(),
                multiplier: 1.0,
            });

        let validator = PolicyValidator::new().with_quotas(vec!["known-quota".to_string()]);
        let result = validator.validate(&policy);

        assert!(result.valid);
        assert!(result.warnings.iter().any(|w| w.code == "W041"));
    }

    #[test]
    fn test_compound_conditions() {
        let policy = Policy::new("compound", "Compound Conditions").with_condition(
            PolicyCondition::and(vec![
                PolicyCondition::JobMatches(JobMatcher::new().with_name_pattern("ml-.*")),
                PolicyCondition::or(vec![
                    PolicyCondition::SubmitterMatches(SubmitterMatcher::new().with_domain("ml")),
                    PolicyCondition::ResourceMatches(ResourceMatcher::new().with_gpu(true, None)),
                ]),
            ]),
        );

        let validator = PolicyValidator::new();
        let result = validator.validate(&policy);

        // Should have warning about no effects
        assert!(result.valid);
    }

    #[test]
    fn test_validation_summary() {
        let result = ValidationResult {
            valid: true,
            errors: Vec::new(),
            warnings: vec![ValidationWarning {
                code: "W001".to_string(),
                message: "test".to_string(),
                location: None,
            }],
        };

        assert_eq!(result.summary(), "Valid with 1 warning(s)");
    }

    #[test]
    fn test_validation_error_display() {
        let error = ValidationError {
            code: "E001".to_string(),
            message: "Test error".to_string(),
            location: Some("effects[0]".to_string()),
        };

        let display = format!("{}", error);
        assert!(display.contains("E001"));
        assert!(display.contains("Test error"));
        assert!(display.contains("effects[0]"));
    }
}
