// Marabunta - Licensed under the MIT License.
//! Policy Conflict Detection
//!
//! This module provides conflict detection between policies. It identifies
//! potential issues when policies might interfere with each other, such as
//! contradictory effects or ambiguous priority.

use std::collections::{HashMap, HashSet};

use super::engine::PolicyEngine;
use super::ir::*;

/// Detects conflicts between policies
pub struct ConflictDetector {
    /// Reference to the policy engine (for access to registered policies)
    policies: HashMap<PolicyId, Policy>,
}

impl ConflictDetector {
    /// Create a new conflict detector with the given policies
    pub fn new(policies: HashMap<PolicyId, Policy>) -> Self {
        Self { policies }
    }

    /// Create a conflict detector from a policy engine
    pub fn from_engine(engine: &PolicyEngine) -> Self {
        let policies: HashMap<PolicyId, Policy> = engine
            .list()
            .into_iter()
            .map(|p| (p.id.clone(), p.clone()))
            .collect();
        Self { policies }
    }

    /// Detect conflicts between all registered policies
    pub fn detect_all_conflicts(&self) -> Vec<PolicyConflict> {
        let policy_list: Vec<&Policy> = self.policies.values().collect();
        self.detect_conflicts(&policy_list)
    }

    /// Detect conflicts between the given policies
    pub fn detect_conflicts(&self, policies: &[&Policy]) -> Vec<PolicyConflict> {
        let mut conflicts = Vec::new();

        // Check each pair of policies
        for i in 0..policies.len() {
            for j in (i + 1)..policies.len() {
                let policy_a = policies[i];
                let policy_b = policies[j];

                // Skip if either is disabled
                if !policy_a.enabled || !policy_b.enabled {
                    continue;
                }

                // Check for various conflict types
                conflicts.extend(self.check_contradictory_effects(policy_a, policy_b));
                conflicts.extend(self.check_overlapping_conditions(policy_a, policy_b));
                conflicts.extend(self.check_priority_ambiguity(policy_a, policy_b));
            }
        }

        // Check for circular dependencies
        conflicts.extend(self.check_circular_dependencies(policies));

        conflicts
    }

    /// Check if a new policy conflicts with existing policies
    pub fn check_new_policy(&self, new_policy: &Policy) -> Vec<PolicyConflict> {
        let mut all_policies: Vec<&Policy> = self.policies.values().collect();
        all_policies.push(new_policy);

        // Filter conflicts to only include those involving the new policy
        self.detect_conflicts(&all_policies)
            .into_iter()
            .filter(|c| c.policy_a == new_policy.id || c.policy_b == new_policy.id)
            .collect()
    }

    /// Check for contradictory effects between two policies
    fn check_contradictory_effects(
        &self,
        policy_a: &Policy,
        policy_b: &Policy,
    ) -> Vec<PolicyConflict> {
        let mut conflicts = Vec::new();

        // Check if conditions could overlap
        if !self.conditions_could_overlap(&policy_a.condition, &policy_b.condition) {
            return conflicts;
        }

        // Look for Require vs Exclude conflicts
        for effect_a in &policy_a.effects {
            for effect_b in &policy_b.effects {
                if let Some(conflict) =
                    self.check_effect_contradiction(policy_a, policy_b, effect_a, effect_b)
                {
                    conflicts.push(conflict);
                }
            }
        }

        conflicts
    }

    /// Check if two effects contradict each other
    fn check_effect_contradiction(
        &self,
        policy_a: &Policy,
        policy_b: &Policy,
        effect_a: &PolicyEffect,
        effect_b: &PolicyEffect,
    ) -> Option<PolicyConflict> {
        match (effect_a, effect_b) {
            // Require vs Exclude on overlapping nodes
            (
                PolicyEffect::Require { selector: sel_a },
                PolicyEffect::Exclude { selector: sel_b },
            )
            | (
                PolicyEffect::Exclude { selector: sel_a },
                PolicyEffect::Require { selector: sel_b },
            ) => {
                if self.selectors_could_overlap(sel_a, sel_b) {
                    Some(PolicyConflict {
                        policy_a: policy_a.id.clone(),
                        policy_b: policy_b.id.clone(),
                        conflict_type: ConflictType::ContradictoryEffects,
                        description: format!(
                            "Policy '{}' requires nodes that policy '{}' excludes",
                            policy_a.name, policy_b.name
                        ),
                        severity: ConflictSeverity::Error,
                        suggested_resolution: Some(
                            "Adjust selectors to be mutually exclusive, or set different conditions"
                                .to_string(),
                        ),
                    })
                } else {
                    None
                }
            }

            // AllowPreemption vs DisallowPreemption
            (PolicyEffect::AllowPreemption { .. }, PolicyEffect::DisallowPreemption)
            | (PolicyEffect::DisallowPreemption, PolicyEffect::AllowPreemption { .. }) => {
                Some(PolicyConflict {
                    policy_a: policy_a.id.clone(),
                    policy_b: policy_b.id.clone(),
                    conflict_type: ConflictType::ContradictoryEffects,
                    description: format!(
                        "Policy '{}' allows preemption while policy '{}' disallows it",
                        policy_a.name, policy_b.name
                    ),
                    severity: ConflictSeverity::Warning,
                    suggested_resolution: Some(
                        "Use conflict_priority to determine which policy wins".to_string(),
                    ),
                })
            }

            // Conflicting priority modifications
            (
                PolicyEffect::SetPriority {
                    priority: p1,
                    mode: m1,
                },
                PolicyEffect::SetPriority {
                    priority: p2,
                    mode: m2,
                },
            ) => {
                if matches!(m1, PriorityMode::Set) && matches!(m2, PriorityMode::Set) && p1 != p2 {
                    Some(PolicyConflict {
                        policy_a: policy_a.id.clone(),
                        policy_b: policy_b.id.clone(),
                        conflict_type: ConflictType::ContradictoryEffects,
                        description: format!(
                            "Both policies set priority to different values: {} vs {}",
                            p1, p2
                        ),
                        severity: ConflictSeverity::Warning,
                        suggested_resolution: Some(
                            "Use Add mode instead of Set, or use conflict_priority".to_string(),
                        ),
                    })
                } else {
                    None
                }
            }

            _ => None,
        }
    }

    /// Check for overlapping conditions with different effects
    fn check_overlapping_conditions(
        &self,
        policy_a: &Policy,
        policy_b: &Policy,
    ) -> Vec<PolicyConflict> {
        let mut conflicts = Vec::new();

        // Check if conditions overlap
        if !self.conditions_could_overlap(&policy_a.condition, &policy_b.condition) {
            return conflicts;
        }

        // Check if effects are different but conditions overlap
        if !self.effects_are_compatible(&policy_a.effects, &policy_b.effects) {
            // Check if priority is the same (ambiguous)
            if policy_a.governance.conflict_priority == policy_b.governance.conflict_priority {
                conflicts.push(PolicyConflict {
                    policy_a: policy_a.id.clone(),
                    policy_b: policy_b.id.clone(),
                    conflict_type: ConflictType::OverlappingConditions,
                    description: format!(
                        "Policies '{}' and '{}' have overlapping conditions with different effects",
                        policy_a.name, policy_b.name
                    ),
                    severity: ConflictSeverity::Info,
                    suggested_resolution: Some(
                        "Set different conflict_priority values to resolve ordering".to_string(),
                    ),
                });
            }
        }

        conflicts
    }

    /// Check for priority ambiguity between policies
    fn check_priority_ambiguity(
        &self,
        policy_a: &Policy,
        policy_b: &Policy,
    ) -> Vec<PolicyConflict> {
        let mut conflicts = Vec::new();

        // Check if both policies could apply and have same priority
        if self.conditions_could_overlap(&policy_a.condition, &policy_b.condition)
            && policy_a.governance.conflict_priority == policy_b.governance.conflict_priority {
                // Check if they have conflicting mandatory effects
                let a_has_mandatory = policy_a.effects.iter().any(|e| {
                    matches!(
                        e,
                        PolicyEffect::Require { .. } | PolicyEffect::Exclude { .. }
                    )
                });
                let b_has_mandatory = policy_b.effects.iter().any(|e| {
                    matches!(
                        e,
                        PolicyEffect::Require { .. } | PolicyEffect::Exclude { .. }
                    )
                });

                if a_has_mandatory && b_has_mandatory {
                    conflicts.push(PolicyConflict {
                        policy_a: policy_a.id.clone(),
                        policy_b: policy_b.id.clone(),
                        conflict_type: ConflictType::PriorityAmbiguity,
                        description: format!(
                            "Policies '{}' and '{}' have same conflict_priority ({}) with mandatory effects",
                            policy_a.name, policy_b.name, policy_a.governance.conflict_priority
                        ),
                        severity: ConflictSeverity::Warning,
                        suggested_resolution: Some(
                            "Set different conflict_priority values".to_string(),
                        ),
                    });
                }
            }

        conflicts
    }

    /// Check for circular dependencies between policies
    fn check_circular_dependencies(&self, policies: &[&Policy]) -> Vec<PolicyConflict> {
        let mut conflicts = Vec::new();

        // Build a dependency graph based on override policies
        let mut depends_on: HashMap<&PolicyId, HashSet<&PolicyId>> = HashMap::new();

        for policy in policies {
            if let OverridePolicyRef::Custom(ref_id) = &policy.governance.override_policy {
                depends_on
                    .entry(&policy.id)
                    .or_default()
                    .insert(ref_id);
            }
        }

        // Simple cycle detection using DFS
        for policy in policies {
            let mut visited = HashSet::new();
            let mut path = Vec::new();

            if self.has_cycle(&policy.id, &depends_on, &mut visited, &mut path) {
                // Found a cycle
                conflicts.push(PolicyConflict {
                    policy_a: policy.id.clone(),
                    policy_b: path.last().unwrap_or(&policy.id).to_string(),
                    conflict_type: ConflictType::CircularDependency,
                    description: format!(
                        "Circular dependency detected: {}",
                        path.iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(" -> ")
                    ),
                    severity: ConflictSeverity::Error,
                    suggested_resolution: Some(
                        "Remove circular reference in override_policy".to_string(),
                    ),
                });
            }
        }

        conflicts
    }

    /// Check if there's a cycle starting from the given node
    fn has_cycle<'a>(
        &self,
        node: &'a PolicyId,
        graph: &HashMap<&'a PolicyId, HashSet<&'a PolicyId>>,
        visited: &mut HashSet<&'a PolicyId>,
        path: &mut Vec<String>,
    ) -> bool {
        if visited.contains(node) {
            return true;
        }

        visited.insert(node);
        path.push(node.clone());

        if let Some(deps) = graph.get(node) {
            for dep in deps {
                if self.has_cycle(dep, graph, visited, path) {
                    return true;
                }
            }
        }

        path.pop();
        false
    }

    /// Check if two conditions could both match the same job
    fn conditions_could_overlap(&self, cond_a: &PolicyCondition, cond_b: &PolicyCondition) -> bool {
        match (cond_a, cond_b) {
            // Always overlaps with everything except Never
            (PolicyCondition::Always, PolicyCondition::Never)
            | (PolicyCondition::Never, PolicyCondition::Always) => false,
            (PolicyCondition::Always, _) | (_, PolicyCondition::Always) => true,

            // Never never overlaps
            (PolicyCondition::Never, _) | (_, PolicyCondition::Never) => false,

            // Job matchers might overlap
            (PolicyCondition::JobMatches(m1), PolicyCondition::JobMatches(m2)) => {
                self.job_matchers_could_overlap(m1, m2)
            }

            // Submitter matchers might overlap
            (PolicyCondition::SubmitterMatches(m1), PolicyCondition::SubmitterMatches(m2)) => {
                self.submitter_matchers_could_overlap(m1, m2)
            }

            // Resource matchers might overlap
            (PolicyCondition::ResourceMatches(m1), PolicyCondition::ResourceMatches(m2)) => {
                self.resource_matchers_could_overlap(m1, m2)
            }

            // Time windows are harder to analyze statically
            (PolicyCondition::TimeWindow { .. }, PolicyCondition::TimeWindow { .. }) => true,

            // Mixed conditions - assume possible overlap
            _ => true,
        }
    }

    /// Check if two job matchers could match the same job
    fn job_matchers_could_overlap(&self, m1: &JobMatcher, m2: &JobMatcher) -> bool {
        // Check priority ranges
        if let (Some((min1, max1)), Some((min2, max2))) = (&m1.priority_range, &m2.priority_range) {
            if max1 < min2 || max2 < min1 {
                return false;
            }
        }

        // Check job types
        if let (Some(types1), Some(types2)) = (&m1.job_type, &m2.job_type) {
            if !types1.iter().any(|t| types2.contains(t)) {
                return false;
            }
        }

        // For patterns and tags, assume possible overlap
        true
    }

    /// Check if two submitter matchers could match the same submitter
    fn submitter_matchers_could_overlap(
        &self,
        m1: &SubmitterMatcher,
        m2: &SubmitterMatcher,
    ) -> bool {
        // Check specific principal IDs
        if let (Some(id1), Some(id2)) = (&m1.principal_id, &m2.principal_id) {
            if id1 != id2 {
                return false;
            }
        }

        // Check domains
        if let (Some(d1), Some(d2)) = (&m1.in_domain, &m2.in_domain) {
            if d1 != d2 {
                return false;
            }
        }

        true
    }

    /// Check if two resource matchers could match the same request
    fn resource_matchers_could_overlap(&self, m1: &ResourceMatcher, m2: &ResourceMatcher) -> bool {
        // Check CPU ranges
        if let (Some(max1), Some(min2)) = (m1.max_cpu, m2.min_cpu) {
            if max1 < min2 {
                return false;
            }
        }
        if let (Some(min1), Some(max2)) = (m1.min_cpu, m2.max_cpu) {
            if min1 > max2 {
                return false;
            }
        }

        // Check memory ranges
        if let (Some(max1), Some(min2)) = (m1.max_memory_gb, m2.min_memory_gb) {
            if max1 < min2 {
                return false;
            }
        }

        // Check GPU requirements
        if let (Some(req1), Some(req2)) = (m1.requires_gpu, m2.requires_gpu) {
            if req1 != req2 {
                return false;
            }
        }

        true
    }

    /// Check if two node selectors could match the same nodes
    fn selectors_could_overlap(&self, sel_a: &NodeSelector, sel_b: &NodeSelector) -> bool {
        match (sel_a, sel_b) {
            (NodeSelector::All, _) | (_, NodeSelector::All) => true,

            (NodeSelector::NodeIds(ids_a), NodeSelector::NodeIds(ids_b)) => {
                ids_a.iter().any(|id| ids_b.contains(id))
            }

            (NodeSelector::Group(g1), NodeSelector::Group(g2)) => g1 == g2,

            // Assume tags could overlap
            (NodeSelector::Tag(_), _) | (_, NodeSelector::Tag(_)) => true,

            _ => true,
        }
    }

    /// Check if two effect lists are compatible (no direct conflicts)
    fn effects_are_compatible(
        &self,
        effects_a: &[PolicyEffect],
        effects_b: &[PolicyEffect],
    ) -> bool {
        for effect_a in effects_a {
            for effect_b in effects_b {
                // Check for direct conflicts
                match (effect_a, effect_b) {
                    (
                        PolicyEffect::Require { selector: s1 },
                        PolicyEffect::Exclude { selector: s2 },
                    )
                    | (
                        PolicyEffect::Exclude { selector: s1 },
                        PolicyEffect::Require { selector: s2 },
                    ) => {
                        if self.selectors_could_overlap(s1, s2) {
                            return false;
                        }
                    }
                    (PolicyEffect::AllowPreemption { .. }, PolicyEffect::DisallowPreemption)
                    | (PolicyEffect::DisallowPreemption, PolicyEffect::AllowPreemption { .. }) => {
                        return false;
                    }
                    // Two Prefer effects with different (non-overlapping) selectors are incompatible
                    // as they compete for node selection
                    (
                        PolicyEffect::Prefer { selector: s1, .. },
                        PolicyEffect::Prefer { selector: s2, .. },
                    ) => {
                        if !self.selectors_could_overlap(s1, s2) {
                            return false;
                        }
                    }
                    _ => {}
                }
            }
        }
        true
    }
}

/// A conflict between two policies
#[derive(Debug, Clone)]
pub struct PolicyConflict {
    /// First policy in the conflict
    pub policy_a: PolicyId,
    /// Second policy in the conflict
    pub policy_b: PolicyId,
    /// Type of conflict
    pub conflict_type: ConflictType,
    /// Human-readable description
    pub description: String,
    /// Severity of the conflict
    pub severity: ConflictSeverity,
    /// Suggested way to resolve the conflict
    pub suggested_resolution: Option<String>,
}

impl std::fmt::Display for PolicyConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{:?}] {} <-> {}: {}",
            self.severity, self.policy_a, self.policy_b, self.description
        )
    }
}

/// Type of conflict between policies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType {
    /// One policy requires what another excludes
    ContradictoryEffects,
    /// Both policies match the same jobs with different effects
    OverlappingConditions,
    /// Unclear which policy should take precedence
    PriorityAmbiguity,
    /// Policies reference each other in override settings
    CircularDependency,
}

/// Severity of a conflict
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConflictSeverity {
    /// Informational - policies work together but reviewer should be aware
    Info,
    /// Warning - policies might not work as expected
    Warning,
    /// Error - policies will definitely cause problems
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn create_policy(id: &str, name: &str) -> Policy {
        Policy::new(id, name)
    }

    #[test]
    fn test_no_conflicts() {
        let policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::JobMatches(
                JobMatcher::new().with_priority_range(0, 50),
            ))
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::JobMatches(
                JobMatcher::new().with_priority_range(51, 100),
            ))
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.8));

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_require_exclude_conflict() {
        let policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::require(NodeSelector::group(
                "production".to_string(),
            )));

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::exclude(NodeSelector::group(
                "production".to_string(),
            )));

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        assert!(!conflicts.is_empty());
        assert!(conflicts
            .iter()
            .any(|c| c.conflict_type == ConflictType::ContradictoryEffects));
    }

    #[test]
    fn test_preemption_conflict() {
        let policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::AllowPreemption {
                by_min_priority: 50,
            });

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::DisallowPreemption);

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        assert!(!conflicts.is_empty());
        assert!(conflicts
            .iter()
            .any(|c| c.conflict_type == ConflictType::ContradictoryEffects));
    }

    #[test]
    fn test_priority_ambiguity() {
        let policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::require(NodeSelector::all()))
            .with_governance(PolicyGovernance::new("admin").with_conflict_priority(10));

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::require(NodeSelector::all()))
            .with_governance(PolicyGovernance::new("admin").with_conflict_priority(10));

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        assert!(conflicts
            .iter()
            .any(|c| c.conflict_type == ConflictType::PriorityAmbiguity));
    }

    #[test]
    fn test_overlapping_conditions() {
        let policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::node_ids(vec!["node-1".to_string()]),
                1.0,
            ))
            .with_governance(PolicyGovernance::new("admin").with_conflict_priority(0));

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::node_ids(vec!["node-2".to_string()]),
                1.0,
            ))
            .with_governance(PolicyGovernance::new("admin").with_conflict_priority(0));

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        // Should have overlapping conditions info
        assert!(conflicts
            .iter()
            .any(|c| c.conflict_type == ConflictType::OverlappingConditions));
    }

    #[test]
    fn test_check_new_policy() {
        let existing = create_policy("existing", "Existing")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::require(NodeSelector::group(
                "prod".to_string(),
            )));

        let mut policies = HashMap::new();
        policies.insert(existing.id.clone(), existing);

        let new_policy = create_policy("new", "New")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::exclude(NodeSelector::group(
                "prod".to_string(),
            )));

        let detector = ConflictDetector::new(policies);
        let conflicts = detector.check_new_policy(&new_policy);

        assert!(!conflicts.is_empty());
        assert!(conflicts
            .iter()
            .all(|c| c.policy_a == "new" || c.policy_b == "new"));
    }

    #[test]
    fn test_disabled_policy_ignored() {
        let mut policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::require(NodeSelector::all()));
        policy_a.enabled = false;

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::exclude(NodeSelector::all()));

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        // Should be empty because policy_a is disabled
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_non_overlapping_job_matchers() {
        let policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::JobMatches(
                JobMatcher::new().with_job_types(vec!["monte_carlo".to_string()]),
            ))
            .with_effect(PolicyEffect::require(NodeSelector::all()));

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::JobMatches(
                JobMatcher::new().with_job_types(vec!["parameter_sweep".to_string()]),
            ))
            .with_effect(PolicyEffect::exclude(NodeSelector::all()));

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        // No conflicts because job types don't overlap
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_conflict_severity_ordering() {
        assert!(ConflictSeverity::Info < ConflictSeverity::Warning);
        assert!(ConflictSeverity::Warning < ConflictSeverity::Error);
    }

    #[test]
    fn test_conflict_display() {
        let conflict = PolicyConflict {
            policy_a: "policy-a".to_string(),
            policy_b: "policy-b".to_string(),
            conflict_type: ConflictType::ContradictoryEffects,
            description: "Test conflict".to_string(),
            severity: ConflictSeverity::Error,
            suggested_resolution: None,
        };

        let display = format!("{}", conflict);
        assert!(display.contains("policy-a"));
        assert!(display.contains("policy-b"));
        assert!(display.contains("Test conflict"));
    }

    #[test]
    fn test_priority_set_conflict() {
        let policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::SetPriority {
                priority: 100,
                mode: PriorityMode::Set,
            });

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::SetPriority {
                priority: 50,
                mode: PriorityMode::Set,
            });

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        assert!(conflicts
            .iter()
            .any(|c| c.conflict_type == ConflictType::ContradictoryEffects));
    }

    #[test]
    fn test_resource_matcher_no_overlap() {
        let policy_a = create_policy("a", "Policy A")
            .with_condition(PolicyCondition::ResourceMatches(
                ResourceMatcher::new().with_cpu_range(Some(1), Some(4)),
            ))
            .with_effect(PolicyEffect::require(NodeSelector::all()));

        let policy_b = create_policy("b", "Policy B")
            .with_condition(PolicyCondition::ResourceMatches(
                ResourceMatcher::new().with_cpu_range(Some(8), Some(16)),
            ))
            .with_effect(PolicyEffect::exclude(NodeSelector::all()));

        let detector = ConflictDetector::new(HashMap::new());
        let conflicts = detector.detect_conflicts(&[&policy_a, &policy_b]);

        // No conflicts because CPU ranges don't overlap
        assert!(conflicts.is_empty());
    }
}
