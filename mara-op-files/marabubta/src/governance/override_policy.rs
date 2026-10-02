// Marabunta - Licensed under the MIT License.
//! Override Policies
//!
//! This module defines policies that control when and how actions can be
//! overridden by principals with higher authority.

use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::authority::AuthorityChecker;
use super::principal::PrincipalId;

/// Policy governing when an action can be overridden.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OverridePolicy {
    /// No one can override this action - it's mandatory.
    Mandatory,

    /// Anyone with sufficient priority can override (default behavior).
    Blueprint {
        /// Minimum priority required to override.
        min_priority: u32,
        /// Whether the overrider must have authority in the same domain.
        require_same_domain: bool,
    },

    /// Anyone can override - this is just a suggestion.
    Advisory,

    /// Override requires an approval workflow.
    RequiresApproval {
        /// Who can approve the override.
        approvers: ApproverSpec,
        /// How long to wait for approval.
        #[serde(with = "duration_serde")]
        timeout: Duration,
        /// What happens if approval times out.
        default_on_timeout: ApprovalDefault,
    },

    /// Trial override - allowed for limited uses, then requires confirmation.
    Trial {
        /// Maximum number of override uses.
        max_uses: u32,
        /// Policy to apply after max uses reached.
        then: Box<OverridePolicy>,
    },

    /// Time-limited override - allowed but must be renewed.
    TimeLimited {
        /// Maximum duration of the override.
        #[serde(with = "duration_serde")]
        max_duration: Duration,
        /// Policy to apply when time expires.
        then: Box<OverridePolicy>,
    },
}

impl OverridePolicy {
    /// Creates a mandatory policy (no override allowed).
    pub fn mandatory() -> Self {
        OverridePolicy::Mandatory
    }

    /// Creates a blueprint policy with default settings.
    pub fn blueprint(min_priority: u32) -> Self {
        OverridePolicy::Blueprint {
            min_priority,
            require_same_domain: true,
        }
    }

    /// Creates a blueprint policy that doesn't require same domain authority.
    pub fn blueprint_any_domain(min_priority: u32) -> Self {
        OverridePolicy::Blueprint {
            min_priority,
            require_same_domain: false,
        }
    }

    /// Creates an advisory policy (always overridable).
    pub fn advisory() -> Self {
        OverridePolicy::Advisory
    }

    /// Creates a policy requiring approval.
    pub fn requires_approval(approvers: ApproverSpec, timeout: Duration) -> Self {
        OverridePolicy::RequiresApproval {
            approvers,
            timeout,
            default_on_timeout: ApprovalDefault::Reject,
        }
    }

    /// Creates a trial policy.
    pub fn trial(max_uses: u32, then: OverridePolicy) -> Self {
        OverridePolicy::Trial {
            max_uses,
            then: Box::new(then),
        }
    }

    /// Creates a time-limited policy.
    pub fn time_limited(max_duration: Duration, then: OverridePolicy) -> Self {
        OverridePolicy::TimeLimited {
            max_duration,
            then: Box::new(then),
        }
    }

    /// Returns a human-readable description of this policy.
    pub fn description(&self) -> String {
        match self {
            OverridePolicy::Mandatory => "Cannot be overridden".to_string(),
            OverridePolicy::Blueprint {
                min_priority,
                require_same_domain,
            } => {
                if *require_same_domain {
                    format!(
                        "Can be overridden by principals with priority >= {} in the same domain",
                        min_priority
                    )
                } else {
                    format!(
                        "Can be overridden by principals with priority >= {}",
                        min_priority
                    )
                }
            }
            OverridePolicy::Advisory => "Advisory only - can be overridden by anyone".to_string(),
            OverridePolicy::RequiresApproval {
                approvers, timeout, ..
            } => {
                format!(
                    "Requires approval from {} (timeout: {}s)",
                    approvers.description(),
                    timeout.as_secs()
                )
            }
            OverridePolicy::Trial { max_uses, then } => {
                format!(
                    "Trial: {} uses allowed, then: {}",
                    max_uses,
                    then.description()
                )
            }
            OverridePolicy::TimeLimited { max_duration, then } => {
                format!(
                    "Time-limited: max {}s, then: {}",
                    max_duration.as_secs(),
                    then.description()
                )
            }
        }
    }
}

impl Default for OverridePolicy {
    fn default() -> Self {
        OverridePolicy::Blueprint {
            min_priority: 0,
            require_same_domain: true,
        }
    }
}

impl std::fmt::Display for OverridePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description())
    }
}

/// Specification for who can approve an override.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApproverSpec {
    /// Specific principals by ID.
    Specific(Vec<PrincipalId>),
    /// Anyone with at least this priority.
    MinPriority(u32),
    /// Anyone with authority over the relevant domain.
    DomainAuthority,
    /// Any of these specs (OR).
    Any(Vec<ApproverSpec>),
    /// All of these specs must approve (AND).
    All(Vec<ApproverSpec>),
}

impl ApproverSpec {
    /// Creates an approver spec for specific principals.
    pub fn specific(ids: Vec<PrincipalId>) -> Self {
        ApproverSpec::Specific(ids)
    }

    /// Creates an approver spec requiring minimum priority.
    pub fn min_priority(priority: u32) -> Self {
        ApproverSpec::MinPriority(priority)
    }

    /// Creates an approver spec requiring domain authority.
    pub fn domain_authority() -> Self {
        ApproverSpec::DomainAuthority
    }

    /// Creates a composite spec where any of the specs is sufficient.
    pub fn any_of(specs: Vec<ApproverSpec>) -> Self {
        ApproverSpec::Any(specs)
    }

    /// Creates a composite spec where all specs must be satisfied.
    pub fn all_of(specs: Vec<ApproverSpec>) -> Self {
        ApproverSpec::All(specs)
    }

    /// Returns a description of this spec.
    pub fn description(&self) -> String {
        match self {
            ApproverSpec::Specific(ids) => {
                if ids.len() == 1 {
                    ids[0].clone()
                } else {
                    format!("one of: {}", ids.join(", "))
                }
            }
            ApproverSpec::MinPriority(p) => format!("anyone with priority >= {}", p),
            ApproverSpec::DomainAuthority => "domain authority".to_string(),
            ApproverSpec::Any(specs) => {
                let parts: Vec<_> = specs.iter().map(|s| s.description()).collect();
                format!("any of ({})", parts.join(" OR "))
            }
            ApproverSpec::All(specs) => {
                let parts: Vec<_> = specs.iter().map(|s| s.description()).collect();
                format!("all of ({})", parts.join(" AND "))
            }
        }
    }

    /// Checks if a principal matches this approver spec.
    pub async fn matches(
        &self,
        principal_id: &PrincipalId,
        authority: &AuthorityChecker,
        domain: &str,
    ) -> bool {
        match self {
            ApproverSpec::Specific(ids) => ids.contains(principal_id),
            ApproverSpec::MinPriority(min) => {
                if let Some(principal) = authority.get_principal(principal_id).await {
                    principal.priority >= *min
                } else {
                    false
                }
            }
            ApproverSpec::DomainAuthority => authority.has_authority(principal_id, domain).await,
            ApproverSpec::Any(specs) => {
                for spec in specs {
                    if Box::pin(spec.matches(principal_id, authority, domain)).await {
                        return true;
                    }
                }
                false
            }
            ApproverSpec::All(specs) => {
                for spec in specs {
                    if !Box::pin(spec.matches(principal_id, authority, domain)).await {
                        return false;
                    }
                }
                true
            }
        }
    }

    /// Finds all principals that match this spec.
    pub async fn find_matching_principals(
        &self,
        authority: &AuthorityChecker,
        domain: &str,
    ) -> Vec<PrincipalId> {
        match self {
            ApproverSpec::Specific(ids) => ids.clone(),
            ApproverSpec::MinPriority(min) => {
                let mut result = Vec::new();
                for principal in authority.list_principals().await {
                    if principal.priority >= *min {
                        result.push(principal.id);
                    }
                }
                result
            }
            ApproverSpec::DomainAuthority => authority
                .authorities_for_domain(domain)
                .await
                .into_iter()
                .map(|p| p.id)
                .collect(),
            ApproverSpec::Any(specs) => {
                let mut result = Vec::new();
                for spec in specs {
                    result.extend(Box::pin(spec.find_matching_principals(authority, domain)).await);
                }
                result.sort();
                result.dedup();
                result
            }
            ApproverSpec::All(specs) => {
                // For All, we need principals that match ALL specs
                if specs.is_empty() {
                    return Vec::new();
                }
                let first_matches =
                    Box::pin(specs[0].find_matching_principals(authority, domain)).await;
                let mut result: Vec<_> = first_matches;

                // Filter down to principals that match all specs
                for spec in &specs[1..] {
                    let spec_matches = futures::executor::block_on(
                        spec.find_matching_principals(authority, domain),
                    );
                    result.retain(|p| spec_matches.contains(p));
                }
                result
            }
        }
    }
}

/// What happens when approval times out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApprovalDefault {
    /// Approve the override on timeout.
    Approve,
    /// Reject the override on timeout.
    Reject,
    /// Escalate to another approver.
    Escalate { to: ApproverSpec },
}

impl ApprovalDefault {
    /// Creates an approval default.
    pub fn approve() -> Self {
        ApprovalDefault::Approve
    }

    /// Creates a rejection default.
    pub fn reject() -> Self {
        ApprovalDefault::Reject
    }

    /// Creates an escalation default.
    pub fn escalate(to: ApproverSpec) -> Self {
        ApprovalDefault::Escalate { to }
    }
}

/// Context information for an override attempt.
#[derive(Debug, Clone)]
pub struct OverrideContext {
    /// The principal who originally set the policy.
    pub original_author: PrincipalId,
    /// The priority of the original author.
    pub original_priority: u32,
    /// How many times this has been overridden (for Trial policies).
    pub override_count: u32,
    /// Requested duration of the override (for TimeLimited policies).
    pub override_duration: Option<Duration>,
}

impl OverrideContext {
    /// Creates a new override context.
    pub fn new(original_author: impl Into<PrincipalId>, original_priority: u32) -> Self {
        Self {
            original_author: original_author.into(),
            original_priority,
            override_count: 0,
            override_duration: None,
        }
    }

    /// Sets the override count.
    pub fn with_override_count(mut self, count: u32) -> Self {
        self.override_count = count;
        self
    }

    /// Sets the requested override duration.
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.override_duration = Some(duration);
        self
    }
}

/// Result of an override check.
#[derive(Debug, Clone)]
pub enum OverrideResult {
    /// Override is allowed.
    Allowed,
    /// Override is denied.
    Denied { reason: String },
    /// Override requires approval from specific principals.
    RequiresApproval { approvers: Vec<PrincipalId> },
    /// Override is allowed as a trial (limited uses).
    TrialAllowed { remaining_uses: u32 },
    /// Override is allowed but time-limited.
    TimeLimitedAllowed { max_duration: Duration },
}

impl OverrideResult {
    /// Returns true if the override is allowed (possibly with conditions).
    pub fn is_allowed(&self) -> bool {
        matches!(
            self,
            OverrideResult::Allowed
                | OverrideResult::TrialAllowed { .. }
                | OverrideResult::TimeLimitedAllowed { .. }
        )
    }

    /// Returns true if the override requires further action.
    pub fn requires_approval(&self) -> bool {
        matches!(self, OverrideResult::RequiresApproval { .. })
    }
}

impl std::fmt::Display for OverrideResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OverrideResult::Allowed => write!(f, "Override allowed"),
            OverrideResult::Denied { reason } => write!(f, "Override denied: {}", reason),
            OverrideResult::RequiresApproval { approvers } => {
                write!(f, "Requires approval from: {}", approvers.join(", "))
            }
            OverrideResult::TrialAllowed { remaining_uses } => {
                write!(
                    f,
                    "Trial override allowed ({} uses remaining)",
                    remaining_uses
                )
            }
            OverrideResult::TimeLimitedAllowed { max_duration } => {
                write!(
                    f,
                    "Time-limited override allowed (max {}s)",
                    max_duration.as_secs()
                )
            }
        }
    }
}

/// Checker for override policies.
pub struct OverrideChecker {
    /// The authority checker used for validating principals.
    authority: AuthorityChecker,
}

impl OverrideChecker {
    /// Creates a new override checker.
    pub fn new(authority: AuthorityChecker) -> Self {
        Self { authority }
    }

    /// Gets a reference to the authority checker.
    pub fn authority(&self) -> &AuthorityChecker {
        &self.authority
    }

    /// Checks if a principal can override according to a policy.
    pub async fn can_override(
        &self,
        principal: &PrincipalId,
        policy: &OverridePolicy,
        domain: &str,
        context: &OverrideContext,
    ) -> OverrideResult {
        match policy {
            OverridePolicy::Mandatory => OverrideResult::Denied {
                reason: "Policy is mandatory and cannot be overridden".to_string(),
            },

            OverridePolicy::Advisory => OverrideResult::Allowed,

            OverridePolicy::Blueprint {
                min_priority,
                require_same_domain,
            } => {
                // Check if principal has sufficient priority
                let principal_data = self.authority.get_principal(principal).await;
                let priority = match principal_data {
                    Some(p) => p.priority,
                    None => {
                        return OverrideResult::Denied {
                            reason: format!("Principal {} not found", principal),
                        };
                    }
                };

                if priority < *min_priority {
                    return OverrideResult::Denied {
                        reason: format!(
                            "Insufficient priority: {} < {} required",
                            priority, min_priority
                        ),
                    };
                }

                // Check domain authority if required
                if *require_same_domain && !self.authority.has_authority(principal, domain).await {
                    return OverrideResult::Denied {
                        reason: format!("No authority over domain: {}", domain),
                    };
                }

                // Check if principal can override the original author
                if priority <= context.original_priority {
                    return OverrideResult::Denied {
                        reason: format!(
                            "Priority {} not higher than original author's {}",
                            priority, context.original_priority
                        ),
                    };
                }

                OverrideResult::Allowed
            }

            OverridePolicy::RequiresApproval {
                approvers,
                timeout: _,
                default_on_timeout: _,
            } => {
                // Find principals who can approve
                let matching_approvers = approvers
                    .find_matching_principals(&self.authority, domain)
                    .await;

                if matching_approvers.is_empty() {
                    return OverrideResult::Denied {
                        reason: "No approvers available for this domain".to_string(),
                    };
                }

                OverrideResult::RequiresApproval {
                    approvers: matching_approvers,
                }
            }

            OverridePolicy::Trial { max_uses, then } => {
                if context.override_count >= *max_uses {
                    // Trial exhausted, apply the 'then' policy
                    return Box::pin(self.can_override(principal, then, domain, context)).await;
                }

                // Check if basic override is allowed
                let basic_result = Box::pin(self.can_override(
                    principal,
                    &OverridePolicy::Advisory, // Trial allows override if within limits
                    domain,
                    context,
                ))
                .await;

                if basic_result.is_allowed() {
                    let remaining = max_uses - context.override_count - 1;
                    OverrideResult::TrialAllowed {
                        remaining_uses: remaining,
                    }
                } else {
                    basic_result
                }
            }

            OverridePolicy::TimeLimited { max_duration, then } => {
                // Check if requested duration exceeds max
                if let Some(requested) = context.override_duration {
                    if requested > *max_duration {
                        return OverrideResult::Denied {
                            reason: format!(
                                "Requested duration {}s exceeds maximum {}s",
                                requested.as_secs(),
                                max_duration.as_secs()
                            ),
                        };
                    }
                }

                // For time-limited overrides, check that principal exists and has domain authority
                // but do NOT require higher priority than original author (that's the point of time-limited)
                let principal_data = self.authority.get_principal(principal).await;
                if principal_data.is_none() {
                    return OverrideResult::Denied {
                        reason: format!("Principal {} not found", principal),
                    };
                }

                // Check domain authority
                if !self.authority.has_authority(principal, domain).await {
                    // Principal has no authority, fall back to 'then' policy
                    return Box::pin(self.can_override(principal, then, domain, context)).await;
                }

                OverrideResult::TimeLimitedAllowed {
                    max_duration: *max_duration,
                }
            }
        }
    }
}

/// Serde module for Duration serialization.
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        duration.as_secs().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::principal::{DomainPattern, Principal};

    async fn setup_test_authority() -> AuthorityChecker {
        let authority = AuthorityChecker::new();

        // CEO - highest authority
        let ceo = Principal::new_user("ceo", "CEO", 1000).with_domain(DomainPattern::global());
        authority.register_principal(ceo).await;

        // Manager - medium authority
        let manager = Principal::new_user("manager", "Manager", 500)
            .with_domain(DomainPattern::prefix("dept:eng"));
        authority.register_principal(manager).await;

        // Employee - lower authority
        let employee = Principal::new_user("employee", "Employee", 100)
            .with_domain(DomainPattern::exact("project:alpha"));
        authority.register_principal(employee).await;

        // Intern - lowest authority
        let intern = Principal::new_user("intern", "Intern", 50);
        authority.register_principal(intern).await;

        authority
    }

    #[tokio::test]
    async fn test_mandatory_policy() {
        let authority = setup_test_authority().await;
        let checker = OverrideChecker::new(authority);

        let context = OverrideContext::new("employee", 100);
        let result = checker
            .can_override(
                &"ceo".to_string(),
                &OverridePolicy::mandatory(),
                "anything",
                &context,
            )
            .await;

        // Even CEO can't override mandatory policy
        assert!(matches!(result, OverrideResult::Denied { .. }));
    }

    #[tokio::test]
    async fn test_advisory_policy() {
        let authority = setup_test_authority().await;
        let checker = OverrideChecker::new(authority);

        let context = OverrideContext::new("ceo", 1000);
        let result = checker
            .can_override(
                &"intern".to_string(),
                &OverridePolicy::advisory(),
                "anything",
                &context,
            )
            .await;

        // Even intern can override advisory
        assert!(matches!(result, OverrideResult::Allowed));
    }

    #[tokio::test]
    async fn test_blueprint_policy_priority() {
        let authority = setup_test_authority().await;
        let checker = OverrideChecker::new(authority);

        let context = OverrideContext::new("employee", 100);
        let policy = OverridePolicy::blueprint(200);

        // Manager (500) can override
        let result = checker
            .can_override(&"manager".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(result.is_allowed());

        // Intern (50) cannot override - insufficient priority
        let result = checker
            .can_override(&"intern".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(matches!(result, OverrideResult::Denied { .. }));
    }

    #[tokio::test]
    async fn test_blueprint_policy_domain() {
        let authority = setup_test_authority().await;
        let checker = OverrideChecker::new(authority);

        let context = OverrideContext::new("intern", 50);
        let policy = OverridePolicy::Blueprint {
            min_priority: 100,
            require_same_domain: true,
        };

        // Employee has authority over project:alpha
        let result = checker
            .can_override(&"employee".to_string(), &policy, "project:alpha", &context)
            .await;
        assert!(result.is_allowed());

        // Employee doesn't have authority over project:beta
        let result = checker
            .can_override(&"employee".to_string(), &policy, "project:beta", &context)
            .await;
        assert!(matches!(result, OverrideResult::Denied { .. }));
    }

    #[tokio::test]
    async fn test_requires_approval_policy() {
        let authority = setup_test_authority().await;
        let checker = OverrideChecker::new(authority);

        let context = OverrideContext::new("employee", 100);
        let policy = OverridePolicy::requires_approval(
            ApproverSpec::min_priority(500),
            Duration::from_secs(3600),
        );

        let result = checker
            .can_override(&"intern".to_string(), &policy, "anything", &context)
            .await;

        // Should require approval
        assert!(matches!(result, OverrideResult::RequiresApproval { .. }));
    }

    #[tokio::test]
    async fn test_trial_policy() {
        let authority = setup_test_authority().await;
        let checker = OverrideChecker::new(authority);

        let policy = OverridePolicy::trial(3, OverridePolicy::mandatory());

        // First use - allowed with remaining
        let context = OverrideContext::new("employee", 100).with_override_count(0);
        let result = checker
            .can_override(&"manager".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(matches!(
            result,
            OverrideResult::TrialAllowed { remaining_uses: 2 }
        ));

        // Third use - last one
        let context = OverrideContext::new("employee", 100).with_override_count(2);
        let result = checker
            .can_override(&"manager".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(matches!(
            result,
            OverrideResult::TrialAllowed { remaining_uses: 0 }
        ));

        // Fourth use - trial exhausted, falls back to mandatory
        let context = OverrideContext::new("employee", 100).with_override_count(3);
        let result = checker
            .can_override(&"manager".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(matches!(result, OverrideResult::Denied { .. }));
    }

    #[tokio::test]
    async fn test_time_limited_policy() {
        let authority = setup_test_authority().await;
        let checker = OverrideChecker::new(authority);

        let policy =
            OverridePolicy::time_limited(Duration::from_secs(3600), OverridePolicy::mandatory());

        // Request within limit
        let context =
            OverrideContext::new("employee", 100).with_duration(Duration::from_secs(1800));
        let result = checker
            .can_override(&"manager".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(matches!(
            result,
            OverrideResult::TimeLimitedAllowed { max_duration } if max_duration == Duration::from_secs(3600)
        ));

        // Request exceeds limit
        let context =
            OverrideContext::new("employee", 100).with_duration(Duration::from_secs(7200));
        let result = checker
            .can_override(&"manager".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(matches!(result, OverrideResult::Denied { .. }));
    }

    #[tokio::test]
    async fn test_approver_spec_specific() {
        let authority = setup_test_authority().await;
        let spec = ApproverSpec::specific(vec!["ceo".to_string(), "manager".to_string()]);

        assert!(spec.matches(&"ceo".to_string(), &authority, "any").await);
        assert!(
            spec.matches(&"manager".to_string(), &authority, "any")
                .await
        );
        assert!(
            !spec
                .matches(&"employee".to_string(), &authority, "any")
                .await
        );
    }

    #[tokio::test]
    async fn test_approver_spec_min_priority() {
        let authority = setup_test_authority().await;
        let spec = ApproverSpec::min_priority(500);

        assert!(spec.matches(&"ceo".to_string(), &authority, "any").await);
        assert!(
            spec.matches(&"manager".to_string(), &authority, "any")
                .await
        );
        assert!(
            !spec
                .matches(&"employee".to_string(), &authority, "any")
                .await
        );
    }

    #[tokio::test]
    async fn test_approver_spec_domain_authority() {
        let authority = setup_test_authority().await;
        let spec = ApproverSpec::domain_authority();

        // Manager has authority over dept:eng
        assert!(
            spec.matches(&"manager".to_string(), &authority, "dept:eng")
                .await
        );

        // Employee has authority over project:alpha
        assert!(
            spec.matches(&"employee".to_string(), &authority, "project:alpha")
                .await
        );

        // Intern has no authority
        assert!(
            !spec
                .matches(&"intern".to_string(), &authority, "anything")
                .await
        );
    }

    #[tokio::test]
    async fn test_approver_spec_any() {
        let authority = setup_test_authority().await;
        let spec = ApproverSpec::any_of(vec![
            ApproverSpec::specific(vec!["intern".to_string()]),
            ApproverSpec::min_priority(1000),
        ]);

        // Matches intern (specific)
        assert!(spec.matches(&"intern".to_string(), &authority, "any").await);

        // Matches CEO (min priority)
        assert!(spec.matches(&"ceo".to_string(), &authority, "any").await);

        // Doesn't match employee
        assert!(
            !spec
                .matches(&"employee".to_string(), &authority, "any")
                .await
        );
    }

    #[tokio::test]
    async fn test_approver_spec_all() {
        let authority = setup_test_authority().await;
        let spec = ApproverSpec::all_of(vec![
            ApproverSpec::min_priority(500),
            ApproverSpec::domain_authority(),
        ]);

        // CEO has both: priority >= 500 and global authority
        assert!(
            spec.matches(&"ceo".to_string(), &authority, "dept:eng")
                .await
        );

        // Manager has priority but only authority over dept:eng
        assert!(
            spec.matches(&"manager".to_string(), &authority, "dept:eng")
                .await
        );

        // Manager doesn't match for project:alpha (no authority there)
        assert!(
            !spec
                .matches(&"manager".to_string(), &authority, "project:alpha")
                .await
        );
    }

    #[test]
    fn test_policy_description() {
        assert_eq!(
            OverridePolicy::mandatory().description(),
            "Cannot be overridden"
        );

        assert!(OverridePolicy::blueprint(500).description().contains("500"));

        assert!(OverridePolicy::advisory()
            .description()
            .contains("Advisory"));
    }

    #[test]
    fn test_override_result_display() {
        assert!(OverrideResult::Allowed.to_string().contains("allowed"));
        assert!(OverrideResult::Denied {
            reason: "test".to_string()
        }
        .to_string()
        .contains("denied"));
    }
}
