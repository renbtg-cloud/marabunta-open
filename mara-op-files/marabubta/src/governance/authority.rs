// Marabunta - Licensed under the MIT License.
//! Authority and Delegation Management
//!
//! This module provides authority checking and delegation management for the
//! governance system. It supports hierarchical authority with delegation chains,
//! constraints, expiration, and revocation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::errors::AuthorityError;
use super::principal::{DomainPattern, Principal, PrincipalId};

/// A delegation of authority from one principal to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delegation {
    /// Unique identifier for this delegation.
    pub id: String,
    /// The principal granting authority.
    pub from: PrincipalId,
    /// The principal receiving authority.
    pub to: PrincipalId,
    /// The domains being delegated.
    pub domains: Vec<DomainPattern>,
    /// Constraints on this delegation.
    pub constraints: DelegationConstraints,
    /// When this delegation was created.
    pub created_at: DateTime<Utc>,
    /// When this delegation expires (None = never).
    pub expires_at: Option<DateTime<Utc>>,
    /// Whether this delegation has been revoked.
    pub revoked: bool,
}

impl Delegation {
    /// Creates a new delegation.
    pub fn new(
        id: impl Into<String>,
        from: impl Into<PrincipalId>,
        to: impl Into<PrincipalId>,
        domains: Vec<DomainPattern>,
    ) -> Self {
        Self {
            id: id.into(),
            from: from.into(),
            to: to.into(),
            domains,
            constraints: DelegationConstraints::default(),
            created_at: Utc::now(),
            expires_at: None,
            revoked: false,
        }
    }

    /// Sets constraints on this delegation.
    pub fn with_constraints(mut self, constraints: DelegationConstraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Sets an expiration time.
    pub fn with_expiry(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Sets the expiration to a duration from now.
    pub fn expires_in(mut self, duration: chrono::Duration) -> Self {
        self.expires_at = Some(Utc::now() + duration);
        self
    }

    /// Checks if this delegation is currently valid (not expired or revoked).
    pub fn is_valid(&self) -> bool {
        !self.revoked && !self.is_expired()
    }

    /// Checks if this delegation has expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|exp| Utc::now() > exp).unwrap_or(false)
    }

    /// Checks if this delegation covers a domain.
    pub fn covers_domain(&self, domain: &str) -> bool {
        self.domains.iter().any(|p| p.matches(domain))
    }
}

/// Constraints on a delegation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationConstraints {
    /// Maximum priority the delegatee can exercise (None = inherit delegator's priority).
    pub max_priority: Option<u32>,
    /// Whether the delegatee can delegate further.
    pub can_redelegate: bool,
    /// Maximum depth of redelegation (None = unlimited).
    pub max_redelegation_depth: Option<u32>,
}

impl Default for DelegationConstraints {
    fn default() -> Self {
        Self {
            max_priority: None,
            can_redelegate: true,
            max_redelegation_depth: None,
        }
    }
}

impl DelegationConstraints {
    /// Creates constraints with no redelegation allowed.
    pub fn no_redelegate() -> Self {
        Self {
            max_priority: None,
            can_redelegate: false,
            max_redelegation_depth: Some(0),
        }
    }

    /// Creates constraints with a maximum priority.
    pub fn with_max_priority(mut self, max: u32) -> Self {
        self.max_priority = Some(max);
        self
    }

    /// Creates constraints that allow redelegation to a certain depth.
    pub fn with_max_depth(mut self, depth: u32) -> Self {
        self.can_redelegate = depth > 0;
        self.max_redelegation_depth = Some(depth);
        self
    }
}

/// A step in an authority chain explaining how authority was granted.
#[derive(Debug, Clone)]
pub enum AuthorityStep {
    /// Principal has direct authority over the domain.
    Direct,
    /// Authority was delegated from another principal.
    Delegated {
        from: PrincipalId,
        delegation_id: String,
    },
    /// Authority was inherited from a group.
    Inherited { from: PrincipalId },
}

impl std::fmt::Display for AuthorityStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthorityStep::Direct => write!(f, "direct authority"),
            AuthorityStep::Delegated {
                from,
                delegation_id,
            } => {
                write!(f, "delegated from {} ({})", from, delegation_id)
            }
            AuthorityStep::Inherited { from } => write!(f, "inherited from {}", from),
        }
    }
}

/// The complete chain of authority explaining how a principal has authority.
#[derive(Debug, Clone)]
pub struct AuthorityChain {
    /// The principal whose authority is being traced.
    pub principal: PrincipalId,
    /// The domain being checked.
    pub domain: String,
    /// Whether the principal has authority.
    pub has_authority: bool,
    /// The steps in the authority chain.
    pub granted_via: Vec<AuthorityStep>,
    /// The effective priority through this chain.
    pub effective_priority: Option<u32>,
}

impl AuthorityChain {
    /// Creates an empty chain indicating no authority.
    pub fn none(principal: PrincipalId, domain: String) -> Self {
        Self {
            principal,
            domain,
            has_authority: false,
            granted_via: Vec::new(),
            effective_priority: None,
        }
    }

    /// Creates a chain with direct authority.
    pub fn direct(principal: PrincipalId, domain: String, priority: u32) -> Self {
        Self {
            principal,
            domain,
            has_authority: true,
            granted_via: vec![AuthorityStep::Direct],
            effective_priority: Some(priority),
        }
    }

    /// Adds a delegation step to the chain.
    pub fn with_delegation(mut self, from: PrincipalId, delegation_id: String) -> Self {
        self.granted_via.push(AuthorityStep::Delegated {
            from,
            delegation_id,
        });
        self
    }

    /// Adds an inheritance step to the chain.
    pub fn with_inheritance(mut self, from: PrincipalId) -> Self {
        self.granted_via.push(AuthorityStep::Inherited { from });
        self
    }
}

impl std::fmt::Display for AuthorityChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.has_authority {
            return write!(
                f,
                "{} has no authority over '{}'",
                self.principal, self.domain
            );
        }

        write!(
            f,
            "{} has authority over '{}' (priority={}): ",
            self.principal,
            self.domain,
            self.effective_priority.unwrap_or(0)
        )?;

        let steps: Vec<_> = self.granted_via.iter().map(|s| s.to_string()).collect();
        write!(f, "{}", steps.join(" -> "))
    }
}

/// Authority checker that validates principals' authority over domains.
///
/// This is a thread-safe implementation using `Arc<RwLock<>>`.
pub struct AuthorityChecker {
    /// Registered principals.
    principals: Arc<RwLock<HashMap<PrincipalId, Principal>>>,
    /// Active delegations.
    delegations: Arc<RwLock<Vec<Delegation>>>,
    /// Index: principal -> delegations they received.
    delegations_to: Arc<RwLock<HashMap<PrincipalId, Vec<String>>>>,
    /// Index: principal -> delegations they granted.
    delegations_from: Arc<RwLock<HashMap<PrincipalId, Vec<String>>>>,
}

impl AuthorityChecker {
    /// Creates a new empty authority checker.
    pub fn new() -> Self {
        Self {
            principals: Arc::new(RwLock::new(HashMap::new())),
            delegations: Arc::new(RwLock::new(Vec::new())),
            delegations_to: Arc::new(RwLock::new(HashMap::new())),
            delegations_from: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Registers a principal.
    pub async fn register_principal(&self, principal: Principal) {
        let mut principals = self.principals.write().await;
        principals.insert(principal.id.clone(), principal);
    }

    /// Gets a principal by ID.
    pub async fn get_principal(&self, id: &PrincipalId) -> Option<Principal> {
        let principals = self.principals.read().await;
        principals.get(id).cloned()
    }

    /// Adds a delegation after validating it.
    pub async fn add_delegation(&self, delegation: Delegation) -> Result<(), AuthorityError> {
        // Validate the delegation
        self.validate_delegation(&delegation).await?;

        self.add_delegation_unchecked(delegation).await;
        Ok(())
    }

    /// Adds a delegation without validation.
    /// Used for importing previously-validated delegations.
    pub async fn add_delegation_unchecked(&self, delegation: Delegation) {
        let delegation_id = delegation.id.clone();
        let from = delegation.from.clone();
        let to = delegation.to.clone();

        // Add to main list
        {
            let mut delegations = self.delegations.write().await;
            delegations.push(delegation);
        }

        // Update indexes
        {
            let mut to_index = self.delegations_to.write().await;
            to_index.entry(to).or_default().push(delegation_id.clone());
        }

        {
            let mut from_index = self.delegations_from.write().await;
            from_index.entry(from).or_default().push(delegation_id);
        }
    }

    /// Validates a delegation before adding it.
    async fn validate_delegation(&self, delegation: &Delegation) -> Result<(), AuthorityError> {
        let principals = self.principals.read().await;

        // Check delegator exists
        let delegator = principals
            .get(&delegation.from)
            .ok_or_else(|| AuthorityError::PrincipalNotFound(delegation.from.clone()))?;

        // Check delegatee exists
        if !principals.contains_key(&delegation.to) {
            return Err(AuthorityError::PrincipalNotFound(delegation.to.clone()));
        }

        // Check delegator has authority over the domains being delegated
        for domain_pattern in &delegation.domains {
            let domain_desc = domain_pattern.description();
            // Use a representative domain for checking - for patterns, we check if
            // the delegator has broader authority
            let has_authority = delegator
                .authority_domains
                .iter()
                .any(|d| domain_pattern.is_subset_of(d));

            if !has_authority {
                // Check if delegator has authority via delegation
                drop(principals); // Release lock to avoid deadlock
                let delegator_has = self.has_authority(&delegation.from, &domain_desc).await;
                if !delegator_has {
                    return Err(AuthorityError::DelegatorNoAuthority(domain_desc));
                }
                // Continue to cycle and redelegation checks below
                // (Re-acquire principals lock is not needed for remaining checks)
                return self.validate_delegation_constraints(delegation).await;
            }
        }

        // Check for cycles (only reached if delegator has direct authority)
        drop(principals); // Release lock
        self.validate_delegation_constraints(delegation).await
    }

    /// Validates cycle and redelegation constraints for a delegation.
    async fn validate_delegation_constraints(
        &self,
        delegation: &Delegation,
    ) -> Result<(), AuthorityError> {
        // Check for cycles
        if self
            .would_create_cycle(&delegation.from, &delegation.to)
            .await
        {
            return Err(AuthorityError::DelegationCycle(format!(
                "{} -> {}",
                delegation.from, delegation.to
            )));
        }

        // Validate redelegation constraints
        self.validate_redelegation_constraints(delegation).await?;

        Ok(())
    }

    /// Checks if adding a delegation would create a cycle.
    async fn would_create_cycle(&self, from: &PrincipalId, to: &PrincipalId) -> bool {
        // BFS from 'to' to see if we can reach 'from' by following existing delegations
        // We need to check: if 'to' already has delegated (directly or indirectly) to 'from',
        // then adding from->to would create a cycle.
        let delegations = self.delegations.read().await;
        let mut visited = HashSet::new();
        let mut queue = vec![to.clone()];

        while let Some(current) = queue.pop() {
            if &current == from {
                return true;
            }

            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            // Find delegations where current is the delegator (from)
            // This traces the path of who 'current' has delegated to
            for d in delegations.iter() {
                if d.from == current && d.is_valid() {
                    queue.push(d.to.clone());
                }
            }
        }

        false
    }

    /// Validates redelegation constraints.
    async fn validate_redelegation_constraints(
        &self,
        delegation: &Delegation,
    ) -> Result<(), AuthorityError> {
        let delegations = self.delegations.read().await;

        // Find the delegation chain to the delegator
        let mut depth = 0u32;
        let mut current = &delegation.from;
        let mut min_max_depth: Option<u32> = None;

        loop {
            // Find a delegation that grants authority to current
            let parent_delegation = delegations
                .iter()
                .find(|d| &d.to == current && d.is_valid());

            match parent_delegation {
                Some(pd) => {
                    // Check if redelegation is allowed
                    if !pd.constraints.can_redelegate {
                        return Err(AuthorityError::CannotRedelegate);
                    }

                    // Track minimum max depth
                    if let Some(max_depth) = pd.constraints.max_redelegation_depth {
                        min_max_depth =
                            Some(min_max_depth.map(|m| m.min(max_depth)).unwrap_or(max_depth));
                    }

                    depth += 1;
                    current = &pd.from;
                }
                None => break, // Reached a principal with direct authority
            }
        }

        // Check if we exceeded max depth
        if let Some(max_depth) = min_max_depth {
            if depth >= max_depth {
                return Err(AuthorityError::MaxRedelegationDepthExceeded { max_depth });
            }
        }

        // Check priority constraints
        if let Some(max_priority) = delegation.constraints.max_priority {
            let principals = self.principals.read().await;
            if let Some(delegator) = principals.get(&delegation.from) {
                if max_priority > delegator.priority {
                    return Err(AuthorityError::PriorityExceedsMax {
                        requested: max_priority,
                        max_allowed: delegator.priority,
                    });
                }
            }
        }

        Ok(())
    }

    /// Revokes a delegation by ID.
    pub async fn revoke_delegation(&self, delegation_id: &str) -> Result<(), AuthorityError> {
        let mut delegations = self.delegations.write().await;

        let delegation = delegations
            .iter_mut()
            .find(|d| d.id == delegation_id)
            .ok_or_else(|| AuthorityError::DelegationNotFound(delegation_id.to_string()))?;

        delegation.revoked = true;
        Ok(())
    }

    /// Checks if a principal has authority over a domain.
    pub async fn has_authority(&self, principal_id: &PrincipalId, domain: &str) -> bool {
        self.authority_chain(principal_id, domain)
            .await
            .has_authority
    }

    /// Gets the effective priority for a principal in a domain.
    pub async fn effective_priority(
        &self,
        principal_id: &PrincipalId,
        domain: &str,
    ) -> Option<u32> {
        self.authority_chain(principal_id, domain)
            .await
            .effective_priority
    }

    /// Checks if one principal can override another in a domain.
    ///
    /// Returns true if `actor` has higher effective priority than `target` in the domain.
    pub async fn can_override(
        &self,
        actor: &PrincipalId,
        target: &PrincipalId,
        domain: &str,
    ) -> bool {
        let actor_priority = self.effective_priority(actor, domain).await;
        let target_priority = self.effective_priority(target, domain).await;

        match (actor_priority, target_priority) {
            (Some(a), Some(t)) => a > t,
            (Some(_), None) => true, // Actor has authority, target doesn't
            _ => false,
        }
    }

    /// Gets all principals with authority over a domain.
    pub async fn authorities_for_domain(&self, domain: &str) -> Vec<Principal> {
        let principals = self.principals.read().await;
        let mut result = Vec::new();

        for principal in principals.values() {
            if self.has_authority(&principal.id, domain).await {
                result.push(principal.clone());
            }
        }

        result
    }

    /// Traces the authority chain for a principal over a domain.
    pub async fn authority_chain(
        &self,
        principal_id: &PrincipalId,
        domain: &str,
    ) -> AuthorityChain {
        let principals = self.principals.read().await;

        // Check if principal exists
        let principal = match principals.get(principal_id) {
            Some(p) => p.clone(),
            None => return AuthorityChain::none(principal_id.clone(), domain.to_string()),
        };

        // Check direct authority
        if principal.has_direct_authority(domain) {
            return AuthorityChain::direct(
                principal_id.clone(),
                domain.to_string(),
                principal.priority,
            );
        }

        // Check delegated authority
        drop(principals); // Release lock for recursive calls

        let delegations = self.delegations.read().await;
        let delegations_to_principal: Vec<_> = delegations
            .iter()
            .filter(|d| &d.to == principal_id && d.is_valid() && d.covers_domain(domain))
            .cloned()
            .collect();
        drop(delegations);

        for delegation in delegations_to_principal {
            // Recursively check if delegator has authority
            let delegator_chain = Box::pin(self.authority_chain(&delegation.from, domain)).await;

            if delegator_chain.has_authority {
                // Calculate effective priority with constraints
                let effective_priority = delegation
                    .constraints
                    .max_priority
                    .map(|max| delegator_chain.effective_priority.unwrap_or(0).min(max))
                    .or(delegator_chain.effective_priority);

                let mut chain = AuthorityChain {
                    principal: principal_id.clone(),
                    domain: domain.to_string(),
                    has_authority: true,
                    granted_via: delegator_chain.granted_via,
                    effective_priority,
                };
                chain = chain.with_delegation(delegation.from.clone(), delegation.id.clone());
                return chain;
            }
        }

        // Check inherited authority from groups
        let principals = self.principals.read().await;
        let principal = principals.get(principal_id).cloned();
        drop(principals);

        if let Some(p) = principal {
            for group_id in &p.delegates_from {
                let group_chain = Box::pin(self.authority_chain(group_id, domain)).await;
                if group_chain.has_authority {
                    let mut chain = AuthorityChain {
                        principal: principal_id.clone(),
                        domain: domain.to_string(),
                        has_authority: true,
                        granted_via: group_chain.granted_via,
                        effective_priority: group_chain.effective_priority,
                    };
                    chain = chain.with_inheritance(group_id.clone());
                    return chain;
                }
            }
        }

        AuthorityChain::none(principal_id.clone(), domain.to_string())
    }

    /// Lists all registered principals.
    pub async fn list_principals(&self) -> Vec<Principal> {
        let principals = self.principals.read().await;
        principals.values().cloned().collect()
    }

    /// Lists all delegations (for administrative purposes).
    pub async fn list_delegations(&self) -> Vec<Delegation> {
        let delegations = self.delegations.read().await;
        delegations.clone()
    }

    /// Lists valid (non-expired, non-revoked) delegations.
    pub async fn list_valid_delegations(&self) -> Vec<Delegation> {
        let delegations = self.delegations.read().await;
        delegations
            .iter()
            .filter(|d| d.is_valid())
            .cloned()
            .collect()
    }

    /// Gets delegations granted by a principal.
    pub async fn delegations_from(&self, principal_id: &PrincipalId) -> Vec<Delegation> {
        let index = self.delegations_from.read().await;
        let delegation_ids = match index.get(principal_id) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };
        drop(index);

        let delegations = self.delegations.read().await;
        delegation_ids
            .iter()
            .filter_map(|id| delegations.iter().find(|d| &d.id == id).cloned())
            .collect()
    }

    /// Gets delegations received by a principal.
    pub async fn delegations_to(&self, principal_id: &PrincipalId) -> Vec<Delegation> {
        let index = self.delegations_to.read().await;
        let delegation_ids = match index.get(principal_id) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };
        drop(index);

        let delegations = self.delegations.read().await;
        delegation_ids
            .iter()
            .filter_map(|id| delegations.iter().find(|d| &d.id == id).cloned())
            .collect()
    }
}

impl Default for AuthorityChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_basic_hierarchy() -> AuthorityChecker {
        let checker = AuthorityChecker::new();

        // CEO has global authority
        let ceo = Principal::new_user("ceo", "Chief Executive Officer", 1000)
            .with_domain(DomainPattern::global());
        checker.register_principal(ceo).await;

        // VP of Engineering has authority over engineering
        let vp_eng = Principal::new_user("vp-eng", "VP Engineering", 800)
            .with_domain(DomainPattern::prefix("dept:eng"));
        checker.register_principal(vp_eng).await;

        // Physics department head
        let physics_head = Principal::new_user("physics-head", "Physics Dept Head", 600)
            .with_domain(DomainPattern::exact("dept:physics"));
        checker.register_principal(physics_head).await;

        // Regular researcher
        let researcher = Principal::new_user("researcher-1", "Researcher", 100);
        checker.register_principal(researcher).await;

        checker
    }

    #[tokio::test]
    async fn test_direct_authority() {
        let checker = setup_basic_hierarchy().await;

        // CEO has authority over everything
        assert!(checker.has_authority(&"ceo".to_string(), "anything").await);
        assert!(
            checker
                .has_authority(&"ceo".to_string(), "dept:physics")
                .await
        );

        // VP Eng has authority over engineering domains
        assert!(
            checker
                .has_authority(&"vp-eng".to_string(), "dept:eng")
                .await
        );
        assert!(
            checker
                .has_authority(&"vp-eng".to_string(), "dept:eng:team1")
                .await
        );
        assert!(
            !checker
                .has_authority(&"vp-eng".to_string(), "dept:physics")
                .await
        );

        // Physics head has authority over physics only
        assert!(
            checker
                .has_authority(&"physics-head".to_string(), "dept:physics")
                .await
        );
        assert!(
            !checker
                .has_authority(&"physics-head".to_string(), "dept:physics:lab1")
                .await
        );

        // Researcher has no direct authority
        assert!(
            !checker
                .has_authority(&"researcher-1".to_string(), "anything")
                .await
        );
    }

    #[tokio::test]
    async fn test_delegation() {
        let checker = setup_basic_hierarchy().await;

        // Physics head delegates to researcher
        let delegation = Delegation::new(
            "del-1",
            "physics-head",
            "researcher-1",
            vec![DomainPattern::exact("dept:physics")],
        );
        checker.add_delegation(delegation).await.unwrap();

        // Researcher now has authority via delegation
        assert!(
            checker
                .has_authority(&"researcher-1".to_string(), "dept:physics")
                .await
        );

        // But not over other domains
        assert!(
            !checker
                .has_authority(&"researcher-1".to_string(), "dept:chemistry")
                .await
        );
    }

    #[tokio::test]
    async fn test_delegation_with_constraints() {
        let checker = setup_basic_hierarchy().await;

        // Delegate with max priority constraint
        let delegation = Delegation::new(
            "del-1",
            "physics-head",
            "researcher-1",
            vec![DomainPattern::exact("dept:physics")],
        )
        .with_constraints(DelegationConstraints::default().with_max_priority(50));

        checker.add_delegation(delegation).await.unwrap();

        // Researcher has authority but limited priority
        let priority = checker
            .effective_priority(&"researcher-1".to_string(), "dept:physics")
            .await;
        assert_eq!(priority, Some(50)); // Limited by constraint, not physics-head's 600
    }

    #[tokio::test]
    async fn test_no_redelegate_constraint() {
        let checker = setup_basic_hierarchy().await;

        // Add another user
        let user2 = Principal::new_user("researcher-2", "Researcher 2", 100);
        checker.register_principal(user2).await;

        // Physics head delegates to researcher with no redelegation
        let delegation = Delegation::new(
            "del-1",
            "physics-head",
            "researcher-1",
            vec![DomainPattern::exact("dept:physics")],
        )
        .with_constraints(DelegationConstraints::no_redelegate());

        checker.add_delegation(delegation).await.unwrap();

        // Researcher tries to redelegate
        let subdelegation = Delegation::new(
            "del-2",
            "researcher-1",
            "researcher-2",
            vec![DomainPattern::exact("dept:physics")],
        );

        let result = checker.add_delegation(subdelegation).await;
        assert!(matches!(result, Err(AuthorityError::CannotRedelegate)));
    }

    #[tokio::test]
    async fn test_delegation_cycle_detection() {
        let checker = AuthorityChecker::new();

        // Create users with authority
        let user_a =
            Principal::new_user("user-a", "User A", 100).with_domain(DomainPattern::global());
        let user_b = Principal::new_user("user-b", "User B", 100);
        let user_c = Principal::new_user("user-c", "User C", 100);

        checker.register_principal(user_a).await;
        checker.register_principal(user_b).await;
        checker.register_principal(user_c).await;

        // A -> B
        checker
            .add_delegation(Delegation::new(
                "del-1",
                "user-a",
                "user-b",
                vec![DomainPattern::global()],
            ))
            .await
            .unwrap();

        // B -> C
        checker
            .add_delegation(Delegation::new(
                "del-2",
                "user-b",
                "user-c",
                vec![DomainPattern::global()],
            ))
            .await
            .unwrap();

        // C -> A (would create cycle)
        let result = checker
            .add_delegation(Delegation::new(
                "del-3",
                "user-c",
                "user-a",
                vec![DomainPattern::global()],
            ))
            .await;

        assert!(matches!(result, Err(AuthorityError::DelegationCycle(_))));
    }

    #[tokio::test]
    async fn test_delegation_expiry() {
        let checker = setup_basic_hierarchy().await;

        // Create an expired delegation
        let delegation = Delegation::new(
            "del-1",
            "physics-head",
            "researcher-1",
            vec![DomainPattern::exact("dept:physics")],
        )
        .with_expiry(Utc::now() - chrono::Duration::hours(1));

        // Still can add expired delegation (for record keeping)
        // But it won't grant authority
        checker.delegations.write().await.push(delegation);

        // Researcher doesn't have authority via expired delegation
        assert!(
            !checker
                .has_authority(&"researcher-1".to_string(), "dept:physics")
                .await
        );
    }

    #[tokio::test]
    async fn test_delegation_revocation() {
        let checker = setup_basic_hierarchy().await;

        // Create delegation
        let delegation = Delegation::new(
            "del-1",
            "physics-head",
            "researcher-1",
            vec![DomainPattern::exact("dept:physics")],
        );
        checker.add_delegation(delegation).await.unwrap();

        // Researcher has authority
        assert!(
            checker
                .has_authority(&"researcher-1".to_string(), "dept:physics")
                .await
        );

        // Revoke delegation
        checker.revoke_delegation("del-1").await.unwrap();

        // Researcher no longer has authority
        assert!(
            !checker
                .has_authority(&"researcher-1".to_string(), "dept:physics")
                .await
        );
    }

    #[tokio::test]
    async fn test_can_override() {
        let checker = setup_basic_hierarchy().await;

        // CEO can override VP
        assert!(
            checker
                .can_override(&"ceo".to_string(), &"vp-eng".to_string(), "dept:eng")
                .await
        );

        // VP cannot override CEO
        assert!(
            !checker
                .can_override(&"vp-eng".to_string(), &"ceo".to_string(), "dept:eng")
                .await
        );

        // VP can override researcher (who has no authority)
        assert!(
            checker
                .can_override(
                    &"vp-eng".to_string(),
                    &"researcher-1".to_string(),
                    "dept:eng"
                )
                .await
        );
    }

    #[tokio::test]
    async fn test_authority_chain() {
        let checker = setup_basic_hierarchy().await;

        // Create delegation chain
        let del1 = Delegation::new(
            "del-1",
            "physics-head",
            "researcher-1",
            vec![DomainPattern::exact("dept:physics")],
        );
        checker.add_delegation(del1).await.unwrap();

        // Trace authority chain
        let chain = checker
            .authority_chain(&"researcher-1".to_string(), "dept:physics")
            .await;

        assert!(chain.has_authority);
        assert!(!chain.granted_via.is_empty());

        // Chain should show delegation
        let has_delegation_step = chain.granted_via.iter().any(
            |step| matches!(step, AuthorityStep::Delegated { from, .. } if from == "physics-head"),
        );
        assert!(has_delegation_step);
    }

    #[tokio::test]
    async fn test_effective_priority() {
        let checker = setup_basic_hierarchy().await;

        // Direct authority - use principal's priority
        assert_eq!(
            checker
                .effective_priority(&"ceo".to_string(), "anything")
                .await,
            Some(1000)
        );
        assert_eq!(
            checker
                .effective_priority(&"physics-head".to_string(), "dept:physics")
                .await,
            Some(600)
        );

        // No authority - no priority
        assert_eq!(
            checker
                .effective_priority(&"researcher-1".to_string(), "anything")
                .await,
            None
        );
    }

    #[tokio::test]
    async fn test_authorities_for_domain() {
        let checker = setup_basic_hierarchy().await;

        let authorities = checker.authorities_for_domain("dept:physics").await;

        // CEO and physics head have authority over physics
        let ids: Vec<_> = authorities.iter().map(|p| p.id.clone()).collect();
        assert!(ids.contains(&"ceo".to_string()));
        assert!(ids.contains(&"physics-head".to_string()));
        assert!(!ids.contains(&"vp-eng".to_string()));
    }
}
