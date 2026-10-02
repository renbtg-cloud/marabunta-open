// Marabunta - Licensed under the MIT License.
//! Quota account management for the marabunta-compute job placement system
//!
//! This module implements quota accounts that track resource usage and allocations.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::errors::{QuotaError, QuotaResult};
use super::types::{
    AccountId, OveragePenalty, Quota, QuotaEnforcement, QuotaId, QuotaLimit, QuotaResource,
};

/// A quota account that tracks resource usage and allocations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaAccount {
    /// Unique identifier for this account
    pub id: AccountId,
    /// Human-readable name
    pub name: String,
    /// Principal who owns this account
    pub owner: String,
    /// Principals who can use this account
    pub members: Vec<String>,

    /// Current resource usage
    pub usage: HashMap<String, ResourceUsage>,

    /// Allocations (budgets assigned to this account)
    pub allocations: Vec<QuotaAllocation>,

    /// When this account was created
    pub created_at: DateTime<Utc>,
    /// Periodic usage snapshots for history
    pub usage_history: Vec<UsageSnapshot>,
}

/// Tracks usage of a specific resource
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    /// Current usage amount
    pub current: f64,
    /// Start of the current period
    pub period_start: DateTime<Utc>,
    /// End of the current period (if applicable)
    pub period_end: Option<DateTime<Utc>>,
    /// Peak usage in this period
    pub peak: f64,
    /// When peak usage occurred
    pub peak_at: DateTime<Utc>,
}

impl ResourceUsage {
    /// Create new resource usage tracking
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            current: 0.0,
            period_start: now,
            period_end: None,
            peak: 0.0,
            peak_at: now,
        }
    }

    /// Create with a specific period start time
    pub fn with_period_start(period_start: DateTime<Utc>) -> Self {
        Self {
            current: 0.0,
            period_start,
            period_end: None,
            peak: 0.0,
            peak_at: period_start,
        }
    }

    /// Record usage, updating peak if necessary
    pub fn record(&mut self, amount: f64) {
        self.current += amount;
        if self.current > self.peak {
            self.peak = self.current;
            self.peak_at = Utc::now();
        }
    }

    /// Release usage (e.g., when a job finishes)
    pub fn release(&mut self, amount: f64) {
        self.current = (self.current - amount).max(0.0);
    }

    /// Reset for a new period
    pub fn reset(&mut self, new_period_start: DateTime<Utc>) {
        self.current = 0.0;
        self.period_start = new_period_start;
        self.period_end = None;
        self.peak = 0.0;
        self.peak_at = new_period_start;
    }
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self::new()
    }
}

/// An allocation of quota to an account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaAllocation {
    /// The quota this allocation is for
    pub quota_id: QuotaId,
    /// The amount allocated
    pub allocated_amount: f64,
    /// Who made this allocation
    pub allocated_by: String,
    /// When the allocation was made
    pub allocated_at: DateTime<Utc>,
    /// When the allocation expires (if applicable)
    pub expires_at: Option<DateTime<Utc>>,
    /// Whether this allocation can be transferred to other accounts
    pub transferable: bool,
}

impl QuotaAllocation {
    /// Create a new allocation
    pub fn new(quota_id: impl Into<QuotaId>, amount: f64, allocated_by: impl Into<String>) -> Self {
        Self {
            quota_id: quota_id.into(),
            allocated_amount: amount,
            allocated_by: allocated_by.into(),
            allocated_at: Utc::now(),
            expires_at: None,
            transferable: false,
        }
    }

    /// Set expiration time
    pub fn with_expiration(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Make transferable
    pub fn transferable(mut self) -> Self {
        self.transferable = true;
        self
    }

    /// Check if this allocation is still valid
    pub fn is_valid(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.map_or(true, |exp| now < exp)
    }
}

/// A snapshot of usage at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageSnapshot {
    /// When this snapshot was taken
    pub timestamp: DateTime<Utc>,
    /// Usage values at snapshot time
    pub usage: HashMap<String, f64>,
}

impl UsageSnapshot {
    /// Create a new snapshot from current usage
    pub fn from_usage(usage: &HashMap<String, ResourceUsage>) -> Self {
        Self {
            timestamp: Utc::now(),
            usage: usage.iter().map(|(k, v)| (k.clone(), v.current)).collect(),
        }
    }
}

/// A request for resources
#[derive(Debug, Clone)]
pub struct ResourceRequest {
    /// Resources being requested
    pub resources: HashMap<QuotaResource, f64>,
    /// Estimated duration (for time-based resources)
    pub duration_estimate: Option<Duration>,
}

impl ResourceRequest {
    /// Create a new resource request
    pub fn new() -> Self {
        Self {
            resources: HashMap::new(),
            duration_estimate: None,
        }
    }

    /// Add a resource to the request
    pub fn with_resource(mut self, resource: QuotaResource, amount: f64) -> Self {
        self.resources.insert(resource, amount);
        self
    }

    /// Set duration estimate
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration_estimate = Some(duration);
        self
    }

    /// Get the total amount for a resource
    pub fn amount_for(&self, resource: &QuotaResource) -> f64 {
        self.resources.get(resource).copied().unwrap_or(0.0)
    }
}

impl Default for ResourceRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of checking if an account can afford a request
#[derive(Debug, Clone)]
pub enum AffordabilityResult {
    /// The request is fully affordable
    Affordable,
    /// The request would exceed a hard limit
    Exceeded {
        resource: QuotaResource,
        requested: f64,
        available: f64,
        enforcement: QuotaEnforcement,
    },
    /// The request would exceed a soft limit
    WouldExceedSoft {
        resource: QuotaResource,
        overage: f64,
        penalty: OveragePenalty,
    },
}

impl QuotaAccount {
    /// Create a new quota account
    pub fn new(
        id: impl Into<AccountId>,
        name: impl Into<String>,
        owner: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            owner: owner.into(),
            members: Vec::new(),
            usage: HashMap::new(),
            allocations: Vec::new(),
            created_at: Utc::now(),
            usage_history: Vec::new(),
        }
    }

    /// Add a member to the account
    pub fn add_member(&mut self, principal: impl Into<String>) {
        let principal = principal.into();
        if !self.members.contains(&principal) && self.owner != principal {
            self.members.push(principal);
        }
    }

    /// Remove a member from the account
    pub fn remove_member(&mut self, principal: &str) -> bool {
        if let Some(pos) = self.members.iter().position(|m| m == principal) {
            self.members.remove(pos);
            true
        } else {
            false
        }
    }

    /// Check if a principal can use this account
    pub fn can_use(&self, principal: &str) -> bool {
        self.owner == principal || self.members.contains(&principal.to_string())
    }

    /// Get current usage for a resource
    pub fn get_usage(&self, resource: &QuotaResource) -> f64 {
        self.usage
            .get(&resource.key())
            .map(|u| u.current)
            .unwrap_or(0.0)
    }

    /// Get remaining allocation for a resource
    pub fn get_remaining(
        &self,
        resource: &QuotaResource,
        quotas: &HashMap<QuotaId, Quota>,
    ) -> Option<f64> {
        let current = self.get_usage(resource);
        let now = Utc::now();

        // Find allocations for this resource
        let total_allocated: f64 = self
            .allocations
            .iter()
            .filter(|a| a.is_valid(now))
            .filter_map(|a| {
                quotas.get(&a.quota_id).and_then(|q| {
                    if q.resource == *resource {
                        Some(a.allocated_amount.min(q.limit.effective_limit()))
                    } else {
                        None
                    }
                })
            })
            .sum();

        if total_allocated > 0.0 {
            Some((total_allocated - current).max(0.0))
        } else {
            None
        }
    }

    /// Check if the account can afford a resource request
    pub fn can_afford(
        &self,
        request: &ResourceRequest,
        quotas: &HashMap<QuotaId, Quota>,
    ) -> AffordabilityResult {
        let now = Utc::now();

        for (resource, requested) in &request.resources {
            let current = self.get_usage(resource);

            // Find the applicable quota for this resource
            for allocation in &self.allocations {
                if !allocation.is_valid(now) {
                    continue;
                }

                if let Some(quota) = quotas.get(&allocation.quota_id) {
                    if quota.resource != *resource {
                        continue;
                    }

                    let effective_limit = allocation
                        .allocated_amount
                        .min(quota.limit.effective_limit());
                    let soft_limit_value =
                        allocation.allocated_amount.min(quota.limit.soft_limit());
                    let available = (effective_limit - current).max(0.0);

                    // First check if we exceed the soft limit (for soft limits)
                    if let QuotaLimit::Soft {
                        limit: _,
                        overage_allowed: _,
                        overage_penalty,
                    } = &quota.limit
                    {
                        if current + requested > soft_limit_value
                            && current + requested <= effective_limit
                        {
                            let overage = (current + requested - soft_limit_value).max(0.0);
                            return AffordabilityResult::WouldExceedSoft {
                                resource: resource.clone(),
                                overage,
                                penalty: overage_penalty.clone(),
                            };
                        }
                    }

                    if current + requested > effective_limit {
                        return AffordabilityResult::Exceeded {
                            resource: resource.clone(),
                            requested: *requested,
                            available,
                            enforcement: quota.enforcement.clone(),
                        };
                    }
                }
            }
        }

        AffordabilityResult::Affordable
    }

    /// Record resource usage
    pub fn record_usage(&mut self, resource: &QuotaResource, amount: f64) {
        let key = resource.key();
        self.usage
            .entry(key)
            .or_default()
            .record(amount);
    }

    /// Release resource usage (e.g., when a job finishes)
    pub fn release_usage(&mut self, resource: &QuotaResource, amount: f64) {
        let key = resource.key();
        if let Some(usage) = self.usage.get_mut(&key) {
            usage.release(amount);
        }
    }

    /// Take a snapshot of current usage
    pub fn snapshot(&mut self) {
        let snapshot = UsageSnapshot::from_usage(&self.usage);
        self.usage_history.push(snapshot);

        // Keep only recent history (e.g., last 1000 snapshots)
        const MAX_HISTORY: usize = 1000;
        if self.usage_history.len() > MAX_HISTORY {
            self.usage_history
                .drain(0..self.usage_history.len() - MAX_HISTORY);
        }
    }

    /// Reset usage for a new period
    pub fn reset_period(&mut self, resource: &QuotaResource) {
        let key = resource.key();
        let now = Utc::now();
        if let Some(usage) = self.usage.get_mut(&key) {
            usage.reset(now);
        }
    }

    /// Add an allocation to this account
    pub fn add_allocation(&mut self, allocation: QuotaAllocation) {
        // Check if there's already an allocation for this quota
        if let Some(existing) = self
            .allocations
            .iter_mut()
            .find(|a| a.quota_id == allocation.quota_id)
        {
            // Update the existing allocation
            existing.allocated_amount += allocation.allocated_amount;
            existing.expires_at = allocation.expires_at;
        } else {
            self.allocations.push(allocation);
        }
    }

    /// Remove an allocation from this account
    pub fn remove_allocation(&mut self, quota_id: &QuotaId) -> Option<QuotaAllocation> {
        if let Some(pos) = self
            .allocations
            .iter()
            .position(|a| &a.quota_id == quota_id)
        {
            Some(self.allocations.remove(pos))
        } else {
            None
        }
    }

    /// Get allocation for a specific quota
    pub fn get_allocation(&self, quota_id: &QuotaId) -> Option<&QuotaAllocation> {
        self.allocations.iter().find(|a| &a.quota_id == quota_id)
    }

    /// Get mutable allocation for a specific quota
    pub fn get_allocation_mut(&mut self, quota_id: &QuotaId) -> Option<&mut QuotaAllocation> {
        self.allocations
            .iter_mut()
            .find(|a| &a.quota_id == quota_id)
    }

    /// Clean up expired allocations
    pub fn cleanup_expired_allocations(&mut self, now: DateTime<Utc>) -> Vec<QuotaAllocation> {
        let mut expired = Vec::new();
        self.allocations.retain(|a| {
            if a.is_valid(now) {
                true
            } else {
                expired.push(a.clone());
                false
            }
        });
        expired
    }

    /// Get total allocation for a resource type
    pub fn total_allocation_for(
        &self,
        resource: &QuotaResource,
        quotas: &HashMap<QuotaId, Quota>,
    ) -> f64 {
        let now = Utc::now();
        self.allocations
            .iter()
            .filter(|a| a.is_valid(now))
            .filter_map(|a| {
                quotas.get(&a.quota_id).and_then(|q| {
                    if q.resource == *resource {
                        Some(a.allocated_amount)
                    } else {
                        None
                    }
                })
            })
            .sum()
    }

    /// Transfer allocation to another account
    pub fn transfer_allocation(
        &mut self,
        quota_id: &QuotaId,
        amount: f64,
        target: &mut QuotaAccount,
    ) -> QuotaResult<()> {
        // First, gather info we need from the allocation
        let (transferable, expires_at, allocated_amount) = {
            let allocation =
                self.get_allocation(quota_id)
                    .ok_or_else(|| QuotaError::AllocationNotFound {
                        quota_id: quota_id.clone(),
                        account_id: self.id.clone(),
                    })?;
            (
                allocation.transferable,
                allocation.expires_at,
                allocation.allocated_amount,
            )
        };

        if !transferable {
            return Err(QuotaError::TransferNotAllowed {
                reason: "Allocation is not transferable".to_string(),
            });
        }

        if allocated_amount < amount {
            return Err(QuotaError::InvalidAmount(format!(
                "Cannot transfer {} when only {} is allocated",
                amount, allocated_amount
            )));
        }

        // Capture self.id before mutable borrow
        let source_id = self.id.clone();

        // Reduce source allocation
        if let Some(allocation) = self.get_allocation_mut(quota_id) {
            allocation.allocated_amount -= amount;
        }

        // Add to target
        let new_allocation = QuotaAllocation {
            quota_id: quota_id.clone(),
            allocated_amount: amount,
            allocated_by: format!("transfer from {}", source_id),
            allocated_at: Utc::now(),
            expires_at,
            transferable,
        };
        target.add_allocation(new_allocation);

        // Remove source allocation if depleted
        if let Some(allocation) = self.get_allocation(quota_id) {
            if allocation.allocated_amount <= 0.0 {
                self.remove_allocation(quota_id);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quotas::types::{QuotaEnforcement, QuotaPeriod, QuotaScope};

    fn create_test_quota(id: &str, resource: QuotaResource, limit: f64) -> Quota {
        Quota::new(
            id,
            format!("Test quota {}", id),
            resource,
            QuotaLimit::Hard(limit),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        )
    }

    #[test]
    fn test_account_creation() {
        let account = QuotaAccount::new("acc-1", "Test Account", "owner@test.com");
        assert_eq!(account.id, "acc-1");
        assert_eq!(account.owner, "owner@test.com");
        assert!(account.members.is_empty());
    }

    #[test]
    fn test_add_remove_member() {
        let mut account = QuotaAccount::new("acc-1", "Test Account", "owner@test.com");
        account.add_member("member1@test.com");
        account.add_member("member2@test.com");

        assert!(account.can_use("owner@test.com"));
        assert!(account.can_use("member1@test.com"));
        assert!(account.can_use("member2@test.com"));
        assert!(!account.can_use("stranger@test.com"));

        account.remove_member("member1@test.com");
        assert!(!account.can_use("member1@test.com"));
    }

    #[test]
    fn test_usage_tracking() {
        let mut account = QuotaAccount::new("acc-1", "Test", "owner@test.com");

        account.record_usage(&QuotaResource::CpuHours, 10.0);
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 10.0);

        account.record_usage(&QuotaResource::CpuHours, 5.0);
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 15.0);

        account.release_usage(&QuotaResource::CpuHours, 3.0);
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 12.0);
    }

    #[test]
    fn test_usage_cannot_go_negative() {
        let mut account = QuotaAccount::new("acc-1", "Test", "owner@test.com");
        account.record_usage(&QuotaResource::CpuHours, 5.0);
        account.release_usage(&QuotaResource::CpuHours, 10.0);
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 0.0);
    }

    #[test]
    fn test_allocation_management() {
        let mut account = QuotaAccount::new("acc-1", "Test", "owner@test.com");

        let allocation = QuotaAllocation::new("quota-1", 100.0, "admin");
        account.add_allocation(allocation);

        assert!(account.get_allocation(&"quota-1".to_string()).is_some());
        assert_eq!(
            account
                .get_allocation(&"quota-1".to_string())
                .unwrap()
                .allocated_amount,
            100.0
        );
    }

    #[test]
    fn test_affordability_check() {
        let mut account = QuotaAccount::new("acc-1", "Test", "owner@test.com");
        let mut quotas = HashMap::new();

        let quota = create_test_quota("cpu-quota", QuotaResource::CpuHours, 100.0);
        quotas.insert("cpu-quota".to_string(), quota);

        let allocation = QuotaAllocation::new("cpu-quota", 100.0, "admin");
        account.add_allocation(allocation);

        // Should be affordable
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 50.0);
        assert!(matches!(
            account.can_afford(&request, &quotas),
            AffordabilityResult::Affordable
        ));

        // Record some usage
        account.record_usage(&QuotaResource::CpuHours, 60.0);

        // Should exceed
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 50.0);
        assert!(matches!(
            account.can_afford(&request, &quotas),
            AffordabilityResult::Exceeded { .. }
        ));
    }

    #[test]
    fn test_soft_limit_affordability() {
        let mut account = QuotaAccount::new("acc-1", "Test", "owner@test.com");
        let mut quotas = HashMap::new();

        let quota = Quota::new(
            "cpu-quota",
            "CPU Quota",
            QuotaResource::CpuHours,
            QuotaLimit::Soft {
                limit: 100.0,
                overage_allowed: 20.0,
                overage_penalty: OveragePenalty::LowerPriority { by: 5 },
            },
            QuotaScope::Global,
            QuotaEnforcement::Deprioritize {
                priority_penalty: 5,
            },
            "admin",
            "test.com",
        );
        quotas.insert("cpu-quota".to_string(), quota);

        let allocation = QuotaAllocation::new("cpu-quota", 120.0, "admin");
        account.add_allocation(allocation);

        account.record_usage(&QuotaResource::CpuHours, 90.0);

        // Request that would exceed soft limit but within overage
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 15.0);
        let result = account.can_afford(&request, &quotas);
        assert!(matches!(
            result,
            AffordabilityResult::WouldExceedSoft { .. }
        ));
    }

    #[test]
    fn test_snapshot() {
        let mut account = QuotaAccount::new("acc-1", "Test", "owner@test.com");
        account.record_usage(&QuotaResource::CpuHours, 50.0);
        account.record_usage(&QuotaResource::GpuHours, 25.0);

        account.snapshot();

        assert_eq!(account.usage_history.len(), 1);
        let snapshot = &account.usage_history[0];
        assert_eq!(snapshot.usage.get("cpu_hours"), Some(&50.0));
        assert_eq!(snapshot.usage.get("gpu_hours"), Some(&25.0));
    }

    #[test]
    fn test_period_reset() {
        let mut account = QuotaAccount::new("acc-1", "Test", "owner@test.com");
        account.record_usage(&QuotaResource::CpuHours, 50.0);

        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 50.0);

        account.reset_period(&QuotaResource::CpuHours);

        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 0.0);
    }

    #[test]
    fn test_allocation_expiration() {
        use chrono::Duration;

        let mut account = QuotaAccount::new("acc-1", "Test", "owner@test.com");

        let now = Utc::now();
        let expired = QuotaAllocation::new("expired-quota", 100.0, "admin")
            .with_expiration(now - Duration::hours(1));
        let valid = QuotaAllocation::new("valid-quota", 100.0, "admin")
            .with_expiration(now + Duration::hours(1));

        account.add_allocation(expired);
        account.add_allocation(valid);

        let cleaned = account.cleanup_expired_allocations(now);

        assert_eq!(cleaned.len(), 1);
        assert_eq!(cleaned[0].quota_id, "expired-quota");
        assert_eq!(account.allocations.len(), 1);
        assert_eq!(account.allocations[0].quota_id, "valid-quota");
    }

    #[test]
    fn test_transfer_allocation() {
        let mut source = QuotaAccount::new("source", "Source", "owner@test.com");
        let mut target = QuotaAccount::new("target", "Target", "other@test.com");

        let allocation = QuotaAllocation::new("quota-1", 100.0, "admin").transferable();
        source.add_allocation(allocation);

        source
            .transfer_allocation(&"quota-1".to_string(), 30.0, &mut target)
            .unwrap();

        assert_eq!(
            source
                .get_allocation(&"quota-1".to_string())
                .unwrap()
                .allocated_amount,
            70.0
        );
        assert_eq!(
            target
                .get_allocation(&"quota-1".to_string())
                .unwrap()
                .allocated_amount,
            30.0
        );
    }

    #[test]
    fn test_non_transferable_allocation() {
        let mut source = QuotaAccount::new("source", "Source", "owner@test.com");
        let mut target = QuotaAccount::new("target", "Target", "other@test.com");

        let allocation = QuotaAllocation::new("quota-1", 100.0, "admin");
        source.add_allocation(allocation);

        let result = source.transfer_allocation(&"quota-1".to_string(), 30.0, &mut target);
        assert!(matches!(result, Err(QuotaError::TransferNotAllowed { .. })));
    }
}
