// Marabunta - Licensed under the MIT License.
//! Quota manager for the marabunta-compute job placement system
//!
//! This module implements the central quota management system that tracks
//! quotas, accounts, reservations, and enforces resource limits.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::account::{AffordabilityResult, QuotaAccount, QuotaAllocation, ResourceRequest};
use super::errors::{QuotaError, QuotaResult};
use super::types::{AccountId, BurstState, Quota, QuotaId, QuotaLimit, QuotaPeriod, QuotaResource};

/// A reservation of quota for a job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaReservation {
    /// The job this reservation is for
    pub job_id: String,
    /// The account charged for this reservation
    pub account_id: AccountId,
    /// Resources reserved
    pub resources: HashMap<QuotaResource, f64>,
    /// When this reservation was created
    pub created_at: DateTime<Utc>,
    /// When this reservation expires (if applicable)
    pub expires_at: Option<DateTime<Utc>>,
    /// Whether this reservation has been consumed (job started running)
    pub consumed: bool,
}

impl QuotaReservation {
    /// Create a new reservation
    pub fn new(
        job_id: impl Into<String>,
        account_id: AccountId,
        resources: HashMap<QuotaResource, f64>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            account_id,
            resources,
            created_at: Utc::now(),
            expires_at: None,
            consumed: false,
        }
    }

    /// Set expiration time
    pub fn with_expiration(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Check if this reservation has expired
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|exp| now >= exp)
    }

    /// Get total resource amount for a specific resource
    pub fn amount_for(&self, resource: &QuotaResource) -> f64 {
        self.resources.get(resource).copied().unwrap_or(0.0)
    }
}

/// Result of checking quota for job submission
#[derive(Debug, Clone)]
pub struct QuotaCheckResult {
    /// Whether the job is allowed
    pub allowed: bool,
    /// Recommended account to use (if allowed)
    pub account_id: Option<AccountId>,
    /// Warnings about quota status
    pub warnings: Vec<QuotaWarning>,
    /// Details for each checked quota
    pub details: Vec<QuotaCheckDetail>,
}

impl QuotaCheckResult {
    /// Create an allowed result
    pub fn allowed(account_id: AccountId) -> Self {
        Self {
            allowed: true,
            account_id: Some(account_id),
            warnings: Vec::new(),
            details: Vec::new(),
        }
    }

    /// Create a denied result
    pub fn denied() -> Self {
        Self {
            allowed: false,
            account_id: None,
            warnings: Vec::new(),
            details: Vec::new(),
        }
    }

    /// Add a warning
    pub fn with_warning(mut self, warning: QuotaWarning) -> Self {
        self.warnings.push(warning);
        self
    }

    /// Add a detail
    pub fn with_detail(mut self, detail: QuotaCheckDetail) -> Self {
        self.details.push(detail);
        self
    }
}

/// Details about a specific quota check
#[derive(Debug, Clone)]
pub struct QuotaCheckDetail {
    /// The quota that was checked
    pub quota_id: QuotaId,
    /// The resource type
    pub resource: QuotaResource,
    /// Amount requested
    pub requested: f64,
    /// Amount available
    pub available: f64,
    /// The limit
    pub limit: f64,
    /// Status of this check
    pub status: QuotaStatus,
}

/// Status of a quota check
#[derive(Debug, Clone)]
pub enum QuotaStatus {
    /// Within limits
    Ok,
    /// Approaching the limit
    Approaching { percent_used: f64 },
    /// Soft limit exceeded
    SoftLimitExceeded { overage: f64 },
    /// Hard limit exceeded
    HardLimitExceeded,
    /// Would exceed burst limit
    WouldExceedBurst,
}

/// A warning about quota status
#[derive(Debug, Clone)]
pub struct QuotaWarning {
    /// Warning code for programmatic handling
    pub code: String,
    /// Human-readable message
    pub message: String,
    /// The quota this warning relates to
    pub quota_id: QuotaId,
}

impl QuotaWarning {
    /// Create a new warning
    pub fn new(code: impl Into<String>, message: impl Into<String>, quota_id: QuotaId) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            quota_id,
        }
    }
}

/// Summary of usage for an account
#[derive(Debug, Clone)]
pub struct UsageSummary {
    /// The account
    pub account_id: AccountId,
    /// Start of the current period
    pub period_start: DateTime<Utc>,
    /// Summary for each resource
    pub resources: HashMap<QuotaResource, ResourceSummary>,
}

/// Summary for a specific resource
#[derive(Debug, Clone)]
pub struct ResourceSummary {
    /// Current usage
    pub used: f64,
    /// Total allocated
    pub allocated: f64,
    /// Effective limit
    pub limit: f64,
    /// Percentage used
    pub percent_used: f64,
    /// Usage trend
    pub trend: UsageTrend,
}

/// Trend in usage
#[derive(Debug, Clone)]
pub enum UsageTrend {
    /// Usage is increasing at the given rate per hour
    Increasing { rate: f64 },
    /// Usage is stable
    Stable,
    /// Usage is decreasing at the given rate per hour
    Decreasing { rate: f64 },
    /// Not enough data to determine trend
    Unknown,
}

/// The central quota manager
pub struct QuotaManager {
    /// All quotas
    quotas: HashMap<QuotaId, Quota>,
    /// All accounts
    accounts: HashMap<AccountId, QuotaAccount>,
    /// Index: principal -> accounts they can use
    principal_accounts: HashMap<String, Vec<AccountId>>,
    /// Active reservations (job_id -> reservation)
    pub(crate) reservations: HashMap<String, QuotaReservation>,
    /// Burst state tracking per account per resource
    pub(crate) burst_states: HashMap<(AccountId, String), BurstState>,
}

impl QuotaManager {
    /// Create a new quota manager
    pub fn new() -> Self {
        Self {
            quotas: HashMap::new(),
            accounts: HashMap::new(),
            principal_accounts: HashMap::new(),
            reservations: HashMap::new(),
            burst_states: HashMap::new(),
        }
    }

    // ========== Quota CRUD ==========

    /// Create a new quota
    pub fn create_quota(&mut self, quota: Quota) -> QuotaResult<QuotaId> {
        if self.quotas.contains_key(&quota.id) {
            return Err(QuotaError::QuotaAlreadyExists(quota.id));
        }
        let id = quota.id.clone();
        self.quotas.insert(id.clone(), quota);
        Ok(id)
    }

    /// Update an existing quota
    pub fn update_quota(&mut self, quota: Quota) -> QuotaResult<()> {
        if !self.quotas.contains_key(&quota.id) {
            return Err(QuotaError::QuotaNotFound(quota.id));
        }
        self.quotas.insert(quota.id.clone(), quota);
        Ok(())
    }

    /// Delete a quota
    pub fn delete_quota(&mut self, id: &QuotaId) -> QuotaResult<Quota> {
        self.quotas
            .remove(id)
            .ok_or_else(|| QuotaError::QuotaNotFound(id.clone()))
    }

    /// Get a quota by ID
    pub fn get_quota(&self, id: &QuotaId) -> Option<&Quota> {
        self.quotas.get(id)
    }

    /// List all quotas
    pub fn list_quotas(&self) -> Vec<&Quota> {
        self.quotas.values().collect()
    }

    // ========== Account CRUD ==========

    /// Create a new account
    pub fn create_account(&mut self, account: QuotaAccount) -> QuotaResult<AccountId> {
        if self.accounts.contains_key(&account.id) {
            return Err(QuotaError::AccountAlreadyExists(account.id));
        }

        let id = account.id.clone();
        let owner = account.owner.clone();
        let members = account.members.clone();

        // Update principal index
        self.principal_accounts
            .entry(owner)
            .or_default()
            .push(id.clone());
        for member in members {
            self.principal_accounts
                .entry(member)
                .or_default()
                .push(id.clone());
        }

        self.accounts.insert(id.clone(), account);
        Ok(id)
    }

    /// Get an account by ID
    pub fn get_account(&self, id: &AccountId) -> Option<&QuotaAccount> {
        self.accounts.get(id)
    }

    /// Get a mutable reference to an account
    pub fn get_account_mut(&mut self, id: &AccountId) -> Option<&mut QuotaAccount> {
        self.accounts.get_mut(id)
    }

    /// List all accounts
    pub fn list_accounts(&self) -> Vec<&QuotaAccount> {
        self.accounts.values().collect()
    }

    /// Delete an account
    pub fn delete_account(&mut self, id: &AccountId) -> QuotaResult<QuotaAccount> {
        let account = self
            .accounts
            .remove(id)
            .ok_or_else(|| QuotaError::AccountNotFound(id.clone()))?;

        // Clean up principal index
        if let Some(accounts) = self.principal_accounts.get_mut(&account.owner) {
            accounts.retain(|a| a != id);
        }
        for member in &account.members {
            if let Some(accounts) = self.principal_accounts.get_mut(member) {
                accounts.retain(|a| a != id);
            }
        }

        Ok(account)
    }

    // ========== Allocation ==========

    /// Allocate quota to an account
    pub fn allocate(
        &mut self,
        quota_id: &QuotaId,
        account_id: &AccountId,
        amount: f64,
        allocated_by: &str,
    ) -> QuotaResult<()> {
        // Verify quota exists
        if !self.quotas.contains_key(quota_id) {
            return Err(QuotaError::QuotaNotFound(quota_id.clone()));
        }

        // Verify account exists
        let account = self
            .accounts
            .get_mut(account_id)
            .ok_or_else(|| QuotaError::AccountNotFound(account_id.clone()))?;

        // Create allocation
        let allocation = QuotaAllocation::new(quota_id.clone(), amount, allocated_by);
        account.add_allocation(allocation);

        Ok(())
    }

    /// Transfer allocation between accounts
    pub fn transfer(
        &mut self,
        from_account: &AccountId,
        to_account: &AccountId,
        resource: &QuotaResource,
        amount: f64,
    ) -> QuotaResult<()> {
        // Find quota for this resource
        let quota_id = self
            .quotas
            .iter()
            .find(|(_, q)| &q.resource == resource)
            .map(|(id, _)| id.clone())
            .ok_or_else(|| QuotaError::QuotaNotFound(format!("for resource {:?}", resource)))?;

        // Need to handle the borrow checker carefully here
        let source_account = self
            .accounts
            .get(from_account)
            .ok_or_else(|| QuotaError::AccountNotFound(from_account.clone()))?;

        let allocation = source_account.get_allocation(&quota_id).ok_or_else(|| {
            QuotaError::AllocationNotFound {
                quota_id: quota_id.clone(),
                account_id: from_account.clone(),
            }
        })?;

        if !allocation.transferable {
            return Err(QuotaError::TransferNotAllowed {
                reason: "Allocation is not transferable".to_string(),
            });
        }

        if allocation.allocated_amount < amount {
            return Err(QuotaError::InvalidAmount(format!(
                "Cannot transfer {} when only {} is allocated",
                amount, allocation.allocated_amount
            )));
        }

        // Now do the actual transfer
        // We need to work around the borrow checker
        let expires_at = allocation.expires_at;
        let transferable = allocation.transferable;

        // Update source
        {
            let source = self.accounts.get_mut(from_account).unwrap();
            let alloc = source.get_allocation_mut(&quota_id).unwrap();
            alloc.allocated_amount -= amount;
            if alloc.allocated_amount <= 0.0 {
                source.remove_allocation(&quota_id);
            }
        }

        // Update target
        {
            let target = self
                .accounts
                .get_mut(to_account)
                .ok_or_else(|| QuotaError::AccountNotFound(to_account.clone()))?;

            let new_allocation = QuotaAllocation {
                quota_id,
                allocated_amount: amount,
                allocated_by: format!("transfer from {}", from_account),
                allocated_at: Utc::now(),
                expires_at,
                transferable,
            };
            target.add_allocation(new_allocation);
        }

        Ok(())
    }

    // ========== Core Operations ==========

    /// Check if a job can be submitted (doesn't reserve, just checks)
    pub fn check_submission(
        &self,
        principal_id: &str,
        request: &ResourceRequest,
    ) -> QuotaCheckResult {
        // Find accounts the principal can use
        let account_ids = match self.principal_accounts.get(principal_id) {
            Some(ids) => ids.clone(),
            None => {
                return QuotaCheckResult::denied().with_warning(QuotaWarning::new(
                    "NO_ACCOUNTS",
                    format!("Principal {} has no accounts", principal_id),
                    String::new(),
                ))
            }
        };

        // Try each account
        for account_id in account_ids {
            let account = match self.accounts.get(&account_id) {
                Some(a) => a,
                None => continue,
            };

            let mut result = QuotaCheckResult::allowed(account_id.clone());
            let mut can_use = true;

            // Check each requested resource
            for (resource, requested) in &request.resources {
                let current = account.get_usage(resource);

                // Find applicable quotas
                for allocation in &account.allocations {
                    if !allocation.is_valid(Utc::now()) {
                        continue;
                    }

                    let quota = match self.quotas.get(&allocation.quota_id) {
                        Some(q) if &q.resource == resource => q,
                        _ => continue,
                    };

                    let effective_limit = allocation
                        .allocated_amount
                        .min(quota.limit.effective_limit());
                    let soft_limit_value =
                        allocation.allocated_amount.min(quota.limit.soft_limit());
                    let available = (effective_limit - current).max(0.0);
                    let percent_used = if effective_limit > 0.0 {
                        (current / effective_limit) * 100.0
                    } else {
                        0.0
                    };

                    // Check soft limit separately - if we exceed soft but not effective
                    let status = if let QuotaLimit::Soft { .. } = &quota.limit {
                        if current + requested > soft_limit_value
                            && current + requested <= effective_limit
                        {
                            let overage = (current + requested - soft_limit_value).max(0.0);
                            result = result.with_warning(QuotaWarning::new(
                                "SOFT_LIMIT_EXCEEDED",
                                format!("Soft limit exceeded by {} for {:?}", overage, resource),
                                allocation.quota_id.clone(),
                            ));
                            QuotaStatus::SoftLimitExceeded { overage }
                        } else if current + requested > effective_limit {
                            can_use = false;
                            QuotaStatus::HardLimitExceeded
                        } else if percent_used >= 80.0 {
                            result = result.with_warning(QuotaWarning::new(
                                "APPROACHING_LIMIT",
                                format!(
                                    "Approaching quota limit for {:?}: {:.1}% used",
                                    resource, percent_used
                                ),
                                allocation.quota_id.clone(),
                            ));
                            QuotaStatus::Approaching { percent_used }
                        } else {
                            QuotaStatus::Ok
                        }
                    } else if let QuotaLimit::Burst {
                        sustained_limit,
                        burst_limit,
                        ..
                    } = &quota.limit
                    {
                        if current + requested > *sustained_limit {
                            let key = (account_id.clone(), resource.key());
                            let can_burst = self
                                .burst_states
                                .get(&key)
                                .map_or(true, |s| s.can_burst(&quota.limit, Utc::now()));

                            if current + requested <= *burst_limit && can_burst {
                                result = result.with_warning(QuotaWarning::new(
                                    "USING_BURST",
                                    format!("Using burst capacity for {:?}", resource),
                                    allocation.quota_id.clone(),
                                ));
                                QuotaStatus::Approaching {
                                    percent_used: (current + requested) / burst_limit * 100.0,
                                }
                            } else {
                                can_use = false;
                                QuotaStatus::WouldExceedBurst
                            }
                        } else if percent_used >= 80.0 {
                            result = result.with_warning(QuotaWarning::new(
                                "APPROACHING_LIMIT",
                                format!(
                                    "Approaching quota limit for {:?}: {:.1}% used",
                                    resource, percent_used
                                ),
                                allocation.quota_id.clone(),
                            ));
                            QuotaStatus::Approaching { percent_used }
                        } else {
                            QuotaStatus::Ok
                        }
                    } else if current + requested > effective_limit {
                        match &quota.limit {
                            QuotaLimit::Hard(_) => {
                                can_use = false;
                                QuotaStatus::HardLimitExceeded
                            }
                            QuotaLimit::Tiered { .. } | QuotaLimit::Unlimited => QuotaStatus::Ok,
                            _ => {
                                can_use = false;
                                QuotaStatus::HardLimitExceeded
                            }
                        }
                    } else if percent_used >= 80.0 {
                        result = result.with_warning(QuotaWarning::new(
                            "APPROACHING_LIMIT",
                            format!(
                                "Approaching quota limit for {:?}: {:.1}% used",
                                resource, percent_used
                            ),
                            allocation.quota_id.clone(),
                        ));
                        QuotaStatus::Approaching { percent_used }
                    } else {
                        QuotaStatus::Ok
                    };

                    result = result.with_detail(QuotaCheckDetail {
                        quota_id: allocation.quota_id.clone(),
                        resource: resource.clone(),
                        requested: *requested,
                        available,
                        limit: effective_limit,
                        status,
                    });
                }
            }

            if can_use {
                return result;
            }
        }

        QuotaCheckResult::denied().with_warning(QuotaWarning::new(
            "NO_AVAILABLE_QUOTA",
            "No accounts with sufficient quota found".to_string(),
            String::new(),
        ))
    }

    /// Reserve quota for a job (call when job is accepted)
    pub fn reserve(
        &mut self,
        job_id: &str,
        account_id: &AccountId,
        request: &ResourceRequest,
    ) -> QuotaResult<QuotaReservation> {
        // Verify account exists and can afford
        let account = self
            .accounts
            .get(account_id)
            .ok_or_else(|| QuotaError::AccountNotFound(account_id.clone()))?;

        match account.can_afford(request, &self.quotas) {
            AffordabilityResult::Affordable | AffordabilityResult::WouldExceedSoft { .. } => {}
            AffordabilityResult::Exceeded {
                resource,
                requested,
                available,
                ..
            } => {
                return Err(QuotaError::QuotaExceeded {
                    resource,
                    requested,
                    available,
                });
            }
        }

        // Record usage (optimistically)
        let account = self.accounts.get_mut(account_id).unwrap();
        for (resource, amount) in &request.resources {
            account.record_usage(resource, *amount);
        }

        // Create reservation
        let reservation =
            QuotaReservation::new(job_id, account_id.clone(), request.resources.clone());
        self.reservations
            .insert(job_id.to_string(), reservation.clone());

        Ok(reservation)
    }

    /// Consume reserved quota (call when job starts running)
    pub fn consume(&mut self, job_id: &str) -> QuotaResult<()> {
        let reservation = self
            .reservations
            .get_mut(job_id)
            .ok_or_else(|| QuotaError::ReservationNotFound(job_id.to_string()))?;

        if reservation.consumed {
            return Ok(()); // Already consumed
        }

        if reservation.is_expired(Utc::now()) {
            return Err(QuotaError::ReservationExpired(job_id.to_string()));
        }

        reservation.consumed = true;

        // Update burst state if applicable
        let account_id = reservation.account_id.clone();
        for resource in reservation.resources.keys() {
            for allocation in self
                .accounts
                .get(&account_id)
                .map(|a| &a.allocations)
                .unwrap_or(&vec![])
            {
                if let Some(quota) = self.quotas.get(&allocation.quota_id) {
                    if &quota.resource == resource {
                        if let QuotaLimit::Burst {
                            sustained_limit, ..
                        } = &quota.limit
                        {
                            let current = self
                                .accounts
                                .get(&account_id)
                                .map(|a| a.get_usage(resource))
                                .unwrap_or(0.0);

                            if current > *sustained_limit {
                                let key = (account_id.clone(), resource.key());
                                let state = self.burst_states.entry(key).or_default();
                                if !state.in_burst {
                                    state.start_burst(Utc::now());
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Release quota (call when job finishes or is cancelled)
    pub fn release(&mut self, job_id: &str, actual_usage: &ResourceRequest) -> QuotaResult<()> {
        let reservation = self
            .reservations
            .remove(job_id)
            .ok_or_else(|| QuotaError::ReservationNotFound(job_id.to_string()))?;

        let account = self
            .accounts
            .get_mut(&reservation.account_id)
            .ok_or_else(|| QuotaError::AccountNotFound(reservation.account_id.clone()))?;

        // Release the reserved amount
        for (resource, reserved_amount) in &reservation.resources {
            account.release_usage(resource, *reserved_amount);
        }

        // Record actual usage (if different from reservation)
        for (resource, actual_amount) in &actual_usage.resources {
            let reserved = reservation.amount_for(resource);
            if *actual_amount > reserved {
                // Used more than reserved - record the difference
                account.record_usage(resource, actual_amount - reserved);
            }
        }

        // Update burst state if applicable
        for resource in reservation.resources.keys() {
            let key = (reservation.account_id.clone(), resource.key());
            if let Some(state) = self.burst_states.get_mut(&key) {
                if state.in_burst {
                    // Check if we're back under sustained limit
                    let current = account.get_usage(resource);
                    for allocation in &account.allocations {
                        if let Some(quota) = self.quotas.get(&allocation.quota_id) {
                            if &quota.resource == resource {
                                if let QuotaLimit::Burst {
                                    sustained_limit, ..
                                } = &quota.limit
                                {
                                    if current <= *sustained_limit {
                                        state.end_burst(Utc::now());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Adjust reservation (job took more/less than expected)
    pub fn adjust_reservation(
        &mut self,
        job_id: &str,
        new_request: &ResourceRequest,
    ) -> QuotaResult<()> {
        let reservation = self
            .reservations
            .get(job_id)
            .ok_or_else(|| QuotaError::ReservationNotFound(job_id.to_string()))?;

        let account_id = reservation.account_id.clone();
        let old_resources = reservation.resources.clone();

        // Calculate differences
        let account = self
            .accounts
            .get_mut(&account_id)
            .ok_or_else(|| QuotaError::AccountNotFound(account_id.clone()))?;

        for (resource, new_amount) in &new_request.resources {
            let old_amount = old_resources.get(resource).copied().unwrap_or(0.0);
            if *new_amount > old_amount {
                // Need more - check if affordable
                let additional = new_amount - old_amount;
                let current = account.get_usage(resource);

                // Find limit
                for allocation in &account.allocations {
                    if let Some(quota) = self.quotas.get(&allocation.quota_id) {
                        if &quota.resource == resource {
                            let limit = allocation
                                .allocated_amount
                                .min(quota.limit.effective_limit());
                            if current + additional > limit {
                                return Err(QuotaError::QuotaExceeded {
                                    resource: resource.clone(),
                                    requested: additional,
                                    available: (limit - current).max(0.0),
                                });
                            }
                        }
                    }
                }

                account.record_usage(resource, additional);
            } else if *new_amount < old_amount {
                // Need less - release
                account.release_usage(resource, old_amount - new_amount);
            }
        }

        // Update reservation
        let reservation = self.reservations.get_mut(job_id).unwrap();
        reservation.resources = new_request.resources.clone();

        Ok(())
    }

    // ========== Queries ==========

    /// Get accounts a principal can use
    pub fn accounts_for_principal(&self, principal_id: &str) -> Vec<&QuotaAccount> {
        self.principal_accounts
            .get(principal_id)
            .map(|ids| ids.iter().filter_map(|id| self.accounts.get(id)).collect())
            .unwrap_or_default()
    }

    /// Get current usage summary for an account
    pub fn usage_summary(&self, account_id: &AccountId) -> Option<UsageSummary> {
        let account = self.accounts.get(account_id)?;

        let mut resources = HashMap::new();

        for (resource_key, usage) in &account.usage {
            // Find the matching QuotaResource and quota
            let resource = self.resource_from_key(resource_key)?;

            // Find allocation for this resource
            let (allocated, limit) = account
                .allocations
                .iter()
                .filter(|a| a.is_valid(Utc::now()))
                .find_map(|a| {
                    self.quotas.get(&a.quota_id).and_then(|q| {
                        if q.resource == resource {
                            Some((
                                a.allocated_amount,
                                a.allocated_amount.min(q.limit.effective_limit()),
                            ))
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or((0.0, 0.0));

            let percent_used = if limit > 0.0 {
                (usage.current / limit) * 100.0
            } else {
                0.0
            };

            // Calculate trend from history
            let trend = self.calculate_trend(account, resource_key);

            resources.insert(
                resource,
                ResourceSummary {
                    used: usage.current,
                    allocated,
                    limit,
                    percent_used,
                    trend,
                },
            );
        }

        Some(UsageSummary {
            account_id: account_id.clone(),
            period_start: account
                .usage
                .values()
                .next()
                .map(|u| u.period_start)
                .unwrap_or_else(Utc::now),
            resources,
        })
    }

    /// Get quotas that apply to an account
    pub fn quotas_for_account(&self, account_id: &AccountId) -> Vec<&Quota> {
        let account = match self.accounts.get(account_id) {
            Some(a) => a,
            None => return Vec::new(),
        };

        account
            .allocations
            .iter()
            .filter(|a| a.is_valid(Utc::now()))
            .filter_map(|a| self.quotas.get(&a.quota_id))
            .collect()
    }

    /// Get a reservation by job ID
    pub fn get_reservation(&self, job_id: &str) -> Option<&QuotaReservation> {
        self.reservations.get(job_id)
    }

    /// List all active reservations
    pub fn list_reservations(&self) -> Vec<&QuotaReservation> {
        self.reservations.values().collect()
    }

    // ========== Maintenance ==========

    /// Reset quotas for a new period
    pub fn reset_periods(&mut self, now: DateTime<Utc>) {
        // First, collect information about what needs to be reset
        let mut resets_needed: Vec<(AccountId, QuotaResource)> = Vec::new();

        for quota in self.quotas.values() {
            match &quota.period {
                QuotaPeriod::Calendar(period) => {
                    let period_start = period.period_start(now);

                    // Check all accounts with allocations for this quota
                    for account in self.accounts.values() {
                        for allocation in &account.allocations {
                            if allocation.quota_id == quota.id {
                                // Check if we need to reset
                                let usage = account.usage.get(&quota.resource.key());
                                if let Some(usage) = usage {
                                    if usage.period_start < period_start {
                                        resets_needed
                                            .push((account.id.clone(), quota.resource.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
                QuotaPeriod::Rolling { duration } => {
                    // Rolling periods reset based on when usage started
                    for account in self.accounts.values() {
                        let key = quota.resource.key();
                        if let Some(usage) = account.usage.get(&key) {
                            if now >= usage.period_start + *duration {
                                resets_needed.push((account.id.clone(), quota.resource.clone()));
                            }
                        }
                    }
                }
                QuotaPeriod::NonExpiring => {
                    // No reset needed
                }
            }
        }

        // Now perform the resets
        for (account_id, resource) in resets_needed {
            if let Some(account) = self.accounts.get_mut(&account_id) {
                account.reset_period(&resource);
            }
        }
    }

    /// Clean up expired reservations
    pub fn cleanup_expired_reservations(&mut self, now: DateTime<Utc>) -> Vec<QuotaReservation> {
        let mut expired = Vec::new();

        let expired_ids: Vec<_> = self
            .reservations
            .iter()
            .filter(|(_, r)| r.is_expired(now))
            .map(|(id, _)| id.clone())
            .collect();

        for id in expired_ids {
            if let Some(reservation) = self.reservations.remove(&id) {
                // Release the reserved resources
                if let Some(account) = self.accounts.get_mut(&reservation.account_id) {
                    for (resource, amount) in &reservation.resources {
                        account.release_usage(resource, *amount);
                    }
                }
                expired.push(reservation);
            }
        }

        expired
    }

    /// Take snapshots of all accounts
    pub fn snapshot_all(&mut self) {
        for account in self.accounts.values_mut() {
            account.snapshot();
        }
    }

    // ========== Helper Methods ==========

    fn resource_from_key(&self, key: &str) -> Option<QuotaResource> {
        match key {
            "cpu_hours" => Some(QuotaResource::CpuHours),
            "gpu_hours" => Some(QuotaResource::GpuHours),
            "memory_gb_hours" => Some(QuotaResource::MemoryGBHours),
            "concurrent_jobs" => Some(QuotaResource::ConcurrentJobs),
            "concurrent_tasks" => Some(QuotaResource::ConcurrentTasks),
            "total_jobs_per_period" => Some(QuotaResource::TotalJobsPerPeriod),
            "storage_gb" => Some(QuotaResource::StorageGB),
            "network_egress_gb" => Some(QuotaResource::NetworkEgressGB),
            s if s.starts_with("custom:") => Some(QuotaResource::Custom(
                s.strip_prefix("custom:").unwrap().to_string(),
            )),
            _ => None,
        }
    }

    fn calculate_trend(&self, account: &QuotaAccount, resource_key: &str) -> UsageTrend {
        if account.usage_history.len() < 2 {
            return UsageTrend::Unknown;
        }

        // Get last few snapshots
        let recent: Vec<_> = account.usage_history.iter().rev().take(10).collect();

        if recent.len() < 2 {
            return UsageTrend::Unknown;
        }

        // Calculate average rate of change
        let mut total_change = 0.0;
        let mut total_time = 0.0;

        for i in 0..recent.len() - 1 {
            let current = recent[i].usage.get(resource_key).copied().unwrap_or(0.0);
            let previous = recent[i + 1]
                .usage
                .get(resource_key)
                .copied()
                .unwrap_or(0.0);
            let time_diff =
                (recent[i].timestamp - recent[i + 1].timestamp).num_seconds() as f64 / 3600.0; // Convert to hours

            if time_diff > 0.0 {
                total_change += current - previous;
                total_time += time_diff;
            }
        }

        if total_time > 0.0 {
            let rate = total_change / total_time;
            if rate > 0.1 {
                UsageTrend::Increasing { rate }
            } else if rate < -0.1 {
                UsageTrend::Decreasing { rate: rate.abs() }
            } else {
                UsageTrend::Stable
            }
        } else {
            UsageTrend::Unknown
        }
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe wrapper for QuotaManager
pub type SharedQuotaManager = Arc<RwLock<QuotaManager>>;

/// Create a new shared quota manager
pub fn shared_quota_manager() -> SharedQuotaManager {
    Arc::new(RwLock::new(QuotaManager::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quotas::types::{CalendarPeriod, OveragePenalty, QuotaEnforcement, QuotaPeriod, QuotaScope};
    use chrono::Duration;

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

    fn setup_manager() -> QuotaManager {
        let mut manager = QuotaManager::new();

        // Create a quota
        let quota = create_test_quota("cpu-quota", QuotaResource::CpuHours, 100.0);
        manager.create_quota(quota).unwrap();

        // Create an account
        let account = QuotaAccount::new("acc-1", "Test Account", "user@test.com");
        manager.create_account(account).unwrap();

        // Allocate quota to account
        manager
            .allocate(
                &"cpu-quota".to_string(),
                &"acc-1".to_string(),
                100.0,
                "admin",
            )
            .unwrap();

        manager
    }

    #[test]
    fn test_create_quota() {
        let mut manager = QuotaManager::new();
        let quota = create_test_quota("test-quota", QuotaResource::CpuHours, 100.0);

        let id = manager.create_quota(quota).unwrap();
        assert_eq!(id, "test-quota");
        assert!(manager.get_quota(&"test-quota".to_string()).is_some());
    }

    #[test]
    fn test_duplicate_quota() {
        let mut manager = QuotaManager::new();
        let quota = create_test_quota("test-quota", QuotaResource::CpuHours, 100.0);
        manager.create_quota(quota.clone()).unwrap();

        let result = manager.create_quota(quota);
        assert!(matches!(result, Err(QuotaError::QuotaAlreadyExists(_))));
    }

    #[test]
    fn test_create_account() {
        let mut manager = QuotaManager::new();
        let account = QuotaAccount::new("acc-1", "Test Account", "user@test.com");

        let id = manager.create_account(account).unwrap();
        assert_eq!(id, "acc-1");
        assert!(manager.get_account(&"acc-1".to_string()).is_some());
    }

    #[test]
    fn test_principal_accounts_index() {
        let mut manager = QuotaManager::new();

        let mut account = QuotaAccount::new("acc-1", "Test Account", "owner@test.com");
        account.add_member("member@test.com");
        manager.create_account(account).unwrap();

        let accounts = manager.accounts_for_principal("owner@test.com");
        assert_eq!(accounts.len(), 1);

        let accounts = manager.accounts_for_principal("member@test.com");
        assert_eq!(accounts.len(), 1);

        let accounts = manager.accounts_for_principal("stranger@test.com");
        assert!(accounts.is_empty());
    }

    #[test]
    fn test_check_submission_allowed() {
        let manager = setup_manager();

        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 50.0);
        let result = manager.check_submission("user@test.com", &request);

        assert!(result.allowed);
        assert_eq!(result.account_id, Some("acc-1".to_string()));
    }

    #[test]
    fn test_check_submission_exceeded() {
        let manager = setup_manager();

        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 150.0);
        let result = manager.check_submission("user@test.com", &request);

        assert!(!result.allowed);
    }

    #[test]
    fn test_reserve_and_release() {
        let mut manager = setup_manager();

        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 50.0);

        // Reserve
        let reservation = manager
            .reserve("job-1", &"acc-1".to_string(), &request)
            .unwrap();
        assert_eq!(reservation.job_id, "job-1");

        // Check usage increased
        let account = manager.get_account(&"acc-1".to_string()).unwrap();
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 50.0);

        // Release
        let actual = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 45.0);
        manager.release("job-1", &actual).unwrap();

        // Check usage decreased
        let account = manager.get_account(&"acc-1".to_string()).unwrap();
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 0.0);
    }

    #[test]
    fn test_reserve_exceeds_quota() {
        let mut manager = setup_manager();

        // First reservation takes 80
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 80.0);
        manager
            .reserve("job-1", &"acc-1".to_string(), &request)
            .unwrap();

        // Second reservation tries to take 30 more (total 110 > 100)
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 30.0);
        let result = manager.reserve("job-2", &"acc-1".to_string(), &request);

        assert!(matches!(result, Err(QuotaError::QuotaExceeded { .. })));
    }

    #[test]
    fn test_consume_reservation() {
        let mut manager = setup_manager();

        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 50.0);
        manager
            .reserve("job-1", &"acc-1".to_string(), &request)
            .unwrap();

        // Consume
        manager.consume("job-1").unwrap();

        let reservation = manager.get_reservation("job-1").unwrap();
        assert!(reservation.consumed);
    }

    #[test]
    fn test_adjust_reservation() {
        let mut manager = setup_manager();

        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 50.0);
        manager
            .reserve("job-1", &"acc-1".to_string(), &request)
            .unwrap();

        // Adjust to need less
        let new_request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 30.0);
        manager.adjust_reservation("job-1", &new_request).unwrap();

        let account = manager.get_account(&"acc-1".to_string()).unwrap();
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 30.0);

        // Adjust to need more
        let new_request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 60.0);
        manager.adjust_reservation("job-1", &new_request).unwrap();

        let account = manager.get_account(&"acc-1".to_string()).unwrap();
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 60.0);
    }

    #[test]
    fn test_cleanup_expired_reservations() {
        let mut manager = setup_manager();

        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 50.0);
        let _reservation = manager
            .reserve("job-1", &"acc-1".to_string(), &request)
            .unwrap();

        // Make it expired
        let expired_time = Utc::now() - Duration::hours(1);
        if let Some(r) = manager.reservations.get_mut("job-1") {
            r.expires_at = Some(expired_time);
        }

        let cleaned = manager.cleanup_expired_reservations(Utc::now());
        assert_eq!(cleaned.len(), 1);

        // Usage should be released
        let account = manager.get_account(&"acc-1".to_string()).unwrap();
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 0.0);
    }

    #[test]
    fn test_soft_limit_warning() {
        let mut manager = QuotaManager::new();

        let quota = Quota::new(
            "cpu-soft",
            "CPU Soft Quota",
            QuotaResource::CpuHours,
            QuotaLimit::Soft {
                limit: 80.0,
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
        manager.create_quota(quota).unwrap();

        let account = QuotaAccount::new("acc-1", "Test", "user@test.com");
        manager.create_account(account).unwrap();
        manager
            .allocate(
                &"cpu-soft".to_string(),
                &"acc-1".to_string(),
                100.0,
                "admin",
            )
            .unwrap();

        // Record 75 hours of usage
        manager
            .get_account_mut(&"acc-1".to_string())
            .unwrap()
            .record_usage(&QuotaResource::CpuHours, 75.0);

        // Request that would exceed soft limit
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 10.0);
        let result = manager.check_submission("user@test.com", &request);

        assert!(result.allowed);
        assert!(result
            .warnings
            .iter()
            .any(|w| w.code == "SOFT_LIMIT_EXCEEDED"));
    }

    #[test]
    fn test_transfer_allocation() {
        let mut manager = QuotaManager::new();

        let quota = create_test_quota("cpu-quota", QuotaResource::CpuHours, 200.0);
        manager.create_quota(quota).unwrap();

        let account1 = QuotaAccount::new("acc-1", "Account 1", "user1@test.com");
        let account2 = QuotaAccount::new("acc-2", "Account 2", "user2@test.com");
        manager.create_account(account1).unwrap();
        manager.create_account(account2).unwrap();

        // Create transferable allocation
        let allocation =
            QuotaAllocation::new("cpu-quota".to_string(), 100.0, "admin").transferable();
        manager
            .get_account_mut(&"acc-1".to_string())
            .unwrap()
            .add_allocation(allocation);

        // Transfer
        manager
            .transfer(
                &"acc-1".to_string(),
                &"acc-2".to_string(),
                &QuotaResource::CpuHours,
                40.0,
            )
            .unwrap();

        let acc1 = manager.get_account(&"acc-1".to_string()).unwrap();
        let acc2 = manager.get_account(&"acc-2".to_string()).unwrap();

        assert_eq!(
            acc1.get_allocation(&"cpu-quota".to_string())
                .unwrap()
                .allocated_amount,
            60.0
        );
        assert_eq!(
            acc2.get_allocation(&"cpu-quota".to_string())
                .unwrap()
                .allocated_amount,
            40.0
        );
    }

    #[test]
    fn test_usage_summary() {
        let mut manager = setup_manager();

        let account = manager.get_account_mut(&"acc-1".to_string()).unwrap();
        account.record_usage(&QuotaResource::CpuHours, 50.0);

        let summary = manager.usage_summary(&"acc-1".to_string()).unwrap();
        assert_eq!(summary.account_id, "acc-1");

        let cpu_summary = summary.resources.get(&QuotaResource::CpuHours).unwrap();
        assert_eq!(cpu_summary.used, 50.0);
        assert_eq!(cpu_summary.limit, 100.0);
        assert_eq!(cpu_summary.percent_used, 50.0);
    }

    #[test]
    fn test_quotas_for_account() {
        let manager = setup_manager();

        let quotas = manager.quotas_for_account(&"acc-1".to_string());
        assert_eq!(quotas.len(), 1);
        assert_eq!(quotas[0].id, "cpu-quota");
    }

    #[test]
    fn test_snapshot_all() {
        let mut manager = setup_manager();

        let account = manager.get_account_mut(&"acc-1".to_string()).unwrap();
        account.record_usage(&QuotaResource::CpuHours, 50.0);

        manager.snapshot_all();

        let account = manager.get_account(&"acc-1".to_string()).unwrap();
        assert_eq!(account.usage_history.len(), 1);
    }

    #[test]
    fn test_reset_periods() {
        let mut manager = QuotaManager::new();

        let quota = Quota::new(
            "daily-quota",
            "Daily CPU Quota",
            QuotaResource::CpuHours,
            QuotaLimit::Hard(100.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        )
        .with_period(QuotaPeriod::Calendar(CalendarPeriod::Daily));

        manager.create_quota(quota).unwrap();

        let account = QuotaAccount::new("acc-1", "Test", "user@test.com");
        manager.create_account(account).unwrap();
        manager
            .allocate(
                &"daily-quota".to_string(),
                &"acc-1".to_string(),
                100.0,
                "admin",
            )
            .unwrap();

        // Record usage with old period start
        {
            let account = manager.get_account_mut(&"acc-1".to_string()).unwrap();
            account.record_usage(&QuotaResource::CpuHours, 50.0);

            // Set period start to yesterday
            let key = QuotaResource::CpuHours.key();
            if let Some(usage) = account.usage.get_mut(&key) {
                usage.period_start = Utc::now() - Duration::days(2);
            }
        }

        // Reset periods
        manager.reset_periods(Utc::now());

        // Usage should be reset
        let account = manager.get_account(&"acc-1".to_string()).unwrap();
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 0.0);
    }
}
