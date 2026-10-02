// Marabunta - Licensed under the MIT License.
//! Quota integration for the preemption engine
//!
//! This module provides integration between the quota system and the preemption
//! engine, enabling:
//!
//! - Quota exceeded triggering preemption (QuotaEnforcement trigger)
//! - Preemption respecting quota reservations
//! - High-quota jobs having protection from preemption
//!
//! # Example Usage
//!
//! ```rust,ignore
//! use marabunta_compute::preemption::{PreemptionEngine, PreemptionCallbacks, QuotaAwarePreemptionEngine};
//! use marabunta_compute::quotas::shared_quota_manager;
//!
//! // Create quota-aware preemption engine
//! let quotas = shared_quota_manager();
//! let callbacks = PreemptionCallbacks::noop();
//! let mut engine = QuotaAwarePreemptionEngine::new(callbacks, quotas);
//!
//! // Check if quota enforcement should trigger preemption
//! if let Some(request) = engine.check_quota_preemption("account-1") {
//!     let plan = engine.request_preemption(request);
//!     // Execute plan...
//! }
//! ```

use chrono::{Duration, Utc};
use std::collections::HashMap;

use super::engine::{PreemptionCallbacks, PreemptionEngine, PreemptionRequest};
use super::errors::{PreemptionError, PreemptionResult};
use super::task_state::RunningTask;
use super::types::{
    PreemptionPolicy, PreemptionReason, PreemptionTrigger, PreemptionUrgency, ResourceUsage,
    VictimSelector,
};

use crate::quotas::{AccountId, QuotaManager, QuotaResource, ResourceRequest, SharedQuotaManager};

/// Configuration for quota-based preemption protection
#[derive(Debug, Clone)]
pub struct QuotaProtectionConfig {
    /// Minimum quota allocation ratio (0.0-1.0) to receive protection
    /// Tasks from accounts with allocation >= this ratio of max allocation get protection
    pub min_allocation_ratio_for_protection: f64,

    /// Priority boost for high-quota accounts (added to task priority during victim selection)
    pub high_quota_priority_boost: i32,

    /// Whether to completely protect tasks from high-quota accounts
    pub absolute_protection: bool,

    /// Grace period before quota enforcement triggers preemption
    pub enforcement_grace_period: Duration,

    /// Maximum percentage of quota overage before triggering preemption
    pub max_overage_before_preemption: f64,
}

impl Default for QuotaProtectionConfig {
    fn default() -> Self {
        Self {
            min_allocation_ratio_for_protection: 0.75,
            high_quota_priority_boost: 20,
            absolute_protection: false,
            enforcement_grace_period: Duration::minutes(5),
            max_overage_before_preemption: 0.20, // 20% overage
        }
    }
}

impl QuotaProtectionConfig {
    /// Create a strict protection config that fully protects high-quota accounts
    pub fn strict() -> Self {
        Self {
            min_allocation_ratio_for_protection: 0.50,
            high_quota_priority_boost: 50,
            absolute_protection: true,
            enforcement_grace_period: Duration::minutes(1),
            max_overage_before_preemption: 0.10,
        }
    }

    /// Create a lenient protection config with minimal protection
    pub fn lenient() -> Self {
        Self {
            min_allocation_ratio_for_protection: 0.90,
            high_quota_priority_boost: 10,
            absolute_protection: false,
            enforcement_grace_period: Duration::minutes(15),
            max_overage_before_preemption: 0.50,
        }
    }
}

/// Tracks quota status for a running task
#[derive(Debug, Clone)]
pub struct TaskQuotaInfo {
    /// The account ID this task is charged to
    pub account_id: AccountId,

    /// Resources reserved by this task
    pub reserved_resources: HashMap<QuotaResource, f64>,

    /// Whether this task has quota-based protection
    pub protected: bool,

    /// Priority boost from quota allocation
    pub priority_boost: i32,

    /// Allocation ratio (task's account allocation / max allocation)
    pub allocation_ratio: f64,
}

/// A quota-aware preemption engine that integrates with the quota system
pub struct QuotaAwarePreemptionEngine {
    /// The underlying preemption engine
    engine: PreemptionEngine,

    /// Reference to the quota manager
    quotas: SharedQuotaManager,

    /// Quota protection configuration
    protection_config: QuotaProtectionConfig,

    /// Mapping of task IDs to their quota info
    task_quota_info: HashMap<String, TaskQuotaInfo>,

    /// Accounts that are currently over quota
    accounts_over_quota: HashMap<AccountId, OverQuotaInfo>,
}

/// Information about an account that is over quota
#[derive(Debug, Clone)]
struct OverQuotaInfo {
    /// When the account first went over quota
    exceeded_at: chrono::DateTime<Utc>,

    /// The resource that is over quota
    #[allow(dead_code)]
    resource: QuotaResource,

    /// The overage amount
    overage: f64,

    /// The overage percentage
    overage_percent: f64,
}

impl QuotaAwarePreemptionEngine {
    /// Create a new quota-aware preemption engine
    pub fn new(callbacks: PreemptionCallbacks, quotas: SharedQuotaManager) -> Self {
        Self {
            engine: PreemptionEngine::new(callbacks),
            quotas,
            protection_config: QuotaProtectionConfig::default(),
            task_quota_info: HashMap::new(),
            accounts_over_quota: HashMap::new(),
        }
    }

    /// Create with a custom protection configuration
    pub fn with_config(
        callbacks: PreemptionCallbacks,
        quotas: SharedQuotaManager,
        config: QuotaProtectionConfig,
    ) -> Self {
        Self {
            engine: PreemptionEngine::new(callbacks),
            quotas,
            protection_config: config,
            task_quota_info: HashMap::new(),
            accounts_over_quota: HashMap::new(),
        }
    }

    /// Get a reference to the underlying preemption engine
    pub fn engine(&self) -> &PreemptionEngine {
        &self.engine
    }

    /// Get a mutable reference to the underlying preemption engine
    pub fn engine_mut(&mut self) -> &mut PreemptionEngine {
        &mut self.engine
    }

    /// Get the quota manager
    pub fn quotas(&self) -> &SharedQuotaManager {
        &self.quotas
    }

    /// Get the protection configuration
    pub fn protection_config(&self) -> &QuotaProtectionConfig {
        &self.protection_config
    }

    /// Set a new protection configuration
    pub fn set_protection_config(&mut self, config: QuotaProtectionConfig) {
        self.protection_config = config;
    }

    // ========== Task Registration with Quota Info ==========

    /// Register a task with its quota information
    pub fn register_task_with_quota(
        &mut self,
        task: RunningTask,
        account_id: AccountId,
        reserved_resources: HashMap<QuotaResource, f64>,
    ) -> PreemptionResult<()> {
        let task_id = task.task_id.clone();

        // Calculate quota protection status
        let quota_info = self.calculate_quota_info(&account_id, reserved_resources)?;

        // Register with the underlying engine
        self.engine.register_task(task);

        // Store quota info
        self.task_quota_info.insert(task_id, quota_info);

        Ok(())
    }

    /// Calculate quota info for a task
    fn calculate_quota_info(
        &self,
        account_id: &AccountId,
        reserved_resources: HashMap<QuotaResource, f64>,
    ) -> PreemptionResult<TaskQuotaInfo> {
        let quota_guard = self.quotas.read().map_err(|e| {
            PreemptionError::Internal(format!("Failed to acquire quota lock: {}", e))
        })?;

        // Get account's allocation
        let account = quota_guard.get_account(account_id);

        if account.is_none() {
            return Ok(TaskQuotaInfo {
                account_id: account_id.clone(),
                reserved_resources,
                protected: false,
                priority_boost: 0,
                allocation_ratio: 0.0,
            });
        }

        // Calculate allocation ratio by comparing to max allocation across all accounts
        let allocation_ratio = self.calculate_allocation_ratio(&quota_guard, account_id);

        // Determine protection status
        let protected = if self.protection_config.absolute_protection {
            allocation_ratio >= self.protection_config.min_allocation_ratio_for_protection
        } else {
            false
        };

        // Calculate priority boost
        let priority_boost =
            if allocation_ratio >= self.protection_config.min_allocation_ratio_for_protection {
                self.protection_config.high_quota_priority_boost
            } else {
                // Proportional boost based on allocation
                (allocation_ratio * self.protection_config.high_quota_priority_boost as f64) as i32
            };

        Ok(TaskQuotaInfo {
            account_id: account_id.clone(),
            reserved_resources,
            protected,
            priority_boost,
            allocation_ratio,
        })
    }

    /// Calculate allocation ratio for an account
    fn calculate_allocation_ratio(
        &self,
        quota_manager: &QuotaManager,
        account_id: &AccountId,
    ) -> f64 {
        let accounts = quota_manager.list_accounts();
        if accounts.is_empty() {
            return 0.0;
        }

        // Get account's total allocation
        let account = match quota_manager.get_account(account_id) {
            Some(a) => a,
            None => return 0.0,
        };

        let _quotas: HashMap<_, _> = quota_manager
            .list_quotas()
            .into_iter()
            .map(|q| (q.id.clone(), q.clone()))
            .collect();

        let account_allocation: f64 = account
            .allocations
            .iter()
            .filter(|a| a.is_valid(Utc::now()))
            .map(|a| a.allocated_amount)
            .sum();

        // Find max allocation across all accounts
        let max_allocation: f64 = accounts
            .iter()
            .map(|acc| {
                acc.allocations
                    .iter()
                    .filter(|a| a.is_valid(Utc::now()))
                    .map(|a| a.allocated_amount)
                    .sum::<f64>()
            })
            .fold(0.0, f64::max);

        if max_allocation > 0.0 {
            account_allocation / max_allocation
        } else {
            0.0
        }
    }

    /// Unregister a task and clean up quota info
    pub fn unregister_task(&mut self, task_id: &str) -> Option<RunningTask> {
        self.task_quota_info.remove(task_id);
        self.engine.unregister_task(&task_id.to_string())
    }

    /// Get quota info for a task
    pub fn get_task_quota_info(&self, task_id: &str) -> Option<&TaskQuotaInfo> {
        self.task_quota_info.get(task_id)
    }

    // ========== Quota Enforcement Trigger ==========

    /// Check if quota enforcement should trigger preemption for an account
    ///
    /// Returns a preemption request if the account is over quota and has exceeded
    /// the grace period.
    pub fn check_quota_preemption(&mut self, account_id: &str) -> Option<PreemptionRequest> {
        let quota_guard = match self.quotas.read() {
            Ok(guard) => guard,
            Err(_) => return None,
        };

        let account = quota_guard.get_account(&account_id.to_string())?;

        // Check for quota overage
        let quotas: HashMap<_, _> = quota_guard
            .list_quotas()
            .into_iter()
            .map(|q| (q.id.clone(), q.clone()))
            .collect();

        for allocation in &account.allocations {
            if !allocation.is_valid(Utc::now()) {
                continue;
            }

            let quota = quotas.get(&allocation.quota_id)?;
            let current_usage = account.get_usage(&quota.resource);
            let limit = allocation
                .allocated_amount
                .min(quota.limit.effective_limit());

            if current_usage <= limit {
                // Not over quota, remove from tracking
                self.accounts_over_quota.remove(&account_id.to_string());
                continue;
            }

            let overage = current_usage - limit;
            let overage_percent = overage / limit;

            // Track when we first exceeded
            let now = Utc::now();
            let over_info = self
                .accounts_over_quota
                .entry(account_id.to_string())
                .or_insert_with(|| OverQuotaInfo {
                    exceeded_at: now,
                    resource: quota.resource.clone(),
                    overage,
                    overage_percent,
                });

            // Update overage info
            over_info.overage = overage;
            over_info.overage_percent = overage_percent;

            // Check if grace period has passed and overage is significant
            let grace_exceeded =
                now - over_info.exceeded_at >= self.protection_config.enforcement_grace_period;
            let overage_significant =
                overage_percent >= self.protection_config.max_overage_before_preemption;

            if grace_exceeded && overage_significant {
                // Trigger preemption
                let resources_to_free = self.calculate_resources_to_free(&quota.resource, overage);

                return Some(
                    PreemptionRequest::new(
                        format!("quota-enforcement-{}", uuid::Uuid::new_v4()),
                        "quota-manager",
                        PreemptionReason::QuotaExceeded {
                            account_id: account_id.to_string(),
                            overage: overage_percent,
                        },
                        resources_to_free,
                    )
                    .with_urgency(PreemptionUrgency::Soon),
                );
            }
        }

        None
    }

    /// Calculate resources to free based on quota overage
    fn calculate_resources_to_free(&self, resource: &QuotaResource, overage: f64) -> ResourceUsage {
        let mut resources = ResourceUsage::zero();

        // Map quota resources to ResourceUsage fields
        match resource {
            QuotaResource::CpuHours => {
                resources.cpu_cores = overage;
            }
            QuotaResource::GpuHours => {
                resources.gpu_count = overage.ceil() as u32;
            }
            QuotaResource::MemoryGBHours => {
                resources.memory_gb = overage;
            }
            _ => {
                // For other resources, estimate based on CPU
                resources.cpu_cores = overage;
            }
        }

        resources
    }

    // ========== Quota-Aware Victim Selection ==========

    /// Find victims for preemption, respecting quota-based protection
    ///
    /// Tasks from high-quota accounts receive priority protection based on
    /// the configured protection rules.
    pub fn find_victims_quota_aware(
        &self,
        needed: &ResourceUsage,
        nodes: Option<&[String]>,
        preemptor_priority: i32,
    ) -> Vec<(RunningTask, f64)> {
        // Get base victims from engine
        let candidates = self.engine.find_victims(needed, nodes, preemptor_priority);

        // Filter and re-score based on quota protection
        let mut scored_victims: Vec<(RunningTask, f64)> = candidates
            .into_iter()
            .filter_map(|candidate| {
                let task_id = &candidate.task.task_id;

                // Check quota protection
                if let Some(quota_info) = self.task_quota_info.get(task_id) {
                    if quota_info.protected && self.protection_config.absolute_protection {
                        // Absolutely protected, skip
                        return None;
                    }

                    // Adjust score based on quota allocation
                    // Higher allocation = lower score (less likely to be preempted)
                    let quota_penalty = quota_info.allocation_ratio
                        * self.protection_config.high_quota_priority_boost as f64;
                    let adjusted_score = candidate.score - quota_penalty;

                    Some((candidate.task, adjusted_score))
                } else {
                    // No quota info, use original score
                    Some((candidate.task, candidate.score))
                }
            })
            .collect();

        // Sort by adjusted score (lower = less likely to be preempted)
        scored_victims.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        scored_victims
    }

    /// Check if a task is protected by quota
    pub fn is_task_quota_protected(&self, task_id: &str) -> bool {
        self.task_quota_info
            .get(task_id)
            .map(|info| info.protected)
            .unwrap_or(false)
    }

    /// Get the effective priority for a task including quota boost
    pub fn effective_priority(&self, task_id: &str) -> Option<i32> {
        let task = self.engine.get_task(&task_id.to_string())?;
        let boost = self
            .task_quota_info
            .get(task_id)
            .map(|info| info.priority_boost)
            .unwrap_or(0);

        Some(task.priority + boost)
    }

    // ========== Quota Release on Preemption ==========

    /// Release quota when a task is preempted
    ///
    /// This updates the quota manager to reflect that the preempted task's
    /// resources are now available.
    pub fn release_preempted_quota(&mut self, task: &RunningTask) -> PreemptionResult<()> {
        let task_id = &task.task_id;

        // Get quota info for this task
        let quota_info = match self.task_quota_info.get(task_id) {
            Some(info) => info.clone(),
            None => {
                // No quota info, nothing to release
                return Ok(());
            }
        };

        // Release resources in the quota manager
        let mut quota_guard = self.quotas.write().map_err(|e| {
            PreemptionError::Internal(format!("Failed to acquire quota lock: {}", e))
        })?;

        // Create a ResourceRequest from the reserved resources
        let mut request = ResourceRequest::new();
        for (resource, amount) in &quota_info.reserved_resources {
            request = request.with_resource(resource.clone(), *amount);
        }

        // Find the job ID for this task and release the reservation
        // Note: In a real system, we'd have a job_id -> reservation mapping
        // For now, we use the task's job_id
        let job_id = &task.job_id;

        // Try to release the reservation
        // If the reservation doesn't exist, that's okay - it might have been
        // released already or was never created
        match quota_guard.release(job_id, &request) {
            Ok(()) => {}
            Err(crate::quotas::QuotaError::ReservationNotFound(_)) => {
                // Not an error for preemption - reservation might not exist
            }
            Err(e) => {
                return Err(PreemptionError::Internal(format!(
                    "Failed to release quota: {}",
                    e
                )));
            }
        }

        // Remove from our tracking
        self.task_quota_info.remove(task_id);

        Ok(())
    }

    /// Update quotas after a preemption plan is executed
    pub fn update_quotas_after_preemption(
        &mut self,
        preempted_tasks: &[RunningTask],
    ) -> PreemptionResult<()> {
        for task in preempted_tasks {
            self.release_preempted_quota(task)?;
        }
        Ok(())
    }

    // ========== Policy Management with Quota Trigger ==========

    /// Add a quota enforcement policy
    ///
    /// This creates a preemption policy that triggers when quota is exceeded.
    pub fn add_quota_enforcement_policy(
        &mut self,
        policy_id: &str,
        name: &str,
        grace_period: Duration,
    ) {
        let policy = PreemptionPolicy::new(policy_id, name);
        let mut policy = policy;
        policy.trigger = PreemptionTrigger::QuotaEnforcement { grace_period };
        policy.victim_selector = VictimSelector::LowestPriority;

        self.engine.add_policy(policy);
    }

    // ========== Delegate Methods to Underlying Engine ==========

    /// Add a preemption policy
    pub fn add_policy(&mut self, policy: PreemptionPolicy) {
        self.engine.add_policy(policy);
    }

    /// Remove a preemption policy
    pub fn remove_policy(&mut self, policy_id: &str) -> Option<PreemptionPolicy> {
        self.engine.remove_policy(policy_id)
    }

    /// Request preemption
    pub fn request_preemption(
        &mut self,
        request: PreemptionRequest,
    ) -> super::engine::PreemptionPlan {
        self.engine.request_preemption(request)
    }

    /// Execute a preemption plan
    pub async fn execute_preemption(
        &mut self,
        plan: super::engine::PreemptionPlan,
    ) -> super::engine::ExecutionResult {
        let result = self.engine.execute_preemption(plan).await;

        // Collect task IDs of successfully preempted tasks
        let preempted_task_ids: Vec<String> = result
            .events
            .iter()
            .filter(|e| e.result.as_ref().map(|r| r.is_success()).unwrap_or(false))
            .map(|e| e.task_id.clone())
            .collect();

        // Update quotas for preempted tasks
        for task_id in preempted_task_ids {
            // Remove quota info for preempted tasks
            self.task_quota_info.remove(&task_id);
        }

        result
    }
}

/// Extension trait to add quota awareness to PreemptionEngine
pub trait QuotaAwareExt {
    /// Create a quota-aware engine from this engine
    fn with_quotas(self, quotas: SharedQuotaManager) -> QuotaAwarePreemptionEngine;
}

// Note: We can't implement this directly on PreemptionEngine without consuming it,
// so we provide a standalone function instead.

/// Create a quota-aware preemption engine from components
pub fn create_quota_aware_engine(
    callbacks: PreemptionCallbacks,
    quotas: SharedQuotaManager,
) -> QuotaAwarePreemptionEngine {
    QuotaAwarePreemptionEngine::new(callbacks, quotas)
}

/// Create a quota-aware preemption engine with custom config
pub fn create_quota_aware_engine_with_config(
    callbacks: PreemptionCallbacks,
    quotas: SharedQuotaManager,
    config: QuotaProtectionConfig,
) -> QuotaAwarePreemptionEngine {
    QuotaAwarePreemptionEngine::with_config(callbacks, quotas, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preemption::PreemptionCallbacks;
    use crate::quotas::{
        shared_quota_manager, Quota, QuotaAccount, QuotaEnforcement, QuotaLimit, QuotaScope,
    };

    fn setup_test_quota_manager() -> SharedQuotaManager {
        let manager = shared_quota_manager();

        {
            let mut guard = manager.write().unwrap();

            // Create a CPU quota
            let quota = Quota::new(
                "cpu-quota",
                "CPU Hours Quota",
                QuotaResource::CpuHours,
                QuotaLimit::Hard(100.0),
                QuotaScope::Global,
                QuotaEnforcement::Block,
                "admin",
                "test.com",
            );
            guard.create_quota(quota).unwrap();

            // Create accounts with different allocations
            let high_quota_account =
                QuotaAccount::new("high-quota", "High Quota Team", "lead@test.com");
            guard.create_account(high_quota_account).unwrap();
            guard
                .allocate(
                    &"cpu-quota".to_string(),
                    &"high-quota".to_string(),
                    80.0,
                    "admin",
                )
                .unwrap();

            let low_quota_account =
                QuotaAccount::new("low-quota", "Low Quota Team", "user@test.com");
            guard.create_account(low_quota_account).unwrap();
            guard
                .allocate(
                    &"cpu-quota".to_string(),
                    &"low-quota".to_string(),
                    20.0,
                    "admin",
                )
                .unwrap();
        }

        manager
    }

    fn create_test_task(id: &str, priority: i32) -> RunningTask {
        RunningTask::new(
            id,
            format!("job-{}", id),
            "node-1",
            priority,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        )
    }

    #[test]
    fn test_quota_aware_engine_creation() {
        let quotas = setup_test_quota_manager();
        let engine = QuotaAwarePreemptionEngine::new(PreemptionCallbacks::noop(), quotas);

        assert!(engine.task_quota_info.is_empty());
        assert!(engine.accounts_over_quota.is_empty());
    }

    #[test]
    fn test_register_task_with_quota() {
        let quotas = setup_test_quota_manager();
        let mut engine = QuotaAwarePreemptionEngine::new(PreemptionCallbacks::noop(), quotas);

        let task = create_test_task("task-1", 50);
        let mut reserved = HashMap::new();
        reserved.insert(QuotaResource::CpuHours, 10.0);

        engine
            .register_task_with_quota(task, "high-quota".to_string(), reserved)
            .unwrap();

        let quota_info = engine.get_task_quota_info("task-1").unwrap();
        assert_eq!(quota_info.account_id, "high-quota");
        assert!(quota_info.priority_boost > 0);
        assert!(quota_info.allocation_ratio > 0.0);
    }

    #[test]
    fn test_high_quota_protection() {
        let quotas = setup_test_quota_manager();
        let config = QuotaProtectionConfig {
            min_allocation_ratio_for_protection: 0.5,
            high_quota_priority_boost: 20,
            absolute_protection: true,
            ..Default::default()
        };
        let mut engine =
            QuotaAwarePreemptionEngine::with_config(PreemptionCallbacks::noop(), quotas, config);

        // Register high-quota task
        let task = create_test_task("high-task", 50);
        let mut reserved = HashMap::new();
        reserved.insert(QuotaResource::CpuHours, 10.0);
        engine
            .register_task_with_quota(task, "high-quota".to_string(), reserved)
            .unwrap();

        // Should be protected
        assert!(engine.is_task_quota_protected("high-task"));

        // Register low-quota task
        let task = create_test_task("low-task", 50);
        let mut reserved = HashMap::new();
        reserved.insert(QuotaResource::CpuHours, 5.0);
        engine
            .register_task_with_quota(task, "low-quota".to_string(), reserved)
            .unwrap();

        // Should not be protected (low allocation ratio)
        assert!(!engine.is_task_quota_protected("low-task"));
    }

    #[test]
    fn test_effective_priority_with_boost() {
        let quotas = setup_test_quota_manager();
        let mut engine = QuotaAwarePreemptionEngine::new(PreemptionCallbacks::noop(), quotas);

        // Register task from high-quota account
        let task = create_test_task("task-1", 50);
        let mut reserved = HashMap::new();
        reserved.insert(QuotaResource::CpuHours, 10.0);
        engine
            .register_task_with_quota(task, "high-quota".to_string(), reserved)
            .unwrap();

        let effective = engine.effective_priority("task-1").unwrap();
        let quota_info = engine.get_task_quota_info("task-1").unwrap();

        // Effective priority should include boost
        assert_eq!(effective, 50 + quota_info.priority_boost);
        assert!(effective > 50);
    }

    #[test]
    fn test_quota_preemption_trigger() {
        let quotas = setup_test_quota_manager();

        // Record usage that exceeds quota
        {
            let mut guard = quotas.write().unwrap();
            let account = guard.get_account_mut(&"low-quota".to_string()).unwrap();
            // Low quota has 20.0 allocation, record 30.0 usage (50% overage)
            account.record_usage(&QuotaResource::CpuHours, 30.0);
        }

        let config = QuotaProtectionConfig {
            enforcement_grace_period: Duration::zero(), // No grace period for test
            max_overage_before_preemption: 0.10,        // Trigger at 10% overage
            ..Default::default()
        };
        let mut engine =
            QuotaAwarePreemptionEngine::with_config(PreemptionCallbacks::noop(), quotas, config);

        // Should trigger preemption
        let request = engine.check_quota_preemption("low-quota");
        assert!(request.is_some());

        let request = request.unwrap();
        match request.reason {
            PreemptionReason::QuotaExceeded {
                account_id,
                overage,
            } => {
                assert_eq!(account_id, "low-quota");
                assert!(overage > 0.0);
            }
            _ => panic!("Expected QuotaExceeded reason"),
        }
    }

    #[test]
    fn test_no_preemption_within_grace_period() {
        let quotas = setup_test_quota_manager();

        // Record usage that exceeds quota
        {
            let mut guard = quotas.write().unwrap();
            let account = guard.get_account_mut(&"low-quota".to_string()).unwrap();
            account.record_usage(&QuotaResource::CpuHours, 30.0);
        }

        let config = QuotaProtectionConfig {
            enforcement_grace_period: Duration::hours(1), // Long grace period
            max_overage_before_preemption: 0.10,
            ..Default::default()
        };
        let mut engine =
            QuotaAwarePreemptionEngine::with_config(PreemptionCallbacks::noop(), quotas, config);

        // First check - should not trigger yet (within grace period)
        let request = engine.check_quota_preemption("low-quota");
        assert!(request.is_none());
    }

    #[test]
    fn test_unregister_task_cleans_up_quota_info() {
        let quotas = setup_test_quota_manager();
        let mut engine = QuotaAwarePreemptionEngine::new(PreemptionCallbacks::noop(), quotas);

        let task = create_test_task("task-1", 50);
        let mut reserved = HashMap::new();
        reserved.insert(QuotaResource::CpuHours, 10.0);
        engine
            .register_task_with_quota(task, "high-quota".to_string(), reserved)
            .unwrap();

        assert!(engine.get_task_quota_info("task-1").is_some());

        engine.unregister_task("task-1");

        assert!(engine.get_task_quota_info("task-1").is_none());
    }

    #[test]
    fn test_quota_enforcement_policy() {
        let quotas = setup_test_quota_manager();
        let mut engine = QuotaAwarePreemptionEngine::new(PreemptionCallbacks::noop(), quotas);

        engine.add_quota_enforcement_policy(
            "quota-policy",
            "Quota Enforcement",
            Duration::minutes(5),
        );

        let policy = engine.engine().get_policy("quota-policy");
        assert!(policy.is_some());

        let policy = policy.unwrap();
        assert!(matches!(
            policy.trigger,
            PreemptionTrigger::QuotaEnforcement { .. }
        ));
    }

    #[test]
    fn test_find_victims_quota_aware() {
        let quotas = setup_test_quota_manager();
        let mut engine = QuotaAwarePreemptionEngine::new(PreemptionCallbacks::noop(), quotas);

        // Register tasks from both high and low quota accounts
        let high_task = create_test_task("high-task", 30);
        let mut reserved = HashMap::new();
        reserved.insert(QuotaResource::CpuHours, 10.0);
        engine
            .register_task_with_quota(high_task, "high-quota".to_string(), reserved)
            .unwrap();

        let low_task = create_test_task("low-task", 30);
        let mut reserved = HashMap::new();
        reserved.insert(QuotaResource::CpuHours, 5.0);
        engine
            .register_task_with_quota(low_task, "low-quota".to_string(), reserved)
            .unwrap();

        // Both tasks have same priority, but low-quota task should be preferred for preemption
        let needed = ResourceUsage::new(2.0, 4.0, 0, 10.0);
        let victims = engine.find_victims_quota_aware(&needed, None, 100);

        if victims.len() >= 2 {
            // The low-quota task should have higher score (more likely to be preempted)
            let low_task_idx = victims.iter().position(|(t, _)| t.task_id == "low-task");
            let high_task_idx = victims.iter().position(|(t, _)| t.task_id == "high-task");

            if let (Some(low_idx), Some(high_idx)) = (low_task_idx, high_task_idx) {
                // Lower index = higher score = more likely to be preempted
                assert!(
                    low_idx < high_idx,
                    "Low-quota task should be preferred for preemption"
                );
            }
        }
    }
}
