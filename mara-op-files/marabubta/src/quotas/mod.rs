// Marabunta - Licensed under the MIT License.
//! Quotas and Resource Budgets system for the marabunta-compute job placement system
//!
//! This module provides comprehensive quota management for controlling and
//! tracking resource usage across users, projects, and the entire system.
//!
//! # Architecture
//!
//! The quota system is built around several key concepts:
//!
//! - **Quotas**: Define limits on specific resources (CPU hours, GPU hours, etc.)
//! - **Accounts**: Group users and track their resource usage
//! - **Allocations**: Assign quota budgets to specific accounts
//! - **Reservations**: Hold quota for pending jobs
//! - **Fair Share**: Ensure equitable resource distribution
//!
//! # Usage Example
//!
//! ```rust
//! use marabunta_compute::quotas::{
//!     QuotaManager, Quota, QuotaAccount, QuotaResource, QuotaLimit,
//!     QuotaScope, QuotaEnforcement, ResourceRequest,
//! };
//!
//! // Create a quota manager
//! let mut manager = QuotaManager::new();
//!
//! // Define a CPU hours quota
//! let quota = Quota::new(
//!     "cpu-monthly",
//!     "Monthly CPU Hours",
//!     QuotaResource::CpuHours,
//!     QuotaLimit::Hard(1000.0),
//!     QuotaScope::Global,
//!     QuotaEnforcement::Block,
//!     "admin@example.com",
//!     "example.com",
//! );
//! manager.create_quota(quota).unwrap();
//!
//! // Create an account
//! let account = QuotaAccount::new("team-alpha", "Team Alpha", "lead@example.com");
//! manager.create_account(account).unwrap();
//!
//! // Allocate quota to the account
//! manager.allocate(
//!     &"cpu-monthly".to_string(),
//!     &"team-alpha".to_string(),
//!     500.0,
//!     "admin@example.com",
//! ).unwrap();
//!
//! // Check if a job can be submitted
//! let request = ResourceRequest::new()
//!     .with_resource(QuotaResource::CpuHours, 50.0);
//! let result = manager.check_submission("lead@example.com", &request);
//! assert!(result.allowed);
//! ```
//!
//! # Quota Types
//!
//! The system supports several types of quota limits:
//!
//! - **Hard limits**: Cannot be exceeded under any circumstances
//! - **Soft limits**: Can be exceeded with penalties (lower priority, higher cost)
//! - **Burst limits**: Can exceed temporarily with cooldown periods
//! - **Tiered limits**: Different rates/priorities at different usage levels
//!
//! # Fair Share Scheduling
//!
//! The fair share calculator ensures equitable resource distribution:
//!
//! ```rust
//! use marabunta_compute::quotas::{FairShareCalculator, FairShareConfig, QuotaResource};
//! use std::collections::HashMap;
//! use chrono::Duration;
//!
//! let config = FairShareConfig::default()
//!     .with_history_window(Duration::days(7))
//!     .with_decay_halflife(Duration::days(1));
//!
//! let mut calc = FairShareCalculator::new(config);
//!
//! // Record usage
//! let mut usage = HashMap::new();
//! usage.insert(QuotaResource::CpuHours, 100.0);
//! calc.record_usage(&"team-alpha".to_string(), &usage);
//!
//! // Get priority modifier based on fair share
//! let modifier = calc.calculate_priority_modifier(&"team-alpha".to_string(), &usage);
//! ```
//!
//! # Thread Safety
//!
//! The `QuotaManager` is not thread-safe by default. For concurrent access,
//! use the `SharedQuotaManager` type alias which wraps it in `Arc<RwLock<>>`:
//!
//! ```rust
//! use marabunta_compute::quotas::shared_quota_manager;
//!
//! let manager = shared_quota_manager();
//!
//! // Read access
//! {
//!     let guard = manager.read().unwrap();
//!     let quotas = guard.list_quotas();
//! }
//!
//! // Write access
//! {
//!     let mut guard = manager.write().unwrap();
//!     // ... modify manager
//! }
//! ```

pub mod account;
pub mod errors;
pub mod fairshare;
pub mod manager;
pub mod persistence;
pub mod types;

// Re-export main types for convenience
pub use account::{
    AffordabilityResult, QuotaAccount, QuotaAllocation, ResourceRequest, ResourceUsage,
    UsageSnapshot,
};
pub use errors::{QuotaError, QuotaResult};
pub use fairshare::{
    FairShareCalculator, FairShareConfig, FairShareSchedulerIntegration, FairShareStatistics,
    TargetShareSource,
};
pub use manager::{
    shared_quota_manager, QuotaCheckDetail, QuotaCheckResult, QuotaManager, QuotaReservation,
    QuotaStatus, QuotaWarning, ResourceSummary, SharedQuotaManager, UsageSummary, UsageTrend,
};
pub use persistence::QuotasPersistence;
pub use types::{
    AccountId, BurstState, CalendarPeriod, OveragePenalty, Quota, QuotaEnforcement, QuotaId,
    QuotaLimit, QuotaPeriod, QuotaResource, QuotaScope, QuotaTier,
};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use chrono::{Duration, Utc};
    use std::collections::HashMap;

    /// Test the complete workflow from quota creation to job completion
    #[test]
    fn test_complete_workflow() {
        let mut manager = QuotaManager::new();

        // 1. Create quotas
        let cpu_quota = Quota::new(
            "cpu-quota",
            "CPU Hours Quota",
            QuotaResource::CpuHours,
            QuotaLimit::Hard(1000.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        );
        manager.create_quota(cpu_quota).unwrap();

        let gpu_quota = Quota::new(
            "gpu-quota",
            "GPU Hours Quota",
            QuotaResource::GpuHours,
            QuotaLimit::Soft {
                limit: 100.0,
                overage_allowed: 20.0,
                overage_penalty: OveragePenalty::LowerPriority { by: 10 },
            },
            QuotaScope::Global,
            QuotaEnforcement::Deprioritize {
                priority_penalty: 10,
            },
            "admin",
            "test.com",
        );
        manager.create_quota(gpu_quota).unwrap();

        // 2. Create accounts
        let mut account = QuotaAccount::new("team-alpha", "Team Alpha", "lead@test.com");
        account.add_member("dev1@test.com");
        account.add_member("dev2@test.com");
        manager.create_account(account).unwrap();

        // 3. Allocate quotas
        manager
            .allocate(
                &"cpu-quota".to_string(),
                &"team-alpha".to_string(),
                500.0,
                "admin",
            )
            .unwrap();
        manager
            .allocate(
                &"gpu-quota".to_string(),
                &"team-alpha".to_string(),
                100.0,
                "admin",
            )
            .unwrap();

        // 4. Check submission (should succeed)
        let request = ResourceRequest::new()
            .with_resource(QuotaResource::CpuHours, 50.0)
            .with_resource(QuotaResource::GpuHours, 10.0);
        let result = manager.check_submission("dev1@test.com", &request);
        assert!(result.allowed);

        // 5. Reserve quota for job
        let reservation = manager
            .reserve("job-001", &"team-alpha".to_string(), &request)
            .unwrap();
        assert_eq!(reservation.job_id, "job-001");

        // 6. Consume reservation when job starts
        manager.consume("job-001").unwrap();

        // 7. Release when job completes
        let actual_usage = ResourceRequest::new()
            .with_resource(QuotaResource::CpuHours, 45.0)
            .with_resource(QuotaResource::GpuHours, 8.0);
        manager.release("job-001", &actual_usage).unwrap();

        // 8. Verify usage was tracked
        let _summary = manager.usage_summary(&"team-alpha".to_string()).unwrap();
        // After release, concurrent usage should be back to 0
        // but we recorded the actual_usage difference
    }

    /// Test quota exceeded scenarios
    #[test]
    fn test_quota_exceeded() {
        let mut manager = QuotaManager::new();

        let quota = Quota::new(
            "cpu-quota",
            "CPU Hours",
            QuotaResource::CpuHours,
            QuotaLimit::Hard(100.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        );
        manager.create_quota(quota).unwrap();

        let account = QuotaAccount::new("team", "Team", "user@test.com");
        manager.create_account(account).unwrap();
        manager
            .allocate(
                &"cpu-quota".to_string(),
                &"team".to_string(),
                100.0,
                "admin",
            )
            .unwrap();

        // First request takes 80
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 80.0);
        manager
            .reserve("job-1", &"team".to_string(), &request)
            .unwrap();

        // Second request for 30 should fail (80 + 30 > 100)
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 30.0);
        let result = manager.check_submission("user@test.com", &request);
        assert!(!result.allowed);
    }

    /// Test soft limit with overage
    #[test]
    fn test_soft_limit_overage() {
        let mut manager = QuotaManager::new();

        let quota = Quota::new(
            "cpu-soft",
            "Soft CPU Quota",
            QuotaResource::CpuHours,
            QuotaLimit::Soft {
                limit: 80.0,
                overage_allowed: 20.0,
                overage_penalty: OveragePenalty::Combined(vec![
                    OveragePenalty::LowerPriority { by: 5 },
                    OveragePenalty::HigherCost { multiplier: 1.5 },
                ]),
            },
            QuotaScope::Global,
            QuotaEnforcement::Deprioritize {
                priority_penalty: 5,
            },
            "admin",
            "test.com",
        );
        manager.create_quota(quota).unwrap();

        let account = QuotaAccount::new("team", "Team", "user@test.com");
        manager.create_account(account).unwrap();
        manager
            .allocate(&"cpu-soft".to_string(), &"team".to_string(), 100.0, "admin")
            .unwrap();

        // Use 75 hours
        manager
            .get_account_mut(&"team".to_string())
            .unwrap()
            .record_usage(&QuotaResource::CpuHours, 75.0);

        // Request 10 more (would exceed soft limit but within overage)
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 10.0);
        let result = manager.check_submission("user@test.com", &request);

        assert!(result.allowed);
        assert!(result
            .warnings
            .iter()
            .any(|w| w.code == "SOFT_LIMIT_EXCEEDED"));
    }

    /// Test fair share integration
    #[test]
    fn test_fair_share_integration() {
        let config = FairShareConfig::default();
        let mut calc = FairShareCalculator::new(config);

        // Team Alpha uses a lot
        let mut heavy_usage = HashMap::new();
        heavy_usage.insert(QuotaResource::CpuHours, 500.0);
        calc.record_usage(&"team-alpha".to_string(), &heavy_usage);

        // Team Beta uses little
        let mut light_usage = HashMap::new();
        light_usage.insert(QuotaResource::CpuHours, 50.0);
        calc.record_usage(&"team-beta".to_string(), &light_usage);

        // Team Beta should be prioritized
        let prioritized = calc.prioritized_accounts();
        assert_eq!(prioritized[0].0, "team-beta"); // Most underserved first
        assert_eq!(prioritized[1].0, "team-alpha"); // Most overserved last

        // Priority modifiers
        let alpha_mod = calc.calculate_priority_modifier(&"team-alpha".to_string(), &heavy_usage);
        let beta_mod = calc.calculate_priority_modifier(&"team-beta".to_string(), &light_usage);

        assert!(beta_mod > alpha_mod); // Beta should get higher priority
    }

    /// Test allocation transfer
    #[test]
    fn test_allocation_transfer() {
        let mut manager = QuotaManager::new();

        let quota = Quota::new(
            "cpu-quota",
            "CPU Hours",
            QuotaResource::CpuHours,
            QuotaLimit::Hard(1000.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        );
        manager.create_quota(quota).unwrap();

        let account1 = QuotaAccount::new("team-1", "Team 1", "user1@test.com");
        let account2 = QuotaAccount::new("team-2", "Team 2", "user2@test.com");
        manager.create_account(account1).unwrap();
        manager.create_account(account2).unwrap();

        // Create transferable allocation
        let allocation =
            QuotaAllocation::new("cpu-quota".to_string(), 200.0, "admin").transferable();
        manager
            .get_account_mut(&"team-1".to_string())
            .unwrap()
            .add_allocation(allocation);

        // Transfer 50 units
        manager
            .transfer(
                &"team-1".to_string(),
                &"team-2".to_string(),
                &QuotaResource::CpuHours,
                50.0,
            )
            .unwrap();

        let t1 = manager.get_account(&"team-1".to_string()).unwrap();
        let t2 = manager.get_account(&"team-2".to_string()).unwrap();

        assert_eq!(
            t1.get_allocation(&"cpu-quota".to_string())
                .unwrap()
                .allocated_amount,
            150.0
        );
        assert_eq!(
            t2.get_allocation(&"cpu-quota".to_string())
                .unwrap()
                .allocated_amount,
            50.0
        );
    }

    /// Test period reset
    #[test]
    fn test_period_reset() {
        let mut manager = QuotaManager::new();

        let quota = Quota::new(
            "daily-cpu",
            "Daily CPU",
            QuotaResource::CpuHours,
            QuotaLimit::Hard(100.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        )
        .with_period(QuotaPeriod::Calendar(CalendarPeriod::Daily));

        manager.create_quota(quota).unwrap();

        let account = QuotaAccount::new("team", "Team", "user@test.com");
        manager.create_account(account).unwrap();
        manager
            .allocate(
                &"daily-cpu".to_string(),
                &"team".to_string(),
                100.0,
                "admin",
            )
            .unwrap();

        // Record usage
        manager
            .get_account_mut(&"team".to_string())
            .unwrap()
            .record_usage(&QuotaResource::CpuHours, 80.0);

        // Set period start to 2 days ago
        {
            let account = manager.get_account_mut(&"team".to_string()).unwrap();
            let key = QuotaResource::CpuHours.key();
            if let Some(usage) = account.usage.get_mut(&key) {
                usage.period_start = Utc::now() - Duration::days(2);
            }
        }

        // Reset periods
        manager.reset_periods(Utc::now());

        // Usage should be reset
        let account = manager.get_account(&"team".to_string()).unwrap();
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 0.0);
    }

    /// Test burst limit behavior
    #[test]
    fn test_burst_limits() {
        let mut manager = QuotaManager::new();

        let quota = Quota::new(
            "cpu-burst",
            "Burst CPU",
            QuotaResource::CpuHours,
            QuotaLimit::Burst {
                sustained_limit: 100.0,
                burst_limit: 150.0,
                burst_duration: Duration::minutes(30),
                cooldown: Duration::hours(1),
            },
            QuotaScope::Global,
            QuotaEnforcement::AllowWithWarning,
            "admin",
            "test.com",
        );
        manager.create_quota(quota).unwrap();

        let account = QuotaAccount::new("team", "Team", "user@test.com");
        manager.create_account(account).unwrap();
        manager
            .allocate(
                &"cpu-burst".to_string(),
                &"team".to_string(),
                150.0,
                "admin",
            )
            .unwrap();

        // Use 90 (under sustained limit)
        manager
            .get_account_mut(&"team".to_string())
            .unwrap()
            .record_usage(&QuotaResource::CpuHours, 90.0);

        // Request 30 more (would need burst)
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 30.0);
        let result = manager.check_submission("user@test.com", &request);

        assert!(result.allowed);
        assert!(result.warnings.iter().any(|w| w.code == "USING_BURST"));
    }

    /// Test multi-resource requests
    #[test]
    fn test_multi_resource_request() {
        let mut manager = QuotaManager::new();

        // CPU quota
        let cpu_quota = Quota::new(
            "cpu-quota",
            "CPU",
            QuotaResource::CpuHours,
            QuotaLimit::Hard(100.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        );
        manager.create_quota(cpu_quota).unwrap();

        // GPU quota
        let gpu_quota = Quota::new(
            "gpu-quota",
            "GPU",
            QuotaResource::GpuHours,
            QuotaLimit::Hard(50.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        );
        manager.create_quota(gpu_quota).unwrap();

        // Memory quota
        let mem_quota = Quota::new(
            "mem-quota",
            "Memory",
            QuotaResource::MemoryGBHours,
            QuotaLimit::Hard(200.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        );
        manager.create_quota(mem_quota).unwrap();

        let account = QuotaAccount::new("team", "Team", "user@test.com");
        manager.create_account(account).unwrap();

        manager
            .allocate(
                &"cpu-quota".to_string(),
                &"team".to_string(),
                100.0,
                "admin",
            )
            .unwrap();
        manager
            .allocate(&"gpu-quota".to_string(), &"team".to_string(), 50.0, "admin")
            .unwrap();
        manager
            .allocate(
                &"mem-quota".to_string(),
                &"team".to_string(),
                200.0,
                "admin",
            )
            .unwrap();

        // Request multiple resources
        let request = ResourceRequest::new()
            .with_resource(QuotaResource::CpuHours, 40.0)
            .with_resource(QuotaResource::GpuHours, 20.0)
            .with_resource(QuotaResource::MemoryGBHours, 80.0);

        let result = manager.check_submission("user@test.com", &request);
        assert!(result.allowed);

        // Reserve and verify all resources tracked
        manager
            .reserve("job-1", &"team".to_string(), &request)
            .unwrap();

        let account = manager.get_account(&"team".to_string()).unwrap();
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 40.0);
        assert_eq!(account.get_usage(&QuotaResource::GpuHours), 20.0);
        assert_eq!(account.get_usage(&QuotaResource::MemoryGBHours), 80.0);
    }

    /// Test approaching limit warning
    #[test]
    fn test_approaching_limit_warning() {
        let mut manager = QuotaManager::new();

        let quota = Quota::new(
            "cpu-quota",
            "CPU",
            QuotaResource::CpuHours,
            QuotaLimit::Hard(100.0),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        );
        manager.create_quota(quota).unwrap();

        let account = QuotaAccount::new("team", "Team", "user@test.com");
        manager.create_account(account).unwrap();
        manager
            .allocate(
                &"cpu-quota".to_string(),
                &"team".to_string(),
                100.0,
                "admin",
            )
            .unwrap();

        // Use 85%
        manager
            .get_account_mut(&"team".to_string())
            .unwrap()
            .record_usage(&QuotaResource::CpuHours, 85.0);

        // Request 5 more (would be at 90%)
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 5.0);
        let result = manager.check_submission("user@test.com", &request);

        assert!(result.allowed);
        assert!(result
            .warnings
            .iter()
            .any(|w| w.code == "APPROACHING_LIMIT"));
    }
}
