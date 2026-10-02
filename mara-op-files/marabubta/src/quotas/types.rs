// Marabunta - Licensed under the MIT License.
//! Quota types and definitions for the marabunta-compute job placement system
//!
//! This module defines the core types for implementing resource quotas and budgets.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Unique identifier for a quota
pub type QuotaId = String;

/// Unique identifier for a quota account
pub type AccountId = String;

/// A quota definition that limits resource usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quota {
    /// Unique identifier for this quota
    pub id: QuotaId,
    /// Human-readable name
    pub name: String,
    /// Detailed description of the quota's purpose
    pub description: String,

    /// What resource does this quota limit?
    pub resource: QuotaResource,

    /// The limit definition
    pub limit: QuotaLimit,

    /// Who does this quota apply to?
    pub scope: QuotaScope,

    /// Behavior when the quota is exceeded
    pub enforcement: QuotaEnforcement,

    /// Period for quota reset
    pub period: QuotaPeriod,

    /// Principal who created this quota
    pub created_by: String,
    /// When this quota was created
    pub created_at: DateTime<Utc>,
    /// The authority domain that governs this quota
    pub authority_domain: String,
}

impl Quota {
    /// Create a new quota with the given parameters
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        resource: QuotaResource,
        limit: QuotaLimit,
        scope: QuotaScope,
        enforcement: QuotaEnforcement,
        created_by: impl Into<String>,
        authority_domain: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            description: String::new(),
            resource,
            limit,
            scope,
            enforcement,
            period: QuotaPeriod::NonExpiring,
            created_by: created_by.into(),
            created_at: Utc::now(),
            authority_domain: authority_domain.into(),
        }
    }

    /// Set the description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the period
    pub fn with_period(mut self, period: QuotaPeriod) -> Self {
        self.period = period;
        self
    }
}

/// The type of resource being limited by a quota
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuotaResource {
    // Compute resources
    /// CPU hours consumed
    CpuHours,
    /// GPU hours consumed
    GpuHours,
    /// Memory GB-hours consumed
    MemoryGBHours,
    /// Memory GB-hours consumed (alternative naming)
    MemoryGbHours,

    // Counts
    /// Number of concurrent jobs
    ConcurrentJobs,
    /// Number of concurrent tasks
    ConcurrentTasks,
    /// Total jobs submitted per period
    TotalJobsPerPeriod,
    /// Total number of jobs
    JobCount,
    /// Total number of tasks
    TaskCount,

    // Storage
    /// Storage in gigabytes
    StorageGB,
    /// Storage in gigabytes (alternative naming)
    StorageGb,

    // Network
    /// Network egress in gigabytes
    NetworkEgressGB,
    /// Network transfer in gigabytes
    NetworkGb,

    // Composite (weighted sum of multiple resources)
    /// A composite resource that is a weighted sum of other resources
    Composite {
        /// Components and their weights
        components: Vec<(Box<QuotaResource>, f64)>,
    },

    // Custom (for extensibility)
    /// A custom resource type identified by name
    Custom(String),
}

impl QuotaResource {
    /// Get a canonical string key for this resource (for use in HashMaps)
    pub fn key(&self) -> String {
        match self {
            QuotaResource::CpuHours => "cpu_hours".to_string(),
            QuotaResource::GpuHours => "gpu_hours".to_string(),
            QuotaResource::MemoryGBHours => "memory_gb_hours".to_string(),
            QuotaResource::MemoryGbHours => "memory_gb_hours_alt".to_string(),
            QuotaResource::ConcurrentJobs => "concurrent_jobs".to_string(),
            QuotaResource::ConcurrentTasks => "concurrent_tasks".to_string(),
            QuotaResource::TotalJobsPerPeriod => "total_jobs_per_period".to_string(),
            QuotaResource::JobCount => "job_count".to_string(),
            QuotaResource::TaskCount => "task_count".to_string(),
            QuotaResource::StorageGB => "storage_gb".to_string(),
            QuotaResource::StorageGb => "storage_gb_alt".to_string(),
            QuotaResource::NetworkEgressGB => "network_egress_gb".to_string(),
            QuotaResource::NetworkGb => "network_gb".to_string(),
            QuotaResource::Composite { components } => {
                let parts: Vec<String> = components
                    .iter()
                    .map(|(r, w)| format!("{}:{}", r.key(), w))
                    .collect();
                format!("composite({})", parts.join(","))
            }
            QuotaResource::Custom(name) => format!("custom:{}", name),
        }
    }
}

impl PartialEq for QuotaResource {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for QuotaResource {}

impl Hash for QuotaResource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

/// The limit definition for a quota
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuotaLimit {
    /// Hard limit - cannot exceed under any circumstances
    Hard(f64),

    /// Soft limit - can exceed but with consequences
    Soft {
        /// The soft limit threshold
        limit: f64,
        /// How much over the limit is permitted
        overage_allowed: f64,
        /// Penalty applied when over the soft limit
        overage_penalty: OveragePenalty,
    },

    /// Burst limit - can exceed temporarily
    Burst {
        /// The sustained limit for normal operation
        sustained_limit: f64,
        /// The maximum burst limit
        burst_limit: f64,
        /// How long the burst can be sustained
        burst_duration: Duration,
        /// Cooldown period after a burst before another is allowed
        cooldown: Duration,
    },

    /// Tiered - different limits at different usage levels
    Tiered {
        /// The tiers defining different usage levels
        tiers: Vec<QuotaTier>,
    },

    /// Unlimited - for tracking without limiting
    Unlimited,
}

impl QuotaLimit {
    /// Get the effective limit value for checking
    pub fn effective_limit(&self) -> f64 {
        match self {
            QuotaLimit::Hard(limit) => *limit,
            QuotaLimit::Soft {
                limit,
                overage_allowed,
                ..
            } => limit + overage_allowed,
            QuotaLimit::Burst { burst_limit, .. } => *burst_limit,
            QuotaLimit::Tiered { tiers } => {
                tiers.last().map(|t| t.threshold).unwrap_or(f64::INFINITY)
            }
            QuotaLimit::Unlimited => f64::INFINITY,
        }
    }

    /// Get the soft/sustained limit (the preferred operating limit)
    pub fn soft_limit(&self) -> f64 {
        match self {
            QuotaLimit::Hard(limit) => *limit,
            QuotaLimit::Soft { limit, .. } => *limit,
            QuotaLimit::Burst {
                sustained_limit, ..
            } => *sustained_limit,
            QuotaLimit::Tiered { tiers } => {
                tiers.first().map(|t| t.threshold).unwrap_or(f64::INFINITY)
            }
            QuotaLimit::Unlimited => f64::INFINITY,
        }
    }
}

/// A tier in a tiered quota system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaTier {
    /// The usage threshold where this tier starts
    pub threshold: f64,
    /// Cost multiplier at this tier (1.0 = normal)
    pub rate_multiplier: f64,
    /// Priority adjustment at this tier (negative = lower priority)
    pub priority_modifier: i32,
}

impl QuotaTier {
    /// Create a new quota tier
    pub fn new(threshold: f64, rate_multiplier: f64, priority_modifier: i32) -> Self {
        Self {
            threshold,
            rate_multiplier,
            priority_modifier,
        }
    }
}

/// Penalty applied when over a soft limit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OveragePenalty {
    /// Lower job priority by the specified amount
    LowerPriority { by: i32 },
    /// Increase cost by the specified multiplier
    HigherCost { multiplier: f64 },
    /// Throttle resource usage to the specified percentage
    Throttle { to_percent: f64 },
    /// Send notification to recipients
    Notify { recipients: Vec<String> },
    /// Combination of multiple penalties
    Combined(Vec<OveragePenalty>),
}

impl OveragePenalty {
    /// Get the priority penalty (if any)
    pub fn priority_penalty(&self) -> i32 {
        match self {
            OveragePenalty::LowerPriority { by } => *by,
            OveragePenalty::Combined(penalties) => {
                penalties.iter().map(|p| p.priority_penalty()).sum()
            }
            _ => 0,
        }
    }

    /// Get the cost multiplier (if any)
    pub fn cost_multiplier(&self) -> f64 {
        match self {
            OveragePenalty::HigherCost { multiplier } => *multiplier,
            OveragePenalty::Combined(penalties) => {
                penalties.iter().map(|p| p.cost_multiplier()).product()
            }
            _ => 1.0,
        }
    }
}

/// The scope of who a quota applies to
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuotaScope {
    /// Applies to a single principal (user)
    Principal(String),

    /// Applies to a group of principals
    Principals(Vec<String>),

    /// Applies to all users in a domain
    Domain(String),

    /// Applies to a specific project
    Project(String),

    /// Applies globally to the entire system
    Global,
}

impl QuotaScope {
    /// Check if a principal matches this scope
    pub fn matches(&self, principal_id: &str) -> bool {
        match self {
            QuotaScope::Principal(p) => p == principal_id,
            QuotaScope::Principals(ps) => ps.contains(&principal_id.to_string()),
            QuotaScope::Domain(domain) => {
                // Assume principal format is "user@domain"
                principal_id.ends_with(&format!("@{}", domain))
            }
            QuotaScope::Project(_) => true, // Project scope is checked differently
            QuotaScope::Global => true,
        }
    }
}

/// How to enforce quota violations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuotaEnforcement {
    /// Block job submission entirely
    Block,

    /// Allow but deprioritize the job
    Deprioritize {
        /// How much to reduce priority
        priority_penalty: i32,
    },

    /// Allow but mark for review
    AllowWithWarning,

    /// Queue the job until quota is available
    Queue {
        /// Maximum time to wait in queue
        max_wait: Duration,
    },

    /// Use a custom enforcement handler
    Custom {
        /// ID of the custom handler to invoke
        handler_id: String,
    },
}

/// Period for quota reset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuotaPeriod {
    /// Rolling window of specified duration
    Rolling { duration: Duration },
    /// Calendar-based period (resets at period boundary)
    Calendar(CalendarPeriod),
    /// Never expires/resets
    NonExpiring,
}

impl QuotaPeriod {
    /// Get the next reset time from the given timestamp
    pub fn next_reset(&self, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            QuotaPeriod::Rolling { duration } => Some(from + *duration),
            QuotaPeriod::Calendar(period) => Some(period.next_boundary(from)),
            QuotaPeriod::NonExpiring => None,
        }
    }

    /// Get the start of the current period containing the given timestamp
    pub fn period_start(&self, at: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            QuotaPeriod::Rolling { duration } => at - *duration,
            QuotaPeriod::Calendar(period) => period.period_start(at),
            QuotaPeriod::NonExpiring => DateTime::UNIX_EPOCH,
        }
    }
}

/// Calendar-based period types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CalendarPeriod {
    /// Reset every hour
    Hourly,
    /// Reset at midnight UTC
    Daily,
    /// Reset at midnight UTC on Monday
    Weekly,
    /// Reset at midnight UTC on the 1st of the month
    Monthly,
    /// Reset at midnight UTC on the 1st of the quarter
    Quarterly,
    /// Reset at midnight UTC on January 1st
    Yearly,
}

impl CalendarPeriod {
    /// Get the next boundary time after the given timestamp
    pub fn next_boundary(&self, from: DateTime<Utc>) -> DateTime<Utc> {
        use chrono::{Datelike, Timelike};

        match self {
            CalendarPeriod::Hourly => {
                let next = from + Duration::hours(1);
                next.with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap()
            }
            CalendarPeriod::Daily => {
                let next = from + Duration::days(1);
                next.with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap()
            }
            CalendarPeriod::Weekly => {
                let days_until_monday = (8 - from.weekday().num_days_from_monday()) % 7;
                let days_until_monday = if days_until_monday == 0 {
                    7
                } else {
                    days_until_monday
                };
                let next = from + Duration::days(days_until_monday as i64);
                next.with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap()
            }
            CalendarPeriod::Monthly => {
                let (year, month) = if from.month() == 12 {
                    (from.year() + 1, 1)
                } else {
                    (from.year(), from.month() + 1)
                };
                from.with_year(year)
                    .unwrap()
                    .with_month(month)
                    .unwrap()
                    .with_day(1)
                    .unwrap()
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap()
            }
            CalendarPeriod::Quarterly => {
                let current_quarter = (from.month() - 1) / 3;
                let (year, month) = if current_quarter == 3 {
                    (from.year() + 1, 1)
                } else {
                    (from.year(), (current_quarter + 1) * 3 + 1)
                };
                from.with_year(year)
                    .unwrap()
                    .with_month(month)
                    .unwrap()
                    .with_day(1)
                    .unwrap()
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap()
            }
            CalendarPeriod::Yearly => from
                .with_year(from.year() + 1)
                .unwrap()
                .with_month(1)
                .unwrap()
                .with_day(1)
                .unwrap()
                .with_hour(0)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
        }
    }

    /// Get the start of the current period containing the given timestamp
    pub fn period_start(&self, at: DateTime<Utc>) -> DateTime<Utc> {
        use chrono::{Datelike, Timelike};

        match self {
            CalendarPeriod::Hourly => at
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
            CalendarPeriod::Daily => at
                .with_hour(0)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
            CalendarPeriod::Weekly => {
                let days_since_monday = at.weekday().num_days_from_monday();
                let monday = at - Duration::days(days_since_monday as i64);
                monday
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap()
            }
            CalendarPeriod::Monthly => at
                .with_day(1)
                .unwrap()
                .with_hour(0)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
            CalendarPeriod::Quarterly => {
                let quarter_start_month = ((at.month() - 1) / 3) * 3 + 1;
                at.with_month(quarter_start_month)
                    .unwrap()
                    .with_day(1)
                    .unwrap()
                    .with_hour(0)
                    .unwrap()
                    .with_minute(0)
                    .unwrap()
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap()
            }
            CalendarPeriod::Yearly => at
                .with_month(1)
                .unwrap()
                .with_day(1)
                .unwrap()
                .with_hour(0)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap(),
        }
    }
}

/// Burst state tracking for burst limits
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct BurstState {
    /// Whether currently in a burst period
    pub in_burst: bool,
    /// When the current burst started
    pub burst_started: Option<DateTime<Utc>>,
    /// When the last burst ended (for cooldown tracking)
    pub last_burst_ended: Option<DateTime<Utc>>,
}


impl BurstState {
    /// Check if burst is available given the configuration
    pub fn can_burst(&self, config: &QuotaLimit, now: DateTime<Utc>) -> bool {
        if let QuotaLimit::Burst {
            cooldown,
            burst_duration,
            ..
        } = config
        {
            // Check if we're currently in a burst that hasn't exceeded duration
            if self.in_burst {
                if let Some(started) = self.burst_started {
                    return now < started + *burst_duration;
                }
            }

            // Check if cooldown has passed since last burst
            if let Some(ended) = self.last_burst_ended {
                return now >= ended + *cooldown;
            }

            // No previous burst, can start one
            true
        } else {
            false
        }
    }

    /// Start a burst period
    pub fn start_burst(&mut self, now: DateTime<Utc>) {
        self.in_burst = true;
        self.burst_started = Some(now);
    }

    /// End the current burst period
    pub fn end_burst(&mut self, now: DateTime<Utc>) {
        self.in_burst = false;
        self.burst_started = None;
        self.last_burst_ended = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    #[test]
    fn test_quota_resource_key() {
        assert_eq!(QuotaResource::CpuHours.key(), "cpu_hours");
        assert_eq!(QuotaResource::GpuHours.key(), "gpu_hours");
        assert_eq!(
            QuotaResource::Custom("test".to_string()).key(),
            "custom:test"
        );
    }

    #[test]
    fn test_quota_resource_equality() {
        assert_eq!(QuotaResource::CpuHours, QuotaResource::CpuHours);
        assert_ne!(QuotaResource::CpuHours, QuotaResource::GpuHours);
    }

    #[test]
    fn test_quota_resource_hash() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(QuotaResource::CpuHours, 100.0);
        map.insert(QuotaResource::GpuHours, 50.0);
        assert_eq!(map.get(&QuotaResource::CpuHours), Some(&100.0));
        assert_eq!(map.get(&QuotaResource::GpuHours), Some(&50.0));
    }

    #[test]
    fn test_quota_limit_effective() {
        assert_eq!(QuotaLimit::Hard(100.0).effective_limit(), 100.0);
        assert_eq!(
            QuotaLimit::Soft {
                limit: 100.0,
                overage_allowed: 20.0,
                overage_penalty: OveragePenalty::LowerPriority { by: 10 },
            }
            .effective_limit(),
            120.0
        );
        assert_eq!(QuotaLimit::Unlimited.effective_limit(), f64::INFINITY);
    }

    #[test]
    fn test_quota_scope_matches() {
        let principal_scope = QuotaScope::Principal("user@example.com".to_string());
        assert!(principal_scope.matches("user@example.com"));
        assert!(!principal_scope.matches("other@example.com"));

        let domain_scope = QuotaScope::Domain("example.com".to_string());
        assert!(domain_scope.matches("user@example.com"));
        assert!(!domain_scope.matches("user@other.com"));

        let global_scope = QuotaScope::Global;
        assert!(global_scope.matches("anyone@anywhere.com"));
    }

    #[test]
    fn test_calendar_period_daily() {
        use chrono::TimeZone;
        let now = Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 0).unwrap();
        let period_start = CalendarPeriod::Daily.period_start(now);
        assert_eq!(period_start.day(), 15);
        assert_eq!(period_start.hour(), 0);
        assert_eq!(period_start.minute(), 0);

        let next = CalendarPeriod::Daily.next_boundary(now);
        assert_eq!(next.day(), 16);
        assert_eq!(next.hour(), 0);
    }

    #[test]
    fn test_calendar_period_monthly() {
        use chrono::TimeZone;
        let now = Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 0).unwrap();
        let period_start = CalendarPeriod::Monthly.period_start(now);
        assert_eq!(period_start.month(), 3);
        assert_eq!(period_start.day(), 1);

        let next = CalendarPeriod::Monthly.next_boundary(now);
        assert_eq!(next.month(), 4);
        assert_eq!(next.day(), 1);
    }

    #[test]
    fn test_calendar_period_yearly_rollover() {
        use chrono::TimeZone;
        let december = Utc.with_ymd_and_hms(2024, 12, 15, 14, 30, 0).unwrap();
        let next = CalendarPeriod::Yearly.next_boundary(december);
        assert_eq!(next.year(), 2025);
        assert_eq!(next.month(), 1);
        assert_eq!(next.day(), 1);
    }

    #[test]
    fn test_overage_penalty_priority() {
        let penalty = OveragePenalty::LowerPriority { by: 5 };
        assert_eq!(penalty.priority_penalty(), 5);

        let combined = OveragePenalty::Combined(vec![
            OveragePenalty::LowerPriority { by: 3 },
            OveragePenalty::LowerPriority { by: 2 },
        ]);
        assert_eq!(combined.priority_penalty(), 5);
    }

    #[test]
    fn test_burst_state() {
        let config = QuotaLimit::Burst {
            sustained_limit: 100.0,
            burst_limit: 150.0,
            burst_duration: Duration::minutes(5),
            cooldown: Duration::minutes(10),
        };

        let mut state = BurstState::default();
        let now = Utc::now();

        assert!(state.can_burst(&config, now));

        state.start_burst(now);
        assert!(state.in_burst);

        // Still within burst duration
        assert!(state.can_burst(&config, now + Duration::minutes(3)));

        // Beyond burst duration
        assert!(!state.can_burst(&config, now + Duration::minutes(6)));
    }
}
