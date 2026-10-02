// Marabunta - Licensed under the MIT License.
//! Health monitoring for the three-tier storage subsystem.
//!
//! Tracks per-tier health snapshots and provides a unified view of
//! storage health across all tiers.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::types::HealthStatus;
use super::StorageTier;

// ---------------------------------------------------------------------------
// TierHealth
// ---------------------------------------------------------------------------

/// Health snapshot for a single storage tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierHealth {
    /// Which tier this report is for.
    pub tier: StorageTier,

    /// Current health status.
    pub status: HealthStatus,

    /// Last measured latency in milliseconds (None if never checked).
    pub latency_ms: Option<f64>,

    /// Unix timestamp of last health check.
    pub last_check: Option<u64>,

    /// Number of adapters currently registered for this tier.
    pub adapter_count: usize,
}

impl TierHealth {
    /// Create a new TierHealth with Unknown status.
    pub fn unknown(tier: StorageTier) -> Self {
        Self {
            tier,
            status: HealthStatus::Unknown,
            latency_ms: None,
            last_check: None,
            adapter_count: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// TierHealthMonitor
// ---------------------------------------------------------------------------

/// Monitors health across all storage tiers.
pub struct TierHealthMonitor {
    /// Latest health snapshot per tier.
    snapshots: HashMap<StorageTier, TierHealth>,
}

impl TierHealthMonitor {
    pub fn new() -> Self {
        let mut snapshots = HashMap::new();
        snapshots.insert(
            StorageTier::Tier1PreExisting,
            TierHealth::unknown(StorageTier::Tier1PreExisting),
        );
        snapshots.insert(
            StorageTier::Tier2Deployed,
            TierHealth::unknown(StorageTier::Tier2Deployed),
        );
        snapshots.insert(
            StorageTier::Tier3CDE,
            TierHealth::unknown(StorageTier::Tier3CDE),
        );
        Self { snapshots }
    }

    /// Update the health snapshot for a tier.
    pub fn update(&mut self, health: TierHealth) {
        self.snapshots.insert(health.tier, health);
    }

    /// Get current health for a tier.
    pub fn get(&self, tier: &StorageTier) -> Option<&TierHealth> {
        self.snapshots.get(tier)
    }

    /// Get all health snapshots.
    pub fn all(&self) -> &HashMap<StorageTier, TierHealth> {
        &self.snapshots
    }

    /// Get health status for a specific tier, returning Unknown if not tracked.
    pub fn status_of(&self, tier: &StorageTier) -> HealthStatus {
        self.snapshots
            .get(tier)
            .map(|h| h.status)
            .unwrap_or(HealthStatus::Unknown)
    }
}

impl Default for TierHealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_health_unknown() {
        let h = TierHealth::unknown(StorageTier::Tier3CDE);
        assert_eq!(h.tier, StorageTier::Tier3CDE);
        assert_eq!(h.status, HealthStatus::Unknown);
        assert!(h.latency_ms.is_none());
        assert!(h.last_check.is_none());
        assert_eq!(h.adapter_count, 0);
    }

    #[test]
    fn test_health_monitor_new_defaults() {
        let monitor = TierHealthMonitor::new();
        assert_eq!(
            monitor.get(&StorageTier::Tier1PreExisting).unwrap().status,
            HealthStatus::Unknown
        );
        assert_eq!(
            monitor.get(&StorageTier::Tier2Deployed).unwrap().status,
            HealthStatus::Unknown
        );
        assert_eq!(
            monitor.get(&StorageTier::Tier3CDE).unwrap().status,
            HealthStatus::Unknown
        );
    }

    #[test]
    fn test_health_monitor_update() {
        let mut monitor = TierHealthMonitor::new();

        // Initially Unknown
        assert_eq!(
            monitor.get(&StorageTier::Tier1PreExisting).unwrap().status,
            HealthStatus::Unknown
        );

        // Update to Healthy
        let updated = TierHealth {
            tier: StorageTier::Tier1PreExisting,
            status: HealthStatus::Healthy,
            latency_ms: Some(2.5),
            last_check: Some(1700000000),
            adapter_count: 1,
        };
        monitor.update(updated);

        let h = monitor.get(&StorageTier::Tier1PreExisting).unwrap();
        assert_eq!(h.status, HealthStatus::Healthy);
        assert_eq!(h.latency_ms, Some(2.5));
        assert_eq!(h.last_check, Some(1700000000));
        assert_eq!(h.adapter_count, 1);
    }

    #[test]
    fn test_health_monitor_status_of() {
        let monitor = TierHealthMonitor::new();
        assert_eq!(
            monitor.status_of(&StorageTier::Tier1PreExisting),
            HealthStatus::Unknown
        );
        assert_eq!(
            monitor.status_of(&StorageTier::Tier2Deployed),
            HealthStatus::Unknown
        );
        assert_eq!(
            monitor.status_of(&StorageTier::Tier3CDE),
            HealthStatus::Unknown
        );
    }

    #[test]
    fn test_health_monitor_all() {
        let monitor = TierHealthMonitor::new();
        let all = monitor.all();
        assert_eq!(all.len(), 3);
        assert!(all.contains_key(&StorageTier::Tier1PreExisting));
        assert!(all.contains_key(&StorageTier::Tier2Deployed));
        assert!(all.contains_key(&StorageTier::Tier3CDE));
    }
}
