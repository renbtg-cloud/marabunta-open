// Marabunta - Licensed under the MIT License.
//! StorageRouter -- routes storage operations to the best available tier.
//!
//! This file contains the `StorageRouter` struct skeleton with tier selection
//! logic stubs. Full routing implementation comes in a later wave.
//!
//! **Read path**: Tier 1 (fastest) > Tier 2 > Tier 3 (fallback).
//! **Write path**: Always Tier 3 first (source of truth), then async replicate up.

use std::collections::HashMap;

use super::adapters::{MarabuntaEngine, DeployedAdapter, PreExistingAdapter};
use super::health::TierHealthMonitor;
use super::types::HealthStatus;
use super::StorageTier;

// ---------------------------------------------------------------------------
// StorageRouter
// ---------------------------------------------------------------------------

/// Routes storage operations to the best available tier.
///
/// Writes: always Tier 3 first, then async replicate to Tier 1/2.
/// Reads: Tier 1 (fastest) > Tier 2 > Tier 3 (fallback).
pub struct StorageRouter {
    /// Pre-existing databases detected by capability scanner.
    tier1: Vec<Box<dyn PreExistingAdapter>>,

    /// Swarm-deployed containerized databases.
    tier2: Vec<Box<dyn DeployedAdapter>>,

    /// Marabunta Distributed Engine -- always available.
    tier3: Option<Box<dyn MarabuntaEngine>>,

    /// Health monitor for all tiers.
    health_monitor: TierHealthMonitor,

    /// Currently selected read tier.
    active_read_tier: StorageTier,
}

impl StorageRouter {
    /// Create a new StorageRouter. Tier 3 (CDE) starts as None until
    /// the MarabuntaEngine implementation is wired in by a later piece.
    pub fn new() -> Self {
        Self {
            tier1: Vec::new(),
            tier2: Vec::new(),
            tier3: None,
            health_monitor: TierHealthMonitor::new(),
            active_read_tier: StorageTier::Tier3CDE,
        }
    }

    /// Register a pre-existing (Tier 1) adapter.
    pub fn add_tier1(&mut self, adapter: Box<dyn PreExistingAdapter>) {
        self.tier1.push(adapter);
        self.recompute_active_tier();
    }

    /// Register a swarm-deployed (Tier 2) adapter.
    pub fn add_tier2(&mut self, adapter: Box<dyn DeployedAdapter>) {
        self.tier2.push(adapter);
        self.recompute_active_tier();
    }

    /// Set the Marabunta Engine (Tier 3) implementation.
    pub fn set_tier3(&mut self, engine: Box<dyn MarabuntaEngine>) {
        self.tier3 = Some(engine);
    }

    /// Remove a Tier 1 adapter by index (hot-swap on disappearance).
    pub fn remove_tier1(&mut self, index: usize) {
        if index < self.tier1.len() {
            self.tier1.remove(index);
            self.recompute_active_tier();
        }
    }

    /// Remove a Tier 2 adapter by index.
    pub fn remove_tier2(&mut self, index: usize) {
        if index < self.tier2.len() {
            self.tier2.remove(index);
            self.recompute_active_tier();
        }
    }

    /// Determine which tier to use for reads based on availability.
    fn recompute_active_tier(&mut self) {
        if !self.tier1.is_empty() {
            self.active_read_tier = StorageTier::Tier1PreExisting;
        } else if !self.tier2.is_empty() {
            self.active_read_tier = StorageTier::Tier2Deployed;
        } else {
            self.active_read_tier = StorageTier::Tier3CDE;
        }
    }

    /// Return the currently selected tier for reads.
    pub fn active_tier(&self) -> StorageTier {
        self.active_read_tier
    }

    /// Return all tiers that have at least one adapter available.
    pub fn available_tiers(&self) -> Vec<StorageTier> {
        let mut tiers = Vec::new();
        if !self.tier1.is_empty() {
            tiers.push(StorageTier::Tier1PreExisting);
        }
        if !self.tier2.is_empty() {
            tiers.push(StorageTier::Tier2Deployed);
        }
        if self.tier3.is_some() {
            tiers.push(StorageTier::Tier3CDE);
        }
        tiers
    }

    /// Adapter count per tier.
    pub fn adapter_counts(&self) -> HashMap<StorageTier, usize> {
        let mut map = HashMap::new();
        map.insert(StorageTier::Tier1PreExisting, self.tier1.len());
        map.insert(StorageTier::Tier2Deployed, self.tier2.len());
        map.insert(
            StorageTier::Tier3CDE,
            if self.tier3.is_some() { 1 } else { 0 },
        );
        map
    }

    /// Collect health status from all tiers.
    /// Stub implementation: tiers with adapters report Healthy,
    /// tiers without adapters report Unavailable,
    /// Tier 3 without CDE reports Unknown.
    pub fn tier_health(&self) -> HashMap<StorageTier, HealthStatus> {
        let mut health = HashMap::new();

        health.insert(
            StorageTier::Tier1PreExisting,
            if self.tier1.is_empty() {
                HealthStatus::Unavailable
            } else {
                HealthStatus::Healthy
            },
        );
        health.insert(
            StorageTier::Tier2Deployed,
            if self.tier2.is_empty() {
                HealthStatus::Unavailable
            } else {
                HealthStatus::Healthy
            },
        );
        health.insert(
            StorageTier::Tier3CDE,
            match &self.tier3 {
                Some(_) => HealthStatus::Healthy,
                None => HealthStatus::Unknown,
            },
        );

        health
    }

    /// Access the health monitor.
    pub fn health_monitor(&self) -> &TierHealthMonitor {
        &self.health_monitor
    }

    /// Access the health monitor mutably.
    pub fn health_monitor_mut(&mut self) -> &mut TierHealthMonitor {
        &mut self.health_monitor
    }
}

impl Default for StorageRouter {
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
    use crate::swarm::storage::adapters::{
        MarabuntaEngine, DeployedAdapter, DeploymentStatus, FragmentId, PreExistingAdapter,
        ReplicationReport,
    };
    use crate::swarm::storage::types::{ResultSet, StorageError, Value};
    use async_trait::async_trait;

    // -- Mock adapters for testing --

    struct MockTier1Adapter {
        name: String,
    }

    #[async_trait]
    impl PreExistingAdapter for MockTier1Adapter {
        async fn connect(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
        async fn health_check(&self) -> Result<HealthStatus, StorageError> {
            Ok(HealthStatus::Healthy)
        }
        async fn execute_query(
            &self,
            _sql: &str,
            _args: &[Value],
        ) -> Result<ResultSet, StorageError> {
            Ok(ResultSet::empty())
        }
        async fn disconnect(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
        fn display_name(&self) -> String {
            self.name.clone()
        }
    }

    struct MockTier2Adapter {
        name: String,
    }

    #[async_trait]
    impl PreExistingAdapter for MockTier2Adapter {
        async fn connect(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
        async fn health_check(&self) -> Result<HealthStatus, StorageError> {
            Ok(HealthStatus::Healthy)
        }
        async fn execute_query(
            &self,
            _sql: &str,
            _args: &[Value],
        ) -> Result<ResultSet, StorageError> {
            Ok(ResultSet::empty())
        }
        async fn disconnect(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
        fn display_name(&self) -> String {
            self.name.clone()
        }
    }

    #[async_trait]
    impl DeployedAdapter for MockTier2Adapter {
        async fn deploy(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
        async fn destroy(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
        async fn status(&self) -> Result<DeploymentStatus, StorageError> {
            Ok(DeploymentStatus::Running)
        }
    }

    struct MockCDE;

    #[async_trait]
    impl MarabuntaEngine for MockCDE {
        async fn put_fragment(&self, _id: &FragmentId, _data: &[u8]) -> Result<(), StorageError> {
            Ok(())
        }
        async fn get_fragment(
            &self,
            _id: &FragmentId,
        ) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(None)
        }
        async fn delete_fragment(&self, _id: &FragmentId) -> Result<(), StorageError> {
            Ok(())
        }
        async fn local_fragments(&self) -> Result<Vec<FragmentId>, StorageError> {
            Ok(Vec::new())
        }
        async fn check_replication(&self) -> Result<ReplicationReport, StorageError> {
            Ok(ReplicationReport {
                total_fragments: 0,
                fully_replicated: 0,
                under_replicated: 0,
                replication_factor: 3,
            })
        }
        async fn distributed_query(
            &self,
            _sql: &str,
            _args: &[Value],
        ) -> Result<ResultSet, StorageError> {
            Ok(ResultSet::empty())
        }
    }

    // -- Tests --

    #[test]
    fn test_router_new_defaults() {
        let router = StorageRouter::new();
        assert_eq!(router.active_tier(), StorageTier::Tier3CDE);
        assert!(router.available_tiers().is_empty());
    }

    #[test]
    fn test_router_active_tier_selection() {
        let mut router = StorageRouter::new();
        assert_eq!(router.active_tier(), StorageTier::Tier3CDE);

        // Add a Tier 2 adapter -> active tier should be Tier 2
        router.add_tier2(Box::new(MockTier2Adapter {
            name: "pg-container".to_string(),
        }));
        assert_eq!(router.active_tier(), StorageTier::Tier2Deployed);

        // Add a Tier 1 adapter -> active tier should be Tier 1 (fastest)
        router.add_tier1(Box::new(MockTier1Adapter {
            name: "pg-native".to_string(),
        }));
        assert_eq!(router.active_tier(), StorageTier::Tier1PreExisting);
    }

    #[test]
    fn test_router_available_tiers() {
        let mut router = StorageRouter::new();
        assert!(router.available_tiers().is_empty());

        router.set_tier3(Box::new(MockCDE));
        assert_eq!(router.available_tiers(), vec![StorageTier::Tier3CDE]);

        router.add_tier1(Box::new(MockTier1Adapter {
            name: "pg".to_string(),
        }));
        assert_eq!(
            router.available_tiers(),
            vec![StorageTier::Tier1PreExisting, StorageTier::Tier3CDE]
        );

        router.add_tier2(Box::new(MockTier2Adapter {
            name: "redis-container".to_string(),
        }));
        assert_eq!(
            router.available_tiers(),
            vec![
                StorageTier::Tier1PreExisting,
                StorageTier::Tier2Deployed,
                StorageTier::Tier3CDE,
            ]
        );
    }

    #[test]
    fn test_router_tier_health_stub() {
        let router = StorageRouter::new();
        let health = router.tier_health();
        assert_eq!(
            health[&StorageTier::Tier1PreExisting],
            HealthStatus::Unavailable
        );
        assert_eq!(
            health[&StorageTier::Tier2Deployed],
            HealthStatus::Unavailable
        );
        assert_eq!(health[&StorageTier::Tier3CDE], HealthStatus::Unknown);
    }

    #[test]
    fn test_router_tier_health_with_adapters() {
        let mut router = StorageRouter::new();
        router.add_tier1(Box::new(MockTier1Adapter {
            name: "pg".to_string(),
        }));
        router.set_tier3(Box::new(MockCDE));

        let health = router.tier_health();
        assert_eq!(
            health[&StorageTier::Tier1PreExisting],
            HealthStatus::Healthy
        );
        assert_eq!(
            health[&StorageTier::Tier2Deployed],
            HealthStatus::Unavailable
        );
        assert_eq!(health[&StorageTier::Tier3CDE], HealthStatus::Healthy);
    }

    #[test]
    fn test_router_hot_swap_remove() {
        let mut router = StorageRouter::new();

        // Add Tier 1 and Tier 2 adapters
        router.add_tier1(Box::new(MockTier1Adapter {
            name: "pg1".to_string(),
        }));
        router.add_tier2(Box::new(MockTier2Adapter {
            name: "redis-c".to_string(),
        }));
        assert_eq!(router.active_tier(), StorageTier::Tier1PreExisting);

        // Remove Tier 1 -> should fall back to Tier 2
        router.remove_tier1(0);
        assert_eq!(router.active_tier(), StorageTier::Tier2Deployed);

        // Remove Tier 2 -> should fall back to Tier 3
        router.remove_tier2(0);
        assert_eq!(router.active_tier(), StorageTier::Tier3CDE);
    }

    #[test]
    fn test_router_adapter_counts() {
        let mut router = StorageRouter::new();
        let counts = router.adapter_counts();
        assert_eq!(counts[&StorageTier::Tier1PreExisting], 0);
        assert_eq!(counts[&StorageTier::Tier2Deployed], 0);
        assert_eq!(counts[&StorageTier::Tier3CDE], 0);

        router.add_tier1(Box::new(MockTier1Adapter {
            name: "a".to_string(),
        }));
        router.add_tier1(Box::new(MockTier1Adapter {
            name: "b".to_string(),
        }));
        router.set_tier3(Box::new(MockCDE));

        let counts = router.adapter_counts();
        assert_eq!(counts[&StorageTier::Tier1PreExisting], 2);
        assert_eq!(counts[&StorageTier::Tier2Deployed], 0);
        assert_eq!(counts[&StorageTier::Tier3CDE], 1);
    }

    #[test]
    fn test_router_remove_out_of_bounds_is_noop() {
        let mut router = StorageRouter::new();
        // Should not panic
        router.remove_tier1(0);
        router.remove_tier1(999);
        router.remove_tier2(0);
        router.remove_tier2(999);
        assert_eq!(router.active_tier(), StorageTier::Tier3CDE);
    }
}
