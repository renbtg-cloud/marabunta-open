// Marabunta - Licensed under the MIT License.
//! Three-Tier Storage Router for the Marabunta Swarm.
//!
//! Provides a unified storage abstraction across three tiers:
//! - **Tier 1**: Pre-existing databases detected on nodes (PostgreSQL, MySQL, etc.)
//! - **Tier 2**: Swarm-deployed containerized databases (human-authorized)
//! - **Tier 3**: Marabunta Distributed Engine -- always available (SQLite fragments + gossip)
//!
//! # The Golden Rule
//!
//! Writes always go to Tier 3 first (source of truth), then async replicate
//! up to Tier 1/2 for query performance. Reads use the fastest available
//! tier: Tier 1 > Tier 2 > Tier 3.
//!
//! Tier 3 is the bedrock. If Tier 1 and Tier 2 disappear, the swarm
//! continues at Tier 3 without interruption.

pub mod adapters;
pub mod health;
pub mod router;
pub mod types;

// Re-exports for convenience
pub use adapters::{
    MarabuntaEngine, DeployedAdapter, DeploymentStatus, FragmentId, PreExistingAdapter,
    ReplicationReport,
};
pub use health::{TierHealth, TierHealthMonitor};
pub use router::StorageRouter;
pub use types::{AuditEvent, EventFilter, HealthStatus, ResultSet, StorageError, Value};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// StorageTier
// ---------------------------------------------------------------------------

/// The three-tier taxonomy for the Marabunta storage model.
///
/// Ordering: Tier1 < Tier2 < Tier3 (lower = faster/preferred for reads).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StorageTier {
    /// Pre-existing databases detected on the node (PostgreSQL, MySQL, etc.)
    /// Fastest for reads. Pure optimization -- swarm never depends on this.
    Tier1PreExisting,

    /// Swarm-deployed containerized databases (human-authorized).
    /// Good performance. Managed lifecycle.
    Tier2Deployed,

    /// Marabunta Distributed Engine -- always available.
    /// SQLite fragments + gossip coordination. Zero infrastructure required.
    Tier3CDE,
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tier1PreExisting => write!(f, "Tier 1 (Pre-Existing)"),
            Self::Tier2Deployed => write!(f, "Tier 2 (Deployed)"),
            Self::Tier3CDE => write!(f, "Tier 3 (CDE)"),
        }
    }
}

// ---------------------------------------------------------------------------
// SwarmStore trait
// ---------------------------------------------------------------------------

/// Unified storage interface for the swarm.
///
/// Implementations automatically route operations to the best available
/// tier. Writes always succeed at Tier 3 minimum. Reads use the fastest
/// available tier.
#[async_trait]
pub trait SwarmStore: Send + Sync {
    // -- Writes (always succeed at Tier 3 minimum) --

    /// Persist an audit event. Written to Tier 3 first, then async
    /// replicated up to Tier 1/2 for query performance.
    async fn write_event(&self, event: AuditEvent) -> Result<(), StorageError>;

    /// Store a key-value pair. Tier 3 is source of truth.
    async fn put_state(&self, key: &str, value: &[u8]) -> Result<(), StorageError>;

    /// Delete a key. Propagated as a tombstone through all tiers.
    async fn delete_state(&self, key: &str) -> Result<(), StorageError>;

    // -- Reads (use best available tier for speed) --

    /// Query audit events matching the given filter.
    async fn query_events(&self, filter: EventFilter) -> Result<Vec<AuditEvent>, StorageError>;

    /// Retrieve a value by key. Returns None if not found.
    async fn get_state(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;

    // -- SQL-compatible queries --

    /// Execute a SQL query. Routes to Tier 1/2 if available, Tier 3 otherwise.
    async fn query_sql(&self, sql: &str, args: &[Value]) -> Result<ResultSet, StorageError>;

    // -- Metadata --

    /// The tier currently being used for reads.
    fn current_tier(&self) -> StorageTier;

    /// All tiers that are currently reachable.
    fn available_tiers(&self) -> Vec<StorageTier>;

    /// Health status of each known tier.
    fn tier_health(&self) -> HashMap<StorageTier, HealthStatus>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_ordering() {
        assert!(StorageTier::Tier1PreExisting < StorageTier::Tier2Deployed);
        assert!(StorageTier::Tier2Deployed < StorageTier::Tier3CDE);
        assert!(StorageTier::Tier1PreExisting < StorageTier::Tier3CDE);
    }

    #[test]
    fn test_tier_display() {
        assert_eq!(
            StorageTier::Tier1PreExisting.to_string(),
            "Tier 1 (Pre-Existing)"
        );
        assert_eq!(
            StorageTier::Tier2Deployed.to_string(),
            "Tier 2 (Deployed)"
        );
        assert_eq!(StorageTier::Tier3CDE.to_string(), "Tier 3 (CDE)");
    }

    #[test]
    fn test_tier_serialize_deserialize() {
        let tier = StorageTier::Tier2Deployed;
        let json = serde_json::to_string(&tier).unwrap();
        let deserialized: StorageTier = serde_json::from_str(&json).unwrap();
        assert_eq!(tier, deserialized);
    }

    #[test]
    fn test_all_tiers_sorted() {
        let mut tiers = vec![
            StorageTier::Tier3CDE,
            StorageTier::Tier1PreExisting,
            StorageTier::Tier2Deployed,
        ];
        tiers.sort();
        assert_eq!(
            tiers,
            vec![
                StorageTier::Tier1PreExisting,
                StorageTier::Tier2Deployed,
                StorageTier::Tier3CDE,
            ]
        );
    }
}
