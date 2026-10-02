// Marabunta - Licensed under the MIT License.
//! Trait definitions for storage tier adapters.
//!
//! Each storage tier has a dedicated trait that adapters implement:
//! - **PreExistingAdapter** (Tier 1): connects to databases already running on the node.
//! - **DeployedAdapter** (Tier 2): manages swarm-deployed containerized databases.
//! - **MarabuntaEngine** (Tier 3): the always-available distributed engine (CDE).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::types::{HealthStatus, ResultSet, StorageError, Value};

// ---------------------------------------------------------------------------
// PreExistingAdapter (Tier 1)
// ---------------------------------------------------------------------------

/// Adapter for connecting to pre-existing (Tier 1) databases discovered
/// by the capability scanner.
#[async_trait]
pub trait PreExistingAdapter: Send + Sync {
    /// Connect to the pre-existing database. Called once on discovery.
    async fn connect(&mut self) -> Result<(), StorageError>;

    /// Check if the database is healthy and responsive.
    async fn health_check(&self) -> Result<HealthStatus, StorageError>;

    /// Execute a query against this database.
    async fn execute_query(&self, sql: &str, args: &[Value]) -> Result<ResultSet, StorageError>;

    /// Disconnect cleanly from the database.
    async fn disconnect(&mut self) -> Result<(), StorageError>;

    /// Human-readable name for this adapter (e.g., "PostgreSQL @ 10.0.1.5:5432").
    fn display_name(&self) -> String;
}

// ---------------------------------------------------------------------------
// DeployedAdapter (Tier 2)
// ---------------------------------------------------------------------------

/// Status of a swarm-deployed database container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentStatus {
    Provisioning,
    Running,
    Stopping,
    Stopped,
    Failed(String),
}

impl std::fmt::Display for DeploymentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provisioning => write!(f, "Provisioning"),
            Self::Running => write!(f, "Running"),
            Self::Stopping => write!(f, "Stopping"),
            Self::Stopped => write!(f, "Stopped"),
            Self::Failed(reason) => write!(f, "Failed: {}", reason),
        }
    }
}

/// Adapter for swarm-deployed (Tier 2) containerized databases.
/// Extends `PreExistingAdapter` with lifecycle management.
#[async_trait]
pub trait DeployedAdapter: PreExistingAdapter {
    /// Deploy a new container instance. Human-authorized only.
    async fn deploy(&mut self) -> Result<(), StorageError>;

    /// Destroy the container instance. Human-authorized only.
    async fn destroy(&mut self) -> Result<(), StorageError>;

    /// Current deployment status.
    async fn status(&self) -> Result<DeploymentStatus, StorageError>;
}

// ---------------------------------------------------------------------------
// MarabuntaEngine (Tier 3)
// ---------------------------------------------------------------------------

/// Fragment identifier for CDE data distribution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FragmentId(pub String);

impl std::fmt::Display for FragmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Report from a replication health check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationReport {
    pub total_fragments: usize,
    pub fully_replicated: usize,
    pub under_replicated: usize,
    pub replication_factor: usize,
}

/// The Tier 3 interface: distributed query engine using SQLite fragments
/// and gossip coordination. This is the "always available" fallback and
/// canonical source of truth.
#[async_trait]
pub trait MarabuntaEngine: Send + Sync {
    /// Store a fragment locally and replicate to N neighbors.
    async fn put_fragment(&self, id: &FragmentId, data: &[u8]) -> Result<(), StorageError>;

    /// Retrieve a fragment by ID (local or remote fetch).
    async fn get_fragment(&self, id: &FragmentId) -> Result<Option<Vec<u8>>, StorageError>;

    /// Delete a fragment (propagates tombstone).
    async fn delete_fragment(&self, id: &FragmentId) -> Result<(), StorageError>;

    /// List all fragment IDs stored locally.
    async fn local_fragments(&self) -> Result<Vec<FragmentId>, StorageError>;

    /// Trigger replication check -- ensure all fragments meet replication factor.
    async fn check_replication(&self) -> Result<ReplicationReport, StorageError>;

    /// Execute a distributed SQL query across CDE fragments.
    async fn distributed_query(&self, sql: &str, args: &[Value]) -> Result<ResultSet, StorageError>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_status_display() {
        assert_eq!(DeploymentStatus::Provisioning.to_string(), "Provisioning");
        assert_eq!(DeploymentStatus::Running.to_string(), "Running");
        assert_eq!(DeploymentStatus::Stopping.to_string(), "Stopping");
        assert_eq!(DeploymentStatus::Stopped.to_string(), "Stopped");
        assert_eq!(
            DeploymentStatus::Failed("oom".to_string()).to_string(),
            "Failed: oom"
        );
    }

    #[test]
    fn test_fragment_id_display() {
        let fid = FragmentId("frag-0042".to_string());
        assert_eq!(fid.to_string(), "frag-0042");
    }

    #[test]
    fn test_replication_report_fields() {
        let report = ReplicationReport {
            total_fragments: 100,
            fully_replicated: 95,
            under_replicated: 5,
            replication_factor: 3,
        };
        assert_eq!(report.total_fragments, 100);
        assert_eq!(report.fully_replicated, 95);
        assert_eq!(report.under_replicated, 5);
        assert_eq!(report.replication_factor, 3);
    }
}
