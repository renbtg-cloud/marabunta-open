// Marabunta - Licensed under the MIT License.
//! CDE Fragment Storage Engine.
//!
//! The CDE (Marabunta Data Engine) provides a fragment-oriented storage layer
//! for the swarm. Data is broken into individually-addressable fragments that
//! can be replicated across nodes and queried via a lightweight SQL-like
//! interface.
//!
//! # Architecture
//!
//! ```text
//!   QueryEngine ──▶ FragmentStore ◀── ReplicationManager
//!                       │
//!                   SQLite (WAL)
//! ```
//!
//! * [`FragmentStore`] — CRUD operations on fragments backed by SQLite.
//! * [`ReplicationManager`] — tracks replica placement and target selection.
//! * [`QueryEngine`] — converts table-level queries into fragment scans.

pub mod fragment;
pub mod replication;
pub mod query;

pub use fragment::{Fragment, FragmentStore};
pub use replication::ReplicationManager;


use std::sync::Arc;

// ============================================================================
// CdeError
// ============================================================================

/// Unified error type for the CDE subsystem.
#[derive(Debug, thiserror::Error)]
pub enum CdeError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("sql parse error: {0}")]
    SqlParse(String),

    #[error("fragment not found: {0}")]
    FragmentNotFound(String),

    #[error("version conflict: stored={stored}, incoming={incoming}")]
    VersionConflict { stored: u64, incoming: u64 },

    #[error("other error: {0}")]
    Other(String),
}

// ============================================================================
// CdeEngine
// ============================================================================

/// Top-level orchestrator that owns the fragment store and exposes
/// convenience constructors for the replication manager and query engine.
pub struct CdeEngine {
    pub store: Arc<FragmentStore>,
}

impl CdeEngine {
    /// Create a new CDE engine backed by the given SQLite database path.
    pub fn new(db_path: &std::path::Path, node_id: String) -> Result<Self, CdeError> {
        let store = FragmentStore::new(db_path, node_id)?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Build a [`ReplicationManager`] sharing this engine's store.
    pub fn replication_manager(&self, replication_factor: usize) -> ReplicationManager {
        ReplicationManager::new(Arc::clone(&self.store), replication_factor)
    }

    /// Build a [`QueryEngine`] sharing this engine's store.
    pub fn query_engine(&self, _max_result_rows: usize) -> crate::swarm::cde::query::QueryEngine {
        crate::swarm::cde::query::QueryEngine
    }
}
