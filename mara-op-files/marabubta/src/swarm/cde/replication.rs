// Marabunta - Licensed under the MIT License.
//! Replication tracking for CDE fragments.
//!
//! The [`ReplicationManager`] decides where replicas should be placed and
//! tracks whether each target node has confirmed receipt. It does *not*
//! perform the actual network transfer — that is the responsibility of the
//! gossip or transport layer. Instead it provides:
//!
//! * **Target selection** — deterministic, excluding the origin node and an
//!   explicit exclude list.
//! * **Confirmation tracking** — a SQLite table recording which replicas
//!   have been confirmed, used to drive re-replication on failure.

use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use super::fragment::FragmentStore;
use super::CdeError;

// ============================================================================
// ReplicaRecord
// ============================================================================

/// Tracks a single replica placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaRecord {
    pub fragment_id: String,
    pub replica_node: String,
    pub confirmed: bool,
    pub replicated_at: u64,
}

// ============================================================================
// ReplicationManager
// ============================================================================

/// Manages replica target selection and confirmation tracking.
pub struct ReplicationManager {
    #[allow(dead_code)]
    store: Arc<FragmentStore>,
    replication_factor: usize,
    meta_conn: Mutex<Connection>,
}

impl ReplicationManager {
    /// Create a new replication manager.
    ///
    /// The manager shares the same [`FragmentStore`] for fragment lookups
    /// and maintains its own SQLite connection for the replication metadata
    /// table (co-located in the same directory as the fragment database).
    pub fn new(store: Arc<FragmentStore>, replication_factor: usize) -> Self {
        // Open an in-memory database for replication metadata by default.
        // In production this would be a file next to the fragment DB, but
        // using :memory: keeps tests simple and avoids path coupling.
        let conn = Connection::open_in_memory()
            .expect("failed to open in-memory replication metadata db");
        conn.pragma_update(None, "journal_mode", "WAL").ok();
        conn.pragma_update(None, "synchronous", "NORMAL").ok();

        let mgr = Self {
            store,
            replication_factor,
            meta_conn: Mutex::new(conn),
        };
        mgr.init_tables().expect("failed to init replication tables");
        mgr
    }

    /// Create the replication tracking table.
    fn init_tables(&self) -> Result<(), CdeError> {
        let conn = self.meta_conn.lock();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS cde_replication (
                fragment_id   TEXT NOT NULL,
                replica_node  TEXT NOT NULL,
                confirmed     INTEGER NOT NULL DEFAULT 0,
                replicated_at INTEGER NOT NULL,
                PRIMARY KEY (fragment_id, replica_node)
            );

            CREATE INDEX IF NOT EXISTS idx_cde_replication_fragment
                ON cde_replication (fragment_id);",
        )?;
        Ok(())
    }

    /// Select up to `replication_factor` target nodes from `known_nodes`,
    /// excluding the `origin` node and any nodes in `exclude`.
    ///
    /// The selection is deterministic (sorted order) so that all nodes in
    /// the swarm agree on the canonical replica set for a given fragment.
    pub fn select_replication_targets(
        &self,
        origin: &str,
        known_nodes: &[String],
        exclude: &[String],
    ) -> Vec<String> {
        let mut candidates: Vec<&String> = known_nodes
            .iter()
            .filter(|n| n.as_str() != origin && !exclude.contains(n))
            .collect();

        // Deterministic ordering.
        candidates.sort();

        candidates
            .into_iter()
            .take(self.replication_factor)
            .cloned()
            .collect()
    }

    /// Record that a fragment has been sent to a replica node.
    pub fn record_replication(
        &self,
        fragment_id: &str,
        replica_node: &str,
        replicated_at: u64,
    ) -> Result<(), CdeError> {
        let conn = self.meta_conn.lock();
        conn.execute(
            "INSERT OR REPLACE INTO cde_replication
                (fragment_id, replica_node, confirmed, replicated_at)
             VALUES (?1, ?2, 0, ?3)",
            params![fragment_id, replica_node, replicated_at],
        )?;
        Ok(())
    }

    /// Mark a replica as confirmed (the target acknowledged receipt).
    pub fn confirm_replication(
        &self,
        fragment_id: &str,
        replica_node: &str,
    ) -> Result<bool, CdeError> {
        let conn = self.meta_conn.lock();
        let updated = conn.execute(
            "UPDATE cde_replication SET confirmed = 1
             WHERE fragment_id = ?1 AND replica_node = ?2",
            params![fragment_id, replica_node],
        )?;
        Ok(updated > 0)
    }

    /// Get all replica records for a fragment.
    pub fn get_replicas(&self, fragment_id: &str) -> Result<Vec<ReplicaRecord>, CdeError> {
        let conn = self.meta_conn.lock();
        let mut stmt = conn.prepare(
            "SELECT fragment_id, replica_node, confirmed, replicated_at
             FROM cde_replication WHERE fragment_id = ?1",
        )?;
        let rows = stmt
            .query_map(params![fragment_id], |row| {
                Ok(ReplicaRecord {
                    fragment_id: row.get(0)?,
                    replica_node: row.get(1)?,
                    confirmed: row.get::<_, i32>(2)? != 0,
                    replicated_at: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Count unconfirmed replicas for a fragment.
    pub fn unconfirmed_count(&self, fragment_id: &str) -> Result<usize, CdeError> {
        let conn = self.meta_conn.lock();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM cde_replication
             WHERE fragment_id = ?1 AND confirmed = 0",
            params![fragment_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Remove all replication records for a fragment (e.g. after deletion).
    pub fn remove_replication_records(&self, fragment_id: &str) -> Result<usize, CdeError> {
        let conn = self.meta_conn.lock();
        let deleted = conn.execute(
            "DELETE FROM cde_replication WHERE fragment_id = ?1",
            params![fragment_id],
        )?;
        Ok(deleted)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::cde::fragment::FragmentStore;

    fn temp_store() -> Arc<FragmentStore> {
        let dir = std::env::temp_dir().join(format!("cde_repl_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("fragments.db");
        Arc::new(FragmentStore::new(&db_path, "test-node".into()).unwrap())
    }

    #[test]
    fn test_select_targets_excludes_origin() {
        let store = temp_store();
        let mgr = ReplicationManager::new(store, 3);

        let nodes: Vec<String> = vec![
            "node-A".into(),
            "node-B".into(),
            "node-C".into(),
            "node-D".into(),
        ];
        let targets = mgr.select_replication_targets("node-A", &nodes, &[]);
        assert!(!targets.contains(&"node-A".to_string()));
        assert_eq!(targets.len(), 3);
    }

    #[test]
    fn test_select_targets_respects_factor() {
        let store = temp_store();
        let mgr = ReplicationManager::new(store, 2);

        let nodes: Vec<String> = vec![
            "node-A".into(),
            "node-B".into(),
            "node-C".into(),
            "node-D".into(),
            "node-E".into(),
        ];
        let targets = mgr.select_replication_targets("node-A", &nodes, &[]);
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn test_select_targets_excludes_list() {
        let store = temp_store();
        let mgr = ReplicationManager::new(store, 3);

        let nodes: Vec<String> = vec![
            "node-A".into(),
            "node-B".into(),
            "node-C".into(),
            "node-D".into(),
        ];
        let exclude = vec!["node-B".to_string()];
        let targets = mgr.select_replication_targets("node-A", &nodes, &exclude);
        assert!(!targets.contains(&"node-A".to_string()));
        assert!(!targets.contains(&"node-B".to_string()));
        assert_eq!(targets.len(), 2); // C and D
    }

    #[test]
    fn test_record_and_confirm_replication() {
        let store = temp_store();
        let mgr = ReplicationManager::new(store, 3);

        mgr.record_replication("f1", "node-B", 1000).unwrap();
        mgr.record_replication("f1", "node-C", 1001).unwrap();

        assert_eq!(mgr.unconfirmed_count("f1").unwrap(), 2);

        let confirmed = mgr.confirm_replication("f1", "node-B").unwrap();
        assert!(confirmed);
        assert_eq!(mgr.unconfirmed_count("f1").unwrap(), 1);

        let replicas = mgr.get_replicas("f1").unwrap();
        assert_eq!(replicas.len(), 2);
        let b_rec = replicas.iter().find(|r| r.replica_node == "node-B").unwrap();
        assert!(b_rec.confirmed);
    }

    #[test]
    fn test_remove_replication_records() {
        let store = temp_store();
        let mgr = ReplicationManager::new(store, 3);

        mgr.record_replication("f2", "node-X", 2000).unwrap();
        mgr.record_replication("f2", "node-Y", 2001).unwrap();

        let removed = mgr.remove_replication_records("f2").unwrap();
        assert_eq!(removed, 2);
        assert!(mgr.get_replicas("f2").unwrap().is_empty());
    }
}
