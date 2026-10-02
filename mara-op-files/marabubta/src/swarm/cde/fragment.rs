// Marabunta - Licensed under the MIT License.
//! Fragment storage backed by SQLite.
//!
//! A [`Fragment`] is the smallest addressable unit of data in the CDE.
//! Each fragment belongs to a logical table and is keyed by a row-key
//! within that table. Fragments carry a monotonic version counter used
//! for conflict resolution during replication (higher version wins).
//!
//! The [`FragmentStore`] wraps a WAL-mode SQLite connection behind a
//! `parking_lot::Mutex` so it can be shared across async tasks via `Arc`.

use std::path::Path;

use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use super::CdeError;

// ============================================================================
// Fragment
// ============================================================================

/// A single fragment of data in the CDE.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fragment {
    pub fragment_id: String,
    pub table_name: String,
    pub row_key: String,
    pub data: Vec<u8>,
    pub version: u64,
    pub origin_node: String,
    pub created_at: u64,
    pub updated_at: u64,
}

// ============================================================================
// FragmentStore
// ============================================================================

/// SQLite-backed fragment storage.
///
/// All mutations are serialized through a `Mutex<Connection>`.  The database
/// is opened in WAL mode for maximum read concurrency with a single writer.
pub struct FragmentStore {
    conn: Mutex<Connection>,
    #[allow(dead_code)]
    node_id: String,
}

impl FragmentStore {
    /// Open (or create) the fragment database at `db_path`.
    pub fn new(db_path: &Path, node_id: String) -> Result<Self, CdeError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(db_path)?;

        // WAL mode for concurrent readers + single writer.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        let store = Self {
            conn: Mutex::new(conn),
            node_id,
        };
        store.init_tables()?;
        Ok(store)
    }

    /// Create the schema if it does not already exist.
    fn init_tables(&self) -> Result<(), CdeError> {
        let conn = self.conn.lock();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS cde_fragments (
                fragment_id  TEXT PRIMARY KEY,
                table_name   TEXT NOT NULL,
                row_key      TEXT NOT NULL,
                data         BLOB NOT NULL,
                version      INTEGER NOT NULL,
                origin_node  TEXT NOT NULL,
                created_at   INTEGER NOT NULL,
                updated_at   INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_cde_fragments_table
                ON cde_fragments (table_name);

            CREATE INDEX IF NOT EXISTS idx_cde_fragments_table_key
                ON cde_fragments (table_name, row_key);",
        )?;
        Ok(())
    }

    /// Insert a fragment, replacing any existing row with the same id.
    pub fn insert_fragment(&self, fragment: &Fragment) -> Result<(), CdeError> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT OR REPLACE INTO cde_fragments
                (fragment_id, table_name, row_key, data, version, origin_node, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                fragment.fragment_id,
                fragment.table_name,
                fragment.row_key,
                fragment.data,
                fragment.version,
                fragment.origin_node,
                fragment.created_at,
                fragment.updated_at,
            ],
        )?;
        Ok(())
    }

    /// Fetch a fragment by its unique id, or `None` if not found.
    pub fn get_fragment(&self, fragment_id: &str) -> Result<Option<Fragment>, CdeError> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT fragment_id, table_name, row_key, data, version, origin_node, created_at, updated_at
             FROM cde_fragments WHERE fragment_id = ?1",
        )?;
        let frag = stmt
            .query_row(params![fragment_id], |row| {
                Ok(Fragment {
                    fragment_id: row.get(0)?,
                    table_name: row.get(1)?,
                    row_key: row.get(2)?,
                    data: row.get(3)?,
                    version: row.get(4)?,
                    origin_node: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                })
            })
            .optional()?;
        Ok(frag)
    }

    /// Look up a fragment by (table_name, row_key). Returns the first match.
    pub fn get_by_table_key(
        &self,
        table_name: &str,
        row_key: &str,
    ) -> Result<Option<Fragment>, CdeError> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT fragment_id, table_name, row_key, data, version, origin_node, created_at, updated_at
             FROM cde_fragments WHERE table_name = ?1 AND row_key = ?2 LIMIT 1",
        )?;
        let frag = stmt
            .query_row(params![table_name, row_key], |row| {
                Ok(Fragment {
                    fragment_id: row.get(0)?,
                    table_name: row.get(1)?,
                    row_key: row.get(2)?,
                    data: row.get(3)?,
                    version: row.get(4)?,
                    origin_node: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                })
            })
            .optional()?;
        Ok(frag)
    }

    /// Update a fragment only when the incoming version is strictly greater
    /// than the stored version. Returns `true` if the update was applied.
    pub fn update_fragment(&self, fragment: &Fragment) -> Result<bool, CdeError> {
        // Check the current version first.
        if let Some(existing) = self.get_fragment(&fragment.fragment_id)? {
            if fragment.version <= existing.version {
                return Ok(false);
            }
        }
        self.insert_fragment(fragment)?;
        Ok(true)
    }

    /// Delete a fragment by id. Returns `true` if a row was actually removed.
    pub fn delete_fragment(&self, fragment_id: &str) -> Result<bool, CdeError> {
        let conn = self.conn.lock();
        let deleted = conn.execute(
            "DELETE FROM cde_fragments WHERE fragment_id = ?1",
            params![fragment_id],
        )?;
        Ok(deleted > 0)
    }

    /// List all fragments belonging to a given table.
    pub fn list_fragments_for_table(&self, table_name: &str) -> Result<Vec<Fragment>, CdeError> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT fragment_id, table_name, row_key, data, version, origin_node, created_at, updated_at
             FROM cde_fragments WHERE table_name = ?1 ORDER BY row_key",
        )?;
        let rows = stmt
            .query_map(params![table_name], |row| {
                Ok(Fragment {
                    fragment_id: row.get(0)?,
                    table_name: row.get(1)?,
                    row_key: row.get(2)?,
                    data: row.get(3)?,
                    version: row.get(4)?,
                    origin_node: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a temporary store with a unique database file.
    fn temp_store() -> FragmentStore {
        let dir = std::env::temp_dir().join(format!("cde_frag_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("fragments.db");
        FragmentStore::new(&db_path, "test-node".into()).unwrap()
    }

    fn sample_fragment(id: &str, table: &str, key: &str, version: u64) -> Fragment {
        Fragment {
            fragment_id: id.into(),
            table_name: table.into(),
            row_key: key.into(),
            data: b"hello".to_vec(),
            version,
            origin_node: "node-A".into(),
            created_at: 1000,
            updated_at: 1000 + version,
        }
    }

    #[test]
    fn test_init_tables_idempotent() {
        let store = temp_store();
        // Calling init_tables again should not fail.
        store.init_tables().unwrap();
        store.init_tables().unwrap();
    }

    #[test]
    fn test_insert_and_get() {
        let store = temp_store();
        let frag = sample_fragment("f1", "users", "row-1", 1);
        store.insert_fragment(&frag).unwrap();

        let got = store.get_fragment("f1").unwrap().expect("should exist");
        assert_eq!(got.fragment_id, "f1");
        assert_eq!(got.table_name, "users");
        assert_eq!(got.row_key, "row-1");
        assert_eq!(got.data, b"hello");
        assert_eq!(got.version, 1);
    }

    #[test]
    fn test_get_nonexistent() {
        let store = temp_store();
        assert!(store.get_fragment("nope").unwrap().is_none());
    }

    #[test]
    fn test_get_by_table_key() {
        let store = temp_store();
        let frag = sample_fragment("f2", "orders", "ord-42", 3);
        store.insert_fragment(&frag).unwrap();

        let got = store
            .get_by_table_key("orders", "ord-42")
            .unwrap()
            .expect("should find by table+key");
        assert_eq!(got.fragment_id, "f2");
        assert_eq!(got.version, 3);

        // Miss on wrong key.
        assert!(store.get_by_table_key("orders", "ord-99").unwrap().is_none());
    }

    #[test]
    fn test_update_version_check() {
        let store = temp_store();
        let frag_v1 = sample_fragment("f3", "items", "i-1", 1);
        store.insert_fragment(&frag_v1).unwrap();

        // Same version should be rejected.
        let frag_v1_dup = sample_fragment("f3", "items", "i-1", 1);
        assert!(!store.update_fragment(&frag_v1_dup).unwrap());

        // Lower version should be rejected.
        let frag_v0 = sample_fragment("f3", "items", "i-1", 0);
        assert!(!store.update_fragment(&frag_v0).unwrap());

        // Higher version should succeed.
        let frag_v2 = sample_fragment("f3", "items", "i-1", 2);
        assert!(store.update_fragment(&frag_v2).unwrap());

        let got = store.get_fragment("f3").unwrap().unwrap();
        assert_eq!(got.version, 2);
    }

    #[test]
    fn test_update_nonexistent_inserts() {
        let store = temp_store();
        let frag = sample_fragment("f-new", "tbl", "k", 5);
        // update_fragment on a missing row should insert it.
        assert!(store.update_fragment(&frag).unwrap());
        assert!(store.get_fragment("f-new").unwrap().is_some());
    }

    #[test]
    fn test_delete() {
        let store = temp_store();
        let frag = sample_fragment("f4", "logs", "l-1", 1);
        store.insert_fragment(&frag).unwrap();

        assert!(store.delete_fragment("f4").unwrap());
        assert!(store.get_fragment("f4").unwrap().is_none());

        // Deleting again should return false.
        assert!(!store.delete_fragment("f4").unwrap());
    }

    #[test]
    fn test_list_fragments_for_table() {
        let store = temp_store();
        store
            .insert_fragment(&sample_fragment("a1", "alpha", "k1", 1))
            .unwrap();
        store
            .insert_fragment(&sample_fragment("a2", "alpha", "k2", 1))
            .unwrap();
        store
            .insert_fragment(&sample_fragment("b1", "beta", "k1", 1))
            .unwrap();

        let alpha = store.list_fragments_for_table("alpha").unwrap();
        assert_eq!(alpha.len(), 2);
        assert!(alpha.iter().all(|f| f.table_name == "alpha"));

        let beta = store.list_fragments_for_table("beta").unwrap();
        assert_eq!(beta.len(), 1);

        let empty = store.list_fragments_for_table("gamma").unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_insert_replace_semantics() {
        let store = temp_store();
        let frag_v1 = sample_fragment("dup", "t", "k", 1);
        store.insert_fragment(&frag_v1).unwrap();

        // insert_fragment with the same id but different data should replace.
        let mut frag_v2 = sample_fragment("dup", "t", "k", 2);
        frag_v2.data = b"world".to_vec();
        store.insert_fragment(&frag_v2).unwrap();

        let got = store.get_fragment("dup").unwrap().unwrap();
        assert_eq!(got.version, 2);
        assert_eq!(got.data, b"world");
    }
}
