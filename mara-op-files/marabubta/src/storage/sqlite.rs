// Marabunta - Licensed under the MIT License.
//! SQLite-backed persistence implementation
//!
//! Provides a durable key-value store using SQLite with namespace support.
//! Suitable for development, testing, and single-node deployments.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::Mutex;

use super::persistence::{PersistenceBackend, PersistenceError};

/// SQLite-backed persistence backend
///
/// Uses a simple key-value table with namespace partitioning.
/// All operations are performed through a mutex-protected connection,
/// making it safe for concurrent async access.
pub struct SqliteBackend {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteBackend {
    /// Create a new SQLite backend with a file path
    ///
    /// Creates the database file and parent directories if they don't exist.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, PersistenceError> {
        let path = path.as_ref();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let conn = Connection::open(path)
            .map_err(|e| PersistenceError::Database(format!("Failed to open database: {}", e)))?;

        let backend = Self {
            conn: Arc::new(Mutex::new(conn)),
        };

        // Initialize schema synchronously since we're in a constructor
        backend.init_schema_sync()?;

        Ok(backend)
    }

    /// Create an in-memory database for testing
    ///
    /// Data is lost when the backend is dropped.
    pub fn in_memory() -> Result<Self, PersistenceError> {
        let conn = Connection::open_in_memory().map_err(|e| {
            PersistenceError::Database(format!("Failed to open in-memory db: {}", e))
        })?;

        let backend = Self {
            conn: Arc::new(Mutex::new(conn)),
        };

        backend.init_schema_sync()?;

        Ok(backend)
    }

    /// Initialize the database schema synchronously
    fn init_schema_sync(&self) -> Result<(), PersistenceError> {
        // We need to block on the async lock for initialization
        // This is safe because we're in the constructor and no other
        // references to self exist yet
        let conn = futures::executor::block_on(self.conn.lock());

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS kv_store (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                value BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (namespace, key)
            );

            CREATE INDEX IF NOT EXISTS idx_namespace ON kv_store(namespace);
            CREATE INDEX IF NOT EXISTS idx_namespace_key_prefix ON kv_store(namespace, key);
            "#,
        )
        .map_err(|e| PersistenceError::Database(format!("Failed to initialize schema: {}", e)))?;

        Ok(())
    }

    /// Initialize the database schema asynchronously
    #[allow(dead_code)]
    pub async fn init_schema(&self) -> Result<(), PersistenceError> {
        let conn = self.conn.lock().await;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS kv_store (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                value BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (namespace, key)
            );

            CREATE INDEX IF NOT EXISTS idx_namespace ON kv_store(namespace);
            CREATE INDEX IF NOT EXISTS idx_namespace_key_prefix ON kv_store(namespace, key);
            "#,
        )
        .map_err(|e| PersistenceError::Database(format!("Failed to initialize schema: {}", e)))?;

        Ok(())
    }

    /// Get the current Unix timestamp in seconds
    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64
    }
}

#[async_trait]
impl PersistenceBackend for SqliteBackend {
    async fn put(&self, namespace: &str, key: &str, value: &[u8]) -> Result<(), PersistenceError> {
        let conn = self.conn.lock().await;
        let now = Self::now();

        conn.execute(
            r#"
            INSERT INTO kv_store (namespace, key, value, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?4)
            ON CONFLICT(namespace, key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            "#,
            params![namespace, key, value, now],
        )
        .map_err(|e| PersistenceError::Database(format!("Put failed: {}", e)))?;

        Ok(())
    }

    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, PersistenceError> {
        let conn = self.conn.lock().await;

        let result: Option<Vec<u8>> = conn
            .query_row(
                "SELECT value FROM kv_store WHERE namespace = ?1 AND key = ?2",
                params![namespace, key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| PersistenceError::Database(format!("Get failed: {}", e)))?;

        Ok(result)
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
        let conn = self.conn.lock().await;

        let rows_affected = conn
            .execute(
                "DELETE FROM kv_store WHERE namespace = ?1 AND key = ?2",
                params![namespace, key],
            )
            .map_err(|e| PersistenceError::Database(format!("Delete failed: {}", e)))?;

        Ok(rows_affected > 0)
    }

    async fn list_keys(
        &self,
        namespace: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, PersistenceError> {
        let conn = self.conn.lock().await;

        let keys = match prefix {
            Some(prefix) => {
                let pattern = format!("{}%", prefix);
                let mut stmt = conn
                    .prepare(
                        "SELECT key FROM kv_store WHERE namespace = ?1 AND key LIKE ?2 ORDER BY key",
                    )
                    .map_err(|e| PersistenceError::Database(format!("Prepare failed: {}", e)))?;

                let rows = stmt
                    .query_map(params![namespace, pattern], |row| row.get::<_, String>(0))
                    .map_err(|e| PersistenceError::Database(format!("Query failed: {}", e)))?;

                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| PersistenceError::Database(format!("Row fetch failed: {}", e)))?
            }
            None => {
                let mut stmt = conn
                    .prepare("SELECT key FROM kv_store WHERE namespace = ?1 ORDER BY key")
                    .map_err(|e| PersistenceError::Database(format!("Prepare failed: {}", e)))?;

                let rows = stmt
                    .query_map(params![namespace], |row| row.get::<_, String>(0))
                    .map_err(|e| PersistenceError::Database(format!("Query failed: {}", e)))?;

                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| PersistenceError::Database(format!("Row fetch failed: {}", e)))?
            }
        };

        Ok(keys)
    }

    async fn exists(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
        let conn = self.conn.lock().await;

        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM kv_store WHERE namespace = ?1 AND key = ?2)",
                params![namespace, key],
                |row| row.get(0),
            )
            .map_err(|e| PersistenceError::Database(format!("Exists check failed: {}", e)))?;

        Ok(exists)
    }

    async fn batch_put(
        &self,
        namespace: &str,
        items: &[(&str, &[u8])],
    ) -> Result<(), PersistenceError> {
        let conn = self.conn.lock().await;
        let now = Self::now();

        // Use a transaction for atomicity
        conn.execute("BEGIN TRANSACTION", [])
            .map_err(|e| PersistenceError::Transaction(format!("Failed to begin: {}", e)))?;

        let result = (|| {
            for (key, value) in items {
                conn.execute(
                    r#"
                    INSERT INTO kv_store (namespace, key, value, created_at, updated_at)
                    VALUES (?1, ?2, ?3, ?4, ?4)
                    ON CONFLICT(namespace, key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    "#,
                    params![namespace, *key, *value, now],
                )
                .map_err(|e| PersistenceError::Database(format!("Batch put failed: {}", e)))?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                conn.execute("COMMIT", []).map_err(|e| {
                    PersistenceError::Transaction(format!("Failed to commit: {}", e))
                })?;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", []);
                Err(e)
            }
        }
    }

    async fn clear_namespace(&self, namespace: &str) -> Result<u64, PersistenceError> {
        let conn = self.conn.lock().await;

        let rows_affected = conn
            .execute(
                "DELETE FROM kv_store WHERE namespace = ?1",
                params![namespace],
            )
            .map_err(|e| PersistenceError::Database(format!("Clear failed: {}", e)))?;

        Ok(rows_affected as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    use crate::storage::persistence::TypedStore;

    #[tokio::test]
    async fn test_put_get_delete_cycle() {
        let backend = SqliteBackend::in_memory().unwrap();

        // Put
        backend.put("test", "key1", b"value1").await.unwrap();

        // Get
        let value = backend.get("test", "key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Exists
        assert!(backend.exists("test", "key1").await.unwrap());

        // Delete
        let deleted = backend.delete("test", "key1").await.unwrap();
        assert!(deleted);

        // Verify deleted
        let value = backend.get("test", "key1").await.unwrap();
        assert_eq!(value, None);
        assert!(!backend.exists("test", "key1").await.unwrap());

        // Delete non-existent
        let deleted = backend.delete("test", "key1").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_list_keys_with_prefix() {
        let backend = SqliteBackend::in_memory().unwrap();

        // Insert test data
        backend.put("ns1", "user:1", b"alice").await.unwrap();
        backend.put("ns1", "user:2", b"bob").await.unwrap();
        backend.put("ns1", "user:10", b"charlie").await.unwrap();
        backend.put("ns1", "config:timeout", b"30").await.unwrap();
        backend.put("ns2", "user:3", b"dave").await.unwrap();

        // List all keys in ns1
        let mut keys = backend.list_keys("ns1", None).await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["config:timeout", "user:1", "user:10", "user:2"]);

        // List with prefix
        let mut keys = backend.list_keys("ns1", Some("user:")).await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["user:1", "user:10", "user:2"]);

        // List with more specific prefix
        let keys = backend.list_keys("ns1", Some("user:1")).await.unwrap();
        // Should include user:1 and user:10
        assert!(keys.contains(&"user:1".to_string()));
        assert!(keys.contains(&"user:10".to_string()));
        assert!(!keys.contains(&"user:2".to_string()));

        // Different namespace should be isolated
        let keys = backend.list_keys("ns2", None).await.unwrap();
        assert_eq!(keys, vec!["user:3"]);
    }

    #[tokio::test]
    async fn test_batch_put_atomicity() {
        let backend = SqliteBackend::in_memory().unwrap();

        // Batch put
        let items: Vec<(&str, &[u8])> = vec![
            ("batch1", b"value1"),
            ("batch2", b"value2"),
            ("batch3", b"value3"),
        ];
        backend.batch_put("atomic", &items).await.unwrap();

        // Verify all values exist
        assert_eq!(
            backend.get("atomic", "batch1").await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            backend.get("atomic", "batch2").await.unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            backend.get("atomic", "batch3").await.unwrap(),
            Some(b"value3".to_vec())
        );

        // Batch update (overwrite)
        let items: Vec<(&str, &[u8])> = vec![("batch1", b"updated1"), ("batch2", b"updated2")];
        backend.batch_put("atomic", &items).await.unwrap();

        assert_eq!(
            backend.get("atomic", "batch1").await.unwrap(),
            Some(b"updated1".to_vec())
        );
        assert_eq!(
            backend.get("atomic", "batch3").await.unwrap(),
            Some(b"value3".to_vec()) // Unchanged
        );
    }

    #[tokio::test]
    async fn test_clear_namespace() {
        let backend = SqliteBackend::in_memory().unwrap();

        // Setup data in multiple namespaces
        backend.put("clear_test", "key1", b"v1").await.unwrap();
        backend.put("clear_test", "key2", b"v2").await.unwrap();
        backend.put("keep_test", "key1", b"v1").await.unwrap();

        // Clear one namespace
        let deleted = backend.clear_namespace("clear_test").await.unwrap();
        assert_eq!(deleted, 2);

        // Verify cleared
        let keys = backend.list_keys("clear_test", None).await.unwrap();
        assert!(keys.is_empty());

        // Other namespace unaffected
        let keys = backend.list_keys("keep_test", None).await.unwrap();
        assert_eq!(keys, vec!["key1"]);
    }

    #[tokio::test]
    async fn test_typed_store_json_serialization() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct TestUser {
            id: u64,
            name: String,
            active: bool,
        }

        let backend = SqliteBackend::in_memory().unwrap();
        let store: TypedStore<SqliteBackend> = TypedStore::new(backend, "users");

        let user = TestUser {
            id: 1,
            name: "Alice".to_string(),
            active: true,
        };

        // Put
        store.put("user:1", &user).await.unwrap();

        // Get
        let retrieved: Option<TestUser> = store.get("user:1").await.unwrap();
        assert_eq!(retrieved, Some(user.clone()));

        // Store more users
        let user2 = TestUser {
            id: 2,
            name: "Bob".to_string(),
            active: false,
        };
        store.put("user:2", &user2).await.unwrap();

        // List keys
        let mut keys = store.list_keys(None).await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["user:1", "user:2"]);

        // Get all
        let mut all: Vec<(String, TestUser)> = store.get_all().await.unwrap();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1, user);
        assert_eq!(all[1].1, user2);

        // Delete
        assert!(store.delete("user:1").await.unwrap());
        assert!(!store.exists("user:1").await.unwrap());
    }

    #[tokio::test]
    async fn test_typed_store_batch_put() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct Config {
            value: String,
        }

        let backend = SqliteBackend::in_memory().unwrap();
        let store: TypedStore<SqliteBackend> = TypedStore::new(backend, "config");

        let items = vec![
            (
                "timeout",
                Config {
                    value: "30".to_string(),
                },
            ),
            (
                "retries",
                Config {
                    value: "3".to_string(),
                },
            ),
            (
                "debug",
                Config {
                    value: "true".to_string(),
                },
            ),
        ];

        store.batch_put(&items).await.unwrap();

        let timeout: Option<Config> = store.get("timeout").await.unwrap();
        assert_eq!(
            timeout,
            Some(Config {
                value: "30".to_string()
            })
        );

        let all: Vec<(String, Config)> = store.get_all().await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_namespace_isolation() {
        let backend = SqliteBackend::in_memory().unwrap();

        // Same key in different namespaces
        backend.put("ns1", "shared_key", b"value1").await.unwrap();
        backend.put("ns2", "shared_key", b"value2").await.unwrap();

        assert_eq!(
            backend.get("ns1", "shared_key").await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            backend.get("ns2", "shared_key").await.unwrap(),
            Some(b"value2".to_vec())
        );

        // Delete from one namespace doesn't affect other
        backend.delete("ns1", "shared_key").await.unwrap();
        assert!(!backend.exists("ns1", "shared_key").await.unwrap());
        assert!(backend.exists("ns2", "shared_key").await.unwrap());
    }

    #[tokio::test]
    async fn test_overwrite_updates_value() {
        let backend = SqliteBackend::in_memory().unwrap();

        backend.put("test", "key", b"original").await.unwrap();
        backend.put("test", "key", b"updated").await.unwrap();

        let value = backend.get("test", "key").await.unwrap();
        assert_eq!(value, Some(b"updated".to_vec()));
    }

    #[tokio::test]
    async fn test_binary_data() {
        let backend = SqliteBackend::in_memory().unwrap();

        // Test with binary data including null bytes
        let binary_data: Vec<u8> = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x42];
        backend.put("binary", "data", &binary_data).await.unwrap();

        let retrieved = backend.get("binary", "data").await.unwrap();
        assert_eq!(retrieved, Some(binary_data));
    }

    #[tokio::test]
    async fn test_empty_operations() {
        let backend = SqliteBackend::in_memory().unwrap();

        // Empty namespace
        let keys = backend.list_keys("empty", None).await.unwrap();
        assert!(keys.is_empty());

        // Clear empty namespace
        let deleted = backend.clear_namespace("empty").await.unwrap();
        assert_eq!(deleted, 0);

        // Empty batch put
        let items: Vec<(&str, &[u8])> = vec![];
        backend.batch_put("empty", &items).await.unwrap();
    }

    #[tokio::test]
    async fn test_special_characters_in_keys() {
        let backend = SqliteBackend::in_memory().unwrap();

        // Keys with special characters
        let special_keys = vec![
            "key/with/slashes",
            "key:with:colons",
            "key.with.dots",
            "key-with-dashes",
            "key_with_underscores",
            "key with spaces",
            "key%encoded",
        ];

        for key in &special_keys {
            backend.put("special", key, b"value").await.unwrap();
        }

        let keys = backend.list_keys("special", None).await.unwrap();
        assert_eq!(keys.len(), special_keys.len());

        for key in &special_keys {
            assert!(backend.exists("special", key).await.unwrap());
        }
    }
}
