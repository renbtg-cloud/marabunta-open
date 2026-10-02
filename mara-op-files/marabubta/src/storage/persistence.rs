// Marabunta - Licensed under the MIT License.
//! Generic persistence layer traits and typed store wrapper
//!
//! This module provides a namespace-aware key-value storage abstraction
//! with JSON serialization support for typed data access.
//!
//! # Features
//!
//! - **Indexes**: Add indexes to frequently queried fields for faster lookups
//! - **Pagination**: Query result pagination with cursor-based navigation
//! - **Batch Operations**: Batch read/write operations to reduce round trips
//! - **Lazy Loading**: Lazy loading for large objects via handles
//! - **Query Analysis**: Query explain/analyze helpers for debugging
//!
//! # Example
//!
//! ```ignore
//! use marabunta_compute::storage::persistence::{TypedStore, IndexedStore, QueryOptions};
//!
//! // Create an indexed store for jobs
//! let store = IndexedStore::new(backend, "jobs");
//! store.create_index("status", |job: &Job| job.status.to_string()).await?;
//! store.create_index("tenant_id", |job: &Job| job.tenant_id.map(|t| t.to_string()).unwrap_or_default()).await?;
//!
//! // Query with pagination
//! let options = QueryOptions::new().with_limit(50);
//! let page = store.query_by_index("status", "running", options).await?;
//!
//! // Batch operations
//! let jobs = store.batch_get(&["job1", "job2", "job3"]).await?;
//! store.batch_put(&[("job1", &job1), ("job2", &job2)]).await?;
//! ```

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Errors that can occur during persistence operations
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Key not found: {namespace}/{key}")]
    NotFound { namespace: String, key: String },

    #[error("Transaction failed: {0}")]
    Transaction(String),

    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Invalid cursor: {0}")]
    InvalidCursor(String),

    #[error("Query timeout")]
    QueryTimeout,
}

impl From<serde_json::Error> for PersistenceError {
    fn from(e: serde_json::Error) -> Self {
        PersistenceError::Serialization(e.to_string())
    }
}

// ============================================================================
// Pagination Types
// ============================================================================

/// Cursor for pagination - encodes position in a result set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    /// The key at the cursor position
    pub key: String,
    /// Optional sort field value for secondary sorting
    pub sort_value: Option<String>,
    /// Direction of pagination
    pub direction: CursorDirection,
}

impl Cursor {
    /// Create a new cursor starting from a key
    pub fn from_key(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            sort_value: None,
            direction: CursorDirection::Forward,
        }
    }

    /// Create cursor with sort value
    pub fn with_sort_value(mut self, value: impl Into<String>) -> Self {
        self.sort_value = Some(value.into());
        self
    }

    /// Set direction to backward
    pub fn backward(mut self) -> Self {
        self.direction = CursorDirection::Backward;
        self
    }

    /// Encode cursor to a string for API responses
    pub fn encode(&self) -> String {
        use base64::Engine;
        let json = serde_json::to_string(self).unwrap_or_default();
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json.as_bytes())
    }

    /// Decode cursor from an encoded string
    pub fn decode(encoded: &str) -> Result<Self, PersistenceError> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|e| PersistenceError::InvalidCursor(e.to_string()))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| PersistenceError::InvalidCursor(e.to_string()))
    }
}

/// Direction of cursor pagination
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorDirection {
    Forward,
    Backward,
}

/// Options for paginated queries
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Maximum number of results to return
    pub limit: Option<usize>,
    /// Cursor for pagination
    pub cursor: Option<Cursor>,
    /// Sort order
    pub sort_order: SortOrder,
    /// Include total count (may be expensive for large datasets)
    pub include_total: bool,
}

impl QueryOptions {
    /// Create new query options
    pub fn new() -> Self {
        Self::default()
    }

    /// Set result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set cursor for pagination
    pub fn with_cursor(mut self, cursor: Cursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Set cursor from encoded string
    pub fn with_cursor_str(mut self, encoded: &str) -> Result<Self, PersistenceError> {
        self.cursor = Some(Cursor::decode(encoded)?);
        Ok(self)
    }

    /// Set sort order
    pub fn with_sort(mut self, order: SortOrder) -> Self {
        self.sort_order = order;
        self
    }

    /// Include total count in results
    pub fn with_total(mut self) -> Self {
        self.include_total = true;
        self
    }
}

/// Sort order for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortOrder {
    #[default]
    Ascending,
    Descending,
}

/// A page of query results with pagination metadata
#[derive(Debug, Clone)]
pub struct Page<T> {
    /// The items in this page
    pub items: Vec<(String, T)>,
    /// Cursor for the next page (None if no more results)
    pub next_cursor: Option<Cursor>,
    /// Cursor for the previous page (None if at start)
    pub prev_cursor: Option<Cursor>,
    /// Total count (only populated if include_total was true)
    pub total: Option<usize>,
    /// Whether there are more results
    pub has_more: bool,
}

impl<T> Page<T> {
    /// Create an empty page
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            next_cursor: None,
            prev_cursor: None,
            total: None,
            has_more: false,
        }
    }

    /// Get encoded next cursor for API responses
    pub fn next_cursor_str(&self) -> Option<String> {
        self.next_cursor.as_ref().map(|c| c.encode())
    }

    /// Get encoded previous cursor for API responses
    pub fn prev_cursor_str(&self) -> Option<String> {
        self.prev_cursor.as_ref().map(|c| c.encode())
    }

    /// Map items to a different type
    pub fn map<U, F: FnMut(T) -> U>(self, mut f: F) -> Page<U> {
        Page {
            items: self.items.into_iter().map(|(k, v)| (k, f(v))).collect(),
            next_cursor: self.next_cursor,
            prev_cursor: self.prev_cursor,
            total: self.total,
            has_more: self.has_more,
        }
    }
}

// ============================================================================
// Lazy Loading Types
// ============================================================================

/// Handle for lazy loading of large objects
#[derive(Debug, Clone)]
pub struct LazyHandle<B: PersistenceBackend> {
    backend: Arc<B>,
    namespace: String,
    key: String,
    /// Cached size in bytes (if known)
    size_hint: Option<u64>,
}

impl<B: PersistenceBackend> LazyHandle<B> {
    /// Create a new lazy handle
    pub fn new(backend: Arc<B>, namespace: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
            key: key.into(),
            size_hint: None,
        }
    }

    /// Create handle with known size
    pub fn with_size(mut self, size: u64) -> Self {
        self.size_hint = Some(size);
        self
    }

    /// Get the key this handle refers to
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the size hint if available
    pub fn size_hint(&self) -> Option<u64> {
        self.size_hint
    }

    /// Load the full value
    pub async fn load<T: DeserializeOwned>(&self) -> Result<Option<T>, PersistenceError> {
        match self.backend.get(&self.namespace, &self.key).await? {
            Some(bytes) => {
                let value: T = serde_json::from_slice(&bytes)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Load raw bytes without deserialization
    pub async fn load_raw(&self) -> Result<Option<Vec<u8>>, PersistenceError> {
        self.backend.get(&self.namespace, &self.key).await
    }

    /// Check if the value exists
    pub async fn exists(&self) -> Result<bool, PersistenceError> {
        self.backend.exists(&self.namespace, &self.key).await
    }
}

// ============================================================================
// Query Analysis Types
// ============================================================================

/// Query execution plan for debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Type of query operation
    pub operation: QueryOperation,
    /// Estimated cost (lower is better)
    pub estimated_cost: f64,
    /// Number of keys to scan
    pub keys_to_scan: Option<usize>,
    /// Indexes being used
    pub indexes_used: Vec<String>,
    /// Whether a full scan is required
    pub requires_full_scan: bool,
    /// Optimization suggestions
    pub suggestions: Vec<String>,
}

impl QueryPlan {
    /// Create a new query plan
    pub fn new(operation: QueryOperation) -> Self {
        Self {
            operation,
            estimated_cost: 0.0,
            keys_to_scan: None,
            indexes_used: Vec::new(),
            requires_full_scan: false,
            suggestions: Vec::new(),
        }
    }

    /// Add an index to the plan
    pub fn with_index(mut self, index: impl Into<String>) -> Self {
        self.indexes_used.push(index.into());
        self
    }

    /// Set estimated cost
    pub fn with_cost(mut self, cost: f64) -> Self {
        self.estimated_cost = cost;
        self
    }

    /// Add a suggestion
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestions.push(suggestion.into());
        self
    }
}

/// Type of query operation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryOperation {
    /// Lookup by primary key
    PrimaryKeyLookup,
    /// Index scan
    IndexScan { index_name: String },
    /// Full namespace scan
    FullScan,
    /// Prefix scan
    PrefixScan { prefix: String },
    /// Range scan
    RangeScan { start: String, end: String },
    /// Batch lookup
    BatchLookup { key_count: usize },
}

/// Query execution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    /// Total execution time in microseconds
    pub execution_time_us: u64,
    /// Number of keys examined
    pub keys_examined: usize,
    /// Number of results returned
    pub results_returned: usize,
    /// Bytes read from storage
    pub bytes_read: u64,
    /// Whether an index was used
    pub index_used: Option<String>,
    /// Cache hit ratio (if applicable)
    pub cache_hit_ratio: Option<f64>,
}

impl QueryStats {
    /// Create new query stats
    pub fn new() -> Self {
        Self {
            execution_time_us: 0,
            keys_examined: 0,
            results_returned: 0,
            bytes_read: 0,
            index_used: None,
            cache_hit_ratio: None,
        }
    }

    /// Record execution time
    pub fn with_time(mut self, time_us: u64) -> Self {
        self.execution_time_us = time_us;
        self
    }

    /// Record keys examined
    pub fn with_keys_examined(mut self, count: usize) -> Self {
        self.keys_examined = count;
        self
    }

    /// Record results returned
    pub fn with_results(mut self, count: usize) -> Self {
        self.results_returned = count;
        self
    }
}

impl Default for QueryStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of an explained query
#[derive(Debug, Clone)]
pub struct ExplainedQuery<T> {
    /// The query results
    pub results: T,
    /// Execution plan used
    pub plan: QueryPlan,
    /// Execution statistics
    pub stats: QueryStats,
}

/// Generic key-value storage trait with namespace support
///
/// This trait provides the fundamental operations for persisting binary data
/// organized by namespaces. Implementations can target different backends
/// such as SQLite, RocksDB, or in-memory stores.
#[async_trait]
pub trait PersistenceBackend: Send + Sync {
    /// Store a value by key in a namespace
    ///
    /// If the key already exists, the value is overwritten and updated_at is refreshed.
    async fn put(&self, namespace: &str, key: &str, value: &[u8]) -> Result<(), PersistenceError>;

    /// Get a value by key from a namespace
    ///
    /// Returns `None` if the key does not exist.
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, PersistenceError>;

    /// Delete a value by key
    ///
    /// Returns `true` if the key existed and was deleted, `false` if it didn't exist.
    async fn delete(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError>;

    /// List all keys in a namespace (with optional prefix filter)
    ///
    /// If `prefix` is `Some`, only keys starting with that prefix are returned.
    /// Keys are returned without the namespace prefix.
    async fn list_keys(
        &self,
        namespace: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, PersistenceError>;

    /// Check if a key exists
    async fn exists(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError>;

    /// Atomic batch write
    ///
    /// All items are written atomically - either all succeed or none are written.
    async fn batch_put(
        &self,
        namespace: &str,
        items: &[(&str, &[u8])],
    ) -> Result<(), PersistenceError>;

    /// Clear all data in a namespace
    ///
    /// Returns the number of keys deleted.
    async fn clear_namespace(&self, namespace: &str) -> Result<u64, PersistenceError>;

    /// Batch get multiple keys at once
    ///
    /// Returns a map of key to value for keys that exist.
    /// Keys that don't exist are not included in the result.
    async fn batch_get(
        &self,
        namespace: &str,
        keys: &[&str],
    ) -> Result<HashMap<String, Vec<u8>>, PersistenceError> {
        // Default implementation: sequential gets
        let mut results = HashMap::new();
        for key in keys {
            if let Some(value) = self.get(namespace, key).await? {
                results.insert(key.to_string(), value);
            }
        }
        Ok(results)
    }

    /// Batch delete multiple keys at once
    ///
    /// Returns the number of keys that were deleted.
    async fn batch_delete(
        &self,
        namespace: &str,
        keys: &[&str],
    ) -> Result<usize, PersistenceError> {
        // Default implementation: sequential deletes
        let mut count = 0;
        for key in keys {
            if self.delete(namespace, key).await? {
                count += 1;
            }
        }
        Ok(count)
    }

    /// List keys with pagination support
    ///
    /// Returns keys starting after `after_key` (exclusive), up to `limit` keys.
    async fn list_keys_paginated(
        &self,
        namespace: &str,
        prefix: Option<&str>,
        after_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>, PersistenceError> {
        // Default implementation: filter after listing
        let mut keys = self.list_keys(namespace, prefix).await?;
        keys.sort();

        if let Some(after) = after_key {
            keys.retain(|k| k.as_str() > after);
        }

        keys.truncate(limit);
        Ok(keys)
    }

    /// Count keys in a namespace (with optional prefix filter)
    ///
    /// This may be more efficient than list_keys().len() for some backends.
    async fn count_keys(
        &self,
        namespace: &str,
        prefix: Option<&str>,
    ) -> Result<usize, PersistenceError> {
        // Default implementation: list and count
        Ok(self.list_keys(namespace, prefix).await?.len())
    }

    /// Get metadata about a key without loading the value
    ///
    /// Returns (exists, size_bytes) if the key exists, None otherwise.
    async fn get_metadata(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<KeyMetadata>, PersistenceError> {
        // Default implementation: load the value
        match self.get(namespace, key).await? {
            Some(bytes) => Ok(Some(KeyMetadata {
                size_bytes: bytes.len() as u64,
                created_at: None,
                updated_at: None,
            })),
            None => Ok(None),
        }
    }
}

/// Metadata about a stored key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    /// Size of the value in bytes
    pub size_bytes: u64,
    /// When the key was created (if tracked)
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    /// When the key was last updated (if tracked)
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Typed wrapper for JSON serialization
///
/// This struct wraps a `PersistenceBackend` and provides typed access
/// to stored data through automatic JSON serialization/deserialization.
///
/// # Example
///
/// ```ignore
/// use marabunta_compute::storage::{TypedStore, SqliteBackend};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User {
///     name: String,
///     email: String,
/// }
///
/// let backend = SqliteBackend::in_memory()?;
/// let store = TypedStore::new(backend, "users");
///
/// store.put("user1", &User { name: "Alice".into(), email: "alice@example.com".into() }).await?;
/// let user: Option<User> = store.get("user1").await?;
/// ```
pub struct TypedStore<B: PersistenceBackend> {
    backend: Arc<B>,
    namespace: String,
}

impl<B: PersistenceBackend> TypedStore<B> {
    /// Create a new typed store with the given backend and namespace
    pub fn new(backend: B, namespace: impl Into<String>) -> Self {
        Self {
            backend: Arc::new(backend),
            namespace: namespace.into(),
        }
    }

    /// Create a new typed store from an Arc'd backend
    pub fn from_arc(backend: Arc<B>, namespace: impl Into<String>) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
        }
    }

    /// Get the namespace this store operates on
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get a reference to the underlying backend
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Store a typed value by key
    ///
    /// The value is serialized to JSON before storage.
    pub async fn put<T: Serialize>(&self, key: &str, value: &T) -> Result<(), PersistenceError> {
        let bytes = serde_json::to_vec(value)?;
        self.backend.put(&self.namespace, key, &bytes).await
    }

    /// Get a typed value by key
    ///
    /// Returns `None` if the key does not exist.
    /// Returns an error if deserialization fails.
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, PersistenceError> {
        match self.backend.get(&self.namespace, key).await? {
            Some(bytes) => {
                let value: T = serde_json::from_slice(&bytes)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Delete a value by key
    ///
    /// Returns `true` if the key existed and was deleted.
    pub async fn delete(&self, key: &str) -> Result<bool, PersistenceError> {
        self.backend.delete(&self.namespace, key).await
    }

    /// List all keys with optional prefix filter
    pub async fn list_keys(&self, prefix: Option<&str>) -> Result<Vec<String>, PersistenceError> {
        self.backend.list_keys(&self.namespace, prefix).await
    }

    /// Check if a key exists
    pub async fn exists(&self, key: &str) -> Result<bool, PersistenceError> {
        self.backend.exists(&self.namespace, key).await
    }

    /// Get all key-value pairs in the namespace
    ///
    /// Returns a vector of (key, value) tuples.
    /// Skips entries that fail to deserialize.
    pub async fn get_all<T: DeserializeOwned>(&self) -> Result<Vec<(String, T)>, PersistenceError> {
        let keys = self.backend.list_keys(&self.namespace, None).await?;
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(bytes) = self.backend.get(&self.namespace, &key).await? {
                if let Ok(value) = serde_json::from_slice::<T>(&bytes) {
                    results.push((key, value));
                }
            }
        }

        Ok(results)
    }

    /// Batch put multiple typed values atomically
    pub async fn batch_put<T: Serialize>(
        &self,
        items: &[(&str, T)],
    ) -> Result<(), PersistenceError> {
        let serialized: Result<Vec<_>, _> = items
            .iter()
            .map(|(k, v)| serde_json::to_vec(v).map(|bytes| (*k, bytes)))
            .collect();

        let serialized = serialized?;
        let refs: Vec<(&str, &[u8])> = serialized.iter().map(|(k, v)| (*k, v.as_slice())).collect();

        self.backend.batch_put(&self.namespace, &refs).await
    }

    /// Clear all data in this namespace
    ///
    /// Returns the number of keys deleted.
    pub async fn clear(&self) -> Result<u64, PersistenceError> {
        self.backend.clear_namespace(&self.namespace).await
    }

    /// Batch get multiple typed values at once
    ///
    /// More efficient than multiple individual gets for bulk operations.
    pub async fn batch_get<T: DeserializeOwned>(
        &self,
        keys: &[&str],
    ) -> Result<HashMap<String, T>, PersistenceError> {
        let raw = self.backend.batch_get(&self.namespace, keys).await?;
        let mut results = HashMap::new();

        for (key, bytes) in raw {
            if let Ok(value) = serde_json::from_slice::<T>(&bytes) {
                results.insert(key, value);
            }
        }

        Ok(results)
    }

    /// Batch delete multiple keys at once
    ///
    /// Returns the number of keys deleted.
    pub async fn batch_delete(&self, keys: &[&str]) -> Result<usize, PersistenceError> {
        self.backend.batch_delete(&self.namespace, keys).await
    }

    /// Get a page of results with pagination
    pub async fn get_page<T: DeserializeOwned + Clone>(
        &self,
        options: QueryOptions,
    ) -> Result<Page<T>, PersistenceError> {
        let limit = options.limit.unwrap_or(100);
        let after_key = options.cursor.as_ref().map(|c| c.key.as_str());

        // Get total if requested
        let total = if options.include_total {
            Some(self.backend.count_keys(&self.namespace, None).await?)
        } else {
            None
        };

        // Fetch one extra to determine if there are more results
        let keys = self
            .backend
            .list_keys_paginated(&self.namespace, None, after_key, limit + 1)
            .await?;

        let has_more = keys.len() > limit;
        let keys: Vec<_> = keys.into_iter().take(limit).collect();

        // Fetch values
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let values = self.batch_get::<T>(&key_refs).await?;

        // Build items in order
        let items: Vec<_> = keys
            .into_iter()
            .filter_map(|k| values.get(&k).cloned().map(|v| (k, v)))
            .collect();

        // Build cursors
        let next_cursor = if has_more {
            items.last().map(|(k, _)| Cursor::from_key(k.clone()))
        } else {
            None
        };

        let prev_cursor = options.cursor.map(|c| c.backward());

        Ok(Page {
            items,
            next_cursor,
            prev_cursor,
            total,
            has_more,
        })
    }

    /// Create a lazy handle for a key
    ///
    /// The value is not loaded until explicitly requested.
    pub fn lazy_handle(&self, key: impl Into<String>) -> LazyHandle<B> {
        LazyHandle::new(Arc::clone(&self.backend), self.namespace.clone(), key)
    }

    /// Get metadata about a key without loading the value
    pub async fn get_metadata(&self, key: &str) -> Result<Option<KeyMetadata>, PersistenceError> {
        self.backend.get_metadata(&self.namespace, key).await
    }

    /// Count items in the store
    pub async fn count(&self) -> Result<usize, PersistenceError> {
        self.backend.count_keys(&self.namespace, None).await
    }

    /// Count items with a prefix
    pub async fn count_with_prefix(&self, prefix: &str) -> Result<usize, PersistenceError> {
        self.backend.count_keys(&self.namespace, Some(prefix)).await
    }

    /// Explain a query operation without executing it
    pub fn explain_get(&self, _key: &str) -> QueryPlan {
        QueryPlan::new(QueryOperation::PrimaryKeyLookup)
            .with_cost(1.0)
    }

    /// Explain a batch get operation
    pub fn explain_batch_get(&self, keys: &[&str]) -> QueryPlan {
        QueryPlan::new(QueryOperation::BatchLookup {
            key_count: keys.len(),
        })
        .with_cost(keys.len() as f64 * 0.5)
    }

    /// Explain a prefix scan operation
    pub fn explain_prefix_scan(&self, prefix: &str) -> QueryPlan {
        QueryPlan::new(QueryOperation::PrefixScan {
            prefix: prefix.to_string(),
        })
        .with_suggestion("Consider adding an index for this prefix pattern".to_string())
    }

    /// Execute a get with statistics
    pub async fn get_with_stats<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<ExplainedQuery<Option<T>>, PersistenceError> {
        let start = std::time::Instant::now();
        let result = self.get(key).await?;
        let elapsed = start.elapsed();

        let stats = QueryStats::new()
            .with_time(elapsed.as_micros() as u64)
            .with_keys_examined(1)
            .with_results(if result.is_some() { 1 } else { 0 });

        let plan = self.explain_get(key);

        Ok(ExplainedQuery {
            results: result,
            plan,
            stats,
        })
    }
}

impl<B: PersistenceBackend> Clone for TypedStore<B> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            namespace: self.namespace.clone(),
        }
    }
}

// ============================================================================
// Indexed Store
// ============================================================================

/// Index definition for a field
#[derive(Clone)]
pub struct IndexDefinition<T> {
    /// Index name
    pub name: String,
    /// Extract the index key from a value
    extractor: Arc<dyn Fn(&T) -> String + Send + Sync>,
}

impl<T> IndexDefinition<T> {
    /// Create a new index definition
    pub fn new<F>(name: impl Into<String>, extractor: F) -> Self
    where
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            extractor: Arc::new(extractor),
        }
    }

    /// Extract index key from a value
    pub fn extract(&self, value: &T) -> String {
        (self.extractor)(value)
    }
}

/// Store with secondary index support
///
/// Maintains indexes alongside the primary data for efficient queries
/// on indexed fields.
pub struct IndexedStore<B: PersistenceBackend, T: Serialize + DeserializeOwned + Send + Sync> {
    /// The underlying typed store
    store: TypedStore<B>,
    /// Index definitions
    indexes: parking_lot::RwLock<Vec<IndexDefinition<T>>>,
    /// Index namespace prefix
    index_prefix: String,
}

impl<B: PersistenceBackend + 'static, T: Serialize + DeserializeOwned + Send + Sync + Clone>
    IndexedStore<B, T>
{
    /// Create a new indexed store
    pub fn new(backend: B, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let index_prefix = format!("{}_idx", namespace);

        Self {
            store: TypedStore::new(backend, namespace),
            indexes: parking_lot::RwLock::new(Vec::new()),
            index_prefix,
        }
    }

    /// Create from an Arc'd backend
    pub fn from_arc(backend: Arc<B>, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let index_prefix = format!("{}_idx", namespace);

        Self {
            store: TypedStore::from_arc(backend, namespace),
            indexes: parking_lot::RwLock::new(Vec::new()),
            index_prefix,
        }
    }

    /// Add an index definition
    ///
    /// Note: This does not rebuild existing data. Call `rebuild_index` for that.
    pub fn add_index<F>(&self, name: impl Into<String>, extractor: F)
    where
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        let mut indexes = self.indexes.write();
        indexes.push(IndexDefinition::new(name, extractor));
    }

    /// Get the underlying store
    pub fn store(&self) -> &TypedStore<B> {
        &self.store
    }

    /// Put a value with index updates
    pub async fn put(&self, key: &str, value: &T) -> Result<(), PersistenceError> {
        // Store the value
        self.store.put(key, value).await?;

        // Update all indexes
        let indexes = self.indexes.read();
        for index in indexes.iter() {
            let index_key = index.extract(value);
            self.add_to_index(&index.name, &index_key, key).await?;
        }

        Ok(())
    }

    /// Get a value by primary key
    pub async fn get(&self, key: &str) -> Result<Option<T>, PersistenceError> {
        self.store.get(key).await
    }

    /// Delete a value with index cleanup
    pub async fn delete(&self, key: &str) -> Result<bool, PersistenceError> {
        // Get the value first to clean up indexes
        if let Some(value) = self.store.get::<T>(key).await? {
            let indexes = self.indexes.read();
            for index in indexes.iter() {
                let index_key = index.extract(&value);
                self.remove_from_index(&index.name, &index_key, key).await?;
            }
        }

        self.store.delete(key).await
    }

    /// Query by index value
    pub async fn query_by_index(
        &self,
        index_name: &str,
        index_value: &str,
        options: QueryOptions,
    ) -> Result<Page<T>, PersistenceError> {
        // Get keys from the index
        let index_namespace = format!("{}:{}", self.index_prefix, index_name);
        let index_key = format!("{}:{}", index_value, "keys");

        let primary_keys: Vec<String> = self
            .store
            .backend()
            .get(&index_namespace, &index_key)
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).unwrap_or_default())
            .unwrap_or_default();

        let total = if options.include_total {
            Some(primary_keys.len())
        } else {
            None
        };

        // Apply pagination
        let limit = options.limit.unwrap_or(100);
        let start_idx = options
            .cursor
            .as_ref()
            .and_then(|c| primary_keys.iter().position(|k| k == &c.key))
            .map(|i| i + 1)
            .unwrap_or(0);

        let paged_keys: Vec<_> = primary_keys
            .into_iter()
            .skip(start_idx)
            .take(limit + 1)
            .collect();

        let has_more = paged_keys.len() > limit;
        let paged_keys: Vec<_> = paged_keys.into_iter().take(limit).collect();

        // Fetch values
        let key_refs: Vec<&str> = paged_keys.iter().map(|s| s.as_str()).collect();
        let values = self.store.batch_get::<T>(&key_refs).await?;

        let items: Vec<_> = paged_keys
            .into_iter()
            .filter_map(|k| values.get(&k).cloned().map(|v| (k, v)))
            .collect();

        let next_cursor = if has_more {
            items.last().map(|(k, _)| Cursor::from_key(k.clone()))
        } else {
            None
        };

        Ok(Page {
            items,
            next_cursor,
            prev_cursor: options.cursor.map(|c| c.backward()),
            total,
            has_more,
        })
    }

    /// Count items matching an index value
    pub async fn count_by_index(
        &self,
        index_name: &str,
        index_value: &str,
    ) -> Result<usize, PersistenceError> {
        let index_namespace = format!("{}:{}", self.index_prefix, index_name);
        let index_key = format!("{}:{}", index_value, "keys");

        let primary_keys: Vec<String> = self
            .store
            .backend()
            .get(&index_namespace, &index_key)
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).unwrap_or_default())
            .unwrap_or_default();

        Ok(primary_keys.len())
    }

    /// List all unique values for an index
    pub async fn list_index_values(&self, index_name: &str) -> Result<Vec<String>, PersistenceError> {
        let index_namespace = format!("{}:{}", self.index_prefix, index_name);
        let keys = self.store.backend().list_keys(&index_namespace, None).await?;

        // Extract unique index values from keys (format: "value:keys")
        let values: Vec<_> = keys
            .into_iter()
            .filter_map(|k| k.strip_suffix(":keys").map(|s| s.to_string()))
            .collect();

        Ok(values)
    }

    /// Rebuild an index from all stored values
    pub async fn rebuild_index(&self, index_name: &str) -> Result<usize, PersistenceError> {
        let index_def = {
            let indexes = self.indexes.read();
            indexes
                .iter()
                .find(|i| i.name == index_name)
                .cloned()
                .ok_or_else(|| PersistenceError::IndexNotFound(index_name.to_string()))?
        };

        // Clear existing index
        let index_namespace = format!("{}:{}", self.index_prefix, index_name);
        self.store.backend().clear_namespace(&index_namespace).await?;

        // Rebuild from all values
        let all = self.store.get_all::<T>().await?;
        let count = all.len();

        for (key, value) in all {
            let index_key = index_def.extract(&value);
            self.add_to_index(index_name, &index_key, &key).await?;
        }

        Ok(count)
    }

    /// Explain a query by index
    pub fn explain_index_query(
        &self,
        index_name: &str,
        _index_value: &str,
    ) -> QueryPlan {
        let indexes = self.indexes.read();
        let has_index = indexes.iter().any(|i| i.name == index_name);

        if has_index {
            QueryPlan::new(QueryOperation::IndexScan {
                index_name: index_name.to_string(),
            })
            .with_index(index_name)
            .with_cost(2.0) // Index lookup + primary key fetch
        } else {
            QueryPlan::new(QueryOperation::FullScan)
                .with_cost(100.0)
                .with_suggestion(format!("Consider adding an index on '{}'", index_name))
        }
    }

    // Internal: add a key to an index
    async fn add_to_index(
        &self,
        index_name: &str,
        index_value: &str,
        primary_key: &str,
    ) -> Result<(), PersistenceError> {
        let index_namespace = format!("{}:{}", self.index_prefix, index_name);
        let index_key = format!("{}:{}", index_value, "keys");

        // Get existing keys
        let mut keys: Vec<String> = self
            .store
            .backend()
            .get(&index_namespace, &index_key)
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).unwrap_or_default())
            .unwrap_or_default();

        // Add if not present
        if !keys.contains(&primary_key.to_string()) {
            keys.push(primary_key.to_string());
            let bytes = serde_json::to_vec(&keys)?;
            self.store
                .backend()
                .put(&index_namespace, &index_key, &bytes)
                .await?;
        }

        Ok(())
    }

    // Internal: remove a key from an index
    async fn remove_from_index(
        &self,
        index_name: &str,
        index_value: &str,
        primary_key: &str,
    ) -> Result<(), PersistenceError> {
        let index_namespace = format!("{}:{}", self.index_prefix, index_name);
        let index_key = format!("{}:{}", index_value, "keys");

        // Get existing keys
        let mut keys: Vec<String> = self
            .store
            .backend()
            .get(&index_namespace, &index_key)
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).unwrap_or_default())
            .unwrap_or_default();

        // Remove if present
        if let Some(pos) = keys.iter().position(|k| k == primary_key) {
            keys.remove(pos);
            if keys.is_empty() {
                self.store
                    .backend()
                    .delete(&index_namespace, &index_key)
                    .await?;
            } else {
                let bytes = serde_json::to_vec(&keys)?;
                self.store
                    .backend()
                    .put(&index_namespace, &index_key, &bytes)
                    .await?;
            }
        }

        Ok(())
    }
}

impl<B: PersistenceBackend + 'static, T: Serialize + DeserializeOwned + Send + Sync + Clone> Clone
    for IndexedStore<B, T>
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            indexes: parking_lot::RwLock::new(self.indexes.read().clone()),
            index_prefix: self.index_prefix.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap as StdHashMap;

    /// Simple in-memory backend for testing
    pub struct TestBackend {
        data: RwLock<StdHashMap<String, StdHashMap<String, Vec<u8>>>>,
    }

    impl TestBackend {
        pub fn new() -> Self {
            Self {
                data: RwLock::new(StdHashMap::new()),
            }
        }
    }

    #[async_trait]
    impl PersistenceBackend for TestBackend {
        async fn put(&self, namespace: &str, key: &str, value: &[u8]) -> Result<(), PersistenceError> {
            let mut data = self.data.write();
            data.entry(namespace.to_string())
                .or_default()
                .insert(key.to_string(), value.to_vec());
            Ok(())
        }

        async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, PersistenceError> {
            let data = self.data.read();
            Ok(data.get(namespace).and_then(|ns| ns.get(key).cloned()))
        }

        async fn delete(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
            let mut data = self.data.write();
            Ok(data
                .get_mut(namespace)
                .map(|ns| ns.remove(key).is_some())
                .unwrap_or(false))
        }

        async fn list_keys(
            &self,
            namespace: &str,
            prefix: Option<&str>,
        ) -> Result<Vec<String>, PersistenceError> {
            let data = self.data.read();
            Ok(data
                .get(namespace)
                .map(|ns| {
                    ns.keys()
                        .filter(|k| prefix.map_or(true, |p| k.starts_with(p)))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default())
        }

        async fn exists(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
            let data = self.data.read();
            Ok(data.get(namespace).map_or(false, |ns| ns.contains_key(key)))
        }

        async fn batch_put(
            &self,
            namespace: &str,
            items: &[(&str, &[u8])],
        ) -> Result<(), PersistenceError> {
            let mut data = self.data.write();
            let ns = data.entry(namespace.to_string()).or_default();
            for (key, value) in items {
                ns.insert(key.to_string(), value.to_vec());
            }
            Ok(())
        }

        async fn clear_namespace(&self, namespace: &str) -> Result<u64, PersistenceError> {
            let mut data = self.data.write();
            Ok(data
                .remove(namespace)
                .map(|ns| ns.len() as u64)
                .unwrap_or(0))
        }
    }

    // Basic compile-time check that traits are object-safe
    fn _assert_object_safe(_: &dyn PersistenceBackend) {}

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestItem {
        id: String,
        status: String,
        priority: u32,
    }

    #[tokio::test]
    async fn test_batch_get() {
        let backend = TestBackend::new();
        let store = TypedStore::new(backend, "test");

        // Put some items
        store.put("item1", &TestItem { id: "1".into(), status: "active".into(), priority: 1 }).await.unwrap();
        store.put("item2", &TestItem { id: "2".into(), status: "pending".into(), priority: 2 }).await.unwrap();
        store.put("item3", &TestItem { id: "3".into(), status: "active".into(), priority: 3 }).await.unwrap();

        // Batch get
        let results: HashMap<String, TestItem> = store.batch_get(&["item1", "item3", "nonexistent"]).await.unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains_key("item1"));
        assert!(results.contains_key("item3"));
        assert!(!results.contains_key("nonexistent"));
    }

    #[tokio::test]
    async fn test_pagination() {
        let backend = TestBackend::new();
        let store = TypedStore::new(backend, "test");

        // Put 10 items
        for i in 0..10 {
            let key = format!("item_{:02}", i);
            store.put(&key, &TestItem {
                id: i.to_string(),
                status: "active".into(),
                priority: i
            }).await.unwrap();
        }

        // Get first page
        let options = QueryOptions::new().with_limit(3).with_total();
        let page1: Page<TestItem> = store.get_page(options).await.unwrap();

        assert_eq!(page1.items.len(), 3);
        assert!(page1.has_more);
        assert_eq!(page1.total, Some(10));
        assert!(page1.next_cursor.is_some());

        // Get second page
        let options = QueryOptions::new()
            .with_limit(3)
            .with_cursor(page1.next_cursor.unwrap());
        let page2: Page<TestItem> = store.get_page(options).await.unwrap();

        assert_eq!(page2.items.len(), 3);
        assert!(page2.has_more);
    }

    #[tokio::test]
    async fn test_cursor_encode_decode() {
        let cursor = Cursor::from_key("test_key").with_sort_value("sort_val");
        let encoded = cursor.encode();
        let decoded = Cursor::decode(&encoded).unwrap();

        assert_eq!(decoded.key, "test_key");
        assert_eq!(decoded.sort_value, Some("sort_val".to_string()));
    }

    #[tokio::test]
    async fn test_lazy_handle() {
        let backend = Arc::new(TestBackend::new());
        let store = TypedStore::from_arc(backend.clone(), "test");

        let item = TestItem { id: "1".into(), status: "active".into(), priority: 1 };
        store.put("item1", &item).await.unwrap();

        // Create lazy handle
        let handle = store.lazy_handle("item1");
        assert_eq!(handle.key(), "item1");

        // Check existence without loading
        assert!(handle.exists().await.unwrap());

        // Load value
        let loaded: Option<TestItem> = handle.load().await.unwrap();
        assert_eq!(loaded, Some(item));
    }

    #[tokio::test]
    async fn test_indexed_store() {
        let backend = TestBackend::new();
        let store: IndexedStore<_, TestItem> = IndexedStore::new(backend, "items");

        // Add index on status field
        store.add_index("status", |item: &TestItem| item.status.clone());

        // Put items
        store.put("item1", &TestItem { id: "1".into(), status: "active".into(), priority: 1 }).await.unwrap();
        store.put("item2", &TestItem { id: "2".into(), status: "pending".into(), priority: 2 }).await.unwrap();
        store.put("item3", &TestItem { id: "3".into(), status: "active".into(), priority: 3 }).await.unwrap();
        store.put("item4", &TestItem { id: "4".into(), status: "completed".into(), priority: 4 }).await.unwrap();

        // Query by index
        let options = QueryOptions::new().with_total();
        let page = store.query_by_index("status", "active", options).await.unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.total, Some(2));

        // Count by index
        let count = store.count_by_index("status", "pending").await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_indexed_store_delete() {
        let backend = TestBackend::new();
        let store: IndexedStore<_, TestItem> = IndexedStore::new(backend, "items");

        store.add_index("status", |item: &TestItem| item.status.clone());

        store.put("item1", &TestItem { id: "1".into(), status: "active".into(), priority: 1 }).await.unwrap();
        store.put("item2", &TestItem { id: "2".into(), status: "active".into(), priority: 2 }).await.unwrap();

        // Verify index has 2 items
        let count = store.count_by_index("status", "active").await.unwrap();
        assert_eq!(count, 2);

        // Delete one item
        store.delete("item1").await.unwrap();

        // Verify index now has 1 item
        let count = store.count_by_index("status", "active").await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_query_explain() {
        let backend = TestBackend::new();
        let store: IndexedStore<_, TestItem> = IndexedStore::new(backend, "items");

        store.add_index("status", |item: &TestItem| item.status.clone());

        // Explain indexed query
        let plan = store.explain_index_query("status", "active");
        assert!(!plan.indexes_used.is_empty());
        assert!(matches!(plan.operation, QueryOperation::IndexScan { .. }));

        // Explain non-indexed query
        let plan = store.explain_index_query("nonexistent", "value");
        assert!(plan.indexes_used.is_empty());
        assert!(matches!(plan.operation, QueryOperation::FullScan));
        assert!(!plan.suggestions.is_empty());
    }

    #[tokio::test]
    async fn test_rebuild_index() {
        let backend = TestBackend::new();
        let store: IndexedStore<_, TestItem> = IndexedStore::new(backend, "items");

        // Put items WITHOUT index
        store.store.put("item1", &TestItem { id: "1".into(), status: "active".into(), priority: 1 }).await.unwrap();
        store.store.put("item2", &TestItem { id: "2".into(), status: "active".into(), priority: 2 }).await.unwrap();

        // Add index after data exists
        store.add_index("status", |item: &TestItem| item.status.clone());

        // Rebuild index
        let count = store.rebuild_index("status").await.unwrap();
        assert_eq!(count, 2);

        // Query should now work
        let page = store.query_by_index("status", "active", QueryOptions::new()).await.unwrap();
        assert_eq!(page.items.len(), 2);
    }

    #[tokio::test]
    async fn test_get_with_stats() {
        let backend = TestBackend::new();
        let store = TypedStore::new(backend, "test");

        let item = TestItem { id: "1".into(), status: "active".into(), priority: 1 };
        store.put("item1", &item).await.unwrap();

        let explained: ExplainedQuery<Option<TestItem>> = store.get_with_stats("item1").await.unwrap();

        assert_eq!(explained.results, Some(item));
        assert!(explained.stats.execution_time_us > 0);
        assert_eq!(explained.stats.keys_examined, 1);
        assert_eq!(explained.stats.results_returned, 1);
    }
}
