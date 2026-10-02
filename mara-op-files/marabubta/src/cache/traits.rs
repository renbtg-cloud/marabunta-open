// Marabunta - Licensed under the MIT License.
//! Core cache traits and types

use std::fmt;
use std::hash::Hash;
use std::time::{Duration, Instant};
use async_trait::async_trait;
use thiserror::Error;

/// Error type for cache operations
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Key not found")]
    NotFound,

    #[error("Cache is full and cannot evict")]
    CacheFull,

    #[error("Entry has expired")]
    Expired,

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Disk spillover error: {0}")]
    DiskSpillover(String),

    #[error("Cache operation failed: {0}")]
    OperationFailed(String),
}

/// A cache entry with metadata
#[derive(Debug, Clone)]
pub struct CacheEntry<V> {
    /// The cached value
    pub value: V,
    /// When this entry was created
    pub created_at: Instant,
    /// When this entry was last accessed
    pub last_accessed: Instant,
    /// Number of times this entry has been accessed
    pub access_count: u64,
    /// Optional time-to-live for this entry
    pub ttl: Option<Duration>,
    /// Size of the entry in bytes (if known)
    pub size_bytes: Option<usize>,
}

impl<V> CacheEntry<V> {
    /// Create a new cache entry
    pub fn new(value: V) -> Self {
        let now = Instant::now();
        Self {
            value,
            created_at: now,
            last_accessed: now,
            access_count: 1,
            ttl: None,
            size_bytes: None,
        }
    }

    /// Create a new cache entry with TTL
    pub fn with_ttl(value: V, ttl: Duration) -> Self {
        let mut entry = Self::new(value);
        entry.ttl = Some(ttl);
        entry
    }

    /// Create a new cache entry with size
    pub fn with_size(value: V, size_bytes: usize) -> Self {
        let mut entry = Self::new(value);
        entry.size_bytes = Some(size_bytes);
        entry
    }

    /// Check if this entry has expired
    pub fn is_expired(&self) -> bool {
        match self.ttl {
            Some(ttl) => self.created_at.elapsed() > ttl,
            None => false,
        }
    }

    /// Get the age of this entry
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get time since last access
    pub fn time_since_access(&self) -> Duration {
        self.last_accessed.elapsed()
    }

    /// Record an access to this entry
    pub fn record_access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }
}

/// Core cache trait for synchronous operations
pub trait Cache<K, V>: Send + Sync
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Get a value from the cache
    fn get(&self, key: &K) -> Option<V>;

    /// Get a value with its metadata
    fn get_entry(&self, key: &K) -> Option<CacheEntry<V>>;

    /// Set a value in the cache
    fn set(&self, key: K, value: V);

    /// Set a value with a specific TTL
    fn set_with_ttl(&self, key: K, value: V, ttl: Duration);

    /// Delete a value from the cache
    fn delete(&self, key: &K) -> Option<V>;

    /// Clear all entries from the cache
    fn clear(&self);

    /// Check if a key exists in the cache
    fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Get the number of entries in the cache
    fn len(&self) -> usize;

    /// Check if the cache is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get all keys in the cache
    fn keys(&self) -> Vec<K>;

    /// Get or insert a value using a closure
    fn get_or_insert<F>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> V,
    {
        if let Some(value) = self.get(&key) {
            return value;
        }
        let value = f();
        self.set(key, value.clone());
        value
    }

    /// Get or insert a value with TTL using a closure
    fn get_or_insert_with_ttl<F>(&self, key: K, ttl: Duration, f: F) -> V
    where
        F: FnOnce() -> V,
    {
        if let Some(value) = self.get(&key) {
            return value;
        }
        let value = f();
        self.set_with_ttl(key, value.clone(), ttl);
        value
    }

    /// Update a value if it exists
    fn update<F>(&self, key: &K, f: F) -> Option<V>
    where
        F: FnOnce(V) -> V,
    {
        if let Some(old_value) = self.get(key) {
            let new_value = f(old_value);
            self.set(key.clone(), new_value.clone());
            Some(new_value)
        } else {
            None
        }
    }

    /// Remove expired entries (for TTL-based caches)
    fn evict_expired(&self) -> usize {
        0 // Default implementation does nothing
    }
}

/// Async cache trait for operations that may involve I/O
#[async_trait]
pub trait AsyncCache<K, V>: Send + Sync
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Get a value from the cache asynchronously
    async fn get(&self, key: &K) -> Result<Option<V>, CacheError>;

    /// Get a value with its metadata asynchronously
    async fn get_entry(&self, key: &K) -> Result<Option<CacheEntry<V>>, CacheError>;

    /// Set a value in the cache asynchronously
    async fn set(&self, key: K, value: V) -> Result<(), CacheError>;

    /// Set a value with a specific TTL asynchronously
    async fn set_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<(), CacheError>;

    /// Delete a value from the cache asynchronously
    async fn delete(&self, key: &K) -> Result<Option<V>, CacheError>;

    /// Clear all entries from the cache asynchronously
    async fn clear(&self) -> Result<(), CacheError>;

    /// Check if a key exists in the cache asynchronously
    async fn contains(&self, key: &K) -> Result<bool, CacheError> {
        Ok(self.get(key).await?.is_some())
    }

    /// Get the number of entries in the cache
    async fn len(&self) -> Result<usize, CacheError>;

    /// Check if the cache is empty
    async fn is_empty(&self) -> Result<bool, CacheError> {
        Ok(self.len().await? == 0)
    }

    /// Get or insert a value using an async closure
    async fn get_or_insert<F, Fut>(&self, key: K, f: F) -> Result<V, CacheError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = V> + Send,
    {
        if let Some(value) = self.get(&key).await? {
            return Ok(value);
        }
        let value = f().await;
        self.set(key, value.clone()).await?;
        Ok(value)
    }

    /// Remove expired entries
    async fn evict_expired(&self) -> Result<usize, CacheError> {
        Ok(0) // Default implementation does nothing
    }
}

/// Display implementation for cache entries
impl<V: fmt::Debug> fmt::Display for CacheEntry<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CacheEntry(age={:?}, accesses={}, expired={})",
            self.age(),
            self.access_count,
            self.is_expired()
        )
    }
}
