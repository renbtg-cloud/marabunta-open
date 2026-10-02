// Marabunta - Licensed under the MIT License.
//! Caching layer for Marabunta Compute
//!
//! This module provides high-performance caching with multiple strategies:
//!
//! - [`Cache`] - Core cache trait with get, set, delete, clear operations
//! - [`LruCache`] - Least Recently Used cache with configurable max size
//! - [`TtlCache`] - Time-To-Live cache with automatic expiration
//! - [`TwoTierCache`] - Memory + optional disk spillover cache
//! - [`CacheStats`] - Statistics tracking for cache performance
//! - Specialized caches for job status, task results, and worker capabilities
//!
//! # Example
//!
//! ```ignore
//! use marabunta_compute::cache::{LruCache, Cache};
//!
//! let cache = LruCache::new(1000);
//! cache.set("key".to_string(), "value".to_string());
//! assert_eq!(cache.get(&"key".to_string()), Some("value".to_string()));
//! ```

mod lru;
mod stats;
mod traits;
mod ttl;
mod two_tier;
mod specialized;
mod warming;

pub use lru::LruCache;
pub use stats::{CacheStats, CacheStatsSnapshot};
pub use traits::{Cache, CacheEntry, CacheError, AsyncCache};
pub use ttl::TtlCache;
pub use two_tier::{TwoTierCache, DiskSpillConfig};
pub use specialized::{JobStatusCache, TaskResultCache, WorkerCapabilitiesCache, CachedJobStatus, CacheManager, CombinedCacheStats};
pub use warming::{CacheWarmer, PreloadStrategy, WarmingResult, PreloadHelper, WarmingConfig};

#[cfg(test)]
mod tests;
