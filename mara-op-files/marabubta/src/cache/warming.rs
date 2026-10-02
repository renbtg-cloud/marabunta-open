// Marabunta - Licensed under the MIT License.
//! Cache warming and preloading utilities

use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tokio::sync::Semaphore;

use super::traits::Cache;

/// Strategy for preloading cache entries
#[derive(Debug, Clone)]
pub enum PreloadStrategy {
    /// Load all entries at once
    Eager,
    /// Load entries in batches with delays between batches
    Batched {
        batch_size: usize,
        delay_between_batches: Duration,
    },
    /// Load entries with rate limiting
    RateLimited {
        max_concurrent: usize,
        delay_per_item: Duration,
    },
    /// Load entries on-demand as they're accessed
    Lazy,
}

impl Default for PreloadStrategy {
    fn default() -> Self {
        PreloadStrategy::Batched {
            batch_size: 100,
            delay_between_batches: Duration::from_millis(10),
        }
    }
}

/// Result of a warming operation
#[derive(Debug, Clone)]
pub struct WarmingResult {
    /// Number of entries successfully loaded
    pub loaded: usize,
    /// Number of entries that failed to load
    pub failed: usize,
    /// Total time taken
    pub duration: Duration,
    /// Errors encountered (key -> error message)
    pub errors: Vec<(String, String)>,
}

impl WarmingResult {
    fn new() -> Self {
        Self {
            loaded: 0,
            failed: 0,
            duration: Duration::ZERO,
            errors: Vec::new(),
        }
    }

    /// Check if all entries were loaded successfully
    pub fn is_complete(&self) -> bool {
        self.failed == 0
    }

    /// Get the success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.loaded + self.failed;
        if total == 0 {
            1.0
        } else {
            self.loaded as f64 / total as f64
        }
    }
}

impl std::fmt::Display for WarmingResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Cache Warming Result:")?;
        writeln!(f, "  Loaded: {}", self.loaded)?;
        writeln!(f, "  Failed: {}", self.failed)?;
        writeln!(f, "  Success Rate: {:.1}%", self.success_rate() * 100.0)?;
        writeln!(f, "  Duration: {:?}", self.duration)?;
        if !self.errors.is_empty() {
            writeln!(f, "  Errors:")?;
            for (key, error) in &self.errors {
                writeln!(f, "    {}: {}", key, error)?;
            }
        }
        Ok(())
    }
}

/// Type alias for async loader functions
pub type AsyncLoader<K, V> = Box<
    dyn Fn(K) -> Pin<Box<dyn Future<Output = Result<V, String>> + Send>> + Send + Sync,
>;

/// A cache warmer that can preload entries from various sources
pub struct CacheWarmer<K, V, C>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
    C: Cache<K, V> + 'static,
{
    cache: Arc<C>,
    strategy: PreloadStrategy,
    /// Progress tracking
    progress: RwLock<WarmingProgress>,
    /// Phantom data for type parameters
    _phantom: std::marker::PhantomData<(K, V)>,
}

/// Progress tracking for warming operations
#[derive(Debug, Clone, Default)]
struct WarmingProgress {
    total: usize,
    completed: usize,
    in_progress: bool,
    started_at: Option<Instant>,
}

impl<K, V, C> CacheWarmer<K, V, C>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
    C: Cache<K, V> + 'static,
{
    /// Create a new cache warmer
    pub fn new(cache: Arc<C>) -> Self {
        Self {
            cache,
            strategy: PreloadStrategy::default(),
            progress: RwLock::new(WarmingProgress::default()),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set the preload strategy
    pub fn with_strategy(mut self, strategy: PreloadStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Get current progress
    pub fn progress(&self) -> (usize, usize) {
        let progress = self.progress.read();
        (progress.completed, progress.total)
    }

    /// Check if warming is in progress
    pub fn is_warming(&self) -> bool {
        self.progress.read().in_progress
    }

    /// Warm the cache with pre-computed values
    pub fn warm_with_values(&self, entries: Vec<(K, V)>) -> WarmingResult {
        let start = Instant::now();
        let mut result = WarmingResult::new();

        {
            let mut progress = self.progress.write();
            progress.total = entries.len();
            progress.completed = 0;
            progress.in_progress = true;
            progress.started_at = Some(start);
        }

        match &self.strategy {
            PreloadStrategy::Eager => {
                for (key, value) in entries {
                    self.cache.set(key, value);
                    result.loaded += 1;
                    self.progress.write().completed += 1;
                }
            }
            PreloadStrategy::Batched { batch_size, delay_between_batches } => {
                for chunk in entries.chunks(*batch_size) {
                    for (key, value) in chunk {
                        self.cache.set(key.clone(), value.clone());
                        result.loaded += 1;
                        self.progress.write().completed += 1;
                    }
                    std::thread::sleep(*delay_between_batches);
                }
            }
            PreloadStrategy::RateLimited { delay_per_item, .. } => {
                for (key, value) in entries {
                    self.cache.set(key, value);
                    result.loaded += 1;
                    self.progress.write().completed += 1;
                    std::thread::sleep(*delay_per_item);
                }
            }
            PreloadStrategy::Lazy => {
                // For lazy strategy, just store the entries without loading
                for (key, value) in entries {
                    self.cache.set(key, value);
                    result.loaded += 1;
                    self.progress.write().completed += 1;
                }
            }
        }

        result.duration = start.elapsed();
        self.progress.write().in_progress = false;
        result
    }

    /// Warm the cache using a synchronous loader function
    pub fn warm_with_loader<F>(&self, keys: Vec<K>, loader: F) -> WarmingResult
    where
        F: Fn(&K) -> Result<V, String>,
    {
        let start = Instant::now();
        let mut result = WarmingResult::new();

        {
            let mut progress = self.progress.write();
            progress.total = keys.len();
            progress.completed = 0;
            progress.in_progress = true;
            progress.started_at = Some(start);
        }

        match &self.strategy {
            PreloadStrategy::Eager => {
                for key in keys {
                    match loader(&key) {
                        Ok(value) => {
                            self.cache.set(key, value);
                            result.loaded += 1;
                        }
                        Err(e) => {
                            result.failed += 1;
                            result.errors.push((key.to_string(), e));
                        }
                    }
                    self.progress.write().completed += 1;
                }
            }
            PreloadStrategy::Batched { batch_size, delay_between_batches } => {
                for chunk in keys.chunks(*batch_size) {
                    for key in chunk {
                        match loader(key) {
                            Ok(value) => {
                                self.cache.set(key.clone(), value);
                                result.loaded += 1;
                            }
                            Err(e) => {
                                result.failed += 1;
                                result.errors.push((key.to_string(), e));
                            }
                        }
                        self.progress.write().completed += 1;
                    }
                    std::thread::sleep(*delay_between_batches);
                }
            }
            PreloadStrategy::RateLimited { delay_per_item, .. } => {
                for key in keys {
                    match loader(&key) {
                        Ok(value) => {
                            self.cache.set(key.clone(), value);
                            result.loaded += 1;
                        }
                        Err(e) => {
                            result.failed += 1;
                            result.errors.push((key.to_string(), e));
                        }
                    }
                    self.progress.write().completed += 1;
                    std::thread::sleep(*delay_per_item);
                }
            }
            PreloadStrategy::Lazy => {
                // For lazy strategy, we don't load anything upfront
                tracing::debug!("Lazy strategy: skipping preload for {} keys", keys.len());
            }
        }

        result.duration = start.elapsed();
        self.progress.write().in_progress = false;
        result
    }

    /// Warm the cache using an async loader function
    pub async fn warm_async<F, Fut>(&self, keys: Vec<K>, loader: F) -> WarmingResult
    where
        F: Fn(K) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<V, String>> + Send,
    {
        let start = Instant::now();
        let mut result = WarmingResult::new();

        {
            let mut progress = self.progress.write();
            progress.total = keys.len();
            progress.completed = 0;
            progress.in_progress = true;
            progress.started_at = Some(start);
        }

        match &self.strategy {
            PreloadStrategy::Eager => {
                let futures: Vec<_> = keys
                    .into_iter()
                    .map(|key| {
                        let loader = loader.clone();
                        let key_str = key.to_string();
                        async move {
                            let result = loader(key.clone()).await;
                            (key, key_str, result)
                        }
                    })
                    .collect();

                let results = futures::future::join_all(futures).await;
                for (key, key_str, res) in results {
                    match res {
                        Ok(value) => {
                            self.cache.set(key, value);
                            result.loaded += 1;
                        }
                        Err(e) => {
                            result.failed += 1;
                            result.errors.push((key_str, e));
                        }
                    }
                    self.progress.write().completed += 1;
                }
            }
            PreloadStrategy::Batched { batch_size, delay_between_batches } => {
                for chunk in keys.chunks(*batch_size) {
                    let futures: Vec<_> = chunk
                        .iter()
                        .cloned()
                        .map(|key| {
                            let loader = loader.clone();
                            let key_str = key.to_string();
                            async move {
                                let result = loader(key.clone()).await;
                                (key, key_str, result)
                            }
                        })
                        .collect();

                    let results = futures::future::join_all(futures).await;
                    for (key, key_str, res) in results {
                        match res {
                            Ok(value) => {
                                self.cache.set(key, value);
                                result.loaded += 1;
                            }
                            Err(e) => {
                                result.failed += 1;
                                result.errors.push((key_str, e));
                            }
                        }
                        self.progress.write().completed += 1;
                    }

                    tokio::time::sleep(*delay_between_batches).await;
                }
            }
            PreloadStrategy::RateLimited { max_concurrent, delay_per_item } => {
                let semaphore = Arc::new(Semaphore::new(*max_concurrent));
                let delay = *delay_per_item;

                let futures: Vec<_> = keys
                    .into_iter()
                    .map(|key| {
                        let loader = loader.clone();
                        let sem = Arc::clone(&semaphore);
                        let key_str = key.to_string();
                        async move {
                            let _permit = sem.acquire().await.unwrap();
                            let result = loader(key.clone()).await;
                            tokio::time::sleep(delay).await;
                            (key, key_str, result)
                        }
                    })
                    .collect();

                let results = futures::future::join_all(futures).await;
                for (key, key_str, res) in results {
                    match res {
                        Ok(value) => {
                            self.cache.set(key, value);
                            result.loaded += 1;
                        }
                        Err(e) => {
                            result.failed += 1;
                            result.errors.push((key_str, e));
                        }
                    }
                    self.progress.write().completed += 1;
                }
            }
            PreloadStrategy::Lazy => {
                tracing::debug!("Lazy strategy: skipping async preload");
            }
        }

        result.duration = start.elapsed();
        self.progress.write().in_progress = false;
        result
    }
}

/// Helper for creating warming configurations
#[derive(Debug, Clone)]
pub struct WarmingConfig<K> {
    /// Keys to warm
    pub keys: Vec<K>,
    /// Strategy to use
    pub strategy: PreloadStrategy,
    /// Whether to warm in background
    pub background: bool,
    /// Priority (higher = more important)
    pub priority: u8,
}

impl<K> Default for WarmingConfig<K> {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            strategy: PreloadStrategy::default(),
            background: true,
            priority: 0,
        }
    }
}

impl<K> WarmingConfig<K> {
    /// Create a new warming config with the specified keys
    pub fn new(keys: Vec<K>) -> Self {
        Self {
            keys,
            ..Default::default()
        }
    }

    /// Set the strategy
    pub fn with_strategy(mut self, strategy: PreloadStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set whether to run in background
    pub fn in_background(mut self, background: bool) -> Self {
        self.background = background;
        self
    }

    /// Set the priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// Preload helper for common patterns
pub struct PreloadHelper;

impl PreloadHelper {
    /// Create a warming config for hot data (frequently accessed)
    pub fn hot_data<K>(keys: Vec<K>) -> WarmingConfig<K> {
        WarmingConfig::new(keys)
            .with_strategy(PreloadStrategy::Eager)
            .in_background(false)
            .with_priority(255)
    }

    /// Create a warming config for warm data (moderately accessed)
    pub fn warm_data<K>(keys: Vec<K>) -> WarmingConfig<K> {
        WarmingConfig::new(keys)
            .with_strategy(PreloadStrategy::Batched {
                batch_size: 50,
                delay_between_batches: Duration::from_millis(5),
            })
            .in_background(true)
            .with_priority(128)
    }

    /// Create a warming config for cold data (rarely accessed)
    pub fn cold_data<K>(keys: Vec<K>) -> WarmingConfig<K> {
        WarmingConfig::new(keys)
            .with_strategy(PreloadStrategy::Lazy)
            .in_background(true)
            .with_priority(0)
    }

    /// Create a warming config for large datasets
    pub fn large_dataset<K>(keys: Vec<K>) -> WarmingConfig<K> {
        WarmingConfig::new(keys)
            .with_strategy(PreloadStrategy::RateLimited {
                max_concurrent: 10,
                delay_per_item: Duration::from_millis(1),
            })
            .in_background(true)
            .with_priority(64)
    }
}
