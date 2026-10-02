// Marabunta - Licensed under the MIT License.
//! Specialized caches for Marabunta Compute domain objects

use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;

use crate::common::{JobId, JobStatus, TaskId, TaskResult, WorkerId, WorkerCapabilities};
use super::lru::LruCache;
use super::ttl::TtlCache;
use super::stats::CacheStats;
use super::traits::Cache;

/// Cache configuration for specialized caches
#[derive(Debug, Clone)]
pub struct SpecializedCacheConfig {
    /// Maximum number of entries
    pub max_entries: usize,
    /// Default TTL for entries
    pub default_ttl: Duration,
    /// Whether to enable statistics
    pub enable_stats: bool,
}

impl Default for SpecializedCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,
            default_ttl: Duration::from_secs(300), // 5 minutes
            enable_stats: true,
        }
    }
}

// ============================================================================
// Job Status Cache
// ============================================================================

/// Cached job status with metadata
#[derive(Debug, Clone)]
pub struct CachedJobStatus {
    pub status: JobStatus,
    pub progress_percent: f32,
    pub tasks_completed: u32,
    pub tasks_total: u32,
    pub error_message: Option<String>,
}

impl CachedJobStatus {
    pub fn new(status: JobStatus) -> Self {
        Self {
            status,
            progress_percent: 0.0,
            tasks_completed: 0,
            tasks_total: 0,
            error_message: None,
        }
    }

    pub fn with_progress(mut self, completed: u32, total: u32) -> Self {
        self.tasks_completed = completed;
        self.tasks_total = total;
        self.progress_percent = if total > 0 {
            (completed as f32 / total as f32) * 100.0
        } else {
            0.0
        };
        self
    }

    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error_message = Some(error.into());
        self
    }
}

/// Specialized cache for job status lookups
pub struct JobStatusCache {
    cache: TtlCache<JobId, CachedJobStatus>,
    /// Subscribers waiting for status changes
    subscribers: RwLock<Vec<(JobId, tokio::sync::oneshot::Sender<CachedJobStatus>)>>,
}

impl JobStatusCache {
    /// Create a new job status cache with default configuration
    pub fn new() -> Self {
        Self::with_config(SpecializedCacheConfig::default())
    }

    /// Create a new job status cache with custom configuration
    pub fn with_config(config: SpecializedCacheConfig) -> Self {
        Self {
            cache: TtlCache::with_max_size(config.default_ttl, config.max_entries),
            subscribers: RwLock::new(Vec::new()),
        }
    }

    /// Get the status of a job
    pub fn get_status(&self, job_id: &JobId) -> Option<CachedJobStatus> {
        self.cache.get(job_id)
    }

    /// Update the status of a job
    pub fn set_status(&self, job_id: JobId, status: CachedJobStatus) {
        // Notify subscribers
        let mut subscribers = self.subscribers.write();
        let mut i = 0;
        while i < subscribers.len() {
            if subscribers[i].0 == job_id {
                let (_, sender) = subscribers.remove(i);
                let _ = sender.send(status.clone());
            } else {
                i += 1;
            }
        }

        self.cache.set(job_id, status);
    }

    /// Update job progress
    pub fn update_progress(&self, job_id: &JobId, completed: u32, total: u32) {
        if let Some(mut status) = self.cache.get(job_id) {
            status = status.with_progress(completed, total);
            self.cache.set(*job_id, status);
        }
    }

    /// Mark a job as completed
    pub fn mark_completed(&self, job_id: &JobId) {
        if let Some(mut status) = self.cache.get(job_id) {
            status.status = JobStatus::Completed;
            status.progress_percent = 100.0;
            self.cache.set(*job_id, status);
        }
    }

    /// Mark a job as failed
    pub fn mark_failed(&self, job_id: &JobId, error: impl Into<String>) {
        let status = self
            .cache
            .get(job_id)
            .map(|s| CachedJobStatus { status: JobStatus::Failed, ..s })
            .unwrap_or_else(|| CachedJobStatus::new(JobStatus::Failed))
            .with_error(error);
        self.cache.set(*job_id, status);
    }

    /// Subscribe to status changes for a job
    pub fn subscribe(&self, job_id: JobId) -> tokio::sync::oneshot::Receiver<CachedJobStatus> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.subscribers.write().push((job_id, tx));
        rx
    }

    /// Get all cached job IDs
    pub fn cached_jobs(&self) -> Vec<JobId> {
        self.cache.keys()
    }

    /// Get statistics
    pub fn stats(&self) -> &CacheStats {
        self.cache.stats()
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of cached entries
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for JobStatusCache {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Task Result Cache
// ============================================================================

/// Cached task result with additional metadata
#[derive(Debug, Clone)]
pub struct CachedTaskResult {
    pub result: TaskResult,
    pub cached_at: std::time::Instant,
    pub worker_id: Option<WorkerId>,
}

impl CachedTaskResult {
    pub fn new(result: TaskResult) -> Self {
        Self {
            result,
            cached_at: std::time::Instant::now(),
            worker_id: None,
        }
    }

    pub fn with_worker(mut self, worker_id: WorkerId) -> Self {
        self.worker_id = Some(worker_id);
        self
    }
}

/// Specialized cache for task results
/// Uses LRU eviction since task results can be large
pub struct TaskResultCache {
    cache: LruCache<TaskId, CachedTaskResult>,
    /// Maximum output size to cache (larger results are not cached)
    max_output_size: usize,
}

impl TaskResultCache {
    /// Create a new task result cache with default configuration
    pub fn new() -> Self {
        Self::with_config(SpecializedCacheConfig::default())
    }

    /// Create a new task result cache with custom configuration
    pub fn with_config(config: SpecializedCacheConfig) -> Self {
        Self {
            cache: LruCache::new(config.max_entries),
            max_output_size: 1024 * 1024, // 1MB default
        }
    }

    /// Create with a custom max output size
    pub fn with_max_output_size(mut self, max_size: usize) -> Self {
        self.max_output_size = max_size;
        self
    }

    /// Get a task result
    pub fn get_result(&self, task_id: &TaskId) -> Option<CachedTaskResult> {
        self.cache.get(task_id)
    }

    /// Cache a task result
    pub fn cache_result(&self, task_id: TaskId, result: TaskResult) {
        // Skip caching if output is too large
        if let Some(ref output) = result.output {
            if output.len() > self.max_output_size {
                tracing::debug!(
                    "Skipping cache for task {} - output too large ({} bytes)",
                    task_id,
                    output.len()
                );
                return;
            }
        }

        self.cache.set(task_id, CachedTaskResult::new(result));
    }

    /// Cache a task result with worker information
    pub fn cache_result_with_worker(
        &self,
        task_id: TaskId,
        result: TaskResult,
        worker_id: WorkerId,
    ) {
        // Skip caching if output is too large
        if let Some(ref output) = result.output {
            if output.len() > self.max_output_size {
                return;
            }
        }

        self.cache
            .set(task_id, CachedTaskResult::new(result).with_worker(worker_id));
    }

    /// Remove a task result from cache
    pub fn invalidate(&self, task_id: &TaskId) -> Option<CachedTaskResult> {
        self.cache.delete(task_id)
    }

    /// Get all cached task IDs
    pub fn cached_tasks(&self) -> Vec<TaskId> {
        self.cache.keys()
    }

    /// Get successful results only
    pub fn successful_results(&self) -> Vec<(TaskId, CachedTaskResult)> {
        self.cache
            .entries()
            .into_iter()
            .filter(|(_, r)| r.result.success)
            .collect()
    }

    /// Get failed results only
    pub fn failed_results(&self) -> Vec<(TaskId, CachedTaskResult)> {
        self.cache
            .entries()
            .into_iter()
            .filter(|(_, r)| !r.result.success)
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> &CacheStats {
        self.cache.stats()
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of cached entries
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for TaskResultCache {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Worker Capabilities Cache
// ============================================================================

/// Extended worker capabilities with computed fields
#[derive(Debug, Clone)]
pub struct CachedWorkerCapabilities {
    pub capabilities: WorkerCapabilities,
    /// Last known load factor (0.0 - 1.0)
    pub current_load: f64,
    /// Number of tasks currently running
    pub running_tasks: u32,
    /// Whether the worker is currently healthy
    pub is_healthy: bool,
    /// Computed score for scheduling (higher is better)
    pub scheduling_score: f64,
    /// Last update timestamp
    pub last_updated: std::time::Instant,
}

impl CachedWorkerCapabilities {
    pub fn new(capabilities: WorkerCapabilities) -> Self {
        let mut cached = Self {
            capabilities,
            current_load: 0.0,
            running_tasks: 0,
            is_healthy: true,
            scheduling_score: 1.0,
            last_updated: std::time::Instant::now(),
        };
        cached.recalculate_score();
        cached
    }

    /// Update the load information
    pub fn update_load(&mut self, load: f64, running_tasks: u32) {
        self.current_load = load;
        self.running_tasks = running_tasks;
        self.last_updated = std::time::Instant::now();
        self.recalculate_score();
    }

    /// Set health status
    pub fn set_health(&mut self, is_healthy: bool) {
        self.is_healthy = is_healthy;
        self.last_updated = std::time::Instant::now();
        self.recalculate_score();
    }

    /// Recalculate the scheduling score
    fn recalculate_score(&mut self) {
        if !self.is_healthy {
            self.scheduling_score = 0.0;
            return;
        }

        // Score based on available capacity
        let cpu_factor = 1.0 - self.current_load;
        let memory_factor = self.capabilities.memory_mb as f64 / 1024.0; // Normalize to GB
        let gpu_bonus = if self.capabilities.has_gpu { 2.0 } else { 1.0 };

        self.scheduling_score = cpu_factor * memory_factor.sqrt() * gpu_bonus;
    }

    /// Check if this worker can handle a specific task type
    pub fn supports_task_type(&self, task_type: &str) -> bool {
        self.capabilities.supported_tasks.iter().any(|t| t == task_type)
    }

    /// Get available memory (estimated)
    pub fn available_memory_mb(&self) -> u64 {
        let used_ratio = self.current_load.min(1.0);
        ((1.0 - used_ratio) * self.capabilities.memory_mb as f64) as u64
    }
}

/// Specialized cache for worker capabilities
/// Uses TTL to ensure capabilities are refreshed periodically
pub struct WorkerCapabilitiesCache {
    cache: TtlCache<WorkerId, CachedWorkerCapabilities>,
    /// Minimum refresh interval
    min_refresh_interval: Duration,
}

impl WorkerCapabilitiesCache {
    /// Create a new worker capabilities cache with default configuration
    pub fn new() -> Self {
        Self::with_config(SpecializedCacheConfig {
            default_ttl: Duration::from_secs(60), // Workers should refresh every minute
            ..Default::default()
        })
    }

    /// Create a new worker capabilities cache with custom configuration
    pub fn with_config(config: SpecializedCacheConfig) -> Self {
        Self {
            cache: TtlCache::with_max_size(config.default_ttl, config.max_entries),
            min_refresh_interval: Duration::from_secs(5),
        }
    }

    /// Get worker capabilities
    pub fn get_capabilities(&self, worker_id: &WorkerId) -> Option<CachedWorkerCapabilities> {
        self.cache.get(worker_id)
    }

    /// Update worker capabilities
    pub fn update_capabilities(&self, worker_id: WorkerId, capabilities: WorkerCapabilities) {
        let cached = CachedWorkerCapabilities::new(capabilities);
        self.cache.set(worker_id, cached);
    }

    /// Update worker load information
    pub fn update_load(&self, worker_id: &WorkerId, load: f64, running_tasks: u32) {
        if let Some(mut cached) = self.cache.get(worker_id) {
            cached.update_load(load, running_tasks);
            self.cache.set(*worker_id, cached);
        }
    }

    /// Update worker health status
    pub fn set_worker_health(&self, worker_id: &WorkerId, is_healthy: bool) {
        if let Some(mut cached) = self.cache.get(worker_id) {
            cached.set_health(is_healthy);
            self.cache.set(*worker_id, cached);
        }
    }

    /// Remove a worker from the cache
    pub fn remove_worker(&self, worker_id: &WorkerId) -> Option<CachedWorkerCapabilities> {
        self.cache.delete(worker_id)
    }

    /// Get all cached workers
    pub fn all_workers(&self) -> Vec<(WorkerId, CachedWorkerCapabilities)> {
        self.cache.entries()
    }

    /// Get all healthy workers
    pub fn healthy_workers(&self) -> Vec<(WorkerId, CachedWorkerCapabilities)> {
        self.cache
            .entries()
            .into_iter()
            .filter(|(_, c)| c.is_healthy)
            .collect()
    }

    /// Get workers that support a specific task type
    pub fn workers_for_task_type(&self, task_type: &str) -> Vec<(WorkerId, CachedWorkerCapabilities)> {
        self.cache
            .entries()
            .into_iter()
            .filter(|(_, c)| c.is_healthy && c.supports_task_type(task_type))
            .collect()
    }

    /// Get the best workers for scheduling (sorted by score descending)
    pub fn best_workers(&self, limit: usize) -> Vec<(WorkerId, CachedWorkerCapabilities)> {
        let mut workers: Vec<_> = self
            .cache
            .entries()
            .into_iter()
            .filter(|(_, c)| c.is_healthy)
            .collect();

        workers.sort_by(|(_, a), (_, b)| {
            b.scheduling_score
                .partial_cmp(&a.scheduling_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        workers.into_iter().take(limit).collect()
    }

    /// Get workers with GPU
    pub fn gpu_workers(&self) -> Vec<(WorkerId, CachedWorkerCapabilities)> {
        self.cache
            .entries()
            .into_iter()
            .filter(|(_, c)| c.is_healthy && c.capabilities.has_gpu)
            .collect()
    }

    /// Get total cluster capacity
    pub fn total_capacity(&self) -> ClusterCapacity {
        let workers = self.healthy_workers();
        ClusterCapacity {
            total_workers: workers.len(),
            total_cpu_cores: workers.iter().map(|(_, c)| c.capabilities.cpu_cores).sum(),
            total_memory_mb: workers.iter().map(|(_, c)| c.capabilities.memory_mb).sum(),
            total_disk_mb: workers.iter().map(|(_, c)| c.capabilities.disk_mb).sum(),
            gpu_workers: workers.iter().filter(|(_, c)| c.capabilities.has_gpu).count(),
            avg_load: if workers.is_empty() {
                0.0
            } else {
                workers.iter().map(|(_, c)| c.current_load).sum::<f64>() / workers.len() as f64
            },
        }
    }

    /// Get statistics
    pub fn stats(&self) -> &CacheStats {
        self.cache.stats()
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of cached entries
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for WorkerCapabilitiesCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of cluster capacity
#[derive(Debug, Clone)]
pub struct ClusterCapacity {
    pub total_workers: usize,
    pub total_cpu_cores: u32,
    pub total_memory_mb: u64,
    pub total_disk_mb: u64,
    pub gpu_workers: usize,
    pub avg_load: f64,
}

impl std::fmt::Display for ClusterCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Cluster Capacity:")?;
        writeln!(f, "  Workers:    {} ({} with GPU)", self.total_workers, self.gpu_workers)?;
        writeln!(f, "  CPU Cores:  {}", self.total_cpu_cores)?;
        writeln!(f, "  Memory:     {} MB", self.total_memory_mb)?;
        writeln!(f, "  Disk:       {} MB", self.total_disk_mb)?;
        writeln!(f, "  Avg Load:   {:.1}%", self.avg_load * 100.0)?;
        Ok(())
    }
}

// ============================================================================
// Combined Cache Manager
// ============================================================================

/// A combined cache manager for all specialized caches
pub struct CacheManager {
    pub job_status: Arc<JobStatusCache>,
    pub task_results: Arc<TaskResultCache>,
    pub worker_capabilities: Arc<WorkerCapabilitiesCache>,
}

impl CacheManager {
    /// Create a new cache manager with default configurations
    pub fn new() -> Self {
        Self {
            job_status: Arc::new(JobStatusCache::new()),
            task_results: Arc::new(TaskResultCache::new()),
            worker_capabilities: Arc::new(WorkerCapabilitiesCache::new()),
        }
    }

    /// Create with custom configurations
    pub fn with_configs(
        job_config: SpecializedCacheConfig,
        task_config: SpecializedCacheConfig,
        worker_config: SpecializedCacheConfig,
    ) -> Self {
        Self {
            job_status: Arc::new(JobStatusCache::with_config(job_config)),
            task_results: Arc::new(TaskResultCache::with_config(task_config)),
            worker_capabilities: Arc::new(WorkerCapabilitiesCache::with_config(worker_config)),
        }
    }

    /// Clear all caches
    pub fn clear_all(&self) {
        self.job_status.clear();
        self.task_results.clear();
        self.worker_capabilities.clear();
    }

    /// Get combined statistics
    pub fn combined_stats(&self) -> CombinedCacheStats {
        CombinedCacheStats {
            job_status: self.job_status.stats().snapshot(),
            task_results: self.task_results.stats().snapshot(),
            worker_capabilities: self.worker_capabilities.stats().snapshot(),
        }
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined statistics from all caches
#[derive(Debug)]
pub struct CombinedCacheStats {
    pub job_status: super::stats::CacheStatsSnapshot,
    pub task_results: super::stats::CacheStatsSnapshot,
    pub worker_capabilities: super::stats::CacheStatsSnapshot,
}

impl std::fmt::Display for CombinedCacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Job Status Cache ===")?;
        write!(f, "{}", self.job_status)?;
        writeln!(f)?;
        writeln!(f, "=== Task Results Cache ===")?;
        write!(f, "{}", self.task_results)?;
        writeln!(f)?;
        writeln!(f, "=== Worker Capabilities Cache ===")?;
        write!(f, "{}", self.worker_capabilities)?;
        Ok(())
    }
}
