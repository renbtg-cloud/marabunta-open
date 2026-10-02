// Marabunta - Licensed under the MIT License.
//! Comprehensive tests for the cache module

use std::sync::Arc;
use std::time::Duration;
use std::thread;

use super::*;
use crate::common::{JobId, JobStatus, TaskId, TaskResult, WorkerId, WorkerCapabilities};

// ============================================================================
// Cache Trait Tests
// ============================================================================

mod cache_entry_tests {
    use super::*;

    #[test]
    fn test_cache_entry_creation() {
        let entry = CacheEntry::new("test_value".to_string());
        assert_eq!(entry.value, "test_value");
        assert_eq!(entry.access_count, 1);
        assert!(!entry.is_expired());
    }

    #[test]
    fn test_cache_entry_with_ttl() {
        let entry = CacheEntry::with_ttl("test", Duration::from_millis(50));
        assert!(!entry.is_expired());
        thread::sleep(Duration::from_millis(60));
        assert!(entry.is_expired());
    }

    #[test]
    fn test_cache_entry_access_tracking() {
        let mut entry = CacheEntry::new(42);
        assert_eq!(entry.access_count, 1);
        entry.record_access();
        assert_eq!(entry.access_count, 2);
        entry.record_access();
        assert_eq!(entry.access_count, 3);
    }

    #[test]
    fn test_cache_entry_age() {
        let entry = CacheEntry::new("test");
        thread::sleep(Duration::from_millis(10));
        assert!(entry.age() >= Duration::from_millis(10));
    }
}

// ============================================================================
// LRU Cache Tests
// ============================================================================

mod lru_cache_tests {
    use super::*;

    #[test]
    fn test_lru_basic_operations() {
        let cache = LruCache::new(10);

        // Test set and get
        cache.set("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));

        // Test non-existent key
        assert_eq!(cache.get(&"key2".to_string()), None);

        // Test delete
        let deleted = cache.delete(&"key1".to_string());
        assert_eq!(deleted, Some("value1".to_string()));
        assert_eq!(cache.get(&"key1".to_string()), None);
    }

    #[test]
    fn test_lru_eviction() {
        let cache = LruCache::new(3);

        cache.set(1, "one");
        cache.set(2, "two");
        cache.set(3, "three");

        // Cache is full, adding one more should evict the oldest (1)
        cache.set(4, "four");

        assert_eq!(cache.get(&1), None); // Evicted
        assert_eq!(cache.get(&2), Some("two"));
        assert_eq!(cache.get(&3), Some("three"));
        assert_eq!(cache.get(&4), Some("four"));
    }

    #[test]
    fn test_lru_access_updates_order() {
        let cache = LruCache::new(3);

        cache.set(1, "one");
        cache.set(2, "two");
        cache.set(3, "three");

        // Access key 1, making it most recently used
        let _ = cache.get(&1);

        // Add key 4, should evict key 2 (least recently used now)
        cache.set(4, "four");

        assert_eq!(cache.get(&1), Some("one")); // Still present
        assert_eq!(cache.get(&2), None); // Evicted
        assert_eq!(cache.get(&3), Some("three"));
        assert_eq!(cache.get(&4), Some("four"));
    }

    #[test]
    fn test_lru_update_existing_key() {
        let cache = LruCache::new(5);

        cache.set("key".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key".to_string()), Some("value1".to_string()));

        cache.set("key".to_string(), "value2".to_string());
        assert_eq!(cache.get(&"key".to_string()), Some("value2".to_string()));

        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_lru_clear() {
        let cache = LruCache::new(10);

        cache.set(1, "one");
        cache.set(2, "two");
        cache.set(3, "three");

        assert_eq!(cache.len(), 3);

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert_eq!(cache.get(&1), None);
    }

    #[test]
    fn test_lru_keys() {
        let cache = LruCache::new(10);

        cache.set(1, "one");
        cache.set(2, "two");
        cache.set(3, "three");

        let mut keys = cache.keys();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn test_lru_contains() {
        let cache = LruCache::new(10);

        cache.set("key".to_string(), "value".to_string());

        assert!(cache.contains(&"key".to_string()));
        assert!(!cache.contains(&"nonexistent".to_string()));
    }

    #[test]
    fn test_lru_get_or_insert() {
        let cache = LruCache::new(10);

        let value1 = cache.get_or_insert("key".to_string(), || "computed".to_string());
        assert_eq!(value1, "computed");

        let value2 = cache.get_or_insert("key".to_string(), || "should_not_compute".to_string());
        assert_eq!(value2, "computed"); // Returns cached value
    }

    #[test]
    fn test_lru_with_ttl() {
        let cache = LruCache::new(10);

        cache.set_with_ttl("key".to_string(), "value".to_string(), Duration::from_millis(50));

        assert_eq!(cache.get(&"key".to_string()), Some("value".to_string()));

        thread::sleep(Duration::from_millis(60));

        // Entry should be expired
        assert_eq!(cache.get(&"key".to_string()), None);
    }

    #[test]
    fn test_lru_resize() {
        let cache = LruCache::new(5);

        for i in 0..5 {
            cache.set(i, format!("value{}", i));
        }
        assert_eq!(cache.len(), 5);

        // Shrink the cache
        let evicted = cache.resize(3);
        assert_eq!(evicted, 2);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_lru_get_many() {
        let cache = LruCache::new(10);

        cache.set(1, "one");
        cache.set(2, "two");
        cache.set(3, "three");

        let results = cache.get_many(&[1, 2, 4, 3]);
        assert_eq!(results, vec![Some("one"), Some("two"), None, Some("three")]);
    }

    #[test]
    fn test_lru_statistics() {
        let cache = LruCache::new(3);

        cache.set(1, "one");
        cache.set(2, "two");

        let _ = cache.get(&1); // Hit
        let _ = cache.get(&2); // Hit
        let _ = cache.get(&3); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits(), 2);
        assert_eq!(stats.misses(), 1);
        assert!(stats.hit_rate() > 0.6);
    }

    #[test]
    fn test_lru_thread_safety() {
        let cache = Arc::new(LruCache::new(1000));
        let mut handles = vec![];

        for i in 0..10 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    cache.set(i * 100 + j, format!("value_{}_{}", i, j));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(cache.len(), 1000);
    }

    #[test]
    fn test_lru_peek() {
        let cache = LruCache::new(3);

        cache.set(1, "one");
        cache.set(2, "two");
        cache.set(3, "three");

        // Peek at 1 without updating access order
        let peeked = cache.peek(&1);
        assert_eq!(peeked, Some("one"));

        // Add new entry - should evict 1 if peek updated order, but it shouldn't
        cache.set(4, "four");

        // Key 1 should be evicted because peek doesn't update order
        assert_eq!(cache.get(&1), None);
    }
}

// ============================================================================
// TTL Cache Tests
// ============================================================================

mod ttl_cache_tests {
    use super::*;

    #[test]
    fn test_ttl_basic_operations() {
        let cache = TtlCache::new(Duration::from_secs(60));

        cache.set("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));

        cache.delete(&"key1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), None);
    }

    #[test]
    fn test_ttl_expiration() {
        let cache = TtlCache::new(Duration::from_millis(50));

        cache.set("key".to_string(), "value".to_string());
        assert_eq!(cache.get(&"key".to_string()), Some("value".to_string()));

        thread::sleep(Duration::from_millis(60));

        assert_eq!(cache.get(&"key".to_string()), None);
    }

    #[test]
    fn test_ttl_custom_per_entry() {
        let cache = TtlCache::new(Duration::from_secs(60));

        cache.set_with_ttl("short".to_string(), "value".to_string(), Duration::from_millis(50));
        cache.set("long".to_string(), "value".to_string());

        thread::sleep(Duration::from_millis(60));

        assert_eq!(cache.get(&"short".to_string()), None);
        assert_eq!(cache.get(&"long".to_string()), Some("value".to_string()));
    }

    #[test]
    fn test_ttl_refresh() {
        let cache = TtlCache::new(Duration::from_millis(100));

        cache.set("key".to_string(), "value".to_string());

        thread::sleep(Duration::from_millis(60));

        // Refresh the TTL
        assert!(cache.refresh(&"key".to_string()));

        thread::sleep(Duration::from_millis(60));

        // Should still be valid because we refreshed
        assert_eq!(cache.get(&"key".to_string()), Some("value".to_string()));
    }

    #[test]
    fn test_ttl_time_until_expiry() {
        let cache = TtlCache::new(Duration::from_millis(100));

        cache.set("key".to_string(), "value".to_string());

        let time_left = cache.time_until_expiry(&"key".to_string());
        assert!(time_left.is_some());
        assert!(time_left.unwrap() <= Duration::from_millis(100));

        thread::sleep(Duration::from_millis(110));

        let time_left = cache.time_until_expiry(&"key".to_string());
        assert!(time_left.is_none());
    }

    #[test]
    fn test_ttl_with_max_size() {
        let cache = TtlCache::with_max_size(Duration::from_secs(60), 3);

        cache.set(1, "one");
        cache.set(2, "two");
        cache.set(3, "three");
        cache.set(4, "four"); // Should evict oldest

        assert_eq!(cache.len(), 3);
        // One of the first entries should be evicted
        let all_present = cache.get(&1).is_some()
            && cache.get(&2).is_some()
            && cache.get(&3).is_some()
            && cache.get(&4).is_some();
        assert!(!all_present);
    }

    #[test]
    fn test_ttl_evict_expired() {
        let cache = TtlCache::new(Duration::from_millis(50));

        cache.set(1, "one");
        cache.set(2, "two");

        thread::sleep(Duration::from_millis(60));

        let evicted = cache.evict_expired();
        assert_eq!(evicted, 2);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_ttl_entries_with_ttl() {
        let cache = TtlCache::new(Duration::from_secs(60));

        cache.set(1, "one");
        cache.set(2, "two");

        let entries = cache.entries_with_ttl();
        assert_eq!(entries.len(), 2);

        for (_, _, ttl) in entries {
            assert!(ttl.is_some());
            assert!(ttl.unwrap() <= Duration::from_secs(60));
        }
    }

    #[test]
    fn test_ttl_get_many() {
        let cache = TtlCache::new(Duration::from_secs(60));

        cache.set(1, "one");
        cache.set(2, "two");
        cache.set(3, "three");

        let results = cache.get_many(&[1, 4, 2]);
        assert_eq!(results, vec![Some("one"), None, Some("two")]);
    }

    #[test]
    fn test_ttl_statistics() {
        let cache = TtlCache::new(Duration::from_secs(60));

        cache.set(1, "one");
        let _ = cache.get(&1); // Hit
        let _ = cache.get(&2); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits(), 1);
        assert_eq!(stats.misses(), 1);
    }
}

// ============================================================================
// Two-Tier Cache Tests
// ============================================================================

mod two_tier_cache_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_two_tier_memory_only() {
        let cache: TwoTierCache<String, String> = TwoTierCache::memory_only(10);

        cache.set("key".to_string(), "value".to_string());
        assert_eq!(cache.get(&"key".to_string()), Some("value".to_string()));

        assert!(!cache.has_disk_layer());
        assert_eq!(cache.memory_len(), 1);
        assert_eq!(cache.disk_len(), 0);
    }

    #[test]
    fn test_two_tier_with_disk() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskSpillConfig::new(temp_dir.path().to_path_buf());

        let cache: TwoTierCache<String, String> =
            TwoTierCache::with_disk(10, config).unwrap();

        cache.set("key".to_string(), "value".to_string());
        assert_eq!(cache.get(&"key".to_string()), Some("value".to_string()));

        assert!(cache.has_disk_layer());
    }

    #[test]
    fn test_two_tier_disk_spill() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskSpillConfig::new(temp_dir.path().to_path_buf());

        // Small memory cache to force spilling
        let cache: TwoTierCache<String, String> =
            TwoTierCache::with_disk(2, config).unwrap();

        // Fill memory cache
        cache.set("key1".to_string(), "value1".to_string());
        cache.set("key2".to_string(), "value2".to_string());

        // This should trigger spill to disk
        cache.set("key3".to_string(), "value3".to_string());

        // All values should be retrievable
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(cache.get(&"key2".to_string()), Some("value2".to_string()));
        assert_eq!(cache.get(&"key3".to_string()), Some("value3".to_string()));
    }

    #[test]
    fn test_two_tier_clear() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskSpillConfig::new(temp_dir.path().to_path_buf());

        let cache: TwoTierCache<String, String> =
            TwoTierCache::with_disk(10, config).unwrap();

        cache.set("key".to_string(), "value".to_string());
        assert!(!cache.is_empty());

        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.memory_len(), 0);
        assert_eq!(cache.disk_len(), 0);
    }

    #[test]
    fn test_two_tier_delete() {
        let cache: TwoTierCache<String, String> = TwoTierCache::memory_only(10);

        cache.set("key".to_string(), "value".to_string());
        let deleted = cache.delete(&"key".to_string());

        assert_eq!(deleted, Some("value".to_string()));
        assert_eq!(cache.get(&"key".to_string()), None);
    }
}

// ============================================================================
// Statistics Tests
// ============================================================================

mod stats_tests {
    use super::*;

    #[test]
    fn test_stats_basic() {
        let stats = CacheStats::new();

        stats.record_hit();
        stats.record_hit();
        stats.record_miss();

        assert_eq!(stats.hits(), 2);
        assert_eq!(stats.misses(), 1);
        assert!((stats.hit_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_stats_eviction_tracking() {
        let stats = CacheStats::new();

        stats.record_eviction();
        stats.record_eviction_bytes(1024);

        assert_eq!(stats.evictions(), 2);
        assert_eq!(stats.bytes_evicted(), 1024);
    }

    #[test]
    fn test_stats_insert_tracking() {
        let stats = CacheStats::new();

        stats.record_insert();
        stats.record_insert_bytes(512);

        assert_eq!(stats.inserts(), 2);
        assert_eq!(stats.bytes_stored(), 512);
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = CacheStats::new();

        stats.record_hit();
        stats.record_miss();
        stats.record_insert();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.hits, 1);
        assert_eq!(snapshot.misses, 1);
        assert_eq!(snapshot.inserts, 1);
    }

    #[test]
    fn test_stats_reset() {
        let stats = CacheStats::new();

        stats.record_hit();
        stats.record_miss();

        stats.reset();

        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
    }

    #[test]
    fn test_stats_operation_time() {
        let stats = CacheStats::new();

        stats.record_operation_time(Duration::from_millis(10));
        stats.record_operation_time(Duration::from_millis(20));

        assert!(stats.total_operation_time() >= Duration::from_millis(30));
    }
}

// ============================================================================
// Specialized Cache Tests
// ============================================================================

mod job_status_cache_tests {
    use super::*;

    #[test]
    fn test_job_status_cache_basic() {
        let cache = JobStatusCache::new();

        let job_id = JobId::new();
        let status = CachedJobStatus::new(JobStatus::Running)
            .with_progress(5, 10);

        cache.set_status(job_id, status.clone());

        let retrieved = cache.get_status(&job_id).unwrap();
        assert!(matches!(retrieved.status, JobStatus::Running));
        assert_eq!(retrieved.tasks_completed, 5);
        assert_eq!(retrieved.tasks_total, 10);
        assert!((retrieved.progress_percent - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_job_status_update_progress() {
        let cache = JobStatusCache::new();

        let job_id = JobId::new();
        cache.set_status(job_id, CachedJobStatus::new(JobStatus::Running));

        cache.update_progress(&job_id, 3, 10);

        let status = cache.get_status(&job_id).unwrap();
        assert_eq!(status.tasks_completed, 3);
        assert!((status.progress_percent - 30.0).abs() < 0.1);
    }

    #[test]
    fn test_job_status_mark_completed() {
        let cache = JobStatusCache::new();

        let job_id = JobId::new();
        cache.set_status(job_id, CachedJobStatus::new(JobStatus::Running));

        cache.mark_completed(&job_id);

        let status = cache.get_status(&job_id).unwrap();
        assert!(matches!(status.status, JobStatus::Completed));
        assert!((status.progress_percent - 100.0).abs() < 0.1);
    }

    #[test]
    fn test_job_status_mark_failed() {
        let cache = JobStatusCache::new();

        let job_id = JobId::new();
        cache.set_status(job_id, CachedJobStatus::new(JobStatus::Running));

        cache.mark_failed(&job_id, "Task failed due to OOM");

        let status = cache.get_status(&job_id).unwrap();
        assert!(matches!(status.status, JobStatus::Failed));
        assert_eq!(status.error_message, Some("Task failed due to OOM".to_string()));
    }

    #[test]
    fn test_job_status_cached_jobs() {
        let cache = JobStatusCache::new();

        let job1 = JobId::new();
        let job2 = JobId::new();

        cache.set_status(job1, CachedJobStatus::new(JobStatus::Running));
        cache.set_status(job2, CachedJobStatus::new(JobStatus::Pending));

        let jobs = cache.cached_jobs();
        assert_eq!(jobs.len(), 2);
    }
}

mod task_result_cache_tests {
    use super::*;

    fn create_test_result(success: bool) -> TaskResult {
        TaskResult {
            success,
            exit_code: Some(if success { 0 } else { 1 }),
            stdout: "output".to_string(),
            stderr: String::new(),
            output: Some(vec![1, 2, 3]),
            duration_ms: 100,
        }
    }

    #[test]
    fn test_task_result_cache_basic() {
        let cache = TaskResultCache::new();

        let task_id = TaskId::new();
        let result = create_test_result(true);

        cache.cache_result(task_id, result.clone());

        let cached = cache.get_result(&task_id).unwrap();
        assert!(cached.result.success);
        assert_eq!(cached.result.stdout, "output");
    }

    #[test]
    fn test_task_result_with_worker() {
        let cache = TaskResultCache::new();

        let task_id = TaskId::new();
        let worker_id = WorkerId::new();
        let result = create_test_result(true);

        cache.cache_result_with_worker(task_id, result, worker_id);

        let cached = cache.get_result(&task_id).unwrap();
        assert_eq!(cached.worker_id, Some(worker_id));
    }

    #[test]
    fn test_task_result_invalidate() {
        let cache = TaskResultCache::new();

        let task_id = TaskId::new();
        cache.cache_result(task_id, create_test_result(true));

        let invalidated = cache.invalidate(&task_id);
        assert!(invalidated.is_some());

        assert!(cache.get_result(&task_id).is_none());
    }

    #[test]
    fn test_task_result_successful_only() {
        let cache = TaskResultCache::new();

        let task1 = TaskId::new();
        let task2 = TaskId::new();

        cache.cache_result(task1, create_test_result(true));
        cache.cache_result(task2, create_test_result(false));

        let successful = cache.successful_results();
        assert_eq!(successful.len(), 1);
        assert!(successful[0].1.result.success);
    }

    #[test]
    fn test_task_result_failed_only() {
        let cache = TaskResultCache::new();

        let task1 = TaskId::new();
        let task2 = TaskId::new();

        cache.cache_result(task1, create_test_result(true));
        cache.cache_result(task2, create_test_result(false));

        let failed = cache.failed_results();
        assert_eq!(failed.len(), 1);
        assert!(!failed[0].1.result.success);
    }

    #[test]
    fn test_task_result_large_output_skipped() {
        let cache = TaskResultCache::new().with_max_output_size(100);

        let task_id = TaskId::new();
        let mut result = create_test_result(true);
        result.output = Some(vec![0; 200]); // Larger than max_output_size

        cache.cache_result(task_id, result);

        // Should not be cached
        assert!(cache.get_result(&task_id).is_none());
    }
}

mod worker_capabilities_cache_tests {
    use super::*;

    fn create_test_capabilities() -> WorkerCapabilities {
        WorkerCapabilities {
            cpu_cores: 8,
            memory_mb: 16384,
            disk_mb: 512000,
            has_gpu: false,
            supported_tasks: vec!["shell".to_string(), "python".to_string()],
        }
    }

    #[test]
    fn test_worker_capabilities_basic() {
        let cache = WorkerCapabilitiesCache::new();

        let worker_id = WorkerId::new();
        let caps = create_test_capabilities();

        cache.update_capabilities(worker_id, caps);

        let cached = cache.get_capabilities(&worker_id).unwrap();
        assert_eq!(cached.capabilities.cpu_cores, 8);
        assert!(cached.is_healthy);
    }

    #[test]
    fn test_worker_capabilities_update_load() {
        let cache = WorkerCapabilitiesCache::new();

        let worker_id = WorkerId::new();
        cache.update_capabilities(worker_id, create_test_capabilities());

        cache.update_load(&worker_id, 0.75, 4);

        let cached = cache.get_capabilities(&worker_id).unwrap();
        assert!((cached.current_load - 0.75).abs() < 0.01);
        assert_eq!(cached.running_tasks, 4);
    }

    #[test]
    fn test_worker_capabilities_health() {
        let cache = WorkerCapabilitiesCache::new();

        let worker_id = WorkerId::new();
        cache.update_capabilities(worker_id, create_test_capabilities());

        cache.set_worker_health(&worker_id, false);

        let cached = cache.get_capabilities(&worker_id).unwrap();
        assert!(!cached.is_healthy);
        assert!((cached.scheduling_score - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_worker_capabilities_supports_task_type() {
        let cache = WorkerCapabilitiesCache::new();

        let worker_id = WorkerId::new();
        cache.update_capabilities(worker_id, create_test_capabilities());

        let cached = cache.get_capabilities(&worker_id).unwrap();
        assert!(cached.supports_task_type("shell"));
        assert!(cached.supports_task_type("python"));
        assert!(!cached.supports_task_type("fortran"));
    }

    #[test]
    fn test_worker_capabilities_healthy_workers() {
        let cache = WorkerCapabilitiesCache::new();

        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();

        cache.update_capabilities(worker1, create_test_capabilities());
        cache.update_capabilities(worker2, create_test_capabilities());

        cache.set_worker_health(&worker2, false);

        let healthy = cache.healthy_workers();
        assert_eq!(healthy.len(), 1);
    }

    #[test]
    fn test_worker_capabilities_best_workers() {
        let cache = WorkerCapabilitiesCache::new();

        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();

        let mut caps1 = create_test_capabilities();
        caps1.memory_mb = 8192;

        let mut caps2 = create_test_capabilities();
        caps2.memory_mb = 32768;

        cache.update_capabilities(worker1, caps1);
        cache.update_capabilities(worker2, caps2);

        let best = cache.best_workers(2);
        assert_eq!(best.len(), 2);
        // Worker2 should be first (more memory = higher score)
        assert_eq!(best[0].1.capabilities.memory_mb, 32768);
    }

    #[test]
    fn test_worker_capabilities_gpu_workers() {
        let cache = WorkerCapabilitiesCache::new();

        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();

        let mut caps1 = create_test_capabilities();
        caps1.has_gpu = true;

        let caps2 = create_test_capabilities(); // No GPU

        cache.update_capabilities(worker1, caps1);
        cache.update_capabilities(worker2, caps2);

        let gpu = cache.gpu_workers();
        assert_eq!(gpu.len(), 1);
        assert!(gpu[0].1.capabilities.has_gpu);
    }

    #[test]
    fn test_worker_capabilities_total_capacity() {
        let cache = WorkerCapabilitiesCache::new();

        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();

        cache.update_capabilities(worker1, create_test_capabilities());
        cache.update_capabilities(worker2, create_test_capabilities());

        let capacity = cache.total_capacity();
        assert_eq!(capacity.total_workers, 2);
        assert_eq!(capacity.total_cpu_cores, 16);
        assert_eq!(capacity.total_memory_mb, 32768);
    }
}

mod cache_manager_tests {
    use super::*;

    #[test]
    fn test_cache_manager_creation() {
        let manager = CacheManager::new();

        assert!(manager.job_status.is_empty());
        assert!(manager.task_results.is_empty());
        assert!(manager.worker_capabilities.is_empty());
    }

    #[test]
    fn test_cache_manager_clear_all() {
        let manager = CacheManager::new();

        manager.job_status.set_status(JobId::new(), CachedJobStatus::new(JobStatus::Running));
        manager.task_results.cache_result(TaskId::new(), TaskResult {
            success: true,
            exit_code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
            output: None,
            duration_ms: 0,
        });
        manager.worker_capabilities.update_capabilities(WorkerId::new(), WorkerCapabilities {
            cpu_cores: 4,
            memory_mb: 8192,
            disk_mb: 256000,
            has_gpu: false,
            supported_tasks: vec![],
        });

        manager.clear_all();

        assert!(manager.job_status.is_empty());
        assert!(manager.task_results.is_empty());
        assert!(manager.worker_capabilities.is_empty());
    }

    #[test]
    fn test_cache_manager_combined_stats() {
        let manager = CacheManager::new();

        // Generate some activity
        let job_id = JobId::new();
        manager.job_status.set_status(job_id, CachedJobStatus::new(JobStatus::Running));
        let _ = manager.job_status.get_status(&job_id);

        let stats = manager.combined_stats();
        assert!(stats.job_status.hits >= 1);
    }
}

// ============================================================================
// Warming Tests
// ============================================================================

mod warming_tests {
    use super::*;

    #[test]
    fn test_warming_with_values() {
        let cache = Arc::new(LruCache::new(100));
        let warmer = CacheWarmer::new(cache.clone());

        let entries: Vec<_> = (0..10)
            .map(|i| (format!("key{}", i), format!("value{}", i)))
            .collect();

        let result = warmer.warm_with_values(entries);

        assert_eq!(result.loaded, 10);
        assert_eq!(result.failed, 0);
        assert!(result.is_complete());
        assert_eq!(cache.len(), 10);
    }

    #[test]
    fn test_warming_with_loader() {
        let cache = Arc::new(LruCache::new(100));
        let warmer = CacheWarmer::new(cache.clone())
            .with_strategy(PreloadStrategy::Eager);

        let keys: Vec<_> = (0..5).map(|i| format!("key{}", i)).collect();

        let result = warmer.warm_with_loader(keys, |key| {
            Ok(format!("loaded_{}", key))
        });

        assert_eq!(result.loaded, 5);
        assert_eq!(result.failed, 0);
        assert_eq!(cache.get(&"key0".to_string()), Some("loaded_key0".to_string()));
    }

    #[test]
    fn test_warming_with_loader_failures() {
        let cache = Arc::new(LruCache::new(100));
        let warmer = CacheWarmer::new(cache.clone());

        let keys: Vec<_> = (0..5).map(|i| format!("key{}", i)).collect();

        let result = warmer.warm_with_loader(keys, |key| {
            if key.contains("2") {
                Err("Failed to load".to_string())
            } else {
                Ok(format!("value_{}", key))
            }
        });

        assert_eq!(result.loaded, 4);
        assert_eq!(result.failed, 1);
        assert!(!result.is_complete());
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_warming_lazy_strategy() {
        let cache = Arc::new(LruCache::new(100));
        let warmer = CacheWarmer::new(cache.clone())
            .with_strategy(PreloadStrategy::Lazy);

        let keys: Vec<_> = (0..5).map(|i| format!("key{}", i)).collect();

        let result = warmer.warm_with_loader(keys, |_| {
            Ok("value".to_string())
        });

        // Lazy strategy doesn't load anything
        assert_eq!(result.loaded, 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_warming_progress() {
        let cache = Arc::new(LruCache::new(100));
        let warmer = CacheWarmer::new(cache.clone());

        assert!(!warmer.is_warming());

        let entries: Vec<_> = (0..10)
            .map(|i| (format!("key{}", i), format!("value{}", i)))
            .collect();

        let _ = warmer.warm_with_values(entries);

        let (completed, total) = warmer.progress();
        assert_eq!(completed, 10);
        assert_eq!(total, 10);
    }

    #[test]
    fn test_preload_helper_hot_data() {
        let config = PreloadHelper::hot_data(vec!["key1", "key2"]);
        assert!(matches!(config.strategy, PreloadStrategy::Eager));
        assert!(!config.background);
        assert_eq!(config.priority, 255);
    }

    #[test]
    fn test_preload_helper_cold_data() {
        let config = PreloadHelper::cold_data(vec!["key1", "key2"]);
        assert!(matches!(config.strategy, PreloadStrategy::Lazy));
        assert!(config.background);
        assert_eq!(config.priority, 0);
    }

    #[tokio::test]
    async fn test_warming_async() {
        let cache = Arc::new(LruCache::new(100));
        let warmer = CacheWarmer::new(cache.clone())
            .with_strategy(PreloadStrategy::Eager);

        let keys: Vec<_> = (0..5).map(|i| format!("key{}", i)).collect();

        let result = warmer.warm_async(keys, |key| async move {
            Ok(format!("async_value_{}", key))
        }).await;

        assert_eq!(result.loaded, 5);
        assert_eq!(result.failed, 0);
        assert_eq!(cache.get(&"key0".to_string()), Some("async_value_key0".to_string()));
    }

    #[tokio::test]
    async fn test_warming_async_rate_limited() {
        let cache = Arc::new(LruCache::new(100));
        let warmer = CacheWarmer::new(cache.clone())
            .with_strategy(PreloadStrategy::RateLimited {
                max_concurrent: 2,
                delay_per_item: Duration::from_millis(1),
            });

        let keys: Vec<_> = (0..5).map(|i| format!("key{}", i)).collect();

        let result = warmer.warm_async(keys, |key| async move {
            Ok(format!("rate_limited_{}", key))
        }).await;

        assert_eq!(result.loaded, 5);
        assert_eq!(cache.len(), 5);
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

mod integration_tests {
    use super::*;

    #[test]
    fn test_full_workflow_job_status() {
        let cache = JobStatusCache::new();

        // Create job
        let job_id = JobId::new();
        cache.set_status(job_id, CachedJobStatus::new(JobStatus::Pending));

        // Start job
        cache.set_status(job_id, CachedJobStatus::new(JobStatus::Running));

        // Update progress
        for i in 1..=10 {
            cache.update_progress(&job_id, i, 10);
            thread::sleep(Duration::from_millis(1));
        }

        // Complete job
        cache.mark_completed(&job_id);

        let status = cache.get_status(&job_id).unwrap();
        assert!(matches!(status.status, JobStatus::Completed));
        assert!((status.progress_percent - 100.0).abs() < 0.1);
    }

    #[test]
    fn test_cache_interoperability() {
        // Test that specialized caches can be used together
        let manager = CacheManager::new();

        // Add workers
        for i in 0..5 {
            let worker_id = WorkerId::new();
            manager.worker_capabilities.update_capabilities(worker_id, WorkerCapabilities {
                cpu_cores: 4,
                memory_mb: 8192,
                disk_mb: 256000,
                has_gpu: i % 2 == 0,
                supported_tasks: vec!["shell".to_string()],
            });
        }

        // Add jobs and tasks
        let job_id = JobId::new();
        manager.job_status.set_status(job_id, CachedJobStatus::new(JobStatus::Running));

        for _ in 0..10 {
            let task_id = TaskId::new();
            manager.task_results.cache_result(task_id, TaskResult {
                success: true,
                exit_code: Some(0),
                stdout: "Done".to_string(),
                stderr: String::new(),
                output: None,
                duration_ms: 50,
            });
        }

        // Verify
        let capacity = manager.worker_capabilities.total_capacity();
        assert_eq!(capacity.total_workers, 5);
        assert_eq!(capacity.gpu_workers, 3);

        assert_eq!(manager.task_results.successful_results().len(), 10);
    }

    #[test]
    fn test_cache_under_load() {
        let cache = Arc::new(LruCache::new(1000));
        let mut handles = vec![];

        // Concurrent writes
        for i in 0..10 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    cache.set(format!("{}_{}", i, j), j);
                }
            }));
        }

        // Concurrent reads
        for _ in 0..5 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..200 {
                    let key = format!("{}_{}", rand::random::<u8>() % 10, rand::random::<u8>() % 100);
                    let _ = cache.get(&key);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(cache.len(), 1000);

        let stats = cache.stats();
        assert!(stats.inserts() >= 1000);
        assert!(stats.hits() + stats.misses() >= 1000);
    }
}
