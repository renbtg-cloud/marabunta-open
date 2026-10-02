// Marabunta - Licensed under the MIT License.
//! Resource hints and requests for task scheduling
//!
//! Tasks can provide hints about their resource needs to help
//! the scheduler make better decisions:
//! - Memory requirements
//! - Estimated execution time
//! - Parallelization hints

use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

/// Resource hints provided by a task
#[derive(Debug)]
pub struct ResourceHints {
    /// Estimated memory needed in bytes
    memory_bytes: AtomicU64,
    /// Estimated time to complete in seconds
    time_seconds: AtomicU32,
    /// Whether the task can be split into parallel chunks
    can_split: AtomicBool,
    /// Minimum number of chunks if splitting
    min_chunks: AtomicU32,
    /// Maximum number of chunks if splitting
    max_chunks: AtomicU32,
    /// Custom hints (key-value pairs)
    custom_hints: RwLock<Vec<(String, String)>>,
}

impl ResourceHints {
    /// Create new resource hints with default values
    pub fn new() -> Self {
        Self {
            memory_bytes: AtomicU64::new(0),
            time_seconds: AtomicU32::new(0),
            can_split: AtomicBool::new(false),
            min_chunks: AtomicU32::new(1),
            max_chunks: AtomicU32::new(1),
            custom_hints: RwLock::new(Vec::new()),
        }
    }

    /// Set estimated memory requirement
    #[inline]
    pub fn set_memory_needed(&self, bytes: u64) {
        self.memory_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Get estimated memory requirement
    #[inline]
    pub fn get_memory_needed(&self) -> u64 {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    /// Set estimated time to complete
    #[inline]
    pub fn set_time_estimate(&self, seconds: u32) {
        self.time_seconds.store(seconds, Ordering::Relaxed);
    }

    /// Get estimated time to complete
    #[inline]
    pub fn get_time_estimate(&self) -> u32 {
        self.time_seconds.load(Ordering::Relaxed)
    }

    /// Set whether the task can be split into parallel chunks
    pub fn set_can_split(&self, can_split: bool, min_chunks: u32, max_chunks: u32) {
        self.can_split.store(can_split, Ordering::Relaxed);
        self.min_chunks.store(min_chunks.max(1), Ordering::Relaxed);
        self.max_chunks
            .store(max_chunks.max(min_chunks), Ordering::Relaxed);
    }

    /// Check if the task can be split
    #[inline]
    pub fn get_can_split(&self) -> bool {
        self.can_split.load(Ordering::Relaxed)
    }

    /// Get minimum number of chunks
    #[inline]
    pub fn get_min_chunks(&self) -> u32 {
        self.min_chunks.load(Ordering::Relaxed)
    }

    /// Get maximum number of chunks
    #[inline]
    pub fn get_max_chunks(&self) -> u32 {
        self.max_chunks.load(Ordering::Relaxed)
    }

    /// Add a custom hint
    pub fn set_custom_hint(&self, key: &str, value: &str) {
        let mut hints = self.custom_hints.write();
        // Update existing or add new
        if let Some(existing) = hints.iter_mut().find(|(k, _)| k == key) {
            existing.1 = value.to_string();
        } else {
            hints.push((key.to_string(), value.to_string()));
        }
    }

    /// Get a custom hint
    pub fn get_custom_hint(&self, key: &str) -> Option<String> {
        self.custom_hints
            .read()
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }

    /// Get all custom hints
    pub fn get_all_custom_hints(&self) -> Vec<(String, String)> {
        self.custom_hints.read().clone()
    }

    /// Clear all hints
    pub fn clear(&self) {
        self.memory_bytes.store(0, Ordering::Relaxed);
        self.time_seconds.store(0, Ordering::Relaxed);
        self.can_split.store(false, Ordering::Relaxed);
        self.min_chunks.store(1, Ordering::Relaxed);
        self.max_chunks.store(1, Ordering::Relaxed);
        self.custom_hints.write().clear();
    }

    /// Get a snapshot of all resource hints
    pub fn snapshot(&self) -> ResourceHintsSnapshot {
        ResourceHintsSnapshot {
            memory_bytes: self.get_memory_needed(),
            time_seconds: self.get_time_estimate(),
            can_split: self.get_can_split(),
            min_chunks: self.get_min_chunks(),
            max_chunks: self.get_max_chunks(),
            custom_hints: self.get_all_custom_hints(),
        }
    }
}

impl Default for ResourceHints {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable snapshot of resource hints
#[derive(Debug, Clone)]
pub struct ResourceHintsSnapshot {
    /// Estimated memory needed in bytes
    pub memory_bytes: u64,
    /// Estimated time to complete in seconds
    pub time_seconds: u32,
    /// Whether the task can be split
    pub can_split: bool,
    /// Minimum number of chunks
    pub min_chunks: u32,
    /// Maximum number of chunks
    pub max_chunks: u32,
    /// Custom hints
    pub custom_hints: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_new_hints_have_defaults() {
        let hints = ResourceHints::new();
        assert_eq!(hints.get_memory_needed(), 0);
        assert_eq!(hints.get_time_estimate(), 0);
        assert!(!hints.get_can_split());
        assert_eq!(hints.get_min_chunks(), 1);
        assert_eq!(hints.get_max_chunks(), 1);
    }

    #[test]
    fn test_memory_hint() {
        let hints = ResourceHints::new();
        hints.set_memory_needed(1024 * 1024 * 512); // 512 MB
        assert_eq!(hints.get_memory_needed(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_time_estimate() {
        let hints = ResourceHints::new();
        hints.set_time_estimate(3600); // 1 hour
        assert_eq!(hints.get_time_estimate(), 3600);
    }

    #[test]
    fn test_can_split() {
        let hints = ResourceHints::new();
        hints.set_can_split(true, 4, 16);
        assert!(hints.get_can_split());
        assert_eq!(hints.get_min_chunks(), 4);
        assert_eq!(hints.get_max_chunks(), 16);
    }

    #[test]
    fn test_split_min_is_at_least_one() {
        let hints = ResourceHints::new();
        hints.set_can_split(true, 0, 10);
        assert_eq!(hints.get_min_chunks(), 1);
    }

    #[test]
    fn test_split_max_at_least_min() {
        let hints = ResourceHints::new();
        hints.set_can_split(true, 10, 5);
        assert_eq!(hints.get_min_chunks(), 10);
        assert_eq!(hints.get_max_chunks(), 10);
    }

    #[test]
    fn test_custom_hints() {
        let hints = ResourceHints::new();
        hints.set_custom_hint("gpu_required", "true");
        hints.set_custom_hint("priority", "high");

        assert_eq!(
            hints.get_custom_hint("gpu_required"),
            Some("true".to_string())
        );
        assert_eq!(hints.get_custom_hint("priority"), Some("high".to_string()));
        assert_eq!(hints.get_custom_hint("nonexistent"), None);
    }

    #[test]
    fn test_custom_hint_update() {
        let hints = ResourceHints::new();
        hints.set_custom_hint("priority", "low");
        hints.set_custom_hint("priority", "high");

        assert_eq!(hints.get_custom_hint("priority"), Some("high".to_string()));
        // Should only have one entry for "priority"
        let all = hints.get_all_custom_hints();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_clear() {
        let hints = ResourceHints::new();
        hints.set_memory_needed(1000);
        hints.set_time_estimate(60);
        hints.set_can_split(true, 2, 8);
        hints.set_custom_hint("key", "value");

        hints.clear();

        assert_eq!(hints.get_memory_needed(), 0);
        assert_eq!(hints.get_time_estimate(), 0);
        assert!(!hints.get_can_split());
        assert_eq!(hints.get_min_chunks(), 1);
        assert_eq!(hints.get_max_chunks(), 1);
        assert!(hints.get_all_custom_hints().is_empty());
    }

    #[test]
    fn test_snapshot() {
        let hints = ResourceHints::new();
        hints.set_memory_needed(1024);
        hints.set_time_estimate(30);
        hints.set_can_split(true, 2, 4);
        hints.set_custom_hint("test", "value");

        let snapshot = hints.snapshot();

        assert_eq!(snapshot.memory_bytes, 1024);
        assert_eq!(snapshot.time_seconds, 30);
        assert!(snapshot.can_split);
        assert_eq!(snapshot.min_chunks, 2);
        assert_eq!(snapshot.max_chunks, 4);
        assert_eq!(snapshot.custom_hints.len(), 1);
    }

    #[test]
    fn test_thread_safety() {
        let hints = Arc::new(ResourceHints::new());
        let mut handles = vec![];

        // Multiple threads updating memory hint
        for i in 0..10 {
            let hints_clone = Arc::clone(&hints);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    hints_clone.set_memory_needed((i * 100 + j) as u64);
                }
            }));
        }

        // Multiple threads reading
        for _ in 0..5 {
            let hints_clone = Arc::clone(&hints);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _ = hints_clone.get_memory_needed();
                    let _ = hints_clone.snapshot();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_large_memory_values() {
        let hints = ResourceHints::new();
        // Test with 1 TB
        hints.set_memory_needed(1024 * 1024 * 1024 * 1024);
        assert_eq!(hints.get_memory_needed(), 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_many_custom_hints() {
        let hints = ResourceHints::new();
        for i in 0..100 {
            hints.set_custom_hint(&format!("key_{}", i), &format!("value_{}", i));
        }

        let all = hints.get_all_custom_hints();
        assert_eq!(all.len(), 100);

        assert_eq!(
            hints.get_custom_hint("key_50"),
            Some("value_50".to_string())
        );
    }
}
