// Marabunta - Licensed under the MIT License.
//! Task execution context
//!
//! The MarabuntaContext is the central structure that tasks interact with.
//! It provides access to all SDK functionality: progress reporting,
//! checkpointing, resource hints, and communication with the worker runtime.

use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::sdk::checkpoint::CheckpointStorage;
use crate::sdk::errors::*;
use crate::sdk::progress::ProgressState;
use crate::sdk::resources::ResourceHints;

/// Maximum result buffer size (64 MB)
pub const MAX_RESULT_SIZE: usize = 64 * 1024 * 1024;

/// Log entry for the task
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub level: i32,
    pub message: String,
    pub timestamp: std::time::SystemTime,
}

/// Task execution context
///
/// This is the main structure that FFI code interacts with.
/// It is designed to be thread-safe and panic-safe.
pub struct MarabuntaContext {
    /// Unique task identifier
    task_id: AtomicU64,
    /// Worker rank (0 to worker_count-1)
    worker_rank: AtomicU32,
    /// Total number of workers
    worker_count: AtomicU32,
    /// Progress state
    progress: ProgressState,
    /// Checkpoint storage
    checkpoint: RwLock<Option<CheckpointStorage>>,
    /// Resource hints
    resources: ResourceHints,
    /// Result buffer
    result_buffer: RwLock<Vec<u8>>,
    /// Whether the task should abort
    abort_flag: AtomicBool,
    /// Whether the task should pause
    pause_flag: AtomicBool,
    /// Whether the context has been initialized
    initialized: AtomicBool,
    /// Log entries
    logs: RwLock<Vec<LogEntry>>,
    /// Checkpoint directory
    checkpoint_dir: RwLock<PathBuf>,
}

impl MarabuntaContext {
    /// Create a new uninitialized context
    ///
    /// Call `initialize` to set up the context for a specific task.
    pub fn new() -> Self {
        Self {
            task_id: AtomicU64::new(0),
            worker_rank: AtomicU32::new(0),
            worker_count: AtomicU32::new(1),
            progress: ProgressState::new(),
            checkpoint: RwLock::new(None),
            resources: ResourceHints::new(),
            result_buffer: RwLock::new(Vec::with_capacity(4096)),
            abort_flag: AtomicBool::new(false),
            pause_flag: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            logs: RwLock::new(Vec::new()),
            checkpoint_dir: RwLock::new(PathBuf::from("/tmp/marabunta_checkpoints")),
        }
    }

    /// Initialize the context for a task
    pub fn initialize(
        &self,
        task_id: u64,
        worker_rank: u32,
        worker_count: u32,
        checkpoint_dir: PathBuf,
    ) -> i32 {
        if self.initialized.swap(true, Ordering::SeqCst) {
            return MARABUNTA_ERR_ALREADY_INITIALIZED;
        }

        self.task_id.store(task_id, Ordering::Relaxed);
        self.worker_rank.store(worker_rank, Ordering::Relaxed);
        self.worker_count
            .store(worker_count.max(1), Ordering::Relaxed);
        *self.checkpoint_dir.write() = checkpoint_dir.clone();

        // Set up checkpoint storage
        *self.checkpoint.write() = Some(CheckpointStorage::new(checkpoint_dir, task_id));

        info!(
            "MarabuntaContext initialized: task_id={}, rank={}/{}",
            task_id, worker_rank, worker_count
        );

        MARABUNTA_OK
    }

    /// Check if the context is initialized
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Relaxed)
    }

    /// Get the task ID
    #[inline]
    pub fn task_id(&self) -> u64 {
        self.task_id.load(Ordering::Relaxed)
    }

    /// Get the worker rank
    #[inline]
    pub fn worker_rank(&self) -> u32 {
        self.worker_rank.load(Ordering::Relaxed)
    }

    /// Get the total worker count
    #[inline]
    pub fn worker_count(&self) -> u32 {
        self.worker_count.load(Ordering::Relaxed)
    }

    /// Get access to progress state
    #[inline]
    pub fn progress(&self) -> &ProgressState {
        &self.progress
    }

    /// Get access to resource hints
    #[inline]
    pub fn resources(&self) -> &ResourceHints {
        &self.resources
    }

    /// Save checkpoint data
    pub fn checkpoint_save(&self, data: &[u8]) -> i32 {
        let checkpoint = self.checkpoint.read();
        match checkpoint.as_ref() {
            Some(cp) => cp.save(data),
            None => MARABUNTA_ERR_NOT_INITIALIZED,
        }
    }

    /// Load checkpoint data
    pub fn checkpoint_load(&self, buffer: &mut [u8]) -> i64 {
        let checkpoint = self.checkpoint.read();
        match checkpoint.as_ref() {
            Some(cp) => cp.load(buffer),
            None => MARABUNTA_ERR_NOT_INITIALIZED as i64,
        }
    }

    /// Check if a checkpoint exists
    pub fn checkpoint_exists(&self) -> bool {
        let checkpoint = self.checkpoint.read();
        checkpoint.as_ref().is_some_and(|cp| cp.exists())
    }

    /// Set the abort flag
    pub fn set_abort(&self) {
        self.abort_flag.store(true, Ordering::Release);
    }

    /// Set the pause flag
    pub fn set_pause(&self, pause: bool) {
        self.pause_flag.store(pause, Ordering::Release);
    }

    /// Check if the task should abort
    #[inline]
    pub fn should_abort(&self) -> bool {
        self.abort_flag.load(Ordering::Acquire)
    }

    /// Check if the task should pause
    #[inline]
    pub fn should_pause(&self) -> bool {
        self.pause_flag.load(Ordering::Acquire)
    }

    /// Yield control (cooperative scheduling)
    ///
    /// Returns MARABUNTA_YIELD_CONTINUE, MARABUNTA_YIELD_PAUSE, or MARABUNTA_YIELD_ABORT
    pub fn yield_point(&self) -> i32 {
        if self.should_abort() {
            MARABUNTA_YIELD_ABORT
        } else if self.should_pause() {
            MARABUNTA_YIELD_PAUSE
        } else {
            MARABUNTA_YIELD_CONTINUE
        }
    }

    /// Set the result data (replaces existing)
    pub fn result_set(&self, data: &[u8]) -> i32 {
        if data.len() > MAX_RESULT_SIZE {
            return MARABUNTA_ERR_BUFFER_TOO_SMALL;
        }
        let mut result = self.result_buffer.write();
        result.clear();
        result.extend_from_slice(data);
        MARABUNTA_OK
    }

    /// Append to the result data
    pub fn result_append(&self, data: &[u8]) -> i32 {
        let mut result = self.result_buffer.write();
        if result.len() + data.len() > MAX_RESULT_SIZE {
            return MARABUNTA_ERR_BUFFER_TOO_SMALL;
        }
        result.extend_from_slice(data);
        MARABUNTA_OK
    }

    /// Get a copy of the result data
    pub fn result_get(&self) -> Vec<u8> {
        self.result_buffer.read().clone()
    }

    /// Get the current result size
    pub fn result_size(&self) -> usize {
        self.result_buffer.read().len()
    }

    /// Clear the result buffer
    pub fn result_clear(&self) {
        self.result_buffer.write().clear();
    }

    /// Log a message
    pub fn log(&self, level: i32, message: &str) {
        let entry = LogEntry {
            level,
            message: message.to_string(),
            timestamp: std::time::SystemTime::now(),
        };

        // Also log to tracing
        match level {
            MARABUNTA_LOG_DEBUG => debug!(task_id = %self.task_id(), "{}", message),
            MARABUNTA_LOG_INFO => info!(task_id = %self.task_id(), "{}", message),
            MARABUNTA_LOG_WARN => warn!(task_id = %self.task_id(), "{}", message),
            MARABUNTA_LOG_ERROR => error!(task_id = %self.task_id(), "{}", message),
            _ => debug!(task_id = %self.task_id(), "{}", message),
        }

        self.logs.write().push(entry);
    }

    /// Get all log entries
    pub fn get_logs(&self) -> Vec<LogEntry> {
        self.logs.read().clone()
    }

    /// Reset the context for reuse
    pub fn reset(&self) {
        self.initialized.store(false, Ordering::SeqCst);
        self.task_id.store(0, Ordering::Relaxed);
        self.worker_rank.store(0, Ordering::Relaxed);
        self.worker_count.store(1, Ordering::Relaxed);
        self.progress.reset();
        self.resources.clear();
        self.result_buffer.write().clear();
        self.abort_flag.store(false, Ordering::Relaxed);
        self.pause_flag.store(false, Ordering::Relaxed);
        self.logs.write().clear();
        *self.checkpoint.write() = None;
    }
}

impl Default for MarabuntaContext {
    fn default() -> Self {
        Self::new()
    }
}

// Ensure MarabuntaContext is Send + Sync for thread safety
unsafe impl Send for MarabuntaContext {}
unsafe impl Sync for MarabuntaContext {}

/// Thread-safe reference to a MarabuntaContext
pub type MarabuntaContextRef = Arc<MarabuntaContext>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::TempDir;

    fn create_test_context() -> (TempDir, MarabuntaContext) {
        let temp_dir = TempDir::new().unwrap();
        let ctx = MarabuntaContext::new();
        ctx.initialize(12345, 0, 4, temp_dir.path().to_path_buf());
        (temp_dir, ctx)
    }

    #[test]
    fn test_new_context_not_initialized() {
        let ctx = MarabuntaContext::new();
        assert!(!ctx.is_initialized());
    }

    #[test]
    fn test_initialize() {
        let temp_dir = TempDir::new().unwrap();
        let ctx = MarabuntaContext::new();
        let result = ctx.initialize(100, 2, 8, temp_dir.path().to_path_buf());
        assert_eq!(result, MARABUNTA_OK);
        assert!(ctx.is_initialized());
        assert_eq!(ctx.task_id(), 100);
        assert_eq!(ctx.worker_rank(), 2);
        assert_eq!(ctx.worker_count(), 8);
    }

    #[test]
    fn test_double_initialize() {
        let temp_dir = TempDir::new().unwrap();
        let ctx = MarabuntaContext::new();
        ctx.initialize(100, 0, 1, temp_dir.path().to_path_buf());
        let result = ctx.initialize(200, 0, 1, temp_dir.path().to_path_buf());
        assert_eq!(result, MARABUNTA_ERR_ALREADY_INITIALIZED);
    }

    #[test]
    fn test_worker_count_at_least_one() {
        let temp_dir = TempDir::new().unwrap();
        let ctx = MarabuntaContext::new();
        ctx.initialize(100, 0, 0, temp_dir.path().to_path_buf());
        assert_eq!(ctx.worker_count(), 1);
    }

    #[test]
    fn test_progress_integration() {
        let (_temp_dir, ctx) = create_test_context();
        ctx.progress().set_percent(50);
        ctx.progress().set_stage("Testing");
        ctx.progress().set_message("Running test");

        assert_eq!(ctx.progress().get_percent(), 50);
        assert_eq!(ctx.progress().get_stage(), "Testing");
        assert_eq!(ctx.progress().get_message(), "Running test");
    }

    #[test]
    fn test_resources_integration() {
        let (_temp_dir, ctx) = create_test_context();
        ctx.resources().set_memory_needed(1024 * 1024);
        ctx.resources().set_time_estimate(60);
        ctx.resources().set_can_split(true, 2, 8);

        assert_eq!(ctx.resources().get_memory_needed(), 1024 * 1024);
        assert_eq!(ctx.resources().get_time_estimate(), 60);
        assert!(ctx.resources().get_can_split());
    }

    #[test]
    fn test_checkpoint_integration() {
        let (_temp_dir, ctx) = create_test_context();

        let data = b"checkpoint data";
        assert_eq!(ctx.checkpoint_save(data), MARABUNTA_OK);
        assert!(ctx.checkpoint_exists());

        let mut buffer = vec![0u8; 100];
        let len = ctx.checkpoint_load(&mut buffer);
        assert_eq!(len, data.len() as i64);
        assert_eq!(&buffer[..data.len()], data);
    }

    #[test]
    fn test_checkpoint_not_initialized() {
        let ctx = MarabuntaContext::new();
        let mut buffer = vec![0u8; 100];
        assert_eq!(
            ctx.checkpoint_load(&mut buffer),
            MARABUNTA_ERR_NOT_INITIALIZED as i64
        );
        assert_eq!(ctx.checkpoint_save(b"data"), MARABUNTA_ERR_NOT_INITIALIZED);
    }

    #[test]
    fn test_abort_flag() {
        let (_temp_dir, ctx) = create_test_context();
        assert!(!ctx.should_abort());
        ctx.set_abort();
        assert!(ctx.should_abort());
    }

    #[test]
    fn test_pause_flag() {
        let (_temp_dir, ctx) = create_test_context();
        assert!(!ctx.should_pause());
        ctx.set_pause(true);
        assert!(ctx.should_pause());
        ctx.set_pause(false);
        assert!(!ctx.should_pause());
    }

    #[test]
    fn test_yield_point() {
        let (_temp_dir, ctx) = create_test_context();
        assert_eq!(ctx.yield_point(), MARABUNTA_YIELD_CONTINUE);

        ctx.set_pause(true);
        assert_eq!(ctx.yield_point(), MARABUNTA_YIELD_PAUSE);

        ctx.set_abort();
        assert_eq!(ctx.yield_point(), MARABUNTA_YIELD_ABORT);
    }

    #[test]
    fn test_result_set() {
        let (_temp_dir, ctx) = create_test_context();
        let data = b"result data";
        assert_eq!(ctx.result_set(data), MARABUNTA_OK);
        assert_eq!(ctx.result_get(), data.to_vec());
    }

    #[test]
    fn test_result_append() {
        let (_temp_dir, ctx) = create_test_context();
        ctx.result_set(b"Hello, ");
        ctx.result_append(b"World!");
        assert_eq!(ctx.result_get(), b"Hello, World!".to_vec());
    }

    #[test]
    fn test_result_size() {
        let (_temp_dir, ctx) = create_test_context();
        assert_eq!(ctx.result_size(), 0);
        ctx.result_set(b"12345");
        assert_eq!(ctx.result_size(), 5);
    }

    #[test]
    fn test_result_clear() {
        let (_temp_dir, ctx) = create_test_context();
        ctx.result_set(b"data");
        ctx.result_clear();
        assert_eq!(ctx.result_size(), 0);
    }

    #[test]
    fn test_result_too_large() {
        let (_temp_dir, ctx) = create_test_context();
        let large_data = vec![0u8; MAX_RESULT_SIZE + 1];
        assert_eq!(ctx.result_set(&large_data), MARABUNTA_ERR_BUFFER_TOO_SMALL);
    }

    #[test]
    fn test_logging() {
        let (_temp_dir, ctx) = create_test_context();
        ctx.log(MARABUNTA_LOG_INFO, "Test message");
        ctx.log(MARABUNTA_LOG_ERROR, "Error message");

        let logs = ctx.get_logs();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].level, MARABUNTA_LOG_INFO);
        assert_eq!(logs[0].message, "Test message");
        assert_eq!(logs[1].level, MARABUNTA_LOG_ERROR);
    }

    #[test]
    fn test_reset() {
        let (_temp_dir, ctx) = create_test_context();
        ctx.progress().set_percent(50);
        ctx.result_set(b"data");
        ctx.log(MARABUNTA_LOG_INFO, "message");

        ctx.reset();

        assert!(!ctx.is_initialized());
        assert_eq!(ctx.task_id(), 0);
        assert_eq!(ctx.progress().get_percent(), 0);
        assert_eq!(ctx.result_size(), 0);
        assert!(ctx.get_logs().is_empty());
    }

    #[test]
    fn test_thread_safety() {
        let (_temp_dir, ctx) = create_test_context();
        let ctx = Arc::new(ctx);
        let mut handles = vec![];

        // Multiple threads updating progress
        for i in 0..5 {
            let ctx_clone = Arc::clone(&ctx);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    ctx_clone.progress().increment(1);
                    ctx_clone.log(MARABUNTA_LOG_DEBUG, &format!("Thread {} working", i));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Progress should be at 100 (clamped)
        assert_eq!(ctx.progress().get_percent(), 100);
    }

    #[test]
    fn test_concurrent_result_updates() {
        let (_temp_dir, ctx) = create_test_context();
        let ctx = Arc::new(ctx);
        let mut handles = vec![];

        // Clear and set from multiple threads
        for i in 0..10 {
            let ctx_clone = Arc::clone(&ctx);
            handles.push(thread::spawn(move || {
                for j in 0..50 {
                    let data = format!("Thread {} iteration {}", i, j);
                    ctx_clone.result_set(data.as_bytes());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Just verify we have some result and no crashes
        assert!(ctx.result_size() > 0);
    }
}
