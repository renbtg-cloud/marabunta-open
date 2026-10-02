// Marabunta - Licensed under the MIT License.
//! C ABI exported functions for the Marabunta Task SDK
//!
//! This module provides the C-compatible interface that allows any language
//! with C FFI support (Fortran, Python, MATLAB, etc.) to write distributed jobs.
//!
//! # Safety
//!
//! All functions in this module:
//! - Are marked `#[no_mangle]` and `extern "C"` for C ABI compatibility
//! - Handle null pointers gracefully
//! - Catch all panics and return error codes
//! - Are thread-safe
//!
//! # Error Handling
//!
//! All functions return i32 error codes:
//! - 0 (MARABUNTA_OK) = success
//! - Negative values = error codes (see errors.rs)
//! - Positive values may have function-specific meanings (e.g., byte counts)

use std::ffi::{c_char, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::ptr;
use std::slice;
use std::sync::atomic::Ordering;

use crate::sdk::context::MarabuntaContext;
use crate::sdk::errors::*;

/// Returns the platform-appropriate default checkpoint directory
fn default_checkpoint_dir() -> String {
    #[cfg(target_os = "android")]
    {
        "/data/local/tmp/marabunta_checkpoints".to_string()
    } // Will be overridden by JNI

    #[cfg(not(target_os = "android"))]
    {
        "/tmp/marabunta_checkpoints".to_string()
    }
}

/// Thread-local task ID counter for auto-generated IDs
static NEXT_TASK_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

// =============================================================================
// Lifecycle Functions
// =============================================================================

/// Initialize a new Marabunta task context
///
/// Returns a pointer to a new MarabuntaContext, or null on failure.
/// The context must be cleaned up with `marabunta_task_cleanup`.
///
/// # Example (C)
/// ```c
/// MarabuntaContext* ctx = marabunta_task_init();
/// if (ctx == NULL) {
///     // Handle error
/// }
/// // ... use context ...
/// marabunta_task_cleanup(ctx);
/// ```
#[no_mangle]
pub extern "C" fn marabunta_task_init() -> *mut MarabuntaContext {
    match catch_unwind(|| {
        let ctx = Box::new(MarabuntaContext::new());

        // Auto-initialize with default values
        let task_id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
        let checkpoint_dir = PathBuf::from(default_checkpoint_dir());

        // Initialize with defaults: task_id=auto, rank=0, count=1
        ctx.initialize(task_id, 0, 1, checkpoint_dir);

        Box::into_raw(ctx)
    }) {
        Ok(ptr) => ptr,
        Err(_) => ptr::null_mut(),
    }
}

/// Initialize a Marabunta task context with specific parameters
///
/// # Arguments
/// * `task_id` - Unique task identifier
/// * `worker_rank` - This worker's rank (0 to worker_count-1)
/// * `worker_count` - Total number of workers
/// * `checkpoint_dir` - Directory for checkpoint storage (null for default)
///
/// Returns a pointer to a new MarabuntaContext, or null on failure.
#[no_mangle]
pub extern "C" fn marabunta_task_init_ex(
    task_id: u64,
    worker_rank: u32,
    worker_count: u32,
    checkpoint_dir: *const c_char,
) -> *mut MarabuntaContext {
    match catch_unwind(AssertUnwindSafe(|| {
        let ctx = Box::new(MarabuntaContext::new());

        let dir = if checkpoint_dir.is_null() {
            PathBuf::from(default_checkpoint_dir())
        } else {
            unsafe {
                match CStr::from_ptr(checkpoint_dir).to_str() {
                    Ok(s) => PathBuf::from(s),
                    Err(_) => PathBuf::from(default_checkpoint_dir()),
                }
            }
        };

        ctx.initialize(task_id, worker_rank, worker_count, dir);
        Box::into_raw(ctx)
    })) {
        Ok(ptr) => ptr,
        Err(_) => ptr::null_mut(),
    }
}

/// Clean up a Marabunta task context
///
/// Frees all resources associated with the context.
/// After calling this, the context pointer is invalid.
///
/// # Safety
/// - `ctx` must be a valid pointer returned by `marabunta_task_init` or `marabunta_task_init_ex`
/// - `ctx` must not be used after this call
#[no_mangle]
pub extern "C" fn marabunta_task_cleanup(ctx: *mut MarabuntaContext) {
    if ctx.is_null() {
        return;
    }

    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        let _ = Box::from_raw(ctx);
    }));
}

// =============================================================================
// Progress Reporting Functions
// =============================================================================

/// Set the progress percentage (0-100)
///
/// Values above 100 are clamped to 100.
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_progress_set(ctx: *mut MarabuntaContext, percent: u8) -> i32 {
    with_context(ctx, |c| {
        c.progress().set_percent(percent);
        MARABUNTA_OK
    })
}

/// Set the current stage name
///
/// Stage names help identify what part of the computation is running.
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if stage_name is null
/// - MARABUNTA_ERR_INVALID_UTF8 if stage_name is not valid UTF-8
#[no_mangle]
pub extern "C" fn marabunta_progress_set_stage(ctx: *mut MarabuntaContext, stage_name: *const c_char) -> i32 {
    with_context(ctx, |c| {
        let name = match unsafe_cstr_to_str(stage_name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        c.progress().set_stage(name);
        MARABUNTA_OK
    })
}

/// Set a progress message
///
/// Messages provide detailed information about current activity.
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if message is null
/// - MARABUNTA_ERR_INVALID_UTF8 if message is not valid UTF-8
#[no_mangle]
pub extern "C" fn marabunta_progress_set_message(ctx: *mut MarabuntaContext, message: *const c_char) -> i32 {
    with_context(ctx, |c| {
        let msg = match unsafe_cstr_to_str(message) {
            Ok(s) => s,
            Err(e) => return e,
        };
        c.progress().set_message(msg);
        MARABUNTA_OK
    })
}

/// Increment progress by a delta
///
/// The result is clamped to 100.
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_progress_increment(ctx: *mut MarabuntaContext, delta: u8) -> i32 {
    with_context(ctx, |c| {
        c.progress().increment(delta);
        MARABUNTA_OK
    })
}

/// Get the current progress percentage
///
/// # Returns
/// - Progress percentage (0-100) on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_progress_get(ctx: *mut MarabuntaContext) -> i32 {
    with_context(ctx, |c| c.progress().get_percent() as i32)
}

// =============================================================================
// Checkpointing Functions
// =============================================================================

/// Save checkpoint data
///
/// Saves the task state for fault tolerance. If the task crashes or is
/// migrated, it can restore from this checkpoint.
///
/// # Arguments
/// * `ctx` - Task context
/// * `data_ptr` - Pointer to data to save
/// * `data_len` - Length of data in bytes
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if data_ptr is null
/// - MARABUNTA_ERR_IO on I/O error
#[no_mangle]
pub extern "C" fn marabunta_checkpoint_save(
    ctx: *mut MarabuntaContext,
    data_ptr: *const u8,
    data_len: usize,
) -> i32 {
    with_context(ctx, |c| {
        if data_ptr.is_null() && data_len > 0 {
            return MARABUNTA_ERR_NULL_POINTER;
        }

        let data = if data_len == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(data_ptr, data_len) }
        };

        c.checkpoint_save(data)
    })
}

/// Load checkpoint data
///
/// Restores previously saved task state.
///
/// # Arguments
/// * `ctx` - Task context
/// * `data_ptr` - Buffer to load data into
/// * `max_len` - Maximum bytes to read (buffer size)
///
/// # Returns
/// - Actual number of bytes read on success (>= 0)
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if data_ptr is null
/// - MARABUNTA_ERR_CHECKPOINT_NOT_FOUND if no checkpoint exists
/// - MARABUNTA_ERR_BUFFER_TOO_SMALL if buffer is too small
/// - MARABUNTA_ERR_IO on I/O error
#[no_mangle]
pub extern "C" fn marabunta_checkpoint_load(
    ctx: *mut MarabuntaContext,
    data_ptr: *mut u8,
    max_len: usize,
) -> i64 {
    with_context_i64(ctx, |c| {
        if data_ptr.is_null() && max_len > 0 {
            return MARABUNTA_ERR_NULL_POINTER as i64;
        }

        let buffer = if max_len == 0 {
            &mut []
        } else {
            unsafe { slice::from_raw_parts_mut(data_ptr, max_len) }
        };

        c.checkpoint_load(buffer)
    })
}

/// Check if a checkpoint exists
///
/// # Returns
/// - 1 if checkpoint exists
/// - 0 if no checkpoint
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_checkpoint_exists(ctx: *mut MarabuntaContext) -> i32 {
    with_context(ctx, |c| if c.checkpoint_exists() { 1 } else { 0 })
}

// =============================================================================
// Yield Points (Cooperative Scheduling)
// =============================================================================

/// Yield control to the scheduler
///
/// Tasks should call this periodically to allow the system to:
/// - Check if the task should be paused
/// - Check if the task should be aborted
/// - Allow other tasks to run
///
/// # Returns
/// - MARABUNTA_YIELD_CONTINUE (0): Keep running
/// - MARABUNTA_YIELD_PAUSE (1): Save state and pause
/// - MARABUNTA_YIELD_ABORT (2): Stop immediately
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_yield(ctx: *mut MarabuntaContext) -> i32 {
    with_context(ctx, |c| c.yield_point())
}

/// Check if the task should pause
///
/// # Returns
/// - 1 if task should pause
/// - 0 if task should continue
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_should_pause(ctx: *mut MarabuntaContext) -> i32 {
    with_context(ctx, |c| if c.should_pause() { 1 } else { 0 })
}

/// Check if the task should abort
///
/// # Returns
/// - 1 if task should abort
/// - 0 if task should continue
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_should_abort(ctx: *mut MarabuntaContext) -> i32 {
    with_context(ctx, |c| if c.should_abort() { 1 } else { 0 })
}

// =============================================================================
// Resource Hints
// =============================================================================

/// Hint the amount of memory needed
///
/// Helps the scheduler place tasks on workers with sufficient memory.
///
/// # Arguments
/// * `bytes` - Estimated memory requirement in bytes
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_hint_memory_needed(ctx: *mut MarabuntaContext, bytes: u64) -> i32 {
    with_context(ctx, |c| {
        c.resources().set_memory_needed(bytes);
        MARABUNTA_OK
    })
}

/// Hint the estimated execution time
///
/// Helps the scheduler with task ordering and deadline management.
///
/// # Arguments
/// * `seconds` - Estimated time to complete in seconds
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_hint_time_estimate(ctx: *mut MarabuntaContext, seconds: u32) -> i32 {
    with_context(ctx, |c| {
        c.resources().set_time_estimate(seconds);
        MARABUNTA_OK
    })
}

/// Hint that the task can be split into parallel chunks
///
/// Allows the scheduler to distribute work across multiple workers.
///
/// # Arguments
/// * `min_chunks` - Minimum number of chunks (at least 1)
/// * `max_chunks` - Maximum number of chunks
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_hint_can_split(
    ctx: *mut MarabuntaContext,
    min_chunks: u32,
    max_chunks: u32,
) -> i32 {
    with_context(ctx, |c| {
        c.resources().set_can_split(true, min_chunks, max_chunks);
        MARABUNTA_OK
    })
}

// =============================================================================
// Communication Functions
// =============================================================================

/// Get the task ID
///
/// # Returns
/// - Task ID on success
/// - 0 if ctx is null (check with marabunta_get_last_error if needed)
#[no_mangle]
pub extern "C" fn marabunta_get_task_id(ctx: *mut MarabuntaContext) -> u64 {
    with_context_u64(ctx, |c| c.task_id())
}

/// Get the total number of workers
///
/// # Returns
/// - Worker count on success (>= 1)
/// - 0 if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_get_worker_count(ctx: *mut MarabuntaContext) -> u32 {
    with_context_u32(ctx, |c| c.worker_count())
}

/// Get this worker's rank
///
/// # Returns
/// - Worker rank (0 to count-1) on success
/// - 0 if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_get_worker_rank(ctx: *mut MarabuntaContext) -> u32 {
    with_context_u32(ctx, |c| c.worker_rank())
}

// =============================================================================
// Logging Functions
// =============================================================================

/// Log a message at the specified level
///
/// # Arguments
/// * `level` - Log level (MARABUNTA_LOG_DEBUG, MARABUNTA_LOG_INFO, MARABUNTA_LOG_WARN, MARABUNTA_LOG_ERROR)
/// * `message` - Message to log (null-terminated string)
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if message is null
/// - MARABUNTA_ERR_INVALID_UTF8 if message is not valid UTF-8
#[no_mangle]
pub extern "C" fn marabunta_log(ctx: *mut MarabuntaContext, level: i32, message: *const c_char) -> i32 {
    with_context(ctx, |c| {
        let msg = match unsafe_cstr_to_str(message) {
            Ok(s) => s,
            Err(e) => return e,
        };
        c.log(level, msg);
        MARABUNTA_OK
    })
}

/// Log a debug message
#[no_mangle]
pub extern "C" fn marabunta_log_debug(ctx: *mut MarabuntaContext, message: *const c_char) -> i32 {
    marabunta_log(ctx, MARABUNTA_LOG_DEBUG, message)
}

/// Log an info message
#[no_mangle]
pub extern "C" fn marabunta_log_info(ctx: *mut MarabuntaContext, message: *const c_char) -> i32 {
    marabunta_log(ctx, MARABUNTA_LOG_INFO, message)
}

/// Log a warning message
#[no_mangle]
pub extern "C" fn marabunta_log_warn(ctx: *mut MarabuntaContext, message: *const c_char) -> i32 {
    marabunta_log(ctx, MARABUNTA_LOG_WARN, message)
}

/// Log an error message
#[no_mangle]
pub extern "C" fn marabunta_log_error(ctx: *mut MarabuntaContext, message: *const c_char) -> i32 {
    marabunta_log(ctx, MARABUNTA_LOG_ERROR, message)
}

// =============================================================================
// Results Functions
// =============================================================================

/// Set the task result data (replaces any existing result)
///
/// # Arguments
/// * `data_ptr` - Pointer to result data
/// * `data_len` - Length of data in bytes
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if data_ptr is null (and data_len > 0)
/// - MARABUNTA_ERR_BUFFER_TOO_SMALL if data exceeds maximum size
#[no_mangle]
pub extern "C" fn marabunta_result_set(
    ctx: *mut MarabuntaContext,
    data_ptr: *const u8,
    data_len: usize,
) -> i32 {
    with_context(ctx, |c| {
        if data_ptr.is_null() && data_len > 0 {
            return MARABUNTA_ERR_NULL_POINTER;
        }

        let data = if data_len == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(data_ptr, data_len) }
        };

        c.result_set(data)
    })
}

/// Set the task result as a JSON string
///
/// # Arguments
/// * `json_string` - Null-terminated JSON string
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if json_string is null
/// - MARABUNTA_ERR_INVALID_UTF8 if json_string is not valid UTF-8
/// - MARABUNTA_ERR_BUFFER_TOO_SMALL if data exceeds maximum size
#[no_mangle]
pub extern "C" fn marabunta_result_set_json(ctx: *mut MarabuntaContext, json_string: *const c_char) -> i32 {
    with_context(ctx, |c| {
        let json = match unsafe_cstr_to_str(json_string) {
            Ok(s) => s,
            Err(e) => return e,
        };
        c.result_set(json.as_bytes())
    })
}

/// Append data to the task result (for streaming results)
///
/// # Arguments
/// * `data_ptr` - Pointer to data to append
/// * `data_len` - Length of data in bytes
///
/// # Returns
/// - MARABUNTA_OK on success
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if data_ptr is null (and data_len > 0)
/// - MARABUNTA_ERR_BUFFER_TOO_SMALL if total result would exceed maximum size
#[no_mangle]
pub extern "C" fn marabunta_result_append(
    ctx: *mut MarabuntaContext,
    data_ptr: *const u8,
    data_len: usize,
) -> i32 {
    with_context(ctx, |c| {
        if data_ptr.is_null() && data_len > 0 {
            return MARABUNTA_ERR_NULL_POINTER;
        }

        let data = if data_len == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(data_ptr, data_len) }
        };

        c.result_append(data)
    })
}

/// Get the current result size
///
/// # Returns
/// - Size in bytes on success (>= 0)
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
#[no_mangle]
pub extern "C" fn marabunta_result_size(ctx: *mut MarabuntaContext) -> i64 {
    with_context_i64(ctx, |c| c.result_size() as i64)
}

/// Get the result data
///
/// # Arguments
/// * `data_ptr` - Buffer to copy result into
/// * `max_len` - Maximum bytes to copy (buffer size)
///
/// # Returns
/// - Actual number of bytes copied on success (>= 0)
/// - MARABUNTA_ERR_NULL_CONTEXT if ctx is null
/// - MARABUNTA_ERR_NULL_POINTER if data_ptr is null (and max_len > 0)
/// - MARABUNTA_ERR_BUFFER_TOO_SMALL if buffer is too small
#[no_mangle]
pub extern "C" fn marabunta_result_get(ctx: *mut MarabuntaContext, data_ptr: *mut u8, max_len: usize) -> i64 {
    with_context_i64(ctx, |c| {
        if data_ptr.is_null() && max_len > 0 {
            return MARABUNTA_ERR_NULL_POINTER as i64;
        }

        let result = c.result_get();
        if result.len() > max_len {
            return MARABUNTA_ERR_BUFFER_TOO_SMALL as i64;
        }

        if !result.is_empty() {
            unsafe {
                ptr::copy_nonoverlapping(result.as_ptr(), data_ptr, result.len());
            }
        }

        result.len() as i64
    })
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Safely convert a C string to a Rust string slice
fn unsafe_cstr_to_str(s: *const c_char) -> Result<&'static str, i32> {
    if s.is_null() {
        return Err(MARABUNTA_ERR_NULL_POINTER);
    }

    unsafe {
        match CStr::from_ptr(s).to_str() {
            Ok(s) => Ok(s),
            Err(_) => Err(MARABUNTA_ERR_INVALID_UTF8),
        }
    }
}

/// Execute a function with a context, handling null checks and panics
fn with_context<F>(ctx: *mut MarabuntaContext, f: F) -> i32
where
    F: FnOnce(&MarabuntaContext) -> i32,
{
    if ctx.is_null() {
        return MARABUNTA_ERR_NULL_CONTEXT;
    }

    let ctx_ref = unsafe { &*ctx };
    match catch_unwind(AssertUnwindSafe(|| f(ctx_ref))) {
        Ok(result) => result,
        Err(_) => MARABUNTA_ERR_PANIC,
    }
}

/// Execute a function with a context, returning i64
fn with_context_i64<F>(ctx: *mut MarabuntaContext, f: F) -> i64
where
    F: FnOnce(&MarabuntaContext) -> i64,
{
    if ctx.is_null() {
        return MARABUNTA_ERR_NULL_CONTEXT as i64;
    }

    let ctx_ref = unsafe { &*ctx };
    match catch_unwind(AssertUnwindSafe(|| f(ctx_ref))) {
        Ok(result) => result,
        Err(_) => MARABUNTA_ERR_PANIC as i64,
    }
}

/// Execute a function with a context, returning u32
fn with_context_u32<F>(ctx: *mut MarabuntaContext, f: F) -> u32
where
    F: FnOnce(&MarabuntaContext) -> u32,
{
    if ctx.is_null() {
        return 0;
    }

    let ctx_ref = unsafe { &*ctx };
    match catch_unwind(AssertUnwindSafe(|| f(ctx_ref))) {
        Ok(result) => result,
        Err(_) => 0,
    }
}

/// Execute a function with a context, returning u64
fn with_context_u64<F>(ctx: *mut MarabuntaContext, f: F) -> u64
where
    F: FnOnce(&MarabuntaContext) -> u64,
{
    if ctx.is_null() {
        return 0;
    }

    let ctx_ref = unsafe { &*ctx };
    match catch_unwind(AssertUnwindSafe(|| f(ctx_ref))) {
        Ok(result) => result,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::ptr;
    use std::thread;
    use tempfile;

    #[test]
    fn test_init_and_cleanup() {
        let ctx = marabunta_task_init();
        assert!(!ctx.is_null());
        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_init_ex() {
        let dir = CString::new("/tmp/test_checkpoints").unwrap();
        let ctx = marabunta_task_init_ex(12345, 2, 8, dir.as_ptr());
        assert!(!ctx.is_null());

        assert_eq!(marabunta_get_task_id(ctx), 12345);
        assert_eq!(marabunta_get_worker_rank(ctx), 2);
        assert_eq!(marabunta_get_worker_count(ctx), 8);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_init_ex_null_dir() {
        let ctx = marabunta_task_init_ex(100, 0, 1, ptr::null());
        assert!(!ctx.is_null());
        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_cleanup_null() {
        // Should not crash
        marabunta_task_cleanup(ptr::null_mut());
    }

    #[test]
    fn test_null_context_returns_error() {
        assert_eq!(
            marabunta_progress_set(ptr::null_mut(), 50),
            MARABUNTA_ERR_NULL_CONTEXT
        );
        assert_eq!(marabunta_yield(ptr::null_mut()), MARABUNTA_ERR_NULL_CONTEXT);
        assert_eq!(
            marabunta_checkpoint_exists(ptr::null_mut()),
            MARABUNTA_ERR_NULL_CONTEXT
        );
    }

    #[test]
    fn test_progress() {
        let ctx = marabunta_task_init();

        assert_eq!(marabunta_progress_set(ctx, 50), MARABUNTA_OK);
        assert_eq!(marabunta_progress_get(ctx), 50);

        assert_eq!(marabunta_progress_increment(ctx, 25), MARABUNTA_OK);
        assert_eq!(marabunta_progress_get(ctx), 75);

        // Test clamping
        assert_eq!(marabunta_progress_set(ctx, 150), MARABUNTA_OK);
        assert_eq!(marabunta_progress_get(ctx), 100);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_progress_stage() {
        let ctx = marabunta_task_init();
        let stage = CString::new("Processing").unwrap();

        assert_eq!(marabunta_progress_set_stage(ctx, stage.as_ptr()), MARABUNTA_OK);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_progress_message() {
        let ctx = marabunta_task_init();
        let msg = CString::new("Doing important work").unwrap();

        assert_eq!(marabunta_progress_set_message(ctx, msg.as_ptr()), MARABUNTA_OK);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_progress_null_string() {
        let ctx = marabunta_task_init();

        assert_eq!(
            marabunta_progress_set_stage(ctx, ptr::null()),
            MARABUNTA_ERR_NULL_POINTER
        );
        assert_eq!(
            marabunta_progress_set_message(ctx, ptr::null()),
            MARABUNTA_ERR_NULL_POINTER
        );

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_checkpoint() {
        // Use a unique temp directory to avoid conflicts with other tests
        let temp_dir = tempfile::TempDir::new().unwrap();
        let dir = CString::new(temp_dir.path().to_str().unwrap()).unwrap();

        // Use a unique task ID based on time to avoid conflicts
        let task_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let ctx = marabunta_task_init_ex(task_id, 0, 1, dir.as_ptr());

        // No checkpoint initially
        assert_eq!(marabunta_checkpoint_exists(ctx), 0);

        // Save checkpoint
        let data = b"test checkpoint data";
        assert_eq!(
            marabunta_checkpoint_save(ctx, data.as_ptr(), data.len()),
            MARABUNTA_OK
        );
        assert_eq!(marabunta_checkpoint_exists(ctx), 1);

        // Load checkpoint
        let mut buffer = vec![0u8; 100];
        let len = marabunta_checkpoint_load(ctx, buffer.as_mut_ptr(), buffer.len());
        assert_eq!(len, data.len() as i64);
        assert_eq!(&buffer[..data.len()], data);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_checkpoint_null_data() {
        let ctx = marabunta_task_init();

        // Null with len > 0 should error
        assert_eq!(
            marabunta_checkpoint_save(ctx, ptr::null(), 10),
            MARABUNTA_ERR_NULL_POINTER
        );

        // Null with len = 0 should work (empty checkpoint)
        assert_eq!(marabunta_checkpoint_save(ctx, ptr::null(), 0), MARABUNTA_OK);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_yield() {
        let ctx = marabunta_task_init();

        // Initially should continue
        assert_eq!(marabunta_yield(ctx), MARABUNTA_YIELD_CONTINUE);
        assert_eq!(marabunta_should_pause(ctx), 0);
        assert_eq!(marabunta_should_abort(ctx), 0);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_resource_hints() {
        let ctx = marabunta_task_init();

        assert_eq!(marabunta_hint_memory_needed(ctx, 1024 * 1024), MARABUNTA_OK);
        assert_eq!(marabunta_hint_time_estimate(ctx, 60), MARABUNTA_OK);
        assert_eq!(marabunta_hint_can_split(ctx, 2, 8), MARABUNTA_OK);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_worker_info() {
        let dir = CString::new("/tmp/test").unwrap();
        let ctx = marabunta_task_init_ex(999, 3, 10, dir.as_ptr());

        assert_eq!(marabunta_get_task_id(ctx), 999);
        assert_eq!(marabunta_get_worker_rank(ctx), 3);
        assert_eq!(marabunta_get_worker_count(ctx), 10);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_logging() {
        let ctx = marabunta_task_init();
        let msg = CString::new("Test message").unwrap();

        assert_eq!(marabunta_log(ctx, MARABUNTA_LOG_INFO, msg.as_ptr()), MARABUNTA_OK);
        assert_eq!(marabunta_log_debug(ctx, msg.as_ptr()), MARABUNTA_OK);
        assert_eq!(marabunta_log_info(ctx, msg.as_ptr()), MARABUNTA_OK);
        assert_eq!(marabunta_log_warn(ctx, msg.as_ptr()), MARABUNTA_OK);
        assert_eq!(marabunta_log_error(ctx, msg.as_ptr()), MARABUNTA_OK);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_logging_null_message() {
        let ctx = marabunta_task_init();

        assert_eq!(
            marabunta_log(ctx, MARABUNTA_LOG_INFO, ptr::null()),
            MARABUNTA_ERR_NULL_POINTER
        );

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_result_set_and_get() {
        let ctx = marabunta_task_init();

        let data = b"result data";
        assert_eq!(marabunta_result_set(ctx, data.as_ptr(), data.len()), MARABUNTA_OK);

        assert_eq!(marabunta_result_size(ctx), data.len() as i64);

        let mut buffer = vec![0u8; 100];
        let len = marabunta_result_get(ctx, buffer.as_mut_ptr(), buffer.len());
        assert_eq!(len, data.len() as i64);
        assert_eq!(&buffer[..data.len()], data);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_result_set_json() {
        let ctx = marabunta_task_init();

        let json = CString::new(r#"{"key": "value"}"#).unwrap();
        assert_eq!(marabunta_result_set_json(ctx, json.as_ptr()), MARABUNTA_OK);

        let mut buffer = vec![0u8; 100];
        let len = marabunta_result_get(ctx, buffer.as_mut_ptr(), buffer.len());
        assert!(len > 0);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_result_append() {
        let ctx = marabunta_task_init();

        let data1 = b"Hello, ";
        let data2 = b"World!";

        assert_eq!(marabunta_result_set(ctx, data1.as_ptr(), data1.len()), MARABUNTA_OK);
        assert_eq!(
            marabunta_result_append(ctx, data2.as_ptr(), data2.len()),
            MARABUNTA_OK
        );

        assert_eq!(marabunta_result_size(ctx), 13);

        let mut buffer = vec![0u8; 100];
        let len = marabunta_result_get(ctx, buffer.as_mut_ptr(), buffer.len());
        assert_eq!(&buffer[..len as usize], b"Hello, World!");

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_result_buffer_too_small() {
        let ctx = marabunta_task_init();

        let data = b"result data that is longer than buffer";
        marabunta_result_set(ctx, data.as_ptr(), data.len());

        let mut small_buffer = vec![0u8; 5];
        let result = marabunta_result_get(ctx, small_buffer.as_mut_ptr(), small_buffer.len());
        assert_eq!(result, MARABUNTA_ERR_BUFFER_TOO_SMALL as i64);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_error_message() {
        unsafe {
            let msg = CStr::from_ptr(marabunta_error_message(MARABUNTA_OK));
            assert_eq!(msg.to_str().unwrap(), "Success");

            let msg = CStr::from_ptr(marabunta_error_message(MARABUNTA_ERR_NULL_CONTEXT));
            assert_eq!(msg.to_str().unwrap(), "Null context pointer");
        }
    }

    #[test]
    fn test_thread_safety() {
        let ctx = marabunta_task_init();
        let ctx_ptr = ctx as usize; // Convert to usize for thread safety

        let mut handles = vec![];

        for i in 0..5 {
            handles.push(thread::spawn(move || {
                let ctx = ctx_ptr as *mut MarabuntaContext;
                for _ in 0..100 {
                    marabunta_progress_increment(ctx, 1);
                    let msg = CString::new(format!("Thread {} working", i)).unwrap();
                    marabunta_log_debug(ctx, msg.as_ptr());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Progress should be at 100 (clamped)
        assert_eq!(marabunta_progress_get(ctx), 100);

        marabunta_task_cleanup(ctx);
    }

    #[test]
    fn test_multiple_contexts() {
        let ctx1 = marabunta_task_init();
        let ctx2 = marabunta_task_init();

        assert!(!ctx1.is_null());
        assert!(!ctx2.is_null());
        assert_ne!(ctx1, ctx2);

        marabunta_progress_set(ctx1, 25);
        marabunta_progress_set(ctx2, 75);

        assert_eq!(marabunta_progress_get(ctx1), 25);
        assert_eq!(marabunta_progress_get(ctx2), 75);

        marabunta_task_cleanup(ctx1);
        marabunta_task_cleanup(ctx2);
    }

    #[test]
    fn test_empty_data_operations() {
        let ctx = marabunta_task_init();

        // Empty checkpoint
        assert_eq!(marabunta_checkpoint_save(ctx, ptr::null(), 0), MARABUNTA_OK);

        // Empty result
        assert_eq!(marabunta_result_set(ctx, ptr::null(), 0), MARABUNTA_OK);
        assert_eq!(marabunta_result_size(ctx), 0);

        marabunta_task_cleanup(ctx);
    }
}
