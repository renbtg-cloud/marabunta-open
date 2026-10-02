// Marabunta - Licensed under the MIT License.
/**
 * @file marabunta_sdk.h
 * @brief Marabunta Compute Task SDK - C API for distributed job execution
 *
 * This SDK allows any language with C FFI support to write distributed jobs
 * for the Marabunta Compute framework. Supported languages include:
 *
 * - C/C++ (direct usage)
 * - Fortran (via ISO_C_BINDING)
 * - Python (via ctypes or cffi)
 * - MATLAB (via loadlibrary/MEX)
 * - Julia (via ccall)
 * - R (via .C() or Rcpp)
 *
 * @section Thread Safety
 * All functions in this SDK are thread-safe. Multiple threads within a task
 * can call these functions concurrently without external synchronization.
 *
 * @section Error Handling
 * All functions return error codes:
 * - MARABUNTA_OK (0): Success
 * - Negative values: Error codes
 * - Positive values: May have function-specific meanings (e.g., byte counts)
 *
 * @section Example
 * @code
 * #include "marabunta_sdk.h"
 * #include <stdio.h>
 *
 * int main() {
 *     MarabuntaContext* ctx = marabunta_task_init();
 *     if (!ctx) {
 *         fprintf(stderr, "Failed to initialize\n");
 *         return 1;
 *     }
 *
 *     // Check for existing checkpoint
 *     if (marabunta_checkpoint_exists(ctx)) {
 *         char buffer[4096];
 *         int64_t len = marabunta_checkpoint_load(ctx, buffer, sizeof(buffer));
 *         if (len > 0) {
 *             // Restore state from buffer...
 *         }
 *     }
 *
 *     // Main computation loop
 *     for (int i = 0; i <= 100; i++) {
 *         // Check for pause/abort signals
 *         int32_t yield_result = marabunta_yield(ctx);
 *         if (yield_result == MARABUNTA_YIELD_ABORT) {
 *             marabunta_log_warn(ctx, "Task aborted");
 *             break;
 *         }
 *         if (yield_result == MARABUNTA_YIELD_PAUSE) {
 *             // Save state before pausing
 *             const char* state = "my_state";
 *             marabunta_checkpoint_save(ctx, state, strlen(state));
 *             marabunta_log_info(ctx, "Task paused, checkpoint saved");
 *             break;
 *         }
 *
 *         // Update progress
 *         marabunta_progress_set(ctx, i);
 *
 *         // Do computation...
 *     }
 *
 *     // Set result
 *     marabunta_result_set_json(ctx, "{\"status\": \"complete\"}");
 *
 *     marabunta_task_cleanup(ctx);
 *     return 0;
 * }
 * @endcode
 *
 * @copyright Copyright (c) 2024 Marabunta Compute Project
 * @license MIT
 */

#ifndef MARABUNTA_SDK_H
#define MARABUNTA_SDK_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* =============================================================================
 * Types
 * ============================================================================= */

/**
 * @brief Opaque task context handle
 *
 * This structure is opaque to C code. Do not attempt to access its internals.
 * Always use the provided API functions to interact with it.
 */
typedef struct MarabuntaContext MarabuntaContext;

/* =============================================================================
 * Error Codes
 * ============================================================================= */

/** Success */
#define MARABUNTA_OK                      0

/** Null context pointer was passed */
#define MARABUNTA_ERR_NULL_CONTEXT       -1

/** Null data pointer was passed where non-null was required */
#define MARABUNTA_ERR_NULL_POINTER       -2

/** String data is not valid UTF-8 */
#define MARABUNTA_ERR_INVALID_UTF8       -3

/** I/O error during file operations */
#define MARABUNTA_ERR_IO                 -4

/** Checkpoint not found */
#define MARABUNTA_ERR_CHECKPOINT_NOT_FOUND -5

/** Buffer too small for the requested data */
#define MARABUNTA_ERR_BUFFER_TOO_SMALL   -6

/** Context already initialized */
#define MARABUNTA_ERR_ALREADY_INITIALIZED -7

/** Context not initialized */
#define MARABUNTA_ERR_NOT_INITIALIZED    -8

/** Task was aborted */
#define MARABUNTA_ERR_ABORTED            -9

/** Invalid argument value */
#define MARABUNTA_ERR_INVALID_ARGUMENT   -10

/** Resource allocation failed */
#define MARABUNTA_ERR_ALLOCATION_FAILED  -11

/** Operation timed out */
#define MARABUNTA_ERR_TIMEOUT            -12

/** Internal panic occurred (should never happen) */
#define MARABUNTA_ERR_PANIC              -99

/* =============================================================================
 * Yield Results
 * ============================================================================= */

/** Continue execution normally */
#define MARABUNTA_YIELD_CONTINUE         0

/** Pause execution, save checkpoint, will resume later */
#define MARABUNTA_YIELD_PAUSE            1

/** Abort execution, task was cancelled */
#define MARABUNTA_YIELD_ABORT            2

/* =============================================================================
 * Log Levels
 * ============================================================================= */

/** Debug level logging */
#define MARABUNTA_LOG_DEBUG              0

/** Info level logging */
#define MARABUNTA_LOG_INFO               1

/** Warning level logging */
#define MARABUNTA_LOG_WARN               2

/** Error level logging */
#define MARABUNTA_LOG_ERROR              3

/* =============================================================================
 * Lifecycle Functions
 * ============================================================================= */

/**
 * @brief Initialize a new Marabunta task context with default settings
 *
 * Creates a new task context with auto-generated task ID, worker rank 0,
 * and worker count 1. This is the simplest way to initialize a task.
 *
 * @return Pointer to new context, or NULL on failure
 *
 * @note The context must be freed with marabunta_task_cleanup()
 *
 * @code
 * MarabuntaContext* ctx = marabunta_task_init();
 * if (!ctx) {
 *     // Handle error
 * }
 * // Use context...
 * marabunta_task_cleanup(ctx);
 * @endcode
 */
MarabuntaContext* marabunta_task_init(void);

/**
 * @brief Initialize a Marabunta task context with specific parameters
 *
 * Creates a new task context with the specified parameters. Use this
 * when running as part of a distributed task.
 *
 * @param task_id Unique identifier for this task
 * @param worker_rank This worker's rank (0 to worker_count-1)
 * @param worker_count Total number of workers
 * @param checkpoint_dir Directory for checkpoint files (NULL for default)
 * @return Pointer to new context, or NULL on failure
 *
 * @note The context must be freed with marabunta_task_cleanup()
 */
MarabuntaContext* marabunta_task_init_ex(
    uint64_t task_id,
    uint32_t worker_rank,
    uint32_t worker_count,
    const char* checkpoint_dir
);

/**
 * @brief Clean up and free a Marabunta task context
 *
 * Releases all resources associated with the context. After calling this,
 * the context pointer is invalid and must not be used.
 *
 * @param ctx Context to clean up (NULL is safely ignored)
 */
void marabunta_task_cleanup(MarabuntaContext* ctx);

/* =============================================================================
 * Progress Reporting
 * ============================================================================= */

/**
 * @brief Set the progress percentage
 *
 * @param ctx Task context
 * @param percent Progress value (0-100, values > 100 are clamped)
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_progress_set(MarabuntaContext* ctx, uint8_t percent);

/**
 * @brief Set the current stage name
 *
 * Stage names help identify which part of the computation is running.
 *
 * @param ctx Task context
 * @param stage_name Null-terminated stage name string
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_progress_set_stage(MarabuntaContext* ctx, const char* stage_name);

/**
 * @brief Set a progress message
 *
 * Messages provide detailed information about current activity.
 *
 * @param ctx Task context
 * @param message Null-terminated message string
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_progress_set_message(MarabuntaContext* ctx, const char* message);

/**
 * @brief Increment progress by a delta
 *
 * @param ctx Task context
 * @param delta Amount to add (result clamped to 100)
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_progress_increment(MarabuntaContext* ctx, uint8_t delta);

/**
 * @brief Get the current progress percentage
 *
 * @param ctx Task context
 * @return Progress (0-100) on success, error code on failure
 */
int32_t marabunta_progress_get(MarabuntaContext* ctx);

/* =============================================================================
 * Checkpointing (Fault Tolerance)
 * ============================================================================= */

/**
 * @brief Save checkpoint data
 *
 * Saves the task state for fault tolerance. If the task crashes or is
 * migrated to another worker, it can restore from this checkpoint.
 *
 * @param ctx Task context
 * @param data_ptr Pointer to data to save
 * @param data_len Length of data in bytes
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_checkpoint_save(
    MarabuntaContext* ctx,
    const uint8_t* data_ptr,
    size_t data_len
);

/**
 * @brief Load checkpoint data
 *
 * Restores previously saved task state.
 *
 * @param ctx Task context
 * @param data_ptr Buffer to load data into
 * @param max_len Maximum bytes to read (buffer size)
 * @return Number of bytes read (>= 0) on success, error code on failure
 */
int64_t marabunta_checkpoint_load(
    MarabuntaContext* ctx,
    uint8_t* data_ptr,
    size_t max_len
);

/**
 * @brief Check if a checkpoint exists
 *
 * @param ctx Task context
 * @return 1 if checkpoint exists, 0 if not, error code on failure
 */
int32_t marabunta_checkpoint_exists(MarabuntaContext* ctx);

/* =============================================================================
 * Yield Points (Cooperative Scheduling)
 * ============================================================================= */

/**
 * @brief Yield control to the scheduler
 *
 * Tasks should call this periodically to allow the system to:
 * - Check if the task should be paused
 * - Check if the task should be aborted
 * - Allow other tasks to run
 *
 * Call this at natural breakpoints in your computation, typically:
 * - At the start of each iteration of a main loop
 * - After completing a significant unit of work
 * - Every few seconds of computation
 *
 * @param ctx Task context
 * @return MARABUNTA_YIELD_CONTINUE, MARABUNTA_YIELD_PAUSE, or MARABUNTA_YIELD_ABORT
 *
 * @code
 * for (int i = 0; i < iterations; i++) {
 *     int result = marabunta_yield(ctx);
 *     if (result == MARABUNTA_YIELD_ABORT) {
 *         break;  // Stop immediately
 *     }
 *     if (result == MARABUNTA_YIELD_PAUSE) {
 *         save_checkpoint(ctx);
 *         break;  // Will resume later
 *     }
 *     // Continue computation...
 * }
 * @endcode
 */
int32_t marabunta_yield(MarabuntaContext* ctx);

/**
 * @brief Check if the task should pause
 *
 * @param ctx Task context
 * @return 1 if should pause, 0 otherwise, error code on failure
 */
int32_t marabunta_should_pause(MarabuntaContext* ctx);

/**
 * @brief Check if the task should abort
 *
 * @param ctx Task context
 * @return 1 if should abort, 0 otherwise, error code on failure
 */
int32_t marabunta_should_abort(MarabuntaContext* ctx);

/* =============================================================================
 * Resource Hints
 * ============================================================================= */

/**
 * @brief Hint the amount of memory needed
 *
 * Helps the scheduler place tasks on workers with sufficient memory.
 *
 * @param ctx Task context
 * @param bytes Estimated memory requirement in bytes
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_hint_memory_needed(MarabuntaContext* ctx, uint64_t bytes);

/**
 * @brief Hint the estimated execution time
 *
 * Helps the scheduler with task ordering and deadline management.
 *
 * @param ctx Task context
 * @param seconds Estimated time to complete
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_hint_time_estimate(MarabuntaContext* ctx, uint32_t seconds);

/**
 * @brief Hint that the task can be split into parallel chunks
 *
 * Tells the scheduler this task can be parallelized across workers.
 *
 * @param ctx Task context
 * @param min_chunks Minimum number of chunks (>= 1)
 * @param max_chunks Maximum number of chunks
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_hint_can_split(
    MarabuntaContext* ctx,
    uint32_t min_chunks,
    uint32_t max_chunks
);

/* =============================================================================
 * Communication
 * ============================================================================= */

/**
 * @brief Get the task ID
 *
 * @param ctx Task context
 * @return Task ID, or 0 if ctx is NULL
 */
uint64_t marabunta_get_task_id(MarabuntaContext* ctx);

/**
 * @brief Get the total number of workers
 *
 * @param ctx Task context
 * @return Worker count (>= 1), or 0 if ctx is NULL
 */
uint32_t marabunta_get_worker_count(MarabuntaContext* ctx);

/**
 * @brief Get this worker's rank
 *
 * The rank is a number from 0 to (worker_count - 1) that uniquely
 * identifies this worker within the task.
 *
 * @param ctx Task context
 * @return Worker rank, or 0 if ctx is NULL
 */
uint32_t marabunta_get_worker_rank(MarabuntaContext* ctx);

/* =============================================================================
 * Logging
 * ============================================================================= */

/**
 * @brief Log a message at the specified level
 *
 * @param ctx Task context
 * @param level Log level (MARABUNTA_LOG_DEBUG, INFO, WARN, ERROR)
 * @param message Null-terminated message string
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_log(MarabuntaContext* ctx, int32_t level, const char* message);

/**
 * @brief Log a debug message
 *
 * @param ctx Task context
 * @param message Null-terminated message string
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_log_debug(MarabuntaContext* ctx, const char* message);

/**
 * @brief Log an info message
 *
 * @param ctx Task context
 * @param message Null-terminated message string
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_log_info(MarabuntaContext* ctx, const char* message);

/**
 * @brief Log a warning message
 *
 * @param ctx Task context
 * @param message Null-terminated message string
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_log_warn(MarabuntaContext* ctx, const char* message);

/**
 * @brief Log an error message
 *
 * @param ctx Task context
 * @param message Null-terminated message string
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_log_error(MarabuntaContext* ctx, const char* message);

/* =============================================================================
 * Results
 * ============================================================================= */

/**
 * @brief Set the task result data
 *
 * Replaces any existing result with the new data.
 *
 * @param ctx Task context
 * @param data_ptr Pointer to result data
 * @param data_len Length of data in bytes
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_result_set(
    MarabuntaContext* ctx,
    const uint8_t* data_ptr,
    size_t data_len
);

/**
 * @brief Set the task result as a JSON string
 *
 * @param ctx Task context
 * @param json_string Null-terminated JSON string
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_result_set_json(MarabuntaContext* ctx, const char* json_string);

/**
 * @brief Append data to the task result
 *
 * For streaming results, call this to append data incrementally.
 *
 * @param ctx Task context
 * @param data_ptr Pointer to data to append
 * @param data_len Length of data in bytes
 * @return MARABUNTA_OK on success, error code on failure
 */
int32_t marabunta_result_append(
    MarabuntaContext* ctx,
    const uint8_t* data_ptr,
    size_t data_len
);

/**
 * @brief Get the current result size
 *
 * @param ctx Task context
 * @return Size in bytes (>= 0), or error code on failure
 */
int64_t marabunta_result_size(MarabuntaContext* ctx);

/**
 * @brief Get the result data
 *
 * @param ctx Task context
 * @param data_ptr Buffer to copy result into
 * @param max_len Maximum bytes to copy (buffer size)
 * @return Number of bytes copied (>= 0), or error code on failure
 */
int64_t marabunta_result_get(
    MarabuntaContext* ctx,
    uint8_t* data_ptr,
    size_t max_len
);

/* =============================================================================
 * Utility
 * ============================================================================= */

/**
 * @brief Get a human-readable error message
 *
 * Returns a static string describing the error code. The returned pointer
 * is valid for the lifetime of the program and should not be freed.
 *
 * @param code Error code
 * @return Error message string
 */
const char* marabunta_error_message(int32_t code);

#ifdef __cplusplus
}
#endif

#endif /* MARABUNTA_SDK_H */
