// Marabunta - Licensed under the MIT License.
//! Marabunta Task SDK - Comprehensive SDK for distributed job execution
//!
//! This SDK provides multiple interfaces for working with the Marabunta Compute framework:
//!
//! ## C ABI Interface (FFI)
//!
//! A C-compatible interface that allows ANY language with C FFI support to write
//! distributed jobs for the Marabunta Compute framework.
//!
//! ### Supported Languages
//!
//! - **C/C++**: Direct API usage
//! - **Fortran**: Via `ISO_C_BINDING` module
//! - **Python**: Via `ctypes` or `cffi`
//! - **MATLAB**: Via `loadlibrary` / MEX
//! - **Julia**: Via `ccall`
//! - **R**: Via `.C()` or `Rcpp`
//! - **Rust**: Native API (this crate)
//! - Any language with C FFI support
//!
//! ## Async Client Interface
//!
//! A high-level async Rust client for job submission with:
//!
//! - Async/await support with proper cancellation
//! - Configurable retry logic with exponential backoff
//! - Request timeout configuration
//! - Job builder pattern for fluent API
//! - Batch job submission
//! - Job templates for common patterns (MapReduce, scatter-gather, etc.)
//! - Type-safe result deserialization
//!
//! # Quick Start (Async Client)
//!
//! ```rust,no_run
//! use marabunta_compute::sdk::client::MarabuntaClient;
//! use marabunta_compute::sdk::builder::JobBuilder;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Arc::new(MarabuntaClient::new("http://localhost:8080")?);
//!
//!     let job = JobBuilder::new("data-processing")
//!         .with_input(serde_json::json!({"file": "data.csv"}))
//!         .with_timeout(std::time::Duration::from_secs(3600))
//!         .with_retries(3)
//!         .build()?;
//!
//!     let result: serde_json::Value = client.submit_and_wait(job).await?;
//!     println!("Result: {:?}", result);
//!     Ok(())
//! }
//! ```
//!
//! # Quick Start (C ABI)
//!
//! ```c
//! #include "marabunta_sdk.h"
//!
//! int main() {
//!     // Initialize
//!     MarabuntaContext* ctx = marabunta_task_init();
//!     if (!ctx) return 1;
//!
//!     // Main computation loop
//!     for (int i = 0; i < 100; i++) {
//!         int yield_result = marabunta_yield(ctx);
//!         if (yield_result == MARABUNTA_YIELD_ABORT) break;
//!         marabunta_progress_set(ctx, i);
//!     }
//!
//!     marabunta_result_set_json(ctx, "{\"status\": \"done\"}");
//!     marabunta_task_cleanup(ctx);
//!     return 0;
//! }
//! ```
//!
//! # Thread Safety
//!
//! All functions in this SDK are thread-safe. Multiple threads within a task
//! can call these functions concurrently without external synchronization.
//!
//! # Error Handling
//!
//! All FFI functions return error codes:
//! - `MARABUNTA_OK` (0): Success
//! - Negative values: Error codes (see [`errors`] module)
//! - Positive values: May have function-specific meanings
//!
//! Async client functions return `Result<T, ClientError>`.
//!
//! # Modules
//!
//! ## Core Modules (C ABI)
//! - [`errors`]: Error codes and yield result constants
//! - [`context`]: Task execution context
//! - [`progress`]: Progress reporting
//! - [`checkpoint`]: State checkpointing
//! - [`resources`]: Resource hints
//! - [`ffi`]: C ABI exported functions
//!
//! ## Client Modules (Async)
//! - [`client`]: Async client for job submission
//! - [`builder`]: Job builder pattern
//! - [`batch`]: Batch job submission
//! - [`templates`]: Common job patterns
//! - [`retry`]: Retry logic with exponential backoff
//! - [`timeout`]: Request timeout configuration
//! - [`result`]: Type-safe result deserialization

// Core C ABI modules
pub mod checkpoint;
pub mod context;
pub mod errors;
pub mod ffi;
pub mod progress;
pub mod resources;

// Async client modules
pub mod batch;
pub mod builder;
pub mod client;
pub mod result;
pub mod retry;
pub mod templates;
pub mod timeout;

// Re-export error codes for convenient access
pub use errors::*;

// Re-export context types for Rust users
pub use checkpoint::{CheckpointStorage, MAX_CHECKPOINT_SIZE};
pub use context::{MarabuntaContext, MarabuntaContextRef, LogEntry, MAX_RESULT_SIZE};
pub use progress::{ProgressSnapshot, ProgressState, MAX_MESSAGE_LEN, MAX_STAGE_NAME_LEN};
pub use resources::{ResourceHints, ResourceHintsSnapshot};

// Re-export FFI functions for Rust users who want the C-style API
pub use ffi::*;

// Re-export async client types for convenient access
pub use batch::{BatchOptions, BatchProgress, BatchSubmitter, BatchJobResult};
pub use builder::{JobBuilder, BuilderError, ResourceRequirements};
pub use client::{
    CancellationToken, ClientError, ClientResult, MarabuntaClient, MarabuntaClientConfig,
    JobHandle, JobRequest, JobResponse, JobStatus,
};
pub use result::{MultiTypeResult, ResultError, ResultExt, TypedResult, ValidatedResult};
pub use retry::{RetryConfig, RetryError, RetryState, RetryStrategy, with_retry};
pub use templates::{
    BatchProcessingTemplate, FanOutFanInTemplate, MapReduceTemplate,
    PipelineStage, PipelineTemplate, ScatterGatherTemplate,
};
pub use timeout::{TimeoutConfig, TimeoutError, TimeoutGuard};

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::ptr;

    /// Integration test: simulate a typical task lifecycle
    #[test]
    fn test_task_lifecycle() {
        // Initialize
        let ctx = marabunta_task_init();
        assert!(!ctx.is_null());

        // Set resource hints
        assert_eq!(marabunta_hint_memory_needed(ctx, 512 * 1024 * 1024), MARABUNTA_OK);
        assert_eq!(marabunta_hint_time_estimate(ctx, 300), MARABUNTA_OK);

        // Simulate computation with progress
        for i in 0..=10 {
            // Check yield point
            let yield_result = marabunta_yield(ctx);
            assert_eq!(yield_result, MARABUNTA_YIELD_CONTINUE);

            // Update progress
            assert_eq!(marabunta_progress_set(ctx, (i * 10) as u8), MARABUNTA_OK);

            let stage = CString::new(format!("Stage {}", i)).unwrap();
            assert_eq!(marabunta_progress_set_stage(ctx, stage.as_ptr()), MARABUNTA_OK);

            // Checkpoint periodically
            if i % 3 == 0 {
                let checkpoint_data = format!("checkpoint_state_{}", i);
                assert_eq!(
                    marabunta_checkpoint_save(ctx, checkpoint_data.as_ptr(), checkpoint_data.len()),
                    MARABUNTA_OK
                );
            }
        }

        // Verify progress
        assert_eq!(marabunta_progress_get(ctx), 100);

        // Set result
        let result = CString::new(r#"{"status": "success", "value": 42}"#).unwrap();
        assert_eq!(marabunta_result_set_json(ctx, result.as_ptr()), MARABUNTA_OK);

        // Cleanup
        marabunta_task_cleanup(ctx);
    }

    /// Test checkpoint recovery scenario
    #[test]
    fn test_checkpoint_recovery() {
        // First "run" - save checkpoint
        let ctx1 = marabunta_task_init();

        let state = b"saved_state_data_123";
        assert_eq!(
            marabunta_checkpoint_save(ctx1, state.as_ptr(), state.len()),
            MARABUNTA_OK
        );

        // Get checkpoint path (same task ID)
        let task_id = marabunta_get_task_id(ctx1);

        marabunta_task_cleanup(ctx1);

        // Second "run" - restore checkpoint
        let dir = CString::new("/tmp/marabunta_checkpoints").unwrap();
        let ctx2 = marabunta_task_init_ex(task_id, 0, 1, dir.as_ptr());

        if marabunta_checkpoint_exists(ctx2) == 1 {
            let mut buffer = vec![0u8; 1024];
            let len = marabunta_checkpoint_load(ctx2, buffer.as_mut_ptr(), buffer.len());
            assert_eq!(len, state.len() as i64);
            assert_eq!(&buffer[..len as usize], state);
        }

        marabunta_task_cleanup(ctx2);
    }

    /// Test parallel worker scenario
    #[test]
    fn test_parallel_workers() {
        let dir = CString::new("/tmp/marabunta_test").unwrap();

        // Simulate 4 workers
        let mut contexts = Vec::new();
        for rank in 0..4 {
            let ctx = marabunta_task_init_ex(1000, rank, 4, dir.as_ptr());
            assert!(!ctx.is_null());
            contexts.push(ctx);
        }

        // Verify worker info
        for (rank, ctx) in contexts.iter().enumerate() {
            assert_eq!(marabunta_get_task_id(*ctx), 1000);
            assert_eq!(marabunta_get_worker_rank(*ctx), rank as u32);
            assert_eq!(marabunta_get_worker_count(*ctx), 4);
        }

        // Cleanup
        for ctx in contexts {
            marabunta_task_cleanup(ctx);
        }
    }

    /// Test streaming results
    #[test]
    fn test_streaming_results() {
        let ctx = marabunta_task_init();

        // Append multiple chunks
        for i in 0..10 {
            let chunk = format!("chunk_{},", i);
            assert_eq!(
                marabunta_result_append(ctx, chunk.as_ptr(), chunk.len()),
                MARABUNTA_OK
            );
        }

        // Verify total size
        let expected_size: i64 = (0..10)
            .map(|i| format!("chunk_{},", i).len())
            .sum::<usize>() as i64;
        assert_eq!(marabunta_result_size(ctx), expected_size);

        marabunta_task_cleanup(ctx);
    }
}
