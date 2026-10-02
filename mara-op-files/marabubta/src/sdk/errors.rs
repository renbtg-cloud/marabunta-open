// Marabunta - Licensed under the MIT License.
//! Error codes for the Marabunta Task SDK C ABI
//!
//! All error codes are negative integers. Zero (MARABUNTA_OK) indicates success.
//! Positive return values may have function-specific meanings (e.g., byte counts).

/// Success - operation completed successfully
pub const MARABUNTA_OK: i32 = 0;

/// Null context pointer was passed
pub const MARABUNTA_ERR_NULL_CONTEXT: i32 = -1;

/// Null data pointer was passed where non-null was required
pub const MARABUNTA_ERR_NULL_POINTER: i32 = -2;

/// String data is not valid UTF-8
pub const MARABUNTA_ERR_INVALID_UTF8: i32 = -3;

/// I/O error during file operations
pub const MARABUNTA_ERR_IO: i32 = -4;

/// Checkpoint not found
pub const MARABUNTA_ERR_CHECKPOINT_NOT_FOUND: i32 = -5;

/// Buffer too small for the requested data
pub const MARABUNTA_ERR_BUFFER_TOO_SMALL: i32 = -6;

/// Context already initialized
pub const MARABUNTA_ERR_ALREADY_INITIALIZED: i32 = -7;

/// Context not initialized
pub const MARABUNTA_ERR_NOT_INITIALIZED: i32 = -8;

/// Task was aborted
pub const MARABUNTA_ERR_ABORTED: i32 = -9;

/// Invalid argument value
pub const MARABUNTA_ERR_INVALID_ARGUMENT: i32 = -10;

/// Resource allocation failed
pub const MARABUNTA_ERR_ALLOCATION_FAILED: i32 = -11;

/// Operation timed out
pub const MARABUNTA_ERR_TIMEOUT: i32 = -12;

/// Internal panic occurred (should never happen)
pub const MARABUNTA_ERR_PANIC: i32 = -99;

/// Yield result: continue execution
pub const MARABUNTA_YIELD_CONTINUE: i32 = 0;

/// Yield result: pause and save state
pub const MARABUNTA_YIELD_PAUSE: i32 = 1;

/// Yield result: abort the task
pub const MARABUNTA_YIELD_ABORT: i32 = 2;

/// Log level: debug
pub const MARABUNTA_LOG_DEBUG: i32 = 0;

/// Log level: info
pub const MARABUNTA_LOG_INFO: i32 = 1;

/// Log level: warning
pub const MARABUNTA_LOG_WARN: i32 = 2;

/// Log level: error
pub const MARABUNTA_LOG_ERROR: i32 = 3;

/// Get a human-readable error message for an error code
#[no_mangle]
pub extern "C" fn marabunta_error_message(code: i32) -> *const std::ffi::c_char {
    let msg = match code {
        MARABUNTA_OK => "Success\0",
        MARABUNTA_ERR_NULL_CONTEXT => "Null context pointer\0",
        MARABUNTA_ERR_NULL_POINTER => "Null pointer\0",
        MARABUNTA_ERR_INVALID_UTF8 => "Invalid UTF-8 string\0",
        MARABUNTA_ERR_IO => "I/O error\0",
        MARABUNTA_ERR_CHECKPOINT_NOT_FOUND => "Checkpoint not found\0",
        MARABUNTA_ERR_BUFFER_TOO_SMALL => "Buffer too small\0",
        MARABUNTA_ERR_ALREADY_INITIALIZED => "Already initialized\0",
        MARABUNTA_ERR_NOT_INITIALIZED => "Not initialized\0",
        MARABUNTA_ERR_ABORTED => "Task aborted\0",
        MARABUNTA_ERR_INVALID_ARGUMENT => "Invalid argument\0",
        MARABUNTA_ERR_ALLOCATION_FAILED => "Allocation failed\0",
        MARABUNTA_ERR_TIMEOUT => "Operation timed out\0",
        MARABUNTA_ERR_PANIC => "Internal panic\0",
        _ => "Unknown error\0",
    };
    msg.as_ptr() as *const std::ffi::c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn test_error_codes_are_negative() {
        assert!(MARABUNTA_ERR_NULL_CONTEXT < 0);
        assert!(MARABUNTA_ERR_NULL_POINTER < 0);
        assert!(MARABUNTA_ERR_INVALID_UTF8 < 0);
        assert!(MARABUNTA_ERR_IO < 0);
        assert!(MARABUNTA_ERR_CHECKPOINT_NOT_FOUND < 0);
        assert!(MARABUNTA_ERR_BUFFER_TOO_SMALL < 0);
        assert!(MARABUNTA_ERR_ALREADY_INITIALIZED < 0);
        assert!(MARABUNTA_ERR_NOT_INITIALIZED < 0);
        assert!(MARABUNTA_ERR_ABORTED < 0);
        assert!(MARABUNTA_ERR_INVALID_ARGUMENT < 0);
        assert!(MARABUNTA_ERR_ALLOCATION_FAILED < 0);
        assert!(MARABUNTA_ERR_TIMEOUT < 0);
        assert!(MARABUNTA_ERR_PANIC < 0);
    }

    #[test]
    fn test_success_is_zero() {
        assert_eq!(MARABUNTA_OK, 0);
    }

    #[test]
    fn test_yield_values() {
        assert_eq!(MARABUNTA_YIELD_CONTINUE, 0);
        assert_eq!(MARABUNTA_YIELD_PAUSE, 1);
        assert_eq!(MARABUNTA_YIELD_ABORT, 2);
    }

    #[test]
    fn test_error_message() {
        unsafe {
            let msg = CStr::from_ptr(marabunta_error_message(MARABUNTA_OK));
            assert_eq!(msg.to_str().unwrap(), "Success");

            let msg = CStr::from_ptr(marabunta_error_message(MARABUNTA_ERR_NULL_CONTEXT));
            assert_eq!(msg.to_str().unwrap(), "Null context pointer");

            let msg = CStr::from_ptr(marabunta_error_message(-9999));
            assert_eq!(msg.to_str().unwrap(), "Unknown error");
        }
    }

    #[test]
    fn test_all_error_codes_unique() {
        let codes = vec![
            MARABUNTA_OK,
            MARABUNTA_ERR_NULL_CONTEXT,
            MARABUNTA_ERR_NULL_POINTER,
            MARABUNTA_ERR_INVALID_UTF8,
            MARABUNTA_ERR_IO,
            MARABUNTA_ERR_CHECKPOINT_NOT_FOUND,
            MARABUNTA_ERR_BUFFER_TOO_SMALL,
            MARABUNTA_ERR_ALREADY_INITIALIZED,
            MARABUNTA_ERR_NOT_INITIALIZED,
            MARABUNTA_ERR_ABORTED,
            MARABUNTA_ERR_INVALID_ARGUMENT,
            MARABUNTA_ERR_ALLOCATION_FAILED,
            MARABUNTA_ERR_TIMEOUT,
            MARABUNTA_ERR_PANIC,
        ];
        let mut unique_codes = codes.clone();
        unique_codes.sort();
        unique_codes.dedup();
        assert_eq!(
            codes.len(),
            unique_codes.len(),
            "Error codes must be unique"
        );
    }
}
