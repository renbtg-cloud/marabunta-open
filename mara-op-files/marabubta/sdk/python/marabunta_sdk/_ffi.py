# Marabunta - Licensed under the MIT License.
"""
Low-level FFI bindings to libmarabunta_sdk.so using ctypes.

This module handles:
- Library discovery and loading
- C function signature definitions
- Error code constants
- Thread-safe library access

Internal module - use the high-level API in marabunta_sdk instead.
"""

import ctypes
import ctypes.util
import os
import sys
import threading
from typing import Optional

# =============================================================================
# Error Codes (from src/sdk/errors.rs)
# =============================================================================

MARABUNTA_OK = 0

# Error codes (all negative)
MARABUNTA_ERR_NULL_CONTEXT = -1
MARABUNTA_ERR_NULL_POINTER = -2
MARABUNTA_ERR_INVALID_UTF8 = -3
MARABUNTA_ERR_IO = -4
MARABUNTA_ERR_CHECKPOINT_NOT_FOUND = -5
MARABUNTA_ERR_BUFFER_TOO_SMALL = -6
MARABUNTA_ERR_ALREADY_INITIALIZED = -7
MARABUNTA_ERR_NOT_INITIALIZED = -8
MARABUNTA_ERR_ABORTED = -9
MARABUNTA_ERR_INVALID_ARGUMENT = -10
MARABUNTA_ERR_ALLOCATION_FAILED = -11
MARABUNTA_ERR_TIMEOUT = -12
MARABUNTA_ERR_PANIC = -99

# Yield result codes
MARABUNTA_YIELD_CONTINUE = 0
MARABUNTA_YIELD_PAUSE = 1
MARABUNTA_YIELD_ABORT = 2

# Log levels
MARABUNTA_LOG_DEBUG = 0
MARABUNTA_LOG_INFO = 1
MARABUNTA_LOG_WARN = 2
MARABUNTA_LOG_ERROR = 3

# Error code to message mapping
ERROR_MESSAGES = {
    MARABUNTA_OK: "Success",
    MARABUNTA_ERR_NULL_CONTEXT: "Null context pointer",
    MARABUNTA_ERR_NULL_POINTER: "Null pointer",
    MARABUNTA_ERR_INVALID_UTF8: "Invalid UTF-8 string",
    MARABUNTA_ERR_IO: "I/O error",
    MARABUNTA_ERR_CHECKPOINT_NOT_FOUND: "Checkpoint not found",
    MARABUNTA_ERR_BUFFER_TOO_SMALL: "Buffer too small",
    MARABUNTA_ERR_ALREADY_INITIALIZED: "Already initialized",
    MARABUNTA_ERR_NOT_INITIALIZED: "Not initialized",
    MARABUNTA_ERR_ABORTED: "Task aborted",
    MARABUNTA_ERR_INVALID_ARGUMENT: "Invalid argument",
    MARABUNTA_ERR_ALLOCATION_FAILED: "Allocation failed",
    MARABUNTA_ERR_TIMEOUT: "Operation timed out",
    MARABUNTA_ERR_PANIC: "Internal panic",
}


def get_error_message(code):
    # type: (int) -> str
    """Get human-readable error message for an error code."""
    return ERROR_MESSAGES.get(code, "Unknown error ({})".format(code))


# =============================================================================
# Library Loading
# =============================================================================

_lib = None  # type: Optional[ctypes.CDLL]
_lib_lock = threading.Lock()
_mock_mode = False


class MarabuntaSDKError(Exception):
    """Exception raised for Marabunta SDK errors."""

    def __init__(self, code, message=None):
        # type: (int, Optional[str]) -> None
        self.code = code
        self.message = message or get_error_message(code)
        super(MarabuntaSDKError, self).__init__(self.message)


class LibraryNotFoundError(MarabuntaSDKError):
    """Raised when libmarabunta_sdk.so cannot be found."""

    def __init__(self, search_paths):
        # type: (list) -> None
        self.search_paths = search_paths
        message = (
            "Could not find libmarabunta_sdk.so. Searched:\n"
            "  {}\n\n"
            "To fix this:\n"
            "  1. Set LD_LIBRARY_PATH to include the directory containing libmarabunta_sdk.so\n"
            "  2. Or install libmarabunta_sdk.so to a standard location (/usr/lib, /usr/local/lib)\n"
            "  3. Or set MARABUNTA_SDK_LIB_PATH to the full path of the library"
        ).format("\n  ".join(search_paths))
        super(LibraryNotFoundError, self).__init__(-1000, message)


def _find_library():
    # type: () -> Optional[str]
    """
    Find libmarabunta_sdk.so by searching multiple locations.

    Search order:
    1. MARABUNTA_SDK_LIB_PATH environment variable (exact path)
    2. LD_LIBRARY_PATH directories
    3. Standard system paths
    4. Relative to this module (for bundled installs)
    5. Cargo target directory (for development)
    """
    search_paths = []

    # 1. Explicit environment variable
    explicit_path = os.environ.get("MARABUNTA_SDK_LIB_PATH")
    if explicit_path:
        search_paths.append(explicit_path)
        if os.path.isfile(explicit_path):
            return explicit_path

    # Library filename varies by platform
    if sys.platform == "darwin":
        lib_names = ["libmarabunta_sdk.dylib", "libmarabunta_sdk.so"]
    elif sys.platform == "win32":
        lib_names = ["marabunta_sdk.dll", "libmarabunta_sdk.dll"]
    else:
        lib_names = ["libmarabunta_sdk.so"]

    # 2. LD_LIBRARY_PATH
    ld_library_path = os.environ.get("LD_LIBRARY_PATH", "")
    if ld_library_path:
        for directory in ld_library_path.split(os.pathsep):
            for lib_name in lib_names:
                path = os.path.join(directory, lib_name)
                search_paths.append(path)
                if os.path.isfile(path):
                    return path

    # 3. Standard system paths
    standard_paths = [
        "/usr/lib",
        "/usr/local/lib",
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib/aarch64-linux-gnu",
        "/opt/marabunta/lib",
    ]
    for directory in standard_paths:
        for lib_name in lib_names:
            path = os.path.join(directory, lib_name)
            search_paths.append(path)
            if os.path.isfile(path):
                return path

    # 4. Relative to this module (bundled)
    module_dir = os.path.dirname(os.path.abspath(__file__))
    for rel_path in [".", "..", "lib", "../lib"]:
        for lib_name in lib_names:
            path = os.path.normpath(os.path.join(module_dir, rel_path, lib_name))
            search_paths.append(path)
            if os.path.isfile(path):
                return path

    # 5. Cargo target directory (development)
    # Walk up to find Cargo.toml, then look in target/
    current = module_dir
    for _ in range(10):
        cargo_toml = os.path.join(current, "Cargo.toml")
        if os.path.isfile(cargo_toml):
            for build_type in ["release", "debug"]:
                for lib_name in lib_names:
                    path = os.path.join(current, "target", build_type, lib_name)
                    search_paths.append(path)
                    if os.path.isfile(path):
                        return path
            break
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent

    # 6. Use ctypes.util.find_library as last resort
    found = ctypes.util.find_library("marabunta_sdk")
    if found:
        return found

    return None


def _setup_function_signatures(lib):
    # type: (ctypes.CDLL) -> None
    """Define C function signatures for type safety and automatic conversion."""

    # Context management
    # marabunta_context_t* marabunta_context_create(void)
    lib.marabunta_context_create.argtypes = []
    lib.marabunta_context_create.restype = ctypes.c_void_p

    # void marabunta_context_destroy(marabunta_context_t* ctx)
    lib.marabunta_context_destroy.argtypes = [ctypes.c_void_p]
    lib.marabunta_context_destroy.restype = None

    # int32_t marabunta_init(marabunta_context_t* ctx)
    lib.marabunta_init.argtypes = [ctypes.c_void_p]
    lib.marabunta_init.restype = ctypes.c_int32

    # int32_t marabunta_shutdown(marabunta_context_t* ctx)
    lib.marabunta_shutdown.argtypes = [ctypes.c_void_p]
    lib.marabunta_shutdown.restype = ctypes.c_int32

    # Progress reporting
    # int32_t marabunta_set_progress(marabunta_context_t* ctx, int32_t percent)
    lib.marabunta_set_progress.argtypes = [ctypes.c_void_p, ctypes.c_int32]
    lib.marabunta_set_progress.restype = ctypes.c_int32

    # int32_t marabunta_set_stage(marabunta_context_t* ctx, const char* stage)
    lib.marabunta_set_stage.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.marabunta_set_stage.restype = ctypes.c_int32

    # int32_t marabunta_set_message(marabunta_context_t* ctx, const char* message)
    lib.marabunta_set_message.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.marabunta_set_message.restype = ctypes.c_int32

    # int32_t marabunta_increment_progress(marabunta_context_t* ctx, int32_t delta)
    lib.marabunta_increment_progress.argtypes = [ctypes.c_void_p, ctypes.c_int32]
    lib.marabunta_increment_progress.restype = ctypes.c_int32

    # Yield and checkpointing
    # int32_t marabunta_should_yield(marabunta_context_t* ctx)
    lib.marabunta_should_yield.argtypes = [ctypes.c_void_p]
    lib.marabunta_should_yield.restype = ctypes.c_int32

    # int32_t marabunta_save_checkpoint(marabunta_context_t* ctx, const void* data, size_t len)
    lib.marabunta_save_checkpoint.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
    lib.marabunta_save_checkpoint.restype = ctypes.c_int32

    # int32_t marabunta_load_checkpoint(marabunta_context_t* ctx, void* buf, size_t buf_len, size_t* out_len)
    lib.marabunta_load_checkpoint.argtypes = [
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)
    ]
    lib.marabunta_load_checkpoint.restype = ctypes.c_int32

    # int32_t marabunta_has_checkpoint(marabunta_context_t* ctx)
    lib.marabunta_has_checkpoint.argtypes = [ctypes.c_void_p]
    lib.marabunta_has_checkpoint.restype = ctypes.c_int32

    # int32_t marabunta_get_checkpoint_size(marabunta_context_t* ctx, size_t* out_size)
    lib.marabunta_get_checkpoint_size.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t)]
    lib.marabunta_get_checkpoint_size.restype = ctypes.c_int32

    # Results
    # int32_t marabunta_set_result(marabunta_context_t* ctx, const void* data, size_t len)
    lib.marabunta_set_result.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
    lib.marabunta_set_result.restype = ctypes.c_int32

    # int32_t marabunta_append_result(marabunta_context_t* ctx, const void* data, size_t len)
    lib.marabunta_append_result.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
    lib.marabunta_append_result.restype = ctypes.c_int32

    # Resource hints
    # int32_t marabunta_hint_memory(marabunta_context_t* ctx, uint64_t bytes)
    lib.marabunta_hint_memory.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.marabunta_hint_memory.restype = ctypes.c_int32

    # int32_t marabunta_hint_time(marabunta_context_t* ctx, uint64_t seconds)
    lib.marabunta_hint_time.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.marabunta_hint_time.restype = ctypes.c_int32

    # int32_t marabunta_hint_splittable(marabunta_context_t* ctx, uint32_t min_chunks, uint32_t max_chunks)
    lib.marabunta_hint_splittable.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32]
    lib.marabunta_hint_splittable.restype = ctypes.c_int32

    # Logging
    # int32_t marabunta_log(marabunta_context_t* ctx, int32_t level, const char* message)
    lib.marabunta_log.argtypes = [ctypes.c_void_p, ctypes.c_int32, ctypes.c_char_p]
    lib.marabunta_log.restype = ctypes.c_int32

    # Task info
    # int32_t marabunta_get_task_id(marabunta_context_t* ctx, char* buf, size_t buf_len)
    lib.marabunta_get_task_id.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
    lib.marabunta_get_task_id.restype = ctypes.c_int32

    # int32_t marabunta_get_worker_rank(marabunta_context_t* ctx)
    lib.marabunta_get_worker_rank.argtypes = [ctypes.c_void_p]
    lib.marabunta_get_worker_rank.restype = ctypes.c_int32

    # int32_t marabunta_get_worker_count(marabunta_context_t* ctx)
    lib.marabunta_get_worker_count.argtypes = [ctypes.c_void_p]
    lib.marabunta_get_worker_count.restype = ctypes.c_int32

    # Error message (standalone function)
    # const char* marabunta_error_message(int32_t code)
    lib.marabunta_error_message.argtypes = [ctypes.c_int32]
    lib.marabunta_error_message.restype = ctypes.c_char_p


def load_library(force_reload=False):
    # type: (bool) -> ctypes.CDLL
    """
    Load and return the Marabunta SDK library.

    Thread-safe. The library is loaded once and cached.

    Args:
        force_reload: If True, reload the library even if already loaded.

    Returns:
        The loaded ctypes CDLL object.

    Raises:
        LibraryNotFoundError: If the library cannot be found.
    """
    global _lib, _mock_mode

    with _lib_lock:
        if _mock_mode:
            raise LibraryNotFoundError(["Mock mode enabled - library not loaded"])

        if _lib is not None and not force_reload:
            return _lib

        lib_path = _find_library()
        if lib_path is None:
            raise LibraryNotFoundError(_get_search_paths())

        try:
            _lib = ctypes.CDLL(lib_path)
            _setup_function_signatures(_lib)
            return _lib
        except OSError as e:
            raise MarabuntaSDKError(
                MARABUNTA_ERR_IO,
                "Failed to load library at {}: {}".format(lib_path, e)
            )


def _get_search_paths():
    # type: () -> list
    """Get list of paths that would be searched for the library."""
    paths = []

    explicit = os.environ.get("MARABUNTA_SDK_LIB_PATH")
    if explicit:
        paths.append("MARABUNTA_SDK_LIB_PATH={}".format(explicit))

    ld_path = os.environ.get("LD_LIBRARY_PATH")
    if ld_path:
        paths.append("LD_LIBRARY_PATH directories")

    paths.extend([
        "/usr/lib, /usr/local/lib (system paths)",
        "Bundled with package (if installed)",
        "Cargo target/ directory (for development)",
    ])

    return paths


def get_library():
    # type: () -> Optional[ctypes.CDLL]
    """
    Get the loaded library, or None if not loaded.

    Does not attempt to load the library.
    """
    return _lib


def is_library_loaded():
    # type: () -> bool
    """Check if the library has been loaded."""
    return _lib is not None


def enable_mock_mode():
    # type: () -> None
    """
    Enable mock mode for testing without the native library.

    When mock mode is enabled, load_library() will raise LibraryNotFoundError.
    Use MockMarabuntaContext instead for testing.
    """
    global _mock_mode, _lib
    with _lib_lock:
        _mock_mode = True
        _lib = None


def disable_mock_mode():
    # type: () -> None
    """Disable mock mode."""
    global _mock_mode
    with _lib_lock:
        _mock_mode = False


def is_mock_mode():
    # type: () -> bool
    """Check if mock mode is enabled."""
    return _mock_mode


# =============================================================================
# Mock Implementation for Testing
# =============================================================================

class MockMarabuntaContext:
    """
    Mock implementation of the Marabunta context for testing.

    Simulates the behavior of the native library without requiring libmarabunta_sdk.so.
    """

    def __init__(self):
        # type: () -> None
        self._initialized = False
        self._progress = 0
        self._stage = ""
        self._message = ""
        self._checkpoint = None  # type: Optional[bytes]
        self._result = b""
        self._task_id = "mock-task-001"
        self._worker_rank = 0
        self._worker_count = 1
        self._should_yield = MARABUNTA_YIELD_CONTINUE
        self._memory_hint = 0
        self._time_hint = 0
        self._splittable_min = 0
        self._splittable_max = 0
        self._log_messages = []  # type: list

    def init(self):
        # type: () -> int
        if self._initialized:
            return MARABUNTA_ERR_ALREADY_INITIALIZED
        self._initialized = True
        return MARABUNTA_OK

    def shutdown(self):
        # type: () -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._initialized = False
        return MARABUNTA_OK

    def set_progress(self, percent):
        # type: (int) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._progress = max(0, min(100, percent))
        return MARABUNTA_OK

    def set_stage(self, stage):
        # type: (str) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._stage = stage
        return MARABUNTA_OK

    def set_message(self, message):
        # type: (str) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._message = message
        return MARABUNTA_OK

    def increment_progress(self, delta):
        # type: (int) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._progress = max(0, min(100, self._progress + delta))
        return MARABUNTA_OK

    def should_yield(self):
        # type: () -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        return self._should_yield

    def save_checkpoint(self, data):
        # type: (bytes) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._checkpoint = data
        return MARABUNTA_OK

    def load_checkpoint(self):
        # type: () -> tuple
        """Returns (error_code, data)"""
        if not self._initialized:
            return (MARABUNTA_ERR_NOT_INITIALIZED, None)
        if self._checkpoint is None:
            return (MARABUNTA_ERR_CHECKPOINT_NOT_FOUND, None)
        return (MARABUNTA_OK, self._checkpoint)

    def has_checkpoint(self):
        # type: () -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        return 1 if self._checkpoint is not None else 0

    def get_checkpoint_size(self):
        # type: () -> tuple
        """Returns (error_code, size)"""
        if not self._initialized:
            return (MARABUNTA_ERR_NOT_INITIALIZED, 0)
        if self._checkpoint is None:
            return (MARABUNTA_ERR_CHECKPOINT_NOT_FOUND, 0)
        return (MARABUNTA_OK, len(self._checkpoint))

    def set_result(self, data):
        # type: (bytes) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._result = data
        return MARABUNTA_OK

    def append_result(self, data):
        # type: (bytes) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._result += data
        return MARABUNTA_OK

    def hint_memory(self, bytes_needed):
        # type: (int) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._memory_hint = bytes_needed
        return MARABUNTA_OK

    def hint_time(self, seconds):
        # type: (int) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._time_hint = seconds
        return MARABUNTA_OK

    def hint_splittable(self, min_chunks, max_chunks):
        # type: (int, int) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._splittable_min = min_chunks
        self._splittable_max = max_chunks
        return MARABUNTA_OK

    def log(self, level, message):
        # type: (int, str) -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        self._log_messages.append((level, message))
        return MARABUNTA_OK

    def get_task_id(self):
        # type: () -> tuple
        """Returns (error_code, task_id)"""
        if not self._initialized:
            return (MARABUNTA_ERR_NOT_INITIALIZED, "")
        return (MARABUNTA_OK, self._task_id)

    def get_worker_rank(self):
        # type: () -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        return self._worker_rank

    def get_worker_count(self):
        # type: () -> int
        if not self._initialized:
            return MARABUNTA_ERR_NOT_INITIALIZED
        return self._worker_count

    # Test helpers
    def set_should_yield(self, value):
        # type: (int) -> None
        """Set what should_yield() returns (for testing)."""
        self._should_yield = value

    def set_task_id(self, task_id):
        # type: (str) -> None
        """Set the task ID (for testing)."""
        self._task_id = task_id

    def set_worker_info(self, rank, count):
        # type: (int, int) -> None
        """Set worker rank and count (for testing)."""
        self._worker_rank = rank
        self._worker_count = count

    def get_log_messages(self):
        # type: () -> list
        """Get all logged messages (for testing)."""
        return list(self._log_messages)

    def clear_log_messages(self):
        # type: () -> None
        """Clear logged messages (for testing)."""
        self._log_messages = []
