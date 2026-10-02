# Marabunta - Licensed under the MIT License.
"""
MarabuntaTask - Main interface for Marabunta compute tasks.

Provides a Pythonic interface for writing distributed compute tasks
that can be paused, checkpointed, and resumed across worker nodes.

Example:
    from marabunta_sdk import MarabuntaTask

    with MarabuntaTask() as task:
        task.set_stage("processing")
        for i, item in enumerate(data):
            if task.should_yield():
                task.save_checkpoint({"index": i})
                return
            process(item)
            task.set_progress(i * 100 // len(data))
        task.set_result({"answer": 42})
"""

import ctypes
import functools
import json
import pickle
import threading
from typing import Any, Callable, Dict, Optional, TypeVar, Union

from . import _ffi

# Type variable for decorator return types
F = TypeVar("F", bound=Callable[..., Any])


class MarabuntaTaskError(Exception):
    """Exception raised for MarabuntaTask errors."""

    def __init__(self, code, message=None):
        # type: (int, Optional[str]) -> None
        self.code = code
        self.message = message or _ffi.get_error_message(code)
        super(MarabuntaTaskError, self).__init__(self.message)


class TaskAbortedError(MarabuntaTaskError):
    """Raised when a task is aborted."""

    def __init__(self):
        # type: () -> None
        super(TaskAbortedError, self).__init__(
            _ffi.MARABUNTA_ERR_ABORTED,
            "Task was aborted by the scheduler"
        )


class TaskPausedError(MarabuntaTaskError):
    """Raised when a task should pause and save state."""

    def __init__(self, checkpoint_data=None):
        # type: (Any) -> None
        self.checkpoint_data = checkpoint_data
        super(TaskPausedError, self).__init__(
            _ffi.MARABUNTA_YIELD_PAUSE,
            "Task should pause and checkpoint"
        )


class MarabuntaTask:
    """
    Main interface for Marabunta compute tasks.

    Provides progress reporting, checkpointing, result handling,
    and resource hints for the Marabunta scheduler.

    Can be used as a context manager for automatic initialization
    and cleanup:

        with MarabuntaTask() as task:
            task.set_progress(50)
            task.set_result({"data": "value"})

    Or manually managed:

        task = MarabuntaTask(auto_init=False)
        task.init()
        try:
            # ... do work ...
        finally:
            task.shutdown()

    Thread Safety:
        MarabuntaTask is thread-safe. All methods can be called from
        any thread. The underlying C library handles synchronization.
    """

    def __init__(self, auto_init=True, use_mock=None):
        # type: (bool, Optional[bool]) -> None
        """
        Create a new MarabuntaTask.

        Args:
            auto_init: If True, automatically initialize when entering
                      context manager. Default True.
            use_mock: If True, use mock implementation (for testing).
                     If None, auto-detect based on library availability.
        """
        self._auto_init = auto_init
        self._initialized = False
        self._lock = threading.RLock()

        # Determine if we should use mock mode
        if use_mock is True:
            self._use_mock = True
        elif use_mock is False:
            self._use_mock = False
        else:
            # Auto-detect: use mock if library not available
            self._use_mock = _ffi.is_mock_mode() or not self._try_load_library()

        if self._use_mock:
            self._mock_ctx = _ffi.MockMarabuntaContext()
            self._ctx = None
            self._lib = None
        else:
            self._mock_ctx = None
            self._ctx = None  # type: Optional[ctypes.c_void_p]
            self._lib = _ffi.load_library()

    def _try_load_library(self):
        # type: () -> bool
        """Try to load the library, return True if successful."""
        try:
            _ffi.load_library()
            return True
        except _ffi.LibraryNotFoundError:
            return False

    def __enter__(self):
        # type: () -> MarabuntaTask
        """Enter context manager, optionally initializing."""
        if self._auto_init:
            self.init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # type: (Any, Any, Any) -> bool
        """Exit context manager, cleaning up resources."""
        self.shutdown()
        return False

    def _check_error(self, result, operation="operation"):
        # type: (int, str) -> int
        """Check result code and raise exception if error."""
        if result < 0:
            raise MarabuntaTaskError(result, "{} failed: {}".format(
                operation, _ffi.get_error_message(result)
            ))
        return result

    # =========================================================================
    # Initialization
    # =========================================================================

    def init(self):
        # type: () -> None
        """
        Initialize the task context.

        Must be called before using any other methods (unless using
        context manager with auto_init=True).

        Raises:
            MarabuntaTaskError: If already initialized or initialization fails.
        """
        with self._lock:
            if self._initialized:
                raise MarabuntaTaskError(
                    _ffi.MARABUNTA_ERR_ALREADY_INITIALIZED,
                    "Task already initialized"
                )

            if self._use_mock:
                result = self._mock_ctx.init()
                self._check_error(result, "init")
            else:
                self._ctx = self._lib.marabunta_context_create()
                if not self._ctx:
                    raise MarabuntaTaskError(
                        _ffi.MARABUNTA_ERR_ALLOCATION_FAILED,
                        "Failed to create context"
                    )
                result = self._lib.marabunta_init(self._ctx)
                if result != _ffi.MARABUNTA_OK:
                    self._lib.marabunta_context_destroy(self._ctx)
                    self._ctx = None
                    self._check_error(result, "init")

            self._initialized = True

    def shutdown(self):
        # type: () -> None
        """
        Shutdown and cleanup the task context.

        Safe to call multiple times.
        """
        with self._lock:
            if not self._initialized:
                return

            if self._use_mock:
                self._mock_ctx.shutdown()
            else:
                if self._ctx:
                    self._lib.marabunta_shutdown(self._ctx)
                    self._lib.marabunta_context_destroy(self._ctx)
                    self._ctx = None

            self._initialized = False

    def is_initialized(self):
        # type: () -> bool
        """Check if the task is initialized."""
        return self._initialized

    # =========================================================================
    # Progress Reporting
    # =========================================================================

    def set_progress(self, percent):
        # type: (int) -> None
        """
        Set the task progress percentage.

        Args:
            percent: Progress percentage (0-100). Values outside
                    this range are clamped.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.set_progress(int(percent))
            else:
                result = self._lib.marabunta_set_progress(self._ctx, int(percent))
            self._check_error(result, "set_progress")

    def set_stage(self, name):
        # type: (str) -> None
        """
        Set the current stage name.

        Useful for showing what phase the task is in (e.g., "loading",
        "processing", "saving").

        Args:
            name: Stage name string.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.set_stage(name)
            else:
                result = self._lib.marabunta_set_stage(self._ctx, name.encode("utf-8"))
            self._check_error(result, "set_stage")

    def set_message(self, msg):
        # type: (str) -> None
        """
        Set a status message.

        For displaying human-readable status information.

        Args:
            msg: Status message string.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.set_message(msg)
            else:
                result = self._lib.marabunta_set_message(self._ctx, msg.encode("utf-8"))
            self._check_error(result, "set_message")

    def increment_progress(self, delta):
        # type: (int) -> None
        """
        Increment progress by a delta value.

        Args:
            delta: Amount to add to current progress.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.increment_progress(int(delta))
            else:
                result = self._lib.marabunta_increment_progress(self._ctx, int(delta))
            self._check_error(result, "increment_progress")

    # =========================================================================
    # Yield and Checkpointing
    # =========================================================================

    def should_yield(self):
        # type: () -> bool
        """
        Check if the task should yield control.

        Call this periodically in long-running loops to allow the
        scheduler to pause or abort the task gracefully.

        Returns:
            True if the task should pause or abort, False to continue.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.should_yield()
            else:
                result = self._lib.marabunta_should_yield(self._ctx)

            if result < 0:
                self._check_error(result, "should_yield")

            return result != _ffi.MARABUNTA_YIELD_CONTINUE

    def check_yield(self):
        # type: () -> None
        """
        Check if should yield and raise appropriate exception.

        Convenience method that raises TaskAbortedError if aborted,
        TaskPausedError if should pause, or returns normally to continue.

        Raises:
            TaskAbortedError: If the task was aborted.
            TaskPausedError: If the task should pause.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.should_yield()
            else:
                result = self._lib.marabunta_should_yield(self._ctx)

            if result < 0:
                self._check_error(result, "check_yield")
            elif result == _ffi.MARABUNTA_YIELD_ABORT:
                raise TaskAbortedError()
            elif result == _ffi.MARABUNTA_YIELD_PAUSE:
                raise TaskPausedError()

    def yield_point(self, state=None):
        # type: (Any) -> None
        """
        Yield point that optionally saves checkpoint state.

        Call this at points where the task can be safely paused.
        If the scheduler requests a pause, this will save the
        checkpoint and raise TaskPausedError.

        Args:
            state: Optional state to checkpoint if pausing.
                  Must be picklable.

        Raises:
            TaskAbortedError: If the task was aborted.
            TaskPausedError: If the task should pause.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.should_yield()
            else:
                result = self._lib.marabunta_should_yield(self._ctx)

            if result < 0:
                self._check_error(result, "yield_point")
            elif result == _ffi.MARABUNTA_YIELD_ABORT:
                raise TaskAbortedError()
            elif result == _ffi.MARABUNTA_YIELD_PAUSE:
                if state is not None:
                    self.save_checkpoint(state)
                raise TaskPausedError(state)

    def save_checkpoint(self, data):
        # type: (Any) -> None
        """
        Save checkpoint data.

        The data is pickled for storage. Can be restored later with
        load_checkpoint().

        Args:
            data: Any picklable Python object.

        Raises:
            MarabuntaTaskError: If checkpoint save fails.
        """
        with self._lock:
            pickled = pickle.dumps(data, protocol=2)  # Protocol 2 for Python 2/3 compat

            if self._use_mock:
                result = self._mock_ctx.save_checkpoint(pickled)
            else:
                result = self._lib.marabunta_save_checkpoint(
                    self._ctx, pickled, len(pickled)
                )
            self._check_error(result, "save_checkpoint")

    def load_checkpoint(self):
        # type: () -> Any
        """
        Load previously saved checkpoint data.

        Returns:
            The unpickled checkpoint data, or None if no checkpoint exists.
        """
        with self._lock:
            if self._use_mock:
                error_code, data = self._mock_ctx.load_checkpoint()
                if error_code == _ffi.MARABUNTA_ERR_CHECKPOINT_NOT_FOUND:
                    return None
                elif error_code != _ffi.MARABUNTA_OK:
                    self._check_error(error_code, "load_checkpoint")
                return pickle.loads(data)
            else:
                # First, check if checkpoint exists and get size
                size_out = ctypes.c_size_t()
                result = self._lib.marabunta_get_checkpoint_size(
                    self._ctx, ctypes.byref(size_out)
                )
                if result == _ffi.MARABUNTA_ERR_CHECKPOINT_NOT_FOUND:
                    return None
                elif result != _ffi.MARABUNTA_OK:
                    self._check_error(result, "load_checkpoint (get size)")

                # Allocate buffer and load
                size = size_out.value
                buf = ctypes.create_string_buffer(size)
                out_len = ctypes.c_size_t()
                result = self._lib.marabunta_load_checkpoint(
                    self._ctx, buf, size, ctypes.byref(out_len)
                )
                self._check_error(result, "load_checkpoint")

                return pickle.loads(buf.raw[:out_len.value])

    def has_checkpoint(self):
        # type: () -> bool
        """
        Check if a checkpoint exists.

        Returns:
            True if a checkpoint exists, False otherwise.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.has_checkpoint()
            else:
                result = self._lib.marabunta_has_checkpoint(self._ctx)

            if result < 0:
                self._check_error(result, "has_checkpoint")
            return result != 0

    # =========================================================================
    # Results
    # =========================================================================

    def set_result(self, data):
        # type: (Any) -> None
        """
        Set the task result.

        The data is JSON-serialized for storage. Use this for the
        final output of the task.

        Args:
            data: Any JSON-serializable Python object.

        Raises:
            MarabuntaTaskError: If result set fails.
            TypeError: If data is not JSON-serializable.
        """
        with self._lock:
            json_data = json.dumps(data).encode("utf-8")

            if self._use_mock:
                result = self._mock_ctx.set_result(json_data)
            else:
                result = self._lib.marabunta_set_result(
                    self._ctx, json_data, len(json_data)
                )
            self._check_error(result, "set_result")

    def append_result(self, data):
        # type: (bytes) -> None
        """
        Append raw bytes to the result.

        For streaming results or binary data.

        Args:
            data: Bytes to append to result.

        Raises:
            MarabuntaTaskError: If append fails.
        """
        with self._lock:
            if isinstance(data, str):
                data = data.encode("utf-8")

            if self._use_mock:
                result = self._mock_ctx.append_result(data)
            else:
                result = self._lib.marabunta_append_result(
                    self._ctx, data, len(data)
                )
            self._check_error(result, "append_result")

    # =========================================================================
    # Resource Hints
    # =========================================================================

    def hint_memory(self, bytes_needed):
        # type: (int) -> None
        """
        Hint expected memory usage.

        Helps the scheduler place the task on appropriate workers.

        Args:
            bytes_needed: Expected memory usage in bytes.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.hint_memory(int(bytes_needed))
            else:
                result = self._lib.marabunta_hint_memory(self._ctx, int(bytes_needed))
            self._check_error(result, "hint_memory")

    def hint_time(self, seconds):
        # type: (int) -> None
        """
        Hint expected execution time.

        Helps the scheduler with planning and timeout decisions.

        Args:
            seconds: Expected execution time in seconds.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.hint_time(int(seconds))
            else:
                result = self._lib.marabunta_hint_time(self._ctx, int(seconds))
            self._check_error(result, "hint_time")

    def hint_splittable(self, min_chunks, max_chunks):
        # type: (int, int) -> None
        """
        Hint that the task can be split into chunks.

        Indicates the task can be parallelized, and provides bounds
        on how many chunks it can be split into.

        Args:
            min_chunks: Minimum number of chunks.
            max_chunks: Maximum number of chunks.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.hint_splittable(
                    int(min_chunks), int(max_chunks)
                )
            else:
                result = self._lib.marabunta_hint_splittable(
                    self._ctx, int(min_chunks), int(max_chunks)
                )
            self._check_error(result, "hint_splittable")

    # =========================================================================
    # Logging
    # =========================================================================

    def log_debug(self, msg):
        # type: (str) -> None
        """Log a debug message."""
        self._log(_ffi.MARABUNTA_LOG_DEBUG, msg)

    def log_info(self, msg):
        # type: (str) -> None
        """Log an info message."""
        self._log(_ffi.MARABUNTA_LOG_INFO, msg)

    def log_warn(self, msg):
        # type: (str) -> None
        """Log a warning message."""
        self._log(_ffi.MARABUNTA_LOG_WARN, msg)

    def log_error(self, msg):
        # type: (str) -> None
        """Log an error message."""
        self._log(_ffi.MARABUNTA_LOG_ERROR, msg)

    def _log(self, level, msg):
        # type: (int, str) -> None
        """Internal logging method."""
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.log(level, msg)
            else:
                result = self._lib.marabunta_log(self._ctx, level, msg.encode("utf-8"))
            # Don't raise on log errors, just ignore
            if result < 0:
                pass  # Silently ignore log errors

    # =========================================================================
    # Task Information
    # =========================================================================

    @property
    def task_id(self):
        # type: () -> str
        """Get the task ID."""
        with self._lock:
            if self._use_mock:
                error_code, task_id = self._mock_ctx.get_task_id()
                self._check_error(error_code, "get_task_id")
                return task_id
            else:
                buf = ctypes.create_string_buffer(256)
                result = self._lib.marabunta_get_task_id(self._ctx, buf, 256)
                self._check_error(result, "get_task_id")
                return buf.value.decode("utf-8")

    @property
    def worker_rank(self):
        # type: () -> int
        """
        Get this worker's rank.

        The rank is a unique integer from 0 to worker_count-1.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.get_worker_rank()
            else:
                result = self._lib.marabunta_get_worker_rank(self._ctx)

            if result < 0:
                self._check_error(result, "get_worker_rank")
            return result

    @property
    def worker_count(self):
        # type: () -> int
        """
        Get the total number of workers for this job.
        """
        with self._lock:
            if self._use_mock:
                result = self._mock_ctx.get_worker_count()
            else:
                result = self._lib.marabunta_get_worker_count(self._ctx)

            if result < 0:
                self._check_error(result, "get_worker_count")
            return result

    # =========================================================================
    # Mock Testing Helpers
    # =========================================================================

    def _get_mock_context(self):
        # type: () -> Optional[_ffi.MockMarabuntaContext]
        """
        Get the mock context (for testing).

        Returns None if not in mock mode.
        """
        return self._mock_ctx


# =============================================================================
# Decorators
# =============================================================================

def marabunta_task(func=None, auto_init=True):
    # type: (Optional[Callable], bool) -> Callable
    """
    Decorator that wraps a function as a Marabunta task.

    The decorated function receives a MarabuntaTask as its first argument.
    The function's return value is automatically set as the task result.

    Example:
        @marabunta_task
        def my_computation(task: MarabuntaTask, input_data):
            task.set_stage("computing")
            result = heavy_math(input_data)
            return result  # Automatically becomes the result

    Args:
        func: The function to decorate.
        auto_init: Whether to auto-initialize the task.

    Returns:
        Decorated function.
    """
    def decorator(fn):
        # type: (Callable) -> Callable
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with MarabuntaTask(auto_init=auto_init) as task:
                result = fn(task, *args, **kwargs)
                if result is not None:
                    try:
                        task.set_result(result)
                    except TypeError:
                        # Result not JSON-serializable, try pickle-based storage
                        task.save_checkpoint({"_final_result": result})
                return result
        return wrapper

    if func is not None:
        # Called as @marabunta_task without parentheses
        return decorator(func)
    else:
        # Called as @marabunta_task() with parentheses
        return decorator
