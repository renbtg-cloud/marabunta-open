# Marabunta - Licensed under the MIT License.
"""
Tests for MarabuntaTask class.
"""

import pytest
from marabunta_sdk import (
    MarabuntaTask,
    MarabuntaTaskError,
    TaskAbortedError,
    TaskPausedError,
    marabunta_task,
    MARABUNTA_YIELD_CONTINUE,
    MARABUNTA_YIELD_PAUSE,
    MARABUNTA_YIELD_ABORT,
    MARABUNTA_ERR_ALREADY_INITIALIZED,
)


class TestMarabuntaTaskBasic:
    """Test basic MarabuntaTask functionality."""

    def test_create_mock_task(self):
        """Test creating a task in mock mode."""
        task = MarabuntaTask(use_mock=True, auto_init=False)
        assert not task.is_initialized()

    def test_context_manager(self):
        """Test using task as context manager."""
        with MarabuntaTask(use_mock=True) as task:
            assert task.is_initialized()
        assert not task.is_initialized()

    def test_manual_init_shutdown(self):
        """Test manual initialization and shutdown."""
        task = MarabuntaTask(use_mock=True, auto_init=False)
        assert not task.is_initialized()

        task.init()
        assert task.is_initialized()

        task.shutdown()
        assert not task.is_initialized()

        # Shutdown is idempotent
        task.shutdown()
        assert not task.is_initialized()

    def test_double_init_raises(self):
        """Test that double initialization raises error."""
        with MarabuntaTask(use_mock=True) as task:
            with pytest.raises(MarabuntaTaskError) as exc_info:
                task.init()
            assert exc_info.value.code == MARABUNTA_ERR_ALREADY_INITIALIZED


class TestMarabuntaTaskProgress:
    """Test progress reporting."""

    def test_set_progress(self):
        """Test setting progress."""
        with MarabuntaTask(use_mock=True) as task:
            task.set_progress(50)
            mock = task._get_mock_context()
            assert mock._progress == 50

    def test_progress_clamping(self):
        """Test that progress is clamped to 0-100."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()

            task.set_progress(-10)
            assert mock._progress == 0

            task.set_progress(150)
            assert mock._progress == 100

    def test_set_stage(self):
        """Test setting stage."""
        with MarabuntaTask(use_mock=True) as task:
            task.set_stage("processing")
            mock = task._get_mock_context()
            assert mock._stage == "processing"

    def test_set_message(self):
        """Test setting message."""
        with MarabuntaTask(use_mock=True) as task:
            task.set_message("Working on item 5")
            mock = task._get_mock_context()
            assert mock._message == "Working on item 5"

    def test_increment_progress(self):
        """Test incrementing progress."""
        with MarabuntaTask(use_mock=True) as task:
            task.set_progress(10)
            task.increment_progress(5)
            mock = task._get_mock_context()
            assert mock._progress == 15


class TestMarabuntaTaskYield:
    """Test yield checking."""

    def test_should_yield_continue(self):
        """Test should_yield returns False when continuing."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_should_yield(MARABUNTA_YIELD_CONTINUE)
            assert task.should_yield() is False

    def test_should_yield_pause(self):
        """Test should_yield returns True when pausing."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_should_yield(MARABUNTA_YIELD_PAUSE)
            assert task.should_yield() is True

    def test_should_yield_abort(self):
        """Test should_yield returns True when aborting."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_should_yield(MARABUNTA_YIELD_ABORT)
            assert task.should_yield() is True

    def test_check_yield_continue(self):
        """Test check_yield does nothing when continuing."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_should_yield(MARABUNTA_YIELD_CONTINUE)
            task.check_yield()  # Should not raise

    def test_check_yield_pause(self):
        """Test check_yield raises TaskPausedError when pausing."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_should_yield(MARABUNTA_YIELD_PAUSE)
            with pytest.raises(TaskPausedError):
                task.check_yield()

    def test_check_yield_abort(self):
        """Test check_yield raises TaskAbortedError when aborting."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_should_yield(MARABUNTA_YIELD_ABORT)
            with pytest.raises(TaskAbortedError):
                task.check_yield()

    def test_yield_point_with_checkpoint(self):
        """Test yield_point saves checkpoint when pausing."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_should_yield(MARABUNTA_YIELD_PAUSE)

            with pytest.raises(TaskPausedError):
                task.yield_point({"index": 42})

            # Verify checkpoint was saved
            assert mock._checkpoint is not None


class TestMarabuntaTaskCheckpoint:
    """Test checkpointing."""

    def test_save_load_checkpoint(self):
        """Test saving and loading checkpoint."""
        with MarabuntaTask(use_mock=True) as task:
            state = {"index": 42, "data": [1, 2, 3], "nested": {"a": "b"}}
            task.save_checkpoint(state)

            loaded = task.load_checkpoint()
            assert loaded == state

    def test_load_checkpoint_none(self):
        """Test loading checkpoint when none exists."""
        with MarabuntaTask(use_mock=True) as task:
            loaded = task.load_checkpoint()
            assert loaded is None

    def test_has_checkpoint(self):
        """Test has_checkpoint."""
        with MarabuntaTask(use_mock=True) as task:
            assert task.has_checkpoint() is False

            task.save_checkpoint({"test": True})
            assert task.has_checkpoint() is True

    def test_checkpoint_complex_types(self):
        """Test checkpointing complex Python types."""
        with MarabuntaTask(use_mock=True) as task:
            import datetime

            state = {
                "set": {1, 2, 3},
                "tuple": (1, 2, 3),
                "bytes": b"hello",
                "none": None,
            }
            task.save_checkpoint(state)

            loaded = task.load_checkpoint()
            assert loaded["set"] == {1, 2, 3}
            assert loaded["tuple"] == (1, 2, 3)
            assert loaded["bytes"] == b"hello"
            assert loaded["none"] is None


class TestMarabuntaTaskResult:
    """Test result handling."""

    def test_set_result_dict(self):
        """Test setting dict result."""
        with MarabuntaTask(use_mock=True) as task:
            task.set_result({"answer": 42})
            mock = task._get_mock_context()
            assert b'"answer": 42' in mock._result

    def test_set_result_list(self):
        """Test setting list result."""
        with MarabuntaTask(use_mock=True) as task:
            task.set_result([1, 2, 3])
            mock = task._get_mock_context()
            assert mock._result == b"[1, 2, 3]"

    def test_append_result(self):
        """Test appending to result."""
        with MarabuntaTask(use_mock=True) as task:
            task.append_result(b"hello")
            task.append_result(b" world")
            mock = task._get_mock_context()
            assert mock._result == b"hello world"

    def test_append_result_string(self):
        """Test appending string to result (auto-encoded)."""
        with MarabuntaTask(use_mock=True) as task:
            task.append_result("hello")
            mock = task._get_mock_context()
            assert mock._result == b"hello"


class TestMarabuntaTaskHints:
    """Test resource hints."""

    def test_hint_memory(self):
        """Test memory hint."""
        with MarabuntaTask(use_mock=True) as task:
            task.hint_memory(2 * 1024**3)  # 2GB
            mock = task._get_mock_context()
            assert mock._memory_hint == 2 * 1024**3

    def test_hint_time(self):
        """Test time hint."""
        with MarabuntaTask(use_mock=True) as task:
            task.hint_time(3600)  # 1 hour
            mock = task._get_mock_context()
            assert mock._time_hint == 3600

    def test_hint_splittable(self):
        """Test splittable hint."""
        with MarabuntaTask(use_mock=True) as task:
            task.hint_splittable(10, 100)
            mock = task._get_mock_context()
            assert mock._splittable_min == 10
            assert mock._splittable_max == 100


class TestMarabuntaTaskLogging:
    """Test logging."""

    def test_log_levels(self):
        """Test all log levels."""
        with MarabuntaTask(use_mock=True) as task:
            task.log_debug("debug message")
            task.log_info("info message")
            task.log_warn("warn message")
            task.log_error("error message")

            mock = task._get_mock_context()
            messages = mock.get_log_messages()

            assert len(messages) == 4
            assert messages[0] == (0, "debug message")  # MARABUNTA_LOG_DEBUG
            assert messages[1] == (1, "info message")   # MARABUNTA_LOG_INFO
            assert messages[2] == (2, "warn message")   # MARABUNTA_LOG_WARN
            assert messages[3] == (3, "error message")  # MARABUNTA_LOG_ERROR


class TestMarabuntaTaskInfo:
    """Test task information properties."""

    def test_task_id(self):
        """Test getting task ID."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_task_id("test-task-123")
            assert task.task_id == "test-task-123"

    def test_worker_rank(self):
        """Test getting worker rank."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_worker_info(rank=5, count=10)
            assert task.worker_rank == 5

    def test_worker_count(self):
        """Test getting worker count."""
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()
            mock.set_worker_info(rank=5, count=10)
            assert task.worker_count == 10


class TestMarabuntaTaskDecorator:
    """Test @marabunta_task decorator."""

    def test_basic_decorator(self):
        """Test basic decorator usage."""
        @marabunta_task
        def my_task(task, x, y):
            task.set_stage("computing")
            return {"sum": x + y}

        # Decorator creates the task and sets result
        result = my_task(10, 20)
        assert result == {"sum": 30}

    def test_decorator_with_parens(self):
        """Test decorator with parentheses."""
        @marabunta_task()
        def my_task(task, x):
            return {"value": x * 2}

        result = my_task(21)
        assert result == {"value": 42}

    def test_decorator_no_return(self):
        """Test decorator with no return value."""
        @marabunta_task
        def my_task(task):
            task.set_progress(100)
            # No return value

        result = my_task()
        assert result is None
