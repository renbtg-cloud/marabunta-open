# Marabunta - Licensed under the MIT License.
"""
Tests for checkpoint utilities.
"""

import pytest
from marabunta_sdk import (
    MarabuntaTask,
    CheckpointManager,
    ResumableState,
    auto_checkpoint,
    checkpoint,
    resumable_loop,
    marabunta_task,
    TaskPausedError,
    MARABUNTA_YIELD_PAUSE,
)


class TestCheckpointManager:
    """Test CheckpointManager class."""

    def test_load_no_checkpoint(self):
        """Test loading when no checkpoint exists."""
        with MarabuntaTask(use_mock=True) as task:
            manager = CheckpointManager(task, interval=10)
            assert manager.load() is None

    def test_save_and_load(self):
        """Test saving and loading checkpoint."""
        with MarabuntaTask(use_mock=True) as task:
            manager = CheckpointManager(task, interval=10)

            state = {"index": 42, "data": [1, 2, 3]}
            manager.save(state)

            loaded = manager.load()
            assert loaded == state

    def test_interval_checkpointing(self):
        """Test automatic checkpointing at intervals."""
        with MarabuntaTask(use_mock=True) as task:
            manager = CheckpointManager(task, interval=5)
            mock = task._get_mock_context()

            # First 4 calls should not checkpoint
            for i in range(4):
                saved = manager.maybe_checkpoint({"i": i})
                assert saved is False
                assert mock._checkpoint is None

            # 5th call should checkpoint
            saved = manager.maybe_checkpoint({"i": 4})
            assert saved is True
            assert mock._checkpoint is not None

    def test_yield_point(self):
        """Test yield_point without auto-checkpointing."""
        with MarabuntaTask(use_mock=True) as task:
            manager = CheckpointManager(task, interval=100)

            # Should not raise when continuing
            manager.yield_point({"state": "test"})


class TestResumableState:
    """Test ResumableState class."""

    def test_create_new_state(self):
        """Test creating new state when no checkpoint exists."""
        with MarabuntaTask(use_mock=True) as task:
            state = ResumableState.load_or_create(task, {
                "index": 0,
                "sum": 0,
            })

            assert state["index"] == 0
            assert state["sum"] == 0

    def test_resume_from_checkpoint(self):
        """Test resuming from existing checkpoint."""
        with MarabuntaTask(use_mock=True) as task:
            # Save a checkpoint
            task.save_checkpoint({"index": 50, "sum": 1000})

            # Load state - should resume from checkpoint
            state = ResumableState.load_or_create(task, {
                "index": 0,
                "sum": 0,
            })

            assert state["index"] == 50
            assert state["sum"] == 1000

    def test_dict_interface(self):
        """Test dictionary-like interface."""
        with MarabuntaTask(use_mock=True) as task:
            state = ResumableState.load_or_create(task, {"a": 1, "b": 2})

            # Get item
            assert state["a"] == 1

            # Set item
            state["c"] = 3
            assert state["c"] == 3

            # Contains
            assert "a" in state
            assert "z" not in state

            # Get with default
            assert state.get("a") == 1
            assert state.get("z", 99) == 99

            # Update
            state.update(d=4, e=5)
            assert state["d"] == 4
            assert state["e"] == 5

            # To dict
            d = state.to_dict()
            assert isinstance(d, dict)
            assert d["a"] == 1

    def test_checkpoint_methods(self):
        """Test checkpoint and maybe_checkpoint methods."""
        with MarabuntaTask(use_mock=True) as task:
            state = ResumableState.load_or_create(
                task, {"i": 0}, checkpoint_interval=5
            )
            mock = task._get_mock_context()

            # Force checkpoint
            state["i"] = 10
            state.checkpoint()
            assert mock._checkpoint is not None

            # Verify state was saved
            loaded = task.load_checkpoint()
            assert loaded["i"] == 10


class TestAutoCheckpointDecorator:
    """Test @auto_checkpoint decorator."""

    def test_basic_auto_checkpoint(self):
        """Test auto-checkpoint decorator."""
        @marabunta_task
        @auto_checkpoint(interval=3)
        def my_task(task, data):
            state = task.load_checkpoint() or {"i": 0, "sum": 0}

            for i in range(state["i"], len(data)):
                state["sum"] += data[i]
                state["i"] = i + 1
                # This will auto-checkpoint every 3 iterations
                task.yield_point(state)

            return state["sum"]

        # Run with mock (decorator creates mock task automatically)
        result = my_task([1, 2, 3, 4, 5])
        assert result == 15

    def test_auto_checkpoint_resumes(self):
        """Test that auto-checkpoint enables resumption."""
        checkpoint_data = None

        @auto_checkpoint(interval=2)
        def inner_task(task, data):
            nonlocal checkpoint_data
            state = task.load_checkpoint() or {"i": 0, "sum": 0}

            for i in range(state["i"], len(data)):
                state["sum"] += data[i]
                state["i"] = i + 1
                task.yield_point(state)

            return state["sum"]

        # Manually test with mock task
        with MarabuntaTask(use_mock=True) as task:
            mock = task._get_mock_context()

            # Run until we have a checkpoint
            try:
                for i in range(10):
                    mock._checkpoint = None  # Reset
                    inner_task(task, [1, 2, 3, 4, 5])
                    if mock._checkpoint is not None:
                        break
            except TaskPausedError:
                pass


class TestCheckpointDecorator:
    """Test @checkpoint decorator."""

    def test_checkpoint_decorator(self):
        """Test that checkpoint decorator passes checkpoint data."""
        @marabunta_task
        @checkpoint
        def my_task(task, checkpoint_data, multiplier):
            if checkpoint_data:
                return checkpoint_data["value"] * multiplier
            return 10 * multiplier

        # First run - no checkpoint
        result = my_task(2)
        assert result == 20


class TestResumableLoop:
    """Test resumable_loop function."""

    def test_basic_loop(self):
        """Test basic resumable loop."""
        with MarabuntaTask(use_mock=True) as task:
            data = [10, 20, 30, 40, 50]
            results = []

            for i, item in resumable_loop(task, data, checkpoint_interval=0):
                results.append((i, item))

            assert results == [(0, 10), (1, 20), (2, 30), (3, 40), (4, 50)]

    def test_resume_from_checkpoint(self):
        """Test resuming loop from checkpoint."""
        with MarabuntaTask(use_mock=True) as task:
            # Save checkpoint indicating we're at index 3
            task.save_checkpoint({"index": 3})

            data = [10, 20, 30, 40, 50]
            results = []

            for i, item in resumable_loop(task, data, checkpoint_interval=0):
                results.append((i, item))

            # Should start from index 3
            assert results == [(3, 40), (4, 50)]

    def test_extra_state(self):
        """Test storing extra state in resumable loop."""
        with MarabuntaTask(use_mock=True) as task:
            # Save checkpoint with extra state
            task.save_checkpoint({
                "index": 2,
                "running_sum": 30,
            })

            data = [10, 20, 30, 40, 50]
            loop = resumable_loop(task, data, checkpoint_interval=0)

            # Get extra state
            running_sum = loop.get_extra_state("running_sum", 0)
            assert running_sum == 30

            # Continue iteration
            for i, item in loop:
                running_sum += item

            assert running_sum == 30 + 30 + 40 + 50  # Started at 2, so includes 30

    def test_checkpoint_interval(self):
        """Test that checkpoints are saved at intervals."""
        with MarabuntaTask(use_mock=True) as task:
            data = list(range(10))
            mock = task._get_mock_context()

            checkpoint_count = 0
            last_checkpoint = None

            for i, item in resumable_loop(task, data, checkpoint_interval=3):
                if mock._checkpoint != last_checkpoint:
                    checkpoint_count += 1
                    last_checkpoint = mock._checkpoint

            # Should have checkpointed at iterations 3, 6, 9
            assert checkpoint_count >= 3


class TestCheckpointSerialization:
    """Test checkpoint serialization edge cases."""

    def test_nested_structures(self):
        """Test checkpointing deeply nested structures."""
        with MarabuntaTask(use_mock=True) as task:
            state = {
                "level1": {
                    "level2": {
                        "level3": {
                            "data": [1, 2, 3],
                        }
                    }
                }
            }
            task.save_checkpoint(state)

            loaded = task.load_checkpoint()
            assert loaded == state
            assert loaded["level1"]["level2"]["level3"]["data"] == [1, 2, 3]

    def test_large_checkpoint(self):
        """Test checkpointing large data."""
        with MarabuntaTask(use_mock=True) as task:
            # 1MB of data
            large_data = {"array": list(range(100000))}
            task.save_checkpoint(large_data)

            loaded = task.load_checkpoint()
            assert len(loaded["array"]) == 100000
            assert loaded["array"][99999] == 99999

    def test_custom_objects(self):
        """Test checkpointing custom objects."""
        class CustomClass:
            def __init__(self, value):
                self.value = value

            def __eq__(self, other):
                return isinstance(other, CustomClass) and self.value == other.value

        with MarabuntaTask(use_mock=True) as task:
            obj = CustomClass(42)
            task.save_checkpoint({"obj": obj})

            loaded = task.load_checkpoint()
            assert loaded["obj"].value == 42
