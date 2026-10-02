# Marabunta - Licensed under the MIT License.
"""
Checkpointing utilities for Marabunta tasks.

Provides decorators and helpers for automatic checkpoint management,
making it easy to write resumable long-running tasks.
"""

import functools
import time
from typing import Any, Callable, Dict, Optional, TypeVar

from .task import MarabuntaTask, TaskPausedError, TaskAbortedError

F = TypeVar("F", bound=Callable[..., Any])


class CheckpointManager:
    """
    Manages checkpointing with automatic intervals.

    Example:
        manager = CheckpointManager(task, interval=100)

        state = manager.load() or {"i": 0, "acc": 0}
        for i in range(state["i"], 1000):
            state["acc"] += compute(i)
            state["i"] = i + 1
            manager.maybe_checkpoint(state)
    """

    def __init__(self, task, interval=100, time_interval=None):
        # type: (MarabuntaTask, int, Optional[float]) -> None
        """
        Create a checkpoint manager.

        Args:
            task: The MarabuntaTask to checkpoint with.
            interval: Checkpoint every N yield points (default 100).
                     Set to 0 to disable count-based checkpointing.
            time_interval: Checkpoint every N seconds (optional).
                          If set, checkpoints when either interval
                          or time_interval is reached.
        """
        self._task = task
        self._interval = interval
        self._time_interval = time_interval
        self._count = 0
        self._last_checkpoint_time = time.time()

    def load(self):
        # type: () -> Any
        """
        Load checkpoint if it exists.

        Returns:
            The checkpoint data, or None if no checkpoint exists.
        """
        return self._task.load_checkpoint()

    def save(self, state):
        # type: (Any) -> None
        """
        Force save a checkpoint.

        Args:
            state: The state to checkpoint.
        """
        self._task.save_checkpoint(state)
        self._count = 0
        self._last_checkpoint_time = time.time()

    def maybe_checkpoint(self, state):
        # type: (Any) -> bool
        """
        Checkpoint if interval has been reached.

        Also checks for yield requests and raises appropriate exceptions.

        Args:
            state: The state to checkpoint.

        Returns:
            True if a checkpoint was saved, False otherwise.

        Raises:
            TaskAbortedError: If the task was aborted.
            TaskPausedError: If the task should pause.
        """
        self._count += 1
        should_checkpoint = False

        # Check count interval
        if self._interval > 0 and self._count >= self._interval:
            should_checkpoint = True

        # Check time interval
        if self._time_interval is not None:
            elapsed = time.time() - self._last_checkpoint_time
            if elapsed >= self._time_interval:
                should_checkpoint = True

        if should_checkpoint:
            self.save(state)

        # Check for yield request
        self._task.yield_point(state)

        return should_checkpoint

    def yield_point(self, state=None):
        # type: (Any) -> None
        """
        Check yield point without auto-checkpointing.

        Args:
            state: Optional state to checkpoint if pausing.

        Raises:
            TaskAbortedError: If the task was aborted.
            TaskPausedError: If the task should pause.
        """
        self._task.yield_point(state)


def auto_checkpoint(interval=100, time_interval=None):
    # type: (int, Optional[float]) -> Callable
    """
    Decorator for automatic checkpoint management.

    The decorated function must accept `task` as its first argument.
    It should use `task.yield_point(state)` at resumable points.

    Example:
        @marabunta_task
        @auto_checkpoint(interval=100)
        def long_running(task: MarabuntaTask, data):
            state = task.load_checkpoint() or {"i": 0, "acc": 0}

            for i in range(state["i"], len(data)):
                state["acc"] += process(data[i])
                state["i"] = i + 1
                task.yield_point(state)  # Auto-checkpoints every 100 calls

            return state["acc"]

    Args:
        interval: Checkpoint every N yield points (default 100).
        time_interval: Optional time-based checkpoint interval in seconds.

    Returns:
        Decorator function.
    """
    def decorator(func):
        # type: (F) -> F
        @functools.wraps(func)
        def wrapper(task, *args, **kwargs):
            # type: (MarabuntaTask, Any, Any) -> Any
            # Create a wrapper task that auto-checkpoints
            wrapped_task = _AutoCheckpointTask(task, interval, time_interval)
            return func(wrapped_task, *args, **kwargs)
        return wrapper  # type: ignore
    return decorator


class _AutoCheckpointTask:
    """
    Wrapper around MarabuntaTask that adds automatic checkpointing.

    Intercepts yield_point() calls and adds checkpoint logic.
    """

    def __init__(self, task, interval, time_interval):
        # type: (MarabuntaTask, int, Optional[float]) -> None
        self._task = task
        self._manager = CheckpointManager(task, interval, time_interval)

    def __getattr__(self, name):
        # type: (str) -> Any
        """Delegate all other attributes to the wrapped task."""
        return getattr(self._task, name)

    def yield_point(self, state=None):
        # type: (Any) -> None
        """Yield point with automatic checkpointing."""
        if state is not None:
            self._manager.maybe_checkpoint(state)
        else:
            self._task.yield_point(state)

    def load_checkpoint(self):
        # type: () -> Any
        """Load checkpoint."""
        return self._manager.load()

    def save_checkpoint(self, state):
        # type: (Any) -> None
        """Save checkpoint."""
        self._manager.save(state)


class ResumableState:
    """
    Helper class for managing resumable computation state.

    Provides a convenient interface for tracking iteration progress
    and partial results.

    Example:
        state = ResumableState.load_or_create(task, {
            "index": 0,
            "partial_sum": 0,
            "items_processed": 0,
        })

        for i in range(state["index"], len(data)):
            state["partial_sum"] += process(data[i])
            state["index"] = i + 1
            state["items_processed"] += 1
            state.maybe_checkpoint()

        return state["partial_sum"]
    """

    def __init__(self, task, initial_state, checkpoint_interval=100):
        # type: (MarabuntaTask, Dict[str, Any], int) -> None
        """
        Create a resumable state.

        Args:
            task: The MarabuntaTask to use for checkpointing.
            initial_state: Initial state dictionary.
            checkpoint_interval: Checkpoint every N operations.
        """
        self._task = task
        self._state = dict(initial_state)
        self._manager = CheckpointManager(task, checkpoint_interval)

    @classmethod
    def load_or_create(cls, task, initial_state, checkpoint_interval=100):
        # type: (MarabuntaTask, Dict[str, Any], int) -> ResumableState
        """
        Load existing checkpoint or create new state.

        Args:
            task: The MarabuntaTask to use.
            initial_state: Initial state if no checkpoint exists.
            checkpoint_interval: Checkpoint every N operations.

        Returns:
            ResumableState instance.
        """
        instance = cls(task, initial_state, checkpoint_interval)
        loaded = task.load_checkpoint()
        if loaded is not None:
            instance._state = loaded
        return instance

    def __getitem__(self, key):
        # type: (str) -> Any
        """Get a state value."""
        return self._state[key]

    def __setitem__(self, key, value):
        # type: (str, Any) -> None
        """Set a state value."""
        self._state[key] = value

    def __contains__(self, key):
        # type: (str) -> bool
        """Check if key exists in state."""
        return key in self._state

    def get(self, key, default=None):
        # type: (str, Any) -> Any
        """Get a state value with default."""
        return self._state.get(key, default)

    def update(self, **kwargs):
        # type: (Any) -> None
        """Update multiple state values."""
        self._state.update(kwargs)

    def to_dict(self):
        # type: () -> Dict[str, Any]
        """Get state as a dictionary."""
        return dict(self._state)

    def checkpoint(self):
        # type: () -> None
        """Force save a checkpoint."""
        self._manager.save(self._state)

    def maybe_checkpoint(self):
        # type: () -> bool
        """
        Checkpoint if interval reached.

        Returns:
            True if checkpoint was saved.

        Raises:
            TaskAbortedError: If the task was aborted.
            TaskPausedError: If the task should pause.
        """
        return self._manager.maybe_checkpoint(self._state)

    def yield_point(self):
        # type: () -> None
        """
        Yield point (always checks, may checkpoint).

        Raises:
            TaskAbortedError: If the task was aborted.
            TaskPausedError: If the task should pause.
        """
        self._manager.maybe_checkpoint(self._state)


def resumable_loop(task, iterable, state_key="index", checkpoint_interval=100):
    # type: (MarabuntaTask, Any, str, int) -> _ResumableLoopIterator
    """
    Create a resumable loop iterator.

    Automatically handles checkpointing and resumption.

    Example:
        for i, item in resumable_loop(task, data):
            result = process(item)
            task.set_progress(i * 100 // len(data))

    Args:
        task: The MarabuntaTask to use.
        iterable: The iterable to loop over (must support indexing).
        state_key: Key to use for storing loop index in checkpoint.
        checkpoint_interval: Checkpoint every N iterations.

    Returns:
        Iterator yielding (index, item) tuples.
    """
    return _ResumableLoopIterator(task, iterable, state_key, checkpoint_interval)


class _ResumableLoopIterator:
    """Iterator for resumable loops."""

    def __init__(self, task, iterable, state_key, checkpoint_interval):
        # type: (MarabuntaTask, Any, str, int) -> None
        self._task = task
        self._iterable = iterable
        self._state_key = state_key
        self._interval = checkpoint_interval

        # Load checkpoint to get starting index
        checkpoint = task.load_checkpoint()
        if checkpoint and state_key in checkpoint:
            self._start_index = checkpoint[state_key]
            self._extra_state = {k: v for k, v in checkpoint.items() if k != state_key}
        else:
            self._start_index = 0
            self._extra_state = {}

        self._current_index = self._start_index
        self._count = 0

        # Get total length if possible
        try:
            self._total = len(iterable)
        except TypeError:
            self._total = None

    def __iter__(self):
        # type: () -> _ResumableLoopIterator
        return self

    def __next__(self):
        # type: () -> tuple
        """Get next (index, item) tuple."""
        # Check if we've reached the end
        if self._total is not None and self._current_index >= self._total:
            raise StopIteration

        try:
            item = self._iterable[self._current_index]
        except (IndexError, KeyError):
            raise StopIteration

        index = self._current_index
        self._current_index += 1
        self._count += 1

        # Maybe checkpoint
        if self._interval > 0 and self._count % self._interval == 0:
            state = {self._state_key: self._current_index}
            state.update(self._extra_state)
            self._task.save_checkpoint(state)
            self._task.check_yield()

        return (index, item)

    # Python 2 compatibility
    next = __next__

    def set_extra_state(self, **kwargs):
        # type: (Any) -> None
        """Set extra state to include in checkpoints."""
        self._extra_state.update(kwargs)

    def get_extra_state(self, key, default=None):
        # type: (str, Any) -> Any
        """Get extra state that was saved in checkpoint."""
        return self._extra_state.get(key, default)


def checkpoint(func):
    # type: (F) -> F
    """
    Simple decorator that loads checkpoint at start.

    The decorated function receives checkpoint data as second argument.

    Example:
        @marabunta_task
        @checkpoint
        def my_task(task, checkpoint_data, input_arg):
            if checkpoint_data:
                state = checkpoint_data
            else:
                state = {"i": 0}
            # ... continue from state ...

    Args:
        func: Function to decorate.

    Returns:
        Decorated function.
    """
    @functools.wraps(func)
    def wrapper(task, *args, **kwargs):
        # type: (MarabuntaTask, Any, Any) -> Any
        checkpoint_data = task.load_checkpoint()
        return func(task, checkpoint_data, *args, **kwargs)
    return wrapper  # type: ignore
