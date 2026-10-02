# Marabunta - Licensed under the MIT License.
"""
Progress tracking utilities for Marabunta tasks.

Provides convenient helpers for common progress reporting patterns.
"""

from typing import Any, Callable, Iterable, Iterator, Optional, TypeVar

from .task import MarabuntaTask

T = TypeVar("T")


class ProgressTracker:
    """
    Track progress through a known number of items.

    Example:
        tracker = ProgressTracker(task, total=1000)
        for item in data:
            process(item)
            tracker.increment()
    """

    def __init__(self, task, total, start=0, end=100):
        # type: (MarabuntaTask, int, int, int) -> None
        """
        Create a progress tracker.

        Args:
            task: The MarabuntaTask to report progress to.
            total: Total number of items to process.
            start: Starting progress percentage (default 0).
            end: Ending progress percentage (default 100).
        """
        self._task = task
        self._total = max(1, total)  # Avoid division by zero
        self._current = 0
        self._start = start
        self._end = end
        self._last_reported = -1

    def increment(self, count=1):
        # type: (int) -> None
        """
        Increment the current item count and update progress.

        Args:
            count: Number of items to increment by.
        """
        self._current += count
        self._update_progress()

    def set_current(self, current):
        # type: (int) -> None
        """
        Set the current item count directly.

        Args:
            current: The current item index.
        """
        self._current = current
        self._update_progress()

    def _update_progress(self):
        # type: () -> None
        """Update progress on the task if changed."""
        # Calculate progress percentage within our range
        ratio = min(1.0, self._current / self._total)
        progress = int(self._start + ratio * (self._end - self._start))

        # Only report if changed (avoid excessive updates)
        if progress != self._last_reported:
            self._task.set_progress(progress)
            self._last_reported = progress

    @property
    def current(self):
        # type: () -> int
        """Get the current item count."""
        return self._current

    @property
    def total(self):
        # type: () -> int
        """Get the total item count."""
        return self._total

    @property
    def percent(self):
        # type: () -> float
        """Get the current percentage (0.0-100.0)."""
        return (self._current / self._total) * 100


def progress_iter(task, iterable, total=None, stage=None, yield_interval=1):
    # type: (MarabuntaTask, Iterable[T], Optional[int], Optional[str], int) -> Iterator[T]
    """
    Iterate with automatic progress reporting and yield checking.

    Example:
        for item in progress_iter(task, data, stage="processing"):
            process(item)

    Args:
        task: The MarabuntaTask to report to.
        iterable: The iterable to wrap.
        total: Total count (auto-detected for sized iterables).
        stage: Optional stage name to set.
        yield_interval: Check yield every N items (default 1).

    Yields:
        Items from the iterable.

    Raises:
        TaskAbortedError: If the task was aborted.
        TaskPausedError: If the task should pause.
    """
    if stage:
        task.set_stage(stage)

    # Try to auto-detect total
    if total is None:
        try:
            total = len(iterable)  # type: ignore
        except TypeError:
            total = None

    if total is not None and total > 0:
        tracker = ProgressTracker(task, total)
        for i, item in enumerate(iterable):
            yield item
            tracker.increment()
            if yield_interval > 0 and (i + 1) % yield_interval == 0:
                task.check_yield()
    else:
        # Unknown total - just check yields
        for i, item in enumerate(iterable):
            yield item
            if yield_interval > 0 and (i + 1) % yield_interval == 0:
                task.check_yield()


def chunked_progress(task, iterable, chunk_size, total=None, stage=None):
    # type: (MarabuntaTask, Iterable[T], int, Optional[int], Optional[str]) -> Iterator[list]
    """
    Iterate in chunks with progress reporting.

    Useful for batch processing with periodic yield checks.

    Example:
        for chunk in chunked_progress(task, data, chunk_size=100):
            process_batch(chunk)

    Args:
        task: The MarabuntaTask to report to.
        iterable: The iterable to chunk.
        chunk_size: Size of each chunk.
        total: Total count (auto-detected for sized iterables).
        stage: Optional stage name to set.

    Yields:
        Lists of items (chunks).
    """
    if stage:
        task.set_stage(stage)

    # Try to auto-detect total
    if total is None:
        try:
            total = len(iterable)  # type: ignore
        except TypeError:
            total = None

    if total is not None and total > 0:
        tracker = ProgressTracker(task, total)
    else:
        tracker = None

    chunk = []  # type: list
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            if tracker:
                tracker.increment(len(chunk))
            task.check_yield()
            chunk = []

    # Yield remaining items
    if chunk:
        yield chunk
        if tracker:
            tracker.increment(len(chunk))


class MultiStageProgress:
    """
    Track progress across multiple stages.

    Each stage gets a portion of the total progress.

    Example:
        stages = MultiStageProgress(task, {
            "loading": 10,
            "processing": 70,
            "saving": 20,
        })

        with stages.stage("loading"):
            load_data()

        with stages.stage("processing"):
            for i in range(100):
                process()
                stages.set_progress(i)  # 0-100 within this stage

        with stages.stage("saving"):
            save_results()
    """

    def __init__(self, task, stages):
        # type: (MarabuntaTask, dict) -> None
        """
        Create a multi-stage progress tracker.

        Args:
            task: The MarabuntaTask to report to.
            stages: Dict mapping stage name to weight (percentage).
                   Weights should sum to 100.
        """
        self._task = task
        self._stages = stages
        self._current_stage = None  # type: Optional[str]
        self._stage_progress = 0

        # Calculate cumulative offsets
        self._offsets = {}  # type: dict
        self._widths = {}  # type: dict
        offset = 0
        total_weight = sum(stages.values())

        for name, weight in stages.items():
            normalized = (weight / total_weight) * 100
            self._offsets[name] = offset
            self._widths[name] = normalized
            offset += normalized

    def stage(self, name):
        # type: (str) -> _StageContext
        """
        Enter a stage (context manager).

        Args:
            name: The stage name (must be in the stages dict).

        Returns:
            Context manager for the stage.
        """
        return _StageContext(self, name)

    def begin_stage(self, name):
        # type: (str) -> None
        """
        Begin a stage manually.

        Args:
            name: The stage name.
        """
        if name not in self._stages:
            raise ValueError("Unknown stage: {}".format(name))
        self._current_stage = name
        self._stage_progress = 0
        self._task.set_stage(name)
        self._update_progress()

    def end_stage(self):
        # type: () -> None
        """End the current stage (sets progress to 100% of stage)."""
        if self._current_stage:
            self._stage_progress = 100
            self._update_progress()
            self._current_stage = None

    def set_progress(self, percent):
        # type: (int) -> None
        """
        Set progress within the current stage.

        Args:
            percent: Progress within the stage (0-100).
        """
        self._stage_progress = max(0, min(100, percent))
        self._update_progress()

    def _update_progress(self):
        # type: () -> None
        """Update the task's overall progress."""
        if self._current_stage is None:
            return

        offset = self._offsets[self._current_stage]
        width = self._widths[self._current_stage]
        overall = int(offset + (self._stage_progress / 100) * width)
        self._task.set_progress(overall)


class _StageContext:
    """Context manager for a multi-stage progress stage."""

    def __init__(self, tracker, name):
        # type: (MultiStageProgress, str) -> None
        self._tracker = tracker
        self._name = name

    def __enter__(self):
        # type: () -> MultiStageProgress
        self._tracker.begin_stage(self._name)
        return self._tracker

    def __exit__(self, exc_type, exc_val, exc_tb):
        # type: (Any, Any, Any) -> bool
        self._tracker.end_stage()
        return False


def with_progress(task, stage=None):
    # type: (MarabuntaTask, Optional[str]) -> Callable
    """
    Decorator that tracks function progress.

    The decorated function should accept a `progress` callback as
    a keyword argument.

    Example:
        @with_progress(task, stage="processing")
        def process_data(data, progress=None):
            for i, item in enumerate(data):
                handle(item)
                if progress:
                    progress(i * 100 // len(data))

    Args:
        task: The MarabuntaTask to report to.
        stage: Optional stage name.

    Returns:
        Decorator function.
    """
    def decorator(func):
        # type: (Callable) -> Callable
        def wrapper(*args, **kwargs):
            if stage:
                task.set_stage(stage)

            def progress_callback(percent):
                # type: (int) -> None
                task.set_progress(percent)

            kwargs["progress"] = progress_callback
            return func(*args, **kwargs)
        return wrapper
    return decorator
