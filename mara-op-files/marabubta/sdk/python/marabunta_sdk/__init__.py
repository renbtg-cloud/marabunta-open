# Marabunta - Licensed under the MIT License.
"""
Marabunta Task SDK - Python bindings for Marabunta distributed computing.

This package provides a Pythonic interface to the Marabunta compute framework,
allowing you to write tasks that can be distributed across thousands of
workers with automatic checkpointing and resumption.
"""

__version__ = "0.1.0"
__author__ = "Marabunta Compute Team"

# Main task interface
from .workflow import workflow, task

# New UX features
from .testing import swarm_test
from .jupyter import load_ipython_extension

from .task import (
    MarabuntaTask,
    MarabuntaTaskError,
    TaskAbortedError,
    TaskPausedError,
    marabunta_task,
)

# Progress utilities
from .progress import (
    ProgressTracker,
    MultiStageProgress,
    progress_iter,
    chunked_progress,
    with_progress,
)

# Checkpoint utilities
from .checkpoint import (
    CheckpointManager,
    ResumableState,
    auto_checkpoint,
    checkpoint,
    resumable_loop,
)

# Async client API
from .client import (
    MarabuntaClient,
    MarabuntaClientConfig,
    JobRequest,
    JobResponse,
    JobStatus,
    JobHandle,
    CancellationToken,
    RetryConfig,
    TimeoutConfig,
    ClientError,
    ConnectionError as ClientConnectionError,
    TimeoutError as ClientTimeoutError,
    CancellationError,
    SubmissionError,
    ExecutionError,
    DeserializationError,
    MaxRetriesExceededError,
    with_retry,
)

# Job builder
from .builder import (
    JobBuilder,
    BuilderError,
    MissingTaskTypeError,
    InvalidTagError,
    ValidationError,
    ResourceRequirements,
    JobPreview,
    job,
)

# Batch submission
from .batch import (
    BatchSubmitter,
    BatchOptions,
    BatchJobResult,
    BatchProgress,
    submit_batch,
)

# Public API
__all__ = [
    # Version
    "__version__",
    "workflow",
    "task",
    "swarm_test",
    "load_ipython_extension",
    # Main task classes
    "MarabuntaTask",
    "MarabuntaTaskError",
    "TaskAbortedError",
    "TaskPausedError",
    # Decorators
    "marabunta_task",
    "auto_checkpoint",
    "checkpoint",
    # Progress utilities
    "ProgressTracker",
    "MultiStageProgress",
    "progress_iter",
    "chunked_progress",
    "with_progress",
    # Checkpoint utilities
    "CheckpointManager",
    "ResumableState",
    "resumable_loop",
    # Async client API
    "MarabuntaClient",
    "MarabuntaClientConfig",
    "JobRequest",
    "JobResponse",
    "JobStatus",
    "JobHandle",
    "CancellationToken",
    "RetryConfig",
    "TimeoutConfig",
    "ClientError",
    "ClientConnectionError",
    "ClientTimeoutError",
    "CancellationError",
    "SubmissionError",
    "ExecutionError",
    "DeserializationError",
    "MaxRetriesExceededError",
    "with_retry",
    # Job builder
    "JobBuilder",
    "BuilderError",
    "MissingTaskTypeError",
    "InvalidTagError",
    "ValidationError",
    "ResourceRequirements",
    "JobPreview",
    "job",
    # Batch submission
    "BatchSubmitter",
    "BatchOptions",
    "BatchJobResult",
    "BatchProgress",
    "submit_batch",
]
