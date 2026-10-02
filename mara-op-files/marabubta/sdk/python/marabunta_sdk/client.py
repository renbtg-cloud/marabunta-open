# Marabunta - Licensed under the MIT License.
"""
Async client for Marabunta compute cluster.

Provides async/await support for job submission with:
- Proper cancellation support
- Configurable retry logic with exponential backoff
- Request timeout configuration
- Type-safe result handling

Example:
    import asyncio
    from marabunta_sdk import MarabuntaClient, JobBuilder

    async def main():
        client = MarabuntaClient("http://localhost:8080")

        job = (JobBuilder("data-processing")
               .with_input({"file": "data.csv"})
               .with_timeout(3600)
               .build())

        result = await client.submit_and_wait(job)
        print(f"Result: {result}")

    asyncio.run(main())
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union

T = TypeVar("T")


class JobStatus(Enum):
    """Status of a job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"

    def is_terminal(self) -> bool:
        """Check if this is a terminal state."""
        return self in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.TIMED_OUT,
        )

    def is_success(self) -> bool:
        """Check if this is a successful state."""
        return self == JobStatus.COMPLETED


class ClientError(Exception):
    """Base exception for client errors."""

    pass


class ConnectionError(ClientError):
    """Connection failed."""

    pass


class TimeoutError(ClientError):
    """Operation timed out."""

    def __init__(self, duration: float):
        self.duration = duration
        super().__init__(f"Operation timed out after {duration}s")


class CancellationError(ClientError):
    """Operation was cancelled."""

    pass


class SubmissionError(ClientError):
    """Job submission failed."""

    pass


class ExecutionError(ClientError):
    """Job execution failed."""

    pass


class DeserializationError(ClientError):
    """Failed to deserialize result."""

    pass


class MaxRetriesExceededError(ClientError):
    """Maximum retries exceeded."""

    def __init__(self, attempts: int, last_error: str):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Max retries exceeded after {attempts} attempts: {last_error}"
        )


@dataclass
class JobRequest:
    """Job submission request."""

    task_type: str
    input: Any = field(default_factory=dict)
    priority: int = 0
    timeout_secs: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)
    max_retries: int = 3
    checkpoint_enabled: bool = True


@dataclass
class JobResponse:
    """Job status response."""

    job_id: str
    status: JobStatus
    progress: int = 0
    stage: str = ""
    message: str = ""
    result: Optional[Any] = None
    error: Optional[str] = None
    submitted_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobResponse":
        """Create from dictionary."""
        status_str = data.get("status", "pending")
        try:
            status = JobStatus(status_str)
        except ValueError:
            status = JobStatus.PENDING

        return cls(
            job_id=data.get("job_id", ""),
            status=status,
            progress=data.get("progress", 0),
            stage=data.get("stage", ""),
            message=data.get("message", ""),
            result=data.get("result"),
            error=data.get("error"),
            submitted_at=data.get("submitted_at"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
        )


class CancellationToken:
    """Token for cancelling async operations."""

    def __init__(self):
        self._cancelled = False
        self._event = asyncio.Event()
        self._callbacks: List[Callable[[], None]] = []

    def cancel(self) -> None:
        """Cancel the operation."""
        if not self._cancelled:
            self._cancelled = True
            self._event.set()
            for callback in self._callbacks:
                try:
                    callback()
                except Exception:
                    pass

    def is_cancelled(self) -> bool:
        """Check if cancelled."""
        return self._cancelled

    async def wait(self) -> None:
        """Wait until cancelled."""
        await self._event.wait()

    def on_cancel(self, callback: Callable[[], None]) -> None:
        """Register a callback to be called on cancellation."""
        if self._cancelled:
            callback()
        else:
            self._callbacks.append(callback)

    def child(self) -> "CancellationToken":
        """Create a child token that is cancelled when parent is cancelled."""
        child = CancellationToken()
        self.on_cancel(child.cancel)
        return child


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_retries: int = 3
    initial_delay: float = 0.1  # seconds
    max_delay: float = 30.0  # seconds
    multiplier: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.25
    retryable_status_codes: List[int] = field(
        default_factory=lambda: [408, 429, 500, 502, 503, 504]
    )

    @classmethod
    def no_retry(cls) -> "RetryConfig":
        """Create a config with no retries."""
        return cls(max_retries=0)

    @classmethod
    def quick(cls) -> "RetryConfig":
        """Create a config optimized for quick retries."""
        return cls(
            max_retries=3,
            initial_delay=0.05,
            max_delay=0.5,
            multiplier=1.5,
        )

    @classmethod
    def persistent(cls) -> "RetryConfig":
        """Create a config optimized for persistent retries."""
        return cls(
            max_retries=10,
            initial_delay=1.0,
            max_delay=300.0,
            multiplier=2.0,
        )


@dataclass
class TimeoutConfig:
    """Configuration for timeouts."""

    connect_timeout: float = 10.0  # seconds
    read_timeout: float = 30.0
    write_timeout: float = 30.0
    total_timeout: Optional[float] = 300.0

    @classmethod
    def no_timeout(cls) -> "TimeoutConfig":
        """Create a config with no timeouts."""
        return cls(
            connect_timeout=float("inf"),
            read_timeout=float("inf"),
            write_timeout=float("inf"),
            total_timeout=None,
        )

    @classmethod
    def quick(cls) -> "TimeoutConfig":
        """Create a config optimized for quick operations."""
        return cls(
            connect_timeout=5.0,
            read_timeout=10.0,
            write_timeout=10.0,
            total_timeout=30.0,
        )

    @classmethod
    def long_running(cls) -> "TimeoutConfig":
        """Create a config optimized for long-running operations."""
        return cls(
            connect_timeout=30.0,
            read_timeout=300.0,
            write_timeout=60.0,
            total_timeout=3600.0,
        )


@dataclass
class MarabuntaClientConfig:
    """Client configuration."""

    base_url: str = "http://localhost:8080"
    retry: RetryConfig = field(default_factory=RetryConfig)
    timeout: TimeoutConfig = field(default_factory=TimeoutConfig)
    auth_token: Optional[str] = None
    default_job_timeout: Optional[float] = 3600.0  # seconds
    poll_interval: float = 0.5  # seconds
    max_concurrent_requests: int = 100


class MarabuntaClient:
    """
    Async client for the Marabunta compute cluster.

    Example:
        async with MarabuntaClient("http://localhost:8080") as client:
            job = JobRequest(task_type="my-task", input={"data": [1, 2, 3]})
            result = await client.submit_and_wait(job)
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        *,
        config: Optional[MarabuntaClientConfig] = None,
        retry: Optional[RetryConfig] = None,
        timeout: Optional[TimeoutConfig] = None,
        auth_token: Optional[str] = None,
    ):
        """
        Create a new client.

        Args:
            base_url: Base URL of the Marabunta API
            config: Full configuration (overrides other args)
            retry: Retry configuration
            timeout: Timeout configuration
            auth_token: Authentication token
        """
        if config is not None:
            self._config = config
        else:
            self._config = MarabuntaClientConfig(
                base_url=base_url,
                retry=retry or RetryConfig(),
                timeout=timeout or TimeoutConfig(),
                auth_token=auth_token,
            )

        # Mock job store for testing
        self._mock_jobs: Dict[str, JobResponse] = {}
        self._next_job_id = 1

    async def __aenter__(self) -> "MarabuntaClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    @property
    def config(self) -> MarabuntaClientConfig:
        """Get the client configuration."""
        return self._config

    async def submit(
        self,
        job: JobRequest,
        cancel: Optional[CancellationToken] = None,
    ) -> str:
        """
        Submit a job and return the job ID immediately.

        Args:
            job: The job request
            cancel: Optional cancellation token

        Returns:
            The job ID

        Raises:
            CancellationError: If cancelled
            SubmissionError: If submission fails
            TimeoutError: If timeout exceeded
        """
        cancel = cancel or CancellationToken()

        if cancel.is_cancelled():
            raise CancellationError()

        # Mock implementation
        job_id = f"job-{self._next_job_id}"
        self._next_job_id += 1

        response = JobResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            progress=0,
            message="Job submitted",
        )
        self._mock_jobs[job_id] = response

        # Simulate async submission
        await asyncio.sleep(0.01)

        return job_id

    async def get_status(
        self,
        job_id: str,
        cancel: Optional[CancellationToken] = None,
    ) -> JobResponse:
        """
        Get the status of a job.

        Args:
            job_id: The job ID
            cancel: Optional cancellation token

        Returns:
            The job response

        Raises:
            CancellationError: If cancelled
            ClientError: If job not found
        """
        cancel = cancel or CancellationToken()

        if cancel.is_cancelled():
            raise CancellationError()

        if job_id not in self._mock_jobs:
            raise ClientError(f"Job {job_id} not found")

        return self._mock_jobs[job_id]

    async def wait_for_result(
        self,
        job_id: str,
        result_type: type = dict,
        cancel: Optional[CancellationToken] = None,
    ) -> Any:
        """
        Wait for a job to complete and return the result.

        Args:
            job_id: The job ID
            result_type: Expected result type (for documentation)
            cancel: Optional cancellation token

        Returns:
            The job result

        Raises:
            CancellationError: If cancelled
            ExecutionError: If job fails
            TimeoutError: If timeout exceeded
        """
        cancel = cancel or CancellationToken()
        poll_interval = self._config.poll_interval
        timeout = self._config.default_job_timeout
        start_time = time.time()

        while True:
            if cancel.is_cancelled():
                raise CancellationError()

            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(timeout)

            response = await self.get_status(job_id, cancel)

            if response.status == JobStatus.COMPLETED:
                return response.result

            if response.status == JobStatus.FAILED:
                raise ExecutionError(response.error or "Unknown error")

            if response.status == JobStatus.CANCELLED:
                raise CancellationError()

            if response.status == JobStatus.TIMED_OUT:
                raise TimeoutError(timeout or 0)

            await asyncio.sleep(poll_interval)

    async def submit_and_wait(
        self,
        job: JobRequest,
        result_type: type = dict,
        cancel: Optional[CancellationToken] = None,
    ) -> Any:
        """
        Submit a job and wait for the result.

        Args:
            job: The job request
            result_type: Expected result type (for documentation)
            cancel: Optional cancellation token

        Returns:
            The job result
        """
        cancel = cancel or CancellationToken()
        job_id = await self.submit(job, cancel)
        return await self.wait_for_result(job_id, result_type, cancel)


    async def submit_workflow(
        self,
        workflow_dict: Dict[str, Any],
        cancel: Optional[CancellationToken] = None,
    ) -> str:
        """
        Submit a JSON workflow definition to the coordinator.
        
        Args:
            workflow_dict: The workflow DAG as a dictionary
            cancel: Optional cancellation token
            
        Returns:
            The workflow ID
        """
        cancel = cancel or CancellationToken()

        if cancel.is_cancelled():
            raise CancellationError()

        # Mock implementation for SDK
        workflow_id = f"wf-{self._next_job_id}"
        self._next_job_id += 1
        
        # Simulate async submission
        await asyncio.sleep(0.01)
        
        return workflow_id

    async def cancel_job(self, job_id: str) -> None:
        """
        Cancel a running job.

        Args:
            job_id: The job ID
        """
        if job_id in self._mock_jobs:
            job = self._mock_jobs[job_id]
            if not job.status.is_terminal():
                job.status = JobStatus.CANCELLED

    def set_job_result(self, job_id: str, result: Any) -> None:
        """Set job result (for testing)."""
        if job_id in self._mock_jobs:
            job = self._mock_jobs[job_id]
            job.status = JobStatus.COMPLETED
            job.result = result
            job.progress = 100

    def set_job_failed(self, job_id: str, error: str) -> None:
        """Set job as failed (for testing)."""
        if job_id in self._mock_jobs:
            job = self._mock_jobs[job_id]
            job.status = JobStatus.FAILED
            job.error = error

    def set_job_progress(self, job_id: str, progress: int, stage: str = "") -> None:
        """Set job progress (for testing)."""
        if job_id in self._mock_jobs:
            job = self._mock_jobs[job_id]
            job.progress = min(100, max(0, progress))
            job.stage = stage
            if job.status == JobStatus.PENDING:
                job.status = JobStatus.RUNNING


class JobHandle:
    """Handle for tracking a submitted job."""

    def __init__(self, job_id: str, client: MarabuntaClient):
        self._job_id = job_id
        self._client = client
        self._cancel = CancellationToken()

    @property
    def job_id(self) -> str:
        """Get the job ID."""
        return self._job_id

    async def status(self) -> JobResponse:
        """Get the current status."""
        return await self._client.get_status(self._job_id, self._cancel)

    async def wait(self, result_type: type = dict) -> Any:
        """Wait for the result."""
        return await self._client.wait_for_result(
            self._job_id, result_type, self._cancel
        )

    async def cancel(self) -> None:
        """Cancel the job."""
        self._cancel.cancel()
        await self._client.cancel_job(self._job_id)

    @property
    def cancellation_token(self) -> CancellationToken:
        """Get the cancellation token."""
        return self._cancel


async def with_retry(
    config: RetryConfig,
    operation: Callable[[], Any],
) -> Any:
    """
    Execute an async operation with retry logic.

    Args:
        config: Retry configuration
        operation: Async operation to execute

    Returns:
        Result of the operation

    Raises:
        MaxRetriesExceededError: If max retries exceeded
    """
    import random

    last_error = None
    delay = config.initial_delay

    for attempt in range(config.max_retries + 1):
        try:
            if asyncio.iscoroutinefunction(operation):
                return await operation()
            else:
                return operation()
        except Exception as e:
            last_error = str(e)

            if attempt >= config.max_retries:
                break

            # Calculate delay with optional jitter
            if config.jitter:
                jitter = delay * config.jitter_factor * (2 * random.random() - 1)
                actual_delay = max(0, delay + jitter)
            else:
                actual_delay = delay

            await asyncio.sleep(actual_delay)

            # Increase delay for next attempt
            delay = min(delay * config.multiplier, config.max_delay)

    raise MaxRetriesExceededError(config.max_retries + 1, last_error or "Unknown error")
