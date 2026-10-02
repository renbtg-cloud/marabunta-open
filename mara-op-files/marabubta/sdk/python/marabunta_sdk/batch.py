# Marabunta - Licensed under the MIT License.
"""
Batch Job Submission for the Marabunta SDK.

Provides functionality for submitting multiple jobs at once
with parallel execution and result collection.

Example:
    import asyncio
    from marabunta_sdk import MarabuntaClient, JobBuilder, BatchSubmitter

    async def main():
        client = MarabuntaClient("http://localhost:8080")

        jobs = [
            JobBuilder("task-1").with_input({"id": 1}).build(),
            JobBuilder("task-2").with_input({"id": 2}).build(),
            JobBuilder("task-3").with_input({"id": 3}).build(),
        ]

        batch = BatchSubmitter(client, max_concurrency=10)
        results = await batch.submit_and_wait(jobs)

        for result in results:
            if result.success:
                print(f"Job {result.index}: {result.value}")
            else:
                print(f"Job {result.index} failed: {result.error}")

    asyncio.run(main())
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, List, Optional, TypeVar

from .client import (
    CancellationToken,
    CancellationError,
    MarabuntaClient,
    JobRequest,
    JobResponse,
    ClientError,
)

T = TypeVar("T")


@dataclass
class BatchOptions:
    """Options for batch submission."""

    max_concurrency: int = 50
    fail_fast: bool = False
    batch_timeout: Optional[float] = 3600.0  # seconds
    submission_delay: Optional[float] = None  # seconds
    continue_on_error: bool = True
    progress_interval: float = 1.0  # seconds


@dataclass
class BatchJobResult(Generic[T]):
    """Result of a single job in a batch."""

    index: int
    job_id: Optional[str]
    success: bool
    value: Optional[T] = None
    error: Optional[str] = None
    duration: float = 0.0

    @property
    def result(self):
        """Get the result or raise an exception."""
        if self.success:
            return self.value
        else:
            raise ClientError(self.error or "Unknown error")


@dataclass
class BatchProgress:
    """Progress information for batch operations."""

    total: int
    submitted: int = 0
    completed: int = 0
    succeeded: int = 0
    failed: int = 0
    running: int = 0

    @property
    def pending(self) -> int:
        """Get number of pending jobs."""
        return self.total - self.completed

    @property
    def percentage(self) -> float:
        """Get completion percentage."""
        if self.total == 0:
            return 100.0
        return (self.completed / self.total) * 100.0

    @property
    def is_complete(self) -> bool:
        """Check if all jobs are complete."""
        return self.completed >= self.total


class BatchSubmitter:
    """
    Batch submitter for parallel job submission and result collection.

    Example:
        batch = BatchSubmitter(client, max_concurrency=10)
        results = await batch.submit_and_wait(jobs)
    """

    def __init__(
        self,
        client: MarabuntaClient,
        *,
        max_concurrency: int = 50,
        fail_fast: bool = False,
        batch_timeout: Optional[float] = 3600.0,
        submission_delay: Optional[float] = None,
        cancel: Optional[CancellationToken] = None,
    ):
        """
        Create a new batch submitter.

        Args:
            client: The Marabunta client
            max_concurrency: Maximum number of concurrent submissions
            fail_fast: Whether to stop on first error
            batch_timeout: Timeout for the entire batch operation
            submission_delay: Delay between submissions (rate limiting)
            cancel: Optional cancellation token
        """
        self._client = client
        self._options = BatchOptions(
            max_concurrency=max(1, max_concurrency),
            fail_fast=fail_fast,
            batch_timeout=batch_timeout,
            submission_delay=submission_delay,
        )
        self._cancel = cancel or CancellationToken()
        self._progress = BatchProgress(total=0)

    def with_options(self, options: BatchOptions) -> "BatchSubmitter":
        """Set batch options."""
        self._options = options
        return self

    def with_cancellation(self, cancel: CancellationToken) -> "BatchSubmitter":
        """Set cancellation token."""
        self._cancel = cancel
        return self

    async def submit_all(self, jobs: List[JobRequest]) -> List[Optional[str]]:
        """
        Submit all jobs and return their IDs immediately.

        Args:
            jobs: List of job requests

        Returns:
            List of job IDs (None for failed submissions)
        """
        semaphore = asyncio.Semaphore(self._options.max_concurrency)
        results: List[Optional[str]] = [None] * len(jobs)

        async def submit_one(index: int, job: JobRequest) -> None:
            if self._cancel.is_cancelled():
                return

            async with semaphore:
                if self._options.submission_delay:
                    await asyncio.sleep(self._options.submission_delay)

                try:
                    job_id = await self._client.submit(job, self._cancel)
                    results[index] = job_id
                except Exception:
                    results[index] = None

        await asyncio.gather(
            *[submit_one(i, job) for i, job in enumerate(jobs)],
            return_exceptions=True,
        )

        return results

    async def submit_and_wait(
        self,
        jobs: List[JobRequest],
        result_type: type = dict,
    ) -> List[BatchJobResult]:
        """
        Submit all jobs and wait for all results.

        Args:
            jobs: List of job requests
            result_type: Expected result type (for documentation)

        Returns:
            List of batch job results
        """
        total = len(jobs)
        self._progress = BatchProgress(total=total)
        start_time = time.time()

        # Submit all jobs
        job_ids = await self.submit_all(jobs)
        self._progress.submitted = sum(1 for jid in job_ids if jid is not None)

        # Wait for results
        results: List[BatchJobResult] = []

        for index, job_id in enumerate(job_ids):
            if self._cancel.is_cancelled():
                break

            if self._options.batch_timeout:
                elapsed = time.time() - start_time
                if elapsed > self._options.batch_timeout:
                    break

            job_start = time.time()

            if job_id is None:
                results.append(
                    BatchJobResult(
                        index=index,
                        job_id=None,
                        success=False,
                        error="Submission failed",
                        duration=0.0,
                    )
                )
                self._progress.failed += 1
            else:
                self._progress.running += 1
                try:
                    value = await self._client.wait_for_result(
                        job_id, result_type, self._cancel
                    )
                    results.append(
                        BatchJobResult(
                            index=index,
                            job_id=job_id,
                            success=True,
                            value=value,
                            duration=time.time() - job_start,
                        )
                    )
                    self._progress.succeeded += 1
                except Exception as e:
                    results.append(
                        BatchJobResult(
                            index=index,
                            job_id=job_id,
                            success=False,
                            error=str(e),
                            duration=time.time() - job_start,
                        )
                    )
                    self._progress.failed += 1

                    if self._options.fail_fast:
                        break
                finally:
                    self._progress.running -= 1

            self._progress.completed += 1

        return results

    async def submit_and_wait_with_progress(
        self,
        jobs: List[JobRequest],
        progress_callback: Callable[[BatchProgress], None],
        result_type: type = dict,
    ) -> List[BatchJobResult]:
        """
        Submit jobs with progress callback.

        Args:
            jobs: List of job requests
            progress_callback: Callback function for progress updates
            result_type: Expected result type

        Returns:
            List of batch job results
        """
        total = len(jobs)
        self._progress = BatchProgress(total=total)
        start_time = time.time()

        # Report initial progress
        progress_callback(self._progress)

        # Submit all jobs
        job_ids = await self.submit_all(jobs)
        self._progress.submitted = sum(1 for jid in job_ids if jid is not None)
        progress_callback(self._progress)

        # Wait for results with progress updates
        results: List[BatchJobResult] = []

        for index, job_id in enumerate(job_ids):
            if self._cancel.is_cancelled():
                break

            if self._options.batch_timeout:
                elapsed = time.time() - start_time
                if elapsed > self._options.batch_timeout:
                    break

            job_start = time.time()

            if job_id is None:
                results.append(
                    BatchJobResult(
                        index=index,
                        job_id=None,
                        success=False,
                        error="Submission failed",
                    )
                )
                self._progress.failed += 1
            else:
                self._progress.running += 1
                try:
                    value = await self._client.wait_for_result(
                        job_id, result_type, self._cancel
                    )
                    results.append(
                        BatchJobResult(
                            index=index,
                            job_id=job_id,
                            success=True,
                            value=value,
                            duration=time.time() - job_start,
                        )
                    )
                    self._progress.succeeded += 1
                except Exception as e:
                    results.append(
                        BatchJobResult(
                            index=index,
                            job_id=job_id,
                            success=False,
                            error=str(e),
                            duration=time.time() - job_start,
                        )
                    )
                    self._progress.failed += 1

                    if self._options.fail_fast:
                        break
                finally:
                    self._progress.running -= 1

            self._progress.completed += 1
            progress_callback(self._progress)

        return results

    @property
    def progress(self) -> BatchProgress:
        """Get current progress."""
        return self._progress

    def cancel(self) -> None:
        """Cancel the batch operation."""
        self._cancel.cancel()


async def submit_batch(
    client: MarabuntaClient,
    jobs: List[JobRequest],
    result_type: type = dict,
) -> List[BatchJobResult]:
    """
    Convenience function to submit a batch of jobs.

    Args:
        client: The Marabunta client
        jobs: List of job requests
        result_type: Expected result type

    Returns:
        List of batch job results
    """
    batch = BatchSubmitter(client)
    return await batch.submit_and_wait(jobs, result_type)
