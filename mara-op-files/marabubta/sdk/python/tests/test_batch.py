# Marabunta - Licensed under the MIT License.
"""
Tests for batch submission functionality.
"""

import asyncio
import pytest
from marabunta_sdk import (
    MarabuntaClient,
    JobBuilder,
    BatchSubmitter,
    BatchOptions,
    BatchJobResult,
    BatchProgress,
    CancellationToken,
    submit_batch,
)


class TestBatchOptions:
    """Test BatchOptions."""

    def test_default_options(self):
        """Test default options."""
        options = BatchOptions()
        assert options.max_concurrency == 50
        assert options.fail_fast is False
        assert options.continue_on_error is True
        assert options.batch_timeout == 3600.0

    def test_custom_options(self):
        """Test custom options."""
        options = BatchOptions(
            max_concurrency=10,
            fail_fast=True,
            batch_timeout=60.0,
            submission_delay=0.1,
        )
        assert options.max_concurrency == 10
        assert options.fail_fast is True
        assert options.batch_timeout == 60.0
        assert options.submission_delay == 0.1


class TestBatchProgress:
    """Test BatchProgress."""

    def test_new_progress(self):
        """Test new progress."""
        progress = BatchProgress(total=10)
        assert progress.total == 10
        assert progress.submitted == 0
        assert progress.completed == 0
        assert progress.pending == 10

    def test_percentage(self):
        """Test percentage calculation."""
        progress = BatchProgress(total=10, completed=5)
        assert progress.percentage == 50.0

    def test_percentage_empty(self):
        """Test percentage with no jobs."""
        progress = BatchProgress(total=0)
        assert progress.percentage == 100.0

    def test_is_complete(self):
        """Test is_complete."""
        progress = BatchProgress(total=10, completed=5)
        assert not progress.is_complete

        progress = BatchProgress(total=10, completed=10)
        assert progress.is_complete


class TestBatchJobResult:
    """Test BatchJobResult."""

    def test_successful_result(self):
        """Test successful result."""
        result = BatchJobResult(
            index=0,
            job_id="job-1",
            success=True,
            value={"answer": 42},
        )
        assert result.success
        assert result.value == {"answer": 42}
        assert result.result == {"answer": 42}

    def test_failed_result(self):
        """Test failed result."""
        result = BatchJobResult(
            index=1,
            job_id="job-2",
            success=False,
            error="Something went wrong",
        )
        assert not result.success
        with pytest.raises(Exception):
            _ = result.result


class TestBatchSubmitter:
    """Test BatchSubmitter."""

    @pytest.mark.asyncio
    async def test_create_submitter(self):
        """Test creating a batch submitter."""
        client = MarabuntaClient("http://localhost:8080")
        batch = BatchSubmitter(client, max_concurrency=10)
        assert batch._options.max_concurrency == 10

    @pytest.mark.asyncio
    async def test_submit_all(self):
        """Test submitting all jobs."""
        client = MarabuntaClient("http://localhost:8080")
        jobs = [
            JobBuilder("task-1").with_input({"id": 1}).build(),
            JobBuilder("task-2").with_input({"id": 2}).build(),
            JobBuilder("task-3").with_input({"id": 3}).build(),
        ]

        batch = BatchSubmitter(client)
        job_ids = await batch.submit_all(jobs)

        assert len(job_ids) == 3
        assert all(jid is not None for jid in job_ids)
        assert all(jid.startswith("job-") for jid in job_ids)

    @pytest.mark.asyncio
    async def test_submit_and_wait(self):
        """Test submit and wait for results."""
        client = MarabuntaClient("http://localhost:8080")
        jobs = [
            JobBuilder("task-1").with_input({"id": 1}).build(),
            JobBuilder("task-2").with_input({"id": 2}).build(),
        ]

        # Set results in background
        async def set_results():
            await asyncio.sleep(0.05)
            for jid in client._mock_jobs.keys():
                client.set_job_result(jid, {"processed": True})

        asyncio.create_task(set_results())

        batch = BatchSubmitter(client)
        results = await batch.submit_and_wait(jobs)

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_submit_with_cancellation(self):
        """Test batch with cancellation."""
        client = MarabuntaClient("http://localhost:8080")
        jobs = [
            JobBuilder("task-1").with_input({}).build(),
            JobBuilder("task-2").with_input({}).build(),
        ]

        cancel = CancellationToken()
        cancel.cancel()

        batch = BatchSubmitter(client, cancel=cancel)
        job_ids = await batch.submit_all(jobs)

        assert all(jid is None for jid in job_ids)

    @pytest.mark.asyncio
    async def test_progress_tracking(self):
        """Test progress tracking."""
        client = MarabuntaClient("http://localhost:8080")
        jobs = [
            JobBuilder("task-1").with_input({}).build(),
        ]

        # Set results in background
        async def set_results():
            await asyncio.sleep(0.01)
            for jid in client._mock_jobs.keys():
                client.set_job_result(jid, {"done": True})

        asyncio.create_task(set_results())

        progress_updates = []

        def on_progress(progress):
            progress_updates.append(progress)

        batch = BatchSubmitter(client)
        await batch.submit_and_wait_with_progress(jobs, on_progress)

        assert len(progress_updates) > 0
        # Last update should show completion
        assert progress_updates[-1].completed == 1

    @pytest.mark.asyncio
    async def test_cancel_batch(self):
        """Test cancelling batch operation."""
        client = MarabuntaClient("http://localhost:8080")
        batch = BatchSubmitter(client)

        batch.cancel()
        assert batch._cancel.is_cancelled()


class TestSubmitBatchFunction:
    """Test submit_batch convenience function."""

    @pytest.mark.asyncio
    async def test_submit_batch(self):
        """Test submit_batch function."""
        client = MarabuntaClient("http://localhost:8080")
        jobs = [
            JobBuilder("task-1").with_input({}).build(),
        ]

        # Set results in background
        async def set_results():
            await asyncio.sleep(0.01)
            for jid in client._mock_jobs.keys():
                client.set_job_result(jid, {"value": 1})

        asyncio.create_task(set_results())

        results = await submit_batch(client, jobs)
        assert len(results) == 1
