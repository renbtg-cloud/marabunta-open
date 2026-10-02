# Marabunta - Licensed under the MIT License.
"""
Tests for the async MarabuntaClient and related classes.
"""

import asyncio
import pytest
from marabunta_sdk import (
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
    CancellationError,
    ExecutionError,
    MaxRetriesExceededError,
    with_retry,
)


class TestJobStatus:
    """Test JobStatus enum."""

    def test_is_terminal(self):
        """Test is_terminal method."""
        assert not JobStatus.PENDING.is_terminal()
        assert not JobStatus.RUNNING.is_terminal()
        assert JobStatus.COMPLETED.is_terminal()
        assert JobStatus.FAILED.is_terminal()
        assert JobStatus.CANCELLED.is_terminal()
        assert JobStatus.TIMED_OUT.is_terminal()

    def test_is_success(self):
        """Test is_success method."""
        assert JobStatus.COMPLETED.is_success()
        assert not JobStatus.FAILED.is_success()
        assert not JobStatus.PENDING.is_success()


class TestCancellationToken:
    """Test CancellationToken."""

    def test_new_token_not_cancelled(self):
        """Test that new token is not cancelled."""
        token = CancellationToken()
        assert not token.is_cancelled()

    def test_cancel(self):
        """Test cancellation."""
        token = CancellationToken()
        token.cancel()
        assert token.is_cancelled()

    def test_cancel_idempotent(self):
        """Test that cancel is idempotent."""
        token = CancellationToken()
        token.cancel()
        token.cancel()
        assert token.is_cancelled()

    @pytest.mark.asyncio
    async def test_wait(self):
        """Test waiting for cancellation."""
        token = CancellationToken()

        async def cancel_later():
            await asyncio.sleep(0.01)
            token.cancel()

        asyncio.create_task(cancel_later())
        await asyncio.wait_for(token.wait(), timeout=1.0)
        assert token.is_cancelled()

    def test_on_cancel_callback(self):
        """Test cancellation callbacks."""
        token = CancellationToken()
        called = []

        token.on_cancel(lambda: called.append(1))
        assert len(called) == 0

        token.cancel()
        assert len(called) == 1

    def test_on_cancel_called_immediately_if_cancelled(self):
        """Test callback is called immediately if already cancelled."""
        token = CancellationToken()
        token.cancel()

        called = []
        token.on_cancel(lambda: called.append(1))
        assert len(called) == 1

    def test_child_token(self):
        """Test child token."""
        parent = CancellationToken()
        child = parent.child()

        assert not child.is_cancelled()
        parent.cancel()
        # Give async task time to propagate
        assert parent.is_cancelled()


class TestRetryConfig:
    """Test RetryConfig."""

    def test_default_config(self):
        """Test default configuration."""
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.initial_delay == 0.1
        assert config.jitter is True

    def test_no_retry_config(self):
        """Test no retry configuration."""
        config = RetryConfig.no_retry()
        assert config.max_retries == 0

    def test_quick_config(self):
        """Test quick configuration."""
        config = RetryConfig.quick()
        assert config.max_retries == 3
        assert config.initial_delay < RetryConfig().initial_delay

    def test_persistent_config(self):
        """Test persistent configuration."""
        config = RetryConfig.persistent()
        assert config.max_retries == 10
        assert config.initial_delay > RetryConfig().initial_delay


class TestTimeoutConfig:
    """Test TimeoutConfig."""

    def test_default_config(self):
        """Test default configuration."""
        config = TimeoutConfig()
        assert config.connect_timeout == 10.0
        assert config.read_timeout == 30.0
        assert config.total_timeout == 300.0

    def test_no_timeout_config(self):
        """Test no timeout configuration."""
        config = TimeoutConfig.no_timeout()
        assert config.total_timeout is None

    def test_quick_config(self):
        """Test quick configuration."""
        config = TimeoutConfig.quick()
        assert config.total_timeout == 30.0

    def test_long_running_config(self):
        """Test long running configuration."""
        config = TimeoutConfig.long_running()
        assert config.total_timeout == 3600.0


class TestMarabuntaClient:
    """Test MarabuntaClient."""

    @pytest.mark.asyncio
    async def test_create_client(self):
        """Test creating a client."""
        client = MarabuntaClient("http://localhost:8080")
        assert client.config.base_url == "http://localhost:8080"

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test using client as context manager."""
        async with MarabuntaClient("http://localhost:8080") as client:
            assert client is not None

    @pytest.mark.asyncio
    async def test_submit_job(self):
        """Test submitting a job."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={"value": 42})
        job_id = await client.submit(job)
        assert job_id.startswith("job-")

    @pytest.mark.asyncio
    async def test_get_status(self):
        """Test getting job status."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        status = await client.get_status(job_id)
        assert status.job_id == job_id
        assert status.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_wait_for_result(self):
        """Test waiting for job result."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        # Set result in background
        async def set_result():
            await asyncio.sleep(0.05)
            client.set_job_result(job_id, {"answer": 42})

        asyncio.create_task(set_result())

        result = await client.wait_for_result(job_id)
        assert result == {"answer": 42}

    @pytest.mark.asyncio
    async def test_submit_and_wait(self):
        """Test submit and wait."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={"value": 1})

        # Set result in background
        async def set_result():
            await asyncio.sleep(0.05)
            # Get the job id from mock jobs
            for jid in client._mock_jobs.keys():
                client.set_job_result(jid, {"result": "success"})

        asyncio.create_task(set_result())

        result = await client.submit_and_wait(job)
        assert result == {"result": "success"}

    @pytest.mark.asyncio
    async def test_cancel_job(self):
        """Test cancelling a job."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        await client.cancel_job(job_id)

        status = await client.get_status(job_id)
        assert status.status == JobStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_submit_with_cancellation(self):
        """Test submit with immediate cancellation."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        cancel = CancellationToken()
        cancel.cancel()

        with pytest.raises(CancellationError):
            await client.submit(job, cancel)

    @pytest.mark.asyncio
    async def test_set_job_progress(self):
        """Test setting job progress."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        client.set_job_progress(job_id, 50, "processing")

        status = await client.get_status(job_id)
        assert status.progress == 50
        assert status.stage == "processing"
        assert status.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_set_job_failed(self):
        """Test setting job as failed."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        client.set_job_failed(job_id, "Something went wrong")

        status = await client.get_status(job_id)
        assert status.status == JobStatus.FAILED
        assert status.error == "Something went wrong"


class TestJobHandle:
    """Test JobHandle."""

    @pytest.mark.asyncio
    async def test_job_handle(self):
        """Test job handle."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        handle = JobHandle(job_id, client)
        assert handle.job_id == job_id

    @pytest.mark.asyncio
    async def test_job_handle_status(self):
        """Test getting status via handle."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        handle = JobHandle(job_id, client)
        status = await handle.status()
        assert status.job_id == job_id

    @pytest.mark.asyncio
    async def test_job_handle_wait(self):
        """Test waiting for result via handle."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        handle = JobHandle(job_id, client)

        # Set result in background
        async def set_result():
            await asyncio.sleep(0.05)
            client.set_job_result(job_id, {"value": 123})

        asyncio.create_task(set_result())

        result = await handle.wait()
        assert result == {"value": 123}

    @pytest.mark.asyncio
    async def test_job_handle_cancel(self):
        """Test cancelling via handle."""
        client = MarabuntaClient("http://localhost:8080")
        job = JobRequest(task_type="test-task", input={})
        job_id = await client.submit(job)

        handle = JobHandle(job_id, client)
        await handle.cancel()

        status = await client.get_status(job_id)
        assert status.status == JobStatus.CANCELLED


class TestWithRetry:
    """Test with_retry function."""

    @pytest.mark.asyncio
    async def test_success_first_try(self):
        """Test success on first try."""
        config = RetryConfig(max_retries=3)

        async def operation():
            return "success"

        result = await with_retry(config, operation)
        assert result == "success"

    @pytest.mark.asyncio
    async def test_success_after_retry(self):
        """Test success after retry."""
        config = RetryConfig(max_retries=3, initial_delay=0.01, jitter=False)
        attempts = [0]

        async def operation():
            attempts[0] += 1
            if attempts[0] < 3:
                raise Exception("fail")
            return "success"

        result = await with_retry(config, operation)
        assert result == "success"
        assert attempts[0] == 3

    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self):
        """Test max retries exceeded."""
        config = RetryConfig(max_retries=2, initial_delay=0.01, jitter=False)

        async def operation():
            raise Exception("always fails")

        with pytest.raises(MaxRetriesExceededError) as exc_info:
            await with_retry(config, operation)

        assert exc_info.value.attempts == 3  # initial + 2 retries
