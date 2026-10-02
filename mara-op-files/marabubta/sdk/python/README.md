<!-- Marabunta - Licensed under the MIT License.
# Marabunta Task SDK for Python

Python bindings for the Marabunta distributed computing framework. Write tasks that can be distributed across thousands of workers with automatic checkpointing and resumption.

## Installation

```bash
pip install marabunta-sdk
```

Or install from source:

```bash
cd sdk/python
pip install -e .
```

### Requirements

- Python 3.7+
- `libmarabunta_sdk.so` (the native library) must be available at runtime

Set the library path:
```bash
export LD_LIBRARY_PATH=/path/to/marabunta/lib:$LD_LIBRARY_PATH
# or
export MARABUNTA_SDK_LIB_PATH=/path/to/libmarabunta_sdk.so
```

## Quick Start

### Basic Usage

```python
from marabunta_sdk import MarabuntaTask

with MarabuntaTask() as task:
    # Report what we're doing
    task.set_stage("loading")
    data = load_data()

    task.set_stage("processing")
    results = []

    for i, item in enumerate(data):
        # Check if scheduler wants us to pause/abort
        if task.should_yield():
            task.save_checkpoint({"index": i, "results": results})
            return

        results.append(process(item))
        task.set_progress(i * 100 // len(data))

    # Set the final result (JSON-serializable)
    task.set_result({"count": len(results), "sum": sum(results)})
```

### Async Client

```python
import asyncio
from marabunta_sdk import MarabuntaClient, JobBuilder, CancellationToken

async def main():
    # Create a client
    client = MarabuntaClient("http://localhost:8080")

    # Build and submit a job
    job = (
        JobBuilder("process-data")
        .with_input({"file": "data.csv"})
        .with_priority(50)
        .with_tag("production")
        .build()
    )

    # Submit and wait for result
    result = await client.submit_and_wait(job)
    print(f"Result: {result}")

asyncio.run(main())
```

### Using Decorators

```python
from marabunta_sdk import marabunta_task, auto_checkpoint

@marabunta_task
def simple_task(task, input_data):
    """Return value automatically becomes the result."""
    task.set_stage("computing")
    return {"answer": heavy_computation(input_data)}

@marabunta_task
@auto_checkpoint(interval=100)  # Checkpoint every 100 yield points
def resumable_task(task, data):
    """Automatically resume from checkpoint."""
    state = task.load_checkpoint() or {"i": 0, "acc": 0}

    for i in range(state["i"], len(data)):
        state["acc"] += process(data[i])
        state["i"] = i + 1
        task.yield_point(state)  # Auto-checkpoints based on interval

    return state["acc"]
```

## Async Client

The `MarabuntaClient` provides async/await support for submitting and managing jobs.

### Basic Client Usage

```python
from marabunta_sdk import MarabuntaClient, MarabuntaClientConfig, JobRequest

# Create with URL
client = MarabuntaClient("http://localhost:8080")

# Create with config
config = MarabuntaClientConfig(
    base_url="http://localhost:8080",
    api_key="secret-key",
    timeout=TimeoutConfig(connect_timeout=5.0, total_timeout=300.0),
    retry=RetryConfig(max_retries=3),
)
client = MarabuntaClient.from_config(config)

# Use as async context manager
async with MarabuntaClient("http://localhost:8080") as client:
    job = JobRequest(task_type="my-task", input={"key": "value"})
    job_id = await client.submit(job)
```

### Cancellation Support

```python
from marabunta_sdk import MarabuntaClient, CancellationToken, JobRequest
import asyncio

async def run_with_cancel():
    client = MarabuntaClient("http://localhost:8080")
    cancel = CancellationToken()

    # Cancel after 10 seconds
    async def timeout():
        await asyncio.sleep(10)
        cancel.cancel()
    asyncio.create_task(timeout())

    job = JobRequest(task_type="long-task", input={})
    try:
        result = await client.submit_and_wait(job, cancel)
    except CancellationError:
        print("Operation was cancelled")

# Child tokens for hierarchical cancellation
parent = CancellationToken()
child = parent.child()
parent.cancel()  # Both parent and child are now cancelled

# Cancellation callbacks
token = CancellationToken()
token.on_cancel(lambda: print("Cancelled!"))
token.cancel()  # Prints "Cancelled!"
```

### Job Handle

```python
from marabunta_sdk import MarabuntaClient, JobHandle, JobRequest

client = MarabuntaClient("http://localhost:8080")
job = JobRequest(task_type="my-task", input={})
job_id = await client.submit(job)

handle = JobHandle(job_id, client)

# Check status
status = await handle.status()
print(f"Status: {status.status}, Progress: {status.progress}%")

# Wait for result
result = await handle.wait()

# Cancel the job
await handle.cancel()
```

## Retry Logic

Configure automatic retry with exponential backoff.

### RetryConfig

```python
from marabunta_sdk import RetryConfig, with_retry

# Default config
config = RetryConfig()  # 3 retries, 0.1s initial delay

# Custom config
config = RetryConfig(
    max_retries=5,
    initial_delay=0.5,
    max_delay=30.0,
    multiplier=2.0,
    jitter=True,
)

# Preset configurations
RetryConfig.no_retry()      # No retries
RetryConfig.quick()         # Fast retries, short delays
RetryConfig.persistent()    # Many retries, longer delays
```

### Using with_retry

```python
from marabunta_sdk import with_retry, RetryConfig

async def unreliable_operation():
    # May fail sometimes
    return await fetch_data()

config = RetryConfig(max_retries=5, initial_delay=0.1)
result = await with_retry(config, unreliable_operation)
```

## Timeout Configuration

Configure timeouts for different phases of operations.

```python
from marabunta_sdk import TimeoutConfig

# Default config
config = TimeoutConfig()  # 10s connect, 30s read, 300s total

# Custom config
config = TimeoutConfig(
    connect_timeout=5.0,
    read_timeout=60.0,
    total_timeout=600.0,
)

# Preset configurations
TimeoutConfig.no_timeout()     # No timeouts
TimeoutConfig.quick()          # Short timeouts for fast operations
TimeoutConfig.long_running()   # Extended timeouts for long jobs
```

## Job Builder

Build jobs with a fluent API for clean, readable configuration.

### Basic Builder

```python
from marabunta_sdk import JobBuilder, job

# Using JobBuilder
job_req = (
    JobBuilder("process-task")
    .with_input({"data": [1, 2, 3]})
    .with_priority(50)
    .with_timeout(3600)
    .build()
)

# Using convenience function
job_req = job("process-task", {"data": [1, 2, 3]}).build()
```

### Full Configuration

```python
from marabunta_sdk import JobBuilder

job_req = (
    JobBuilder("complex-task")
    # Input data
    .with_input({"file": "data.csv", "options": {"format": "csv"}})

    # Priority (-100 to 100)
    .with_priority(75)
    # Or use helpers:
    .high_priority()    # 100
    .normal_priority()  # 0
    .low_priority()     # -100

    # Timeout in seconds
    .with_timeout(3600)

    # Tags for organization
    .with_tag("production")
    .with_tag("urgent")
    .with_tags(["ml", "training"])

    # Metadata for tracking
    .with_metadata("user", "alice")
    .with_metadata("project", "demo")
    .with_metadata_dict({"version": "1.0", "env": "prod"})

    # Retry configuration
    .with_retries(5)
    .no_retries()  # Disable retries

    # Checkpointing
    .with_checkpoint(True)
    .no_checkpoint()  # Disable checkpointing

    # Resource requirements
    .with_min_cpu(4)
    .with_max_cpu(16)
    .with_min_memory_gb(8)
    .with_max_memory_gb(32)
    .with_gpu(2)

    # Dependencies
    .depends_on("job-123")
    .depends_on_all(["job-456", "job-789"])

    .build()
)
```

### Preview and Validation

```python
from marabunta_sdk import JobBuilder

builder = (
    JobBuilder("my-task")
    .with_input({"key": "value"})
    .with_tag("test")
)

# Preview without building
preview = builder.preview()
print(f"Task: {preview.task_type}")
print(f"Tags: {preview.tags}")
print(f"Resources: {preview.resources}")

# Validate configuration (raises on error)
builder.validate()
```

## Batch Job Submission

Submit multiple jobs efficiently with parallel execution.

### Basic Batch Submission

```python
from marabunta_sdk import MarabuntaClient, JobBuilder, BatchSubmitter, submit_batch

client = MarabuntaClient("http://localhost:8080")

# Create multiple jobs
jobs = [
    JobBuilder("process").with_input({"id": i}).build()
    for i in range(100)
]

# Simple batch submission
results = await submit_batch(client, jobs)
```

### BatchSubmitter with Options

```python
from marabunta_sdk import BatchSubmitter, BatchOptions

options = BatchOptions(
    max_concurrency=20,      # Max parallel submissions
    fail_fast=False,         # Continue on error
    continue_on_error=True,  # Don't abort on failures
    batch_timeout=3600.0,    # Overall timeout
    submission_delay=0.01,   # Delay between submissions
)

batch = BatchSubmitter(client, **options.__dict__)

# Submit all jobs
job_ids = await batch.submit_all(jobs)

# Submit and wait for results
results = await batch.submit_and_wait(jobs)
```

### Progress Tracking

```python
from marabunta_sdk import BatchSubmitter, BatchProgress

def on_progress(progress: BatchProgress):
    print(f"Progress: {progress.percentage:.1f}%")
    print(f"Submitted: {progress.submitted}/{progress.total}")
    print(f"Completed: {progress.completed}/{progress.total}")
    print(f"Failed: {progress.failed}")

batch = BatchSubmitter(client)
results = await batch.submit_and_wait_with_progress(jobs, on_progress)
```

### Cancellation

```python
from marabunta_sdk import BatchSubmitter, CancellationToken

cancel = CancellationToken()
batch = BatchSubmitter(client, cancel=cancel)

# Cancel from another task
cancel.cancel()

# Or cancel via batch
batch.cancel()
```

## Job Templates

Use pre-built templates for common distributed computing patterns.

### MapReduce Template

```python
from marabunta_sdk import MapReduceTemplate

template = (
    MapReduceTemplate("word-counter")
    .with_reducer("word-aggregator")
    .with_chunk_size(100)
    .with_map_concurrency(50)
    .with_map_timeout(300)
    .with_reduce_timeout(600)
    .with_tag("batch")
)
```

### Scatter-Gather Template

```python
from marabunta_sdk import ScatterGatherTemplate

template = (
    ScatterGatherTemplate("parallel-processor")
    .with_concurrency(20)
    .with_timeout(600)
    .with_fail_on_error(False)
)
```

### Pipeline Template

```python
from marabunta_sdk import PipelineTemplate, PipelineStage

# Simple pipeline
template = (
    PipelineTemplate()
    .then("extract")
    .then("transform")
    .then("load")
    .with_tag("etl")
)

# Pipeline with custom stages
stage = (
    PipelineStage(task_type="heavy-compute")
    .with_timeout(3600)
    .with_parallelism(4)
)
template = PipelineTemplate().add_stage(stage)
```

### Batch Processing Template

```python
from marabunta_sdk import BatchProcessingTemplate

template = (
    BatchProcessingTemplate("batch-processor")
    .with_batch_size(50)
    .with_concurrency(10)
    .with_timeout(300)
    .with_tag("batch")
)
```

### Fan-Out/Fan-In Template

```python
from marabunta_sdk import FanOutFanInTemplate

template = (
    FanOutFanInTemplate()
    .fan_out_to("analyzer-1")
    .fan_out_to("analyzer-2")
    .fan_out_to("analyzer-3")
    .aggregate_with("combiner")
    .with_timeout(600)
    .with_tag("analysis")
)
```

## Type-Safe Result Deserialization

Safely extract and deserialize job results with type checking.

### TypedResult

```python
from dataclasses import dataclass
from marabunta_sdk import TypedResult

@dataclass
class ComputeResult:
    value: int
    name: str

# From dict
data = {"value": 42, "name": "answer"}
result = TypedResult.from_dict(data, ComputeResult)
print(result.value.value)  # 42

# From JSON string
json_str = '{"value": 42, "name": "answer"}'
result = TypedResult.from_json(json_str, ComputeResult)
typed_value = result.into_value()

# Access raw data
result.get_field("extra_field")
result.has_field("optional_field")
```

### MultiTypeResult

Handle results with unknown or dynamic types.

```python
from marabunta_sdk import MultiTypeResult

result = MultiTypeResult.from_json(some_value)

# Check type
print(result.type_name)  # "string", "integer", "float", "boolean", "array", "object", "null"

# Extract with type checking
if result.type_name == "string":
    value = result.as_string()
elif result.type_name == "integer":
    value = result.as_int()
elif result.type_name == "object":
    value = result.as_dict()

# Type coercion (returns None if not compatible)
int_val = result.as_int()      # Returns None for non-numeric
float_val = result.as_float()  # Coerces int to float
```

### ValidatedResult

Handle results with validation errors.

```python
from marabunta_sdk import ValidatedResult

# Valid result
result = ValidatedResult.valid(42)
if result.is_valid:
    value = result.valid_value()

# Result with errors
result = ValidatedResult.with_errors(42, ["warning: data truncated"])
print(result.errors)  # ["warning: data truncated"]
print(result.valid_value())  # None (has errors)
print(result.value)  # 42 (raw value)
```

### ResultExtractor

Chain field extraction for nested data.

```python
from marabunta_sdk import ResultExtractor

data = {
    "user": {
        "profile": {
            "name": "Alice",
            "age": 30
        }
    }
}

extractor = ResultExtractor(data)

# Chained field extraction
name = extractor.field("user").field("profile").field("name").as_string()
age = extractor.field("user").field("profile").field("age").as_int()

# Type conversions (raise TypeMismatchError on failure)
extractor.as_string()  # str
extractor.as_int()     # int (also accepts float, truncates)
extractor.as_float()   # float
extractor.as_bool()    # bool
extractor.as_list()    # list
extractor.as_dict()    # dict

# Transformations
result = extractor.map(lambda d: d["user"]["name"].upper())

# Default values
value = extractor.or_default("fallback")
```

## Progress Helpers

```python
from marabunta_sdk import MarabuntaTask, progress_iter, MultiStageProgress

with MarabuntaTask() as task:
    # Automatic progress tracking
    for item in progress_iter(task, data, stage="processing"):
        process(item)

    # Multi-stage progress
    stages = MultiStageProgress(task, {
        "loading": 10,
        "processing": 70,
        "saving": 20,
    })

    with stages.stage("loading"):
        data = load()

    with stages.stage("processing"):
        for i, item in enumerate(data):
            process(item)
            stages.set_progress(i * 100 // len(data))

    with stages.stage("saving"):
        save(results)
```

## Checkpoint Management

```python
from marabunta_sdk import MarabuntaTask, ResumableState, resumable_loop

with MarabuntaTask() as task:
    # Using ResumableState helper
    state = ResumableState.load_or_create(task, {
        "index": 0,
        "partial_sum": 0,
    })

    for i in range(state["index"], 1000):
        state["partial_sum"] += compute(i)
        state["index"] = i + 1
        state.maybe_checkpoint()  # Checkpoints every 100 iterations

    # Or using resumable_loop
    for i, item in resumable_loop(task, data):
        process(item)
```

## Resource Hints

```python
with MarabuntaTask() as task:
    # Tell scheduler what resources we need
    task.hint_memory(2 * 1024**3)  # Need 2GB RAM
    task.hint_time(3600)           # About an hour
    task.hint_splittable(10, 100)  # Can split into 10-100 chunks
```

## Logging

```python
with MarabuntaTask() as task:
    task.log_debug("Starting computation")
    task.log_info("Processing {} items".format(len(data)))
    task.log_warn("Data quality issue detected")
    task.log_error("Failed to process item")
```

## Task Information

```python
with MarabuntaTask() as task:
    print(f"Task ID: {task.task_id}")
    print(f"Worker {task.worker_rank} of {task.worker_count}")
```

## API Reference

### MarabuntaTask

The main class for interacting with the Marabunta framework.

#### Context Manager

```python
with MarabuntaTask(auto_init=True) as task:
    # task is automatically initialized and cleaned up
    pass
```

#### Progress Methods

- `set_progress(percent: int)` - Set progress (0-100)
- `set_stage(name: str)` - Set current stage name
- `set_message(msg: str)` - Set status message
- `increment_progress(delta: int)` - Increment progress by delta

#### Yield and Checkpointing

- `should_yield() -> bool` - Check if should pause/abort
- `check_yield()` - Check and raise exception if should yield
- `yield_point(state=None)` - Yield point with optional checkpoint
- `save_checkpoint(data: Any)` - Save checkpoint (pickled)
- `load_checkpoint() -> Any` - Load checkpoint (unpickled)
- `has_checkpoint() -> bool` - Check if checkpoint exists

#### Results

- `set_result(data: Any)` - Set result (JSON-serialized)
- `append_result(data: bytes)` - Append raw bytes to result

#### Resource Hints

- `hint_memory(bytes: int)` - Hint expected memory usage
- `hint_time(seconds: int)` - Hint expected execution time
- `hint_splittable(min: int, max: int)` - Hint task can be split

#### Logging

- `log_debug(msg: str)` - Log debug message
- `log_info(msg: str)` - Log info message
- `log_warn(msg: str)` - Log warning message
- `log_error(msg: str)` - Log error message

#### Properties

- `task_id: str` - The task's unique identifier
- `worker_rank: int` - This worker's rank (0 to count-1)
- `worker_count: int` - Total number of workers

### MarabuntaClient

Async client for job submission and management.

- `submit(job, cancel=None) -> str` - Submit a job
- `get_status(job_id) -> JobResponse` - Get job status
- `wait_for_result(job_id, cancel=None)` - Wait for result
- `submit_and_wait(job, cancel=None)` - Submit and wait
- `cancel_job(job_id)` - Cancel a job

### JobBuilder

Fluent API for building job requests.

- `with_input(data)` - Set input data
- `with_priority(n)` / `high_priority()` / `normal_priority()` / `low_priority()`
- `with_timeout(secs)` - Set timeout
- `with_tag(tag)` / `with_tags(tags)` - Add tags
- `with_metadata(key, value)` / `with_metadata_dict(dict)` - Add metadata
- `with_retries(n)` / `no_retries()` - Configure retries
- `with_checkpoint(bool)` / `no_checkpoint()` - Configure checkpointing
- `with_min_cpu(n)` / `with_max_cpu(n)` - CPU requirements
- `with_min_memory_gb(n)` / `with_max_memory_gb(n)` - Memory requirements
- `with_gpu(n)` - GPU requirements
- `depends_on(job_id)` / `depends_on_all(job_ids)` - Set dependencies
- `preview()` - Get preview without building
- `validate()` - Validate configuration
- `build()` - Build JobRequest

### Decorators

- `@marabunta_task` - Wrap function as a Marabunta task
- `@auto_checkpoint(interval=100)` - Add automatic checkpointing
- `@checkpoint` - Load checkpoint as second argument

### Progress Utilities

- `ProgressTracker(task, total)` - Track progress through items
- `MultiStageProgress(task, stages)` - Track multi-stage progress
- `progress_iter(task, iterable)` - Iterate with progress
- `chunked_progress(task, iterable, chunk_size)` - Iterate in chunks

### Checkpoint Utilities

- `CheckpointManager(task, interval)` - Manage checkpoints
- `ResumableState.load_or_create(task, initial)` - Resumable state dict
- `resumable_loop(task, iterable)` - Resumable loop iterator

## Testing

The SDK includes a mock implementation for testing without the native library:

```python
from marabunta_sdk import MarabuntaTask, enable_mock_mode

# Enable mock mode globally
enable_mock_mode()

# Or create a mock task directly
with MarabuntaTask(use_mock=True) as task:
    task.set_progress(50)
    task.save_checkpoint({"state": "test"})

    # Access mock internals for assertions
    mock = task._get_mock_context()
    assert mock._progress == 50
```

Run the test suite:

```bash
cd sdk/python
pip install -e ".[dev]"
pytest
```

## Thread Safety

MarabuntaTask is thread-safe. All methods use internal locking and the underlying C library is designed for concurrent access.

## Error Handling

```python
from marabunta_sdk import (
    MarabuntaTask,
    MarabuntaTaskError,
    TaskAbortedError,
    TaskPausedError,
    CancellationError,
    MaxRetriesExceededError,
    BuilderError,
    ValidationError,
    DeserializationError,
)

# Task errors
with MarabuntaTask() as task:
    try:
        for item in data:
            task.check_yield()  # May raise
            process(item)
    except TaskAbortedError:
        # Task was aborted by scheduler
        task.log_error("Task aborted")
    except TaskPausedError:
        # Task should pause - state already checkpointed
        pass
    except MarabuntaTaskError as e:
        # Other SDK errors
        task.log_error(f"SDK error {e.code}: {e.message}")

# Client errors
try:
    result = await client.submit_and_wait(job, cancel_token)
except CancellationError:
    print("Operation cancelled")
except MaxRetriesExceededError as e:
    print(f"Failed after {e.attempts} attempts")

# Builder errors
try:
    JobBuilder("task").with_tag("invalid tag!").validate()
except ValidationError as e:
    print(f"Validation failed: {e}")

# Result errors
try:
    result = TypedResult.from_json("not valid json", dict)
except DeserializationError:
    print("Failed to parse JSON")
```

## License

MIT License - see LICENSE file for details.
