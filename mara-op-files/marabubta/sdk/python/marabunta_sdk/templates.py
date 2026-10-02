# Marabunta - Licensed under the MIT License.
"""
Job Templates for Common Patterns.

Provides predefined templates for common distributed computing patterns:
- MapReduce operations
- Scatter-gather patterns
- Pipeline processing
- Batch processing
- Fan-out/fan-in patterns

Example:
    from marabunta_sdk import MarabuntaClient, MapReduceTemplate

    async def main():
        client = MarabuntaClient("http://localhost:8080")

        data = list(range(1000))
        mapreduce = (MapReduceTemplate("sum-task")
                     .with_chunk_size(100)
                     .with_reducer("aggregate-task"))

        result = await mapreduce.execute(client, data)
        print(f"Total: {result}")
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar

from .client import MarabuntaClient, ClientError, CancellationToken
from .builder import JobBuilder
from .batch import BatchSubmitter

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class MapReduceTemplate:
    """
    Template for MapReduce operations.

    Splits input data into chunks, processes each chunk with a mapper,
    then combines results with a reducer.

    Example:
        template = (MapReduceTemplate("map-task")
                    .with_reducer("reduce-task")
                    .with_chunk_size(100))
        result = await template.execute(client, data)
    """

    mapper_task: str
    reducer_task: Optional[str] = None
    chunk_size: int = 100
    map_concurrency: int = 50
    map_timeout: Optional[int] = None
    reduce_timeout: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)

    def with_reducer(self, reducer_task: str) -> "MapReduceTemplate":
        """Set the reducer task type."""
        self.reducer_task = reducer_task
        return self

    def with_chunk_size(self, size: int) -> "MapReduceTemplate":
        """Set the chunk size."""
        self.chunk_size = max(1, size)
        return self

    def with_map_concurrency(self, concurrency: int) -> "MapReduceTemplate":
        """Set map concurrency."""
        self.map_concurrency = max(1, concurrency)
        return self

    def with_map_timeout(self, timeout: int) -> "MapReduceTemplate":
        """Set map timeout."""
        self.map_timeout = timeout
        return self

    def with_reduce_timeout(self, timeout: int) -> "MapReduceTemplate":
        """Set reduce timeout."""
        self.reduce_timeout = timeout
        return self

    def with_tag(self, tag: str) -> "MapReduceTemplate":
        """Add a tag."""
        self.tags.append(tag)
        return self

    def with_metadata(self, key: str, value: str) -> "MapReduceTemplate":
        """Add metadata."""
        self.metadata[key] = value
        return self

    async def execute(
        self,
        client: MarabuntaClient,
        input_data: List[Any],
        cancel: Optional[CancellationToken] = None,
    ) -> Any:
        """
        Execute the MapReduce operation.

        Args:
            client: The Marabunta client
            input_data: List of input items to process
            cancel: Optional cancellation token

        Returns:
            The reduced result
        """
        cancel = cancel or CancellationToken()

        # Split input into chunks
        chunks = [
            input_data[i : i + self.chunk_size]
            for i in range(0, len(input_data), self.chunk_size)
        ]

        # Create map jobs
        map_jobs = []
        for i, chunk in enumerate(chunks):
            builder = (
                JobBuilder(self.mapper_task)
                .with_input(chunk)
                .with_metadata("chunk_index", str(i))
                .with_metadata("operation", "map")
            )

            if self.map_timeout:
                builder = builder.with_timeout(self.map_timeout)

            for tag in self.tags:
                builder = builder.with_tag(tag)

            for key, value in self.metadata.items():
                builder = builder.with_metadata(key, value)

            map_jobs.append(builder.build())

        # Execute map phase
        batch = BatchSubmitter(client, max_concurrency=self.map_concurrency)
        map_results = await batch.submit_and_wait(map_jobs)

        # Collect successful map results
        intermediate_results = []
        for result in map_results:
            if not result.success:
                raise ClientError(f"Map phase failed: {result.error}")
            intermediate_results.append(result.value)

        # If no reducer, return intermediate results
        if self.reducer_task is None:
            return intermediate_results

        # Execute reduce phase
        reduce_builder = (
            JobBuilder(self.reducer_task)
            .with_input(intermediate_results)
            .with_metadata("operation", "reduce")
        )

        if self.reduce_timeout:
            reduce_builder = reduce_builder.with_timeout(self.reduce_timeout)

        for tag in self.tags:
            reduce_builder = reduce_builder.with_tag(tag)

        reduce_job = reduce_builder.build()
        return await client.submit_and_wait(reduce_job, cancel=cancel)


@dataclass
class ScatterGatherTemplate:
    """
    Template for Scatter-Gather pattern.

    Sends the same task to multiple workers with different parameters,
    then gathers all results.

    Example:
        template = ScatterGatherTemplate("worker-task").with_concurrency(20)
        results = await template.execute(client, inputs)
    """

    task_type: str
    concurrency: int = 50
    timeout: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    fail_on_error: bool = False

    def with_concurrency(self, concurrency: int) -> "ScatterGatherTemplate":
        """Set concurrency."""
        self.concurrency = max(1, concurrency)
        return self

    def with_timeout(self, timeout: int) -> "ScatterGatherTemplate":
        """Set timeout."""
        self.timeout = timeout
        return self

    def with_tag(self, tag: str) -> "ScatterGatherTemplate":
        """Add a tag."""
        self.tags.append(tag)
        return self

    def with_fail_on_error(self, fail: bool = True) -> "ScatterGatherTemplate":
        """Set fail on error behavior."""
        self.fail_on_error = fail
        return self

    async def execute(
        self,
        client: MarabuntaClient,
        inputs: List[Any],
        cancel: Optional[CancellationToken] = None,
    ) -> List[Any]:
        """
        Execute the scatter-gather operation.

        Args:
            client: The Marabunta client
            inputs: List of inputs (one per worker)
            cancel: Optional cancellation token

        Returns:
            List of results
        """
        cancel = cancel or CancellationToken()

        # Create jobs for each input
        jobs = []
        for i, input_data in enumerate(inputs):
            builder = (
                JobBuilder(self.task_type)
                .with_input(input_data)
                .with_metadata("scatter_index", str(i))
            )

            if self.timeout:
                builder = builder.with_timeout(self.timeout)

            for tag in self.tags:
                builder = builder.with_tag(tag)

            jobs.append(builder.build())

        # Execute all jobs
        batch = BatchSubmitter(
            client,
            max_concurrency=self.concurrency,
            fail_fast=self.fail_on_error,
        )
        results = await batch.submit_and_wait(jobs)

        return [r.value if r.success else None for r in results]


@dataclass
class PipelineStage:
    """A single stage in a pipeline."""

    task_type: str
    timeout: Optional[int] = None
    parallel: bool = False
    parallelism: int = 1

    def with_timeout(self, timeout: int) -> "PipelineStage":
        """Set timeout."""
        self.timeout = timeout
        return self

    def with_parallelism(self, workers: int) -> "PipelineStage":
        """Enable parallelism."""
        self.parallel = True
        self.parallelism = max(1, workers)
        return self


@dataclass
class PipelineTemplate:
    """
    Template for Pipeline processing.

    Chains multiple tasks in sequence, where the output of each task
    becomes the input of the next.

    Example:
        template = (PipelineTemplate()
                    .then("stage-1")
                    .then("stage-2")
                    .then("stage-3"))
        result = await template.execute(client, initial_input)
    """

    stages: List[PipelineStage] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    def add_stage(self, stage: PipelineStage) -> "PipelineTemplate":
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self

    def then(self, task_type: str) -> "PipelineTemplate":
        """Add a simple stage by task type."""
        self.stages.append(PipelineStage(task_type=task_type))
        return self

    def with_tag(self, tag: str) -> "PipelineTemplate":
        """Add a tag."""
        self.tags.append(tag)
        return self

    async def execute(
        self,
        client: MarabuntaClient,
        input_data: Any,
        cancel: Optional[CancellationToken] = None,
    ) -> Any:
        """
        Execute the pipeline.

        Args:
            client: The Marabunta client
            input_data: Initial input data
            cancel: Optional cancellation token

        Returns:
            Final output data
        """
        cancel = cancel or CancellationToken()

        if not self.stages:
            raise ClientError("Pipeline has no stages")

        current_data = input_data

        for i, stage in enumerate(self.stages):
            builder = (
                JobBuilder(stage.task_type)
                .with_input(current_data)
                .with_metadata("pipeline_stage", str(i))
                .with_metadata("pipeline_stage_name", stage.task_type)
            )

            if stage.timeout:
                builder = builder.with_timeout(stage.timeout)

            for tag in self.tags:
                builder = builder.with_tag(tag)

            job = builder.build()
            current_data = await client.submit_and_wait(job, cancel=cancel)

        return current_data


@dataclass
class BatchProcessingTemplate:
    """
    Template for batch processing with automatic chunking.

    Example:
        template = (BatchProcessingTemplate("processor")
                    .with_batch_size(50)
                    .with_concurrency(5))
        results = await template.process(client, items)
    """

    task_type: str
    batch_size: int = 100
    concurrency: int = 10
    timeout: Optional[int] = None
    tags: List[str] = field(default_factory=list)

    def with_batch_size(self, size: int) -> "BatchProcessingTemplate":
        """Set batch size."""
        self.batch_size = max(1, size)
        return self

    def with_concurrency(self, concurrency: int) -> "BatchProcessingTemplate":
        """Set concurrency."""
        self.concurrency = max(1, concurrency)
        return self

    def with_timeout(self, timeout: int) -> "BatchProcessingTemplate":
        """Set timeout."""
        self.timeout = timeout
        return self

    def with_tag(self, tag: str) -> "BatchProcessingTemplate":
        """Add a tag."""
        self.tags.append(tag)
        return self

    async def process(
        self,
        client: MarabuntaClient,
        items: List[Any],
        cancel: Optional[CancellationToken] = None,
    ) -> List[Any]:
        """
        Process all items in batches.

        Args:
            client: The Marabunta client
            items: Items to process
            cancel: Optional cancellation token

        Returns:
            Processed results (flattened)
        """
        cancel = cancel or CancellationToken()

        # Split into batches
        batches = [
            items[i : i + self.batch_size]
            for i in range(0, len(items), self.batch_size)
        ]

        # Create jobs
        jobs = []
        for i, batch in enumerate(batches):
            builder = (
                JobBuilder(self.task_type)
                .with_input(batch)
                .with_metadata("batch_index", str(i))
            )

            if self.timeout:
                builder = builder.with_timeout(self.timeout)

            for tag in self.tags:
                builder = builder.with_tag(tag)

            jobs.append(builder.build())

        # Execute batches
        batch_submitter = BatchSubmitter(client, max_concurrency=self.concurrency)
        results = await batch_submitter.submit_and_wait(jobs)

        # Flatten results
        all_results = []
        for result in results:
            if not result.success:
                raise ClientError(f"Batch processing failed: {result.error}")
            if isinstance(result.value, list):
                all_results.extend(result.value)
            else:
                all_results.append(result.value)

        return all_results


@dataclass
class FanOutFanInTemplate:
    """
    Template for fan-out/fan-in pattern.

    Sends a single input to multiple different tasks, then aggregates results.

    Example:
        template = (FanOutFanInTemplate()
                    .fan_out_to("analyzer-1")
                    .fan_out_to("analyzer-2")
                    .fan_out_to("analyzer-3")
                    .aggregate_with("aggregator"))
        result = await template.execute(client, input_data)
    """

    fan_out_tasks: List[str] = field(default_factory=list)
    aggregator_task: Optional[str] = None
    timeout: Optional[int] = None
    tags: List[str] = field(default_factory=list)

    def fan_out_to(self, task_type: str) -> "FanOutFanInTemplate":
        """Add a fan-out task."""
        self.fan_out_tasks.append(task_type)
        return self

    def aggregate_with(self, task_type: str) -> "FanOutFanInTemplate":
        """Set the aggregator task."""
        self.aggregator_task = task_type
        return self

    def with_timeout(self, timeout: int) -> "FanOutFanInTemplate":
        """Set timeout."""
        self.timeout = timeout
        return self

    def with_tag(self, tag: str) -> "FanOutFanInTemplate":
        """Add a tag."""
        self.tags.append(tag)
        return self

    async def execute(
        self,
        client: MarabuntaClient,
        input_data: Any,
        cancel: Optional[CancellationToken] = None,
    ) -> Any:
        """
        Execute the fan-out/fan-in operation.

        Args:
            client: The Marabunta client
            input_data: Input data to send to all tasks
            cancel: Optional cancellation token

        Returns:
            Aggregated result
        """
        cancel = cancel or CancellationToken()

        if not self.fan_out_tasks:
            raise ClientError("No fan-out tasks specified")

        # Create jobs for each fan-out task
        jobs = []
        for i, task_type in enumerate(self.fan_out_tasks):
            builder = (
                JobBuilder(task_type)
                .with_input(input_data)
                .with_metadata("fan_out_index", str(i))
                .with_metadata("fan_out_task", task_type)
            )

            if self.timeout:
                builder = builder.with_timeout(self.timeout)

            for tag in self.tags:
                builder = builder.with_tag(tag)

            jobs.append(builder.build())

        # Execute fan-out phase
        batch = BatchSubmitter(client)
        results = await batch.submit_and_wait(jobs)

        # Collect results
        fan_out_results = []
        for result in results:
            if not result.success:
                raise ClientError(f"Fan-out phase failed: {result.error}")
            fan_out_results.append(result.value)

        # If no aggregator, return results directly
        if self.aggregator_task is None:
            return fan_out_results

        # Execute aggregator
        aggregator_job = (
            JobBuilder(self.aggregator_task)
            .with_input(fan_out_results)
            .with_metadata("operation", "aggregate")
            .build()
        )

        return await client.submit_and_wait(aggregator_job, cancel=cancel)
