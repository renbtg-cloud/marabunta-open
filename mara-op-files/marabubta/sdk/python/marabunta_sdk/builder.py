# Marabunta - Licensed under the MIT License.
"""
Job Builder Pattern for Fluent API.

Provides a fluent builder pattern for constructing job requests
with type safety and validation.

Example:
    from marabunta_sdk import JobBuilder

    job = (JobBuilder("data-processing")
           .with_input({"file": "data.csv"})
           .with_priority(10)
           .with_timeout(3600)
           .with_tag("production")
           .with_tag("high-priority")
           .with_metadata("user", "john")
           .with_retries(5)
           .with_checkpoint(True)
           .build())
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypeVar, Union

from .client import JobRequest


class BuilderError(Exception):
    """Error raised when building a job fails."""

    pass


class MissingTaskTypeError(BuilderError):
    """Task type is required."""

    def __init__(self):
        super().__init__("Task type is required")


class InvalidTagError(BuilderError):
    """Invalid tag."""

    def __init__(self, message: str):
        super().__init__(f"Invalid tag: {message}")


class ValidationError(BuilderError):
    """Validation failed."""

    def __init__(self, message: str):
        super().__init__(f"Validation failed: {message}")


@dataclass
class ResourceRequirements:
    """Resource requirements for a job."""

    min_cpu_cores: Optional[int] = None
    max_cpu_cores: Optional[int] = None
    min_memory_bytes: Optional[int] = None
    max_memory_bytes: Optional[int] = None
    min_disk_bytes: Optional[int] = None
    gpu_count: Optional[int] = None
    gpu_min_memory: Optional[int] = None
    gpu_types: List[str] = field(default_factory=list)


@dataclass
class JobPreview:
    """Preview of a job configuration."""

    task_type: Optional[str]
    input: Optional[Any]
    priority: int
    timeout_secs: Optional[int]
    tags: List[str]
    metadata: Dict[str, str]
    max_retries: int
    checkpoint_enabled: bool
    dependencies: List[str]
    resources: ResourceRequirements


class JobBuilder:
    """
    Builder for creating job requests with a fluent API.

    Example:
        job = (JobBuilder("data-processing")
               .with_input({"data": [1, 2, 3]})
               .with_priority(10)
               .with_timeout(3600)
               .with_tag("production")
               .build())
    """

    # Pattern for valid tags
    TAG_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")

    def __init__(self, task_type: Optional[str] = None):
        """
        Create a new job builder.

        Args:
            task_type: Optional task type (can be set later with task_type())
        """
        self._task_type: Optional[str] = task_type
        self._input: Optional[Any] = None
        self._priority: int = 0
        self._timeout_secs: Optional[int] = None
        self._tags: List[str] = []
        self._metadata: Dict[str, str] = {}
        self._max_retries: int = 3
        self._checkpoint_enabled: bool = True
        self._dependencies: List[str] = []
        self._resources: ResourceRequirements = ResourceRequirements()

    def task_type(self, task_type: str) -> "JobBuilder":
        """Set the task type."""
        self._task_type = task_type
        return self

    def with_input(self, input_data: Any) -> "JobBuilder":
        """Set the input data."""
        self._input = input_data
        return self

    def with_priority(self, priority: int) -> "JobBuilder":
        """Set the job priority (higher = more important)."""
        self._priority = priority
        return self

    def high_priority(self) -> "JobBuilder":
        """Set high priority (100)."""
        return self.with_priority(100)

    def normal_priority(self) -> "JobBuilder":
        """Set normal priority (0)."""
        return self.with_priority(0)

    def low_priority(self) -> "JobBuilder":
        """Set low priority (-100)."""
        return self.with_priority(-100)

    def with_timeout(self, seconds: int) -> "JobBuilder":
        """Set the job timeout in seconds."""
        self._timeout_secs = seconds
        return self

    def with_tag(self, tag: str) -> "JobBuilder":
        """Add a tag to the job."""
        self._tags.append(tag)
        return self

    def with_tags(self, tags: List[str]) -> "JobBuilder":
        """Add multiple tags to the job."""
        self._tags.extend(tags)
        return self

    def with_metadata(self, key: str, value: str) -> "JobBuilder":
        """Add metadata to the job."""
        self._metadata[key] = value
        return self

    def with_metadata_dict(self, metadata: Dict[str, str]) -> "JobBuilder":
        """Add multiple metadata entries."""
        self._metadata.update(metadata)
        return self

    def with_retries(self, max_retries: int) -> "JobBuilder":
        """Set the maximum number of retries."""
        self._max_retries = max_retries
        return self

    def no_retries(self) -> "JobBuilder":
        """Disable retries."""
        return self.with_retries(0)

    def with_checkpoint(self, enabled: bool = True) -> "JobBuilder":
        """Enable or disable checkpointing."""
        self._checkpoint_enabled = enabled
        return self

    def no_checkpoint(self) -> "JobBuilder":
        """Disable checkpointing."""
        return self.with_checkpoint(False)

    def depends_on(self, job_id: str) -> "JobBuilder":
        """Add a job dependency."""
        self._dependencies.append(job_id)
        return self

    def depends_on_all(self, job_ids: List[str]) -> "JobBuilder":
        """Add multiple job dependencies."""
        self._dependencies.extend(job_ids)
        return self

    def with_min_cpu(self, cores: int) -> "JobBuilder":
        """Set minimum CPU cores."""
        self._resources.min_cpu_cores = cores
        return self

    def with_max_cpu(self, cores: int) -> "JobBuilder":
        """Set maximum CPU cores."""
        self._resources.max_cpu_cores = cores
        return self

    def with_min_memory(self, bytes_: int) -> "JobBuilder":
        """Set minimum memory in bytes."""
        self._resources.min_memory_bytes = bytes_
        return self

    def with_min_memory_gb(self, gb: int) -> "JobBuilder":
        """Set minimum memory in gigabytes."""
        return self.with_min_memory(gb * 1024 * 1024 * 1024)

    def with_max_memory(self, bytes_: int) -> "JobBuilder":
        """Set maximum memory in bytes."""
        self._resources.max_memory_bytes = bytes_
        return self

    def with_min_disk(self, bytes_: int) -> "JobBuilder":
        """Set minimum disk space in bytes."""
        self._resources.min_disk_bytes = bytes_
        return self

    def with_gpu(self, count: int = 1) -> "JobBuilder":
        """Set GPU requirements."""
        self._resources.gpu_count = count
        return self

    def validate(self) -> None:
        """
        Validate the builder configuration.

        Raises:
            MissingTaskTypeError: If task type is not set
            InvalidTagError: If a tag is invalid
            ValidationError: If validation fails
        """
        if not self._task_type:
            raise MissingTaskTypeError()

        # Validate tags
        for tag in self._tags:
            if not tag:
                raise InvalidTagError("Tag cannot be empty")
            if not self.TAG_PATTERN.match(tag):
                raise InvalidTagError(
                    f"Tag '{tag}' contains invalid characters. "
                    "Only alphanumeric characters, hyphens, and underscores are allowed."
                )

        # Validate resource requirements
        if (
            self._resources.min_cpu_cores is not None
            and self._resources.max_cpu_cores is not None
            and self._resources.min_cpu_cores > self._resources.max_cpu_cores
        ):
            raise ValidationError(
                "min_cpu_cores cannot be greater than max_cpu_cores"
            )

        if (
            self._resources.min_memory_bytes is not None
            and self._resources.max_memory_bytes is not None
            and self._resources.min_memory_bytes > self._resources.max_memory_bytes
        ):
            raise ValidationError(
                "min_memory_bytes cannot be greater than max_memory_bytes"
            )

    def build(self) -> JobRequest:
        """
        Build the job request.

        Returns:
            The job request

        Raises:
            BuilderError: If validation fails
        """
        self.validate()

        return JobRequest(
            task_type=self._task_type,  # type: ignore
            input=self._input if self._input is not None else {},
            priority=self._priority,
            timeout_secs=self._timeout_secs,
            tags=self._tags.copy(),
            metadata=self._metadata.copy(),
            max_retries=self._max_retries,
            checkpoint_enabled=self._checkpoint_enabled,
        )

    def preview(self) -> JobPreview:
        """Get a preview of the job configuration."""
        return JobPreview(
            task_type=self._task_type,
            input=self._input,
            priority=self._priority,
            timeout_secs=self._timeout_secs,
            tags=self._tags.copy(),
            metadata=self._metadata.copy(),
            max_retries=self._max_retries,
            checkpoint_enabled=self._checkpoint_enabled,
            dependencies=self._dependencies.copy(),
            resources=self._resources,
        )


def job(task_type: str, input_data: Any = None) -> JobBuilder:
    """
    Convenience function to create a job builder.

    Args:
        task_type: The task type
        input_data: Optional input data

    Returns:
        A JobBuilder instance
    """
    builder = JobBuilder(task_type)
    if input_data is not None:
        builder = builder.with_input(input_data)
    return builder
