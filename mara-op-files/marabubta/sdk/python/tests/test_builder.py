# Marabunta - Licensed under the MIT License.
"""
Tests for the JobBuilder class.
"""

import pytest
from marabunta_sdk import (
    JobBuilder,
    BuilderError,
    MissingTaskTypeError,
    InvalidTagError,
    ValidationError,
    ResourceRequirements,
    job,
)


class TestJobBuilderBasic:
    """Test basic JobBuilder functionality."""

    def test_create_builder(self):
        """Test creating a basic builder."""
        builder = JobBuilder("test-task")
        job_req = builder.build()
        assert job_req.task_type == "test-task"

    def test_with_input(self):
        """Test setting input."""
        job_req = (
            JobBuilder("test-task")
            .with_input({"key": "value", "number": 42})
            .build()
        )
        assert job_req.input == {"key": "value", "number": 42}

    def test_default_input(self):
        """Test default input is empty dict."""
        job_req = JobBuilder("test-task").build()
        assert job_req.input == {}

    def test_with_priority(self):
        """Test setting priority."""
        job_req = JobBuilder("test-task").with_priority(50).build()
        assert job_req.priority == 50

    def test_high_priority(self):
        """Test high priority helper."""
        job_req = JobBuilder("test-task").high_priority().build()
        assert job_req.priority == 100

    def test_normal_priority(self):
        """Test normal priority helper."""
        job_req = JobBuilder("test-task").normal_priority().build()
        assert job_req.priority == 0

    def test_low_priority(self):
        """Test low priority helper."""
        job_req = JobBuilder("test-task").low_priority().build()
        assert job_req.priority == -100


class TestJobBuilderTimeout:
    """Test timeout configuration."""

    def test_with_timeout(self):
        """Test setting timeout."""
        job_req = JobBuilder("test-task").with_timeout(3600).build()
        assert job_req.timeout_secs == 3600

    def test_no_timeout(self):
        """Test no timeout by default."""
        job_req = JobBuilder("test-task").build()
        assert job_req.timeout_secs is None


class TestJobBuilderTags:
    """Test tag handling."""

    def test_single_tag(self):
        """Test adding a single tag."""
        job_req = JobBuilder("test-task").with_tag("production").build()
        assert job_req.tags == ["production"]

    def test_multiple_tags(self):
        """Test adding multiple tags."""
        job_req = (
            JobBuilder("test-task")
            .with_tag("production")
            .with_tag("high-priority")
            .with_tag("urgent")
            .build()
        )
        assert job_req.tags == ["production", "high-priority", "urgent"]

    def test_with_tags_list(self):
        """Test adding tags from a list."""
        job_req = (
            JobBuilder("test-task")
            .with_tags(["tag1", "tag2", "tag3"])
            .build()
        )
        assert job_req.tags == ["tag1", "tag2", "tag3"]

    def test_valid_tag_formats(self):
        """Test valid tag formats."""
        job_req = (
            JobBuilder("test-task")
            .with_tag("simple")
            .with_tag("with-hyphen")
            .with_tag("with_underscore")
            .with_tag("mixed-tag_123")
            .build()
        )
        assert len(job_req.tags) == 4

    def test_invalid_tag_empty(self):
        """Test that empty tags are rejected."""
        with pytest.raises(InvalidTagError):
            JobBuilder("test-task").with_tag("").validate()

    def test_invalid_tag_special_chars(self):
        """Test that tags with special chars are rejected."""
        with pytest.raises(InvalidTagError):
            JobBuilder("test-task").with_tag("invalid tag!").validate()

    def test_invalid_tag_spaces(self):
        """Test that tags with spaces are rejected."""
        with pytest.raises(InvalidTagError):
            JobBuilder("test-task").with_tag("has space").validate()


class TestJobBuilderMetadata:
    """Test metadata handling."""

    def test_single_metadata(self):
        """Test adding single metadata."""
        job_req = (
            JobBuilder("test-task")
            .with_metadata("user", "alice")
            .build()
        )
        assert job_req.metadata == {"user": "alice"}

    def test_multiple_metadata(self):
        """Test adding multiple metadata."""
        job_req = (
            JobBuilder("test-task")
            .with_metadata("user", "alice")
            .with_metadata("project", "demo")
            .with_metadata("env", "production")
            .build()
        )
        assert job_req.metadata == {
            "user": "alice",
            "project": "demo",
            "env": "production",
        }

    def test_with_metadata_dict(self):
        """Test adding metadata from dict."""
        job_req = (
            JobBuilder("test-task")
            .with_metadata_dict({"key1": "value1", "key2": "value2"})
            .build()
        )
        assert job_req.metadata == {"key1": "value1", "key2": "value2"}


class TestJobBuilderRetries:
    """Test retry configuration."""

    def test_with_retries(self):
        """Test setting retries."""
        job_req = JobBuilder("test-task").with_retries(5).build()
        assert job_req.max_retries == 5

    def test_no_retries(self):
        """Test disabling retries."""
        job_req = JobBuilder("test-task").no_retries().build()
        assert job_req.max_retries == 0

    def test_default_retries(self):
        """Test default retries."""
        job_req = JobBuilder("test-task").build()
        assert job_req.max_retries == 3


class TestJobBuilderCheckpoint:
    """Test checkpoint configuration."""

    def test_with_checkpoint_enabled(self):
        """Test enabling checkpoint."""
        job_req = JobBuilder("test-task").with_checkpoint(True).build()
        assert job_req.checkpoint_enabled is True

    def test_with_checkpoint_disabled(self):
        """Test disabling checkpoint."""
        job_req = JobBuilder("test-task").with_checkpoint(False).build()
        assert job_req.checkpoint_enabled is False

    def test_no_checkpoint(self):
        """Test no_checkpoint helper."""
        job_req = JobBuilder("test-task").no_checkpoint().build()
        assert job_req.checkpoint_enabled is False

    def test_default_checkpoint(self):
        """Test default checkpoint is enabled."""
        job_req = JobBuilder("test-task").build()
        assert job_req.checkpoint_enabled is True


class TestJobBuilderDependencies:
    """Test job dependencies."""

    def test_depends_on(self):
        """Test adding a dependency."""
        builder = JobBuilder("test-task").depends_on("job-1")
        preview = builder.preview()
        assert preview.dependencies == ["job-1"]

    def test_depends_on_all(self):
        """Test adding multiple dependencies."""
        builder = (
            JobBuilder("test-task")
            .depends_on("job-1")
            .depends_on_all(["job-2", "job-3"])
        )
        preview = builder.preview()
        assert preview.dependencies == ["job-1", "job-2", "job-3"]


class TestJobBuilderResources:
    """Test resource requirements."""

    def test_with_min_cpu(self):
        """Test setting min CPU."""
        builder = JobBuilder("test-task").with_min_cpu(4)
        preview = builder.preview()
        assert preview.resources.min_cpu_cores == 4

    def test_with_max_cpu(self):
        """Test setting max CPU."""
        builder = JobBuilder("test-task").with_max_cpu(16)
        preview = builder.preview()
        assert preview.resources.max_cpu_cores == 16

    def test_with_min_memory(self):
        """Test setting min memory."""
        builder = JobBuilder("test-task").with_min_memory(1024 * 1024 * 1024)
        preview = builder.preview()
        assert preview.resources.min_memory_bytes == 1024 * 1024 * 1024

    def test_with_min_memory_gb(self):
        """Test setting min memory in GB."""
        builder = JobBuilder("test-task").with_min_memory_gb(8)
        preview = builder.preview()
        assert preview.resources.min_memory_bytes == 8 * 1024 * 1024 * 1024

    def test_with_gpu(self):
        """Test setting GPU requirements."""
        builder = JobBuilder("test-task").with_gpu(2)
        preview = builder.preview()
        assert preview.resources.gpu_count == 2

    def test_invalid_cpu_range(self):
        """Test that invalid CPU range is rejected."""
        with pytest.raises(ValidationError):
            (
                JobBuilder("test-task")
                .with_min_cpu(16)
                .with_max_cpu(4)
                .validate()
            )

    def test_invalid_memory_range(self):
        """Test that invalid memory range is rejected."""
        with pytest.raises(ValidationError):
            (
                JobBuilder("test-task")
                .with_min_memory(1000)
                .with_max_memory(100)
                .validate()
            )


class TestJobBuilderValidation:
    """Test validation."""

    def test_missing_task_type(self):
        """Test that missing task type raises error."""
        with pytest.raises(MissingTaskTypeError):
            JobBuilder().build()

    def test_set_task_type_later(self):
        """Test setting task type after creation."""
        job_req = JobBuilder().task_type("test-task").build()
        assert job_req.task_type == "test-task"


class TestJobBuilderPreview:
    """Test preview functionality."""

    def test_preview(self):
        """Test getting preview."""
        builder = (
            JobBuilder("test-task")
            .with_input({"key": "value"})
            .with_priority(10)
            .with_tag("test")
        )
        preview = builder.preview()

        assert preview.task_type == "test-task"
        assert preview.input == {"key": "value"}
        assert preview.priority == 10
        assert preview.tags == ["test"]


class TestJobFunction:
    """Test the job() convenience function."""

    def test_job_function_basic(self):
        """Test basic job function."""
        job_req = job("test-task").build()
        assert job_req.task_type == "test-task"

    def test_job_function_with_input(self):
        """Test job function with input."""
        job_req = job("test-task", {"value": 42}).build()
        assert job_req.task_type == "test-task"
        assert job_req.input == {"value": 42}


class TestJobBuilderChaining:
    """Test method chaining."""

    def test_full_chain(self):
        """Test full method chain."""
        job_req = (
            JobBuilder("complex-task")
            .with_input({"data": [1, 2, 3]})
            .with_priority(50)
            .with_timeout(3600)
            .with_tag("production")
            .with_tag("urgent")
            .with_metadata("user", "alice")
            .with_metadata("project", "demo")
            .with_retries(5)
            .with_checkpoint(True)
            .with_min_cpu(2)
            .with_min_memory_gb(4)
            .build()
        )

        assert job_req.task_type == "complex-task"
        assert job_req.input == {"data": [1, 2, 3]}
        assert job_req.priority == 50
        assert job_req.timeout_secs == 3600
        assert job_req.tags == ["production", "urgent"]
        assert job_req.metadata["user"] == "alice"
        assert job_req.max_retries == 5
        assert job_req.checkpoint_enabled is True
