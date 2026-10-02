# Marabunta - Licensed under the MIT License.
"""
Tests for job templates.
"""

import pytest
from marabunta_sdk import (
    MapReduceTemplate,
    ScatterGatherTemplate,
    PipelineTemplate,
    PipelineStage,
    BatchProcessingTemplate,
    FanOutFanInTemplate,
)


class TestMapReduceTemplate:
    """Test MapReduceTemplate."""

    def test_create_template(self):
        """Test creating a template."""
        template = MapReduceTemplate("mapper-task")
        assert template.mapper_task == "mapper-task"
        assert template.reducer_task is None

    def test_with_reducer(self):
        """Test setting reducer."""
        template = MapReduceTemplate("mapper").with_reducer("reducer")
        assert template.reducer_task == "reducer"

    def test_with_chunk_size(self):
        """Test setting chunk size."""
        template = MapReduceTemplate("mapper").with_chunk_size(50)
        assert template.chunk_size == 50

    def test_chunk_size_minimum(self):
        """Test chunk size minimum is 1."""
        template = MapReduceTemplate("mapper").with_chunk_size(0)
        assert template.chunk_size == 1

    def test_with_map_concurrency(self):
        """Test setting map concurrency."""
        template = MapReduceTemplate("mapper").with_map_concurrency(20)
        assert template.map_concurrency == 20

    def test_with_timeouts(self):
        """Test setting timeouts."""
        template = (
            MapReduceTemplate("mapper")
            .with_map_timeout(60)
            .with_reduce_timeout(120)
        )
        assert template.map_timeout == 60
        assert template.reduce_timeout == 120

    def test_with_tag(self):
        """Test adding tags."""
        template = MapReduceTemplate("mapper").with_tag("prod").with_tag("urgent")
        assert template.tags == ["prod", "urgent"]

    def test_with_metadata(self):
        """Test adding metadata."""
        template = MapReduceTemplate("mapper").with_metadata("user", "alice")
        assert template.metadata == {"user": "alice"}


class TestScatterGatherTemplate:
    """Test ScatterGatherTemplate."""

    def test_create_template(self):
        """Test creating a template."""
        template = ScatterGatherTemplate("worker-task")
        assert template.task_type == "worker-task"

    def test_with_concurrency(self):
        """Test setting concurrency."""
        template = ScatterGatherTemplate("worker").with_concurrency(20)
        assert template.concurrency == 20

    def test_concurrency_minimum(self):
        """Test concurrency minimum is 1."""
        template = ScatterGatherTemplate("worker").with_concurrency(0)
        assert template.concurrency == 1

    def test_with_timeout(self):
        """Test setting timeout."""
        template = ScatterGatherTemplate("worker").with_timeout(300)
        assert template.timeout == 300

    def test_with_fail_on_error(self):
        """Test setting fail on error."""
        template = ScatterGatherTemplate("worker").with_fail_on_error(True)
        assert template.fail_on_error is True


class TestPipelineTemplate:
    """Test PipelineTemplate."""

    def test_create_empty_template(self):
        """Test creating empty template."""
        template = PipelineTemplate()
        assert template.stages == []

    def test_then_adds_stage(self):
        """Test then() adds a stage."""
        template = PipelineTemplate().then("stage-1").then("stage-2")
        assert len(template.stages) == 2
        assert template.stages[0].task_type == "stage-1"
        assert template.stages[1].task_type == "stage-2"

    def test_add_stage(self):
        """Test add_stage()."""
        stage = PipelineStage(task_type="custom-stage", timeout=60)
        template = PipelineTemplate().add_stage(stage)
        assert len(template.stages) == 1
        assert template.stages[0].timeout == 60

    def test_with_tag(self):
        """Test adding tags."""
        template = PipelineTemplate().with_tag("pipeline-tag")
        assert template.tags == ["pipeline-tag"]


class TestPipelineStage:
    """Test PipelineStage."""

    def test_create_stage(self):
        """Test creating a stage."""
        stage = PipelineStage(task_type="processor")
        assert stage.task_type == "processor"
        assert stage.timeout is None
        assert stage.parallel is False

    def test_with_timeout(self):
        """Test setting timeout."""
        stage = PipelineStage(task_type="processor").with_timeout(120)
        assert stage.timeout == 120

    def test_with_parallelism(self):
        """Test enabling parallelism."""
        stage = PipelineStage(task_type="processor").with_parallelism(4)
        assert stage.parallel is True
        assert stage.parallelism == 4

    def test_parallelism_minimum(self):
        """Test parallelism minimum is 1."""
        stage = PipelineStage(task_type="processor").with_parallelism(0)
        assert stage.parallelism == 1


class TestBatchProcessingTemplate:
    """Test BatchProcessingTemplate."""

    def test_create_template(self):
        """Test creating a template."""
        template = BatchProcessingTemplate("processor")
        assert template.task_type == "processor"
        assert template.batch_size == 100
        assert template.concurrency == 10

    def test_with_batch_size(self):
        """Test setting batch size."""
        template = BatchProcessingTemplate("processor").with_batch_size(50)
        assert template.batch_size == 50

    def test_batch_size_minimum(self):
        """Test batch size minimum is 1."""
        template = BatchProcessingTemplate("processor").with_batch_size(0)
        assert template.batch_size == 1

    def test_with_concurrency(self):
        """Test setting concurrency."""
        template = BatchProcessingTemplate("processor").with_concurrency(5)
        assert template.concurrency == 5

    def test_with_timeout(self):
        """Test setting timeout."""
        template = BatchProcessingTemplate("processor").with_timeout(60)
        assert template.timeout == 60

    def test_with_tag(self):
        """Test adding tags."""
        template = BatchProcessingTemplate("processor").with_tag("batch")
        assert template.tags == ["batch"]


class TestFanOutFanInTemplate:
    """Test FanOutFanInTemplate."""

    def test_create_template(self):
        """Test creating a template."""
        template = FanOutFanInTemplate()
        assert template.fan_out_tasks == []
        assert template.aggregator_task is None

    def test_fan_out_to(self):
        """Test adding fan-out tasks."""
        template = (
            FanOutFanInTemplate()
            .fan_out_to("analyzer-1")
            .fan_out_to("analyzer-2")
            .fan_out_to("analyzer-3")
        )
        assert len(template.fan_out_tasks) == 3
        assert template.fan_out_tasks == ["analyzer-1", "analyzer-2", "analyzer-3"]

    def test_aggregate_with(self):
        """Test setting aggregator."""
        template = FanOutFanInTemplate().aggregate_with("aggregator")
        assert template.aggregator_task == "aggregator"

    def test_with_timeout(self):
        """Test setting timeout."""
        template = FanOutFanInTemplate().with_timeout(300)
        assert template.timeout == 300

    def test_with_tag(self):
        """Test adding tags."""
        template = FanOutFanInTemplate().with_tag("fanout")
        assert template.tags == ["fanout"]

    def test_full_configuration(self):
        """Test full configuration."""
        template = (
            FanOutFanInTemplate()
            .fan_out_to("task-a")
            .fan_out_to("task-b")
            .fan_out_to("task-c")
            .aggregate_with("combiner")
            .with_timeout(600)
            .with_tag("production")
        )
        assert len(template.fan_out_tasks) == 3
        assert template.aggregator_task == "combiner"
        assert template.timeout == 600
        assert template.tags == ["production"]
