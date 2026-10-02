// Marabunta - Licensed under the MIT License.
//! Job Templates for Common Patterns
//!
//! This module provides predefined templates for common distributed computing
//! patterns, making it easy to set up:
//!
//! - MapReduce operations
//! - Scatter-gather patterns
//! - Pipeline processing
//! - Batch processing
//! - Fan-out/fan-in patterns
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::sdk::templates::{MapReduceTemplate, ScatterGatherTemplate};
//! use marabunta_compute::sdk::client::MarabuntaClient;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Arc::new(MarabuntaClient::new("http://localhost:8080")?);
//!
//!     // MapReduce example
//!     let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
//!     let mapreduce = MapReduceTemplate::new("sum-task")
//!         .with_chunk_size(3)
//!         .with_reducer("aggregate-task");
//!
//!     let result: i64 = mapreduce.execute(client.clone(), data).await?;
//!
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};

use crate::sdk::batch::BatchSubmitter;
use crate::sdk::builder::JobBuilder;
use crate::sdk::client::{ClientError, ClientResult, MarabuntaClient, JobRequest};

/// Template for MapReduce operations
///
/// Splits input data into chunks, processes each chunk with a mapper,
/// then combines results with a reducer.
#[derive(Debug, Clone)]
pub struct MapReduceTemplate {
    /// Task type for the map operation
    mapper_task: String,
    /// Task type for the reduce operation
    reducer_task: Option<String>,
    /// Chunk size for splitting input
    chunk_size: usize,
    /// Maximum concurrency for map phase
    map_concurrency: usize,
    /// Timeout for individual map tasks
    map_timeout: Option<Duration>,
    /// Timeout for reduce task
    reduce_timeout: Option<Duration>,
    /// Additional tags for jobs
    tags: Vec<String>,
    /// Metadata for jobs
    metadata: HashMap<String, String>,
}

impl MapReduceTemplate {
    /// Create a new MapReduce template
    pub fn new(mapper_task: impl Into<String>) -> Self {
        Self {
            mapper_task: mapper_task.into(),
            reducer_task: None,
            chunk_size: 100,
            map_concurrency: 50,
            map_timeout: None,
            reduce_timeout: None,
            tags: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Set the reducer task type
    pub fn with_reducer(mut self, reducer_task: impl Into<String>) -> Self {
        self.reducer_task = Some(reducer_task.into());
        self
    }

    /// Set the chunk size
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size.max(1);
        self
    }

    /// Set map concurrency
    pub fn with_map_concurrency(mut self, concurrency: usize) -> Self {
        self.map_concurrency = concurrency.max(1);
        self
    }

    /// Set map timeout
    pub fn with_map_timeout(mut self, timeout: Duration) -> Self {
        self.map_timeout = Some(timeout);
        self
    }

    /// Set reduce timeout
    pub fn with_reduce_timeout(mut self, timeout: Duration) -> Self {
        self.reduce_timeout = Some(timeout);
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Execute the MapReduce operation
    pub async fn execute<T, M, R>(
        &self,
        client: Arc<MarabuntaClient>,
        input: Vec<T>,
    ) -> ClientResult<R>
    where
        T: Serialize + Clone,
        M: DeserializeOwned + Serialize,
        R: DeserializeOwned,
    {
        // Split input into chunks
        let chunks: Vec<Vec<T>> = input
            .chunks(self.chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        // Create map jobs
        let map_jobs: Vec<JobRequest> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, chunk)| {
                let mut builder = JobBuilder::new(&self.mapper_task)
                    .with_input(serde_json::to_value(&chunk).unwrap_or_default())
                    .with_metadata("chunk_index", i.to_string())
                    .with_metadata("operation", "map");

                if let Some(timeout) = self.map_timeout {
                    builder = builder.with_timeout(timeout);
                }

                for tag in &self.tags {
                    builder = builder.with_tag(tag.clone());
                }

                for (k, v) in &self.metadata {
                    builder = builder.with_metadata(k.clone(), v.clone());
                }

                builder.build().unwrap()
            })
            .collect();

        // Execute map phase
        let batch = BatchSubmitter::new(client.clone())
            .with_concurrency(self.map_concurrency);

        let map_results = batch.submit_and_wait::<M>(map_jobs).await;

        // Collect successful map results
        let mut intermediate_results: Vec<M> = Vec::new();
        for result in map_results {
            match result.result {
                Ok(value) => intermediate_results.push(value),
                Err(e) => return Err(e),
            }
        }

        // If no reducer specified, try to convert intermediate results directly
        if self.reducer_task.is_none() {
            // Try to deserialize the intermediate results as the final result
            let json = serde_json::to_value(&intermediate_results)
                .map_err(|e| ClientError::Serialization(e.to_string()))?;
            return serde_json::from_value(json)
                .map_err(|e| ClientError::Deserialization(e.to_string()));
        }

        // Execute reduce phase
        let reduce_job = {
            let mut builder = JobBuilder::new(self.reducer_task.as_ref().unwrap())
                .with_input(serde_json::to_value(&intermediate_results).unwrap_or_default())
                .with_metadata("operation", "reduce");

            if let Some(timeout) = self.reduce_timeout {
                builder = builder.with_timeout(timeout);
            }

            for tag in &self.tags {
                builder = builder.with_tag(tag.clone());
            }

            builder.build().unwrap()
        };

        client.submit_and_wait(reduce_job).await
    }
}

/// Template for Scatter-Gather pattern
///
/// Sends the same task to multiple workers with different parameters,
/// then gathers all results.
#[derive(Debug, Clone)]
pub struct ScatterGatherTemplate {
    /// Task type for all workers
    task_type: String,
    /// Maximum concurrency
    concurrency: usize,
    /// Timeout for individual tasks
    timeout: Option<Duration>,
    /// Tags for jobs
    tags: Vec<String>,
    /// Whether to fail if any task fails
    fail_on_error: bool,
}

impl ScatterGatherTemplate {
    /// Create a new scatter-gather template
    pub fn new(task_type: impl Into<String>) -> Self {
        Self {
            task_type: task_type.into(),
            concurrency: 50,
            timeout: None,
            tags: Vec::new(),
            fail_on_error: false,
        }
    }

    /// Set concurrency
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set fail on error behavior
    pub fn with_fail_on_error(mut self, fail: bool) -> Self {
        self.fail_on_error = fail;
        self
    }

    /// Execute the scatter-gather operation
    pub async fn execute<T, R>(
        &self,
        client: Arc<MarabuntaClient>,
        inputs: Vec<T>,
    ) -> ClientResult<Vec<ClientResult<R>>>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        // Create jobs for each input
        let jobs: Vec<JobRequest> = inputs
            .into_iter()
            .enumerate()
            .map(|(i, input)| {
                let mut builder = JobBuilder::new(&self.task_type)
                    .with_input(serde_json::to_value(&input).unwrap_or_default())
                    .with_metadata("scatter_index", i.to_string());

                if let Some(timeout) = self.timeout {
                    builder = builder.with_timeout(timeout);
                }

                for tag in &self.tags {
                    builder = builder.with_tag(tag.clone());
                }

                builder.build().unwrap()
            })
            .collect();

        // Execute all jobs
        let batch = BatchSubmitter::new(client)
            .with_concurrency(self.concurrency)
            .with_fail_fast(self.fail_on_error);

        let results = batch.submit_and_wait::<R>(jobs).await;

        Ok(results.into_iter().map(|r| r.result).collect())
    }
}

/// Template for Pipeline processing
///
/// Chains multiple tasks in sequence, where the output of each task
/// becomes the input of the next.
#[derive(Debug, Clone)]
pub struct PipelineTemplate {
    /// List of task types in order
    stages: Vec<PipelineStage>,
    /// Tags for all jobs
    tags: Vec<String>,
}

/// A single stage in a pipeline
#[derive(Debug, Clone)]
pub struct PipelineStage {
    /// Task type for this stage
    pub task_type: String,
    /// Timeout for this stage
    pub timeout: Option<Duration>,
    /// Whether this stage can be parallelized
    pub parallel: bool,
    /// Number of parallel workers if parallel
    pub parallelism: usize,
}

impl PipelineStage {
    /// Create a new pipeline stage
    pub fn new(task_type: impl Into<String>) -> Self {
        Self {
            task_type: task_type.into(),
            timeout: None,
            parallel: false,
            parallelism: 1,
        }
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Enable parallelism
    pub fn parallel(mut self, workers: usize) -> Self {
        self.parallel = true;
        self.parallelism = workers.max(1);
        self
    }
}

impl PipelineTemplate {
    /// Create a new pipeline template
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// Add a stage to the pipeline
    pub fn add_stage(mut self, stage: PipelineStage) -> Self {
        self.stages.push(stage);
        self
    }

    /// Add a simple stage by task type
    pub fn then(mut self, task_type: impl Into<String>) -> Self {
        self.stages.push(PipelineStage::new(task_type));
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Execute the pipeline
    pub async fn execute<T, R>(
        &self,
        client: Arc<MarabuntaClient>,
        input: T,
    ) -> ClientResult<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        if self.stages.is_empty() {
            return Err(ClientError::InvalidConfig("Pipeline has no stages".into()));
        }

        let mut current_data = serde_json::to_value(&input)
            .map_err(|e| ClientError::Serialization(e.to_string()))?;

        for (i, stage) in self.stages.iter().enumerate() {
            let mut builder = JobBuilder::new(&stage.task_type)
                .with_input(current_data)
                .with_metadata("pipeline_stage", i.to_string())
                .with_metadata("pipeline_stage_name", &stage.task_type);

            if let Some(timeout) = stage.timeout {
                builder = builder.with_timeout(timeout);
            }

            for tag in &self.tags {
                builder = builder.with_tag(tag.clone());
            }

            let job = builder.build().unwrap();
            current_data = client.submit_and_wait::<serde_json::Value>(job).await?;
        }

        serde_json::from_value(current_data)
            .map_err(|e| ClientError::Deserialization(e.to_string()))
    }
}

impl Default for PipelineTemplate {
    fn default() -> Self {
        Self::new()
    }
}

/// Template for batch processing with automatic chunking
#[derive(Debug, Clone)]
pub struct BatchProcessingTemplate {
    /// Task type for processing
    task_type: String,
    /// Number of items per batch
    batch_size: usize,
    /// Maximum concurrency
    concurrency: usize,
    /// Timeout per batch
    timeout: Option<Duration>,
    /// Tags for jobs
    tags: Vec<String>,
}

impl BatchProcessingTemplate {
    /// Create a new batch processing template
    pub fn new(task_type: impl Into<String>) -> Self {
        Self {
            task_type: task_type.into(),
            batch_size: 100,
            concurrency: 10,
            timeout: None,
            tags: Vec::new(),
        }
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Set concurrency
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Process all items in batches
    pub async fn process<T, R>(
        &self,
        client: Arc<MarabuntaClient>,
        items: Vec<T>,
    ) -> ClientResult<Vec<R>>
    where
        T: Serialize + Clone,
        R: DeserializeOwned,
    {
        // Split into batches
        let batches: Vec<Vec<T>> = items
            .chunks(self.batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        // Create jobs
        let jobs: Vec<JobRequest> = batches
            .into_iter()
            .enumerate()
            .map(|(i, batch)| {
                let mut builder = JobBuilder::new(&self.task_type)
                    .with_input(serde_json::to_value(&batch).unwrap_or_default())
                    .with_metadata("batch_index", i.to_string());

                if let Some(timeout) = self.timeout {
                    builder = builder.with_timeout(timeout);
                }

                for tag in &self.tags {
                    builder = builder.with_tag(tag.clone());
                }

                builder.build().unwrap()
            })
            .collect();

        // Execute batches
        let batch_submitter = BatchSubmitter::new(client)
            .with_concurrency(self.concurrency);

        let results = batch_submitter.submit_and_wait::<Vec<R>>(jobs).await;

        // Flatten results
        let mut all_results = Vec::new();
        for batch_result in results {
            match batch_result.result {
                Ok(batch_values) => all_results.extend(batch_values),
                Err(e) => return Err(e),
            }
        }

        Ok(all_results)
    }
}

/// Template for fan-out/fan-in pattern
///
/// Sends a single input to multiple different tasks, then aggregates results.
#[derive(Debug, Clone)]
pub struct FanOutFanInTemplate {
    /// Tasks to fan out to
    fan_out_tasks: Vec<String>,
    /// Aggregator task (optional)
    aggregator_task: Option<String>,
    /// Timeout for fan-out tasks
    timeout: Option<Duration>,
    /// Tags for jobs
    tags: Vec<String>,
}

impl FanOutFanInTemplate {
    /// Create a new fan-out/fan-in template
    pub fn new() -> Self {
        Self {
            fan_out_tasks: Vec::new(),
            aggregator_task: None,
            timeout: None,
            tags: Vec::new(),
        }
    }

    /// Add a fan-out task
    pub fn fan_out_to(mut self, task_type: impl Into<String>) -> Self {
        self.fan_out_tasks.push(task_type.into());
        self
    }

    /// Set the aggregator task
    pub fn aggregate_with(mut self, task_type: impl Into<String>) -> Self {
        self.aggregator_task = Some(task_type.into());
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Execute the fan-out/fan-in operation
    pub async fn execute<T, R>(
        &self,
        client: Arc<MarabuntaClient>,
        input: T,
    ) -> ClientResult<R>
    where
        T: Serialize + Clone,
        R: DeserializeOwned,
    {
        if self.fan_out_tasks.is_empty() {
            return Err(ClientError::InvalidConfig("No fan-out tasks specified".into()));
        }

        let input_json = serde_json::to_value(&input)
            .map_err(|e| ClientError::Serialization(e.to_string()))?;

        // Create jobs for each fan-out task
        let jobs: Vec<JobRequest> = self
            .fan_out_tasks
            .iter()
            .enumerate()
            .map(|(i, task_type)| {
                let mut builder = JobBuilder::new(task_type)
                    .with_input(input_json.clone())
                    .with_metadata("fan_out_index", i.to_string())
                    .with_metadata("fan_out_task", task_type);

                if let Some(timeout) = self.timeout {
                    builder = builder.with_timeout(timeout);
                }

                for tag in &self.tags {
                    builder = builder.with_tag(tag.clone());
                }

                builder.build().unwrap()
            })
            .collect();

        // Execute fan-out phase
        let batch = BatchSubmitter::new(client.clone());
        let results = batch.submit_and_wait::<serde_json::Value>(jobs).await;

        // Collect results
        let mut fan_out_results: Vec<serde_json::Value> = Vec::new();
        for result in results {
            match result.result {
                Ok(value) => fan_out_results.push(value),
                Err(e) => return Err(e),
            }
        }

        // If no aggregator, return results directly
        if self.aggregator_task.is_none() {
            let json = serde_json::to_value(&fan_out_results)
                .map_err(|e| ClientError::Serialization(e.to_string()))?;
            return serde_json::from_value(json)
                .map_err(|e| ClientError::Deserialization(e.to_string()));
        }

        // Execute aggregator
        let aggregator_job = JobBuilder::new(self.aggregator_task.as_ref().unwrap())
            .with_input(serde_json::to_value(&fan_out_results).unwrap_or_default())
            .with_metadata("operation", "aggregate")
            .build()
            .unwrap();

        client.submit_and_wait(aggregator_job).await
    }
}

impl Default for FanOutFanInTemplate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_mapreduce_template_creation() {
        let template = MapReduceTemplate::new("mapper")
            .with_reducer("reducer")
            .with_chunk_size(50)
            .with_map_concurrency(10)
            .with_tag("test");

        assert_eq!(template.mapper_task, "mapper");
        assert_eq!(template.reducer_task, Some("reducer".to_string()));
        assert_eq!(template.chunk_size, 50);
        assert_eq!(template.map_concurrency, 10);
        assert_eq!(template.tags, vec!["test"]);
    }

    #[test]
    fn test_scatter_gather_template() {
        let template = ScatterGatherTemplate::new("worker")
            .with_concurrency(20)
            .with_timeout(Duration::from_secs(60))
            .with_fail_on_error(true);

        assert_eq!(template.task_type, "worker");
        assert_eq!(template.concurrency, 20);
        assert_eq!(template.timeout, Some(Duration::from_secs(60)));
        assert!(template.fail_on_error);
    }

    #[test]
    fn test_pipeline_template() {
        let template = PipelineTemplate::new()
            .then("stage-1")
            .then("stage-2")
            .add_stage(PipelineStage::new("stage-3").with_timeout(Duration::from_secs(30)))
            .with_tag("pipeline");

        assert_eq!(template.stages.len(), 3);
        assert_eq!(template.stages[0].task_type, "stage-1");
        assert_eq!(template.stages[2].timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_pipeline_stage() {
        let stage = PipelineStage::new("task")
            .with_timeout(Duration::from_secs(60))
            .parallel(4);

        assert_eq!(stage.task_type, "task");
        assert!(stage.parallel);
        assert_eq!(stage.parallelism, 4);
    }

    #[test]
    fn test_batch_processing_template() {
        let template = BatchProcessingTemplate::new("processor")
            .with_batch_size(50)
            .with_concurrency(5)
            .with_timeout(Duration::from_secs(120));

        assert_eq!(template.task_type, "processor");
        assert_eq!(template.batch_size, 50);
        assert_eq!(template.concurrency, 5);
    }

    #[test]
    fn test_fan_out_fan_in_template() {
        let template = FanOutFanInTemplate::new()
            .fan_out_to("task-a")
            .fan_out_to("task-b")
            .fan_out_to("task-c")
            .aggregate_with("aggregator")
            .with_timeout(Duration::from_secs(300));

        assert_eq!(template.fan_out_tasks.len(), 3);
        assert_eq!(template.aggregator_task, Some("aggregator".to_string()));
    }

    #[test]
    fn test_min_values() {
        // Chunk size minimum
        let mr = MapReduceTemplate::new("task").with_chunk_size(0);
        assert_eq!(mr.chunk_size, 1);

        // Concurrency minimum
        let sg = ScatterGatherTemplate::new("task").with_concurrency(0);
        assert_eq!(sg.concurrency, 1);

        // Batch size minimum
        let bp = BatchProcessingTemplate::new("task").with_batch_size(0);
        assert_eq!(bp.batch_size, 1);
    }
}
