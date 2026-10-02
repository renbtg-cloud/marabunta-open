// Marabunta - Licensed under the MIT License.
//! Job Builder Pattern for Fluent API
//!
//! This module provides a fluent builder pattern for constructing job requests
//! with type safety and validation.
//!
//! # Example
//!
//! ```rust
//! use marabunta_compute::sdk::builder::JobBuilder;
//! use std::time::Duration;
//!
//! let job = JobBuilder::new("data-processing")
//!     .with_input(serde_json::json!({"file": "data.csv"}))
//!     .with_priority(10)
//!     .with_timeout(Duration::from_secs(3600))
//!     .with_tag("production")
//!     .with_tag("high-priority")
//!     .with_metadata("user", "john")
//!     .with_retries(5)
//!     .with_checkpoint(true)
//!     .build()
//!     .expect("Valid job configuration");
//! ```

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::sdk::client::JobRequest;

/// Errors that can occur when building a job
#[derive(Error, Debug, Clone)]
pub enum BuilderError {
    #[error("Task type is required")]
    MissingTaskType,

    #[error("Input is required")]
    MissingInput,

    #[error("Invalid priority: {0}")]
    InvalidPriority(String),

    #[error("Invalid timeout: {0}")]
    InvalidTimeout(String),

    #[error("Invalid tag: {0}")]
    InvalidTag(String),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),
}

/// Result type for builder operations
pub type BuilderResult<T> = Result<T, BuilderError>;

/// Builder for creating job requests with a fluent API
#[derive(Debug, Clone)]
pub struct JobBuilder {
    task_type: Option<String>,
    input: Option<serde_json::Value>,
    priority: i32,
    timeout: Option<Duration>,
    tags: Vec<String>,
    metadata: HashMap<String, String>,
    max_retries: u32,
    checkpoint_enabled: bool,
    dependencies: Vec<String>,
    resources: ResourceRequirements,
}

/// Resource requirements for a job
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceRequirements {
    /// Minimum CPU cores required
    #[serde(default)]
    pub min_cpu_cores: Option<u32>,
    /// Maximum CPU cores to use
    #[serde(default)]
    pub max_cpu_cores: Option<u32>,
    /// Minimum memory in bytes
    #[serde(default)]
    pub min_memory_bytes: Option<u64>,
    /// Maximum memory in bytes
    #[serde(default)]
    pub max_memory_bytes: Option<u64>,
    /// Minimum disk space in bytes
    #[serde(default)]
    pub min_disk_bytes: Option<u64>,
    /// GPU requirements
    #[serde(default)]
    pub gpu: Option<GpuRequirements>,
}

/// GPU requirements for a job
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GpuRequirements {
    /// Number of GPUs required
    pub count: u32,
    /// Minimum GPU memory in bytes
    pub min_memory: Option<u64>,
    /// Required GPU types (e.g., ["nvidia", "cuda"])
    pub types: Vec<String>,
}

impl Default for JobBuilder {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl JobBuilder {
    /// Create a new job builder with the given task type
    pub fn new(task_type: impl Into<String>) -> Self {
        Self {
            task_type: Some(task_type.into()),
            input: None,
            priority: 0,
            timeout: None,
            tags: Vec::new(),
            metadata: HashMap::new(),
            max_retries: 3,
            checkpoint_enabled: true,
            dependencies: Vec::new(),
            resources: ResourceRequirements::default(),
        }
    }

    /// Create an empty job builder (task type must be set later)
    pub fn new_empty() -> Self {
        Self {
            task_type: None,
            input: None,
            priority: 0,
            timeout: None,
            tags: Vec::new(),
            metadata: HashMap::new(),
            max_retries: 3,
            checkpoint_enabled: true,
            dependencies: Vec::new(),
            resources: ResourceRequirements::default(),
        }
    }

    /// Set the task type
    pub fn task_type(mut self, task_type: impl Into<String>) -> Self {
        self.task_type = Some(task_type.into());
        self
    }

    /// Set the input data
    pub fn with_input(mut self, input: serde_json::Value) -> Self {
        self.input = Some(input);
        self
    }

    /// Set the input data from a serializable type
    pub fn with_input_from<T: Serialize>(mut self, input: &T) -> BuilderResult<Self> {
        self.input = Some(
            serde_json::to_value(input)
                .map_err(|e| BuilderError::ValidationFailed(format!("Failed to serialize input: {}", e)))?,
        );
        Ok(self)
    }

    /// Set the job priority (higher = more important)
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set high priority
    pub fn high_priority(self) -> Self {
        self.with_priority(100)
    }

    /// Set normal priority
    pub fn normal_priority(self) -> Self {
        self.with_priority(0)
    }

    /// Set low priority
    pub fn low_priority(self) -> Self {
        self.with_priority(-100)
    }

    /// Set the job timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set timeout in seconds (convenience method)
    pub fn with_timeout_secs(self, seconds: u64) -> Self {
        self.with_timeout(Duration::from_secs(seconds))
    }

    /// Add a tag to the job
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add multiple tags to the job
    pub fn with_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags.extend(tags.into_iter().map(Into::into));
        self
    }

    /// Add metadata to the job
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Add multiple metadata entries
    pub fn with_metadata_map(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata.extend(metadata);
        self
    }

    /// Set the maximum number of retries
    pub fn with_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Disable retries
    pub fn no_retries(self) -> Self {
        self.with_retries(0)
    }

    /// Enable or disable checkpointing
    pub fn with_checkpoint(mut self, enabled: bool) -> Self {
        self.checkpoint_enabled = enabled;
        self
    }

    /// Disable checkpointing
    pub fn no_checkpoint(self) -> Self {
        self.with_checkpoint(false)
    }

    /// Add a job dependency (this job will only run after the dependency completes)
    pub fn depends_on(mut self, job_id: impl Into<String>) -> Self {
        self.dependencies.push(job_id.into());
        self
    }

    /// Add multiple job dependencies
    pub fn depends_on_all<I, S>(mut self, job_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.dependencies.extend(job_ids.into_iter().map(Into::into));
        self
    }

    /// Set minimum CPU cores
    pub fn with_min_cpu(mut self, cores: u32) -> Self {
        self.resources.min_cpu_cores = Some(cores);
        self
    }

    /// Set maximum CPU cores
    pub fn with_max_cpu(mut self, cores: u32) -> Self {
        self.resources.max_cpu_cores = Some(cores);
        self
    }

    /// Set minimum memory requirement
    pub fn with_min_memory(mut self, bytes: u64) -> Self {
        self.resources.min_memory_bytes = Some(bytes);
        self
    }

    /// Set minimum memory in gigabytes (convenience method)
    pub fn with_min_memory_gb(self, gb: u64) -> Self {
        self.with_min_memory(gb * 1024 * 1024 * 1024)
    }

    /// Set maximum memory
    pub fn with_max_memory(mut self, bytes: u64) -> Self {
        self.resources.max_memory_bytes = Some(bytes);
        self
    }

    /// Set minimum disk space requirement
    pub fn with_min_disk(mut self, bytes: u64) -> Self {
        self.resources.min_disk_bytes = Some(bytes);
        self
    }

    /// Set GPU requirements
    pub fn with_gpu(mut self, count: u32) -> Self {
        self.resources.gpu = Some(GpuRequirements {
            count,
            min_memory: None,
            types: Vec::new(),
        });
        self
    }

    /// Set GPU requirements with details
    pub fn with_gpu_requirements(mut self, requirements: GpuRequirements) -> Self {
        self.resources.gpu = Some(requirements);
        self
    }

    /// Validate the builder configuration
    pub fn validate(&self) -> BuilderResult<()> {
        if self.task_type.is_none() {
            return Err(BuilderError::MissingTaskType);
        }

        // Validate tags don't contain invalid characters
        for tag in &self.tags {
            if tag.is_empty() {
                return Err(BuilderError::InvalidTag("Tag cannot be empty".into()));
            }
            if tag.contains(|c: char| !c.is_alphanumeric() && c != '-' && c != '_') {
                return Err(BuilderError::InvalidTag(format!(
                    "Tag '{}' contains invalid characters",
                    tag
                )));
            }
        }

        // Validate resource requirements
        if let (Some(min_cpu), Some(max_cpu)) = (
            self.resources.min_cpu_cores,
            self.resources.max_cpu_cores,
        ) {
            if min_cpu > max_cpu {
                return Err(BuilderError::ValidationFailed(
                    "min_cpu_cores cannot be greater than max_cpu_cores".into(),
                ));
            }
        }

        if let (Some(min_mem), Some(max_mem)) = (
            self.resources.min_memory_bytes,
            self.resources.max_memory_bytes,
        ) {
            if min_mem > max_mem {
                return Err(BuilderError::ValidationFailed(
                    "min_memory_bytes cannot be greater than max_memory_bytes".into(),
                ));
            }
        }

        Ok(())
    }

    /// Build the job request
    pub fn build(self) -> BuilderResult<JobRequest> {
        self.validate()?;

        let task_type = self.task_type.ok_or(BuilderError::MissingTaskType)?;
        let input = self.input.unwrap_or(serde_json::Value::Object(Default::default()));

        Ok(JobRequest {
            task_type,
            input,
            priority: self.priority,
            timeout_secs: self.timeout.map(|d| d.as_secs()),
            tags: self.tags,
            metadata: self.metadata,
            max_retries: self.max_retries,
            checkpoint_enabled: self.checkpoint_enabled,
        })
    }

    /// Build the job request, panicking on invalid configuration
    pub fn build_unchecked(self) -> JobRequest {
        self.build().expect("Invalid job configuration")
    }

    /// Get the current configuration as a preview
    pub fn preview(&self) -> JobPreview {
        JobPreview {
            task_type: self.task_type.clone(),
            input: self.input.clone(),
            priority: self.priority,
            timeout: self.timeout,
            tags: self.tags.clone(),
            metadata: self.metadata.clone(),
            max_retries: self.max_retries,
            checkpoint_enabled: self.checkpoint_enabled,
            dependencies: self.dependencies.clone(),
            resources: self.resources.clone(),
        }
    }
}

/// Preview of a job configuration (for inspection before building)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPreview {
    pub task_type: Option<String>,
    pub input: Option<serde_json::Value>,
    pub priority: i32,
    pub timeout: Option<Duration>,
    pub tags: Vec<String>,
    pub metadata: HashMap<String, String>,
    pub max_retries: u32,
    pub checkpoint_enabled: bool,
    pub dependencies: Vec<String>,
    pub resources: ResourceRequirements,
}

/// Macro for creating job builders with a more concise syntax
#[macro_export]
macro_rules! job {
    ($task_type:expr) => {
        $crate::sdk::builder::JobBuilder::new($task_type)
    };
    ($task_type:expr, $input:expr) => {
        $crate::sdk::builder::JobBuilder::new($task_type).with_input($input)
    };
    ($task_type:expr, $input:expr, priority = $priority:expr) => {
        $crate::sdk::builder::JobBuilder::new($task_type)
            .with_input($input)
            .with_priority($priority)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_builder() {
        let job = JobBuilder::new("test-task")
            .with_input(json!({"key": "value"}))
            .build()
            .unwrap();

        assert_eq!(job.task_type, "test-task");
        assert_eq!(job.input, json!({"key": "value"}));
        assert_eq!(job.priority, 0);
        assert!(job.checkpoint_enabled);
    }

    #[test]
    fn test_builder_with_all_options() {
        let job = JobBuilder::new("complex-task")
            .with_input(json!({"data": [1, 2, 3]}))
            .with_priority(50)
            .with_timeout(Duration::from_secs(3600))
            .with_tag("production")
            .with_tag("urgent")
            .with_metadata("user", "alice")
            .with_metadata("project", "demo")
            .with_retries(5)
            .with_checkpoint(true)
            .build()
            .unwrap();

        assert_eq!(job.task_type, "complex-task");
        assert_eq!(job.priority, 50);
        assert_eq!(job.timeout_secs, Some(3600));
        assert_eq!(job.tags, vec!["production", "urgent"]);
        assert_eq!(job.metadata.get("user").unwrap(), "alice");
        assert_eq!(job.max_retries, 5);
        assert!(job.checkpoint_enabled);
    }

    #[test]
    fn test_priority_helpers() {
        let high = JobBuilder::new("task").high_priority().build().unwrap();
        assert_eq!(high.priority, 100);

        let normal = JobBuilder::new("task").normal_priority().build().unwrap();
        assert_eq!(normal.priority, 0);

        let low = JobBuilder::new("task").low_priority().build().unwrap();
        assert_eq!(low.priority, -100);
    }

    #[test]
    fn test_no_retries() {
        let job = JobBuilder::new("task").no_retries().build().unwrap();
        assert_eq!(job.max_retries, 0);
    }

    #[test]
    fn test_no_checkpoint() {
        let job = JobBuilder::new("task").no_checkpoint().build().unwrap();
        assert!(!job.checkpoint_enabled);
    }

    #[test]
    fn test_missing_task_type() {
        let result = JobBuilder::new_empty().build();
        assert!(matches!(result, Err(BuilderError::MissingTaskType)));
    }

    #[test]
    fn test_invalid_tag() {
        let result = JobBuilder::new("task").with_tag("invalid tag!").validate();
        assert!(matches!(result, Err(BuilderError::InvalidTag(_))));
    }

    #[test]
    fn test_empty_tag() {
        let result = JobBuilder::new("task").with_tag("").validate();
        assert!(matches!(result, Err(BuilderError::InvalidTag(_))));
    }

    #[test]
    fn test_valid_tags() {
        let result = JobBuilder::new("task")
            .with_tag("valid-tag")
            .with_tag("also_valid")
            .with_tag("tag123")
            .validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_resource_requirements() {
        let builder = JobBuilder::new("task")
            .with_min_cpu(2)
            .with_max_cpu(8)
            .with_min_memory_gb(4)
            .with_gpu(1);

        let preview = builder.preview();
        assert_eq!(preview.resources.min_cpu_cores, Some(2));
        assert_eq!(preview.resources.max_cpu_cores, Some(8));
        assert_eq!(preview.resources.min_memory_bytes, Some(4 * 1024 * 1024 * 1024));
        assert!(preview.resources.gpu.is_some());
    }

    #[test]
    fn test_invalid_resource_requirements() {
        let result = JobBuilder::new("task")
            .with_min_cpu(8)
            .with_max_cpu(2)
            .validate();

        assert!(matches!(result, Err(BuilderError::ValidationFailed(_))));
    }

    #[test]
    fn test_dependencies() {
        let builder = JobBuilder::new("task")
            .depends_on("job-1")
            .depends_on("job-2")
            .depends_on_all(vec!["job-3", "job-4"]);

        let preview = builder.preview();
        assert_eq!(preview.dependencies, vec!["job-1", "job-2", "job-3", "job-4"]);
    }

    #[test]
    fn test_with_tags_iterator() {
        let tags = vec!["tag1", "tag2", "tag3"];
        let job = JobBuilder::new("task").with_tags(tags).build().unwrap();
        assert_eq!(job.tags, vec!["tag1", "tag2", "tag3"]);
    }

    #[test]
    fn test_timeout_secs_convenience() {
        let job = JobBuilder::new("task")
            .with_timeout_secs(600)
            .build()
            .unwrap();
        assert_eq!(job.timeout_secs, Some(600));
    }

    #[test]
    fn test_build_unchecked() {
        let job = JobBuilder::new("task").build_unchecked();
        assert_eq!(job.task_type, "task");
    }

    #[test]
    #[should_panic(expected = "Invalid job configuration")]
    fn test_build_unchecked_panics() {
        JobBuilder::new_empty().build_unchecked();
    }

    #[test]
    fn test_default_input() {
        let job = JobBuilder::new("task").build().unwrap();
        assert_eq!(job.input, json!({}));
    }

    #[test]
    fn test_input_from_serializable() {
        #[derive(Serialize)]
        struct Input {
            value: i32,
        }

        let job = JobBuilder::new("task")
            .with_input_from(&Input { value: 42 })
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(job.input, json!({"value": 42}));
    }

    #[test]
    fn test_job_macro() {
        let job1 = job!("task").build().unwrap();
        assert_eq!(job1.task_type, "task");

        let job2 = job!("task", json!({"x": 1})).build().unwrap();
        assert_eq!(job2.input, json!({"x": 1}));
    }

    #[test]
    fn test_preview() {
        let builder = JobBuilder::new("task")
            .with_input(json!({"key": "value"}))
            .with_priority(10)
            .with_tag("test");

        let preview = builder.preview();
        assert_eq!(preview.task_type, Some("task".to_string()));
        assert_eq!(preview.priority, 10);
        assert_eq!(preview.tags, vec!["test"]);
    }
}
