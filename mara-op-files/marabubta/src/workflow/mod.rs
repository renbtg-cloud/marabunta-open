// Marabunta - Licensed under the MIT License.
//! Workflow Engine for Marabunta Compute
//!
//! Multi-job workflows with conditional logic, parallel execution, and loops.
//!
//! # Overview
//!
//! The workflow engine enables complex job orchestration:
//!
//! - **Sequential steps**: Jobs run one after another
//! - **Parallel steps**: Multiple jobs run concurrently
//! - **Conditional logic**: Branch based on job outcomes or output matching
//! - **Loops**: Iterate with dynamic conditions
//! - **Variable interpolation**: Pass data between steps
//!
//! # Example Workflow (YAML)
//!
//! ```yaml
//! name: data-pipeline
//! version: "1.0"
//! description: "Process and analyze dataset"
//!
//! variables:
//!   input_bucket: "s3://data/raw"
//!   output_bucket: "s3://data/processed"
//!
//! steps:
//!   - id: fetch_data
//!     type: job
//!     job:
//!       name: "Fetch raw data"
//!       runtime: python3
//!       file: fetch.py
//!       args:
//!         source: "{{ variables.input_bucket }}"
//!
//!   - id: validate
//!     type: job
//!     depends_on: [fetch_data]
//!     job:
//!       name: "Validate data"
//!       runtime: python3
//!       file: validate.py
//!
//!   - id: process_branch
//!     type: conditional
//!     depends_on: [validate]
//!     condition:
//!       type: on_success
//!       step: validate
//!     then:
//!       - id: transform
//!         type: parallel
//!         branches:
//!           - id: normalize
//!             job:
//!               name: "Normalize data"
//!               runtime: python3
//!               file: normalize.py
//!           - id: enrich
//!             job:
//!               name: "Enrich data"
//!               runtime: python3
//!               file: enrich.py
//!     else:
//!       - id: notify_failure
//!         type: job
//!         job:
//!           name: "Send failure notification"
//!           runtime: python3
//!           file: notify.py
//!           args:
//!             message: "Validation failed"
//!
//!   - id: aggregate
//!     type: job
//!     depends_on: [process_branch]
//!     job:
//!       name: "Aggregate results"
//!       runtime: python3
//!       file: aggregate.py
//!       args:
//!         output: "{{ variables.output_bucket }}"
//! ```
//!
//! # Modules
//!
//! - [`types`] - Workflow data structures and YAML schema
//! - [`engine`] - Workflow execution engine with state machine
//! - [`persistence`] - Save/load workflows and execution state
//! - [`api`] - REST API endpoints for workflow management

pub mod api;
pub mod engine;
pub mod persistence;
pub mod types;

pub use api::{create_workflow_router, WorkflowApiState};
pub use engine::{WorkflowExecutor, WorkflowExecutorConfig};
pub use persistence::{WorkflowStore, WorkflowStoreError};
pub use types::*;
