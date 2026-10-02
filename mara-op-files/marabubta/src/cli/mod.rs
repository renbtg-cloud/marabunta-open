// Marabunta - Licensed under the MIT License.
//! Marabunta Compute CLI Module
//!
//! Provides command-line tools for submitting, monitoring, and managing
//! distributed computing jobs on the Marabunta Compute platform.
//!
//! # Submodules
//!
//! - [`submit`] - Job submission command
//! - [`status`] - Job status monitoring
//! - [`results`] - Job results retrieval
//! - [`cancel`] - Job cancellation
//! - [`nodes`] - Node listing and filtering
//! - [`tokens`] - Contribution token management
//! - [`config`] - CLI configuration
//! - [`client`] - Coordinator client
//! - [`display`] - Output formatting helpers
//! - [`types`] - CLI-specific types and enums
//! - [`maintenance`] - Maintenance window management commands
//! - [`workflow`] - Workflow management commands
//! - [`backup`] - Backup and restore commands
//! - [`policy`] - Policy DSL management commands
//! - [`interactive`] - Interactive REPL mode
//! - [`table`] - Table formatting for list outputs
//! - [`progress`] - Progress bars and spinners
//! - [`completion`] - Shell completion scripts
//! - [`output`] - JSON output mode for scripting

pub mod backup;
pub mod cancel;
pub mod client;
pub mod completion;
pub mod config;
pub mod display;
pub mod interactive;
pub mod maintenance;
pub mod nodes;
pub mod output;
pub mod policy;
pub mod progress;
pub mod results;
pub mod status;
pub mod submit;
pub mod table;
pub mod tokens;
pub mod types;
pub mod workflow;

pub use backup::{execute_backup, BackupArgs, BackupCommand};
pub use cancel::{execute_cancel, CancelArgs};
pub use client::{ClientError, CoordinatorClient};
pub use completion::{generate_completion, Shell};
pub use config::{execute_config, Config, ConfigArgs, ConfigCommand, ConfigError};
pub use display::*;
pub use interactive::run_repl;
pub use maintenance::{execute_maintenance, MaintenanceArgs, MaintenanceCommand};
pub use nodes::{execute_nodes, NodesArgs};
pub use output::{ErrorResponse, JsonOutput, Output, Outputter, SuccessResponse};
pub use policy::{execute_policy, PolicyArgs, PolicyCommand};
pub use progress::{JobProgressTracker, MultiProgress, ProgressStylePreset, ProgressTracker, UploadProgressTracker};
pub use results::{execute_results, ResultsArgs};
pub use status::{execute_status, StatusArgs};
pub use submit::{execute_submit, SubmitArgs};
pub use table::{jobs_table, nodes_table, tasks_table, tokens_table, workers_table, Alignment, Column, Table, TableStyle};
pub use tokens::{execute_tokens, TokensArgs, TokensCommand};
pub use types::*;
pub use workflow::{execute_workflow, WorkflowArgs, WorkflowCommand};
