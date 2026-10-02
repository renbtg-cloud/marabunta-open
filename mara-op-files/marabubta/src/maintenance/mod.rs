// Marabunta - Licensed under the MIT License.
//! Maintenance window management for Marabunta Compute
//!
//! This module provides comprehensive maintenance window functionality,
//! including scheduled downtime, node draining, and task migration.
//!
//! # Overview
//!
//! Maintenance windows allow operators to perform planned or emergency
//! maintenance on nodes while minimizing disruption to running jobs.
//! The system supports:
//!
//! - **Planned Maintenance**: Scheduled in advance with graceful task migration
//! - **Emergency Maintenance**: Immediate action with optional force-drain
//! - **Rolling Maintenance**: One node at a time to maintain cluster capacity
//!
//! # Components
//!
//! - [`types`]: Core types for maintenance windows, states, and errors
//! - [`drain`]: Node draining and task migration functionality
//! - [`scheduler`]: Maintenance window scheduling and coordination
//!
//! # Usage
//!
//! ```no_run
//! use marabunta_compute::maintenance::{
//!     MaintenanceWindow, MaintenanceType, MaintenanceScheduler,
//!     MaintenanceSchedulerConfig,
//! };
//! use std::collections::HashSet;
//! use chrono::{Utc, Duration};
//! use tokio::sync::mpsc;
//!
//! # async fn example() {
//! // Create scheduler
//! let (event_tx, _event_rx) = mpsc::channel(100);
//! let (scheduler, _drain_rx) = MaintenanceScheduler::new(
//!     MaintenanceSchedulerConfig::default(),
//!     event_tx,
//! );
//!
//! // Schedule planned maintenance
//! let mut nodes = HashSet::new();
//! // nodes.insert(node_id);
//!
//! let window = MaintenanceWindow::new(
//!     "Weekly Update",
//!     MaintenanceType::Planned,
//!     Utc::now() + Duration::hours(24),
//!     Utc::now() + Duration::hours(26),
//!     nodes,
//! );
//!
//! let id = scheduler.schedule_maintenance(window).await.unwrap();
//! # }
//! ```
//!
//! # Integration with Job Scheduler
//!
//! The maintenance system integrates with the job scheduler to:
//!
//! 1. Prevent scheduling tasks on nodes in maintenance
//! 2. Preemptively migrate tasks before scheduled maintenance
//! 3. Track node availability for capacity planning
//!
//! Use `MaintenanceScheduler::drain_manager()` to check node exclusion:
//!
//! ```no_run
//! # use marabunta_compute::maintenance::MaintenanceScheduler;
//! # use marabunta_compute::common::WorkerId;
//! # fn example(scheduler: &MaintenanceScheduler, node_id: &WorkerId) {
//! if scheduler.drain_manager().is_excluded(node_id) {
//!     // Don't schedule tasks on this node
//! }
//! # }
//! ```

pub mod drain;
pub mod scheduler;
pub mod types;

// Re-export main types
pub use drain::{
    DrainCoordinator, DrainEvent, DrainPolicy, DrainRequest,
    NodeDrainManager, TaskMigrator,
};
pub use types::{DrainInfo, DrainStatus};

pub use scheduler::{
    MaintenanceEvent, MaintenanceScheduler, MaintenanceSchedulerConfig, MaintenanceSchedulerHandle,
};
pub use types::{
    MaintenanceError, MaintenanceState, MaintenanceSummary, MaintenanceType, MaintenanceWindow,
    MaintenanceWindowId,
};
