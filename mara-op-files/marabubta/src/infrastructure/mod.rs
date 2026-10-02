// Marabunta - Licensed under the MIT License.
//! Infrastructure Node Management Module
//!
//! This module handles fully-monitored, accountable compute nodes including:
//! - Servers and dedicated machines
//! - Corporate workstations
//! - High-SLA production infrastructure
//!
//! Key features:
//! - Comprehensive hardware specification tracking
//! - Real-time metrics collection and anomaly detection
//! - Health monitoring with configurable alerting
//! - Capability-based node discovery
//! - Full audit trail for compliance
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::infrastructure::{
//!     NodeRegistry, HealthChecker, AuditLog, InMemoryAuditStorage,
//!     InfrastructureNode, HardwareSpecs, Location, SlaTier,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create the registry
//!     let registry = NodeRegistry::new();
//!
//!     // Set up health monitoring
//!     let (alert_tx, alert_rx) = tokio::sync::mpsc::channel(100);
//!     let health_checker = HealthChecker::new(Default::default(), alert_tx);
//!
//!     // Set up audit logging
//!     let audit_storage = InMemoryAuditStorage::new();
//!     let audit_log = AuditLog::new(Box::new(audit_storage));
//! }
//! ```

pub mod audit;
pub mod health;
pub mod metrics;
pub mod node;
pub mod registry;

// Re-export main types for convenience
pub use audit::{
    AuditError, AuditEvent, AuditFilter, AuditLog, AuditStorage, FileAuditStorage,
    InMemoryAuditStorage,
};
pub use health::{Alert, AlertSeverity, HealthChecker, HealthConfig, HealthState};
pub use metrics::{
    AggregatedMetrics, GpuMetrics, MetricsAggregator, MetricsAnomaly, NodeMetrics, TimeWindow,
};
pub use node::{
    Department, HardwareSpecs, InfrastructureNode, Location, NodeId, NodeStatus, SlaTier,
    TaskHistory,
};
pub use registry::{Capability, NodeQuery, NodeRegistry, NodeSelection};
