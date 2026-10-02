// Marabunta - Licensed under the MIT License.
//! L4 State Machine Workflow Engine — foundation types.
//!
//! This module defines the type vocabulary for **state-machine workflows**
//! as specified in S19 of the Marabunta Swarm architecture. It is entirely
//! separate from the DAG-based workflow engine at `src/workflow/`, which
//! handles sequential/parallel job orchestration.
//!
//! State machine workflows model named states with guarded transitions,
//! human/system actors, scheduled triggers, lifecycle hooks, and full
//! audit trail integration (L3). They are designed for plugin authors to
//! define document approval chains, compliance workflows, financial
//! governance processes, and similar business process automation.

pub mod types;
pub mod parser;
pub mod conditions;
pub mod lock;
pub mod runtime;
pub mod actions;
pub mod triggers;
pub mod timers;
pub mod hooks;
pub mod api;
pub mod plugin;

pub use types::*;
