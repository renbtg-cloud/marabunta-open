// Marabunta - Licensed under the MIT License.
//! Integration tests for Marabunta Compute
//!
//! This module contains integration tests that verify the behavior of
//! multiple components working together, including:
//!
//! - Coordinator startup and API operations
//! - Job submission and status tracking
//! - Node registration and management
//! - Complete job lifecycle (submission, scheduling, execution, completion)
//! - Task dependencies and DAG execution
//! - Job cancellation and failure handling
//! - Master-worker task execution flow

pub mod coordinator;
#[cfg(test)]
pub mod android;
#[cfg(test)]
pub mod job_lifecycle;
#[cfg(test)]
pub mod master_worker;
#[cfg(test)]
pub mod swarm;
#[cfg(test)]
pub mod workflow;
#[cfg(test)]
pub mod organic_swarm;
#[cfg(test)]
pub mod admission;
#[cfg(test)]
pub mod stress_1000;
#[cfg(test)]
pub mod chaos;
#[cfg(test)]
pub mod management;
#[cfg(test)]
pub mod combo_infra;
#[cfg(test)]
pub mod helpers;
#[cfg(test)]
pub mod byzantine;
#[cfg(test)]
pub mod stress;
#[cfg(test)]
pub mod sandbox_escape;
#[cfg(test)]
pub mod protocol_correctness;
#[cfg(test)]
// pub mod marabunta;
#[cfg(test)]
pub mod neuromancer;
#[cfg(test)]
pub mod postgres;
#[cfg(test)]
pub mod config_compliance;
#[cfg(test)]
pub mod observe;
