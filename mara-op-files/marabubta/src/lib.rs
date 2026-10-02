// Marabunta - Licensed under the MIT License.
//! Marabunta Compute - Low-Budget Massive Parallel Computing Framework
//!
//! A distributed computing system designed to run millions of jobs
//! across thousands of cheap, unreliable machines with full fault tolerance.
//!
//! # Modules
//!
//! - [`common`] - Shared types and utilities
//! - [`coordinator`] - Cluster coordination with Raft consensus
//! - [`control_plane`] - Control plane monitoring, metrics, and observability
//! - [`master`] - Regional master nodes
//! - [`worker`] - Task execution workers
//! - [`protocol`] - Network protocol definitions
//! - [`storage`] - Distributed storage
//! - [`infrastructure`] - Infrastructure node management for monitored compute resources
//! - [`phantom`] - Phantom Protocol for anonymous BYOD participation
//! - [`wan`] - WAN bootstrap server and NAT traversal for internet-scale discovery
//! - [`cli`] - Command-line interface for job submission and management
//! - [`sdk`] - C ABI Task SDK for cross-language distributed job execution
//! - [`placement`] - Tags and groups system for job placement and node organization
//! - [`governance`] - Principal hierarchy, authority delegation, and override policies
//! - [`preemption`] - Preemption engine for job placement with priority-based task scheduling
//! - [`quotas`] - Quotas and resource budgets for job placement and resource management
//! - [`failure`] - Comprehensive failure handling and graceful degradation
//! - [`policy`] - Policy IR and evaluation engine for job placement
//! - [`config`] - Configuration management with TOML support and env overrides
//! - [`error`] - Unified error handling with error codes and actionable suggestions
//! - [`metrics`] - Prometheus metrics export for cluster observability
//! - [`maintenance`] - Maintenance window scheduling, node draining, and planned downtime
//! - [`security`] - TLS, authentication, and node-to-node security
//! - [`switchboard`] - Math expression intake, classification, and compute routing
//! - [`ratelimit`] - Rate limiting for API endpoints with token bucket algorithm
//! - [`backup`] - Backup and recovery for cluster state with scheduling and retention
//! - [`workflow`] - Multi-job workflow engine with conditional logic and parallel execution
//! - [`tenancy`] - Multi-tenancy isolation with resource, network, and data separation
//! - [`web_ui`] - Web-based dashboard for cluster monitoring and job management
//! - [`logging`] - Structured logging with JSON format, trace correlation, and sampling
//! - [`health`] - Kubernetes-style health probes, cluster status, and profiling endpoints
//! - [`alerting`] - Alerting system with rules, sinks, silences, and inhibitions
//! - [`compression`] - Multi-algorithm compression with streaming support and HTTP integration
//! - [`tracing`] - Distributed tracing with W3C Trace Context, Jaeger export, and Axum middleware
//! - [`batching`] - Request batching with coalescing for efficient batch operations
//! - [`cache`] - Caching layer with LRU, TTL, two-tier, and specialized caches
//! - [`pool`] - Connection pooling with health checks, metrics, and backpressure handling
//! - [`safety`] - Cross-platform thermal monitoring and hardware safety

pub mod alerting;
pub mod backup;
pub mod batching;
pub mod cache;
pub mod marabunta;
pub mod cli;
pub mod compression;
pub mod common;
pub mod config;
pub mod error;
pub mod failure;
pub mod governance;
pub mod health;
pub mod infrastructure;
pub mod logging;
pub mod maintenance;
pub mod metrics;
pub mod phantom;
pub mod placement;
pub mod plugin;
pub mod policy;
pub mod pool;
pub mod preemption;
pub mod protocol;
pub mod quotas;
pub mod ratelimit;
pub mod safety;
pub mod sdk;
pub mod security;
pub mod storage;
pub mod switchboard;
pub mod swarm;
pub mod tenancy;
pub mod tracing;
pub mod wan;
pub mod web_ui;
pub mod workflow;
pub mod highestsec;

#[cfg(test)]
mod integration_tests;

pub use common::*;

pub mod api;
pub mod chaos;
