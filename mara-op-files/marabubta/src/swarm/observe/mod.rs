// Marabunta - Licensed under the MIT License.
//! Observe-and-Interfere (OAI) — thin orchestration over existing swarm infrastructure.
//!
//! This module provides operators with structured observation of swarm state
//! and guarded, audited intervention capabilities. It adds no new distributed
//! primitives — all state comes from existing subsystems (KnowledgeStore,
//! EventBus, FleetManager, etc.) and all mutations route through existing
//! subsystem APIs.

pub mod types;
pub mod errors;
pub mod config;
pub mod operator;
pub mod subscription;
pub mod views;
pub mod bridge;
pub mod guard;
pub mod staleness;
pub mod lock;
pub mod impact;
pub mod rollback;
pub mod engine;
pub mod veto;
pub mod tier1;
pub mod tier2;
pub mod tier3;
pub mod tier4;
pub mod tier5;
pub mod tier6;
pub mod pg;
pub mod session;
pub mod api_observe;
pub mod api_intervene;
pub mod api_operator;
pub mod cli;
pub mod cli_observe;
pub mod cli_intervene;
pub mod cli_operator;
pub mod api_audit;
