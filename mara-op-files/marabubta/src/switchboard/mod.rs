// Marabunta - Licensed under the MIT License.
//! Switchboard module for graduated failure handling and mathematical expression routing.
//!
//! The switchboard provides a 7-level failure hierarchy (L0-L6) and routes
//! mathematical expressions through multiple oracle backends with intelligent fallback.

pub mod api;
pub mod classify;
pub mod config;
pub mod errors;
pub mod failure;
pub mod handlers;
pub mod ingest;
pub mod oracles;
pub mod pipeline;
pub mod security;
pub mod session;
pub mod types;

pub use api::{create_switchboard_router, SwitchboardApiState};
pub use classify::{analyze_structure, classify, extract_symbols, ExpressionStructure};
pub use config::*;
pub use errors::SwitchboardError;
pub use ingest::{detect_ambiguities, ingest, parse_latex, parse_plain_text, MathpixClient};
pub use failure::{FailureAttempt, FailureContext, FailureLevel, FailureResponse, Suggestion};
pub use pipeline::{default_registry, execute_handler, process_expression, ExecuteResponse, PipelineResponse};
pub use session::{SessionState, SessionStore, SwitchboardSession};
pub use types::*;
