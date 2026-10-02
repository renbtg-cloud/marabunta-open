// Marabunta - Licensed under the MIT License.
//! Darwin Auto-Remediation Engine: Event-Driven Failure Orchestration.
//!
//! This module provides the core abstractions for the Darwin engine, enabling
//! highly flexible, plugin-driven failure handling and auto-remediation.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, error};
use crate::marabunta::sandbox::CrashSnapshot;
use crate::swarm::neuromancer::types::{Anomaly, NeuromancerError, Predator, MarabuntaEvent};
use crate::swarm::neuromancer::bus::NeuromancerBus;

pub mod engine;
pub mod vcs;
pub mod ticketing;
pub mod inference;
pub mod archiver;
pub mod webhook;

pub use engine::DarwinEngine;

/// The universally standard payload passed down the remediation pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureContext {
    /// The raw WASM crash snapshot.
    pub snapshot: CrashSnapshot,
    /// The node where the failure occurred.
    pub node_id: String,
    /// The region or swarm context.
    pub region: String,
    /// Decoded stack trace (if available).
    pub stack_trace: Option<String>,
    /// Source file path where the crash occurred.
    pub source_file: Option<String>,
    /// Line number where the crash occurred.
    pub line_number: Option<u32>,
    /// The result of any LLM inference performed during the pipeline.
    pub inference_result: Option<String>,
    /// The URL or path to the archived time-travel dump.
    pub artifact_url: Option<String>,
    /// The ID of any generated ticket (e.g., JIRA-1234).
    pub ticket_id: Option<String>,
    /// Metadata for the remediation process.
    pub metadata: HashMap<String, String>,
}

impl FailureContext {
    pub fn new(snapshot: CrashSnapshot, node_id: String, region: String) -> Self {
        Self {
            snapshot,
            node_id,
            region,
            stack_trace: None,
            source_file: None,
            line_number: None,
            inference_result: None,
            artifact_url: None,
            ticket_id: None,
            metadata: HashMap::new(),
        }
    }
}

/// A decision made by a remediation hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookDecision {
    /// Continue to the next hook in the pipeline.
    Continue,
    /// Stop the remediation pipeline immediately.
    Stop,
}

/// The core abstraction for a Darwin remediation plugin.
#[async_trait]
pub trait RemediationHook: Send + Sync {
    /// The unique name of the hook.
    fn name(&self) -> &'static str;

    /// Called when a failure is detected to perform a specific remediation step.
    async fn on_failure(&self, context: &mut FailureContext) -> Result<HookDecision, NeuromancerError>;
}

pub struct Darwin {
    bus: Arc<NeuromancerBus>,
    engine: DarwinEngine,
}

impl Darwin {
    pub fn new(bus: Arc<NeuromancerBus>, engine: DarwinEngine) -> Self {
        Self {
            bus,
            engine,
        }
    }

    /// Listen for PanicCaptured events on the Neuromancer bus and trigger remediation.
    pub async fn start_listening(&self) {
        let mut rx = self.bus.subscribe();
        info!("Darwin: Listening for swarm panic events...");

        while let Ok(event) = rx.recv().await {
            if let MarabuntaEvent::PanicCaptured { node, snapshot, .. } = event {
                info!(node_id = %node, "Darwin: Intercepted boundary panic. Commencing remediation pipeline...");
                
                // Trigger the engine to handle the failure.
                // In a real scenario, we'd determine the region from the node context.
                if let Err(e) = self.engine.handle_failure(snapshot, node.to_string(), "unknown-region".into()).await {
                    error!("Darwin: Remediation pipeline failed for node {}: {}", node, e);
                }
            }
        }
    }
}

#[async_trait]
impl Predator for Darwin {
    fn name(&self) -> &'static str { "Darwin" }
    async fn analyze(&self) -> Result<Option<Anomaly>, NeuromancerError> {
        // Darwin is reactive and event-driven via start_listening()
        Ok(None)
    }
}
