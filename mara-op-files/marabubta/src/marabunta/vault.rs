// Marabunta - Licensed under the MIT License.
//! Programmable Vault API for administrative command authorization.
//!
//! Provides an N-of-M multi-signature verification engine where the
//! authorization logic (intervals, geographic constraints, etc.) is
//! user-programmable via WASM or Rust modules.

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use crate::marabunta::identity::NodeId;
use thiserror::Error;

/// Errors related to administrative command authorization.
#[derive(Debug, Error)]
pub enum VaultError {
    #[error("insufficient signatures: required {required}, found {found}")]
    InsufficientSignatures { required: usize, found: usize },

    #[error("invalid signature for node {0}")]
    InvalidSignature(NodeId),

    #[error("temporal constraint violation: {0}")]
    TemporalViolation(String),

    #[error("logic rule violation: {0}")]
    LogicViolation(String),

    #[error("unauthorized command: {0}")]
    Unauthorized(String),
}

/// Supported administrative commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum AdminCommand {
    /// Forcibly shut down the swarm node.
    Kill,
    /// Change the system-wide tracing level.
    SetLogLevel { level: String },
    /// Blacklist a specific IP block or NodeId.
    Isolate { target: String, reason: String },
    /// Update the programmable vault logic itself.
    UpdateVaultLogic { new_logic_hash: [u8; 32] },
    /// Reset the Dead Man's Switch TTL.
    KeepGoing { next_expiry: SystemTime },
    /// Immediate system-wide halt and memory purge (Sleeper Protocol).
    Extinction,
    /// Authorize a diplomatic treaty with another federation.
    SignTreaty { treaty_id: String, partner: crate::marabunta::identity::FederationId },
    /// Join or create a computational coalition (Wolf Pack).
    FormCoalition { 
        coalition_id: String, 
        members: Vec<crate::marabunta::identity::FederationId>,
        associated_topology: Option<crate::common::types::TopologyId>
    },
}

/// An administrative action bundle containing the command and its authorizing signatures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminAction {
    /// The command to be executed.
    pub command: AdminCommand,
    /// List of signatures: (SignerNodeId, SignatureBytes, Timestamp).
    pub signatures: Vec<(NodeId, Vec<u8>, SystemTime)>,
}

/// Trait for programmable vault logic.
///
/// Users can implement this trait to define secret, refined rules for
/// administrative authorization (e.g. "Signature 2 must be exactly 14s after Signature 1").
pub trait VaultLogic: Send + Sync {
    /// Verify if the given action satisfies the programmable rules.
    fn verify_action(&self, action: &AdminAction) -> Result<(), VaultError>;
}

/// A default, configurable Multi-Sig implementation.
pub struct DefaultVaultLogic {
    pub required_signatures: usize,
    pub min_interval: Duration,
    pub max_interval: Duration,
}

impl VaultLogic for DefaultVaultLogic {
    fn verify_action(&self, action: &AdminAction) -> Result<(), VaultError> {
        if action.signatures.len() < self.required_signatures {
            return Err(VaultError::InsufficientSignatures {
                required: self.required_signatures,
                found: action.signatures.len(),
            });
        }

        // Logic Rule: Check intervals between signatures if more than one is provided
        if action.signatures.len() > 1 {
            let mut last_ts = action.signatures[0].2;
            for i in 1..action.signatures.len() {
                let current_ts = action.signatures[i].2;
                let delta = current_ts.duration_since(last_ts).map_err(|_| {
                    VaultError::TemporalViolation("Signatures must be chronologically ordered".to_string())
                })?;

                if delta < self.min_interval || delta > self.max_interval {
                    return Err(VaultError::TemporalViolation(format!(
                        "Interval between signature {} and {} is {:?}, must be between {:?} and {:?}",
                        i, i + 1, delta, self.min_interval, self.max_interval
                    )));
                }
                last_ts = current_ts;
            }
        }

        Ok(())
    }
}
