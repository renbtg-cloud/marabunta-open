// Marabunta - Licensed under the MIT License.
//! Pillar 13.1: Justice System (Court of Arbitration)
//!
//! Inter-swarm agreement negotiation, capacity lending, and constellation mapping.
//! Enforces on-chain resolution of execution disputes. Slashing of staked 
//! MMX collateral is managed via Juries when nodes falsify JCL execution results.
//!
//! # Lifecycle
//!
//! 1. Swarm A drafts a agreement and sends it as a **Proposal** to Swarm B.
//! 2. Swarm B may accept, reject, or counter-propose with modifications.
//! 3. Once both parties accept, each signs the agreement (SHA-256 of a canonical
//!    payload). The agreement becomes **Active**.
//! 4. While active, the [`LendingMeter`] tracks resource loans between the
//!    swarms and accumulates billing records.
//! 5. Either party may suspend, amend, or terminate the agreement through the
//!    [`AgreementNegotiation`] state machine.
//!
//! # Constellation
//!
//! The [`ConstellationBuilder`] aggregates knowledge about all known swarms
//! and their treaties into a [`ConstellationView`], which can be rendered as
//! a graph (using `petgraph`) for visualization or pathfinding.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use petgraph::algo::dijkstra;
use petgraph::graph::{DiGraph, NodeIndex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use uuid::Uuid;


// ============================================================================
// SwarmId  (locally defined -- sovereignty module does not exist yet)
// ============================================================================

/// Unique identifier for an independent swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SwarmId(pub Uuid);

impl SwarmId {
    /// Generate a new random swarm identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SwarmId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SwarmId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "swarm-{}", &self.0.to_string()[..8])
    }
}

// ============================================================================
// PermeabilityRule  (locally defined -- membrane module does not exist yet)
// ============================================================================

/// A rule governing what may cross the membrane between two swarms.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PermeabilityRule {
    /// Human-readable name for this rule.
    pub name: String,
    /// Whether the rule allows or denies the crossing.
    pub allow: bool,
    /// Resource type this rule applies to (e.g. "jobs", "data", "gossip").
    pub resource_type: String,
    /// Optional condition expression (evaluated contextually).
    pub condition: Option<String>,
}

// ============================================================================
// SeveranceConditions  (locally defined -- membrane module does not exist yet)
// ============================================================================

/// Conditions under which a agreement or membrane should be automatically severed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SeveranceConditions {
    /// Sever if the remote swarm has been unreachable for this many seconds.
    pub unreachable_timeout_secs: u64,
    /// Sever if the violation count exceeds this threshold.
    pub max_violations: u32,
    /// Sever if trust score drops below this value (0.0 - 1.0).
    pub min_trust_score: f32,
    /// Whether automatic severance is enabled at all.
    pub auto_sever: bool,
}

impl Default for SeveranceConditions {
    fn default() -> Self {
        Self {
            unreachable_timeout_secs: 3600,
            max_violations: 10,
            min_trust_score: 0.3,
            auto_sever: true,
        }
    }
}

// ============================================================================
// SwarmSummary  (locally defined -- sovereignty module does not exist yet)
// ============================================================================

/// Summary information about a swarm, used for constellation building.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmSummary {
    /// Unique identifier of the swarm.
    pub id: SwarmId,
    /// Human-readable name.
    pub name: String,
    /// Number of alive nodes.
    pub alive_nodes: usize,
    /// Total compute capacity (cores).
    pub total_cores: u32,
    /// Total memory capacity (MB).
    pub total_memory_mb: u64,
}

// ============================================================================
// PsycheFacets  (locally defined -- used for lending evaluation)
// ============================================================================

/// A simplified view of swarm psyche facets used for lending policy evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsycheFacets {
    /// Resilience level (0-100).
    pub resilience: u8,
    /// Vitality level (0-100).
    pub vitality: u8,
    /// Currently active archetype names.
    pub active_archetypes: Vec<String>,
}

// ============================================================================
// CrossingLog  (locally defined -- membrane module does not exist yet)
// ============================================================================

/// Log of membrane crossings between swarms.
#[derive(Debug, Clone)]
pub struct CrossingLog {
    /// Entries: (from_swarm, to_swarm, timestamp, resource_type).
    entries: Vec<CrossingEntry>,
}

/// A single crossing log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossingEntry {
    /// Source swarm.
    pub from: SwarmId,
    /// Destination swarm.
    pub to: SwarmId,
    /// When the crossing occurred.
    pub timestamp: DateTime<Utc>,
    /// Type of resource that crossed.
    pub resource_type: String,
}

impl CrossingLog {
    /// Create a new empty crossing log.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Record a crossing.
    pub fn record(&mut self, from: SwarmId, to: SwarmId, resource_type: String) {
        self.entries.push(CrossingEntry {
            from,
            to,
            timestamp: Utc::now(),
            resource_type,
        });
    }

    /// Return all entries.
    pub fn entries(&self) -> &[CrossingEntry] {
        &self.entries
    }

    /// Return the count of crossings between two specific swarms.
    pub fn count_between(&self, a: SwarmId, b: SwarmId) -> usize {
        self.entries
            .iter()
            .filter(|e| {
                (e.from == a && e.to == b) || (e.from == b && e.to == a)
            })
            .count()
    }
}

impl Default for CrossingLog {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// AgreementId
// ============================================================================

/// Unique identifier for a agreement between two swarms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct AgreementId(pub Uuid);

impl AgreementId {
    /// Generate a new random agreement identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AgreementId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for AgreementId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "agreement-{}", &self.0.to_string()[..8])
    }
}

// ============================================================================
// BillingModel
// ============================================================================

/// How capacity lending is billed between agreement parties.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum BillingModel {
    /// No charge -- capacity is shared freely.
    #[default]
    Free,
    /// Tit-for-tat: lending is repaid in kind.
    Reciprocal,
    /// Usage-based metering at configurable rates.
    Metered,
    /// A fixed fee regardless of usage.
    Fixed,
}


impl fmt::Display for BillingModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BillingModel::Free => write!(f, "free"),
            BillingModel::Reciprocal => write!(f, "reciprocal"),
            BillingModel::Metered => write!(f, "metered"),
            BillingModel::Fixed => write!(f, "fixed"),
        }
    }
}

// ============================================================================
// LendingBilling
// ============================================================================

/// Billing parameters for capacity lending.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LendingBilling {
    /// The billing model to use.
    pub model: BillingModel,
    /// Rate per core-hour (in abstract billing units).
    pub rate_per_core_hour: f64,
    /// Rate per GB-hour (in abstract billing units).
    pub rate_per_gb_hour: f64,
    /// Minimum charge in seconds (prevents micro-billing).
    pub minimum_charge_secs: u64,
}

impl Default for LendingBilling {
    fn default() -> Self {
        Self {
            model: BillingModel::Free,
            rate_per_core_hour: 0.0,
            rate_per_gb_hour: 0.0,
            minimum_charge_secs: 60,
        }
    }
}

// ============================================================================
// LendingDataPolicy
// ============================================================================

/// Data handling policy for capacity lending sessions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LendingDataPolicy {
    /// Whether the borrower may cache data from the lender.
    pub allow_data_caching: bool,
    /// Whether data in transit and at rest must be encrypted.
    pub require_encryption: bool,
    /// Whether data residency compliance must be verified.
    pub require_residency_compliance: bool,
    /// Maximum data retention in seconds after the session ends.
    pub max_data_retention_secs: Option<u64>,
}

impl Default for LendingDataPolicy {
    fn default() -> Self {
        Self {
            allow_data_caching: false,
            require_encryption: true,
            require_residency_compliance: false,
            max_data_retention_secs: Some(3600),
        }
    }
}

// ============================================================================
// LendingPolicy
// ============================================================================

/// Policy governing how capacity is lent and borrowed under a agreement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LendingPolicy {
    /// Whether lending is enabled at all.
    pub enabled: bool,
    /// Maximum percentage of local capacity that may be lent out (0-100).
    pub max_lend_pct: u8,
    /// Maximum percentage of capacity that may be borrowed (0-100).
    pub max_borrow_pct: u8,
    /// Minimum local resilience facet value required before lending is allowed.
    pub min_local_resilience: u8,
    /// Minimum local vitality facet value required before lending is allowed.
    pub min_local_vitality: u8,
    /// Archetype names during which lending is blocked.
    pub block_during_archetypes: Vec<String>,
    /// Billing parameters.
    pub billing: LendingBilling,
    /// Data handling policy.
    pub data_policy: LendingDataPolicy,
    /// Optional time limit per lending session in seconds.
    pub time_limit_secs: Option<u64>,
}

impl Default for LendingPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            max_lend_pct: 20,
            max_borrow_pct: 30,
            min_local_resilience: 50,
            min_local_vitality: 50,
            block_during_archetypes: Vec::new(),
            billing: LendingBilling::default(),
            data_policy: LendingDataPolicy::default(),
            time_limit_secs: None,
        }
    }
}

// ============================================================================
// AgreementStatus
// ============================================================================

/// Lifecycle status of a agreement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum AgreementStatus {
    /// Agreement is being drafted, not yet proposed.
    Draft,
    /// Agreement has been proposed by one party.
    Proposed {
        /// The swarm that proposed the agreement.
        by: SwarmId,
    },
    /// Agreement has been counter-proposed with modifications.
    CounterProposed {
        /// The swarm that counter-proposed.
        by: SwarmId,
    },
    /// Both parties have accepted the agreement terms.
    Accepted,
    /// Agreement is active and in force.
    Active,
    /// Agreement has been temporarily suspended.
    Suspended {
        /// Reason for suspension.
        reason: String,
        /// Who suspended it.
        by: SwarmId,
    },
    /// Agreement has expired past its `expires_at` date.
    Expired,
    /// Agreement has been permanently terminated.
    Terminated {
        /// Reason for termination.
        reason: String,
        /// Who terminated it.
        by: SwarmId,
    },
}

impl fmt::Display for AgreementStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AgreementStatus::Draft => write!(f, "draft"),
            AgreementStatus::Proposed { by } => write!(f, "proposed({})", by),
            AgreementStatus::CounterProposed { by } => write!(f, "counter_proposed({})", by),
            AgreementStatus::Accepted => write!(f, "accepted"),
            AgreementStatus::Active => write!(f, "active"),
            AgreementStatus::Suspended { reason, by } => {
                write!(f, "suspended({}, {})", by, reason)
            }
            AgreementStatus::Expired => write!(f, "expired"),
            AgreementStatus::Terminated { reason, by } => {
                write!(f, "terminated({}, {})", by, reason)
            }
        }
    }
}

// ============================================================================
// AgreementAmendment
// ============================================================================

/// A recorded change to a agreement's terms.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgreementAmendment {
    /// The version number this amendment produced.
    pub version: u64,
    /// Which swarm proposed the amendment.
    pub proposed_by: SwarmId,
    /// Human-readable description of what changed.
    pub changes: String,
    /// When the amendment was proposed.
    pub timestamp: DateTime<Utc>,
    /// Whether the other party accepted the amendment.
    pub accepted: bool,
}

// ============================================================================
// Agreement
// ============================================================================

/// A signed agreement between two swarms governing their cooperation.
///
/// Treaties are versioned and immutable once active. Modifications go through
/// the amendment process, which creates a new version. Both parties must sign
/// the agreement for it to become active.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agreement {
    /// Unique identifier.
    pub id: AgreementId,
    /// Version counter, incremented on each amendment.
    pub version: u64,
    /// First party to the agreement.
    pub party_a: SwarmId,
    /// Second party to the agreement.
    pub party_b: SwarmId,
    /// Human-readable agreement name.
    pub name: String,
    /// Longer description of the agreement's purpose.
    pub description: String,
    /// Rules governing what may cross the membrane between the two swarms.
    pub permeability: Vec<PermeabilityRule>,
    /// Conditions for automatic severance.
    pub severance: SeveranceConditions,
    /// Capacity lending policy.
    pub lending: LendingPolicy,
    /// Current lifecycle status.
    pub status: AgreementStatus,
    /// Signature from party A (SHA-256 of the signing payload).
    pub signed_by_a: Option<Vec<u8>>,
    /// Signature from party B.
    pub signed_by_b: Option<Vec<u8>>,
    /// When the agreement becomes effective.
    pub effective_at: Option<DateTime<Utc>>,
    /// When the agreement expires.
    pub expires_at: Option<DateTime<Utc>>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp.
    pub updated_at: DateTime<Utc>,
    /// History of amendments to this agreement.
    pub amendment_history: Vec<AgreementAmendment>,
}

impl Agreement {
    /// Validate the agreement's internal consistency.
    ///
    /// Returns a list of validation errors (empty if valid).
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.name.trim().is_empty() {
            errors.push("agreement name must not be empty".to_string());
        }

        if self.party_a == self.party_b {
            errors.push("party_a and party_b must be different swarms".to_string());
        }

        if self.lending.max_lend_pct > 100 {
            errors.push(format!(
                "max_lend_pct ({}) exceeds 100",
                self.lending.max_lend_pct
            ));
        }

        if self.lending.max_borrow_pct > 100 {
            errors.push(format!(
                "max_borrow_pct ({}) exceeds 100",
                self.lending.max_borrow_pct
            ));
        }

        if let (Some(eff), Some(exp)) = (self.effective_at, self.expires_at) {
            if exp <= eff {
                errors.push("expires_at must be after effective_at".to_string());
            }
        }

        if self.version == 0 {
            errors.push("version must be >= 1".to_string());
        }

        errors
    }

    /// Whether the agreement is currently active.
    pub fn is_active(&self) -> bool {
        matches!(self.status, AgreementStatus::Active)
    }

    /// Whether the agreement has expired based on its `expires_at` field.
    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires_at {
            Utc::now() >= expires
        } else {
            false
        }
    }

    /// Number of days until the agreement expires, or `None` if no expiry is set.
    pub fn days_until_expiry(&self) -> Option<i64> {
        self.expires_at.map(|exp| {
            let delta = exp - Utc::now();
            delta.num_days()
        })
    }

    /// Compute the canonical signing payload for this agreement.
    ///
    /// The payload is a deterministic byte representation of the agreement's
    /// core fields (excluding signatures and timestamps that change after
    /// signing). Both parties must agree on this payload before signing.
    pub fn signing_payload(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(self.id.0.as_bytes());
        payload.extend_from_slice(&self.version.to_be_bytes());
        payload.extend_from_slice(self.party_a.0.as_bytes());
        payload.extend_from_slice(self.party_b.0.as_bytes());
        payload.extend_from_slice(self.name.as_bytes());
        payload.extend_from_slice(self.description.as_bytes());

        // Include permeability rules in canonical order.
        for rule in &self.permeability {
            payload.extend_from_slice(rule.name.as_bytes());
            payload.push(if rule.allow { 1 } else { 0 });
            payload.extend_from_slice(rule.resource_type.as_bytes());
        }

        // Include lending policy essentials.
        payload.push(if self.lending.enabled { 1 } else { 0 });
        payload.push(self.lending.max_lend_pct);
        payload.push(self.lending.max_borrow_pct);

        payload
    }

    /// Verify that both signatures match the signing payload.
    ///
    /// `key_a` and `key_b` are the expected signing keys for party A and
    /// party B respectively. In this implementation, a valid signature is
    /// the SHA-256 hash of `(signing_payload || key)`.
    pub fn verify_signatures(&self, key_a: &[u8], key_b: &[u8]) -> bool {
        let payload = self.signing_payload();

        let expected_a = {
            let mut hasher = Sha256::new();
            hasher.update(&payload);
            hasher.update(key_a);
            hasher.finalize().to_vec()
        };

        let expected_b = {
            let mut hasher = Sha256::new();
            hasher.update(&payload);
            hasher.update(key_b);
            hasher.finalize().to_vec()
        };

        let sig_a_ok = self
            .signed_by_a
            .as_ref() == Some(&expected_a);
        let sig_b_ok = self
            .signed_by_b
            .as_ref() == Some(&expected_b);

        sig_a_ok && sig_b_ok
    }

    /// Compute a signature for the given key.
    ///
    /// Returns the SHA-256 hash of `(signing_payload || key)`.
    pub fn compute_signature(&self, key: &[u8]) -> Vec<u8> {
        let payload = self.signing_payload();
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        hasher.update(key);
        hasher.finalize().to_vec()
    }
}

/// Create a new draft agreement with sensible defaults.
pub fn new_draft_agreement(
    party_a: SwarmId,
    party_b: SwarmId,
    name: String,
    description: String,
) -> Agreement {
    let now = Utc::now();
    Agreement {
        id: AgreementId::new(),
        version: 1,
        party_a,
        party_b,
        name,
        description,
        permeability: Vec::new(),
        severance: SeveranceConditions::default(),
        lending: LendingPolicy::default(),
        status: AgreementStatus::Draft,
        signed_by_a: None,
        signed_by_b: None,
        effective_at: None,
        expires_at: None,
        created_at: now,
        updated_at: now,
        amendment_history: Vec::new(),
    }
}

// ============================================================================
// NegotiationAction
// ============================================================================

/// An action that can be taken during agreement negotiation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum NegotiationAction {
    /// Propose a new agreement.
    Propose,
    /// Counter-propose with modifications.
    CounterPropose,
    /// Accept the current terms.
    Accept,
    /// Reject the agreement entirely.
    Reject,
    /// Sign the agreement.
    Sign,
    /// Suspend an active agreement.
    Suspend { reason: String },
    /// Resume a suspended agreement.
    Resume,
    /// Terminate a agreement.
    Terminate { reason: String },
    /// Amend specific terms.
    Amend { changes: String },
}

impl fmt::Display for NegotiationAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NegotiationAction::Propose => write!(f, "propose"),
            NegotiationAction::CounterPropose => write!(f, "counter_propose"),
            NegotiationAction::Accept => write!(f, "accept"),
            NegotiationAction::Reject => write!(f, "reject"),
            NegotiationAction::Sign => write!(f, "sign"),
            NegotiationAction::Suspend { .. } => write!(f, "suspend"),
            NegotiationAction::Resume => write!(f, "resume"),
            NegotiationAction::Terminate { .. } => write!(f, "terminate"),
            NegotiationAction::Amend { .. } => write!(f, "amend"),
        }
    }
}

// ============================================================================
// NegotiationMessage
// ============================================================================

/// A message exchanged during agreement negotiation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NegotiationMessage {
    /// Which swarm sent this message.
    pub from: SwarmId,
    /// The action being performed.
    pub action: NegotiationAction,
    /// Timestamp of the message.
    pub timestamp: DateTime<Utc>,
    /// Optional updated agreement (for propose/counter-propose/amend).
    pub agreement_snapshot: Option<Agreement>,
    /// Optional signing key for sign actions.
    pub signing_key: Option<Vec<u8>>,
}

// ============================================================================
// AgreementNegotiation
// ============================================================================

/// State machine for negotiating, signing, and managing a agreement.
///
/// Each negotiation maintains a reference to the agreement being negotiated
/// and a full message history. State transitions are validated: for example,
/// you cannot accept a agreement that has not been proposed, and you cannot
/// sign a agreement that has not been accepted.
#[derive(Debug, Clone)]
pub struct AgreementNegotiation {
    /// The agreement under negotiation.
    agreement: Agreement,
    /// Full history of negotiation messages.
    messages: Vec<NegotiationMessage>,
}

/// Errors that can occur during negotiation.
#[derive(Debug, Clone, PartialEq)]
pub struct NegotiationError {
    /// Human-readable error message.
    pub message: String,
}

impl fmt::Display for NegotiationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "negotiation error: {}", self.message)
    }
}

impl std::error::Error for NegotiationError {}

impl AgreementNegotiation {
    /// Create a new negotiation from a draft agreement.
    pub fn new(agreement: Agreement) -> Self {
        Self {
            agreement,
            messages: Vec::new(),
        }
    }

    /// Get a reference to the current agreement state.
    pub fn agreement(&self) -> &Agreement {
        &self.agreement
    }

    /// Get the full message history.
    pub fn messages(&self) -> &[NegotiationMessage] {
        &self.messages
    }

    /// Propose the agreement to the other party.
    ///
    /// Only valid when the agreement is in Draft status.
    pub fn propose(&mut self, by: SwarmId) -> Result<(), NegotiationError> {
        if !matches!(self.agreement.status, AgreementStatus::Draft) {
            return Err(NegotiationError {
                message: format!(
                    "cannot propose: agreement is in '{}' status, expected 'draft'",
                    self.agreement.status
                ),
            });
        }

        if by != self.agreement.party_a && by != self.agreement.party_b {
            return Err(NegotiationError {
                message: "proposer must be one of the agreement parties".to_string(),
            });
        }

        self.agreement.status = AgreementStatus::Proposed { by };
        self.agreement.updated_at = Utc::now();
        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::Propose,
            timestamp: Utc::now(),
            agreement_snapshot: Some(self.agreement.clone()),
            signing_key: None,
        });

        debug!(agreement = %self.agreement.id, by = %by, "agreement proposed");
        Ok(())
    }

    /// Counter-propose with a modified agreement.
    ///
    /// Valid when the agreement is Proposed or CounterProposed, and the counter
    /// must come from the party that did **not** make the last proposal.
    pub fn counter_propose(
        &mut self,
        by: SwarmId,
        modified: Agreement,
    ) -> Result<(), NegotiationError> {
        match &self.agreement.status {
            AgreementStatus::Proposed { by: proposer } => {
                if *proposer == by {
                    return Err(NegotiationError {
                        message: "cannot counter-propose your own proposal".to_string(),
                    });
                }
            }
            AgreementStatus::CounterProposed { by: proposer } => {
                if *proposer == by {
                    return Err(NegotiationError {
                        message: "cannot counter-propose your own counter-proposal".to_string(),
                    });
                }
            }
            other => {
                return Err(NegotiationError {
                    message: format!(
                        "cannot counter-propose: agreement is in '{}' status",
                        other
                    ),
                });
            }
        }

        self.agreement = modified;
        self.agreement.status = AgreementStatus::CounterProposed { by };
        self.agreement.version += 1;
        self.agreement.updated_at = Utc::now();
        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::CounterPropose,
            timestamp: Utc::now(),
            agreement_snapshot: Some(self.agreement.clone()),
            signing_key: None,
        });

        debug!(agreement = %self.agreement.id, by = %by, "agreement counter-proposed");
        Ok(())
    }

    /// Accept the current agreement terms.
    ///
    /// Valid when the agreement is Proposed or CounterProposed, and the acceptor
    /// must be the party that did **not** make the last proposal.
    pub fn accept(&mut self, by: SwarmId) -> Result<(), NegotiationError> {
        match &self.agreement.status {
            AgreementStatus::Proposed { by: proposer } => {
                if *proposer == by {
                    return Err(NegotiationError {
                        message: "cannot accept your own proposal".to_string(),
                    });
                }
            }
            AgreementStatus::CounterProposed { by: proposer } => {
                if *proposer == by {
                    return Err(NegotiationError {
                        message: "cannot accept your own counter-proposal".to_string(),
                    });
                }
            }
            other => {
                return Err(NegotiationError {
                    message: format!(
                        "cannot accept: agreement is in '{}' status",
                        other
                    ),
                });
            }
        }

        self.agreement.status = AgreementStatus::Accepted;
        self.agreement.updated_at = Utc::now();
        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::Accept,
            timestamp: Utc::now(),
            agreement_snapshot: None,
            signing_key: None,
        });

        debug!(agreement = %self.agreement.id, by = %by, "agreement accepted");
        Ok(())
    }

    /// Reject the agreement entirely.
    ///
    /// Valid in Proposed or CounterProposed status. Moves the agreement to Terminated.
    pub fn reject(&mut self, by: SwarmId, reason: String) -> Result<(), NegotiationError> {
        match &self.agreement.status {
            AgreementStatus::Proposed { .. } | AgreementStatus::CounterProposed { .. } => {}
            other => {
                return Err(NegotiationError {
                    message: format!(
                        "cannot reject: agreement is in '{}' status",
                        other
                    ),
                });
            }
        }

        self.agreement.status = AgreementStatus::Terminated {
            reason: reason.clone(),
            by,
        };
        self.agreement.updated_at = Utc::now();
        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::Reject,
            timestamp: Utc::now(),
            agreement_snapshot: None,
            signing_key: None,
        });

        info!(agreement = %self.agreement.id, by = %by, reason = %reason, "agreement rejected");
        Ok(())
    }

    /// Sign the agreement. Both parties must sign for the agreement to become active.
    ///
    /// Valid when the agreement is Accepted or when one party has already signed
    /// (still Accepted). Once both signatures are present, the agreement becomes
    /// Active.
    pub fn sign(&mut self, by: SwarmId, key: &[u8]) -> Result<(), NegotiationError> {
        if !matches!(self.agreement.status, AgreementStatus::Accepted) {
            return Err(NegotiationError {
                message: format!(
                    "cannot sign: agreement is in '{}' status, expected 'accepted'",
                    self.agreement.status
                ),
            });
        }

        let signature = self.agreement.compute_signature(key);

        if by == self.agreement.party_a {
            self.agreement.signed_by_a = Some(signature);
        } else if by == self.agreement.party_b {
            self.agreement.signed_by_b = Some(signature);
        } else {
            return Err(NegotiationError {
                message: "signer must be one of the agreement parties".to_string(),
            });
        }

        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::Sign,
            timestamp: Utc::now(),
            agreement_snapshot: None,
            signing_key: Some(key.to_vec()),
        });

        // If both parties have signed, activate the agreement.
        if self.agreement.signed_by_a.is_some() && self.agreement.signed_by_b.is_some() {
            self.agreement.status = AgreementStatus::Active;
            self.agreement.effective_at = Some(Utc::now());
            info!(agreement = %self.agreement.id, "agreement activated (both parties signed)");
        } else {
            debug!(agreement = %self.agreement.id, by = %by, "agreement signed by one party");
        }

        self.agreement.updated_at = Utc::now();
        Ok(())
    }

    /// Suspend an active agreement.
    pub fn suspend(&mut self, by: SwarmId, reason: String) -> Result<(), NegotiationError> {
        if !matches!(self.agreement.status, AgreementStatus::Active) {
            return Err(NegotiationError {
                message: format!(
                    "cannot suspend: agreement is in '{}' status, expected 'active'",
                    self.agreement.status
                ),
            });
        }

        if by != self.agreement.party_a && by != self.agreement.party_b {
            return Err(NegotiationError {
                message: "suspender must be one of the agreement parties".to_string(),
            });
        }

        self.agreement.status = AgreementStatus::Suspended {
            reason: reason.clone(),
            by,
        };
        self.agreement.updated_at = Utc::now();
        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::Suspend {
                reason: reason.clone(),
            },
            timestamp: Utc::now(),
            agreement_snapshot: None,
            signing_key: None,
        });

        warn!(agreement = %self.agreement.id, by = %by, reason = %reason, "agreement suspended");
        Ok(())
    }

    /// Resume a suspended agreement back to active status.
    pub fn resume(&mut self, by: SwarmId) -> Result<(), NegotiationError> {
        if !matches!(self.agreement.status, AgreementStatus::Suspended { .. }) {
            return Err(NegotiationError {
                message: format!(
                    "cannot resume: agreement is in '{}' status, expected 'suspended'",
                    self.agreement.status
                ),
            });
        }

        if by != self.agreement.party_a && by != self.agreement.party_b {
            return Err(NegotiationError {
                message: "resumer must be one of the agreement parties".to_string(),
            });
        }

        self.agreement.status = AgreementStatus::Active;
        self.agreement.updated_at = Utc::now();
        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::Resume,
            timestamp: Utc::now(),
            agreement_snapshot: None,
            signing_key: None,
        });

        info!(agreement = %self.agreement.id, by = %by, "agreement resumed");
        Ok(())
    }

    /// Terminate a agreement permanently.
    ///
    /// Valid in Active or Suspended status.
    pub fn terminate(&mut self, by: SwarmId, reason: String) -> Result<(), NegotiationError> {
        match &self.agreement.status {
            AgreementStatus::Active | AgreementStatus::Suspended { .. } => {}
            other => {
                return Err(NegotiationError {
                    message: format!(
                        "cannot terminate: agreement is in '{}' status",
                        other
                    ),
                });
            }
        }

        if by != self.agreement.party_a && by != self.agreement.party_b {
            return Err(NegotiationError {
                message: "terminator must be one of the agreement parties".to_string(),
            });
        }

        self.agreement.status = AgreementStatus::Terminated {
            reason: reason.clone(),
            by,
        };
        self.agreement.updated_at = Utc::now();
        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::Terminate {
                reason: reason.clone(),
            },
            timestamp: Utc::now(),
            agreement_snapshot: None,
            signing_key: None,
        });

        info!(agreement = %self.agreement.id, by = %by, reason = %reason, "agreement terminated");
        Ok(())
    }

    /// Amend an active agreement.
    ///
    /// Creates a new version and records the amendment in history. The
    /// amendment is marked as accepted immediately (caller should use the
    /// propose/accept cycle for contested amendments).
    pub fn amend(
        &mut self,
        by: SwarmId,
        changes: String,
        modified: Agreement,
    ) -> Result<(), NegotiationError> {
        if !matches!(self.agreement.status, AgreementStatus::Active) {
            return Err(NegotiationError {
                message: format!(
                    "cannot amend: agreement is in '{}' status, expected 'active'",
                    self.agreement.status
                ),
            });
        }

        if by != self.agreement.party_a && by != self.agreement.party_b {
            return Err(NegotiationError {
                message: "amender must be one of the agreement parties".to_string(),
            });
        }

        let new_version = self.agreement.version + 1;

        let amendment = AgreementAmendment {
            version: new_version,
            proposed_by: by,
            changes: changes.clone(),
            timestamp: Utc::now(),
            accepted: true,
        };

        self.agreement = modified;
        self.agreement.version = new_version;
        self.agreement.status = AgreementStatus::Active;
        self.agreement.updated_at = Utc::now();
        self.agreement.amendment_history.push(amendment);

        self.messages.push(NegotiationMessage {
            from: by,
            action: NegotiationAction::Amend {
                changes: changes.clone(),
            },
            timestamp: Utc::now(),
            agreement_snapshot: Some(self.agreement.clone()),
            signing_key: None,
        });

        info!(
            agreement = %self.agreement.id,
            by = %by,
            version = new_version,
            "agreement amended"
        );
        Ok(())
    }
}

// ============================================================================
// AgreementStore
// ============================================================================

/// Thread-safe store for treaties, with version history and query methods.
///
/// Backed by a [`DashMap`] for concurrent access. Each agreement is stored
/// with its full version history, enabling rollback and audit.
pub struct AgreementStore {
    /// Active treaties keyed by their ID.
    treaties: DashMap<AgreementId, Agreement>,
    /// Version history: agreement ID -> list of past versions.
    version_history: DashMap<AgreementId, Vec<Agreement>>,
}

impl AgreementStore {
    /// Create a new empty agreement store.
    pub fn new() -> Self {
        Self {
            treaties: DashMap::new(),
            version_history: DashMap::new(),
        }
    }

    /// Insert or update a agreement.
    ///
    /// If the agreement already exists with a lower version, the old version
    /// is archived in the version history.
    pub fn upsert(&self, agreement: Agreement) {
        let id = agreement.id;
        if let Some(existing) = self.treaties.get(&id) {
            if existing.version < agreement.version {
                let old = existing.clone();
                drop(existing);
                self.version_history
                    .entry(id)
                    .or_default()
                    .push(old);
                self.treaties.insert(id, agreement);
                debug!(agreement = %id, "agreement updated (new version)");
            }
            // If incoming version is not newer, ignore silently.
        } else {
            self.treaties.insert(id, agreement);
            debug!(agreement = %id, "agreement inserted");
        }
    }

    /// Get a agreement by its ID.
    pub fn get(&self, id: &AgreementId) -> Option<Agreement> {
        self.treaties.get(id).map(|r| r.value().clone())
    }

    /// Remove a agreement by its ID.
    pub fn remove(&self, id: &AgreementId) -> Option<Agreement> {
        self.treaties.remove(id).map(|(_, v)| v)
    }

    /// Find all treaties involving a specific swarm.
    pub fn find_by_swarm(&self, swarm_id: SwarmId) -> Vec<Agreement> {
        self.treaties
            .iter()
            .filter(|r| {
                let t = r.value();
                t.party_a == swarm_id || t.party_b == swarm_id
            })
            .map(|r| r.value().clone())
            .collect()
    }

    /// Find all active treaties.
    pub fn find_active(&self) -> Vec<Agreement> {
        self.treaties
            .iter()
            .filter(|r| r.value().is_active())
            .map(|r| r.value().clone())
            .collect()
    }

    /// Find treaties expiring within the given duration.
    pub fn find_expiring_within(&self, duration: Duration) -> Vec<Agreement> {
        let cutoff = Utc::now() + chrono::Duration::from_std(duration)
            .unwrap_or_else(|_| chrono::Duration::seconds(0));
        self.treaties
            .iter()
            .filter(|r| {
                if let Some(exp) = r.value().expires_at {
                    exp <= cutoff && r.value().is_active()
                } else {
                    false
                }
            })
            .map(|r| r.value().clone())
            .collect()
    }

    /// Get the version history for a agreement.
    pub fn get_versions(&self, id: &AgreementId) -> Vec<Agreement> {
        self.version_history
            .get(id)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    /// Total number of treaties in the store.
    pub fn count(&self) -> usize {
        self.treaties.len()
    }

    /// Return all treaties as a vector.
    pub fn all(&self) -> Vec<Agreement> {
        self.treaties
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }
}

impl Default for AgreementStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// LendingSession
// ============================================================================

/// An active capacity lending session between two swarms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LendingSession {
    /// Which swarm is lending capacity.
    pub lender: SwarmId,
    /// Which swarm is borrowing capacity.
    pub borrower: SwarmId,
    /// When the session started.
    pub started_at: DateTime<Utc>,
    /// What percentage of the lender's capacity is being lent.
    pub capacity_pct: u8,
    /// Number of CPU cores being lent.
    pub cores_lent: u32,
    /// Amount of memory being lent (MB).
    pub memory_lent_mb: u64,
    /// Accumulated cost so far (in billing units).
    pub accumulated_cost: f64,
    /// When the session should be auto-recalled (if set).
    pub auto_recall_at: Option<DateTime<Utc>>,
}

// ============================================================================
// BillingRecord
// ============================================================================

/// A completed billing record for a lending session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillingRecord {
    /// Which swarm lent the capacity.
    pub lender: SwarmId,
    /// Which swarm borrowed the capacity.
    pub borrower: SwarmId,
    /// The billing period (start, end).
    pub period: (DateTime<Utc>, DateTime<Utc>),
    /// Total core-hours consumed.
    pub core_hours: f64,
    /// Total GB-hours consumed.
    pub gb_hours: f64,
    /// Total cost (in billing units).
    pub total_cost: f64,
}

// ============================================================================
// LendingMeter
// ============================================================================

/// Tracks active lending sessions and billing records.
///
/// Thread-safe via [`DashMap`] for sessions and [`parking_lot::Mutex`]
/// for the billing history.
pub struct LendingMeter {
    /// Active lending sessions keyed by (lender, borrower).
    active_lends: DashMap<(SwarmId, SwarmId), LendingSession>,
    /// Completed billing records.
    billing_records: parking_lot::Mutex<VecDeque<BillingRecord>>,
    /// Maximum billing records to retain.
    max_billing_records: usize,
}

impl LendingMeter {
    /// Create a new lending meter.
    pub fn new() -> Self {
        Self {
            active_lends: DashMap::new(),
            billing_records: parking_lot::Mutex::new(VecDeque::new()),
            max_billing_records: 10000,
        }
    }

    /// Create a new lending meter with a custom billing history limit.
    pub fn with_max_billing_records(max: usize) -> Self {
        Self {
            active_lends: DashMap::new(),
            billing_records: parking_lot::Mutex::new(VecDeque::new()),
            max_billing_records: max,
        }
    }

    /// Start a new lending session.
    ///
    /// Returns an error string if a session between these parties already exists.
    pub fn start_lending(
        &self,
        lender: SwarmId,
        borrower: SwarmId,
        capacity_pct: u8,
        cores: u32,
        memory_mb: u64,
        time_limit_secs: Option<u64>,
    ) -> Result<(), String> {
        let key = (lender, borrower);
        if self.active_lends.contains_key(&key) {
            return Err(format!(
                "lending session already exists between {} and {}",
                lender, borrower
            ));
        }

        let auto_recall_at = time_limit_secs.map(|secs| {
            Utc::now()
                + chrono::Duration::from_std(Duration::from_secs(secs))
                    .unwrap_or_else(|_| chrono::Duration::seconds(secs as i64))
        });

        let session = LendingSession {
            lender,
            borrower,
            started_at: Utc::now(),
            capacity_pct,
            cores_lent: cores,
            memory_lent_mb: memory_mb,
            accumulated_cost: 0.0,
            auto_recall_at,
        };

        self.active_lends.insert(key, session);
        info!(lender = %lender, borrower = %borrower, cores = cores, memory_mb = memory_mb, "lending session started");
        Ok(())
    }

    /// Stop a lending session and generate a billing record.
    ///
    /// Returns the billing record, or an error if no session exists.
    pub fn stop_lending(
        &self,
        lender: SwarmId,
        borrower: SwarmId,
        billing: &LendingBilling,
    ) -> Result<BillingRecord, String> {
        let key = (lender, borrower);
        let (_, session) = self
            .active_lends
            .remove(&key)
            .ok_or_else(|| {
                format!(
                    "no active lending session between {} and {}",
                    lender, borrower
                )
            })?;

        let now = Utc::now();
        let duration_secs = (now - session.started_at).num_seconds().max(0) as f64;
        let effective_secs = duration_secs.max(billing.minimum_charge_secs as f64);
        let hours = effective_secs / 3600.0;

        let core_hours = session.cores_lent as f64 * hours;
        let gb_hours = (session.memory_lent_mb as f64 / 1024.0) * hours;

        let total_cost = match billing.model {
            BillingModel::Free => 0.0,
            BillingModel::Reciprocal => 0.0,
            BillingModel::Metered => {
                core_hours * billing.rate_per_core_hour + gb_hours * billing.rate_per_gb_hour
            }
            BillingModel::Fixed => billing.rate_per_core_hour, // Fixed uses rate_per_core_hour as the flat fee.
        };

        let record = BillingRecord {
            lender,
            borrower,
            period: (session.started_at, now),
            core_hours,
            gb_hours,
            total_cost,
        };

        let mut records = self.billing_records.lock();
        records.push_back(record.clone());
        while records.len() > self.max_billing_records {
            records.pop_front();
        }

        info!(
            lender = %lender,
            borrower = %borrower,
            core_hours = core_hours,
            gb_hours = gb_hours,
            total_cost = total_cost,
            "lending session stopped, billing record created"
        );

        Ok(record)
    }

    /// Evaluate whether lending is allowed given the current psyche facets.
    ///
    /// Checks the lending policy against the swarm's current resilience,
    /// vitality, and active archetypes.
    pub fn evaluate_lending_allowed(
        &self,
        policy: &LendingPolicy,
        facets: &PsycheFacets,
    ) -> Result<(), String> {
        if !policy.enabled {
            return Err("lending is not enabled in the policy".to_string());
        }

        if facets.resilience < policy.min_local_resilience {
            return Err(format!(
                "local resilience ({}) is below minimum ({})",
                facets.resilience, policy.min_local_resilience
            ));
        }

        if facets.vitality < policy.min_local_vitality {
            return Err(format!(
                "local vitality ({}) is below minimum ({})",
                facets.vitality, policy.min_local_vitality
            ));
        }

        for archetype in &facets.active_archetypes {
            if policy.block_during_archetypes.contains(archetype) {
                return Err(format!(
                    "lending is blocked during archetype '{}'",
                    archetype
                ));
            }
        }

        Ok(())
    }

    /// Get all currently active lending sessions.
    pub fn active_sessions(&self) -> Vec<LendingSession> {
        self.active_lends.iter().map(|r| r.value().clone()).collect()
    }

    /// Total percentage of capacity currently lent out by a specific swarm.
    pub fn total_lent_pct(&self, lender: SwarmId) -> u8 {
        let total: u16 = self
            .active_lends
            .iter()
            .filter(|r| r.value().lender == lender)
            .map(|r| r.value().capacity_pct as u16)
            .sum();
        total.min(255) as u8
    }

    /// Total percentage of capacity currently borrowed by a specific swarm.
    pub fn total_borrowed_pct(&self, borrower: SwarmId) -> u8 {
        let total: u16 = self
            .active_lends
            .iter()
            .filter(|r| r.value().borrower == borrower)
            .map(|r| r.value().capacity_pct as u16)
            .sum();
        total.min(255) as u8
    }

    /// Get the billing history.
    pub fn billing_history(&self) -> Vec<BillingRecord> {
        self.billing_records.lock().iter().cloned().collect()
    }

    /// Update accumulated costs for all active sessions based on current rates.
    pub fn update_costs(&self, billing: &LendingBilling) {
        for mut session in self.active_lends.iter_mut() {
            let s = session.value_mut();
            let now = Utc::now();
            let duration_secs = (now - s.started_at).num_seconds().max(0) as f64;
            let hours = duration_secs / 3600.0;

            let core_hours = s.cores_lent as f64 * hours;
            let gb_hours = (s.memory_lent_mb as f64 / 1024.0) * hours;

            s.accumulated_cost = match billing.model {
                BillingModel::Free | BillingModel::Reciprocal => 0.0,
                BillingModel::Metered => {
                    core_hours * billing.rate_per_core_hour + gb_hours * billing.rate_per_gb_hour
                }
                BillingModel::Fixed => billing.rate_per_core_hour,
            };
        }
    }

    /// Check for sessions that should be auto-recalled and return their keys.
    pub fn check_auto_recall(&self) -> Vec<(SwarmId, SwarmId)> {
        let now = Utc::now();
        self.active_lends
            .iter()
            .filter_map(|r| {
                let s = r.value();
                if let Some(recall_at) = s.auto_recall_at {
                    if now >= recall_at {
                        return Some((s.lender, s.borrower));
                    }
                }
                None
            })
            .collect()
    }

    /// Number of active sessions.
    pub fn active_count(&self) -> usize {
        self.active_lends.len()
    }
}

impl Default for LendingMeter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ConstellationView and related types
// ============================================================================

/// A node in the constellation graph representing a swarm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstellationNode {
    /// Swarm identity.
    pub swarm_id: SwarmId,
    /// Human-readable label.
    pub label: String,
    /// Number of alive nodes in this swarm.
    pub alive_nodes: usize,
    /// Total cores.
    pub total_cores: u32,
    /// Total memory (MB).
    pub total_memory_mb: u64,
    /// Whether this is the local swarm.
    pub is_local: bool,
}

/// An edge in the constellation graph representing a agreement relationship.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstellationEdge {
    /// Agreement governing this relationship.
    pub agreement_id: AgreementId,
    /// Agreement name.
    pub agreement_name: String,
    /// Whether the agreement is active.
    pub is_active: bool,
    /// Whether lending is enabled.
    pub lending_enabled: bool,
    /// Number of crossings observed between these swarms.
    pub crossings: usize,
    /// Weight for pathfinding (lower = better connectivity).
    pub weight: f64,
}

/// Aggregate statistics for the constellation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstellationStats {
    /// Total number of swarms.
    pub total_swarms: usize,
    /// Total number of treaties.
    pub total_treaties: usize,
    /// Number of active treaties.
    pub active_treaties: usize,
    /// Total crossings observed.
    pub total_crossings: usize,
    /// Total cores across all swarms.
    pub total_cores: u64,
    /// Total memory across all swarms (MB).
    pub total_memory_mb: u64,
    /// Total alive nodes across all swarms.
    pub total_alive_nodes: usize,
}

/// A complete view of the multi-swarm constellation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstellationView {
    /// All swarms in the constellation.
    pub nodes: Vec<ConstellationNode>,
    /// All agreement relationships.
    pub edges: Vec<(SwarmId, SwarmId, ConstellationEdge)>,
    /// Aggregate statistics.
    pub stats: ConstellationStats,
}

/// Pathfinding result between two swarms in the constellation.
#[derive(Debug, Clone)]
pub struct ConstellationPath {
    /// Ordered list of swarms on the path.
    pub hops: Vec<SwarmId>,
    /// Total weight (cost) of the path.
    pub total_weight: f64,
    /// Whether a path was found.
    pub reachable: bool,
}

// ============================================================================
// ConstellationBuilder
// ============================================================================

/// Builds a [`ConstellationView`] from the agreement store, summaries,
/// crossing log, and lending meter.
///
/// Optionally performs pathfinding between two swarms using Dijkstra's
/// algorithm on a `petgraph` directed graph.
pub struct ConstellationBuilder {
    /// Known swarm summaries.
    summaries: Vec<SwarmSummary>,
    /// The local swarm's ID.
    local_swarm_id: Option<SwarmId>,
    /// Agreement store reference.
    agreement_store: Arc<AgreementStore>,
    /// Crossing log reference.
    crossing_log: Option<CrossingLog>,
    /// Lending meter reference.
    lending_meter: Option<Arc<LendingMeter>>,
}

impl ConstellationBuilder {
    /// Create a new constellation builder.
    pub fn new(agreement_store: Arc<AgreementStore>) -> Self {
        Self {
            summaries: Vec::new(),
            local_swarm_id: None,
            agreement_store,
            crossing_log: None,
            lending_meter: None,
        }
    }

    /// Set the local swarm identity.
    pub fn with_local_swarm(mut self, id: SwarmId) -> Self {
        self.local_swarm_id = Some(id);
        self
    }

    /// Add swarm summaries.
    pub fn with_summaries(mut self, summaries: Vec<SwarmSummary>) -> Self {
        self.summaries = summaries;
        self
    }

    /// Set the crossing log.
    pub fn with_crossing_log(mut self, log: CrossingLog) -> Self {
        self.crossing_log = Some(log);
        self
    }

    /// Set the lending meter.
    pub fn with_lending_meter(mut self, meter: Arc<LendingMeter>) -> Self {
        self.lending_meter = Some(meter);
        self
    }

    /// Build the constellation view.
    pub fn build(&self) -> ConstellationView {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Build constellation nodes from summaries.
        for summary in &self.summaries {
            nodes.push(ConstellationNode {
                swarm_id: summary.id,
                label: summary.name.clone(),
                alive_nodes: summary.alive_nodes,
                total_cores: summary.total_cores,
                total_memory_mb: summary.total_memory_mb,
                is_local: self.local_swarm_id == Some(summary.id),
            });
        }

        // Build edges from treaties.
        let all_treaties = self.agreement_store.all();
        for agreement in &all_treaties {
            let crossings = self
                .crossing_log
                .as_ref()
                .map(|log| log.count_between(agreement.party_a, agreement.party_b))
                .unwrap_or(0);

            // Weight: active treaties with more crossings have lower weight (better).
            let weight = if agreement.is_active() {
                1.0 / (1.0 + crossings as f64)
            } else {
                100.0
            };

            edges.push((
                agreement.party_a,
                agreement.party_b,
                ConstellationEdge {
                    agreement_id: agreement.id,
                    agreement_name: agreement.name.clone(),
                    is_active: agreement.is_active(),
                    lending_enabled: agreement.lending.enabled,
                    crossings,
                    weight,
                },
            ));
        }

        // Compute stats.
        let stats = ConstellationStats {
            total_swarms: nodes.len(),
            total_treaties: all_treaties.len(),
            active_treaties: all_treaties.iter().filter(|t| t.is_active()).count(),
            total_crossings: edges.iter().map(|(_, _, e)| e.crossings).sum(),
            total_cores: nodes.iter().map(|n| n.total_cores as u64).sum(),
            total_memory_mb: nodes.iter().map(|n| n.total_memory_mb).sum(),
            total_alive_nodes: nodes.iter().map(|n| n.alive_nodes).sum(),
        };

        ConstellationView {
            nodes,
            edges,
            stats,
        }
    }

    /// Build the constellation with pathfinding between two swarms.
    ///
    /// Uses Dijkstra's algorithm on a directed graph where edges are
    /// weighted by the inverse of connectivity quality.
    pub fn build_with_pathfinding(
        &self,
        from: SwarmId,
        to: SwarmId,
    ) -> (ConstellationView, ConstellationPath) {
        let view = self.build();

        // Build a petgraph for pathfinding.
        let mut graph = DiGraph::<SwarmId, f64>::new();
        let mut node_indices: HashMap<SwarmId, NodeIndex> = HashMap::new();

        // Add nodes.
        for node in &view.nodes {
            let idx = graph.add_node(node.swarm_id);
            node_indices.insert(node.swarm_id, idx);
        }

        // Ensure from/to are in the graph even if not in summaries.
        node_indices.entry(from).or_insert_with(|| {
            let idx = graph.add_node(from);
            idx
        });
        node_indices.entry(to).or_insert_with(|| {
            let idx = graph.add_node(to);
            idx
        });

        // Add edges (bidirectional).
        for (a, b, edge) in &view.edges {
            if let (Some(&idx_a), Some(&idx_b)) = (node_indices.get(a), node_indices.get(b)) {
                graph.add_edge(idx_a, idx_b, edge.weight);
                graph.add_edge(idx_b, idx_a, edge.weight);
            }
        }

        // Run Dijkstra from `from`.
        let from_idx = node_indices.get(&from).copied();
        let to_idx = node_indices.get(&to).copied();

        let path = match (from_idx, to_idx) {
            (Some(src), Some(dst)) => {
                let costs = dijkstra(&graph, src, Some(dst), |e| *e.weight());
                if let Some(&cost) = costs.get(&dst) {
                    // Reconstruct path by backtracking through costs.
                    // Dijkstra in petgraph doesn't give us the path directly,
                    // so we do a simple BFS reconstruction.
                    let hops = reconstruct_path(&graph, &costs, &node_indices, src, dst);
                    ConstellationPath {
                        hops,
                        total_weight: cost,
                        reachable: true,
                    }
                } else {
                    ConstellationPath {
                        hops: Vec::new(),
                        total_weight: f64::INFINITY,
                        reachable: false,
                    }
                }
            }
            _ => ConstellationPath {
                hops: Vec::new(),
                total_weight: f64::INFINITY,
                reachable: false,
            },
        };

        (view, path)
    }
}

/// Reconstruct the shortest path from Dijkstra costs by greedy backtracking.
fn reconstruct_path(
    graph: &DiGraph<SwarmId, f64>,
    costs: &HashMap<NodeIndex, f64>,
    node_map: &HashMap<SwarmId, NodeIndex>,
    src: NodeIndex,
    dst: NodeIndex,
) -> Vec<SwarmId> {
    if src == dst {
        return vec![*graph.node_weight(src).unwrap_or(&SwarmId(Uuid::nil()))];
    }

    // Build reverse map: NodeIndex -> SwarmId.
    let idx_to_swarm: HashMap<NodeIndex, SwarmId> = node_map
        .iter()
        .map(|(&sid, &idx)| (idx, sid))
        .collect();

    // Greedy backtrack from dst to src using costs.
    let mut path = Vec::new();
    let mut current = dst;
    let mut visited = std::collections::HashSet::new();
    visited.insert(current);

    if let Some(&swarm) = idx_to_swarm.get(&current) {
        path.push(swarm);
    }

    let max_iterations = costs.len() + 1;
    for _ in 0..max_iterations {
        if current == src {
            break;
        }

        // Find the neighbor of `current` with the lowest cost that leads
        // us closer to src.
        let mut best_neighbor = None;
        let mut best_cost = f64::INFINITY;

        for neighbor in graph.neighbors_directed(current, petgraph::Direction::Incoming) {
            if visited.contains(&neighbor) {
                continue;
            }
            if let Some(&c) = costs.get(&neighbor) {
                if c < best_cost {
                    best_cost = c;
                    best_neighbor = Some(neighbor);
                }
            }
        }

        match best_neighbor {
            Some(next) => {
                visited.insert(next);
                if let Some(&swarm) = idx_to_swarm.get(&next) {
                    path.push(swarm);
                }
                current = next;
            }
            None => break,
        }
    }

    path.reverse();
    path
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_swarm_ids() -> (SwarmId, SwarmId) {
        (SwarmId::new(), SwarmId::new())
    }

    fn make_draft(a: SwarmId, b: SwarmId) -> Agreement {
        new_draft_agreement(a, b, "Test Agreement".to_string(), "A test agreement".to_string())
    }

    // ---------------------------------------------------------------
    // Agreement validation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_agreement_validation_valid() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let errors = agreement.validate();
        assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_agreement_validation_empty_name() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        agreement.name = "  ".to_string();
        let errors = agreement.validate();
        assert!(errors.iter().any(|e| e.contains("name")));
    }

    #[test]
    fn test_agreement_validation_same_parties() {
        let a = SwarmId::new();
        let mut agreement = make_draft(a, SwarmId::new());
        agreement.party_b = a;
        let errors = agreement.validate();
        assert!(errors.iter().any(|e| e.contains("different")));
    }

    #[test]
    fn test_agreement_validation_lend_pct_overflow() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        agreement.lending.max_lend_pct = 150;
        let errors = agreement.validate();
        assert!(errors.iter().any(|e| e.contains("max_lend_pct")));
    }

    #[test]
    fn test_agreement_validation_borrow_pct_overflow() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        agreement.lending.max_borrow_pct = 200;
        let errors = agreement.validate();
        assert!(errors.iter().any(|e| e.contains("max_borrow_pct")));
    }

    #[test]
    fn test_agreement_validation_expiry_before_effective() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        agreement.effective_at = Some(Utc::now() + chrono::Duration::hours(2));
        agreement.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
        let errors = agreement.validate();
        assert!(errors.iter().any(|e| e.contains("expires_at")));
    }

    #[test]
    fn test_agreement_validation_version_zero() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        agreement.version = 0;
        let errors = agreement.validate();
        assert!(errors.iter().any(|e| e.contains("version")));
    }

    // ---------------------------------------------------------------
    // Agreement signing tests
    // ---------------------------------------------------------------

    #[test]
    fn test_agreement_signing_payload_deterministic() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let p1 = agreement.signing_payload();
        let p2 = agreement.signing_payload();
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_agreement_compute_and_verify_signatures() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        let key_a = b"secret_key_a";
        let key_b = b"secret_key_b";

        agreement.signed_by_a = Some(agreement.compute_signature(key_a));
        agreement.signed_by_b = Some(agreement.compute_signature(key_b));

        assert!(agreement.verify_signatures(key_a, key_b));
    }

    #[test]
    fn test_agreement_verify_signatures_wrong_key() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        let key_a = b"secret_key_a";
        let key_b = b"secret_key_b";

        agreement.signed_by_a = Some(agreement.compute_signature(key_a));
        agreement.signed_by_b = Some(agreement.compute_signature(key_b));

        assert!(!agreement.verify_signatures(b"wrong_key", key_b));
    }

    #[test]
    fn test_agreement_verify_no_signatures() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        assert!(!agreement.verify_signatures(b"key_a", b"key_b"));
    }

    // ---------------------------------------------------------------
    // Agreement status and expiry tests
    // ---------------------------------------------------------------

    #[test]
    fn test_agreement_is_active() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        assert!(!agreement.is_active());
        agreement.status = AgreementStatus::Active;
        assert!(agreement.is_active());
    }

    #[test]
    fn test_agreement_is_expired() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        assert!(!agreement.is_expired());
        agreement.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
        assert!(agreement.is_expired());
    }

    #[test]
    fn test_agreement_days_until_expiry() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        assert!(agreement.days_until_expiry().is_none());
        agreement.expires_at = Some(Utc::now() + chrono::Duration::days(10));
        let days = agreement.days_until_expiry().expect("should have days");
        assert!(days >= 9 && days <= 10);
    }

    // ---------------------------------------------------------------
    // Negotiation lifecycle tests
    // ---------------------------------------------------------------

    #[test]
    fn test_negotiation_propose() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        assert!(neg.propose(a).is_ok());
        assert!(matches!(neg.agreement().status, AgreementStatus::Proposed { by } if by == a));
        assert_eq!(neg.messages().len(), 1);
    }

    #[test]
    fn test_negotiation_propose_invalid_status() {
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        agreement.status = AgreementStatus::Active;
        let mut neg = AgreementNegotiation::new(agreement);
        assert!(neg.propose(a).is_err());
    }

    #[test]
    fn test_negotiation_counter_propose() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose should succeed");

        let mut modified = neg.agreement().clone();
        modified.lending.max_lend_pct = 50;
        assert!(neg.counter_propose(b, modified).is_ok());
        assert!(matches!(neg.agreement().status, AgreementStatus::CounterProposed { by } if by == b));
    }

    #[test]
    fn test_negotiation_cannot_counter_own_proposal() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose should succeed");

        let modified = neg.agreement().clone();
        assert!(neg.counter_propose(a, modified).is_err());
    }

    #[test]
    fn test_negotiation_accept() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose should succeed");
        assert!(neg.accept(b).is_ok());
        assert!(matches!(neg.agreement().status, AgreementStatus::Accepted));
    }

    #[test]
    fn test_negotiation_cannot_accept_own_proposal() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose should succeed");
        assert!(neg.accept(a).is_err());
    }

    #[test]
    fn test_negotiation_reject() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose should succeed");
        assert!(neg.reject(b, "not interested".to_string()).is_ok());
        assert!(matches!(neg.agreement().status, AgreementStatus::Terminated { .. }));
    }

    #[test]
    fn test_negotiation_sign_both_parties() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose");
        neg.accept(b).expect("accept");
        neg.sign(a, b"key_a").expect("sign a");
        // After one signature, still accepted.
        assert!(matches!(neg.agreement().status, AgreementStatus::Accepted));
        neg.sign(b, b"key_b").expect("sign b");
        // After both signatures, active.
        assert!(matches!(neg.agreement().status, AgreementStatus::Active));
    }

    #[test]
    fn test_negotiation_sign_invalid_status() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);
        assert!(neg.sign(a, b"key").is_err());
    }

    #[test]
    fn test_negotiation_suspend_and_resume() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose");
        neg.accept(b).expect("accept");
        neg.sign(a, b"key_a").expect("sign a");
        neg.sign(b, b"key_b").expect("sign b");

        neg.suspend(a, "maintenance".to_string()).expect("suspend");
        assert!(matches!(neg.agreement().status, AgreementStatus::Suspended { .. }));

        neg.resume(b).expect("resume");
        assert!(matches!(neg.agreement().status, AgreementStatus::Active));
    }

    #[test]
    fn test_negotiation_terminate() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose");
        neg.accept(b).expect("accept");
        neg.sign(a, b"key_a").expect("sign a");
        neg.sign(b, b"key_b").expect("sign b");

        neg.terminate(a, "no longer needed".to_string())
            .expect("terminate");
        assert!(matches!(neg.agreement().status, AgreementStatus::Terminated { .. }));
    }

    #[test]
    fn test_negotiation_amend() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        neg.propose(a).expect("propose");
        neg.accept(b).expect("accept");
        neg.sign(a, b"key_a").expect("sign a");
        neg.sign(b, b"key_b").expect("sign b");

        let mut modified = neg.agreement().clone();
        modified.lending.max_lend_pct = 40;
        neg.amend(a, "increased lending cap".to_string(), modified)
            .expect("amend");

        assert_eq!(neg.agreement().version, 2);
        assert_eq!(neg.agreement().amendment_history.len(), 1);
        assert_eq!(neg.agreement().lending.max_lend_pct, 40);
    }

    // ---------------------------------------------------------------
    // AgreementStore tests
    // ---------------------------------------------------------------

    #[test]
    fn test_store_upsert_and_get() {
        let store = AgreementStore::new();
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let id = agreement.id;

        store.upsert(agreement);
        assert_eq!(store.count(), 1);
        assert!(store.get(&id).is_some());
    }

    #[test]
    fn test_store_version_history() {
        let store = AgreementStore::new();
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        let id = agreement.id;

        store.upsert(agreement.clone());

        agreement.version = 2;
        agreement.lending.max_lend_pct = 50;
        store.upsert(agreement);

        assert_eq!(store.get(&id).map(|t| t.version), Some(2));
        let history = store.get_versions(&id);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].version, 1);
    }

    #[test]
    fn test_store_remove() {
        let store = AgreementStore::new();
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let id = agreement.id;

        store.upsert(agreement);
        assert!(store.remove(&id).is_some());
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn test_store_find_by_swarm() {
        let store = AgreementStore::new();
        let a = SwarmId::new();
        let b = SwarmId::new();
        let c = SwarmId::new();

        store.upsert(make_draft(a, b));
        store.upsert(make_draft(a, c));
        store.upsert(make_draft(b, c));

        let a_treaties = store.find_by_swarm(a);
        assert_eq!(a_treaties.len(), 2);

        let c_treaties = store.find_by_swarm(c);
        assert_eq!(c_treaties.len(), 2);
    }

    #[test]
    fn test_store_find_active() {
        let store = AgreementStore::new();
        let (a, b) = make_swarm_ids();

        let mut active = make_draft(a, b);
        active.status = AgreementStatus::Active;
        store.upsert(active);

        store.upsert(make_draft(a, SwarmId::new())); // draft

        let actives = store.find_active();
        assert_eq!(actives.len(), 1);
    }

    #[test]
    fn test_store_find_expiring_within() {
        let store = AgreementStore::new();
        let (a, b) = make_swarm_ids();

        let mut expiring = make_draft(a, b);
        expiring.status = AgreementStatus::Active;
        expiring.expires_at = Some(Utc::now() + chrono::Duration::hours(12));
        store.upsert(expiring);

        let mut far = make_draft(a, SwarmId::new());
        far.status = AgreementStatus::Active;
        far.expires_at = Some(Utc::now() + chrono::Duration::days(30));
        store.upsert(far);

        let expiring = store.find_expiring_within(Duration::from_secs(86400));
        assert_eq!(expiring.len(), 1);
    }

    // ---------------------------------------------------------------
    // LendingMeter tests
    // ---------------------------------------------------------------

    #[test]
    fn test_lending_start_and_stop() {
        let meter = LendingMeter::new();
        let (a, b) = make_swarm_ids();

        meter
            .start_lending(a, b, 10, 4, 8192, None)
            .expect("start lending");
        assert_eq!(meter.active_count(), 1);

        let billing = LendingBilling {
            model: BillingModel::Metered,
            rate_per_core_hour: 0.10,
            rate_per_gb_hour: 0.05,
            minimum_charge_secs: 60,
        };

        let record = meter.stop_lending(a, b, &billing).expect("stop lending");
        assert_eq!(meter.active_count(), 0);
        assert!(record.total_cost >= 0.0);
    }

    #[test]
    fn test_lending_duplicate_session_rejected() {
        let meter = LendingMeter::new();
        let (a, b) = make_swarm_ids();

        meter
            .start_lending(a, b, 10, 4, 8192, None)
            .expect("first start");
        assert!(meter.start_lending(a, b, 10, 4, 8192, None).is_err());
    }

    #[test]
    fn test_lending_stop_nonexistent_session() {
        let meter = LendingMeter::new();
        let (a, b) = make_swarm_ids();
        let billing = LendingBilling::default();
        assert!(meter.stop_lending(a, b, &billing).is_err());
    }

    #[test]
    fn test_lending_total_lent_pct() {
        let meter = LendingMeter::new();
        let a = SwarmId::new();
        let b = SwarmId::new();
        let c = SwarmId::new();

        meter.start_lending(a, b, 10, 4, 8192, None).expect("s1");
        meter.start_lending(a, c, 15, 2, 4096, None).expect("s2");

        assert_eq!(meter.total_lent_pct(a), 25);
        assert_eq!(meter.total_borrowed_pct(b), 10);
    }

    #[test]
    fn test_lending_evaluate_allowed_disabled() {
        let meter = LendingMeter::new();
        let policy = LendingPolicy::default(); // enabled = false
        let facets = PsycheFacets {
            resilience: 80,
            vitality: 80,
            active_archetypes: Vec::new(),
        };
        assert!(meter.evaluate_lending_allowed(&policy, &facets).is_err());
    }

    #[test]
    fn test_lending_evaluate_allowed_low_resilience() {
        let meter = LendingMeter::new();
        let policy = LendingPolicy {
            enabled: true,
            min_local_resilience: 70,
            ..LendingPolicy::default()
        };
        let facets = PsycheFacets {
            resilience: 50,
            vitality: 80,
            active_archetypes: Vec::new(),
        };
        let result = meter.evaluate_lending_allowed(&policy, &facets);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("resilience"));
    }

    #[test]
    fn test_lending_evaluate_allowed_blocked_archetype() {
        let meter = LendingMeter::new();
        let policy = LendingPolicy {
            enabled: true,
            min_local_resilience: 0,
            min_local_vitality: 0,
            block_during_archetypes: vec!["defensive".to_string()],
            ..LendingPolicy::default()
        };
        let facets = PsycheFacets {
            resilience: 80,
            vitality: 80,
            active_archetypes: vec!["defensive".to_string()],
        };
        assert!(meter.evaluate_lending_allowed(&policy, &facets).is_err());
    }

    #[test]
    fn test_lending_evaluate_allowed_ok() {
        let meter = LendingMeter::new();
        let policy = LendingPolicy {
            enabled: true,
            min_local_resilience: 50,
            min_local_vitality: 50,
            ..LendingPolicy::default()
        };
        let facets = PsycheFacets {
            resilience: 80,
            vitality: 80,
            active_archetypes: Vec::new(),
        };
        assert!(meter.evaluate_lending_allowed(&policy, &facets).is_ok());
    }

    #[test]
    fn test_lending_billing_free_model() {
        let meter = LendingMeter::new();
        let (a, b) = make_swarm_ids();

        meter
            .start_lending(a, b, 10, 4, 8192, None)
            .expect("start");

        let billing = LendingBilling {
            model: BillingModel::Free,
            ..LendingBilling::default()
        };

        let record = meter.stop_lending(a, b, &billing).expect("stop");
        assert!((record.total_cost - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_lending_auto_recall() {
        let meter = LendingMeter::new();
        let (a, b) = make_swarm_ids();

        // Session with 0 second time limit (should immediately be recallable).
        meter
            .start_lending(a, b, 10, 4, 8192, Some(0))
            .expect("start");

        // Wait a tiny bit for the auto_recall_at to be in the past.
        std::thread::sleep(std::time::Duration::from_millis(10));

        let recalls = meter.check_auto_recall();
        assert_eq!(recalls.len(), 1);
        assert_eq!(recalls[0], (a, b));
    }

    #[test]
    fn test_lending_billing_history() {
        let meter = LendingMeter::new();
        let (a, b) = make_swarm_ids();

        meter.start_lending(a, b, 10, 4, 8192, None).expect("s");
        let billing = LendingBilling::default();
        meter.stop_lending(a, b, &billing).expect("stop");

        assert_eq!(meter.billing_history().len(), 1);
    }

    #[test]
    fn test_lending_update_costs() {
        let meter = LendingMeter::new();
        let (a, b) = make_swarm_ids();

        meter.start_lending(a, b, 10, 4, 8192, None).expect("s");

        let billing = LendingBilling {
            model: BillingModel::Metered,
            rate_per_core_hour: 1.0,
            rate_per_gb_hour: 0.5,
            minimum_charge_secs: 0,
        };

        meter.update_costs(&billing);

        let sessions = meter.active_sessions();
        assert_eq!(sessions.len(), 1);
        // Cost should be >= 0 (might be 0 if elapsed time is negligible).
        assert!(sessions[0].accumulated_cost >= 0.0);
    }

    // ---------------------------------------------------------------
    // Constellation tests
    // ---------------------------------------------------------------

    #[test]
    fn test_constellation_build_empty() {
        let store = Arc::new(AgreementStore::new());
        let builder = ConstellationBuilder::new(store);
        let view = builder.build();
        assert_eq!(view.stats.total_swarms, 0);
        assert_eq!(view.stats.total_treaties, 0);
    }

    #[test]
    fn test_constellation_build_with_swarms_and_treaties() {
        let store = Arc::new(AgreementStore::new());
        let a = SwarmId::new();
        let b = SwarmId::new();

        let mut agreement = make_draft(a, b);
        agreement.status = AgreementStatus::Active;
        store.upsert(agreement);

        let summaries = vec![
            SwarmSummary {
                id: a,
                name: "Alpha".to_string(),
                alive_nodes: 10,
                total_cores: 40,
                total_memory_mb: 65536,
            },
            SwarmSummary {
                id: b,
                name: "Beta".to_string(),
                alive_nodes: 5,
                total_cores: 20,
                total_memory_mb: 32768,
            },
        ];

        let builder = ConstellationBuilder::new(store)
            .with_summaries(summaries)
            .with_local_swarm(a);

        let view = builder.build();
        assert_eq!(view.stats.total_swarms, 2);
        assert_eq!(view.stats.total_treaties, 1);
        assert_eq!(view.stats.active_treaties, 1);
        assert_eq!(view.stats.total_alive_nodes, 15);
        assert_eq!(view.stats.total_cores, 60);

        // Check local swarm flag.
        let local_node = view.nodes.iter().find(|n| n.swarm_id == a).expect("should find A");
        assert!(local_node.is_local);
    }

    #[test]
    fn test_constellation_pathfinding_direct() {
        let store = Arc::new(AgreementStore::new());
        let a = SwarmId::new();
        let b = SwarmId::new();

        let mut agreement = make_draft(a, b);
        agreement.status = AgreementStatus::Active;
        store.upsert(agreement);

        let summaries = vec![
            SwarmSummary {
                id: a,
                name: "A".to_string(),
                alive_nodes: 5,
                total_cores: 10,
                total_memory_mb: 1024,
            },
            SwarmSummary {
                id: b,
                name: "B".to_string(),
                alive_nodes: 5,
                total_cores: 10,
                total_memory_mb: 1024,
            },
        ];

        let builder = ConstellationBuilder::new(store).with_summaries(summaries);
        let (_, path) = builder.build_with_pathfinding(a, b);
        assert!(path.reachable);
        assert_eq!(path.hops.len(), 2);
        assert_eq!(path.hops[0], a);
        assert_eq!(path.hops[1], b);
    }

    #[test]
    fn test_constellation_pathfinding_unreachable() {
        let store = Arc::new(AgreementStore::new());
        let a = SwarmId::new();
        let b = SwarmId::new();

        // No treaties connecting a and b.
        let summaries = vec![
            SwarmSummary {
                id: a,
                name: "A".to_string(),
                alive_nodes: 5,
                total_cores: 10,
                total_memory_mb: 1024,
            },
            SwarmSummary {
                id: b,
                name: "B".to_string(),
                alive_nodes: 5,
                total_cores: 10,
                total_memory_mb: 1024,
            },
        ];

        let builder = ConstellationBuilder::new(store).with_summaries(summaries);
        let (_, path) = builder.build_with_pathfinding(a, b);
        assert!(!path.reachable);
    }

    #[test]
    fn test_constellation_pathfinding_transitive() {
        let store = Arc::new(AgreementStore::new());
        let a = SwarmId::new();
        let b = SwarmId::new();
        let c = SwarmId::new();

        // A <-> B and B <-> C, so A can reach C via B.
        let mut t1 = make_draft(a, b);
        t1.status = AgreementStatus::Active;
        store.upsert(t1);

        let mut t2 = make_draft(b, c);
        t2.status = AgreementStatus::Active;
        store.upsert(t2);

        let summaries = vec![
            SwarmSummary {
                id: a,
                name: "A".to_string(),
                alive_nodes: 5,
                total_cores: 10,
                total_memory_mb: 1024,
            },
            SwarmSummary {
                id: b,
                name: "B".to_string(),
                alive_nodes: 5,
                total_cores: 10,
                total_memory_mb: 1024,
            },
            SwarmSummary {
                id: c,
                name: "C".to_string(),
                alive_nodes: 5,
                total_cores: 10,
                total_memory_mb: 1024,
            },
        ];

        let builder = ConstellationBuilder::new(store).with_summaries(summaries);
        let (_, path) = builder.build_with_pathfinding(a, c);
        assert!(path.reachable);
        assert_eq!(path.hops.len(), 3);
    }

    // ---------------------------------------------------------------
    // Crossing log tests
    // ---------------------------------------------------------------

    #[test]
    fn test_crossing_log() {
        let mut log = CrossingLog::new();
        let a = SwarmId::new();
        let b = SwarmId::new();

        log.record(a, b, "jobs".to_string());
        log.record(b, a, "data".to_string());

        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.count_between(a, b), 2);
    }

    // ---------------------------------------------------------------
    // Billing model display
    // ---------------------------------------------------------------

    #[test]
    fn test_billing_model_display() {
        assert_eq!(format!("{}", BillingModel::Free), "free");
        assert_eq!(format!("{}", BillingModel::Metered), "metered");
        assert_eq!(format!("{}", BillingModel::Reciprocal), "reciprocal");
        assert_eq!(format!("{}", BillingModel::Fixed), "fixed");
    }

    // ---------------------------------------------------------------
    // AgreementStatus display
    // ---------------------------------------------------------------

    #[test]
    fn test_agreement_status_display() {
        let s = AgreementStatus::Draft;
        assert_eq!(format!("{}", s), "draft");

        let s = AgreementStatus::Active;
        assert_eq!(format!("{}", s), "active");

        let s = AgreementStatus::Expired;
        assert_eq!(format!("{}", s), "expired");
    }

    // ---------------------------------------------------------------
    // LendingPolicy defaults
    // ---------------------------------------------------------------

    #[test]
    fn test_lending_policy_defaults() {
        let p = LendingPolicy::default();
        assert!(!p.enabled);
        assert_eq!(p.max_lend_pct, 20);
        assert_eq!(p.max_borrow_pct, 30);
        assert_eq!(p.min_local_resilience, 50);
        assert_eq!(p.min_local_vitality, 50);
        assert!(p.block_during_archetypes.is_empty());
    }

    // ---------------------------------------------------------------
    // SwarmId, AgreementId display/default
    // ---------------------------------------------------------------

    #[test]
    fn test_swarm_id_display() {
        let id = SwarmId::new();
        let display = format!("{}", id);
        assert!(display.starts_with("swarm-"));
    }

    #[test]
    fn test_agreement_id_display() {
        let id = AgreementId::new();
        let display = format!("{}", id);
        assert!(display.starts_with("agreement-"));
    }

    // ---------------------------------------------------------------
    // NegotiationAction display
    // ---------------------------------------------------------------

    #[test]
    fn test_negotiation_action_display() {
        assert_eq!(format!("{}", NegotiationAction::Propose), "propose");
        assert_eq!(format!("{}", NegotiationAction::Accept), "accept");
        assert_eq!(format!("{}", NegotiationAction::Reject), "reject");
        assert_eq!(format!("{}", NegotiationAction::Sign), "sign");
    }

    // ---------------------------------------------------------------
    // Constellation with crossing log
    // ---------------------------------------------------------------

    #[test]
    fn test_constellation_with_crossing_log() {
        let store = Arc::new(AgreementStore::new());
        let a = SwarmId::new();
        let b = SwarmId::new();

        let mut agreement = make_draft(a, b);
        agreement.status = AgreementStatus::Active;
        store.upsert(agreement);

        let mut log = CrossingLog::new();
        log.record(a, b, "jobs".to_string());
        log.record(a, b, "data".to_string());
        log.record(b, a, "results".to_string());

        let summaries = vec![
            SwarmSummary { id: a, name: "A".into(), alive_nodes: 5, total_cores: 10, total_memory_mb: 1024 },
            SwarmSummary { id: b, name: "B".into(), alive_nodes: 3, total_cores: 6, total_memory_mb: 512 },
        ];

        let builder = ConstellationBuilder::new(store)
            .with_summaries(summaries)
            .with_crossing_log(log);

        let view = builder.build();
        assert_eq!(view.stats.total_crossings, 3);
    }

    // ---------------------------------------------------------------
    // Store: stale version ignored
    // ---------------------------------------------------------------

    #[test]
    fn test_store_stale_version_ignored() {
        let store = AgreementStore::new();
        let (a, b) = make_swarm_ids();
        let mut agreement = make_draft(a, b);
        agreement.version = 5;
        let id = agreement.id;

        store.upsert(agreement.clone());

        let mut old = agreement.clone();
        old.version = 3;
        old.lending.max_lend_pct = 99;
        store.upsert(old);

        // Should still be version 5.
        assert_eq!(store.get(&id).map(|t| t.version), Some(5));
        assert_ne!(store.get(&id).map(|t| t.lending.max_lend_pct), Some(99));
    }

    // ---------------------------------------------------------------
    // SeveranceConditions defaults
    // ---------------------------------------------------------------

    #[test]
    fn test_severance_conditions_defaults() {
        let s = SeveranceConditions::default();
        assert_eq!(s.unreachable_timeout_secs, 3600);
        assert_eq!(s.max_violations, 10);
        assert!(s.auto_sever);
    }

    // ---------------------------------------------------------------
    // LendingDataPolicy defaults
    // ---------------------------------------------------------------

    #[test]
    fn test_lending_data_policy_defaults() {
        let p = LendingDataPolicy::default();
        assert!(!p.allow_data_caching);
        assert!(p.require_encryption);
        assert!(!p.require_residency_compliance);
        assert_eq!(p.max_data_retention_secs, Some(3600));
    }

    // ---------------------------------------------------------------
    // Full negotiation lifecycle: propose -> counter -> accept -> sign -> amend -> suspend -> resume -> terminate
    // ---------------------------------------------------------------

    #[test]
    fn test_full_negotiation_lifecycle() {
        let (a, b) = make_swarm_ids();
        let agreement = make_draft(a, b);
        let mut neg = AgreementNegotiation::new(agreement);

        // Propose.
        neg.propose(a).expect("propose");

        // Counter-propose.
        let mut modified = neg.agreement().clone();
        modified.lending.max_lend_pct = 30;
        neg.counter_propose(b, modified).expect("counter");

        // Accept counter.
        neg.accept(a).expect("accept");

        // Sign.
        neg.sign(a, b"key_a").expect("sign a");
        neg.sign(b, b"key_b").expect("sign b");
        assert!(neg.agreement().is_active());

        // Amend.
        let mut amended = neg.agreement().clone();
        amended.lending.max_borrow_pct = 50;
        neg.amend(b, "increase borrow cap".to_string(), amended)
            .expect("amend");
        assert_eq!(neg.agreement().lending.max_borrow_pct, 50);

        // Suspend.
        neg.suspend(a, "upgrade".to_string()).expect("suspend");

        // Resume.
        neg.resume(b).expect("resume");

        // Terminate.
        neg.terminate(a, "done".to_string()).expect("terminate");

        // Total messages.
        assert_eq!(neg.messages().len(), 9);
    }
}
