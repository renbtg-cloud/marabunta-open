// Marabunta - Licensed under the MIT License.
//! Malicious node simulator for byzantine fault tolerance testing.
//!
//! Provides a `MaliciousNode` that wraps a real `GossipEngine` and
//! `KnowledgeStore` but can apply configurable behavior modifications
//! to outbound gossip messages — forged timestamps, fake identities,
//! oversized arrays, self-authored reputation, etc.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::sync::mpsc;

use crate::swarm::auth::NodeIdentity;
use crate::swarm::config::*;
use crate::swarm::gossip::{GossipConfig, GossipEngine, MergeResult};
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::*;

/// Describes a single malicious behavior to apply to outbound gossip.
#[derive(Debug, Clone)]
pub enum MaliciousBehavior {
    /// Offset the message timestamp by the given duration (positive = future).
    ClockSkew { offset: ChronoDuration },
    /// Claim a different sender_id (impersonation).
    Impersonate { target: NodeId },
    /// Claim every possible trait regardless of actual capabilities.
    FakeAllTraits,
    /// Inject a forged witness report saying `target` is alive.
    ForgeWitnessAlive { target: NodeId },
    /// Inject a forged witness report saying `target` is dead.
    ForgeWitnessDead { target: NodeId },
    /// Inject `count` ghost NodeInfo entries into known_nodes.
    InjectGhostNodes { count: usize },
    /// Pad arrays to `entries` elements with fake data.
    OversizedMessages { entries: usize },
    /// Include a reputation record that praises the sender itself.
    InflateOwnReputation,
    /// Use an invalid (random) signature instead of signing properly.
    InvalidSignature,
    /// Send messages with no signature at all (strip any existing).
    StripSignature,
    /// Forge known_nodes entries claiming false status for a target.
    ForgeNodeStatus { target: NodeId, status: NodeStatus },
}

/// A simulated malicious node for integration testing.
///
/// Builds on top of real `GossipEngine` + `KnowledgeStore`, but
/// applies `MaliciousBehavior` modifications to outbound messages.
pub struct MaliciousNode {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub knowledge: Arc<KnowledgeStore>,
    pub gossip: Arc<GossipEngine>,
    pub identity: Arc<NodeIdentity>,
    behaviors: Vec<MaliciousBehavior>,
    _outbound_rx: mpsc::Receiver<(SocketAddr, SwarmMessage)>,
}

impl MaliciousNode {
    /// Create a new malicious node with a real identity on the given port.
    pub fn new(port: u16) -> Self {
        let identity = NodeIdentity::generate();
        let id = identity.node_id;
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        let identity = Arc::new(identity);

        let knowledge = Arc::new(KnowledgeStore::new(id));
        let (outbound_tx, outbound_rx) = mpsc::channel(256);

        let config = GossipConfig {
            interval: GOSSIP_INTERVAL,
            fanout: GOSSIP_FANOUT,
            jitter_percent: 0,
        };

        let gossip = Arc::new(
            GossipEngine::new(id, 1, knowledge.clone(), outbound_tx)
                .with_config(config)
                .with_identity(identity.clone()),
        );

        // Register self in own knowledge store.
        knowledge.merge_node(NodeInfo {
            node_id: id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.1,
            capacity: ResourceSnapshot::default(),
            address: Some(addr),
            via: id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
        });

        MaliciousNode {
            id,
            addr,
            knowledge,
            gossip,
            identity,
            behaviors: Vec::new(),
            _outbound_rx: outbound_rx,
        }
    }

    /// Add a malicious behavior (builder pattern).
    pub fn with_behavior(mut self, b: MaliciousBehavior) -> Self {
        self.behaviors.push(b);
        self
    }

    /// Build a gossip message with all malicious modifications applied.
    pub fn build_evil_gossip(&self) -> GossipMessage {
        let mut msg = self.gossip.build_message(
            &HashSet::from([Trait::CanExecute]),
            0.1,
            &ResourceSnapshot::default(),
        );

        for behavior in &self.behaviors {
            match behavior {
                MaliciousBehavior::ClockSkew { offset } => {
                    msg.timestamp = msg.timestamp + *offset;
                    // Also shift all known_nodes timestamps.
                    for node in &mut msg.known_nodes {
                        node.last_seen = node.last_seen + *offset;
                    }
                }
                MaliciousBehavior::Impersonate { target } => {
                    msg.sender_id = *target;
                    // Signature will now be invalid for the claimed sender_id.
                }
                MaliciousBehavior::FakeAllTraits => {
                    msg.my_traits = Trait::ALL.iter().copied().collect();
                }
                MaliciousBehavior::ForgeWitnessAlive { target } => {
                    msg.witness_reports.push(WitnessReport {
                        reporter: NodeId::new(), // Fake reporter
                        subject: *target,
                        last_seen: Utc::now(),
                        signature: vec![],
                    });
                }
                MaliciousBehavior::ForgeWitnessDead { target } => {
                    msg.witness_reports.push(WitnessReport {
                        reporter: NodeId::new(), // Fake reporter
                        subject: *target,
                        last_seen: Utc::now() - ChronoDuration::hours(1),
                        signature: vec![],
                    });
                }
                MaliciousBehavior::InjectGhostNodes { count } => {
                    for _ in 0..*count {
                        msg.known_nodes.push(NodeInfo {
                            node_id: NodeId::new(),
                            last_seen: Utc::now(),
                            traits: HashSet::from([Trait::CanExecute]),
                            load: 0.0,
                            capacity: ResourceSnapshot::default(),
                            address: None,
                            via: self.id,
                            status: NodeStatus::Alive,
                            generation: 1,
                            trust_level: TrustLevel::Direct,
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            geo_region: None,
                        });
                    }
                }
                MaliciousBehavior::OversizedMessages { entries } => {
                    // Pad known_nodes to the requested size.
                    while msg.known_nodes.len() < *entries {
                        msg.known_nodes.push(NodeInfo {
                            node_id: NodeId::new(),
                            last_seen: Utc::now(),
                            traits: HashSet::from([Trait::CanExecute]),
                            load: 0.0,
                            capacity: ResourceSnapshot::default(),
                            address: None,
                            via: self.id,
                            status: NodeStatus::Alive,
                            generation: 1,
                            trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
                        });
                    }
                }
                MaliciousBehavior::InflateOwnReputation => {
                    use crate::swarm::reputation::{EntityId, ReputationRecord};
                    let mut record = ReputationRecord::new(EntityId::Node(self.id));
                    record.score = 1.0; // Max reputation
                    record.version = 999;
                    msg.known_reputation.push(record);
                }
                MaliciousBehavior::InvalidSignature => {
                    // Replace signature with random garbage.
                    msg.envelope_signature = [0xDE, 0xAD, 0xBE, 0xEF].repeat(16);
                }
                MaliciousBehavior::StripSignature => {
                    msg.envelope_signature.clear();
                    msg.sender_public_key.clear();
                }
                MaliciousBehavior::ForgeNodeStatus { target, status } => {
                    msg.known_nodes.push(NodeInfo {
                        node_id: *target,
                        last_seen: Utc::now(),
                        traits: HashSet::new(),
                        load: 0.0,
                        capacity: ResourceSnapshot::default(),
                        address: None,
                        via: self.id,
                        status: *status,
                        generation: 1,
                        trust_level: TrustLevel::Direct,
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            geo_region: None, // Attacker claims Direct
                    });
                }
            }
        }

        // Re-sign after all modifications so the signature is valid for the
        // adversarial content — UNLESS a behavior explicitly wants a broken
        // signature (InvalidSignature, StripSignature, Impersonate).
        let wants_bad_sig = self.behaviors.iter().any(|b| matches!(
            b,
            MaliciousBehavior::InvalidSignature
            | MaliciousBehavior::StripSignature
            | MaliciousBehavior::Impersonate { .. }
        ));
        if !wants_bad_sig {
            msg.sender_public_key = self.identity.public_key.to_bytes().to_vec();
            msg.envelope_signature = crate::swarm::auth::sign_gossip(
                &self.identity,
                &msg.sender_id,
                &msg.timestamp,
                msg.generation,
            );
        }

        msg
    }

    /// Send an evil gossip message to each target engine and collect results.
    pub fn evil_gossip_round(&self, targets: &[&GossipEngine]) -> Vec<MergeResult> {
        let msg = self.build_evil_gossip();
        targets
            .iter()
            .map(|engine| engine.handle_gossip(msg.clone()))
            .collect()
    }

    // -- Convenience constructors --

    /// A node whose clock is offset by the given duration.
    pub fn timestamp_attacker(port: u16, offset: ChronoDuration) -> Self {
        Self::new(port).with_behavior(MaliciousBehavior::ClockSkew { offset })
    }

    /// A node that claims to be `target`.
    pub fn impersonator(port: u16, target: NodeId) -> Self {
        Self::new(port).with_behavior(MaliciousBehavior::Impersonate { target })
    }

    /// A node that sends invalid signatures.
    pub fn bad_signer(port: u16) -> Self {
        Self::new(port).with_behavior(MaliciousBehavior::InvalidSignature)
    }

    /// A node that injects `count` ghost nodes per gossip round.
    pub fn ghost_injector(port: u16, count: usize) -> Self {
        Self::new(port).with_behavior(MaliciousBehavior::InjectGhostNodes { count })
    }

    /// A node that sends oversized gossip arrays.
    pub fn oversized_sender(port: u16, entries: usize) -> Self {
        Self::new(port).with_behavior(MaliciousBehavior::OversizedMessages { entries })
    }
}
