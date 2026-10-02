// Marabunta - Licensed under the MIT License.
//! Attestation chain and audit trail for the Marabunta protocol.
//!
//! ALCOA-compliant audit trail with 5-link attestation chains:
//! Submission → Routing → Execution → Aggregation → Delivery.

use crate::marabunta::crypto::{self, DILITHIUM_PK_LEN, DILITHIUM_SIG_LEN};
use crate::marabunta::identity::NodeId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single link in the attestation chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AttestationLink {
    /// Job submitted by client.
    Submission {
        job_id: Uuid,
        submitter_id: NodeId,
        code_hash: [u8; 32],
        input_hash: [u8; 32],
        timestamp: DateTime<Utc>,
        signature: Vec<u8>,
    },
    /// Job routed to executors.
    Routing {
        job_id: Uuid,
        router_id: NodeId,
        target_nodes: Vec<NodeId>,
        timestamp: DateTime<Utc>,
        signature: Vec<u8>,
    },
    /// Job executed by a node.
    Execution {
        job_id: Uuid,
        executor_id: NodeId,
        input_hash: [u8; 32],
        output_hash: [u8; 32],
        timestamp: DateTime<Utc>,
        signature: Vec<u8>,
    },
    /// Results aggregated (for redundant execution).
    Aggregation {
        job_id: Uuid,
        aggregator_id: NodeId,
        result_hashes: Vec<[u8; 32]>,
        consensus: bool,
        timestamp: DateTime<Utc>,
        signature: Vec<u8>,
    },
    /// Result delivered to client.
    Delivery {
        job_id: Uuid,
        deliverer_id: NodeId,
        output_hash: [u8; 32],
        timestamp: DateTime<Utc>,
        signature: Vec<u8>,
    },
}

impl AttestationLink {
    /// Get the signer's NodeId.
    pub fn signer_id(&self) -> NodeId {
        match self {
            Self::Submission { submitter_id, .. } => *submitter_id,
            Self::Routing { router_id, .. } => *router_id,
            Self::Execution { executor_id, .. } => *executor_id,
            Self::Aggregation { aggregator_id, .. } => *aggregator_id,
            Self::Delivery { deliverer_id, .. } => *deliverer_id,
        }
    }

    /// Get the signature bytes.
    pub fn signature(&self) -> &[u8] {
        match self {
            Self::Submission { signature, .. } => signature,
            Self::Routing { signature, .. } => signature,
            Self::Execution { signature, .. } => signature,
            Self::Aggregation { signature, .. } => signature,
            Self::Delivery { signature, .. } => signature,
        }
    }

    /// Get the job ID.
    pub fn job_id(&self) -> Uuid {
        match self {
            Self::Submission { job_id, .. } => *job_id,
            Self::Routing { job_id, .. } => *job_id,
            Self::Execution { job_id, .. } => *job_id,
            Self::Aggregation { job_id, .. } => *job_id,
            Self::Delivery { job_id, .. } => *job_id,
        }
    }

    /// Compute the message that was signed for this link.
    pub fn signed_message(&self) -> Vec<u8> {
        let mut msg = Vec::new();
        match self {
            Self::Submission {
                job_id,
                code_hash,
                input_hash,
                timestamp,
                ..
            } => {
                msg.extend_from_slice(job_id.as_bytes());
                msg.extend_from_slice(code_hash);
                msg.extend_from_slice(input_hash);
                msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
            }
            Self::Routing {
                job_id,
                target_nodes,
                timestamp,
                ..
            } => {
                msg.extend_from_slice(job_id.as_bytes());
                for node in target_nodes {
                    msg.extend_from_slice(node.as_bytes());
                }
                msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
            }
            Self::Execution {
                job_id,
                input_hash,
                output_hash,
                timestamp,
                ..
            } => {
                msg.extend_from_slice(job_id.as_bytes());
                msg.extend_from_slice(input_hash);
                msg.extend_from_slice(output_hash);
                msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
            }
            Self::Aggregation {
                job_id,
                result_hashes,
                consensus,
                timestamp,
                ..
            } => {
                msg.extend_from_slice(job_id.as_bytes());
                for hash in result_hashes {
                    msg.extend_from_slice(hash);
                }
                msg.push(*consensus as u8);
                msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
            }
            Self::Delivery {
                job_id,
                output_hash,
                timestamp,
                ..
            } => {
                msg.extend_from_slice(job_id.as_bytes());
                msg.extend_from_slice(output_hash);
                msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
            }
        }
        msg
    }
}

/// A complete attestation chain for a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttestationChain {
    pub job_id: Uuid,
    pub links: Vec<AttestationLink>,
}

impl AttestationChain {
    pub fn new(job_id: Uuid) -> Self {
        Self {
            job_id,
            links: Vec::new(),
        }
    }

    /// Add a link to the chain.
    pub fn add_link(&mut self, link: AttestationLink) {
        self.links.push(link);
    }

    /// Verify every link in the chain using provided public keys.
    /// Returns (valid_count, total_count).
    pub fn verify(
        &self,
        public_keys: &std::collections::HashMap<NodeId, [u8; DILITHIUM_PK_LEN]>,
    ) -> (usize, usize) {
        let total = self.links.len();
        let mut valid = 0;

        for link in &self.links {
            let signer = link.signer_id();
            if let Some(pk_bytes) = public_keys.get(&signer) {
                let msg = link.signed_message();
                let sig = link.signature();
                if sig.len() == DILITHIUM_SIG_LEN {
                    let sig_arr: &[u8; DILITHIUM_SIG_LEN] = sig.try_into().unwrap();
                    if let Ok(true) = crypto::dilithium_verify_bytes(pk_bytes, &msg, sig_arr) {
                        valid += 1;
                    }
                }
            }
        }

        (valid, total)
    }

    /// Whether the chain is complete (has all 5 link types).
    pub fn is_complete(&self) -> bool {
        let has_submission = self.links.iter().any(|l| matches!(l, AttestationLink::Submission { .. }));
        let has_routing = self.links.iter().any(|l| matches!(l, AttestationLink::Routing { .. }));
        let has_execution = self.links.iter().any(|l| matches!(l, AttestationLink::Execution { .. }));
        let has_aggregation = self.links.iter().any(|l| matches!(l, AttestationLink::Aggregation { .. }));
        let has_delivery = self.links.iter().any(|l| matches!(l, AttestationLink::Delivery { .. }));
        has_submission && has_routing && has_execution && has_aggregation && has_delivery
    }
}

/// Routing receipt — swarm-side proof (no plaintext).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingReceipt {
    pub job_id: Uuid,
    pub router_id: NodeId,
    pub target_count: usize,
    pub timestamp: DateTime<Utc>,
    pub signature: Vec<u8>,
}

/// Execution receipt — swarm-side proof (no plaintext).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub job_id: Uuid,
    pub executor_id: NodeId,
    pub output_hash: [u8; 32],
    pub fuel_consumed: u64,
    pub timestamp: DateTime<Utc>,
    pub signature: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marabunta::crypto::DilithiumKeyPair;

    fn make_signed_link(
        kp: &DilithiumKeyPair,
        node_id: NodeId,
        job_id: Uuid,
    ) -> Vec<AttestationLink> {
        let code_hash = crypto::shake256(b"code");
        let input_hash = crypto::shake256(b"input");
        let output_hash = crypto::shake256(b"output");
        let timestamp = Utc::now();

        let mut links = Vec::new();

        // Submission
        let mut msg = Vec::new();
        msg.extend_from_slice(job_id.as_bytes());
        msg.extend_from_slice(&code_hash);
        msg.extend_from_slice(&input_hash);
        msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
        let sig = kp.sign(&msg).unwrap().to_vec();
        links.push(AttestationLink::Submission {
            job_id,
            submitter_id: node_id,
            code_hash,
            input_hash,
            timestamp,
            signature: sig,
        });

        // Routing
        let mut msg = Vec::new();
        msg.extend_from_slice(job_id.as_bytes());
        msg.extend_from_slice(node_id.as_bytes());
        msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
        let sig = kp.sign(&msg).unwrap().to_vec();
        links.push(AttestationLink::Routing {
            job_id,
            router_id: node_id,
            target_nodes: vec![node_id],
            timestamp,
            signature: sig,
        });

        // Execution
        let mut msg = Vec::new();
        msg.extend_from_slice(job_id.as_bytes());
        msg.extend_from_slice(&input_hash);
        msg.extend_from_slice(&output_hash);
        msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
        let sig = kp.sign(&msg).unwrap().to_vec();
        links.push(AttestationLink::Execution {
            job_id,
            executor_id: node_id,
            input_hash,
            output_hash,
            timestamp,
            signature: sig,
        });

        // Aggregation
        let mut msg = Vec::new();
        msg.extend_from_slice(job_id.as_bytes());
        msg.extend_from_slice(&output_hash);
        msg.push(1u8);
        msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
        let sig = kp.sign(&msg).unwrap().to_vec();
        links.push(AttestationLink::Aggregation {
            job_id,
            aggregator_id: node_id,
            result_hashes: vec![output_hash],
            consensus: true,
            timestamp,
            signature: sig,
        });

        // Delivery
        let mut msg = Vec::new();
        msg.extend_from_slice(job_id.as_bytes());
        msg.extend_from_slice(&output_hash);
        msg.extend_from_slice(&timestamp.timestamp().to_le_bytes());
        let sig = kp.sign(&msg).unwrap().to_vec();
        links.push(AttestationLink::Delivery {
            job_id,
            deliverer_id: node_id,
            output_hash,
            timestamp,
            signature: sig,
        });

        links
    }

    #[test]
    fn test_full_chain_verify() {
        let kp = DilithiumKeyPair::generate().unwrap();
        let node_id = NodeId(crypto::shake256(&kp.public_key_bytes()));
        let job_id = Uuid::new_v4();

        let mut chain = AttestationChain::new(job_id);
        for link in make_signed_link(&kp, node_id, job_id) {
            chain.add_link(link);
        }

        assert!(chain.is_complete());

        let mut keys = std::collections::HashMap::new();
        keys.insert(node_id, kp.public_key_bytes());

        let (valid, total) = chain.verify(&keys);
        assert_eq!(valid, 5);
        assert_eq!(total, 5);
    }

    #[test]
    fn test_tampered_chain_fails() {
        let kp = DilithiumKeyPair::generate().unwrap();
        let node_id = NodeId(crypto::shake256(&kp.public_key_bytes()));
        let job_id = Uuid::new_v4();

        let mut chain = AttestationChain::new(job_id);
        let mut links = make_signed_link(&kp, node_id, job_id);
        // Tamper with the execution link's output hash
        if let AttestationLink::Execution {
            ref mut output_hash,
            ..
        } = links[2]
        {
            output_hash[0] ^= 0xff;
        }
        for link in links {
            chain.add_link(link);
        }

        let mut keys = std::collections::HashMap::new();
        keys.insert(node_id, kp.public_key_bytes());

        let (valid, total) = chain.verify(&keys);
        assert_eq!(total, 5);
        assert!(valid < 5); // Tampered link should fail
    }

    #[test]
    fn test_incomplete_chain() {
        let chain = AttestationChain::new(Uuid::new_v4());
        assert!(!chain.is_complete());
    }

    #[test]
    fn test_link_signer_id() {
        let node_id = NodeId([0xAA; 32]);
        let link = AttestationLink::Submission {
            job_id: Uuid::new_v4(),
            submitter_id: node_id,
            code_hash: [0; 32],
            input_hash: [0; 32],
            timestamp: Utc::now(),
            signature: vec![],
        };
        assert_eq!(link.signer_id(), node_id);
    }
}
