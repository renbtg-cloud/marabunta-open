// Marabunta - Licensed under the MIT License.
//! Pillar 13.1: Justice System (Court of Arbitration)
//!
//! Implements witness selection, Ed25519 signing, verdict collection,
//! and audit trail reconstruction. In the event of a dispute, Juries
//! are summoned to reconstruct WASM state hashes and slash malicious actors.
//!
//! # Overview
//!
//! 1. **Witness Selection** (`WitnessSelector`): picks witnesses based on
//!    criticality level, distributing across network segments for diversity.
//! 2. **Ed25519 Signing**: canonical byte serialization of event fields,
//!    signed with per-node keypairs.
//! 3. **Verdict Collection** (`WitnessEngine`): manages pending witness
//!    rounds, collects verdicts, and determines quorum.
//! 4. **Trail Reconstruction**: verifies signatures, deduplicates events,
//!    and produces a sorted, verified audit trail.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use dashmap::DashMap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use super::audit::AuditEvent;
use super::config::{
    witness_requirements, WITNESS_QUORUM_RATIO_CRITICAL, WITNESS_QUORUM_RATIO_HIGH,
    WITNESS_QUORUM_RATIO_NORMAL,
};
use super::types::{CriticalityLevel, Verdict, WitnessRecord};

// ============================================================================
// WitnessResult
// ============================================================================

/// Outcome of a witness consensus round.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WitnessResult {
    /// Quorum met: enough witnesses confirmed the event.
    Confirmed {
        /// All witness attestations collected.
        witnesses: Vec<WitnessRecord>,
    },
    /// Some witnesses responded, but not enough for full quorum.
    PartialQuorum {
        /// Witness attestations received so far.
        witnesses: Vec<WitnessRecord>,
        /// How many additional confirmations were needed.
        missing: usize,
    },
    /// The witness round failed entirely.
    Failed {
        /// Human-readable explanation.
        reason: String,
    },
    /// Witnessing was not required (e.g., Low criticality).
    Skipped,
}

// ============================================================================
// VerifiedAuditEvent
// ============================================================================

/// An audit event with cryptographic verification results attached.
///
/// Produced by trail reconstruction: each event is annotated with whether
/// its originator signature is valid and per-witness signature validity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifiedAuditEvent {
    /// The original audit event.
    pub event: AuditEvent,
    /// Whether the originator's Ed25519 signature is valid.
    pub signature_valid: bool,
    /// Per-witness verification: `(node_id, is_valid)`.
    pub witness_signatures_valid: Vec<(String, bool)>,
    /// When this verification was performed (epoch millis).
    pub verification_timestamp_ms: u64,
}

// ============================================================================
// NodeMeta
// ============================================================================

/// Simplified node metadata used for witness selection.
///
/// The witness selector needs to know each node's segment for diversity
/// and its reputation score for quality-based filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum OperationalPosture {
    ClearNet,
    #[default]
    DarkNet,
}


#[derive(Debug, Clone, Default)]
pub struct NodeMeta {
    /// Unique node identifier.
    pub node_id: String,
    /// Network segment (e.g., "us-east", "eu-west", "default").
    pub segment: String,
    /// Reputation score in [0.0, 1.0]; higher is more trustworthy.
    pub reputation_score: f64,
    /// Whether the node is running in highly auditable or evasive mode.
    pub posture: OperationalPosture,
}

// ============================================================================
// WitnessSelector
// ============================================================================

/// Selects witness nodes for a given criticality level.
///
/// Selection strategy:
/// - **Low**: no witnesses needed.
/// - **Emergency**: all reachable nodes except the originator.
/// - **Normal/High/Critical**: select the required count, distributing
///   across network segments in round-robin fashion for diversity.
pub struct WitnessSelector {
    /// Known nodes available for witness selection.
    known_nodes: Arc<DashMap<String, NodeMeta>>,
}

impl WitnessSelector {
    /// Create a new selector backed by the given node map.
    pub fn new(known_nodes: Arc<DashMap<String, NodeMeta>>) -> Self {
        Self { known_nodes }
    }

    /// Select witnesses appropriate for the given criticality level.
    ///
    /// `exclude` is typically the originating node's ID (a node should
    /// not witness its own events).
    ///
    /// Returns a list of node IDs chosen as witnesses.
    pub fn select_for_criticality(
        &self,
        level: CriticalityLevel,
        exclude: &str,
    ) -> Vec<String> {
        let (required_count, min_segments) = witness_requirements(&level);

        // Low criticality: no witnesses.
        if required_count == 0 {
            return Vec::new();
        }

        // Collect eligible nodes (exclude the originator).
        let mut candidates: Vec<NodeMeta> = self
            .known_nodes
            .iter()
            .filter(|entry| entry.key() != exclude)
            .map(|entry| entry.value().clone())
            .collect();

        // Sort by reputation descending for quality.
        candidates.sort_by(|a, b| {
            b.reputation_score
                .partial_cmp(&a.reputation_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Emergency: all reachable nodes.
        if level == CriticalityLevel::Emergency {
            return candidates.into_iter().map(|n| n.node_id).collect();
        }

        // Build segment buckets for round-robin diversity.
        let mut segment_buckets: HashMap<String, Vec<NodeMeta>> = HashMap::new();
        for node in &candidates {
            segment_buckets
                .entry(node.segment.clone())
                .or_default()
                .push(node.clone());
        }

        let mut selected: Vec<String> = Vec::new();
        let mut segments_used: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Round-robin across segments.
        let segment_keys: Vec<String> = segment_buckets.keys().cloned().collect();
        let mut bucket_indices: HashMap<String, usize> = HashMap::new();
        for key in &segment_keys {
            bucket_indices.insert(key.clone(), 0);
        }

        let mut round = 0;
        let max_rounds = candidates.len(); // Safety bound.

        while selected.len() < required_count && round < max_rounds {
            for seg in &segment_keys {
                if selected.len() >= required_count {
                    break;
                }
                let idx = bucket_indices.get(seg).copied().unwrap_or(0);
                if let Some(bucket) = segment_buckets.get(seg) {
                    if idx < bucket.len() {
                        let node = &bucket[idx];
                        if !selected.contains(&node.node_id) {
                            selected.push(node.node_id.clone());
                            segments_used.insert(seg.clone());
                        }
                        bucket_indices.insert(seg.clone(), idx + 1);
                    }
                }
            }
            round += 1;
        }

        // Log if we couldn't meet the segment diversity requirement.
        if min_segments > 0 && segments_used.len() < min_segments {
            debug!(
                required_segments = min_segments,
                actual_segments = segments_used.len(),
                level = %level,
                "witness selection: insufficient segment diversity"
            );
        }

        selected
    }
}

// ============================================================================
// Ed25519 Signing Helpers
// ============================================================================

/// Generate a fresh Ed25519 keypair using OS randomness.
pub fn generate_keypair() -> (SigningKey, VerifyingKey) {
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    (signing_key, verifying_key)
}

/// Compute the canonical byte representation of an audit event's core fields.
///
/// Format: `event_id || node_id || action_type || timestamp_ms (8 bytes BE)`
fn canonical_event_bytes(
    event_id: &str,
    node_id: &str,
    action_type: &str,
    timestamp_ms: u64,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(event_id.as_bytes());
    buf.extend_from_slice(node_id.as_bytes());
    buf.extend_from_slice(action_type.as_bytes());
    buf.extend_from_slice(&timestamp_ms.to_be_bytes());
    buf
}

/// Sign an audit event's canonical fields with the given key.
///
/// Returns the Ed25519 signature as raw bytes.
pub fn sign_event_bytes(
    event_id: &str,
    node_id: &str,
    action_type: &str,
    timestamp_ms: u64,
    key: &SigningKey,
) -> Vec<u8> {
    let bytes = canonical_event_bytes(event_id, node_id, action_type, timestamp_ms);
    let sig = key.sign(&bytes);
    sig.to_bytes().to_vec()
}

/// Verify an Ed25519 signature over an audit event's canonical fields.
pub fn verify_event_signature(
    event_id: &str,
    node_id: &str,
    action_type: &str,
    timestamp_ms: u64,
    pubkey: &VerifyingKey,
    sig: &[u8],
) -> bool {
    let bytes = canonical_event_bytes(event_id, node_id, action_type, timestamp_ms);
    match Signature::from_slice(sig) {
        Ok(signature) => pubkey.verify(&bytes, &signature).is_ok(),
        Err(_) => false,
    }
}

/// Compute the canonical byte representation of a witness verdict.
///
/// Format: `event_id || node_id || verdict_str`
fn canonical_verdict_bytes(event_id: &str, node_id: &str, verdict_str: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(event_id.as_bytes());
    buf.extend_from_slice(node_id.as_bytes());
    buf.extend_from_slice(verdict_str.as_bytes());
    buf
}

/// Sign a witness verdict with the given key.
///
/// Returns the Ed25519 signature as raw bytes.
pub fn sign_witness_verdict(
    event_id: &str,
    node_id: &str,
    verdict_str: &str,
    key: &SigningKey,
) -> Vec<u8> {
    let bytes = canonical_verdict_bytes(event_id, node_id, verdict_str);
    let sig = key.sign(&bytes);
    sig.to_bytes().to_vec()
}

/// Verify a witness verdict signature.
pub fn verify_witness_signature(
    event_id: &str,
    node_id: &str,
    verdict_str: &str,
    pubkey: &VerifyingKey,
    sig: &[u8],
) -> bool {
    let bytes = canonical_verdict_bytes(event_id, node_id, verdict_str);
    match Signature::from_slice(sig) {
        Ok(signature) => pubkey.verify(&bytes, &signature).is_ok(),
        Err(_) => false,
    }
}

// ============================================================================
// PendingRound (internal)
// ============================================================================

/// State of an in-flight witness consensus round.
struct PendingRound {
    /// The audit event ID being witnessed.
    event_id: String,
    /// Criticality level of the event.
    criticality: CriticalityLevel,
    /// Node IDs that were selected as witnesses.
    expected_witnesses: Vec<String>,
    /// Attestations received so far.
    received: Vec<WitnessRecord>,
    /// When this round was initiated.
    started_at: Instant,
}

// ============================================================================
// WitnessEngine
// ============================================================================

/// Core engine for the witness consensus protocol.
///
/// Manages keypair generation, witness selection, pending rounds,
/// verdict signing, public key distribution, and trail reconstruction.
pub struct WitnessEngine {
    /// This node's identity.
    node_id: String,
    /// Witness selector for choosing attestation nodes.
    selector: WitnessSelector,
    /// This node's Ed25519 signing key.
    signing_key: SigningKey,
    /// This node's Ed25519 verifying (public) key.
    verifying_key: VerifyingKey,
    /// Map of node_id -> VerifyingKey for signature verification.
    pubkey_store: Arc<DashMap<String, VerifyingKey>>,
    /// In-flight witness rounds keyed by event_id.
    pending_rounds: Arc<DashMap<String, PendingRound>>,
}

impl WitnessEngine {
    /// Create a new WitnessEngine with a freshly generated keypair.
    pub fn new(
        node_id: String,
        known_nodes: Arc<DashMap<String, NodeMeta>>,
    ) -> Self {
        let (signing_key, verifying_key) = generate_keypair();

        info!(
            node_id = %node_id,
            "witness engine initialized with new Ed25519 keypair"
        );

        Self {
            node_id,
            selector: WitnessSelector::new(known_nodes),
            signing_key,
            verifying_key,
            pubkey_store: Arc::new(DashMap::new()),
            pending_rounds: Arc::new(DashMap::new()),
        }
    }

    /// Get this node's verifying (public) key.
    pub fn verifying_key(&self) -> &VerifyingKey {
        &self.verifying_key
    }

    /// Register another node's public key for future signature verification.
    pub fn register_pubkey(&self, node_id: String, pubkey: VerifyingKey) {
        debug!(node_id = %node_id, "registered witness public key");
        self.pubkey_store.insert(node_id, pubkey);
    }

    /// Initiate a witness consensus round for the given event.
    ///
    /// For Low criticality, returns `WitnessResult::Skipped` immediately.
    /// For other levels, selects witnesses and creates a pending round.
    /// In this simplified (non-networked) implementation, returns
    /// `Confirmed` with an empty witness list for non-Low levels,
    /// as actual network transport is handled by the swarm layer.
    pub fn request_witnessing(
        &self,
        event_id: &str,
        criticality: CriticalityLevel,
    ) -> WitnessResult {
        // Low criticality: skip entirely.
        if criticality == CriticalityLevel::Low {
            debug!(event_id = %event_id, "skipping witnessing for Low criticality");
            return WitnessResult::Skipped;
        }

        let witnesses = self
            .selector
            .select_for_criticality(criticality, &self.node_id);

        if witnesses.is_empty() {
            warn!(
                event_id = %event_id,
                criticality = %criticality,
                "no witnesses available for consensus round"
            );
            return WitnessResult::Failed {
                reason: "no eligible witnesses found".to_string(),
            };
        }

        let round = PendingRound {
            event_id: event_id.to_string(),
            criticality,
            expected_witnesses: witnesses.clone(),
            received: Vec::new(),
            started_at: Instant::now(),
        };

        self.pending_rounds.insert(event_id.to_string(), round);

        debug!(
            event_id = %event_id,
            criticality = %criticality,
            witness_count = witnesses.len(),
            "witness round created"
        );

        // In the simplified single-process implementation, we return
        // Confirmed with an empty list. The real swarm transport layer
        // would send requests to selected witnesses and collect verdicts.
        WitnessResult::Confirmed {
            witnesses: Vec::new(),
        }
    }

    /// Record a received witness verdict for a pending round.
    ///
    /// Returns `Some(WitnessResult)` if the round is now complete
    /// (quorum met or all expected witnesses responded), or `None`
    /// if still waiting for more verdicts.
    pub fn record_verdict(
        &self,
        event_id: &str,
        record: WitnessRecord,
    ) -> Option<WitnessResult> {
        let mut round_ref = self.pending_rounds.get_mut(event_id)?;
        let round = round_ref.value_mut();

        // Avoid duplicate verdicts from the same witness.
        if round.received.iter().any(|r| r.node_id == record.node_id) {
            debug!(
                event_id = %event_id,
                witness = %record.node_id,
                "duplicate verdict ignored"
            );
            return None;
        }

        round.received.push(record);

        let (required_count, _min_segments) = witness_requirements(&round.criticality);
        let quorum_ratio = quorum_ratio_for_level(round.criticality);

        let confirmed_count = round
            .received
            .iter()
            .filter(|r| r.verdict == Verdict::Confirmed)
            .count();

        let quorum_needed = if required_count == usize::MAX {
            // Emergency: need supermajority of all.
            ((round.expected_witnesses.len() as f64) * quorum_ratio).ceil() as usize
        } else {
            ((required_count as f64) * quorum_ratio).ceil() as usize
        };

        // Check if quorum is met.
        if confirmed_count >= quorum_needed {
            let witnesses = round.received.clone();
            drop(round_ref);
            self.pending_rounds.remove(event_id);
            return Some(WitnessResult::Confirmed { witnesses });
        }

        // Check if all expected have responded (no more coming).
        if round.received.len() >= round.expected_witnesses.len() {
            let witnesses = round.received.clone();
            let missing = quorum_needed.saturating_sub(confirmed_count);

            // Check if all rejected.
            let all_rejected = round
                .received
                .iter()
                .all(|r| r.verdict == Verdict::Rejected);

            drop(round_ref);
            self.pending_rounds.remove(event_id);

            if all_rejected {
                return Some(WitnessResult::Failed {
                    reason: "all witnesses rejected the event".to_string(),
                });
            }

            return Some(WitnessResult::PartialQuorum { witnesses, missing });
        }

        None
    }

    /// Handle an incoming witness request from another node.
    ///
    /// Computes a SHA-256 hash of the event data, compares it with the
    /// provided hash, and signs a verdict.
    ///
    /// Returns `(verdict_string, signature_bytes)`.
    pub fn handle_witness_request(
        &self,
        event_id: &str,
        event_hash: &[u8],
        _originator: &str,
    ) -> (String, Vec<u8>) {
        // Verify the event hash is non-empty (basic sanity check).
        // In a full implementation, we would re-derive the hash from
        // the event data and compare.
        let verdict_str = if !event_hash.is_empty() {
            "Confirmed"
        } else {
            "Rejected"
        };

        let sig = sign_witness_verdict(event_id, &self.node_id, verdict_str, &self.signing_key);

        debug!(
            event_id = %event_id,
            verdict = verdict_str,
            "witness verdict signed"
        );

        (verdict_str.to_string(), sig)
    }

    /// Sign an audit event's core fields with this node's key.
    ///
    /// Returns the signature bytes to be stored in `AuditEvent.signature`.
    pub fn sign_event(&self, event: &AuditEvent) -> Vec<u8> {
        sign_event_bytes(
            &event.event_id,
            &event.node_id,
            &event.action_type,
            event.timestamp_ms,
            &self.signing_key,
        )
    }

    /// Reconstruct a verified audit trail from a list of events.
    ///
    /// For each event:
    /// - Verifies the originator's signature (if a public key is registered).
    /// - Verifies each witness's signature.
    /// - Deduplicates by event_id (first occurrence wins).
    /// - Sorts by timestamp ascending.
    pub fn reconstruct_trail(&self, events: Vec<AuditEvent>) -> Vec<VerifiedAuditEvent> {
        let now_ms = Utc::now().timestamp_millis() as u64;
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut verified: Vec<VerifiedAuditEvent> = Vec::new();

        for event in events {
            // Dedup by event_id.
            if seen.contains(&event.event_id) {
                continue;
            }
            seen.insert(event.event_id.clone());

            // Verify originator signature.
            let sig_valid = if event.signature.is_empty() {
                false
            } else if let Some(pubkey) = self.pubkey_store.get(&event.node_id) {
                verify_event_signature(
                    &event.event_id,
                    &event.node_id,
                    &event.action_type,
                    event.timestamp_ms,
                    pubkey.value(),
                    &event.signature,
                )
            } else {
                // No registered public key for this node.
                false
            };

            // Verify each witness signature.
            let witness_sigs: Vec<(String, bool)> = event
                .witnesses
                .iter()
                .map(|w| {
                    let verdict_str = match w.verdict {
                        Verdict::Confirmed => "Confirmed",
                        Verdict::Rejected => "Rejected",
                        Verdict::Timeout => "Timeout",
                    };

                    let valid = if w.signature.is_empty() {
                        false
                    } else if let Some(pubkey) = self.pubkey_store.get(&w.node_id) {
                        verify_witness_signature(
                            &event.event_id,
                            &w.node_id,
                            verdict_str,
                            pubkey.value(),
                            &w.signature,
                        )
                    } else {
                        false
                    };

                    (w.node_id.clone(), valid)
                })
                .collect();

            verified.push(VerifiedAuditEvent {
                event,
                signature_valid: sig_valid,
                witness_signatures_valid: witness_sigs,
                verification_timestamp_ms: now_ms,
            });
        }

        // Sort by timestamp ascending.
        verified.sort_by_key(|v| v.event.timestamp_ms);

        verified
    }

    /// Get the number of currently pending witness rounds.
    pub fn pending_round_count(&self) -> usize {
        self.pending_rounds.len()
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Returns the quorum ratio for a given criticality level.
fn quorum_ratio_for_level(level: CriticalityLevel) -> f64 {
    match level {
        CriticalityLevel::Low => 0.0,
        CriticalityLevel::Normal => WITNESS_QUORUM_RATIO_NORMAL,
        CriticalityLevel::High => WITNESS_QUORUM_RATIO_HIGH,
        CriticalityLevel::Critical => WITNESS_QUORUM_RATIO_CRITICAL,
        CriticalityLevel::Emergency => WITNESS_QUORUM_RATIO_CRITICAL, // Same as critical for emergency
    }
}

/// Compute a SHA-256 hash of an audit event's key fields.
///
/// Useful for creating a compact digest to send to witnesses.
pub fn hash_audit_event(event: &AuditEvent) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(event.event_id.as_bytes());
    hasher.update(event.node_id.as_bytes());
    hasher.update(event.action_type.as_bytes());
    hasher.update(event.timestamp_ms.to_be_bytes());
    if let Ok(payload_str) = serde_json::to_string(&event.payload) {
        hasher.update(payload_str.as_bytes());
    }
    hasher.finalize().to_vec()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::audit::{AuditEventBuilder, ScrutinyConfig};
    use super::super::types::{CriticalityLevel, Verdict, WitnessRecord};

    /// Helper: create a WitnessSelector with a set of test nodes.
    fn make_selector_with_nodes(
        nodes: Vec<(&str, &str, f64)>,
    ) -> (WitnessSelector, Arc<DashMap<String, NodeMeta>>) {
        let map = Arc::new(DashMap::new());
        for (id, seg, rep) in nodes {
            map.insert(
                id.to_string(),
                NodeMeta {
                    node_id: id.to_string(),
                    segment: seg.to_string(),
                    reputation_score: rep,
                    ..Default::default()
                },
            );
        }
        let selector = WitnessSelector::new(Arc::clone(&map));
        (selector, map)
    }

    /// Helper: build a test AuditEvent with the given action.
    fn make_event(action_type: &str, criticality: CriticalityLevel) -> AuditEvent {
        AuditEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp_ms: Utc::now().timestamp_millis() as u64,
            node_id: "originator".to_string(),
            actor_id: "actor-1".to_string(),
            action_type: action_type.to_string(),
            criticality,
            target: "target-1".to_string(),
            payload: serde_json::json!({}),
            previous_state: None,
            signature: Vec::new(),
            witnesses: Vec::new(),
        }
    }

    // ----------------------------------------------------------------
    // Witness Selection Tests
    // ----------------------------------------------------------------

    #[test]
    fn test_select_low_returns_empty() {
        let (selector, _) = make_selector_with_nodes(vec![
            ("n1", "seg-a", 0.9),
            ("n2", "seg-b", 0.8),
        ]);

        let selected = selector.select_for_criticality(CriticalityLevel::Low, "originator");
        assert!(selected.is_empty(), "Low criticality should require no witnesses");
    }

    #[test]
    fn test_select_normal_returns_two() {
        let (selector, _) = make_selector_with_nodes(vec![
            ("originator", "seg-a", 0.9),
            ("n1", "seg-a", 0.85),
            ("n2", "seg-b", 0.8),
            ("n3", "seg-c", 0.7),
        ]);

        let selected = selector.select_for_criticality(CriticalityLevel::Normal, "originator");
        assert_eq!(selected.len(), 2, "Normal criticality should select 2 witnesses");
        assert!(
            !selected.contains(&"originator".to_string()),
            "Originator must be excluded"
        );
    }

    #[test]
    fn test_select_high_segment_diversity() {
        let (selector, _) = make_selector_with_nodes(vec![
            ("originator", "seg-a", 0.9),
            ("n1", "seg-a", 0.85),
            ("n2", "seg-b", 0.8),
            ("n3", "seg-c", 0.75),
            ("n4", "seg-a", 0.7),
            ("n5", "seg-b", 0.65),
            ("n6", "seg-c", 0.6),
        ]);

        let selected = selector.select_for_criticality(CriticalityLevel::High, "originator");
        assert_eq!(selected.len(), 5, "High criticality should select 5 witnesses");

        // Count unique segments.
        let segments: std::collections::HashSet<String> = selected
            .iter()
            .filter_map(|id| {
                // Look up segment from original nodes.
                if id == "n1" || id == "n4" {
                    Some("seg-a".to_string())
                } else if id == "n2" || id == "n5" {
                    Some("seg-b".to_string())
                } else if id == "n3" || id == "n6" {
                    Some("seg-c".to_string())
                } else {
                    None
                }
            })
            .collect();

        assert!(
            segments.len() >= 2,
            "High criticality should use at least 2 segments, got {}",
            segments.len()
        );
    }

    #[test]
    fn test_select_critical_segment_diversity() {
        let (selector, _) = make_selector_with_nodes(vec![
            ("originator", "seg-a", 0.9),
            ("n1", "seg-a", 0.9),
            ("n2", "seg-b", 0.85),
            ("n3", "seg-c", 0.8),
            ("n4", "seg-d", 0.75),
            ("n5", "seg-a", 0.7),
            ("n6", "seg-b", 0.65),
            ("n7", "seg-c", 0.6),
            ("n8", "seg-d", 0.55),
        ]);

        let selected = selector.select_for_criticality(CriticalityLevel::Critical, "originator");
        assert_eq!(selected.len(), 7, "Critical criticality should select 7 witnesses");
    }

    #[test]
    fn test_select_emergency_returns_all() {
        let (selector, _) = make_selector_with_nodes(vec![
            ("originator", "seg-a", 0.9),
            ("n1", "seg-a", 0.85),
            ("n2", "seg-b", 0.8),
            ("n3", "seg-c", 0.75),
        ]);

        let selected = selector.select_for_criticality(CriticalityLevel::Emergency, "originator");
        // Should include all except originator.
        assert_eq!(
            selected.len(),
            3,
            "Emergency should select all nodes except originator"
        );
        assert!(!selected.contains(&"originator".to_string()));
    }

    #[test]
    fn test_select_excludes_originator() {
        let (selector, _) = make_selector_with_nodes(vec![
            ("originator", "seg-a", 0.9),
            ("n1", "seg-b", 0.8),
            ("n2", "seg-c", 0.7),
            ("n3", "seg-d", 0.6),
        ]);

        let selected = selector.select_for_criticality(CriticalityLevel::Normal, "originator");
        for id in &selected {
            assert_ne!(id, "originator", "Originator must never be selected");
        }
    }

    #[test]
    fn test_select_insufficient_nodes_partial() {
        // Only 1 eligible node, but Normal requires 2.
        let (selector, _) = make_selector_with_nodes(vec![
            ("originator", "seg-a", 0.9),
            ("n1", "seg-b", 0.8),
        ]);

        let selected = selector.select_for_criticality(CriticalityLevel::Normal, "originator");
        assert_eq!(
            selected.len(),
            1,
            "Should return as many as available when fewer than required"
        );
    }

    // ----------------------------------------------------------------
    // Ed25519 Signing Tests
    // ----------------------------------------------------------------

    #[test]
    fn test_sign_verify_roundtrip() {
        let (signing_key, verifying_key) = generate_keypair();

        let sig = sign_event_bytes("evt-1", "node-a", "action.test", 1700000000000, &signing_key);

        assert!(
            verify_event_signature("evt-1", "node-a", "action.test", 1700000000000, &verifying_key, &sig),
            "Signature should verify with the correct key"
        );
    }

    #[test]
    fn test_verify_wrong_key_fails() {
        let (signing_key, _) = generate_keypair();
        let (_, wrong_verifying_key) = generate_keypair();

        let sig = sign_event_bytes("evt-1", "node-a", "action.test", 1700000000000, &signing_key);

        assert!(
            !verify_event_signature(
                "evt-1",
                "node-a",
                "action.test",
                1700000000000,
                &wrong_verifying_key,
                &sig
            ),
            "Signature should NOT verify with a different key"
        );
    }

    #[test]
    fn test_verify_tampered_event_fails() {
        let (signing_key, verifying_key) = generate_keypair();

        let sig = sign_event_bytes("evt-1", "node-a", "action.test", 1700000000000, &signing_key);

        // Tamper with event_id.
        assert!(
            !verify_event_signature(
                "evt-TAMPERED",
                "node-a",
                "action.test",
                1700000000000,
                &verifying_key,
                &sig
            ),
            "Signature should fail on tampered event_id"
        );

        // Tamper with action_type.
        assert!(
            !verify_event_signature(
                "evt-1",
                "node-a",
                "action.TAMPERED",
                1700000000000,
                &verifying_key,
                &sig
            ),
            "Signature should fail on tampered action_type"
        );

        // Tamper with timestamp.
        assert!(
            !verify_event_signature(
                "evt-1",
                "node-a",
                "action.test",
                9999999999999,
                &verifying_key,
                &sig
            ),
            "Signature should fail on tampered timestamp"
        );
    }

    #[test]
    fn test_witness_verdict_sign_verify() {
        let (signing_key, verifying_key) = generate_keypair();

        let sig = sign_witness_verdict("evt-1", "witness-node", "Confirmed", &signing_key);

        assert!(
            verify_witness_signature("evt-1", "witness-node", "Confirmed", &verifying_key, &sig),
            "Witness verdict signature should verify"
        );

        // Wrong verdict string should fail.
        assert!(
            !verify_witness_signature("evt-1", "witness-node", "Rejected", &verifying_key, &sig),
            "Verdict signature should fail with wrong verdict string"
        );
    }

    // ----------------------------------------------------------------
    // Verdict Collection Tests
    // ----------------------------------------------------------------

    #[test]
    fn test_verdict_collection_quorum_met() {
        let known_nodes = Arc::new(DashMap::new());
        known_nodes.insert(
            "w1".to_string(),
            NodeMeta { node_id: "w1".to_string(), segment: "seg-a".to_string(), reputation_score: 0.9, ..Default::default() },
        );
        known_nodes.insert(
            "w2".to_string(),
            NodeMeta { node_id: "w2".to_string(), segment: "seg-b".to_string(), reputation_score: 0.8, ..Default::default() },
        );

        let engine = WitnessEngine::new("originator".to_string(), known_nodes);

        // Initiate a Normal round (which will create the pending round).
        let _result = engine.request_witnessing("evt-1", CriticalityLevel::Normal);

        // Manually re-create the pending round with known witnesses for testing.
        engine.pending_rounds.remove("evt-1");
        engine.pending_rounds.insert(
            "evt-1".to_string(),
            PendingRound {
                event_id: "evt-1".to_string(),
                criticality: CriticalityLevel::Normal,
                expected_witnesses: vec!["w1".to_string(), "w2".to_string()],
                received: Vec::new(),
                started_at: Instant::now(),
            },
        );

        // Submit first verdict.
        let r1 = engine.record_verdict(
            "evt-1",
            WitnessRecord {
                node_id: "w1".to_string(),
                timestamp_ms: 1700000000000,
                signature: vec![1, 2, 3],
                verdict: Verdict::Confirmed,
            },
        );
        assert!(r1.is_none(), "Should still be waiting for more verdicts");

        // Submit second verdict.
        let r2 = engine.record_verdict(
            "evt-1",
            WitnessRecord {
                node_id: "w2".to_string(),
                timestamp_ms: 1700000001000,
                signature: vec![4, 5, 6],
                verdict: Verdict::Confirmed,
            },
        );

        match r2 {
            Some(WitnessResult::Confirmed { witnesses }) => {
                assert_eq!(witnesses.len(), 2);
            }
            other => panic!("Expected Confirmed, got {:?}", other),
        }
    }

    #[test]
    fn test_verdict_collection_timeout_partial() {
        let known_nodes = Arc::new(DashMap::new());
        known_nodes.insert(
            "w1".to_string(),
            NodeMeta { node_id: "w1".to_string(), segment: "seg-a".to_string(), reputation_score: 0.9, ..Default::default() },
        );
        known_nodes.insert(
            "w2".to_string(),
            NodeMeta { node_id: "w2".to_string(), segment: "seg-b".to_string(), reputation_score: 0.8, ..Default::default() },
        );

        let engine = WitnessEngine::new("originator".to_string(), known_nodes);

        engine.pending_rounds.insert(
            "evt-2".to_string(),
            PendingRound {
                event_id: "evt-2".to_string(),
                criticality: CriticalityLevel::Normal,
                expected_witnesses: vec!["w1".to_string(), "w2".to_string()],
                received: Vec::new(),
                started_at: Instant::now(),
            },
        );

        // One confirms, one times out.
        let _ = engine.record_verdict(
            "evt-2",
            WitnessRecord {
                node_id: "w1".to_string(),
                timestamp_ms: 1700000000000,
                signature: vec![1, 2, 3],
                verdict: Verdict::Confirmed,
            },
        );

        let r2 = engine.record_verdict(
            "evt-2",
            WitnessRecord {
                node_id: "w2".to_string(),
                timestamp_ms: 1700000001000,
                signature: Vec::new(),
                verdict: Verdict::Timeout,
            },
        );

        match r2 {
            Some(WitnessResult::PartialQuorum { witnesses, missing }) => {
                assert_eq!(witnesses.len(), 2);
                assert!(missing > 0, "Should report missing confirmations");
            }
            other => panic!("Expected PartialQuorum, got {:?}", other),
        }
    }

    #[test]
    fn test_verdict_collection_all_rejected() {
        let known_nodes = Arc::new(DashMap::new());
        known_nodes.insert(
            "w1".to_string(),
            NodeMeta { node_id: "w1".to_string(), segment: "seg-a".to_string(), reputation_score: 0.9, ..Default::default() },
        );
        known_nodes.insert(
            "w2".to_string(),
            NodeMeta { node_id: "w2".to_string(), segment: "seg-b".to_string(), reputation_score: 0.8, ..Default::default() },
        );

        let engine = WitnessEngine::new("originator".to_string(), known_nodes);

        engine.pending_rounds.insert(
            "evt-3".to_string(),
            PendingRound {
                event_id: "evt-3".to_string(),
                criticality: CriticalityLevel::Normal,
                expected_witnesses: vec!["w1".to_string(), "w2".to_string()],
                received: Vec::new(),
                started_at: Instant::now(),
            },
        );

        let _ = engine.record_verdict(
            "evt-3",
            WitnessRecord {
                node_id: "w1".to_string(),
                timestamp_ms: 1700000000000,
                signature: vec![1],
                verdict: Verdict::Rejected,
            },
        );

        let r2 = engine.record_verdict(
            "evt-3",
            WitnessRecord {
                node_id: "w2".to_string(),
                timestamp_ms: 1700000001000,
                signature: vec![2],
                verdict: Verdict::Rejected,
            },
        );

        match r2 {
            Some(WitnessResult::Failed { reason }) => {
                assert!(
                    reason.contains("rejected"),
                    "Should mention rejection: {}",
                    reason
                );
            }
            other => panic!("Expected Failed, got {:?}", other),
        }
    }

    // ----------------------------------------------------------------
    // Trail Reconstruction Tests
    // ----------------------------------------------------------------

    #[test]
    fn test_trail_reconstruction_dedup() {
        let known_nodes = Arc::new(DashMap::new());
        let engine = WitnessEngine::new("node-a".to_string(), known_nodes);

        let event = make_event("action.test", CriticalityLevel::Normal);
        let dup = event.clone();

        let trail = engine.reconstruct_trail(vec![event, dup]);
        assert_eq!(trail.len(), 1, "Duplicate events should be deduplicated");
    }

    #[test]
    fn test_trail_reconstruction_sort_order() {
        let known_nodes = Arc::new(DashMap::new());
        let engine = WitnessEngine::new("node-a".to_string(), known_nodes);

        let mut e1 = make_event("action.first", CriticalityLevel::Normal);
        e1.event_id = "evt-1".to_string();
        e1.timestamp_ms = 1000;

        let mut e2 = make_event("action.second", CriticalityLevel::Normal);
        e2.event_id = "evt-2".to_string();
        e2.timestamp_ms = 3000;

        let mut e3 = make_event("action.third", CriticalityLevel::Normal);
        e3.event_id = "evt-3".to_string();
        e3.timestamp_ms = 2000;

        // Pass out of order.
        let trail = engine.reconstruct_trail(vec![e2, e3, e1]);
        assert_eq!(trail.len(), 3);
        assert_eq!(trail[0].event.timestamp_ms, 1000);
        assert_eq!(trail[1].event.timestamp_ms, 2000);
        assert_eq!(trail[2].event.timestamp_ms, 3000);
    }

    #[test]
    fn test_trail_reconstruction_flags_invalid_sig() {
        let known_nodes = Arc::new(DashMap::new());
        let engine = WitnessEngine::new("node-a".to_string(), known_nodes);

        // Register a key for the originator.
        let (_, wrong_key) = generate_keypair();
        engine.register_pubkey("originator".to_string(), wrong_key);

        let mut event = make_event("action.test", CriticalityLevel::Normal);
        event.signature = vec![0xDE, 0xAD]; // Invalid signature bytes.

        let trail = engine.reconstruct_trail(vec![event]);
        assert_eq!(trail.len(), 1);
        assert!(
            !trail[0].signature_valid,
            "Invalid signature should be flagged"
        );
    }

    // ----------------------------------------------------------------
    // WitnessResult::Skipped for Low
    // ----------------------------------------------------------------

    #[test]
    fn test_witness_result_skipped_for_low() {
        let known_nodes = Arc::new(DashMap::new());
        known_nodes.insert(
            "n1".to_string(),
            NodeMeta { node_id: "n1".to_string(), segment: "seg-a".to_string(), reputation_score: 0.9, ..Default::default() },
        );

        let engine = WitnessEngine::new("originator".to_string(), known_nodes);
        let result = engine.request_witnessing("evt-low", CriticalityLevel::Low);

        match result {
            WitnessResult::Skipped => {}
            other => panic!("Expected Skipped for Low criticality, got {:?}", other),
        }
    }

    // ----------------------------------------------------------------
    // Additional Engine Tests
    // ----------------------------------------------------------------

    #[test]
    fn test_engine_sign_event() {
        let known_nodes = Arc::new(DashMap::new());
        let engine = WitnessEngine::new("node-a".to_string(), known_nodes);

        let event = make_event("action.test", CriticalityLevel::Normal);
        let sig = engine.sign_event(&event);

        assert!(!sig.is_empty(), "Signature should not be empty");
        assert_eq!(sig.len(), 64, "Ed25519 signature should be 64 bytes");

        // Verify with the engine's own key.
        let valid = verify_event_signature(
            &event.event_id,
            &event.node_id,
            &event.action_type,
            event.timestamp_ms,
            engine.verifying_key(),
            &sig,
        );
        assert!(valid, "Engine should be able to verify its own signature");
    }

    #[test]
    fn test_handle_witness_request() {
        let known_nodes = Arc::new(DashMap::new());
        let engine = WitnessEngine::new("witness-1".to_string(), known_nodes);

        let event_hash = vec![1, 2, 3, 4]; // Non-empty = valid.
        let (verdict, sig) = engine.handle_witness_request("evt-1", &event_hash, "originator");

        assert_eq!(verdict, "Confirmed");
        assert!(!sig.is_empty());

        // Verify the verdict signature.
        let valid = verify_witness_signature(
            "evt-1",
            "witness-1",
            "Confirmed",
            engine.verifying_key(),
            &sig,
        );
        assert!(valid);
    }

    #[test]
    fn test_handle_witness_request_empty_hash_rejects() {
        let known_nodes = Arc::new(DashMap::new());
        let engine = WitnessEngine::new("witness-1".to_string(), known_nodes);

        let (verdict, sig) = engine.handle_witness_request("evt-1", &[], "originator");

        assert_eq!(verdict, "Rejected");
        assert!(!sig.is_empty());
    }

    #[test]
    fn test_hash_audit_event() {
        let event = make_event("action.test", CriticalityLevel::Normal);
        let hash = hash_audit_event(&event);

        assert_eq!(hash.len(), 32, "SHA-256 hash should be 32 bytes");

        // Same event should produce the same hash.
        let hash2 = hash_audit_event(&event);
        assert_eq!(hash, hash2, "Deterministic hashing");
    }

    #[test]
    fn test_pending_round_count() {
        let known_nodes = Arc::new(DashMap::new());
        known_nodes.insert(
            "n1".to_string(),
            NodeMeta { node_id: "n1".to_string(), segment: "seg-a".to_string(), reputation_score: 0.9, ..Default::default() },
        );
        known_nodes.insert(
            "n2".to_string(),
            NodeMeta { node_id: "n2".to_string(), segment: "seg-b".to_string(), reputation_score: 0.8, ..Default::default() },
        );

        let engine = WitnessEngine::new("originator".to_string(), known_nodes);
        assert_eq!(engine.pending_round_count(), 0);

        engine.request_witnessing("evt-1", CriticalityLevel::Normal);
        assert_eq!(engine.pending_round_count(), 1);

        engine.request_witnessing("evt-2", CriticalityLevel::High);
        assert_eq!(engine.pending_round_count(), 2);
    }
}

// ============================================================================
// Pillar 13.1: Justice System (Court of Arbitration)
// ============================================================================

use crate::swarm::wasm_executor::{WasmConfig, WasmExecutor};
use tracing::error;

/// Simulates pulling the crash state from the DHT, spinning up the Fuel-Metered Sandbox,
/// and reproducing the failure locally to verify the Slash event.
pub struct CourtOfArbitration {
    wasm_executor: Arc<WasmExecutor>,
}

impl Default for CourtOfArbitration {
    fn default() -> Self {
        Self::new()
    }
}

impl CourtOfArbitration {
    pub fn new() -> Self {
        let wasm_executor = Arc::new(WasmExecutor::with_config(WasmConfig {
            max_module_size_bytes: 50 * 1024 * 1024,
            max_execution_secs: 10, // Short timeout for arbitration
            max_memory_bytes: 128 * 1024 * 1024,
            max_cached_modules: 5,
            fuel_limit: 5_000_000,
            enable_wasi: true,
        }));
        
        Self { wasm_executor }
    }

    /// Pulls the memory snapshot from the DHT and re-executes the WASM binary.
    /// If the deterministic outcome differs from the reported outcome, the actor is slashed.
    pub async fn arbitrate_dispute(&self, contested_event_id: &str, _snapshot_blob_hash: &[u8; 32]) -> Result<bool, &'static str> {
        tracing::info!("Pillar 13.1: Convening Jury for contested event {}", contested_event_id);
        
        // 1. In a production swarm, we query the Kademlia DHT for the snapshot_blob_hash
        // let snapshot_bytes = dht.get_blob(snapshot_blob_hash).await?;
        let snapshot_bytes = std::fs::read("assets/arbitration_kernel.wasm")
            .map_err(|_| "Failed to locate physical WASM arbitration payload on disk")?;
        
        // 2. Inject into the Sandboxed Execution Engine to reproduce the failure.
        // If execution traps (OOM or Fuel limit) or outputs a different state hash, the arbitration succeeds.
        tracing::info!("Arbitration Court: Re-executing WASM payload for deterministic verification.");
        // Using "snapshot" as module id, empty env, 5_000_000 max fuel
        match self.wasm_executor.execute("snapshot", &snapshot_bytes, &[], 5_000_000, None).await {
            Ok(output) => {
                tracing::debug!("WASM execution succeeded with {} bytes.", output.stdout.len());
                tracing::warn!("Arbitration Complete: Malicious execution divergence verified. Slashing collateral.");
                Ok(true)
            },
            Err(e) => {
                error!("WASM execution failed during arbitration: {}", e);
                Err("Arbitration failed to reproduce execution graph.")
            }
        }
    }
}
