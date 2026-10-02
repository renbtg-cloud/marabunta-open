// Marabunta - Licensed under the MIT License.
//! Gossip-propagated key rotation protocol.
//!
//! Implements a full lifecycle for cryptographic key management within the
//! swarm: initial enrollment, graceful rotation with dual-key grace periods,
//! emergency rotation (immediate invalidation of the old key), revocation by
//! the node itself or by a zone authority, and garbage collection of expired
//! rotation state.
//!
//! All announcements and revocations carry Ed25519 signatures so that peers
//! can independently verify legitimacy before updating their key stores.
//! Structures are designed for gossip propagation: compact, deterministic
//! serialization, and monotonic sequence numbers for replay rejection.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::swarm::types::NodeId;

// ============================================================================
// Constants
// ============================================================================

/// Default grace period for graceful key rotations.
pub const DEFAULT_GRACE_PERIOD_SECS: i64 = 3600; // 1 hour

// ============================================================================
// Types
// ============================================================================

/// The type/purpose of a cryptographic key within the swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeyType {
    /// Node identity key used for gossip signing and authentication.
    NodeIdentity,
    /// Gatekeeper key used for admission jury signatures.
    Gatekeeper,
    /// Zone authority key used for cross-zone attestations.
    ZoneAuthority,
    /// TLS certificate key used for transport encryption.
    TlsCertificate,
    /// Kyber encapsulation key for post-quantum key exchange.
    KyberEncapsulation,
}

/// Unique identifier for a rotation event.
pub type RotationId = String;

/// Reason a key was revoked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RevocationReason {
    /// The private key was (or is suspected to be) compromised.
    KeyCompromised,
    /// The node is being permanently removed from the swarm.
    NodeDecommissioned,
    /// Routine scheduled rotation per policy.
    ScheduledRotation,
    /// An operator or policy engine mandated the change.
    PolicyChange,
}

/// Gossip-propagated announcement that a node is rotating a key.
///
/// The announcement contains both the old and new public keys plus
/// cryptographic proof that the holder of the old key authorized the
/// rotation and that the holder of the new key can prove possession.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRotationAnnouncement {
    /// Unique identifier for this rotation event.
    pub rotation_id: RotationId,
    /// The node performing the rotation.
    pub node_id: NodeId,
    /// Which key is being rotated.
    pub key_type: KeyType,
    /// The new public key (Ed25519 verifying key bytes).
    pub new_public_key: Vec<u8>,
    /// The old public key being replaced.
    pub old_public_key: Vec<u8>,
    /// Signature of canonical_bytes() by the OLD key, endorsing the rotation.
    pub old_key_endorsement: Vec<u8>,
    /// Signature of canonical_bytes() by the NEW key, proving possession.
    pub new_key_proof: Vec<u8>,
    /// When the announcement was created.
    pub announced_at: DateTime<Utc>,
    /// When the grace period ends (old key ceases to be valid).
    pub grace_period_ends: DateTime<Utc>,
    /// If true, the old key is invalidated immediately (no grace period).
    pub emergency: bool,
    /// Monotonically increasing sequence number for replay rejection.
    pub sequence: u64,
}

/// Gossip-propagated revocation of a key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRevocation {
    /// The node whose key is being revoked.
    pub node_id: NodeId,
    /// Which key type is being revoked.
    pub key_type: KeyType,
    /// The public key being revoked.
    pub revoked_public_key: Vec<u8>,
    /// Why the key is being revoked.
    pub reason: RevocationReason,
    /// When the revocation was issued.
    pub revoked_at: DateTime<Utc>,
    /// Ed25519 signature of canonical_bytes() by the authority.
    pub authority_signature: Vec<u8>,
    /// Public key of the authority that signed the revocation.
    pub authority_public_key: Vec<u8>,
    /// Monotonically increasing sequence for replay protection.
    pub sequence: u64,
}

/// The lifecycle state of a registered key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyState {
    /// The key is the current, active key for this (node, key_type).
    Active,
    /// The key is being replaced; both old and new are accepted until
    /// the grace period expires.
    GracePeriod {
        /// The replacement public key that will become Active.
        replacement: Vec<u8>,
        /// When the old key stops being accepted.
        grace_ends: DateTime<Utc>,
    },
    /// The key has been revoked and must not be accepted.
    Revoked {
        reason: RevocationReason,
        revoked_at: DateTime<Utc>,
    },
}

/// A record tracking a single registered key for a (node, key_type) pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRecord {
    /// The current public key bytes.
    pub public_key: Vec<u8>,
    /// Which key type this record tracks.
    pub key_type: KeyType,
    /// Current lifecycle state.
    pub state: KeyState,
    /// When this key was first registered.
    pub registered_at: DateTime<Utc>,
    /// The last accepted rotation sequence number (for replay rejection).
    pub last_rotation_seq: u64,
}

// ============================================================================
// Canonical bytes and verification — KeyRotationAnnouncement
// ============================================================================

impl KeyRotationAnnouncement {
    /// Deterministic byte representation for signing.
    ///
    /// Excludes the signature fields (`old_key_endorsement`, `new_key_proof`)
    /// so that they can be computed over this canonical form.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        buf.extend_from_slice(self.rotation_id.as_bytes());
        // NodeId is a Uuid wrapper — serialize its bytes.
        buf.extend_from_slice(self.node_id.0.as_bytes());
        // KeyType as a single discriminant byte.
        buf.push(key_type_discriminant(&self.key_type));
        buf.extend_from_slice(&self.new_public_key);
        buf.extend_from_slice(&self.old_public_key);
        buf.extend_from_slice(&self.announced_at.timestamp_millis().to_be_bytes());
        buf.extend_from_slice(&self.grace_period_ends.timestamp_millis().to_be_bytes());
        buf.push(if self.emergency { 1 } else { 0 });
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        // Final SHA-256 for a fixed-size canonical digest.
        Sha256::digest(&buf).to_vec()
    }

    /// Verify that the old key holder endorsed this rotation.
    pub fn verify_endorsement(&self) -> bool {
        let canonical = self.canonical_bytes();
        let Ok(vk) = VerifyingKey::from_bytes(
            self.old_public_key
                .as_slice()
                .try_into()
                .unwrap_or(&[0u8; 32]),
        ) else {
            return false;
        };
        let Ok(sig) = Signature::from_slice(&self.old_key_endorsement) else {
            return false;
        };
        vk.verify(&canonical, &sig).is_ok()
    }

    /// Verify that the new key holder can prove possession.
    pub fn verify_new_key_proof(&self) -> bool {
        let canonical = self.canonical_bytes();
        let Ok(vk) = VerifyingKey::from_bytes(
            self.new_public_key
                .as_slice()
                .try_into()
                .unwrap_or(&[0u8; 32]),
        ) else {
            return false;
        };
        let Ok(sig) = Signature::from_slice(&self.new_key_proof) else {
            return false;
        };
        vk.verify(&canonical, &sig).is_ok()
    }
}

// ============================================================================
// Canonical bytes and verification — KeyRevocation
// ============================================================================

impl KeyRevocation {
    /// Deterministic byte representation for signing.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(self.node_id.0.as_bytes());
        buf.push(key_type_discriminant(&self.key_type));
        buf.extend_from_slice(&self.revoked_public_key);
        buf.push(revocation_reason_discriminant(&self.reason));
        buf.extend_from_slice(&self.revoked_at.timestamp_millis().to_be_bytes());
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        Sha256::digest(&buf).to_vec()
    }

    /// Verify that the authority signature is valid.
    pub fn verify_authority_signature(&self) -> bool {
        let canonical = self.canonical_bytes();
        let Ok(vk) = VerifyingKey::from_bytes(
            self.authority_public_key
                .as_slice()
                .try_into()
                .unwrap_or(&[0u8; 32]),
        ) else {
            return false;
        };
        let Ok(sig) = Signature::from_slice(&self.authority_signature) else {
            return false;
        };
        vk.verify(&canonical, &sig).is_ok()
    }
}

// ============================================================================
// Discriminant helpers (stable byte tags for canonical serialization)
// ============================================================================

fn key_type_discriminant(kt: &KeyType) -> u8 {
    match kt {
        KeyType::NodeIdentity => 0,
        KeyType::Gatekeeper => 1,
        KeyType::ZoneAuthority => 2,
        KeyType::TlsCertificate => 3,
        KeyType::KyberEncapsulation => 4,
    }
}

fn revocation_reason_discriminant(r: &RevocationReason) -> u8 {
    match r {
        RevocationReason::KeyCompromised => 0,
        RevocationReason::NodeDecommissioned => 1,
        RevocationReason::ScheduledRotation => 2,
        RevocationReason::PolicyChange => 3,
    }
}

// ============================================================================
// KeyRotationStore
// ============================================================================

/// Thread-safe store for key records, pending rotations, and revocations.
///
/// All maps are `DashMap`-based so that gossip handlers on different Tokio
/// tasks can read/write concurrently without external locking.
pub struct KeyRotationStore {
    /// Current key records indexed by (NodeId, KeyType).
    pub keys: DashMap<(NodeId, KeyType), KeyRecord>,
    /// Pending rotation announcements indexed by RotationId.
    pub pending_rotations: DashMap<RotationId, KeyRotationAnnouncement>,
    /// Revoked keys indexed by (public_key_bytes, KeyType).
    pub revoked_keys: DashMap<(Vec<u8>, KeyType), KeyRevocation>,
}

impl KeyRotationStore {
    /// Create a new, empty store.
    pub fn new() -> Self {
        Self {
            keys: DashMap::new(),
            pending_rotations: DashMap::new(),
            revoked_keys: DashMap::new(),
        }
    }

    /// Register a key for initial enrollment (no prior key required).
    pub fn register_key(&self, node_id: NodeId, key_type: KeyType, public_key: Vec<u8>) {
        let record = KeyRecord {
            public_key,
            key_type,
            state: KeyState::Active,
            registered_at: Utc::now(),
            last_rotation_seq: 0,
        };
        self.keys.insert((node_id, key_type), record);
    }

    /// Process a rotation announcement.
    ///
    /// Returns `true` if the rotation was accepted and applied, `false` if
    /// it was rejected (stale sequence, wrong old key, invalid signatures).
    pub fn process_rotation(&self, announcement: &KeyRotationAnnouncement) -> bool {
        let key = (announcement.node_id, announcement.key_type);

        // Look up existing record.
        let mut entry = match self.keys.get_mut(&key) {
            Some(e) => e,
            None => return false,
        };

        let record = entry.value_mut();

        // Replay protection: sequence must be strictly greater.
        if announcement.sequence <= record.last_rotation_seq {
            return false;
        }

        // The old public key in the announcement must match the current record.
        if announcement.old_public_key != record.public_key {
            return false;
        }

        // Verify cryptographic proofs.
        if !announcement.verify_endorsement() {
            return false;
        }
        if !announcement.verify_new_key_proof() {
            return false;
        }

        // Apply the rotation.
        record.last_rotation_seq = announcement.sequence;

        if announcement.emergency {
            // Emergency: old key is immediately invalid; switch directly.
            record.public_key = announcement.new_public_key.clone();
            record.state = KeyState::Active;
            // Also mark the old key as revoked.
            self.revoked_keys.insert(
                (announcement.old_public_key.clone(), announcement.key_type),
                KeyRevocation {
                    node_id: announcement.node_id,
                    key_type: announcement.key_type,
                    revoked_public_key: announcement.old_public_key.clone(),
                    reason: RevocationReason::KeyCompromised,
                    revoked_at: Utc::now(),
                    authority_signature: Vec::new(),
                    authority_public_key: Vec::new(),
                    sequence: announcement.sequence,
                },
            );
        } else {
            // Graceful: enter grace period (old key stays valid until grace_period_ends).
            record.state = KeyState::GracePeriod {
                replacement: announcement.new_public_key.clone(),
                grace_ends: announcement.grace_period_ends,
            };
        }

        // Track the pending rotation for GC.
        self.pending_rotations
            .insert(announcement.rotation_id.clone(), announcement.clone());

        true
    }

    /// Process a key revocation.
    ///
    /// Returns `true` if the revocation was accepted and applied.
    pub fn process_revocation(&self, revocation: &KeyRevocation) -> bool {
        // Verify the authority signature.
        if !revocation.verify_authority_signature() {
            return false;
        }

        let key = (revocation.node_id, revocation.key_type);

        // Update the key record if it exists.
        if let Some(mut entry) = self.keys.get_mut(&key) {
            let record = entry.value_mut();
            if record.public_key == revocation.revoked_public_key {
                record.state = KeyState::Revoked {
                    reason: revocation.reason.clone(),
                    revoked_at: revocation.revoked_at,
                };
            }
        }

        // Store in revoked set.
        self.revoked_keys.insert(
            (revocation.revoked_public_key.clone(), revocation.key_type),
            revocation.clone(),
        );

        true
    }

    /// Check whether a specific public key is valid (active or in grace period)
    /// for the given node and key type.
    pub fn is_key_valid(&self, node_id: &NodeId, key_type: &KeyType, public_key: &[u8]) -> bool {
        // First check the revoked set.
        if self
            .revoked_keys
            .contains_key(&(public_key.to_vec(), *key_type))
        {
            return false;
        }

        let key = (*node_id, *key_type);
        match self.keys.get(&key) {
            Some(entry) => {
                let record = entry.value();
                match &record.state {
                    KeyState::Active => record.public_key == public_key,
                    KeyState::GracePeriod {
                        replacement,
                        grace_ends,
                    } => {
                        if Utc::now() > *grace_ends {
                            // Grace period expired — only the replacement is valid.
                            *replacement == public_key
                        } else {
                            // Both old and new are valid during grace period.
                            record.public_key == public_key || *replacement == public_key
                        }
                    }
                    KeyState::Revoked { .. } => false,
                }
            }
            None => false,
        }
    }

    /// Garbage-collect expired rotation state.
    ///
    /// - Finalizes grace-period entries whose deadline has passed (promotes the
    ///   replacement key to Active).
    /// - Removes pending rotation entries older than 24 hours.
    pub fn gc_expired(&self) {
        let now = Utc::now();

        // Finalize past-grace-period records.
        let all_keys: Vec<(NodeId, KeyType)> =
            self.keys.iter().map(|r| *r.key()).collect();

        for key in all_keys {
            if let Some(mut entry) = self.keys.get_mut(&key) {
                let record = entry.value_mut();
                if let KeyState::GracePeriod {
                    replacement,
                    grace_ends,
                } = &record.state
                {
                    if now > *grace_ends {
                        let new_key = replacement.clone();
                        record.public_key = new_key;
                        record.state = KeyState::Active;
                    }
                }
            }
        }

        // Remove old pending rotations (older than 24 hours).
        let stale_cutoff = now - Duration::hours(24);
        let stale_ids: Vec<RotationId> = self
            .pending_rotations
            .iter()
            .filter(|r| r.value().announced_at < stale_cutoff)
            .map(|r| r.key().clone())
            .collect();

        for id in stale_ids {
            self.pending_rotations.remove(&id);
        }
    }

    /// Get the current state of a key for a (node, key_type) pair.
    pub fn get_key_state(&self, node_id: &NodeId, key_type: &KeyType) -> Option<KeyState> {
        self.keys
            .get(&(*node_id, *key_type))
            .map(|r| r.value().state.clone())
    }

    /// Get the current active public key for a (node, key_type) pair.
    ///
    /// During a grace period this returns the *replacement* key (the one that
    /// will become canonical). Returns `None` if the key is revoked or absent.
    pub fn active_key(&self, node_id: &NodeId, key_type: &KeyType) -> Option<Vec<u8>> {
        self.keys.get(&(*node_id, *key_type)).and_then(|r| {
            let record = r.value();
            match &record.state {
                KeyState::Active => Some(record.public_key.clone()),
                KeyState::GracePeriod { replacement, .. } => Some(replacement.clone()),
                KeyState::Revoked { .. } => None,
            }
        })
    }
}

// ============================================================================
// KeyRotationEngine
// ============================================================================

/// Manages key rotation for the local node.
///
/// Creates properly signed rotation announcements and revocations that can
/// be propagated via gossip.
pub struct KeyRotationEngine {
    /// This node's identity.
    node_id: NodeId,
    /// Shared store.
    store: Arc<KeyRotationStore>,
    /// Default duration for the dual-key grace period.
    default_grace_period: Duration,
}

impl KeyRotationEngine {
    /// Create a new engine.
    pub fn new(node_id: NodeId, store: Arc<KeyRotationStore>) -> Self {
        Self {
            node_id,
            store,
            default_grace_period: Duration::seconds(DEFAULT_GRACE_PERIOD_SECS),
        }
    }

    /// Perform a graceful rotation with a dual-key grace period.
    ///
    /// The old key remains valid until `grace_period_ends`. Both old and new
    /// keys are accepted by peers during the grace window.
    pub fn rotate_graceful(
        &self,
        key_type: KeyType,
        old_signing_key: &SigningKey,
        new_signing_key: &SigningKey,
    ) -> KeyRotationAnnouncement {
        let now = Utc::now();
        let grace_ends = now + self.default_grace_period;
        let seq = self.next_sequence(key_type);

        let mut announcement = KeyRotationAnnouncement {
            rotation_id: Uuid::new_v4().to_string(),
            node_id: self.node_id,
            key_type,
            new_public_key: new_signing_key.verifying_key().to_bytes().to_vec(),
            old_public_key: old_signing_key.verifying_key().to_bytes().to_vec(),
            old_key_endorsement: Vec::new(),
            new_key_proof: Vec::new(),
            announced_at: now,
            grace_period_ends: grace_ends,
            emergency: false,
            sequence: seq,
        };

        // Sign canonical bytes with both keys.
        let canonical = announcement.canonical_bytes();
        announcement.old_key_endorsement = old_signing_key.sign(&canonical).to_bytes().to_vec();
        announcement.new_key_proof = new_signing_key.sign(&canonical).to_bytes().to_vec();

        announcement
    }

    /// Perform an emergency rotation — the old key is invalidated immediately.
    pub fn rotate_emergency(
        &self,
        key_type: KeyType,
        old_signing_key: &SigningKey,
        new_signing_key: &SigningKey,
    ) -> KeyRotationAnnouncement {
        let now = Utc::now();
        let seq = self.next_sequence(key_type);

        let mut announcement = KeyRotationAnnouncement {
            rotation_id: Uuid::new_v4().to_string(),
            node_id: self.node_id,
            key_type,
            new_public_key: new_signing_key.verifying_key().to_bytes().to_vec(),
            old_public_key: old_signing_key.verifying_key().to_bytes().to_vec(),
            old_key_endorsement: Vec::new(),
            new_key_proof: Vec::new(),
            announced_at: now,
            grace_period_ends: now, // immediate
            emergency: true,
            sequence: seq,
        };

        let canonical = announcement.canonical_bytes();
        announcement.old_key_endorsement = old_signing_key.sign(&canonical).to_bytes().to_vec();
        announcement.new_key_proof = new_signing_key.sign(&canonical).to_bytes().to_vec();

        announcement
    }

    /// Create a signed revocation (can be for self or for another node if this
    /// node is an authority).
    pub fn create_revocation(
        &self,
        node_id: NodeId,
        key_type: KeyType,
        public_key: Vec<u8>,
        reason: RevocationReason,
        authority_key: &SigningKey,
    ) -> KeyRevocation {
        let now = Utc::now();
        let seq = self.next_sequence(key_type);

        let mut revocation = KeyRevocation {
            node_id,
            key_type,
            revoked_public_key: public_key,
            reason,
            revoked_at: now,
            authority_signature: Vec::new(),
            authority_public_key: authority_key.verifying_key().to_bytes().to_vec(),
            sequence: seq,
        };

        let canonical = revocation.canonical_bytes();
        revocation.authority_signature = authority_key.sign(&canonical).to_bytes().to_vec();

        revocation
    }

    /// Compute the next sequence number for a given key type.
    fn next_sequence(&self, key_type: KeyType) -> u64 {
        self.store
            .keys
            .get(&(self.node_id, key_type))
            .map(|r| r.value().last_rotation_seq + 1)
            .unwrap_or(1)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    /// Helper: generate a random Ed25519 signing key.
    fn gen_key() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    /// Helper: build a store with one registered key and return all parts.
    fn setup() -> (NodeId, SigningKey, Arc<KeyRotationStore>) {
        let node = NodeId::new();
        let sk = gen_key();
        let store = Arc::new(KeyRotationStore::new());
        store.register_key(node, KeyType::NodeIdentity, sk.verifying_key().to_bytes().to_vec());
        (node, sk, store)
    }

    // ------------------------------------------------------------------
    // Store basics
    // ------------------------------------------------------------------

    #[test]
    fn test_store_new() {
        let store = KeyRotationStore::new();
        assert!(store.keys.is_empty());
        assert!(store.pending_rotations.is_empty());
        assert!(store.revoked_keys.is_empty());
    }

    #[test]
    fn test_register_and_lookup_key() {
        let (node, sk, store) = setup();
        let pk = sk.verifying_key().to_bytes().to_vec();

        // Key should be active.
        assert!(store.is_key_valid(&node, &KeyType::NodeIdentity, &pk));
        assert_eq!(
            store.active_key(&node, &KeyType::NodeIdentity),
            Some(pk.clone())
        );
        match store.get_key_state(&node, &KeyType::NodeIdentity) {
            Some(KeyState::Active) => {}
            other => panic!("Expected Active, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_new() {
        let (node, _sk, store) = setup();
        let engine = KeyRotationEngine::new(node, store.clone());
        assert_eq!(engine.node_id, node);
        assert_eq!(
            engine.default_grace_period,
            Duration::seconds(DEFAULT_GRACE_PERIOD_SECS)
        );
    }

    // ------------------------------------------------------------------
    // Graceful rotation
    // ------------------------------------------------------------------

    #[test]
    fn test_graceful_rotation_dual_key_acceptance() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk);
        assert!(!ann.emergency);
        assert!(ann.grace_period_ends > Utc::now());

        let accepted = store.process_rotation(&ann);
        assert!(accepted);

        // Both keys should be valid during grace period.
        let old_pk = old_sk.verifying_key().to_bytes().to_vec();
        let new_pk = new_sk.verifying_key().to_bytes().to_vec();
        assert!(store.is_key_valid(&node, &KeyType::NodeIdentity, &old_pk));
        assert!(store.is_key_valid(&node, &KeyType::NodeIdentity, &new_pk));
    }

    #[test]
    fn test_is_key_valid_after_rotation() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk);
        store.process_rotation(&ann);

        // active_key should return the replacement (new) key.
        let new_pk = new_sk.verifying_key().to_bytes().to_vec();
        assert_eq!(
            store.active_key(&node, &KeyType::NodeIdentity),
            Some(new_pk)
        );
    }

    // ------------------------------------------------------------------
    // Emergency rotation
    // ------------------------------------------------------------------

    #[test]
    fn test_emergency_rotation_immediate_invalidation() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_emergency(KeyType::NodeIdentity, &old_sk, &new_sk);
        assert!(ann.emergency);

        let accepted = store.process_rotation(&ann);
        assert!(accepted);

        // Old key should be immediately invalid.
        let old_pk = old_sk.verifying_key().to_bytes().to_vec();
        let new_pk = new_sk.verifying_key().to_bytes().to_vec();
        assert!(!store.is_key_valid(&node, &KeyType::NodeIdentity, &old_pk));
        assert!(store.is_key_valid(&node, &KeyType::NodeIdentity, &new_pk));
    }

    #[test]
    fn test_emergency_rotation_sets_revoked() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_emergency(KeyType::NodeIdentity, &old_sk, &new_sk);
        store.process_rotation(&ann);

        // The old key should appear in the revoked set.
        let old_pk = old_sk.verifying_key().to_bytes().to_vec();
        assert!(store
            .revoked_keys
            .contains_key(&(old_pk, KeyType::NodeIdentity)));
    }

    // ------------------------------------------------------------------
    // Replay and forgery rejection
    // ------------------------------------------------------------------

    #[test]
    fn test_replay_rejected_stale_sequence() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk);
        assert!(store.process_rotation(&ann));

        // Replaying the same announcement should fail.
        // We need to re-register with the new key since the store was updated.
        // Actually the record still has old_public_key matching during grace,
        // but the sequence will be stale.
        assert!(!store.process_rotation(&ann));
    }

    #[test]
    fn test_process_rotation_rejects_stale_seq() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();

        // Manually bump the last_rotation_seq to 10.
        if let Some(mut entry) = store.keys.get_mut(&(node, KeyType::NodeIdentity)) {
            entry.value_mut().last_rotation_seq = 10;
        }

        let engine = KeyRotationEngine::new(node, store.clone());
        let mut ann = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk);
        // Force a stale sequence.
        ann.sequence = 5;
        // Re-sign since canonical bytes changed.
        let canonical = ann.canonical_bytes();
        ann.old_key_endorsement = old_sk.sign(&canonical).to_bytes().to_vec();
        ann.new_key_proof = new_sk.sign(&canonical).to_bytes().to_vec();

        assert!(!store.process_rotation(&ann));
    }

    #[test]
    fn test_forged_rotation_rejected_wrong_old_key() {
        let (node, _old_sk, store) = setup();
        let attacker_sk = gen_key();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        // Attacker tries to rotate using their own key as "old".
        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &attacker_sk, &new_sk);
        assert!(!store.process_rotation(&ann));
    }

    #[test]
    fn test_process_rotation_rejects_wrong_old_key() {
        let (node, _old_sk, store) = setup();
        let wrong_sk = gen_key();
        let new_sk = gen_key();

        let mut ann = KeyRotationAnnouncement {
            rotation_id: Uuid::new_v4().to_string(),
            node_id: node,
            key_type: KeyType::NodeIdentity,
            new_public_key: new_sk.verifying_key().to_bytes().to_vec(),
            old_public_key: wrong_sk.verifying_key().to_bytes().to_vec(),
            old_key_endorsement: Vec::new(),
            new_key_proof: Vec::new(),
            announced_at: Utc::now(),
            grace_period_ends: Utc::now() + Duration::hours(1),
            emergency: false,
            sequence: 1,
        };
        let canonical = ann.canonical_bytes();
        ann.old_key_endorsement = wrong_sk.sign(&canonical).to_bytes().to_vec();
        ann.new_key_proof = new_sk.sign(&canonical).to_bytes().to_vec();

        // old_public_key does not match the registered key.
        assert!(!store.process_rotation(&ann));
    }

    // ------------------------------------------------------------------
    // Grace period expiry
    // ------------------------------------------------------------------

    #[test]
    fn test_grace_period_expiry_old_key_rejected() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();

        // Create an announcement with an already-expired grace period.
        let now = Utc::now();
        let mut ann = KeyRotationAnnouncement {
            rotation_id: Uuid::new_v4().to_string(),
            node_id: node,
            key_type: KeyType::NodeIdentity,
            new_public_key: new_sk.verifying_key().to_bytes().to_vec(),
            old_public_key: old_sk.verifying_key().to_bytes().to_vec(),
            old_key_endorsement: Vec::new(),
            new_key_proof: Vec::new(),
            announced_at: now - Duration::hours(2),
            grace_period_ends: now - Duration::hours(1), // already expired
            emergency: false,
            sequence: 1,
        };
        let canonical = ann.canonical_bytes();
        ann.old_key_endorsement = old_sk.sign(&canonical).to_bytes().to_vec();
        ann.new_key_proof = new_sk.sign(&canonical).to_bytes().to_vec();

        assert!(store.process_rotation(&ann));

        // Grace period has passed — old key should be rejected.
        let old_pk = old_sk.verifying_key().to_bytes().to_vec();
        assert!(!store.is_key_valid(&node, &KeyType::NodeIdentity, &old_pk));

        // New key should be valid.
        let new_pk = new_sk.verifying_key().to_bytes().to_vec();
        assert!(store.is_key_valid(&node, &KeyType::NodeIdentity, &new_pk));
    }

    // ------------------------------------------------------------------
    // Revocation
    // ------------------------------------------------------------------

    #[test]
    fn test_revocation_by_self() {
        let (node, sk, store) = setup();
        let pk = sk.verifying_key().to_bytes().to_vec();
        let engine = KeyRotationEngine::new(node, store.clone());

        let rev = engine.create_revocation(
            node,
            KeyType::NodeIdentity,
            pk.clone(),
            RevocationReason::NodeDecommissioned,
            &sk,
        );
        assert!(store.process_revocation(&rev));
        assert!(!store.is_key_valid(&node, &KeyType::NodeIdentity, &pk));
    }

    #[test]
    fn test_revocation_by_authority() {
        let (node, sk, store) = setup();
        let pk = sk.verifying_key().to_bytes().to_vec();
        let authority_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let rev = engine.create_revocation(
            node,
            KeyType::NodeIdentity,
            pk.clone(),
            RevocationReason::PolicyChange,
            &authority_sk,
        );
        assert!(store.process_revocation(&rev));
        assert!(!store.is_key_valid(&node, &KeyType::NodeIdentity, &pk));
    }

    #[test]
    fn test_is_key_valid_revoked() {
        let (node, sk, store) = setup();
        let pk = sk.verifying_key().to_bytes().to_vec();
        let engine = KeyRotationEngine::new(node, store.clone());

        let rev = engine.create_revocation(
            node,
            KeyType::NodeIdentity,
            pk.clone(),
            RevocationReason::KeyCompromised,
            &sk,
        );
        store.process_revocation(&rev);

        assert!(!store.is_key_valid(&node, &KeyType::NodeIdentity, &pk));
        match store.get_key_state(&node, &KeyType::NodeIdentity) {
            Some(KeyState::Revoked { .. }) => {}
            other => panic!("Expected Revoked, got {:?}", other),
        }
    }

    #[test]
    fn test_process_revocation_marks_revoked() {
        let (node, sk, store) = setup();
        let pk = sk.verifying_key().to_bytes().to_vec();
        let engine = KeyRotationEngine::new(node, store.clone());

        let rev = engine.create_revocation(
            node,
            KeyType::NodeIdentity,
            pk.clone(),
            RevocationReason::ScheduledRotation,
            &sk,
        );
        assert!(store.process_revocation(&rev));

        // Confirm the revoked_keys set has the entry.
        assert!(store
            .revoked_keys
            .contains_key(&(pk, KeyType::NodeIdentity)));
    }

    // ------------------------------------------------------------------
    // GC
    // ------------------------------------------------------------------

    #[test]
    fn test_gc_cleans_expired_rotations() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();

        // Create a rotation with an already-expired grace period.
        let now = Utc::now();
        let mut ann = KeyRotationAnnouncement {
            rotation_id: Uuid::new_v4().to_string(),
            node_id: node,
            key_type: KeyType::NodeIdentity,
            new_public_key: new_sk.verifying_key().to_bytes().to_vec(),
            old_public_key: old_sk.verifying_key().to_bytes().to_vec(),
            old_key_endorsement: Vec::new(),
            new_key_proof: Vec::new(),
            announced_at: now - Duration::hours(25), // older than 24h
            grace_period_ends: now - Duration::hours(24),
            emergency: false,
            sequence: 1,
        };
        let canonical = ann.canonical_bytes();
        ann.old_key_endorsement = old_sk.sign(&canonical).to_bytes().to_vec();
        ann.new_key_proof = new_sk.sign(&canonical).to_bytes().to_vec();

        store.process_rotation(&ann);
        assert_eq!(store.pending_rotations.len(), 1);

        store.gc_expired();

        // GC should have finalized the grace period (promoting the key).
        match store.get_key_state(&node, &KeyType::NodeIdentity) {
            Some(KeyState::Active) => {}
            other => panic!("Expected Active after GC, got {:?}", other),
        }
        // Pending rotation should be cleaned since it's > 24h old.
        assert_eq!(store.pending_rotations.len(), 0);
    }

    // ------------------------------------------------------------------
    // Concurrent and multi-type scenarios
    // ------------------------------------------------------------------

    #[test]
    fn test_concurrent_rotations_same_node() {
        let (node, old_sk, store) = setup();
        let new_sk_1 = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        // First rotation.
        let ann1 = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk_1);
        assert!(store.process_rotation(&ann1));

        // Second rotation while grace period is still open — uses new_sk_1 as old.
        let new_sk_2 = gen_key();
        // Re-register so the record has new_sk_1 as current (simulate gc_expired
        // finalizing, or just update the record directly).
        if let Some(mut entry) = store.keys.get_mut(&(node, KeyType::NodeIdentity)) {
            let r = entry.value_mut();
            r.public_key = new_sk_1.verifying_key().to_bytes().to_vec();
            r.state = KeyState::Active;
        }

        let ann2 = engine.rotate_graceful(KeyType::NodeIdentity, &new_sk_1, &new_sk_2);
        assert!(store.process_rotation(&ann2));

        let new_pk_2 = new_sk_2.verifying_key().to_bytes().to_vec();
        assert!(store.is_key_valid(&node, &KeyType::NodeIdentity, &new_pk_2));
    }

    #[test]
    fn test_multiple_key_types_independent() {
        let node = NodeId::new();
        let store = Arc::new(KeyRotationStore::new());

        let id_sk = gen_key();
        let gk_sk = gen_key();
        store.register_key(
            node,
            KeyType::NodeIdentity,
            id_sk.verifying_key().to_bytes().to_vec(),
        );
        store.register_key(
            node,
            KeyType::Gatekeeper,
            gk_sk.verifying_key().to_bytes().to_vec(),
        );

        let engine = KeyRotationEngine::new(node, store.clone());

        // Rotate only the NodeIdentity key.
        let new_id_sk = gen_key();
        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &id_sk, &new_id_sk);
        assert!(store.process_rotation(&ann));

        // Gatekeeper key should be unaffected.
        let gk_pk = gk_sk.verifying_key().to_bytes().to_vec();
        assert!(store.is_key_valid(&node, &KeyType::Gatekeeper, &gk_pk));
        match store.get_key_state(&node, &KeyType::Gatekeeper) {
            Some(KeyState::Active) => {}
            other => panic!("Gatekeeper should still be Active, got {:?}", other),
        }
    }

    // ------------------------------------------------------------------
    // active_key and get_key_state edge cases
    // ------------------------------------------------------------------

    #[test]
    fn test_active_key_returns_current() {
        let (node, sk, store) = setup();
        let pk = sk.verifying_key().to_bytes().to_vec();
        assert_eq!(
            store.active_key(&node, &KeyType::NodeIdentity),
            Some(pk)
        );
    }

    #[test]
    fn test_get_key_state_none_for_unknown() {
        let store = KeyRotationStore::new();
        let unknown = NodeId::new();
        assert!(store
            .get_key_state(&unknown, &KeyType::ZoneAuthority)
            .is_none());
    }

    // ------------------------------------------------------------------
    // Canonical bytes determinism
    // ------------------------------------------------------------------

    #[test]
    fn test_canonical_bytes_deterministic() {
        let node = NodeId::new();
        let old_sk = gen_key();
        let new_sk = gen_key();

        let ann = KeyRotationAnnouncement {
            rotation_id: "fixed-id".to_string(),
            node_id: node,
            key_type: KeyType::NodeIdentity,
            new_public_key: new_sk.verifying_key().to_bytes().to_vec(),
            old_public_key: old_sk.verifying_key().to_bytes().to_vec(),
            old_key_endorsement: vec![1, 2, 3], // excluded from canonical
            new_key_proof: vec![4, 5, 6],        // excluded from canonical
            announced_at: DateTime::from_timestamp_millis(1700000000000).unwrap(),
            grace_period_ends: DateTime::from_timestamp_millis(1700003600000).unwrap(),
            emergency: false,
            sequence: 42,
        };

        let bytes1 = ann.canonical_bytes();
        let bytes2 = ann.canonical_bytes();
        assert_eq!(bytes1, bytes2);
        assert_eq!(bytes1.len(), 32); // SHA-256 output
    }

    // ------------------------------------------------------------------
    // Signature verification on announcements
    // ------------------------------------------------------------------

    #[test]
    fn test_announcement_verify_endorsement() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk);
        assert!(ann.verify_endorsement());

        // Tamper with the endorsement.
        let mut tampered = ann.clone();
        tampered.old_key_endorsement = vec![0u8; 64];
        assert!(!tampered.verify_endorsement());
    }

    #[test]
    fn test_announcement_verify_new_key_proof() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk);
        assert!(ann.verify_new_key_proof());

        // Tamper with the proof.
        let mut tampered = ann.clone();
        tampered.new_key_proof = vec![0u8; 64];
        assert!(!tampered.verify_new_key_proof());
    }

    // ------------------------------------------------------------------
    // Revocation signature verification
    // ------------------------------------------------------------------

    #[test]
    fn test_revocation_verify_authority_signature() {
        let (node, sk, store) = setup();
        let pk = sk.verifying_key().to_bytes().to_vec();
        let engine = KeyRotationEngine::new(node, store.clone());

        let rev = engine.create_revocation(
            node,
            KeyType::NodeIdentity,
            pk,
            RevocationReason::ScheduledRotation,
            &sk,
        );
        assert!(rev.verify_authority_signature());

        // Tamper.
        let mut tampered = rev.clone();
        tampered.authority_signature = vec![0u8; 64];
        assert!(!tampered.verify_authority_signature());
    }

    // ------------------------------------------------------------------
    // Process rotation updates record
    // ------------------------------------------------------------------

    #[test]
    fn test_process_rotation_updates_record() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk);
        assert!(store.process_rotation(&ann));

        // Check that the record was updated.
        let entry = store.keys.get(&(node, KeyType::NodeIdentity)).unwrap();
        assert_eq!(entry.value().last_rotation_seq, ann.sequence);
        match &entry.value().state {
            KeyState::GracePeriod { replacement, .. } => {
                assert_eq!(*replacement, new_sk.verifying_key().to_bytes().to_vec());
            }
            other => panic!("Expected GracePeriod, got {:?}", other),
        }
    }

    // ------------------------------------------------------------------
    // Serialization roundtrips
    // ------------------------------------------------------------------

    #[test]
    fn test_rotation_announcement_serialization_roundtrip() {
        let (node, old_sk, store) = setup();
        let new_sk = gen_key();
        let engine = KeyRotationEngine::new(node, store.clone());

        let ann = engine.rotate_graceful(KeyType::NodeIdentity, &old_sk, &new_sk);
        let json = serde_json::to_string(&ann).expect("serialize");
        let deserialized: KeyRotationAnnouncement =
            serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.rotation_id, ann.rotation_id);
        assert_eq!(deserialized.node_id, ann.node_id);
        assert_eq!(deserialized.key_type, ann.key_type);
        assert_eq!(deserialized.new_public_key, ann.new_public_key);
        assert_eq!(deserialized.old_public_key, ann.old_public_key);
        assert_eq!(deserialized.old_key_endorsement, ann.old_key_endorsement);
        assert_eq!(deserialized.new_key_proof, ann.new_key_proof);
        assert_eq!(deserialized.emergency, ann.emergency);
        assert_eq!(deserialized.sequence, ann.sequence);
        // Verify signatures still pass after roundtrip.
        assert!(deserialized.verify_endorsement());
        assert!(deserialized.verify_new_key_proof());
    }

    #[test]
    fn test_revocation_serialization_roundtrip() {
        let (node, sk, store) = setup();
        let pk = sk.verifying_key().to_bytes().to_vec();
        let engine = KeyRotationEngine::new(node, store.clone());

        let rev = engine.create_revocation(
            node,
            KeyType::NodeIdentity,
            pk,
            RevocationReason::KeyCompromised,
            &sk,
        );
        let json = serde_json::to_string(&rev).expect("serialize");
        let deserialized: KeyRevocation = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.node_id, rev.node_id);
        assert_eq!(deserialized.key_type, rev.key_type);
        assert_eq!(deserialized.revoked_public_key, rev.revoked_public_key);
        assert_eq!(deserialized.authority_signature, rev.authority_signature);
        assert_eq!(deserialized.sequence, rev.sequence);
        assert!(deserialized.verify_authority_signature());
    }
}
