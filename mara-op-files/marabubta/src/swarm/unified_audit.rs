// Marabunta - Licensed under the MIT License.
//! Event sink unification bridging highestsec audit events to the swarm event system.
//!
//! The [`UnifiedAuditStore`] maintains a hash-chained, append-only log of
//! audit events from all sources (swarm, highestsec, blind pipeline). Each
//! event is assigned a monotonic sequence number and linked to its
//! predecessor via a SHA-256 hash chain, enabling tamper detection.

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ============================================================================
// UnifiedAuditEvent
// ============================================================================

/// A single entry in the unified audit trail, hash-chained to its predecessor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedAuditEvent {
    /// Origin subsystem: "swarm", "highestsec", or "blind".
    pub event_source: String,
    /// Descriptive event type (e.g. "job.submit", "zone.created").
    pub event_type: String,
    /// When this event occurred.
    pub timestamp: DateTime<Utc>,
    /// Optional node that generated or is the subject of this event.
    pub node_id: Option<String>,
    /// Optional sovereignty zone identifier.
    pub zone_id: Option<String>,
    /// SHA-256 hash of this event's content + prev_hash.
    pub event_hash: [u8; 32],
    /// Hash of the immediately preceding event (genesis uses zeroes).
    pub prev_hash: [u8; 32],
    /// Monotonically increasing sequence number (starts at 1).
    pub sequence: u64,
    /// Arbitrary JSON payload.
    pub payload: serde_json::Value,
    /// Optional GLOBAL_ALLIANCE_T1/national classification label.
    pub classification: Option<String>,
    /// Whether this event has been passed through the sanitizer.
    pub sanitized: bool,
}

// ============================================================================
// UnifiedAuditStore
// ============================================================================

/// Thread-safe, hash-chained audit event store.
///
/// Events are stored in a [`DashMap`] keyed by sequence number for
/// O(1) lookup. The hash chain is maintained via an atomic sequence
/// counter and a mutex-protected previous-hash value.
pub struct UnifiedAuditStore {
    events: DashMap<u64, UnifiedAuditEvent>,
    sequence: AtomicU64,
    prev_hash: Mutex<[u8; 32]>,
}

impl UnifiedAuditStore {
    /// Create a new, empty audit store. The genesis prev_hash is all zeroes.
    pub fn new() -> Self {
        Self {
            events: DashMap::new(),
            sequence: AtomicU64::new(0),
            prev_hash: Mutex::new([0u8; 32]),
        }
    }

    /// Append a new audit event. Returns a clone of the stored event
    /// (including its computed hash and sequence number).
    pub fn append(
        &self,
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
        node_id: Option<String>,
        zone_id: Option<String>,
        classification: Option<String>,
    ) -> UnifiedAuditEvent {
        let timestamp = Utc::now();
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        let prev = {
            let prev_guard = self.prev_hash.lock();
            *prev_guard
        };

        // Compute the event hash.
        let event_hash = Self::compute_hash(
            source,
            event_type,
            &timestamp,
            seq,
            &payload,
            &prev,
        );

        let event = UnifiedAuditEvent {
            event_source: source.to_string(),
            event_type: event_type.to_string(),
            timestamp,
            node_id,
            zone_id,
            event_hash,
            prev_hash: prev,
            sequence: seq,
            payload,
            classification,
            sanitized: false,
        };

        // Update the chain head.
        {
            let mut prev_guard = self.prev_hash.lock();
            *prev_guard = event_hash;
        }

        self.events.insert(seq, event.clone());
        event
    }

    /// Retrieve an event by its sequence number.
    pub fn get(&self, seq: u64) -> Option<UnifiedAuditEvent> {
        self.events.get(&seq).map(|r| r.value().clone())
    }

    /// Return all events with sequence numbers in `[from, to]` (inclusive),
    /// sorted by sequence.
    pub fn query_range(&self, from: u64, to: u64) -> Vec<UnifiedAuditEvent> {
        let mut results: Vec<UnifiedAuditEvent> = Vec::new();
        for seq in from..=to {
            if let Some(entry) = self.events.get(&seq) {
                results.push(entry.value().clone());
            }
        }
        results.sort_by_key(|e| e.sequence);
        results
    }

    /// Verify the integrity of the entire hash chain.
    ///
    /// Returns `true` if every event's `prev_hash` matches the preceding
    /// event's `event_hash` and every event's own hash is correctly
    /// computed. Returns `false` at the first inconsistency.
    pub fn verify_chain(&self) -> bool {
        let current_seq = self.sequence.load(Ordering::SeqCst);
        if current_seq == 0 {
            return true; // empty chain is valid
        }

        let mut expected_prev = [0u8; 32]; // genesis

        for seq in 1..=current_seq {
            let event = match self.events.get(&seq) {
                Some(e) => e.value().clone(),
                None => return false, // missing event breaks chain
            };

            if event.prev_hash != expected_prev {
                return false;
            }

            let recomputed = Self::compute_hash(
                &event.event_source,
                &event.event_type,
                &event.timestamp,
                event.sequence,
                &event.payload,
                &event.prev_hash,
            );

            if event.event_hash != recomputed {
                return false;
            }

            expected_prev = event.event_hash;
        }

        true
    }

    /// Number of events in the store.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Compute the SHA-256 hash for an event given its fields and the
    /// previous event's hash.
    fn compute_hash(
        source: &str,
        event_type: &str,
        timestamp: &DateTime<Utc>,
        sequence: u64,
        payload: &serde_json::Value,
        prev_hash: &[u8; 32],
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(source.as_bytes());
        hasher.update(event_type.as_bytes());
        hasher.update(timestamp.to_rfc3339().as_bytes());
        hasher.update(sequence.to_le_bytes());
        hasher.update(payload.to_string().as_bytes());
        hasher.update(prev_hash);
        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }
}

impl Default for UnifiedAuditStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_store_is_empty() {
        let store = UnifiedAuditStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_append_increments_sequence() {
        let store = UnifiedAuditStore::new();
        let e1 = store.append("swarm", "node.join", json!({}), None, None, None);
        let e2 = store.append("highestsec", "zone.created", json!({}), None, None, None);
        assert_eq!(e1.sequence, 1);
        assert_eq!(e2.sequence, 2);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_get_existing_event() {
        let store = UnifiedAuditStore::new();
        let e = store.append("swarm", "test", json!({"key": "value"}), None, None, None);
        let retrieved = store.get(e.sequence).expect("should exist");
        assert_eq!(retrieved.event_type, "test");
        assert_eq!(retrieved.payload["key"], "value");
    }

    #[test]
    fn test_get_nonexistent_returns_none() {
        let store = UnifiedAuditStore::new();
        assert!(store.get(999).is_none());
    }

    #[test]
    fn test_query_range() {
        let store = UnifiedAuditStore::new();
        for i in 0..5 {
            store.append("swarm", &format!("event_{}", i), json!({}), None, None, None);
        }
        let range = store.query_range(2, 4);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].sequence, 2);
        assert_eq!(range[2].sequence, 4);
    }

    #[test]
    fn test_hash_chain_genesis() {
        let store = UnifiedAuditStore::new();
        let e = store.append("swarm", "genesis", json!({}), None, None, None);
        assert_eq!(e.prev_hash, [0u8; 32]);
        assert_ne!(e.event_hash, [0u8; 32]);
    }

    #[test]
    fn test_hash_chain_linkage() {
        let store = UnifiedAuditStore::new();
        let e1 = store.append("swarm", "first", json!({}), None, None, None);
        let e2 = store.append("swarm", "second", json!({}), None, None, None);
        assert_eq!(e2.prev_hash, e1.event_hash);
    }

    #[test]
    fn test_verify_chain_empty() {
        let store = UnifiedAuditStore::new();
        assert!(store.verify_chain());
    }

    #[test]
    fn test_verify_chain_valid() {
        let store = UnifiedAuditStore::new();
        for i in 0..10 {
            store.append(
                "highestsec",
                &format!("event_{}", i),
                json!({"i": i}),
                Some(format!("node-{}", i)),
                Some("zone-eu".to_string()),
                Some("RESTRICTED".to_string()),
            );
        }
        assert!(store.verify_chain());
        assert_eq!(store.len(), 10);
    }

    #[test]
    fn test_verify_chain_detects_tampering() {
        let store = UnifiedAuditStore::new();
        store.append("swarm", "first", json!({}), None, None, None);
        store.append("swarm", "second", json!({}), None, None, None);

        // Tamper with event 1's hash.
        if let Some(mut entry) = store.events.get_mut(&1) {
            entry.event_hash = [0xFFu8; 32];
        }

        assert!(!store.verify_chain());
    }

    #[test]
    fn test_event_fields_populated() {
        let store = UnifiedAuditStore::new();
        let e = store.append(
            "blind",
            "pipeline.complete",
            json!({"job_id": "abc"}),
            Some("node-123".to_string()),
            Some("zone-de".to_string()),
            Some("SECRET".to_string()),
        );
        assert_eq!(e.event_source, "blind");
        assert_eq!(e.event_type, "pipeline.complete");
        assert_eq!(e.node_id, Some("node-123".to_string()));
        assert_eq!(e.zone_id, Some("zone-de".to_string()));
        assert_eq!(e.classification, Some("SECRET".to_string()));
        assert!(!e.sanitized);
    }
}
