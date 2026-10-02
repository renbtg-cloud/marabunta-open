// Marabunta - Licensed under the MIT License.
//! Tamper-evident audit log with hash-chain integrity and pluggable persistence.
//!
//! The [`AuditLog`] maintains a ring buffer of [`AuditEntry`] records, each
//! linked to its predecessor by a SHA-256 hash chain. This allows verifying
//! that no entries have been inserted, removed, or modified after the fact.
//!
//! # Features
//!
//! - **Hash chain**: every entry contains `hash` (SHA-256 of its own content
//!   concatenated with `prev_hash`). Tampering with any entry invalidates all
//!   subsequent hashes.
//! - **Ring buffer**: bounded at 1,000,000 entries by default. Older entries
//!   are evicted when the limit is reached.
//! - **Query/filter**: flexible [`AuditFilter`] supporting actor, action,
//!   target, outcome, time range, pagination.
//! - **Export**: JSON, CSV, and NDJSON formats.
//! - **File persistence**: optional append-only NDJSON file with configurable
//!   flush interval. Supports loading from disk (skipping corrupt lines).
//! - **Thread safety**: all operations are protected by `parking_lot` locks.
//!
//! # Usage
//!
//! ```ignore
//! let log = AuditLog::new();
//! log.record(
//!     AuditActor { actor_type: "node".into(), id: "node-abc".into(), display_name: None },
//!     "job.submit".into(),
//!     AuditTarget { target_type: "job".into(), id: "job-123".into(), display_name: None },
//!     AuditOutcome::Success,
//!     serde_json::json!({"chunks": 10}),
//! );
//! assert!(log.verify_chain().valid);
//! ```

use std::collections::VecDeque;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use super::types::Verdict;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::{debug, info, warn};

// ============================================================================
// Constants
// ============================================================================

/// Default maximum entries in the ring buffer.
const DEFAULT_MAX_ENTRIES: usize = 1_000_000;

/// Default flush interval for persistence.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// The genesis hash (used as prev_hash for the first entry).
const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

// ============================================================================
// AuditActor
// ============================================================================

/// Who performed the audited action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditActor {
    /// Type of actor (e.g. "node", "user", "api", "system").
    pub actor_type: String,
    /// Unique identifier for the actor.
    pub id: String,
    /// Optional human-readable display name.
    pub display_name: Option<String>,
}

impl fmt::Display for AuditActor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref name) = self.display_name {
            write!(f, "{}:{} ({})", self.actor_type, self.id, name)
        } else {
            write!(f, "{}:{}", self.actor_type, self.id)
        }
    }
}

// ============================================================================
// AuditTarget
// ============================================================================

/// What was acted upon.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditTarget {
    /// Type of target (e.g. "job", "node", "blob", "agreement").
    pub target_type: String,
    /// Unique identifier for the target.
    pub id: String,
    /// Optional human-readable display name.
    pub display_name: Option<String>,
}

impl fmt::Display for AuditTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref name) = self.display_name {
            write!(f, "{}:{} ({})", self.target_type, self.id, name)
        } else {
            write!(f, "{}:{}", self.target_type, self.id)
        }
    }
}

// ============================================================================
// AuditOutcome
// ============================================================================

/// The result of an audited action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum AuditOutcome {
    /// Action completed successfully.
    Success,
    /// Action failed with a reason.
    Failure {
        /// Why the action failed.
        reason: String,
    },
    /// Action was denied (authorization/policy).
    Denied {
        /// Why the action was denied.
        reason: String,
    },
}

impl fmt::Display for AuditOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuditOutcome::Success => write!(f, "success"),
            AuditOutcome::Failure { reason } => write!(f, "failure({})", reason),
            AuditOutcome::Denied { reason } => write!(f, "denied({})", reason),
        }
    }
}

impl AuditOutcome {
    /// Whether the outcome is a success.
    pub fn is_success(&self) -> bool {
        matches!(self, AuditOutcome::Success)
    }

    /// Whether the outcome is a failure.
    pub fn is_failure(&self) -> bool {
        matches!(self, AuditOutcome::Failure { .. })
    }

    /// Whether the outcome is denied.
    pub fn is_denied(&self) -> bool {
        matches!(self, AuditOutcome::Denied { .. })
    }
}

// ============================================================================
// AuditEntry
// ============================================================================

/// A single audit log entry with hash-chain integrity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Monotonically increasing entry ID.
    pub id: u64,
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
    /// Who performed the action.
    pub actor: AuditActor,
    /// What action was performed.
    pub action: String,
    /// What was acted upon.
    pub target: AuditTarget,
    /// The outcome of the action.
    pub outcome: AuditOutcome,
    /// Arbitrary structured details.
    pub details: serde_json::Value,
    /// Source IP address (if applicable).
    pub source_ip: Option<String>,
    /// API endpoint (if applicable).
    pub api_endpoint: Option<String>,
    /// How long the action took (if applicable).
    pub duration_ms: Option<u64>,
    /// SHA-256 hash of this entry's content + prev_hash.
    pub hash: String,
    /// SHA-256 hash of the preceding entry.
    pub prev_hash: String,
}

impl AuditEntry {
    /// Compute the hash for this entry.
    ///
    /// The hash is SHA-256 of the canonical content string concatenated
    /// with the previous entry's hash.
    fn compute_hash(
        id: u64,
        timestamp: &DateTime<Utc>,
        actor: &AuditActor,
        action: &str,
        target: &AuditTarget,
        outcome: &AuditOutcome,
        details: &serde_json::Value,
        prev_hash: &str,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(id.to_be_bytes());
        hasher.update(timestamp.to_rfc3339().as_bytes());
        hasher.update(actor.actor_type.as_bytes());
        hasher.update(actor.id.as_bytes());
        hasher.update(action.as_bytes());
        hasher.update(target.target_type.as_bytes());
        hasher.update(target.id.as_bytes());

        // Include outcome type.
        match outcome {
            AuditOutcome::Success => hasher.update(b"success"),
            AuditOutcome::Failure { reason } => {
                hasher.update(b"failure:");
                hasher.update(reason.as_bytes());
            }
            AuditOutcome::Denied { reason } => {
                hasher.update(b"denied:");
                hasher.update(reason.as_bytes());
            }
        }

        // Include details (deterministic serialization via serde_json).
        if let Ok(details_str) = serde_json::to_string(details) {
            hasher.update(details_str.as_bytes());
        }

        hasher.update(prev_hash.as_bytes());

        format!("{:x}", hasher.finalize())
    }

    /// Verify that this entry's hash matches its content.
    pub fn verify(&self) -> bool {
        let expected = Self::compute_hash(
            self.id,
            &self.timestamp,
            &self.actor,
            &self.action,
            &self.target,
            &self.outcome,
            &self.details,
            &self.prev_hash,
        );
        self.hash == expected
    }
}

// ============================================================================
// AuditFilter
// ============================================================================

/// Filter criteria for querying the audit log.
///
/// All fields are optional. When multiple fields are set, they are ANDed
/// together (all must match).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuditFilter {
    /// Filter by actor type.
    pub actor_type: Option<String>,
    /// Filter by actor ID.
    pub actor_id: Option<String>,
    /// Filter by action name (exact match).
    pub action: Option<String>,
    /// Filter by target type.
    pub target_type: Option<String>,
    /// Filter by target ID.
    pub target_id: Option<String>,
    /// Filter by outcome type ("success", "failure", "denied").
    pub outcome: Option<String>,
    /// Only entries after this timestamp.
    pub since: Option<DateTime<Utc>>,
    /// Only entries before this timestamp.
    pub until: Option<DateTime<Utc>>,
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
    /// Number of entries to skip.
    pub offset: Option<usize>,
}

impl AuditFilter {
    /// Check whether an entry matches this filter.
    fn matches(&self, entry: &AuditEntry) -> bool {
        if let Some(ref at) = self.actor_type {
            if entry.actor.actor_type != *at {
                return false;
            }
        }
        if let Some(ref ai) = self.actor_id {
            if entry.actor.id != *ai {
                return false;
            }
        }
        if let Some(ref action) = self.action {
            if entry.action != *action {
                return false;
            }
        }
        if let Some(ref tt) = self.target_type {
            if entry.target.target_type != *tt {
                return false;
            }
        }
        if let Some(ref ti) = self.target_id {
            if entry.target.id != *ti {
                return false;
            }
        }
        if let Some(ref outcome) = self.outcome {
            let entry_outcome = match &entry.outcome {
                AuditOutcome::Success => "success",
                AuditOutcome::Failure { .. } => "failure",
                AuditOutcome::Denied { .. } => "denied",
            };
            if entry_outcome != outcome.as_str() {
                return false;
            }
        }
        if let Some(since) = self.since {
            if entry.timestamp < since {
                return false;
            }
        }
        if let Some(until) = self.until {
            if entry.timestamp > until {
                return false;
            }
        }
        true
    }
}

// ============================================================================
// ChainVerification
// ============================================================================

/// Result of verifying the audit log's hash chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainVerification {
    /// Whether the entire chain is valid.
    pub valid: bool,
    /// Total number of entries checked.
    pub total_entries: usize,
    /// Number of entries with valid hashes.
    pub valid_entries: usize,
    /// Number of entries with invalid hashes.
    pub invalid_entries: usize,
    /// ID of the first invalid entry, if any.
    pub first_invalid_id: Option<u64>,
    /// Whether Ed25519 signature verification was performed (requires witness engine).
    #[serde(default)]
    pub signature_verification: bool,
    /// Number of audit events with valid originator signatures.
    #[serde(default)]
    pub signed_events: usize,
    /// Number of witness attestations verified.
    #[serde(default)]
    pub witness_attestations_verified: usize,
}

// ============================================================================
// AuditLog
// ============================================================================

/// Thread-safe, hash-chained audit log with optional file persistence.
///
/// Records are stored in a ring buffer (bounded at `max_entries`) and linked
/// by SHA-256 hashes for tamper detection. The log can be queried, exported,
/// and optionally persisted to disk as NDJSON.
pub struct AuditLog {
    /// Ring buffer of audit entries.
    entries: parking_lot::Mutex<VecDeque<AuditEntry>>,
    /// Next entry ID (monotonically increasing).
    next_id: AtomicU64,
    /// Hash of the last entry (chain head).
    last_hash: parking_lot::Mutex<String>,
    /// Maximum entries in the ring buffer.
    max_entries: usize,
    /// Optional file path for persistence.
    persistence_path: Option<PathBuf>,
    /// Write buffer for batched persistence.
    write_buffer: parking_lot::Mutex<Vec<String>>,
    /// Flush interval for the persistence loop.
    flush_interval: Duration,
    /// Recent AuditEvents with signatures for verification.
    /// Bounded to the same max_entries limit.
    recent_events: parking_lot::Mutex<VecDeque<AuditEvent>>,
    /// Optional channel for PG time-travel persistence.
    /// Audit entries are mirrored here for the PG audit_events table.
    pg_writer: Option<tokio::sync::mpsc::UnboundedSender<AuditEntry>>,
}

impl AuditLog {
    /// Create a new in-memory audit log with default settings.
    pub fn new() -> Self {
        Self {
            entries: parking_lot::Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            last_hash: parking_lot::Mutex::new(GENESIS_HASH.to_string()),
            max_entries: DEFAULT_MAX_ENTRIES,
            persistence_path: None,
            write_buffer: parking_lot::Mutex::new(Vec::new()),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            recent_events: parking_lot::Mutex::new(VecDeque::new()),
            pg_writer: None,
        }
    }

    /// Create a new audit log with file persistence.
    pub fn with_persistence(path: PathBuf) -> Self {
        Self {
            entries: parking_lot::Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            last_hash: parking_lot::Mutex::new(GENESIS_HASH.to_string()),
            max_entries: DEFAULT_MAX_ENTRIES,
            persistence_path: Some(path),
            write_buffer: parking_lot::Mutex::new(Vec::new()),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            recent_events: parking_lot::Mutex::new(VecDeque::new()),
            pg_writer: None,
        }
    }

    /// Attach a PG writer channel for time-travel audit persistence.
    pub fn with_pg_writer(mut self, tx: tokio::sync::mpsc::UnboundedSender<AuditEntry>) -> Self {
        self.pg_writer = Some(tx);
        self
    }

    /// Set a custom maximum entry count.
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    /// Set a custom flush interval.
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Record a new audit entry.
    ///
    /// Assigns a monotonically increasing ID, computes the hash chain,
    /// and appends the entry to the ring buffer. If persistence is configured,
    /// the entry is also buffered for disk flush.
    pub fn record(
        &self,
        actor: AuditActor,
        action: String,
        target: AuditTarget,
        outcome: AuditOutcome,
        details: serde_json::Value,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let timestamp = Utc::now();
        let prev_hash = self.last_hash.lock().clone();

        let hash = AuditEntry::compute_hash(
            id,
            &timestamp,
            &actor,
            &action,
            &target,
            &outcome,
            &details,
            &prev_hash,
        );

        let entry = AuditEntry {
            id,
            timestamp,
            actor,
            action,
            target,
            outcome,
            details,
            source_ip: None,
            api_endpoint: None,
            duration_ms: None,
            hash: hash.clone(),
            prev_hash,
        };

        // Update chain head.
        *self.last_hash.lock() = hash;

        // Serialize for persistence buffer.
        if self.persistence_path.is_some() {
            if let Ok(json) = serde_json::to_string(&entry) {
                self.write_buffer.lock().push(json);
            }
        }

        // Add to ring buffer.
        let mut entries = self.entries.lock();
        entries.push_back(entry.clone());
        while entries.len() > self.max_entries {
            entries.pop_front();
        }

        // Mirror to PG for time-travel queries.
        if let Some(ref tx) = self.pg_writer {
            let _ = tx.send(entry);
        }

        id
    }

    /// Record a signed AuditEvent (with Ed25519 signature and optional witnesses).
    ///
    /// This stores the enriched event for signature verification while also
    /// recording the underlying hash-chain entry.
    pub fn record_event(&self, event: AuditEvent) -> u64 {
        let id = self.record(
            AuditActor {
                actor_type: "node".to_string(),
                id: event.actor_id.clone(),
                display_name: None,
            },
            event.action_type.clone(),
            AuditTarget {
                target_type: "resource".to_string(),
                id: event.target.clone(),
                display_name: None,
            },
            AuditOutcome::Success,
            event.payload.clone(),
        );

        // Store the enriched event for signature verification.
        let mut events = self.recent_events.lock();
        events.push_back(event);
        while events.len() > self.max_entries {
            events.pop_front();
        }

        id
    }

    /// Get recent AuditEvents (with signatures) for verification.
    pub fn get_recent_events(&self) -> Vec<AuditEvent> {
        self.recent_events.lock().iter().cloned().collect()
    }

    /// Convenience: record a successful action.
    pub fn record_success(
        &self,
        actor: AuditActor,
        action: String,
        target: AuditTarget,
        details: serde_json::Value,
    ) -> u64 {
        self.record(actor, action, target, AuditOutcome::Success, details)
    }

    /// Convenience: record a failed action.
    pub fn record_failure(
        &self,
        actor: AuditActor,
        action: String,
        target: AuditTarget,
        reason: String,
        details: serde_json::Value,
    ) -> u64 {
        self.record(
            actor,
            action,
            target,
            AuditOutcome::Failure { reason },
            details,
        )
    }

    /// Convenience: record a denied action.
    pub fn record_denied(
        &self,
        actor: AuditActor,
        action: String,
        target: AuditTarget,
        reason: String,
        details: serde_json::Value,
    ) -> u64 {
        self.record(
            actor,
            action,
            target,
            AuditOutcome::Denied { reason },
            details,
        )
    }

    /// Query the audit log with a filter.
    ///
    /// Returns entries matching the filter, applying offset and limit.
    pub fn query(&self, filter: &AuditFilter) -> Vec<AuditEntry> {
        let entries = self.entries.lock();
        let matching: Vec<&AuditEntry> = entries
            .iter()
            .filter(|e| filter.matches(e))
            .collect();

        let offset = filter.offset.unwrap_or(0);
        let limit = filter.limit.unwrap_or(usize::MAX);

        matching
            .into_iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Count entries matching a filter.
    pub fn count(&self, filter: &AuditFilter) -> usize {
        let entries = self.entries.lock();
        entries.iter().filter(|e| filter.matches(e)).count()
    }

    /// Get a specific entry by ID.
    pub fn get(&self, id: u64) -> Option<AuditEntry> {
        let entries = self.entries.lock();
        entries.iter().find(|e| e.id == id).cloned()
    }

    /// Return the most recent N entries.
    pub fn recent(&self, limit: usize) -> Vec<AuditEntry> {
        let entries = self.entries.lock();
        entries
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Total number of entries in the log.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }

    /// Verify the hash chain integrity of the entire log.
    ///
    /// Returns a [`ChainVerification`] describing the result. If any entry's
    /// hash does not match its content + the preceding entry's hash, the
    /// chain is marked as invalid.
    pub fn verify_chain(&self) -> ChainVerification {
        let entries = self.entries.lock();
        let total = entries.len();
        let mut valid_count = 0;
        let mut invalid_count = 0;
        let mut first_invalid_id = None;

        let mut expected_prev_hash = GENESIS_HASH.to_string();

        for entry in entries.iter() {
            // Check prev_hash linkage.
            let hash_matches = entry.verify() && entry.prev_hash == expected_prev_hash;

            if hash_matches {
                valid_count += 1;
            } else {
                invalid_count += 1;
                if first_invalid_id.is_none() {
                    first_invalid_id = Some(entry.id);
                }
            }

            expected_prev_hash = entry.hash.clone();
        }

        ChainVerification {
            valid: invalid_count == 0,
            total_entries: total,
            valid_entries: valid_count,
            invalid_entries: invalid_count,
            first_invalid_id,
            signature_verification: false,
            signed_events: 0,
            witness_attestations_verified: 0,
        }
    }

    /// Verify the chain with Ed25519 signature validation using a WitnessEngine.
    ///
    /// Extends basic hash-chain verification with:
    /// - Originator signature verification (requires registered public keys)
    /// - Witness attestation verification
    pub fn verify_chain_with_signatures(
        &self,
        audit_events: &[AuditEvent],
        witness_engine: &super::witness::WitnessEngine,
    ) -> ChainVerification {
        let mut base = self.verify_chain();
        base.signature_verification = true;

        let verified = witness_engine.reconstruct_trail(audit_events.to_vec());
        let signed_count = verified.iter().filter(|v| v.signature_valid).count();
        let witness_count: usize = verified
            .iter()
            .flat_map(|v| &v.witness_signatures_valid)
            .filter(|(_, valid)| *valid)
            .count();

        base.signed_events = signed_count;
        base.witness_attestations_verified = witness_count;
        base
    }

    /// Export the log as a JSON array.
    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        let entries = self.entries.lock();
        let entries_vec: Vec<&AuditEntry> = entries.iter().collect();
        serde_json::to_string_pretty(&entries_vec)
    }

    /// Export the log as CSV.
    pub fn export_csv(&self) -> String {
        let entries = self.entries.lock();
        let mut csv = String::from(
            "id,timestamp,actor_type,actor_id,action,target_type,target_id,outcome,hash\n",
        );

        for entry in entries.iter() {
            let outcome_str = match &entry.outcome {
                AuditOutcome::Success => "success".to_string(),
                AuditOutcome::Failure { reason } => format!("failure:{}", reason),
                AuditOutcome::Denied { reason } => format!("denied:{}", reason),
            };

            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{},{}\n",
                entry.id,
                entry.timestamp.to_rfc3339(),
                entry.actor.actor_type,
                entry.actor.id,
                entry.action,
                entry.target.target_type,
                entry.target.id,
                outcome_str,
                entry.hash,
            ));
        }

        csv
    }

    /// Export the log as newline-delimited JSON (NDJSON).
    pub fn export_ndjson(&self) -> String {
        let entries = self.entries.lock();
        let mut ndjson = String::new();

        for entry in entries.iter() {
            if let Ok(line) = serde_json::to_string(entry) {
                ndjson.push_str(&line);
                ndjson.push('\n');
            }
        }

        ndjson
    }

    /// Flush the write buffer to disk.
    ///
    /// This is a no-op if no persistence path is configured.
    pub fn flush_to_disk(&self) -> Result<(), std::io::Error> {
        let path = match &self.persistence_path {
            Some(p) => p,
            None => return Ok(()),
        };

        let buffer: Vec<String> = {
            let mut buf = self.write_buffer.lock();
            std::mem::take(&mut *buf)
        };

        if buffer.is_empty() {
            return Ok(());
        }

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        for line in &buffer {
            writeln!(file, "{}", line)?;
        }

        debug!(
            path = %path.display(),
            entries = buffer.len(),
            "audit log flushed to disk"
        );

        Ok(())
    }

    /// Load entries from disk, skipping corrupt lines.
    ///
    /// Each line should be a JSON-serialized [`AuditEntry`]. Lines that fail
    /// to parse are logged at warn level and skipped.
    ///
    /// Returns the number of entries loaded.
    pub fn load_from_disk(&self) -> Result<usize, std::io::Error> {
        let path = match &self.persistence_path {
            Some(p) => p,
            None => return Ok(0),
        };

        if !path.exists() {
            return Ok(0);
        }

        let content = std::fs::read_to_string(path)?;
        let mut loaded = 0;
        let mut max_id = 0_u64;
        let mut last_hash_loaded = GENESIS_HASH.to_string();

        let mut entries = self.entries.lock();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            match serde_json::from_str::<AuditEntry>(line) {
                Ok(entry) => {
                    if entry.id > max_id {
                        max_id = entry.id;
                    }
                    last_hash_loaded = entry.hash.clone();
                    entries.push_back(entry);
                    loaded += 1;
                }
                Err(e) => {
                    warn!(
                        line = line_num + 1,
                        error = %e,
                        "skipping corrupt audit log line"
                    );
                }
            }
        }

        // Enforce max entries.
        while entries.len() > self.max_entries {
            entries.pop_front();
        }

        drop(entries);

        // Update next_id and last_hash.
        self.next_id.store(max_id + 1, Ordering::SeqCst);
        *self.last_hash.lock() = last_hash_loaded;

        info!(
            path = %path.display(),
            loaded = loaded,
            "audit log loaded from disk"
        );

        Ok(loaded)
    }

    /// Prune entries older than `max_age`.
    ///
    /// Returns the number of entries pruned.
    pub fn retention_prune(&self, max_age: Duration) -> usize {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(max_age)
                .unwrap_or_else(|_| chrono::Duration::seconds(0));

        let mut entries = self.entries.lock();
        let before = entries.len();

        entries.retain(|e| e.timestamp >= cutoff);

        let pruned = before - entries.len();
        if pruned > 0 {
            debug!(pruned = pruned, "audit log retention prune complete");
        }
        pruned
    }

    /// Spawn a background persistence loop that flushes the write buffer
    /// to disk at regular intervals.
    pub fn spawn_persistence_loop(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let interval = self.flush_interval;
        tokio::spawn(async move {
            info!(
                interval_secs = interval.as_secs(),
                "audit persistence loop started"
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            // Final flush before shutdown.
                            if let Err(e) = self.flush_to_disk() {
                                warn!(error = %e, "final audit flush failed");
                            }
                            info!("audit persistence loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown_rx.borrow() {
                    if let Err(e) = self.flush_to_disk() {
                        warn!(error = %e, "final audit flush failed");
                    }
                    break;
                }

                if let Err(e) = self.flush_to_disk() {
                    warn!(error = %e, "audit flush failed");
                }
            }
        })
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// AuditRecorder
// ============================================================================

/// Convenience wrapper around [`AuditLog`] with a default actor.
///
/// This simplifies recording from contexts where the actor is always the
/// same (e.g. a specific subsystem or node).
pub struct AuditRecorder {
    /// Underlying audit log.
    log: Arc<AuditLog>,
    /// Default actor for all recordings.
    default_actor: AuditActor,
}

impl AuditRecorder {
    /// Create a new recorder with a default actor.
    pub fn new(log: Arc<AuditLog>, default_actor: AuditActor) -> Self {
        Self {
            log,
            default_actor,
        }
    }

    /// Record a successful action.
    pub fn success(
        &self,
        action: String,
        target: AuditTarget,
        details: serde_json::Value,
    ) -> u64 {
        self.log.record_success(
            self.default_actor.clone(),
            action,
            target,
            details,
        )
    }

    /// Record a failed action.
    pub fn failure(
        &self,
        action: String,
        target: AuditTarget,
        reason: String,
        details: serde_json::Value,
    ) -> u64 {
        self.log.record_failure(
            self.default_actor.clone(),
            action,
            target,
            reason,
            details,
        )
    }

    /// Record a denied action.
    pub fn denied(
        &self,
        action: String,
        target: AuditTarget,
        reason: String,
        details: serde_json::Value,
    ) -> u64 {
        self.log.record_denied(
            self.default_actor.clone(),
            action,
            target,
            reason,
            details,
        )
    }

    /// Get a reference to the underlying audit log.
    pub fn log(&self) -> &Arc<AuditLog> {
        &self.log
    }
}

// ============================================================================
// Enhanced AuditEvent (W1A — Adaptive Scrutiny)
// ============================================================================

use std::collections::HashMap;
use super::types::{CriticalityLevel, WitnessRecord};

/// An enriched audit event with criticality classification,
/// cryptographic signature, and witness attestations.
///
/// This is the high-level event type that flows through the audit
/// pipeline. It is condensed into an [`AuditEntry`] for hash-chain
/// storage in the ring buffer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// UUID v7 (time-ordered) unique event identifier.
    pub event_id: String,
    /// Milliseconds since Unix epoch.
    pub timestamp_ms: u64,
    /// Node that originated this event.
    pub node_id: String,
    /// Identity of the actor who performed the action.
    pub actor_id: String,
    /// Dot-separated action type (e.g. "node.role.change").
    pub action_type: String,
    /// Criticality tier, set by ScrutinyConfig::classify().
    pub criticality: CriticalityLevel,
    /// Target of the action (e.g. node ID, job ID, blob hash).
    pub target: String,
    /// Arbitrary structured payload.
    pub payload: serde_json::Value,
    /// Snapshot of the target's state before the action, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_state: Option<serde_json::Value>,
    /// Ed25519 signature by the originating node over canonical fields.
    #[serde(default)]
    pub signature: Vec<u8>,
    /// Witness attestations (populated asynchronously after creation).
    #[serde(default)]
    pub witnesses: Vec<WitnessRecord>,
}

// ============================================================================
// ScrutinyConfig (W1A)
// ============================================================================

/// Maps action-type patterns to criticality levels for adaptive scrutiny.
///
/// Patterns support a trailing `*` wildcard:
///   "node.role.*" matches "node.role.change", "node.role.shed", etc.
/// Exact matches take priority over wildcards. Among wildcards, the
/// longest prefix wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrutinyConfig {
    /// Pattern -> CriticalityLevel mapping.
    pub action_map: HashMap<String, CriticalityLevel>,
    /// Fallback criticality when no pattern matches.
    #[serde(default)]
    pub default_criticality: CriticalityLevel,
}

impl ScrutinyConfig {
    /// Create a new ScrutinyConfig with the given default criticality.
    pub fn new(default_criticality: CriticalityLevel) -> Self {
        Self {
            action_map: HashMap::new(),
            default_criticality,
        }
    }

    /// Classify an action_type string into a CriticalityLevel.
    ///
    /// Resolution order:
    /// 1. Exact match in action_map.
    /// 2. Longest glob prefix match (patterns ending in `*`).
    /// 3. `self.default_criticality` as fallback.
    pub fn classify(&self, action_type: &str) -> CriticalityLevel {
        // Exact match first
        if let Some(level) = self.action_map.get(action_type) {
            return *level;
        }

        // Glob prefix match -- longest prefix wins
        let mut best: Option<(usize, CriticalityLevel)> = None;
        for (pattern, level) in &self.action_map {
            if let Some(prefix) = pattern.strip_suffix('*') {
                if action_type.starts_with(prefix) {
                    let len = prefix.len();
                    if best.map_or(true, |(best_len, _)| len > best_len) {
                        best = Some((len, *level));
                    }
                }
            }
        }

        best.map(|(_, level)| level)
            .unwrap_or(self.default_criticality)
    }
}

impl Default for ScrutinyConfig {
    fn default() -> Self {
        Self {
            action_map: HashMap::new(),
            default_criticality: CriticalityLevel::Normal,
        }
    }
}

// ============================================================================
// AuditEventBuilder (W1A)
// ============================================================================

/// Builder for ergonomic AuditEvent construction.
///
/// # Example
/// ```ignore
/// let event = AuditEventBuilder::new("node.role.change", "node-abc")
///     .actor("operator-1")
///     .target("node-xyz")
///     .payload(serde_json::json!({"new_role": "leader"}))
///     .previous_state(serde_json::json!({"old_role": "follower"}))
///     .build(&scrutiny_config);
/// ```
pub struct AuditEventBuilder {
    action_type: String,
    node_id: String,
    actor_id: Option<String>,
    target: Option<String>,
    payload: serde_json::Value,
    previous_state: Option<serde_json::Value>,
}

impl AuditEventBuilder {
    /// Create a new builder with the required action type and originating node ID.
    pub fn new(action_type: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self {
            action_type: action_type.into(),
            node_id: node_id.into(),
            actor_id: None,
            target: None,
            payload: serde_json::Value::Null,
            previous_state: None,
        }
    }

    /// Set the actor ID (who performed the action).
    pub fn actor(mut self, id: impl Into<String>) -> Self {
        self.actor_id = Some(id.into());
        self
    }

    /// Set the target of the action.
    pub fn target(mut self, t: impl Into<String>) -> Self {
        self.target = Some(t.into());
        self
    }

    /// Set the structured payload.
    pub fn payload(mut self, v: serde_json::Value) -> Self {
        self.payload = v;
        self
    }

    /// Set the previous state snapshot.
    pub fn previous_state(mut self, v: serde_json::Value) -> Self {
        self.previous_state = Some(v);
        self
    }

    /// Build the AuditEvent, auto-classifying criticality via the config.
    ///
    /// Generates UUID v7 for time-ordered event ID, sets timestamp to
    /// current epoch millis, leaves signature zeroed and witnesses empty
    /// (populated later by the witness protocol in Wave 3).
    pub fn build(self, config: &ScrutinyConfig) -> AuditEvent {
        let criticality = config.classify(&self.action_type);
        let now = Utc::now();
        let timestamp_ms = now.timestamp_millis() as u64;
        let event_id = uuid::Uuid::now_v7().to_string();

        AuditEvent {
            event_id,
            timestamp_ms,
            node_id: self.node_id,
            actor_id: self.actor_id.unwrap_or_default(),
            action_type: self.action_type,
            criticality,
            target: self.target.unwrap_or_default(),
            payload: self.payload,
            previous_state: self.previous_state,
            signature: Vec::new(),
            witnesses: Vec::new(),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod scrutiny_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_classify_exact_match() {
        let mut config = ScrutinyConfig::new(CriticalityLevel::Normal);
        config.action_map.insert("node.role.change".to_string(), CriticalityLevel::High);
        config.action_map.insert("key.rotate".to_string(), CriticalityLevel::Critical);

        assert_eq!(config.classify("node.role.change"), CriticalityLevel::High);
        assert_eq!(config.classify("key.rotate"), CriticalityLevel::Critical);
    }

    #[test]
    fn test_classify_glob_match() {
        let mut config = ScrutinyConfig::new(CriticalityLevel::Low);
        config.action_map.insert("node.role.*".to_string(), CriticalityLevel::High);

        assert_eq!(config.classify("node.role.change"), CriticalityLevel::High);
        assert_eq!(config.classify("node.role.shed"), CriticalityLevel::High);
        // Should NOT match a non-prefix
        assert_eq!(config.classify("node.status.change"), CriticalityLevel::Low);
    }

    #[test]
    fn test_classify_longest_prefix_wins() {
        let mut config = ScrutinyConfig::new(CriticalityLevel::Low);
        config.action_map.insert("node.*".to_string(), CriticalityLevel::Normal);
        config.action_map.insert("node.role.*".to_string(), CriticalityLevel::High);

        // "node.role.*" is more specific (longer prefix) than "node.*"
        assert_eq!(config.classify("node.role.change"), CriticalityLevel::High);
        // "node.status.x" only matches "node.*"
        assert_eq!(config.classify("node.status.x"), CriticalityLevel::Normal);
    }

    #[test]
    fn test_classify_default_fallback() {
        let config = ScrutinyConfig::new(CriticalityLevel::Normal);
        assert_eq!(config.classify("unknown.action"), CriticalityLevel::Normal);

        let config2 = ScrutinyConfig::new(CriticalityLevel::Low);
        assert_eq!(config2.classify("completely.unknown"), CriticalityLevel::Low);
    }

    #[test]
    fn test_witness_count_per_tier() {
        use super::super::config::witness_requirements;

        assert_eq!(witness_requirements(&CriticalityLevel::Low), (0, 0));
        assert_eq!(witness_requirements(&CriticalityLevel::Normal), (2, 1));
        assert_eq!(witness_requirements(&CriticalityLevel::High), (5, 2));
        assert_eq!(witness_requirements(&CriticalityLevel::Critical), (7, 3));
        assert_eq!(witness_requirements(&CriticalityLevel::Emergency), (usize::MAX, usize::MAX));
    }

    #[test]
    fn test_criticality_ordering() {
        assert!(CriticalityLevel::Low < CriticalityLevel::Normal);
        assert!(CriticalityLevel::Normal < CriticalityLevel::High);
        assert!(CriticalityLevel::High < CriticalityLevel::Critical);
        assert!(CriticalityLevel::Critical < CriticalityLevel::Emergency);

        // std::cmp::max should pick the highest
        assert_eq!(
            std::cmp::max(CriticalityLevel::Low, CriticalityLevel::Emergency),
            CriticalityLevel::Emergency
        );
        assert_eq!(
            std::cmp::max(CriticalityLevel::High, CriticalityLevel::Normal),
            CriticalityLevel::High
        );
    }

    #[test]
    fn test_audit_event_builder() {
        let mut config = ScrutinyConfig::new(CriticalityLevel::Normal);
        config.action_map.insert("node.role.*".to_string(), CriticalityLevel::High);

        let event = AuditEventBuilder::new("node.role.change", "node-abc")
            .actor("operator-1")
            .target("node-xyz")
            .payload(json!({"new_role": "leader"}))
            .previous_state(json!({"old_role": "follower"}))
            .build(&config);

        assert_eq!(event.action_type, "node.role.change");
        assert_eq!(event.node_id, "node-abc");
        assert_eq!(event.actor_id, "operator-1");
        assert_eq!(event.target, "node-xyz");
        assert_eq!(event.criticality, CriticalityLevel::High);
        assert_eq!(event.payload, json!({"new_role": "leader"}));
        assert_eq!(event.previous_state, Some(json!({"old_role": "follower"})));
        assert!(event.signature.is_empty());
        assert!(event.witnesses.is_empty());
        assert!(!event.event_id.is_empty());
        assert!(event.timestamp_ms > 0);
    }

    #[test]
    fn test_witness_record_serde() {
        let record = WitnessRecord {
            node_id: "witness-node-1".to_string(),
            timestamp_ms: 1700000000000,
            signature: vec![1, 2, 3, 4, 5],
            verdict: Verdict::Confirmed,
        };

        // Round-trip through JSON
        let json_str = serde_json::to_string(&record).expect("serialize WitnessRecord");
        let deserialized: WitnessRecord =
            serde_json::from_str(&json_str).expect("deserialize WitnessRecord");

        assert_eq!(deserialized.node_id, "witness-node-1");
        assert_eq!(deserialized.timestamp_ms, 1700000000000);
        assert_eq!(deserialized.signature, vec![1, 2, 3, 4, 5]);
        assert_eq!(deserialized.verdict, Verdict::Confirmed);

        // Also test other verdict variants
        let rejected = WitnessRecord {
            node_id: "w2".to_string(),
            timestamp_ms: 1700000001000,
            signature: vec![],
            verdict: Verdict::Rejected,
        };
        let json_rejected = serde_json::to_string(&rejected).expect("serialize");
        let de_rejected: WitnessRecord =
            serde_json::from_str(&json_rejected).expect("deserialize");
        assert_eq!(de_rejected.verdict, Verdict::Rejected);

        let timeout = WitnessRecord {
            node_id: "w3".to_string(),
            timestamp_ms: 1700000002000,
            signature: vec![],
            verdict: Verdict::Timeout,
        };
        let json_timeout = serde_json::to_string(&timeout).expect("serialize");
        let de_timeout: WitnessRecord =
            serde_json::from_str(&json_timeout).expect("deserialize");
        assert_eq!(de_timeout.verdict, Verdict::Timeout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_actor(name: &str) -> AuditActor {
        AuditActor {
            actor_type: "node".to_string(),
            id: name.to_string(),
            display_name: Some(format!("Node {}", name)),
        }
    }

    fn make_target(name: &str) -> AuditTarget {
        AuditTarget {
            target_type: "job".to_string(),
            id: name.to_string(),
            display_name: None,
        }
    }

    // ---------------------------------------------------------------
    // Basic recording tests
    // ---------------------------------------------------------------

    #[test]
    fn test_record_and_get() {
        let log = AuditLog::new();
        let id = log.record(
            make_actor("node-1"),
            "job.submit".to_string(),
            make_target("job-1"),
            AuditOutcome::Success,
            json!({"chunks": 5}),
        );

        let entry = log.get(id).expect("should find entry");
        assert_eq!(entry.id, id);
        assert_eq!(entry.action, "job.submit");
        assert!(entry.outcome.is_success());
    }

    #[test]
    fn test_record_success_convenience() {
        let log = AuditLog::new();
        let id = log.record_success(
            make_actor("node-1"),
            "job.complete".to_string(),
            make_target("job-1"),
            json!({}),
        );
        let entry = log.get(id).expect("should find entry");
        assert!(entry.outcome.is_success());
    }

    #[test]
    fn test_record_failure_convenience() {
        let log = AuditLog::new();
        let id = log.record_failure(
            make_actor("node-1"),
            "job.execute".to_string(),
            make_target("job-1"),
            "timeout".to_string(),
            json!({}),
        );
        let entry = log.get(id).expect("should find entry");
        assert!(entry.outcome.is_failure());
    }

    #[test]
    fn test_record_denied_convenience() {
        let log = AuditLog::new();
        let id = log.record_denied(
            make_actor("node-1"),
            "blob.access".to_string(),
            make_target("blob-1"),
            "unauthorized".to_string(),
            json!({}),
        );
        let entry = log.get(id).expect("should find entry");
        assert!(entry.outcome.is_denied());
    }

    #[test]
    fn test_monotonic_ids() {
        let log = AuditLog::new();
        let id1 = log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        let id2 = log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        let id3 = log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        assert!(id2 > id1);
        assert!(id3 > id2);
    }

    // ---------------------------------------------------------------
    // Hash chain integrity tests
    // ---------------------------------------------------------------

    #[test]
    fn test_verify_chain_empty() {
        let log = AuditLog::new();
        let v = log.verify_chain();
        assert!(v.valid);
        assert_eq!(v.total_entries, 0);
    }

    #[test]
    fn test_verify_chain_valid() {
        let log = AuditLog::new();
        for i in 0..10 {
            log.record_success(
                make_actor("node-1"),
                format!("action.{}", i),
                make_target("target-1"),
                json!({"i": i}),
            );
        }

        let v = log.verify_chain();
        assert!(v.valid);
        assert_eq!(v.total_entries, 10);
        assert_eq!(v.valid_entries, 10);
        assert_eq!(v.invalid_entries, 0);
        assert!(v.first_invalid_id.is_none());
    }

    #[test]
    fn test_verify_chain_detects_tampering() {
        let log = AuditLog::new();
        for i in 0..5 {
            log.record_success(
                make_actor("node-1"),
                format!("action.{}", i),
                make_target("target-1"),
                json!({}),
            );
        }

        // Tamper with the third entry.
        {
            let mut entries = log.entries.lock();
            if let Some(entry) = entries.get_mut(2) {
                entry.action = "tampered_action".to_string();
            }
        }

        let v = log.verify_chain();
        assert!(!v.valid);
        assert!(v.invalid_entries > 0);
        assert!(v.first_invalid_id.is_some());
    }

    #[test]
    fn test_entry_verify_standalone() {
        let log = AuditLog::new();
        let id = log.record_success(
            make_actor("a"),
            "test".into(),
            make_target("t"),
            json!({}),
        );
        let entry = log.get(id).expect("entry");
        assert!(entry.verify());
    }

    #[test]
    fn test_chain_linkage() {
        let log = AuditLog::new();
        log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        log.record_success(make_actor("a"), "y".into(), make_target("t"), json!({}));

        let entries = log.entries.lock();
        assert_eq!(entries[0].prev_hash, GENESIS_HASH);
        assert_eq!(entries[1].prev_hash, entries[0].hash);
    }

    // ---------------------------------------------------------------
    // Query and filter tests
    // ---------------------------------------------------------------

    #[test]
    fn test_query_by_action() {
        let log = AuditLog::new();
        log.record_success(make_actor("a"), "job.submit".into(), make_target("t1"), json!({}));
        log.record_success(make_actor("a"), "job.complete".into(), make_target("t1"), json!({}));
        log.record_success(make_actor("a"), "job.submit".into(), make_target("t2"), json!({}));

        let filter = AuditFilter {
            action: Some("job.submit".to_string()),
            ..Default::default()
        };

        let results = log.query(&filter);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_by_actor_type() {
        let log = AuditLog::new();
        log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        log.record_success(
            AuditActor { actor_type: "user".into(), id: "u1".into(), display_name: None },
            "x".into(),
            make_target("t"),
            json!({}),
        );

        let filter = AuditFilter {
            actor_type: Some("user".to_string()),
            ..Default::default()
        };

        assert_eq!(log.query(&filter).len(), 1);
    }

    #[test]
    fn test_query_by_outcome() {
        let log = AuditLog::new();
        log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        log.record_failure(make_actor("a"), "y".into(), make_target("t"), "err".into(), json!({}));
        log.record_denied(make_actor("a"), "z".into(), make_target("t"), "no".into(), json!({}));

        let filter = AuditFilter {
            outcome: Some("failure".to_string()),
            ..Default::default()
        };
        assert_eq!(log.query(&filter).len(), 1);

        let filter = AuditFilter {
            outcome: Some("denied".to_string()),
            ..Default::default()
        };
        assert_eq!(log.query(&filter).len(), 1);
    }

    #[test]
    fn test_query_by_time_range() {
        let log = AuditLog::new();
        let before = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        std::thread::sleep(std::time::Duration::from_millis(10));
        let after = Utc::now();

        let filter = AuditFilter {
            since: Some(before),
            until: Some(after),
            ..Default::default()
        };
        assert_eq!(log.query(&filter).len(), 1);

        // Before the recording.
        let filter = AuditFilter {
            until: Some(before),
            ..Default::default()
        };
        assert_eq!(log.query(&filter).len(), 0);
    }

    #[test]
    fn test_query_pagination() {
        let log = AuditLog::new();
        for i in 0..20 {
            log.record_success(
                make_actor("a"),
                format!("action.{}", i),
                make_target("t"),
                json!({}),
            );
        }

        // Page 1: first 5.
        let filter = AuditFilter {
            limit: Some(5),
            offset: Some(0),
            ..Default::default()
        };
        let page1 = log.query(&filter);
        assert_eq!(page1.len(), 5);

        // Page 2: next 5.
        let filter = AuditFilter {
            limit: Some(5),
            offset: Some(5),
            ..Default::default()
        };
        let page2 = log.query(&filter);
        assert_eq!(page2.len(), 5);
        assert_ne!(page1[0].id, page2[0].id);
    }

    #[test]
    fn test_query_by_target_id() {
        let log = AuditLog::new();
        log.record_success(make_actor("a"), "x".into(), make_target("job-1"), json!({}));
        log.record_success(make_actor("a"), "x".into(), make_target("job-2"), json!({}));

        let filter = AuditFilter {
            target_id: Some("job-1".to_string()),
            ..Default::default()
        };
        assert_eq!(log.query(&filter).len(), 1);
    }

    #[test]
    fn test_count() {
        let log = AuditLog::new();
        for _ in 0..15 {
            log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        }
        log.record_failure(make_actor("a"), "x".into(), make_target("t"), "err".into(), json!({}));

        assert_eq!(log.count(&AuditFilter::default()), 16);
        assert_eq!(
            log.count(&AuditFilter {
                outcome: Some("success".to_string()),
                ..Default::default()
            }),
            15
        );
    }

    // ---------------------------------------------------------------
    // Recent entries test
    // ---------------------------------------------------------------

    #[test]
    fn test_recent() {
        let log = AuditLog::new();
        for i in 0..10 {
            log.record_success(
                make_actor("a"),
                format!("action.{}", i),
                make_target("t"),
                json!({}),
            );
        }

        let recent = log.recent(3);
        assert_eq!(recent.len(), 3);
        // Should be the last 3 actions.
        assert_eq!(recent[0].action, "action.7");
        assert_eq!(recent[1].action, "action.8");
        assert_eq!(recent[2].action, "action.9");
    }

    // ---------------------------------------------------------------
    // Ring buffer eviction test
    // ---------------------------------------------------------------

    #[test]
    fn test_ring_buffer_eviction() {
        let log = AuditLog::new().with_max_entries(10);
        for i in 0..20 {
            log.record_success(
                make_actor("a"),
                format!("action.{}", i),
                make_target("t"),
                json!({}),
            );
        }

        assert_eq!(log.len(), 10);
        // The oldest entries should have been evicted.
        let entries = log.entries.lock();
        assert_eq!(entries.front().map(|e| e.action.as_str()), Some("action.10"));
    }

    // ---------------------------------------------------------------
    // Export tests
    // ---------------------------------------------------------------

    #[test]
    fn test_export_json() {
        let log = AuditLog::new();
        log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        log.record_success(make_actor("b"), "y".into(), make_target("t"), json!({}));

        let json_str = log.export_json().expect("should serialize");
        let parsed: Vec<serde_json::Value> =
            serde_json::from_str(&json_str).expect("should parse");
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_export_csv() {
        let log = AuditLog::new();
        log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));

        let csv = log.export_csv();
        assert!(csv.starts_with("id,timestamp"));
        assert_eq!(csv.lines().count(), 2); // Header + 1 row
    }

    #[test]
    fn test_export_ndjson() {
        let log = AuditLog::new();
        log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
        log.record_success(make_actor("b"), "y".into(), make_target("t"), json!({}));

        let ndjson = log.export_ndjson();
        let lines: Vec<&str> = ndjson.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        // Each line should parse as JSON.
        for line in &lines {
            serde_json::from_str::<AuditEntry>(line).expect("each line should parse");
        }
    }

    // ---------------------------------------------------------------
    // Persistence roundtrip tests
    // ---------------------------------------------------------------

    #[test]
    fn test_persistence_roundtrip() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("audit.ndjson");

        // Write some entries.
        {
            let log = AuditLog::with_persistence(path.clone());
            for i in 0..5 {
                log.record_success(
                    make_actor("a"),
                    format!("action.{}", i),
                    make_target("t"),
                    json!({"i": i}),
                );
            }
            log.flush_to_disk().expect("flush");
        }

        // Load them back.
        {
            let log = AuditLog::with_persistence(path.clone());
            let loaded = log.load_from_disk().expect("load");
            assert_eq!(loaded, 5);
            assert_eq!(log.len(), 5);

            // Chain should still be valid.
            let v = log.verify_chain();
            assert!(v.valid);
        }
    }

    #[test]
    fn test_load_from_disk_skips_corrupt_lines() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("audit_corrupt.ndjson");

        // Write entries with a corrupt line in the middle.
        {
            let log = AuditLog::with_persistence(path.clone());
            log.record_success(make_actor("a"), "x".into(), make_target("t"), json!({}));
            log.flush_to_disk().expect("flush");
        }

        // Manually append a corrupt line.
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .expect("open");
            writeln!(file, "{{not valid json]]").expect("write corrupt line");
        }

        // Append another valid entry.
        {
            let log = AuditLog::with_persistence(path.clone());
            log.record_success(make_actor("b"), "y".into(), make_target("t2"), json!({}));
            log.flush_to_disk().expect("flush");
        }

        // Load should get 2 valid entries (skipping the corrupt line).
        {
            let log = AuditLog::with_persistence(path.clone());
            let loaded = log.load_from_disk().expect("load");
            assert_eq!(loaded, 2);
        }
    }

    #[test]
    fn test_flush_no_persistence() {
        let log = AuditLog::new();
        assert!(log.flush_to_disk().is_ok()); // No-op
    }

    #[test]
    fn test_load_no_persistence() {
        let log = AuditLog::new();
        assert_eq!(log.load_from_disk().expect("should succeed"), 0);
    }

    // ---------------------------------------------------------------
    // Retention prune test
    // ---------------------------------------------------------------

    #[test]
    fn test_retention_prune() {
        let log = AuditLog::new();

        // Record entries with timestamps in the past.
        log.record_success(make_actor("a"), "old".into(), make_target("t"), json!({}));
        // Manually set the timestamp far in the past.
        {
            let mut entries = log.entries.lock();
            if let Some(entry) = entries.back_mut() {
                entry.timestamp = Utc::now() - chrono::Duration::hours(48);
            }
        }

        // Record a recent entry.
        log.record_success(make_actor("b"), "new".into(), make_target("t"), json!({}));

        let pruned = log.retention_prune(Duration::from_secs(3600)); // 1 hour
        assert_eq!(pruned, 1);
        assert_eq!(log.len(), 1);
    }

    // ---------------------------------------------------------------
    // Concurrent recording test
    // ---------------------------------------------------------------

    #[test]
    fn test_concurrent_recording() {
        let log = Arc::new(AuditLog::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let log = Arc::clone(&log);
            handles.push(std::thread::spawn(move || {
                for j in 0..100 {
                    log.record_success(
                        make_actor(&format!("node-{}", i)),
                        format!("action.{}.{}", i, j),
                        make_target("target"),
                        json!({"thread": i, "iteration": j}),
                    );
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread should complete");
        }

        assert_eq!(log.len(), 1000);

        // All IDs should be unique.
        let entries = log.entries.lock();
        let mut ids: Vec<u64> = entries.iter().map(|e| e.id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 1000);
    }

    // ---------------------------------------------------------------
    // AuditRecorder tests
    // ---------------------------------------------------------------

    #[test]
    fn test_audit_recorder_success() {
        let log = Arc::new(AuditLog::new());
        let recorder = AuditRecorder::new(
            Arc::clone(&log),
            make_actor("system"),
        );

        let id = recorder.success(
            "health.check".into(),
            make_target("node-1"),
            json!({"status": "ok"}),
        );

        let entry = log.get(id).expect("entry");
        assert_eq!(entry.actor.id, "system");
        assert!(entry.outcome.is_success());
    }

    #[test]
    fn test_audit_recorder_failure() {
        let log = Arc::new(AuditLog::new());
        let recorder = AuditRecorder::new(Arc::clone(&log), make_actor("system"));

        let id = recorder.failure(
            "job.execute".into(),
            make_target("job-1"),
            "oom".into(),
            json!({}),
        );

        let entry = log.get(id).expect("entry");
        assert!(entry.outcome.is_failure());
    }

    #[test]
    fn test_audit_recorder_denied() {
        let log = Arc::new(AuditLog::new());
        let recorder = AuditRecorder::new(Arc::clone(&log), make_actor("gateway"));

        let id = recorder.denied(
            "api.access".into(),
            make_target("endpoint"),
            "invalid token".into(),
            json!({}),
        );

        let entry = log.get(id).expect("entry");
        assert!(entry.outcome.is_denied());
    }

    #[test]
    fn test_audit_recorder_log_ref() {
        let log = Arc::new(AuditLog::new());
        let recorder = AuditRecorder::new(Arc::clone(&log), make_actor("x"));
        assert!(recorder.log().is_empty());
        recorder.success("x".into(), make_target("t"), json!({}));
        assert!(!recorder.log().is_empty());
    }

    // ---------------------------------------------------------------
    // Display tests
    // ---------------------------------------------------------------

    #[test]
    fn test_actor_display() {
        let actor = AuditActor {
            actor_type: "node".into(),
            id: "n1".into(),
            display_name: Some("Node 1".into()),
        };
        assert_eq!(format!("{}", actor), "node:n1 (Node 1)");

        let actor_no_name = AuditActor {
            actor_type: "node".into(),
            id: "n1".into(),
            display_name: None,
        };
        assert_eq!(format!("{}", actor_no_name), "node:n1");
    }

    #[test]
    fn test_target_display() {
        let target = AuditTarget {
            target_type: "job".into(),
            id: "j1".into(),
            display_name: None,
        };
        assert_eq!(format!("{}", target), "job:j1");
    }

    #[test]
    fn test_outcome_display() {
        assert_eq!(format!("{}", AuditOutcome::Success), "success");
        assert_eq!(
            format!("{}", AuditOutcome::Failure { reason: "err".into() }),
            "failure(err)"
        );
        assert_eq!(
            format!("{}", AuditOutcome::Denied { reason: "no".into() }),
            "denied(no)"
        );
    }

    // ---------------------------------------------------------------
    // Empty and default tests
    // ---------------------------------------------------------------

    #[test]
    fn test_empty_log() {
        let log = AuditLog::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert!(log.get(1).is_none());
        assert!(log.recent(10).is_empty());
    }

    #[test]
    fn test_default_log() {
        let log = AuditLog::default();
        assert!(log.is_empty());
    }

    // ---------------------------------------------------------------
    // Complex filter combination
    // ---------------------------------------------------------------

    #[test]
    fn test_complex_filter() {
        let log = AuditLog::new();
        log.record_success(make_actor("admin"), "node.drain".into(), make_target("n1"), json!({}));
        log.record_failure(
            make_actor("admin"),
            "node.drain".into(),
            make_target("n2"),
            "offline".into(),
            json!({}),
        );
        log.record_success(make_actor("user"), "job.submit".into(), make_target("j1"), json!({}));

        let filter = AuditFilter {
            actor_id: Some("admin".to_string()),
            action: Some("node.drain".to_string()),
            outcome: Some("success".to_string()),
            ..Default::default()
        };

        let results = log.query(&filter);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].target.id, "n1");
    }

    // ---------------------------------------------------------------
    // Verify chain after eviction
    // ---------------------------------------------------------------

    #[test]
    fn test_chain_after_eviction_still_internally_consistent() {
        let log = AuditLog::new().with_max_entries(5);
        for i in 0..10 {
            log.record_success(
                make_actor("a"),
                format!("action.{}", i),
                make_target("t"),
                json!({}),
            );
        }

        assert_eq!(log.len(), 5);

        // The remaining 5 entries should form a valid chain among themselves.
        // However, the first remaining entry's prev_hash won't match genesis
        // because earlier entries were evicted. verify_chain checks linkage
        // starting from genesis, so the first entry after eviction will fail.
        // This is expected behavior for a ring buffer audit log.
        let v = log.verify_chain();
        // First entry's prev_hash doesn't match genesis.
        assert!(!v.valid);
        // But the remaining entries should be internally linked.
        // At least some should be valid (the ones that chain from each other).
        assert!(v.valid_entries >= 4);
    }
}
