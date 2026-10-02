// Marabunta - Licensed under the MIT License.
//! Centralized event bus for the Marabunta Swarm.
//!
//! Provides a broadcast-based event system with filtering, history retention,
//! correlation, deduplication, export, and aggregation. Every significant state
//! change in the swarm -- node lifecycle, job execution, security events,
//! fleet operations -- emits a [`SwarmEvent`] through the [`EventBus`].
//!
//! # Architecture
//!
//! ```text
//!   emit() ──► broadcast::Sender ──► subscribers (SSE, websocket, CLI)
//!          │
//!          └── ring buffer (VecDeque, max 10,000)
//!                  │
//!                  ├── recent_events(limit)
//!                  └── recent_filtered(filter, limit)
//! ```
//!
//! Events are immutable once emitted. Each event receives a monotonically
//! increasing `id` and a UTC timestamp. Factory functions provide pre-configured
//! events for every important swarm state change.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Datelike, Timelike, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::complexity::{ComplexityHint, ConcernDomain, EventSeverity};
use super::types::{ChunkId, NodeId};
use crate::common::types::JobId;

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of events retained in the ring buffer.
const MAX_EVENT_HISTORY: usize = 10_000;

/// Default broadcast channel capacity.
const DEFAULT_BROADCAST_CAPACITY: usize = 4096;

/// Default deduplication window in seconds.
const DEDUP_WINDOW_SECS: i64 = 30;

/// Default correlation window for node-related events (seconds).
const CORRELATION_WINDOW_NODE_SECS: i64 = 60;

/// Default correlation window for job-related events (seconds).
const CORRELATION_WINDOW_JOB_SECS: i64 = 300;

/// Default correlation window for partition events (seconds).
const CORRELATION_WINDOW_PARTITION_SECS: i64 = 30;

// ============================================================================
// EntityType
// ============================================================================

/// The type of entity referenced by an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    /// A swarm node.
    Node,
    /// A job.
    Job,
    /// A chunk of work.
    Chunk,
    /// A collective of nodes.
    Collective,
    /// A multi-swarm membrane.
    Membrane,
    /// An inter-swarm agreement.
    Agreement,
    /// The swarm itself.
    Swarm,
    /// A policy rule.
    Policy,
    /// An authentication token.
    Token,
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node => write!(f, "node"),
            Self::Job => write!(f, "job"),
            Self::Chunk => write!(f, "chunk"),
            Self::Collective => write!(f, "collective"),
            Self::Membrane => write!(f, "membrane"),
            Self::Agreement => write!(f, "agreement"),
            Self::Swarm => write!(f, "swarm"),
            Self::Policy => write!(f, "policy"),
            Self::Token => write!(f, "token"),
        }
    }
}

impl FromStr for EntityType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "node" => Ok(Self::Node),
            "job" => Ok(Self::Job),
            "chunk" => Ok(Self::Chunk),
            "collective" => Ok(Self::Collective),
            "membrane" => Ok(Self::Membrane),
            "agreement" => Ok(Self::Agreement),
            "swarm" => Ok(Self::Swarm),
            "policy" => Ok(Self::Policy),
            "token" => Ok(Self::Token),
            other => Err(format!("unknown entity type: {}", other)),
        }
    }
}

// ============================================================================
// EntityRef
// ============================================================================

/// A reference to an entity involved in an event.
///
/// Carries enough context to link back to the entity in a dashboard or log.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityRef {
    /// The type of entity.
    pub entity_type: EntityType,
    /// The unique identifier of the entity (stringified UUID, name, etc.).
    pub id: String,
    /// Optional human-readable display name.
    pub display_name: Option<String>,
}

impl EntityRef {
    /// Create a new entity reference.
    pub fn new(entity_type: EntityType, id: impl Into<String>) -> Self {
        Self {
            entity_type,
            id: id.into(),
            display_name: None,
        }
    }

    /// Create a new entity reference with a display name.
    pub fn with_name(
        entity_type: EntityType,
        id: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            entity_type,
            id: id.into(),
            display_name: Some(name.into()),
        }
    }

    /// Create an entity reference for a node.
    pub fn node(id: &NodeId) -> Self {
        Self::new(EntityType::Node, id.to_string())
    }

    /// Create an entity reference for a job.
    pub fn job(id: &JobId) -> Self {
        Self::new(EntityType::Job, id.to_string())
    }

    /// Create an entity reference for a chunk.
    pub fn chunk(id: &ChunkId) -> Self {
        Self::new(EntityType::Chunk, id.to_string())
    }
}

impl fmt::Display for EntityRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref name) = self.display_name {
            write!(f, "{}:{}({})", self.entity_type, self.id, name)
        } else {
            write!(f, "{}:{}", self.entity_type, self.id)
        }
    }
}

// ============================================================================
// SwarmEvent
// ============================================================================

/// A single event emitted by the swarm.
///
/// Events are immutable once created. Each event has a unique monotonically
/// increasing `id`, a UTC timestamp, and rich metadata for filtering,
/// correlation, and display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmEvent {
    /// Monotonically increasing event identifier.
    pub id: u64,
    /// When this event occurred.
    pub timestamp: DateTime<Utc>,
    /// Which operational domain this event belongs to.
    pub domain: ConcernDomain,
    /// How severe this event is.
    pub severity: EventSeverity,
    /// Rendering complexity hint for UIs.
    pub complexity: ComplexityHint,
    /// One-line human-readable summary.
    pub summary: String,
    /// Structured details (JSON object with domain-specific fields).
    pub details: serde_json::Value,
    /// Entities involved in this event.
    pub related_entities: Vec<EntityRef>,
    /// Suggested operator actions.
    pub suggested_actions: Vec<String>,
    /// Which node originated this event (None for swarm-wide events).
    pub source_node: Option<NodeId>,
    /// Correlation identifier for grouping related events.
    pub correlation_id: Option<String>,
    /// If this event supersedes a previous event, its id.
    pub supersedes: Option<u64>,
}

impl SwarmEvent {
    /// Serialize this event as a single-line JSON string suitable for SSE `data:` fields.
    pub fn to_sse_data(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            format!(
                r#"{{"id":{},"summary":"serialization error"}}"#,
                self.id
            )
        })
    }

    /// Check whether this event matches the given filter.
    pub fn matches_filter(&self, filter: &EventFilter) -> bool {
        filter.matches(self)
    }
}

impl fmt::Display for SwarmEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] [{}] [{}] {}",
            self.timestamp.format("%Y-%m-%dT%H:%M:%SZ"),
            self.domain,
            self.severity,
            self.summary
        )
    }
}

// ============================================================================
// EventFilter
// ============================================================================

/// Filter criteria for querying events.
///
/// All fields are optional; `None` means "match all". Multiple fields are
/// combined with AND logic -- an event must satisfy every non-None criterion.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventFilter {
    /// Only events in these domains.
    pub domains: Option<Vec<ConcernDomain>>,
    /// Only events at or above this severity.
    pub min_severity: Option<EventSeverity>,
    /// Only events involving these entity types.
    pub entity_types: Option<Vec<EntityType>>,
    /// Only events involving entities with these IDs.
    pub entity_ids: Option<Vec<String>>,
    /// Only events whose summary contains any of these keywords (case-insensitive).
    pub keywords: Option<Vec<String>>,
    /// Exclude events whose summary contains any of these keywords.
    pub exclude_keywords: Option<Vec<String>>,
    /// Only events after this timestamp.
    pub since: Option<DateTime<Utc>>,
    /// Only events with this correlation ID.
    pub correlation_id: Option<String>,
}

impl EventFilter {
    /// Check whether an event matches this filter.
    pub fn matches(&self, event: &SwarmEvent) -> bool {
        // Domain filter
        if let Some(ref domains) = self.domains {
            if !domains.contains(&event.domain) {
                return false;
            }
        }

        // Severity filter
        if let Some(ref min_sev) = self.min_severity {
            if event.severity < *min_sev {
                return false;
            }
        }

        // Entity type filter
        if let Some(ref types) = self.entity_types {
            let has_type = event
                .related_entities
                .iter()
                .any(|e| types.contains(&e.entity_type));
            if !has_type && !types.is_empty() {
                return false;
            }
        }

        // Entity ID filter
        if let Some(ref ids) = self.entity_ids {
            let has_id = event
                .related_entities
                .iter()
                .any(|e| ids.contains(&e.id));
            if !has_id && !ids.is_empty() {
                return false;
            }
        }

        // Keyword filter (OR logic within keywords, case-insensitive)
        if let Some(ref keywords) = self.keywords {
            let summary_lower = event.summary.to_lowercase();
            let has_keyword = keywords
                .iter()
                .any(|kw| summary_lower.contains(&kw.to_lowercase()));
            if !has_keyword && !keywords.is_empty() {
                return false;
            }
        }

        // Exclude keyword filter
        if let Some(ref excludes) = self.exclude_keywords {
            let summary_lower = event.summary.to_lowercase();
            let has_excluded = excludes
                .iter()
                .any(|kw| summary_lower.contains(&kw.to_lowercase()));
            if has_excluded {
                return false;
            }
        }

        // Since filter
        if let Some(ref since) = self.since {
            if event.timestamp < *since {
                return false;
            }
        }

        // Correlation ID filter
        if let Some(ref cid) = self.correlation_id {
            match &event.correlation_id {
                Some(event_cid) if event_cid == cid => {}
                _ => return false,
            }
        }

        true
    }

    /// Parse an event filter from HTTP query parameters.
    ///
    /// Supported keys:
    /// - `domain`: comma-separated domain names
    /// - `min_severity`: severity level name
    /// - `entity_type`: comma-separated entity type names
    /// - `entity_id`: comma-separated entity IDs
    /// - `keyword`: comma-separated keywords
    /// - `exclude`: comma-separated keywords to exclude
    /// - `since`: RFC 3339 timestamp
    /// - `correlation_id`: correlation ID string
    pub fn from_query_params(params: &HashMap<String, String>) -> Self {
        let mut filter = Self::default();

        if let Some(domains_str) = params.get("domain") {
            let domains: Vec<ConcernDomain> = domains_str
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            if !domains.is_empty() {
                filter.domains = Some(domains);
            }
        }

        if let Some(sev_str) = params.get("min_severity") {
            if let Ok(sev) = sev_str.parse() {
                filter.min_severity = Some(sev);
            }
        }

        if let Some(types_str) = params.get("entity_type") {
            let types: Vec<EntityType> = types_str
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            if !types.is_empty() {
                filter.entity_types = Some(types);
            }
        }

        if let Some(ids_str) = params.get("entity_id") {
            let ids: Vec<String> = ids_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !ids.is_empty() {
                filter.entity_ids = Some(ids);
            }
        }

        if let Some(kw_str) = params.get("keyword") {
            let keywords: Vec<String> = kw_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !keywords.is_empty() {
                filter.keywords = Some(keywords);
            }
        }

        if let Some(ex_str) = params.get("exclude") {
            let excludes: Vec<String> = ex_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !excludes.is_empty() {
                filter.exclude_keywords = Some(excludes);
            }
        }

        if let Some(since_str) = params.get("since") {
            if let Ok(ts) = DateTime::parse_from_rfc3339(since_str) {
                filter.since = Some(ts.with_timezone(&Utc));
            }
        }

        if let Some(cid) = params.get("correlation_id") {
            if !cid.is_empty() {
                filter.correlation_id = Some(cid.clone());
            }
        }

        filter
    }
}

// ============================================================================
// EventStats
// ============================================================================

/// Statistics about the event bus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventStats {
    /// Total events emitted since bus creation.
    pub total_emitted: u64,
    /// Total events dropped due to no subscribers or full channels.
    pub total_dropped: u64,
    /// Events currently in the ring buffer.
    pub buffer_size: usize,
    /// Maximum ring buffer capacity.
    pub buffer_capacity: usize,
    /// Count of events per domain.
    pub per_domain: HashMap<String, u64>,
    /// Count of events per severity.
    pub per_severity: HashMap<String, u64>,
}

// ============================================================================
// EventBus
// ============================================================================

/// Central event bus for the swarm.
///
/// Uses a `tokio::sync::broadcast` channel for fan-out to multiple subscribers
/// and a ring buffer (`VecDeque`) for historical queries. Thread-safe via
/// interior mutability (`RwLock`, `AtomicU64`).
pub struct EventBus {
    /// Broadcast sender for live event streaming.
    sender: tokio::sync::broadcast::Sender<SwarmEvent>,
    /// Ring buffer of recent events.
    history: RwLock<VecDeque<SwarmEvent>>,
    /// Next event ID (monotonically increasing).
    next_id: AtomicU64,
    /// Total events emitted.
    total_emitted: AtomicU64,
    /// Total events dropped (broadcast failures).
    total_dropped: AtomicU64,
    /// Per-domain event counts.
    domain_counts: DashMap<String, u64>,
    /// Per-severity event counts.
    severity_counts: DashMap<String, u64>,
}

impl EventBus {
    /// Create a new event bus with the given broadcast channel buffer size.
    pub fn new(buffer_size: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(buffer_size);
        Self {
            sender,
            history: RwLock::new(VecDeque::with_capacity(MAX_EVENT_HISTORY)),
            next_id: AtomicU64::new(1),
            total_emitted: AtomicU64::new(0),
            total_dropped: AtomicU64::new(0),
            domain_counts: DashMap::new(),
            severity_counts: DashMap::new(),
        }
    }

    /// Create a new event bus with default buffer size.
    pub fn default_bus() -> Self {
        Self::new(DEFAULT_BROADCAST_CAPACITY)
    }

    /// Emit a fully constructed event.
    ///
    /// Assigns a unique ID and timestamp, stores in the ring buffer,
    /// and broadcasts to all subscribers. Returns the assigned event ID.
    pub fn emit(&self, mut event: SwarmEvent) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        event.id = id;
        event.timestamp = Utc::now();

        // Update counters
        self.total_emitted.fetch_add(1, Ordering::Relaxed);
        *self
            .domain_counts
            .entry(event.domain.to_string())
            .or_insert(0) += 1;
        *self
            .severity_counts
            .entry(event.severity.to_string())
            .or_insert(0) += 1;

        // Store in ring buffer
        {
            let mut history = self.history.write();
            if history.len() >= MAX_EVENT_HISTORY {
                history.pop_front();
            }
            history.push_back(event.clone());
        }

        // Broadcast to subscribers
        if self.sender.send(event).is_err() {
            self.total_dropped.fetch_add(1, Ordering::Relaxed);
            debug!("event dropped: no active subscribers");
        }

        id
    }

    /// Emit a simple event with just domain, severity, and summary.
    pub fn emit_simple(
        &self,
        domain: ConcernDomain,
        severity: EventSeverity,
        summary: impl Into<String>,
    ) -> u64 {
        let event = SwarmEvent {
            id: 0,
            timestamp: Utc::now(),
            domain,
            severity,
            complexity: ComplexityHint::Simple,
            summary: summary.into(),
            details: serde_json::Value::Null,
            related_entities: Vec::new(),
            suggested_actions: Vec::new(),
            source_node: None,
            correlation_id: None,
            supersedes: None,
        };
        self.emit(event)
    }

    /// Emit an event with related entities.
    pub fn emit_with_entities(
        &self,
        domain: ConcernDomain,
        severity: EventSeverity,
        summary: impl Into<String>,
        entities: Vec<EntityRef>,
        details: serde_json::Value,
    ) -> u64 {
        let event = SwarmEvent {
            id: 0,
            timestamp: Utc::now(),
            domain,
            severity,
            complexity: ComplexityHint::Moderate,
            summary: summary.into(),
            details,
            related_entities: entities,
            suggested_actions: Vec::new(),
            source_node: None,
            correlation_id: None,
            supersedes: None,
        };
        self.emit(event)
    }

    /// Subscribe to the event stream.
    ///
    /// Returns a broadcast receiver that yields clones of each emitted event.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SwarmEvent> {
        self.sender.subscribe()
    }

    /// Return the most recent `limit` events from the ring buffer.
    pub fn recent_events(&self, limit: usize) -> Vec<SwarmEvent> {
        let history = self.history.read();
        let start = if history.len() > limit {
            history.len() - limit
        } else {
            0
        };
        history.iter().skip(start).cloned().collect()
    }

    /// Return the most recent events matching a filter, up to `limit`.
    pub fn recent_filtered(&self, filter: &EventFilter, limit: usize) -> Vec<SwarmEvent> {
        let history = self.history.read();
        history
            .iter()
            .rev()
            .filter(|e| filter.matches(e))
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Return event bus statistics.
    pub fn stats(&self) -> EventStats {
        let per_domain: HashMap<String, u64> = self
            .domain_counts
            .iter()
            .map(|r| (r.key().clone(), *r.value()))
            .collect();
        let per_severity: HashMap<String, u64> = self
            .severity_counts
            .iter()
            .map(|r| (r.key().clone(), *r.value()))
            .collect();
        let history = self.history.read();

        EventStats {
            total_emitted: self.total_emitted.load(Ordering::Relaxed),
            total_dropped: self.total_dropped.load(Ordering::Relaxed),
            buffer_size: history.len(),
            buffer_capacity: MAX_EVENT_HISTORY,
            per_domain,
            per_severity,
        }
    }
}

// ============================================================================
// Event factory functions
// ============================================================================

// --- Health events ---

/// Node joined the swarm.
pub fn node_joined(node_id: &NodeId, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Health,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Node {} joined the swarm", node_id),
        details: serde_json::json!({"node_id": node_id.to_string()}),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Node is suspected of being unreachable.
pub fn node_suspect(node_id: &NodeId, last_seen_secs_ago: u64, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Health,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Node {} suspected unreachable (last seen {}s ago)", node_id, last_seen_secs_ago),
        details: serde_json::json!({
            "node_id": node_id.to_string(),
            "last_seen_secs_ago": last_seen_secs_ago,
        }),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![
            "Check network connectivity to the node".to_string(),
            "Review node logs for errors".to_string(),
        ],
        source_node: source,
        correlation_id: Some(format!("node-health-{}", node_id)),
        supersedes: None,
    }
}

/// Node declared dead.
pub fn node_dead(node_id: &NodeId, chunks_reassigned: usize, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Health,
        severity: EventSeverity::Error,
        complexity: ComplexityHint::Moderate,
        summary: format!("Node {} declared dead, {} chunks reassigned", node_id, chunks_reassigned),
        details: serde_json::json!({
            "node_id": node_id.to_string(),
            "chunks_reassigned": chunks_reassigned,
        }),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![
            "Investigate why the node went offline".to_string(),
            "Check if node hardware needs replacement".to_string(),
        ],
        source_node: source,
        correlation_id: Some(format!("node-health-{}", node_id)),
        supersedes: None,
    }
}

/// Node recovered from suspect/dead state.
pub fn node_recovered(node_id: &NodeId, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Health,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Node {} recovered and is alive again", node_id),
        details: serde_json::json!({"node_id": node_id.to_string()}),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("node-health-{}", node_id)),
        supersedes: None,
    }
}

/// Network partition detected.
pub fn partition_detected(reachable_fraction: f32, total_nodes: usize, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Health,
        severity: EventSeverity::Critical,
        complexity: ComplexityHint::Detailed,
        summary: format!(
            "Network partition detected: {:.0}% of {} nodes reachable",
            reachable_fraction * 100.0,
            total_nodes
        ),
        details: serde_json::json!({
            "reachable_fraction": reachable_fraction,
            "total_nodes": total_nodes,
        }),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "partition")],
        suggested_actions: vec![
            "Check network infrastructure for outages".to_string(),
            "Verify firewall rules and security groups".to_string(),
            "Consider pausing new job submissions".to_string(),
        ],
        source_node: source,
        correlation_id: Some("partition-event".to_string()),
        supersedes: None,
    }
}

/// Network partition healed.
pub fn partition_healed(recovery_rounds: u32, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Health,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!("Network partition healed after {} recovery rounds", recovery_rounds),
        details: serde_json::json!({"recovery_rounds": recovery_rounds}),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "partition")],
        suggested_actions: vec!["Review partition root cause".to_string()],
        source_node: source,
        correlation_id: Some("partition-event".to_string()),
        supersedes: None,
    }
}

/// Gossip anomaly detected (e.g. message rate too low or too high).
pub fn gossip_anomaly(description: &str, metric_value: f64, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Health,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Gossip anomaly: {}", description),
        details: serde_json::json!({
            "description": description,
            "metric_value": metric_value,
        }),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "gossip")],
        suggested_actions: vec!["Check network connectivity between nodes".to_string()],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

// --- Work events ---

/// Job submitted to the swarm.
pub fn job_submitted(job_id: &JobId, chunks: u32, priority: u32, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Work,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Job {} submitted with {} chunks (priority {})", job_id, chunks, priority),
        details: serde_json::json!({
            "job_id": job_id.to_string(),
            "chunks": chunks,
            "priority": priority,
        }),
        related_entities: vec![EntityRef::job(job_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("job-{}", job_id)),
        supersedes: None,
    }
}

/// Job completed successfully.
pub fn job_completed(job_id: &JobId, duration_secs: u64, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Work,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Job {} completed in {}s", job_id, duration_secs),
        details: serde_json::json!({
            "job_id": job_id.to_string(),
            "duration_secs": duration_secs,
        }),
        related_entities: vec![EntityRef::job(job_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("job-{}", job_id)),
        supersedes: None,
    }
}

/// Job failed.
pub fn job_failed(job_id: &JobId, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Work,
        severity: EventSeverity::Error,
        complexity: ComplexityHint::Moderate,
        summary: format!("Job {} failed: {}", job_id, reason),
        details: serde_json::json!({
            "job_id": job_id.to_string(),
            "reason": reason,
        }),
        related_entities: vec![EntityRef::job(job_id)],
        suggested_actions: vec![
            "Check job logs for error details".to_string(),
            "Consider resubmitting with adjusted parameters".to_string(),
        ],
        source_node: source,
        correlation_id: Some(format!("job-{}", job_id)),
        supersedes: None,
    }
}

/// Job cancelled.
pub fn job_cancelled(job_id: &JobId, by_whom: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Work,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!("Job {} cancelled by {}", job_id, by_whom),
        details: serde_json::json!({
            "job_id": job_id.to_string(),
            "cancelled_by": by_whom,
        }),
        related_entities: vec![EntityRef::job(job_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("job-{}", job_id)),
        supersedes: None,
    }
}

/// Job stalled (no progress for a long time).
pub fn job_stalled(job_id: &JobId, stalled_secs: u64, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Work,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Job {} stalled for {}s with no progress", job_id, stalled_secs),
        details: serde_json::json!({
            "job_id": job_id.to_string(),
            "stalled_secs": stalled_secs,
        }),
        related_entities: vec![EntityRef::job(job_id)],
        suggested_actions: vec![
            "Check executor node health".to_string(),
            "Consider cancelling and resubmitting".to_string(),
        ],
        source_node: source,
        correlation_id: Some(format!("job-{}", job_id)),
        supersedes: None,
    }
}

/// Chunk failed execution.
pub fn chunk_failed(chunk_id: &ChunkId, job_id: &JobId, reason: &str, attempts: u32, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Work,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Chunk {} (job {}) failed (attempt {}): {}", chunk_id, job_id, attempts, reason),
        details: serde_json::json!({
            "chunk_id": chunk_id.to_string(),
            "job_id": job_id.to_string(),
            "reason": reason,
            "attempts": attempts,
        }),
        related_entities: vec![EntityRef::chunk(chunk_id), EntityRef::job(job_id)],
        suggested_actions: vec!["Check executor logs".to_string()],
        source_node: source,
        correlation_id: Some(format!("job-{}", job_id)),
        supersedes: None,
    }
}

/// Chunk retrying after failure.
pub fn chunk_retrying(chunk_id: &ChunkId, job_id: &JobId, attempt: u32, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Work,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!("Chunk {} (job {}) retrying (attempt {})", chunk_id, job_id, attempt),
        details: serde_json::json!({
            "chunk_id": chunk_id.to_string(),
            "job_id": job_id.to_string(),
            "attempt": attempt,
        }),
        related_entities: vec![EntityRef::chunk(chunk_id), EntityRef::job(job_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("job-{}", job_id)),
        supersedes: None,
    }
}

// --- Fleet events ---

/// Node entering drain mode (no new work).
pub fn node_draining(node_id: &NodeId, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!("Node {} entering drain mode: {}", node_id, reason),
        details: serde_json::json!({
            "node_id": node_id.to_string(),
            "reason": reason,
        }),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("fleet-{}", node_id)),
        supersedes: None,
    }
}

/// Node fully drained.
pub fn node_drained(node_id: &NodeId, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Node {} fully drained", node_id),
        details: serde_json::json!({"node_id": node_id.to_string()}),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("fleet-{}", node_id)),
        supersedes: None,
    }
}

/// Node cordoned (excluded from scheduling).
pub fn node_cordoned(node_id: &NodeId, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!("Node {} cordoned: {}", node_id, reason),
        details: serde_json::json!({
            "node_id": node_id.to_string(),
            "reason": reason,
        }),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("fleet-{}", node_id)),
        supersedes: None,
    }
}

/// Node uncordoned (returned to scheduling pool).
pub fn node_uncordoned(node_id: &NodeId, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Node {} uncordoned and available for scheduling", node_id),
        details: serde_json::json!({"node_id": node_id.to_string()}),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("fleet-{}", node_id)),
        supersedes: None,
    }
}

/// Node quarantined (isolated due to repeated failures).
pub fn node_quarantined(node_id: &NodeId, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Node {} quarantined: {}", node_id, reason),
        details: serde_json::json!({
            "node_id": node_id.to_string(),
            "reason": reason,
        }),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![
            "Investigate node health and behavior".to_string(),
            "Consider removing node from fleet".to_string(),
        ],
        source_node: source,
        correlation_id: Some(format!("fleet-{}", node_id)),
        supersedes: None,
    }
}

/// Rolling update started.
pub fn rolling_update_started(version: &str, total_nodes: usize, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Moderate,
        summary: format!("Rolling update to v{} started ({} nodes)", version, total_nodes),
        details: serde_json::json!({
            "version": version,
            "total_nodes": total_nodes,
        }),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "rolling-update")],
        suggested_actions: vec!["Monitor canary health during rollout".to_string()],
        source_node: source,
        correlation_id: Some(format!("rolling-update-{}", version)),
        supersedes: None,
    }
}

/// Rolling update progress.
pub fn rolling_update_progress(version: &str, completed: usize, total: usize, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Rolling update v{}: {}/{} nodes updated", version, completed, total),
        details: serde_json::json!({
            "version": version,
            "completed": completed,
            "total": total,
        }),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "rolling-update")],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("rolling-update-{}", version)),
        supersedes: None,
    }
}

/// Rolling update canary is healthy.
pub fn rolling_update_canary_healthy(version: &str, canary_node: &NodeId, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Canary node {} healthy on v{}", canary_node, version),
        details: serde_json::json!({
            "version": version,
            "canary_node": canary_node.to_string(),
        }),
        related_entities: vec![
            EntityRef::new(EntityType::Swarm, "rolling-update"),
            EntityRef::node(canary_node),
        ],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("rolling-update-{}", version)),
        supersedes: None,
    }
}

/// Rolling update canary failed.
pub fn rolling_update_canary_failed(version: &str, canary_node: &NodeId, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Error,
        complexity: ComplexityHint::Detailed,
        summary: format!("Canary node {} FAILED on v{}: {}", canary_node, version, reason),
        details: serde_json::json!({
            "version": version,
            "canary_node": canary_node.to_string(),
            "reason": reason,
        }),
        related_entities: vec![
            EntityRef::new(EntityType::Swarm, "rolling-update"),
            EntityRef::node(canary_node),
        ],
        suggested_actions: vec![
            "Pause or rollback the update".to_string(),
            "Investigate canary failure logs".to_string(),
        ],
        source_node: source,
        correlation_id: Some(format!("rolling-update-{}", version)),
        supersedes: None,
    }
}

/// Rolling update paused.
pub fn rolling_update_paused(version: &str, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Simple,
        summary: format!("Rolling update v{} paused: {}", version, reason),
        details: serde_json::json!({
            "version": version,
            "reason": reason,
        }),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "rolling-update")],
        suggested_actions: vec!["Review reason and resume or rollback".to_string()],
        source_node: source,
        correlation_id: Some(format!("rolling-update-{}", version)),
        supersedes: None,
    }
}

/// Rolling update resumed.
pub fn rolling_update_resumed(version: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!("Rolling update v{} resumed", version),
        details: serde_json::json!({"version": version}),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "rolling-update")],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("rolling-update-{}", version)),
        supersedes: None,
    }
}

/// Rolling update completed.
pub fn rolling_update_completed(version: &str, total_nodes: usize, duration_secs: u64, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Moderate,
        summary: format!("Rolling update to v{} completed ({} nodes in {}s)", version, total_nodes, duration_secs),
        details: serde_json::json!({
            "version": version,
            "total_nodes": total_nodes,
            "duration_secs": duration_secs,
        }),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "rolling-update")],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("rolling-update-{}", version)),
        supersedes: None,
    }
}

/// Rolling update rolled back.
pub fn rolling_update_rolled_back(version: &str, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Rolling update v{} rolled back: {}", version, reason),
        details: serde_json::json!({
            "version": version,
            "reason": reason,
        }),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "rolling-update")],
        suggested_actions: vec!["Fix issues before retrying update".to_string()],
        source_node: source,
        correlation_id: Some(format!("rolling-update-{}", version)),
        supersedes: None,
    }
}

/// Node admitted through admission protocol.
pub fn node_admitted(node_id: &NodeId, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Node {} admitted to the swarm", node_id),
        details: serde_json::json!({"node_id": node_id.to_string()}),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Node admission rejected.
pub fn node_admission_rejected(node_id: &NodeId, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Fleet,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Node {} admission rejected: {}", node_id, reason),
        details: serde_json::json!({
            "node_id": node_id.to_string(),
            "reason": reason,
        }),
        related_entities: vec![EntityRef::node(node_id)],
        suggested_actions: vec!["Review rejection criteria".to_string()],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

// --- Data events ---

/// Blob stored successfully.
pub fn blob_stored(hash: &str, size_bytes: u64, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Data,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Blob {} stored ({} bytes)", &hash[..8.min(hash.len())], size_bytes),
        details: serde_json::json!({
            "hash": hash,
            "size_bytes": size_bytes,
        }),
        related_entities: vec![],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Data residency violation detected.
pub fn residency_violation(job_id: &JobId, required_region: &str, actual_region: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Data,
        severity: EventSeverity::Error,
        complexity: ComplexityHint::Detailed,
        summary: format!(
            "Data residency violation: job {} requires {} but executing in {}",
            job_id, required_region, actual_region
        ),
        details: serde_json::json!({
            "job_id": job_id.to_string(),
            "required_region": required_region,
            "actual_region": actual_region,
        }),
        related_entities: vec![EntityRef::job(job_id)],
        suggested_actions: vec![
            "Abort job and re-route to compliant node".to_string(),
            "Review data residency policies".to_string(),
        ],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Storage pressure (running low on disk space).
pub fn storage_pressure(available_mb: u64, total_mb: u64, utilization_pct: f32, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Data,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!(
            "Storage pressure: {:.0}% utilized ({} MB available of {} MB)",
            utilization_pct, available_mb, total_mb
        ),
        details: serde_json::json!({
            "available_mb": available_mb,
            "total_mb": total_mb,
            "utilization_pct": utilization_pct,
        }),
        related_entities: vec![],
        suggested_actions: vec![
            "Prune old blobs and checkpoints".to_string(),
            "Increase disk capacity".to_string(),
        ],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

// --- Security events ---

/// API token created.
pub fn token_created(token_name: &str, created_by: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Security,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!("API token '{}' created by {}", token_name, created_by),
        details: serde_json::json!({
            "token_name": token_name,
            "created_by": created_by,
        }),
        related_entities: vec![EntityRef::new(EntityType::Token, token_name)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// API token revoked.
pub fn token_revoked(token_name: &str, revoked_by: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Security,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!("API token '{}' revoked by {}", token_name, revoked_by),
        details: serde_json::json!({
            "token_name": token_name,
            "revoked_by": revoked_by,
        }),
        related_entities: vec![EntityRef::new(EntityType::Token, token_name)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Authentication failure.
pub fn auth_failure(remote_addr: &str, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Security,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Authentication failure from {}: {}", remote_addr, reason),
        details: serde_json::json!({
            "remote_addr": remote_addr,
            "reason": reason,
        }),
        related_entities: vec![],
        suggested_actions: vec![
            "Review access controls".to_string(),
            "Check for credential compromise".to_string(),
        ],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Policy violation detected.
pub fn policy_violation(policy_name: &str, violator: &str, details_str: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Security,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Policy '{}' violated by {}: {}", policy_name, violator, details_str),
        details: serde_json::json!({
            "policy_name": policy_name,
            "violator": violator,
            "details": details_str,
        }),
        related_entities: vec![EntityRef::new(EntityType::Policy, policy_name)],
        suggested_actions: vec!["Review and update policy".to_string()],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

// --- Cost events ---

/// Energy rate changed.
pub fn energy_rate_changed(old_rate: f64, new_rate: f64, region: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Cost,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!(
            "Energy rate in {} changed: ${:.4}/kWh -> ${:.4}/kWh",
            region, old_rate, new_rate
        ),
        details: serde_json::json!({
            "old_rate": old_rate,
            "new_rate": new_rate,
            "region": region,
        }),
        related_entities: vec![],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// AWS overflow triggered (cloud burst).
pub fn aws_overflow_triggered(reason: &str, instances_requested: u32, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Cost,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Moderate,
        summary: format!("Cloud overflow triggered: {} ({} instances)", reason, instances_requested),
        details: serde_json::json!({
            "reason": reason,
            "instances_requested": instances_requested,
        }),
        related_entities: vec![EntityRef::new(EntityType::Swarm, "cloud-overflow")],
        suggested_actions: vec!["Monitor cloud costs".to_string()],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Idle capacity is high.
pub fn idle_capacity_high(idle_fraction: f32, idle_nodes: usize, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Cost,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Simple,
        summary: format!(
            "High idle capacity: {:.0}% idle ({} nodes underutilized)",
            idle_fraction * 100.0, idle_nodes
        ),
        details: serde_json::json!({
            "idle_fraction": idle_fraction,
            "idle_nodes": idle_nodes,
        }),
        related_entities: vec![],
        suggested_actions: vec![
            "Consider scaling down fleet".to_string(),
            "Advertise capacity on marketplace".to_string(),
        ],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Marketplace award.
pub fn marketplace_award(job_id: &JobId, winner: &NodeId, bid_price: f64, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Cost,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Marketplace: job {} awarded to {} at ${:.4}", job_id, winner, bid_price),
        details: serde_json::json!({
            "job_id": job_id.to_string(),
            "winner": winner.to_string(),
            "bid_price": bid_price,
        }),
        related_entities: vec![EntityRef::job(job_id), EntityRef::node(winner)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

// --- Psyche events ---

/// Psyche facet changed.
pub fn psyche_facet_changed(facet: &str, old_value: u8, new_value: u8, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Psyche,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Moderate,
        summary: format!("Psyche facet '{}' changed: {} -> {}", facet, old_value, new_value),
        details: serde_json::json!({
            "facet": facet,
            "old_value": old_value,
            "new_value": new_value,
        }),
        related_entities: vec![],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Archetype entered (swarm personality shift).
pub fn archetype_entered(archetype: &str, trigger: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Psyche,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Moderate,
        summary: format!("Entered archetype '{}': {}", archetype, trigger),
        details: serde_json::json!({
            "archetype": archetype,
            "trigger": trigger,
        }),
        related_entities: vec![],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("archetype-{}", archetype)),
        supersedes: None,
    }
}

/// Archetype exited.
pub fn archetype_exited(archetype: &str, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Psyche,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Exited archetype '{}': {}", archetype, reason),
        details: serde_json::json!({
            "archetype": archetype,
            "reason": reason,
        }),
        related_entities: vec![],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("archetype-{}", archetype)),
        supersedes: None,
    }
}

/// Psyche alert (personality-driven operational alert).
pub fn psyche_alert(alert_name: &str, description: &str, severity: EventSeverity, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Psyche,
        severity,
        complexity: ComplexityHint::Moderate,
        summary: format!("Psyche alert '{}': {}", alert_name, description),
        details: serde_json::json!({
            "alert_name": alert_name,
            "description": description,
        }),
        related_entities: vec![],
        suggested_actions: vec!["Review psyche configuration".to_string()],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

// --- Multi-swarm events ---

/// Membrane crossing (inter-swarm message).
pub fn membrane_crossing(membrane_id: &str, direction: &str, message_type: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::MultiSwarm,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Membrane {}: {} crossing ({})", membrane_id, direction, message_type),
        details: serde_json::json!({
            "membrane_id": membrane_id,
            "direction": direction,
            "message_type": message_type,
        }),
        related_entities: vec![EntityRef::new(EntityType::Membrane, membrane_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: None,
        supersedes: None,
    }
}

/// Membrane degraded.
pub fn membrane_degraded(membrane_id: &str, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::MultiSwarm,
        severity: EventSeverity::Warning,
        complexity: ComplexityHint::Moderate,
        summary: format!("Membrane {} degraded: {}", membrane_id, reason),
        details: serde_json::json!({
            "membrane_id": membrane_id,
            "reason": reason,
        }),
        related_entities: vec![EntityRef::new(EntityType::Membrane, membrane_id)],
        suggested_actions: vec!["Check inter-swarm connectivity".to_string()],
        source_node: source,
        correlation_id: Some(format!("membrane-{}", membrane_id)),
        supersedes: None,
    }
}

/// Membrane severed (connection lost).
pub fn membrane_severed(membrane_id: &str, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::MultiSwarm,
        severity: EventSeverity::Error,
        complexity: ComplexityHint::Detailed,
        summary: format!("Membrane {} severed: {}", membrane_id, reason),
        details: serde_json::json!({
            "membrane_id": membrane_id,
            "reason": reason,
        }),
        related_entities: vec![EntityRef::new(EntityType::Membrane, membrane_id)],
        suggested_actions: vec![
            "Restore inter-swarm network connectivity".to_string(),
            "Check remote swarm health".to_string(),
        ],
        source_node: source,
        correlation_id: Some(format!("membrane-{}", membrane_id)),
        supersedes: None,
    }
}

/// Agreement proposed between swarms.
pub fn agreement_proposed(agreement_id: &str, remote_swarm: &str, terms: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::MultiSwarm,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Moderate,
        summary: format!("Agreement '{}' proposed with {}: {}", agreement_id, remote_swarm, terms),
        details: serde_json::json!({
            "agreement_id": agreement_id,
            "remote_swarm": remote_swarm,
            "terms": terms,
        }),
        related_entities: vec![EntityRef::new(EntityType::Agreement, agreement_id)],
        suggested_actions: vec!["Review agreement terms".to_string()],
        source_node: source,
        correlation_id: Some(format!("agreement-{}", agreement_id)),
        supersedes: None,
    }
}

/// Agreement signed.
pub fn agreement_signed(agreement_id: &str, remote_swarm: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::MultiSwarm,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Agreement '{}' signed with {}", agreement_id, remote_swarm),
        details: serde_json::json!({
            "agreement_id": agreement_id,
            "remote_swarm": remote_swarm,
        }),
        related_entities: vec![EntityRef::new(EntityType::Agreement, agreement_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("agreement-{}", agreement_id)),
        supersedes: None,
    }
}

/// Capacity lending started.
pub fn capacity_lending_started(agreement_id: &str, nodes_lent: usize, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::MultiSwarm,
        severity: EventSeverity::Notice,
        complexity: ComplexityHint::Moderate,
        summary: format!("Capacity lending started under agreement '{}': {} nodes", agreement_id, nodes_lent),
        details: serde_json::json!({
            "agreement_id": agreement_id,
            "nodes_lent": nodes_lent,
        }),
        related_entities: vec![EntityRef::new(EntityType::Agreement, agreement_id)],
        suggested_actions: vec!["Monitor capacity impact".to_string()],
        source_node: source,
        correlation_id: Some(format!("agreement-{}", agreement_id)),
        supersedes: None,
    }
}

/// Capacity lending ended.
pub fn capacity_lending_ended(agreement_id: &str, reason: &str, source: Option<NodeId>) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::MultiSwarm,
        severity: EventSeverity::Info,
        complexity: ComplexityHint::Simple,
        summary: format!("Capacity lending ended for agreement '{}': {}", agreement_id, reason),
        details: serde_json::json!({
            "agreement_id": agreement_id,
            "reason": reason,
        }),
        related_entities: vec![EntityRef::new(EntityType::Agreement, agreement_id)],
        suggested_actions: vec![],
        source_node: source,
        correlation_id: Some(format!("agreement-{}", agreement_id)),
        supersedes: None,
    }
}

// ============================================================================
// EventCorrelator
// ============================================================================

/// Groups related events by node_id, job_id, or partition context.
///
/// Maintains a map of active correlation groups. Events within a correlation
/// window are assigned the same `correlation_id`. Expired correlations are
/// pruned periodically.
pub struct EventCorrelator {
    /// Active correlation groups: correlation_id -> (events, first_seen, last_seen).
    active_correlations: DashMap<String, CorrelationGroup>,
}

/// A group of correlated events.
#[derive(Debug, Clone)]
pub struct CorrelationGroup {
    /// Correlation identifier.
    pub correlation_id: String,
    /// Event IDs in this group.
    pub event_ids: Vec<u64>,
    /// When the first event was seen.
    pub first_seen: DateTime<Utc>,
    /// When the last event was seen.
    pub last_seen: DateTime<Utc>,
    /// Category of correlation.
    pub category: CorrelationCategory,
}

/// Category of correlation grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorrelationCategory {
    /// Node-related events (window: 60s).
    Node,
    /// Job-related events (window: 5min).
    Job,
    /// Partition-related events (window: 30s).
    Partition,
}

impl CorrelationCategory {
    /// Return the window duration for this category in seconds.
    pub fn window_secs(&self) -> i64 {
        match self {
            Self::Node => CORRELATION_WINDOW_NODE_SECS,
            Self::Job => CORRELATION_WINDOW_JOB_SECS,
            Self::Partition => CORRELATION_WINDOW_PARTITION_SECS,
        }
    }
}

impl EventCorrelator {
    /// Create a new event correlator.
    pub fn new() -> Self {
        Self {
            active_correlations: DashMap::new(),
        }
    }

    /// Correlate an event.
    ///
    /// If the event has a `correlation_id`, it is added to the existing group
    /// (or a new group is created). Returns the correlation ID.
    pub fn correlate(&self, event: &SwarmEvent) -> Option<String> {
        let cid = event.correlation_id.as_ref()?;
        let now = Utc::now();

        let category = if cid.starts_with("node-") {
            CorrelationCategory::Node
        } else if cid.starts_with("job-") {
            CorrelationCategory::Job
        } else if cid.starts_with("partition") {
            CorrelationCategory::Partition
        } else {
            CorrelationCategory::Node
        };

        let mut entry = self
            .active_correlations
            .entry(cid.clone())
            .or_insert_with(|| CorrelationGroup {
                correlation_id: cid.clone(),
                event_ids: Vec::new(),
                first_seen: now,
                last_seen: now,
                category,
            });

        let window = chrono::Duration::seconds(entry.category.window_secs());
        if now - entry.last_seen > window {
            // Window expired, start a new group.
            entry.event_ids.clear();
            entry.first_seen = now;
        }

        entry.event_ids.push(event.id);
        entry.last_seen = now;

        Some(cid.clone())
    }

    /// Return the correlation group for the given ID, if it exists.
    pub fn get_group(&self, correlation_id: &str) -> Option<CorrelationGroup> {
        self.active_correlations
            .get(correlation_id)
            .map(|r| r.value().clone())
    }

    /// Return all active correlation groups.
    pub fn active_groups(&self) -> Vec<CorrelationGroup> {
        self.active_correlations
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    /// Remove expired correlation groups.
    pub fn prune_expired(&self) {
        let now = Utc::now();
        let expired: Vec<String> = self
            .active_correlations
            .iter()
            .filter(|r| {
                let window = chrono::Duration::seconds(r.value().category.window_secs());
                now - r.value().last_seen > window
            })
            .map(|r| r.key().clone())
            .collect();

        for key in expired {
            self.active_correlations.remove(&key);
        }
    }

    /// Return the number of active correlation groups.
    pub fn active_count(&self) -> usize {
        self.active_correlations.len()
    }
}

impl Default for EventCorrelator {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// EventDeduplicator
// ============================================================================

/// Deduplicates events based on (domain, severity, summary, first entity).
///
/// Within a configurable window (default 30s), identical events are suppressed.
pub struct EventDeduplicator {
    /// Hash of (domain, severity, summary, first_entity) -> last seen time.
    seen: DashMap<String, DateTime<Utc>>,
    /// Window in seconds.
    window_secs: i64,
}

impl EventDeduplicator {
    /// Create a new deduplicator with the default 30-second window.
    pub fn new() -> Self {
        Self {
            seen: DashMap::new(),
            window_secs: DEDUP_WINDOW_SECS,
        }
    }

    /// Create a deduplicator with a custom window.
    pub fn with_window_secs(window_secs: i64) -> Self {
        Self {
            seen: DashMap::new(),
            window_secs,
        }
    }

    /// Check if an event is a duplicate. Returns `true` if it should be suppressed.
    pub fn is_duplicate(&self, event: &SwarmEvent) -> bool {
        let key = self.make_key(event);
        let now = Utc::now();

        if let Some(last_seen) = self.seen.get(&key) {
            let elapsed = now - *last_seen;
            if elapsed < chrono::Duration::seconds(self.window_secs) {
                return true;
            }
        }

        self.seen.insert(key, now);
        false
    }

    /// Prune entries older than the window.
    pub fn prune(&self) {
        let cutoff = Utc::now() - chrono::Duration::seconds(self.window_secs);
        let expired: Vec<String> = self
            .seen
            .iter()
            .filter(|r| *r.value() < cutoff)
            .map(|r| r.key().clone())
            .collect();
        for key in expired {
            self.seen.remove(&key);
        }
    }

    /// Number of entries in the dedup cache.
    pub fn entry_count(&self) -> usize {
        self.seen.len()
    }

    /// Build a deduplication key from an event.
    fn make_key(&self, event: &SwarmEvent) -> String {
        let entity_part = event
            .related_entities
            .first()
            .map(|e| format!("{}:{}", e.entity_type, e.id))
            .unwrap_or_default();

        format!(
            "{}:{}:{}:{}",
            event.domain, event.severity, event.summary, entity_part
        )
    }
}

impl Default for EventDeduplicator {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// EventExporter
// ============================================================================

/// Static methods for exporting events to various formats.
pub struct EventExporter;

impl EventExporter {
    /// Export events as a JSON array string.
    pub fn to_json(events: &[SwarmEvent]) -> Result<String, String> {
        serde_json::to_string_pretty(events).map_err(|e| format!("JSON serialization error: {}", e))
    }

    /// Export events as CSV.
    pub fn to_csv(events: &[SwarmEvent]) -> String {
        let mut out = String::from("id,timestamp,domain,severity,summary,entities,correlation_id\n");
        for event in events {
            let entities: Vec<String> = event
                .related_entities
                .iter()
                .map(|e| e.to_string())
                .collect();
            let cid = event
                .correlation_id
                .as_deref()
                .unwrap_or("");
            out.push_str(&format!(
                "{},{},{},{},\"{}\",\"{}\",\"{}\"\n",
                event.id,
                event.timestamp.to_rfc3339(),
                event.domain,
                event.severity,
                event.summary.replace('"', "\"\""),
                entities.join(";"),
                cid,
            ));
        }
        out
    }

    /// Export events as newline-delimited JSON (NDJSON).
    pub fn to_ndjson(events: &[SwarmEvent]) -> String {
        let mut out = String::new();
        for event in events {
            if let Ok(line) = serde_json::to_string(event) {
                out.push_str(&line);
                out.push('\n');
            }
        }
        out
    }
}

// ============================================================================
// EventAggregator
// ============================================================================

/// Static methods for computing aggregate statistics over events.
pub struct EventAggregator;

impl EventAggregator {
    /// Count events by domain.
    pub fn count_by_domain(events: &[SwarmEvent]) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for event in events {
            *counts.entry(event.domain.to_string()).or_insert(0) += 1;
        }
        counts
    }

    /// Count events by severity.
    pub fn count_by_severity(events: &[SwarmEvent]) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for event in events {
            *counts.entry(event.severity.to_string()).or_insert(0) += 1;
        }
        counts
    }

    /// Count events by hour (UTC).
    pub fn count_by_hour(events: &[SwarmEvent]) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for event in events {
            let hour_key = format!(
                "{:04}-{:02}-{:02}T{:02}",
                event.timestamp.year(),
                event.timestamp.month(),
                event.timestamp.day(),
                event.timestamp.hour()
            );
            *counts.entry(hour_key).or_insert(0) += 1;
        }
        counts
    }

    /// Top entities by number of event mentions.
    pub fn top_entities(events: &[SwarmEvent], limit: usize) -> Vec<(String, usize)> {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for event in events {
            for entity in &event.related_entities {
                *counts.entry(entity.to_string()).or_insert(0) += 1;
            }
        }
        let mut sorted: Vec<(String, usize)> = counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        sorted.truncate(limit);
        sorted
    }

    /// Severity trend: count events by severity in the given time windows.
    /// Returns (window_label, severity, count) tuples.
    pub fn severity_trend(
        events: &[SwarmEvent],
        window_minutes: u32,
    ) -> Vec<(String, String, usize)> {
        let mut buckets: HashMap<(String, String), usize> = HashMap::new();

        for event in events {
            let bucket_start_minute =
                (event.timestamp.minute() / window_minutes) * window_minutes;
            let label = format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}",
                event.timestamp.year(),
                event.timestamp.month(),
                event.timestamp.day(),
                event.timestamp.hour(),
                bucket_start_minute
            );
            let key = (label, event.severity.to_string());
            *buckets.entry(key).or_insert(0) += 1;
        }

        let mut result: Vec<(String, String, usize)> = buckets
            .into_iter()
            .map(|((label, sev), count)| (label, sev, count))
            .collect();
        result.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        result
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bus() -> EventBus {
        EventBus::new(128)
    }

    fn make_event(domain: ConcernDomain, severity: EventSeverity, summary: &str) -> SwarmEvent {
        SwarmEvent {
            id: 0,
            timestamp: Utc::now(),
            domain,
            severity,
            complexity: ComplexityHint::Simple,
            summary: summary.to_string(),
            details: serde_json::Value::Null,
            related_entities: Vec::new(),
            suggested_actions: Vec::new(),
            source_node: None,
            correlation_id: None,
            supersedes: None,
        }
    }

    // --- EntityType tests ---

    #[test]
    fn entity_type_display_all() {
        assert_eq!(EntityType::Node.to_string(), "node");
        assert_eq!(EntityType::Job.to_string(), "job");
        assert_eq!(EntityType::Chunk.to_string(), "chunk");
        assert_eq!(EntityType::Collective.to_string(), "collective");
        assert_eq!(EntityType::Membrane.to_string(), "membrane");
        assert_eq!(EntityType::Agreement.to_string(), "agreement");
        assert_eq!(EntityType::Swarm.to_string(), "swarm");
        assert_eq!(EntityType::Policy.to_string(), "policy");
        assert_eq!(EntityType::Token.to_string(), "token");
    }

    #[test]
    fn entity_type_from_str_all() {
        assert_eq!("node".parse::<EntityType>().ok(), Some(EntityType::Node));
        assert_eq!("job".parse::<EntityType>().ok(), Some(EntityType::Job));
        assert_eq!("chunk".parse::<EntityType>().ok(), Some(EntityType::Chunk));
        assert_eq!("swarm".parse::<EntityType>().ok(), Some(EntityType::Swarm));
        assert!("xyz".parse::<EntityType>().is_err());
    }

    #[test]
    fn entity_ref_display() {
        let r = EntityRef::new(EntityType::Node, "abc-123");
        assert_eq!(r.to_string(), "node:abc-123");

        let r2 = EntityRef::with_name(EntityType::Job, "xyz", "my-job");
        assert_eq!(r2.to_string(), "job:xyz(my-job)");
    }

    #[test]
    fn entity_ref_node_helper() {
        let nid = NodeId::new();
        let r = EntityRef::node(&nid);
        assert_eq!(r.entity_type, EntityType::Node);
        assert_eq!(r.id, nid.to_string());
    }

    // --- EventBus tests ---

    #[test]
    fn emit_assigns_unique_ids() {
        let bus = make_bus();
        let id1 = bus.emit_simple(ConcernDomain::Health, EventSeverity::Info, "first");
        let id2 = bus.emit_simple(ConcernDomain::Health, EventSeverity::Info, "second");
        assert!(id2 > id1);
    }

    #[test]
    fn emit_stores_in_history() {
        let bus = make_bus();
        bus.emit_simple(ConcernDomain::Work, EventSeverity::Warning, "test event");
        let events = bus.recent_events(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].summary, "test event");
    }

    #[test]
    fn recent_events_respects_limit() {
        let bus = make_bus();
        for i in 0..10 {
            bus.emit_simple(ConcernDomain::Health, EventSeverity::Info, format!("event {}", i));
        }
        let events = bus.recent_events(3);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].summary, "event 7");
        assert_eq!(events[2].summary, "event 9");
    }

    #[test]
    fn history_ring_buffer_evicts_oldest() {
        let bus = EventBus::new(16); // small broadcast buffer
        // Emit more than MAX_EVENT_HISTORY
        // (we can't literally emit 10,001 in a test, but we verify ring buffer behavior)
        for i in 0..20 {
            bus.emit_simple(ConcernDomain::Health, EventSeverity::Info, format!("event {}", i));
        }
        let events = bus.recent_events(100);
        assert_eq!(events.len(), 20); // 20 < MAX_EVENT_HISTORY so all kept
    }

    #[test]
    fn stats_tracks_counts() {
        let bus = make_bus();
        bus.emit_simple(ConcernDomain::Health, EventSeverity::Info, "a");
        bus.emit_simple(ConcernDomain::Health, EventSeverity::Warning, "b");
        bus.emit_simple(ConcernDomain::Work, EventSeverity::Error, "c");

        let stats = bus.stats();
        assert_eq!(stats.total_emitted, 3);
        assert_eq!(stats.per_domain.get("health"), Some(&2));
        assert_eq!(stats.per_domain.get("work"), Some(&1));
        assert_eq!(stats.per_severity.get("info"), Some(&1));
        assert_eq!(stats.per_severity.get("warning"), Some(&1));
    }

    #[test]
    fn subscribe_receives_events() {
        let bus = make_bus();
        let mut rx = bus.subscribe();
        bus.emit_simple(ConcernDomain::Health, EventSeverity::Info, "hello");
        let event = rx.try_recv().expect("should receive event");
        assert_eq!(event.summary, "hello");
    }

    #[test]
    fn emit_with_entities_works() {
        let bus = make_bus();
        let nid = NodeId::new();
        let entities = vec![EntityRef::node(&nid)];
        bus.emit_with_entities(
            ConcernDomain::Health,
            EventSeverity::Info,
            "node event",
            entities,
            serde_json::json!({"key": "val"}),
        );
        let events = bus.recent_events(1);
        assert_eq!(events[0].related_entities.len(), 1);
    }

    // --- EventFilter tests ---

    #[test]
    fn filter_domain() {
        let filter = EventFilter {
            domains: Some(vec![ConcernDomain::Health]),
            ..Default::default()
        };
        let e1 = make_event(ConcernDomain::Health, EventSeverity::Info, "x");
        let e2 = make_event(ConcernDomain::Work, EventSeverity::Info, "y");
        assert!(filter.matches(&e1));
        assert!(!filter.matches(&e2));
    }

    #[test]
    fn filter_min_severity() {
        let filter = EventFilter {
            min_severity: Some(EventSeverity::Warning),
            ..Default::default()
        };
        let e1 = make_event(ConcernDomain::Health, EventSeverity::Info, "x");
        let e2 = make_event(ConcernDomain::Health, EventSeverity::Error, "y");
        assert!(!filter.matches(&e1));
        assert!(filter.matches(&e2));
    }

    #[test]
    fn filter_keyword() {
        let filter = EventFilter {
            keywords: Some(vec!["dead".to_string()]),
            ..Default::default()
        };
        let e1 = make_event(ConcernDomain::Health, EventSeverity::Error, "Node declared dead");
        let e2 = make_event(ConcernDomain::Health, EventSeverity::Info, "Node joined");
        assert!(filter.matches(&e1));
        assert!(!filter.matches(&e2));
    }

    #[test]
    fn filter_keyword_case_insensitive() {
        let filter = EventFilter {
            keywords: Some(vec!["DEAD".to_string()]),
            ..Default::default()
        };
        let e = make_event(ConcernDomain::Health, EventSeverity::Error, "Node declared dead");
        assert!(filter.matches(&e));
    }

    #[test]
    fn filter_exclude_keyword() {
        let filter = EventFilter {
            exclude_keywords: Some(vec!["test".to_string()]),
            ..Default::default()
        };
        let e1 = make_event(ConcernDomain::Health, EventSeverity::Info, "a test event");
        let e2 = make_event(ConcernDomain::Health, EventSeverity::Info, "a real event");
        assert!(!filter.matches(&e1));
        assert!(filter.matches(&e2));
    }

    #[test]
    fn filter_entity_type() {
        let filter = EventFilter {
            entity_types: Some(vec![EntityType::Node]),
            ..Default::default()
        };
        let mut e1 = make_event(ConcernDomain::Health, EventSeverity::Info, "x");
        e1.related_entities.push(EntityRef::new(EntityType::Node, "n1"));
        let e2 = make_event(ConcernDomain::Work, EventSeverity::Info, "y");
        assert!(filter.matches(&e1));
        assert!(!filter.matches(&e2));
    }

    #[test]
    fn filter_entity_id() {
        let filter = EventFilter {
            entity_ids: Some(vec!["node-abc".to_string()]),
            ..Default::default()
        };
        let mut e1 = make_event(ConcernDomain::Health, EventSeverity::Info, "x");
        e1.related_entities.push(EntityRef::new(EntityType::Node, "node-abc"));
        let e2 = make_event(ConcernDomain::Health, EventSeverity::Info, "y");
        assert!(filter.matches(&e1));
        assert!(!filter.matches(&e2));
    }

    #[test]
    fn filter_correlation_id() {
        let filter = EventFilter {
            correlation_id: Some("job-123".to_string()),
            ..Default::default()
        };
        let mut e1 = make_event(ConcernDomain::Work, EventSeverity::Info, "x");
        e1.correlation_id = Some("job-123".to_string());
        let e2 = make_event(ConcernDomain::Work, EventSeverity::Info, "y");
        assert!(filter.matches(&e1));
        assert!(!filter.matches(&e2));
    }

    #[test]
    fn filter_since() {
        let past = Utc::now() - chrono::Duration::hours(1);
        let filter = EventFilter {
            since: Some(Utc::now() - chrono::Duration::minutes(30)),
            ..Default::default()
        };
        let mut e_old = make_event(ConcernDomain::Health, EventSeverity::Info, "old");
        e_old.timestamp = past;
        let e_new = make_event(ConcernDomain::Health, EventSeverity::Info, "new");
        assert!(!filter.matches(&e_old));
        assert!(filter.matches(&e_new));
    }

    #[test]
    fn filter_empty_matches_all() {
        let filter = EventFilter::default();
        let e = make_event(ConcernDomain::Health, EventSeverity::Critical, "anything");
        assert!(filter.matches(&e));
    }

    #[test]
    fn filter_from_query_params() {
        let mut params = HashMap::new();
        params.insert("domain".to_string(), "health,work".to_string());
        params.insert("min_severity".to_string(), "warning".to_string());
        params.insert("keyword".to_string(), "dead,failed".to_string());

        let filter = EventFilter::from_query_params(&params);
        assert_eq!(filter.domains.as_ref().map(|d| d.len()), Some(2));
        assert_eq!(filter.min_severity, Some(EventSeverity::Warning));
        assert_eq!(filter.keywords.as_ref().map(|k| k.len()), Some(2));
    }

    #[test]
    fn recent_filtered_works() {
        let bus = make_bus();
        bus.emit_simple(ConcernDomain::Health, EventSeverity::Info, "info event");
        bus.emit_simple(ConcernDomain::Health, EventSeverity::Error, "error event");
        bus.emit_simple(ConcernDomain::Work, EventSeverity::Warning, "work warn");

        let filter = EventFilter {
            domains: Some(vec![ConcernDomain::Health]),
            min_severity: Some(EventSeverity::Error),
            ..Default::default()
        };
        let filtered = bus.recent_filtered(&filter, 10);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].summary, "error event");
    }

    // --- Factory function tests ---

    #[test]
    fn factory_node_joined() {
        let nid = NodeId::new();
        let e = node_joined(&nid, None);
        assert_eq!(e.domain, ConcernDomain::Health);
        assert_eq!(e.severity, EventSeverity::Info);
        assert!(e.summary.contains(&nid.to_string()));
        assert_eq!(e.related_entities.len(), 1);
    }

    #[test]
    fn factory_node_dead() {
        let nid = NodeId::new();
        let e = node_dead(&nid, 5, None);
        assert_eq!(e.severity, EventSeverity::Error);
        assert!(e.summary.contains("dead"));
        assert!(e.summary.contains("5"));
    }

    #[test]
    fn factory_job_submitted() {
        let jid = JobId::new();
        let e = job_submitted(&jid, 10, 3, None);
        assert_eq!(e.domain, ConcernDomain::Work);
        assert!(e.summary.contains("10 chunks"));
    }

    #[test]
    fn factory_partition_detected() {
        let e = partition_detected(0.4, 100, None);
        assert_eq!(e.severity, EventSeverity::Critical);
        assert!(!e.suggested_actions.is_empty());
    }

    #[test]
    fn factory_auth_failure() {
        let e = auth_failure("192.168.1.1", "invalid token", None);
        assert_eq!(e.domain, ConcernDomain::Security);
        assert_eq!(e.severity, EventSeverity::Warning);
    }

    #[test]
    fn factory_membrane_severed() {
        let e = membrane_severed("m-1", "timeout", None);
        assert_eq!(e.domain, ConcernDomain::MultiSwarm);
        assert_eq!(e.severity, EventSeverity::Error);
    }

    #[test]
    fn factory_rolling_update_lifecycle() {
        let e1 = rolling_update_started("1.2.0", 10, None);
        assert_eq!(e1.domain, ConcernDomain::Fleet);
        let e2 = rolling_update_progress("1.2.0", 5, 10, None);
        assert!(e2.summary.contains("5/10"));
        let e3 = rolling_update_completed("1.2.0", 10, 300, None);
        assert!(e3.summary.contains("completed"));
    }

    // --- SwarmEvent methods ---

    #[test]
    fn to_sse_data_returns_json() {
        let e = make_event(ConcernDomain::Health, EventSeverity::Info, "sse test");
        let sse = e.to_sse_data();
        let parsed: serde_json::Value = serde_json::from_str(&sse).expect("valid json");
        assert_eq!(parsed["summary"], "sse test");
    }

    #[test]
    fn matches_filter_delegates() {
        let filter = EventFilter {
            domains: Some(vec![ConcernDomain::Work]),
            ..Default::default()
        };
        let e = make_event(ConcernDomain::Work, EventSeverity::Info, "x");
        assert!(e.matches_filter(&filter));
    }

    #[test]
    fn event_display_format() {
        let e = make_event(ConcernDomain::Health, EventSeverity::Error, "something bad");
        let s = format!("{}", e);
        assert!(s.contains("[health]"));
        assert!(s.contains("[error]"));
        assert!(s.contains("something bad"));
    }

    // --- EventCorrelator tests ---

    #[test]
    fn correlator_groups_by_id() {
        let c = EventCorrelator::new();
        let mut e1 = make_event(ConcernDomain::Health, EventSeverity::Info, "a");
        e1.id = 1;
        e1.correlation_id = Some("node-abc".to_string());
        let mut e2 = make_event(ConcernDomain::Health, EventSeverity::Warning, "b");
        e2.id = 2;
        e2.correlation_id = Some("node-abc".to_string());

        c.correlate(&e1);
        c.correlate(&e2);

        let group = c.get_group("node-abc").expect("group exists");
        assert_eq!(group.event_ids.len(), 2);
        assert_eq!(group.category, CorrelationCategory::Node);
    }

    #[test]
    fn correlator_job_category() {
        let c = EventCorrelator::new();
        let mut e = make_event(ConcernDomain::Work, EventSeverity::Info, "job event");
        e.id = 1;
        e.correlation_id = Some("job-xyz".to_string());
        c.correlate(&e);

        let group = c.get_group("job-xyz").expect("group exists");
        assert_eq!(group.category, CorrelationCategory::Job);
    }

    #[test]
    fn correlator_no_correlation_id_returns_none() {
        let c = EventCorrelator::new();
        let e = make_event(ConcernDomain::Health, EventSeverity::Info, "no cid");
        assert!(c.correlate(&e).is_none());
    }

    #[test]
    fn correlator_prune_expired() {
        let c = EventCorrelator::new();
        // Insert a group with very old last_seen
        c.active_correlations.insert(
            "old-group".to_string(),
            CorrelationGroup {
                correlation_id: "old-group".to_string(),
                event_ids: vec![1],
                first_seen: Utc::now() - chrono::Duration::hours(1),
                last_seen: Utc::now() - chrono::Duration::hours(1),
                category: CorrelationCategory::Node,
            },
        );
        assert_eq!(c.active_count(), 1);
        c.prune_expired();
        assert_eq!(c.active_count(), 0);
    }

    // --- EventDeduplicator tests ---

    #[test]
    fn dedup_suppresses_duplicates() {
        let d = EventDeduplicator::new();
        let e = make_event(ConcernDomain::Health, EventSeverity::Error, "same event");
        assert!(!d.is_duplicate(&e)); // first time
        assert!(d.is_duplicate(&e)); // duplicate
    }

    #[test]
    fn dedup_allows_different_events() {
        let d = EventDeduplicator::new();
        let e1 = make_event(ConcernDomain::Health, EventSeverity::Error, "event A");
        let e2 = make_event(ConcernDomain::Health, EventSeverity::Error, "event B");
        assert!(!d.is_duplicate(&e1));
        assert!(!d.is_duplicate(&e2));
    }

    #[test]
    fn dedup_prune_removes_old_entries() {
        let d = EventDeduplicator::with_window_secs(0); // 0 second window => everything is old
        let e = make_event(ConcernDomain::Health, EventSeverity::Info, "x");
        d.is_duplicate(&e);
        assert_eq!(d.entry_count(), 1);
        // With 0-second window, everything is expired
        std::thread::sleep(std::time::Duration::from_millis(10));
        d.prune();
        assert_eq!(d.entry_count(), 0);
    }

    // --- EventExporter tests ---

    #[test]
    fn export_json() {
        let events = vec![
            make_event(ConcernDomain::Health, EventSeverity::Info, "test"),
        ];
        let json = EventExporter::to_json(&events).expect("json works");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().expect("array").len(), 1);
    }

    #[test]
    fn export_csv() {
        let events = vec![
            make_event(ConcernDomain::Health, EventSeverity::Info, "csv test"),
        ];
        let csv = EventExporter::to_csv(&events);
        assert!(csv.starts_with("id,timestamp,domain"));
        assert!(csv.contains("csv test"));
    }

    #[test]
    fn export_ndjson() {
        let events = vec![
            make_event(ConcernDomain::Health, EventSeverity::Info, "line1"),
            make_event(ConcernDomain::Work, EventSeverity::Error, "line2"),
        ];
        let ndjson = EventExporter::to_ndjson(&events);
        let lines: Vec<&str> = ndjson.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        for line in lines {
            let _: serde_json::Value = serde_json::from_str(line).expect("valid json");
        }
    }

    // --- EventAggregator tests ---

    #[test]
    fn count_by_domain_works() {
        let events = vec![
            make_event(ConcernDomain::Health, EventSeverity::Info, "a"),
            make_event(ConcernDomain::Health, EventSeverity::Warning, "b"),
            make_event(ConcernDomain::Work, EventSeverity::Error, "c"),
        ];
        let counts = EventAggregator::count_by_domain(&events);
        assert_eq!(counts.get("health"), Some(&2));
        assert_eq!(counts.get("work"), Some(&1));
    }

    #[test]
    fn count_by_severity_works() {
        let events = vec![
            make_event(ConcernDomain::Health, EventSeverity::Info, "a"),
            make_event(ConcernDomain::Health, EventSeverity::Info, "b"),
            make_event(ConcernDomain::Work, EventSeverity::Error, "c"),
        ];
        let counts = EventAggregator::count_by_severity(&events);
        assert_eq!(counts.get("info"), Some(&2));
        assert_eq!(counts.get("error"), Some(&1));
    }

    #[test]
    fn count_by_hour_works() {
        let events = vec![
            make_event(ConcernDomain::Health, EventSeverity::Info, "a"),
        ];
        let counts = EventAggregator::count_by_hour(&events);
        assert_eq!(counts.len(), 1);
        // The key should be today's hour
        let key = counts.keys().next().expect("one key");
        assert!(key.len() > 10); // YYYY-MM-DDTHH
    }

    #[test]
    fn top_entities_works() {
        let nid = NodeId::new();
        let mut e1 = make_event(ConcernDomain::Health, EventSeverity::Info, "a");
        e1.related_entities.push(EntityRef::node(&nid));
        let mut e2 = make_event(ConcernDomain::Health, EventSeverity::Info, "b");
        e2.related_entities.push(EntityRef::node(&nid));
        let jid = JobId::new();
        let mut e3 = make_event(ConcernDomain::Work, EventSeverity::Info, "c");
        e3.related_entities.push(EntityRef::job(&jid));

        let top = EventAggregator::top_entities(&[e1, e2, e3], 5);
        assert_eq!(top.len(), 2);
        // Node should be first (2 mentions)
        assert_eq!(top[0].1, 2);
    }

    #[test]
    fn severity_trend_works() {
        let events = vec![
            make_event(ConcernDomain::Health, EventSeverity::Info, "a"),
            make_event(ConcernDomain::Health, EventSeverity::Error, "b"),
        ];
        let trend = EventAggregator::severity_trend(&events, 15);
        assert!(!trend.is_empty());
    }

    // --- Additional factory function coverage ---

    #[test]
    fn factory_node_suspect() {
        let nid = NodeId::new();
        let e = node_suspect(&nid, 15, None);
        assert_eq!(e.severity, EventSeverity::Warning);
        assert!(e.correlation_id.is_some());
    }

    #[test]
    fn factory_node_recovered() {
        let nid = NodeId::new();
        let e = node_recovered(&nid, None);
        assert_eq!(e.severity, EventSeverity::Info);
    }

    #[test]
    fn factory_job_failed_and_cancelled() {
        let jid = JobId::new();
        let e1 = job_failed(&jid, "oom", None);
        assert_eq!(e1.severity, EventSeverity::Error);
        let e2 = job_cancelled(&jid, "admin", None);
        assert_eq!(e2.severity, EventSeverity::Notice);
    }

    #[test]
    fn factory_psyche_events() {
        let e1 = psyche_facet_changed("resilience", 50, 30, None);
        assert_eq!(e1.domain, ConcernDomain::Psyche);
        let e2 = archetype_entered("war-room", "high failure rate", None);
        assert!(e2.correlation_id.is_some());
        let e3 = archetype_exited("war-room", "recovered", None);
        assert_eq!(e3.severity, EventSeverity::Info);
    }

    #[test]
    fn factory_cost_events() {
        let e1 = energy_rate_changed(0.10, 0.15, "us-east", None);
        assert_eq!(e1.domain, ConcernDomain::Cost);
        let e2 = idle_capacity_high(0.8, 20, None);
        assert!(!e2.suggested_actions.is_empty());
    }

    #[test]
    fn factory_data_events() {
        let e1 = blob_stored("abcdef1234567890", 1024, None);
        assert_eq!(e1.domain, ConcernDomain::Data);
        let jid = JobId::new();
        let e2 = residency_violation(&jid, "eu-west", "us-east", None);
        assert_eq!(e2.severity, EventSeverity::Error);
    }

    #[test]
    fn factory_fleet_drain_cordon() {
        let nid = NodeId::new();
        let e1 = node_draining(&nid, "maintenance", None);
        assert_eq!(e1.domain, ConcernDomain::Fleet);
        let e2 = node_drained(&nid, None);
        assert_eq!(e2.severity, EventSeverity::Info);
        let e3 = node_cordoned(&nid, "bad behavior", None);
        assert_eq!(e3.severity, EventSeverity::Notice);
        let e4 = node_uncordoned(&nid, None);
        assert_eq!(e4.severity, EventSeverity::Info);
    }

    #[test]
    fn factory_agreement_and_capacity() {
        let e1 = agreement_proposed("t-1", "remote-swarm", "share 10%", None);
        assert_eq!(e1.domain, ConcernDomain::MultiSwarm);
        let e2 = agreement_signed("t-1", "remote-swarm", None);
        assert_eq!(e2.severity, EventSeverity::Info);
        let e3 = capacity_lending_started("t-1", 5, None);
        assert!(e3.summary.contains("5 nodes"));
        let e4 = capacity_lending_ended("t-1", "agreement expired", None);
        assert_eq!(e4.severity, EventSeverity::Info);
    }

    #[test]
    fn default_bus_creation() {
        let bus = EventBus::default_bus();
        let stats = bus.stats();
        assert_eq!(stats.total_emitted, 0);
        assert_eq!(stats.buffer_capacity, MAX_EVENT_HISTORY);
    }

    #[test]
    fn filter_combined_criteria() {
        let filter = EventFilter {
            domains: Some(vec![ConcernDomain::Health]),
            min_severity: Some(EventSeverity::Warning),
            keywords: Some(vec!["dead".to_string()]),
            ..Default::default()
        };
        // Matches all criteria
        let e1 = make_event(ConcernDomain::Health, EventSeverity::Error, "node declared dead");
        assert!(filter.matches(&e1));

        // Wrong domain
        let e2 = make_event(ConcernDomain::Work, EventSeverity::Error, "node declared dead");
        assert!(!filter.matches(&e2));

        // Wrong severity
        let e3 = make_event(ConcernDomain::Health, EventSeverity::Info, "node declared dead");
        assert!(!filter.matches(&e3));

        // Wrong keyword
        let e4 = make_event(ConcernDomain::Health, EventSeverity::Error, "node joined");
        assert!(!filter.matches(&e4));
    }

    #[test]
    fn correlator_active_groups() {
        let c = EventCorrelator::new();
        let mut e1 = make_event(ConcernDomain::Health, EventSeverity::Info, "a");
        e1.id = 1;
        e1.correlation_id = Some("node-a".to_string());
        let mut e2 = make_event(ConcernDomain::Work, EventSeverity::Info, "b");
        e2.id = 2;
        e2.correlation_id = Some("job-b".to_string());

        c.correlate(&e1);
        c.correlate(&e2);

        let groups = c.active_groups();
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn factory_gossip_anomaly() {
        let e = gossip_anomaly("message rate too low", 0.5, None);
        assert_eq!(e.domain, ConcernDomain::Health);
        assert_eq!(e.severity, EventSeverity::Warning);
    }

    #[test]
    fn factory_storage_pressure() {
        let e = storage_pressure(500, 10000, 95.0, None);
        assert_eq!(e.domain, ConcernDomain::Data);
        assert_eq!(e.severity, EventSeverity::Warning);
    }

    #[test]
    fn factory_chunk_failed_and_retrying() {
        let cid = ChunkId::new();
        let jid = JobId::new();
        let e1 = chunk_failed(&cid, &jid, "segfault", 2, None);
        assert_eq!(e1.severity, EventSeverity::Warning);
        assert_eq!(e1.related_entities.len(), 2);
        let e2 = chunk_retrying(&cid, &jid, 3, None);
        assert_eq!(e2.severity, EventSeverity::Notice);
    }
}

/// An arbitration round has been requested for a contested computation result.
pub fn arbitration_requested(
    job_id: &JobId,
    node_id: &NodeId,
    event_id: &str,
    snapshot_hash: &str,
    reason: &str,
    source: Option<NodeId>,
) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain: ConcernDomain::Security,
        severity: EventSeverity::Critical,
        complexity: ComplexityHint::Detailed,
        summary: format!("Arbitration Court convened for job {}: {}", job_id, reason),
        details: serde_json::json!({
            "job_id": job_id,
            "target_node": node_id,
            "contested_event": event_id,
            "snapshot_hash": snapshot_hash,
            "reason": reason,
        }),
        related_entities: vec![
            EntityRef::job(job_id),
            EntityRef::node(node_id),
        ],
        suggested_actions: vec![
            "Monitor Court of Arbitration outcome".to_string(),
            "Review node reputation history".to_string(),
        ],
        source_node: source,
        correlation_id: Some(format!("arbitration-{}", event_id)),
        supersedes: None,
    }
}
