// Marabunta - Licensed under the MIT License.
//! Audit Trail for Accountability
//!
//! This module provides comprehensive audit logging for infrastructure node operations,
//! including task assignments, configuration changes, and alert history.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;

use crate::common::TaskId;

use super::health::Alert;
use super::node::NodeId;

/// Audit event types for tracking all significant operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum AuditEvent {
    /// Node was registered in the system.
    NodeRegistered {
        node_id: NodeId,
        by: String,
        hostname: String,
        datacenter: String,
        at: DateTime<Utc>,
    },
    /// Node was removed from the system.
    NodeDeregistered {
        node_id: NodeId,
        by: String,
        reason: String,
        at: DateTime<Utc>,
    },
    /// Node status changed.
    NodeStatusChanged {
        node_id: NodeId,
        old_status: String,
        new_status: String,
        reason: Option<String>,
        at: DateTime<Utc>,
    },
    /// Task was assigned to a node.
    TaskAssigned {
        task_id: TaskId,
        node_id: NodeId,
        at: DateTime<Utc>,
    },
    /// Task completed successfully.
    TaskCompleted {
        task_id: TaskId,
        node_id: NodeId,
        duration_ms: u64,
        at: DateTime<Utc>,
    },
    /// Task failed.
    TaskFailed {
        task_id: TaskId,
        node_id: NodeId,
        error: String,
        duration_ms: u64,
        at: DateTime<Utc>,
    },
    /// Configuration was changed.
    ConfigChanged {
        node_id: NodeId,
        field: String,
        old_value: String,
        new_value: String,
        by: String,
        at: DateTime<Utc>,
    },
    /// Alert was triggered.
    AlertTriggered {
        node_id: NodeId,
        alert_type: String,
        severity: String,
        message: String,
        at: DateTime<Utc>,
    },
    /// Alert was resolved.
    AlertResolved {
        node_id: NodeId,
        alert_type: String,
        at: DateTime<Utc>,
    },
    /// Maintenance window started.
    MaintenanceStarted {
        node_id: NodeId,
        by: String,
        reason: String,
        scheduled_end: DateTime<Utc>,
        at: DateTime<Utc>,
    },
    /// Maintenance window ended.
    MaintenanceEnded {
        node_id: NodeId,
        by: String,
        at: DateTime<Utc>,
    },
    /// SLA violation detected.
    SlaViolation {
        node_id: NodeId,
        required_uptime: f64,
        actual_uptime: f64,
        period_start: DateTime<Utc>,
        period_end: DateTime<Utc>,
        at: DateTime<Utc>,
    },
}

impl AuditEvent {
    /// Creates a node registered event.
    pub fn node_registered(
        node_id: NodeId,
        by: impl Into<String>,
        hostname: impl Into<String>,
        datacenter: impl Into<String>,
    ) -> Self {
        Self::NodeRegistered {
            node_id,
            by: by.into(),
            hostname: hostname.into(),
            datacenter: datacenter.into(),
            at: Utc::now(),
        }
    }

    /// Creates a node deregistered event.
    pub fn node_deregistered(
        node_id: NodeId,
        by: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::NodeDeregistered {
            node_id,
            by: by.into(),
            reason: reason.into(),
            at: Utc::now(),
        }
    }

    /// Creates a task assigned event.
    pub fn task_assigned(task_id: TaskId, node_id: NodeId) -> Self {
        Self::TaskAssigned {
            task_id,
            node_id,
            at: Utc::now(),
        }
    }

    /// Creates a task completed event.
    pub fn task_completed(task_id: TaskId, node_id: NodeId, duration_ms: u64) -> Self {
        Self::TaskCompleted {
            task_id,
            node_id,
            duration_ms,
            at: Utc::now(),
        }
    }

    /// Creates a task failed event.
    pub fn task_failed(
        task_id: TaskId,
        node_id: NodeId,
        error: impl Into<String>,
        duration_ms: u64,
    ) -> Self {
        Self::TaskFailed {
            task_id,
            node_id,
            error: error.into(),
            duration_ms,
            at: Utc::now(),
        }
    }

    /// Creates a config changed event.
    pub fn config_changed(
        node_id: NodeId,
        field: impl Into<String>,
        old_value: impl Into<String>,
        new_value: impl Into<String>,
        by: impl Into<String>,
    ) -> Self {
        Self::ConfigChanged {
            node_id,
            field: field.into(),
            old_value: old_value.into(),
            new_value: new_value.into(),
            by: by.into(),
            at: Utc::now(),
        }
    }

    /// Creates an alert triggered event from an Alert.
    pub fn from_alert(alert: &Alert) -> Self {
        Self::AlertTriggered {
            node_id: alert.node_id(),
            alert_type: format!("{:?}", alert.alert_type()),
            severity: alert.severity().to_string(),
            message: alert.to_string(),
            at: Utc::now(),
        }
    }

    /// Returns the timestamp of the event.
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            AuditEvent::NodeRegistered { at, .. } => *at,
            AuditEvent::NodeDeregistered { at, .. } => *at,
            AuditEvent::NodeStatusChanged { at, .. } => *at,
            AuditEvent::TaskAssigned { at, .. } => *at,
            AuditEvent::TaskCompleted { at, .. } => *at,
            AuditEvent::TaskFailed { at, .. } => *at,
            AuditEvent::ConfigChanged { at, .. } => *at,
            AuditEvent::AlertTriggered { at, .. } => *at,
            AuditEvent::AlertResolved { at, .. } => *at,
            AuditEvent::MaintenanceStarted { at, .. } => *at,
            AuditEvent::MaintenanceEnded { at, .. } => *at,
            AuditEvent::SlaViolation { at, .. } => *at,
        }
    }

    /// Returns the node ID associated with the event, if any.
    pub fn node_id(&self) -> Option<NodeId> {
        match self {
            AuditEvent::NodeRegistered { node_id, .. } => Some(*node_id),
            AuditEvent::NodeDeregistered { node_id, .. } => Some(*node_id),
            AuditEvent::NodeStatusChanged { node_id, .. } => Some(*node_id),
            AuditEvent::TaskAssigned { node_id, .. } => Some(*node_id),
            AuditEvent::TaskCompleted { node_id, .. } => Some(*node_id),
            AuditEvent::TaskFailed { node_id, .. } => Some(*node_id),
            AuditEvent::ConfigChanged { node_id, .. } => Some(*node_id),
            AuditEvent::AlertTriggered { node_id, .. } => Some(*node_id),
            AuditEvent::AlertResolved { node_id, .. } => Some(*node_id),
            AuditEvent::MaintenanceStarted { node_id, .. } => Some(*node_id),
            AuditEvent::MaintenanceEnded { node_id, .. } => Some(*node_id),
            AuditEvent::SlaViolation { node_id, .. } => Some(*node_id),
        }
    }

    /// Returns the event type name.
    pub fn event_type(&self) -> &'static str {
        match self {
            AuditEvent::NodeRegistered { .. } => "node_registered",
            AuditEvent::NodeDeregistered { .. } => "node_deregistered",
            AuditEvent::NodeStatusChanged { .. } => "node_status_changed",
            AuditEvent::TaskAssigned { .. } => "task_assigned",
            AuditEvent::TaskCompleted { .. } => "task_completed",
            AuditEvent::TaskFailed { .. } => "task_failed",
            AuditEvent::ConfigChanged { .. } => "config_changed",
            AuditEvent::AlertTriggered { .. } => "alert_triggered",
            AuditEvent::AlertResolved { .. } => "alert_resolved",
            AuditEvent::MaintenanceStarted { .. } => "maintenance_started",
            AuditEvent::MaintenanceEnded { .. } => "maintenance_ended",
            AuditEvent::SlaViolation { .. } => "sla_violation",
        }
    }
}

/// Filter criteria for querying audit events.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// Filter by node ID.
    pub node_id: Option<NodeId>,
    /// Filter by event types.
    pub event_types: Vec<String>,
    /// Filter by start time (inclusive).
    pub start_time: Option<DateTime<Utc>>,
    /// Filter by end time (exclusive).
    pub end_time: Option<DateTime<Utc>>,
    /// Maximum number of results.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: Option<usize>,
}

impl AuditFilter {
    /// Creates a new empty filter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters by node ID.
    pub fn for_node(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Filters by event types.
    pub fn with_event_types(mut self, types: Vec<String>) -> Self {
        self.event_types = types;
        self
    }

    /// Filters by time range.
    pub fn in_time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    /// Filters events from the last duration.
    pub fn last_duration(mut self, duration: Duration) -> Self {
        self.start_time = Some(Utc::now() - duration);
        self.end_time = None;
        self
    }

    /// Limits results.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Offsets results for pagination.
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Checks if an event matches this filter.
    fn matches(&self, event: &AuditEvent) -> bool {
        // Check node ID
        if let Some(node_id) = self.node_id {
            if event.node_id() != Some(node_id) {
                return false;
            }
        }

        // Check event types
        if !self.event_types.is_empty()
            && !self.event_types.contains(&event.event_type().to_string())
        {
            return false;
        }

        // Check time range
        let timestamp = event.timestamp();
        if let Some(start) = self.start_time {
            if timestamp < start {
                return false;
            }
        }
        if let Some(end) = self.end_time {
            if timestamp >= end {
                return false;
            }
        }

        true
    }
}

/// Errors from audit operations.
#[derive(Debug, thiserror::Error)]
pub enum AuditError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Storage error: {0}")]
    Storage(String),
}

/// Trait for audit storage backends.
#[async_trait]
pub trait AuditStorage: Send + Sync {
    /// Appends an event to the audit log.
    async fn append(&self, event: AuditEvent) -> Result<(), AuditError>;

    /// Queries events matching the filter.
    async fn query(&self, filter: AuditFilter) -> Result<Vec<AuditEvent>, AuditError>;

    /// Returns the total number of events.
    async fn count(&self) -> Result<usize, AuditError>;
}

/// In-memory audit storage for testing and development.
pub struct InMemoryAuditStorage {
    events: Arc<RwLock<VecDeque<AuditEvent>>>,
    max_events: usize,
}

impl InMemoryAuditStorage {
    /// Creates a new in-memory storage with default capacity.
    pub fn new() -> Self {
        Self::with_capacity(100_000)
    }

    /// Creates a new in-memory storage with specified capacity.
    pub fn with_capacity(max_events: usize) -> Self {
        Self {
            events: Arc::new(RwLock::new(VecDeque::with_capacity(max_events))),
            max_events,
        }
    }
}

impl Default for InMemoryAuditStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AuditStorage for InMemoryAuditStorage {
    async fn append(&self, event: AuditEvent) -> Result<(), AuditError> {
        let mut events = self.events.write().await;
        events.push_back(event);

        // Trim if over capacity
        while events.len() > self.max_events {
            events.pop_front();
        }

        Ok(())
    }

    async fn query(&self, filter: AuditFilter) -> Result<Vec<AuditEvent>, AuditError> {
        let events = self.events.read().await;

        let mut results: Vec<_> = events
            .iter()
            .filter(|e| filter.matches(e))
            .cloned()
            .collect();

        // Apply offset
        if let Some(offset) = filter.offset {
            results = results.into_iter().skip(offset).collect();
        }

        // Apply limit
        if let Some(limit) = filter.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    async fn count(&self) -> Result<usize, AuditError> {
        let events = self.events.read().await;
        Ok(events.len())
    }
}

/// File-based audit storage for production use.
///
/// Stores events as newline-delimited JSON (NDJSON) for efficient appending
/// and streaming reads.
pub struct FileAuditStorage {
    path: PathBuf,
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl FileAuditStorage {
    /// Creates a new file-based storage.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Ensures the storage file exists.
    pub async fn init(&self) -> Result<(), AuditError> {
        if !self.path.exists() {
            if let Some(parent) = self.path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            File::create(&self.path).await?;
        }
        Ok(())
    }

    /// Rotates the log file if it exceeds the size limit.
    pub async fn rotate_if_needed(&self, max_size_bytes: u64) -> Result<bool, AuditError> {
        let metadata = tokio::fs::metadata(&self.path).await?;

        if metadata.len() > max_size_bytes {
            let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
            let rotated_path = self.path.with_extension(format!("log.{}", timestamp));

            let _lock = self.write_lock.lock().await;
            tokio::fs::rename(&self.path, &rotated_path).await?;
            File::create(&self.path).await?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait]
impl AuditStorage for FileAuditStorage {
    async fn append(&self, event: AuditEvent) -> Result<(), AuditError> {
        let _lock = self.write_lock.lock().await;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        let mut line = serde_json::to_string(&event)?;
        line.push('\n');

        file.write_all(line.as_bytes()).await?;
        file.flush().await?;

        Ok(())
    }

    async fn query(&self, filter: AuditFilter) -> Result<Vec<AuditEvent>, AuditError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut results = Vec::new();
        let mut skipped = 0;

        while let Some(line) = lines.next_line().await? {
            if line.is_empty() {
                continue;
            }

            let event: AuditEvent = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(_) => continue, // Skip malformed lines
            };

            if filter.matches(&event) {
                // Apply offset
                if let Some(offset) = filter.offset {
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }
                }

                results.push(event);

                // Apply limit
                if let Some(limit) = filter.limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    async fn count(&self) -> Result<usize, AuditError> {
        if !self.path.exists() {
            return Ok(0);
        }

        let file = File::open(&self.path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut count = 0;
        while lines.next_line().await?.is_some() {
            count += 1;
        }

        Ok(count)
    }
}

/// Main audit log interface.
///
/// Provides a high-level API for recording and querying audit events.
pub struct AuditLog {
    storage: Box<dyn AuditStorage>,
}

impl AuditLog {
    /// Creates a new audit log with the given storage backend.
    pub fn new(storage: Box<dyn AuditStorage>) -> Self {
        Self { storage }
    }

    /// Creates an audit log with in-memory storage.
    pub fn in_memory() -> Self {
        Self::new(Box::new(InMemoryAuditStorage::new()))
    }

    /// Creates an audit log with file-based storage.
    pub fn file_based(path: impl Into<PathBuf>) -> Self {
        Self::new(Box::new(FileAuditStorage::new(path)))
    }

    /// Records an audit event.
    pub async fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        self.storage.append(event).await
    }

    /// Records a node registration.
    pub async fn record_node_registered(
        &self,
        node_id: NodeId,
        by: impl Into<String>,
        hostname: impl Into<String>,
        datacenter: impl Into<String>,
    ) -> Result<(), AuditError> {
        self.record(AuditEvent::node_registered(
            node_id, by, hostname, datacenter,
        ))
        .await
    }

    /// Records a node deregistration.
    pub async fn record_node_deregistered(
        &self,
        node_id: NodeId,
        by: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<(), AuditError> {
        self.record(AuditEvent::node_deregistered(node_id, by, reason))
            .await
    }

    /// Records a task assignment.
    pub async fn record_task_assigned(
        &self,
        task_id: TaskId,
        node_id: NodeId,
    ) -> Result<(), AuditError> {
        self.record(AuditEvent::task_assigned(task_id, node_id))
            .await
    }

    /// Records a task completion.
    pub async fn record_task_completed(
        &self,
        task_id: TaskId,
        node_id: NodeId,
        duration_ms: u64,
    ) -> Result<(), AuditError> {
        self.record(AuditEvent::task_completed(task_id, node_id, duration_ms))
            .await
    }

    /// Records a task failure.
    pub async fn record_task_failed(
        &self,
        task_id: TaskId,
        node_id: NodeId,
        error: impl Into<String>,
        duration_ms: u64,
    ) -> Result<(), AuditError> {
        self.record(AuditEvent::task_failed(
            task_id,
            node_id,
            error,
            duration_ms,
        ))
        .await
    }

    /// Records a configuration change.
    pub async fn record_config_changed(
        &self,
        node_id: NodeId,
        field: impl Into<String>,
        old_value: impl Into<String>,
        new_value: impl Into<String>,
        by: impl Into<String>,
    ) -> Result<(), AuditError> {
        self.record(AuditEvent::config_changed(
            node_id, field, old_value, new_value, by,
        ))
        .await
    }

    /// Records an alert.
    pub async fn record_alert(&self, alert: &Alert) -> Result<(), AuditError> {
        self.record(AuditEvent::from_alert(alert)).await
    }

    /// Queries audit events.
    pub async fn query(&self, filter: AuditFilter) -> Result<Vec<AuditEvent>, AuditError> {
        self.storage.query(filter).await
    }

    /// Gets recent events for a node.
    pub async fn recent_events_for_node(
        &self,
        node_id: NodeId,
        limit: usize,
    ) -> Result<Vec<AuditEvent>, AuditError> {
        self.query(AuditFilter::new().for_node(node_id).limit(limit))
            .await
    }

    /// Gets events in a time range.
    pub async fn events_in_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<AuditEvent>, AuditError> {
        self.query(AuditFilter::new().in_time_range(start, end))
            .await
    }

    /// Gets task events for a node.
    pub async fn task_events_for_node(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<AuditEvent>, AuditError> {
        self.query(AuditFilter::new().for_node(node_id).with_event_types(vec![
            "task_assigned".to_string(),
            "task_completed".to_string(),
            "task_failed".to_string(),
        ]))
        .await
    }

    /// Gets alert events for a node.
    pub async fn alert_events_for_node(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<AuditEvent>, AuditError> {
        self.query(AuditFilter::new().for_node(node_id).with_event_types(vec![
            "alert_triggered".to_string(),
            "alert_resolved".to_string(),
        ]))
        .await
    }

    /// Returns the total number of events.
    pub async fn event_count(&self) -> Result<usize, AuditError> {
        self.storage.count().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_audit_event_creation() {
        let node_id = NodeId::new();
        let task_id = TaskId::new();

        let registered = AuditEvent::node_registered(node_id, "admin", "server-1", "us-east-1");
        assert_eq!(registered.event_type(), "node_registered");
        assert_eq!(registered.node_id(), Some(node_id));

        let assigned = AuditEvent::task_assigned(task_id, node_id);
        assert_eq!(assigned.event_type(), "task_assigned");
    }

    #[tokio::test]
    async fn test_in_memory_storage() {
        let storage = InMemoryAuditStorage::new();
        let node_id = NodeId::new();

        // Append events
        for i in 0..10 {
            let task_id = TaskId::new();
            storage
                .append(AuditEvent::task_assigned(task_id, node_id))
                .await
                .unwrap();
            storage
                .append(AuditEvent::task_completed(task_id, node_id, 100 + i))
                .await
                .unwrap();
        }

        assert_eq!(storage.count().await.unwrap(), 20);

        // Query for node
        let filter = AuditFilter::new().for_node(node_id);
        let results = storage.query(filter).await.unwrap();
        assert_eq!(results.len(), 20);

        // Query with limit
        let filter = AuditFilter::new().for_node(node_id).limit(5);
        let results = storage.query(filter).await.unwrap();
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_in_memory_storage_capacity() {
        let storage = InMemoryAuditStorage::with_capacity(10);
        let node_id = NodeId::new();

        // Add more events than capacity
        for _ in 0..20 {
            let task_id = TaskId::new();
            storage
                .append(AuditEvent::task_assigned(task_id, node_id))
                .await
                .unwrap();
        }

        // Should only have 10 events
        assert_eq!(storage.count().await.unwrap(), 10);
    }

    #[tokio::test]
    async fn test_audit_filter_by_event_type() {
        let storage = InMemoryAuditStorage::new();
        let node_id = NodeId::new();
        let task_id = TaskId::new();

        storage
            .append(AuditEvent::task_assigned(task_id, node_id))
            .await
            .unwrap();
        storage
            .append(AuditEvent::task_completed(task_id, node_id, 100))
            .await
            .unwrap();
        storage
            .append(AuditEvent::task_failed(task_id, node_id, "error", 50))
            .await
            .unwrap();

        let filter = AuditFilter::new().with_event_types(vec!["task_completed".to_string()]);
        let results = storage.query(filter).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].event_type(), "task_completed");
    }

    #[tokio::test]
    async fn test_audit_filter_by_time() {
        let storage = InMemoryAuditStorage::new();
        let node_id = NodeId::new();

        let now = Utc::now();

        // Add event
        let task_id = TaskId::new();
        storage
            .append(AuditEvent::task_assigned(task_id, node_id))
            .await
            .unwrap();

        // Query for events after now - should have 1
        let filter = AuditFilter::new()
            .in_time_range(now - Duration::seconds(1), now + Duration::seconds(10));
        let results = storage.query(filter).await.unwrap();
        assert_eq!(results.len(), 1);

        // Query for events before now - should have 0
        let filter =
            AuditFilter::new().in_time_range(now - Duration::hours(1), now - Duration::seconds(1));
        let results = storage.query(filter).await.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_file_storage() {
        let temp_dir = tempdir().unwrap();
        let log_path = temp_dir.path().join("audit.log");

        let storage = FileAuditStorage::new(&log_path);
        storage.init().await.unwrap();

        let node_id = NodeId::new();
        let task_id = TaskId::new();

        // Append events
        storage
            .append(AuditEvent::task_assigned(task_id, node_id))
            .await
            .unwrap();
        storage
            .append(AuditEvent::task_completed(task_id, node_id, 100))
            .await
            .unwrap();

        // Count
        assert_eq!(storage.count().await.unwrap(), 2);

        // Query
        let filter = AuditFilter::new().for_node(node_id);
        let results = storage.query(filter).await.unwrap();
        assert_eq!(results.len(), 2);

        // Verify file exists and has content
        let content: String = tokio::fs::read_to_string(&log_path).await.unwrap();
        assert!(content.contains("task_assigned"));
        assert!(content.contains("task_completed"));
    }

    #[tokio::test]
    async fn test_audit_log_high_level_api() {
        let audit_log = AuditLog::in_memory();
        let node_id = NodeId::new();
        let task_id = TaskId::new();

        // Record events using high-level API
        audit_log
            .record_node_registered(node_id, "admin", "server-1", "us-east-1")
            .await
            .unwrap();
        audit_log
            .record_task_assigned(task_id, node_id)
            .await
            .unwrap();
        audit_log
            .record_task_completed(task_id, node_id, 150)
            .await
            .unwrap();
        audit_log
            .record_config_changed(node_id, "sla_tier", "Standard", "Production", "admin")
            .await
            .unwrap();

        // Query using high-level API
        let task_events = audit_log.task_events_for_node(node_id).await.unwrap();
        assert_eq!(task_events.len(), 2);

        let recent = audit_log.recent_events_for_node(node_id, 2).await.unwrap();
        assert_eq!(recent.len(), 2);

        assert_eq!(audit_log.event_count().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn test_audit_filter_pagination() {
        let storage = InMemoryAuditStorage::new();
        let node_id = NodeId::new();

        // Add 20 events
        for _ in 0..20 {
            let task_id = TaskId::new();
            storage
                .append(AuditEvent::task_assigned(task_id, node_id))
                .await
                .unwrap();
        }

        // Get first page
        let filter = AuditFilter::new().limit(5).offset(0);
        let page1 = storage.query(filter).await.unwrap();
        assert_eq!(page1.len(), 5);

        // Get second page
        let filter = AuditFilter::new().limit(5).offset(5);
        let page2 = storage.query(filter).await.unwrap();
        assert_eq!(page2.len(), 5);

        // Verify different events
        assert_ne!(
            page1.first().unwrap().timestamp(),
            page2.first().unwrap().timestamp()
        );
    }

    #[test]
    fn test_audit_event_serialization() {
        let node_id = NodeId::new();
        let event = AuditEvent::node_registered(node_id, "admin", "server-1", "us-east-1");

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("node_registered"));

        let deserialized: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.event_type(), "node_registered");
    }
}
