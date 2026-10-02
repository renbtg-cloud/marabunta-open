// Marabunta - Licensed under the MIT License.
//! Data locality tracking and scoring for task scheduling
//!
//! This module provides infrastructure for tracking where data lives across the cluster
//! and scoring workers based on data locality to minimize network transfer.
//!
//! # Overview
//!
//! Data locality is crucial for distributed computing performance. Tasks that can run
//! on workers that already have their input data avoid costly network transfers.
//!
//! # Example
//!
//! ```rust
//! use marabunta_compute::placement::locality::{
//!     DataId, DataLocation, LocalityRegistry, LocalityPreference, DataRef,
//!     LocalityScorer, LocalityConfig,
//! };
//! use marabunta_compute::common::types::WorkerId;
//!
//! // Create a locality registry
//! let mut registry = LocalityRegistry::new();
//!
//! // Register data location
//! let data_id = DataId::new("dataset-001");
//! let worker1 = WorkerId::new();
//! let worker2 = WorkerId::new();
//!
//! registry.register_data(DataLocation {
//!     data_id: data_id.clone(),
//!     node_ids: vec![worker1, worker2],
//!     size_bytes: 1024 * 1024 * 100, // 100 MB
//!     replication_factor: 2,
//! });
//!
//! // Score workers by locality
//! let scorer = LocalityScorer::new(LocalityConfig::default());
//! let data_refs = vec![DataRef {
//!     data_id,
//!     preference: LocalityPreference::Preferred,
//! }];
//!
//! let score = scorer.score_worker(&worker1, &data_refs, &registry);
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

use crate::common::types::WorkerId;

/// Unique identifier for a data item
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataId(pub String);

impl DataId {
    /// Create a new data ID
    pub fn new(id: impl Into<String>) -> Self {
        DataId(id.into())
    }

    /// Generate a random data ID
    pub fn random() -> Self {
        DataId(Uuid::new_v4().to_string())
    }

    /// Create a data ID from a path or URI
    pub fn from_path(path: &str) -> Self {
        // Use deterministic ID based on path for consistent lookups
        DataId(Uuid::new_v5(&Uuid::NAMESPACE_URL, path.as_bytes()).to_string())
    }
}

impl fmt::Display for DataId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for DataId {
    fn from(s: &str) -> Self {
        DataId(s.to_string())
    }
}

impl From<String> for DataId {
    fn from(s: String) -> Self {
        DataId(s)
    }
}

/// Error types for locality operations
#[derive(Error, Debug, Clone)]
pub enum LocalityError {
    #[error("Data not found: {0}")]
    DataNotFound(String),
    #[error("Worker not found: {0}")]
    WorkerNotFound(String),
    #[error("Invalid locality configuration: {0}")]
    InvalidConfig(String),
    #[error("Data registration failed: {0}")]
    RegistrationFailed(String),
}

/// Locality preference for data access
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum LocalityPreference {
    /// Data must be local - task cannot run without it
    Required,
    /// Prefer local data but can transfer if needed
    #[default]
    Preferred,
    /// No locality preference - data can come from anywhere
    Any,
}


impl LocalityPreference {
    /// Get the weight multiplier for this preference level
    pub fn weight(&self) -> f64 {
        match self {
            LocalityPreference::Required => 1.0,
            LocalityPreference::Preferred => 0.8,
            LocalityPreference::Any => 0.0,
        }
    }
}

/// Reference to data needed by a task
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataRef {
    /// ID of the data item
    pub data_id: DataId,
    /// Locality preference for this data
    pub preference: LocalityPreference,
    /// Expected size of the data in bytes (for transfer cost estimation)
    pub estimated_size_bytes: Option<u64>,
}

impl DataRef {
    /// Create a new data reference
    pub fn new(data_id: impl Into<DataId>) -> Self {
        DataRef {
            data_id: data_id.into(),
            preference: LocalityPreference::default(),
            estimated_size_bytes: None,
        }
    }

    /// Create a required data reference
    pub fn required(data_id: impl Into<DataId>) -> Self {
        DataRef {
            data_id: data_id.into(),
            preference: LocalityPreference::Required,
            estimated_size_bytes: None,
        }
    }

    /// Create a preferred data reference
    pub fn preferred(data_id: impl Into<DataId>) -> Self {
        DataRef {
            data_id: data_id.into(),
            preference: LocalityPreference::Preferred,
            estimated_size_bytes: None,
        }
    }

    /// Create an any-locality data reference
    pub fn any(data_id: impl Into<DataId>) -> Self {
        DataRef {
            data_id: data_id.into(),
            preference: LocalityPreference::Any,
            estimated_size_bytes: None,
        }
    }

    /// Set the estimated size
    pub fn with_size(mut self, size_bytes: u64) -> Self {
        self.estimated_size_bytes = Some(size_bytes);
        self
    }
}

/// Location information for a data item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataLocation {
    /// Unique identifier for this data
    pub data_id: DataId,
    /// Workers that have this data locally
    pub node_ids: Vec<WorkerId>,
    /// Size of the data in bytes
    pub size_bytes: u64,
    /// Replication factor (number of copies)
    pub replication_factor: u32,
}

impl DataLocation {
    /// Create a new data location entry
    pub fn new(data_id: impl Into<DataId>, size_bytes: u64) -> Self {
        DataLocation {
            data_id: data_id.into(),
            node_ids: Vec::new(),
            size_bytes,
            replication_factor: 1,
        }
    }

    /// Add a node that has this data
    pub fn add_node(&mut self, worker_id: WorkerId) {
        if !self.node_ids.contains(&worker_id) {
            self.node_ids.push(worker_id);
            self.replication_factor = self.node_ids.len() as u32;
        }
    }

    /// Remove a node from this data's location list
    pub fn remove_node(&mut self, worker_id: &WorkerId) {
        self.node_ids.retain(|id| id != worker_id);
        self.replication_factor = self.node_ids.len() as u32;
    }

    /// Check if a worker has this data
    pub fn has_node(&self, worker_id: &WorkerId) -> bool {
        self.node_ids.contains(worker_id)
    }

    /// Check if data is available (has at least one node)
    pub fn is_available(&self) -> bool {
        !self.node_ids.is_empty()
    }
}

/// Extended location info with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataLocationInfo {
    /// Basic location information
    pub location: DataLocation,
    /// When this data was first registered
    pub created_at: DateTime<Utc>,
    /// Last time the location was updated
    pub updated_at: DateTime<Utc>,
    /// Data type/format hint
    pub data_type: Option<String>,
    /// Whether data is marked as hot (frequently accessed)
    pub is_hot: bool,
    /// Access count for this data
    pub access_count: u64,
}

impl DataLocationInfo {
    /// Create new location info
    pub fn new(location: DataLocation) -> Self {
        let now = Utc::now();
        DataLocationInfo {
            location,
            created_at: now,
            updated_at: now,
            data_type: None,
            is_hot: false,
            access_count: 0,
        }
    }

    /// Mark this data as accessed
    pub fn record_access(&mut self) {
        self.access_count += 1;
        self.updated_at = Utc::now();
    }
}

/// Registry for tracking data locations across the cluster
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalityRegistry {
    /// Data ID to location mapping
    locations: HashMap<DataId, DataLocationInfo>,
    /// Worker to data mapping (reverse index)
    worker_data: HashMap<WorkerId, HashSet<DataId>>,
    /// Total data size tracked
    total_size_bytes: u64,
}

impl LocalityRegistry {
    /// Create a new empty locality registry
    pub fn new() -> Self {
        LocalityRegistry {
            locations: HashMap::new(),
            worker_data: HashMap::new(),
            total_size_bytes: 0,
        }
    }

    /// Register a data location
    pub fn register_data(&mut self, location: DataLocation) {
        let data_id = location.data_id.clone();
        let size = location.size_bytes;

        // Update worker index
        for worker_id in &location.node_ids {
            self.worker_data
                .entry(*worker_id)
                .or_default()
                .insert(data_id.clone());
        }

        // Insert or update location
        if let Some(existing) = self.locations.get_mut(&data_id) {
            self.total_size_bytes -= existing.location.size_bytes;
            existing.location = location;
            existing.updated_at = Utc::now();
            self.total_size_bytes += size;
        } else {
            self.locations
                .insert(data_id, DataLocationInfo::new(location));
            self.total_size_bytes += size;
        }
    }

    /// Remove a data entry
    pub fn unregister_data(&mut self, data_id: &DataId) -> Option<DataLocationInfo> {
        if let Some(info) = self.locations.remove(data_id) {
            // Clean up worker index
            for worker_id in &info.location.node_ids {
                if let Some(data_set) = self.worker_data.get_mut(worker_id) {
                    data_set.remove(data_id);
                }
            }
            self.total_size_bytes -= info.location.size_bytes;
            Some(info)
        } else {
            None
        }
    }

    /// Add a worker to a data's location list
    pub fn add_data_node(
        &mut self,
        data_id: &DataId,
        worker_id: WorkerId,
    ) -> Result<(), LocalityError> {
        let info = self
            .locations
            .get_mut(data_id)
            .ok_or_else(|| LocalityError::DataNotFound(data_id.to_string()))?;

        info.location.add_node(worker_id);
        info.updated_at = Utc::now();

        self.worker_data
            .entry(worker_id)
            .or_default()
            .insert(data_id.clone());

        Ok(())
    }

    /// Remove a worker from a data's location list
    pub fn remove_data_node(
        &mut self,
        data_id: &DataId,
        worker_id: &WorkerId,
    ) -> Result<(), LocalityError> {
        let info = self
            .locations
            .get_mut(data_id)
            .ok_or_else(|| LocalityError::DataNotFound(data_id.to_string()))?;

        info.location.remove_node(worker_id);
        info.updated_at = Utc::now();

        if let Some(data_set) = self.worker_data.get_mut(worker_id) {
            data_set.remove(data_id);
        }

        Ok(())
    }

    /// Get location info for a data item
    pub fn get_location(&self, data_id: &DataId) -> Option<&DataLocationInfo> {
        self.locations.get(data_id)
    }

    /// Get mutable location info
    pub fn get_location_mut(&mut self, data_id: &DataId) -> Option<&mut DataLocationInfo> {
        self.locations.get_mut(data_id)
    }

    /// Get all data located on a worker
    pub fn get_worker_data(&self, worker_id: &WorkerId) -> HashSet<DataId> {
        self.worker_data.get(worker_id).cloned().unwrap_or_default()
    }

    /// Check if a worker has specific data
    pub fn worker_has_data(&self, worker_id: &WorkerId, data_id: &DataId) -> bool {
        self.worker_data
            .get(worker_id)
            .map(|data| data.contains(data_id))
            .unwrap_or(false)
    }

    /// Find workers that have the specified data
    pub fn find_workers_with_data(&self, data_id: &DataId) -> Vec<WorkerId> {
        self.locations
            .get(data_id)
            .map(|info| info.location.node_ids.clone())
            .unwrap_or_default()
    }

    /// Find the best worker for a set of data references
    pub fn find_best_workers(&self, data_refs: &[DataRef]) -> Vec<(WorkerId, usize)> {
        let mut worker_scores: HashMap<WorkerId, usize> = HashMap::new();

        for data_ref in data_refs {
            if let Some(info) = self.locations.get(&data_ref.data_id) {
                for worker_id in &info.location.node_ids {
                    *worker_scores.entry(*worker_id).or_default() += 1;
                }
            }
        }

        let mut results: Vec<_> = worker_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by score descending
        results
    }

    /// Handle worker failure - remove worker from all data locations
    pub fn handle_worker_failure(&mut self, worker_id: &WorkerId) {
        if let Some(data_ids) = self.worker_data.remove(worker_id) {
            for data_id in data_ids {
                if let Some(info) = self.locations.get_mut(&data_id) {
                    info.location.remove_node(worker_id);
                    info.updated_at = Utc::now();
                }
            }
        }
    }

    /// Get total number of tracked data items
    pub fn data_count(&self) -> usize {
        self.locations.len()
    }

    /// Get total size of all tracked data
    pub fn total_size(&self) -> u64 {
        self.total_size_bytes
    }

    /// Get all data locations
    pub fn all_locations(&self) -> impl Iterator<Item = (&DataId, &DataLocationInfo)> {
        self.locations.iter()
    }

    /// Record an access to data
    pub fn record_access(&mut self, data_id: &DataId) {
        if let Some(info) = self.locations.get_mut(data_id) {
            info.record_access();
        }
    }

    /// Get hot data (frequently accessed)
    pub fn get_hot_data(&self, threshold: u64) -> Vec<&DataLocationInfo> {
        self.locations
            .values()
            .filter(|info| info.access_count >= threshold)
            .collect()
    }
}

/// Configuration for locality scoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalityConfig {
    /// Weight for locality in overall placement score (0.0-1.0)
    pub locality_weight: f64,
    /// Cost per byte of network transfer (for estimation)
    pub transfer_cost_per_byte: f64,
    /// Base network latency cost in milliseconds
    pub base_latency_cost_ms: f64,
    /// Whether to consider data size in scoring
    pub consider_size: bool,
    /// Minimum locality score to accept (0.0-1.0)
    pub min_locality_score: f64,
    /// Whether strict locality is required for Required preference
    pub strict_required: bool,
}

impl Default for LocalityConfig {
    fn default() -> Self {
        LocalityConfig {
            locality_weight: 0.5,
            transfer_cost_per_byte: 0.00001, // 10 microseconds per byte
            base_latency_cost_ms: 1.0,
            consider_size: true,
            min_locality_score: 0.0,
            strict_required: true,
        }
    }
}

impl LocalityConfig {
    /// Create a high-locality configuration
    pub fn high_locality() -> Self {
        LocalityConfig {
            locality_weight: 0.8,
            min_locality_score: 0.3,
            ..Default::default()
        }
    }

    /// Create a low-locality (throughput-focused) configuration
    pub fn low_locality() -> Self {
        LocalityConfig {
            locality_weight: 0.2,
            min_locality_score: 0.0,
            ..Default::default()
        }
    }
}

/// Scorer for calculating worker locality scores
#[derive(Debug, Clone)]
pub struct LocalityScorer {
    config: LocalityConfig,
}

impl LocalityScorer {
    /// Create a new locality scorer with the given config
    pub fn new(config: LocalityConfig) -> Self {
        LocalityScorer { config }
    }

    /// Score a worker based on data locality
    ///
    /// Returns a score between 0.0 and 1.0, where:
    /// - 1.0 means all required/preferred data is local
    /// - 0.0 means no data is local
    pub fn score_worker(
        &self,
        worker_id: &WorkerId,
        data_refs: &[DataRef],
        registry: &LocalityRegistry,
    ) -> LocalityScore {
        if data_refs.is_empty() {
            return LocalityScore::perfect();
        }

        let mut total_weight = 0.0;
        let mut local_weight = 0.0;
        let mut missing_required = false;
        let mut total_transfer_size = 0u64;
        let mut local_data_count = 0usize;

        for data_ref in data_refs {
            let weight = data_ref.preference.weight();
            total_weight += weight;

            let is_local = registry.worker_has_data(worker_id, &data_ref.data_id);

            if is_local {
                local_weight += weight;
                local_data_count += 1;
            } else {
                // Check if Required data is missing
                if data_ref.preference == LocalityPreference::Required {
                    missing_required = true;
                }

                // Add to transfer size
                if let Some(size) = data_ref.estimated_size_bytes {
                    total_transfer_size += size;
                } else if let Some(info) = registry.get_location(&data_ref.data_id) {
                    total_transfer_size += info.location.size_bytes;
                }
            }
        }

        // Calculate base score
        let base_score = if total_weight > 0.0 {
            local_weight / total_weight
        } else {
            1.0
        };

        // Calculate transfer cost
        let transfer_cost = if self.config.consider_size {
            total_transfer_size as f64 * self.config.transfer_cost_per_byte
                + if total_transfer_size > 0 {
                    self.config.base_latency_cost_ms
                } else {
                    0.0
                }
        } else {
            0.0
        };

        // Determine if worker is eligible
        let eligible = if self.config.strict_required && missing_required {
            false
        } else { base_score >= self.config.min_locality_score };

        LocalityScore {
            score: base_score,
            local_data_count,
            total_data_count: data_refs.len(),
            transfer_size_bytes: total_transfer_size,
            estimated_transfer_cost: transfer_cost,
            missing_required,
            eligible,
        }
    }

    /// Get the weighted locality score for combining with other factors
    pub fn weighted_score(&self, score: &LocalityScore) -> f64 {
        score.score * self.config.locality_weight
    }

    /// Get the config
    pub fn config(&self) -> &LocalityConfig {
        &self.config
    }
}

/// Result of locality scoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalityScore {
    /// Base locality score (0.0-1.0)
    pub score: f64,
    /// Number of data items that are local
    pub local_data_count: usize,
    /// Total number of data items requested
    pub total_data_count: usize,
    /// Total bytes that need to be transferred
    pub transfer_size_bytes: u64,
    /// Estimated transfer cost (time in ms)
    pub estimated_transfer_cost: f64,
    /// Whether any Required data is missing
    pub missing_required: bool,
    /// Whether the worker is eligible based on locality constraints
    pub eligible: bool,
}

impl LocalityScore {
    /// Create a perfect locality score (all data local)
    pub fn perfect() -> Self {
        LocalityScore {
            score: 1.0,
            local_data_count: 0,
            total_data_count: 0,
            transfer_size_bytes: 0,
            estimated_transfer_cost: 0.0,
            missing_required: false,
            eligible: true,
        }
    }

    /// Create a zero locality score (no data local)
    pub fn zero(total_data_count: usize) -> Self {
        LocalityScore {
            score: 0.0,
            local_data_count: 0,
            total_data_count,
            transfer_size_bytes: 0,
            estimated_transfer_cost: 0.0,
            missing_required: false,
            eligible: true,
        }
    }

    /// Check if all data is local
    pub fn is_perfect(&self) -> bool {
        self.score >= 1.0 - f64::EPSILON
    }

    /// Get the percentage of data that is local
    pub fn local_percentage(&self) -> f64 {
        if self.total_data_count == 0 {
            100.0
        } else {
            (self.local_data_count as f64 / self.total_data_count as f64) * 100.0
        }
    }
}

/// Data placement hint for tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPlacementHint {
    /// Data references for this task
    pub data_refs: Vec<DataRef>,
    /// Preferred workers (from previous execution or user hint)
    pub preferred_workers: Vec<WorkerId>,
    /// Avoid these workers if possible
    pub avoid_workers: Vec<WorkerId>,
    /// Maximum acceptable transfer size
    pub max_transfer_bytes: Option<u64>,
}

impl DataPlacementHint {
    /// Create a new empty placement hint
    pub fn new() -> Self {
        DataPlacementHint {
            data_refs: Vec::new(),
            preferred_workers: Vec::new(),
            avoid_workers: Vec::new(),
            max_transfer_bytes: None,
        }
    }

    /// Create a placement hint from data references
    pub fn from_data_refs(data_refs: Vec<DataRef>) -> Self {
        DataPlacementHint {
            data_refs,
            preferred_workers: Vec::new(),
            avoid_workers: Vec::new(),
            max_transfer_bytes: None,
        }
    }

    /// Add a data reference
    pub fn add_data_ref(&mut self, data_ref: DataRef) {
        self.data_refs.push(data_ref);
    }

    /// Add a preferred worker
    pub fn prefer_worker(&mut self, worker_id: WorkerId) {
        if !self.preferred_workers.contains(&worker_id) {
            self.preferred_workers.push(worker_id);
        }
    }

    /// Add a worker to avoid
    pub fn avoid_worker(&mut self, worker_id: WorkerId) {
        if !self.avoid_workers.contains(&worker_id) {
            self.avoid_workers.push(worker_id);
        }
    }

    /// Set maximum transfer size
    pub fn with_max_transfer(mut self, max_bytes: u64) -> Self {
        self.max_transfer_bytes = Some(max_bytes);
        self
    }

    /// Check if a worker should be avoided
    pub fn should_avoid(&self, worker_id: &WorkerId) -> bool {
        self.avoid_workers.contains(worker_id)
    }

    /// Check if a worker is preferred
    pub fn is_preferred(&self, worker_id: &WorkerId) -> bool {
        self.preferred_workers.contains(worker_id)
    }
}

impl Default for DataPlacementHint {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about data movement in the cluster
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalityStats {
    /// Total tasks scheduled
    pub total_tasks: u64,
    /// Tasks scheduled with perfect locality
    pub perfect_locality_tasks: u64,
    /// Tasks with partial locality
    pub partial_locality_tasks: u64,
    /// Tasks with no locality (all data remote)
    pub no_locality_tasks: u64,
    /// Total bytes transferred
    pub total_bytes_transferred: u64,
    /// Total bytes saved by locality
    pub bytes_saved_by_locality: u64,
    /// Average locality score
    pub average_locality_score: f64,
}

impl LocalityStats {
    /// Create new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a task scheduling decision
    pub fn record_task(&mut self, score: &LocalityScore, total_data_size: u64) {
        self.total_tasks += 1;

        if score.is_perfect() {
            self.perfect_locality_tasks += 1;
            self.bytes_saved_by_locality += total_data_size;
        } else if score.local_data_count > 0 {
            self.partial_locality_tasks += 1;
            // Estimate saved bytes based on local ratio
            let saved = (total_data_size as f64 * score.score) as u64;
            self.bytes_saved_by_locality += saved;
        } else {
            self.no_locality_tasks += 1;
        }

        self.total_bytes_transferred += score.transfer_size_bytes;

        // Update average (running average)
        let n = self.total_tasks as f64;
        self.average_locality_score = (self.average_locality_score * (n - 1.0) + score.score) / n;
    }

    /// Get the percentage of tasks with perfect locality
    pub fn perfect_locality_percentage(&self) -> f64 {
        if self.total_tasks == 0 {
            0.0
        } else {
            (self.perfect_locality_tasks as f64 / self.total_tasks as f64) * 100.0
        }
    }

    /// Get the data locality effectiveness ratio
    pub fn effectiveness_ratio(&self) -> f64 {
        let total_touched = self.bytes_saved_by_locality + self.total_bytes_transferred;
        if total_touched == 0 {
            1.0
        } else {
            self.bytes_saved_by_locality as f64 / total_touched as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_registry() -> LocalityRegistry {
        let mut registry = LocalityRegistry::new();

        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();
        let worker3 = WorkerId::new();

        // Data 1 on workers 1 and 2
        let mut loc1 = DataLocation::new("data-1", 1024 * 1024 * 100); // 100 MB
        loc1.add_node(worker1);
        loc1.add_node(worker2);
        registry.register_data(loc1);

        // Data 2 only on worker 2
        let mut loc2 = DataLocation::new("data-2", 1024 * 1024 * 50); // 50 MB
        loc2.add_node(worker2);
        registry.register_data(loc2);

        // Data 3 on worker 3
        let mut loc3 = DataLocation::new("data-3", 1024 * 1024 * 200); // 200 MB
        loc3.add_node(worker3);
        registry.register_data(loc3);

        registry
    }

    #[test]
    fn test_data_id_creation() {
        let id1 = DataId::new("test-data");
        let id2 = DataId::from_path("/data/file.parquet");
        let id3 = DataId::random();

        assert_eq!(id1.0, "test-data");
        assert_ne!(id2.0, id3.0);
    }

    #[test]
    fn test_data_location() {
        let mut loc = DataLocation::new("test", 1024);
        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();

        assert!(!loc.is_available());

        loc.add_node(worker1);
        assert!(loc.is_available());
        assert!(loc.has_node(&worker1));
        assert!(!loc.has_node(&worker2));
        assert_eq!(loc.replication_factor, 1);

        loc.add_node(worker2);
        assert_eq!(loc.replication_factor, 2);

        loc.remove_node(&worker1);
        assert!(!loc.has_node(&worker1));
        assert_eq!(loc.replication_factor, 1);
    }

    #[test]
    fn test_registry_basic_operations() {
        let mut registry = LocalityRegistry::new();
        let worker1 = WorkerId::new();
        let data_id = DataId::new("test-data");

        // Register data
        let mut loc = DataLocation::new(data_id.clone(), 1024);
        loc.add_node(worker1);
        registry.register_data(loc);

        // Check lookup
        assert!(registry.get_location(&data_id).is_some());
        assert!(registry.worker_has_data(&worker1, &data_id));
        assert_eq!(registry.data_count(), 1);
        assert_eq!(registry.total_size(), 1024);

        // Unregister
        let removed = registry.unregister_data(&data_id);
        assert!(removed.is_some());
        assert!(registry.get_location(&data_id).is_none());
        assert_eq!(registry.data_count(), 0);
    }

    #[test]
    fn test_find_workers_with_data() {
        let registry = create_test_registry();

        let workers = registry.find_workers_with_data(&DataId::new("data-1"));
        assert_eq!(workers.len(), 2);
    }

    #[test]
    fn test_find_best_workers() {
        let mut registry = LocalityRegistry::new();
        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();

        // Worker 1 has data-1 only
        let mut loc1 = DataLocation::new("data-1", 100);
        loc1.add_node(worker1);
        registry.register_data(loc1);

        // Worker 2 has both data-1 and data-2
        registry
            .add_data_node(&DataId::new("data-1"), worker2)
            .unwrap();

        let mut loc2 = DataLocation::new("data-2", 100);
        loc2.add_node(worker2);
        registry.register_data(loc2);

        // Find best worker for both data items
        let data_refs = vec![DataRef::new("data-1"), DataRef::new("data-2")];

        let best = registry.find_best_workers(&data_refs);
        assert!(!best.is_empty());
        assert_eq!(best[0].0, worker2); // Worker 2 should score higher
        assert_eq!(best[0].1, 2); // Has both data items
    }

    #[test]
    fn test_handle_worker_failure() {
        let mut registry = LocalityRegistry::new();
        let worker1 = WorkerId::new();
        let data_id = DataId::new("test-data");

        let mut loc = DataLocation::new(data_id.clone(), 1024);
        loc.add_node(worker1);
        registry.register_data(loc);

        assert!(registry.worker_has_data(&worker1, &data_id));

        registry.handle_worker_failure(&worker1);

        assert!(!registry.worker_has_data(&worker1, &data_id));
        // Data should still exist but with no nodes
        let info = registry.get_location(&data_id).unwrap();
        assert!(!info.location.is_available());
    }

    #[test]
    fn test_locality_scorer_perfect_locality() {
        let mut registry = LocalityRegistry::new();
        let worker1 = WorkerId::new();
        let data_id = DataId::new("test-data");

        let mut loc = DataLocation::new(data_id.clone(), 1024);
        loc.add_node(worker1);
        registry.register_data(loc);

        let scorer = LocalityScorer::new(LocalityConfig::default());
        let data_refs = vec![DataRef::new(data_id)];

        let score = scorer.score_worker(&worker1, &data_refs, &registry);

        assert!(score.is_perfect());
        assert_eq!(score.local_data_count, 1);
        assert_eq!(score.transfer_size_bytes, 0);
        assert!(score.eligible);
    }

    #[test]
    fn test_locality_scorer_no_locality() {
        let registry = LocalityRegistry::new();
        let worker1 = WorkerId::new();
        let data_id = DataId::new("test-data");

        let scorer = LocalityScorer::new(LocalityConfig::default());
        let data_refs = vec![DataRef::new(data_id)];

        let score = scorer.score_worker(&worker1, &data_refs, &registry);

        assert_eq!(score.score, 0.0);
        assert_eq!(score.local_data_count, 0);
        assert!(score.eligible); // Still eligible since not Required
    }

    #[test]
    fn test_locality_scorer_required_missing() {
        let registry = LocalityRegistry::new();
        let worker1 = WorkerId::new();
        let data_id = DataId::new("test-data");

        let config = LocalityConfig {
            strict_required: true,
            ..Default::default()
        };
        let scorer = LocalityScorer::new(config);
        let data_refs = vec![DataRef::required(data_id)];

        let score = scorer.score_worker(&worker1, &data_refs, &registry);

        assert!(score.missing_required);
        assert!(!score.eligible);
    }

    #[test]
    fn test_locality_scorer_partial_locality() {
        let mut registry = LocalityRegistry::new();
        let worker1 = WorkerId::new();

        let mut loc = DataLocation::new("data-1", 1024);
        loc.add_node(worker1);
        registry.register_data(loc);

        // data-2 not on worker1
        registry.register_data(DataLocation::new("data-2", 1024));

        let scorer = LocalityScorer::new(LocalityConfig::default());
        let data_refs = vec![DataRef::new("data-1"), DataRef::new("data-2")];

        let score = scorer.score_worker(&worker1, &data_refs, &registry);

        assert!(score.score > 0.0 && score.score < 1.0);
        assert_eq!(score.local_data_count, 1);
        assert_eq!(score.total_data_count, 2);
    }

    #[test]
    fn test_locality_preference_weights() {
        assert_eq!(LocalityPreference::Required.weight(), 1.0);
        assert_eq!(LocalityPreference::Preferred.weight(), 0.8);
        assert_eq!(LocalityPreference::Any.weight(), 0.0);
    }

    #[test]
    fn test_data_placement_hint() {
        let mut hint = DataPlacementHint::new();
        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();

        hint.add_data_ref(DataRef::new("data-1"));
        hint.prefer_worker(worker1);
        hint.avoid_worker(worker2);

        assert!(hint.is_preferred(&worker1));
        assert!(!hint.is_preferred(&worker2));
        assert!(!hint.should_avoid(&worker1));
        assert!(hint.should_avoid(&worker2));
        assert_eq!(hint.data_refs.len(), 1);
    }

    #[test]
    fn test_locality_stats() {
        let mut stats = LocalityStats::new();

        // Perfect locality task
        let perfect_score = LocalityScore {
            score: 1.0,
            local_data_count: 2,
            total_data_count: 2,
            transfer_size_bytes: 0,
            estimated_transfer_cost: 0.0,
            missing_required: false,
            eligible: true,
        };
        stats.record_task(&perfect_score, 1000);

        // Partial locality task
        let partial_score = LocalityScore {
            score: 0.5,
            local_data_count: 1,
            total_data_count: 2,
            transfer_size_bytes: 500,
            estimated_transfer_cost: 5.0,
            missing_required: false,
            eligible: true,
        };
        stats.record_task(&partial_score, 1000);

        // No locality task
        let no_score = LocalityScore::zero(2);
        stats.record_task(&no_score, 1000);

        assert_eq!(stats.total_tasks, 3);
        assert_eq!(stats.perfect_locality_tasks, 1);
        assert_eq!(stats.partial_locality_tasks, 1);
        assert_eq!(stats.no_locality_tasks, 1);
    }

    #[test]
    fn test_locality_config_presets() {
        let high = LocalityConfig::high_locality();
        let low = LocalityConfig::low_locality();

        assert!(high.locality_weight > low.locality_weight);
        assert!(high.min_locality_score > low.min_locality_score);
    }

    #[test]
    fn test_data_ref_builders() {
        let required = DataRef::required("data-1");
        let preferred = DataRef::preferred("data-2");
        let any = DataRef::any("data-3");
        let with_size = DataRef::new("data-4").with_size(1024);

        assert_eq!(required.preference, LocalityPreference::Required);
        assert_eq!(preferred.preference, LocalityPreference::Preferred);
        assert_eq!(any.preference, LocalityPreference::Any);
        assert_eq!(with_size.estimated_size_bytes, Some(1024));
    }

    #[test]
    fn test_record_access() {
        let mut registry = LocalityRegistry::new();
        let data_id = DataId::new("test-data");

        registry.register_data(DataLocation::new(data_id.clone(), 1024));

        let initial_count = registry.get_location(&data_id).unwrap().access_count;

        registry.record_access(&data_id);
        registry.record_access(&data_id);

        let final_count = registry.get_location(&data_id).unwrap().access_count;
        assert_eq!(final_count, initial_count + 2);
    }

    #[test]
    fn test_hot_data_detection() {
        let mut registry = LocalityRegistry::new();

        let hot_data = DataId::new("hot-data");
        let cold_data = DataId::new("cold-data");

        registry.register_data(DataLocation::new(hot_data.clone(), 1024));
        registry.register_data(DataLocation::new(cold_data.clone(), 1024));

        // Access hot data many times
        for _ in 0..10 {
            registry.record_access(&hot_data);
        }
        // Access cold data once
        registry.record_access(&cold_data);

        let hot_items = registry.get_hot_data(5);
        assert_eq!(hot_items.len(), 1);
        assert_eq!(hot_items[0].location.data_id, hot_data);
    }
}
