// Marabunta - Licensed under the MIT License.
//! Auto-detected ephemeral node groups
//!
//! This module provides automatic detection of node groups based on various
//! properties like region, hardware, network proximity, and load.
//!
//! # Overview
//!
//! Auto-groups are ephemeral groups that are automatically created and updated
//! based on the current state of nodes in the cluster. They complement the
//! static groups defined in the groups module.
//!
//! ```rust
//! use marabunta_compute::placement::auto_groups::{
//!     AutoGroupRegistry, RegionDetector, HardwareDetector,
//!     AutoGroupConfig, DetectorConfig,
//! };
//! use marabunta_compute::placement::groups::{NodeRegistry, GroupRegistry};
//!
//! // Create registries
//! let mut nodes = NodeRegistry::new();
//! let mut groups = GroupRegistry::new();
//! let config = AutoGroupConfig::default();
//!
//! // Create auto-group registry with detectors
//! let mut auto_registry = AutoGroupRegistry::new(config);
//! auto_registry.register_detector(Box::new(RegionDetector::new()));
//! auto_registry.register_detector(Box::new(HardwareDetector::new()));
//!
//! // Run detection and merge into groups
//! auto_registry.run_detection(&nodes);
//! auto_registry.merge_into(&mut groups, &nodes);
//! ```

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use thiserror::Error;

use super::groups::{
    Group, GroupId, GroupMembership, GroupMetadata, GroupRegistry, GroupType, NodeId, NodeInfo,
    NodeRegistry,
};
use super::tags::{TagExpr, TagValue};

#[cfg(test)]
use super::tags::{Tag, TagNamespace, TagSet};

/// Error types for auto-group operations
#[derive(Error, Debug, Clone)]
pub enum AutoGroupError {
    #[error("Detector not found: {0}")]
    DetectorNotFound(String),
    #[error("Detection failed: {0}")]
    DetectionFailed(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Group merge failed: {0}")]
    MergeFailed(String),
}

/// Configuration for a specific detector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectorConfig {
    /// Whether this detector is enabled
    pub enabled: bool,
    /// Minimum number of nodes to form a group
    pub min_group_size: usize,
    /// Maximum number of groups this detector can create
    pub max_groups: Option<usize>,
    /// Custom naming pattern for groups (supports {detector}, {value} placeholders)
    pub naming_pattern: String,
    /// Confidence threshold for including nodes (0.0-1.0)
    pub confidence_threshold: f64,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        DetectorConfig {
            enabled: true,
            min_group_size: 1,
            max_groups: None,
            naming_pattern: "auto:{detector}:{value}".to_string(),
            confidence_threshold: 0.5,
        }
    }
}

impl DetectorConfig {
    /// Create a config with detector enabled
    pub fn enabled() -> Self {
        Self::default()
    }

    /// Create a config with detector disabled
    pub fn disabled() -> Self {
        DetectorConfig {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set minimum group size
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_group_size = size;
        self
    }

    /// Set maximum number of groups
    pub fn with_max_groups(mut self, max: usize) -> Self {
        self.max_groups = Some(max);
        self
    }

    /// Set naming pattern
    pub fn with_naming_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.naming_pattern = pattern.into();
        self
    }

    /// Set confidence threshold
    pub fn with_confidence_threshold(mut self, threshold: f64) -> Self {
        self.confidence_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Generate a group name from the pattern
    pub fn generate_name(&self, detector: &str, value: &str) -> String {
        self.naming_pattern
            .replace("{detector}", detector)
            .replace("{value}", value)
    }
}

/// Configuration for the auto-group system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoGroupConfig {
    /// Master enable/disable for all auto-detection
    pub enabled: bool,
    /// Interval between detection runs (in seconds)
    pub detection_interval_secs: u64,
    /// Whether to trigger detection on node join
    pub detect_on_node_join: bool,
    /// Whether to trigger detection on node leave
    pub detect_on_node_leave: bool,
    /// Whether to trigger detection on significant metric changes
    pub detect_on_metric_change: bool,
    /// Threshold for what constitutes a "significant" metric change (percentage)
    pub metric_change_threshold: f64,
    /// How long ephemeral groups remain valid without re-detection (in seconds)
    pub group_ttl_secs: u64,
    /// Whether to automatically clean up expired groups
    pub auto_cleanup: bool,
    /// Prefix for all auto-group IDs
    pub group_id_prefix: String,
    /// Per-detector configurations
    pub detectors: HashMap<String, DetectorConfig>,
}

impl Default for AutoGroupConfig {
    fn default() -> Self {
        let mut detectors = HashMap::new();
        detectors.insert("region".to_string(), DetectorConfig::default());
        detectors.insert("hardware".to_string(), DetectorConfig::default());
        detectors.insert("network".to_string(), DetectorConfig::default());
        detectors.insert("load".to_string(), DetectorConfig::default());

        AutoGroupConfig {
            enabled: true,
            detection_interval_secs: 60,
            detect_on_node_join: true,
            detect_on_node_leave: true,
            detect_on_metric_change: true,
            metric_change_threshold: 10.0,
            group_ttl_secs: 300,
            auto_cleanup: true,
            group_id_prefix: "auto".to_string(),
            detectors,
        }
    }
}

impl AutoGroupConfig {
    /// Get detector config, falling back to default
    pub fn detector_config(&self, name: &str) -> DetectorConfig {
        self.detectors.get(name).cloned().unwrap_or_default()
    }

    /// Check if a specific detector is enabled
    pub fn is_detector_enabled(&self, name: &str) -> bool {
        self.enabled && self.detector_config(name).enabled
    }

    /// Get the detection interval as a Duration
    pub fn detection_interval(&self) -> Duration {
        Duration::seconds(self.detection_interval_secs as i64)
    }

    /// Get the group TTL as a Duration
    pub fn group_ttl(&self) -> Duration {
        Duration::seconds(self.group_ttl_secs as i64)
    }
}

/// Result of detecting a group
#[derive(Debug, Clone)]
pub struct DetectedGroup {
    /// Unique identifier for this group within the detector
    pub key: String,
    /// Human-readable name
    pub name: String,
    /// Description of the group
    pub description: String,
    /// Nodes that belong to this group with their confidence scores
    pub members: HashMap<NodeId, f64>,
    /// When this group was detected
    pub detected_at: DateTime<Utc>,
    /// The detector that created this group
    pub detector_id: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl DetectedGroup {
    /// Create a new detected group
    pub fn new(
        key: impl Into<String>,
        name: impl Into<String>,
        description: impl Into<String>,
        detector_id: impl Into<String>,
    ) -> Self {
        DetectedGroup {
            key: key.into(),
            name: name.into(),
            description: description.into(),
            members: HashMap::new(),
            detected_at: Utc::now(),
            detector_id: detector_id.into(),
            metadata: HashMap::new(),
        }
    }

    /// Add a member with confidence score
    pub fn add_member(&mut self, node_id: NodeId, confidence: f64) {
        self.members.insert(node_id, confidence.clamp(0.0, 1.0));
    }

    /// Add multiple members with the same confidence
    pub fn add_members<I: IntoIterator<Item = NodeId>>(&mut self, nodes: I, confidence: f64) {
        for node_id in nodes {
            self.add_member(node_id, confidence);
        }
    }

    /// Get members above a confidence threshold
    pub fn members_above_threshold(&self, threshold: f64) -> HashSet<NodeId> {
        self.members
            .iter()
            .filter(|(_, &conf)| conf >= threshold)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Convert to a Group
    pub fn to_group(&self, config: &DetectorConfig, id_prefix: &str) -> Group {
        let group_id = GroupId::new(format!("{}:{}:{}", id_prefix, self.detector_id, self.key));
        let members = self.members_above_threshold(config.confidence_threshold);

        Group {
            id: group_id,
            name: self.name.clone(),
            description: self.description.clone(),
            membership: GroupMembership::Explicit(members),
            metadata: GroupMetadata {
                created_by: format!("detector:{}", self.detector_id),
                created_at: self.detected_at,
                group_type: GroupType::Ephemeral,
                authority_domain: Some(format!("auto:{}", self.detector_id)),
            },
        }
    }
}

/// Trait for implementing group detectors
pub trait GroupDetector: Send + Sync {
    /// Unique identifier for this detector
    fn id(&self) -> &str;

    /// Human-readable name
    fn name(&self) -> &str;

    /// Description of what this detector does
    fn description(&self) -> &str;

    /// Run detection on the given nodes
    fn detect(&self, nodes: &[&NodeInfo], config: &DetectorConfig) -> Vec<DetectedGroup>;

    /// Check if this detector should run based on the trigger
    fn should_run_on(&self, trigger: &DetectionTrigger) -> bool {
        match trigger {
            DetectionTrigger::Periodic => true,
            DetectionTrigger::NodeJoin(_) => true,
            DetectionTrigger::NodeLeave(_) => true,
            DetectionTrigger::MetricChange { .. } => false,
        }
    }
}

impl fmt::Debug for dyn GroupDetector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupDetector")
            .field("id", &self.id())
            .field("name", &self.name())
            .finish()
    }
}

/// Trigger for running detection
#[derive(Debug, Clone)]
pub enum DetectionTrigger {
    /// Periodic scheduled detection
    Periodic,
    /// A node joined the cluster
    NodeJoin(NodeId),
    /// A node left the cluster
    NodeLeave(NodeId),
    /// A metric changed significantly
    MetricChange {
        node_id: NodeId,
        metric: String,
        old_value: f64,
        new_value: f64,
    },
}

/// Region detector - groups nodes by cloud region or location tags
#[derive(Debug, Clone)]
pub struct RegionDetector {
    /// Tag keys to check for region information
    region_tags: Vec<String>,
}

impl RegionDetector {
    /// Create a new region detector with default settings
    pub fn new() -> Self {
        RegionDetector {
            region_tags: vec![
                "location:region".to_string(),
                "location:datacenter".to_string(),
                "location:zone".to_string(),
                "location:building".to_string(),
            ],
        }
    }

    /// Create with custom region tags
    pub fn with_tags(tags: Vec<String>) -> Self {
        RegionDetector { region_tags: tags }
    }

    /// Extract region value from a node
    fn extract_region(&self, node: &NodeInfo) -> Option<(String, String)> {
        for tag_key in &self.region_tags {
            if let Some(tag) = node.tags.get(tag_key) {
                let value = match &tag.value {
                    TagValue::String(s) => s.clone(),
                    TagValue::Hierarchical(h) => h.join(":"),
                    _ => continue,
                };
                return Some((tag_key.clone(), value));
            }
        }
        None
    }
}

impl Default for RegionDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupDetector for RegionDetector {
    fn id(&self) -> &str {
        "region"
    }

    fn name(&self) -> &str {
        "Region Detector"
    }

    fn description(&self) -> &str {
        "Groups nodes by cloud region, datacenter, or location tags"
    }

    fn detect(&self, nodes: &[&NodeInfo], config: &DetectorConfig) -> Vec<DetectedGroup> {
        // Group nodes by region
        let mut region_nodes: HashMap<String, Vec<NodeId>> = HashMap::new();

        for node in nodes {
            if let Some((_, region)) = self.extract_region(node) {
                region_nodes
                    .entry(region)
                    .or_default()
                    .push(node.id.clone());
            }
        }

        // Filter by minimum size and create groups
        let mut groups: Vec<DetectedGroup> = region_nodes
            .into_iter()
            .filter(|(_, members)| members.len() >= config.min_group_size)
            .map(|(region, members)| {
                let key = region.replace(':', "-").to_lowercase();
                let name = config.generate_name("region", &key);
                let mut group = DetectedGroup::new(
                    key,
                    name,
                    format!("Nodes in region: {}", region),
                    self.id(),
                );
                group.add_members(members, 1.0);
                group.with_metadata("region", region)
            })
            .collect();

        // Sort by size (largest first) and limit if configured
        groups.sort_by(|a, b| b.members.len().cmp(&a.members.len()));
        if let Some(max) = config.max_groups {
            groups.truncate(max);
        }

        groups
    }
}

/// Memory class thresholds (in GB)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryClasses {
    pub low_max: f64,
    pub medium_max: f64,
    pub high_max: f64,
}

impl Default for MemoryClasses {
    fn default() -> Self {
        MemoryClasses {
            low_max: 16.0,
            medium_max: 64.0,
            high_max: 256.0,
        }
    }
}

/// Hardware detector - groups nodes by CPU/GPU/memory class
#[derive(Debug, Clone)]
pub struct HardwareDetector {
    /// Memory tag key
    memory_tag: String,
    /// GPU tag key
    gpu_tag: String,
    /// CPU tag key
    #[allow(dead_code)]
    cpu_tag: String,
    /// Memory class thresholds
    memory_classes: MemoryClasses,
}

impl HardwareDetector {
    /// Create a new hardware detector
    pub fn new() -> Self {
        HardwareDetector {
            memory_tag: "hardware:memory".to_string(),
            gpu_tag: "hardware:gpu".to_string(),
            cpu_tag: "hardware:cpu".to_string(),
            memory_classes: MemoryClasses::default(),
        }
    }

    /// Create with custom memory classes
    pub fn with_memory_classes(mut self, classes: MemoryClasses) -> Self {
        self.memory_classes = classes;
        self
    }

    /// Classify memory amount into a class
    fn classify_memory(&self, memory_gb: f64) -> &'static str {
        if memory_gb <= self.memory_classes.low_max {
            "low-memory"
        } else if memory_gb <= self.memory_classes.medium_max {
            "medium-memory"
        } else if memory_gb <= self.memory_classes.high_max {
            "high-memory"
        } else {
            "ultra-memory"
        }
    }

    /// Check if node has GPU
    fn has_gpu(&self, node: &NodeInfo) -> bool {
        node.tags.contains(&self.gpu_tag)
    }

    /// Get memory class for a node
    fn get_memory_class(&self, node: &NodeInfo) -> Option<&'static str> {
        node.tags
            .get(&self.memory_tag)
            .and_then(|tag| tag.value.as_number().map(|mem| self.classify_memory(mem)))
    }
}

impl Default for HardwareDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupDetector for HardwareDetector {
    fn id(&self) -> &str {
        "hardware"
    }

    fn name(&self) -> &str {
        "Hardware Detector"
    }

    fn description(&self) -> &str {
        "Groups nodes by CPU, GPU, and memory classification"
    }

    fn detect(&self, nodes: &[&NodeInfo], config: &DetectorConfig) -> Vec<DetectedGroup> {
        let mut groups: Vec<DetectedGroup> = Vec::new();

        // GPU vs non-GPU
        let mut gpu_nodes = Vec::new();
        let mut cpu_only_nodes = Vec::new();
        for node in nodes {
            if self.has_gpu(node) {
                gpu_nodes.push(node.id.clone());
            } else {
                cpu_only_nodes.push(node.id.clone());
            }
        }

        if gpu_nodes.len() >= config.min_group_size {
            let name = config.generate_name("hardware", "gpu");
            let mut group = DetectedGroup::new("gpu", name, "Nodes with GPU capability", self.id());
            group.add_members(gpu_nodes, 1.0);
            groups.push(group);
        }

        if cpu_only_nodes.len() >= config.min_group_size {
            let name = config.generate_name("hardware", "cpu-only");
            let mut group =
                DetectedGroup::new("cpu-only", name, "Nodes without GPU (CPU-only)", self.id());
            group.add_members(cpu_only_nodes, 1.0);
            groups.push(group);
        }

        // Memory classes
        let mut memory_groups: HashMap<&'static str, Vec<NodeId>> = HashMap::new();
        for node in nodes {
            if let Some(class) = self.get_memory_class(node) {
                memory_groups
                    .entry(class)
                    .or_default()
                    .push(node.id.clone());
            }
        }

        for (class, members) in memory_groups {
            if members.len() >= config.min_group_size {
                let name = config.generate_name("hardware", class);
                let mut group = DetectedGroup::new(
                    class,
                    name,
                    format!("Nodes with {} resources", class),
                    self.id(),
                );
                group.add_members(members, 1.0);
                groups.push(group);
            }
        }

        // Sort and limit
        groups.sort_by(|a, b| b.members.len().cmp(&a.members.len()));
        if let Some(max) = config.max_groups {
            groups.truncate(max);
        }

        groups
    }
}

/// Latency bucket for network grouping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyBuckets {
    /// Very low latency threshold (ms)
    pub ultra_low_max: f64,
    /// Low latency threshold (ms)
    pub low_max: f64,
    /// Medium latency threshold (ms)
    pub medium_max: f64,
}

impl Default for LatencyBuckets {
    fn default() -> Self {
        LatencyBuckets {
            ultra_low_max: 1.0,
            low_max: 5.0,
            medium_max: 20.0,
        }
    }
}

/// Network detector - groups nodes by network latency proximity
#[derive(Debug, Clone)]
pub struct NetworkDetector {
    /// Tag key for network latency info
    latency_tag: String,
    /// Tag key for network segment
    network_tag: String,
    /// Latency buckets for classification
    latency_buckets: LatencyBuckets,
}

impl NetworkDetector {
    /// Create a new network detector
    pub fn new() -> Self {
        NetworkDetector {
            latency_tag: "ephemeral:latency-class".to_string(),
            network_tag: "hardware:network-segment".to_string(),
            latency_buckets: LatencyBuckets::default(),
        }
    }

    /// Create with custom latency buckets
    pub fn with_latency_buckets(mut self, buckets: LatencyBuckets) -> Self {
        self.latency_buckets = buckets;
        self
    }

    /// Get network segment for a node
    fn get_network_segment(&self, node: &NodeInfo) -> Option<String> {
        node.tags
            .get(&self.network_tag)
            .and_then(|tag| match &tag.value {
                TagValue::String(s) => Some(s.clone()),
                TagValue::Hierarchical(h) => Some(h.join(":")),
                _ => None,
            })
    }

    /// Get latency class for a node
    fn get_latency_class(&self, node: &NodeInfo) -> Option<String> {
        node.tags
            .get(&self.latency_tag)
            .and_then(|tag| match &tag.value {
                TagValue::String(s) => Some(s.clone()),
                TagValue::Number(latency) => Some(self.classify_latency(*latency)),
                _ => None,
            })
    }

    /// Classify latency into a bucket
    fn classify_latency(&self, latency_ms: f64) -> String {
        if latency_ms <= self.latency_buckets.ultra_low_max {
            "ultra-low-latency".to_string()
        } else if latency_ms <= self.latency_buckets.low_max {
            "low-latency".to_string()
        } else if latency_ms <= self.latency_buckets.medium_max {
            "medium-latency".to_string()
        } else {
            "high-latency".to_string()
        }
    }
}

impl Default for NetworkDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupDetector for NetworkDetector {
    fn id(&self) -> &str {
        "network"
    }

    fn name(&self) -> &str {
        "Network Detector"
    }

    fn description(&self) -> &str {
        "Groups nodes by network segment and latency proximity"
    }

    fn detect(&self, nodes: &[&NodeInfo], config: &DetectorConfig) -> Vec<DetectedGroup> {
        let mut groups: Vec<DetectedGroup> = Vec::new();

        // Group by network segment
        let mut segment_groups: HashMap<String, Vec<NodeId>> = HashMap::new();
        for node in nodes {
            if let Some(segment) = self.get_network_segment(node) {
                segment_groups
                    .entry(segment)
                    .or_default()
                    .push(node.id.clone());
            }
        }

        for (segment, members) in segment_groups {
            if members.len() >= config.min_group_size {
                let key = segment.replace(':', "-").to_lowercase();
                let name = config.generate_name("network", &format!("segment-{}", key));
                let mut group = DetectedGroup::new(
                    format!("segment-{}", key),
                    name,
                    format!("Nodes in network segment: {}", segment),
                    self.id(),
                );
                group.add_members(members, 1.0);
                groups.push(group.with_metadata("segment", segment));
            }
        }

        // Group by latency class
        let mut latency_groups: HashMap<String, Vec<NodeId>> = HashMap::new();
        for node in nodes {
            if let Some(class) = self.get_latency_class(node) {
                latency_groups
                    .entry(class)
                    .or_default()
                    .push(node.id.clone());
            }
        }

        for (class, members) in latency_groups {
            if members.len() >= config.min_group_size {
                let name = config.generate_name("network", &class);
                let mut group = DetectedGroup::new(
                    class.clone(),
                    name,
                    format!("Nodes with {} network characteristics", class),
                    self.id(),
                );
                group.add_members(members, 1.0);
                groups.push(group.with_metadata("latency_class", class));
            }
        }

        // Sort and limit
        groups.sort_by(|a, b| b.members.len().cmp(&a.members.len()));
        if let Some(max) = config.max_groups {
            groups.truncate(max);
        }

        groups
    }

    fn should_run_on(&self, trigger: &DetectionTrigger) -> bool {
        match trigger {
            DetectionTrigger::Periodic => true,
            DetectionTrigger::NodeJoin(_) => true,
            DetectionTrigger::NodeLeave(_) => true,
            DetectionTrigger::MetricChange { metric, .. } => {
                metric.contains("latency") || metric.contains("network")
            }
        }
    }
}

/// Load level thresholds (0.0-1.0)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadThresholds {
    /// Threshold below which node is considered idle
    pub idle_max: f64,
    /// Threshold below which node is considered light load
    pub light_max: f64,
    /// Threshold below which node is considered moderate load
    pub moderate_max: f64,
}

impl Default for LoadThresholds {
    fn default() -> Self {
        LoadThresholds {
            idle_max: 0.1,
            light_max: 0.4,
            moderate_max: 0.7,
        }
    }
}

/// Load detector - groups nodes by current load level
#[derive(Debug, Clone)]
pub struct LoadDetector {
    /// Tag key for CPU load
    cpu_load_tag: String,
    /// Tag key for memory usage
    #[allow(dead_code)]
    memory_usage_tag: String,
    /// Tag key for task count
    #[allow(dead_code)]
    task_count_tag: String,
    /// Load thresholds
    thresholds: LoadThresholds,
}

impl LoadDetector {
    /// Create a new load detector
    pub fn new() -> Self {
        LoadDetector {
            cpu_load_tag: "ephemeral:cpu-load".to_string(),
            memory_usage_tag: "ephemeral:memory-usage".to_string(),
            task_count_tag: "ephemeral:task-count".to_string(),
            thresholds: LoadThresholds::default(),
        }
    }

    /// Create with custom thresholds
    pub fn with_thresholds(mut self, thresholds: LoadThresholds) -> Self {
        self.thresholds = thresholds;
        self
    }

    /// Get CPU load for a node (0.0-1.0)
    fn get_cpu_load(&self, node: &NodeInfo) -> Option<f64> {
        node.tags
            .get(&self.cpu_load_tag)
            .and_then(|tag| tag.value.as_number())
    }

    /// Classify load level
    fn classify_load(&self, load: f64) -> &'static str {
        if load <= self.thresholds.idle_max {
            "idle"
        } else if load <= self.thresholds.light_max {
            "light-load"
        } else if load <= self.thresholds.moderate_max {
            "moderate-load"
        } else {
            "heavy-load"
        }
    }
}

impl Default for LoadDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupDetector for LoadDetector {
    fn id(&self) -> &str {
        "load"
    }

    fn name(&self) -> &str {
        "Load Detector"
    }

    fn description(&self) -> &str {
        "Groups nodes by current CPU/memory load levels"
    }

    fn detect(&self, nodes: &[&NodeInfo], config: &DetectorConfig) -> Vec<DetectedGroup> {
        let mut groups: Vec<DetectedGroup> = Vec::new();

        // Group by load level
        let mut load_groups: HashMap<&'static str, Vec<(NodeId, f64)>> = HashMap::new();
        for node in nodes {
            if let Some(load) = self.get_cpu_load(node) {
                let class = self.classify_load(load);
                load_groups
                    .entry(class)
                    .or_default()
                    .push((node.id.clone(), load));
            }
        }

        for (class, members) in load_groups {
            if members.len() >= config.min_group_size {
                let name = config.generate_name("load", class);
                let mut group = DetectedGroup::new(
                    class,
                    name,
                    format!("Nodes with {} status", class),
                    self.id(),
                );
                // Use inverse load as confidence (idle nodes have higher confidence)
                for (node_id, load) in members {
                    let confidence = if class == "idle" {
                        1.0 - load // Higher confidence for lower load
                    } else {
                        0.8 // Fixed confidence for non-idle
                    };
                    group.add_member(node_id, confidence);
                }
                groups.push(group.with_metadata("load_class", class.to_string()));
            }
        }

        // Sort and limit
        groups.sort_by(|a, b| b.members.len().cmp(&a.members.len()));
        if let Some(max) = config.max_groups {
            groups.truncate(max);
        }

        groups
    }

    fn should_run_on(&self, trigger: &DetectionTrigger) -> bool {
        match trigger {
            DetectionTrigger::Periodic => true,
            DetectionTrigger::NodeJoin(_) => false, // New nodes don't have load yet
            DetectionTrigger::NodeLeave(_) => true,
            DetectionTrigger::MetricChange { metric, .. } => {
                metric.contains("load") || metric.contains("cpu") || metric.contains("memory")
            }
        }
    }
}

/// Information about an active auto-detected group
#[derive(Debug, Clone)]
struct ActiveAutoGroup {
    /// The detected group data
    detected: DetectedGroup,
    /// When this group expires
    expires_at: DateTime<Utc>,
    /// The group ID in the main registry (if merged)
    merged_id: Option<GroupId>,
}

/// Registry for managing auto-detected groups
pub struct AutoGroupRegistry {
    /// Configuration
    config: AutoGroupConfig,
    /// Registered detectors
    detectors: Vec<Box<dyn GroupDetector>>,
    /// Currently active auto-detected groups
    active_groups: HashMap<String, ActiveAutoGroup>,
    /// Last detection time
    last_detection: Option<DateTime<Utc>>,
    /// Detection run count
    detection_count: u64,
}

impl AutoGroupRegistry {
    /// Create a new auto-group registry
    pub fn new(config: AutoGroupConfig) -> Self {
        AutoGroupRegistry {
            config,
            detectors: Vec::new(),
            active_groups: HashMap::new(),
            last_detection: None,
            detection_count: 0,
        }
    }

    /// Create with default configuration and all built-in detectors
    pub fn with_default_detectors() -> Self {
        let mut registry = Self::new(AutoGroupConfig::default());
        registry.register_detector(Box::new(RegionDetector::new()));
        registry.register_detector(Box::new(HardwareDetector::new()));
        registry.register_detector(Box::new(NetworkDetector::new()));
        registry.register_detector(Box::new(LoadDetector::new()));
        registry
    }

    /// Register a detector
    pub fn register_detector(&mut self, detector: Box<dyn GroupDetector>) {
        self.detectors.push(detector);
    }

    /// Get the configuration
    pub fn config(&self) -> &AutoGroupConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: AutoGroupConfig) {
        self.config = config;
    }

    /// Check if detection should run based on time
    pub fn should_run_periodic(&self) -> bool {
        if !self.config.enabled {
            return false;
        }
        match self.last_detection {
            None => true,
            Some(last) => Utc::now() - last >= self.config.detection_interval(),
        }
    }

    /// Run detection on all registered detectors
    pub fn run_detection(&mut self, nodes: &NodeRegistry) {
        self.run_detection_with_trigger(nodes, DetectionTrigger::Periodic);
    }

    /// Run detection with a specific trigger
    pub fn run_detection_with_trigger(&mut self, nodes: &NodeRegistry, trigger: DetectionTrigger) {
        if !self.config.enabled {
            return;
        }

        // Collect all node references
        let node_refs: Vec<&NodeInfo> = nodes.all_nodes().collect();

        // Run each enabled detector
        for detector in &self.detectors {
            if !self.config.is_detector_enabled(detector.id()) {
                continue;
            }

            if !detector.should_run_on(&trigger) {
                continue;
            }

            let detector_config = self.config.detector_config(detector.id());
            let detected = detector.detect(&node_refs, &detector_config);

            // Store detected groups
            let expires_at = Utc::now() + self.config.group_ttl();
            for group in detected {
                let key = format!("{}:{}", detector.id(), group.key);
                self.active_groups.insert(
                    key,
                    ActiveAutoGroup {
                        detected: group,
                        expires_at,
                        merged_id: None,
                    },
                );
            }
        }

        self.last_detection = Some(Utc::now());
        self.detection_count += 1;
    }

    /// Cleanup expired groups
    pub fn cleanup_expired(&mut self) {
        let now = Utc::now();
        self.active_groups.retain(|_, g| g.expires_at > now);
    }

    /// Get all active detected groups
    pub fn active_groups(&self) -> impl Iterator<Item = &DetectedGroup> {
        self.active_groups.values().map(|g| &g.detected)
    }

    /// Get a specific detected group by key
    pub fn get_group(&self, key: &str) -> Option<&DetectedGroup> {
        self.active_groups.get(key).map(|g| &g.detected)
    }

    /// Get the number of active groups
    pub fn active_count(&self) -> usize {
        self.active_groups.len()
    }

    /// Get detection statistics
    pub fn stats(&self) -> AutoGroupStats {
        AutoGroupStats {
            detection_count: self.detection_count,
            last_detection: self.last_detection,
            active_groups: self.active_groups.len(),
            detector_count: self.detectors.len(),
        }
    }

    /// Merge auto-detected groups into a GroupRegistry
    pub fn merge_into(
        &mut self,
        groups: &mut GroupRegistry,
        _nodes: &NodeRegistry,
    ) -> Result<Vec<GroupId>, AutoGroupError> {
        if self.config.auto_cleanup {
            self.cleanup_expired();
        }

        let mut merged_ids = Vec::new();
        let prefix = &self.config.group_id_prefix;

        for (key, active) in &mut self.active_groups {
            let detector_config = self.config.detector_config(&active.detected.detector_id);
            let group = active.detected.to_group(&detector_config, prefix);
            let group_id = group.id.clone();

            // Check if group already exists
            if let Some(existing_id) = &active.merged_id {
                // Update existing group
                if let Some(existing) = groups.get_mut(existing_id) {
                    let members = active
                        .detected
                        .members_above_threshold(detector_config.confidence_threshold);
                    existing.membership = GroupMembership::Explicit(members);
                }
            } else {
                // Check if a group with this ID already exists (from previous run)
                if groups.get(&group_id).is_some() {
                    // Unregister old version first
                    groups.unregister(&group_id);
                }

                // Register new group
                match groups.register(group) {
                    Ok(id) => {
                        active.merged_id = Some(id.clone());
                        merged_ids.push(id);
                    }
                    Err(e) => {
                        // Log but continue - group might already exist with same name
                        eprintln!("Failed to merge auto-group {}: {}", key, e);
                    }
                }
            }
        }

        Ok(merged_ids)
    }

    /// Remove all auto-groups from a GroupRegistry
    pub fn remove_from(&self, groups: &mut GroupRegistry) -> Vec<GroupId> {
        let mut removed = Vec::new();
        let prefix = format!("{}:", self.config.group_id_prefix);

        // Find all groups with our prefix
        let auto_group_ids: Vec<GroupId> = groups
            .all_groups()
            .filter(|g| g.id.0.starts_with(&prefix))
            .map(|g| g.id.clone())
            .collect();

        for id in auto_group_ids {
            if groups.unregister(&id).is_some() {
                removed.push(id);
            }
        }

        removed
    }

    /// Create a TagExpr that matches nodes in an auto-group
    /// Usage: `group:auto:high-memory` in tag expressions
    pub fn create_group_tag_expr(&self, group_key: &str) -> Option<TagExpr> {
        let key = if group_key.starts_with(&self.config.group_id_prefix) {
            group_key.to_string()
        } else {
            // Try to find matching group
            self.active_groups
                .keys()
                .find(|k| k.ends_with(group_key))
                .cloned()?
        };

        let group = self.active_groups.get(&key)?;
        let config = self.config.detector_config(&group.detected.detector_id);
        let members = group
            .detected
            .members_above_threshold(config.confidence_threshold);

        if members.is_empty() {
            return None;
        }

        // Create an expression that matches any of the member node IDs
        // This is a simplified approach - in practice you might want to
        // add node-id tags to nodes
        Some(TagExpr::True) // Placeholder - actual implementation would depend on node ID tagging
    }
}

impl fmt::Debug for AutoGroupRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AutoGroupRegistry")
            .field("config", &self.config)
            .field("detector_count", &self.detectors.len())
            .field("active_groups", &self.active_groups.len())
            .field("last_detection", &self.last_detection)
            .finish()
    }
}

/// Statistics about auto-group detection
#[derive(Debug, Clone)]
pub struct AutoGroupStats {
    /// Total number of detection runs
    pub detection_count: u64,
    /// Last detection time
    pub last_detection: Option<DateTime<Utc>>,
    /// Number of currently active groups
    pub active_groups: usize,
    /// Number of registered detectors
    pub detector_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_nodes() -> NodeRegistry {
        let mut registry = NodeRegistry::new();

        // Node 1: GPU, high memory, region us-east, low load
        let mut tags1 = TagSet::new();
        tags1.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));
        tags1.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(128.0),
        ));
        tags1.insert(Tag::new(
            TagNamespace::Location,
            "region",
            TagValue::String("us-east".to_string()),
        ));
        tags1.insert(Tag::new(
            TagNamespace::Ephemeral,
            "cpu-load",
            TagValue::Number(0.05),
        ));
        registry.register(NodeInfo::with_tags("node-1", tags1));

        // Node 2: GPU, medium memory, region us-east, moderate load
        let mut tags2 = TagSet::new();
        tags2.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));
        tags2.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(64.0),
        ));
        tags2.insert(Tag::new(
            TagNamespace::Location,
            "region",
            TagValue::String("us-east".to_string()),
        ));
        tags2.insert(Tag::new(
            TagNamespace::Ephemeral,
            "cpu-load",
            TagValue::Number(0.55),
        ));
        registry.register(NodeInfo::with_tags("node-2", tags2));

        // Node 3: No GPU, low memory, region eu-west, heavy load
        let mut tags3 = TagSet::new();
        tags3.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(16.0),
        ));
        tags3.insert(Tag::new(
            TagNamespace::Location,
            "region",
            TagValue::String("eu-west".to_string()),
        ));
        tags3.insert(Tag::new(
            TagNamespace::Ephemeral,
            "cpu-load",
            TagValue::Number(0.85),
        ));
        registry.register(NodeInfo::with_tags("node-3", tags3));

        // Node 4: No GPU, ultra memory, region us-east, idle
        let mut tags4 = TagSet::new();
        tags4.insert(Tag::new(
            TagNamespace::Hardware,
            "memory",
            TagValue::Number(512.0),
        ));
        tags4.insert(Tag::new(
            TagNamespace::Location,
            "region",
            TagValue::String("us-east".to_string()),
        ));
        tags4.insert(Tag::new(
            TagNamespace::Ephemeral,
            "cpu-load",
            TagValue::Number(0.02),
        ));
        registry.register(NodeInfo::with_tags("node-4", tags4));

        registry
    }

    #[test]
    fn test_region_detector() {
        let nodes = create_test_nodes();
        let detector = RegionDetector::new();
        let config = DetectorConfig::default();

        let node_refs: Vec<&NodeInfo> = nodes.all_nodes().collect();
        let groups = detector.detect(&node_refs, &config);

        // Should have 2 regions: us-east (3 nodes) and eu-west (1 node)
        assert_eq!(groups.len(), 2);

        let us_east = groups.iter().find(|g| g.key == "us-east");
        assert!(us_east.is_some());
        assert_eq!(us_east.unwrap().members.len(), 3);

        let eu_west = groups.iter().find(|g| g.key == "eu-west");
        assert!(eu_west.is_some());
        assert_eq!(eu_west.unwrap().members.len(), 1);
    }

    #[test]
    fn test_region_detector_min_size() {
        let nodes = create_test_nodes();
        let detector = RegionDetector::new();
        let config = DetectorConfig::default().with_min_size(2);

        let node_refs: Vec<&NodeInfo> = nodes.all_nodes().collect();
        let groups = detector.detect(&node_refs, &config);

        // Only us-east should qualify (3 nodes >= 2)
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].key, "us-east");
    }

    #[test]
    fn test_hardware_detector() {
        let nodes = create_test_nodes();
        let detector = HardwareDetector::new();
        let config = DetectorConfig::default();

        let node_refs: Vec<&NodeInfo> = nodes.all_nodes().collect();
        let groups = detector.detect(&node_refs, &config);

        // Should have: gpu (2), cpu-only (2), various memory classes
        let gpu_group = groups.iter().find(|g| g.key == "gpu");
        assert!(gpu_group.is_some());
        assert_eq!(gpu_group.unwrap().members.len(), 2);

        let cpu_only = groups.iter().find(|g| g.key == "cpu-only");
        assert!(cpu_only.is_some());
        assert_eq!(cpu_only.unwrap().members.len(), 2);

        // Check memory classes
        let high_mem = groups.iter().find(|g| g.key == "high-memory");
        assert!(high_mem.is_some()); // node-1 with 128GB

        let ultra_mem = groups.iter().find(|g| g.key == "ultra-memory");
        assert!(ultra_mem.is_some()); // node-4 with 512GB
    }

    #[test]
    fn test_load_detector() {
        let nodes = create_test_nodes();
        let detector = LoadDetector::new();
        let config = DetectorConfig::default();

        let node_refs: Vec<&NodeInfo> = nodes.all_nodes().collect();
        let groups = detector.detect(&node_refs, &config);

        // Should have: idle (2), moderate-load (1), heavy-load (1)
        let idle = groups.iter().find(|g| g.key == "idle");
        assert!(idle.is_some());
        assert_eq!(idle.unwrap().members.len(), 2); // node-1, node-4

        let moderate = groups.iter().find(|g| g.key == "moderate-load");
        assert!(moderate.is_some());
        assert_eq!(moderate.unwrap().members.len(), 1); // node-2

        let heavy = groups.iter().find(|g| g.key == "heavy-load");
        assert!(heavy.is_some());
        assert_eq!(heavy.unwrap().members.len(), 1); // node-3
    }

    #[test]
    fn test_auto_group_registry() {
        let nodes = create_test_nodes();
        let mut registry = AutoGroupRegistry::with_default_detectors();

        // Run detection
        registry.run_detection(&nodes);

        // Should have groups from all detectors
        assert!(registry.active_count() > 0);

        // Check stats
        let stats = registry.stats();
        assert_eq!(stats.detection_count, 1);
        assert!(stats.last_detection.is_some());
        assert_eq!(stats.detector_count, 4);
    }

    #[test]
    fn test_merge_into_group_registry() {
        let nodes = create_test_nodes();
        let mut auto_registry = AutoGroupRegistry::with_default_detectors();
        let mut groups = GroupRegistry::new();

        // Run detection and merge
        auto_registry.run_detection(&nodes);
        let merged = auto_registry.merge_into(&mut groups, &nodes).unwrap();

        // Should have merged some groups
        assert!(!merged.is_empty());

        // Verify groups exist in registry
        for id in &merged {
            assert!(groups.get(id).is_some());
        }

        // Verify ephemeral type
        for group in groups.all_groups() {
            if group.id.0.starts_with("auto:") {
                assert_eq!(group.metadata.group_type, GroupType::Ephemeral);
            }
        }
    }

    #[test]
    fn test_cleanup_expired() {
        let nodes = create_test_nodes();
        let mut config = AutoGroupConfig::default();
        config.group_ttl_secs = 0; // Expire immediately
        let mut registry = AutoGroupRegistry::new(config);
        registry.register_detector(Box::new(RegionDetector::new()));

        // Run detection
        registry.run_detection(&nodes);
        assert!(registry.active_count() > 0);

        // Wait a tiny bit and cleanup
        std::thread::sleep(std::time::Duration::from_millis(10));
        registry.cleanup_expired();

        // All groups should be expired
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn test_detector_config_naming() {
        let config = DetectorConfig::default().with_naming_pattern("group:{detector}:{value}");

        assert_eq!(
            config.generate_name("hardware", "gpu"),
            "group:hardware:gpu"
        );
        assert_eq!(
            config.generate_name("region", "us-east"),
            "group:region:us-east"
        );
    }

    #[test]
    fn test_detection_trigger_filtering() {
        let region_detector = RegionDetector::new();
        let load_detector = LoadDetector::new();

        // Region detector should run on node join
        assert!(region_detector.should_run_on(&DetectionTrigger::NodeJoin(NodeId::new("test"))));

        // Load detector should NOT run on node join (new nodes don't have load)
        assert!(!load_detector.should_run_on(&DetectionTrigger::NodeJoin(NodeId::new("test"))));

        // Load detector should run on load metric change
        assert!(
            load_detector.should_run_on(&DetectionTrigger::MetricChange {
                node_id: NodeId::new("test"),
                metric: "cpu-load".to_string(),
                old_value: 0.5,
                new_value: 0.9,
            })
        );
    }

    #[test]
    fn test_detected_group_confidence_threshold() {
        let mut group = DetectedGroup::new("test", "Test", "Test group", "test-detector");
        group.add_member(NodeId::new("high-conf"), 0.9);
        group.add_member(NodeId::new("medium-conf"), 0.6);
        group.add_member(NodeId::new("low-conf"), 0.3);

        // Threshold 0.5 should include high and medium
        let above_50 = group.members_above_threshold(0.5);
        assert_eq!(above_50.len(), 2);
        assert!(above_50.contains(&NodeId::new("high-conf")));
        assert!(above_50.contains(&NodeId::new("medium-conf")));

        // Threshold 0.8 should only include high
        let above_80 = group.members_above_threshold(0.8);
        assert_eq!(above_80.len(), 1);
        assert!(above_80.contains(&NodeId::new("high-conf")));
    }

    #[test]
    fn test_remove_from_registry() {
        let nodes = create_test_nodes();
        let mut auto_registry = AutoGroupRegistry::with_default_detectors();
        let mut groups = GroupRegistry::new();

        // Add a static group first
        let static_group =
            Group::explicit("static-group", "Static", std::collections::HashSet::new());
        groups.register(static_group).unwrap();

        // Run detection and merge
        auto_registry.run_detection(&nodes);
        auto_registry.merge_into(&mut groups, &nodes).unwrap();

        let total_before = groups.len();
        assert!(total_before > 1); // At least static + some auto

        // Remove auto groups
        let removed = auto_registry.remove_from(&mut groups);
        assert!(!removed.is_empty());

        // Only static group should remain
        assert_eq!(groups.len(), 1);
        assert!(groups.get_by_name("static-group").is_some());
    }

    #[test]
    fn test_disabled_detector() {
        let nodes = create_test_nodes();
        let mut config = AutoGroupConfig::default();
        config
            .detectors
            .insert("region".to_string(), DetectorConfig::disabled());

        let mut registry = AutoGroupRegistry::new(config);
        registry.register_detector(Box::new(RegionDetector::new()));

        registry.run_detection(&nodes);

        // No region groups should be detected
        let has_region = registry.active_groups().any(|g| g.detector_id == "region");
        assert!(!has_region);
    }

    #[test]
    fn test_max_groups_limit() {
        let mut nodes = NodeRegistry::new();
        // Create many regions
        for i in 0..10 {
            let mut tags = TagSet::new();
            tags.insert(Tag::new(
                TagNamespace::Location,
                "region",
                TagValue::String(format!("region-{}", i)),
            ));
            registry_node(&mut nodes, &format!("node-{}", i), tags);
        }

        let detector = RegionDetector::new();
        let config = DetectorConfig::default().with_max_groups(3);

        let node_refs: Vec<&NodeInfo> = nodes.all_nodes().collect();
        let groups = detector.detect(&node_refs, &config);

        // Should be limited to 3 groups
        assert_eq!(groups.len(), 3);
    }

    fn registry_node(registry: &mut NodeRegistry, id: &str, tags: TagSet) {
        registry.register(NodeInfo::with_tags(id, tags));
    }
}
