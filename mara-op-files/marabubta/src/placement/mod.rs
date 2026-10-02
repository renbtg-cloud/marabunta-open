// Marabunta - Licensed under the MIT License.
//! Placement system for job scheduling and node management
//!
//! This module provides a comprehensive tags and groups system for
//! organizing compute nodes and enabling flexible job placement.
//!
//! # Tags
//!
//! Tags are key-value labels attached to nodes. They support multiple
//! value types (string, number, boolean, list, hierarchical) and can
//! be matched using a powerful expression language.
//!
//! ```rust
//! use marabunta_compute::placement::tags::{Tag, TagNamespace, TagValue, TagSet, TagExpr};
//!
//! // Create tags
//! let mut tags = TagSet::new();
//! tags.insert(Tag::new(TagNamespace::Hardware, "gpu", TagValue::Present));
//! tags.insert(Tag::new(TagNamespace::Hardware, "memory", TagValue::Number(128.0)));
//! tags.insert(Tag::new(TagNamespace::Location, "building", "building-a"));
//!
//! // Match with expressions
//! let expr = TagExpr::parse("hardware:gpu AND hardware:memory > 64").unwrap();
//! assert!(tags.matches(&expr));
//! ```
//!
//! # Groups
//!
//! Groups are named collections of nodes. They can be:
//! - **Explicit**: Static list of node IDs
//! - **Dynamic**: Membership determined by tag expression
//! - **Composite**: Union, intersection, or difference of other groups
//!
//! ```rust
//! use marabunta_compute::placement::groups::{Group, GroupRegistry, NodeRegistry, NodeInfo};
//! use marabunta_compute::placement::tags::TagExpr;
//!
//! let mut groups = GroupRegistry::new();
//! let nodes = NodeRegistry::new();
//!
//! // Create a dynamic group for all GPU nodes
//! let gpu_group = Group::dynamic(
//!     "gpu-nodes",
//!     "All nodes with GPU capability",
//!     TagExpr::Has("hardware:gpu".into()),
//! );
//! groups.register(gpu_group).unwrap();
//! ```
//!
//! # Auto-Groups
//!
//! Auto-groups are ephemeral groups that are automatically detected based on
//! node properties like region, hardware, network proximity, and load.
//!
//! ```rust
//! use marabunta_compute::placement::auto_groups::{
//!     AutoGroupRegistry, RegionDetector, HardwareDetector,
//! };
//! use marabunta_compute::placement::groups::{NodeRegistry, GroupRegistry};
//!
//! // Create auto-group registry with built-in detectors
//! let mut auto_registry = AutoGroupRegistry::with_default_detectors();
//!
//! // Run detection periodically
//! let nodes = NodeRegistry::new();
//! auto_registry.run_detection(&nodes);
//!
//! // Merge into main group registry
//! let mut groups = GroupRegistry::new();
//! auto_registry.merge_into(&mut groups, &nodes);
//! ```
//!
//! # Data Locality
//!
//! The locality module provides data location tracking and locality-aware
//! scheduling to minimize network transfer during task execution.
//!
//! ```rust
//! use marabunta_compute::placement::locality::{
//!     DataId, DataLocation, LocalityRegistry, LocalityScorer, LocalityConfig,
//!     DataRef, LocalityPreference,
//! };
//! use marabunta_compute::common::types::WorkerId;
//!
//! // Create a locality registry
//! let mut registry = LocalityRegistry::new();
//!
//! // Register data location
//! let data_id = DataId::new("dataset-001");
//! let worker1 = WorkerId::new();
//!
//! let mut location = DataLocation::new(data_id.clone(), 1024 * 1024 * 100);
//! location.add_node(worker1);
//! registry.register_data(location);
//!
//! // Score workers by locality for scheduling
//! let scorer = LocalityScorer::new(LocalityConfig::default());
//! let data_refs = vec![DataRef::preferred(data_id)];
//! let score = scorer.score_worker(&worker1, &data_refs, &registry);
//! ```

pub mod auto_groups;
pub mod groups;
pub mod locality;
pub mod tags;

// Re-export commonly used types
pub use auto_groups::{
    AutoGroupConfig, AutoGroupError, AutoGroupRegistry, AutoGroupStats, DetectedGroup,
    DetectionTrigger, DetectorConfig, GroupDetector, HardwareDetector, LatencyBuckets,
    LoadDetector, LoadThresholds, MemoryClasses, NetworkDetector, RegionDetector,
};
pub use groups::{
    Group, GroupError, GroupId, GroupMembership, GroupMetadata, GroupRegistry, GroupType, NodeId,
    NodeInfo, NodeRegistry, NodeStatus,
};
pub use locality::{
    DataId, DataLocation, DataLocationInfo, DataPlacementHint, DataRef, LocalityConfig,
    LocalityError, LocalityPreference, LocalityRegistry, LocalityScore, LocalityScorer,
    LocalityStats,
};
pub use tags::{
    ConflictResolution, ParseError, Tag, TagError, TagExpr, TagMetadata, TagNamespace, TagSet,
    TagSource, TagValue,
};
