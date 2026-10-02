// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

// -- Entity identification --

/// Reference to any swarm entity. Used in guards, observations, and audit trails.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityRef {
    pub entity_type: EntityType,
    pub id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Node,
    Job,
    Chunk,
    Collective,
    MarketplacePosting,
    Plugin,
    Config,
    Operator,
    Session,
}

impl fmt::Display for EntityRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.entity_type, self.id)
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityType::Node => write!(f, "node"),
            EntityType::Job => write!(f, "job"),
            EntityType::Chunk => write!(f, "chunk"),
            EntityType::Collective => write!(f, "collective"),
            EntityType::MarketplacePosting => write!(f, "marketplace_posting"),
            EntityType::Plugin => write!(f, "plugin"),
            EntityType::Config => write!(f, "config"),
            EntityType::Operator => write!(f, "operator"),
            EntityType::Session => write!(f, "session"),
        }
    }
}

// -- Intervention identification --

/// Unique identifier for an intervention request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InterventionId(pub String);

impl Default for InterventionId {
    fn default() -> Self {
        Self::new()
    }
}

impl InterventionId {
    pub fn new() -> Self {
        Self(format!("intv-{}", Uuid::new_v4().as_simple()))
    }
}

impl fmt::Display for InterventionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The six intervention tiers, ordered by blast radius.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterventionTier {
    /// Tier 1: Node lifecycle (drain, cordon, quarantine) via FleetManager
    NodeLifecycle = 1,
    /// Tier 2: Work control (pause chunk, cancel job) via WorkEngine
    WorkControl = 2,
    /// Tier 3: Organic protocols (eject, override award, adjust rep)
    Organic = 3,
    /// Tier 4: Neuromancer (checkpoint, honeypot, threshold override)
    Neuromancer = 4,
    /// Tier 5: Plugin management (restart, drain connections)
    Plugin = 5,
    /// Tier 6: Configuration (hot-reload, gossip tuning)
    Configuration = 6,
}

impl fmt::Display for InterventionTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeLifecycle => write!(f, "tier1:node_lifecycle"),
            Self::WorkControl => write!(f, "tier2:work_control"),
            Self::Organic => write!(f, "tier3:organic"),
            Self::Neuromancer => write!(f, "tier4:neuromancer"),
            Self::Plugin => write!(f, "tier5:plugin"),
            Self::Configuration => write!(f, "tier6:configuration"),
        }
    }
}

/// The action within a tier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InterventionAction {
    pub tier: InterventionTier,
    pub name: String,
}

impl fmt::Display for InterventionAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.tier, self.name)
    }
}

/// Reversibility classification for interventions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reversibility {
    /// Can be undone automatically (e.g., cordon->uncordon, parameter change->revert).
    Reversible,
    /// Effects dissipate on their own (e.g., drain completes, then node returns to normal).
    SelfResolving,
    /// Cannot be undone without external action (e.g., kill, eject from collective).
    Irreversible,
}

/// Urgency classification that determines staleness tolerance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterventionUrgency {
    /// Emergency: 30s staleness tolerance (quarantine, kill).
    Emergency,
    /// Operational: 5s staleness tolerance (drain, cordon).
    Operational,
    /// Precise: 500ms staleness tolerance (set parameter, reassign chunk).
    Precise,
}

impl InterventionUrgency {
    /// Default staleness window for this urgency class.
    pub fn default_staleness_ms(&self) -> u64 {
        match self {
            Self::Emergency => 30_000,
            Self::Operational => 5_000,
            Self::Precise => 500,
        }
    }
}

// -- Observation types --

/// Subscription identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(pub String);

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionId {
    pub fn new() -> Self {
        Self(format!("sub-{}", Uuid::new_v4().as_simple()))
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Observation mode for querying entity state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationMode {
    /// Live streaming via EventBus.
    Stream,
    /// Historical query (KnowledgeStore + PG time-travel).
    Query { at: Option<DateTime<Utc>> },
    /// Causal trace by correlation_id.
    Trace { correlation_id: String },
}

// -- Operator identification --

/// Operator identifier (distinct from NodeId — operators are humans/scripts).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OperatorId(pub String);

impl OperatorId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl fmt::Display for OperatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Incident session identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub String);

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionId {
    pub fn new() -> Self {
        Self(format!("sess-{}", Uuid::new_v4().as_simple()))
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_entity_ref_display() {
        let er = EntityRef {
            entity_type: EntityType::Node,
            id: "abc123".to_string(),
        };
        assert_eq!(er.to_string(), "node:abc123");

        let er2 = EntityRef {
            entity_type: EntityType::MarketplacePosting,
            id: "post-1".to_string(),
        };
        assert_eq!(er2.to_string(), "marketplace_posting:post-1");
    }

    #[test]
    fn test_intervention_id_uniqueness() {
        let ids: HashSet<String> = (0..100).map(|_| InterventionId::new().0).collect();
        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn test_intervention_tier_ordering() {
        assert!(InterventionTier::NodeLifecycle < InterventionTier::WorkControl);
        assert!(InterventionTier::WorkControl < InterventionTier::Organic);
        assert!(InterventionTier::Organic < InterventionTier::Neuromancer);
        assert!(InterventionTier::Neuromancer < InterventionTier::Plugin);
        assert!(InterventionTier::Plugin < InterventionTier::Configuration);
    }

    #[test]
    fn test_urgency_staleness_defaults() {
        assert_eq!(InterventionUrgency::Emergency.default_staleness_ms(), 30_000);
        assert_eq!(InterventionUrgency::Operational.default_staleness_ms(), 5_000);
        assert_eq!(InterventionUrgency::Precise.default_staleness_ms(), 500);
    }

    #[test]
    fn test_entity_ref_serde_roundtrip() {
        let er = EntityRef {
            entity_type: EntityType::Job,
            id: "job-42".to_string(),
        };
        let json = serde_json::to_string(&er).expect("serialize");
        let back: EntityRef = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(er, back);
    }

    #[test]
    fn test_subscription_id_uniqueness() {
        let ids: HashSet<String> = (0..100).map(|_| SubscriptionId::new().0).collect();
        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn test_session_id_uniqueness() {
        let ids: HashSet<String> = (0..100).map(|_| SessionId::new().0).collect();
        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn test_intervention_action_display() {
        let action = InterventionAction {
            tier: InterventionTier::NodeLifecycle,
            name: "drain".to_string(),
        };
        assert_eq!(action.to_string(), "tier1:node_lifecycle:drain");
    }

    #[test]
    fn test_observation_mode_serde_roundtrip() {
        let modes = vec![
            ObservationMode::Stream,
            ObservationMode::Query { at: None },
            ObservationMode::Trace { correlation_id: "corr-1".to_string() },
        ];
        for mode in modes {
            let json = serde_json::to_string(&mode).expect("serialize");
            let back: ObservationMode = serde_json::from_str(&json).expect("deserialize");
            let json2 = serde_json::to_string(&back).expect("re-serialize");
            assert_eq!(json, json2);
        }
    }
}
