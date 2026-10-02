// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use super::errors::OaiError;
use super::types::{InterventionTier, OperatorId};

/// Safety tier controlling how much friction the system applies.
///
/// Higher = more friction. See spec 04 and 12.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
#[derive(Default)]
pub enum SafetyTier {
    /// No warnings, no blocks. For automated scripts and experts.
    Unrestricted = 0,
    /// Informed: see warnings but no hard blocks.
    Informed = 1,
    /// Guarded: hard blocks for dangerous ops, overridable with --force.
    #[default]
    Guarded = 2,
    /// Supervised: hard blocks, no --force override available.
    Supervised = 3,
}

impl SafetyTier {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Unrestricted),
            1 => Some(Self::Informed),
            2 => Some(Self::Guarded),
            3 => Some(Self::Supervised),
            _ => None,
        }
    }

    /// Whether --force can override hard blocks at this tier.
    pub fn force_available(&self) -> bool {
        matches!(self, Self::Unrestricted | Self::Informed | Self::Guarded)
    }
}


/// Scope controlling which entities an operator can affect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum OperatorScope {
    /// Can only affect entities they created (jobs they submitted, etc.).
    #[default]
    Own,
    /// Can affect entities created by any member of their team.
    Team { team_id: String },
    /// Can affect any entity in the swarm.
    All,
}


/// Per-tier permission bitmask. Each bit enables a tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierPermissions {
    tiers: [bool; 6],
}

impl TierPermissions {
    /// No permissions.
    pub fn none() -> Self {
        Self { tiers: [false; 6] }
    }

    /// All permissions.
    pub fn all() -> Self {
        Self { tiers: [true; 6] }
    }

    /// Read-only (no tier permissions at all — observation only).
    pub fn observer() -> Self {
        Self::none()
    }

    /// Tier 1-2 only (node lifecycle + work control).
    pub fn operational() -> Self {
        let mut t = Self::none();
        t.tiers[0] = true;
        t.tiers[1] = true;
        t
    }

    pub fn allows(&self, tier: InterventionTier) -> bool {
        let idx = (tier as usize).saturating_sub(1);
        idx < 6 && self.tiers[idx]
    }

    pub fn set(&mut self, tier: InterventionTier, allowed: bool) {
        let idx = (tier as usize).saturating_sub(1);
        if idx < 6 {
            self.tiers[idx] = allowed;
        }
    }
}

impl Default for TierPermissions {
    fn default() -> Self {
        Self::operational()
    }
}

/// Optional parameter boundaries restricting operator's config changes.
///
/// Example: an operator can set `gossip_interval_ms` but only within [500, 5000].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterBoundary {
    pub parameter: String,
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
}

/// Full operator profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operator {
    pub id: OperatorId,
    pub name: String,
    pub team: Option<String>,
    pub safety_tier: SafetyTier,
    pub scope: OperatorScope,
    pub tier_permissions: TierPermissions,
    pub parameter_boundaries: Vec<ParameterBoundary>,
    pub created_at: DateTime<Utc>,
    pub last_active: DateTime<Utc>,
}

impl Operator {
    /// Check if this operator can perform an intervention on the given tier.
    pub fn check_tier_permission(&self, tier: InterventionTier) -> Result<(), OaiError> {
        if self.tier_permissions.allows(tier) {
            Ok(())
        } else {
            Err(OaiError::TierPermissionDenied {
                operator: self.id.clone(),
                tier,
            })
        }
    }

    /// Check if this operator's scope covers the given entity.
    ///
    /// `entity_owner`: the OperatorId that created this entity (if known).
    /// `entity_team`: the team that owns this entity (if known).
    pub fn check_scope(
        &self,
        entity: &super::types::EntityRef,
        entity_owner: Option<&OperatorId>,
        entity_team: Option<&str>,
    ) -> Result<(), OaiError> {
        match &self.scope {
            OperatorScope::All => Ok(()),
            OperatorScope::Team { team_id } => {
                if entity_team == Some(team_id.as_str()) {
                    Ok(())
                } else if entity_owner == Some(&self.id) {
                    Ok(())
                } else {
                    Err(OaiError::ScopeViolation {
                        operator: self.id.clone(),
                        entity: entity.clone(),
                        operator_scope: format!("team:{}", team_id),
                    })
                }
            }
            OperatorScope::Own => {
                if entity_owner == Some(&self.id) {
                    Ok(())
                } else {
                    Err(OaiError::ScopeViolation {
                        operator: self.id.clone(),
                        entity: entity.clone(),
                        operator_scope: "own".to_string(),
                    })
                }
            }
        }
    }

    /// Check a parameter boundary.
    pub fn check_parameter_boundary(
        &self,
        parameter: &str,
        value: &serde_json::Value,
    ) -> Result<(), OaiError> {
        for b in &self.parameter_boundaries {
            if b.parameter == parameter {
                if let (Some(v), Some(min)) = (value.as_f64(), b.min.as_ref().and_then(|m| m.as_f64())) {
                    if v < min {
                        return Err(OaiError::ParameterBoundaryExceeded {
                            parameter: parameter.to_string(),
                            requested: value.clone(),
                            boundary_min: b.min.clone(),
                            boundary_max: b.max.clone(),
                        });
                    }
                }
                if let (Some(v), Some(max)) = (value.as_f64(), b.max.as_ref().and_then(|m| m.as_f64())) {
                    if v > max {
                        return Err(OaiError::ParameterBoundaryExceeded {
                            parameter: parameter.to_string(),
                            requested: value.clone(),
                            boundary_min: b.min.clone(),
                            boundary_max: b.max.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

/// Shipped operator profile presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfilePreset {
    /// Read-only observation. No intervention permissions.
    Spectator,
    /// Observation + Tier 1-2 interventions. Guarded safety.
    Researcher,
    /// Observation + Tier 1-3 interventions. Guarded safety.
    Engineer,
    /// Full Tier 1-5 access. Informed safety.
    PlatformAdmin,
    /// Full access including Tier 4 (Neuromancer). Informed safety.
    SecurityOps,
    /// Unrestricted. All tiers, all scopes, no blocks.
    Unrestricted,
}

impl ProfilePreset {
    /// Create an Operator with this preset's defaults.
    pub fn to_operator(&self, id: OperatorId, name: String) -> Operator {
        let (safety, scope, perms) = match self {
            Self::Spectator => (
                SafetyTier::Supervised,
                OperatorScope::All,
                TierPermissions::none(),
            ),
            Self::Researcher => (
                SafetyTier::Guarded,
                OperatorScope::Own,
                TierPermissions::operational(),
            ),
            Self::Engineer => {
                let mut p = TierPermissions::operational();
                p.set(InterventionTier::Organic, true);
                (SafetyTier::Guarded, OperatorScope::All, p)
            }
            Self::PlatformAdmin => {
                let mut p = TierPermissions::all();
                p.set(InterventionTier::Neuromancer, false);
                (SafetyTier::Informed, OperatorScope::All, p)
            }
            Self::SecurityOps => (
                SafetyTier::Informed,
                OperatorScope::All,
                TierPermissions::all(),
            ),
            Self::Unrestricted => (
                SafetyTier::Unrestricted,
                OperatorScope::All,
                TierPermissions::all(),
            ),
        };

        let now = Utc::now();
        Operator {
            id,
            name,
            team: None,
            safety_tier: safety,
            scope,
            tier_permissions: perms,
            parameter_boundaries: Vec::new(),
            created_at: now,
            last_active: now,
        }
    }
}

/// In-memory store for operator profiles. Thread-safe via DashMap.
pub struct OperatorStore {
    operators: DashMap<OperatorId, Operator>,
}

impl Default for OperatorStore {
    fn default() -> Self {
        Self::new()
    }
}

impl OperatorStore {
    pub fn new() -> Self {
        Self {
            operators: DashMap::new(),
        }
    }

    /// Register an operator from a preset.
    pub fn register_from_preset(
        &self,
        id: OperatorId,
        name: String,
        preset: ProfilePreset,
    ) -> Operator {
        let op = preset.to_operator(id.clone(), name);
        self.operators.insert(id, op.clone());
        op
    }

    /// Register a fully customized operator.
    pub fn register(&self, operator: Operator) {
        self.operators.insert(operator.id.clone(), operator);
    }

    /// Get an operator by ID.
    pub fn get(&self, id: &OperatorId) -> Option<Operator> {
        self.operators.get(id).map(|r| r.value().clone())
    }

    /// Update an operator's last_active timestamp.
    pub fn touch(&self, id: &OperatorId) {
        if let Some(mut op) = self.operators.get_mut(id) {
            op.last_active = Utc::now();
        }
    }

    /// Remove an operator.
    pub fn remove(&self, id: &OperatorId) -> Option<Operator> {
        self.operators.remove(id).map(|(_, v)| v)
    }

    /// List all operators.
    pub fn list(&self) -> Vec<Operator> {
        self.operators.iter().map(|r| r.value().clone()).collect()
    }

    /// Count operators.
    pub fn count(&self) -> usize {
        self.operators.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::observe::types::{EntityRef, EntityType};

    fn make_entity(etype: EntityType, id: &str) -> EntityRef {
        EntityRef { entity_type: etype, id: id.to_string() }
    }

    #[test]
    fn test_safety_tier_ordering() {
        assert!(SafetyTier::Unrestricted < SafetyTier::Informed);
        assert!(SafetyTier::Informed < SafetyTier::Guarded);
        assert!(SafetyTier::Guarded < SafetyTier::Supervised);
    }

    #[test]
    fn test_safety_tier_from_u8() {
        assert_eq!(SafetyTier::from_u8(0), Some(SafetyTier::Unrestricted));
        assert_eq!(SafetyTier::from_u8(1), Some(SafetyTier::Informed));
        assert_eq!(SafetyTier::from_u8(2), Some(SafetyTier::Guarded));
        assert_eq!(SafetyTier::from_u8(3), Some(SafetyTier::Supervised));
        assert_eq!(SafetyTier::from_u8(4), None);
        assert_eq!(SafetyTier::from_u8(255), None);
    }

    #[test]
    fn test_safety_tier_force_available() {
        assert!(SafetyTier::Unrestricted.force_available());
        assert!(SafetyTier::Informed.force_available());
        assert!(SafetyTier::Guarded.force_available());
        assert!(!SafetyTier::Supervised.force_available());
    }

    #[test]
    fn test_tier_permissions_none() {
        let p = TierPermissions::none();
        assert!(!p.allows(InterventionTier::NodeLifecycle));
        assert!(!p.allows(InterventionTier::WorkControl));
        assert!(!p.allows(InterventionTier::Organic));
        assert!(!p.allows(InterventionTier::Neuromancer));
        assert!(!p.allows(InterventionTier::Plugin));
        assert!(!p.allows(InterventionTier::Configuration));
    }

    #[test]
    fn test_tier_permissions_all() {
        let p = TierPermissions::all();
        assert!(p.allows(InterventionTier::NodeLifecycle));
        assert!(p.allows(InterventionTier::WorkControl));
        assert!(p.allows(InterventionTier::Organic));
        assert!(p.allows(InterventionTier::Neuromancer));
        assert!(p.allows(InterventionTier::Plugin));
        assert!(p.allows(InterventionTier::Configuration));
    }

    #[test]
    fn test_tier_permissions_operational() {
        let p = TierPermissions::operational();
        assert!(p.allows(InterventionTier::NodeLifecycle));
        assert!(p.allows(InterventionTier::WorkControl));
        assert!(!p.allows(InterventionTier::Organic));
        assert!(!p.allows(InterventionTier::Neuromancer));
        assert!(!p.allows(InterventionTier::Plugin));
        assert!(!p.allows(InterventionTier::Configuration));
    }

    #[test]
    fn test_preset_spectator() {
        let op = ProfilePreset::Spectator.to_operator(OperatorId::new("s1"), "Spectator".into());
        assert_eq!(op.safety_tier, SafetyTier::Supervised);
        assert_eq!(op.scope, OperatorScope::All);
        assert!(!op.tier_permissions.allows(InterventionTier::NodeLifecycle));
        assert!(!op.tier_permissions.allows(InterventionTier::Configuration));
    }

    #[test]
    fn test_preset_researcher() {
        let op = ProfilePreset::Researcher.to_operator(OperatorId::new("r1"), "Researcher".into());
        assert_eq!(op.safety_tier, SafetyTier::Guarded);
        assert_eq!(op.scope, OperatorScope::Own);
        assert!(op.tier_permissions.allows(InterventionTier::NodeLifecycle));
        assert!(op.tier_permissions.allows(InterventionTier::WorkControl));
        assert!(!op.tier_permissions.allows(InterventionTier::Organic));
    }

    #[test]
    fn test_preset_engineer() {
        let op = ProfilePreset::Engineer.to_operator(OperatorId::new("e1"), "Engineer".into());
        assert_eq!(op.safety_tier, SafetyTier::Guarded);
        assert_eq!(op.scope, OperatorScope::All);
        assert!(op.tier_permissions.allows(InterventionTier::NodeLifecycle));
        assert!(op.tier_permissions.allows(InterventionTier::WorkControl));
        assert!(op.tier_permissions.allows(InterventionTier::Organic));
        assert!(!op.tier_permissions.allows(InterventionTier::Neuromancer));
    }

    #[test]
    fn test_preset_platform_admin() {
        let op = ProfilePreset::PlatformAdmin.to_operator(OperatorId::new("pa1"), "Admin".into());
        assert_eq!(op.safety_tier, SafetyTier::Informed);
        assert_eq!(op.scope, OperatorScope::All);
        assert!(op.tier_permissions.allows(InterventionTier::NodeLifecycle));
        assert!(op.tier_permissions.allows(InterventionTier::Plugin));
        assert!(op.tier_permissions.allows(InterventionTier::Configuration));
        assert!(!op.tier_permissions.allows(InterventionTier::Neuromancer));
    }

    #[test]
    fn test_preset_security_ops() {
        let op = ProfilePreset::SecurityOps.to_operator(OperatorId::new("so1"), "SecOps".into());
        assert_eq!(op.safety_tier, SafetyTier::Informed);
        assert_eq!(op.scope, OperatorScope::All);
        assert!(op.tier_permissions.allows(InterventionTier::Neuromancer));
        assert!(op.tier_permissions.allows(InterventionTier::Configuration));
    }

    #[test]
    fn test_preset_unrestricted() {
        let op = ProfilePreset::Unrestricted.to_operator(OperatorId::new("u1"), "Root".into());
        assert_eq!(op.safety_tier, SafetyTier::Unrestricted);
        assert_eq!(op.scope, OperatorScope::All);
        assert!(op.tier_permissions.allows(InterventionTier::NodeLifecycle));
        assert!(op.tier_permissions.allows(InterventionTier::Neuromancer));
        assert!(op.tier_permissions.allows(InterventionTier::Configuration));
    }

    #[test]
    fn test_check_tier_permission_allowed() {
        let op = ProfilePreset::Engineer.to_operator(OperatorId::new("e1"), "Eng".into());
        assert!(op.check_tier_permission(InterventionTier::NodeLifecycle).is_ok());
        assert!(op.check_tier_permission(InterventionTier::Organic).is_ok());
    }

    #[test]
    fn test_check_tier_permission_denied() {
        let op = ProfilePreset::Spectator.to_operator(OperatorId::new("s1"), "Spec".into());
        assert!(op.check_tier_permission(InterventionTier::NodeLifecycle).is_err());
        assert!(op.check_tier_permission(InterventionTier::Configuration).is_err());
    }

    #[test]
    fn test_check_scope_own() {
        let op = ProfilePreset::Researcher.to_operator(OperatorId::new("r1"), "Res".into());
        let entity = make_entity(EntityType::Job, "j1");

        // Own entity -> ok
        assert!(op.check_scope(&entity, Some(&OperatorId::new("r1")), None).is_ok());
        // Other's entity -> denied
        assert!(op.check_scope(&entity, Some(&OperatorId::new("other")), None).is_err());
        // No owner -> denied
        assert!(op.check_scope(&entity, None, None).is_err());
    }

    #[test]
    fn test_check_scope_team() {
        let op = Operator {
            id: OperatorId::new("t1"),
            name: "Team Op".into(),
            team: Some("alpha".into()),
            safety_tier: SafetyTier::Guarded,
            scope: OperatorScope::Team { team_id: "alpha".into() },
            tier_permissions: TierPermissions::all(),
            parameter_boundaries: vec![],
            created_at: Utc::now(),
            last_active: Utc::now(),
        };
        let entity = make_entity(EntityType::Node, "n1");

        // Same team -> ok
        assert!(op.check_scope(&entity, None, Some("alpha")).is_ok());
        // Different team -> denied
        assert!(op.check_scope(&entity, None, Some("beta")).is_err());
        // Own entity, wrong team -> ok (own always passes)
        assert!(op.check_scope(&entity, Some(&OperatorId::new("t1")), Some("beta")).is_ok());
    }

    #[test]
    fn test_check_scope_all() {
        let op = ProfilePreset::Unrestricted.to_operator(OperatorId::new("u1"), "Root".into());
        let entity = make_entity(EntityType::Node, "any-node");
        assert!(op.check_scope(&entity, None, None).is_ok());
        assert!(op.check_scope(&entity, Some(&OperatorId::new("other")), Some("beta")).is_ok());
    }

    #[test]
    fn test_parameter_boundary_within() {
        let op = Operator {
            id: OperatorId::new("b1"),
            name: "Bounded".into(),
            team: None,
            safety_tier: SafetyTier::Guarded,
            scope: OperatorScope::All,
            tier_permissions: TierPermissions::all(),
            parameter_boundaries: vec![
                ParameterBoundary {
                    parameter: "gossip_interval_ms".into(),
                    min: Some(serde_json::json!(500)),
                    max: Some(serde_json::json!(5000)),
                },
            ],
            created_at: Utc::now(),
            last_active: Utc::now(),
        };
        assert!(op.check_parameter_boundary("gossip_interval_ms", &serde_json::json!(1000)).is_ok());
        assert!(op.check_parameter_boundary("gossip_interval_ms", &serde_json::json!(500)).is_ok());
        assert!(op.check_parameter_boundary("gossip_interval_ms", &serde_json::json!(5000)).is_ok());
        // Unknown parameter -> ok (no boundary defined)
        assert!(op.check_parameter_boundary("unknown_param", &serde_json::json!(999999)).is_ok());
    }

    #[test]
    fn test_parameter_boundary_below_min() {
        let op = Operator {
            id: OperatorId::new("b2"),
            name: "Bounded".into(),
            team: None,
            safety_tier: SafetyTier::Guarded,
            scope: OperatorScope::All,
            tier_permissions: TierPermissions::all(),
            parameter_boundaries: vec![
                ParameterBoundary {
                    parameter: "gossip_interval_ms".into(),
                    min: Some(serde_json::json!(500)),
                    max: Some(serde_json::json!(5000)),
                },
            ],
            created_at: Utc::now(),
            last_active: Utc::now(),
        };
        assert!(op.check_parameter_boundary("gossip_interval_ms", &serde_json::json!(100)).is_err());
    }

    #[test]
    fn test_parameter_boundary_above_max() {
        let op = Operator {
            id: OperatorId::new("b3"),
            name: "Bounded".into(),
            team: None,
            safety_tier: SafetyTier::Guarded,
            scope: OperatorScope::All,
            tier_permissions: TierPermissions::all(),
            parameter_boundaries: vec![
                ParameterBoundary {
                    parameter: "gossip_interval_ms".into(),
                    min: Some(serde_json::json!(500)),
                    max: Some(serde_json::json!(5000)),
                },
            ],
            created_at: Utc::now(),
            last_active: Utc::now(),
        };
        assert!(op.check_parameter_boundary("gossip_interval_ms", &serde_json::json!(10000)).is_err());
    }

    #[test]
    fn test_operator_store_crud() {
        let store = OperatorStore::new();
        assert_eq!(store.count(), 0);

        let op = store.register_from_preset(
            OperatorId::new("op1"),
            "Test Op".into(),
            ProfilePreset::Engineer,
        );
        assert_eq!(store.count(), 1);
        assert_eq!(op.name, "Test Op");

        let got = store.get(&OperatorId::new("op1")).expect("should exist");
        assert_eq!(got.name, "Test Op");
        assert_eq!(got.safety_tier, SafetyTier::Guarded);

        let all = store.list();
        assert_eq!(all.len(), 1);

        let removed = store.remove(&OperatorId::new("op1"));
        assert!(removed.is_some());
        assert_eq!(store.count(), 0);
        assert!(store.get(&OperatorId::new("op1")).is_none());
    }

    #[test]
    fn test_operator_store_touch_updates_last_active() {
        let store = OperatorStore::new();
        let op = store.register_from_preset(
            OperatorId::new("op1"),
            "Test".into(),
            ProfilePreset::Researcher,
        );
        let before = op.last_active;
        std::thread::sleep(std::time::Duration::from_millis(10));
        store.touch(&OperatorId::new("op1"));
        let after = store.get(&OperatorId::new("op1")).expect("exists").last_active;
        assert!(after >= before);
    }

    #[test]
    fn test_operator_serde_roundtrip() {
        let op = ProfilePreset::SecurityOps.to_operator(OperatorId::new("so1"), "SecOps".into());
        let json = serde_json::to_string(&op).expect("serialize");
        let back: Operator = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.id, op.id);
        assert_eq!(back.safety_tier, SafetyTier::Informed);
        assert!(back.tier_permissions.allows(InterventionTier::Neuromancer));
    }
}
