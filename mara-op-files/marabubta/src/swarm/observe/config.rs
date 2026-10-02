// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};

/// Top-level OAI configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct OaiConfig {
    /// Observation settings.
    pub observation: ObservationConfig,
    /// Guard and staleness settings.
    pub guard: GuardConfig,
    /// Intervention lock settings.
    pub lock: LockConfig,
    /// Operator defaults.
    pub operator: OperatorDefaults,
    /// PG backbone settings.
    pub pg: PgBackboneConfig,
    /// Impact assessment thresholds.
    pub impact: ImpactConfig,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservationConfig {
    /// Max managed subscriptions per operator.
    pub max_subscriptions_per_operator: usize,
    /// Subscription channel buffer size (backpressure: drop-oldest on overflow).
    pub subscription_buffer_size: usize,
    /// Whether to bridge NeuromancerBus events into EventBus.
    pub bridge_neuromancer_events: bool,
}

impl Default for ObservationConfig {
    fn default() -> Self {
        Self {
            max_subscriptions_per_operator: 10,
            subscription_buffer_size: 1024,
            bridge_neuromancer_events: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GuardConfig {
    /// Emergency staleness window in ms (quarantine, kill).
    pub emergency_staleness_ms: u64,
    /// Operational staleness window in ms (drain, cordon).
    pub operational_staleness_ms: u64,
    /// Precise staleness window in ms (set parameter, reassign).
    pub precise_staleness_ms: u64,
    /// Whether auto-generated guards are enabled by default.
    pub auto_guard_enabled: bool,
}

impl Default for GuardConfig {
    fn default() -> Self {
        Self {
            emergency_staleness_ms: 30_000,
            operational_staleness_ms: 5_000,
            precise_staleness_ms: 500,
            auto_guard_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LockConfig {
    /// Default lock TTL in ms for non-emergency operations.
    pub default_ttl_ms: u64,
    /// Emergency lock TTL in ms.
    pub emergency_ttl_ms: u64,
    /// Lock acquisition timeout in ms.
    pub acquire_timeout_ms: u64,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            default_ttl_ms: 30_000,
            emergency_ttl_ms: 10_000,
            acquire_timeout_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OperatorDefaults {
    /// Default safety tier for new operators (0=unrestricted, 3=supervised).
    pub default_safety_tier: u8,
    /// Default scope for new operators.
    pub default_scope: String,
}

impl Default for OperatorDefaults {
    fn default() -> Self {
        Self {
            default_safety_tier: 2,
            default_scope: "own".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PgBackboneConfig {
    /// Whether to persist events to PG.
    pub persist_events: bool,
    /// Batch size for PG event writes.
    pub event_batch_size: usize,
    /// Flush interval for PG event writer in ms.
    pub event_flush_interval_ms: u64,
    /// Retention period for hot data in days.
    pub hot_retention_days: u32,
    /// Retention period for warm data in days.
    pub warm_retention_days: u32,
}

impl Default for PgBackboneConfig {
    fn default() -> Self {
        Self {
            persist_events: true,
            event_batch_size: 100,
            event_flush_interval_ms: 1_000,
            hot_retention_days: 7,
            warm_retention_days: 90,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ImpactConfig {
    /// Maximum percentage of nodes affected by a single intervention.
    /// With maxNodes=4, this means max 1 node (25%).
    pub max_blast_radius_pct: f64,
    /// Minimum nodes that must remain operational after intervention.
    /// With maxNodes=4, this should be at least 2.
    pub minimum_viable_nodes: usize,
    /// Whether to enable optimistic execution for reversible interventions.
    pub optimistic_execution: bool,
    /// Post-condition verification timeout in ms.
    pub post_condition_timeout_ms: u64,
}

impl Default for ImpactConfig {
    fn default() -> Self {
        Self {
            max_blast_radius_pct: 25.0,
            minimum_viable_nodes: 2,
            optimistic_execution: true,
            post_condition_timeout_ms: 10_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = OaiConfig::default();
        assert_eq!(cfg.observation.max_subscriptions_per_operator, 10);
        assert_eq!(cfg.observation.subscription_buffer_size, 1024);
        assert!(cfg.observation.bridge_neuromancer_events);
        assert_eq!(cfg.guard.emergency_staleness_ms, 30_000);
        assert_eq!(cfg.guard.operational_staleness_ms, 5_000);
        assert_eq!(cfg.guard.precise_staleness_ms, 500);
        assert!(cfg.guard.auto_guard_enabled);
        assert_eq!(cfg.lock.default_ttl_ms, 30_000);
        assert_eq!(cfg.lock.emergency_ttl_ms, 10_000);
        assert_eq!(cfg.lock.acquire_timeout_ms, 5_000);
        assert_eq!(cfg.operator.default_safety_tier, 2);
        assert_eq!(cfg.operator.default_scope, "own");
        assert!(cfg.pg.persist_events);
        assert_eq!(cfg.pg.event_batch_size, 100);
        assert_eq!(cfg.pg.event_flush_interval_ms, 1_000);
        assert_eq!(cfg.pg.hot_retention_days, 7);
        assert_eq!(cfg.pg.warm_retention_days, 90);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = OaiConfig::default();
        let json = serde_json::to_string(&cfg).expect("serialize");
        let back: OaiConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.impact.max_blast_radius_pct, 25.0);
        assert_eq!(back.impact.minimum_viable_nodes, 2);
    }

    #[test]
    fn test_config_serde_with_missing_fields() {
        let json = r#"{"guard": {"emergency_staleness_ms": 60000}}"#;
        let cfg: OaiConfig = serde_json::from_str(json).expect("deserialize partial");
        assert_eq!(cfg.guard.emergency_staleness_ms, 60_000);
        // Other guard fields should be defaults
        assert_eq!(cfg.guard.operational_staleness_ms, 5_000);
        assert_eq!(cfg.guard.precise_staleness_ms, 500);
        // Other sections should be defaults
        assert_eq!(cfg.impact.max_blast_radius_pct, 25.0);
        assert_eq!(cfg.observation.max_subscriptions_per_operator, 10);
    }

    #[test]
    fn test_maxnodes4_impact_defaults() {
        let cfg = ImpactConfig::default();
        assert_eq!(cfg.max_blast_radius_pct, 25.0);
        assert_eq!(cfg.minimum_viable_nodes, 2);
        assert!(cfg.optimistic_execution);
        assert_eq!(cfg.post_condition_timeout_ms, 10_000);
    }
}
