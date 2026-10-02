// Marabunta - Licensed under the MIT License.
//! Configuration metadata registry.
//!
//! Every setting in the swarm carries metadata: which tier it belongs to
//! (hardwired / startup / runtime), its category, UI visibility, value type,
//! constraints, and compliance tags. The [`ConfigRegistry`] is the single
//! source of truth for this metadata.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Core enums
// ============================================================================

/// Which tier a setting lives in.
///
/// The override chain is: Hardwired → Startup → Runtime.
/// Hardwired values are immutable walls set by compliance profiles at build
/// time. Startup values come from TOML / CLI. Runtime values are live-patched
/// via the API and persisted to PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigTier {
    /// Set by compliance profile at build time — cannot be changed.
    Hardwired,
    /// Loaded from TOML / CLI at startup — requires restart to change.
    Startup,
    /// Changeable at runtime via API — persisted to PostgreSQL.
    Runtime,
}

impl std::fmt::Display for ConfigTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hardwired => write!(f, "hardwired"),
            Self::Startup => write!(f, "startup"),
            Self::Runtime => write!(f, "runtime"),
        }
    }
}

/// How the setting appears in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiVisibility {
    /// Shown and editable (only for runtime-tier settings).
    VisibleMutable,
    /// Shown but read-only (startup or hardwired settings).
    VisibleReadonly,
    /// Not shown in the UI at all.
    Hidden,
}

impl std::fmt::Display for UiVisibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VisibleMutable => write!(f, "visible_mutable"),
            Self::VisibleReadonly => write!(f, "visible_readonly"),
            Self::Hidden => write!(f, "hidden"),
        }
    }
}

/// Logical category for grouping settings in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigCategory {
    Gossip,
    Failure,
    Transport,
    Work,
    Traits,
    Admission,
    Marketplace,
    Reputation,
    Neuromancer,
    Plugin,
    Security,
    Sandbox,
    Scheduling,
    Pricing,
    Energy,
    Display,
    Bootstrap,
    Knowledge,
    Profile,
    Collective,
    Policy,
    Api,
    Blob,
    Discovery,
    Relay,
    Checkpoint,
    Aggregation,
    Update,
    Management,
    Verification,
    Webhook,
    Witness,
    Postgres,
}

impl std::fmt::Display for ConfigCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_value(self)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| format!("{:?}", self));
        write!(f, "{}", s)
    }
}

/// The data type of a setting value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ValueType {
    String,
    Int,
    Float,
    Bool,
    /// Duration in human-readable form (e.g. "5s", "500ms").
    Duration,
    /// Enum with a fixed set of variants.
    Enum { variants: Vec<String> },
}

/// Constraints on a setting value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ValueConstraints {
    /// Minimum value (inclusive) for Int/Float/Duration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<serde_json::Value>,
    /// Maximum value (inclusive) for Int/Float/Duration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<serde_json::Value>,
    /// Regex pattern for String values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
}

// ============================================================================
// Setting metadata
// ============================================================================

/// Metadata for a single configuration setting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSettingMeta {
    /// Dotted key path (e.g. "gossip.interval", "failure.suspect_threshold").
    pub key: String,
    /// Which tier this setting belongs to by default.
    pub tier: ConfigTier,
    /// How the setting appears in the UI.
    pub ui_visibility: UiVisibility,
    /// Logical category for grouping.
    pub category: ConfigCategory,
    /// Human-readable description.
    pub description: String,
    /// Whether changing this value requires a restart.
    pub restart_required: bool,
    /// Compliance profiles that lock this setting (e.g. ["soc2", "hipaa"]).
    pub compliance_tags: Vec<String>,
    /// Data type of the value.
    pub value_type: ValueType,
    /// Constraints on the value.
    pub constraints: ValueConstraints,
    /// Default value as JSON.
    pub default_value: serde_json::Value,
}

/// A compliance override that locks a setting value at build time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceOverride {
    /// The config key this override applies to.
    pub key: String,
    /// The locked value.
    pub value: serde_json::Value,
    /// How it should appear in the UI.
    pub ui_visibility: UiVisibility,
    /// What kind of constraint this is (e.g. "min", "max", "exact").
    pub constraint_type: String,
    /// Human-readable justification for the lock.
    pub justification: String,
    /// Which compliance profile set this override.
    pub profile_name: String,
}

// ============================================================================
// Validation error
// ============================================================================

/// Errors from config validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ConfigValidationError {
    UnknownKey { key: String },
    TypeMismatch { key: String, expected: String, got: String },
    BelowMin { key: String, min: serde_json::Value, got: serde_json::Value },
    AboveMax { key: String, max: serde_json::Value, got: serde_json::Value },
    InvalidEnum { key: String, expected: Vec<String>, got: String },
    PatternMismatch { key: String, pattern: String, got: String },
    HardwiredLock { key: String, profile: String, justification: String },
}

impl std::fmt::Display for ConfigValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownKey { key } => write!(f, "unknown config key: {}", key),
            Self::TypeMismatch { key, expected, got } =>
                write!(f, "{}: expected {}, got {}", key, expected, got),
            Self::BelowMin { key, min, got } =>
                write!(f, "{}: value {} below minimum {}", key, got, min),
            Self::AboveMax { key, max, got } =>
                write!(f, "{}: value {} above maximum {}", key, got, max),
            Self::InvalidEnum { key, expected, got } =>
                write!(f, "{}: '{}' not in {:?}", key, got, expected),
            Self::PatternMismatch { key, pattern, got } =>
                write!(f, "{}: '{}' does not match pattern '{}'", key, got, pattern),
            Self::HardwiredLock { key, profile, justification } =>
                write!(f, "{}: locked by {} — {}", key, profile, justification),
        }
    }
}

impl std::error::Error for ConfigValidationError {}

// ============================================================================
// Config registry
// ============================================================================

/// Central registry of all configuration settings and their metadata.
///
/// Built once at startup, immutable thereafter. The registry does not store
/// values — it only describes them. Actual values live in `SwarmConfig` and
/// `LiveConfig`.
pub struct ConfigRegistry {
    settings: HashMap<String, ConfigSettingMeta>,
    overrides: Vec<ComplianceOverride>,
}

impl ConfigRegistry {
    /// Create a new registry with all known settings and optional compliance
    /// overrides.
    pub fn new(compliance_overrides: Vec<ComplianceOverride>) -> Self {
        let mut settings = HashMap::new();
        Self::register_all(&mut settings);
        Self {
            settings,
            overrides: compliance_overrides,
        }
    }

    /// Look up metadata for a setting by key.
    pub fn get(&self, key: &str) -> Option<&ConfigSettingMeta> {
        self.settings.get(key)
    }

    /// Check if a key is locked by a compliance override.
    pub fn is_hardwired(&self, key: &str) -> Option<&ComplianceOverride> {
        self.overrides.iter().find(|o| o.key == key)
    }

    /// Validate a proposed value for a setting key.
    pub fn validate(
        &self,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), ConfigValidationError> {
        // Check compliance locks first
        if let Some(ovr) = self.is_hardwired(key) {
            return Err(ConfigValidationError::HardwiredLock {
                key: key.to_string(),
                profile: ovr.profile_name.clone(),
                justification: ovr.justification.clone(),
            });
        }

        let meta = self.get(key).ok_or_else(|| ConfigValidationError::UnknownKey {
            key: key.to_string(),
        })?;

        // Type check
        match &meta.value_type {
            ValueType::Bool => {
                if !value.is_boolean() {
                    return Err(ConfigValidationError::TypeMismatch {
                        key: key.to_string(),
                        expected: "bool".into(),
                        got: type_name_of(value),
                    });
                }
            }
            ValueType::Int => {
                if !value.is_i64() && !value.is_u64() {
                    return Err(ConfigValidationError::TypeMismatch {
                        key: key.to_string(),
                        expected: "int".into(),
                        got: type_name_of(value),
                    });
                }
                Self::validate_numeric_range(key, value, &meta.constraints)?;
            }
            ValueType::Float => {
                if !value.is_f64() && !value.is_i64() && !value.is_u64() {
                    return Err(ConfigValidationError::TypeMismatch {
                        key: key.to_string(),
                        expected: "float".into(),
                        got: type_name_of(value),
                    });
                }
                Self::validate_numeric_range(key, value, &meta.constraints)?;
            }
            ValueType::String | ValueType::Duration => {
                if !value.is_string() {
                    return Err(ConfigValidationError::TypeMismatch {
                        key: key.to_string(),
                        expected: "string".into(),
                        got: type_name_of(value),
                    });
                }
                if let Some(ref pat) = meta.constraints.pattern {
                    let s = value.as_str().unwrap();
                    if regex::Regex::new(pat).is_ok_and(|re| !re.is_match(s)) {
                        return Err(ConfigValidationError::PatternMismatch {
                            key: key.to_string(),
                            pattern: pat.clone(),
                            got: s.to_string(),
                        });
                    }
                }
            }
            ValueType::Enum { variants } => {
                let s = value.as_str().ok_or_else(|| ConfigValidationError::TypeMismatch {
                    key: key.to_string(),
                    expected: "string (enum)".into(),
                    got: type_name_of(value),
                })?;
                if !variants.iter().any(|v| v == s) {
                    return Err(ConfigValidationError::InvalidEnum {
                        key: key.to_string(),
                        expected: variants.clone(),
                        got: s.to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Return all settings as a sorted vec (for schema API).
    pub fn schema(&self) -> Vec<&ConfigSettingMeta> {
        let mut entries: Vec<_> = self.settings.values().collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        entries
    }

    /// Return settings filtered by category.
    pub fn by_category(&self, cat: ConfigCategory) -> Vec<&ConfigSettingMeta> {
        let mut entries: Vec<_> = self.settings.values()
            .filter(|m| m.category == cat)
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        entries
    }

    /// Total number of registered settings.
    pub fn len(&self) -> usize {
        self.settings.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.settings.is_empty()
    }

    /// All compliance overrides.
    pub fn compliance_overrides(&self) -> &[ComplianceOverride] {
        &self.overrides
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    fn validate_numeric_range(
        key: &str,
        value: &serde_json::Value,
        constraints: &ValueConstraints,
    ) -> Result<(), ConfigValidationError> {
        let v = value.as_f64().unwrap_or(0.0);
        if let Some(ref min) = constraints.min {
            if let Some(m) = min.as_f64() {
                if v < m {
                    return Err(ConfigValidationError::BelowMin {
                        key: key.to_string(),
                        min: min.clone(),
                        got: value.clone(),
                    });
                }
            }
        }
        if let Some(ref max) = constraints.max {
            if let Some(m) = max.as_f64() {
                if v > m {
                    return Err(ConfigValidationError::AboveMax {
                        key: key.to_string(),
                        max: max.clone(),
                        got: value.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn register_all(m: &mut HashMap<String, ConfigSettingMeta>) {
        // Helper to reduce boilerplate
        macro_rules! reg {
            ($key:expr, $tier:expr, $vis:expr, $cat:expr, $desc:expr,
             $restart:expr, $vtype:expr, $default:expr $(, constraints: $constraints:expr)? $(, compliance: $compliance:expr)?) => {
                m.insert($key.to_string(), ConfigSettingMeta {
                    key: $key.to_string(),
                    tier: $tier,
                    ui_visibility: $vis,
                    category: $cat,
                    description: $desc.to_string(),
                    restart_required: $restart,
                    compliance_tags: { let mut _v = Vec::new(); $( _v.push($compliance.to_string()); )? _v },
                    value_type: $vtype,
                    constraints: { let _c = ValueConstraints::default(); $( let _c = $constraints; )? _c },
                    default_value: $default,
                });
            };
        }

        use ConfigCategory as C;
        use ConfigTier as T;
        use UiVisibility as V;
        use ValueType as VT;

        // ====================================================================
        // Gossip
        // ====================================================================
        reg!("gossip.interval", T::Runtime, V::VisibleMutable, C::Gossip,
            "How often each node sends gossip messages",
            false, VT::Duration, json!("1s"),
            constraints: ValueConstraints {
                min: Some(json!("100ms")), max: Some(json!("60s")), ..Default::default()
            });
        reg!("gossip.fanout", T::Runtime, V::VisibleMutable, C::Gossip,
            "Number of random peers to gossip with per round",
            false, VT::Int, json!(3),
            constraints: ValueConstraints {
                min: Some(json!(1)), max: Some(json!(20)), ..Default::default()
            });
        reg!("gossip.min_fanout", T::Startup, V::VisibleReadonly, C::Gossip,
            "Minimum fanout regardless of cluster size",
            true, VT::Int, json!(3));
        reg!("gossip.jitter_percent", T::Runtime, V::VisibleMutable, C::Gossip,
            "Jitter range for gossip interval (±%)",
            false, VT::Int, json!(20),
            constraints: ValueConstraints {
                min: Some(json!(0)), max: Some(json!(50)), ..Default::default()
            });
        reg!("gossip.same_region_ratio", T::Runtime, V::VisibleMutable, C::Gossip,
            "Fraction of gossip fanout directed to same-region peers",
            false, VT::Float, json!(0.7),
            constraints: ValueConstraints {
                min: Some(json!(0.0)), max: Some(json!(1.0)), ..Default::default()
            });
        reg!("gossip.max_nodes_per_message", T::Runtime, V::VisibleMutable, C::Gossip,
            "Maximum NodeInfo entries per gossip message",
            false, VT::Int, json!(50),
            constraints: ValueConstraints {
                min: Some(json!(5)), max: Some(json!(500)), ..Default::default()
            });
        reg!("gossip.max_jobs_per_message", T::Runtime, V::VisibleMutable, C::Gossip,
            "Maximum job entries per gossip message",
            false, VT::Int, json!(20));
        reg!("gossip.max_assignments_per_message", T::Runtime, V::VisibleMutable, C::Gossip,
            "Maximum assignment entries per gossip message",
            false, VT::Int, json!(100));
        reg!("gossip.max_message_size", T::Startup, V::VisibleReadonly, C::Gossip,
            "Maximum wire size of a single SwarmMessage (bytes)",
            true, VT::Int, json!(16777216));
        reg!("gossip.max_profiles_per_message", T::Runtime, V::VisibleMutable, C::Gossip,
            "Maximum profile entries per gossip message",
            false, VT::Int, json!(30));
        reg!("gossip.max_collectives_per_message", T::Runtime, V::VisibleMutable, C::Gossip,
            "Maximum collective entries per gossip message",
            false, VT::Int, json!(15));
        reg!("gossip.max_reputation_per_message", T::Runtime, V::VisibleMutable, C::Gossip,
            "Maximum reputation entries per gossip message",
            false, VT::Int, json!(50));
        reg!("gossip.max_postings_per_message", T::Runtime, V::VisibleMutable, C::Gossip,
            "Maximum marketplace posting entries per gossip message",
            false, VT::Int, json!(40));

        // ====================================================================
        // Failure detection
        // ====================================================================
        reg!("failure.suspect_threshold", T::Runtime, V::VisibleMutable, C::Failure,
            "After this silence, a node is marked Suspect",
            false, VT::Duration, json!("10s"),
            constraints: ValueConstraints {
                min: Some(json!("1s")), max: Some(json!("300s")), ..Default::default()
            });
        reg!("failure.dead_threshold", T::Runtime, V::VisibleMutable, C::Failure,
            "After this silence, a node is marked Dead and its work is reassigned",
            false, VT::Duration, json!("30s"),
            constraints: ValueConstraints {
                min: Some(json!("5s")), max: Some(json!("600s")), ..Default::default()
            });
        reg!("failure.node_timeout", T::Runtime, V::VisibleMutable, C::Failure,
            "After this silence, a node is evicted from the knowledge store",
            false, VT::Duration, json!("120s"));
        reg!("failure.check_interval", T::Runtime, V::VisibleMutable, C::Failure,
            "How often the failure detector sweeps",
            false, VT::Duration, json!("2s"));
        reg!("failure.quorum_fraction", T::Runtime, V::VisibleMutable, C::Failure,
            "Fraction of peers that must agree a node is unreachable before death",
            false, VT::Float, json!(0.5),
            constraints: ValueConstraints {
                min: Some(json!(0.1)), max: Some(json!(1.0)), ..Default::default()
            });
        reg!("failure.witness_decay_secs", T::Runtime, V::VisibleMutable, C::Failure,
            "Witness reports older than this are discarded during quorum evaluation",
            false, VT::Int, json!(60));
        reg!("failure.partition_recovery_rounds", T::Runtime, V::VisibleMutable, C::Failure,
            "Healthy gossip rounds required before partition is considered healed",
            false, VT::Int, json!(2));

        // ====================================================================
        // Transport
        // ====================================================================
        reg!("transport.connect_timeout", T::Runtime, V::VisibleMutable, C::Transport,
            "Timeout for establishing a TCP connection to a peer",
            false, VT::Duration, json!("5s"));
        reg!("transport.read_timeout", T::Runtime, V::VisibleMutable, C::Transport,
            "Timeout for reading a complete message frame",
            false, VT::Duration, json!("10s"));
        reg!("transport.idle_timeout", T::Runtime, V::VisibleMutable, C::Transport,
            "Maximum idle time before closing an unused connection",
            false, VT::Duration, json!("60s"));
        reg!("transport.max_outbound_connections", T::Runtime, V::VisibleMutable, C::Transport,
            "Maximum number of concurrent outbound connections",
            false, VT::Int, json!(256),
            constraints: ValueConstraints {
                min: Some(json!(8)), max: Some(json!(4096)), ..Default::default()
            });
        reg!("transport.listen_backlog", T::Startup, V::VisibleReadonly, C::Transport,
            "TCP listen backlog",
            true, VT::Int, json!(512));

        // ====================================================================
        // Work distribution
        // ====================================================================
        reg!("work.max_load", T::Runtime, V::VisibleMutable, C::Work,
            "Maximum load before a node stops accepting new chunks",
            false, VT::Float, json!(0.8),
            constraints: ValueConstraints {
                min: Some(json!(0.1)), max: Some(json!(1.0)), ..Default::default()
            });
        reg!("work.check_interval", T::Runtime, V::VisibleMutable, C::Work,
            "How often the work loop checks for available chunks",
            false, VT::Duration, json!("500ms"));
        reg!("work.chunk_timeout", T::Runtime, V::VisibleMutable, C::Work,
            "Default timeout for chunk execution",
            false, VT::Duration, json!("300s"));
        reg!("work.max_chunk_attempts", T::Runtime, V::VisibleMutable, C::Work,
            "Maximum attempts before a chunk is marked permanently failed",
            false, VT::Int, json!(3),
            constraints: ValueConstraints {
                min: Some(json!(1)), max: Some(json!(20)), ..Default::default()
            });
        reg!("work.max_concurrent_chunks", T::Runtime, V::VisibleMutable, C::Work,
            "Maximum number of chunks a single node can execute concurrently",
            false, VT::Int, json!(4),
            constraints: ValueConstraints {
                min: Some(json!(1)), max: Some(json!(64)), ..Default::default()
            });
        reg!("work.conflict_backoff", T::Runtime, V::VisibleMutable, C::Work,
            "How long to wait after a conflict-resolution loss before reclaiming work",
            false, VT::Duration, json!("200ms"));
        reg!("work.max_pending_chunks", T::Runtime, V::VisibleMutable, C::Work,
            "Maximum number of pending chunks in the queue",
            false, VT::Int, json!(10000),
            constraints: ValueConstraints {
                min: Some(json!(100)), max: Some(json!(1000000)), ..Default::default()
            });

        // ====================================================================
        // Trait evaluation
        // ====================================================================
        reg!("traits.eval_interval", T::Runtime, V::VisibleMutable, C::Traits,
            "How often trait evaluation runs",
            false, VT::Duration, json!("5s"));
        reg!("traits.jitter_percent", T::Runtime, V::VisibleMutable, C::Traits,
            "Jitter range for trait evaluation interval (±%)",
            false, VT::Int, json!(20));
        reg!("traits.threshold_execute_cpu", T::Runtime, V::VisibleMutable, C::Traits,
            "Minimum CPU availability fraction to claim can_execute",
            false, VT::Float, json!(0.1),
            constraints: ValueConstraints {
                min: Some(json!(0.0)), max: Some(json!(1.0)), ..Default::default()
            });
        reg!("traits.threshold_aggregate_ram_mb", T::Runtime, V::VisibleMutable, C::Traits,
            "Minimum available RAM (MB) to claim can_aggregate",
            false, VT::Int, json!(512));
        reg!("traits.threshold_forward_load", T::Runtime, V::VisibleMutable, C::Traits,
            "Maximum load to claim can_forward",
            false, VT::Float, json!(0.5));
        reg!("traits.threshold_store_disk_mb", T::Runtime, V::VisibleMutable, C::Traits,
            "Minimum available disk (MB) to claim can_store_state",
            false, VT::Int, json!(1024));
        reg!("traits.threshold_relay_bandwidth_mbps", T::Runtime, V::VisibleMutable, C::Traits,
            "Minimum bandwidth (Mbps) to claim can_relay",
            false, VT::Float, json!(10.0));

        // ====================================================================
        // Bootstrap
        // ====================================================================
        reg!("bootstrap.retry_interval", T::Startup, V::VisibleReadonly, C::Bootstrap,
            "Delay between bootstrap retry attempts",
            true, VT::Duration, json!("5s"));
        reg!("bootstrap.max_retries", T::Startup, V::VisibleReadonly, C::Bootstrap,
            "Maximum number of bootstrap attempts before giving up",
            true, VT::Int, json!(12));
        reg!("bootstrap.peer_request_count", T::Startup, V::VisibleReadonly, C::Bootstrap,
            "How many peers to request from a bootstrap server",
            true, VT::Int, json!(100));

        // ====================================================================
        // Knowledge store
        // ====================================================================
        reg!("knowledge.max_known_nodes", T::Runtime, V::VisibleMutable, C::Knowledge,
            "Maximum number of nodes tracked in the knowledge store",
            false, VT::Int, json!(10000));
        reg!("knowledge.max_known_jobs", T::Runtime, V::VisibleMutable, C::Knowledge,
            "Maximum number of active jobs tracked",
            false, VT::Int, json!(5000));
        reg!("knowledge.max_known_assignments", T::Runtime, V::VisibleMutable, C::Knowledge,
            "Maximum number of assignments tracked",
            false, VT::Int, json!(50000));
        reg!("knowledge.prune_interval", T::Runtime, V::VisibleMutable, C::Knowledge,
            "How often the knowledge store prunes stale entries",
            false, VT::Duration, json!("15s"));

        // ====================================================================
        // Profile
        // ====================================================================
        reg!("profile.broadcast_interval", T::Runtime, V::VisibleMutable, C::Profile,
            "How often each node broadcasts its profile",
            false, VT::Duration, json!("30s"));
        reg!("profile.stale_age", T::Runtime, V::VisibleMutable, C::Profile,
            "A profile is considered stale after this age",
            false, VT::Duration, json!("300s"));
        reg!("profile.max_specializations", T::Runtime, V::VisibleMutable, C::Profile,
            "Maximum number of specializations a node can declare",
            false, VT::Int, json!(10));

        // ====================================================================
        // Collective
        // ====================================================================
        reg!("collective.eval_interval", T::Runtime, V::VisibleMutable, C::Collective,
            "How often collective quality is re-evaluated",
            false, VT::Duration, json!("10s"));
        reg!("collective.proposal_timeout", T::Runtime, V::VisibleMutable, C::Collective,
            "Timeout for a collective formation proposal",
            false, VT::Duration, json!("30s"));
        reg!("collective.min_members", T::Runtime, V::VisibleMutable, C::Collective,
            "Minimum number of members to form a collective",
            false, VT::Int, json!(2));
        reg!("collective.max_members", T::Runtime, V::VisibleMutable, C::Collective,
            "Maximum number of members in a collective",
            false, VT::Int, json!(12));
        reg!("collective.max_per_node", T::Runtime, V::VisibleMutable, C::Collective,
            "Maximum collectives a single node may join",
            false, VT::Int, json!(3));
        reg!("collective.min_quality", T::Runtime, V::VisibleMutable, C::Collective,
            "Quality below this threshold triggers dissolution",
            false, VT::Float, json!(0.3));

        // ====================================================================
        // Marketplace
        // ====================================================================
        reg!("marketplace.bid_window", T::Runtime, V::VisibleMutable, C::Marketplace,
            "How long to keep the bidding window open",
            false, VT::Duration, json!("5s"));
        reg!("marketplace.posting_expiry", T::Runtime, V::VisibleMutable, C::Marketplace,
            "Postings expire after this duration if not awarded",
            false, VT::Duration, json!("300s"));
        reg!("marketplace.auto_bid_interval", T::Runtime, V::VisibleMutable, C::Marketplace,
            "How often nodes auto-bid on suitable postings",
            false, VT::Duration, json!("2s"));
        reg!("marketplace.award_cycle_interval", T::Runtime, V::VisibleMutable, C::Marketplace,
            "How often the marketplace runs its award cycle",
            false, VT::Duration, json!("5s"));
        reg!("marketplace.maintenance_interval", T::Runtime, V::VisibleMutable, C::Marketplace,
            "How often marketplace maintenance runs",
            false, VT::Duration, json!("30s"));
        reg!("marketplace.min_capability_match", T::Runtime, V::VisibleMutable, C::Marketplace,
            "Minimum capability match score to be eligible for a posting",
            false, VT::Float, json!(0.5),
            constraints: ValueConstraints {
                min: Some(json!(0.0)), max: Some(json!(1.0)), ..Default::default()
            });
        reg!("marketplace.max_bids_per_posting", T::Runtime, V::VisibleMutable, C::Marketplace,
            "Maximum number of bids accepted per posting",
            false, VT::Int, json!(50));

        // ====================================================================
        // Reputation
        // ====================================================================
        reg!("reputation.rolling_window_size", T::Runtime, V::VisibleMutable, C::Reputation,
            "Number of recent jobs in the rolling reputation window",
            false, VT::Int, json!(50));
        reg!("reputation.newcomer_threshold", T::Runtime, V::VisibleMutable, C::Reputation,
            "Nodes with fewer completed jobs are considered newcomers",
            false, VT::Int, json!(50));
        reg!("reputation.stale_age", T::Runtime, V::VisibleMutable, C::Reputation,
            "Reputation data considered stale after this age",
            false, VT::Duration, json!("3600s"));

        // ====================================================================
        // Policy
        // ====================================================================
        reg!("policy.maintenance_interval", T::Runtime, V::VisibleMutable, C::Policy,
            "How often policy maintenance runs",
            false, VT::Duration, json!("60s"));
        reg!("policy.violation_max_age", T::Runtime, V::VisibleMutable, C::Policy,
            "Maximum age of a violation before pruning",
            false, VT::Duration, json!("86400s"));
        reg!("policy.rate_limit_default_per_hour", T::Runtime, V::VisibleMutable, C::Policy,
            "Default rate limit (requests per hour)",
            false, VT::Int, json!(100));

        // ====================================================================
        // Admission
        // ====================================================================
        reg!("admission.jury_min_size", T::Runtime, V::VisibleMutable, C::Admission,
            "Minimum number of jurors for admission",
            false, VT::Int, json!(5),
            constraints: ValueConstraints {
                min: Some(json!(3)), max: Some(json!(15)), ..Default::default()
            });
        reg!("admission.jury_max_size", T::Runtime, V::VisibleMutable, C::Admission,
            "Maximum number of jurors for admission",
            false, VT::Int, json!(7));
        reg!("admission.probation_min_tasks", T::Runtime, V::VisibleMutable, C::Admission,
            "Minimum tasks during probation period",
            false, VT::Int, json!(50));
        reg!("admission.probation_max_tasks", T::Runtime, V::VisibleMutable, C::Admission,
            "Maximum tasks during probation period",
            false, VT::Int, json!(200));
        reg!("admission.test_timeout", T::Runtime, V::VisibleMutable, C::Admission,
            "Timeout for admission test tasks",
            false, VT::Duration, json!("10s"));
        reg!("admission.verdict_timeout", T::Runtime, V::VisibleMutable, C::Admission,
            "Timeout for juror verdict collection",
            false, VT::Duration, json!("60s"));

        // ====================================================================
        // Security / Auth
        // ====================================================================
        reg!("security.api_auth_required", T::Startup, V::VisibleReadonly, C::Security,
            "Whether API authentication is required",
            true, VT::Bool, json!(true),
            compliance: "soc2");
        reg!("security.auth_max_message_age", T::Runtime, V::VisibleMutable, C::Security,
            "Maximum staleness of a signed gossip message before rejection",
            false, VT::Duration, json!("300s"));
        reg!("security.auth_token_expiry", T::Runtime, V::VisibleMutable, C::Security,
            "Default API token expiry",
            false, VT::Duration, json!("2592000s"),
            compliance: "soc2");

        // ====================================================================
        // Sandbox
        // ====================================================================
        reg!("sandbox.default_memory_mb", T::Runtime, V::VisibleMutable, C::Sandbox,
            "Default memory limit for sandboxed execution (MB)",
            false, VT::Int, json!(512),
            constraints: ValueConstraints {
                min: Some(json!(32)), max: Some(json!(65536)), ..Default::default()
            });
        reg!("sandbox.default_cpu_shares", T::Runtime, V::VisibleMutable, C::Sandbox,
            "Default CPU shares (cgroup cpu.weight)",
            false, VT::Int, json!(100));
        reg!("sandbox.default_max_pids", T::Runtime, V::VisibleMutable, C::Sandbox,
            "Default PID limit to prevent fork bombs",
            false, VT::Int, json!(64));
        reg!("sandbox.default_disk_mb", T::Runtime, V::VisibleMutable, C::Sandbox,
            "Default disk limit for job tmpfs (MB)",
            false, VT::Int, json!(256));
        reg!("sandbox.jobs_dir", T::Startup, V::VisibleReadonly, C::Sandbox,
            "Base directory for sandboxed job execution",
            true, VT::String, json!("/tmp/marabunta/jobs"));

        // ====================================================================
        // API
        // ====================================================================
        reg!("api.port", T::Startup, V::VisibleReadonly, C::Api,
            "HTTP API listen port",
            true, VT::Int, json!(8080));
        reg!("api.max_body_size", T::Startup, V::VisibleReadonly, C::Api,
            "Maximum request body size (bytes)",
            true, VT::Int, json!(33554432));
        reg!("api.max_blob_size", T::Startup, V::VisibleReadonly, C::Api,
            "Maximum blob upload size (bytes)",
            true, VT::Int, json!(1073741824));

        // ====================================================================
        // Blob store
        // ====================================================================
        reg!("blob.max_storage_bytes", T::Runtime, V::VisibleMutable, C::Blob,
            "Maximum blob storage size (bytes)",
            false, VT::Int, json!(10737418240_i64));
        reg!("blob.max_per_message", T::Runtime, V::VisibleMutable, C::Blob,
            "Maximum blobs gossipped per message",
            false, VT::Int, json!(100));
        reg!("blob.transfer_chunk_size", T::Runtime, V::VisibleMutable, C::Blob,
            "Blob transfer chunk size (bytes)",
            false, VT::Int, json!(65536));

        // ====================================================================
        // Discovery
        // ====================================================================
        reg!("discovery.interval", T::Runtime, V::VisibleMutable, C::Discovery,
            "How often to re-probe installed software",
            false, VT::Duration, json!("3600s"));
        reg!("discovery.probe_timeout", T::Runtime, V::VisibleMutable, C::Discovery,
            "Timeout for probing a single binary",
            false, VT::Duration, json!("5s"));

        // ====================================================================
        // Energy
        // ====================================================================
        reg!("energy.default_price_kwh", T::Runtime, V::VisibleMutable, C::Energy,
            "Default energy price (USD/kWh) when no schedule loaded",
            false, VT::Float, json!(0.12));
        reg!("energy.scoring_weight", T::Runtime, V::VisibleMutable, C::Energy,
            "Weight for energy cost in marketplace scoring",
            false, VT::Float, json!(0.2),
            constraints: ValueConstraints {
                min: Some(json!(0.0)), max: Some(json!(1.0)), ..Default::default()
            });

        // ====================================================================
        // Relay / NAT
        // ====================================================================
        reg!("relay.stun_probe_timeout", T::Runtime, V::VisibleMutable, C::Relay,
            "Timeout for STUN probe requests",
            false, VT::Duration, json!("5s"));
        reg!("relay.hole_punch_timeout", T::Runtime, V::VisibleMutable, C::Relay,
            "Timeout for hole-punching attempts before relay fallback",
            false, VT::Duration, json!("5s"));
        reg!("relay.max_sessions", T::Runtime, V::VisibleMutable, C::Relay,
            "Maximum relay sessions per relay node",
            false, VT::Int, json!(256));
        reg!("relay.max_bandwidth", T::Runtime, V::VisibleMutable, C::Relay,
            "Maximum bandwidth per relay session (bytes/s)",
            false, VT::Int, json!(1048576));

        // ====================================================================
        // Checkpoint
        // ====================================================================
        reg!("checkpoint.interval", T::Runtime, V::VisibleMutable, C::Checkpoint,
            "Default checkpoint interval for long-running chunks",
            false, VT::Duration, json!("60s"));

        // ====================================================================
        // Aggregation
        // ====================================================================
        reg!("aggregation.reduce_timeout", T::Runtime, V::VisibleMutable, C::Aggregation,
            "Maximum reduce script execution time",
            false, VT::Duration, json!("600s"));

        // ====================================================================
        // Update
        // ====================================================================
        reg!("update.enabled", T::Startup, V::VisibleReadonly, C::Update,
            "Whether auto-update is enabled",
            true, VT::Bool, json!(false));
        reg!("update.check_interval", T::Runtime, V::VisibleMutable, C::Update,
            "How often to check for updates",
            false, VT::Duration, json!("21600s"));

        // ====================================================================
        // Management layer
        // ====================================================================
        reg!("management.enabled", T::Startup, V::VisibleReadonly, C::Management,
            "Whether the management layer subsystems are enabled",
            true, VT::Bool, json!(true));
        reg!("management.swarm_name", T::Runtime, V::VisibleMutable, C::Management,
            "Human-readable swarm name",
            false, VT::String, json!("unnamed-swarm"));
        reg!("management.psyche_interval_secs", T::Runtime, V::VisibleMutable, C::Management,
            "How often the psyche calculator recomputes facets",
            false, VT::Int, json!(10));
        reg!("management.event_bus_buffer_size", T::Startup, V::VisibleReadonly, C::Management,
            "EventBus ring buffer capacity",
            true, VT::Int, json!(10000));
        reg!("management.alert_eval_interval_secs", T::Runtime, V::VisibleMutable, C::Management,
            "How often alert rules are evaluated",
            false, VT::Int, json!(10));
        reg!("management.healthcheck_interval_secs", T::Runtime, V::VisibleMutable, C::Management,
            "How often health probes are executed",
            false, VT::Int, json!(10));
        reg!("management.audit_max_entries", T::Runtime, V::VisibleMutable, C::Management,
            "Maximum audit log entries before FIFO eviction",
            false, VT::Int, json!(1000000),
            compliance: "soc2");

        // ====================================================================
        // Verification
        // ====================================================================
        reg!("verification.default_replicas", T::Runtime, V::VisibleMutable, C::Verification,
            "Default verification replicas for redundant execution",
            false, VT::Int, json!(2),
            constraints: ValueConstraints {
                min: Some(json!(1)), max: Some(json!(5)), ..Default::default()
            },
            compliance: "soc2");
        reg!("verification.spot_check_rate", T::Runtime, V::VisibleMutable, C::Verification,
            "Spot check rate for trusted nodes",
            false, VT::Float, json!(0.05),
            constraints: ValueConstraints {
                min: Some(json!(0.0)), max: Some(json!(1.0)), ..Default::default()
            });

        // ====================================================================
        // Webhook
        // ====================================================================
        reg!("webhook.timeout_secs", T::Runtime, V::VisibleMutable, C::Webhook,
            "Webhook delivery timeout per request (seconds)",
            false, VT::Int, json!(30));
        reg!("webhook.max_retries", T::Runtime, V::VisibleMutable, C::Webhook,
            "Maximum webhook delivery retries",
            false, VT::Int, json!(5));
        reg!("webhook.max_concurrent", T::Runtime, V::VisibleMutable, C::Webhook,
            "Maximum concurrent webhook deliveries",
            false, VT::Int, json!(32));

        // ====================================================================
        // Pricing
        // ====================================================================
        reg!("pricing.base_node_hour_usd", T::Runtime, V::VisibleMutable, C::Pricing,
            "Base cost per node-hour in USD",
            false, VT::Float, json!(0.002));
        reg!("pricing.transaction_fee_pct", T::Runtime, V::VisibleMutable, C::Pricing,
            "Platform transaction fee percentage",
            false, VT::Float, json!(0.02),
            constraints: ValueConstraints {
                min: Some(json!(0.0)), max: Some(json!(0.5)), ..Default::default()
            });
        reg!("pricing.rush_multiplier", T::Runtime, V::VisibleMutable, C::Pricing,
            "Rush priority cost multiplier",
            false, VT::Float, json!(3.0));
        reg!("pricing.economy_multiplier", T::Runtime, V::VisibleMutable, C::Pricing,
            "Economy priority cost multiplier",
            false, VT::Float, json!(0.5));

        // ====================================================================
        // Scheduling
        // ====================================================================
        reg!("scheduling.tick_ms", T::Runtime, V::VisibleMutable, C::Scheduling,
            "Scheduler tick interval (milliseconds)",
            false, VT::Int, json!(500));
        reg!("scheduling.offpeak_start_hour", T::Runtime, V::VisibleMutable, C::Scheduling,
            "Economy jobs: off-peak hours start (UTC hour)",
            false, VT::Int, json!(22),
            constraints: ValueConstraints {
                min: Some(json!(0)), max: Some(json!(23)), ..Default::default()
            });
        reg!("scheduling.offpeak_end_hour", T::Runtime, V::VisibleMutable, C::Scheduling,
            "Economy jobs: off-peak hours end (UTC hour)",
            false, VT::Int, json!(6),
            constraints: ValueConstraints {
                min: Some(json!(0)), max: Some(json!(23)), ..Default::default()
            });
        reg!("scheduling.max_queue_depth", T::Runtime, V::VisibleMutable, C::Scheduling,
            "Maximum queue depth before rejecting new jobs",
            false, VT::Int, json!(10000));
        reg!("scheduling.rush_reserve_fraction", T::Runtime, V::VisibleMutable, C::Scheduling,
            "Fraction of swarm capacity reserved for rush jobs",
            false, VT::Float, json!(0.30),
            constraints: ValueConstraints {
                min: Some(json!(0.0)), max: Some(json!(1.0)), ..Default::default()
            });

        // ====================================================================
        // Witness
        // ====================================================================
        reg!("witness.count_normal", T::Runtime, V::VisibleMutable, C::Witness,
            "Required witnesses for Normal-tier actions",
            false, VT::Int, json!(2));
        reg!("witness.count_high", T::Runtime, V::VisibleMutable, C::Witness,
            "Required witnesses for High-tier actions",
            false, VT::Int, json!(5));
        reg!("witness.count_critical", T::Runtime, V::VisibleMutable, C::Witness,
            "Minimum witnesses for Critical-tier actions",
            false, VT::Int, json!(7));
        reg!("witness.quorum_ratio_normal", T::Runtime, V::VisibleMutable, C::Witness,
            "Quorum ratio for Normal-tier witnesses",
            false, VT::Float, json!(1.0));
        reg!("witness.quorum_ratio_high", T::Runtime, V::VisibleMutable, C::Witness,
            "Quorum ratio for High-tier witnesses",
            false, VT::Float, json!(0.8));
        reg!("witness.quorum_ratio_critical", T::Runtime, V::VisibleMutable, C::Witness,
            "Quorum ratio for Critical-tier witnesses",
            false, VT::Float, json!(0.72));

        // ====================================================================
        // PostgreSQL
        // ====================================================================
        reg!("postgres.enabled", T::Startup, V::VisibleReadonly, C::Postgres,
            "Whether the swarm-managed PostgreSQL subsystem is enabled",
            true, VT::Bool, json!(true));
        reg!("postgres.port", T::Startup, V::VisibleReadonly, C::Postgres,
            "PostgreSQL listen port",
            true, VT::Int, json!(5433));
        reg!("postgres.min_ram_for_pg_mb", T::Startup, V::VisibleReadonly, C::Postgres,
            "Minimum RAM (MB) for a node to host PostgreSQL",
            true, VT::Int, json!(4096));
        reg!("postgres.replication_factor", T::Runtime, V::VisibleMutable, C::Postgres,
            "Target number of PG replicas",
            false, VT::Int, json!(2),
            constraints: ValueConstraints {
                min: Some(json!(0)), max: Some(json!(5)), ..Default::default()
            });
        reg!("postgres.pool_size", T::Runtime, V::VisibleMutable, C::Postgres,
            "Connection pool size per PG node",
            false, VT::Int, json!(10),
            constraints: ValueConstraints {
                min: Some(json!(1)), max: Some(json!(100)), ..Default::default()
            });
        reg!("postgres.enable_time_travel", T::Startup, V::VisibleReadonly, C::Postgres,
            "Whether time-travel queries are enabled",
            true, VT::Bool, json!(true));
        reg!("postgres.history_retention_days", T::Runtime, V::VisibleMutable, C::Postgres,
            "How many days of history to retain",
            false, VT::Int, json!(90),
            constraints: ValueConstraints {
                min: Some(json!(7)), max: Some(json!(3650)), ..Default::default()
            });

        // ====================================================================
        // Neuromancer
        // ====================================================================
        reg!("neuromancer.enabled", T::Startup, V::VisibleReadonly, C::Neuromancer,
            "Whether the Neuromancer intelligence/security_domain subsystem is enabled",
            true, VT::Bool, json!(false));

        // ====================================================================
        // Display (node identity)
        // ====================================================================
        reg!("display.listen_addr", T::Startup, V::VisibleReadonly, C::Display,
            "This node's listen address",
            true, VT::String, json!("0.0.0.0:4200"));
    }
}

/// Describe a JSON value type for error messages.
fn type_name_of(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(_) => "bool".to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() { "int".to_string() } else { "float".to_string() }
        }
        serde_json::Value::String(_) => "string".to_string(),
        serde_json::Value::Array(_) => "array".to_string(),
        serde_json::Value::Object(_) => "object".to_string(),
    }
}

/// Convenience macro for JSON literals (re-export serde_json::json internally).
macro_rules! json {
    ($($tt:tt)*) => { serde_json::json!($($tt)*) };
}
use json;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> ConfigRegistry {
        ConfigRegistry::new(vec![])
    }

    #[test]
    fn test_registry_not_empty() {
        let r = registry();
        assert!(r.len() > 100, "expected 100+ settings, got {}", r.len());
    }

    #[test]
    fn test_all_keys_have_descriptions() {
        let r = registry();
        for meta in r.schema() {
            assert!(!meta.description.is_empty(), "key {} has empty description", meta.key);
        }
    }

    #[test]
    fn test_all_keys_have_defaults() {
        let r = registry();
        for meta in r.schema() {
            assert!(!meta.default_value.is_null(), "key {} has null default", meta.key);
        }
    }

    #[test]
    fn test_get_known_key() {
        let r = registry();
        let meta = r.get("gossip.interval").unwrap();
        assert_eq!(meta.category, ConfigCategory::Gossip);
        assert_eq!(meta.tier, ConfigTier::Runtime);
    }

    #[test]
    fn test_get_unknown_key() {
        let r = registry();
        assert!(r.get("nonexistent.key").is_none());
    }

    #[test]
    fn test_validate_int_in_range() {
        let r = registry();
        assert!(r.validate("gossip.fanout", &json!(5)).is_ok());
    }

    #[test]
    fn test_validate_int_below_min() {
        let r = registry();
        let err = r.validate("gossip.fanout", &json!(0)).unwrap_err();
        assert!(matches!(err, ConfigValidationError::BelowMin { .. }));
    }

    #[test]
    fn test_validate_int_above_max() {
        let r = registry();
        let err = r.validate("gossip.fanout", &json!(100)).unwrap_err();
        assert!(matches!(err, ConfigValidationError::AboveMax { .. }));
    }

    #[test]
    fn test_validate_float_in_range() {
        let r = registry();
        assert!(r.validate("failure.quorum_fraction", &json!(0.7)).is_ok());
    }

    #[test]
    fn test_validate_float_below_min() {
        let r = registry();
        let err = r.validate("failure.quorum_fraction", &json!(0.05)).unwrap_err();
        assert!(matches!(err, ConfigValidationError::BelowMin { .. }));
    }

    #[test]
    fn test_validate_float_above_max() {
        let r = registry();
        let err = r.validate("failure.quorum_fraction", &json!(1.5)).unwrap_err();
        assert!(matches!(err, ConfigValidationError::AboveMax { .. }));
    }

    #[test]
    fn test_validate_type_mismatch_bool() {
        let r = registry();
        let err = r.validate("security.api_auth_required", &json!("yes")).unwrap_err();
        assert!(matches!(err, ConfigValidationError::TypeMismatch { .. }));
    }

    #[test]
    fn test_validate_type_mismatch_int() {
        let r = registry();
        let err = r.validate("gossip.fanout", &json!("three")).unwrap_err();
        assert!(matches!(err, ConfigValidationError::TypeMismatch { .. }));
    }

    #[test]
    fn test_validate_unknown_key() {
        let r = registry();
        let err = r.validate("nonexistent", &json!(42)).unwrap_err();
        assert!(matches!(err, ConfigValidationError::UnknownKey { .. }));
    }

    #[test]
    fn test_validate_bool_ok() {
        let r = registry();
        assert!(r.validate("security.api_auth_required", &json!(true)).is_ok());
        assert!(r.validate("security.api_auth_required", &json!(false)).is_ok());
    }

    #[test]
    fn test_validate_string_ok() {
        let r = registry();
        assert!(r.validate("management.swarm_name", &json!("my-swarm")).is_ok());
    }

    #[test]
    fn test_hardwired_detection() {
        let overrides = vec![ComplianceOverride {
            key: "security.api_auth_required".to_string(),
            value: json!(true),
            ui_visibility: UiVisibility::VisibleReadonly,
            constraint_type: "exact".to_string(),
            justification: "SOC2 requires authentication".to_string(),
            profile_name: "soc2".to_string(),
        }];
        let r = ConfigRegistry::new(overrides);
        let ovr = r.is_hardwired("security.api_auth_required").unwrap();
        assert_eq!(ovr.profile_name, "soc2");
    }

    #[test]
    fn test_hardwired_not_found() {
        let r = registry();
        assert!(r.is_hardwired("gossip.fanout").is_none());
    }

    #[test]
    fn test_hardwired_rejects_validate() {
        let overrides = vec![ComplianceOverride {
            key: "gossip.fanout".to_string(),
            value: json!(3),
            ui_visibility: UiVisibility::VisibleReadonly,
            constraint_type: "exact".to_string(),
            justification: "locked for compliance".to_string(),
            profile_name: "test".to_string(),
        }];
        let r = ConfigRegistry::new(overrides);
        let err = r.validate("gossip.fanout", &json!(5)).unwrap_err();
        assert!(matches!(err, ConfigValidationError::HardwiredLock { .. }));
    }

    #[test]
    fn test_by_category_gossip() {
        let r = registry();
        let gossip = r.by_category(ConfigCategory::Gossip);
        assert!(gossip.len() >= 5, "expected at least 5 gossip settings");
        for meta in &gossip {
            assert_eq!(meta.category, ConfigCategory::Gossip);
        }
    }

    #[test]
    fn test_by_category_failure() {
        let r = registry();
        let failure = r.by_category(ConfigCategory::Failure);
        assert!(failure.len() >= 4, "expected at least 4 failure settings");
    }

    #[test]
    fn test_by_category_postgres() {
        let r = registry();
        let pg = r.by_category(ConfigCategory::Postgres);
        assert!(pg.len() >= 5, "expected at least 5 postgres settings");
    }

    #[test]
    fn test_schema_sorted() {
        let r = registry();
        let schema = r.schema();
        for pair in schema.windows(2) {
            assert!(pair[0].key <= pair[1].key,
                "schema not sorted: {} > {}", pair[0].key, pair[1].key);
        }
    }

    #[test]
    fn test_config_tier_display() {
        assert_eq!(ConfigTier::Hardwired.to_string(), "hardwired");
        assert_eq!(ConfigTier::Startup.to_string(), "startup");
        assert_eq!(ConfigTier::Runtime.to_string(), "runtime");
    }

    #[test]
    fn test_ui_visibility_display() {
        assert_eq!(UiVisibility::VisibleMutable.to_string(), "visible_mutable");
        assert_eq!(UiVisibility::VisibleReadonly.to_string(), "visible_readonly");
        assert_eq!(UiVisibility::Hidden.to_string(), "hidden");
    }

    #[test]
    fn test_config_tier_serde_roundtrip() {
        let tier = ConfigTier::Runtime;
        let json = serde_json::to_string(&tier).unwrap();
        let back: ConfigTier = serde_json::from_str(&json).unwrap();
        assert_eq!(tier, back);
    }

    #[test]
    fn test_validation_error_display() {
        let err = ConfigValidationError::UnknownKey { key: "foo".into() };
        assert!(err.to_string().contains("foo"));
    }

    #[test]
    fn test_runtime_settings_are_visible_mutable() {
        let r = registry();
        for meta in r.schema() {
            if meta.tier == ConfigTier::Runtime {
                assert_eq!(meta.ui_visibility, UiVisibility::VisibleMutable,
                    "runtime setting {} should be VisibleMutable", meta.key);
            }
        }
    }

    #[test]
    fn test_startup_settings_are_visible_readonly() {
        let r = registry();
        for meta in r.schema() {
            if meta.tier == ConfigTier::Startup {
                assert_eq!(meta.ui_visibility, UiVisibility::VisibleReadonly,
                    "startup setting {} should be VisibleReadonly", meta.key);
            }
        }
    }

    #[test]
    fn test_startup_settings_require_restart() {
        let r = registry();
        for meta in r.schema() {
            if meta.tier == ConfigTier::Startup {
                assert!(meta.restart_required,
                    "startup setting {} should require restart", meta.key);
            }
        }
    }

    #[test]
    fn test_runtime_settings_no_restart() {
        let r = registry();
        for meta in r.schema() {
            if meta.tier == ConfigTier::Runtime {
                assert!(!meta.restart_required,
                    "runtime setting {} should not require restart", meta.key);
            }
        }
    }
}
