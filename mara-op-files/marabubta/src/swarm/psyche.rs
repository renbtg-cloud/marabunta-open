// Marabunta - Licensed under the MIT License.
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::io::{BufRead, Write as _};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use tracing::{info, warn, debug};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tract_onnx::prelude::*;
use tract_onnx::prelude::Tensor;
use tract_onnx::tract_core::ndarray::Array2;
use crate::swarm::KnowledgeStore;
use crate::swarm::types::NodeStatus;
use crate::swarm::config::{PSYCHE_HISTORY_MAX, PSYCHE_COMPUTE_INTERVAL_SECS};
use crate::swarm::complexity::ComplexityStyle;
use crate::swarm::events::{EventBus, SwarmEvent};

pub struct PredictiveRouter {
    model: RunnableModel<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>,
}

impl PredictiveRouter {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Load the actual compiled ONNX model from disk
        let model_path = "assets/partition_predictor.onnx";
        
        let runnable = tract_onnx::onnx()
            .model_for_path(model_path)?
            .into_optimized()?
            .into_runnable()?;
        
        Ok(Self { model: runnable })
    }

    /// Evaluates the 7 psyche facets as a feature vector to predict a network partition.
    pub fn predict_partition(&self, facets: [f32; 7]) -> Result<bool, Box<dyn std::error::Error>> {
        tracing::debug!("PredictiveRouter: Running ML inference on Swarm Psyche: {:?}", facets);
        
        // Create a Tensor with shape [1, 7] (batch_size=1, 7 features)
        let tensor = Array2::from_shape_vec((1, 7), facets.to_vec())?.into_tensor();
        
        // Execute the physical ONNX graph
        let result = self.model.run(tvec!(tensor.into()))?;
        
        // Extract the prediction
        let prediction: f32 = *result[0].to_array_view::<f32>()?.iter().next().unwrap();
        
        if prediction > 0.80 {
            tracing::warn!("CRITICAL: Federated ML predicts imminent network partition with {:.1}% confidence!", prediction * 100.0);
            tracing::info!("Proactive Resharding Triggered: Pre-caching BFT DAG and Reed-Solomon shards across structural boundaries.");
            return Ok(true);
        }

        Ok(false)
    }
}


// ============================================================================
// FacetLevel
// ============================================================================

/// The five discrete levels for each psyche facet.
///
/// Levels are ordered from L1 (lowest / most concerning) to L5 (highest /
/// most capable). The numeric mapping is 0--4 for compact serialization
/// and arithmetic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FacetLevel {
    /// Level 1 -- lowest / most concerning state.
    L1 = 0,
    /// Level 2 -- below average.
    L2 = 1,
    /// Level 3 -- middle / baseline.
    L3 = 2,
    /// Level 4 -- above average / healthy.
    L4 = 3,
    /// Level 5 -- highest / peak performance.
    L5 = 4,
}

impl FacetLevel {
    /// Convert a `u8` to a `FacetLevel`, clamping to the valid range 0--4.
    ///
    /// Values above 4 are clamped to `L5`.
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => FacetLevel::L1,
            1 => FacetLevel::L2,
            2 => FacetLevel::L3,
            3 => FacetLevel::L4,
            _ => FacetLevel::L5,
        }
    }

    /// Return the numeric representation (0--4).
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    /// Map to a 0.0--1.0 fraction: L1 = 0.0, L2 = 0.25, L3 = 0.5, L4 = 0.75, L5 = 1.0.
    pub fn as_f64(&self) -> f64 {
        match self {
            FacetLevel::L1 => 0.0,
            FacetLevel::L2 => 0.25,
            FacetLevel::L3 => 0.5,
            FacetLevel::L4 => 0.75,
            FacetLevel::L5 => 1.0,
        }
    }

    /// Quantize a 0.0--1.0 fraction to a `FacetLevel`.
    ///
    /// Boundaries: `[0.0, 0.2) = L1`, `[0.2, 0.4) = L2`, `[0.4, 0.6) = L3`,
    /// `[0.6, 0.8) = L4`, `[0.8, 1.0] = L5`.
    ///
    /// NaN, negative infinity, and values below 0.0 map to L1.
    /// Positive infinity and values above 1.0 map to L5.
    pub fn from_fraction(f: f64) -> Self {
        if f.is_nan() || f < 0.0 {
            return FacetLevel::L1;
        }
        if f >= 0.8 {
            FacetLevel::L5
        } else if f >= 0.6 {
            FacetLevel::L4
        } else if f >= 0.4 {
            FacetLevel::L3
        } else if f >= 0.2 {
            FacetLevel::L2
        } else {
            FacetLevel::L1
        }
    }

    /// Human-readable name that varies by facet.
    ///
    /// Each facet has its own vocabulary for the five levels, reflecting
    /// the domain-specific meaning. For example, Exertion L1 is "Idle"
    /// while Vitality L1 is "Critical".
    pub fn name_for(&self, facet: Facet) -> &'static str {
        match facet {
            Facet::Exertion => match self {
                FacetLevel::L1 => "Idle",
                FacetLevel::L2 => "Light",
                FacetLevel::L3 => "Moderate",
                FacetLevel::L4 => "Heavy",
                FacetLevel::L5 => "Maxed",
            },
            Facet::Vitality => match self {
                FacetLevel::L1 => "Critical",
                FacetLevel::L2 => "Weak",
                FacetLevel::L3 => "Fair",
                FacetLevel::L4 => "Strong",
                FacetLevel::L5 => "Thriving",
            },
            Facet::Momentum => match self {
                FacetLevel::L1 => "Contracting",
                FacetLevel::L2 => "Slowing",
                FacetLevel::L3 => "Steady",
                FacetLevel::L4 => "Accelerating",
                FacetLevel::L5 => "Surging",
            },
            Facet::Foresight => match self {
                FacetLevel::L1 => "Blind",
                FacetLevel::L2 => "Uncertain",
                FacetLevel::L3 => "Aware",
                FacetLevel::L4 => "Prepared",
                FacetLevel::L5 => "Prescient",
            },
            Facet::Cohesion => match self {
                FacetLevel::L1 => "Shattered",
                FacetLevel::L2 => "Fractured",
                FacetLevel::L3 => "Partial",
                FacetLevel::L4 => "Connected",
                FacetLevel::L5 => "Unified",
            },
            Facet::Resilience => match self {
                FacetLevel::L1 => "Brittle",
                FacetLevel::L2 => "Fragile",
                FacetLevel::L3 => "Adequate",
                FacetLevel::L4 => "Robust",
                FacetLevel::L5 => "Antifragile",
            },
            Facet::Efficiency => match self {
                FacetLevel::L1 => "Wasteful",
                FacetLevel::L2 => "Loose",
                FacetLevel::L3 => "Balanced",
                FacetLevel::L4 => "Lean",
                FacetLevel::L5 => "Optimal",
            },
        }
    }

    /// CSS/HTML hex color for dashboard rendering.
    ///
    /// L1 = red, L2 = orange, L3 = yellow, L4 = green, L5 = blue.
    pub fn color_hex(&self) -> &'static str {
        match self {
            FacetLevel::L1 => "#e74c3c",
            FacetLevel::L2 => "#e67e22",
            FacetLevel::L3 => "#f1c40f",
            FacetLevel::L4 => "#2ecc71",
            FacetLevel::L5 => "#3498db",
        }
    }

    /// Character for ASCII bar rendering.
    ///
    /// Returns the filled block character used to represent a filled segment.
    pub fn bar_char(&self) -> char {
        '\u{2588}' // █
    }

    /// The unfilled bar character used for empty segments.
    pub fn empty_char() -> char {
        '\u{2591}' // ░
    }
}

impl Default for FacetLevel {
    fn default() -> Self {
        FacetLevel::L3
    }
}

impl fmt::Display for FacetLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FacetLevel::L1 => write!(f, "L1"),
            FacetLevel::L2 => write!(f, "L2"),
            FacetLevel::L3 => write!(f, "L3"),
            FacetLevel::L4 => write!(f, "L4"),
            FacetLevel::L5 => write!(f, "L5"),
        }
    }
}

impl FromStr for FacetLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "L1" | "1" => Ok(FacetLevel::L1),
            "L2" | "2" => Ok(FacetLevel::L2),
            "L3" | "3" => Ok(FacetLevel::L3),
            "L4" | "4" => Ok(FacetLevel::L4),
            "L5" | "5" => Ok(FacetLevel::L5),
            other => Err(format!("unknown facet level: {}", other)),
        }
    }
}

// ============================================================================
// Facet
// ============================================================================

/// The seven psyche facets that together describe the swarm's operational state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Facet {
    /// Aggregate load across all alive nodes.
    Exertion,
    /// Node liveness and chunk success rate.
    Vitality,
    /// Rate of change in cluster size and throughput.
    Momentum,
    /// Awareness of upcoming scheduled work.
    Foresight,
    /// Network connectivity and gossip reach.
    Cohesion,
    /// Spare capacity, geographic diversity, redundancy headroom.
    Resilience,
    /// Load distribution evenness and idle-node ratio.
    Efficiency,
}

impl Facet {
    /// Returns a static slice containing all seven facets in canonical order.
    pub fn all() -> &'static [Facet] {
        &[
            Facet::Exertion,
            Facet::Vitality,
            Facet::Momentum,
            Facet::Foresight,
            Facet::Cohesion,
            Facet::Resilience,
            Facet::Efficiency,
        ]
    }

    /// A short description of what this facet measures.
    pub fn description(&self) -> &'static str {
        match self {
            Facet::Exertion => "Current aggregate load across all alive nodes",
            Facet::Vitality => "Node liveness and recent chunk success rate",
            Facet::Momentum => "Rate of change in cluster size and throughput",
            Facet::Foresight => "Awareness of upcoming scheduled work",
            Facet::Cohesion => "Network connectivity and gossip reach",
            Facet::Resilience => "Spare capacity, geo-diversity, and redundancy headroom",
            Facet::Efficiency => "Load distribution evenness and idle-node ratio",
        }
    }
}

impl fmt::Display for Facet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Facet::Exertion => "exertion",
            Facet::Vitality => "vitality",
            Facet::Momentum => "momentum",
            Facet::Foresight => "foresight",
            Facet::Cohesion => "cohesion",
            Facet::Resilience => "resilience",
            Facet::Efficiency => "efficiency",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for Facet {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "exertion" => Ok(Facet::Exertion),
            "vitality" => Ok(Facet::Vitality),
            "momentum" => Ok(Facet::Momentum),
            "foresight" => Ok(Facet::Foresight),
            "cohesion" => Ok(Facet::Cohesion),
            "resilience" => Ok(Facet::Resilience),
            "efficiency" => Ok(Facet::Efficiency),
            other => Err(format!("unknown facet: {}", other)),
        }
    }
}

// ============================================================================
// Trend
// ============================================================================

/// Direction a facet is trending over its sliding window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Trend {
    /// The facet value is increasing.
    Rising,
    /// The facet value is decreasing.
    Falling,
    /// The facet value is approximately constant.
    Stable,
}

impl fmt::Display for Trend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Trend::Rising => write!(f, "rising"),
            Trend::Falling => write!(f, "falling"),
            Trend::Stable => write!(f, "stable"),
        }
    }
}

impl Default for Trend {
    fn default() -> Self {
        Trend::Stable
    }
}

// ============================================================================
// SwarmPsyche
// ============================================================================

/// The swarm's multi-dimensional operational state of mind.
///
/// Each field is a quantized `FacetLevel` (L1--L5) for one of the seven
/// facets. The `matched_archetypes` vector lists the names of all currently
/// active archetype rules, sorted by descending priority.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SwarmPsyche {
    /// Aggregate load across all alive nodes.
    pub exertion: FacetLevel,
    /// Node liveness and chunk success rate.
    pub vitality: FacetLevel,
    /// Rate of change in cluster size and throughput.
    pub momentum: FacetLevel,
    /// Awareness of upcoming scheduled work.
    pub foresight: FacetLevel,
    /// Network connectivity and gossip reach.
    pub cohesion: FacetLevel,
    /// Spare capacity, geo-diversity, redundancy headroom.
    pub resilience: FacetLevel,
    /// Load distribution evenness and idle-node ratio.
    pub efficiency: FacetLevel,
    /// Names of all currently matching archetypes, sorted by priority (descending).
    pub matched_archetypes: Vec<String>,
    /// UTC timestamp when this psyche snapshot was computed.
    pub computed_at: DateTime<Utc>,
}

impl Default for SwarmPsyche {
    /// Default psyche: all facets at L3 (middle / baseline), no matched archetypes.
    fn default() -> Self {
        Self {
            exertion: FacetLevel::L3,
            vitality: FacetLevel::L3,
            momentum: FacetLevel::L3,
            foresight: FacetLevel::L3,
            cohesion: FacetLevel::L3,
            resilience: FacetLevel::L3,
            efficiency: FacetLevel::L3,
            matched_archetypes: Vec::new(),
            computed_at: Utc::now(),
        }
    }
}

impl SwarmPsyche {
    /// Get the level of a specific facet.
    pub fn get_facet(&self, facet: Facet) -> FacetLevel {
        match facet {
            Facet::Exertion => self.exertion,
            Facet::Vitality => self.vitality,
            Facet::Momentum => self.momentum,
            Facet::Foresight => self.foresight,
            Facet::Cohesion => self.cohesion,
            Facet::Resilience => self.resilience,
            Facet::Efficiency => self.efficiency,
        }
    }

    /// Set the level of a specific facet.
    pub fn set_facet(&mut self, facet: Facet, level: FacetLevel) {
        match facet {
            Facet::Exertion => self.exertion = level,
            Facet::Vitality => self.vitality = level,
            Facet::Momentum => self.momentum = level,
            Facet::Foresight => self.foresight = level,
            Facet::Cohesion => self.cohesion = level,
            Facet::Resilience => self.resilience = level,
            Facet::Efficiency => self.efficiency = level,
        }
    }

    /// Returns `true` if all seven facets are at L4 or above.
    pub fn all_green(&self) -> bool {
        Facet::all().iter().all(|f| self.get_facet(*f) >= FacetLevel::L4)
    }

    /// Returns `true` if any facet is at L1 (the lowest / most critical level).
    pub fn any_critical(&self) -> bool {
        Facet::all().iter().any(|f| self.get_facet(*f) == FacetLevel::L1)
    }

    /// Returns the name of the highest-priority matched archetype, or `None`.
    pub fn dominant_archetype(&self) -> Option<&str> {
        self.matched_archetypes.first().map(|s| s.as_str())
    }

    /// Render a compact ASCII bar chart of all facets.
    ///
    /// Format per facet: `Exertion  [████░] Heavy`
    /// All facets are separated by ` | `.
    pub fn to_ascii_bar(&self) -> String {
        let mut parts = Vec::with_capacity(7);
        for facet in Facet::all() {
            let level = self.get_facet(*facet);
            let filled = level.as_u8() + 1; // L1=1 filled, L5=5 filled
            let empty = 5 - filled;
            let bar: String = std::iter::repeat(level.bar_char())
                .take(filled as usize)
                .chain(std::iter::repeat(FacetLevel::empty_char()).take(empty as usize))
                .collect();
            let name = level.name_for(*facet);
            parts.push(format!("{} [{}] {}", facet, bar, name));
        }
        parts.join(" | ")
    }

    /// Derive suggested complexity styles from the current facet values.
    ///
    /// The mapping is heuristic:
    /// - Any critical facet (L1) suggests `Investigative` for deep-dive.
    /// - High exertion (L4+) suggests `Observable` for real-time monitoring.
    /// - Low momentum (L1/L2) suggests `Glanceable` for summary views.
    /// - Otherwise `Browseable` as baseline.
    pub fn suggested_styles(&self) -> Vec<ComplexityStyle> {
        let mut styles = Vec::new();

        if self.any_critical() {
            styles.push(ComplexityStyle::Investigative);
        }

        if self.exertion >= FacetLevel::L4 {
            styles.push(ComplexityStyle::Observable);
        }

        if self.momentum <= FacetLevel::L2 && !self.any_critical() {
            styles.push(ComplexityStyle::Glanceable);
        }

        if self.all_green() {
            styles.push(ComplexityStyle::Glanceable);
        }

        if styles.is_empty() {
            styles.push(ComplexityStyle::Browseable);
        }

        // Deduplicate while preserving order.
        let mut seen = HashSet::new();
        styles.retain(|s| seen.insert(*s));
        styles
    }

    /// Compute facet-by-facet differences between two psyche snapshots.
    ///
    /// Returns a vector of `(facet, from_level, to_level)` tuples for every
    /// facet where the level changed between `self` (the older snapshot)
    /// and `other` (the newer snapshot).
    pub fn diff(&self, other: &SwarmPsyche) -> Vec<(Facet, FacetLevel, FacetLevel)> {
        let mut changes = Vec::new();
        for facet in Facet::all() {
            let from = self.get_facet(*facet);
            let to = other.get_facet(*facet);
            if from != to {
                changes.push((*facet, from, to));
            }
        }
        changes
    }
}

// ============================================================================
// FacetMatcher
// ============================================================================

/// A predicate for matching a single facet's level and/or trend.
///
/// All specified conditions are ANDed together. `None` fields are wildcards
/// that match any value. An empty `FacetMatcher` (all `None`) matches
/// everything.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FacetMatcher {
    /// Facet must be at or above this level.
    pub min: Option<FacetLevel>,
    /// Facet must be at or below this level.
    pub max: Option<FacetLevel>,
    /// Facet must be one of these exact levels.
    pub exact: Option<Vec<FacetLevel>>,
    /// Facet must be trending in this direction.
    pub trending: Option<Trend>,
}

impl FacetMatcher {
    /// Check whether a given level and optional trend satisfy this matcher.
    pub fn matches(&self, level: FacetLevel, trend: Option<Trend>) -> bool {
        if let Some(min) = self.min {
            if level < min {
                return false;
            }
        }
        if let Some(max) = self.max {
            if level > max {
                return false;
            }
        }
        if let Some(ref exact) = self.exact {
            if !exact.contains(&level) {
                return false;
            }
        }
        if let Some(required_trend) = self.trending {
            match trend {
                Some(actual_trend) => {
                    if actual_trend != required_trend {
                        return false;
                    }
                }
                None => {
                    // No trend data available -- cannot confirm trending requirement.
                    return false;
                }
            }
        }
        true
    }

    /// Create a matcher that requires the facet to be at or above the given level.
    pub fn min(level: FacetLevel) -> Self {
        Self {
            min: Some(level),
            max: None,
            exact: None,
            trending: None,
        }
    }

    /// Create a matcher that requires the facet to be at or below the given level.
    pub fn max(level: FacetLevel) -> Self {
        Self {
            min: None,
            max: Some(level),
            exact: None,
            trending: None,
        }
    }

    /// Create a matcher that requires the facet to be exactly the given level.
    pub fn exact_level(level: FacetLevel) -> Self {
        Self {
            min: None,
            max: None,
            exact: Some(vec![level]),
            trending: None,
        }
    }

    /// Create a matcher that requires the facet to be one of the given levels.
    pub fn one_of(levels: Vec<FacetLevel>) -> Self {
        Self {
            min: None,
            max: None,
            exact: Some(levels),
            trending: None,
        }
    }

    /// Create a matcher that requires a specific trend direction.
    pub fn with_trend(trend: Trend) -> Self {
        Self {
            min: None,
            max: None,
            exact: None,
            trending: Some(trend),
        }
    }

    /// Builder: add a minimum constraint.
    pub fn and_min(mut self, level: FacetLevel) -> Self {
        self.min = Some(level);
        self
    }

    /// Builder: add a maximum constraint.
    pub fn and_max(mut self, level: FacetLevel) -> Self {
        self.max = Some(level);
        self
    }

    /// Builder: add a trend constraint.
    pub fn and_trend(mut self, trend: Trend) -> Self {
        self.trending = Some(trend);
        self
    }
}

// ============================================================================
// ArchetypeRule
// ============================================================================

/// A named pattern across the seven facets that identifies a recognizable
/// operational posture.
///
/// Each facet has an optional `FacetMatcher`. A `None` matcher for a facet
/// means "any level matches" (wildcard). The rule matches when all non-None
/// matchers are satisfied.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchetypeRule {
    /// Unique name of this archetype (e.g., "war-room", "zen-garden").
    pub name: String,
    /// Human-readable description of what this archetype represents.
    pub description: String,
    /// Matcher for the Exertion facet (`None` = wildcard).
    pub exertion: Option<FacetMatcher>,
    /// Matcher for the Vitality facet (`None` = wildcard).
    pub vitality: Option<FacetMatcher>,
    /// Matcher for the Momentum facet (`None` = wildcard).
    pub momentum: Option<FacetMatcher>,
    /// Matcher for the Foresight facet (`None` = wildcard).
    pub foresight: Option<FacetMatcher>,
    /// Matcher for the Cohesion facet (`None` = wildcard).
    pub cohesion: Option<FacetMatcher>,
    /// Matcher for the Resilience facet (`None` = wildcard).
    pub resilience: Option<FacetMatcher>,
    /// Matcher for the Efficiency facet (`None` = wildcard).
    pub efficiency: Option<FacetMatcher>,
    /// Whether to emit an event when this archetype activates.
    pub alert_on_match: bool,
    /// Whether to emit an event when this archetype deactivates.
    pub alert_on_exit: bool,
    /// Higher priority archetypes are listed first in `matched_archetypes`.
    pub priority: u32,
    /// Suggested complexity styles to use when this archetype is active.
    pub suggested_styles: Vec<ComplexityStyle>,
    /// Whether this archetype is a builtin (cannot be removed or overwritten).
    pub is_builtin: bool,
}

impl ArchetypeRule {
    /// Check whether this rule matches the given psyche and trends.
    pub fn matches(&self, psyche: &SwarmPsyche, trends: &FacetTrends) -> bool {
        let check = |matcher: &Option<FacetMatcher>, facet: Facet| -> bool {
            match matcher {
                None => true,
                Some(m) => m.matches(psyche.get_facet(facet), trends.get(facet)),
            }
        };

        check(&self.exertion, Facet::Exertion)
            && check(&self.vitality, Facet::Vitality)
            && check(&self.momentum, Facet::Momentum)
            && check(&self.foresight, Facet::Foresight)
            && check(&self.cohesion, Facet::Cohesion)
            && check(&self.resilience, Facet::Resilience)
            && check(&self.efficiency, Facet::Efficiency)
    }

    /// Validate the rule for internal consistency.
    ///
    /// Returns `Ok(())` if the rule is valid, or `Err(reason)` if there
    /// is a problem (e.g., name is empty, contradictory min/max).
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("archetype rule name must not be empty".into());
        }
        if self.name.len() > 128 {
            return Err("archetype rule name must be 128 characters or fewer".into());
        }

        // Validate each per-facet matcher for contradictions.
        let matchers: [(&Option<FacetMatcher>, &str); 7] = [
            (&self.exertion, "exertion"),
            (&self.vitality, "vitality"),
            (&self.momentum, "momentum"),
            (&self.foresight, "foresight"),
            (&self.cohesion, "cohesion"),
            (&self.resilience, "resilience"),
            (&self.efficiency, "efficiency"),
        ];

        for (matcher_opt, facet_name) in &matchers {
            if let Some(matcher) = matcher_opt {
                if let (Some(min), Some(max)) = (matcher.min, matcher.max) {
                    if min > max {
                        return Err(format!(
                            "archetype '{}': {} matcher has min ({}) > max ({})",
                            self.name, facet_name, min, max
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Get the matcher for a specific facet.
    pub fn matcher_for(&self, facet: Facet) -> &Option<FacetMatcher> {
        match facet {
            Facet::Exertion => &self.exertion,
            Facet::Vitality => &self.vitality,
            Facet::Momentum => &self.momentum,
            Facet::Foresight => &self.foresight,
            Facet::Cohesion => &self.cohesion,
            Facet::Resilience => &self.resilience,
            Facet::Efficiency => &self.efficiency,
        }
    }
}

// ============================================================================
// ArchetypeStore
// ============================================================================

/// Thread-safe registry of archetype rules.
///
/// On construction, the 12 builtin archetypes are automatically loaded.
/// Custom archetypes can be added and removed at runtime, but builtins
/// cannot be removed or overwritten.
pub struct ArchetypeStore {
    rules: DashMap<String, ArchetypeRule>,
}

impl ArchetypeStore {
    /// Create a new store pre-populated with all builtin archetypes.
    pub fn new() -> Self {
        let store = Self {
            rules: DashMap::new(),
        };
        for rule in builtin_archetypes() {
            store.rules.insert(rule.name.clone(), rule);
        }
        store
    }

    /// Add a custom archetype rule.
    ///
    /// Validates the rule and rejects additions that would overwrite a builtin.
    pub fn add(&self, rule: ArchetypeRule) -> Result<(), String> {
        rule.validate()?;

        // Check for builtin collision.
        if let Some(existing) = self.rules.get(&rule.name) {
            if existing.is_builtin {
                return Err(format!(
                    "cannot overwrite builtin archetype '{}'",
                    rule.name
                ));
            }
        }

        self.rules.insert(rule.name.clone(), rule);
        Ok(())
    }

    /// Remove a custom archetype rule.
    ///
    /// Returns an error if the rule is builtin or does not exist.
    pub fn remove(&self, name: &str) -> Result<(), String> {
        match self.rules.get(name) {
            None => Err(format!("archetype '{}' not found", name)),
            Some(entry) => {
                if entry.is_builtin {
                    return Err(format!("cannot remove builtin archetype '{}'", name));
                }
                drop(entry);
                self.rules.remove(name);
                Ok(())
            }
        }
    }

    /// Get a copy of a specific archetype rule.
    pub fn get(&self, name: &str) -> Option<ArchetypeRule> {
        self.rules.get(name).map(|r| r.value().clone())
    }

    /// List all archetype rules, sorted by priority (descending).
    pub fn list(&self) -> Vec<ArchetypeRule> {
        let mut rules: Vec<ArchetypeRule> = self.rules.iter().map(|r| r.value().clone()).collect();
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        rules
    }

    /// Evaluate all rules against the given psyche and trends.
    ///
    /// Returns a vector of matching archetype names, sorted by priority
    /// (descending).
    pub fn evaluate(&self, psyche: &SwarmPsyche, trends: &FacetTrends) -> Vec<String> {
        let mut matches: Vec<(u32, String)> = self
            .rules
            .iter()
            .filter(|r| r.value().matches(psyche, trends))
            .map(|r| (r.value().priority, r.value().name.clone()))
            .collect();
        matches.sort_by(|a, b| b.0.cmp(&a.0));
        matches.into_iter().map(|(_, name)| name).collect()
    }

    /// Parse archetype rules from TOML content.
    ///
    /// Expected format:
    /// ```toml
    /// [[archetype]]
    /// name = "my-archetype"
    /// description = "Custom archetype"
    /// priority = 50
    /// alert_on_match = false
    /// alert_on_exit = false
    ///
    /// [archetype.exertion]
    /// min = "L3"
    ///
    /// [archetype.vitality]
    /// max = "L2"
    /// ```
    pub fn load_from_toml(content: &str) -> Result<Vec<ArchetypeRule>, String> {
        let parsed: toml::Value =
            toml::from_str(content).map_err(|e| format!("TOML parse error: {}", e))?;

        let archetypes = parsed
            .get("archetype")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "expected [[archetype]] array in TOML".to_string())?;

        let mut rules = Vec::new();

        for entry in archetypes {
            let name = entry
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "each archetype must have a 'name' field".to_string())?
                .to_string();

            let description = entry
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let priority = entry
                .get("priority")
                .and_then(|v| v.as_integer())
                .unwrap_or(50) as u32;

            let alert_on_match = entry
                .get("alert_on_match")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let alert_on_exit = entry
                .get("alert_on_exit")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let parse_matcher = |entry: &toml::Value, key: &str| -> Result<Option<FacetMatcher>, String> {
                let section = match entry.get(key) {
                    Some(v) => v,
                    None => return Ok(None),
                };

                let min = section
                    .get("min")
                    .and_then(|v| v.as_str())
                    .map(|s| FacetLevel::from_str(s))
                    .transpose()
                    .map_err(|e| format!("{}.min: {}", key, e))?;

                let max = section
                    .get("max")
                    .and_then(|v| v.as_str())
                    .map(|s| FacetLevel::from_str(s))
                    .transpose()
                    .map_err(|e| format!("{}.max: {}", key, e))?;

                let exact = section
                    .get("exact")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .map(FacetLevel::from_str)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()
                    .map_err(|e| format!("{}.exact: {}", key, e))?;

                let trending = section
                    .get("trending")
                    .and_then(|v| v.as_str())
                    .map(|s| match s.to_lowercase().as_str() {
                        "rising" => Ok(Trend::Rising),
                        "falling" => Ok(Trend::Falling),
                        "stable" => Ok(Trend::Stable),
                        other => Err(format!("unknown trend: {}", other)),
                    })
                    .transpose()
                    .map_err(|e| format!("{}.trending: {}", key, e))?;

                Ok(Some(FacetMatcher {
                    min,
                    max,
                    exact,
                    trending,
                }))
            };

            let rule = ArchetypeRule {
                name,
                description,
                exertion: parse_matcher(entry, "exertion")?,
                vitality: parse_matcher(entry, "vitality")?,
                momentum: parse_matcher(entry, "momentum")?,
                foresight: parse_matcher(entry, "foresight")?,
                cohesion: parse_matcher(entry, "cohesion")?,
                resilience: parse_matcher(entry, "resilience")?,
                efficiency: parse_matcher(entry, "efficiency")?,
                alert_on_match,
                alert_on_exit,
                priority,
                suggested_styles: Vec::new(),
                is_builtin: false,
            };

            rule.validate()?;
            rules.push(rule);
        }

        Ok(rules)
    }
}

// ============================================================================
// FacetTrends (sliding window trend detection)
// ============================================================================

/// Sliding-window trend tracker for all seven facets.
pub struct FacetTrends {
    windows: HashMap<Facet, SlidingWindow>,
}

impl FacetTrends {
    /// Create a new trend tracker with empty windows.
    pub fn new() -> Self {
        let mut windows = HashMap::new();
        for facet in Facet::all() {
            windows.insert(*facet, SlidingWindow::new(Duration::from_secs(300)));
        }
        Self { windows }
    }

    /// Push a new data point for a specific facet.
    pub fn push(&mut self, facet: Facet, timestamp: DateTime<Utc>, value: f64) {
        if let Some(window) = self.windows.get_mut(&facet) {
            window.push(timestamp, value);
        }
    }

    /// Get the current trend for a specific facet.
    pub fn get(&self, facet: Facet) -> Option<Trend> {
        self.windows.get(&facet).map(|w| w.trend())
    }

    /// Get trends for all facets as a map.
    pub fn all_trends(&self) -> HashMap<Facet, Trend> {
        self.windows
            .iter()
            .map(|(facet, window)| (*facet, window.trend()))
            .collect()
    }

    /// Get the average value for a facet over its window.
    pub fn average(&self, facet: Facet) -> Option<f64> {
        self.windows.get(&facet).map(|w| w.average())
    }

    /// Get the minimum value for a facet over its window.
    pub fn min_value(&self, facet: Facet) -> Option<f64> {
        self.windows.get(&facet).map(|w| w.min())
    }

    /// Get the maximum value for a facet over its window.
    pub fn max_value(&self, facet: Facet) -> Option<f64> {
        self.windows.get(&facet).map(|w| w.max())
    }

    /// Prune all windows of entries older than their max age.
    pub fn prune(&mut self) {
        for window in self.windows.values_mut() {
            window.prune();
        }
    }
}

/// A time-stamped sliding window of floating-point values.
///
/// Entries older than `max_age` are pruned on push and on explicit prune.
struct SlidingWindow {
    values: VecDeque<(DateTime<Utc>, f64)>,
    max_age: Duration,
}

impl SlidingWindow {
    /// Create an empty sliding window with the given max age.
    fn new(max_age: Duration) -> Self {
        Self {
            values: VecDeque::new(),
            max_age,
        }
    }

    /// Push a timestamped value and prune stale entries.
    fn push(&mut self, timestamp: DateTime<Utc>, value: f64) {
        self.values.push_back((timestamp, value));
        self.prune();
    }

    /// Remove entries older than `max_age`.
    fn prune(&mut self) {
        let cutoff = Utc::now() - chrono::Duration::from_std(self.max_age).unwrap_or_default();
        while let Some((ts, _)) = self.values.front() {
            if *ts < cutoff {
                self.values.pop_front();
            } else {
                break;
            }
        }
    }

    /// Compute the trend via simple linear regression on the timestamps.
    ///
    /// The slope threshold for Rising/Falling is 0.1 per unit time (where
    /// time is normalized to the window duration). Returns `Stable` for
    /// empty or single-element windows.
    fn trend(&self) -> Trend {
        if self.values.len() < 2 {
            return Trend::Stable;
        }

        // Use linear regression: y = a + b*x
        // where x = seconds since first timestamp, y = value.
        let first_ts = self.values.front().map(|(ts, _)| *ts).unwrap_or_else(Utc::now);
        let n = self.values.len() as f64;

        let mut sum_x = 0.0_f64;
        let mut sum_y = 0.0_f64;
        let mut sum_xy = 0.0_f64;
        let mut sum_xx = 0.0_f64;

        for (ts, val) in &self.values {
            let x = (*ts - first_ts).num_milliseconds() as f64 / 1000.0;
            sum_x += x;
            sum_y += val;
            sum_xy += x * val;
            sum_xx += x * x;
        }

        let denom = n * sum_xx - sum_x * sum_x;
        if denom.abs() < 1e-12 {
            return Trend::Stable;
        }

        let slope = (n * sum_xy - sum_x * sum_y) / denom;

        // Normalize slope by the time span to get a rate per window-duration.
        let time_span = (self.values.back().unwrap().0 - first_ts)
            .num_milliseconds() as f64
            / 1000.0;
        let normalized_slope = if time_span > 0.0 {
            slope * time_span
        } else {
            0.0
        };

        if normalized_slope > 0.1 {
            Trend::Rising
        } else if normalized_slope < -0.1 {
            Trend::Falling
        } else {
            Trend::Stable
        }
    }

    /// Average of all values in the window. Returns 0.0 if empty.
    fn average(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.values.iter().map(|(_, v)| v).sum();
        sum / self.values.len() as f64
    }

    /// Minimum value in the window. Returns 0.0 if empty.
    fn min(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        self.values
            .iter()
            .map(|(_, v)| *v)
            .fold(f64::INFINITY, f64::min)
    }

    /// Maximum value in the window. Returns 0.0 if empty.
    fn max(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        self.values
            .iter()
            .map(|(_, v)| *v)
            .fold(f64::NEG_INFINITY, f64::max)
    }

    /// Number of entries in the window.
    fn len(&self) -> usize {
        self.values.len()
    }
}

// ============================================================================
// PsycheDiff / FacetChange / PsycheComparator
// ============================================================================

/// A single facet's change between two psyche snapshots.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FacetChange {
    /// Which facet changed.
    pub facet: Facet,
    /// The level in the older snapshot.
    pub from: FacetLevel,
    /// The level in the newer snapshot.
    pub to: FacetLevel,
    /// Human-readable direction: "improved", "degraded", or "unchanged".
    pub direction: String,
}

/// Structured diff between two psyche snapshots.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PsycheDiff {
    /// Per-facet changes (only includes facets that actually changed).
    pub changes: Vec<FacetChange>,
    /// Archetypes that are in `newer` but not in `older`.
    pub archetypes_entered: Vec<String>,
    /// Archetypes that are in `older` but not in `newer`.
    pub archetypes_exited: Vec<String>,
    /// Overall direction: "improving", "degrading", "stable", or "mixed".
    pub overall_direction: String,
}

/// Stateless utilities for comparing psyche snapshots.
pub struct PsycheComparator;

impl PsycheComparator {
    /// Compute a structured diff between two psyche snapshots.
    ///
    /// `a` is the older snapshot, `b` is the newer snapshot.
    pub fn diff(a: &SwarmPsyche, b: &SwarmPsyche) -> PsycheDiff {
        let mut changes = Vec::new();
        let mut improved = 0_i32;
        let mut degraded = 0_i32;

        for facet in Facet::all() {
            let from = a.get_facet(*facet);
            let to = b.get_facet(*facet);
            if from != to {
                let direction = if to > from {
                    improved += 1;
                    "improved".to_string()
                } else {
                    degraded += 1;
                    "degraded".to_string()
                };
                changes.push(FacetChange {
                    facet: *facet,
                    from,
                    to,
                    direction,
                });
            }
        }

        let a_archs: HashSet<&String> = a.matched_archetypes.iter().collect();
        let b_archs: HashSet<&String> = b.matched_archetypes.iter().collect();

        let archetypes_entered: Vec<String> = b_archs
            .difference(&a_archs)
            .map(|s| (*s).clone())
            .collect();
        let archetypes_exited: Vec<String> = a_archs
            .difference(&b_archs)
            .map(|s| (*s).clone())
            .collect();

        let overall_direction = if improved > 0 && degraded == 0 {
            "improving".to_string()
        } else if degraded > 0 && improved == 0 {
            "degrading".to_string()
        } else if improved == 0 && degraded == 0 {
            "stable".to_string()
        } else {
            "mixed".to_string()
        };

        PsycheDiff {
            changes,
            archetypes_entered,
            archetypes_exited,
            overall_direction,
        }
    }

    /// Generate a human-readable summary of a diff.
    pub fn diff_summary(diff: &PsycheDiff) -> String {
        let mut lines = Vec::new();

        if diff.changes.is_empty() {
            lines.push("No facet changes.".to_string());
        } else {
            for change in &diff.changes {
                lines.push(format!(
                    "{}: {} -> {} ({})",
                    change.facet, change.from, change.to, change.direction
                ));
            }
        }

        if !diff.archetypes_entered.is_empty() {
            lines.push(format!(
                "Archetypes entered: {}",
                diff.archetypes_entered.join(", ")
            ));
        }
        if !diff.archetypes_exited.is_empty() {
            lines.push(format!(
                "Archetypes exited: {}",
                diff.archetypes_exited.join(", ")
            ));
        }

        lines.push(format!("Overall: {}", diff.overall_direction));
        lines.join("\n")
    }

    /// Compute a similarity score (0.0--1.0) between two psyche snapshots.
    ///
    /// 1.0 means identical facet levels; 0.0 means maximally different
    /// (every facet at opposite extremes).
    pub fn similarity_score(a: &SwarmPsyche, b: &SwarmPsyche) -> f64 {
        let max_distance = 4.0 * 7.0; // 7 facets, max distance 4 each (L1 to L5)
        let mut total_distance = 0.0_f64;

        for facet in Facet::all() {
            let a_val = a.get_facet(*facet).as_u8() as f64;
            let b_val = b.get_facet(*facet).as_u8() as f64;
            total_distance += (a_val - b_val).abs();
        }

        1.0 - (total_distance / max_distance)
    }
}

// ============================================================================
// PsycheBreakdown / FacetBreakdown / BreakdownInput / DataQuality
// ============================================================================

/// Detailed computation trace for diagnostics.
///
/// Shows exactly how each facet was calculated, including raw inputs,
/// weights, formulas, and data quality assessments.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PsycheBreakdown {
    /// When this breakdown was computed.
    pub computed_at: DateTime<Utc>,
    /// Per-facet breakdowns, keyed by facet name.
    pub facets: HashMap<String, FacetBreakdown>,
}

/// Detailed breakdown of a single facet's computation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FacetBreakdown {
    /// The computed level.
    pub level: FacetLevel,
    /// The raw 0.0--1.0 score before quantization to a level.
    pub raw_score: f64,
    /// The inputs that were used in the computation.
    pub inputs: Vec<BreakdownInput>,
    /// Human-readable formula description.
    pub formula: String,
    /// Assessment of input data quality.
    pub data_quality: DataQuality,
}

/// A single input to a facet computation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BreakdownInput {
    /// Name of this input (e.g., "alive_nodes", "avg_load").
    pub name: String,
    /// Numeric value of this input.
    pub value: f64,
    /// Weight of this input in the combined score.
    pub weight: f64,
    /// Where this input came from.
    pub source: String,
}

/// Assessment of the data quality underlying a facet computation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataQuality {
    /// All inputs available and fresh.
    Full,
    /// Some inputs missing, using defaults.
    Partial,
    /// Inputs older than 2 compute intervals.
    Stale,
    /// No data at all, using safe default (L3).
    Missing,
}

impl fmt::Display for DataQuality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataQuality::Full => write!(f, "full"),
            DataQuality::Partial => write!(f, "partial"),
            DataQuality::Stale => write!(f, "stale"),
            DataQuality::Missing => write!(f, "missing"),
        }
    }
}

// ============================================================================
// PsychePersistence
// ============================================================================

/// Save and load psyche history to/from disk as newline-delimited JSON (NDJSON).
///
/// Each line is a serialized `SwarmPsyche`. Files are rotated when they
/// exceed 50 MB.
pub struct PsychePersistence {
    path: PathBuf,
}

impl PsychePersistence {
    /// Create a persistence backend at the given path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Save the entire history as NDJSON, overwriting the file.
    pub fn save(&self, history: &VecDeque<SwarmPsyche>) -> Result<(), String> {
        let tmp_path = self.path.with_extension("tmp");

        let file = std::fs::File::create(&tmp_path)
            .map_err(|e| format!("failed to create temp file: {}", e))?;
        let mut writer = std::io::BufWriter::new(file);

        for snapshot in history {
            let line =
                serde_json::to_string(snapshot).map_err(|e| format!("serialize error: {}", e))?;
            writer
                .write_all(line.as_bytes())
                .map_err(|e| format!("write error: {}", e))?;
            writer
                .write_all(b"\n")
                .map_err(|e| format!("write error: {}", e))?;
        }

        writer
            .flush()
            .map_err(|e| format!("flush error: {}", e))?;
        drop(writer);

        std::fs::rename(&tmp_path, &self.path)
            .map_err(|e| format!("rename error: {}", e))?;

        Ok(())
    }

    /// Load history from NDJSON, skipping corrupt lines with a warning.
    pub fn load(&self) -> Result<VecDeque<SwarmPsyche>, String> {
        let file = match std::fs::File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(VecDeque::new());
            }
            Err(e) => return Err(format!("failed to open {}: {}", self.path.display(), e)),
        };

        let reader = std::io::BufReader::new(file);
        let mut history = VecDeque::new();
        let mut corrupt_count = 0_u64;

        for (line_num, line_result) in reader.lines().enumerate() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    warn!(
                        line = line_num + 1,
                        error = %e,
                        "psyche persistence: skipping unreadable line"
                    );
                    corrupt_count += 1;
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<SwarmPsyche>(&line) {
                Ok(snapshot) => history.push_back(snapshot),
                Err(e) => {
                    warn!(
                        line = line_num + 1,
                        error = %e,
                        "psyche persistence: skipping corrupt line"
                    );
                    corrupt_count += 1;
                }
            }
        }

        if corrupt_count > 0 {
            warn!(
                corrupt_lines = corrupt_count,
                loaded = history.len(),
                "psyche persistence: loaded with some corrupt lines"
            );
        }

        Ok(history)
    }

    /// Append a single snapshot to the file. Rotates if the file exceeds 50 MB.
    pub fn save_incremental(&self, snapshot: &SwarmPsyche) -> Result<(), String> {
        const MAX_FILE_SIZE: u64 = 50 * 1024 * 1024; // 50 MB

        // Check file size and rotate if needed.
        if let Ok(metadata) = std::fs::metadata(&self.path) {
            if metadata.len() > MAX_FILE_SIZE {
                let rotated = self.path.with_extension("old.ndjson");
                std::fs::rename(&self.path, &rotated)
                    .map_err(|e| format!("failed to rotate file: {}", e))?;
                info!(
                    rotated_to = %rotated.display(),
                    "psyche persistence: file rotated at 50 MB"
                );
            }
        }

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("failed to open file for append: {}", e))?;

        let line =
            serde_json::to_string(snapshot).map_err(|e| format!("serialize error: {}", e))?;
        file.write_all(line.as_bytes())
            .map_err(|e| format!("write error: {}", e))?;
        file.write_all(b"\n")
            .map_err(|e| format!("write error: {}", e))?;

        Ok(())
    }
}

// ============================================================================
// PsycheForecaster
// ============================================================================

/// Predicts future psyche states via linear extrapolation from history.
pub struct PsycheForecaster {
    history: Arc<RwLock<VecDeque<SwarmPsyche>>>,
}

impl PsycheForecaster {
    /// Create a new forecaster backed by the given shared history.
    pub fn new(history: Arc<RwLock<VecDeque<SwarmPsyche>>>) -> Self {
        Self { history }
    }

    /// Forecast the psyche state N minutes into the future.
    ///
    /// Uses linear extrapolation from the last N data points for each facet,
    /// clamping results to the L1--L5 range.
    ///
    /// Returns `None` if there are fewer than 6 data points in history.
    pub fn forecast(&self, minutes_ahead: u64) -> Option<SwarmPsyche> {
        let history = self.history.read();
        if history.len() < 6 {
            return None;
        }

        let now = Utc::now();
        let target = now + chrono::Duration::minutes(minutes_ahead as i64);
        let mut psyche = SwarmPsyche::default();
        psyche.computed_at = target;

        for facet in Facet::all() {
            let points: Vec<(f64, f64)> = history
                .iter()
                .map(|p| {
                    let x = (p.computed_at - now).num_seconds() as f64;
                    let y = p.get_facet(*facet).as_f64();
                    (x, y)
                })
                .collect();

            let extrapolated = linear_extrapolate(&points, (minutes_ahead * 60) as f64);
            psyche.set_facet(*facet, FacetLevel::from_fraction(extrapolated.clamp(0.0, 1.0)));
        }

        Some(psyche)
    }

    /// Estimate the probability (0.0--1.0) that an archetype will be active
    /// in `minutes_ahead` minutes.
    ///
    /// Based on whether the forecasted facet levels would satisfy the
    /// archetype's matchers. Returns `None` if forecasting is not possible.
    pub fn forecast_archetype_entry(
        &self,
        archetype: &ArchetypeRule,
        minutes_ahead: u64,
    ) -> Option<f64> {
        let forecasted = self.forecast(minutes_ahead)?;

        // Check each facet's distance to the archetype's requirements.
        let mut match_count = 0_u32;
        let mut total_facets = 0_u32;

        for facet in Facet::all() {
            let matcher = archetype.matcher_for(*facet);
            if let Some(m) = matcher {
                total_facets += 1;
                // Check if forecasted level satisfies this matcher (ignoring trend).
                if m.matches(forecasted.get_facet(*facet), None) {
                    match_count += 1;
                }
            }
        }

        if total_facets == 0 {
            // All wildcards -- always matches.
            return Some(1.0);
        }

        Some(match_count as f64 / total_facets as f64)
    }

    /// Estimate the time until a facet reaches a target level based on
    /// the current trend.
    ///
    /// Returns `None` if the trend is moving away, stable, or if there
    /// is insufficient history.
    pub fn time_to_level(&self, facet: Facet, target: FacetLevel) -> Option<Duration> {
        let history = self.history.read();
        if history.len() < 6 {
            return None;
        }

        let now = Utc::now();
        let points: Vec<(f64, f64)> = history
            .iter()
            .map(|p| {
                let x = (p.computed_at - now).num_seconds() as f64;
                let y = p.get_facet(facet).as_f64();
                (x, y)
            })
            .collect();

        let (slope, intercept) = linear_regression(&points)?;
        let target_val = target.as_f64();
        let current_val = intercept; // at x=0 (now)

        if slope.abs() < 1e-9 {
            return None; // Stable, won't reach target.
        }

        // t = (target_val - current_val) / slope
        let t_seconds = (target_val - current_val) / slope;
        if t_seconds <= 0.0 {
            return None; // Already past or moving away.
        }

        Some(Duration::from_secs_f64(t_seconds))
    }

    /// Compute a stability score (0.0 = highly volatile, 1.0 = perfectly stable).
    ///
    /// Based on the variance of each facet's level over the history window.
    pub fn stability_score(&self) -> f64 {
        let history = self.history.read();
        if history.len() < 2 {
            return 1.0; // Not enough data to measure volatility.
        }

        let mut total_variance = 0.0_f64;

        for facet in Facet::all() {
            let values: Vec<f64> = history.iter().map(|p| p.get_facet(*facet).as_f64()).collect();
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                / values.len() as f64;
            total_variance += variance;
        }

        // Max possible variance per facet is 0.25 (all values alternating L1/L5).
        // Total max = 0.25 * 7 = 1.75.
        let max_total_variance = 0.25 * 7.0;
        let normalized = (total_variance / max_total_variance).min(1.0);
        1.0 - normalized
    }
}

/// Simple linear regression returning (slope, intercept).
/// Returns `None` if the data is insufficient or degenerate.
fn linear_regression(points: &[(f64, f64)]) -> Option<(f64, f64)> {
    let n = points.len() as f64;
    if n < 2.0 {
        return None;
    }

    let mut sum_x = 0.0_f64;
    let mut sum_y = 0.0_f64;
    let mut sum_xy = 0.0_f64;
    let mut sum_xx = 0.0_f64;

    for (x, y) in points {
        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_xx += x * x;
    }

    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() < 1e-12 {
        return None;
    }

    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    Some((slope, intercept))
}

/// Linearly extrapolate from a set of points to predict the value at `target_x`.
fn linear_extrapolate(points: &[(f64, f64)], target_x: f64) -> f64 {
    match linear_regression(points) {
        Some((slope, intercept)) => intercept + slope * target_x,
        None => {
            // Fall back to last known value.
            points.last().map(|(_, y)| *y).unwrap_or(0.5)
        }
    }
}

// ============================================================================
// PsycheCalculator
// ============================================================================

/// The engine that periodically computes the swarm's psyche from live data.
///
/// Reads raw node/job/assignment data from `KnowledgeStore`, computes each
/// of the seven facets, evaluates archetype rules, detects archetype
/// transitions, and emits events through the `EventBus`.
pub struct PsycheCalculator {
    pub archetype_store: Arc<ArchetypeStore>,
    /// Shared knowledge store with live swarm data.
    knowledge: Arc<KnowledgeStore>,
    /// Event bus for emitting archetype transition events.
    event_bus: Option<Arc<EventBus>>,
    /// The most recently computed psyche snapshot.
    current: Arc<RwLock<SwarmPsyche>>,
    /// Rolling history of psyche snapshots.
    history: Arc<RwLock<VecDeque<SwarmPsyche>>>,
    /// Sliding-window trend tracker for each facet.
    trends: Arc<RwLock<FacetTrends>>,
    /// Maximum number of history entries to retain.
    max_history: usize,
    /// How often to recompute (default: 10 seconds).
    compute_interval: Duration,
    /// Previous archetype matches (for transition detection).
    prev_archetypes: Arc<RwLock<Vec<String>>>,
}

impl PsycheCalculator {
    /// Create a new calculator with the given knowledge store and event bus.
    pub fn new(
        knowledge: Arc<KnowledgeStore>,
        event_bus: Option<Arc<EventBus>>,
        archetype_store: Arc<ArchetypeStore>,
    ) -> Self {
        Self {
            knowledge,
            event_bus,
            archetype_store,
            current: Arc::new(RwLock::new(SwarmPsyche::default())),
            history: Arc::new(RwLock::new(VecDeque::new())),
            trends: Arc::new(RwLock::new(FacetTrends::new())),
            max_history: PSYCHE_HISTORY_MAX,
            compute_interval: Duration::from_secs(PSYCHE_COMPUTE_INTERVAL_SECS),
            prev_archetypes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get a clone of the current psyche snapshot.
    pub fn current(&self) -> SwarmPsyche {
        self.current.read().clone()
    }

    /// Get the most recent `limit` history entries (newest first).
    pub fn history(&self, limit: usize) -> Vec<SwarmPsyche> {
        let history = self.history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    /// Get the current trends for all facets.
    pub fn trends(&self) -> HashMap<Facet, Trend> {
        self.trends.read().all_trends()
    }

    /// Create a forecaster backed by this calculator's history.
    pub fn forecaster(&self) -> PsycheForecaster {
        PsycheForecaster::new(Arc::clone(&self.history))
    }

    /// Export the entire history as CSV.
    ///
    /// Columns: timestamp, exertion, vitality, momentum, foresight, cohesion,
    /// resilience, efficiency, archetypes
    pub fn export_history_csv(&self) -> String {
        let history = self.history.read();
        let mut csv = String::from(
            "timestamp,exertion,vitality,momentum,foresight,cohesion,resilience,efficiency,archetypes\n",
        );

        for snapshot in history.iter() {
            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{},{}\n",
                snapshot.computed_at.to_rfc3339(),
                snapshot.exertion.as_u8(),
                snapshot.vitality.as_u8(),
                snapshot.momentum.as_u8(),
                snapshot.foresight.as_u8(),
                snapshot.cohesion.as_u8(),
                snapshot.resilience.as_u8(),
                snapshot.efficiency.as_u8(),
                snapshot.matched_archetypes.join(";"),
            ));
        }

        csv
    }

    /// Find the peak (max) and trough (min) levels for a facet over the
    /// last `hours` hours of history.
    ///
    /// Returns (peak, trough). If history is empty, returns (L3, L3).
    pub fn peak_and_trough(&self, facet: Facet, hours: usize) -> (FacetLevel, FacetLevel) {
        let history = self.history.read();
        let cutoff = Utc::now() - chrono::Duration::hours(hours as i64);

        let mut peak = FacetLevel::L1;
        let mut trough = FacetLevel::L5;
        let mut found = false;

        for snapshot in history.iter() {
            if snapshot.computed_at >= cutoff {
                let level = snapshot.get_facet(facet);
                if level > peak {
                    peak = level;
                }
                if level < trough {
                    trough = level;
                }
                found = true;
            }
        }

        if !found {
            return (FacetLevel::L3, FacetLevel::L3);
        }

        (peak, trough)
    }

    /// Perform a single psyche computation cycle.
    ///
    /// 1. Gather raw data from the KnowledgeStore.
    /// 2. Compute each facet.
    /// 3. Evaluate archetypes.
    /// 4. Update trends.
    /// 5. Store in current + history.
    /// 6. Return the new psyche snapshot.
    pub fn compute_once(&self) -> SwarmPsyche {
        let now = Utc::now();

        let exertion = self.compute_exertion();
        let vitality = self.compute_vitality();
        let momentum = self.compute_momentum();
        let foresight = self.compute_foresight();
        let cohesion = self.compute_cohesion();
        let resilience = self.compute_resilience();
        let efficiency = self.compute_efficiency();

        // Update trends with the new raw fractional values.
        {
            let mut trends = self.trends.write();
            trends.push(Facet::Exertion, now, exertion.as_f64());
            trends.push(Facet::Vitality, now, vitality.as_f64());
            trends.push(Facet::Momentum, now, momentum.as_f64());
            trends.push(Facet::Foresight, now, foresight.as_f64());
            trends.push(Facet::Cohesion, now, cohesion.as_f64());
            trends.push(Facet::Resilience, now, resilience.as_f64());
            trends.push(Facet::Efficiency, now, efficiency.as_f64());
            trends.prune();
        }

        // Evaluate archetypes.
        let matched_archetypes = {
            let trends = self.trends.read();
            let mut temp_psyche = SwarmPsyche {
                exertion,
                vitality,
                momentum,
                foresight,
                cohesion,
                resilience,
                efficiency,
                matched_archetypes: Vec::new(),
                computed_at: now,
            };
            let matches = self.archetype_store.evaluate(&temp_psyche, &trends);
            temp_psyche.matched_archetypes = matches.clone();
            matches
        };

        let psyche = SwarmPsyche {
            exertion,
            vitality,
            momentum,
            foresight,
            cohesion,
            resilience,
            efficiency,
            matched_archetypes,
            computed_at: now,
        };

        // Store.
        {
            let mut current = self.current.write();
            *current = psyche.clone();
        }
        {
            let mut history = self.history.write();
            history.push_back(psyche.clone());
            while history.len() > self.max_history {
                history.pop_front();
            }
        }

        debug!(
            exertion = %psyche.exertion,
            vitality = %psyche.vitality,
            momentum = %psyche.momentum,
            foresight = %psyche.foresight,
            cohesion = %psyche.cohesion,
            resilience = %psyche.resilience,
            efficiency = %psyche.efficiency,
            archetypes = ?psyche.matched_archetypes,
            "psyche computed"
        );

        psyche
    }

    /// Perform a computation and also return a detailed breakdown.
    pub fn compute_with_breakdown(&self) -> (SwarmPsyche, PsycheBreakdown) {
        let now = Utc::now();

        let (exertion, exertion_bd) = self.compute_exertion_with_breakdown();
        let (vitality, vitality_bd) = self.compute_vitality_with_breakdown();
        let (momentum, momentum_bd) = self.compute_momentum_with_breakdown();
        let (foresight, foresight_bd) = self.compute_foresight_with_breakdown();
        let (cohesion, cohesion_bd) = self.compute_cohesion_with_breakdown();
        let (resilience, resilience_bd) = self.compute_resilience_with_breakdown();
        let (efficiency, efficiency_bd) = self.compute_efficiency_with_breakdown();

        // Update trends.
        {
            let mut trends = self.trends.write();
            trends.push(Facet::Exertion, now, exertion.as_f64());
            trends.push(Facet::Vitality, now, vitality.as_f64());
            trends.push(Facet::Momentum, now, momentum.as_f64());
            trends.push(Facet::Foresight, now, foresight.as_f64());
            trends.push(Facet::Cohesion, now, cohesion.as_f64());
            trends.push(Facet::Resilience, now, resilience.as_f64());
            trends.push(Facet::Efficiency, now, efficiency.as_f64());
            trends.prune();
        }

        let matched_archetypes = {
            let trends = self.trends.read();
            let temp_psyche = SwarmPsyche {
                exertion,
                vitality,
                momentum,
                foresight,
                cohesion,
                resilience,
                efficiency,
                matched_archetypes: Vec::new(),
                computed_at: now,
            };
            self.archetype_store.evaluate(&temp_psyche, &trends)
        };

        let psyche = SwarmPsyche {
            exertion,
            vitality,
            momentum,
            foresight,
            cohesion,
            resilience,
            efficiency,
            matched_archetypes,
            computed_at: now,
        };

        // Store.
        {
            let mut current = self.current.write();
            *current = psyche.clone();
        }
        {
            let mut history = self.history.write();
            history.push_back(psyche.clone());
            while history.len() > self.max_history {
                history.pop_front();
            }
        }

        let mut facets = HashMap::new();
        facets.insert("exertion".to_string(), exertion_bd);
        facets.insert("vitality".to_string(), vitality_bd);
        facets.insert("momentum".to_string(), momentum_bd);
        facets.insert("foresight".to_string(), foresight_bd);
        facets.insert("cohesion".to_string(), cohesion_bd);
        facets.insert("resilience".to_string(), resilience_bd);
        facets.insert("efficiency".to_string(), efficiency_bd);

        let breakdown = PsycheBreakdown {
            computed_at: now,
            facets,
        };

        (psyche, breakdown)
    }

    // ========================================================================
    // Individual facet computations
    // ========================================================================

    /// Compute the Exertion facet: aggregate load across alive nodes.
    ///
    /// `avg_load` across all alive nodes:
    /// - <20% -> L1 (Idle)
    /// - <40% -> L2 (Light)
    /// - <60% -> L3 (Moderate)
    /// - <80% -> L4 (Heavy)
    /// - >=80% -> L5 (Maxed)
    fn compute_exertion(&self) -> FacetLevel {
        let alive_nodes = self.knowledge.get_live_nodes();
        if alive_nodes.is_empty() {
            return FacetLevel::L3; // No data -> safe default.
        }

        let total_load: f64 = alive_nodes.iter().map(|n| n.load as f64).sum();
        let avg_load = total_load / alive_nodes.len() as f64;

        // Also factor in pending chunk pressure.
        let pending = self.knowledge.pending_chunk_count() as f64;
        let max_capacity = (alive_nodes.len() * 4) as f64; // MAX_CONCURRENT_CHUNKS = 4
        let pending_pressure = if max_capacity > 0.0 {
            (pending / max_capacity).min(1.0)
        } else {
            0.0
        };

        let combined = 0.7 * avg_load + 0.3 * pending_pressure;
        FacetLevel::from_fraction(combined)
    }

    /// Compute Exertion with a detailed breakdown.
    fn compute_exertion_with_breakdown(&self) -> (FacetLevel, FacetBreakdown) {
        let alive_nodes = self.knowledge.get_live_nodes();
        if alive_nodes.is_empty() {
            return (
                FacetLevel::L3,
                FacetBreakdown {
                    level: FacetLevel::L3,
                    raw_score: 0.5,
                    inputs: vec![],
                    formula: "no alive nodes, using safe default L3".into(),
                    data_quality: DataQuality::Missing,
                },
            );
        }

        let total_load: f64 = alive_nodes.iter().map(|n| n.load as f64).sum();
        let avg_load = total_load / alive_nodes.len() as f64;
        let pending = self.knowledge.pending_chunk_count() as f64;
        let max_capacity = (alive_nodes.len() * 4) as f64;
        let pending_pressure = if max_capacity > 0.0 {
            (pending / max_capacity).min(1.0)
        } else {
            0.0
        };
        let combined = 0.7 * avg_load + 0.3 * pending_pressure;
        let level = FacetLevel::from_fraction(combined);

        let breakdown = FacetBreakdown {
            level,
            raw_score: combined,
            inputs: vec![
                BreakdownInput {
                    name: "avg_load".into(),
                    value: avg_load,
                    weight: 0.7,
                    source: "knowledge_store".into(),
                },
                BreakdownInput {
                    name: "pending_pressure".into(),
                    value: pending_pressure,
                    weight: 0.3,
                    source: "knowledge_store".into(),
                },
                BreakdownInput {
                    name: "alive_nodes".into(),
                    value: alive_nodes.len() as f64,
                    weight: 0.0,
                    source: "knowledge_store".into(),
                },
            ],
            formula: "0.7 * avg_load + 0.3 * pending_pressure".into(),
            data_quality: DataQuality::Full,
        };

        (level, breakdown)
    }

    /// Compute the Vitality facet: node liveness and chunk success rate.
    ///
    /// `alive_ratio = alive / (alive + suspect + dead)`
    /// `chunk_success_rate = completed / (completed + failed)` over recent assignments
    /// `combined = 0.6 * alive_ratio + 0.4 * chunk_success_rate`
    fn compute_vitality(&self) -> FacetLevel {
        let all_nodes = self.knowledge.get_all_nodes();
        if all_nodes.is_empty() {
            return FacetLevel::L3;
        }

        let alive = all_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Alive)
            .count() as f64;
        let total = all_nodes.len() as f64;
        let alive_ratio = alive / total;

        // Chunk success rate from active jobs.
        let jobs = self.knowledge.get_active_jobs();
        let mut completed = 0_u64;
        let mut failed = 0_u64;
        for job in &jobs {
            completed += job.chunks_completed as u64;
            failed += job.chunks_failed as u64;
        }
        let chunk_success_rate = if completed + failed > 0 {
            completed as f64 / (completed + failed) as f64
        } else {
            1.0 // No chunks processed -> assume healthy.
        };

        let combined = 0.6 * alive_ratio + 0.4 * chunk_success_rate;
        FacetLevel::from_fraction(combined)
    }

    /// Compute Vitality with a detailed breakdown.
    fn compute_vitality_with_breakdown(&self) -> (FacetLevel, FacetBreakdown) {
        let all_nodes = self.knowledge.get_all_nodes();
        if all_nodes.is_empty() {
            return (
                FacetLevel::L3,
                FacetBreakdown {
                    level: FacetLevel::L3,
                    raw_score: 0.5,
                    inputs: vec![],
                    formula: "no nodes known, using safe default L3".into(),
                    data_quality: DataQuality::Missing,
                },
            );
        }

        let alive = all_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Alive)
            .count() as f64;
        let total = all_nodes.len() as f64;
        let alive_ratio = alive / total;

        let jobs = self.knowledge.get_active_jobs();
        let mut completed = 0_u64;
        let mut failed = 0_u64;
        for job in &jobs {
            completed += job.chunks_completed as u64;
            failed += job.chunks_failed as u64;
        }
        let chunk_success_rate = if completed + failed > 0 {
            completed as f64 / (completed + failed) as f64
        } else {
            1.0
        };

        let combined = 0.6 * alive_ratio + 0.4 * chunk_success_rate;
        let level = FacetLevel::from_fraction(combined);

        (
            level,
            FacetBreakdown {
                level,
                raw_score: combined,
                inputs: vec![
                    BreakdownInput {
                        name: "alive_ratio".into(),
                        value: alive_ratio,
                        weight: 0.6,
                        source: "knowledge_store".into(),
                    },
                    BreakdownInput {
                        name: "chunk_success_rate".into(),
                        value: chunk_success_rate,
                        weight: 0.4,
                        source: "knowledge_store".into(),
                    },
                    BreakdownInput {
                        name: "alive_nodes".into(),
                        value: alive,
                        weight: 0.0,
                        source: "knowledge_store".into(),
                    },
                    BreakdownInput {
                        name: "total_nodes".into(),
                        value: total,
                        weight: 0.0,
                        source: "knowledge_store".into(),
                    },
                ],
                formula: "0.6 * alive_ratio + 0.4 * chunk_success_rate".into(),
                data_quality: DataQuality::Full,
            },
        )
    }

    /// Compute the Momentum facet: rate of change in cluster size and throughput.
    ///
    /// Uses the trend tracker to estimate delta. If trends are unavailable,
    /// falls back to L3 (Steady).
    fn compute_momentum(&self) -> FacetLevel {
        let trends = self.trends.read();

        // Use vitality trend as a proxy for node growth/shrink.
        let vitality_trend = trends.get(Facet::Vitality).unwrap_or(Trend::Stable);
        // Use exertion trend as a proxy for throughput change.
        let exertion_trend = trends.get(Facet::Exertion).unwrap_or(Trend::Stable);

        let trend_score = |t: Trend| -> f64 {
            match t {
                Trend::Rising => 0.8,
                Trend::Stable => 0.5,
                Trend::Falling => 0.2,
            }
        };

        let combined = 0.5 * trend_score(vitality_trend) + 0.5 * trend_score(exertion_trend);
        FacetLevel::from_fraction(combined)
    }

    /// Compute Momentum with a detailed breakdown.
    fn compute_momentum_with_breakdown(&self) -> (FacetLevel, FacetBreakdown) {
        let trends = self.trends.read();
        let vitality_trend = trends.get(Facet::Vitality).unwrap_or(Trend::Stable);
        let exertion_trend = trends.get(Facet::Exertion).unwrap_or(Trend::Stable);

        let trend_score = |t: Trend| -> f64 {
            match t {
                Trend::Rising => 0.8,
                Trend::Stable => 0.5,
                Trend::Falling => 0.2,
            }
        };

        let v_score = trend_score(vitality_trend);
        let e_score = trend_score(exertion_trend);
        let combined = 0.5 * v_score + 0.5 * e_score;
        let level = FacetLevel::from_fraction(combined);

        (
            level,
            FacetBreakdown {
                level,
                raw_score: combined,
                inputs: vec![
                    BreakdownInput {
                        name: "vitality_trend_score".into(),
                        value: v_score,
                        weight: 0.5,
                        source: "trend_tracker".into(),
                    },
                    BreakdownInput {
                        name: "exertion_trend_score".into(),
                        value: e_score,
                        weight: 0.5,
                        source: "trend_tracker".into(),
                    },
                ],
                formula: "0.5 * vitality_trend_score + 0.5 * exertion_trend_score".into(),
                data_quality: DataQuality::Partial,
            },
        )
    }

    /// Compute the Foresight facet: awareness of upcoming scheduled work.
    ///
    /// Based on the number of pending/scheduled jobs:
    /// - 0 -> L1 (Blind)
    /// - 1-5 -> L2 (Uncertain)
    /// - 6-20 -> L3 (Aware)
    /// - 21-100 -> L4 (Prepared)
    /// - >100 -> L5 (Prescient)
    fn compute_foresight(&self) -> FacetLevel {
        let pending_jobs = self
            .knowledge
            .get_jobs_by_status(super::types::SwarmJobStatus::Pending);
        let in_progress_jobs = self
            .knowledge
            .get_jobs_by_status(super::types::SwarmJobStatus::InProgress);

        let scheduled_count = pending_jobs.len() + in_progress_jobs.len();

        match scheduled_count {
            0 => FacetLevel::L1,
            1..=5 => FacetLevel::L2,
            6..=20 => FacetLevel::L3,
            21..=100 => FacetLevel::L4,
            _ => FacetLevel::L5,
        }
    }

    /// Compute Foresight with a detailed breakdown.
    fn compute_foresight_with_breakdown(&self) -> (FacetLevel, FacetBreakdown) {
        let pending_jobs = self
            .knowledge
            .get_jobs_by_status(super::types::SwarmJobStatus::Pending);
        let in_progress_jobs = self
            .knowledge
            .get_jobs_by_status(super::types::SwarmJobStatus::InProgress);

        let scheduled_count = pending_jobs.len() + in_progress_jobs.len();
        let level = match scheduled_count {
            0 => FacetLevel::L1,
            1..=5 => FacetLevel::L2,
            6..=20 => FacetLevel::L3,
            21..=100 => FacetLevel::L4,
            _ => FacetLevel::L5,
        };

        (
            level,
            FacetBreakdown {
                level,
                raw_score: level.as_f64(),
                inputs: vec![
                    BreakdownInput {
                        name: "pending_jobs".into(),
                        value: pending_jobs.len() as f64,
                        weight: 1.0,
                        source: "knowledge_store".into(),
                    },
                    BreakdownInput {
                        name: "in_progress_jobs".into(),
                        value: in_progress_jobs.len() as f64,
                        weight: 1.0,
                        source: "knowledge_store".into(),
                    },
                ],
                formula: "bracket(pending + in_progress): 0=L1, 1-5=L2, 6-20=L3, 21-100=L4, >100=L5".into(),
                data_quality: DataQuality::Full,
            },
        )
    }

    /// Compute the Cohesion facet: network connectivity and gossip reach.
    ///
    /// Uses the ratio of alive nodes to total known nodes as a proxy for
    /// gossip reach (since we lack direct gossip-round metrics in the
    /// KnowledgeStore).
    fn compute_cohesion(&self) -> FacetLevel {
        let all_nodes = self.knowledge.get_all_nodes();
        if all_nodes.is_empty() {
            return FacetLevel::L3;
        }

        let alive = all_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Alive)
            .count() as f64;
        let suspect = all_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Suspect)
            .count() as f64;
        let dead = all_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Dead)
            .count() as f64;
        let total = all_nodes.len() as f64;

        // Gossip reach approximation.
        let reach_pct = alive / total;

        // Partition approximation: if more than 20% are suspect/dead, treat it
        // like a partition.
        let unhealthy_ratio = (suspect + dead) / total;
        let partition_count = if unhealthy_ratio > 0.4 {
            2 // Severe partition.
        } else if unhealthy_ratio > 0.2 {
            1 // Mild partition.
        } else {
            0
        };

        if partition_count > 1 {
            FacetLevel::L1
        } else if partition_count == 1 {
            FacetLevel::L2
        } else if reach_pct > 0.95 {
            FacetLevel::L5
        } else if reach_pct > 0.80 {
            FacetLevel::L4
        } else if reach_pct > 0.60 {
            FacetLevel::L3
        } else {
            FacetLevel::L2
        }
    }

    /// Compute Cohesion with a detailed breakdown.
    fn compute_cohesion_with_breakdown(&self) -> (FacetLevel, FacetBreakdown) {
        let all_nodes = self.knowledge.get_all_nodes();
        if all_nodes.is_empty() {
            return (
                FacetLevel::L3,
                FacetBreakdown {
                    level: FacetLevel::L3,
                    raw_score: 0.5,
                    inputs: vec![],
                    formula: "no nodes known, using safe default L3".into(),
                    data_quality: DataQuality::Missing,
                },
            );
        }

        let alive = all_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Alive)
            .count() as f64;
        let suspect = all_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Suspect)
            .count() as f64;
        let dead = all_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Dead)
            .count() as f64;
        let total = all_nodes.len() as f64;
        let reach_pct = alive / total;
        let unhealthy_ratio = (suspect + dead) / total;
        let partition_count = if unhealthy_ratio > 0.4 {
            2
        } else if unhealthy_ratio > 0.2 {
            1
        } else {
            0
        };

        let level = if partition_count > 1 {
            FacetLevel::L1
        } else if partition_count == 1 {
            FacetLevel::L2
        } else if reach_pct > 0.95 {
            FacetLevel::L5
        } else if reach_pct > 0.80 {
            FacetLevel::L4
        } else if reach_pct > 0.60 {
            FacetLevel::L3
        } else {
            FacetLevel::L2
        };

        (
            level,
            FacetBreakdown {
                level,
                raw_score: reach_pct,
                inputs: vec![
                    BreakdownInput {
                        name: "reach_pct".into(),
                        value: reach_pct,
                        weight: 1.0,
                        source: "knowledge_store".into(),
                    },
                    BreakdownInput {
                        name: "partition_count".into(),
                        value: partition_count as f64,
                        weight: 0.0,
                        source: "heuristic".into(),
                    },
                    BreakdownInput {
                        name: "unhealthy_ratio".into(),
                        value: unhealthy_ratio,
                        weight: 0.0,
                        source: "knowledge_store".into(),
                    },
                ],
                formula: "partition_count>1=L1, partition_count=1=L2, then bracket(reach_pct)".into(),
                data_quality: DataQuality::Partial,
            },
        )
    }

    /// Compute the Resilience facet: spare capacity, geo-diversity, redundancy.
    ///
    /// `spare_capacity = 1.0 - avg_load`
    /// `alive_redundancy = alive_nodes / max(min_required_nodes, 1)`
    /// `combined = 0.5 * spare_capacity + 0.5 * min(alive_redundancy, 1.0)`
    ///
    /// Geographic diversity is approximated as a bonus if there are alive
    /// nodes (since we lack direct geo_region data on NodeInfo).
    fn compute_resilience(&self) -> FacetLevel {
        let alive_nodes = self.knowledge.get_live_nodes();
        if alive_nodes.is_empty() {
            return FacetLevel::L1; // No alive nodes = brittle.
        }

        let total_load: f64 = alive_nodes.iter().map(|n| n.load as f64).sum();
        let avg_load = total_load / alive_nodes.len() as f64;
        let spare_capacity = 1.0 - avg_load;

        // Alive redundancy: assume at least 3 nodes are needed for resilience.
        let min_required = 3.0_f64;
        let alive_redundancy = (alive_nodes.len() as f64 / min_required).min(1.0);

        let combined = 0.5 * spare_capacity + 0.5 * alive_redundancy;
        FacetLevel::from_fraction(combined)
    }

    /// Compute Resilience with a detailed breakdown.
    fn compute_resilience_with_breakdown(&self) -> (FacetLevel, FacetBreakdown) {
        let alive_nodes = self.knowledge.get_live_nodes();
        if alive_nodes.is_empty() {
            return (
                FacetLevel::L1,
                FacetBreakdown {
                    level: FacetLevel::L1,
                    raw_score: 0.0,
                    inputs: vec![],
                    formula: "no alive nodes = brittle".into(),
                    data_quality: DataQuality::Missing,
                },
            );
        }

        let total_load: f64 = alive_nodes.iter().map(|n| n.load as f64).sum();
        let avg_load = total_load / alive_nodes.len() as f64;
        let spare_capacity = 1.0 - avg_load;
        let min_required = 3.0_f64;
        let alive_redundancy = (alive_nodes.len() as f64 / min_required).min(1.0);
        let combined = 0.5 * spare_capacity + 0.5 * alive_redundancy;
        let level = FacetLevel::from_fraction(combined);

        (
            level,
            FacetBreakdown {
                level,
                raw_score: combined,
                inputs: vec![
                    BreakdownInput {
                        name: "spare_capacity".into(),
                        value: spare_capacity,
                        weight: 0.5,
                        source: "knowledge_store".into(),
                    },
                    BreakdownInput {
                        name: "alive_redundancy".into(),
                        value: alive_redundancy,
                        weight: 0.5,
                        source: "knowledge_store".into(),
                    },
                    BreakdownInput {
                        name: "avg_load".into(),
                        value: avg_load,
                        weight: 0.0,
                        source: "knowledge_store".into(),
                    },
                ],
                formula: "0.5 * spare_capacity + 0.5 * min(alive_nodes/3, 1.0)".into(),
                data_quality: DataQuality::Full,
            },
        )
    }

    /// Compute the Efficiency facet: load distribution evenness and idle ratio.
    ///
    /// `idle_ratio = nodes_with_load < 5% / alive_nodes`
    /// `utilization_evenness = 1.0 - gini_coefficient(all_node_loads)`
    /// `combined = 0.5 * (1.0 - idle_ratio) + 0.5 * utilization_evenness`
    fn compute_efficiency(&self) -> FacetLevel {
        let alive_nodes = self.knowledge.get_live_nodes();
        if alive_nodes.is_empty() {
            return FacetLevel::L3;
        }

        let loads: Vec<f32> = alive_nodes.iter().map(|n| n.load).collect();
        let idle_count = loads.iter().filter(|l| **l < 0.05).count() as f64;
        let idle_ratio = idle_count / alive_nodes.len() as f64;
        let gini = gini_coefficient(&loads);
        let utilization_evenness = 1.0 - gini;

        let combined = 0.5 * (1.0 - idle_ratio) + 0.5 * utilization_evenness;
        FacetLevel::from_fraction(combined)
    }

    /// Compute Efficiency with a detailed breakdown.
    fn compute_efficiency_with_breakdown(&self) -> (FacetLevel, FacetBreakdown) {
        let alive_nodes = self.knowledge.get_live_nodes();
        if alive_nodes.is_empty() {
            return (
                FacetLevel::L3,
                FacetBreakdown {
                    level: FacetLevel::L3,
                    raw_score: 0.5,
                    inputs: vec![],
                    formula: "no alive nodes, using safe default L3".into(),
                    data_quality: DataQuality::Missing,
                },
            );
        }

        let loads: Vec<f32> = alive_nodes.iter().map(|n| n.load).collect();
        let idle_count = loads.iter().filter(|l| **l < 0.05).count() as f64;
        let idle_ratio = idle_count / alive_nodes.len() as f64;
        let gini = gini_coefficient(&loads);
        let utilization_evenness = 1.0 - gini;
        let combined = 0.5 * (1.0 - idle_ratio) + 0.5 * utilization_evenness;
        let level = FacetLevel::from_fraction(combined);

        (
            level,
            FacetBreakdown {
                level,
                raw_score: combined,
                inputs: vec![
                    BreakdownInput {
                        name: "idle_ratio".into(),
                        value: idle_ratio,
                        weight: 0.5,
                        source: "knowledge_store".into(),
                    },
                    BreakdownInput {
                        name: "utilization_evenness".into(),
                        value: utilization_evenness,
                        weight: 0.5,
                        source: "gini_coefficient".into(),
                    },
                    BreakdownInput {
                        name: "gini_coefficient".into(),
                        value: gini,
                        weight: 0.0,
                        source: "calculated".into(),
                    },
                ],
                formula: "0.5 * (1 - idle_ratio) + 0.5 * (1 - gini_coefficient)".into(),
                data_quality: DataQuality::Full,
            },
        )
    }

    /// Spawn the background computation loop.
    ///
    /// Runs every `compute_interval` until the shutdown signal is received.
    /// On each tick:
    /// 1. Calls `compute_once()`.
    /// 2. Compares matched archetypes with the previous cycle's archetypes.
    /// 3. Emits events for archetype entries/exits (if the archetype's
    ///    `alert_on_match` / `alert_on_exit` flags are set).
    pub fn spawn_loop(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let interval = self.compute_interval;
        tokio::spawn(async move {
            info!(
                interval_secs = interval.as_secs(),
                "psyche calculator loop starting"
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            info!("psyche calculator loop shutting down");
                            break;
                        }
                    }
                }

                let psyche = self.compute_once();

                // Detect archetype transitions.
                let current_archetypes = psyche.matched_archetypes.clone();
                let prev = {
                    let prev = self.prev_archetypes.read();
                    prev.clone()
                };

                let prev_set: HashSet<&String> = prev.iter().collect();
                let curr_set: HashSet<&String> = current_archetypes.iter().collect();

                let entered: Vec<&String> = curr_set.difference(&prev_set).copied().collect();
                let exited: Vec<&String> = prev_set.difference(&curr_set).copied().collect();

                // Emit events for transitions.
                if let Some(ref event_bus) = self.event_bus {
                    for name in &entered {
                        if let Some(rule) = self.archetype_store.get(name) {
                            if rule.alert_on_match {
                                event_bus.emit_simple(
                                    super::complexity::ConcernDomain::Psyche,
                                    super::complexity::EventSeverity::Notice,
                                    format!("Archetype entered: {}", name),
                                );
                                debug!(archetype = %name, "archetype entered");
                            }
                        }
                    }

                    for name in &exited {
                        if let Some(rule) = self.archetype_store.get(name) {
                            if rule.alert_on_exit {
                                event_bus.emit_simple(
                                    super::complexity::ConcernDomain::Psyche,
                                    super::complexity::EventSeverity::Info,
                                    format!("Archetype exited: {}", name),
                                );
                                debug!(archetype = %name, "archetype exited");
                            }
                        }
                    }
                }

                // Update prev_archetypes.
                {
                    let mut prev = self.prev_archetypes.write();
                    *prev = current_archetypes;
                }
            }
        })
    }
}

// ============================================================================
// Gini coefficient
// ============================================================================

/// Compute the Gini coefficient of a set of values.
///
/// Returns 0.0 for perfect equality and approaches 1.0 for perfect inequality.
/// Empty or single-element arrays return 0.0.
///
/// Uses the standard formula: `sum(|xi - xj|) / (2 * n * mean)` for all pairs.
fn gini_coefficient(values: &[f32]) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }

    let n = values.len() as f64;
    let mean: f64 = values.iter().map(|v| *v as f64).sum::<f64>() / n;

    if mean.abs() < 1e-12 {
        return 0.0; // All zeros.
    }

    let mut sum_abs_diff = 0.0_f64;
    for i in 0..values.len() {
        for j in 0..values.len() {
            sum_abs_diff += (values[i] as f64 - values[j] as f64).abs();
        }
    }

    sum_abs_diff / (2.0 * n * n * mean)
}

// ============================================================================
// Builtin archetypes
// ============================================================================

/// Construct the 12 builtin archetype rules.
///
/// These rules are automatically loaded into every `ArchetypeStore` and
/// cannot be removed or overwritten.
fn builtin_archetypes() -> Vec<ArchetypeRule> {
    vec![
        // 1. Zen Garden: low exertion, high vitality/cohesion/resilience/efficiency.
        ArchetypeRule {
            name: "zen-garden".into(),
            description: "Calm, healthy, well-connected swarm with spare capacity and even load distribution.".into(),
            exertion: Some(FacetMatcher::max(FacetLevel::L2)),
            vitality: Some(FacetMatcher::min(FacetLevel::L4)),
            momentum: None,
            foresight: None,
            cohesion: Some(FacetMatcher::min(FacetLevel::L4)),
            resilience: Some(FacetMatcher::min(FacetLevel::L4)),
            efficiency: Some(FacetMatcher::min(FacetLevel::L4)),
            alert_on_match: false,
            alert_on_exit: false,
            priority: 80,
            suggested_styles: vec![ComplexityStyle::Glanceable],
            is_builtin: true,
        },
        // 2. Factory Floor: moderate-to-high exertion, healthy, steady momentum, well-connected.
        ArchetypeRule {
            name: "factory-floor".into(),
            description: "Productive workhorse mode: busy but healthy, steady throughput, good connectivity.".into(),
            exertion: Some(FacetMatcher::min(FacetLevel::L3)),
            vitality: Some(FacetMatcher::min(FacetLevel::L4)),
            momentum: Some(FacetMatcher::exact_level(FacetLevel::L3)),
            foresight: None,
            cohesion: Some(FacetMatcher::min(FacetLevel::L4)),
            resilience: Some(FacetMatcher::min(FacetLevel::L3)),
            efficiency: Some(FacetMatcher::min(FacetLevel::L3)),
            alert_on_match: false,
            alert_on_exit: false,
            priority: 70,
            suggested_styles: vec![ComplexityStyle::Observable],
            is_builtin: true,
        },
        // 3. Marathon Runner: high exertion, healthy, steady pace, resilient.
        ArchetypeRule {
            name: "marathon-runner".into(),
            description: "Sustained high-load operation with healthy nodes and steady pace.".into(),
            exertion: Some(FacetMatcher::min(FacetLevel::L4)),
            vitality: Some(FacetMatcher::min(FacetLevel::L4)),
            momentum: Some(FacetMatcher::exact_level(FacetLevel::L3)),
            foresight: None,
            cohesion: None,
            resilience: Some(FacetMatcher::min(FacetLevel::L3)),
            efficiency: None,
            alert_on_match: false,
            alert_on_exit: false,
            priority: 65,
            suggested_styles: vec![ComplexityStyle::Observable],
            is_builtin: true,
        },
        // 4. Storm Warning: high foresight + accelerating momentum.
        ArchetypeRule {
            name: "storm-warning".into(),
            description: "High incoming workload awareness with accelerating growth -- prepare for surge.".into(),
            exertion: None,
            vitality: None,
            momentum: Some(FacetMatcher::min(FacetLevel::L4)),
            foresight: Some(FacetMatcher::min(FacetLevel::L4)),
            cohesion: None,
            resilience: None,
            efficiency: None,
            alert_on_match: true,
            alert_on_exit: true,
            priority: 90,
            suggested_styles: vec![ComplexityStyle::Orchestrated, ComplexityStyle::Observable],
            is_builtin: true,
        },
        // 5. Calm Before Storm: low exertion, healthy, accelerating, resilient.
        ArchetypeRule {
            name: "calm-before-storm".into(),
            description: "Low load now but momentum building -- the quiet before a burst of activity.".into(),
            exertion: Some(FacetMatcher::max(FacetLevel::L2)),
            vitality: Some(FacetMatcher::min(FacetLevel::L4)),
            momentum: Some(FacetMatcher::min(FacetLevel::L4)),
            foresight: None,
            cohesion: None,
            resilience: Some(FacetMatcher::min(FacetLevel::L3)),
            efficiency: None,
            alert_on_match: true,
            alert_on_exit: false,
            priority: 85,
            suggested_styles: vec![ComplexityStyle::Orchestrated],
            is_builtin: true,
        },
        // 6. War Room: maxed exertion, low vitality, low cohesion, fragile resilience.
        ArchetypeRule {
            name: "war-room".into(),
            description: "Crisis mode: overloaded, nodes dying, network fragmenting, no spare capacity.".into(),
            exertion: Some(FacetMatcher::exact_level(FacetLevel::L5)),
            vitality: Some(FacetMatcher::max(FacetLevel::L3)),
            momentum: None,
            foresight: None,
            cohesion: Some(FacetMatcher::max(FacetLevel::L3)),
            resilience: Some(FacetMatcher::max(FacetLevel::L2)),
            efficiency: None,
            alert_on_match: true,
            alert_on_exit: true,
            priority: 100,
            suggested_styles: vec![ComplexityStyle::Investigative, ComplexityStyle::Scriptable],
            is_builtin: true,
        },
        // 7. Walking Wounded: very low vitality and resilience.
        ArchetypeRule {
            name: "walking-wounded".into(),
            description: "Many nodes dead or suspect, minimal spare capacity -- swarm is hurting.".into(),
            exertion: None,
            vitality: Some(FacetMatcher::max(FacetLevel::L2)),
            momentum: None,
            foresight: None,
            cohesion: None,
            resilience: Some(FacetMatcher::max(FacetLevel::L2)),
            efficiency: None,
            alert_on_match: true,
            alert_on_exit: true,
            priority: 95,
            suggested_styles: vec![ComplexityStyle::Investigative],
            is_builtin: true,
        },
        // 8. Phoenix Rising: vitality trending up, strong momentum, adequate resilience.
        ArchetypeRule {
            name: "phoenix-rising".into(),
            description: "Recovery in progress: vitality improving, momentum high, resilience rebuilding.".into(),
            exertion: None,
            vitality: Some(FacetMatcher::with_trend(Trend::Rising)),
            momentum: Some(FacetMatcher::min(FacetLevel::L4)),
            foresight: None,
            cohesion: None,
            resilience: Some(FacetMatcher::min(FacetLevel::L3)),
            efficiency: None,
            alert_on_match: true,
            alert_on_exit: false,
            priority: 75,
            suggested_styles: vec![ComplexityStyle::Observable],
            is_builtin: true,
        },
        // 9. Tightrope: high exertion, healthy, efficient, but fragile resilience.
        ArchetypeRule {
            name: "tightrope".into(),
            description: "Running hot and efficient but with no safety margin -- one failure away from crisis.".into(),
            exertion: Some(FacetMatcher::min(FacetLevel::L4)),
            vitality: Some(FacetMatcher::min(FacetLevel::L4)),
            momentum: None,
            foresight: None,
            cohesion: None,
            resilience: Some(FacetMatcher::max(FacetLevel::L2)),
            efficiency: Some(FacetMatcher::min(FacetLevel::L4)),
            alert_on_match: true,
            alert_on_exit: false,
            priority: 88,
            suggested_styles: vec![ComplexityStyle::Observable, ComplexityStyle::Investigative],
            is_builtin: true,
        },
        // 10. Coasting: low exertion, healthy, but stagnant momentum and poor efficiency.
        ArchetypeRule {
            name: "coasting".into(),
            description: "Healthy but underutilized: low load, slowing momentum, poor efficiency suggests wasted resources.".into(),
            exertion: Some(FacetMatcher::max(FacetLevel::L2)),
            vitality: Some(FacetMatcher::min(FacetLevel::L4)),
            momentum: Some(FacetMatcher::max(FacetLevel::L2)),
            foresight: None,
            cohesion: None,
            resilience: None,
            efficiency: Some(FacetMatcher::max(FacetLevel::L2)),
            alert_on_match: false,
            alert_on_exit: false,
            priority: 50,
            suggested_styles: vec![ComplexityStyle::Browseable],
            is_builtin: true,
        },
        // 11. Growing Pains: surging momentum but low vitality and cohesion.
        ArchetypeRule {
            name: "growing-pains".into(),
            description: "Rapid expansion straining the swarm: new nodes joining faster than the mesh can absorb.".into(),
            exertion: None,
            vitality: Some(FacetMatcher::max(FacetLevel::L3)),
            momentum: Some(FacetMatcher::exact_level(FacetLevel::L5)),
            foresight: None,
            cohesion: Some(FacetMatcher::max(FacetLevel::L3)),
            resilience: None,
            efficiency: None,
            alert_on_match: true,
            alert_on_exit: true,
            priority: 82,
            suggested_styles: vec![ComplexityStyle::Orchestrated, ComplexityStyle::Observable],
            is_builtin: true,
        },
        // 12. Sweet Spot: all facets L3 or L4 -- balanced, productive, sustainable.
        ArchetypeRule {
            name: "sweet-spot".into(),
            description: "All facets in the healthy middle range: balanced load, good health, steady pace, adequate reserves.".into(),
            exertion: Some(FacetMatcher {
                min: Some(FacetLevel::L3),
                max: Some(FacetLevel::L4),
                exact: None,
                trending: None,
            }),
            vitality: Some(FacetMatcher {
                min: Some(FacetLevel::L3),
                max: Some(FacetLevel::L4),
                exact: None,
                trending: None,
            }),
            momentum: Some(FacetMatcher {
                min: Some(FacetLevel::L3),
                max: Some(FacetLevel::L4),
                exact: None,
                trending: None,
            }),
            foresight: Some(FacetMatcher {
                min: Some(FacetLevel::L3),
                max: Some(FacetLevel::L4),
                exact: None,
                trending: None,
            }),
            cohesion: Some(FacetMatcher {
                min: Some(FacetLevel::L3),
                max: Some(FacetLevel::L4),
                exact: None,
                trending: None,
            }),
            resilience: Some(FacetMatcher {
                min: Some(FacetLevel::L3),
                max: Some(FacetLevel::L4),
                exact: None,
                trending: None,
            }),
            efficiency: Some(FacetMatcher {
                min: Some(FacetLevel::L3),
                max: Some(FacetLevel::L4),
                exact: None,
                trending: None,
            }),
            alert_on_match: false,
            alert_on_exit: false,
            priority: 60,
            suggested_styles: vec![ComplexityStyle::Glanceable],
            is_builtin: true,
        },
    ]
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // FacetLevel tests
    // -----------------------------------------------------------------------

    #[test]
    fn facet_level_from_u8_clamps_correctly() {
        assert_eq!(FacetLevel::from_u8(0), FacetLevel::L1);
        assert_eq!(FacetLevel::from_u8(1), FacetLevel::L2);
        assert_eq!(FacetLevel::from_u8(2), FacetLevel::L3);
        assert_eq!(FacetLevel::from_u8(3), FacetLevel::L4);
        assert_eq!(FacetLevel::from_u8(4), FacetLevel::L5);
        assert_eq!(FacetLevel::from_u8(5), FacetLevel::L5);
        assert_eq!(FacetLevel::from_u8(100), FacetLevel::L5);
        assert_eq!(FacetLevel::from_u8(255), FacetLevel::L5);
    }

    #[test]
    fn facet_level_from_fraction_edge_cases() {
        assert_eq!(FacetLevel::from_fraction(0.0), FacetLevel::L1);
        assert_eq!(FacetLevel::from_fraction(0.19), FacetLevel::L1);
        assert_eq!(FacetLevel::from_fraction(0.2), FacetLevel::L2);
        assert_eq!(FacetLevel::from_fraction(0.39), FacetLevel::L2);
        assert_eq!(FacetLevel::from_fraction(0.4), FacetLevel::L3);
        assert_eq!(FacetLevel::from_fraction(0.5), FacetLevel::L3);
        assert_eq!(FacetLevel::from_fraction(0.6), FacetLevel::L4);
        assert_eq!(FacetLevel::from_fraction(0.8), FacetLevel::L5);
        assert_eq!(FacetLevel::from_fraction(1.0), FacetLevel::L5);
        // Edge cases: negative, NaN, infinity
        assert_eq!(FacetLevel::from_fraction(-0.1), FacetLevel::L1);
        assert_eq!(FacetLevel::from_fraction(-100.0), FacetLevel::L1);
        assert_eq!(FacetLevel::from_fraction(f64::NAN), FacetLevel::L1);
        assert_eq!(FacetLevel::from_fraction(f64::NEG_INFINITY), FacetLevel::L1);
        assert_eq!(FacetLevel::from_fraction(1.1), FacetLevel::L5);
        assert_eq!(FacetLevel::from_fraction(f64::INFINITY), FacetLevel::L5);
    }

    #[test]
    fn facet_level_name_for_spot_check() {
        // Check a sampling of facet/level combinations.
        assert_eq!(FacetLevel::L1.name_for(Facet::Exertion), "Idle");
        assert_eq!(FacetLevel::L3.name_for(Facet::Exertion), "Moderate");
        assert_eq!(FacetLevel::L5.name_for(Facet::Exertion), "Maxed");

        assert_eq!(FacetLevel::L1.name_for(Facet::Vitality), "Critical");
        assert_eq!(FacetLevel::L4.name_for(Facet::Vitality), "Strong");
        assert_eq!(FacetLevel::L5.name_for(Facet::Vitality), "Thriving");

        assert_eq!(FacetLevel::L1.name_for(Facet::Momentum), "Contracting");
        assert_eq!(FacetLevel::L3.name_for(Facet::Momentum), "Steady");
        assert_eq!(FacetLevel::L5.name_for(Facet::Momentum), "Surging");

        assert_eq!(FacetLevel::L1.name_for(Facet::Foresight), "Blind");
        assert_eq!(FacetLevel::L5.name_for(Facet::Foresight), "Prescient");

        assert_eq!(FacetLevel::L1.name_for(Facet::Cohesion), "Shattered");
        assert_eq!(FacetLevel::L5.name_for(Facet::Cohesion), "Unified");

        assert_eq!(FacetLevel::L1.name_for(Facet::Resilience), "Brittle");
        assert_eq!(FacetLevel::L5.name_for(Facet::Resilience), "Antifragile");

        assert_eq!(FacetLevel::L1.name_for(Facet::Efficiency), "Wasteful");
        assert_eq!(FacetLevel::L5.name_for(Facet::Efficiency), "Optimal");
    }

    #[test]
    fn facet_level_as_f64_roundtrip() {
        for level in &[FacetLevel::L1, FacetLevel::L2, FacetLevel::L3, FacetLevel::L4, FacetLevel::L5] {
            let f = level.as_f64();
            let reconstructed = FacetLevel::from_fraction(f);
            assert_eq!(*level, reconstructed, "roundtrip failed for {:?} -> {} -> {:?}", level, f, reconstructed);
        }
    }

    #[test]
    fn facet_level_color_hex() {
        assert_eq!(FacetLevel::L1.color_hex(), "#e74c3c");
        assert_eq!(FacetLevel::L2.color_hex(), "#e67e22");
        assert_eq!(FacetLevel::L3.color_hex(), "#f1c40f");
        assert_eq!(FacetLevel::L4.color_hex(), "#2ecc71");
        assert_eq!(FacetLevel::L5.color_hex(), "#3498db");
    }

    // -----------------------------------------------------------------------
    // SwarmPsyche tests
    // -----------------------------------------------------------------------

    #[test]
    fn swarm_psyche_default_is_all_l3() {
        let p = SwarmPsyche::default();
        for facet in Facet::all() {
            assert_eq!(p.get_facet(*facet), FacetLevel::L3, "default {:?} should be L3", facet);
        }
        assert!(p.matched_archetypes.is_empty());
    }

    #[test]
    fn swarm_psyche_get_set_roundtrip() {
        let mut p = SwarmPsyche::default();
        for facet in Facet::all() {
            p.set_facet(*facet, FacetLevel::L5);
            assert_eq!(p.get_facet(*facet), FacetLevel::L5);
            p.set_facet(*facet, FacetLevel::L1);
            assert_eq!(p.get_facet(*facet), FacetLevel::L1);
        }
    }

    #[test]
    fn swarm_psyche_all_green() {
        let mut p = SwarmPsyche::default();
        // All L3 -> not all green.
        assert!(!p.all_green());

        // Set all to L4 -> all green.
        for facet in Facet::all() {
            p.set_facet(*facet, FacetLevel::L4);
        }
        assert!(p.all_green());

        // Set one to L3 -> not all green.
        p.set_facet(Facet::Cohesion, FacetLevel::L3);
        assert!(!p.all_green());

        // All L5 -> all green.
        for facet in Facet::all() {
            p.set_facet(*facet, FacetLevel::L5);
        }
        assert!(p.all_green());
    }

    #[test]
    fn swarm_psyche_any_critical() {
        let p = SwarmPsyche::default();
        assert!(!p.any_critical()); // All L3 -> no critical.

        let mut p2 = SwarmPsyche::default();
        p2.set_facet(Facet::Vitality, FacetLevel::L1);
        assert!(p2.any_critical());

        // All L2 -> no critical.
        let mut p3 = SwarmPsyche::default();
        for facet in Facet::all() {
            p3.set_facet(*facet, FacetLevel::L2);
        }
        assert!(!p3.any_critical());
    }

    #[test]
    fn swarm_psyche_to_ascii_bar() {
        let p = SwarmPsyche::default();
        let bar = p.to_ascii_bar();
        // Should contain all facet names.
        assert!(bar.contains("exertion"));
        assert!(bar.contains("vitality"));
        assert!(bar.contains("momentum"));
        assert!(bar.contains("foresight"));
        assert!(bar.contains("cohesion"));
        assert!(bar.contains("resilience"));
        assert!(bar.contains("efficiency"));
        // Default L3 should show "Moderate" for exertion, "Fair" for vitality, etc.
        assert!(bar.contains("Moderate"));
        assert!(bar.contains("Fair"));
        assert!(bar.contains("Steady"));
    }

    #[test]
    fn swarm_psyche_diff_detects_changes() {
        let a = SwarmPsyche::default();
        let mut b = SwarmPsyche::default();
        b.set_facet(Facet::Exertion, FacetLevel::L5);
        b.set_facet(Facet::Vitality, FacetLevel::L1);

        let diffs = a.diff(&b);
        assert_eq!(diffs.len(), 2);

        let exertion_diff = diffs.iter().find(|(f, _, _)| *f == Facet::Exertion).unwrap();
        assert_eq!(exertion_diff.1, FacetLevel::L3);
        assert_eq!(exertion_diff.2, FacetLevel::L5);

        let vitality_diff = diffs.iter().find(|(f, _, _)| *f == Facet::Vitality).unwrap();
        assert_eq!(vitality_diff.1, FacetLevel::L3);
        assert_eq!(vitality_diff.2, FacetLevel::L1);
    }

    #[test]
    fn swarm_psyche_diff_no_changes() {
        let a = SwarmPsyche::default();
        let b = SwarmPsyche::default();
        assert!(a.diff(&b).is_empty());
    }

    // -----------------------------------------------------------------------
    // FacetMatcher tests
    // -----------------------------------------------------------------------

    #[test]
    fn facet_matcher_min_only() {
        let m = FacetMatcher::min(FacetLevel::L3);
        assert!(!m.matches(FacetLevel::L1, None));
        assert!(!m.matches(FacetLevel::L2, None));
        assert!(m.matches(FacetLevel::L3, None));
        assert!(m.matches(FacetLevel::L4, None));
        assert!(m.matches(FacetLevel::L5, None));
    }

    #[test]
    fn facet_matcher_max_only() {
        let m = FacetMatcher::max(FacetLevel::L3);
        assert!(m.matches(FacetLevel::L1, None));
        assert!(m.matches(FacetLevel::L2, None));
        assert!(m.matches(FacetLevel::L3, None));
        assert!(!m.matches(FacetLevel::L4, None));
        assert!(!m.matches(FacetLevel::L5, None));
    }

    #[test]
    fn facet_matcher_exact_list() {
        let m = FacetMatcher::one_of(vec![FacetLevel::L2, FacetLevel::L4]);
        assert!(!m.matches(FacetLevel::L1, None));
        assert!(m.matches(FacetLevel::L2, None));
        assert!(!m.matches(FacetLevel::L3, None));
        assert!(m.matches(FacetLevel::L4, None));
        assert!(!m.matches(FacetLevel::L5, None));
    }

    #[test]
    fn facet_matcher_trending() {
        let m = FacetMatcher::with_trend(Trend::Rising);
        assert!(m.matches(FacetLevel::L3, Some(Trend::Rising)));
        assert!(!m.matches(FacetLevel::L3, Some(Trend::Falling)));
        assert!(!m.matches(FacetLevel::L3, Some(Trend::Stable)));
        // No trend data -> fails.
        assert!(!m.matches(FacetLevel::L3, None));
    }

    #[test]
    fn facet_matcher_contradictory_min_max() {
        let m = FacetMatcher {
            min: Some(FacetLevel::L4),
            max: Some(FacetLevel::L2),
            exact: None,
            trending: None,
        };
        // min > max means nothing can match.
        for level in &[FacetLevel::L1, FacetLevel::L2, FacetLevel::L3, FacetLevel::L4, FacetLevel::L5] {
            assert!(!m.matches(*level, None));
        }
    }

    // -----------------------------------------------------------------------
    // ArchetypeRule matching tests
    // -----------------------------------------------------------------------

    fn make_psyche(
        ex: FacetLevel,
        vi: FacetLevel,
        mo: FacetLevel,
        fo: FacetLevel,
        co: FacetLevel,
        re: FacetLevel,
        ef: FacetLevel,
    ) -> SwarmPsyche {
        SwarmPsyche {
            exertion: ex,
            vitality: vi,
            momentum: mo,
            foresight: fo,
            cohesion: co,
            resilience: re,
            efficiency: ef,
            matched_archetypes: Vec::new(),
            computed_at: Utc::now(),
        }
    }

    fn empty_trends() -> FacetTrends {
        FacetTrends::new()
    }

    #[test]
    fn archetype_war_room_matches_expected_state() {
        let builtins = builtin_archetypes();
        let war_room = builtins.iter().find(|r| r.name == "war-room").unwrap();

        // Exertion=L5, Vitality<=L3, Cohesion<=L3, Resilience<=L2
        let psyche = make_psyche(
            FacetLevel::L5, FacetLevel::L2, FacetLevel::L3,
            FacetLevel::L3, FacetLevel::L2, FacetLevel::L1, FacetLevel::L3,
        );
        assert!(war_room.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_war_room_does_not_match_when_vitality_strong() {
        let builtins = builtin_archetypes();
        let war_room = builtins.iter().find(|r| r.name == "war-room").unwrap();

        // Vitality=L4 (Strong) should NOT match war room.
        let psyche = make_psyche(
            FacetLevel::L5, FacetLevel::L4, FacetLevel::L3,
            FacetLevel::L3, FacetLevel::L2, FacetLevel::L1, FacetLevel::L3,
        );
        assert!(!war_room.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_zen_garden_matches_expected_state() {
        let builtins = builtin_archetypes();
        let zen = builtins.iter().find(|r| r.name == "zen-garden").unwrap();

        let psyche = make_psyche(
            FacetLevel::L1, FacetLevel::L5, FacetLevel::L3,
            FacetLevel::L3, FacetLevel::L5, FacetLevel::L4, FacetLevel::L4,
        );
        assert!(zen.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_zen_garden_does_not_match_heavy_exertion() {
        let builtins = builtin_archetypes();
        let zen = builtins.iter().find(|r| r.name == "zen-garden").unwrap();

        // Exertion=L4 (Heavy) should NOT match zen garden.
        let psyche = make_psyche(
            FacetLevel::L4, FacetLevel::L5, FacetLevel::L3,
            FacetLevel::L3, FacetLevel::L5, FacetLevel::L4, FacetLevel::L4,
        );
        assert!(!zen.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_sweet_spot_matches_all_l3() {
        let builtins = builtin_archetypes();
        let sweet = builtins.iter().find(|r| r.name == "sweet-spot").unwrap();

        let psyche = SwarmPsyche::default(); // All L3.
        assert!(sweet.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_sweet_spot_matches_all_l4() {
        let builtins = builtin_archetypes();
        let sweet = builtins.iter().find(|r| r.name == "sweet-spot").unwrap();

        let psyche = make_psyche(
            FacetLevel::L4, FacetLevel::L4, FacetLevel::L4,
            FacetLevel::L4, FacetLevel::L4, FacetLevel::L4, FacetLevel::L4,
        );
        assert!(sweet.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_sweet_spot_does_not_match_l5() {
        let builtins = builtin_archetypes();
        let sweet = builtins.iter().find(|r| r.name == "sweet-spot").unwrap();

        let mut psyche = SwarmPsyche::default();
        psyche.set_facet(Facet::Exertion, FacetLevel::L5);
        assert!(!sweet.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_sweet_spot_does_not_match_l1() {
        let builtins = builtin_archetypes();
        let sweet = builtins.iter().find(|r| r.name == "sweet-spot").unwrap();

        let mut psyche = SwarmPsyche::default();
        psyche.set_facet(Facet::Vitality, FacetLevel::L1);
        assert!(!sweet.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_walking_wounded_matches() {
        let builtins = builtin_archetypes();
        let ww = builtins.iter().find(|r| r.name == "walking-wounded").unwrap();

        let psyche = make_psyche(
            FacetLevel::L3, FacetLevel::L2, FacetLevel::L3,
            FacetLevel::L3, FacetLevel::L3, FacetLevel::L2, FacetLevel::L3,
        );
        assert!(ww.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_coasting_matches() {
        let builtins = builtin_archetypes();
        let c = builtins.iter().find(|r| r.name == "coasting").unwrap();

        let psyche = make_psyche(
            FacetLevel::L1, FacetLevel::L4, FacetLevel::L2,
            FacetLevel::L3, FacetLevel::L3, FacetLevel::L3, FacetLevel::L2,
        );
        assert!(c.matches(&psyche, &empty_trends()));
    }

    #[test]
    fn archetype_tightrope_matches() {
        let builtins = builtin_archetypes();
        let t = builtins.iter().find(|r| r.name == "tightrope").unwrap();

        let psyche = make_psyche(
            FacetLevel::L4, FacetLevel::L4, FacetLevel::L3,
            FacetLevel::L3, FacetLevel::L3, FacetLevel::L2, FacetLevel::L5,
        );
        assert!(t.matches(&psyche, &empty_trends()));
    }

    // -----------------------------------------------------------------------
    // ArchetypeStore tests
    // -----------------------------------------------------------------------

    #[test]
    fn archetype_store_loads_builtins() {
        let store = ArchetypeStore::new();
        let rules = store.list();
        assert_eq!(rules.len(), 12);
        // All should be builtin.
        for rule in &rules {
            assert!(rule.is_builtin, "expected builtin: {}", rule.name);
        }
    }

    #[test]
    fn archetype_store_custom_add_remove() {
        let store = ArchetypeStore::new();
        let custom = ArchetypeRule {
            name: "custom-test".into(),
            description: "test".into(),
            exertion: None,
            vitality: None,
            momentum: None,
            foresight: None,
            cohesion: None,
            resilience: None,
            efficiency: None,
            alert_on_match: false,
            alert_on_exit: false,
            priority: 10,
            suggested_styles: vec![],
            is_builtin: false,
        };
        assert!(store.add(custom).is_ok());
        assert_eq!(store.list().len(), 13);
        assert!(store.get("custom-test").is_some());

        assert!(store.remove("custom-test").is_ok());
        assert_eq!(store.list().len(), 12);
        assert!(store.get("custom-test").is_none());
    }

    #[test]
    fn archetype_store_rejects_removal_of_builtins() {
        let store = ArchetypeStore::new();
        let result = store.remove("war-room");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot remove builtin"));
    }

    #[test]
    fn archetype_store_rejects_overwrite_of_builtins() {
        let store = ArchetypeStore::new();
        let rule = ArchetypeRule {
            name: "war-room".into(),
            description: "trying to overwrite".into(),
            exertion: None,
            vitality: None,
            momentum: None,
            foresight: None,
            cohesion: None,
            resilience: None,
            efficiency: None,
            alert_on_match: false,
            alert_on_exit: false,
            priority: 999,
            suggested_styles: vec![],
            is_builtin: false,
        };
        let result = store.add(rule);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot overwrite builtin"));
    }

    #[test]
    fn archetype_store_toml_loading_valid() {
        let toml = r#"
[[archetype]]
name = "my-rule"
description = "A custom rule"
priority = 42

[archetype.exertion]
min = "L3"

[archetype.vitality]
max = "L2"
"#;
        let rules = ArchetypeStore::load_from_toml(toml).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name, "my-rule");
        assert_eq!(rules[0].priority, 42);
        assert!(rules[0].exertion.is_some());
        assert!(rules[0].vitality.is_some());
        assert!(rules[0].momentum.is_none());
    }

    #[test]
    fn archetype_store_toml_loading_invalid() {
        let toml = "this is not valid toml at all [[[";
        let result = ArchetypeStore::load_from_toml(toml);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // SlidingWindow / FacetTrends tests
    // -----------------------------------------------------------------------

    #[test]
    fn sliding_window_rising_trend() {
        let mut window = SlidingWindow::new(Duration::from_secs(300));
        let base = Utc::now();
        for i in 0..10 {
            let ts = base + chrono::Duration::seconds(i * 10);
            window.push(ts, i as f64 * 0.1);
        }
        assert_eq!(window.trend(), Trend::Rising);
    }

    #[test]
    fn sliding_window_falling_trend() {
        let mut window = SlidingWindow::new(Duration::from_secs(300));
        let base = Utc::now();
        for i in 0..10 {
            let ts = base + chrono::Duration::seconds(i * 10);
            window.push(ts, 1.0 - i as f64 * 0.1);
        }
        assert_eq!(window.trend(), Trend::Falling);
    }

    #[test]
    fn sliding_window_stable_trend() {
        let mut window = SlidingWindow::new(Duration::from_secs(300));
        let base = Utc::now();
        for i in 0..10 {
            let ts = base + chrono::Duration::seconds(i * 10);
            window.push(ts, 0.5);
        }
        assert_eq!(window.trend(), Trend::Stable);
    }

    #[test]
    fn sliding_window_v_shape_is_stable() {
        let mut window = SlidingWindow::new(Duration::from_secs(300));
        let base = Utc::now();
        // Goes down then up -> net zero.
        let values = [0.5, 0.4, 0.3, 0.2, 0.1, 0.1, 0.2, 0.3, 0.4, 0.5];
        for (i, val) in values.iter().enumerate() {
            let ts = base + chrono::Duration::seconds(i as i64 * 10);
            window.push(ts, *val);
        }
        assert_eq!(window.trend(), Trend::Stable);
    }

    #[test]
    fn sliding_window_empty_is_stable() {
        let window = SlidingWindow::new(Duration::from_secs(300));
        assert_eq!(window.trend(), Trend::Stable);
    }

    #[test]
    fn sliding_window_single_element_is_stable() {
        let mut window = SlidingWindow::new(Duration::from_secs(300));
        window.push(Utc::now(), 0.5);
        assert_eq!(window.trend(), Trend::Stable);
    }

    // -----------------------------------------------------------------------
    // Gini coefficient tests
    // -----------------------------------------------------------------------

    #[test]
    fn gini_equal_values() {
        let vals = vec![0.5, 0.5, 0.5, 0.5];
        let g = gini_coefficient(&vals);
        assert!((g - 0.0).abs() < 1e-6, "expected ~0.0, got {}", g);
    }

    #[test]
    fn gini_one_dominates() {
        let vals = vec![0.0, 0.0, 0.0, 1.0];
        let g = gini_coefficient(&vals);
        // Gini for [0,0,0,1] = 6/(2*4*0.25*4) = 6/8 = 0.75
        assert!(g > 0.5, "expected high gini, got {}", g);
    }

    #[test]
    fn gini_empty_array() {
        let vals: Vec<f32> = vec![];
        assert!((gini_coefficient(&vals) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn gini_single_element() {
        let vals = vec![0.7];
        assert!((gini_coefficient(&vals) - 0.0).abs() < 1e-6);
    }

    // -----------------------------------------------------------------------
    // PsycheCalculator compute tests (with empty knowledge store)
    // -----------------------------------------------------------------------

    fn make_knowledge_store() -> Arc<KnowledgeStore> {
        use super::super::types::NodeId;
        Arc::new(KnowledgeStore::new(NodeId::new()))
    }

    #[test]
    fn calculator_compute_once_returns_valid_psyche() {
        let knowledge = make_knowledge_store();
        let archetype_store = Arc::new(ArchetypeStore::new());
        let calc = PsycheCalculator::new(knowledge, None, archetype_store);

        let psyche = calc.compute_once();
        // With empty knowledge store, most facets should be at safe defaults.
        assert!(psyche.computed_at <= Utc::now());
    }

    #[test]
    fn calculator_history_accumulates() {
        let knowledge = make_knowledge_store();
        let archetype_store = Arc::new(ArchetypeStore::new());
        let calc = PsycheCalculator::new(knowledge, None, archetype_store);

        calc.compute_once();
        calc.compute_once();
        calc.compute_once();

        let history = calc.history(10);
        assert_eq!(history.len(), 3);
    }

    #[test]
    fn calculator_history_wraps_at_max() {
        let knowledge = make_knowledge_store();
        let archetype_store = Arc::new(ArchetypeStore::new());
        let mut calc = PsycheCalculator::new(knowledge, None, archetype_store);
        calc.max_history = 5;

        for _ in 0..10 {
            calc.compute_once();
        }

        let history = calc.history(100);
        assert_eq!(history.len(), 5);
    }

    #[test]
    fn calculator_export_csv() {
        let knowledge = make_knowledge_store();
        let archetype_store = Arc::new(ArchetypeStore::new());
        let calc = PsycheCalculator::new(knowledge, None, archetype_store);

        calc.compute_once();
        calc.compute_once();

        let csv = calc.export_history_csv();
        let lines: Vec<&str> = csv.trim().split('\n').collect();
        assert_eq!(lines.len(), 3); // header + 2 rows
        assert!(lines[0].starts_with("timestamp,"));
    }

    #[test]
    fn calculator_peak_and_trough_empty_history() {
        let knowledge = make_knowledge_store();
        let archetype_store = Arc::new(ArchetypeStore::new());
        let calc = PsycheCalculator::new(knowledge, None, archetype_store);

        let (peak, trough) = calc.peak_and_trough(Facet::Exertion, 24);
        assert_eq!(peak, FacetLevel::L3);
        assert_eq!(trough, FacetLevel::L3);
    }

    // -----------------------------------------------------------------------
    // PsycheComparator tests
    // -----------------------------------------------------------------------

    #[test]
    fn comparator_identical_psyches_similarity_1() {
        let a = SwarmPsyche::default();
        let b = SwarmPsyche::default();
        let score = PsycheComparator::similarity_score(&a, &b);
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn comparator_opposite_psyches_low_similarity() {
        let a = make_psyche(
            FacetLevel::L1, FacetLevel::L1, FacetLevel::L1,
            FacetLevel::L1, FacetLevel::L1, FacetLevel::L1, FacetLevel::L1,
        );
        let b = make_psyche(
            FacetLevel::L5, FacetLevel::L5, FacetLevel::L5,
            FacetLevel::L5, FacetLevel::L5, FacetLevel::L5, FacetLevel::L5,
        );
        let score = PsycheComparator::similarity_score(&a, &b);
        assert!(score < 0.01, "expected near 0, got {}", score);
    }

    #[test]
    fn comparator_diff_correct_direction() {
        let a = SwarmPsyche::default(); // All L3.
        let mut b = SwarmPsyche::default();
        b.set_facet(Facet::Exertion, FacetLevel::L5);
        b.set_facet(Facet::Vitality, FacetLevel::L1);

        let diff = PsycheComparator::diff(&a, &b);
        assert_eq!(diff.overall_direction, "mixed");

        let ex_change = diff.changes.iter().find(|c| c.facet == Facet::Exertion).unwrap();
        assert_eq!(ex_change.direction, "improved");

        let vi_change = diff.changes.iter().find(|c| c.facet == Facet::Vitality).unwrap();
        assert_eq!(vi_change.direction, "degraded");
    }

    #[test]
    fn comparator_diff_stable() {
        let a = SwarmPsyche::default();
        let b = SwarmPsyche::default();
        let diff = PsycheComparator::diff(&a, &b);
        assert_eq!(diff.overall_direction, "stable");
        assert!(diff.changes.is_empty());
    }

    #[test]
    fn comparator_diff_improving() {
        let a = SwarmPsyche::default();
        let mut b = SwarmPsyche::default();
        b.set_facet(Facet::Exertion, FacetLevel::L4);
        b.set_facet(Facet::Vitality, FacetLevel::L4);

        let diff = PsycheComparator::diff(&a, &b);
        assert_eq!(diff.overall_direction, "improving");
    }

    #[test]
    fn comparator_diff_summary_readable() {
        let a = SwarmPsyche::default();
        let mut b = SwarmPsyche::default();
        b.set_facet(Facet::Exertion, FacetLevel::L5);

        let diff = PsycheComparator::diff(&a, &b);
        let summary = PsycheComparator::diff_summary(&diff);
        assert!(summary.contains("exertion"));
        assert!(summary.contains("L3 -> L5"));
        assert!(summary.contains("improving"));
    }

    // -----------------------------------------------------------------------
    // PsycheForecaster tests
    // -----------------------------------------------------------------------

    #[test]
    fn forecaster_insufficient_data_returns_none() {
        let history = Arc::new(RwLock::new(VecDeque::new()));
        let forecaster = PsycheForecaster::new(history);
        assert!(forecaster.forecast(5).is_none());
    }

    #[test]
    fn forecaster_stable_history_forecast_unchanged() {
        let mut h = VecDeque::new();
        let base = Utc::now();
        for i in 0..10 {
            let mut p = SwarmPsyche::default();
            p.computed_at = base - chrono::Duration::seconds((10 - i) * 10);
            h.push_back(p);
        }
        let history = Arc::new(RwLock::new(h));
        let forecaster = PsycheForecaster::new(history);

        let forecast = forecaster.forecast(5).unwrap();
        // All facets were L3 (0.5) throughout -> forecast should be near L3.
        for facet in Facet::all() {
            let level = forecast.get_facet(*facet);
            assert!(
                level == FacetLevel::L3 || level == FacetLevel::L2 || level == FacetLevel::L4,
                "expected near L3 for {:?}, got {:?}",
                facet,
                level
            );
        }
    }

    #[test]
    fn forecaster_stability_score_constant_history() {
        let mut h = VecDeque::new();
        let base = Utc::now();
        for i in 0..10 {
            let mut p = SwarmPsyche::default();
            p.computed_at = base - chrono::Duration::seconds((10 - i) * 10);
            h.push_back(p);
        }
        let history = Arc::new(RwLock::new(h));
        let forecaster = PsycheForecaster::new(history);

        let score = forecaster.stability_score();
        assert!(
            (score - 1.0).abs() < 1e-6,
            "constant history should give stability 1.0, got {}",
            score
        );
    }

    // -----------------------------------------------------------------------
    // PsycheBreakdown tests
    // -----------------------------------------------------------------------

    #[test]
    fn breakdown_all_facets_present() {
        let knowledge = make_knowledge_store();
        let archetype_store = Arc::new(ArchetypeStore::new());
        let calc = PsycheCalculator::new(knowledge, None, archetype_store);

        let (_psyche, breakdown) = calc.compute_with_breakdown();
        assert_eq!(breakdown.facets.len(), 7);
        assert!(breakdown.facets.contains_key("exertion"));
        assert!(breakdown.facets.contains_key("vitality"));
        assert!(breakdown.facets.contains_key("momentum"));
        assert!(breakdown.facets.contains_key("foresight"));
        assert!(breakdown.facets.contains_key("cohesion"));
        assert!(breakdown.facets.contains_key("resilience"));
        assert!(breakdown.facets.contains_key("efficiency"));
    }

    #[test]
    fn breakdown_missing_data_quality() {
        let knowledge = make_knowledge_store();
        let archetype_store = Arc::new(ArchetypeStore::new());
        let calc = PsycheCalculator::new(knowledge, None, archetype_store);

        let (_psyche, breakdown) = calc.compute_with_breakdown();
        // With empty knowledge store, exertion, vitality, cohesion, efficiency
        // should report Missing data quality.
        assert_eq!(
            breakdown.facets["exertion"].data_quality,
            DataQuality::Missing
        );
        assert_eq!(
            breakdown.facets["vitality"].data_quality,
            DataQuality::Missing
        );
    }

    // -----------------------------------------------------------------------
    // Serde roundtrip tests
    // -----------------------------------------------------------------------

    #[test]
    fn serde_roundtrip_swarm_psyche() {
        let p = SwarmPsyche::default();
        let json = serde_json::to_string(&p).unwrap();
        let deserialized: SwarmPsyche = serde_json::from_str(&json).unwrap();
        assert_eq!(p.exertion, deserialized.exertion);
        assert_eq!(p.vitality, deserialized.vitality);
        assert_eq!(p.momentum, deserialized.momentum);
        assert_eq!(p.foresight, deserialized.foresight);
        assert_eq!(p.cohesion, deserialized.cohesion);
        assert_eq!(p.resilience, deserialized.resilience);
        assert_eq!(p.efficiency, deserialized.efficiency);
    }

    #[test]
    fn serde_roundtrip_psyche_diff() {
        let diff = PsycheDiff {
            changes: vec![FacetChange {
                facet: Facet::Exertion,
                from: FacetLevel::L1,
                to: FacetLevel::L5,
                direction: "improved".into(),
            }],
            archetypes_entered: vec!["war-room".into()],
            archetypes_exited: vec![],
            overall_direction: "improving".into(),
        };
        let json = serde_json::to_string(&diff).unwrap();
        let deserialized: PsycheDiff = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.changes.len(), 1);
        assert_eq!(deserialized.archetypes_entered, vec!["war-room".to_string()]);
    }

    #[test]
    fn serde_roundtrip_psyche_breakdown() {
        let bd = PsycheBreakdown {
            computed_at: Utc::now(),
            facets: {
                let mut m = HashMap::new();
                m.insert(
                    "exertion".into(),
                    FacetBreakdown {
                        level: FacetLevel::L3,
                        raw_score: 0.45,
                        inputs: vec![BreakdownInput {
                            name: "avg_load".into(),
                            value: 0.45,
                            weight: 0.7,
                            source: "knowledge_store".into(),
                        }],
                        formula: "test formula".into(),
                        data_quality: DataQuality::Full,
                    },
                );
                m
            },
        };
        let json = serde_json::to_string(&bd).unwrap();
        let deserialized: PsycheBreakdown = serde_json::from_str(&json).unwrap();
        assert!(deserialized.facets.contains_key("exertion"));
        assert_eq!(deserialized.facets["exertion"].data_quality, DataQuality::Full);
    }

    // -----------------------------------------------------------------------
    // PsychePersistence tests
    // -----------------------------------------------------------------------

    #[test]
    fn persistence_save_and_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("psyche_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("psyche_history.ndjson");
        let persistence = PsychePersistence::new(path.clone());

        let mut history = VecDeque::new();
        for i in 0..5 {
            let mut p = SwarmPsyche::default();
            p.set_facet(Facet::Exertion, FacetLevel::from_u8(i));
            history.push_back(p);
        }

        persistence.save(&history).unwrap();
        let loaded = persistence.load().unwrap();
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].exertion, FacetLevel::L1);
        assert_eq!(loaded[4].exertion, FacetLevel::L5);

        // Cleanup.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn persistence_corrupt_line_skipped() {
        let dir = std::env::temp_dir().join(format!("psyche_test_corrupt_{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("psyche_corrupt.ndjson");

        // Write one good line and one corrupt line.
        let good = serde_json::to_string(&SwarmPsyche::default()).unwrap();
        let content = format!("{}\nthis is not json\n{}\n", good, good);
        std::fs::write(&path, content).unwrap();

        let persistence = PsychePersistence::new(path.clone());
        let loaded = persistence.load().unwrap();
        assert_eq!(loaded.len(), 2); // 2 good lines, 1 corrupt skipped.

        // Cleanup.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn persistence_incremental_append() {
        let dir = std::env::temp_dir().join(format!("psyche_test_inc_{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("psyche_inc.ndjson");
        let persistence = PsychePersistence::new(path.clone());

        persistence.save_incremental(&SwarmPsyche::default()).unwrap();
        persistence.save_incremental(&SwarmPsyche::default()).unwrap();
        persistence.save_incremental(&SwarmPsyche::default()).unwrap();

        let loaded = persistence.load().unwrap();
        assert_eq!(loaded.len(), 3);

        // Cleanup.
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------
    // Facet / Trend Display/FromStr tests
    // -----------------------------------------------------------------------

    #[test]
    fn facet_display_and_from_str() {
        for facet in Facet::all() {
            let s = facet.to_string();
            let parsed: Facet = s.parse().unwrap();
            assert_eq!(*facet, parsed);
        }
    }

    #[test]
    fn facet_level_display_and_from_str() {
        for level in &[FacetLevel::L1, FacetLevel::L2, FacetLevel::L3, FacetLevel::L4, FacetLevel::L5] {
            let s = level.to_string();
            let parsed: FacetLevel = s.parse().unwrap();
            assert_eq!(*level, parsed);
        }
    }

    #[test]
    fn archetype_rule_validate_empty_name() {
        let rule = ArchetypeRule {
            name: "".into(),
            description: "test".into(),
            exertion: None,
            vitality: None,
            momentum: None,
            foresight: None,
            cohesion: None,
            resilience: None,
            efficiency: None,
            alert_on_match: false,
            alert_on_exit: false,
            priority: 10,
            suggested_styles: vec![],
            is_builtin: false,
        };
        assert!(rule.validate().is_err());
    }

    #[test]
    fn archetype_rule_validate_contradictory_matcher() {
        let rule = ArchetypeRule {
            name: "test".into(),
            description: "test".into(),
            exertion: Some(FacetMatcher {
                min: Some(FacetLevel::L5),
                max: Some(FacetLevel::L1),
                exact: None,
                trending: None,
            }),
            vitality: None,
            momentum: None,
            foresight: None,
            cohesion: None,
            resilience: None,
            efficiency: None,
            alert_on_match: false,
            alert_on_exit: false,
            priority: 10,
            suggested_styles: vec![],
            is_builtin: false,
        };
        assert!(rule.validate().is_err());
    }

    #[test]
    fn all_builtin_archetypes_validate() {
        for rule in builtin_archetypes() {
            rule.validate().unwrap_or_else(|e| {
                panic!("builtin archetype '{}' failed validation: {}", rule.name, e);
            });
        }
    }
}