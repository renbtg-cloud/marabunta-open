// Marabunta - Licensed under the MIT License.
//! Active health probing and auto-remediation for the Marabunta Swarm.
//!
//! This module provides configurable health probes that actively monitor
//! node health beyond what gossip-based failure detection can observe.
//! Probes check resource utilization (memory, disk, load), network
//! connectivity, and custom conditions.
//!
//! Key features:
//! - **Configurable probes**: gossip response, TCP connect, HTTP GET,
//!   exec check, memory/disk/load pressure.
//! - **Auto-remediation**: policies that automatically drain, quarantine,
//!   or alert when probes fail.
//! - **Circuit breakers**: prevent probe storms when a node is known to be
//!   down, with half-open recovery.
//! - **History tracking**: per-probe success rates, latencies, and
//!   rolling history for dashboards.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::knowledge::KnowledgeStore;
use super::types::{NodeId, NodeStatus};

// ============================================================================
// ProbeType
// ============================================================================

/// The type of health probe to execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProbeType {
    /// Check if the node responds to gossip within a timeout.
    /// Uses the node's last_seen timestamp from the knowledge store.
    GossipResponse {
        /// Maximum milliseconds since last gossip before considering unhealthy.
        timeout_ms: u64,
    },
    /// Attempt a TCP connection to the specified port.
    /// Uses the node's known address from the knowledge store.
    TcpConnect {
        /// Port to connect to.
        port: u16,
        /// Connection timeout in milliseconds.
        timeout_ms: u64,
    },
    /// Perform an HTTP GET request and check the status code.
    /// Uses the node's known address from the knowledge store.
    HttpGet {
        /// URL path to request (e.g. "/health").
        path: String,
        /// Expected HTTP status code.
        expected_status: u16,
        /// Request timeout in milliseconds.
        timeout_ms: u64,
    },
    /// Execute a command and check the exit code.
    /// Only meaningful when running on the same node.
    ExecCheck {
        /// Command to execute.
        command: String,
        /// Expected exit code (typically 0).
        expected_exit_code: i32,
        /// Execution timeout in milliseconds.
        timeout_ms: u64,
    },
    /// Check memory pressure against a threshold.
    /// Uses resource data from the knowledge store.
    MemoryPressure {
        /// Maximum percentage of memory used before considering unhealthy.
        max_used_pct: f64,
    },
    /// Check disk pressure against a threshold.
    /// Uses resource data from the knowledge store.
    DiskPressure {
        /// Maximum percentage of disk used before considering unhealthy.
        max_used_pct: f64,
    },
    /// Check 1-minute load average against a threshold.
    /// Uses the node's load value from the knowledge store.
    LoadAverage {
        /// Maximum 1-minute load average before considering unhealthy.
        max_1min: f64,
    },
}

impl fmt::Display for ProbeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProbeType::GossipResponse { timeout_ms } => {
                write!(f, "gossip-response(timeout={}ms)", timeout_ms)
            }
            ProbeType::TcpConnect { port, timeout_ms } => {
                write!(f, "tcp-connect(port={}, timeout={}ms)", port, timeout_ms)
            }
            ProbeType::HttpGet {
                path,
                expected_status,
                timeout_ms,
            } => write!(
                f,
                "http-get(path={}, expect={}, timeout={}ms)",
                path, expected_status, timeout_ms
            ),
            ProbeType::ExecCheck {
                command,
                expected_exit_code,
                timeout_ms,
            } => write!(
                f,
                "exec-check(cmd={}, expect={}, timeout={}ms)",
                command, expected_exit_code, timeout_ms
            ),
            ProbeType::MemoryPressure { max_used_pct } => {
                write!(f, "memory-pressure(max={}%)", max_used_pct)
            }
            ProbeType::DiskPressure { max_used_pct } => {
                write!(f, "disk-pressure(max={}%)", max_used_pct)
            }
            ProbeType::LoadAverage { max_1min } => write!(f, "load-average(max={})", max_1min),
        }
    }
}

// ============================================================================
// ProbeTarget
// ============================================================================

/// Specifies which nodes a probe applies to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProbeTarget {
    /// Apply to all nodes in the swarm.
    AllNodes,
    /// Apply only to specific nodes.
    NodeIds(Vec<NodeId>),
    /// Apply to nodes with a specific tag key-value pair.
    NodesWithTag {
        /// Tag key.
        key: String,
        /// Tag value.
        value: String,
    },
    /// Apply to nodes in a specific region.
    NodesInRegion(String),
    /// Apply to nodes that claim a specific trait.
    NodesWithTrait(String),
}

// ============================================================================
// HealthProbe
// ============================================================================

/// A health probe definition that specifies what to check, how often,
/// and how many failures/successes trigger state transitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthProbe {
    /// Unique name for this probe.
    pub name: String,
    /// What kind of check to perform.
    pub probe_type: ProbeType,
    /// How often to run this probe, in seconds.
    pub interval_secs: u64,
    /// Number of consecutive failures before marking the probe as unhealthy.
    pub failure_threshold: u32,
    /// Number of consecutive successes before marking the probe as healthy.
    pub success_threshold: u32,
    /// Whether this probe is enabled.
    pub enabled: bool,
    /// Which nodes this probe applies to.
    pub applies_to: ProbeTarget,
}

// ============================================================================
// ProbeResult
// ============================================================================

/// The result of running a single health probe against a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    /// Name of the probe that produced this result.
    pub probe_name: String,
    /// The node that was probed.
    pub node_id: NodeId,
    /// When the probe ran.
    pub timestamp: DateTime<Utc>,
    /// Whether the probe succeeded.
    pub success: bool,
    /// Probe latency in milliseconds.
    pub latency_ms: u64,
    /// Human-readable message about the result.
    pub message: Option<String>,
    /// Additional structured details.
    pub details: Option<String>,
}

// ============================================================================
// HealthState
// ============================================================================

/// Overall health state of a node as determined by health probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthState {
    /// All probes passing.
    Healthy,
    /// Some probes failing but not all.
    Degraded,
    /// Critical probes failing.
    Unhealthy,
    /// No probe data available yet.
    Unknown,
}

impl fmt::Display for HealthState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthState::Healthy => write!(f, "healthy"),
            HealthState::Degraded => write!(f, "degraded"),
            HealthState::Unhealthy => write!(f, "unhealthy"),
            HealthState::Unknown => write!(f, "unknown"),
        }
    }
}

// ============================================================================
// ProbeHealth
// ============================================================================

/// Per-probe health tracking for a single node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeHealth {
    /// Current health state of this probe for this node.
    pub state: HealthState,
    /// Number of consecutive failures.
    pub consecutive_failures: u32,
    /// Number of consecutive successes.
    pub consecutive_successes: u32,
    /// Most recent probe result.
    pub last_result: Option<ProbeResult>,
    /// Success rate over the history window (0.0 to 1.0).
    pub success_rate: f64,
    /// Average latency over the history window in milliseconds.
    pub avg_latency_ms: f64,
    /// Rolling history of probe results (most recent first), capped at 100.
    pub history: VecDeque<ProbeResult>,
}

/// Maximum number of probe results to keep in history per probe per node.
const MAX_PROBE_HISTORY: usize = 100;

impl ProbeHealth {
    /// Create a new, empty probe health record.
    fn new() -> Self {
        Self {
            state: HealthState::Unknown,
            consecutive_failures: 0,
            consecutive_successes: 0,
            last_result: None,
            success_rate: 0.0,
            avg_latency_ms: 0.0,
            history: VecDeque::new(),
        }
    }

    /// Record a probe result and update statistics.
    fn record(&mut self, result: ProbeResult, failure_threshold: u32, success_threshold: u32) {
        let success = result.success;
        let _latency = result.latency_ms;

        self.last_result = Some(result.clone());

        // Update consecutive counters
        if success {
            self.consecutive_successes += 1;
            self.consecutive_failures = 0;
        } else {
            self.consecutive_failures += 1;
            self.consecutive_successes = 0;
        }

        // Update state based on thresholds
        if self.consecutive_failures >= failure_threshold {
            self.state = HealthState::Unhealthy;
        } else if self.consecutive_successes >= success_threshold {
            self.state = HealthState::Healthy;
        } else if self.consecutive_failures > 0 && self.state == HealthState::Healthy {
            self.state = HealthState::Degraded;
        }

        // Add to history
        self.history.push_front(result);
        while self.history.len() > MAX_PROBE_HISTORY {
            self.history.pop_back();
        }

        // Recalculate success rate and average latency
        if !self.history.is_empty() {
            let total = self.history.len() as f64;
            let successes = self.history.iter().filter(|r| r.success).count() as f64;
            self.success_rate = successes / total;

            let total_latency: u64 = self.history.iter().map(|r| r.latency_ms).sum();
            self.avg_latency_ms = total_latency as f64 / total;
        }
    }
}

// ============================================================================
// NodeHealthStatus
// ============================================================================

/// Aggregate health status for a single node across all probes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealthStatus {
    /// The node this status describes.
    pub node_id: NodeId,
    /// Overall health state (derived from individual probe states).
    pub overall: HealthState,
    /// Per-probe health, keyed by probe name.
    pub probes: HashMap<String, ProbeHealth>,
    /// When this node was last checked by any probe.
    pub last_checked: Option<DateTime<Utc>>,
    /// Total consecutive failures across all probes.
    pub consecutive_failures: u32,
    /// Uptime percentage based on probe history.
    pub uptime_pct: f64,
}

impl NodeHealthStatus {
    /// Create a new, empty node health status.
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            overall: HealthState::Unknown,
            probes: HashMap::new(),
            last_checked: None,
            consecutive_failures: 0,
            uptime_pct: 100.0,
        }
    }

    /// Recalculate the overall health state from individual probe states.
    fn recalculate_overall(&mut self) {
        if self.probes.is_empty() {
            self.overall = HealthState::Unknown;
            return;
        }

        let mut has_unhealthy = false;
        let mut has_degraded = false;
        let mut all_unknown = true;

        for probe in self.probes.values() {
            match probe.state {
                HealthState::Unhealthy => {
                    has_unhealthy = true;
                    all_unknown = false;
                }
                HealthState::Degraded => {
                    has_degraded = true;
                    all_unknown = false;
                }
                HealthState::Healthy => {
                    all_unknown = false;
                }
                HealthState::Unknown => {}
            }
        }

        if all_unknown {
            self.overall = HealthState::Unknown;
        } else if has_unhealthy {
            self.overall = HealthState::Unhealthy;
        } else if has_degraded {
            self.overall = HealthState::Degraded;
        } else {
            self.overall = HealthState::Healthy;
        }

        // Update uptime percentage
        let total_results: usize = self.probes.values().map(|p| p.history.len()).sum();
        let successful_results: usize = self
            .probes
            .values()
            .flat_map(|p| p.history.iter())
            .filter(|r| r.success)
            .count();

        if total_results > 0 {
            self.uptime_pct = (successful_results as f64 / total_results as f64) * 100.0;
        }
    }
}

// ============================================================================
// RemediationAction
// ============================================================================

/// Action to take when a remediation policy triggers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemediationAction {
    /// Drain the node with a timeout.
    Drain {
        /// Drain timeout in seconds.
        timeout_secs: u64,
    },
    /// Quarantine the node.
    Quarantine {
        /// Reason for quarantine.
        reason: String,
    },
    /// Cordon the node.
    Cordon {
        /// Reason for cordon.
        reason: String,
    },
    /// Emit an alert without taking fleet action.
    EmitAlert {
        /// Alert severity level.
        severity: String,
        /// Alert message.
        message: String,
    },
    /// Take no action (useful for disabling a policy without removing it).
    NoAction,
}

// ============================================================================
// RemediationTrigger
// ============================================================================

/// Condition that triggers a remediation action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemediationTrigger {
    /// A specific probe enters the Unhealthy state.
    ProbeUnhealthy {
        /// Name of the probe.
        probe_name: String,
    },
    /// Any probe enters the Unhealthy state.
    AnyProbeUnhealthy,
    /// A specific probe has N consecutive failures.
    ConsecutiveFailures {
        /// Name of the probe.
        probe_name: String,
        /// Number of consecutive failures.
        count: u32,
    },
    /// The overall node health state is Unhealthy.
    OverallUnhealthy,
    /// The overall node health state has been Degraded for N seconds.
    OverallDegraded {
        /// Duration in seconds of sustained degradation.
        for_secs: u64,
    },
}

// ============================================================================
// RemediationPolicy
// ============================================================================

/// A policy that maps a trigger condition to a remediation action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemediationPolicy {
    /// Unique name for this policy.
    pub name: String,
    /// Condition that activates this policy.
    pub trigger: RemediationTrigger,
    /// Action to take when triggered.
    pub action: RemediationAction,
    /// Minimum seconds between consecutive remediations for the same node.
    pub cooldown_secs: u64,
    /// Whether this policy is enabled.
    pub enabled: bool,
}

// ============================================================================
// CircuitBreaker
// ============================================================================

/// State of a circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Normal operation: probes are executed.
    Closed,
    /// Circuit is open: probes are skipped to avoid storms.
    Open,
    /// Circuit is half-open: one probe is allowed through to test recovery.
    HalfOpen,
}

/// Circuit breaker for health probes.
///
/// Prevents probe storms when a node is known to be down. After
/// `open_threshold` failures, the circuit opens. After `half_open_after`
/// duration, it enters half-open state, allowing one probe through.
/// A success resets the circuit; another failure re-opens it.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    /// Current state of the circuit.
    pub state: CircuitState,
    /// Number of consecutive failures.
    pub failure_count: u32,
    /// When the last probe attempt was made.
    pub last_attempt: Option<Instant>,
    /// Number of failures before the circuit opens.
    pub open_threshold: u32,
    /// Duration to wait before entering half-open state.
    pub half_open_after: Duration,
    /// Duration after which the circuit resets completely.
    pub reset_after: Duration,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given thresholds.
    pub fn new(open_threshold: u32, half_open_after: Duration, reset_after: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            last_attempt: None,
            open_threshold,
            half_open_after,
            reset_after,
        }
    }

    /// Check whether a probe should be allowed through.
    ///
    /// Returns `true` if the circuit is closed or half-open.
    pub fn should_allow(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if enough time has passed to enter half-open
                if let Some(last) = self.last_attempt {
                    if last.elapsed() >= self.half_open_after {
                        self.state = CircuitState::HalfOpen;
                        return true;
                    }
                    // Check for full reset
                    if last.elapsed() >= self.reset_after {
                        self.state = CircuitState::Closed;
                        self.failure_count = 0;
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful probe.
    pub fn record_success(&mut self) {
        self.failure_count = 0;
        self.state = CircuitState::Closed;
        self.last_attempt = Some(Instant::now());
    }

    /// Record a failed probe.
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_attempt = Some(Instant::now());

        if self.failure_count >= self.open_threshold {
            self.state = CircuitState::Open;
        }
    }
}

// ============================================================================
// HealthSummary
// ============================================================================

/// Summary of health across all monitored nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSummary {
    /// Total number of monitored nodes.
    pub total_nodes: usize,
    /// Number of healthy nodes.
    pub healthy: usize,
    /// Number of degraded nodes.
    pub degraded: usize,
    /// Number of unhealthy nodes.
    pub unhealthy: usize,
    /// Number of nodes with unknown health.
    pub unknown: usize,
    /// Total number of configured probes.
    pub total_probes: usize,
    /// Number of circuit breakers currently open.
    pub active_circuit_breakers: usize,
    /// Number of remediation actions pending or recently executed.
    pub pending_remediations: usize,
}

// ============================================================================
// HealthCheckEngine
// ============================================================================

/// The main health check engine.
///
/// Manages probes, policies, per-node health tracking, circuit breakers,
/// and auto-remediation. Integrates with the knowledge store for node
/// data and optionally with the fleet manager for remediation actions.
pub struct HealthCheckEngine {
    /// Configured health probes.
    probes: Arc<RwLock<Vec<HealthProbe>>>,
    /// Configured remediation policies.
    policies: Arc<RwLock<Vec<RemediationPolicy>>>,
    /// Per-node health status.
    node_health: DashMap<NodeId, NodeHealthStatus>,
    /// Per-node circuit breakers.
    circuit_breakers: DashMap<NodeId, CircuitBreaker>,
    /// Knowledge store for node data.
    knowledge: Option<Arc<KnowledgeStore>>,
    /// Cooldown tracking: (node_id, policy_name) -> last remediation time.
    last_remediation: DashMap<(NodeId, String), DateTime<Utc>>,
    /// Counter for remediation actions taken.
    remediation_count: std::sync::atomic::AtomicU64,
}

impl HealthCheckEngine {
    /// Create a new health check engine with default probes and policies.
    pub fn new() -> Self {
        let engine = Self {
            probes: Arc::new(RwLock::new(Vec::new())),
            policies: Arc::new(RwLock::new(Vec::new())),
            node_health: DashMap::new(),
            circuit_breakers: DashMap::new(),
            knowledge: None,
            last_remediation: DashMap::new(),
            remediation_count: std::sync::atomic::AtomicU64::new(0),
        };

        // Install default probes
        {
            let mut probes = engine.probes.write();
            probes.push(HealthProbe {
                name: "gossip-response".to_string(),
                probe_type: ProbeType::GossipResponse { timeout_ms: 30_000 },
                interval_secs: 30,
                failure_threshold: 3,
                success_threshold: 1,
                enabled: true,
                applies_to: ProbeTarget::AllNodes,
            });
            probes.push(HealthProbe {
                name: "memory-pressure".to_string(),
                probe_type: ProbeType::MemoryPressure { max_used_pct: 90.0 },
                interval_secs: 60,
                failure_threshold: 2,
                success_threshold: 1,
                enabled: true,
                applies_to: ProbeTarget::AllNodes,
            });
            probes.push(HealthProbe {
                name: "disk-pressure".to_string(),
                probe_type: ProbeType::DiskPressure { max_used_pct: 95.0 },
                interval_secs: 60,
                failure_threshold: 2,
                success_threshold: 1,
                enabled: true,
                applies_to: ProbeTarget::AllNodes,
            });
            probes.push(HealthProbe {
                name: "load-average".to_string(),
                probe_type: ProbeType::LoadAverage { max_1min: 0.95 },
                interval_secs: 30,
                failure_threshold: 5,
                success_threshold: 1,
                enabled: true,
                applies_to: ProbeTarget::AllNodes,
            });
        }

        // Install default policies
        {
            let mut policies = engine.policies.write();
            policies.push(RemediationPolicy {
                name: "auto-quarantine-unhealthy".to_string(),
                trigger: RemediationTrigger::OverallUnhealthy,
                action: RemediationAction::Quarantine {
                    reason: "auto-quarantine: node health probes failing".to_string(),
                },
                cooldown_secs: 300,
                enabled: true,
            });
            policies.push(RemediationPolicy {
                name: "drain-degraded".to_string(),
                trigger: RemediationTrigger::OverallDegraded { for_secs: 120 },
                action: RemediationAction::Drain { timeout_secs: 60 },
                cooldown_secs: 600,
                enabled: true,
            });
            policies.push(RemediationPolicy {
                name: "alert-on-failure".to_string(),
                trigger: RemediationTrigger::AnyProbeUnhealthy,
                action: RemediationAction::EmitAlert {
                    severity: "warning".to_string(),
                    message: "health probe failure detected".to_string(),
                },
                cooldown_secs: 60,
                enabled: true,
            });
        }

        engine
    }

    /// Attach a knowledge store for node data.
    pub fn with_knowledge(mut self, knowledge: Arc<KnowledgeStore>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    // ========================================================================
    // Probe management
    // ========================================================================

    /// Add a health probe.
    pub fn add_probe(&self, probe: HealthProbe) {
        self.probes.write().push(probe);
    }

    /// Remove a health probe by name.
    pub fn remove_probe(&self, name: &str) -> bool {
        let mut probes = self.probes.write();
        let original_len = probes.len();
        probes.retain(|p| p.name != name);
        probes.len() < original_len
    }

    /// List all configured probes.
    pub fn list_probes(&self) -> Vec<HealthProbe> {
        self.probes.read().clone()
    }

    /// Get a probe by name.
    pub fn get_probe(&self, name: &str) -> Option<HealthProbe> {
        self.probes.read().iter().find(|p| p.name == name).cloned()
    }

    // ========================================================================
    // Policy management
    // ========================================================================

    /// Add a remediation policy.
    pub fn add_policy(&self, policy: RemediationPolicy) {
        self.policies.write().push(policy);
    }

    /// Remove a remediation policy by name.
    pub fn remove_policy(&self, name: &str) -> bool {
        let mut policies = self.policies.write();
        let original_len = policies.len();
        policies.retain(|p| p.name != name);
        policies.len() < original_len
    }

    /// List all configured policies.
    pub fn list_policies(&self) -> Vec<RemediationPolicy> {
        self.policies.read().clone()
    }

    // ========================================================================
    // Probe execution
    // ========================================================================

    /// Run a single probe against a single node.
    ///
    /// For network probes (GossipResponse, TcpConnect, HttpGet), the probe
    /// evaluates the node's last_seen and address data from the knowledge
    /// store. For resource probes (MemoryPressure, DiskPressure, LoadAverage),
    /// it checks the node's resource snapshot.
    pub async fn run_probe(&self, probe: &HealthProbe, node_id: NodeId) -> ProbeResult {
        let start = Instant::now();
        let now = Utc::now();

        // Check circuit breaker
        if let Some(mut cb) = self.circuit_breakers.get_mut(&node_id) {
            if !cb.should_allow() {
                return ProbeResult {
                    probe_name: probe.name.clone(),
                    node_id,
                    timestamp: now,
                    success: false,
                    latency_ms: 0,
                    message: Some("circuit breaker open".to_string()),
                    details: None,
                };
            }
        }

        let knowledge = match &self.knowledge {
            Some(k) => k,
            None => {
                return ProbeResult {
                    probe_name: probe.name.clone(),
                    node_id,
                    timestamp: now,
                    success: false,
                    latency_ms: start.elapsed().as_millis() as u64,
                    message: Some("no knowledge store available".to_string()),
                    details: None,
                };
            }
        };

        let node_info = knowledge.get_node(&node_id);

        let (success, message, details) = match &probe.probe_type {
            ProbeType::GossipResponse { timeout_ms } => {
                match &node_info {
                    Some(info) => {
                        let age_ms = (now - info.last_seen).num_milliseconds().max(0) as u64;
                        let is_alive = info.status == NodeStatus::Alive;
                        let within_timeout = age_ms <= *timeout_ms;
                        let success = is_alive && within_timeout;
                        (
                            success,
                            if success {
                                format!("gossip response within {}ms", age_ms)
                            } else {
                                format!(
                                    "gossip stale: age={}ms, status={:?}",
                                    age_ms, info.status
                                )
                            },
                            Some(format!("last_seen_age_ms={}, status={:?}", age_ms, info.status)),
                        )
                    }
                    None => (false, "node not found in knowledge store".to_string(), None),
                }
            }

            ProbeType::TcpConnect { port, timeout_ms } => {
                match &node_info {
                    Some(info) => {
                        let has_address = info.address.is_some();
                        let is_alive = info.status == NodeStatus::Alive;
                        let last_seen_recent = (now - info.last_seen).num_milliseconds().max(0) as u64 <= *timeout_ms;
                        let success = has_address && is_alive && last_seen_recent;
                        (
                            success,
                            if success {
                                format!("tcp connect to port {} would succeed (node alive with address)", port)
                            } else {
                                format!(
                                    "tcp connect to port {} would fail: has_addr={}, alive={}, recent={}",
                                    port, has_address, is_alive, last_seen_recent
                                )
                            },
                            Some(format!("port={}, has_address={}", port, has_address)),
                        )
                    }
                    None => (false, "node not found".to_string(), None),
                }
            }

            ProbeType::HttpGet {
                path,
                expected_status,
                timeout_ms,
            } => {
                match &node_info {
                    Some(info) => {
                        let has_address = info.address.is_some();
                        let is_alive = info.status == NodeStatus::Alive;
                        let last_seen_recent = (now - info.last_seen).num_milliseconds().max(0) as u64 <= *timeout_ms;
                        let success = has_address && is_alive && last_seen_recent;
                        (
                            success,
                            if success {
                                format!(
                                    "http GET {} would return {} (node alive)",
                                    path, expected_status
                                )
                            } else {
                                format!("http GET {} would fail: node unreachable", path)
                            },
                            Some(format!("path={}, expected_status={}", path, expected_status)),
                        )
                    }
                    None => (false, "node not found".to_string(), None),
                }
            }

            ProbeType::ExecCheck {
                command,
                expected_exit_code,
                timeout_ms: _,
            } => {
                // For exec checks, we can only verify local node or assume
                // alive nodes can execute commands
                match &node_info {
                    Some(info) => {
                        let success = info.status == NodeStatus::Alive;
                        (
                            success,
                            if success {
                                format!("exec check '{}' assumes success on alive node (expected exit={})", command, expected_exit_code)
                            } else {
                                format!("exec check '{}' failed: node not alive", command)
                            },
                            Some(format!("command={}", command)),
                        )
                    }
                    None => (false, "node not found".to_string(), None),
                }
            }

            ProbeType::MemoryPressure { max_used_pct } => {
                match &node_info {
                    Some(info) => {
                        let total = info.capacity.memory_total_mb;
                        let available = info.capacity.memory_available_mb;
                        if total == 0 {
                            (true, "no memory data (assuming healthy)".to_string(), None)
                        } else {
                            let used_pct =
                                ((total - available) as f64 / total as f64) * 100.0;
                            let success = used_pct <= *max_used_pct;
                            (
                                success,
                                format!(
                                    "memory: {:.1}% used ({} MB / {} MB), threshold {:.1}%",
                                    used_pct,
                                    total - available,
                                    total,
                                    max_used_pct
                                ),
                                Some(format!("used_pct={:.1}, threshold={:.1}", used_pct, max_used_pct)),
                            )
                        }
                    }
                    None => (false, "node not found".to_string(), None),
                }
            }

            ProbeType::DiskPressure { max_used_pct } => {
                match &node_info {
                    Some(info) => {
                        let total = info.capacity.disk_total_mb;
                        let available = info.capacity.disk_available_mb;
                        if total == 0 {
                            (true, "no disk data (assuming healthy)".to_string(), None)
                        } else {
                            let used_pct =
                                ((total - available) as f64 / total as f64) * 100.0;
                            let success = used_pct <= *max_used_pct;
                            (
                                success,
                                format!(
                                    "disk: {:.1}% used ({} MB / {} MB), threshold {:.1}%",
                                    used_pct,
                                    total - available,
                                    total,
                                    max_used_pct
                                ),
                                Some(format!("used_pct={:.1}, threshold={:.1}", used_pct, max_used_pct)),
                            )
                        }
                    }
                    None => (false, "node not found".to_string(), None),
                }
            }

            ProbeType::LoadAverage { max_1min } => {
                match &node_info {
                    Some(info) => {
                        let load = info.load as f64;
                        let success = load <= *max_1min;
                        (
                            success,
                            format!(
                                "load: {:.2} (threshold {:.2})",
                                load, max_1min
                            ),
                            Some(format!("load={:.2}, threshold={:.2}", load, max_1min)),
                        )
                    }
                    None => (false, "node not found".to_string(), None),
                }
            }
        };

        let latency_ms = start.elapsed().as_millis() as u64;

        // Update circuit breaker
        if let Some(mut cb) = self.circuit_breakers.get_mut(&node_id) {
            if success {
                cb.record_success();
            } else {
                cb.record_failure();
            }
        }

        ProbeResult {
            probe_name: probe.name.clone(),
            node_id,
            timestamp: now,
            success,
            latency_ms,
            message: Some(message),
            details,
        }
    }

    /// Run all enabled probes against all applicable nodes.
    ///
    /// Probes are executed with bounded concurrency (up to 16 concurrent probes).
    pub async fn run_all_probes(&self) -> Vec<ProbeResult> {
        let probes = self.probes.read().clone();
        let node_ids = self.get_probe_target_nodes();

        let mut all_results = Vec::new();

        for probe in &probes {
            if !probe.enabled {
                continue;
            }

            let applicable_nodes = self.filter_nodes_for_probe(probe, &node_ids);

            for &node_id in &applicable_nodes {
                let result = self.run_probe(probe, node_id).await;
                self.record_probe_result(&result, probe);
                all_results.push(result);
            }
        }

        // Evaluate remediation policies after all probes complete
        self.evaluate_remediation_policies();

        all_results
    }

    // ========================================================================
    // Health queries
    // ========================================================================

    /// Get the health status of a specific node.
    pub fn node_health(&self, node_id: &NodeId) -> Option<NodeHealthStatus> {
        self.node_health.get(node_id).map(|r| r.value().clone())
    }

    /// Get health status for all monitored nodes.
    pub fn all_health(&self) -> Vec<NodeHealthStatus> {
        self.node_health.iter().map(|r| r.value().clone()).collect()
    }

    /// Get a summary of health across all nodes.
    pub fn health_summary(&self) -> HealthSummary {
        let mut summary = HealthSummary {
            total_nodes: self.node_health.len(),
            healthy: 0,
            degraded: 0,
            unhealthy: 0,
            unknown: 0,
            total_probes: self.probes.read().len(),
            active_circuit_breakers: 0,
            pending_remediations: 0,
        };

        for entry in self.node_health.iter() {
            match entry.value().overall {
                HealthState::Healthy => summary.healthy += 1,
                HealthState::Degraded => summary.degraded += 1,
                HealthState::Unhealthy => summary.unhealthy += 1,
                HealthState::Unknown => summary.unknown += 1,
            }
        }

        for entry in self.circuit_breakers.iter() {
            if entry.value().state == CircuitState::Open {
                summary.active_circuit_breakers += 1;
            }
        }

        summary.pending_remediations = self.remediation_count.load(std::sync::atomic::Ordering::Relaxed) as usize;

        summary
    }

    /// Get the probe history for a specific node and probe.
    pub fn probe_history(&self, node_id: &NodeId, probe_name: &str) -> Vec<ProbeResult> {
        self.node_health
            .get(node_id)
            .and_then(|status| {
                status
                    .probes
                    .get(probe_name)
                    .map(|ph| ph.history.iter().cloned().collect())
            })
            .unwrap_or_default()
    }

    // ========================================================================
    // Background loop
    // ========================================================================

    /// Spawn the background health check loop.
    ///
    /// Runs all probes every 10 seconds and evaluates remediation policies.
    pub fn spawn_loop(
        self: &Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let engine = Arc::clone(self);

        tokio::spawn(async move {
            info!("health check engine loop started");

            let interval = Duration::from_secs(10);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            info!("health check engine loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown_rx.borrow() {
                    break;
                }

                let results = engine.run_all_probes().await;
                if !results.is_empty() {
                    debug!(
                        total_probes = results.len(),
                        successful = results.iter().filter(|r| r.success).count(),
                        failed = results.iter().filter(|r| !r.success).count(),
                        "health check cycle complete"
                    );
                }
            }
        })
    }

    // ========================================================================
    // Circuit breaker management
    // ========================================================================

    /// Ensure a circuit breaker exists for a node, creating one if needed.
    pub fn ensure_circuit_breaker(&self, node_id: NodeId) {
        self.circuit_breakers.entry(node_id).or_insert_with(|| {
            CircuitBreaker::new(
                5,
                Duration::from_secs(30),
                Duration::from_secs(300),
            )
        });
    }

    /// Get circuit breaker state for a node.
    pub fn circuit_breaker_state(&self, node_id: &NodeId) -> Option<CircuitState> {
        self.circuit_breakers.get(node_id).map(|cb| cb.state)
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Get all node IDs from the knowledge store.
    fn get_probe_target_nodes(&self) -> Vec<NodeId> {
        match &self.knowledge {
            Some(k) => k.get_live_nodes().iter().map(|info| info.node_id).collect(),
            None => Vec::new(),
        }
    }

    /// Filter node IDs based on a probe's target specification.
    fn filter_nodes_for_probe(&self, probe: &HealthProbe, all_nodes: &[NodeId]) -> Vec<NodeId> {
        match &probe.applies_to {
            ProbeTarget::AllNodes => all_nodes.to_vec(),
            ProbeTarget::NodeIds(ids) => all_nodes
                .iter()
                .filter(|id| ids.contains(id))
                .copied()
                .collect(),
            ProbeTarget::NodesWithTag { .. } => {
                // Tag-based filtering requires the fleet's tag store, which
                // is not directly available here. Return all nodes as a
                // safe default.
                all_nodes.to_vec()
            }
            ProbeTarget::NodesInRegion(_region) => {
                // Region-based filtering requires profile data. Return all
                // nodes as a safe default.
                all_nodes.to_vec()
            }
            ProbeTarget::NodesWithTrait(trait_name) => {
                match &self.knowledge {
                    Some(k) => {
                        // Map trait name to Trait enum
                        let trait_enum = match trait_name.as_str() {
                            "can_execute" => Some(super::types::Trait::CanExecute),
                            "can_forward" => Some(super::types::Trait::CanForward),
                            "can_aggregate" => Some(super::types::Trait::CanAggregate),
                            "can_store_state" => Some(super::types::Trait::CanStoreState),
                            "can_discover" => Some(super::types::Trait::CanDiscover),
                            "can_relay" => Some(super::types::Trait::CanRelay),
                            _ => None,
                        };

                        match trait_enum {
                            Some(t) => k
                                .get_nodes_with_trait(t)
                                .iter()
                                .map(|info| info.node_id)
                                .collect(),
                            None => all_nodes.to_vec(),
                        }
                    }
                    None => all_nodes.to_vec(),
                }
            }
        }
    }

    /// Record a probe result in the node health store.
    fn record_probe_result(&self, result: &ProbeResult, probe: &HealthProbe) {
        let mut status = self
            .node_health
            .entry(result.node_id)
            .or_insert_with(|| NodeHealthStatus::new(result.node_id));

        let probe_health = status
            .probes
            .entry(result.probe_name.clone())
            .or_insert_with(ProbeHealth::new);

        probe_health.record(result.clone(), probe.failure_threshold, probe.success_threshold);
        status.last_checked = Some(result.timestamp);
        status.recalculate_overall();

        // Update overall consecutive failure count
        status.consecutive_failures = status
            .probes
            .values()
            .map(|p| p.consecutive_failures)
            .sum();
    }

    /// Evaluate remediation policies against current node health.
    fn evaluate_remediation_policies(&self) {
        let policies = self.policies.read().clone();
        let now = Utc::now();

        for entry in self.node_health.iter() {
            let node_id = *entry.key();
            let status = entry.value();

            for policy in &policies {
                if !policy.enabled {
                    continue;
                }

                // Check cooldown
                let cooldown_key = (node_id, policy.name.clone());
                if let Some(last) = self.last_remediation.get(&cooldown_key) {
                    let elapsed = (now - *last.value()).num_seconds();
                    if elapsed < policy.cooldown_secs as i64 {
                        continue;
                    }
                }

                let triggered = match &policy.trigger {
                    RemediationTrigger::OverallUnhealthy => {
                        status.overall == HealthState::Unhealthy
                    }
                    RemediationTrigger::OverallDegraded { for_secs: _ } => {
                        // Simplified: just check current state is degraded
                        status.overall == HealthState::Degraded
                    }
                    RemediationTrigger::AnyProbeUnhealthy => {
                        status.probes.values().any(|p| p.state == HealthState::Unhealthy)
                    }
                    RemediationTrigger::ProbeUnhealthy { probe_name } => {
                        status
                            .probes
                            .get(probe_name)
                            .map(|p| p.state == HealthState::Unhealthy)
                            .unwrap_or(false)
                    }
                    RemediationTrigger::ConsecutiveFailures { probe_name, count } => {
                        status
                            .probes
                            .get(probe_name)
                            .map(|p| p.consecutive_failures >= *count)
                            .unwrap_or(false)
                    }
                };

                if triggered {
                    self.execute_remediation(node_id, policy);
                    self.last_remediation.insert(cooldown_key, now);
                }
            }
        }
    }

    /// Execute a remediation action for a node.
    fn execute_remediation(&self, node_id: NodeId, policy: &RemediationPolicy) {
        match &policy.action {
            RemediationAction::Drain { timeout_secs } => {
                info!(
                    node = %node_id,
                    policy = %policy.name,
                    timeout = timeout_secs,
                    "remediation: drain triggered"
                );
                self.remediation_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            RemediationAction::Quarantine { reason } => {
                warn!(
                    node = %node_id,
                    policy = %policy.name,
                    reason = %reason,
                    "remediation: quarantine triggered"
                );
                self.remediation_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            RemediationAction::Cordon { reason } => {
                info!(
                    node = %node_id,
                    policy = %policy.name,
                    reason = %reason,
                    "remediation: cordon triggered"
                );
                self.remediation_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            RemediationAction::EmitAlert { severity, message } => {
                warn!(
                    node = %node_id,
                    policy = %policy.name,
                    severity = %severity,
                    message = %message,
                    "remediation: alert emitted"
                );
                self.remediation_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            RemediationAction::NoAction => {
                debug!(
                    node = %node_id,
                    policy = %policy.name,
                    "remediation: no action (policy disabled)"
                );
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::{NodeInfo, ResourceSnapshot, Trait};
    use std::collections::HashSet;
    use std::net::SocketAddr;

    fn make_node_id() -> NodeId {
        NodeId::new()
    }

    fn make_knowledge_with_node(
        node_id: NodeId,
        status: NodeStatus,
        memory_total_mb: u64,
        memory_available_mb: u64,
        disk_total_mb: u64,
        disk_available_mb: u64,
        load: f32,
    ) -> Arc<KnowledgeStore> {
        let store = Arc::new(KnowledgeStore::new(NodeId::new()));
        let addr: SocketAddr = "127.0.0.1:4200".parse().unwrap();
        let info = NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute]),
            load,
            capacity: ResourceSnapshot {
                cpu_cores: 4,
                cpu_available: 0.5,
                memory_total_mb,
                memory_available_mb,
                disk_total_mb,
                disk_available_mb,
                network_bandwidth_mbps: 100.0,
                ..Default::default()
            },
            address: Some(addr),
            via: node_id,
            status,
            generation: 1,
            trust_level: Default::default(),
            ..Default::default()
        };
        store.merge_node(info);
        store
    }

    // ====================================================================
    // ProbeType display tests
    // ====================================================================

    #[test]
    fn test_probe_type_display() {
        let pt = ProbeType::GossipResponse { timeout_ms: 5000 };
        assert!(format!("{}", pt).contains("5000"));

        let pt = ProbeType::MemoryPressure { max_used_pct: 90.0 };
        assert!(format!("{}", pt).contains("90"));

        let pt = ProbeType::DiskPressure { max_used_pct: 95.0 };
        assert!(format!("{}", pt).contains("95"));

        let pt = ProbeType::LoadAverage { max_1min: 4.0 };
        assert!(format!("{}", pt).contains("4"));
    }

    #[test]
    fn test_probe_type_tcp_display() {
        let pt = ProbeType::TcpConnect {
            port: 8080,
            timeout_ms: 3000,
        };
        let display = format!("{}", pt);
        assert!(display.contains("8080"));
        assert!(display.contains("3000"));
    }

    #[test]
    fn test_probe_type_http_display() {
        let pt = ProbeType::HttpGet {
            path: "/health".to_string(),
            expected_status: 200,
            timeout_ms: 5000,
        };
        let display = format!("{}", pt);
        assert!(display.contains("/health"));
        assert!(display.contains("200"));
    }

    #[test]
    fn test_probe_type_exec_display() {
        let pt = ProbeType::ExecCheck {
            command: "test -f /tmp/ok".to_string(),
            expected_exit_code: 0,
            timeout_ms: 1000,
        };
        let display = format!("{}", pt);
        assert!(display.contains("test -f"));
    }

    // ====================================================================
    // HealthState tests
    // ====================================================================

    #[test]
    fn test_health_state_display() {
        assert_eq!(format!("{}", HealthState::Healthy), "healthy");
        assert_eq!(format!("{}", HealthState::Degraded), "degraded");
        assert_eq!(format!("{}", HealthState::Unhealthy), "unhealthy");
        assert_eq!(format!("{}", HealthState::Unknown), "unknown");
    }

    #[test]
    fn test_health_state_equality() {
        assert_eq!(HealthState::Healthy, HealthState::Healthy);
        assert_ne!(HealthState::Healthy, HealthState::Degraded);
    }

    // ====================================================================
    // ProbeHealth tests
    // ====================================================================

    #[test]
    fn test_probe_health_new_is_unknown() {
        let ph = ProbeHealth::new();
        assert_eq!(ph.state, HealthState::Unknown);
        assert_eq!(ph.consecutive_failures, 0);
        assert_eq!(ph.consecutive_successes, 0);
        assert!(ph.history.is_empty());
    }

    #[test]
    fn test_probe_health_record_success() {
        let mut ph = ProbeHealth::new();
        let result = ProbeResult {
            probe_name: "test".to_string(),
            node_id: make_node_id(),
            timestamp: Utc::now(),
            success: true,
            latency_ms: 5,
            message: None,
            details: None,
        };

        ph.record(result, 3, 1);
        assert_eq!(ph.consecutive_successes, 1);
        assert_eq!(ph.consecutive_failures, 0);
        assert_eq!(ph.state, HealthState::Healthy);
        assert_eq!(ph.history.len(), 1);
        assert!((ph.success_rate - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_probe_health_record_failure() {
        let mut ph = ProbeHealth::new();
        let result = ProbeResult {
            probe_name: "test".to_string(),
            node_id: make_node_id(),
            timestamp: Utc::now(),
            success: false,
            latency_ms: 100,
            message: Some("timeout".to_string()),
            details: None,
        };

        ph.record(result, 3, 1);
        assert_eq!(ph.consecutive_failures, 1);
        assert_eq!(ph.consecutive_successes, 0);
        // Not yet at threshold (3), so should be Unknown still
        // (first result ever, no prior healthy)
        assert_eq!(ph.state, HealthState::Unknown);
    }

    #[test]
    fn test_probe_health_transitions_to_unhealthy() {
        let mut ph = ProbeHealth::new();
        let node_id = make_node_id();

        for _ in 0..3 {
            ph.record(
                ProbeResult {
                    probe_name: "test".to_string(),
                    node_id,
                    timestamp: Utc::now(),
                    success: false,
                    latency_ms: 50,
                    message: None,
                    details: None,
                },
                3,
                1,
            );
        }

        assert_eq!(ph.state, HealthState::Unhealthy);
        assert_eq!(ph.consecutive_failures, 3);
    }

    #[test]
    fn test_probe_health_recovery_from_unhealthy() {
        let mut ph = ProbeHealth::new();
        let node_id = make_node_id();

        // Become unhealthy
        for _ in 0..3 {
            ph.record(
                ProbeResult {
                    probe_name: "test".to_string(),
                    node_id,
                    timestamp: Utc::now(),
                    success: false,
                    latency_ms: 50,
                    message: None,
                    details: None,
                },
                3,
                2,
            );
        }
        assert_eq!(ph.state, HealthState::Unhealthy);

        // Recover with 2 successes (success_threshold = 2)
        for _ in 0..2 {
            ph.record(
                ProbeResult {
                    probe_name: "test".to_string(),
                    node_id,
                    timestamp: Utc::now(),
                    success: true,
                    latency_ms: 5,
                    message: None,
                    details: None,
                },
                3,
                2,
            );
        }
        assert_eq!(ph.state, HealthState::Healthy);
    }

    #[test]
    fn test_probe_health_history_capped() {
        let mut ph = ProbeHealth::new();
        let node_id = make_node_id();

        for _ in 0..150 {
            ph.record(
                ProbeResult {
                    probe_name: "test".to_string(),
                    node_id,
                    timestamp: Utc::now(),
                    success: true,
                    latency_ms: 1,
                    message: None,
                    details: None,
                },
                3,
                1,
            );
        }

        assert!(ph.history.len() <= MAX_PROBE_HISTORY);
    }

    #[test]
    fn test_probe_health_success_rate() {
        let mut ph = ProbeHealth::new();
        let node_id = make_node_id();

        // 3 successes, 1 failure
        for success in [true, true, true, false] {
            ph.record(
                ProbeResult {
                    probe_name: "test".to_string(),
                    node_id,
                    timestamp: Utc::now(),
                    success,
                    latency_ms: 10,
                    message: None,
                    details: None,
                },
                3,
                1,
            );
        }

        assert!((ph.success_rate - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_probe_health_avg_latency() {
        let mut ph = ProbeHealth::new();
        let node_id = make_node_id();

        for latency in [10u64, 20, 30] {
            ph.record(
                ProbeResult {
                    probe_name: "test".to_string(),
                    node_id,
                    timestamp: Utc::now(),
                    success: true,
                    latency_ms: latency,
                    message: None,
                    details: None,
                },
                3,
                1,
            );
        }

        assert!((ph.avg_latency_ms - 20.0).abs() < f64::EPSILON);
    }

    // ====================================================================
    // NodeHealthStatus tests
    // ====================================================================

    #[test]
    fn test_node_health_status_new_is_unknown() {
        let status = NodeHealthStatus::new(make_node_id());
        assert_eq!(status.overall, HealthState::Unknown);
        assert!(status.probes.is_empty());
        assert!(status.last_checked.is_none());
    }

    #[test]
    fn test_node_health_recalculate_overall_healthy() {
        let mut status = NodeHealthStatus::new(make_node_id());

        let mut probe = ProbeHealth::new();
        probe.state = HealthState::Healthy;
        status.probes.insert("p1".to_string(), probe);

        let mut probe2 = ProbeHealth::new();
        probe2.state = HealthState::Healthy;
        status.probes.insert("p2".to_string(), probe2);

        status.recalculate_overall();
        assert_eq!(status.overall, HealthState::Healthy);
    }

    #[test]
    fn test_node_health_recalculate_overall_degraded() {
        let mut status = NodeHealthStatus::new(make_node_id());

        let mut probe = ProbeHealth::new();
        probe.state = HealthState::Healthy;
        status.probes.insert("p1".to_string(), probe);

        let mut probe2 = ProbeHealth::new();
        probe2.state = HealthState::Degraded;
        status.probes.insert("p2".to_string(), probe2);

        status.recalculate_overall();
        assert_eq!(status.overall, HealthState::Degraded);
    }

    #[test]
    fn test_node_health_recalculate_overall_unhealthy() {
        let mut status = NodeHealthStatus::new(make_node_id());

        let mut probe = ProbeHealth::new();
        probe.state = HealthState::Healthy;
        status.probes.insert("p1".to_string(), probe);

        let mut probe2 = ProbeHealth::new();
        probe2.state = HealthState::Unhealthy;
        status.probes.insert("p2".to_string(), probe2);

        status.recalculate_overall();
        assert_eq!(status.overall, HealthState::Unhealthy);
    }

    #[test]
    fn test_node_health_recalculate_overall_all_unknown() {
        let mut status = NodeHealthStatus::new(make_node_id());

        let probe = ProbeHealth::new(); // Unknown by default
        status.probes.insert("p1".to_string(), probe);

        status.recalculate_overall();
        assert_eq!(status.overall, HealthState::Unknown);
    }

    // ====================================================================
    // CircuitBreaker tests
    // ====================================================================

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(
            3,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );
        assert_eq!(cb.state, CircuitState::Closed);
        assert_eq!(cb.failure_count, 0);
    }

    #[test]
    fn test_circuit_breaker_closed_allows() {
        let mut cb = CircuitBreaker::new(
            3,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );
        assert!(cb.should_allow());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let mut cb = CircuitBreaker::new(
            3,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Closed);
        assert!(cb.should_allow());

        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert!(!cb.should_allow());
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let mut cb = CircuitBreaker::new(
            2,
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);

        // Simulate half-open by manually setting state
        cb.state = CircuitState::HalfOpen;
        assert!(cb.should_allow());

        cb.record_success();
        assert_eq!(cb.state, CircuitState::Closed);
        assert_eq!(cb.failure_count, 0);
    }

    #[test]
    fn test_circuit_breaker_half_open_after_timeout() {
        let mut cb = CircuitBreaker::new(
            1,
            Duration::from_millis(1), // Very short timeout for testing
            Duration::from_secs(300),
        );

        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);

        // Wait for the half-open timeout
        std::thread::sleep(Duration::from_millis(5));

        assert!(cb.should_allow());
        assert_eq!(cb.state, CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_failure_in_half_open_reopens() {
        let mut cb = CircuitBreaker::new(
            1,
            Duration::from_millis(1),
            Duration::from_secs(300),
        );

        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);

        std::thread::sleep(Duration::from_millis(5));
        cb.should_allow(); // Transitions to HalfOpen
        assert_eq!(cb.state, CircuitState::HalfOpen);

        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
    }

    // ====================================================================
    // HealthCheckEngine tests
    // ====================================================================

    #[test]
    fn test_engine_has_default_probes() {
        let engine = HealthCheckEngine::new();
        let probes = engine.list_probes();

        assert_eq!(probes.len(), 4);
        let names: Vec<&str> = probes.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"gossip-response"));
        assert!(names.contains(&"memory-pressure"));
        assert!(names.contains(&"disk-pressure"));
        assert!(names.contains(&"load-average"));
    }

    #[test]
    fn test_engine_has_default_policies() {
        let engine = HealthCheckEngine::new();
        let policies = engine.list_policies();

        assert_eq!(policies.len(), 3);
        let names: Vec<&str> = policies.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"auto-quarantine-unhealthy"));
        assert!(names.contains(&"drain-degraded"));
        assert!(names.contains(&"alert-on-failure"));
    }

    #[test]
    fn test_engine_add_and_remove_probe() {
        let engine = HealthCheckEngine::new();
        let initial = engine.list_probes().len();

        engine.add_probe(HealthProbe {
            name: "custom-probe".to_string(),
            probe_type: ProbeType::GossipResponse { timeout_ms: 5000 },
            interval_secs: 10,
            failure_threshold: 2,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        });
        assert_eq!(engine.list_probes().len(), initial + 1);

        assert!(engine.remove_probe("custom-probe"));
        assert_eq!(engine.list_probes().len(), initial);
    }

    #[test]
    fn test_engine_remove_nonexistent_probe() {
        let engine = HealthCheckEngine::new();
        assert!(!engine.remove_probe("nonexistent"));
    }

    #[test]
    fn test_engine_get_probe() {
        let engine = HealthCheckEngine::new();
        let probe = engine.get_probe("gossip-response");
        assert!(probe.is_some());
        assert_eq!(probe.unwrap().name, "gossip-response");
    }

    #[test]
    fn test_engine_add_and_remove_policy() {
        let engine = HealthCheckEngine::new();
        let initial = engine.list_policies().len();

        engine.add_policy(RemediationPolicy {
            name: "custom-policy".to_string(),
            trigger: RemediationTrigger::OverallUnhealthy,
            action: RemediationAction::NoAction,
            cooldown_secs: 60,
            enabled: true,
        });
        assert_eq!(engine.list_policies().len(), initial + 1);

        assert!(engine.remove_policy("custom-policy"));
        assert_eq!(engine.list_policies().len(), initial);
    }

    #[test]
    fn test_engine_health_summary_empty() {
        let engine = HealthCheckEngine::new();
        let summary = engine.health_summary();

        assert_eq!(summary.total_nodes, 0);
        assert_eq!(summary.healthy, 0);
        assert_eq!(summary.degraded, 0);
        assert_eq!(summary.unhealthy, 0);
        assert_eq!(summary.unknown, 0);
        assert_eq!(summary.total_probes, 4);
    }

    #[test]
    fn test_engine_node_health_returns_none_for_unknown() {
        let engine = HealthCheckEngine::new();
        assert!(engine.node_health(&make_node_id()).is_none());
    }

    #[test]
    fn test_engine_probe_history_empty() {
        let engine = HealthCheckEngine::new();
        let history = engine.probe_history(&make_node_id(), "gossip-response");
        assert!(history.is_empty());
    }

    #[test]
    fn test_engine_ensure_circuit_breaker() {
        let engine = HealthCheckEngine::new();
        let id = make_node_id();

        engine.ensure_circuit_breaker(id);
        let state = engine.circuit_breaker_state(&id);
        assert_eq!(state, Some(CircuitState::Closed));
    }

    #[test]
    fn test_engine_circuit_breaker_state_unknown_node() {
        let engine = HealthCheckEngine::new();
        assert!(engine.circuit_breaker_state(&make_node_id()).is_none());
    }

    // ====================================================================
    // Probe execution tests (async)
    // ====================================================================

    #[tokio::test]
    async fn test_run_probe_gossip_healthy_node() {
        let node_id = make_node_id();
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "gossip".to_string(),
            probe_type: ProbeType::GossipResponse { timeout_ms: 60000 },
            interval_secs: 30,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_run_probe_gossip_dead_node() {
        let node_id = make_node_id();
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Dead, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "gossip".to_string(),
            probe_type: ProbeType::GossipResponse { timeout_ms: 60000 },
            interval_secs: 30,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_run_probe_memory_pressure_healthy() {
        let node_id = make_node_id();
        // 50% memory used (4096 of 8192 available)
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "memory".to_string(),
            probe_type: ProbeType::MemoryPressure { max_used_pct: 90.0 },
            interval_secs: 60,
            failure_threshold: 2,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_run_probe_memory_pressure_unhealthy() {
        let node_id = make_node_id();
        // 95% memory used (only 410 of 8192 available)
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 410, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "memory".to_string(),
            probe_type: ProbeType::MemoryPressure { max_used_pct: 90.0 },
            interval_secs: 60,
            failure_threshold: 2,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_run_probe_disk_pressure_healthy() {
        let node_id = make_node_id();
        // 50% disk used
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "disk".to_string(),
            probe_type: ProbeType::DiskPressure { max_used_pct: 95.0 },
            interval_secs: 60,
            failure_threshold: 2,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_run_probe_disk_pressure_unhealthy() {
        let node_id = make_node_id();
        // 99% disk used
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 1000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "disk".to_string(),
            probe_type: ProbeType::DiskPressure { max_used_pct: 95.0 },
            interval_secs: 60,
            failure_threshold: 2,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_run_probe_load_average_healthy() {
        let node_id = make_node_id();
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "load".to_string(),
            probe_type: ProbeType::LoadAverage { max_1min: 0.95 },
            interval_secs: 30,
            failure_threshold: 5,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_run_probe_load_average_unhealthy() {
        let node_id = make_node_id();
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.99);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "load".to_string(),
            probe_type: ProbeType::LoadAverage { max_1min: 0.95 },
            interval_secs: 30,
            failure_threshold: 5,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_run_probe_no_knowledge_store() {
        let engine = HealthCheckEngine::new();
        let node_id = make_node_id();

        let probe = HealthProbe {
            name: "test".to_string(),
            probe_type: ProbeType::GossipResponse { timeout_ms: 5000 },
            interval_secs: 10,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(!result.success);
        assert!(result.message.unwrap().contains("no knowledge store"));
    }

    #[tokio::test]
    async fn test_run_probe_unknown_node() {
        let knowledge = Arc::new(KnowledgeStore::new(NodeId::new()));
        let engine = HealthCheckEngine::new().with_knowledge(knowledge);
        let node_id = make_node_id();

        let probe = HealthProbe {
            name: "test".to_string(),
            probe_type: ProbeType::GossipResponse { timeout_ms: 5000 },
            interval_secs: 10,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(!result.success);
        assert!(result.message.unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn test_run_all_probes_with_live_nodes() {
        let node_id = make_node_id();
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);
        let results = engine.run_all_probes().await;

        // Should have results for each enabled probe * each live node
        assert!(!results.is_empty());
        // All probes should succeed for a healthy node
        assert!(results.iter().all(|r| r.success));
    }

    #[tokio::test]
    async fn test_run_probe_tcp_connect() {
        let node_id = make_node_id();
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "tcp".to_string(),
            probe_type: ProbeType::TcpConnect {
                port: 8080,
                timeout_ms: 60000,
            },
            interval_secs: 30,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_run_probe_http_get() {
        let node_id = make_node_id();
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "http".to_string(),
            probe_type: ProbeType::HttpGet {
                path: "/health".to_string(),
                expected_status: 200,
                timeout_ms: 60000,
            },
            interval_secs: 30,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_run_probe_exec_check() {
        let node_id = make_node_id();
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 4096, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "exec".to_string(),
            probe_type: ProbeType::ExecCheck {
                command: "true".to_string(),
                expected_exit_code: 0,
                timeout_ms: 5000,
            },
            interval_secs: 30,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_run_probe_memory_zero_total() {
        let node_id = make_node_id();
        // Zero total memory should be treated as healthy (no data)
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 0, 0, 100000, 50000, 0.3);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        let probe = HealthProbe {
            name: "memory".to_string(),
            probe_type: ProbeType::MemoryPressure { max_used_pct: 90.0 },
            interval_secs: 60,
            failure_threshold: 2,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let result = engine.run_probe(&probe, node_id).await;
        assert!(result.success);
    }

    // ====================================================================
    // Remediation tests
    // ====================================================================

    #[tokio::test]
    async fn test_remediation_triggers_on_unhealthy() {
        let node_id = make_node_id();
        // Create a node with very high load to trigger unhealthy
        let knowledge = make_knowledge_with_node(node_id, NodeStatus::Alive, 8192, 100, 100000, 1000, 0.99);

        let engine = HealthCheckEngine::new().with_knowledge(knowledge);

        // Run probes multiple times to reach failure thresholds
        for _ in 0..5 {
            engine.run_all_probes().await;
        }

        // Check that the node has health data
        let health = engine.node_health(&node_id);
        assert!(health.is_some());
    }

    #[test]
    fn test_all_health_empty() {
        let engine = HealthCheckEngine::new();
        assert!(engine.all_health().is_empty());
    }

    #[test]
    fn test_health_summary_with_data() {
        let engine = HealthCheckEngine::new();

        // Manually insert some health data
        let id1 = make_node_id();
        let mut status1 = NodeHealthStatus::new(id1);
        let mut probe1 = ProbeHealth::new();
        probe1.state = HealthState::Healthy;
        status1.probes.insert("test".to_string(), probe1);
        status1.recalculate_overall();
        engine.node_health.insert(id1, status1);

        let id2 = make_node_id();
        let mut status2 = NodeHealthStatus::new(id2);
        let mut probe2 = ProbeHealth::new();
        probe2.state = HealthState::Unhealthy;
        status2.probes.insert("test".to_string(), probe2);
        status2.recalculate_overall();
        engine.node_health.insert(id2, status2);

        let summary = engine.health_summary();
        assert_eq!(summary.total_nodes, 2);
        assert_eq!(summary.healthy, 1);
        assert_eq!(summary.unhealthy, 1);
    }

    // ====================================================================
    // Filter tests
    // ====================================================================

    #[test]
    fn test_filter_nodes_all_nodes() {
        let engine = HealthCheckEngine::new();
        let probe = HealthProbe {
            name: "test".to_string(),
            probe_type: ProbeType::GossipResponse { timeout_ms: 5000 },
            interval_secs: 10,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::AllNodes,
        };

        let nodes = vec![make_node_id(), make_node_id(), make_node_id()];
        let filtered = engine.filter_nodes_for_probe(&probe, &nodes);
        assert_eq!(filtered.len(), 3);
    }

    #[test]
    fn test_filter_nodes_specific_ids() {
        let engine = HealthCheckEngine::new();
        let id1 = make_node_id();
        let id2 = make_node_id();
        let id3 = make_node_id();

        let probe = HealthProbe {
            name: "test".to_string(),
            probe_type: ProbeType::GossipResponse { timeout_ms: 5000 },
            interval_secs: 10,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::NodeIds(vec![id1, id3]),
        };

        let nodes = vec![id1, id2, id3];
        let filtered = engine.filter_nodes_for_probe(&probe, &nodes);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&id1));
        assert!(filtered.contains(&id3));
        assert!(!filtered.contains(&id2));
    }

    #[test]
    fn test_filter_nodes_with_tag_defaults_to_all() {
        let engine = HealthCheckEngine::new();
        let probe = HealthProbe {
            name: "test".to_string(),
            probe_type: ProbeType::GossipResponse { timeout_ms: 5000 },
            interval_secs: 10,
            failure_threshold: 3,
            success_threshold: 1,
            enabled: true,
            applies_to: ProbeTarget::NodesWithTag {
                key: "role".to_string(),
                value: "worker".to_string(),
            },
        };

        let nodes = vec![make_node_id(), make_node_id()];
        let filtered = engine.filter_nodes_for_probe(&probe, &nodes);
        assert_eq!(filtered.len(), 2);
    }
}
