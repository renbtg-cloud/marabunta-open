// Marabunta - Licensed under the MIT License.
//! Distributed scatter/gather engine for the plugin system.
//!
//! The [`ScatterEngine`] lets plugins distribute work units across swarm nodes
//! and collect results. Each scatter operation fans out [`HandleRequest`]s to
//! eligible nodes (selected by trait matching and locality hints), waits for
//! [`HandleResponse`]s with a configurable timeout, and assembles a
//! [`ScatterResponse`] with per-unit latency measurements.
//!
//! The engine does not send messages over the network directly. Instead it uses
//! a pending-requests pattern: for each outbound unit it stores a
//! [`tokio::sync::oneshot::Sender`] in a [`DashMap`] keyed by a unique request
//! ID. The plugin host (or transport layer) is responsible for reading these
//! entries, forwarding the [`HandleRequest`] over the wire, and calling
//! [`ScatterEngine::deliver_response`] when the answer arrives.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::plugin::config::PLUGIN_DEFAULT_SCATTER_TIMEOUT_MS;
use crate::plugin::types::{
    HandleRequest, HandleResponse, Locality, PluginError, PluginResult, ScatterHints,
    ScatterRequest, ScatterResponse, ScatterResult, ScatterUnit,
};
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::{NodeId, NodeInfo, NodeStatus, Trait};

// ============================================================================
// ScatterEngine
// ============================================================================

/// Distributes work units across swarm nodes and gathers results.
///
/// Each scatter call fans work out to eligible nodes. Responses are collected
/// through the [`deliver_response`](ScatterEngine::deliver_response) callback,
/// typically driven by the transport layer when a reply message arrives.
pub struct ScatterEngine {
    /// Identity of the local node running this engine.
    node_id: NodeId,

    /// Shared knowledge store for looking up live nodes and their traits.
    knowledge: Arc<KnowledgeStore>,

    /// In-flight requests awaiting a response.
    ///
    /// Keyed by a unique request ID (UUID string). The value is the oneshot
    /// sender that will deliver the [`HandleResponse`] to the waiting scatter
    /// future.
    pending_requests: Arc<DashMap<String, tokio::sync::oneshot::Sender<HandleResponse>>>,

    /// Maximum number of scatter units that may be in-flight simultaneously.
    max_concurrent: usize,
}

impl ScatterEngine {
    /// Create a new scatter engine.
    ///
    /// * `node_id`        - identity of the local node.
    /// * `knowledge`      - shared knowledge store used for node discovery.
    /// * `max_concurrent` - ceiling on simultaneous in-flight scatter units.
    pub fn new(
        node_id: NodeId,
        knowledge: Arc<KnowledgeStore>,
        max_concurrent: usize,
    ) -> Self {
        Self {
            node_id,
            knowledge,
            pending_requests: Arc::new(DashMap::new()),
            max_concurrent,
        }
    }

    /// Return a reference to the pending-requests map.
    ///
    /// The plugin host reads this map to discover newly inserted entries and
    /// forward them over the wire.
    pub fn pending_requests(
        &self,
    ) -> &Arc<DashMap<String, tokio::sync::oneshot::Sender<HandleResponse>>> {
        &self.pending_requests
    }

    // ========================================================================
    // Scatter (fan-out + gather)
    // ========================================================================

    /// Scatter work units across the swarm and gather results.
    ///
    /// For every [`ScatterUnit`] in the request:
    ///
    /// 1. If `target_node` is non-empty, that specific node is used.
    /// 2. Otherwise, eligible nodes are discovered from the knowledge store
    ///    by filtering on liveness, required traits, and locality hints.
    /// 3. A [`HandleRequest`] is created with a unique `request_id`, and its
    ///    oneshot sender is stored in `pending_requests`.
    /// 4. The engine waits for each response (or timeout).
    /// 5. Results are collected into a [`ScatterResponse`].
    pub async fn scatter(&self, request: ScatterRequest) -> PluginResult<ScatterResponse> {
        if request.units.is_empty() {
            return Ok(ScatterResponse {
                results: Vec::new(),
            });
        }

        if request.units.len() > self.max_concurrent {
            return Err(PluginError::CapacityExceeded(format!(
                "scatter request contains {} units but max_concurrent is {}",
                request.units.len(),
                self.max_concurrent,
            )));
        }

        let timeout_ms = if request.hints.timeout_ms == 0 {
            PLUGIN_DEFAULT_SCATTER_TIMEOUT_MS
        } else {
            request.hints.timeout_ms
        };
        let timeout_dur = Duration::from_millis(timeout_ms as u64);

        let mut join_handles = Vec::with_capacity(request.units.len());

        for unit in &request.units {
            // Determine target nodes.
            let targets = if !unit.target_node.is_empty() {
                self.resolve_explicit_target(&unit.target_node)?
            } else {
                let candidates = self.select_target_nodes(unit, &request.hints);
                if candidates.is_empty() {
                    // No eligible node found -- record an immediate error.
                    join_handles.push(ImmediateResult::Error {
                        error: "no eligible node found for scatter unit".to_string(),
                    });
                    continue;
                }
                candidates
            };

            // Use the first eligible target (fan-out to multiple replicas is
            // intentionally left to higher-level retry/replication logic).
            let target = targets[0];

            let request_id = Uuid::new_v4().to_string();
            let (tx, rx) = tokio::sync::oneshot::channel::<HandleResponse>();

            self.pending_requests.insert(request_id.clone(), tx);

            let handle_req = HandleRequest {
                request_id: request_id.clone(),
                payload: unit.payload.clone(),
                from_node: self.node_id.0.to_string(),
                metadata: HashMap::new(),
            };

            let pending = Arc::clone(&self.pending_requests);
            let req_id_for_cleanup = request_id.clone();

            join_handles.push(ImmediateResult::Pending {
                target,
                request_id,
                handle_req,
                rx,
                pending,
                req_id_for_cleanup,
                timeout_dur,
            });
        }

        // Wait for all results concurrently, preserving positional ordering
        // so that results[i] corresponds to request.units[i].
        let total = join_handles.len();
        let mut results: Vec<Option<ScatterResult>> = (0..total).map(|_| None).collect();
        let mut futures: Vec<(usize, tokio::task::JoinHandle<ScatterResult>)> = Vec::with_capacity(total);

        for (idx, item) in join_handles.into_iter().enumerate() {
            match item {
                ImmediateResult::Error { error } => {
                    results[idx] = Some(ScatterResult {
                        node_id: String::new(),
                        success: false,
                        response: Vec::new(),
                        error,
                        latency_ms: 0,
                    });
                }
                ImmediateResult::Pending {
                    target,
                    request_id: _,
                    handle_req: _,
                    rx,
                    pending,
                    req_id_for_cleanup,
                    timeout_dur: td,
                } => {
                    futures.push((idx, tokio::spawn(async move {
                        let start = Instant::now();
                        let outcome = tokio::time::timeout(td, rx).await;
                        let elapsed = start.elapsed();
                        let latency_ms = elapsed.as_millis().min(u32::MAX as u128) as u32;

                        match outcome {
                            Ok(Ok(resp)) => {
                                let success = resp.error.is_empty();
                                ScatterResult {
                                    node_id: target.0.to_string(),
                                    success,
                                    response: resp.payload,
                                    error: resp.error,
                                    latency_ms,
                                }
                            }
                            Ok(Err(_recv_err)) => {
                                // The sender was dropped without sending a
                                // response. Clean up and report failure.
                                pending.remove(&req_id_for_cleanup);
                                ScatterResult {
                                    node_id: target.0.to_string(),
                                    success: false,
                                    response: Vec::new(),
                                    error: "response channel closed unexpectedly".to_string(),
                                    latency_ms,
                                }
                            }
                            Err(_timeout) => {
                                // Deadline exceeded. Remove the pending entry so
                                // a late response does not leak memory.
                                pending.remove(&req_id_for_cleanup);
                                ScatterResult {
                                    node_id: target.0.to_string(),
                                    success: false,
                                    response: Vec::new(),
                                    error: format!(
                                        "scatter unit timed out after {}ms",
                                        td.as_millis()
                                    ),
                                    latency_ms,
                                }
                            }
                        }
                    })));
                }
            }
        }

        for (idx, handle) in futures {
            match handle.await {
                Ok(result) => results[idx] = Some(result),
                Err(join_err) => {
                    results[idx] = Some(ScatterResult {
                        node_id: String::new(),
                        success: false,
                        response: Vec::new(),
                        error: format!("scatter task panicked: {}", join_err),
                        latency_ms: 0,
                    });
                }
            }
        }

        let results: Vec<ScatterResult> = results.into_iter().map(|r| r.unwrap()).collect();

        debug!(
            total = results.len(),
            successes = results.iter().filter(|r| r.success).count(),
            "scatter: gathered results"
        );

        Ok(ScatterResponse { results })
    }

    // ========================================================================
    // Node selection
    // ========================================================================

    /// Find the best target nodes for a scatter unit.
    ///
    /// Nodes must be alive and possess every trait listed in
    /// `unit.required_traits`. The returned list is ordered by a composite
    /// score that combines current load with hardware capacity, so that a
    /// lightly-loaded 96-core server ranks above a lightly-loaded Raspberry
    /// Pi. Locality hints are applied on top.
    pub fn select_target_nodes(
        &self,
        unit: &ScatterUnit,
        hints: &ScatterHints,
    ) -> Vec<NodeId> {
        // Convert string trait names to Trait enums.
        let required: Vec<Trait> = unit
            .required_traits
            .iter()
            .filter_map(|name| Self::trait_name_to_enum(name))
            .collect();

        // Fetch all live nodes from the knowledge store.
        let live_nodes = self.knowledge.get_live_nodes();

        // Filter to nodes that possess every required trait.
        let mut eligible: Vec<(NodeId, f64)> = live_nodes
            .into_iter()
            .filter(|node| {
                node.status == NodeStatus::Alive
                    && required.iter().all(|t| node.traits.contains(t))
            })
            .map(|node| {
                let score = (1.0 - node.load as f64) * capacity_factor(&node);
                (node.node_id, score)
            })
            .collect();

        // Apply locality hints.
        match hints.locality {
            Locality::PreferLocal => {
                // Prefer the local node if it is in the eligible set.
                // Move it to the front.
                if let Some(pos) = eligible.iter().position(|(id, _)| *id == self.node_id) {
                    let local = eligible.remove(pos);
                    // Sort the rest by score (descending — higher is better).
                    eligible.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                    eligible.insert(0, local);
                } else {
                    eligible.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                }
            }
            Locality::RequireLocal => {
                // Only the local node is acceptable.
                eligible.retain(|(id, _)| *id == self.node_id);
            }
            Locality::Any => {
                // Sort by score (descending — higher is better).
                eligible.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            }
        }

        eligible.into_iter().map(|(id, _)| id).collect()
    }

    // ========================================================================
    // Trait mapping
    // ========================================================================

    /// Map a human-readable trait name string back to its [`Trait`] enum value.
    ///
    /// Accepts both the snake_case display form (e.g. `"can_execute"`) and the
    /// PascalCase variant name (e.g. `"CanExecute"`).
    pub fn trait_name_to_enum(name: &str) -> Option<Trait> {
        match name {
            "can_execute" | "CanExecute" => Some(Trait::CanExecute),
            "can_forward" | "CanForward" => Some(Trait::CanForward),
            "can_aggregate" | "CanAggregate" => Some(Trait::CanAggregate),
            "can_store_state" | "CanStoreState" => Some(Trait::CanStoreState),
            "can_discover" | "CanDiscover" => Some(Trait::CanDiscover),
            "can_relay" | "CanRelay" => Some(Trait::CanRelay),
            _ => {
                warn!(trait_name = %name, "scatter: unknown trait name");
                None
            }
        }
    }

    // ========================================================================
    // Response delivery
    // ========================================================================

    /// Deliver a response to a pending scatter request.
    ///
    /// Called by the plugin host (or transport layer) when a [`HandleResponse`]
    /// arrives from a remote node. The `request_id` must match an entry in
    /// `pending_requests`; if it does not, the response is silently dropped
    /// (the scatter call has already timed out or been cancelled).
    pub fn deliver_response(&self, request_id: &str, response: HandleResponse) {
        if let Some((_, sender)) = self.pending_requests.remove(request_id) {
            if sender.send(response).is_err() {
                debug!(
                    request_id = %request_id,
                    "scatter: response delivered but receiver already dropped"
                );
            }
        } else {
            debug!(
                request_id = %request_id,
                "scatter: no pending request for response (already timed out or cancelled)"
            );
        }
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Resolve an explicit target node string to a [`NodeId`].
    ///
    /// Parses the string as a UUID and verifies the node exists and is alive
    /// in the knowledge store.
    fn resolve_explicit_target(&self, target: &str) -> PluginResult<Vec<NodeId>> {
        let uuid = Uuid::parse_str(target).map_err(|e| {
            PluginError::Scatter(format!(
                "invalid target_node UUID '{}': {}",
                target, e
            ))
        })?;

        let node_id = NodeId(uuid);
        match self.knowledge.get_node(&node_id) {
            Some(info) if info.status == NodeStatus::Alive => Ok(vec![node_id]),
            Some(_) => Err(PluginError::Scatter(format!(
                "target node {} is not alive",
                target
            ))),
            None => Err(PluginError::Scatter(format!(
                "target node {} not found in knowledge store",
                target
            ))),
        }
    }
}

// ============================================================================
// Resource-weighted capacity scoring
// ============================================================================

/// Compute a capacity factor for a node based on its hardware resources.
///
/// The baseline is an 8-core / 8 GB node = 1.0.  Nodes with more resources
/// score higher (capped at 4.0 to prevent extreme skew), while nodes with
/// fewer resources score lower (floored at 0.1 so they still get *some*
/// work).  When resource data is missing (zeros in the [`ResourceSnapshot`]),
/// the function returns 1.0 so behavior degrades gracefully to the old
/// load-only ranking.
fn capacity_factor(node: &NodeInfo) -> f64 {
    let cores = node.capacity.cpu_cores;
    let mem_mb = node.capacity.memory_total_mb;

    // If resource info is unavailable, fall back to neutral weight.
    if cores == 0 && mem_mb == 0 {
        return 1.0;
    }

    // Normalize: 8 cores = 1.0, 8192 MB = 1.0.
    let cpu_score = if cores > 0 {
        cores as f64 / 8.0
    } else {
        1.0
    };
    let mem_score = if mem_mb > 0 {
        mem_mb as f64 / 8192.0
    } else {
        1.0
    };

    // Geometric mean blends CPU and memory evenly.
    let raw = (cpu_score * mem_score).sqrt();

    // Clamp to [0.1, 4.0] to prevent extreme skew.
    raw.clamp(0.1, 4.0)
}

// ============================================================================
// Internal enum for scatter dispatch
// ============================================================================

/// Intermediate representation used during scatter fan-out.
///
/// Each scatter unit is either resolved to a pending async future or rejected
/// immediately with an error.
#[allow(dead_code)]
enum ImmediateResult {
    /// The unit could not be dispatched (e.g. no eligible node).
    Error {
        error: String,
    },
    /// The unit is in-flight; wait on `rx` for the response.
    Pending {
        target: NodeId,
        request_id: String,
        handle_req: HandleRequest,
        rx: tokio::sync::oneshot::Receiver<HandleResponse>,
        pending: Arc<DashMap<String, tokio::sync::oneshot::Sender<HandleResponse>>>,
        req_id_for_cleanup: String,
        timeout_dur: Duration,
    },
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::types::{NodeInfo, NodeStatus, ResourceSnapshot, Trait};
    use chrono::Utc;
    use std::collections::HashSet;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_knowledge(self_id: NodeId) -> Arc<KnowledgeStore> {
        Arc::new(KnowledgeStore::new(self_id))
    }

    fn make_node_info(node_id: NodeId, traits: HashSet<Trait>, load: f32) -> NodeInfo {
        NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits,
            load,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: node_id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        }
    }

    fn make_engine(self_id: NodeId, knowledge: Arc<KnowledgeStore>) -> ScatterEngine {
        ScatterEngine::new(self_id, knowledge, 128)
    }

    fn default_hints() -> ScatterHints {
        ScatterHints::default()
    }

    // -----------------------------------------------------------------------
    // trait_name_to_enum
    // -----------------------------------------------------------------------

    #[test]
    fn trait_name_to_enum_snake_case() {
        assert_eq!(
            ScatterEngine::trait_name_to_enum("can_execute"),
            Some(Trait::CanExecute)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("can_forward"),
            Some(Trait::CanForward)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("can_aggregate"),
            Some(Trait::CanAggregate)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("can_store_state"),
            Some(Trait::CanStoreState)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("can_discover"),
            Some(Trait::CanDiscover)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("can_relay"),
            Some(Trait::CanRelay)
        );
    }

    #[test]
    fn trait_name_to_enum_pascal_case() {
        assert_eq!(
            ScatterEngine::trait_name_to_enum("CanExecute"),
            Some(Trait::CanExecute)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("CanForward"),
            Some(Trait::CanForward)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("CanAggregate"),
            Some(Trait::CanAggregate)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("CanStoreState"),
            Some(Trait::CanStoreState)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("CanDiscover"),
            Some(Trait::CanDiscover)
        );
        assert_eq!(
            ScatterEngine::trait_name_to_enum("CanRelay"),
            Some(Trait::CanRelay)
        );
    }

    #[test]
    fn trait_name_to_enum_unknown_returns_none() {
        assert_eq!(ScatterEngine::trait_name_to_enum("can_fly"), None);
        assert_eq!(ScatterEngine::trait_name_to_enum(""), None);
        assert_eq!(ScatterEngine::trait_name_to_enum("CanDoAnything"), None);
    }

    // -----------------------------------------------------------------------
    // select_target_nodes
    // -----------------------------------------------------------------------

    #[test]
    fn select_targets_filters_by_trait() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let node_a = NodeId::new();
        let node_b = NodeId::new();

        knowledge.merge_node(make_node_info(
            node_a,
            HashSet::from([Trait::CanExecute, Trait::CanStoreState]),
            0.2,
        ));
        knowledge.merge_node(make_node_info(
            node_b,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = make_engine(self_id, knowledge);

        let unit = ScatterUnit {
            target_node: String::new(),
            payload: vec![1, 2, 3],
            required_traits: vec!["CanStoreState".to_string()],
        };

        let targets = engine.select_target_nodes(&unit, &default_hints());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0], node_a);
    }

    #[test]
    fn select_targets_orders_by_load() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let heavy = NodeId::new();
        let light = NodeId::new();

        knowledge.merge_node(make_node_info(
            heavy,
            HashSet::from([Trait::CanExecute]),
            0.9,
        ));
        knowledge.merge_node(make_node_info(
            light,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = make_engine(self_id, knowledge);

        let unit = ScatterUnit {
            target_node: String::new(),
            payload: vec![],
            required_traits: vec!["CanExecute".to_string()],
        };

        let targets = engine.select_target_nodes(&unit, &default_hints());
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0], light);
        assert_eq!(targets[1], heavy);
    }

    #[test]
    fn select_targets_prefer_local() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        // Add self as a node with higher load.
        knowledge.merge_node(make_node_info(
            self_id,
            HashSet::from([Trait::CanExecute]),
            0.8,
        ));

        // Add a remote node with lower load.
        let remote = NodeId::new();
        knowledge.merge_node(make_node_info(
            remote,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = make_engine(self_id, knowledge);

        let unit = ScatterUnit {
            target_node: String::new(),
            payload: vec![],
            required_traits: vec!["CanExecute".to_string()],
        };
        let hints = ScatterHints {
            locality: Locality::PreferLocal,
            ..default_hints()
        };

        let targets = engine.select_target_nodes(&unit, &hints);
        assert_eq!(targets.len(), 2);
        // Local node should be first despite higher load.
        assert_eq!(targets[0], self_id);
        assert_eq!(targets[1], remote);
    }

    #[test]
    fn select_targets_require_local() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        knowledge.merge_node(make_node_info(
            self_id,
            HashSet::from([Trait::CanExecute]),
            0.5,
        ));

        let remote = NodeId::new();
        knowledge.merge_node(make_node_info(
            remote,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = make_engine(self_id, knowledge);

        let unit = ScatterUnit {
            target_node: String::new(),
            payload: vec![],
            required_traits: vec!["CanExecute".to_string()],
        };
        let hints = ScatterHints {
            locality: Locality::RequireLocal,
            ..default_hints()
        };

        let targets = engine.select_target_nodes(&unit, &hints);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0], self_id);
    }

    #[test]
    fn select_targets_require_local_empty_when_self_not_eligible() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        // Self does not have the required trait.
        knowledge.merge_node(make_node_info(
            self_id,
            HashSet::from([Trait::CanExecute]),
            0.5,
        ));

        let engine = make_engine(self_id, knowledge);

        let unit = ScatterUnit {
            target_node: String::new(),
            payload: vec![],
            required_traits: vec!["CanStoreState".to_string()],
        };
        let hints = ScatterHints {
            locality: Locality::RequireLocal,
            ..default_hints()
        };

        let targets = engine.select_target_nodes(&unit, &hints);
        assert!(targets.is_empty());
    }

    #[test]
    fn select_targets_excludes_dead_nodes() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let dead_id = NodeId::new();
        knowledge.merge_node(make_node_info(
            dead_id,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));
        knowledge.mark_node_dead(&dead_id);

        let alive_id = NodeId::new();
        knowledge.merge_node(make_node_info(
            alive_id,
            HashSet::from([Trait::CanExecute]),
            0.5,
        ));

        let engine = make_engine(self_id, knowledge);

        let unit = ScatterUnit {
            target_node: String::new(),
            payload: vec![],
            required_traits: vec!["CanExecute".to_string()],
        };

        let targets = engine.select_target_nodes(&unit, &default_hints());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0], alive_id);
    }

    #[test]
    fn select_targets_no_required_traits_returns_all_live() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let n1 = NodeId::new();
        let n2 = NodeId::new();
        knowledge.merge_node(make_node_info(n1, HashSet::new(), 0.3));
        knowledge.merge_node(make_node_info(n2, HashSet::new(), 0.2));

        let engine = make_engine(self_id, knowledge);

        let unit = ScatterUnit {
            target_node: String::new(),
            payload: vec![],
            required_traits: vec![],
        };

        let targets = engine.select_target_nodes(&unit, &default_hints());
        assert_eq!(targets.len(), 2);
        // Sorted by load: n2 (0.2) before n1 (0.3).
        assert_eq!(targets[0], n2);
        assert_eq!(targets[1], n1);
    }

    #[test]
    fn select_targets_multiple_required_traits() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let full = NodeId::new();
        knowledge.merge_node(make_node_info(
            full,
            HashSet::from([Trait::CanExecute, Trait::CanStoreState, Trait::CanRelay]),
            0.3,
        ));

        let partial = NodeId::new();
        knowledge.merge_node(make_node_info(
            partial,
            HashSet::from([Trait::CanExecute, Trait::CanStoreState]),
            0.1,
        ));

        let engine = make_engine(self_id, knowledge);

        let unit = ScatterUnit {
            target_node: String::new(),
            payload: vec![],
            required_traits: vec![
                "CanExecute".to_string(),
                "CanStoreState".to_string(),
                "CanRelay".to_string(),
            ],
        };

        let targets = engine.select_target_nodes(&unit, &default_hints());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0], full);
    }

    // -----------------------------------------------------------------------
    // deliver_response
    // -----------------------------------------------------------------------

    #[test]
    fn deliver_response_resolves_pending() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        let request_id = Uuid::new_v4().to_string();
        let (tx, mut rx) = tokio::sync::oneshot::channel::<HandleResponse>();
        engine.pending_requests.insert(request_id.clone(), tx);

        let response = HandleResponse {
            payload: b"hello".to_vec(),
            error: String::new(),
        };

        engine.deliver_response(&request_id, response);

        let received = rx.try_recv().unwrap();
        assert_eq!(received.payload, b"hello");
        assert!(received.error.is_empty());
        assert!(engine.pending_requests.is_empty());
    }

    #[test]
    fn deliver_response_unknown_id_is_noop() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        let response = HandleResponse {
            payload: vec![],
            error: String::new(),
        };

        // Should not panic.
        engine.deliver_response("nonexistent-id", response);
        assert!(engine.pending_requests.is_empty());
    }

    // -----------------------------------------------------------------------
    // scatter (async tests)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn scatter_empty_units_returns_empty() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        let request = ScatterRequest {
            units: vec![],
            hints: default_hints(),
        };

        let response = engine.scatter(request).await.unwrap();
        assert!(response.results.is_empty());
    }

    #[tokio::test]
    async fn scatter_exceeds_max_concurrent() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        // Set max_concurrent to 2.
        let engine = ScatterEngine::new(self_id, knowledge, 2);

        let request = ScatterRequest {
            units: vec![
                ScatterUnit {
                    target_node: String::new(),
                    payload: vec![],
                    required_traits: vec![],
                },
                ScatterUnit {
                    target_node: String::new(),
                    payload: vec![],
                    required_traits: vec![],
                },
                ScatterUnit {
                    target_node: String::new(),
                    payload: vec![],
                    required_traits: vec![],
                },
            ],
            hints: default_hints(),
        };

        let result = engine.scatter(request).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::CapacityExceeded(msg) => {
                assert!(msg.contains("3 units"));
                assert!(msg.contains("max_concurrent is 2"));
            }
            other => panic!("expected CapacityExceeded, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn scatter_no_eligible_nodes_returns_error_result() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        // Knowledge store is empty, so no nodes are eligible.
        let engine = make_engine(self_id, knowledge);

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: String::new(),
                payload: b"work".to_vec(),
                required_traits: vec!["CanExecute".to_string()],
            }],
            hints: default_hints(),
        };

        let response = engine.scatter(request).await.unwrap();
        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].success);
        assert!(response.results[0].error.contains("no eligible node"));
    }

    #[tokio::test]
    async fn scatter_explicit_target_not_found() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        let fake_uuid = Uuid::new_v4().to_string();
        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: fake_uuid,
                payload: vec![],
                required_traits: vec![],
            }],
            hints: default_hints(),
        };

        let result = engine.scatter(request).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Scatter(msg) => assert!(msg.contains("not found")),
            other => panic!("expected Scatter error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn scatter_explicit_target_invalid_uuid() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: "not-a-uuid".to_string(),
                payload: vec![],
                required_traits: vec![],
            }],
            hints: default_hints(),
        };

        let result = engine.scatter(request).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Scatter(msg) => assert!(msg.contains("invalid target_node UUID")),
            other => panic!("expected Scatter error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn scatter_explicit_target_dead_node() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let dead_id = NodeId::new();
        knowledge.merge_node(make_node_info(
            dead_id,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));
        knowledge.mark_node_dead(&dead_id);

        let engine = make_engine(self_id, knowledge);

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: dead_id.0.to_string(),
                payload: vec![],
                required_traits: vec![],
            }],
            hints: default_hints(),
        };

        let result = engine.scatter(request).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Scatter(msg) => assert!(msg.contains("not alive")),
            other => panic!("expected Scatter error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn scatter_with_deliver_response_success() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let target = NodeId::new();
        knowledge.merge_node(make_node_info(
            target,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = Arc::new(make_engine(self_id, Arc::clone(&knowledge)));

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: String::new(),
                payload: b"compute-this".to_vec(),
                required_traits: vec!["CanExecute".to_string()],
            }],
            hints: ScatterHints {
                timeout_ms: 5000,
                ..default_hints()
            },
        };

        // Spawn a task that will deliver the response after the scatter has
        // registered the pending request.
        let engine_clone = Arc::clone(&engine);
        let responder = tokio::spawn(async move {
            // Wait until there is a pending request.
            loop {
                if !engine_clone.pending_requests.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            // Grab the request ID.
            let request_id = engine_clone
                .pending_requests
                .iter()
                .next()
                .map(|entry| entry.key().clone())
                .unwrap();

            engine_clone.deliver_response(
                &request_id,
                HandleResponse {
                    payload: b"result-data".to_vec(),
                    error: String::new(),
                },
            );
        });

        let response = engine.scatter(request).await.unwrap();
        responder.await.unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].success);
        assert_eq!(response.results[0].response, b"result-data");
        assert!(response.results[0].error.is_empty());
        assert_eq!(response.results[0].node_id, target.0.to_string());
        assert!(response.results[0].latency_ms < 5000);
    }

    #[tokio::test]
    async fn scatter_with_deliver_response_error() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let target = NodeId::new();
        knowledge.merge_node(make_node_info(
            target,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = Arc::new(make_engine(self_id, Arc::clone(&knowledge)));

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: String::new(),
                payload: b"bad-request".to_vec(),
                required_traits: vec!["CanExecute".to_string()],
            }],
            hints: ScatterHints {
                timeout_ms: 5000,
                ..default_hints()
            },
        };

        let engine_clone = Arc::clone(&engine);
        let responder = tokio::spawn(async move {
            loop {
                if !engine_clone.pending_requests.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            let request_id = engine_clone
                .pending_requests
                .iter()
                .next()
                .map(|entry| entry.key().clone())
                .unwrap();

            engine_clone.deliver_response(
                &request_id,
                HandleResponse {
                    payload: vec![],
                    error: "processing failed".to_string(),
                },
            );
        });

        let response = engine.scatter(request).await.unwrap();
        responder.await.unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].success);
        assert_eq!(response.results[0].error, "processing failed");
    }

    #[tokio::test]
    async fn scatter_timeout_returns_error_result() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let target = NodeId::new();
        knowledge.merge_node(make_node_info(
            target,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = make_engine(self_id, knowledge);

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: String::new(),
                payload: b"slow".to_vec(),
                required_traits: vec!["CanExecute".to_string()],
            }],
            hints: ScatterHints {
                timeout_ms: 50, // Very short timeout.
                ..default_hints()
            },
        };

        // Do not deliver any response -- let it time out.
        let response = engine.scatter(request).await.unwrap();
        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].success);
        assert!(response.results[0].error.contains("timed out"));
        assert_eq!(response.results[0].node_id, target.0.to_string());
        // After timeout the pending request should be cleaned up.
        assert!(engine.pending_requests.is_empty());
    }

    #[tokio::test]
    async fn scatter_multiple_units_mixed_results() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let exec_node = NodeId::new();
        knowledge.merge_node(make_node_info(
            exec_node,
            HashSet::from([Trait::CanExecute]),
            0.2,
        ));

        let engine = Arc::new(make_engine(self_id, Arc::clone(&knowledge)));

        let request = ScatterRequest {
            units: vec![
                // Unit 0: targeted at exec_node (will get a response).
                ScatterUnit {
                    target_node: String::new(),
                    payload: b"unit-0".to_vec(),
                    required_traits: vec!["CanExecute".to_string()],
                },
                // Unit 1: requires a trait nobody has (immediate error).
                ScatterUnit {
                    target_node: String::new(),
                    payload: b"unit-1".to_vec(),
                    required_traits: vec!["CanRelay".to_string()],
                },
            ],
            hints: ScatterHints {
                timeout_ms: 5000,
                ..default_hints()
            },
        };

        let engine_clone = Arc::clone(&engine);
        let responder = tokio::spawn(async move {
            loop {
                if !engine_clone.pending_requests.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            let request_id = engine_clone
                .pending_requests
                .iter()
                .next()
                .map(|entry| entry.key().clone())
                .unwrap();

            engine_clone.deliver_response(
                &request_id,
                HandleResponse {
                    payload: b"ok-0".to_vec(),
                    error: String::new(),
                },
            );
        });

        let response = engine.scatter(request).await.unwrap();
        responder.await.unwrap();

        assert_eq!(response.results.len(), 2);

        // Unit 0: success.
        assert!(response.results[0].success);
        assert_eq!(response.results[0].response, b"ok-0");

        // Unit 1: no eligible node.
        assert!(!response.results[1].success);
        assert!(response.results[1].error.contains("no eligible node"));
    }

    #[tokio::test]
    async fn scatter_with_explicit_target_and_deliver() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let target = NodeId::new();
        knowledge.merge_node(make_node_info(
            target,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = Arc::new(make_engine(self_id, Arc::clone(&knowledge)));

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: target.0.to_string(),
                payload: b"targeted-work".to_vec(),
                required_traits: vec![],
            }],
            hints: ScatterHints {
                timeout_ms: 5000,
                ..default_hints()
            },
        };

        let engine_clone = Arc::clone(&engine);
        let responder = tokio::spawn(async move {
            loop {
                if !engine_clone.pending_requests.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            let request_id = engine_clone
                .pending_requests
                .iter()
                .next()
                .map(|entry| entry.key().clone())
                .unwrap();

            engine_clone.deliver_response(
                &request_id,
                HandleResponse {
                    payload: b"targeted-result".to_vec(),
                    error: String::new(),
                },
            );
        });

        let response = engine.scatter(request).await.unwrap();
        responder.await.unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].success);
        assert_eq!(response.results[0].response, b"targeted-result");
        assert_eq!(response.results[0].node_id, target.0.to_string());
    }

    #[tokio::test]
    async fn scatter_zero_timeout_uses_default() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let target = NodeId::new();
        knowledge.merge_node(make_node_info(
            target,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = Arc::new(make_engine(self_id, Arc::clone(&knowledge)));

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: String::new(),
                payload: vec![],
                required_traits: vec!["CanExecute".to_string()],
            }],
            hints: ScatterHints {
                timeout_ms: 0, // Should use the default.
                ..default_hints()
            },
        };

        let engine_clone = Arc::clone(&engine);
        let responder = tokio::spawn(async move {
            loop {
                if !engine_clone.pending_requests.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            let request_id = engine_clone
                .pending_requests
                .iter()
                .next()
                .map(|entry| entry.key().clone())
                .unwrap();

            engine_clone.deliver_response(
                &request_id,
                HandleResponse {
                    payload: b"default-timeout-ok".to_vec(),
                    error: String::new(),
                },
            );
        });

        let response = engine.scatter(request).await.unwrap();
        responder.await.unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(response.results[0].success);
        assert_eq!(response.results[0].response, b"default-timeout-ok");
    }

    #[tokio::test]
    async fn scatter_channel_closed_returns_error() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let target = NodeId::new();
        knowledge.merge_node(make_node_info(
            target,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = Arc::new(make_engine(self_id, Arc::clone(&knowledge)));

        let request = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: String::new(),
                payload: vec![],
                required_traits: vec!["CanExecute".to_string()],
            }],
            hints: ScatterHints {
                timeout_ms: 5000,
                ..default_hints()
            },
        };

        // Drop the sender without sending to simulate a channel close.
        let engine_clone = Arc::clone(&engine);
        let dropper = tokio::spawn(async move {
            loop {
                if !engine_clone.pending_requests.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            let request_id = engine_clone
                .pending_requests
                .iter()
                .next()
                .map(|entry| entry.key().clone())
                .unwrap();

            // Remove the sender, which will drop it.
            engine_clone.pending_requests.remove(&request_id);
        });

        let response = engine.scatter(request).await.unwrap();
        dropper.await.unwrap();

        assert_eq!(response.results.len(), 1);
        assert!(!response.results[0].success);
        assert!(response.results[0].error.contains("channel closed"));
    }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    #[test]
    fn engine_new_creates_empty_pending() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        assert!(engine.pending_requests.is_empty());
        assert_eq!(engine.max_concurrent, 128);
    }

    #[test]
    fn pending_requests_accessor() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        let pending = engine.pending_requests();
        assert!(pending.is_empty());

        // Verify it shares the same Arc.
        let (tx, _rx) = tokio::sync::oneshot::channel::<HandleResponse>();
        pending.insert("test-id".to_string(), tx);
        assert_eq!(engine.pending_requests.len(), 1);
    }

    // -----------------------------------------------------------------------
    // resolve_explicit_target (indirectly via scatter)
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_explicit_target_valid() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);

        let target = NodeId::new();
        knowledge.merge_node(make_node_info(
            target,
            HashSet::from([Trait::CanExecute]),
            0.1,
        ));

        let engine = make_engine(self_id, knowledge);
        let result = engine.resolve_explicit_target(&target.0.to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![target]);
    }

    #[test]
    fn resolve_explicit_target_not_found() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        let missing = Uuid::new_v4().to_string();
        let result = engine.resolve_explicit_target(&missing);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_explicit_target_bad_uuid() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge(self_id);
        let engine = make_engine(self_id, knowledge);

        let result = engine.resolve_explicit_target("garbage");
        assert!(result.is_err());
    }
}
