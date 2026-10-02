// Marabunta - Licensed under the MIT License.
//! WebSocket-based browser compute node bridge for the Marabunta Swarm.
//!
//! This module allows browsers to join the swarm as actual compute nodes
//! via WebSocket connections. Browser tabs connect to `/ws/compute`, receive
//! a [`NodeId`], and can execute JavaScript and WebAssembly tasks distributed
//! by the swarm.
//!
//! # Architecture
//!
//! ```text
//!   Browser A (4 cores)  ──┐
//!   Browser B (8 cores)  ──┤── WebSocket ──► BrowserBridge ──► KnowledgeStore
//!   Browser C (2 cores)  ──┘                     │
//!                                                ▼
//!                                          Task Distribution
//!                                          Result Collection
//! ```
//!
//! The [`BrowserBridge`] maintains a concurrent map of connected browser nodes,
//! handles task distribution, result collection, and heartbeat monitoring.
//! Browser nodes are registered in the swarm's [`KnowledgeStore`] as
//! [`NodeType::Browser`] so they are visible to all other subsystems.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket};
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::knowledge::KnowledgeStore;
use super::profile::{NodeType, ProfileStore};
use super::types::{
    ChunkId, ChunkResult, ChunkStatus, NodeId, NodeInfo, NodeStatus, ResourceSnapshot, Trait,
};
use crate::common::types::JobId;

// ============================================================================
// Configuration constants
// ============================================================================

/// How often to send stats broadcasts to all connected browsers (5s).
const STATS_BROADCAST_INTERVAL: Duration = Duration::from_secs(5);

/// Heartbeat timeout: if a browser doesn't heartbeat within 30s, mark dead.
const BROWSER_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// How often to check for heartbeat timeouts (10s).
const HEARTBEAT_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// How often to send ping frames to keep the connection alive.
const PING_INTERVAL: Duration = Duration::from_secs(15);

/// Maximum time a browser task can run before being considered timed out.
const BROWSER_TASK_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum number of concurrent browser connections.
const MAX_BROWSER_CONNECTIONS: usize = 1024;

// ============================================================================
// Wire protocol types (JSON messages)
// ============================================================================

/// Messages from browser to server.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BrowserInbound {
    /// Initial join message with browser capabilities.
    Join {
        cores: u32,
        capabilities: Vec<String>,
        #[serde(default, rename = "userAgent")]
        user_agent: String,
    },
    /// Periodic heartbeat with current load.
    Heartbeat {
        load: f32,
        #[serde(default, rename = "tasksActive")]
        tasks_active: u32,
    },
    /// Task execution result.
    Result {
        #[serde(rename = "taskId")]
        task_id: String,
        output: String,
        #[serde(default, rename = "durationMs")]
        duration_ms: u64,
    },
    /// Task execution error.
    Error {
        #[serde(rename = "taskId")]
        task_id: String,
        error: String,
    },
}

/// Messages from server to browser.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BrowserOutbound {
    /// Sent after join to confirm connection.
    Welcome {
        #[serde(rename = "nodeId")]
        node_id: String,
        #[serde(rename = "swarmSize")]
        swarm_size: usize,
    },
    /// A task to execute.
    Task {
        #[serde(rename = "taskId")]
        task_id: String,
        #[serde(rename = "taskType")]
        task_type: String,
        code: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        input: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", rename = "moduleUrl")]
        module_url: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", rename = "funcName")]
        func_name: Option<String>,
    },
    /// Periodic stats update.
    Stats {
        #[serde(rename = "totalNodes")]
        total_nodes: usize,
        #[serde(rename = "totalBrowsers")]
        total_browsers: usize,
        #[serde(rename = "jobsActive")]
        jobs_active: usize,
        #[serde(rename = "tasksPerSecond")]
        tasks_per_second: f64,
    },
    /// Keep-alive ping.
    Ping,
}

// ============================================================================
// BrowserNode — per-connection state
// ============================================================================

/// State for a single connected browser node.
pub struct BrowserNode {
    pub node_id: NodeId,
    pub connected_at: Instant,
    pub last_heartbeat: Instant,
    pub user_agent: String,
    pub cores: u32,
    pub capabilities: Vec<String>,
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub tasks_active: AtomicU64,
    pub current_load: RwLock<f32>,
    /// Channel to send messages to this specific browser.
    pub tx: mpsc::UnboundedSender<BrowserOutbound>,
}

impl BrowserNode {
    fn new(
        node_id: NodeId,
        cores: u32,
        capabilities: Vec<String>,
        user_agent: String,
        tx: mpsc::UnboundedSender<BrowserOutbound>,
    ) -> Self {
        Self {
            node_id,
            connected_at: Instant::now(),
            last_heartbeat: Instant::now(),
            user_agent,
            cores,
            capabilities,
            tasks_completed: AtomicU64::new(0),
            tasks_failed: AtomicU64::new(0),
            tasks_active: AtomicU64::new(0),
            current_load: RwLock::new(0.0),
            tx,
        }
    }

    fn uptime_secs(&self) -> u64 {
        self.connected_at.elapsed().as_secs()
    }

    fn is_available(&self) -> bool {
        let load = *self.current_load.read();
        let active = self.tasks_active.load(Ordering::Relaxed);
        load < 0.9 && active < self.cores as u64
    }
}

// ============================================================================
// PendingTask — tracks tasks dispatched to browsers
// ============================================================================

/// A task that has been sent to a browser and is awaiting completion.
struct PendingTask {
    #[allow(dead_code)]
    task_id: String,
    chunk_id: ChunkId,
    job_id: JobId,
    assigned_to: NodeId,
    dispatched_at: Instant,
}

// ============================================================================
// BrowserBridge — the main bridge
// ============================================================================

/// WebSocket bridge that allows browsers to participate as swarm compute nodes.
///
/// The bridge manages WebSocket connections, distributes tasks to browsers,
/// collects results, and maintains browser nodes in the swarm knowledge store.
pub struct BrowserBridge {
    /// Shared swarm knowledge store.
    knowledge: Arc<KnowledgeStore>,
    /// Profile store for registering browser profiles.
    profile_store: Arc<ProfileStore>,
    /// All connected browser nodes, keyed by NodeId.
    browsers: Arc<DashMap<NodeId, BrowserNode>>,
    /// Tasks pending completion, keyed by task_id string.
    pending_tasks: Arc<DashMap<String, PendingTask>>,
    /// The local swarm node's ID (for "via" field in knowledge store).
    local_node_id: NodeId,
    /// Broadcast channel for pushing tasks to all connection handlers.
    #[allow(dead_code)]
    task_tx: broadcast::Sender<BrowserOutbound>,
    /// Global counters for aggregate stats.
    total_tasks_completed: AtomicU64,
    total_tasks_dispatched: AtomicU64,
    started_at: Instant,
}

impl BrowserBridge {
    /// Create a new browser bridge.
    pub fn new(
        local_node_id: NodeId,
        knowledge: Arc<KnowledgeStore>,
        profile_store: Arc<ProfileStore>,
    ) -> Self {
        let (task_tx, _) = broadcast::channel(256);

        Self {
            knowledge,
            profile_store,
            browsers: Arc::new(DashMap::new()),
            pending_tasks: Arc::new(DashMap::new()),
            local_node_id,
            task_tx,
            total_tasks_completed: AtomicU64::new(0),
            total_tasks_dispatched: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }

    /// Number of currently connected browser nodes.
    pub fn browser_count(&self) -> usize {
        self.browsers.len()
    }

    /// Get summary information about all connected browsers.
    pub fn browser_summaries(&self) -> Vec<BrowserSummary> {
        self.browsers
            .iter()
            .map(|entry| {
                let b = entry.value();
                BrowserSummary {
                    node_id: b.node_id,
                    cores: b.cores,
                    capabilities: b.capabilities.clone(),
                    user_agent: b.user_agent.clone(),
                    tasks_completed: b.tasks_completed.load(Ordering::Relaxed),
                    tasks_failed: b.tasks_failed.load(Ordering::Relaxed),
                    tasks_active: b.tasks_active.load(Ordering::Relaxed),
                    uptime_secs: b.uptime_secs(),
                    load: *b.current_load.read(),
                }
            })
            .collect()
    }

    /// Get aggregate stats for all browsers.
    pub fn aggregate_stats(&self) -> BrowserAggregateStats {
        let total_cores: u32 = self.browsers.iter().map(|e| e.value().cores).sum();
        let total_completed = self.total_tasks_completed.load(Ordering::Relaxed);
        let uptime = self.started_at.elapsed().as_secs_f64();
        let tasks_per_second = if uptime > 0.0 {
            total_completed as f64 / uptime
        } else {
            0.0
        };

        BrowserAggregateStats {
            connected_browsers: self.browsers.len(),
            total_cores,
            total_tasks_completed: total_completed,
            total_tasks_dispatched: self.total_tasks_dispatched.load(Ordering::Relaxed),
            tasks_per_second,
            active_jobs: self.knowledge.get_active_jobs().len(),
        }
    }

    /// Submit a task to be executed by a browser node.
    ///
    /// Picks the most available browser and sends the task. Returns true if
    /// a browser accepted the task, false if no browsers are available.
    pub fn dispatch_task(
        &self,
        chunk_id: ChunkId,
        job_id: JobId,
        task_type: &str,
        code: &str,
        input: Option<&str>,
    ) -> bool {
        // Find the most available browser.
        let best = self
            .browsers
            .iter()
            .filter(|e| e.value().is_available())
            .min_by_key(|e| e.value().tasks_active.load(Ordering::Relaxed));

        if let Some(entry) = best {
            let browser = entry.value();
            let task_id = Uuid::new_v4().to_string();

            let outbound = BrowserOutbound::Task {
                task_id: task_id.clone(),
                task_type: task_type.to_string(),
                code: code.to_string(),
                input: input.map(|s| s.to_string()),
                module_url: None,
                func_name: None,
            };

            if browser.tx.send(outbound).is_ok() {
                browser.tasks_active.fetch_add(1, Ordering::Relaxed);
                self.total_tasks_dispatched.fetch_add(1, Ordering::Relaxed);

                self.pending_tasks.insert(
                    task_id,
                    PendingTask {
                        task_id: Uuid::new_v4().to_string(),
                        chunk_id,
                        job_id,
                        assigned_to: browser.node_id,
                        dispatched_at: Instant::now(),
                    },
                );

                debug!(
                    browser = %browser.node_id,
                    chunk = %chunk_id,
                    "dispatched task to browser"
                );
                return true;
            }
        }

        false
    }

    /// Handle a new WebSocket connection.
    ///
    /// This is called from the axum WebSocket upgrade handler. It takes
    /// ownership of the WebSocket and runs the connection loop until the
    /// browser disconnects or times out.
    pub async fn handle_connection(self: Arc<Self>, ws: WebSocket) {
        if self.browsers.len() >= MAX_BROWSER_CONNECTIONS {
            warn!("browser bridge: max connections reached, rejecting");
            return;
        }

        let (mut ws_tx, mut ws_rx) = ws.split();
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<BrowserOutbound>();

        // We need to wait for the join message before we know the node ID.
        let mut node_id: Option<NodeId> = None;
        let bridge = Arc::clone(&self);

        use futures::{SinkExt, StreamExt};

        // Spawn the outbound writer: reads from the mpsc channel and writes to WebSocket.
        let writer_handle = tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if let Ok(json) = serde_json::to_string(&msg) {
                    if ws_tx.send(Message::Text(json)).await.is_err() {
                        break;
                    }
                }
            }
        });

        // Read loop: process inbound messages.
        let mut joined = false;
        #[allow(unused_assignments)]
        let mut assigned_node_id = NodeId::new(); // placeholder, overwritten on join

        while let Some(msg_result) = ws_rx.next().await {
            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    debug!(error = %e, "browser ws read error");
                    break;
                }
            };

            match msg {
                Message::Text(text) => {
                    let parsed: Result<BrowserInbound, _> = serde_json::from_str(&text);
                    match parsed {
                        Ok(inbound) => {
                            match inbound {
                                BrowserInbound::Join {
                                    cores,
                                    capabilities,
                                    user_agent,
                                } => {
                                    if joined {
                                        continue; // ignore duplicate joins
                                    }

                                    assigned_node_id = NodeId::new();
                                    node_id = Some(assigned_node_id);

                                    // Register the browser node.
                                    let browser_node = BrowserNode::new(
                                        assigned_node_id,
                                        cores,
                                        capabilities.clone(),
                                        user_agent.clone(),
                                        outbound_tx.clone(),
                                    );
                                    bridge.browsers.insert(assigned_node_id, browser_node);

                                    // Register in the swarm knowledge store.
                                    bridge.register_browser_in_knowledge(
                                        assigned_node_id,
                                        cores,
                                        &capabilities,
                                    );

                                    // Send welcome.
                                    let swarm_size = bridge.knowledge.node_count();
                                    let _ = outbound_tx.send(BrowserOutbound::Welcome {
                                        node_id: assigned_node_id.0.to_string(),
                                        swarm_size,
                                    });

                                    joined = true;

                                    info!(
                                        node_id = %assigned_node_id,
                                        cores = cores,
                                        capabilities = ?capabilities,
                                        browsers = bridge.browsers.len(),
                                        "browser node joined the swarm"
                                    );
                                }

                                BrowserInbound::Heartbeat { load, tasks_active } => {
                                    if let Some(nid) = node_id {
                                        if let Some(entry) = bridge.browsers.get(&nid) {
                                            // Update heartbeat timestamp via interior mutability.
                                            // BrowserNode uses atomics and RwLock for fields we
                                            // need to update, so a shared ref suffices.
                                            *entry.value().current_load.write() = load;
                                            entry.value().tasks_active.store(
                                                tasks_active as u64,
                                                Ordering::Relaxed,
                                            );
                                        }
                                        // Touch the heartbeat via a mutable ref.
                                        if let Some(mut entry) = bridge.browsers.get_mut(&nid) {
                                            entry.value_mut().last_heartbeat = Instant::now();
                                        }

                                        // Update load in knowledge store.
                                        bridge.update_browser_load(nid, load);
                                    }
                                }

                                BrowserInbound::Result {
                                    task_id,
                                    output,
                                    duration_ms,
                                } => {
                                    bridge.handle_task_result(
                                        &task_id,
                                        &output,
                                        duration_ms,
                                        true,
                                        "",
                                    );
                                    if let Some(nid) = node_id {
                                        if let Some(entry) = bridge.browsers.get(&nid) {
                                            entry
                                                .value()
                                                .tasks_completed
                                                .fetch_add(1, Ordering::Relaxed);
                                            let active =
                                                entry.value().tasks_active.load(Ordering::Relaxed);
                                            if active > 0 {
                                                entry.value().tasks_active.store(
                                                    active - 1,
                                                    Ordering::Relaxed,
                                                );
                                            }
                                        }
                                    }
                                }

                                BrowserInbound::Error { task_id, error } => {
                                    bridge.handle_task_result(&task_id, "", 0, false, &error);
                                    if let Some(nid) = node_id {
                                        if let Some(entry) = bridge.browsers.get(&nid) {
                                            entry
                                                .value()
                                                .tasks_failed
                                                .fetch_add(1, Ordering::Relaxed);
                                            let active =
                                                entry.value().tasks_active.load(Ordering::Relaxed);
                                            if active > 0 {
                                                entry.value().tasks_active.store(
                                                    active - 1,
                                                    Ordering::Relaxed,
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!(error = %e, "browser ws: failed to parse inbound message");
                        }
                    }
                }

                Message::Close(_) => {
                    debug!("browser ws: received close frame");
                    break;
                }

                // Ignore binary, ping, pong frames at this level.
                _ => {}
            }
        }

        // Cleanup on disconnect.
        if let Some(nid) = node_id {
            bridge.handle_disconnect(nid);
        }

        // Abort the writer task.
        writer_handle.abort();
    }

    /// Spawn background loops: stats broadcaster, heartbeat checker, task timeout checker.
    pub fn spawn_background_loops(
        self: &Arc<Self>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();

        // 1. Stats broadcaster.
        {
            let bridge = Arc::clone(self);
            let mut shutdown = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(STATS_BROADCAST_INTERVAL) => {}
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }

                    if *shutdown.borrow() {
                        break;
                    }

                    let stats = bridge.aggregate_stats();
                    let msg = BrowserOutbound::Stats {
                        total_nodes: bridge.knowledge.node_count(),
                        total_browsers: stats.connected_browsers,
                        jobs_active: stats.active_jobs,
                        tasks_per_second: stats.tasks_per_second,
                    };

                    for entry in bridge.browsers.iter() {
                        let _ = entry.value().tx.send(msg.clone());
                    }
                }
            }));
        }

        // 2. Heartbeat checker.
        {
            let bridge = Arc::clone(self);
            let mut shutdown = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(HEARTBEAT_CHECK_INTERVAL) => {}
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }

                    if *shutdown.borrow() {
                        break;
                    }

                    let now = Instant::now();
                    let dead_browsers: Vec<NodeId> = bridge
                        .browsers
                        .iter()
                        .filter(|e| now.duration_since(e.value().last_heartbeat) > BROWSER_HEARTBEAT_TIMEOUT)
                        .map(|e| e.value().node_id)
                        .collect();

                    for nid in dead_browsers {
                        warn!(node_id = %nid, "browser heartbeat timeout, disconnecting");
                        bridge.handle_disconnect(nid);
                    }
                }
            }));
        }

        // 3. Task timeout checker.
        {
            let bridge = Arc::clone(self);
            let mut shutdown = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }

                    if *shutdown.borrow() {
                        break;
                    }

                    let now = Instant::now();
                    let timed_out: Vec<String> = bridge
                        .pending_tasks
                        .iter()
                        .filter(|e| now.duration_since(e.value().dispatched_at) > BROWSER_TASK_TIMEOUT)
                        .map(|e| e.key().clone())
                        .collect();

                    for task_id in timed_out {
                        warn!(task_id = %task_id, "browser task timed out");
                        bridge.handle_task_result(&task_id, "", 0, false, "task timed out");
                    }
                }
            }));
        }

        // 4. Ping sender.
        {
            let bridge = Arc::clone(self);
            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(PING_INTERVAL) => {}
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }

                    if *shutdown_rx.borrow() {
                        break;
                    }

                    for entry in bridge.browsers.iter() {
                        let _ = entry.value().tx.send(BrowserOutbound::Ping);
                    }
                }
            }));
        }

        handles
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Register a browser node in the swarm knowledge store.
    fn register_browser_in_knowledge(
        &self,
        node_id: NodeId,
        cores: u32,
        _capabilities: &[String],
    ) {
        let node_info = NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.0,
            capacity: ResourceSnapshot {
                cpu_cores: cores,
                cpu_available: 1.0,
                memory_total_mb: 0, // unknown for browsers
                memory_available_mb: 0,
                disk_total_mb: 0,
                disk_available_mb: 0,
                network_bandwidth_mbps: 0.0,
                current_tdp_watts: 15.0,
                ask_usd_per_megagas: 0.0001,
            },
            address: None,
            via: self.local_node_id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            is_training: false,
            is_pgwire_active: false,
            chaos_state: Default::default(),
            };
        self.knowledge.merge_node(node_info);

        // Register a browser profile.
        let profile = super::profile::NodeProfile::detect(node_id, NodeType::Browser);
        self.profile_store.upsert(profile);
    }

    /// Update a browser's load in the knowledge store.
    fn update_browser_load(&self, node_id: NodeId, load: f32) {
        if let Some(mut info) = self.knowledge.get_node(&node_id) {
            info.last_seen = Utc::now();
            info.load = load;
            info.status = NodeStatus::Alive;
            self.knowledge.merge_node(info);
        }
    }

    /// Handle a completed or failed task from a browser.
    fn handle_task_result(
        &self,
        task_id: &str,
        output: &str,
        duration_ms: u64,
        success: bool,
        error: &str,
    ) {
        if let Some((_, pending)) = self.pending_tasks.remove(task_id) {
            let result = ChunkResult {
                output_blob_hash: None,
                success,
                output: output.as_bytes().to_vec(),
                stdout: output.to_string(),
                stderr: error.to_string(),
                duration_ms,
                completed_at: Utc::now(),
                fuel_consumed: 0, execution_error: None,  is_e2ee: false, blind_execution_proof: None, journal_dump: None };

            let assignment = super::types::Assignment {
                chunk_id: pending.chunk_id,
                job_id: pending.job_id,
                assigned_to: Some(pending.assigned_to),
                assigned_at: Utc::now(),
                status: if success {
                    ChunkStatus::Completed
                } else {
                    ChunkStatus::Failed
                },
                result: Some(result),
                agreed_price: 50,
                attempts: 1,
                replica_group_id: None,
                failed_nodes: Vec::new(),
            };
            self.knowledge.merge_assignment(assignment);

            if success {
                self.total_tasks_completed.fetch_add(1, Ordering::Relaxed);
            }

            debug!(
                task_id = task_id,
                success = success,
                duration_ms = duration_ms,
                "browser task completed"
            );
        }
    }

    /// Handle browser disconnection: cleanup state, reassign pending tasks.
    fn handle_disconnect(&self, node_id: NodeId) {
        // Remove from the browser map.
        if let Some((_, browser)) = self.browsers.remove(&node_id) {
            info!(
                node_id = %node_id,
                uptime_secs = browser.uptime_secs(),
                tasks_completed = browser.tasks_completed.load(Ordering::Relaxed),
                browsers_remaining = self.browsers.len(),
                "browser node disconnected"
            );
        }

        // Mark node as dead in knowledge store.
        self.knowledge.mark_node_dead(&node_id);

        // Reassign any pending tasks from this browser.
        let orphaned: Vec<String> = self
            .pending_tasks
            .iter()
            .filter(|e| e.value().assigned_to == node_id)
            .map(|e| e.key().clone())
            .collect();

        for task_id in orphaned {
            if let Some((_, pending)) = self.pending_tasks.remove(&task_id) {
                // Reset the chunk to pending so it can be reassigned.
                let assignment = super::types::Assignment {
                    chunk_id: pending.chunk_id,
                    job_id: pending.job_id,
                    assigned_to: None,
                    assigned_at: Utc::now(),
                    status: ChunkStatus::Pending,
                    agreed_price: 50,
                    result: None,
                    attempts: 0,
                    replica_group_id: None,
                    failed_nodes: Vec::new(),
                };
                self.knowledge.merge_assignment(assignment);

                debug!(
                    task_id = task_id,
                    chunk = %pending.chunk_id,
                    "reassigned orphaned browser task"
                );
            }
        }
    }
}

// ============================================================================
// Response types for the REST API
// ============================================================================

/// Summary of a single connected browser node.
#[derive(Debug, Clone, Serialize)]
pub struct BrowserSummary {
    pub node_id: NodeId,
    pub cores: u32,
    pub capabilities: Vec<String>,
    pub user_agent: String,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub tasks_active: u64,
    pub uptime_secs: u64,
    pub load: f32,
}

/// Aggregate stats across all connected browsers.
#[derive(Debug, Clone, Serialize)]
pub struct BrowserAggregateStats {
    pub connected_browsers: usize,
    pub total_cores: u32,
    pub total_tasks_completed: u64,
    pub total_tasks_dispatched: u64,
    pub tasks_per_second: f64,
    pub active_jobs: usize,
}

/// Full browser list response for the REST endpoint.
#[derive(Debug, Serialize)]
pub struct BrowserListResponse {
    pub browsers: Vec<BrowserSummary>,
    pub aggregate: BrowserAggregateStats,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bridge() -> Arc<BrowserBridge> {
        let node_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(node_id));
        let profile_store = Arc::new(ProfileStore::new());
        Arc::new(BrowserBridge::new(node_id, knowledge, profile_store))
    }

    #[test]
    fn bridge_creation() {
        let bridge = make_bridge();
        assert_eq!(bridge.browser_count(), 0);
        assert_eq!(bridge.aggregate_stats().connected_browsers, 0);
    }

    #[test]
    fn browser_summaries_empty() {
        let bridge = make_bridge();
        assert!(bridge.browser_summaries().is_empty());
    }

    #[test]
    fn aggregate_stats_default() {
        let bridge = make_bridge();
        let stats = bridge.aggregate_stats();
        assert_eq!(stats.connected_browsers, 0);
        assert_eq!(stats.total_cores, 0);
        assert_eq!(stats.total_tasks_completed, 0);
    }

    #[test]
    fn dispatch_task_no_browsers() {
        let bridge = make_bridge();
        let result = bridge.dispatch_task(
            ChunkId::new(),
            JobId::new(),
            "js",
            "return 42;",
            None,
        );
        assert!(!result);
    }

    #[test]
    fn inbound_message_parsing() {
        let join_json = r#"{"type":"join","cores":4,"capabilities":["wasm","js"],"userAgent":"test"}"#;
        let parsed: BrowserInbound = serde_json::from_str(join_json).unwrap();
        assert!(matches!(parsed, BrowserInbound::Join { cores: 4, .. }));

        let hb_json = r#"{"type":"heartbeat","load":0.5,"tasksActive":2}"#;
        let parsed: BrowserInbound = serde_json::from_str(hb_json).unwrap();
        assert!(matches!(parsed, BrowserInbound::Heartbeat { .. }));

        let result_json = r#"{"type":"result","taskId":"abc","output":"42","durationMs":100}"#;
        let parsed: BrowserInbound = serde_json::from_str(result_json).unwrap();
        assert!(matches!(parsed, BrowserInbound::Result { .. }));

        let error_json = r#"{"type":"error","taskId":"abc","error":"boom"}"#;
        let parsed: BrowserInbound = serde_json::from_str(error_json).unwrap();
        assert!(matches!(parsed, BrowserInbound::Error { .. }));
    }

    #[test]
    fn outbound_message_serialization() {
        let welcome = BrowserOutbound::Welcome {
            node_id: "test-123".to_string(),
            swarm_size: 5,
        };
        let json = serde_json::to_string(&welcome).unwrap();
        assert!(json.contains("welcome"));
        assert!(json.contains("test-123"));

        let task = BrowserOutbound::Task {
            task_id: "t1".to_string(),
            task_type: "js".to_string(),
            code: "return 1+1".to_string(),
            input: Some("{}".to_string()),
            module_url: None,
            func_name: None,
        };
        let json = serde_json::to_string(&task).unwrap();
        assert!(json.contains("task"));
        assert!(json.contains("return 1+1"));

        let stats = BrowserOutbound::Stats {
            total_nodes: 10,
            total_browsers: 3,
            jobs_active: 2,
            tasks_per_second: 1.5,
        };
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("stats"));

        let ping = BrowserOutbound::Ping;
        let json = serde_json::to_string(&ping).unwrap();
        assert!(json.contains("ping"));
    }

    #[test]
    fn browser_node_availability() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let node = BrowserNode::new(
            NodeId::new(),
            4,
            vec!["js".to_string()],
            "test".to_string(),
            tx,
        );

        assert!(node.is_available());

        // High load makes unavailable.
        *node.current_load.write() = 0.95;
        assert!(!node.is_available());

        // Reset load, max out tasks.
        *node.current_load.write() = 0.1;
        node.tasks_active.store(4, Ordering::Relaxed);
        assert!(!node.is_available());
    }

    #[test]
    fn handle_disconnect_cleanup() {
        let bridge = make_bridge();
        let nid = NodeId::new();

        // Register a fake browser node.
        let (tx, _rx) = mpsc::unbounded_channel();
        bridge.browsers.insert(
            nid,
            BrowserNode::new(nid, 2, vec![], "test".to_string(), tx),
        );
        bridge
            .register_browser_in_knowledge(nid, 2, &[]);

        assert_eq!(bridge.browser_count(), 1);

        // Disconnect.
        bridge.handle_disconnect(nid);
        assert_eq!(bridge.browser_count(), 0);

        // Knowledge store should have marked node dead.
        let node_info = bridge.knowledge.get_node(&nid).unwrap();
        assert_eq!(node_info.status, NodeStatus::Dead);
    }

    #[test]
    fn task_result_handling() {
        let bridge = make_bridge();
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        let nid = NodeId::new();

        // Register a pending task.
        bridge.pending_tasks.insert(
            "task-1".to_string(),
            PendingTask {
                task_id: "task-1".to_string(),
                chunk_id,
                job_id,
                assigned_to: nid,
                dispatched_at: Instant::now(),
            },
        );

        // Also need an assignment in the knowledge store.
        bridge.knowledge.merge_assignment(super::super::types::Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(nid),
            assigned_at: Utc::now(),
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        });

        // Handle result.
        bridge.handle_task_result("task-1", "42", 100, true, "");

        // Check that the assignment was updated.
        let assignment = bridge.knowledge.get_assignment(&chunk_id).unwrap();
        assert_eq!(assignment.status, ChunkStatus::Completed);
        assert!(assignment.result.is_some());

        // Pending task should be removed.
        assert!(bridge.pending_tasks.is_empty());
    }

    #[test]
    fn register_browser_in_knowledge_store() {
        let bridge = make_bridge();
        let nid = NodeId::new();

        bridge.register_browser_in_knowledge(nid, 8, &["wasm".to_string(), "js".to_string()]);

        // Verify node is in knowledge store.
        let info = bridge.knowledge.get_node(&nid).unwrap();
        assert_eq!(info.capacity.cpu_cores, 8);
        assert_eq!(info.status, NodeStatus::Alive);
        assert!(info.traits.contains(&Trait::CanExecute));

        // Verify profile is in profile store.
        let profile = bridge.profile_store.get(&nid).unwrap();
        assert_eq!(profile.node_type, NodeType::Browser);
    }

    #[test]
    fn browser_list_response_serialization() {
        let resp = BrowserListResponse {
            browsers: vec![BrowserSummary {
                node_id: NodeId::new(),
                cores: 4,
                capabilities: vec!["js".to_string()],
                user_agent: "test".to_string(),
                tasks_completed: 10,
                tasks_failed: 1,
                tasks_active: 2,
                uptime_secs: 300,
                load: 0.5,
            }],
            aggregate: BrowserAggregateStats {
                connected_browsers: 1,
                total_cores: 4,
                total_tasks_completed: 10,
                total_tasks_dispatched: 13,
                tasks_per_second: 0.033,
                active_jobs: 2,
            },
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("browsers"));
        assert!(json.contains("aggregate"));
    }
}
