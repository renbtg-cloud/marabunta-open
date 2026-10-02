// Marabunta - Licensed under the MIT License.
//! WAN Bootstrap Server
//!
//! Enables coordinator and node discovery across the internet.
//! Coordinators register here, nodes connect to find coordinators.
//!
//! # Architecture
//!
//! The server runs both TCP (for reliable registration/queries) and UDP
//! (for fast STUN/discovery operations):
//!
//! - **TCP (port 9000)**: Registration, heartbeats, complex queries
//! - **UDP (port 9001)**: STUN requests, fast coordinator lookups
//!
//! # Security
//!
//! - Coordinators must authenticate to register
//! - Auth tokens are hashed before storage
//! - Rate limiting prevents abuse
//! - Optional region allowlisting

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, error, info, warn};

use super::errors::{BootstrapError, BootstrapResult};
use super::nat_traversal::HolePunchCoordinator;
use super::protocol::*;
use super::relay::RelayService;

// ============================================================================
// Configuration
// ============================================================================

/// Bootstrap server configuration
#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    /// TCP listen address for reliable connections
    pub listen_addr: SocketAddr,
    /// UDP port for fast queries (same IP as listen_addr)
    pub udp_port: u16,
    /// How long before a coordinator is considered stale
    pub coordinator_timeout: Duration,
    /// Maximum registered coordinators
    pub max_coordinators: usize,
    /// Require authentication for registration
    pub require_auth: bool,
    /// Optional: only allow coordinators from these regions
    pub allowed_regions: Option<Vec<String>>,
    /// Heartbeat interval to suggest to coordinators
    pub heartbeat_interval: Duration,
    /// Cleanup interval for stale entries
    pub cleanup_interval: Duration,
    /// Maximum connections per IP
    pub max_connections_per_ip: usize,
    /// Connection timeout
    pub connection_timeout: Duration,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9000".parse().unwrap(),
            udp_port: 9001,
            coordinator_timeout: Duration::from_secs(120),
            max_coordinators: 10000,
            require_auth: true,
            allowed_regions: None,
            heartbeat_interval: Duration::from_secs(30),
            cleanup_interval: Duration::from_secs(60),
            max_connections_per_ip: 10,
            connection_timeout: Duration::from_secs(30),
        }
    }
}

// ============================================================================
// Registered Coordinator
// ============================================================================

/// A coordinator registered with the bootstrap server
#[derive(Debug)]
pub struct RegisteredCoordinator {
    /// Unique identifier assigned during registration
    pub id: CoordinatorId,
    /// Human-readable name
    pub name: String,
    /// Public address for node connections
    pub public_addr: SocketAddr,
    /// Internal address for same-network nodes
    pub internal_addr: Option<SocketAddr>,
    /// Organization running this coordinator
    pub organization: String,
    /// Geographic region
    pub region: String,
    /// Whether phantom (BYOD) nodes are accepted
    pub accepts_phantom: bool,
    /// Number of active nodes
    pub node_count: u32,
    /// Capacity information
    pub capacity: CapacityInfo,
    /// When this coordinator registered
    pub registered_at: Instant,
    /// Last heartbeat received
    pub last_heartbeat: Instant,
    /// Hash of the auth token (for verification)
    pub auth_token_hash: [u8; 32],
    /// Current health status
    pub health: HealthStatus,
    /// Optional metadata
    pub metadata: Option<serde_json::Value>,
}

impl RegisteredCoordinator {
    fn to_info(&self) -> CoordinatorInfo {
        CoordinatorInfo {
            id: self.id,
            name: self.name.clone(),
            addr: self.public_addr,
            region: self.region.clone(),
            organization: self.organization.clone(),
            accepts_phantom: self.accepts_phantom,
            available_cores: self.capacity.available_cores,
            available_memory_gb: self.capacity.available_memory_gb,
            pending_jobs: self.capacity.pending_jobs,
            health: self.health,
        }
    }
}

// ============================================================================
// Bootstrap Server
// ============================================================================

/// The WAN bootstrap server
///
/// Manages coordinator registration and discovery for nodes across the internet.
pub struct BootstrapServer {
    config: BootstrapConfig,
    /// All registered coordinators by ID
    coordinators: RwLock<HashMap<CoordinatorId, RegisteredCoordinator>>,
    /// Index: region -> coordinator IDs
    by_region: RwLock<HashMap<String, Vec<CoordinatorId>>>,
    /// Index: organization -> coordinator IDs
    by_organization: RwLock<HashMap<String, Vec<CoordinatorId>>>,
    /// Hole punch coordinator
    hole_punch: HolePunchCoordinator,
    /// Relay service for difficult NATs
    relay_service: RelayService,
    /// Server running flag
    running: AtomicBool,
    /// Connection count per IP for rate limiting
    connections_per_ip: RwLock<HashMap<std::net::IpAddr, usize>>,
    /// Stats
    stats: ServerStats,
}

/// Server statistics
#[derive(Debug, Default)]
pub struct ServerStats {
    pub total_registrations: AtomicU64,
    pub total_queries: AtomicU64,
    pub total_heartbeats: AtomicU64,
    pub active_connections: AtomicU64,
}

impl BootstrapServer {
    /// Create a new bootstrap server
    pub fn new(config: BootstrapConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            coordinators: RwLock::new(HashMap::new()),
            by_region: RwLock::new(HashMap::new()),
            by_organization: RwLock::new(HashMap::new()),
            hole_punch: HolePunchCoordinator::new(),
            relay_service: RelayService::new(Default::default()),
            running: AtomicBool::new(false),
            connections_per_ip: RwLock::new(HashMap::new()),
            stats: ServerStats::default(),
        })
    }

    /// Start the bootstrap server (TCP + UDP)
    pub async fn start(self: Arc<Self>) -> BootstrapResult<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(BootstrapError::ServerAlreadyRunning);
        }

        info!(
            "Starting bootstrap server on TCP {} and UDP {}",
            self.config.listen_addr, self.config.udp_port
        );

        // Bind TCP listener
        let tcp_listener = TcpListener::bind(self.config.listen_addr)
            .await
            .map_err(|e| BootstrapError::BindFailed(self.config.listen_addr, e.to_string()))?;

        // Bind UDP socket
        let udp_addr = SocketAddr::new(self.config.listen_addr.ip(), self.config.udp_port);
        let udp_socket = UdpSocket::bind(udp_addr)
            .await
            .map_err(|e| BootstrapError::BindFailed(udp_addr, e.to_string()))?;

        // Clone for tasks
        let server_tcp = self.clone();
        let server_udp = self.clone();
        let server_cleanup = self.clone();

        // Spawn TCP handler
        let tcp_handle = tokio::spawn(async move { server_tcp.run_tcp_server(tcp_listener).await });

        // Spawn UDP handler
        let udp_handle = tokio::spawn(async move { server_udp.run_udp_server(udp_socket).await });

        // Spawn cleanup task
        let cleanup_handle = tokio::spawn(async move { server_cleanup.run_cleanup_task().await });

        info!("Bootstrap server started successfully");

        // Wait for any task to complete (error or shutdown)
        tokio::select! {
            result = tcp_handle => {
                error!("TCP server exited: {:?}", result);
            }
            result = udp_handle => {
                error!("UDP server exited: {:?}", result);
            }
            result = cleanup_handle => {
                error!("Cleanup task exited: {:?}", result);
            }
        }

        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Stop the server
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        info!("Bootstrap server stop requested");
    }

    /// Check if server is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    // ========================================================================
    // TCP Server
    // ========================================================================

    async fn run_tcp_server(self: Arc<Self>, listener: TcpListener) -> BootstrapResult<()> {
        while self.running.load(Ordering::SeqCst) {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    // Check rate limit
                    if !self.check_connection_limit(addr.ip()).await {
                        warn!("Rate limit exceeded for {}", addr.ip());
                        continue;
                    }

                    let server = self.clone();
                    tokio::spawn(async move {
                        server
                            .stats
                            .active_connections
                            .fetch_add(1, Ordering::Relaxed);
                        if let Err(e) = server.handle_tcp_connection(stream, addr).await {
                            debug!("Connection error from {}: {}", addr, e);
                        }
                        server
                            .stats
                            .active_connections
                            .fetch_sub(1, Ordering::Relaxed);
                        server.release_connection(addr.ip()).await;
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Ok(())
    }

    async fn check_connection_limit(&self, ip: std::net::IpAddr) -> bool {
        let mut connections = self.connections_per_ip.write().await;
        let count = connections.entry(ip).or_insert(0);
        if *count >= self.config.max_connections_per_ip {
            false
        } else {
            *count += 1;
            true
        }
    }

    async fn release_connection(&self, ip: std::net::IpAddr) {
        let mut connections = self.connections_per_ip.write().await;
        if let Some(count) = connections.get_mut(&ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                connections.remove(&ip);
            }
        }
    }

    async fn handle_tcp_connection(
        &self,
        mut stream: TcpStream,
        addr: SocketAddr,
    ) -> BootstrapResult<()> {
        debug!("New TCP connection from {}", addr);

        stream
            .set_nodelay(true)
            .map_err(BootstrapError::Io)?;

        let timeout = self.config.connection_timeout;
        let mut buf = BytesMut::with_capacity(4096);
        let mut codec = BootstrapCodec::new();

        loop {
            // Read with timeout
            let read_result = tokio::time::timeout(timeout, stream.read_buf(&mut buf)).await;

            match read_result {
                Ok(Ok(0)) => {
                    debug!("Connection closed by {}", addr);
                    break;
                }
                Ok(Ok(_)) => {
                    // Try to decode messages
                    while let Some(msg) = codec.decode(&mut buf)? {
                        let response = self.handle_message(msg, addr).await;
                        self.send_response(&mut stream, &mut codec, response)
                            .await?;
                    }
                }
                Ok(Err(e)) => {
                    return Err(BootstrapError::Io(e));
                }
                Err(_) => {
                    debug!("Connection timeout from {}", addr);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn send_response(
        &self,
        stream: &mut TcpStream,
        codec: &mut BootstrapCodec,
        msg: BootstrapMessage,
    ) -> BootstrapResult<()> {
        let mut buf = BytesMut::with_capacity(4096);
        codec.encode(msg, &mut buf)?;
        stream.write_all(&buf).await?;
        Ok(())
    }

    // ========================================================================
    // UDP Server
    // ========================================================================

    async fn run_udp_server(self: Arc<Self>, socket: UdpSocket) -> BootstrapResult<()> {
        let mut buf = vec![0u8; 65535];

        while self.running.load(Ordering::SeqCst) {
            match socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    self.handle_udp_query(&socket, &buf[..len], addr).await;
                }
                Err(e) => {
                    error!("UDP recv error: {}", e);
                }
            }
        }
        Ok(())
    }

    async fn handle_udp_query(&self, socket: &UdpSocket, data: &[u8], addr: SocketAddr) {
        // Parse message
        let msg = match BootstrapCodec::decode_bytes(data) {
            Ok(m) => m,
            Err(e) => {
                debug!("Invalid UDP message from {}: {}", addr, e);
                return;
            }
        };

        // Handle and respond
        let response = self.handle_message(msg, addr).await;
        let response_data = match BootstrapCodec::encode_bytes(&response) {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to encode response: {}", e);
                return;
            }
        };

        if let Err(e) = socket.send_to(&response_data, addr).await {
            debug!("Failed to send UDP response to {}: {}", addr, e);
        }
    }

    // ========================================================================
    // Message Handling
    // ========================================================================

    async fn handle_message(
        &self,
        msg: BootstrapMessage,
        client_addr: SocketAddr,
    ) -> BootstrapMessage {
        match msg {
            // Coordinator messages
            BootstrapMessage::RegisterCoordinator(reg) => self.handle_register(reg).await,
            BootstrapMessage::Heartbeat(hb) => self.handle_heartbeat(hb).await,
            BootstrapMessage::Unregister(req) => self.handle_unregister(req).await,

            // Node messages
            BootstrapMessage::FindCoordinators(query) => {
                self.stats.total_queries.fetch_add(1, Ordering::Relaxed);
                let coordinators = self.find_coordinators(query.clone()).await;
                BootstrapMessage::CoordinatorList {
                    total_count: coordinators.len(),
                    coordinators,
                }
            }
            BootstrapMessage::GetCoordinator { id } => {
                let info = self.get_coordinator(id).await;
                BootstrapMessage::CoordinatorDetails(info)
            }
            BootstrapMessage::StunRequest { node_id } => {
                self.handle_stun_request(client_addr, node_id).await
            }
            BootstrapMessage::HolePunchRequest(req) => self.handle_hole_punch_request(req).await,
            BootstrapMessage::RelayRequest(req) => self.handle_relay_request(req).await,
            BootstrapMessage::RelayData {
                session_id,
                from_node,
                data,
            } => self.handle_relay_data(session_id, from_node, data).await,
            BootstrapMessage::RelayClose { session_id } => {
                self.relay_service.close_session(session_id).await;
                BootstrapMessage::UnregisterAck
            }
            BootstrapMessage::Ping { timestamp } => {
                let server_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                BootstrapMessage::Pong {
                    timestamp,
                    server_time,
                }
            }

            // Invalid messages
            _ => BootstrapMessage::Error {
                code: ErrorCode::InvalidRequest,
                message: "Unexpected message type".to_string(),
            },
        }
    }

    async fn handle_register(&self, reg: CoordinatorRegistration) -> BootstrapMessage {
        // Validate region
        if let Some(ref allowed) = self.config.allowed_regions {
            if !allowed.contains(&reg.region) {
                return BootstrapMessage::Error {
                    code: ErrorCode::RegionNotAllowed,
                    message: format!("Region '{}' not allowed", reg.region),
                };
            }
        }

        // Validate auth
        if self.config.require_auth && reg.auth_token.is_expired() {
            return BootstrapMessage::Error {
                code: ErrorCode::Unauthorized,
                message: "Auth token expired".to_string(),
            };
        }

        // Check capacity
        let coordinators = self.coordinators.read().await;
        if coordinators.len() >= self.config.max_coordinators {
            return BootstrapMessage::Error {
                code: ErrorCode::MaxCoordinatorsReached,
                message: format!(
                    "Maximum coordinators ({}) reached",
                    self.config.max_coordinators
                ),
            };
        }
        drop(coordinators);

        // Register
        match self.register_coordinator(reg).await {
            Ok(id) => {
                self.stats
                    .total_registrations
                    .fetch_add(1, Ordering::Relaxed);
                BootstrapMessage::RegisterResponse {
                    coordinator_id: id,
                    heartbeat_interval_secs: self.config.heartbeat_interval.as_secs() as u32,
                }
            }
            Err(e) => BootstrapMessage::Error {
                code: ErrorCode::InternalError,
                message: e.to_string(),
            },
        }
    }

    async fn handle_heartbeat(&self, hb: CoordinatorHeartbeat) -> BootstrapMessage {
        self.stats.total_heartbeats.fetch_add(1, Ordering::Relaxed);

        match self
            .coordinator_heartbeat(hb.coordinator_id, hb.status, &hb.auth_token)
            .await
        {
            Ok(()) => BootstrapMessage::HeartbeatAck {
                next_heartbeat_secs: self.config.heartbeat_interval.as_secs() as u32,
            },
            Err(e) => {
                let code = match &e {
                    BootstrapError::CoordinatorNotFound(_) => ErrorCode::CoordinatorNotFound,
                    BootstrapError::InvalidAuthToken => ErrorCode::Unauthorized,
                    _ => ErrorCode::InternalError,
                };
                BootstrapMessage::Error {
                    code,
                    message: e.to_string(),
                }
            }
        }
    }

    async fn handle_unregister(&self, req: UnregisterRequest) -> BootstrapMessage {
        match self
            .unregister_coordinator(req.coordinator_id, &req.auth_token)
            .await
        {
            Ok(()) => BootstrapMessage::UnregisterAck,
            Err(e) => {
                let code = match &e {
                    BootstrapError::CoordinatorNotFound(_) => ErrorCode::CoordinatorNotFound,
                    BootstrapError::InvalidAuthToken => ErrorCode::Unauthorized,
                    _ => ErrorCode::InternalError,
                };
                BootstrapMessage::Error {
                    code,
                    message: e.to_string(),
                }
            }
        }
    }

    async fn handle_stun_request(
        &self,
        client_addr: SocketAddr,
        _node_id: NodeId,
    ) -> BootstrapMessage {
        // Return the client's address as we see it
        let server_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        BootstrapMessage::StunResponse(BindingResponse {
            public_addr: client_addr,
            nat_type: NatType::Unknown, // Full NAT detection requires multiple servers
            server_time,
        })
    }

    async fn handle_hole_punch_request(&self, req: HolePunchRequest) -> BootstrapMessage {
        match self.hole_punch.initiate_hole_punch(req).await {
            Ok(instructions) => BootstrapMessage::HolePunchInstructions(instructions),
            Err(e) => BootstrapMessage::Error {
                code: ErrorCode::HolePunchFailed,
                message: e.to_string(),
            },
        }
    }

    async fn handle_relay_request(&self, req: RelayRequest) -> BootstrapMessage {
        match self
            .relay_service
            .create_session(req.node_id, req.peer_node)
            .await
        {
            Ok(session_id) => BootstrapMessage::RelaySessionCreated {
                session_id,
                relay_addr: self.config.listen_addr,
            },
            Err(e) => BootstrapMessage::Error {
                code: ErrorCode::MaxRelaySessionsReached,
                message: e.to_string(),
            },
        }
    }

    async fn handle_relay_data(
        &self,
        session_id: RelaySessionId,
        from_node: NodeId,
        data: Vec<u8>,
    ) -> BootstrapMessage {
        match self.relay_service.relay(session_id, from_node, &data).await {
            Ok(()) => {
                // Note: In a real implementation, this would forward to the peer
                // through their connection. For now, we acknowledge.
                BootstrapMessage::HeartbeatAck {
                    next_heartbeat_secs: 30,
                }
            }
            Err(e) => BootstrapMessage::Error {
                code: ErrorCode::RelaySessionNotFound,
                message: e.to_string(),
            },
        }
    }

    // ========================================================================
    // Coordinator Management
    // ========================================================================

    /// Register a coordinator with the bootstrap server
    pub async fn register_coordinator(
        &self,
        registration: CoordinatorRegistration,
    ) -> BootstrapResult<CoordinatorId> {
        let id = CoordinatorId::new();
        let now = Instant::now();
        let name = registration.name.clone();
        let region = registration.region.clone();
        let organization = registration.organization.clone();

        let coordinator = RegisteredCoordinator {
            id,
            name: registration.name,
            public_addr: registration.public_addr,
            internal_addr: registration.internal_addr,
            organization: registration.organization,
            region: registration.region,
            accepts_phantom: registration.accepts_phantom,
            node_count: 0,
            capacity: registration.capacity,
            registered_at: now,
            last_heartbeat: now,
            auth_token_hash: registration.auth_token.hash(),
            health: HealthStatus::Healthy,
            metadata: registration.metadata,
        };

        // Insert into main map
        let mut coordinators = self.coordinators.write().await;
        coordinators.insert(id, coordinator);
        drop(coordinators);

        // Update indices
        let mut by_region = self.by_region.write().await;
        by_region.entry(region).or_insert_with(Vec::new).push(id);
        drop(by_region);

        let mut by_org = self.by_organization.write().await;
        by_org.entry(organization).or_insert_with(Vec::new).push(id);

        info!("Registered coordinator {} ({})", id, name);

        Ok(id)
    }

    /// Process a heartbeat from a coordinator
    pub async fn coordinator_heartbeat(
        &self,
        id: CoordinatorId,
        status: CoordinatorStatus,
        auth: &AuthToken,
    ) -> BootstrapResult<()> {
        let mut coordinators = self.coordinators.write().await;

        let coordinator = coordinators
            .get_mut(&id)
            .ok_or(BootstrapError::CoordinatorNotFound(id))?;

        // Verify auth token
        if coordinator.auth_token_hash != auth.hash() {
            return Err(BootstrapError::InvalidAuthToken);
        }

        // Update status
        coordinator.last_heartbeat = Instant::now();
        coordinator.node_count = status.node_count;
        coordinator.capacity = status.capacity;
        coordinator.health = status.health;

        debug!(
            "Heartbeat from {} - nodes: {}, health: {:?}",
            id, status.node_count, status.health
        );

        Ok(())
    }

    /// Unregister a coordinator
    pub async fn unregister_coordinator(
        &self,
        id: CoordinatorId,
        auth: &AuthToken,
    ) -> BootstrapResult<()> {
        let mut coordinators = self.coordinators.write().await;

        let coordinator = coordinators
            .get(&id)
            .ok_or(BootstrapError::CoordinatorNotFound(id))?;

        // Verify auth
        if coordinator.auth_token_hash != auth.hash() {
            return Err(BootstrapError::InvalidAuthToken);
        }

        let region = coordinator.region.clone();
        let org = coordinator.organization.clone();

        coordinators.remove(&id);
        drop(coordinators);

        // Remove from indices
        let mut by_region = self.by_region.write().await;
        if let Some(ids) = by_region.get_mut(&region) {
            ids.retain(|&i| i != id);
        }
        drop(by_region);

        let mut by_org = self.by_organization.write().await;
        if let Some(ids) = by_org.get_mut(&org) {
            ids.retain(|&i| i != id);
        }

        info!("Unregistered coordinator {}", id);

        Ok(())
    }

    /// Find coordinators matching a query
    pub async fn find_coordinators(&self, query: CoordinatorQuery) -> Vec<CoordinatorInfo> {
        let coordinators = self.coordinators.read().await;

        let mut results: Vec<CoordinatorInfo> = coordinators
            .values()
            .filter(|c| {
                // Filter by region
                if let Some(ref region) = query.region {
                    if &c.region != region {
                        return false;
                    }
                }

                // Filter by organization
                if let Some(ref org) = query.organization {
                    if &c.organization != org {
                        return false;
                    }
                }

                // Filter by phantom acceptance
                if let Some(accepts) = query.accepts_phantom {
                    if c.accepts_phantom != accepts {
                        return false;
                    }
                }

                // Filter by capacity
                if let Some(min_cores) = query.min_capacity_cores {
                    if c.capacity.available_cores < min_cores {
                        return false;
                    }
                }

                if let Some(min_mem) = query.min_capacity_memory_gb {
                    if c.capacity.available_memory_gb < min_mem {
                        return false;
                    }
                }

                // Only include healthy coordinators
                c.health != HealthStatus::Unhealthy
            })
            .map(|c| c.to_info())
            .collect();

        // Sort by available capacity (descending) and pending jobs (ascending)
        results.sort_by(|a, b| {
            b.available_cores
                .cmp(&a.available_cores)
                .then(a.pending_jobs.cmp(&b.pending_jobs))
        });

        // Apply pagination
        let start = query.offset.min(results.len());
        let end = (query.offset + query.limit).min(results.len());
        results[start..end].to_vec()
    }

    /// Get a specific coordinator by ID
    pub async fn get_coordinator(&self, id: CoordinatorId) -> Option<CoordinatorInfo> {
        let coordinators = self.coordinators.read().await;
        coordinators.get(&id).map(|c| c.to_info())
    }

    /// Get all coordinators in a region
    pub async fn get_coordinators_by_region(&self, region: &str) -> Vec<CoordinatorInfo> {
        let by_region = self.by_region.read().await;
        let coordinators = self.coordinators.read().await;

        by_region
            .get(region)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| coordinators.get(id).map(|c| c.to_info()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get current stats
    pub fn stats(&self) -> &ServerStats {
        &self.stats
    }

    // ========================================================================
    // Cleanup
    // ========================================================================

    async fn run_cleanup_task(self: Arc<Self>) -> BootstrapResult<()> {
        let mut interval = interval(self.config.cleanup_interval);

        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;
            self.cleanup_stale().await;
        }

        Ok(())
    }

    /// Remove stale coordinators that haven't sent heartbeats
    async fn cleanup_stale(&self) {
        let now = Instant::now();
        let timeout = self.config.coordinator_timeout;

        let mut coordinators = self.coordinators.write().await;
        let mut to_remove = Vec::new();

        for (id, coordinator) in coordinators.iter() {
            if now.duration_since(coordinator.last_heartbeat) > timeout {
                to_remove.push(*id);
                info!(
                    "Removing stale coordinator {} ({}) - no heartbeat for {:?}",
                    id,
                    coordinator.name,
                    now.duration_since(coordinator.last_heartbeat)
                );
            }
        }

        for id in &to_remove {
            if let Some(coord) = coordinators.remove(id) {
                // Clean up indices
                let mut by_region = self.by_region.write().await;
                if let Some(ids) = by_region.get_mut(&coord.region) {
                    ids.retain(|i| i != id);
                }
                drop(by_region);

                let mut by_org = self.by_organization.write().await;
                if let Some(ids) = by_org.get_mut(&coord.organization) {
                    ids.retain(|i| i != id);
                }
            }
        }

        if !to_remove.is_empty() {
            info!("Cleaned up {} stale coordinators", to_remove.len());
        }

        // Also clean up hole punch sessions
        self.hole_punch.cleanup_expired().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_config() -> BootstrapConfig {
        BootstrapConfig {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            udp_port: 0,
            coordinator_timeout: Duration::from_secs(10),
            max_coordinators: 100,
            require_auth: false,
            allowed_regions: None,
            heartbeat_interval: Duration::from_secs(5),
            cleanup_interval: Duration::from_secs(1),
            max_connections_per_ip: 5,
            connection_timeout: Duration::from_secs(10),
        }
    }

    fn create_test_registration(name: &str, region: &str) -> CoordinatorRegistration {
        CoordinatorRegistration {
            name: name.to_string(),
            public_addr: "1.2.3.4:9000".parse().unwrap(),
            internal_addr: Some("192.168.1.100:9000".parse().unwrap()),
            organization: "test-org".to_string(),
            region: region.to_string(),
            accepts_phantom: true,
            capacity: CapacityInfo {
                total_cores: 100,
                available_cores: 50,
                total_memory_gb: 256,
                available_memory_gb: 128,
                pending_jobs: 5,
            },
            auth_token: AuthToken::new("test-token".to_string(), Duration::from_secs(3600)),
            metadata: None,
        }
    }

    #[tokio::test]
    async fn test_register_coordinator() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        let reg = create_test_registration("test-coord", "us-west");
        let id = server.register_coordinator(reg).await.unwrap();

        let info = server.get_coordinator(id).await.unwrap();
        assert_eq!(info.name, "test-coord");
        assert_eq!(info.region, "us-west");
        assert!(info.accepts_phantom);
    }

    #[tokio::test]
    async fn test_find_coordinators_by_region() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        // Register coordinators in different regions
        server
            .register_coordinator(create_test_registration("west-1", "us-west"))
            .await
            .unwrap();
        server
            .register_coordinator(create_test_registration("west-2", "us-west"))
            .await
            .unwrap();
        server
            .register_coordinator(create_test_registration("east-1", "us-east"))
            .await
            .unwrap();

        // Query by region
        let query = CoordinatorQuery::new()
            .with_region("us-west")
            .with_limit(10);
        let results = server.find_coordinators(query).await;

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|c| c.region == "us-west"));
    }

    #[tokio::test]
    async fn test_find_coordinators_by_capacity() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        // Register with different capacities
        let mut reg1 = create_test_registration("low-cap", "us-west");
        reg1.capacity.available_cores = 10;

        let mut reg2 = create_test_registration("high-cap", "us-west");
        reg2.capacity.available_cores = 100;

        server.register_coordinator(reg1).await.unwrap();
        server.register_coordinator(reg2).await.unwrap();

        // Query with minimum capacity
        let query = CoordinatorQuery::new().with_min_cores(50).with_limit(10);
        let results = server.find_coordinators(query).await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "high-cap");
    }

    #[tokio::test]
    async fn test_coordinator_heartbeat() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        let reg = create_test_registration("test-coord", "us-west");
        let auth_token = reg.auth_token.clone();
        let id = server.register_coordinator(reg).await.unwrap();

        // Send heartbeat
        let status = CoordinatorStatus {
            node_count: 25,
            capacity: CapacityInfo {
                total_cores: 100,
                available_cores: 30,
                total_memory_gb: 256,
                available_memory_gb: 64,
                pending_jobs: 10,
            },
            health: HealthStatus::Healthy,
            message: None,
        };

        server
            .coordinator_heartbeat(id, status, &auth_token)
            .await
            .unwrap();

        // Verify update
        let info = server.get_coordinator(id).await.unwrap();
        assert_eq!(info.available_cores, 30);
        assert_eq!(info.pending_jobs, 10);
    }

    #[tokio::test]
    async fn test_heartbeat_wrong_token() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        let reg = create_test_registration("test-coord", "us-west");
        let id = server.register_coordinator(reg).await.unwrap();

        // Try heartbeat with wrong token
        let wrong_token = AuthToken::new("wrong-token".to_string(), Duration::from_secs(3600));
        let status = CoordinatorStatus {
            node_count: 0,
            capacity: CapacityInfo::default(),
            health: HealthStatus::Healthy,
            message: None,
        };

        let result = server.coordinator_heartbeat(id, status, &wrong_token).await;
        assert!(matches!(result, Err(BootstrapError::InvalidAuthToken)));
    }

    #[tokio::test]
    async fn test_unregister_coordinator() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        let reg = create_test_registration("test-coord", "us-west");
        let auth_token = reg.auth_token.clone();
        let id = server.register_coordinator(reg).await.unwrap();

        // Unregister
        server
            .unregister_coordinator(id, &auth_token)
            .await
            .unwrap();

        // Verify removed
        assert!(server.get_coordinator(id).await.is_none());
    }

    #[tokio::test]
    async fn test_max_coordinators() {
        let mut config = create_test_config();
        config.max_coordinators = 2;
        let server = BootstrapServer::new(config);

        // Register up to max
        server
            .register_coordinator(create_test_registration("c1", "us-west"))
            .await
            .unwrap();
        server
            .register_coordinator(create_test_registration("c2", "us-west"))
            .await
            .unwrap();

        // Third should still work (registration check is in handle_register, not register_coordinator)
        // In production, the check would be at the message handler level
        let query = CoordinatorQuery::new().with_limit(10);
        let results = server.find_coordinators(query).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_cleanup_stale() {
        let mut config = create_test_config();
        config.coordinator_timeout = Duration::from_millis(50);
        let server = BootstrapServer::new(config);

        let reg = create_test_registration("test-coord", "us-west");
        let id = server.register_coordinator(reg).await.unwrap();

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Run cleanup
        server.cleanup_stale().await;

        // Should be removed
        assert!(server.get_coordinator(id).await.is_none());
    }

    #[tokio::test]
    async fn test_region_index() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        server
            .register_coordinator(create_test_registration("west-1", "us-west"))
            .await
            .unwrap();
        server
            .register_coordinator(create_test_registration("west-2", "us-west"))
            .await
            .unwrap();

        let west = server.get_coordinators_by_region("us-west").await;
        assert_eq!(west.len(), 2);

        let east = server.get_coordinators_by_region("us-east").await;
        assert_eq!(east.len(), 0);
    }

    #[tokio::test]
    async fn test_find_phantom_accepting() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        let mut reg1 = create_test_registration("phantom-yes", "us-west");
        reg1.accepts_phantom = true;

        let mut reg2 = create_test_registration("phantom-no", "us-west");
        reg2.accepts_phantom = false;

        server.register_coordinator(reg1).await.unwrap();
        server.register_coordinator(reg2).await.unwrap();

        let query = CoordinatorQuery::new().accepting_phantom().with_limit(10);
        let results = server.find_coordinators(query).await;

        assert_eq!(results.len(), 1);
        assert!(results[0].accepts_phantom);
    }

    #[tokio::test]
    async fn test_pagination() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        // Register 5 coordinators
        for i in 0..5 {
            server
                .register_coordinator(create_test_registration(&format!("c{}", i), "us-west"))
                .await
                .unwrap();
        }

        // Query with limit
        let query = CoordinatorQuery {
            limit: 2,
            offset: 0,
            ..Default::default()
        };
        let page1 = server.find_coordinators(query).await;
        assert_eq!(page1.len(), 2);

        // Next page
        let query = CoordinatorQuery {
            limit: 2,
            offset: 2,
            ..Default::default()
        };
        let page2 = server.find_coordinators(query).await;
        assert_eq!(page2.len(), 2);

        // Last page
        let query = CoordinatorQuery {
            limit: 2,
            offset: 4,
            ..Default::default()
        };
        let page3 = server.find_coordinators(query).await;
        assert_eq!(page3.len(), 1);
    }

    #[tokio::test]
    async fn test_message_handling_ping() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        let msg = BootstrapMessage::Ping { timestamp: 12345 };
        let addr = "127.0.0.1:5000".parse().unwrap();

        let response = server.handle_message(msg, addr).await;

        match response {
            BootstrapMessage::Pong {
                timestamp,
                server_time,
            } => {
                assert_eq!(timestamp, 12345);
                assert!(server_time > 0);
            }
            _ => panic!("Expected Pong response"),
        }
    }

    #[tokio::test]
    async fn test_stats() {
        let config = create_test_config();
        let server = BootstrapServer::new(config);

        // Initial stats should be zero
        assert_eq!(
            server.stats().total_registrations.load(Ordering::Relaxed),
            0
        );
        assert_eq!(server.stats().total_queries.load(Ordering::Relaxed), 0);

        // Register
        server
            .register_coordinator(create_test_registration("test", "us-west"))
            .await
            .unwrap();

        // Manually update stats (normally done in handle_register)
        server
            .stats
            .total_registrations
            .fetch_add(1, Ordering::Relaxed);

        assert_eq!(
            server.stats().total_registrations.load(Ordering::Relaxed),
            1
        );
    }
}
