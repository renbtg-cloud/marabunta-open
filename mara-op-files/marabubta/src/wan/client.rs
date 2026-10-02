// Marabunta - Licensed under the MIT License.
//! Bootstrap client for nodes and coordinators
//!
//! Provides a client library for connecting to the bootstrap server.
//! Coordinators use this to register and maintain their presence.
//! Nodes use this to discover coordinators and perform NAT traversal.
//!
//! # Example: Coordinator Registration
//!
//! ```rust,no_run
//! use marabunta_compute::wan::{BootstrapClient, CoordinatorRegistration, AuthToken, CapacityInfo};
//! use std::time::Duration;
//!
//! async fn register_coordinator() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = BootstrapClient::new(vec!["bootstrap.example.com:9000".parse()?]);
//!     client.connect().await?;
//!
//!     let registration = CoordinatorRegistration {
//!         name: "my-coordinator".to_string(),
//!         public_addr: "1.2.3.4:9000".parse()?,
//!         internal_addr: None,
//!         organization: "my-org".to_string(),
//!         region: "us-west".to_string(),
//!         accepts_phantom: true,
//!         capacity: CapacityInfo::default(),
//!         auth_token: AuthToken::new("secret".to_string(), Duration::from_secs(3600)),
//!         metadata: None,
//!     };
//!
//!     let id = client.register_coordinator(registration).await?;
//!     println!("Registered as {}", id);
//!     Ok(())
//! }
//! ```
//!
//! # Example: Node Discovery
//!
//! ```rust,no_run
//! use marabunta_compute::wan::{BootstrapClient, CoordinatorQuery};
//!
//! async fn find_coordinators() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = BootstrapClient::new(vec!["bootstrap.example.com:9000".parse()?]);
//!     client.connect().await?;
//!
//!     let query = CoordinatorQuery::new()
//!         .with_region("us-west")
//!         .accepting_phantom()
//!         .with_limit(10);
//!
//!     let coordinators = client.find_coordinators(query).await?;
//!     for coord in coordinators {
//!         println!("{}: {} ({} cores available)", coord.id, coord.name, coord.available_cores);
//!     }
//!     Ok(())
//! }
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, error, info, warn};

use super::errors::{BootstrapError, BootstrapResult};
use super::protocol::*;

// ============================================================================
// Client Configuration
// ============================================================================

/// Configuration for the bootstrap client
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Retry count for failed requests
    pub max_retries: u32,
    /// Delay between retries
    pub retry_delay: Duration,
    /// Heartbeat interval (for coordinators)
    pub heartbeat_interval: Duration,
    /// Use UDP for simple queries (faster)
    pub prefer_udp: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_delay: Duration::from_millis(500),
            heartbeat_interval: Duration::from_secs(30),
            prefer_udp: true,
        }
    }
}

// ============================================================================
// Connection State
// ============================================================================

/// State of the TCP connection
struct TcpConnection {
    stream: TcpStream,
    read_buf: BytesMut,
    codec: BootstrapCodec,
}

impl TcpConnection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(4096),
            codec: BootstrapCodec::new(),
        }
    }

    async fn send(&mut self, msg: BootstrapMessage) -> BootstrapResult<()> {
        let mut buf = BytesMut::with_capacity(4096);
        self.codec.encode(msg, &mut buf)?;
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    async fn recv(&mut self, timeout_duration: Duration) -> BootstrapResult<BootstrapMessage> {
        loop {
            // Try to decode from existing buffer
            if let Some(msg) = self.codec.decode(&mut self.read_buf)? {
                return Ok(msg);
            }

            // Read more data
            let n = timeout(timeout_duration, self.stream.read_buf(&mut self.read_buf))
                .await
                .map_err(|_| BootstrapError::ConnectionTimeout)?
                .map_err(BootstrapError::Io)?;

            if n == 0 {
                return Err(BootstrapError::ConnectionClosed);
            }
        }
    }

    async fn request(
        &mut self,
        msg: BootstrapMessage,
        timeout_duration: Duration,
    ) -> BootstrapResult<BootstrapMessage> {
        self.send(msg).await?;
        self.recv(timeout_duration).await
    }
}

// ============================================================================
// Bootstrap Client
// ============================================================================

/// Client for connecting to the bootstrap server
///
/// Supports both TCP (reliable) and UDP (fast queries) connections.
pub struct BootstrapClient {
    /// Bootstrap server addresses (multiple for redundancy)
    server_addrs: Vec<SocketAddr>,
    /// Client configuration
    config: ClientConfig,
    /// Active TCP connection
    tcp_connection: Mutex<Option<TcpConnection>>,
    /// UDP socket for fast queries
    udp_socket: Mutex<Option<UdpSocket>>,
    /// Current server index (for round-robin)
    current_server: AtomicU64,
    /// Coordinator ID (after registration)
    coordinator_id: RwLock<Option<CoordinatorId>>,
    /// Auth token (for coordinators)
    auth_token: RwLock<Option<AuthToken>>,
    /// Node ID (for nodes)
    node_id: NodeId,
    /// Last known public address (from STUN)
    public_addr: RwLock<Option<SocketAddr>>,
}

impl BootstrapClient {
    /// Create a new bootstrap client
    pub fn new(server_addrs: Vec<SocketAddr>) -> Self {
        Self {
            server_addrs,
            config: ClientConfig::default(),
            tcp_connection: Mutex::new(None),
            udp_socket: Mutex::new(None),
            current_server: AtomicU64::new(0),
            coordinator_id: RwLock::new(None),
            auth_token: RwLock::new(None),
            node_id: NodeId::new(),
            public_addr: RwLock::new(None),
        }
    }

    /// Create with custom configuration
    pub fn with_config(server_addrs: Vec<SocketAddr>, config: ClientConfig) -> Self {
        Self {
            server_addrs,
            config,
            tcp_connection: Mutex::new(None),
            udp_socket: Mutex::new(None),
            current_server: AtomicU64::new(0),
            coordinator_id: RwLock::new(None),
            auth_token: RwLock::new(None),
            node_id: NodeId::new(),
            public_addr: RwLock::new(None),
        }
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the coordinator ID (if registered)
    pub async fn coordinator_id(&self) -> Option<CoordinatorId> {
        *self.coordinator_id.read().await
    }

    /// Get last known public address
    pub async fn public_addr(&self) -> Option<SocketAddr> {
        *self.public_addr.read().await
    }

    // ========================================================================
    // Connection Management
    // ========================================================================

    /// Connect to the bootstrap server (TCP)
    pub async fn connect(&self) -> BootstrapResult<()> {
        let mut connection = self.tcp_connection.lock().await;

        // Already connected?
        if connection.is_some() {
            return Ok(());
        }

        // Try servers in order
        for i in 0..self.server_addrs.len() {
            let idx = (self.current_server.load(Ordering::Relaxed) as usize + i)
                % self.server_addrs.len();
            let addr = self.server_addrs[idx];

            match timeout(self.config.connect_timeout, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    stream.set_nodelay(true)?;
                    *connection = Some(TcpConnection::new(stream));
                    self.current_server.store(idx as u64, Ordering::Relaxed);
                    info!("Connected to bootstrap server at {}", addr);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    warn!("Failed to connect to {}: {}", addr, e);
                }
                Err(_) => {
                    warn!("Connection timeout to {}", addr);
                }
            }
        }

        Err(BootstrapError::NoServersAvailable)
    }

    /// Disconnect from the server
    pub async fn disconnect(&self) {
        let mut connection = self.tcp_connection.lock().await;
        *connection = None;
        info!("Disconnected from bootstrap server");
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        self.tcp_connection.lock().await.is_some()
    }

    /// Initialize UDP socket for fast queries
    async fn ensure_udp_socket(&self) -> BootstrapResult<()> {
        let mut socket = self.udp_socket.lock().await;
        if socket.is_none() {
            *socket = Some(UdpSocket::bind("0.0.0.0:0").await?);
        }
        Ok(())
    }

    // ========================================================================
    // Request/Response
    // ========================================================================

    /// Send a request and get a response (TCP)
    async fn tcp_request(&self, msg: BootstrapMessage) -> BootstrapResult<BootstrapMessage> {
        let mut connection = self.tcp_connection.lock().await;
        let conn = connection
            .as_mut()
            .ok_or(BootstrapError::ConnectionClosed)?;

        conn.request(msg, self.config.request_timeout).await
    }

    /// Send a request and get a response (UDP)
    async fn udp_request(&self, msg: BootstrapMessage) -> BootstrapResult<BootstrapMessage> {
        self.ensure_udp_socket().await?;

        let socket = self.udp_socket.lock().await;
        let socket = socket.as_ref().unwrap();

        // Encode message
        let data = BootstrapCodec::encode_bytes(&msg)?;

        // Send to current server (UDP port is typically +1 from TCP)
        let idx = self.current_server.load(Ordering::Relaxed) as usize;
        let tcp_addr = self.server_addrs[idx];
        let udp_addr = SocketAddr::new(tcp_addr.ip(), tcp_addr.port() + 1);

        socket.send_to(&data, udp_addr).await?;

        // Wait for response
        let mut buf = vec![0u8; 65535];
        let n = timeout(self.config.request_timeout, socket.recv(&mut buf))
            .await
            .map_err(|_| BootstrapError::ConnectionTimeout)?
            .map_err(BootstrapError::Io)?;

        let response = BootstrapCodec::decode_bytes(&buf[..n])?;
        Ok(response)
    }

    /// Send a request with automatic retry
    async fn request_with_retry(&self, msg: BootstrapMessage) -> BootstrapResult<BootstrapMessage> {
        let mut last_error = BootstrapError::NoServersAvailable;

        for attempt in 0..self.config.max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_delay).await;
                // Try to reconnect
                self.disconnect().await;
                if let Err(e) = self.connect().await {
                    last_error = e;
                    continue;
                }
            }

            match self.tcp_request(msg.clone()).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    warn!("Request failed (attempt {}): {}", attempt + 1, e);
                    last_error = e;
                }
            }
        }

        Err(last_error)
    }

    // ========================================================================
    // Coordinator Operations
    // ========================================================================

    /// Register as a coordinator
    pub async fn register_coordinator(
        &self,
        registration: CoordinatorRegistration,
    ) -> BootstrapResult<CoordinatorId> {
        // Store auth token for heartbeats
        *self.auth_token.write().await = Some(registration.auth_token.clone());

        let response = self
            .request_with_retry(BootstrapMessage::RegisterCoordinator(registration))
            .await?;

        match response {
            BootstrapMessage::RegisterResponse {
                coordinator_id,
                heartbeat_interval_secs,
            } => {
                *self.coordinator_id.write().await = Some(coordinator_id);
                info!(
                    "Registered as coordinator {} (heartbeat every {}s)",
                    coordinator_id, heartbeat_interval_secs
                );
                Ok(coordinator_id)
            }
            BootstrapMessage::Error { code, message } => {
                error!("Registration failed: {:?} - {}", code, message);
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Send a heartbeat (for coordinators)
    pub async fn send_heartbeat(&self, status: CoordinatorStatus) -> BootstrapResult<()> {
        let coordinator_id = self
            .coordinator_id
            .read()
            .await
            .ok_or(BootstrapError::Internal(
                "Not registered as coordinator".to_string(),
            ))?;

        let auth_token = self
            .auth_token
            .read()
            .await
            .clone()
            .ok_or(BootstrapError::Internal("No auth token".to_string()))?;

        let heartbeat = CoordinatorHeartbeat {
            coordinator_id,
            status,
            auth_token,
        };

        let response = self
            .request_with_retry(BootstrapMessage::Heartbeat(heartbeat))
            .await?;

        match response {
            BootstrapMessage::HeartbeatAck { .. } => {
                debug!("Heartbeat acknowledged");
                Ok(())
            }
            BootstrapMessage::Error { code, message } => {
                error!("Heartbeat failed: {:?} - {}", code, message);
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Unregister from the bootstrap server (for coordinators)
    pub async fn unregister(&self) -> BootstrapResult<()> {
        let coordinator_id = self
            .coordinator_id
            .read()
            .await
            .ok_or(BootstrapError::Internal(
                "Not registered as coordinator".to_string(),
            ))?;

        let auth_token = self
            .auth_token
            .read()
            .await
            .clone()
            .ok_or(BootstrapError::Internal("No auth token".to_string()))?;

        let request = UnregisterRequest {
            coordinator_id,
            auth_token,
            reason: Some("Client requested unregister".to_string()),
        };

        let response = self
            .request_with_retry(BootstrapMessage::Unregister(request))
            .await?;

        match response {
            BootstrapMessage::UnregisterAck => {
                *self.coordinator_id.write().await = None;
                *self.auth_token.write().await = None;
                info!("Unregistered from bootstrap server");
                Ok(())
            }
            BootstrapMessage::Error { code: _, message } => {
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    // ========================================================================
    // Node Operations
    // ========================================================================

    /// Find coordinators matching a query
    pub async fn find_coordinators(
        &self,
        query: CoordinatorQuery,
    ) -> BootstrapResult<Vec<CoordinatorInfo>> {
        // Prefer UDP for fast queries
        let response = if self.config.prefer_udp {
            match self
                .udp_request(BootstrapMessage::FindCoordinators(query.clone()))
                .await
            {
                Ok(r) => r,
                Err(_) => {
                    // Fallback to TCP
                    self.request_with_retry(BootstrapMessage::FindCoordinators(query))
                        .await?
                }
            }
        } else {
            self.request_with_retry(BootstrapMessage::FindCoordinators(query))
                .await?
        };

        match response {
            BootstrapMessage::CoordinatorList { coordinators, .. } => {
                debug!("Found {} coordinators", coordinators.len());
                Ok(coordinators)
            }
            BootstrapMessage::Error { code: _, message } => {
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Get a specific coordinator by ID
    pub async fn get_coordinator(
        &self,
        id: CoordinatorId,
    ) -> BootstrapResult<Option<CoordinatorInfo>> {
        let response = if self.config.prefer_udp {
            match self
                .udp_request(BootstrapMessage::GetCoordinator { id })
                .await
            {
                Ok(r) => r,
                Err(_) => {
                    self.request_with_retry(BootstrapMessage::GetCoordinator { id })
                        .await?
                }
            }
        } else {
            self.request_with_retry(BootstrapMessage::GetCoordinator { id })
                .await?
        };

        match response {
            BootstrapMessage::CoordinatorDetails(info) => Ok(info),
            BootstrapMessage::Error { code: _, message } => {
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Discover public address using STUN
    pub async fn discover_public_addr(&self) -> BootstrapResult<BindingResponse> {
        let response = self
            .udp_request(BootstrapMessage::StunRequest {
                node_id: self.node_id,
            })
            .await?;

        match response {
            BootstrapMessage::StunResponse(binding) => {
                *self.public_addr.write().await = Some(binding.public_addr);
                info!("Discovered public address: {}", binding.public_addr);
                Ok(binding)
            }
            BootstrapMessage::Error { code: _, message } => {
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Request hole punch coordination
    pub async fn request_hole_punch(
        &self,
        peer_node: NodeId,
    ) -> BootstrapResult<PunchInstructions> {
        // First, ensure we know our public address
        let public_addr = match *self.public_addr.read().await {
            Some(addr) => addr,
            None => {
                let binding = self.discover_public_addr().await?;
                binding.public_addr
            }
        };

        let request = HolePunchRequest {
            node_id: self.node_id,
            node_addr: public_addr,
            target_node: peer_node,
            nat_type: NatType::Unknown, // Would be determined by STUN
        };

        let response = self
            .request_with_retry(BootstrapMessage::HolePunchRequest(request))
            .await?;

        match response {
            BootstrapMessage::HolePunchInstructions(instructions) => {
                info!(
                    "Received hole punch instructions: peer={}, punch_at={}",
                    instructions.peer_public_addr, instructions.punch_at_ms
                );
                Ok(instructions)
            }
            BootstrapMessage::Error { code: _, message } => {
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Request a relay session
    pub async fn request_relay(
        &self,
        peer_node: NodeId,
        reason: RelayReason,
    ) -> BootstrapResult<(RelaySessionId, SocketAddr)> {
        let request = RelayRequest {
            node_id: self.node_id,
            peer_node,
            reason,
        };

        let response = self
            .request_with_retry(BootstrapMessage::RelayRequest(request))
            .await?;

        match response {
            BootstrapMessage::RelaySessionCreated {
                session_id,
                relay_addr,
            } => {
                info!(
                    "Relay session created: {} (relay: {})",
                    session_id, relay_addr
                );
                Ok((session_id, relay_addr))
            }
            BootstrapMessage::Error { code: _, message } => {
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Send data through relay
    pub async fn relay_send(
        &self,
        session_id: RelaySessionId,
        data: Vec<u8>,
    ) -> BootstrapResult<()> {
        let response = self
            .tcp_request(BootstrapMessage::RelayData {
                session_id,
                from_node: self.node_id,
                data,
            })
            .await?;

        match response {
            BootstrapMessage::HeartbeatAck { .. } => Ok(()),
            BootstrapMessage::Error { code: _, message } => {
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Close a relay session
    pub async fn relay_close(&self, session_id: RelaySessionId) -> BootstrapResult<()> {
        let response = self
            .tcp_request(BootstrapMessage::RelayClose { session_id })
            .await?;

        match response {
            BootstrapMessage::UnregisterAck => Ok(()),
            BootstrapMessage::Error { code: _, message } => {
                Err(BootstrapError::ProtocolError(message))
            }
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    // ========================================================================
    // Utility
    // ========================================================================

    /// Ping the server and measure round-trip time
    pub async fn ping(&self) -> BootstrapResult<Duration> {
        let start = Instant::now();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let response = self
            .tcp_request(BootstrapMessage::Ping { timestamp })
            .await?;

        match response {
            BootstrapMessage::Pong { timestamp: ts, .. } if ts == timestamp => {
                let rtt = start.elapsed();
                debug!("Ping RTT: {:?}", rtt);
                Ok(rtt)
            }
            BootstrapMessage::Pong { .. } => Err(BootstrapError::ProtocolError(
                "Timestamp mismatch".to_string(),
            )),
            _ => Err(BootstrapError::UnexpectedResponse),
        }
    }

    /// Start a background heartbeat task (for coordinators)
    pub fn start_heartbeat_task(
        self: &Arc<Self>,
        status_provider: impl Fn() -> CoordinatorStatus + Send + Sync + 'static,
    ) -> tokio::task::JoinHandle<()> {
        let client = Arc::clone(self);
        let interval = self.config.heartbeat_interval;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                ticker.tick().await;

                if client.coordinator_id().await.is_none() {
                    debug!("Heartbeat task: not registered, skipping");
                    continue;
                }

                let status = status_provider();
                if let Err(e) = client.send_heartbeat(status).await {
                    error!("Heartbeat failed: {}", e);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = BootstrapClient::new(vec![
            "127.0.0.1:9000".parse().unwrap(),
            "127.0.0.1:9001".parse().unwrap(),
        ]);

        assert_eq!(client.server_addrs.len(), 2);
        assert!(client.node_id.0.to_string().len() > 0);
    }

    #[test]
    fn test_client_config() {
        let config = ClientConfig {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(15),
            max_retries: 5,
            retry_delay: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(10),
            prefer_udp: false,
        };

        let client =
            BootstrapClient::with_config(vec!["127.0.0.1:9000".parse().unwrap()], config.clone());

        assert_eq!(client.config.max_retries, 5);
        assert_eq!(client.config.prefer_udp, false);
    }

    #[tokio::test]
    async fn test_is_connected_initially_false() {
        let client = BootstrapClient::new(vec!["127.0.0.1:9000".parse().unwrap()]);
        assert!(!client.is_connected().await);
    }

    #[tokio::test]
    async fn test_coordinator_id_initially_none() {
        let client = BootstrapClient::new(vec!["127.0.0.1:9000".parse().unwrap()]);
        assert!(client.coordinator_id().await.is_none());
    }

    #[tokio::test]
    async fn test_public_addr_initially_none() {
        let client = BootstrapClient::new(vec!["127.0.0.1:9000".parse().unwrap()]);
        assert!(client.public_addr().await.is_none());
    }

    #[tokio::test]
    async fn test_connect_to_unavailable_server() {
        let client = BootstrapClient::with_config(
            vec!["127.0.0.1:59999".parse().unwrap()], // Unlikely to be in use
            ClientConfig {
                connect_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        );

        let result = client.connect().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(BootstrapError::NoServersAvailable)));
    }

    #[tokio::test]
    async fn test_disconnect() {
        let client = BootstrapClient::new(vec!["127.0.0.1:9000".parse().unwrap()]);

        // Disconnect should work even if not connected
        client.disconnect().await;
        assert!(!client.is_connected().await);
    }

    #[test]
    fn test_coordinator_query_builder() {
        let query = CoordinatorQuery::new()
            .with_region("us-west")
            .with_organization("my-org")
            .accepting_phantom()
            .with_min_cores(8)
            .with_limit(5);

        assert_eq!(query.region, Some("us-west".to_string()));
        assert_eq!(query.organization, Some("my-org".to_string()));
        assert_eq!(query.accepts_phantom, Some(true));
        assert_eq!(query.min_capacity_cores, Some(8));
        assert_eq!(query.limit, 5);
    }

    // Integration tests would require a running bootstrap server
    // These would be in a separate integration test file
}
