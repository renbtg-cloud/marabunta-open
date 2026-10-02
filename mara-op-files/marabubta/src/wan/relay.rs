// Marabunta - Licensed under the MIT License.
//! Relay service for nodes that can't do P2P
//!
//! When hole punching fails (typically due to symmetric NAT), traffic
//! goes through the relay server. This is a fallback mechanism to ensure
//! connectivity even in the most restrictive network environments.
//!
//! # Architecture
//!
//! ```text
//! Node A -----> Relay Server -----> Node B
//!   (Symmetric NAT)              (Any NAT)
//! ```
//!
//! # Limitations
//!
//! - Higher latency than direct P2P
//! - Bandwidth limited per session
//! - Session timeout for resource management
//! - Should only be used when hole punching fails

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info};

use super::errors::{RelayError, RelayResult};
use super::protocol::{NodeId, RelaySessionId};

// ============================================================================
// Configuration
// ============================================================================

/// Relay service configuration
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Maximum concurrent relay sessions
    pub max_sessions: usize,
    /// Session timeout duration
    pub session_timeout: Duration,
    /// Maximum bandwidth per session (bytes/sec)
    pub bandwidth_limit_per_session: u64,
    /// Cleanup interval for expired sessions
    pub cleanup_interval: Duration,
    /// Maximum data size per relay call
    pub max_data_size: usize,
    /// Whether to allow relay between nodes in the same session only
    pub require_session: bool,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            max_sessions: 1000,
            session_timeout: Duration::from_secs(300), // 5 minutes
            bandwidth_limit_per_session: 1024 * 1024,  // 1 MB/sec
            cleanup_interval: Duration::from_secs(30),
            max_data_size: 64 * 1024, // 64 KB
            require_session: true,
        }
    }
}

// ============================================================================
// Relay Session
// ============================================================================

/// State of a relay session
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Session is active
    Active,
    /// Session is paused (e.g., bandwidth exceeded)
    Paused,
    /// Session is closed
    Closed,
}

/// A relay session between two nodes
#[derive(Debug)]
pub struct RelaySession {
    /// Session ID
    pub id: RelaySessionId,
    /// First node
    pub node_a: NodeId,
    /// Second node
    pub node_b: NodeId,
    /// When the session was created
    pub created_at: Instant,
    /// Last activity time
    pub last_activity: Instant,
    /// Total bytes relayed
    pub bytes_relayed: u64,
    /// Bytes relayed in current window (for rate limiting)
    pub bytes_this_window: u64,
    /// Start of current rate limit window
    pub window_start: Instant,
    /// Session state
    pub state: SessionState,
}

impl RelaySession {
    fn new(id: RelaySessionId, node_a: NodeId, node_b: NodeId) -> Self {
        let now = Instant::now();
        Self {
            id,
            node_a,
            node_b,
            created_at: now,
            last_activity: now,
            bytes_relayed: 0,
            bytes_this_window: 0,
            window_start: now,
            state: SessionState::Active,
        }
    }

    /// Check if a node is part of this session
    pub fn has_node(&self, node: NodeId) -> bool {
        self.node_a == node || self.node_b == node
    }

    /// Get the peer for a node in this session
    pub fn peer_of(&self, node: NodeId) -> Option<NodeId> {
        if node == self.node_a {
            Some(self.node_b)
        } else if node == self.node_b {
            Some(self.node_a)
        } else {
            None
        }
    }

    /// Check if the session has timed out
    pub fn is_expired(&self, timeout: Duration) -> bool {
        Instant::now().duration_since(self.last_activity) > timeout
    }

    /// Reset rate limit window if needed
    fn reset_window_if_needed(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.window_start) >= Duration::from_secs(1) {
            self.bytes_this_window = 0;
            self.window_start = now;
        }
    }

    /// Check if bandwidth limit would be exceeded
    fn would_exceed_bandwidth(&self, bytes: u64, limit: u64) -> bool {
        self.bytes_this_window + bytes > limit
    }

    /// Record bytes relayed
    fn record_bytes(&mut self, bytes: u64) {
        self.bytes_relayed += bytes;
        self.bytes_this_window += bytes;
        self.last_activity = Instant::now();
    }
}

// ============================================================================
// Relay Service
// ============================================================================

/// Statistics for the relay service
#[derive(Debug, Default)]
pub struct RelayStats {
    /// Total sessions created
    pub total_sessions: AtomicU64,
    /// Currently active sessions
    pub active_sessions: AtomicU64,
    /// Total bytes relayed
    pub total_bytes_relayed: AtomicU64,
    /// Total relay operations
    pub total_relay_ops: AtomicU64,
    /// Sessions closed due to timeout
    pub timeout_closes: AtomicU64,
    /// Sessions closed due to bandwidth exceeded
    pub bandwidth_exceeded: AtomicU64,
}

/// Relay service for nodes that can't establish P2P connections
pub struct RelayService {
    /// Configuration
    config: RelayConfig,
    /// Active sessions by ID
    sessions: RwLock<HashMap<RelaySessionId, RelaySession>>,
    /// Sessions by node (for quick lookup)
    by_node: RwLock<HashMap<NodeId, Vec<RelaySessionId>>>,
    /// Pending data for nodes (when peer hasn't polled yet)
    pending_data: RwLock<HashMap<(RelaySessionId, NodeId), Vec<Vec<u8>>>>,
    /// Statistics
    stats: RelayStats,
}

impl RelayService {
    /// Create a new relay service
    pub fn new(config: RelayConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            by_node: RwLock::new(HashMap::new()),
            pending_data: RwLock::new(HashMap::new()),
            stats: RelayStats::default(),
        }
    }

    /// Create a relay session between two nodes
    pub async fn create_session(
        &self,
        node_a: NodeId,
        node_b: NodeId,
    ) -> RelayResult<RelaySessionId> {
        // Check capacity
        let sessions = self.sessions.read().await;
        if sessions.len() >= self.config.max_sessions {
            return Err(RelayError::MaxSessionsReached(self.config.max_sessions));
        }
        drop(sessions);

        // Create session
        let id = RelaySessionId::new();
        let session = RelaySession::new(id, node_a, node_b);

        // Insert session
        let mut sessions = self.sessions.write().await;
        sessions.insert(id, session);
        drop(sessions);

        // Update node index
        let mut by_node = self.by_node.write().await;
        by_node.entry(node_a).or_insert_with(Vec::new).push(id);
        by_node.entry(node_b).or_insert_with(Vec::new).push(id);

        // Update stats
        self.stats.total_sessions.fetch_add(1, Ordering::Relaxed);
        self.stats.active_sessions.fetch_add(1, Ordering::Relaxed);

        info!(
            "Created relay session {} between {} and {}",
            id, node_a, node_b
        );

        Ok(id)
    }

    /// Relay data from one node to another
    ///
    /// Data is stored for the peer to retrieve. In a real implementation,
    /// this would push directly to the peer's connection.
    pub async fn relay(
        &self,
        session_id: RelaySessionId,
        from_node: NodeId,
        data: &[u8],
    ) -> RelayResult<()> {
        // Validate data size
        if data.len() > self.config.max_data_size {
            return Err(RelayError::InvalidData);
        }

        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(&session_id)
            .ok_or(RelayError::SessionNotFound(session_id))?;

        // Verify node is in session
        if !session.has_node(from_node) {
            return Err(RelayError::NodeNotInSession(from_node, session_id));
        }

        // Check session state
        if session.state != SessionState::Active {
            return Err(RelayError::SessionExpired(session_id));
        }

        // Check timeout
        if session.is_expired(self.config.session_timeout) {
            session.state = SessionState::Closed;
            self.stats.timeout_closes.fetch_add(1, Ordering::Relaxed);
            return Err(RelayError::SessionTimeout(session_id));
        }

        // Reset rate limit window if needed
        session.reset_window_if_needed();

        // Check bandwidth limit
        if session
            .would_exceed_bandwidth(data.len() as u64, self.config.bandwidth_limit_per_session)
        {
            self.stats
                .bandwidth_exceeded
                .fetch_add(1, Ordering::Relaxed);
            return Err(RelayError::BandwidthLimitExceeded(session_id));
        }

        // Get peer
        let peer = session.peer_of(from_node).unwrap();

        // Record the relay
        session.record_bytes(data.len() as u64);
        drop(sessions);

        // Store for peer to retrieve
        let mut pending = self.pending_data.write().await;
        pending
            .entry((session_id, peer))
            .or_insert_with(Vec::new)
            .push(data.to_vec());

        // Update stats
        self.stats
            .total_bytes_relayed
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.stats.total_relay_ops.fetch_add(1, Ordering::Relaxed);

        debug!(
            "Relayed {} bytes from {} to {} (session {})",
            data.len(),
            from_node,
            peer,
            session_id
        );

        Ok(())
    }

    /// Receive pending data for a node
    ///
    /// Returns all pending data and clears the buffer.
    pub async fn receive(
        &self,
        session_id: RelaySessionId,
        node: NodeId,
    ) -> RelayResult<Vec<Vec<u8>>> {
        // Verify session exists and node is in it
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(&session_id)
            .ok_or(RelayError::SessionNotFound(session_id))?;

        if !session.has_node(node) {
            return Err(RelayError::NodeNotInSession(node, session_id));
        }
        drop(sessions);

        // Get and clear pending data
        let mut pending = self.pending_data.write().await;
        let data = pending.remove(&(session_id, node)).unwrap_or_default();

        Ok(data)
    }

    /// Close a relay session
    pub async fn close_session(&self, session_id: RelaySessionId) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.remove(&session_id) {
            // Clean up node index
            let mut by_node = self.by_node.write().await;
            if let Some(ids) = by_node.get_mut(&session.node_a) {
                ids.retain(|id| *id != session_id);
            }
            if let Some(ids) = by_node.get_mut(&session.node_b) {
                ids.retain(|id| *id != session_id);
            }
            drop(by_node);

            // Clean up pending data
            let mut pending = self.pending_data.write().await;
            pending.remove(&(session_id, session.node_a));
            pending.remove(&(session_id, session.node_b));

            // Update stats
            self.stats.active_sessions.fetch_sub(1, Ordering::Relaxed);

            info!(
                "Closed relay session {} (relayed {} bytes)",
                session_id, session.bytes_relayed
            );
        }
    }

    /// Close all sessions for a node
    pub async fn close_sessions_for_node(&self, node: NodeId) {
        let sessions_to_close: Vec<RelaySessionId> = {
            let by_node = self.by_node.read().await;
            by_node.get(&node).cloned().unwrap_or_default()
        };

        for session_id in sessions_to_close {
            self.close_session(session_id).await;
        }
    }

    /// Get session info
    pub async fn get_session(&self, session_id: RelaySessionId) -> Option<RelaySessionInfo> {
        let sessions = self.sessions.read().await;
        sessions.get(&session_id).map(|s| RelaySessionInfo {
            id: s.id,
            node_a: s.node_a,
            node_b: s.node_b,
            bytes_relayed: s.bytes_relayed,
            state: s.state,
            age: Instant::now().duration_since(s.created_at),
        })
    }

    /// Get all sessions for a node
    pub async fn get_sessions_for_node(&self, node: NodeId) -> Vec<RelaySessionId> {
        let by_node = self.by_node.read().await;
        by_node.get(&node).cloned().unwrap_or_default()
    }

    /// Cleanup expired sessions
    pub async fn cleanup_expired(&self) {
        let timeout = self.config.session_timeout;
        let expired: Vec<RelaySessionId> = {
            let sessions = self.sessions.read().await;
            sessions
                .iter()
                .filter(|(_, s)| s.is_expired(timeout))
                .map(|(id, _)| *id)
                .collect()
        };

        for id in expired {
            self.close_session(id).await;
            self.stats.timeout_closes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get service statistics
    pub fn stats(&self) -> &RelayStats {
        &self.stats
    }

    /// Get current session count
    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

/// Public info about a relay session
#[derive(Debug, Clone)]
pub struct RelaySessionInfo {
    pub id: RelaySessionId,
    pub node_a: NodeId,
    pub node_b: NodeId,
    pub bytes_relayed: u64,
    pub state: SessionState,
    pub age: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> RelayConfig {
        RelayConfig {
            max_sessions: 10,
            session_timeout: Duration::from_secs(60),
            bandwidth_limit_per_session: 1024, // 1 KB/sec for testing
            cleanup_interval: Duration::from_secs(1),
            max_data_size: 1024,
            require_session: true,
        }
    }

    #[tokio::test]
    async fn test_create_session() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        let session_id = service.create_session(node_a, node_b).await.unwrap();

        let info = service.get_session(session_id).await.unwrap();
        assert_eq!(info.node_a, node_a);
        assert_eq!(info.node_b, node_b);
        assert_eq!(info.bytes_relayed, 0);
        assert_eq!(info.state, SessionState::Active);
    }

    #[tokio::test]
    async fn test_max_sessions() {
        let mut config = create_test_config();
        config.max_sessions = 2;
        let service = RelayService::new(config);

        let node1 = NodeId::new();
        let node2 = NodeId::new();
        let node3 = NodeId::new();
        let node4 = NodeId::new();
        let node5 = NodeId::new();
        let node6 = NodeId::new();

        // First two should succeed
        service.create_session(node1, node2).await.unwrap();
        service.create_session(node3, node4).await.unwrap();

        // Third should fail
        let result = service.create_session(node5, node6).await;
        assert!(matches!(result, Err(RelayError::MaxSessionsReached(2))));
    }

    #[tokio::test]
    async fn test_relay_data() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        let session_id = service.create_session(node_a, node_b).await.unwrap();

        // Relay data from A to B
        let data = b"Hello, B!";
        service.relay(session_id, node_a, data).await.unwrap();

        // B receives the data
        let received = service.receive(session_id, node_b).await.unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], data);

        // No more pending data
        let received = service.receive(session_id, node_b).await.unwrap();
        assert!(received.is_empty());
    }

    #[tokio::test]
    async fn test_relay_bidirectional() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        let session_id = service.create_session(node_a, node_b).await.unwrap();

        // A sends to B
        service.relay(session_id, node_a, b"Hello B").await.unwrap();

        // B sends to A
        service.relay(session_id, node_b, b"Hello A").await.unwrap();

        // Both receive
        let a_received = service.receive(session_id, node_a).await.unwrap();
        let b_received = service.receive(session_id, node_b).await.unwrap();

        assert_eq!(a_received.len(), 1);
        assert_eq!(b_received.len(), 1);
        assert_eq!(a_received[0], b"Hello A");
        assert_eq!(b_received[0], b"Hello B");
    }

    #[tokio::test]
    async fn test_relay_unknown_session() {
        let service = RelayService::new(create_test_config());
        let node = NodeId::new();
        let unknown_session = RelaySessionId::new();

        let result = service.relay(unknown_session, node, b"data").await;
        assert!(matches!(result, Err(RelayError::SessionNotFound(_))));
    }

    #[tokio::test]
    async fn test_relay_wrong_node() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let node_c = NodeId::new();

        let session_id = service.create_session(node_a, node_b).await.unwrap();

        // Node C tries to relay (not in session)
        let result = service.relay(session_id, node_c, b"data").await;
        assert!(matches!(result, Err(RelayError::NodeNotInSession(_, _))));
    }

    #[tokio::test]
    async fn test_bandwidth_limit() {
        let mut config = create_test_config();
        config.bandwidth_limit_per_session = 100; // 100 bytes/sec
        let service = RelayService::new(config);

        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let session_id = service.create_session(node_a, node_b).await.unwrap();

        // First relay should succeed
        service.relay(session_id, node_a, &[0u8; 50]).await.unwrap();

        // Second relay should also succeed (still under 100)
        service.relay(session_id, node_a, &[0u8; 40]).await.unwrap();

        // Third relay should fail (would exceed 100)
        let result = service.relay(session_id, node_a, &[0u8; 20]).await;
        assert!(matches!(result, Err(RelayError::BandwidthLimitExceeded(_))));
    }

    #[tokio::test]
    async fn test_close_session() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        let session_id = service.create_session(node_a, node_b).await.unwrap();
        assert_eq!(service.session_count().await, 1);

        service.close_session(session_id).await;
        assert_eq!(service.session_count().await, 0);

        // Session no longer exists
        assert!(service.get_session(session_id).await.is_none());
    }

    #[tokio::test]
    async fn test_close_sessions_for_node() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let node_c = NodeId::new();

        // Create two sessions involving node_a
        service.create_session(node_a, node_b).await.unwrap();
        service.create_session(node_a, node_c).await.unwrap();
        assert_eq!(service.session_count().await, 2);

        // Close all sessions for node_a
        service.close_sessions_for_node(node_a).await;
        assert_eq!(service.session_count().await, 0);
    }

    #[tokio::test]
    async fn test_get_sessions_for_node() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let node_c = NodeId::new();

        let session1 = service.create_session(node_a, node_b).await.unwrap();
        let session2 = service.create_session(node_a, node_c).await.unwrap();

        let sessions = service.get_sessions_for_node(node_a).await;
        assert_eq!(sessions.len(), 2);
        assert!(sessions.contains(&session1));
        assert!(sessions.contains(&session2));

        let sessions_b = service.get_sessions_for_node(node_b).await;
        assert_eq!(sessions_b.len(), 1);
        assert!(sessions_b.contains(&session1));
    }

    #[tokio::test]
    async fn test_session_expiry() {
        let mut config = create_test_config();
        config.session_timeout = Duration::from_millis(50);
        let service = RelayService::new(config);

        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let session_id = service.create_session(node_a, node_b).await.unwrap();

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try to relay - should fail with timeout
        let result = service.relay(session_id, node_a, b"data").await;
        assert!(matches!(result, Err(RelayError::SessionTimeout(_))));
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let mut config = create_test_config();
        config.session_timeout = Duration::from_millis(50);
        let service = RelayService::new(config);

        let node_a = NodeId::new();
        let node_b = NodeId::new();
        service.create_session(node_a, node_b).await.unwrap();

        assert_eq!(service.session_count().await, 1);

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cleanup
        service.cleanup_expired().await;

        assert_eq!(service.session_count().await, 0);
    }

    #[tokio::test]
    async fn test_data_size_limit() {
        let mut config = create_test_config();
        config.max_data_size = 100;
        let service = RelayService::new(config);

        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let session_id = service.create_session(node_a, node_b).await.unwrap();

        // Too large
        let result = service.relay(session_id, node_a, &[0u8; 200]).await;
        assert!(matches!(result, Err(RelayError::InvalidData)));

        // Just right
        service.relay(session_id, node_a, &[0u8; 50]).await.unwrap();
    }

    #[tokio::test]
    async fn test_stats() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        assert_eq!(service.stats().total_sessions.load(Ordering::Relaxed), 0);

        service.create_session(node_a, node_b).await.unwrap();

        assert_eq!(service.stats().total_sessions.load(Ordering::Relaxed), 1);
        assert_eq!(service.stats().active_sessions.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_multiple_pending_messages() {
        let service = RelayService::new(create_test_config());
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        let session_id = service.create_session(node_a, node_b).await.unwrap();

        // Send multiple messages
        service.relay(session_id, node_a, b"msg1").await.unwrap();
        service.relay(session_id, node_a, b"msg2").await.unwrap();
        service.relay(session_id, node_a, b"msg3").await.unwrap();

        // Receive all at once
        let received = service.receive(session_id, node_b).await.unwrap();
        assert_eq!(received.len(), 3);
        assert_eq!(received[0], b"msg1");
        assert_eq!(received[1], b"msg2");
        assert_eq!(received[2], b"msg3");
    }

    #[test]
    fn test_session_peer_of() {
        let id = RelaySessionId::new();
        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let session = RelaySession::new(id, node_a, node_b);

        assert_eq!(session.peer_of(node_a), Some(node_b));
        assert_eq!(session.peer_of(node_b), Some(node_a));

        let other = NodeId::new();
        assert_eq!(session.peer_of(other), None);
    }

    #[test]
    fn test_session_has_node() {
        let id = RelaySessionId::new();
        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let session = RelaySession::new(id, node_a, node_b);

        assert!(session.has_node(node_a));
        assert!(session.has_node(node_b));

        let other = NodeId::new();
        assert!(!session.has_node(other));
    }
}
