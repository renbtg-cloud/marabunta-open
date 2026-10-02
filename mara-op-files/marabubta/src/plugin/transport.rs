// Marabunta - Licensed under the MIT License.
//! Transport layer for out-of-process plugin communication.
//!
//! Provides length-delimited JSON framing over Unix sockets (primary) and
//! TCP (fallback for non-Unix systems). Every frame is a 4-byte big-endian
//! length prefix followed by a JSON-serialized [`PluginWireMessage`].
//!
//! The two main types are:
//!
//! - [`PluginConnection`] / [`TcpPluginConnection`]: a bidirectional
//!   connection to a plugin process. Supports concurrent `send` and `recv`
//!   through independent `tokio::sync::Mutex`-guarded halves.
//!
//! - [`PluginListener`] / [`TcpPluginListener`]: a server that accepts
//!   incoming plugin connections.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;
use tracing::debug;

use crate::plugin::config::PLUGIN_MAX_MESSAGE_SIZE;
use crate::plugin::types::{PluginError, PluginResult, PluginWireMessage};

// ============================================================================
// PluginConnection (Unix socket)
// ============================================================================

/// A connection to an out-of-process plugin via a Unix socket.
///
/// Uses length-delimited JSON frames (4-byte big-endian length prefix +
/// JSON payload). The read and write halves are independently locked so
/// that one task can send while another task receives concurrently.
pub struct PluginConnection {
    reader: tokio::sync::Mutex<BufReader<tokio::net::unix::OwnedReadHalf>>,
    writer: tokio::sync::Mutex<BufWriter<tokio::net::unix::OwnedWriteHalf>>,
    max_message_size: usize,
}

impl PluginConnection {
    /// Connect to an existing Unix socket at `path`.
    pub async fn connect(path: &Path) -> PluginResult<Self> {
        let stream = UnixStream::connect(path).await.map_err(|e| {
            PluginError::Transport(format!(
                "failed to connect to plugin socket {}: {}",
                path.display(),
                e,
            ))
        })?;
        debug!(path = %path.display(), "connected to plugin socket");
        Ok(Self::from_stream(stream))
    }

    /// Wrap an already-accepted [`UnixStream`] into a `PluginConnection`.
    pub fn from_stream(stream: UnixStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: tokio::sync::Mutex::new(BufReader::new(read_half)),
            writer: tokio::sync::Mutex::new(BufWriter::new(write_half)),
            max_message_size: PLUGIN_MAX_MESSAGE_SIZE,
        }
    }

    /// Send a [`PluginWireMessage`] over the connection.
    ///
    /// 1. Serialize message to JSON bytes.
    /// 2. Check against the maximum message size.
    /// 3. Write a 4-byte big-endian length prefix.
    /// 4. Write the JSON payload.
    /// 5. Flush.
    pub async fn send(&self, msg: &PluginWireMessage) -> PluginResult<()> {
        let json_bytes = serde_json::to_vec(msg).map_err(|e| {
            PluginError::Serialization(format!("failed to serialize PluginWireMessage: {}", e))
        })?;

        if json_bytes.len() > self.max_message_size {
            return Err(PluginError::Transport(format!(
                "message size {} exceeds maximum {}",
                json_bytes.len(),
                self.max_message_size,
            )));
        }

        let len_prefix = (json_bytes.len() as u32).to_be_bytes();

        let mut writer = self.writer.lock().await;
        writer.write_all(&len_prefix).await.map_err(|e| {
            PluginError::Transport(format!("failed to write length prefix: {}", e))
        })?;
        writer.write_all(&json_bytes).await.map_err(|e| {
            PluginError::Transport(format!("failed to write message payload: {}", e))
        })?;
        writer.flush().await.map_err(|e| {
            PluginError::Transport(format!("failed to flush writer: {}", e))
        })?;

        Ok(())
    }

    /// Receive a [`PluginWireMessage`] from the connection.
    ///
    /// 1. Read a 4-byte big-endian length prefix.
    /// 2. Validate against the maximum message size.
    /// 3. Read exactly that many bytes.
    /// 4. Deserialize from JSON.
    pub async fn recv(&self) -> PluginResult<PluginWireMessage> {
        let mut reader = self.reader.lock().await;

        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await.map_err(|e| {
            PluginError::Transport(format!("failed to read length prefix: {}", e))
        })?;

        let msg_len = u32::from_be_bytes(len_buf) as usize;

        if msg_len > self.max_message_size {
            return Err(PluginError::Transport(format!(
                "incoming message size {} exceeds maximum {}",
                msg_len, self.max_message_size,
            )));
        }

        let mut payload = vec![0u8; msg_len];
        reader.read_exact(&mut payload).await.map_err(|e| {
            PluginError::Transport(format!("failed to read message payload: {}", e))
        })?;

        let msg: PluginWireMessage = serde_json::from_slice(&payload).map_err(|e| {
            PluginError::Serialization(format!("failed to deserialize PluginWireMessage: {}", e))
        })?;

        Ok(msg)
    }

    /// Send a message and then receive the response (request-response pattern).
    pub async fn send_recv(
        &self,
        msg: &PluginWireMessage,
    ) -> PluginResult<PluginWireMessage> {
        self.send(msg).await?;
        self.recv().await
    }
}

// ============================================================================
// PluginListener (Unix socket)
// ============================================================================

/// Listener for incoming plugin connections on a Unix socket.
///
/// On [`Drop`], the socket file is automatically removed.
pub struct PluginListener {
    listener: tokio::net::UnixListener,
    socket_path: PathBuf,
}

impl PluginListener {
    /// Bind a new listener on the given Unix socket path.
    ///
    /// If a stale socket file already exists at `path`, it is removed
    /// before binding.
    pub async fn bind(path: &Path) -> PluginResult<Self> {
        // Remove stale socket file if it exists.
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| {
                PluginError::Transport(format!(
                    "failed to remove stale socket {}: {}",
                    path.display(),
                    e,
                ))
            })?;
        }

        // Ensure the parent directory exists.
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    PluginError::Transport(format!(
                        "failed to create socket directory {}: {}",
                        parent.display(),
                        e,
                    ))
                })?;
            }
        }

        let listener = tokio::net::UnixListener::bind(path).map_err(|e| {
            PluginError::Transport(format!(
                "failed to bind Unix listener on {}: {}",
                path.display(),
                e,
            ))
        })?;

        debug!(path = %path.display(), "plugin listener bound");

        Ok(Self {
            listener,
            socket_path: path.to_path_buf(),
        })
    }

    /// Accept a new plugin connection.
    pub async fn accept(&self) -> PluginResult<PluginConnection> {
        let (stream, _addr) = self.listener.accept().await.map_err(|e| {
            PluginError::Transport(format!("failed to accept plugin connection: {}", e))
        })?;
        debug!("accepted plugin connection");
        Ok(PluginConnection::from_stream(stream))
    }

    /// Return the filesystem path of the Unix socket.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl Drop for PluginListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

// ============================================================================
// TcpPluginConnection
// ============================================================================

/// A connection to an out-of-process plugin via TCP.
///
/// Provides the same length-delimited JSON framing as [`PluginConnection`]
/// but over a TCP socket instead of a Unix socket. Intended for non-Unix
/// platforms or cross-machine plugin communication.
pub struct TcpPluginConnection {
    reader: tokio::sync::Mutex<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    writer: tokio::sync::Mutex<BufWriter<tokio::net::tcp::OwnedWriteHalf>>,
    max_message_size: usize,
}

impl TcpPluginConnection {
    /// Connect to a plugin at the given TCP address.
    pub async fn connect(addr: &str) -> PluginResult<Self> {
        let stream = tokio::net::TcpStream::connect(addr).await.map_err(|e| {
            PluginError::Transport(format!(
                "failed to connect to plugin at {}: {}",
                addr, e,
            ))
        })?;
        debug!(addr = %addr, "connected to plugin via TCP");
        Ok(Self::from_stream(stream))
    }

    /// Wrap an already-accepted [`TcpStream`](tokio::net::TcpStream) into a
    /// `TcpPluginConnection`.
    pub fn from_stream(stream: tokio::net::TcpStream) -> Self {
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: tokio::sync::Mutex::new(BufReader::new(read_half)),
            writer: tokio::sync::Mutex::new(BufWriter::new(write_half)),
            max_message_size: PLUGIN_MAX_MESSAGE_SIZE,
        }
    }

    /// Send a [`PluginWireMessage`] over the TCP connection.
    pub async fn send(&self, msg: &PluginWireMessage) -> PluginResult<()> {
        let json_bytes = serde_json::to_vec(msg).map_err(|e| {
            PluginError::Serialization(format!("failed to serialize PluginWireMessage: {}", e))
        })?;

        if json_bytes.len() > self.max_message_size {
            return Err(PluginError::Transport(format!(
                "message size {} exceeds maximum {}",
                json_bytes.len(),
                self.max_message_size,
            )));
        }

        let len_prefix = (json_bytes.len() as u32).to_be_bytes();

        let mut writer = self.writer.lock().await;
        writer.write_all(&len_prefix).await.map_err(|e| {
            PluginError::Transport(format!("failed to write length prefix: {}", e))
        })?;
        writer.write_all(&json_bytes).await.map_err(|e| {
            PluginError::Transport(format!("failed to write message payload: {}", e))
        })?;
        writer.flush().await.map_err(|e| {
            PluginError::Transport(format!("failed to flush writer: {}", e))
        })?;

        Ok(())
    }

    /// Receive a [`PluginWireMessage`] from the TCP connection.
    pub async fn recv(&self) -> PluginResult<PluginWireMessage> {
        let mut reader = self.reader.lock().await;

        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await.map_err(|e| {
            PluginError::Transport(format!("failed to read length prefix: {}", e))
        })?;

        let msg_len = u32::from_be_bytes(len_buf) as usize;

        if msg_len > self.max_message_size {
            return Err(PluginError::Transport(format!(
                "incoming message size {} exceeds maximum {}",
                msg_len, self.max_message_size,
            )));
        }

        let mut payload = vec![0u8; msg_len];
        reader.read_exact(&mut payload).await.map_err(|e| {
            PluginError::Transport(format!("failed to read message payload: {}", e))
        })?;

        let msg: PluginWireMessage = serde_json::from_slice(&payload).map_err(|e| {
            PluginError::Serialization(format!("failed to deserialize PluginWireMessage: {}", e))
        })?;

        Ok(msg)
    }

    /// Send a message and then receive the response (request-response pattern).
    pub async fn send_recv(
        &self,
        msg: &PluginWireMessage,
    ) -> PluginResult<PluginWireMessage> {
        self.send(msg).await?;
        self.recv().await
    }
}

// ============================================================================
// TcpPluginListener
// ============================================================================

/// Listener for incoming plugin connections over TCP.
///
/// Provides the same API surface as [`PluginListener`] but binds a TCP
/// socket instead of a Unix socket.
pub struct TcpPluginListener {
    listener: tokio::net::TcpListener,
}

impl TcpPluginListener {
    /// Bind a TCP listener on the given address (e.g. `"127.0.0.1:4201"`).
    pub async fn bind(addr: &str) -> PluginResult<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            PluginError::Transport(format!(
                "failed to bind TCP plugin listener on {}: {}",
                addr, e,
            ))
        })?;
        debug!(addr = %addr, "TCP plugin listener bound");
        Ok(Self { listener })
    }

    /// Accept a new plugin connection.
    pub async fn accept(&self) -> PluginResult<TcpPluginConnection> {
        let (stream, peer_addr) = self.listener.accept().await.map_err(|e| {
            PluginError::Transport(format!("failed to accept TCP plugin connection: {}", e))
        })?;
        debug!(peer = %peer_addr, "accepted TCP plugin connection");
        Ok(TcpPluginConnection::from_stream(stream))
    }

    /// Return the local address the listener is bound to.
    pub fn local_addr(&self) -> PluginResult<std::net::SocketAddr> {
        self.listener.local_addr().map_err(|e| {
            PluginError::Transport(format!("failed to get local address: {}", e))
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::plugin::types::{
        Consistency, Endpoint, FetchOptions, FetchRequest, FetchResponse, HandleRequest,
        HandleResponse, HealthRequest, HealthResponse, RegisterRequest, RegisterResponse,
        StartRequest, StartResponse, StopRequest, StopResponse, StoreOptions, StoreRequest,
        StoreResponse,
    };

    // -----------------------------------------------------------------------
    // Unix socket tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unix_listener_connect_and_accept() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("test.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();
        assert!(sock_path.exists());
        assert_eq!(listener.socket_path(), sock_path);

        let accept_handle = tokio::spawn(async move { listener.accept().await.unwrap() });

        let client = PluginConnection::connect(&sock_path).await.unwrap();

        let _server = accept_handle.await.unwrap();
        // Both sides connected successfully -- no assertions needed beyond
        // the fact that we didn't panic or error.
        drop(client);
    }

    #[tokio::test]
    async fn unix_send_recv_register_request() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("register.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();

        let accept_handle = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            conn.recv().await.unwrap()
        });

        let client = PluginConnection::connect(&sock_path).await.unwrap();

        let req = PluginWireMessage::RegisterReq(RegisterRequest {
            name: "postgres".into(),
            version: "1.0.0".into(),
            traits: vec!["CanStoreState".into()],
            endpoints: vec![Endpoint {
                name: "pgwire".into(),
                protocol: "tcp".into(),
                default_port: 5432,
            }],
        });

        client.send(&req).await.unwrap();

        let received = accept_handle.await.unwrap();
        match received {
            PluginWireMessage::RegisterReq(r) => {
                assert_eq!(r.name, "postgres");
                assert_eq!(r.version, "1.0.0");
                assert_eq!(r.traits, vec!["CanStoreState"]);
                assert_eq!(r.endpoints.len(), 1);
                assert_eq!(r.endpoints[0].default_port, 5432);
            }
            other => panic!("expected RegisterReq, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn unix_send_recv_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("roundtrip.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();

        let accept_handle = tokio::spawn(async move {
            let server_conn = listener.accept().await.unwrap();
            // Read the request, then send back a response.
            let request = server_conn.recv().await.unwrap();
            match request {
                PluginWireMessage::RegisterReq(_) => {
                    let resp = PluginWireMessage::RegisterResp(RegisterResponse {
                        plugin_id: "pg-001".into(),
                        node_id: "node-abc".into(),
                        swarm_config: vec![1, 2, 3],
                    });
                    server_conn.send(&resp).await.unwrap();
                }
                other => panic!("unexpected message: {:?}", other),
            }
        });

        let client = PluginConnection::connect(&sock_path).await.unwrap();

        let req = PluginWireMessage::RegisterReq(RegisterRequest {
            name: "postgres".into(),
            version: "2.0.0".into(),
            traits: vec![],
            endpoints: vec![],
        });

        let resp = client.send_recv(&req).await.unwrap();
        match resp {
            PluginWireMessage::RegisterResp(r) => {
                assert_eq!(r.plugin_id, "pg-001");
                assert_eq!(r.node_id, "node-abc");
                assert_eq!(r.swarm_config, vec![1, 2, 3]);
            }
            other => panic!("expected RegisterResp, got {:?}", other),
        }

        accept_handle.await.unwrap();
    }

    #[tokio::test]
    async fn unix_send_recv_store_and_fetch() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("store_fetch.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();

        let accept_handle = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();

            // Receive StoreReq.
            let msg = conn.recv().await.unwrap();
            match &msg {
                PluginWireMessage::StoreReq(r) => {
                    assert_eq!(r.key, b"user:42");
                    assert_eq!(r.value, b"alice");
                }
                other => panic!("expected StoreReq, got {:?}", other),
            }

            // Send StoreResp.
            conn.send(&PluginWireMessage::StoreResp(StoreResponse {
                success: true,
                error: String::new(),
                version: 1,
            }))
            .await
            .unwrap();

            // Receive FetchReq.
            let msg = conn.recv().await.unwrap();
            match &msg {
                PluginWireMessage::FetchReq(r) => {
                    assert_eq!(r.key, b"user:42");
                }
                other => panic!("expected FetchReq, got {:?}", other),
            }

            // Send FetchResp.
            conn.send(&PluginWireMessage::FetchResp(FetchResponse {
                found: true,
                value: b"alice".to_vec(),
                version: 1,
                error: String::new(),
            }))
            .await
            .unwrap();
        });

        let client = PluginConnection::connect(&sock_path).await.unwrap();

        // Store.
        let store_resp = client
            .send_recv(&PluginWireMessage::StoreReq(StoreRequest {
                key: b"user:42".to_vec(),
                value: b"alice".to_vec(),
                options: StoreOptions {
                    consistency: Consistency::Strong,
                    replicas: 3,
                    ttl_seconds: 0,
                },
            }))
            .await
            .unwrap();

        match store_resp {
            PluginWireMessage::StoreResp(r) => {
                assert!(r.success);
                assert_eq!(r.version, 1);
            }
            other => panic!("expected StoreResp, got {:?}", other),
        }

        // Fetch.
        let fetch_resp = client
            .send_recv(&PluginWireMessage::FetchReq(FetchRequest {
                key: b"user:42".to_vec(),
                options: FetchOptions::default(),
            }))
            .await
            .unwrap();

        match fetch_resp {
            PluginWireMessage::FetchResp(r) => {
                assert!(r.found);
                assert_eq!(r.value, b"alice");
            }
            other => panic!("expected FetchResp, got {:?}", other),
        }

        accept_handle.await.unwrap();
    }

    #[tokio::test]
    async fn unix_send_recv_handle_and_health() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("handle_health.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();

        let accept_handle = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();

            // HandleReq -> HandleResp
            let msg = conn.recv().await.unwrap();
            match &msg {
                PluginWireMessage::HandleReq(r) => {
                    assert_eq!(r.request_id, "req-1");
                    assert_eq!(r.payload, b"work");
                    assert_eq!(r.from_node, "node-x");
                }
                other => panic!("expected HandleReq, got {:?}", other),
            }
            conn.send(&PluginWireMessage::HandleResp(HandleResponse {
                payload: b"result".to_vec(),
                error: String::new(),
            }))
            .await
            .unwrap();

            // HealthReq -> HealthResp
            let msg = conn.recv().await.unwrap();
            assert!(matches!(msg, PluginWireMessage::HealthReq(_)));
            let mut details = HashMap::new();
            details.insert("connections".into(), "42".into());
            conn.send(&PluginWireMessage::HealthResp(HealthResponse {
                healthy: true,
                status: "running".into(),
                details,
            }))
            .await
            .unwrap();
        });

        let client = PluginConnection::connect(&sock_path).await.unwrap();

        // Handle request.
        let mut metadata = HashMap::new();
        metadata.insert("trace_id".into(), "abc".into());
        let resp = client
            .send_recv(&PluginWireMessage::HandleReq(HandleRequest {
                request_id: "req-1".into(),
                payload: b"work".to_vec(),
                from_node: "node-x".into(),
                metadata,
            }))
            .await
            .unwrap();
        match resp {
            PluginWireMessage::HandleResp(r) => {
                assert_eq!(r.payload, b"result");
                assert!(r.error.is_empty());
            }
            other => panic!("expected HandleResp, got {:?}", other),
        }

        // Health check.
        let resp = client
            .send_recv(&PluginWireMessage::HealthReq(HealthRequest {}))
            .await
            .unwrap();
        match resp {
            PluginWireMessage::HealthResp(r) => {
                assert!(r.healthy);
                assert_eq!(r.status, "running");
                assert_eq!(r.details.get("connections").unwrap(), "42");
            }
            other => panic!("expected HealthResp, got {:?}", other),
        }

        accept_handle.await.unwrap();
    }

    #[tokio::test]
    async fn unix_send_recv_start_and_stop() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("start_stop.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();

        let accept_handle = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();

            // StartReq -> StartResp
            let msg = conn.recv().await.unwrap();
            match &msg {
                PluginWireMessage::StartReq(r) => {
                    assert_eq!(r.config, b"my-config");
                }
                other => panic!("expected StartReq, got {:?}", other),
            }
            conn.send(&PluginWireMessage::StartResp(StartResponse {
                success: true,
                error: String::new(),
            }))
            .await
            .unwrap();

            // StopReq -> StopResp
            let msg = conn.recv().await.unwrap();
            match &msg {
                PluginWireMessage::StopReq(r) => {
                    assert_eq!(r.timeout_seconds, 30);
                }
                other => panic!("expected StopReq, got {:?}", other),
            }
            conn.send(&PluginWireMessage::StopResp(StopResponse { clean: true }))
                .await
                .unwrap();
        });

        let client = PluginConnection::connect(&sock_path).await.unwrap();

        let resp = client
            .send_recv(&PluginWireMessage::StartReq(StartRequest {
                config: b"my-config".to_vec(),
            }))
            .await
            .unwrap();
        match resp {
            PluginWireMessage::StartResp(r) => {
                assert!(r.success);
            }
            other => panic!("expected StartResp, got {:?}", other),
        }

        let resp = client
            .send_recv(&PluginWireMessage::StopReq(StopRequest {
                timeout_seconds: 30,
            }))
            .await
            .unwrap();
        match resp {
            PluginWireMessage::StopResp(r) => {
                assert!(r.clean);
            }
            other => panic!("expected StopResp, got {:?}", other),
        }

        accept_handle.await.unwrap();
    }

    #[tokio::test]
    async fn unix_message_size_limit_send() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("size_limit.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();

        let accept_handle = tokio::spawn(async move {
            let _conn = listener.accept().await.unwrap();
            // Keep alive until test finishes.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });

        let client = PluginConnection::connect(&sock_path).await.unwrap();

        // Construct a message whose JSON serialization exceeds the limit.
        // We set a custom limit through direct field manipulation in the test.
        // Since max_message_size is private, we use a message that would
        // normally be small, but we test against the real limit by creating
        // a truly huge payload.
        let huge_payload = vec![0u8; PLUGIN_MAX_MESSAGE_SIZE + 1];
        let msg = PluginWireMessage::StoreReq(StoreRequest {
            key: b"k".to_vec(),
            value: huge_payload,
            options: StoreOptions::default(),
        });

        let result = client.send(&msg).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = format!("{}", err);
        assert!(
            err_msg.contains("exceeds maximum"),
            "expected size limit error, got: {}",
            err_msg,
        );

        accept_handle.abort();
    }

    #[tokio::test]
    async fn unix_message_size_limit_recv() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("size_limit_recv.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();

        let sock_path_clone = sock_path.clone();
        let send_handle = tokio::spawn(async move {
            // Connect with a raw stream and send an oversized length prefix.
            let mut stream = UnixStream::connect(&sock_path_clone).await.unwrap();
            let fake_len: u32 = (PLUGIN_MAX_MESSAGE_SIZE as u32) + 1;
            stream.write_all(&fake_len.to_be_bytes()).await.unwrap();
            stream.flush().await.unwrap();
            // Keep alive.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });

        let conn = listener.accept().await.unwrap();
        let result = conn.recv().await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("exceeds maximum"),
            "expected size limit error, got: {}",
            err_msg,
        );

        send_handle.abort();
    }

    #[tokio::test]
    async fn unix_listener_removes_stale_socket() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("stale.sock");

        // Create a stale socket file.
        std::fs::write(&sock_path, b"stale").unwrap();
        assert!(sock_path.exists());

        // Binding should succeed after removing the stale file.
        let listener = PluginListener::bind(&sock_path).await.unwrap();
        assert!(sock_path.exists());

        // Drop should clean up.
        drop(listener);
        assert!(!sock_path.exists());
    }

    #[tokio::test]
    async fn unix_listener_drop_cleans_up() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("cleanup.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();
        assert!(sock_path.exists());

        drop(listener);
        assert!(!sock_path.exists());
    }

    #[tokio::test]
    async fn unix_multiple_messages_sequential() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock_path = tmp.path().join("multi.sock");

        let listener = PluginListener::bind(&sock_path).await.unwrap();

        let accept_handle = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            // Echo back each message we receive.
            for _ in 0..5 {
                let msg = conn.recv().await.unwrap();
                conn.send(&msg).await.unwrap();
            }
        });

        let client = PluginConnection::connect(&sock_path).await.unwrap();

        for i in 0..5u32 {
            let msg = PluginWireMessage::HealthReq(HealthRequest {});
            let resp = client.send_recv(&msg).await.unwrap();
            assert!(matches!(resp, PluginWireMessage::HealthReq(_)));
        }

        accept_handle.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // TCP tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tcp_listener_connect_and_accept() {
        let listener = TcpPluginListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        let accept_handle = tokio::spawn(async move { listener.accept().await.unwrap() });

        let _client = TcpPluginConnection::connect(&addr_str).await.unwrap();
        let _server = accept_handle.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_send_recv_roundtrip() {
        let listener = TcpPluginListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let accept_handle = tokio::spawn(async move {
            let server_conn = listener.accept().await.unwrap();
            let request = server_conn.recv().await.unwrap();
            match request {
                PluginWireMessage::RegisterReq(r) => {
                    assert_eq!(r.name, "redis");
                    let resp = PluginWireMessage::RegisterResp(RegisterResponse {
                        plugin_id: "redis-001".into(),
                        node_id: "node-xyz".into(),
                        swarm_config: vec![],
                    });
                    server_conn.send(&resp).await.unwrap();
                }
                other => panic!("unexpected message: {:?}", other),
            }
        });

        let client = TcpPluginConnection::connect(&addr).await.unwrap();

        let req = PluginWireMessage::RegisterReq(RegisterRequest {
            name: "redis".into(),
            version: "7.0.0".into(),
            traits: vec![],
            endpoints: vec![],
        });

        let resp = client.send_recv(&req).await.unwrap();
        match resp {
            PluginWireMessage::RegisterResp(r) => {
                assert_eq!(r.plugin_id, "redis-001");
                assert_eq!(r.node_id, "node-xyz");
            }
            other => panic!("expected RegisterResp, got {:?}", other),
        }

        accept_handle.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_message_size_limit_send() {
        let listener = TcpPluginListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let accept_handle = tokio::spawn(async move {
            let _conn = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });

        let client = TcpPluginConnection::connect(&addr).await.unwrap();

        let huge_payload = vec![0u8; PLUGIN_MAX_MESSAGE_SIZE + 1];
        let msg = PluginWireMessage::StoreReq(StoreRequest {
            key: b"k".to_vec(),
            value: huge_payload,
            options: StoreOptions::default(),
        });

        let result = client.send(&msg).await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("exceeds maximum"),
            "expected size limit error, got: {}",
            err_msg,
        );

        accept_handle.abort();
    }

    #[tokio::test]
    async fn tcp_message_size_limit_recv() {
        let listener = TcpPluginListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let send_handle = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let fake_len: u32 = (PLUGIN_MAX_MESSAGE_SIZE as u32) + 1;
            stream.write_all(&fake_len.to_be_bytes()).await.unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });

        let conn = listener.accept().await.unwrap();
        let result = conn.recv().await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("exceeds maximum"),
            "expected size limit error, got: {}",
            err_msg,
        );

        send_handle.abort();
    }

    #[tokio::test]
    async fn tcp_multiple_messages_sequential() {
        let listener = TcpPluginListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let accept_handle = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            for _ in 0..5 {
                let msg = conn.recv().await.unwrap();
                conn.send(&msg).await.unwrap();
            }
        });

        let client = TcpPluginConnection::connect(&addr).await.unwrap();

        for _ in 0..5 {
            let msg = PluginWireMessage::HealthReq(HealthRequest {});
            let resp = client.send_recv(&msg).await.unwrap();
            assert!(matches!(resp, PluginWireMessage::HealthReq(_)));
        }

        accept_handle.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_send_recv_start_stop() {
        let listener = TcpPluginListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let accept_handle = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();

            let msg = conn.recv().await.unwrap();
            assert!(matches!(msg, PluginWireMessage::StartReq(_)));
            conn.send(&PluginWireMessage::StartResp(StartResponse {
                success: true,
                error: String::new(),
            }))
            .await
            .unwrap();

            let msg = conn.recv().await.unwrap();
            assert!(matches!(msg, PluginWireMessage::StopReq(_)));
            conn.send(&PluginWireMessage::StopResp(StopResponse { clean: true }))
                .await
                .unwrap();
        });

        let client = TcpPluginConnection::connect(&addr).await.unwrap();

        let resp = client
            .send_recv(&PluginWireMessage::StartReq(StartRequest {
                config: b"tcp-config".to_vec(),
            }))
            .await
            .unwrap();
        assert!(matches!(resp, PluginWireMessage::StartResp(_)));

        let resp = client
            .send_recv(&PluginWireMessage::StopReq(StopRequest {
                timeout_seconds: 60,
            }))
            .await
            .unwrap();
        assert!(matches!(resp, PluginWireMessage::StopResp(_)));

        accept_handle.await.unwrap();
    }
}
