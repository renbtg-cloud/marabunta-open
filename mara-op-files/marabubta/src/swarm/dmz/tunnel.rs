// Marabunta - Licensed under the MIT License.
use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Request, State},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use dashmap::DashMap;
use tokio::sync::oneshot;
use tracing::{info, warn, error};
use uuid::Uuid;

use crate::swarm::transport::SwarmTransport;
use crate::swarm::types::{NodeId, SwarmMessage};
use crate::swarm::knowledge::KnowledgeStore;

/// State shared across the Axum reverse-proxy handlers.
struct TunnelState {
    transport: Arc<SwarmTransport>,
    knowledge: Arc<KnowledgeStore>,
    /// Maps an incoming HTTP request's Stream ID to a oneshot sender 
    /// waiting for the Swarm worker's HTTP response.
    pending_requests: Arc<DashMap<Uuid, oneshot::Sender<Vec<u8>>>>,
    /// STUB: Hardcoded routing. In reality, this maps `host` headers to specific `NodeId`s.
    target_node: NodeId, 
}

/// The DMZ HTTP Tunnel exposes Swarm-hosted WASM web servers to the public internet.
pub struct DmzTunnel {
    state: Arc<TunnelState>,
    port: u16,
}

impl DmzTunnel {
    pub fn new(transport: Arc<SwarmTransport>, knowledge: Arc<KnowledgeStore>, target_node: NodeId, port: u16) -> Self {
        let state = Arc::new(TunnelState {
            transport,
            knowledge,
            pending_requests: Arc::new(DashMap::new()),
            target_node,
        });

        Self { state, port }
    }

    /// Handle incoming responses from the Swarm network (called by SwarmTransport dispatch).
    pub fn handle_egress(&self, stream_id: Uuid, raw_bytes: Vec<u8>) {
        if let Some((_, sender)) = self.state.pending_requests.remove(&stream_id) {
            let _ = sender.send(raw_bytes);
        } else {
            warn!(%stream_id, "Received TunnelEgress for unknown or expired stream");
        }
    }

    /// Start the public-facing HTTP server.
    pub async fn run(self) {
        let app = Router::new()
            .route("/{*path}", any(proxy_handler))
            .with_state(self.state.clone());

        let addr = format!("0.0.0.0:{}", self.port);
        info!("DMZ HTTP Tunnel listening on {}", addr);
        
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}

/// Intercepts public HTTP requests, packages them, and gossips them to the Swarm worker.
async fn proxy_handler(
    State(state): State<Arc<TunnelState>>,
    req: Request<Body>,
) -> Response {
    let stream_id = Uuid::new_v4();
    
    // In a full implementation, we serialize method, uri, headers, and body bytes.
    // For Phase 5, we send a basic stub payload to prove the wiring.
    let simulated_http_req = format!("{} {}", req.method(), req.uri()).into_bytes();

    let (tx, rx) = oneshot::channel();
    state.pending_requests.insert(stream_id, tx);

    let msg = SwarmMessage::TunnelIngress {
        stream_id,
        raw_bytes: simulated_http_req,
    };

    // Route to worker
    if let Some(node_info) = state.knowledge.get_node(&state.target_node) {
        if let Some(addr) = node_info.address {
            if let Err(e) = state.transport.send(addr, msg).await {
                error!("Failed to route HTTP Ingress to {}: {}", addr, e);
            } else {
                info!(%stream_id, target = %state.target_node, "Routed HTTP ingress to Swarm worker");
            }
        }
    } else {
        error!(target = %state.target_node, "Target node not found in KnowledgeStore");
    }

    // Await worker response
    match tokio::time::timeout(std::time::Duration::from_secs(10), rx).await {
        Ok(Ok(bytes)) => {
            // STUB: Parse HTTP response headers/body from bytes. For now, just return raw text.
            Response::builder()
                .status(200)
                .body(Body::from(bytes))
                .unwrap()
        }
        _ => {
            state.pending_requests.remove(&stream_id);
            Response::builder()
                .status(504)
                .body(Body::from("Gateway Timeout: Swarm worker did not respond"))
                .unwrap()
        }
    }
}
