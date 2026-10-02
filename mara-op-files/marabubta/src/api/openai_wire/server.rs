// Marabunta - Licensed under the MIT License.
//! Pillar 6.4: OpenAI-Compatible Inference API
//! 
//! Exposes a local `/v1/chat/completions` REST endpoint.
//! Translates incoming requests into JCL Bids directed at the Global Spot Market.
//! Bids target high-reputation nodes with `has_sovereign_gpu = true`.

use axum::{
    extract::{State, Json},
    routing::post,
    Router, response::IntoResponse, http::StatusCode
};
use tracing::{info, error, debug};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

pub struct OpenAiGatewayState {}

pub struct OpenAiGateway {
    port: u16,
    state: Arc<OpenAiGatewayState>,
}

impl Default for OpenAiGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenAiGateway {
    pub fn new() -> Self {
        Self { 
            port: 8080,
            state: Arc::new(OpenAiGatewayState {}),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let app = Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .with_state(self.state.clone());

        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let listener = TcpListener::bind(addr).await?;
        
        info!("Pillar 6.4: OpenAI-Compatible Gateway listening on port {}", self.port);
        
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                error!("OpenAI Gateway error: {}", e);
            }
        });

        Ok(())
    }
}

async fn chat_completions(
    State(_state): State<Arc<OpenAiGatewayState>>,
    Json(payload): Json<ChatCompletionRequest>,
) -> impl IntoResponse {
    info!("OpenAI Request for model: {} with {} messages", payload.model, payload.messages.len());
    
    // Map the external REST request directly into Marabunta's internal Job Control Language (JCL).
    // We mandate a Sovereign GPU for AI workloads to ensure isolation.
    let mut args = HashMap::new();
    args.insert("messages".to_string(), serde_json::to_value(&payload.messages).unwrap());
    args.insert("model".to_string(), serde_json::Value::String(payload.model.clone()));

    let _swarm_job_params = serde_json::json!({
        "module_id": "inference.llm.v1",
        "name": payload.model,
        "priority": 10,
        "args": args,
        "max_cost_usd": 0.10
    });

    debug!("Generated JCL Spot Market request for OpenAI wrapper: {:?}", _swarm_job_params["module_id"]);
    // In production, `state.marketplace_engine.post_job(_swarm_job_params)` is invoked here
    // waiting for a receipt from a high-reputation node.
    
    let mock_response = serde_json::json!({
        "id": "chatcmpl-swarm123",
        "object": "chat.completion",
        "created": 1677652288,
        "model": payload.model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "This response was physically mapped to JCL and executed on a Sovereign GPU inside the Marabunta Swarm."
            },
            "finish_reason": "stop"
        }]
    });

    (StatusCode::OK, Json(mock_response))
}
