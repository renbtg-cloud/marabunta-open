// Marabunta - Licensed under the MIT License.
use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use reqwest::StatusCode;

use crate::swarm::api::{ApiState, ApiError};
use crate::chaos::types::DeathType;

#[derive(Deserialize)]
pub struct ChaosStrikeRequest {
    #[serde(default)]
    pub target_nodes: Vec<String>,
    #[serde(default)]
    pub target_region: Option<String>,
    pub death_type: DeathType,
}

#[derive(Deserialize)]
pub struct ChaosReviveRequest {
    #[serde(default)]
    pub target_nodes: Vec<String>,
    #[serde(default)]
    pub target_region: Option<String>,
}

#[derive(Serialize)]
pub struct ChaosStatusResponse {
    pub node_id: String,
    pub is_active: bool,
    pub current_death: Option<DeathType>,
    pub elapsed_ms: Option<u128>,
}

// --- Handlers ---

pub async fn issue_strike(
    headers: axum::http::HeaderMap,
    State(state): State<ApiState>,
    Json(payload): Json<ChaosStrikeRequest>,
) -> Result<impl IntoResponse, ApiError> {
    crate::swarm::api::require_permission(&state, &headers, "admin")?;

    let mut target_node_ids = Vec::new();
    for id_str in &payload.target_nodes {
        let uuid = uuid::Uuid::parse_str(id_str)
            .map_err(|_| ApiError::BadRequest(format!("invalid node ID: {}", id_str)))?;
        target_node_ids.push(crate::swarm::types::NodeId(uuid));
    }

    let msg = crate::swarm::types::SwarmMessage::ChaosStrike {
        target_nodes: target_node_ids.clone(),
        target_region: payload.target_region.clone(),
        death_type: payload.death_type.clone(),
        issuer_signature: vec![], // Simulated signature for War Games
        from: state.node_id,
    };

    // To broadcast, we inject it directly into the outbound sender with a placeholder target
    let placeholder_addr: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();
    let _ = state.work_engine.outbound_tx().send((placeholder_addr, msg)).await;
    
    // Apply locally if applicable
    let should_strike = target_node_ids.contains(&state.node_id) || payload.target_region.as_deref().map_or(false, |r| {
        let profile = state.work_engine.profile().read();
        profile.geo_region.as_deref() == Some(r)
    });
    
    if should_strike {
        state.chaos_engine.strike(payload.death_type);
    }

    Ok((axum::http::StatusCode::ACCEPTED, Json(serde_json::json!({ "status": "strike issued" }))))
}

pub async fn issue_revive(
    headers: axum::http::HeaderMap,
    State(state): State<ApiState>,
    Json(payload): Json<ChaosReviveRequest>,
) -> Result<impl IntoResponse, ApiError> {
    crate::swarm::api::require_permission(&state, &headers, "admin")?;

    let mut target_node_ids = Vec::new();
    for id_str in &payload.target_nodes {
        let uuid = uuid::Uuid::parse_str(id_str)
            .map_err(|_| ApiError::BadRequest(format!("invalid node ID: {}", id_str)))?;
        target_node_ids.push(crate::swarm::types::NodeId(uuid));
    }

    let msg = crate::swarm::types::SwarmMessage::ChaosRevive {
        target_nodes: target_node_ids.clone(),
        target_region: payload.target_region.clone(),
        issuer_signature: vec![], // Simulated signature for War Games
        from: state.node_id,
    };

    let placeholder_addr: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();
    let _ = state.work_engine.outbound_tx().send((placeholder_addr, msg)).await;
    
    // Apply locally if applicable
    let should_revive = target_node_ids.contains(&state.node_id) || payload.target_region.as_deref().map_or(false, |r| {
        let profile = state.work_engine.profile().read();
        profile.geo_region.as_deref() == Some(r)
    });
    
    if should_revive {
        state.chaos_engine.revive();
    }

    Ok((axum::http::StatusCode::ACCEPTED, Json(serde_json::json!({ "status": "revive issued" }))))
}

pub async fn get_status(
    headers: axum::http::HeaderMap,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, ApiError> {
    crate::swarm::api::require_permission(&state, &headers, "view_nodes")?;

    let st = state.chaos_engine.get_state();
    let elapsed_ms = st.started_at.and_then(|s| s.elapsed().ok()).map(|d| d.as_millis());

    Ok(Json(ChaosStatusResponse {
        node_id: state.node_id.0.to_string(),
        is_active: st.is_active,
        current_death: st.current_death,
        elapsed_ms,
    }))
}
