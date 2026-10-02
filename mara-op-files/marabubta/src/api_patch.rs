
#[derive(serde::Deserialize)]
pub struct AtonementRequest {
    pub node_id: String,
    pub pow_nonce: u64,
}

async fn submit_atonement(
    axum::extract::State(state): axum::extract::State<ApiState>,
    axum::Json(payload): axum::Json<AtonementRequest>,
) -> Result<axum::Json<serde_json::Value>, ApiError> {
    let node_id_uuid = uuid::Uuid::parse_str(&payload.node_id)
        .map_err(|_| ApiError::BadRequest("invalid node_id format".to_string()))?;
    let node_id = crate::swarm::types::NodeId(node_id_uuid);
    
    let msg = crate::swarm::types::SwarmMessage::SubmitAtonement {
        node_id,
        pow_nonce: payload.pow_nonce,
    };
    
    // Broadcast the atonement to the local neighbors so it propagates to the Ledger.
    let neighbors = state.knowledge.sample_nodes(5);
    for neighbor in neighbors {
        if let Some(addr) = neighbor.address {
            // Need a way to send outbound.
            // work_engine doesn`t expose outbound_tx directly, but let`s check.
            let _ = state.work_engine.outbound_tx.try_send((addr, msg.clone()));
        }
    }

    Ok(axum::Json(serde_json::json!({
        "status": "atonement_submitted",
        "node_id": payload.node_id,
        "nonce": payload.pow_nonce
    })))
}
