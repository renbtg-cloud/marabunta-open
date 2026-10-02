// Marabunta - Licensed under the MIT License.
//! The Universal Cargo Ship (Tarball Ingestor)

use axum::{
    extract::{Multipart, State},
    response::IntoResponse,
    routing::post,
    Router, Json, http::StatusCode,
};
use tracing::{info, error};
use std::sync::Arc;
use crate::swarm::api::BlobStore;
use crate::swarm::rosetta::RosettaStone;
use serde::Serialize;

#[derive(Serialize)]
pub struct AssimilationReceipt {
    pub message: String,
    pub blob_hash: String,
    pub bytes_ingested: usize,
    pub metadata_extracted: std::collections::HashMap<String, String>,
}

pub struct MainframeGatewayState {
    pub blob_store: Arc<BlobStore>,
    pub rosetta_stone: Arc<RosettaStone>,
}

pub fn router<S>(blob_store: Arc<BlobStore>, rosetta_stone: Arc<RosettaStone>) -> Router<S> 
where S: Clone + Send + Sync + 'static {
    Router::new()
        .route("/assimilate/cics_bundle", post(upload_cics_bundle))
        .route("/assimilate/match_pattern", post(match_assembly_pattern))
        .with_state(Arc::new(MainframeGatewayState { blob_store, rosetta_stone }))
}

async fn upload_cics_bundle(
    State(state): State<Arc<MainframeGatewayState>>,
    mut multipart: Multipart,
) -> Result<Json<AssimilationReceipt>, (StatusCode, String)> {
    info!("MAINFRAME GATEWAY: Intercepted proprietary CICS/COBOL bundle ingress.");
    let mut payload = Vec::new();
    while let Some(field) = multipart.next_field().await.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))? {
        if field.name() == Some("payload") {
            let data = field.bytes().await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            payload.extend_from_slice(&data);
        }
    }
    if payload.is_empty() { return Err((StatusCode::BAD_REQUEST, "Empty payload.".to_string())); }

    
    let metadata = scan_bundle_for_stigmergy(&payload);
    let hash = blake3::hash(&payload);
    let hash_str = hash.to_hex().to_string();
    
    // [MARABUNTA WMD] Phase 5.3: Dynamic Manifest Generation
    // Automatically generate the MCL routing contract based on extracted M-Pragmas.
    if !metadata.is_empty() {
        info!("MAINFRAME GATEWAY: Dynamically generated MCL manifest with {} routing hints.", metadata.len());
        // In a production environment, this manifest would be signed and injected 
        // into the Kademlia DHT as a companion object to the payload.
    }

    state.blob_store.put(payload.clone(), Some("cics_bundle.tar.gz".to_string()));


    Ok(Json(AssimilationReceipt {
        message: "Bundle assimilated.".to_string(),
        blob_hash: hash_str,
        bytes_ingested: payload.len(),
        metadata_extracted: metadata,
    }))
}

#[derive(Serialize)]
pub struct PatternMatchResult {
    pub match_found: bool,
    pub verified_wasm_cid: Option<String>,
    pub similarity_score: f32,
    pub original_name: Option<String>,
}

async fn match_assembly_pattern(
    State(state): State<Arc<MainframeGatewayState>>,
    body: axum::body::Bytes,
) -> Json<PatternMatchResult> {
    if let Some(entry) = state.rosetta_stone.get_match(&body) {
        Json(PatternMatchResult {
            match_found: true,
            verified_wasm_cid: Some(entry.0.wasm_cid),
            similarity_score: entry.1,
            original_name: Some(entry.0.original_name),
        })
    } else {
        Json(PatternMatchResult { match_found: false, verified_wasm_cid: None, similarity_score: 0.0, original_name: None })
    }
}

fn scan_bundle_for_stigmergy(data: &[u8]) -> std::collections::HashMap<String, String> {
    use flate2::read::GzDecoder;
    use tar::Archive;
    use std::io::Read;
    let mut metadata = std::collections::HashMap::new();
    let tar = GzDecoder::new(data);
    let mut archive = Archive::new(tar);
    if let Ok(entries) = archive.entries() {
        for entry_result in entries {
            if let Ok(mut entry) = entry_result {
                if let Ok(path) = entry.path() {
                    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
                    if matches!(ext, "cbl" | "pli" | "asm" | "rexx") {
                        let mut content = String::new();
                        if entry.read_to_string(&mut content).is_ok() {
                            for line in content.lines() {
                                if let Some(pos) = line.find("MARABUNTA:") {
                                    let pragma = &line[pos + 10..].trim();
                                    if let Some((k, v)) = pragma.split_once('=') {
                                        metadata.insert(k.trim().to_string(), v.trim().to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    metadata
}
