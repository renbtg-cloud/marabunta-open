use crate::swarm::blobstore::BlobStore;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::put,
    Router,
};
use std::sync::Arc;
use std::net::SocketAddr;

pub async fn start_s3_gateway(blob_store: Arc<BlobStore>, port: u16) {
    let app = Router::new()
        .route("/*path", put(handle_put_object))
        .with_state(blob_store);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Starting S3-Wire compatibility layer on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn handle_put_object(
    State(blob_store): State<Arc<BlobStore>>,
    Path(path): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    match blob_store.store_bytes(&body, Some(path.clone()), None).await {
        Ok(blob_ref) => {
            tracing::info!("S3-Wire: Stored object {} -> {}", path, crate::swarm::blobstore::hash_hex(&blob_ref.hash));
            StatusCode::OK
        }
        Err(e) => {
            tracing::error!("S3-Wire: Failed to store object {}: {}", path, e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
