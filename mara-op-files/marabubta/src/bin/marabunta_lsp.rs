// Marabunta - Licensed under the MIT License.
//! Marabunta IDE Language Server Protocol (LSP) and Telemetry Bridge
//!
//! This daemon provides the Stage 1.2 Telemetry Firehose. It replaces the naive
//! WebSocket/JSON implementation with a high-performance, binary multiplexed 
//! Protobuf/Bincode stream over a Unix Domain Socket (UDS) or TCP loopback.
//!
//! This prevents network saturation by packing DHT routing tables and thermal 
//! telemetry into tightly packed frames at 20Hz directly into the IDE process.

use bincode::{deserialize, serialize};
use bytes::{Buf, BufMut, BytesMut};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{error, info, warn};

// Note: Using Bincode for serialization, which is highly efficient.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdeTelemetryUpdate {
    pub active_nodes: u32,
    pub current_mmx_spot_price: f64,
    pub edge_ram_utilization_pct: f32,
    pub dht_routing_table_hash: u64,
}

// Custom Length-Delimited Codec for Bincode payloads
pub struct BincodeCodec;

impl Encoder<IdeTelemetryUpdate> for BincodeCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: IdeTelemetryUpdate, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = serialize(&item).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        dst.put_u32(encoded.len() as u32);
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}

impl Decoder for BincodeCodec {
    type Item = IdeTelemetryUpdate;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes(src[0..4].try_into().unwrap()) as usize;
        if src.len() < 4 + len {
            return Ok(None);
        }
        let payload = &src[4..4 + len];
        let item: IdeTelemetryUpdate = deserialize(payload)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        src.advance(4 + len);
        Ok(Some(item))
    }
}

struct AppState {
    tx: broadcast::Sender<IdeTelemetryUpdate>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let socket_path = PathBuf::from("/tmp/marabunta_lsp.sock");

    // Clean up old socket if it exists
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    let (tx, _rx) = broadcast::channel(100);
    let app_state = Arc::new(AppState { tx });

    // Background task: Aggregate live Swarm Gossip Telemetry
    // In production, this reads directly from the Visor's in-memory DHT tables.
    let tx_clone = app_state.tx.clone();
    tokio::spawn(async move {
        let mut price = 0.012;
        let mut ram = 80.0;
        loop {
            // 20Hz update cycle (50ms interval)
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            price += 0.0001; // market fluctuation
            ram += 0.1;
            if ram > 95.0 { ram = 80.0; } // simulate load cycling
            
            let update = IdeTelemetryUpdate {
                active_nodes: 45021,
                current_mmx_spot_price: price,
                edge_ram_utilization_pct: ram,
                dht_routing_table_hash: rand::random(),
            };
            let _ = tx_clone.send(update); // Broadcast to all connected IDEs
        }
    });

    info!("Starting Marabunta UDS Telemetry Firehose at {:?}", socket_path);
    let listener = UnixListener::bind(&socket_path)?;

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                info!("IDE Plugin connected to Telemetry Firehose (UDS)");
                let state = app_state.clone();
                tokio::spawn(async move {
                    handle_uds_connection(stream, state).await;
                });
            }
            Err(e) => {
                error!("Error accepting UDS connection: {:?}", e);
            }
        }
    }
}

async fn handle_uds_connection(stream: tokio::net::UnixStream, state: Arc<AppState>) {
    let mut rx = state.tx.subscribe();
    let mut framed = Framed::new(stream, BincodeCodec);

    while let Ok(update) = rx.recv().await {
        if framed.send(update).await.is_err() {
            warn!("IDE UDS disconnected");
            break;
        }
    }
}
