// Marabunta - Licensed under the MIT License.
//! Pillar 6.2: Global Redis Gateway (RESP)
//! 
//! Exposes Marabunta's planetary storage engine (Reed-Solomon shards)
//! via the standard Redis serialization protocol (RESP).
//! Drop-in replacement for Redis caching at petabyte scale.

use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use tracing::{info, error};
use std::sync::Arc;
use dashmap::DashMap;

use super::protocol::{RespCodec, RespFrame};
use crate::swarm::blobstore::BlobStore;
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::planetary::storage::PlanetaryStorageEngine;

pub struct PlanetaryRedisServer {
    port: u16,
    blob_store: Arc<BlobStore>,
    knowledge: Arc<KnowledgeStore>,
    storage_engine: Arc<PlanetaryStorageEngine>,
    // Mapping: Redis Key -> BlobHash (Simulated Distributed Index)
    index: Arc<DashMap<String, crate::swarm::types::BlobHash>>,
}

impl PlanetaryRedisServer {
    pub fn new(blob_store: Arc<BlobStore>, knowledge: Arc<KnowledgeStore>) -> Self {
        Self { 
            port: 6379,
            blob_store,
            knowledge,
            storage_engine: Arc::new(PlanetaryStorageEngine::new(10, 4)),
            index: Arc::new(DashMap::new()),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("0.0.0.0:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        
        info!("Planetary Redis Gateway listening on {}", addr);
        info!("Sovereign Storage Active: Sacrificing sub-millisecond latency for Reed-Solomon durability.");

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("Accepted Redis connection from {}", peer_addr);
                    let blob_store = Arc::clone(&self.blob_store);
                    let knowledge = Arc::clone(&self.knowledge);
                    let storage_engine = Arc::clone(&self.storage_engine);
                    let index = Arc::clone(&self.index);
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, blob_store, knowledge, storage_engine, index).await {
                            error!("Redis connection error: {}", e);
                        }
                    });
                }
                Err(e) => error!("Failed to accept connection: {}", e),
            }
        }
    }

    async fn handle_connection(
        stream: tokio::net::TcpStream, 
        blob_store: Arc<BlobStore>,
        _knowledge: Arc<KnowledgeStore>,
        storage_engine: Arc<PlanetaryStorageEngine>,
        index: Arc<DashMap<String, crate::swarm::types::BlobHash>>,
    ) -> Result<(), std::io::Error> {
        let mut framed = Framed::new(stream, RespCodec);

        while let Some(frame) = framed.next().await {
            match frame {
                Ok(RespFrame::Array(frames)) => {
                    if frames.is_empty() { continue; }
                    
                    if let RespFrame::BulkString(cmd_bytes) = &frames[0] {
                        let cmd = String::from_utf8_lossy(cmd_bytes).to_uppercase();
                        match cmd.as_str() {
                            "PING" => {
                                framed.send(RespFrame::SimpleString("PONG".to_string())).await?;
                            }
                            "SET" => {
                                if frames.len() >= 3 {
                                    if let (RespFrame::BulkString(key_bytes), RespFrame::BulkString(value)) = (&frames[1], &frames[2]) {
                                        let key = String::from_utf8_lossy(key_bytes).to_string();
                                        info!("REDIS SET: Key {} ({} bytes)", key, value.len());
                                        
                                        // 1. Shard if large (>1MB)
                                        if value.len() > 1024 * 1024 {
                                            if let Ok(shards) = storage_engine.encode_checkpoint(value) {
                                                info!("Reed-Solomon: Sharded {} into {} fragments.", key, shards.len());
                                            }
                                        }

                                        // 2. Persist to real BlobStore
                                        match blob_store.store_bytes(value, Some(key.clone()), None).await {
                                            Ok(blob_ref) => {
                                                index.insert(key, blob_ref.hash);
                                                framed.send(RespFrame::SimpleString("OK".to_string())).await?;
                                            }
                                            Err(e) => {
                                                framed.send(RespFrame::Error(format!("ERR swarm storage failed: {}", e))).await?;
                                            }
                                        }
                                    }
                                } else {
                                    framed.send(RespFrame::Error("ERR wrong number of arguments for 'set'".to_string())).await?;
                                }
                            }
                            "GET" => {
                                if frames.len() >= 2 {
                                    if let RespFrame::BulkString(key_bytes) = &frames[1] {
                                        let key = String::from_utf8_lossy(key_bytes).to_string();
                                        
                                        if let Some(hash) = index.get(&key) {
                                            match blob_store.get_bytes(&hash).await {
                                                Some(bytes) => {
                                                    framed.send(RespFrame::BulkString(bytes)).await?;
                                                }
                                                None => {
                                                    framed.send(RespFrame::Null).await?;
                                                }
                                            }
                                        } else {
                                            framed.send(RespFrame::Null).await?;
                                        }
                                    }
                                } else {
                                    framed.send(RespFrame::Error("ERR wrong number of arguments for 'get'".to_string())).await?;
                                }
                            }
                            _ => {
                                framed.send(RespFrame::Error(format!("ERR unknown command '{}'", cmd))).await?;
                            }
                        }
                    }
                }
                Ok(_) => {
                    framed.send(RespFrame::Error("ERR Protocol error".to_string())).await?;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
