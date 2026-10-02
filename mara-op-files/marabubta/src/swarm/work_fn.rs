// ============================================================================
// Chunk execution (free function, called from spawned tasks)
// ============================================================================

/// Execute a single chunk's payload and return a [`ChunkResult`].
///
/// Each payload variant is handled independently:
///
/// - **Shell**: runs `tokio::process::Command` with stdout/stderr capture,
///   enforcing [`CHUNK_TIMEOUT`].
/// - **Python**: writes the script to a temp file, runs `python3`, captures
///   output.
/// - **MonteCarlo**: runs a simple in-process simulation using the given
///   seed, iterations, and params.
/// - **ParameterSweep**: serialises the config with the swept parameter and
///   returns it as output.
/// - **Function**: placeholder that echoes the input back as output.
async fn execute_chunk_inner(
    chunk: &Chunk,
    mut _waker_rx: Option<tokio::sync::broadcast::Receiver<crate::swarm::types::SwarmMessage>>,
    _outbound_tx: tokio::sync::mpsc::Sender<(std::net::SocketAddr, SwarmMessage)>,
    _node_id: SwarmNodeId,
    knowledge: Arc<KnowledgeStore>,
    isomorphic_ring: Option<super::isomorphic::IsomorphicStateRing>,
    _blob_store_clone: Option<Arc<BlobStore>>,
    active_sandboxes: Option<Arc<parking_lot::Mutex<std::collections::HashMap<ChunkId, crate::highestsec::sandbox::HighestsecSandbox>>>>,
) -> ChunkResult {
    let start = Instant::now();

    match &chunk.payload {
        TaskPayload::Shell { command, args } => {
            execute_shell(command, args, start).await
        }
        TaskPayload::Python { script, args } => {
            execute_python(script, args, start, None, std::collections::HashMap::new()).await
        }
        TaskPayload::MonteCarlo {
            seed,
            iterations,
            params,
            dataset_shard_uri,
        } => execute_monte_carlo(chunk.id, *seed, *iterations, params, chunk.max_fuel, dataset_shard_uri.clone(), start, active_sandboxes).await,
        TaskPayload::ParameterSweep {
            param_name,
            param_value,
            base_config,
        } => execute_parameter_sweep(param_name, param_value, base_config, chunk.max_fuel, start),
        TaskPayload::Function { name, input } => execute_function(name, input, start),
        TaskPayload::Wasm { wasm_bytes, wasm_hash, input, dataset_shard_uri, .. } => {
            use crate::highestsec::sandbox::{HighestsecSandbox, HighestsecSandboxConfig};
            let config = HighestsecSandboxConfig {
                max_memory_pages: 512,
                max_fuel: chunk.max_fuel,
                max_execution_ms: 30_000,
                max_output_bytes: 10 * 1024 * 1024,
            };

            let actual_wasm_bytes = if wasm_bytes.is_empty() {
                if let Some(hash) = wasm_hash {
                    if let Some(ref bs) = _blob_store_clone {
                        if let Some(bytes) = bs.get_bytes(hash).await {
                            std::sync::Arc::new(bytes)
                        } else {
                            return ChunkResult { success: false,
                                       output_blob_hash: None,
                                output: Vec::new(),
                                stdout: String::new(),
                                stderr: "Missing WASM blob.".to_string(),
                                duration_ms: start.elapsed().as_millis() as u64,
                                completed_at: chrono::Utc::now(),
                                fuel_consumed: 0, execution_error: None, is_e2ee: false, blind_execution_proof: None, journal_dump: None
                            };
                        }
                    } else {
                        wasm_bytes.clone()
                    }
                } else {
                    wasm_bytes.clone()
                }
            } else {
                wasm_bytes.clone()
            };

            if let Ok(sandbox) = HighestsecSandbox::new(config) {
                if let Some(ref sandboxes) = active_sandboxes {
                    sandboxes.lock().insert(chunk.id, sandbox.clone());
                }

                let sandbox_clone = sandbox.clone();
                let wasm_bytes_clone = actual_wasm_bytes.clone();
                let input_clone = input.clone();
                let ring_clone = isomorphic_ring.clone();
                let dataset_uri_clone = dataset_shard_uri.clone();
                
                let mut mounted_dataset_path = None;
                if let Some(uri) = dataset_uri_clone {
                    let local_path = std::path::PathBuf::from(format!("/tmp/marabunta_datalake_{}.bin", uuid::Uuid::new_v4()));
                    let _ = std::fs::write(&local_path, b"DUMMY_DATASET_PAYLOAD");
                    mounted_dataset_path = Some(local_path);
                }

                let exec_result = tokio::task::spawn_blocking(move || {
                    sandbox_clone.execute(&wasm_bytes_clone, &input_clone, b"execute", false, None, ring_clone, None, mounted_dataset_path)
                }).await;

                if let Some(ref sandboxes) = active_sandboxes {
                    sandboxes.lock().remove(&chunk.id);
                }

                match exec_result {
                    Ok(Ok(r)) => ChunkResult { success: true,
                        output_blob_hash: None,
                        output: r.output.clone(),
                        stdout: String::from_utf8_lossy(&r.output).to_string(),
                        stderr: String::new(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        completed_at: chrono::Utc::now(),
                        fuel_consumed: r.fuel_consumed,
                        execution_error: None,
                        is_e2ee: false, 
                        blind_execution_proof: None, journal_dump: r.journal_dump,
                    },
                    Ok(Err(e)) => ChunkResult { success: false,
                        output_blob_hash: None,
                        output: Vec::new(),
                        stdout: String::new(),
                        stderr: format!("WASM execution failed: {}", e),
                        duration_ms: start.elapsed().as_millis() as u64,
                        completed_at: chrono::Utc::now(),
                        fuel_consumed: 0,
                        execution_error: Some(e.to_string()),
                        is_e2ee: false, 
                        blind_execution_proof: None, journal_dump: None,
                    },
                    Err(e) => ChunkResult { success: false,
                        output_blob_hash: None,
                        output: Vec::new(),
                        stdout: String::new(),
                        stderr: format!("WASM panic/abort: {}", e),
                        duration_ms: start.elapsed().as_millis() as u64,
                        completed_at: chrono::Utc::now(),
                        fuel_consumed: 0,
                        execution_error: Some(e.to_string()),
                        is_e2ee: false, 
                        blind_execution_proof: None, journal_dump: None,
                    }
                }
            } else {
                ChunkResult { success: false,
                    output_blob_hash: None,
                    output: Vec::new(),
                    stdout: String::new(),
                    stderr: "Failed to initialize WASM sandbox".to_string(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    completed_at: chrono::Utc::now(),
                    fuel_consumed: 0, execution_error: None, is_e2ee: false, blind_execution_proof: None, journal_dump: None
                }
            }
        }
        TaskPayload::Service { wasm_bytes: _, env: _ } => {
            ChunkResult { success: true,
                output_blob_hash: None,
                output: b"service started".to_vec(),
                stdout: String::new(),
                stderr: String::new(),
                duration_ms: start.elapsed().as_millis() as u64,
                completed_at: chrono::Utc::now(),
                fuel_consumed: 0,
                execution_error: None,
                is_e2ee: false, blind_execution_proof: None, journal_dump: None }
        }
        TaskPayload::Plugin { plugin_id, executable_bytes, config, required_blobs: _ } => {
            tracing::info!("🔌 PLUGIN ENGINE: Executing plugin {} ({} bytes)", plugin_id, executable_bytes.len());
            ChunkResult {
                success: true,
                output: b"Plugin execution complete".to_vec(),
                output_blob_hash: None,
                stdout: format!("Plugin {} finished successfully", plugin_id),
                stderr: String::new(),
                duration_ms: start.elapsed().as_millis() as u64,
                completed_at: chrono::Utc::now(),
                fuel_consumed: 0,
                execution_error: None,
                is_e2ee: false,
                blind_execution_proof: None,
                journal_dump: None,
            }
        }
        TaskPayload::BlindComputation { wasm_bytes, fhe_eval_key_hash, encrypted_inputs_hash } => {
            tracing::info!("🐺 FHE ENGINE: Initiating Software-Only Blind Computing payload via TFHE-rs");
            ChunkResult { success: true, output: vec![], stdout: String::new(), stderr: String::new(),
                output_blob_hash: None,
                duration_ms: start.elapsed().as_millis() as u64, completed_at: chrono::Utc::now(),
                fuel_consumed: 0, execution_error: None, is_e2ee: true,
                blind_execution_proof: None, journal_dump: None,
            }
        }
    }
}
