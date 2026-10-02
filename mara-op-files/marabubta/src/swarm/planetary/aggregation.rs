// Marabunta - Licensed under the MIT License.
//! Phase 3.1: In-Network Aggregation Trees
//! Prevents Thundering Herd DDOS by reducing chunk results at regional Relays.

use serde::{Deserialize, Serialize};
use tracing::info;
use wasmtime::{Engine, Linker, Module, Store};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationRole {
    LeafWorker,
    RegionalRelay { child_count: u32 },
    GlobalOrigin,
}

pub trait ReductionTree {
    fn assign_parent_relay(&self, origin_id: [u8; 32]) -> [u8; 32];
    fn execute_reduce_in_place(&self, reduce_wasm: &[u8], payloads: Vec<Vec<u8>>) -> Result<Vec<u8>, &'static str>;
}

pub struct MapReduceOrchestrator {
    pub role: AggregationRole,
    engine: Engine,
}

impl MapReduceOrchestrator {
    pub fn new(role: AggregationRole) -> Self {
        Self {
            role,
            engine: Engine::default(),
        }
    }
}

impl ReductionTree for MapReduceOrchestrator {
    fn assign_parent_relay(&self, origin_id: [u8; 32]) -> [u8; 32] {
        // Deterministic relay assignment via XOR distance (Kademlia logic)
        // In a full implementation, we find the closest known relay in Bucket 1
        let mut mock_relay = [0u8; 32];
        for i in 0..32 {
            mock_relay[i] = origin_id[i] ^ 0x42; 
        }
        mock_relay
    }

    fn execute_reduce_in_place(&self, reduce_wasm: &[u8], payloads: Vec<Vec<u8>>) -> Result<Vec<u8>, &'static str> {
        info!("Executing In-Network Reduce on {} child payloads...", payloads.len());
        
        let mut store = Store::new(&self.engine, ());
        let module = Module::new(&self.engine, reduce_wasm).map_err(|_| "Failed to compile REDUCE module")?;
        let linker = Linker::new(&self.engine);

        let instance = linker.instantiate(&mut store, &module).map_err(|_| "Failed to instantiate REDUCE module")?;
        let memory = instance.get_memory(&mut store, "memory").ok_or("No exported memory")?;

        // 1. Concatenate all child payloads into the WASM linear memory
        let mut offset = 0;
        for payload in &payloads {
            memory.write(&mut store, offset, payload).map_err(|_| "Failed to write payload to memory")?;
            offset += payload.len();
        }

        // 2. Execute the user-provided REDUCE block
        let reduce_func = instance.get_typed_func::<(i32, i32), i32>(&mut store, "reduce")
            .map_err(|_| "REDUCE module must export `reduce(ptr, len) -> len`")?;

        let reduced_len = reduce_func.call(&mut store, (0, offset as i32)).map_err(|_| "WASM execution trap")?;

        // 3. Extract the single merged payload to pass up the tree
        let mut merged_output = vec![0u8; reduced_len as usize];
        memory.read(&store, 0, &mut merged_output).map_err(|_| "Failed to read output")?;

        info!("Reduce-in-Place complete. Compressed {} payloads into {} bytes.", payloads.len(), merged_output.len());
        Ok(merged_output)
    }
}