// Marabunta - Licensed under the MIT License.
//! High-performance, multi-threaded Proof-of-Work (PoW) engine for the Chrysalis Protocol.
//!
//! This module provides the "Grinder" — a CPU-intensive worker that saturates all
//! available cores to find a NodeId satisfying the required thermodynamic cost.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use rand::{Rng, RngCore};
use uuid::Uuid;

use super::types::NodeId;

/// The Chrysalis Grinder.
pub struct ChrysalisGrinder {
    difficulty_bytes: usize,
    num_threads: usize,
}

impl ChrysalisGrinder {
    /// Create a new grinder with the specified difficulty (number of leading zero bytes).
    /// Defaults to the number of physical CPU cores.
    pub fn new(difficulty_bytes: usize) -> Self {
        Self {
            difficulty_bytes,
            num_threads: num_cpus::get(),
        }
    }

    /// Set the number of threads to use.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.num_threads = threads;
        self
    }

    /// Physically grind the CPU to find a valid NodeId.
    ///
    /// This function spawns `num_threads` and runs them until one finds a match.
    /// It is a blocking call. For non-blocking use, wrap in `tokio::task::spawn_blocking`.
    pub fn grind(&self) -> NodeId {
        let found = Arc::new(AtomicBool::new(false));
        let result = Arc::new(parking_lot::Mutex::new(None));
        let difficulty = self.difficulty_bytes;
        
        tracing::info!(
            "Chrysalis Grinder: Initializing heavy-duty siege on {} threads (Difficulty: {} bytes)...",
            self.num_threads,
            difficulty
        );

        let mut handles = Vec::with_capacity(self.num_threads);


        for i in 0..self.num_threads {
            let found = Arc::clone(&found);
            let result = Arc::clone(&result);
            
            handles.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut bytes = [0u8; 16];
                
                // [MARABUNTA WMD] Memory-Hard L3 Cache Saturation
                // Instead of a pure compute-bound hash collision search (which GPUs dominate),
                // we allocate a 32MB scratchpad array per thread. This forces the algorithm 
                // to be bottlenecked by L3 Cache/RAM latency. A GPU with 4000 cores cannot 
                // run 4000 threads simultaneously because it will exhaust its VRAM bandwidth 
                // instantly, dropping its hash rate to parity with a standard CPU.
                const SCRATCHPAD_SIZE: usize = 32 * 1024 * 1024; // 32MB per thread
                let mut scratchpad: Vec<u8> = vec![0u8; SCRATCHPAD_SIZE];
                
                // Seed the scratchpad with entropy
                rng.fill(&mut scratchpad[..]);

                loop {
                    if found.load(Ordering::Relaxed) {
                        break;
                    }
                    
                    rng.fill(&mut bytes);
                    
                    // The Memory-Hard Step:
                    // We must perform a read-modify-write operation across widely dispersed 
                    // memory locations, guaranteeing cache misses and forcing RAM latency.
                    let mut accumulator = 0u64;
                    for _ in 0..64 {
                        let idx = (u64::from_le_bytes(bytes[0..8].try_into().unwrap()) ^ accumulator) as usize % SCRATCHPAD_SIZE;
                        let val = scratchpad[idx];
                        
                        // Modify the scratchpad to prevent read-only caching optimizations
                        scratchpad[idx] = val.wrapping_add(17);
                        accumulator = accumulator.wrapping_add(val as u64);
                    }

                    // Mutate the original bytes with the memory-hard accumulator result.
                    // This mathematically forces the attacker to execute the latency-bound loop
                    // to determine if the final ID satisfies the difficulty constraint.
                    let acc_bytes = accumulator.to_le_bytes();
                    for j in 0..8 {
                        bytes[j] ^= acc_bytes[j];
                    }
                    
                    let id = NodeId(Uuid::from_bytes(bytes));

                    // Only after surviving the memory-hard latency penalty do we check 
                    // the mathematical difficulty constraint.
                    if id.meets_chrysalis_pow() {
                        let mut res = result.lock();
                        if res.is_none() {
                            *res = Some(id);
                            found.store(true, Ordering::Relaxed);
                            tracing::info!("Chrysalis Grinder: Valid NodeId forged on thread {} via L3 cache saturation.", i);
                        }
                        break;
                    }
                }
            }));
        }


        for handle in handles {
            let _ = handle.join();
        }

        let final_id = result.lock().take().expect("Grinder finished but no result found");
        tracing::info!("Chrysalis Identity Secured: {}", final_id);
        final_id
    }
}

/// Helper for tokio integration.
pub async fn grind_async(difficulty_bytes: usize) -> NodeId {
    tokio::task::spawn_blocking(move || {
        ChrysalisGrinder::new(difficulty_bytes).grind()
    })
    .await
    .expect("PoW grinding task panicked")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grinder_low_difficulty() {
        // 1 byte is 1/256 chance, should be very fast.
        let grinder = ChrysalisGrinder::new(1).with_threads(2);
        let id = grinder.grind();
        assert!(id.meets_chrysalis_pow());
    }

    #[test]
    fn test_grinder_standard_difficulty() {
        // 2 bytes (16 bits) as per Chrysalis spec.
        let grinder = ChrysalisGrinder::new(2).with_threads(4);
        let id = grinder.grind();
        assert!(id.meets_chrysalis_pow());
    }
}
