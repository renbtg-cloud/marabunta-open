// Marabunta - Licensed under the MIT License.
//! Stigmergic Rosetta Stone: The Assembly Pattern Ledger (Phase 4.1).

use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use crate::swarm::crdt::EpidemicStateMap;
use crate::swarm::types::IsoKey;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RosettaEntry {
    pub wasm_cid: String,
    pub original_name: String,
    pub source_metadata: String,
    pub lsh_signature: Vec<u64>,
}

pub struct RosettaStone {
    patterns: Arc<RwLock<EpidemicStateMap<IsoKey, RosettaEntry>>>,
}

impl RosettaStone {
    pub fn new() -> Self {
        Self {
            patterns: Arc::new(RwLock::new(EpidemicStateMap::new())),
        }
    }

    pub fn register_pattern(&self, asm_opcodes: &[u8], entry_metadata: RosettaEntry, node_id: String) {
        let hash = blake3::hash(asm_opcodes);
        let key: IsoKey = *hash.as_bytes();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
        
        let mut entry = entry_metadata;
        entry.lsh_signature = self.compute_lsh(asm_opcodes);

        self.patterns.write().insert(key, entry, timestamp, node_id);
        tracing::info!("ROSETTA STONE: New legacy assembly pattern registered.");
    }

    pub fn get_match(&self, target_opcodes: &[u8]) -> Option<(RosettaEntry, f32)> {
        let target_lsh = self.compute_lsh(target_opcodes);
        let patterns = self.patterns.read();
        
        let mut best_match: Option<(RosettaEntry, f32)> = None;

        for (_, reg) in patterns.iter() {
            let similarity = self.calculate_jaccard_similarity(&target_lsh, &reg.value.lsh_signature);
            
            if similarity >= 0.95 {
                if best_match.as_ref().map_or(true, |(_, s)| similarity > *s) {
                    best_match = Some((reg.value.clone(), similarity));
                }
            }
        }
        best_match
    }

    pub fn compute_lsh(&self, opcodes: &[u8]) -> Vec<u64> {
        let mut shingles = Vec::new();
        if opcodes.len() < 4 {
            let h = blake3::hash(opcodes);
            return vec![u64::from_le_bytes(h.as_bytes()[0..8].try_into().unwrap())];
        }
        
        for window in opcodes.windows(4) {
            let h = blake3::hash(window);
            let val = u64::from_le_bytes(h.as_bytes()[0..8].try_into().unwrap());
            shingles.push(val);
        }
        
        shingles.sort_unstable();
        shingles.dedup();
        shingles.into_iter().take(8).collect()
    }

    fn calculate_jaccard_similarity(&self, a: &[u64], b: &[u64]) -> f32 {
        if a.is_empty() || b.is_empty() { return 0.0; }
        let mut intersection = 0;
        for val_a in a {
            if b.contains(val_a) { intersection += 1; }
        }
        let union = (a.len() + b.len() - intersection) as f32;
        (intersection as f32) / union
    }
}

impl Default for RosettaStone {
    fn default() -> Self {
        Self::new()
    }
}
