// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PlacementHint {
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamSeed {
    pub value: u64,
    pub task_type: String,
    pub predicted_input: Vec<u8>,
    pub confidence: f32,
    pub reason: String,
}

impl PlacementHint {
    pub fn any() -> Self {
        Self::default()
    }
}
