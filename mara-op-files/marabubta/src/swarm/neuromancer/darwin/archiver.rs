// Marabunta - Licensed under the MIT License.
//! Artifact Archiver for Darwin: Compressing and storing crash snapshots.

use async_trait::async_trait;
use crate::swarm::neuromancer::types::NeuromancerError;
use crate::swarm::neuromancer::darwin::{RemediationHook, FailureContext, HookDecision};
use std::io::Write;

pub struct ArtifactArchiver {
    pub storage_path: String,
}

#[async_trait]
impl RemediationHook for ArtifactArchiver {
    fn name(&self) -> &'static str { "artifact_archiver" }

    async fn on_failure(&self, context: &mut FailureContext) -> Result<HookDecision, NeuromancerError> {
        // 1. Compress the memory dump using zstd
        let mut encoder = zstd::Encoder::new(Vec::new(), 3).map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        encoder.write_all(&context.snapshot.memory_dump).map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        let compressed_dump = encoder.finish().map_err(|e| NeuromancerError::Internal(e.to_string()))?;

        // 2. Store the artifact (local filesystem for now)
        let artifact_name = format!("mrb-ttdump-{}.zst", context.node_id);
        let full_path = format!("{}/{}", self.storage_path, artifact_name);
        std::fs::write(&full_path, compressed_dump).map_err(|e| NeuromancerError::Internal(e.to_string()))?;

        // 3. Update context
        context.artifact_url = Some(full_path);
        
        Ok(HookDecision::Continue)
    }
}
