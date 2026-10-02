// Marabunta - Licensed under the MIT License.
//! Phase 3: GitOps Dotted Version Vectors
//!
//! Manages concurrent financial logging across partitioned network segments.
//! Uses Dotted Version Vectors to ensure that JSON execution artifacts (e.g., MMX cost,
//! node UUIDs) merge cleanly into the central `git` repository upon partition healing.

use git2::{Repository, Signature, IndexAddOption};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, error};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DottedVersionVector {
    pub clocks: HashMap<String, u64>, // NodeID -> Sequence Number
}

impl DottedVersionVector {
    pub fn increment(&mut self, node_id: &str) {
        let count = self.clocks.entry(node_id.to_string()).or_insert(0);
        *count += 1;
    }
}

pub struct GitOpsArchiver {
    repo_path: String,
    local_node_id: String,
    vector_clock: std::sync::Mutex<DottedVersionVector>,
}

impl GitOpsArchiver {
    pub fn new(repo_path: &str, local_node_id: &str) -> Self {
        Self {
            repo_path: repo_path.to_string(),
            local_node_id: local_node_id.to_string(),
            vector_clock: std::sync::Mutex::new(DottedVersionVector::default()),
        }
    }

    pub fn archive_query(
        &self,
        job_id: &str,
        jcl: &str,
        _payload: &str,
        prediction_cost: &str,
    ) -> Result<String, ()> {
        let repo = match Repository::open(&self.repo_path) {
            Ok(r) => r,
            Err(e) => {
                error!("GitOps: Failed to open repository at {}: {}", self.repo_path, e);
                return Err(());
            }
        };

        let mut clock = self.vector_clock.lock().unwrap();
        clock.increment(&self.local_node_id);
        let seq = *clock.clocks.get(&self.local_node_id).unwrap();

        let artifact = serde_json::json!({
            "job_id": job_id,
            "jcl_command": jcl,
            "projected_cost_usd": prediction_cost,
            "vector_clock": *clock,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        let file_path = format!("{}/jobs/{}_{}.json", self.repo_path, job_id, seq);
        let _ = std::fs::create_dir_all(format!("{}/jobs", self.repo_path));
        let _ = std::fs::write(&file_path, serde_json::to_string_pretty(&artifact).unwrap());

        let mut index = repo.index().map_err(|_| ())?;
        let _ = index.add_all(["jobs/*"].iter(), IndexAddOption::DEFAULT, None);
        let oid = index.write_tree().map_err(|_| ())?;
        let tree = repo.find_tree(oid).map_err(|_| ())?;

        let branch_name = format!("refs/heads/partition_{}", self.local_node_id);
        let sig = Signature::now("Marabunta Visor", "visor@cambrianradiation.com").unwrap();
        
        let parent_commit = repo.find_reference(&branch_name).and_then(|r| r.peel_to_commit()).ok();
        let mut parents = Vec::new();
        if let Some(p) = &parent_commit {
            parents.push(p);
        }

        let commit_id = repo.commit(
            Some(&branch_name),
            &sig,
            &sig,
            &format!("feat(finops): Archive job {} via Dotted Version Vector", job_id),
            &tree,
            &parents.to_vec(),
        ).map_err(|_| ())?;

        info!("GitOps: Successfully committed {} to branch partition_{}", commit_id, self.local_node_id);
        Ok(commit_id.to_string())
    }
}
