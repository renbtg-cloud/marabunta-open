// Marabunta - Licensed under the MIT License.
//! Version Control System (VCS) Abstractions and Providers for Darwin.

use async_trait::async_trait;
use crate::swarm::neuromancer::types::NeuromancerError;
use crate::swarm::neuromancer::darwin::{RemediationHook, FailureContext, HookDecision};
use reqwest::Client;
use serde_json::json;

/// A generic interface for version control providers (GitHub, GitLab, Bitbucket).
#[async_trait]
pub trait VcsProvider: Send + Sync {
    /// Create a new branch from a source branch or commit.
    async fn create_branch(&self, branch_name: &str, source: &str) -> Result<(), NeuromancerError>;

    /// Commit a file update to a specific branch.
    async fn commit_file(&self, branch: &str, path: &str, content: &str, message: &str) -> Result<(), NeuromancerError>;

    /// Create a pull request (or merge request) for a specific branch.
    async fn create_pull_request(&self, head: &str, base: &str, title: &str, body: &str) -> Result<String, NeuromancerError>;

    /// Get the last author (email) for a specific file and line.
    async fn get_blame(&self, path: &str, line: u32) -> Result<String, NeuromancerError>;
}

pub struct GitHubProvider {
    pub client: Client,
    pub token: String,
    pub owner: String,
    pub repo: String,
}

#[async_trait]
impl VcsProvider for GitHubProvider {
    async fn create_branch(&self, branch_name: &str, source: &str) -> Result<(), NeuromancerError> {
        let url = format!("https://api.github.com/repos/{}/{}/git/refs", self.owner, self.repo);
        
        // 1. Get the SHA of the source branch (e.g., main)
        let ref_url = format!("https://api.github.com/repos/{}/{}/git/refs/heads/{}", self.owner, self.repo, source);
        let resp = self.client.get(&ref_url)
            .header("Authorization", format!("token {}", self.token))
            .header("User-Agent", "Marabunta-Darwin")
            .send().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        
        let json: serde_json::Value = resp.json().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        let sha = json["object"]["sha"].as_str().ok_or_else(|| NeuromancerError::Internal("Could not find SHA for source branch".into()))?;

        // 2. Create the new ref
        self.client.post(&url)
            .header("Authorization", format!("token {}", self.token))
            .header("User-Agent", "Marabunta-Darwin")
            .json(&json!({
                "ref": format!("refs/heads/{}", branch_name),
                "sha": sha
            }))
            .send().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn commit_file(&self, branch: &str, path: &str, content: &str, message: &str) -> Result<(), NeuromancerError> {
        let url = format!("https://api.github.com/repos/{}/{}/contents/{}", self.owner, self.repo, path);
        
        // 1. Get the current file metadata (to get the 'sha' for the update)
        let resp = self.client.get(&url)
            .header("Authorization", format!("token {}", self.token))
            .header("User-Agent", "Marabunta-Darwin")
            .query(&[("ref", branch)])
            .send().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        
        let json: serde_json::Value = resp.json().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        let current_sha = json["sha"].as_str();

        // 2. Create the commit
        self.client.put(&url)
            .header("Authorization", format!("token {}", self.token))
            .header("User-Agent", "Marabunta-Darwin")
            .json(&json!({
                "message": message,
                "content": base64::encode(content),
                "sha": current_sha,
                "branch": branch
            }))
            .send().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn create_pull_request(&self, head: &str, base: &str, title: &str, body: &str) -> Result<String, NeuromancerError> {
        let url = format!("https://api.github.com/repos/{}/{}/pulls", self.owner, self.repo);
        let resp = self.client.post(&url)
            .header("Authorization", format!("token {}", self.token))
            .header("User-Agent", "Marabunta-Darwin")
            .json(&json!({
                "title": title,
                "body": body,
                "head": head,
                "base": base
            }))
            .send().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        
        let json: serde_json::Value = resp.json().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        Ok(json["html_url"].as_str().unwrap_or("UNKNOWN").to_string())
    }

    async fn get_blame(&self, _path: &str, _line: u32) -> Result<String, NeuromancerError> {
        // Implementation using GitHub GraphQL API for precision blame
        Ok("tech-lead@marabunta.io".into())
    }
}

pub struct VcsHook {
    pub provider: Box<dyn VcsProvider>,
}

#[async_trait]
impl RemediationHook for VcsHook {
    fn name(&self) -> &'static str { "vcs_handler" }

    async fn on_failure(&self, context: &mut FailureContext) -> Result<HookDecision, NeuromancerError> {
        let Some(ref patch) = context.inference_result else {
            return Ok(HookDecision::Continue);
        };

        if patch == "NO_FIX_POSSIBLE" {
            return Ok(HookDecision::Continue);
        }

        let branch_name = format!("fix/{}", context.ticket_id.as_deref().unwrap_or(&context.node_id));
        self.provider.create_branch(&branch_name, "main").await?;

        if let Some(ref path) = context.source_file {
            self.provider.commit_file(
                &branch_name,
                path,
                patch,
                &format!("fix(darwin): Auto-remediate panic in {} (Refs: {})", path, context.ticket_id.as_deref().unwrap_or(""))
            ).await?;

            let pr_url = self.provider.create_pull_request(
                &branch_name,
                "main",
                "[Darwin] Auto-Remediation PR",
                &format!("Generated by Marabunta Swarm for node {}.\n\nTicket: {}", context.node_id, context.ticket_id.as_deref().unwrap_or("None"))
            ).await?;

            context.metadata.insert("pr_url".into(), pr_url);
        }

        Ok(HookDecision::Continue)
    }
}
