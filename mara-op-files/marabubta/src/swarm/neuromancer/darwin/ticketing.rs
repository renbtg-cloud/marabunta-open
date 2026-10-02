// Marabunta - Licensed under the MIT License.
//! Ticketing System Abstractions and Providers.
//!
//! Provides the generic `IssueTracker` trait and its implementation for JIRA.

use async_trait::async_trait;
use crate::swarm::neuromancer::types::NeuromancerError;
use crate::swarm::neuromancer::darwin::{RemediationHook, FailureContext, HookDecision};
use reqwest::Client;
use serde_json::json;

/// A generic interface for issue tracking systems (JIRA, etc.).
#[async_trait]
pub trait IssueTracker: Send + Sync {
    /// Create a new issue/ticket and return its identifier.
    async fn create_issue(&self, title: &str, description: &str, assignee: Option<&str>) -> Result<String, NeuromancerError>;
}

pub struct JiraHook {
    pub client: Client,
    pub api_url: String,
    pub user_email: String,
    pub api_token: String,
    pub project_key: String,
    pub fallback_assignee: String,
}

#[async_trait]
impl RemediationHook for JiraHook {
    fn name(&self) -> &'static str { "jira_tracker" }

    async fn on_failure(&self, context: &mut FailureContext) -> Result<HookDecision, NeuromancerError> {
        // 1. Prepare the ticket details
        let title = format!("[Darwin] Auto-Remediation Failure on Node {}", context.node_id);
        let description = format!(
            "Node {} in region {} experienced a WASM panic.\n\nInference Result: {}\n\nTime-Travel Artifact: {}\n\nStack Trace:\n{}",
            context.node_id,
            context.region,
            context.inference_result.as_deref().unwrap_or("No inference performed."),
            context.artifact_url.as_deref().unwrap_or("No artifact captured."),
            context.stack_trace.as_deref().unwrap_or("Not available.")
        );

        // 2. Call the JIRA REST API v3
        let auth = base64::encode(format!("{}:{}", self.user_email, self.api_token));
        let response = self.client.post(format!("{}/rest/api/3/issue", self.api_url))
            .header("Authorization", format!("Basic {}", auth))
            .json(&json!({
                "fields": {
                    "project": { "key": self.project_key },
                    "summary": title,
                    "description": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {
                                "type": "paragraph",
                                "content": [{ "type": "text", "text": description }]
                            }
                        ]
                    },
                    "issuetype": { "name": "Bug" }
                }
            }))
            .send()
            .await
            .map_err(|e| NeuromancerError::Internal(e.to_string()))?;

        // 3. Extract the ticket key (e.g., MRB-1234)
        let result: serde_json::Value = response.json().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        let ticket_id = result["key"].as_str().unwrap_or("UNKNOWN-TICKET").to_string();

        // 4. Update the context
        context.ticket_id = Some(ticket_id);
        
        Ok(HookDecision::Continue)
    }
}
