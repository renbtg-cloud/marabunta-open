// Marabunta - Licensed under the MIT License.
//! Client for communicating with Marabunta Coordinator
//!
//! Provides async HTTP client for all coordinator API operations.

use reqwest::{Client, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;
use std::time::Duration;

use crate::cli::config::Config;
use crate::cli::types::{
    CliError, JobResults, JobSpec, JobStatusResponse, NodeFilter, NodeInfo, PlacementPlan,
    TokenBalance, TokenTransaction,
};

// ─────────────────────────────────────────────────────────────────────────────
// CLIENT ERROR
// ─────────────────────────────────────────────────────────────────────────────

/// Client-specific errors
#[derive(Debug)]
pub enum ClientError {
    /// Connection failed
    ConnectionFailed(String),
    /// Request failed
    RequestFailed(String),
    /// Invalid response
    InvalidResponse(String),
    /// Not found
    NotFound(String),
    /// Unauthorized
    Unauthorized(String),
    /// Rate limited
    RateLimited(String),
    /// Server error
    ServerError(String),
    /// Timeout
    Timeout(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            ClientError::RequestFailed(msg) => write!(f, "Request failed: {}", msg),
            ClientError::InvalidResponse(msg) => write!(f, "Invalid response: {}", msg),
            ClientError::NotFound(msg) => write!(f, "Not found: {}", msg),
            ClientError::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            ClientError::RateLimited(msg) => write!(f, "Rate limited: {}", msg),
            ClientError::ServerError(msg) => write!(f, "Server error: {}", msg),
            ClientError::Timeout(msg) => write!(f, "Timeout: {}", msg),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<reqwest::Error> for ClientError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            ClientError::Timeout(err.to_string())
        } else if err.is_connect() {
            ClientError::ConnectionFailed(err.to_string())
        } else {
            ClientError::RequestFailed(err.to_string())
        }
    }
}

impl From<ClientError> for CliError {
    fn from(err: ClientError) -> Self {
        CliError::Client(err.to_string())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RESPONSE TYPES
// ─────────────────────────────────────────────────────────────────────────────

/// Submit response from coordinator
#[derive(Debug, serde::Deserialize)]
pub struct SubmitResponse {
    pub job_id: String,
    pub message: Option<String>,
}

/// Health check response
#[derive(Debug, serde::Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: Option<String>,
    pub uptime_secs: Option<u64>,
}

/// Generic API error response
#[derive(Debug, serde::Deserialize)]
pub struct ApiError {
    pub error: String,
    pub code: Option<String>,
    pub details: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// COORDIGLOBAL_ALLIANCE_T1R CLIENT
// ─────────────────────────────────────────────────────────────────────────────

/// Client for communicating with Marabunta Coordinator
pub struct CoordinatorClient {
    /// Base URL of the coordinator
    url: String,
    /// HTTP client
    http_client: Client,
    /// Optional API key
    api_key: Option<String>,
}

impl CoordinatorClient {
    /// Create a new coordinator client and verify connection
    pub async fn connect(url: &str) -> Result<Self, ClientError> {
        Self::connect_with_key(url, None).await
    }

    /// Create a new coordinator client with API key
    pub async fn connect_with_key(url: &str, api_key: Option<String>) -> Result<Self, ClientError> {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;

        let client = Self {
            url: url.trim_end_matches('/').to_string(),
            http_client,
            api_key,
        };

        // Test connection
        client.health_check().await?;

        Ok(client)
    }

    /// Create client from config
    pub async fn from_config(config: &Config) -> Result<Self, ClientError> {
        Self::connect_with_key(&config.coordinator_url, config.org_key.clone()).await
    }

    /// Health check
    pub async fn health_check(&self) -> Result<HealthResponse, ClientError> {
        self.get("/health").await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // JOB OPERATIONS
    // ─────────────────────────────────────────────────────────────────────────

    /// Submit a new job
    pub async fn submit_job(&self, spec: JobSpec) -> Result<String, ClientError> {
        let response: SubmitResponse = self.post("/jobs", &spec).await?;
        Ok(response.job_id)
    }

    /// Get job status
    pub async fn get_job_status(&self, job_id: &str) -> Result<JobStatusResponse, ClientError> {
        self.get(&format!("/jobs/{}", job_id)).await
    }

    /// Get job results
    pub async fn get_results(&self, job_id: &str) -> Result<JobResults, ClientError> {
        self.get(&format!("/jobs/{}/results", job_id)).await
    }

    /// Get partial job results
    pub async fn get_partial_results(&self, job_id: &str) -> Result<JobResults, ClientError> {
        self.get(&format!("/jobs/{}/results?partial=true", job_id))
            .await
    }

    /// Cancel a job
    pub async fn cancel_job(&self, job_id: &str) -> Result<(), ClientError> {
        self.post_empty(&format!("/jobs/{}/cancel", job_id)).await
    }

    /// Get placement plan for a job
    pub async fn get_placement_plan(&self, job_id: &str) -> Result<PlacementPlan, ClientError> {
        self.get(&format!("/jobs/{}/placement", job_id)).await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // NODE OPERATIONS
    // ─────────────────────────────────────────────────────────────────────────

    /// List all nodes
    pub async fn list_nodes(&self, filter: NodeFilter) -> Result<Vec<NodeInfo>, ClientError> {
        let mut url = "/nodes".to_string();
        let mut params = Vec::new();

        if let Some(runtime) = &filter.runtime {
            params.push(format!("runtime={}", runtime));
        }
        if let Some(memory_min) = filter.memory_min {
            params.push(format!("memory_min={}", memory_min));
        }
        if let Some(region) = &filter.region {
            params.push(format!("region={}", region));
        }
        if let Some(arch) = &filter.architecture {
            params.push(format!("arch={}", arch));
        }
        if let Some(status) = &filter.status {
            params.push(format!("status={}", status));
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        self.get(&url).await
    }

    /// Get single node info
    pub async fn get_node(&self, node_id: &str) -> Result<NodeInfo, ClientError> {
        self.get(&format!("/nodes/{}", node_id)).await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TOKEN OPERATIONS
    // ─────────────────────────────────────────────────────────────────────────

    /// Get token balance
    pub async fn get_token_balance(&self) -> Result<TokenBalance, ClientError> {
        self.get("/tokens/balance").await
    }

    /// Get token transactions
    pub async fn get_token_transactions(
        &self,
        limit: Option<u32>,
    ) -> Result<Vec<TokenTransaction>, ClientError> {
        let url = match limit {
            Some(l) => format!("/tokens/transactions?limit={}", l),
            None => "/tokens/transactions".to_string(),
        };
        self.get(&url).await
    }

    /// Claim pending rewards
    pub async fn claim_rewards(&self) -> Result<f64, ClientError> {
        #[derive(serde::Deserialize)]
        struct ClaimResponse {
            amount: f64,
        }
        let response: ClaimResponse = self.post_empty_with_response("/tokens/claim").await?;
        Ok(response.amount)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // WORKFLOW OPERATIONS
    // ─────────────────────────────────────────────────────────────────────────

    /// Create a new workflow
    pub async fn create_workflow(
        &self,
        workflow: serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        self.post("/api/workflows", &workflow).await
    }

    /// List workflows
    pub async fn list_workflows(
        &self,
        name: Option<&str>,
        tag: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, ClientError> {
        let mut url = format!("/api/workflows?limit={}", limit);
        if let Some(n) = name {
            url.push_str(&format!("&name={}", n));
        }
        if let Some(t) = tag {
            url.push_str(&format!("&tag={}", t));
        }
        self.get(&url).await
    }

    /// Get a workflow by ID
    pub async fn get_workflow(&self, id: &str) -> Result<serde_json::Value, ClientError> {
        self.get(&format!("/api/workflows/{}", id)).await
    }

    /// Delete a workflow
    pub async fn delete_workflow(&self, id: &str) -> Result<(), ClientError> {
        self.delete(&format!("/api/workflows/{}", id)).await
    }

    /// Run a workflow
    pub async fn run_workflow(
        &self,
        workflow_id: &str,
        variables: Option<std::collections::HashMap<String, serde_json::Value>>,
    ) -> Result<serde_json::Value, ClientError> {
        let body = serde_json::json!({
            "variables": variables
        });
        self.post(&format!("/api/workflows/{}/run", workflow_id), &body)
            .await
    }

    /// Get run status
    pub async fn get_run_status(&self, run_id: &str) -> Result<serde_json::Value, ClientError> {
        self.get(&format!("/api/runs/{}/status", run_id)).await
    }

    /// Cancel a run
    pub async fn cancel_run(&self, run_id: &str) -> Result<(), ClientError> {
        self.post_empty(&format!("/api/runs/{}/cancel", run_id))
            .await
    }

    /// List runs
    pub async fn list_runs(
        &self,
        workflow_id: Option<&str>,
        status: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, ClientError> {
        let mut url = format!("/api/runs?limit={}", limit);
        if let Some(wf) = workflow_id {
            url = format!("/api/workflows/{}/runs?limit={}", wf, limit);
        }
        if let Some(s) = status {
            url.push_str(&format!("&status={}", s));
        }
        self.get(&url).await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HTTP HELPERS
    // ─────────────────────────────────────────────────────────────────────────

    /// Build request with common headers
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.url, path);
        let mut request = self.http_client.request(method, &url);

        if let Some(key) = &self.api_key {
            request = request.header("Authorization", format!("Bearer {}", key));
        }

        request = request.header("Content-Type", "application/json");
        request = request.header("Accept", "application/json");
        request = request.header("User-Agent", "marabunta-cli/0.1.0");

        request
    }

    /// GET request
    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let response = self.request(reqwest::Method::GET, path).send().await?;

        self.handle_response(response).await
    }

    /// POST request with body
    async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let response = self
            .request(reqwest::Method::POST, path)
            .json(body)
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// POST request without body
    async fn post_empty(&self, path: &str) -> Result<(), ClientError> {
        let response = self.request(reqwest::Method::POST, path).send().await?;

        self.handle_empty_response(response).await
    }

    /// POST request without body, expecting response
    async fn post_empty_with_response<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<T, ClientError> {
        let response = self.request(reqwest::Method::POST, path).send().await?;

        self.handle_response(response).await
    }

    /// DELETE request
    #[allow(dead_code)]
    async fn delete(&self, path: &str) -> Result<(), ClientError> {
        let response = self.request(reqwest::Method::DELETE, path).send().await?;

        self.handle_empty_response(response).await
    }

    /// Handle response with JSON body
    async fn handle_response<T: DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, ClientError> {
        let status = response.status();

        if status.is_success() {
            response
                .json()
                .await
                .map_err(|e| ClientError::InvalidResponse(e.to_string()))
        } else {
            self.handle_error_response(status, response).await
        }
    }

    /// Handle response without body
    async fn handle_empty_response(&self, response: reqwest::Response) -> Result<(), ClientError> {
        let status = response.status();

        if status.is_success() {
            Ok(())
        } else {
            self.handle_error_response::<()>(status, response).await
        }
    }

    /// Handle error response
    async fn handle_error_response<T>(
        &self,
        status: StatusCode,
        response: reqwest::Response,
    ) -> Result<T, ClientError> {
        // Try to parse error body
        let error_msg = if let Ok(api_error) = response.json::<ApiError>().await {
            api_error.error
        } else {
            status.to_string()
        };

        Err(match status {
            StatusCode::NOT_FOUND => ClientError::NotFound(error_msg),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                ClientError::Unauthorized(error_msg)
            }
            StatusCode::TOO_MANY_REQUESTS => ClientError::RateLimited(error_msg),
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => ClientError::ServerError(error_msg),
            _ => ClientError::RequestFailed(error_msg),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MOCK CLIENT FOR TESTING
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
pub mod mock {
    use super::*;

    /// Mock client for testing
    pub struct MockCoordinatorClient {
        pub jobs: std::collections::HashMap<String, JobStatusResponse>,
        pub nodes: Vec<NodeInfo>,
    }

    impl MockCoordinatorClient {
        pub fn new() -> Self {
            Self {
                jobs: std::collections::HashMap::new(),
                nodes: Vec::new(),
            }
        }

        pub fn with_job(mut self, job: JobStatusResponse) -> Self {
            self.jobs.insert(job.job_id.clone(), job);
            self
        }

        pub fn with_nodes(mut self, nodes: Vec<NodeInfo>) -> Self {
            self.nodes = nodes;
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_error_display() {
        let err = ClientError::NotFound("job-123".to_string());
        assert!(err.to_string().contains("Not found"));
        assert!(err.to_string().contains("job-123"));
    }

    #[test]
    fn test_url_building() {
        // This would require a mock server to test properly
        // For now, just verify the types compile correctly
    }
}
