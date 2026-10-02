// Marabunta - Licensed under the MIT License.
//! LLM Inference Engine for Darwin: Generating surgical patches and analyzing failures.

use async_trait::async_trait;
use crate::swarm::neuromancer::types::NeuromancerError;
use crate::swarm::neuromancer::darwin::{RemediationHook, FailureContext, HookDecision};
use reqwest::Client;
use serde_json::json;

pub struct LlmInferenceHook {
    pub client: Client,
    pub api_endpoint: String,
    pub prompt_template: String,
    pub min_confidence: f32,
}

#[async_trait]
impl RemediationHook for LlmInferenceHook {
    fn name(&self) -> &'static str { "llm_inference" }

    async fn on_failure(&self, context: &mut FailureContext) -> Result<HookDecision, NeuromancerError> {
        // 1. Prepare the prompt from the template
        let prompt = self.prompt_template
            .replace("{stack_trace}", context.stack_trace.as_deref().unwrap_or("Unknown"))
            .replace("{node_id}", &context.node_id);

        // 2. Call the LLM API (Generic REST implementation)
        let response = self.client.post(&self.api_endpoint)
            .json(&json!({
                "model": "marabunta-darwin-v1",
                "prompt": prompt,
                "stream": false,
                "temperature": 0.2
            }))
            .send()
            .await
            .map_err(|e| NeuromancerError::Internal(e.to_string()))?;

        // 3. Extract the inference result
        let result: serde_json::Value = response.json().await.map_err(|e| NeuromancerError::Internal(e.to_string()))?;
        let raw_inference = result["response"].as_str().unwrap_or("").to_string();

        // 4. Safely extract only code blocks to prevent conversational leakage
        let extracted_code = extract_code_blocks(&raw_inference);
        
        let final_inference = if extracted_code.trim().is_empty() {
            "NO_FIX_POSSIBLE".to_string()
        } else {
            extracted_code
        };

        // 5. Update the context with the parsed inference result
        context.inference_result = Some(final_inference);
        
        // 6. Decision logic: if the LLM failed to provide code, we stop the fix deployment
        if context.inference_result.as_deref() == Some("NO_FIX_POSSIBLE") {
            tracing::warn!("Darwin Inference: No valid code block extracted from LLM. Aborting VCS commit.");
            return Ok(HookDecision::Continue); // Still continue to archiving and ticketing, but VCS will skip
        }

        Ok(HookDecision::Continue)
    }
}

/// Helper function to extract content strictly from markdown code blocks.
/// e.g. extracts `foo` from ` ```rust\nfoo\n``` `
fn extract_code_blocks(raw_text: &str) -> String {
    let mut is_code_block = false;
    let mut code_content = String::new();

    for line in raw_text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            is_code_block = !is_code_block;
            continue; // Skip the delimiter line itself
        }
        
        if is_code_block {
            code_content.push_str(line);
            code_content.push('\n');
        }
    }

    code_content
}
