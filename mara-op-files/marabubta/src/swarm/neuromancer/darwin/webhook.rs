// Marabunta - Licensed under the MIT License.
//! Webhook Remediator for Darwin: Pushing failure events to external endpoints.

use async_trait::async_trait;
use crate::swarm::neuromancer::types::NeuromancerError;
use crate::swarm::neuromancer::darwin::{RemediationHook, FailureContext, HookDecision};
use reqwest::Client;

pub struct WebhookHook {
    pub client: Client,
    pub endpoint_url: String,
}

#[async_trait]
impl RemediationHook for WebhookHook {
    fn name(&self) -> &'static str { "webhook_remediator" }

    async fn on_failure(&self, context: &mut FailureContext) -> Result<HookDecision, NeuromancerError> {
        // Serialize the context to a JSON payload and POST it.
        self.client.post(&self.endpoint_url)
            .json(context)
            .send()
            .await
            .map_err(|e| NeuromancerError::Internal(e.to_string()))?;

        Ok(HookDecision::Continue)
    }
}
