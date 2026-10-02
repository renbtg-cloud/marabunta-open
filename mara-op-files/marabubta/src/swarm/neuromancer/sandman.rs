// Marabunta - Licensed under the MIT License.
use std::sync::Arc;
use crate::swarm::neuromancer::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::SandmanConfig;
use crate::swarm::neuromancer::types::{Anomaly, NeuromancerError, Predator};

pub struct Sandman {
    _bus: Arc<NeuromancerBus>,
    _config: SandmanConfig,
}

impl Sandman {
    pub fn new(config: SandmanConfig, bus: Arc<NeuromancerBus>) -> Self {
        Self { _bus: bus, _config: config }
    }
}

#[async_trait::async_trait]
impl Predator for Sandman {
    fn name(&self) -> &'static str {
        "Sandman"
    }

    async fn analyze(&self) -> Result<Option<Anomaly>, NeuromancerError> {
        Ok(None)
    }
}
