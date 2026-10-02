// Marabunta - Licensed under the MIT License.
//! Bootstrap discovery and relay selection for the Marabunta protocol.
//!
//! 4-step bootstrap chain: hardcoded relays → DNS-over-HTTPS → CDN-signed JSON → peer exchange.
//! Relay scoring based on latency, capacity, reputation, and ASN diversity.

use crate::marabunta::config;
use crate::marabunta::identity::NodeId;
use crate::marabunta::transport::{RelayEndpoint, TransportMethod};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Relay advertisement from the network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayAdvertisement {
    pub relay_id: NodeId,
    pub endpoints: Vec<RelayEndpoint>,
    pub region: String,
    pub capacity: RelayCapacity,
    pub signature: Vec<u8>,
}

/// Relay capacity information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayCapacity {
    pub max_connections: u32,
    pub current_connections: u32,
    pub bandwidth_mbps: u32,
    pub latency_ms: u32,
}

impl RelayCapacity {
    /// Available connection slots.
    pub fn available_slots(&self) -> u32 {
        self.max_connections.saturating_sub(self.current_connections)
    }

    /// Load factor (0.0 = empty, 1.0 = full).
    pub fn load_factor(&self) -> f64 {
        if self.max_connections == 0 {
            return 1.0;
        }
        self.current_connections as f64 / self.max_connections as f64
    }
}

/// Bootstrap discovery errors.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("all bootstrap methods failed")]
    AllMethodsFailed,

    #[error("no suitable relays found")]
    NoRelaysFound,

    #[error("signature verification failed")]
    SignatureInvalid,

    #[error("discovery error: {0}")]
    Other(String),
}

/// Bootstrap chain for initial relay discovery.
pub struct BootstrapChain {
    hardcoded_relays: Vec<String>,
}

impl BootstrapChain {
    pub fn new(hardcoded_relays: Vec<String>) -> Self {
        Self { hardcoded_relays }
    }

    /// Get the hardcoded relay endpoints (step 1).
    pub fn hardcoded_endpoints(&self) -> Vec<RelayEndpoint> {
        self.hardcoded_relays
            .iter()
            .map(|url| RelayEndpoint {
                url: url.clone(),
                region: region_from_url(url),
                transport_method: TransportMethod::WebSocket,
            })
            .collect()
    }

    /// Attempt bootstrap through the 4-step fallthrough chain.
    /// Returns relay endpoints in priority order.
    pub async fn discover(&self) -> Result<Vec<RelayEndpoint>, DiscoveryError> {
        // Step 1: Hardcoded relays
        let hardcoded = self.hardcoded_endpoints();
        if !hardcoded.is_empty() {
            return Ok(hardcoded);
        }

        // Steps 2-4 would be: DNS-over-HTTPS TXT, CDN signed JSON, peer exchange
        // For now, return error if no hardcoded relays
        Err(DiscoveryError::AllMethodsFailed)
    }
}

impl Default for BootstrapChain {
    fn default() -> Self {
        Self::new(vec![
            "wss://relay-us.marabunta.io:443".into(),
            "wss://relay-eu.marabunta.io:443".into(),
            "wss://relay-ap.marabunta.io:443".into(),
            "wss://relay-sa.marabunta.io:443".into(),
            "wss://relay-af.marabunta.io:443".into(),
        ])
    }
}

/// Relay selector with scoring and diversity constraints.
pub struct RelaySelector;

impl RelaySelector {
    /// Score a relay based on latency, capacity, reputation weight, and diversity.
    pub fn score(relay: &RelayAdvertisement, my_latency_ms: u32) -> f64 {
        let latency_score = 1.0 / (1.0 + my_latency_ms as f64 / 100.0);
        let capacity_score = 1.0 - relay.capacity.load_factor();
        let reputation_score = 0.8; // placeholder — would come from gossip

        latency_score * config::RELAY_SCORE_LATENCY_WEIGHT
            + capacity_score * config::RELAY_SCORE_CAPACITY_WEIGHT
            + reputation_score * config::RELAY_SCORE_REPUTATION_WEIGHT
            + diversity_score(&relay.region) * config::RELAY_SCORE_DIVERSITY_WEIGHT
    }

    /// Select the best relays with diversity constraints.
    pub fn select_relays(
        candidates: &[RelayAdvertisement],
        count: usize,
    ) -> Vec<RelayAdvertisement> {
        if candidates.is_empty() {
            return vec![];
        }

        let mut scored: Vec<(f64, &RelayAdvertisement)> = candidates
            .iter()
            .map(|r| (Self::score(r, r.capacity.latency_ms), r))
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let mut selected = Vec::with_capacity(count);
        let mut region_counts: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();

        for (_, relay) in scored {
            let region_count = region_counts.get(relay.region.as_str()).copied().unwrap_or(0);
            if region_count >= config::RELAY_MAX_SAME_REGION {
                continue;
            }
            *region_counts.entry(&relay.region).or_insert(0) += 1;
            selected.push(relay.clone());
            if selected.len() >= count {
                break;
            }
        }

        selected
    }
}

/// Extract a region hint from a relay URL.
fn region_from_url(url: &str) -> String {
    if url.contains("-us") {
        "us".into()
    } else if url.contains("-eu") {
        "eu".into()
    } else if url.contains("-ap") {
        "ap".into()
    } else if url.contains("-sa") {
        "sa".into()
    } else if url.contains("-af") {
        "af".into()
    } else {
        "unknown".into()
    }
}

/// Diversity score for a region (placeholder for ASN diversity).
fn diversity_score(_region: &str) -> f64 {
    0.5 // default — would be computed from current relay set
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_relay(region: &str, latency: u32, load: u32, max: u32) -> RelayAdvertisement {
        RelayAdvertisement {
            relay_id: NodeId([0; 32]),
            endpoints: vec![],
            region: region.into(),
            capacity: RelayCapacity {
                max_connections: max,
                current_connections: load,
                bandwidth_mbps: 100,
                latency_ms: latency,
            },
            signature: vec![],
        }
    }

    #[test]
    fn test_relay_capacity() {
        let cap = RelayCapacity {
            max_connections: 100,
            current_connections: 30,
            bandwidth_mbps: 100,
            latency_ms: 20,
        };
        assert_eq!(cap.available_slots(), 70);
        assert!((cap.load_factor() - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_bootstrap_hardcoded() {
        let chain = BootstrapChain::default();
        let endpoints = chain.hardcoded_endpoints();
        assert_eq!(endpoints.len(), 5);
        assert_eq!(endpoints[0].region, "us");
        assert_eq!(endpoints[1].region, "eu");
    }

    #[test]
    fn test_relay_scoring() {
        let relay = make_relay("us", 20, 30, 100);
        let score = RelaySelector::score(&relay, 20);
        assert!(score > 0.0);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_lower_latency_scores_higher() {
        let fast = make_relay("us", 10, 50, 100);
        let slow = make_relay("us", 200, 50, 100);
        let fast_score = RelaySelector::score(&fast, 10);
        let slow_score = RelaySelector::score(&slow, 200);
        assert!(fast_score > slow_score);
    }

    #[test]
    fn test_select_relays_diversity() {
        let candidates = vec![
            make_relay("us", 10, 20, 100),
            make_relay("us", 15, 30, 100),
            make_relay("us", 20, 40, 100),
            make_relay("eu", 50, 20, 100),
            make_relay("ap", 80, 10, 100),
        ];

        let selected = RelaySelector::select_relays(&candidates, 4);
        assert_eq!(selected.len(), 4);

        // Max 2 from same region
        let us_count = selected.iter().filter(|r| r.region == "us").count();
        assert!(us_count <= config::RELAY_MAX_SAME_REGION);
    }

    #[test]
    fn test_select_relays_empty() {
        let selected = RelaySelector::select_relays(&[], 5);
        assert!(selected.is_empty());
    }

    #[tokio::test]
    async fn test_bootstrap_discover() {
        let chain = BootstrapChain::default();
        let relays = chain.discover().await.unwrap();
        assert_eq!(relays.len(), 5);
    }
}
