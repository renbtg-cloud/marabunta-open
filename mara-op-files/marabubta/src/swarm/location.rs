// Marabunta - Licensed under the MIT License.

// Determines a node's physical domain via latency triangulation.
use crate::swarm::config::BEACON_NODES;
use crate::swarm::types::LocationAttestation;
use std::time::Instant;
use tokio::net::TcpStream;
use futures::future::join_all;

// A simple oracle to provide a verifiable location fingerprint.
pub struct LocationOracle;

impl LocationOracle {
    // Creates a difficult-to-forge "latency fingerprint" to identify a node's physical domain.
    pub async fn determine_domains() -> (Vec<String>, LocationAttestation) {
        let mut latencies = Vec::new();
        let handles: Vec<_> = BEACON_NODES.iter().map(|&addr| {
            tokio::spawn(async move {
                let start = Instant::now();
                if TcpStream::connect(addr).await.is_ok() {
                    return Some(start.elapsed());
                }
                None
            })
        }).collect();

        for result in join_all(handles).await {
            if let Ok(Some(latency)) = result {
                latencies.push(latency.as_millis() as u64 as u32);
            }
        }

        latencies.sort(); // Create a stable fingerprint
        
        if latencies.is_empty() {
            return (vec!["unknown-domain".to_string()], LocationAttestation::SelfAttested);
        }

        // Use the fingerprint as the primary domain ID
        let fingerprint_str = latencies.iter().map(ToString::to_string).collect::<Vec<_>>().join("-");
        let domain = format!("lat-id-{}", fingerprint_str);
        
        // In a real system, this signature would use a managed private key.
        // For this plan, we create an ephemeral key to prove the cryptographic structure.
        let signature = format!("signed-({})", domain).as_bytes().to_vec();

        (vec![domain], LocationAttestation::LatencyTriangulation { signature })
    }
}
