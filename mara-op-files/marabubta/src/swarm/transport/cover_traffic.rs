// Marabunta - Licensed under the MIT License.
//! Pillar 15.3: Evasion Evasion
//!
//! The Covert Traffic Engine (IoT Camouflage).
//! This module intercepts outbound QUIC/UDP streams originating from a DarkNet Proxy.
//! If `evasion.camouflage == "IoT"`, the engine fragments, pads, and jitters the 
//! transmission to mimic the exact packet size and frequency of benign consumer IoT devices
//! (e.g., smart thermostats, security cameras) to defeat Deep Packet Inspection (DPI).

use tokio::time::{sleep, Duration};
use rand::{Rng, thread_rng};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, trace};

/// Simulates the exact byte signature and transmission frequency of known IoT appliances.
#[derive(Clone, Debug, PartialEq)]
pub enum CamouflageProfile {
    SmartThermostat,
    DoorbellCamera,
    SmartSpeaker,
    Disabled,
}

pub struct CoverTrafficSystem {
    pub profile: CamouflageProfile,
    pub max_chunk_size_bytes: usize,
    pub base_delay_ms: u64,
    pub jitter_variance_ms: u64,
}

impl CoverTrafficSystem {
    pub fn new(profile_str: &str) -> Self {
        let (profile, max_chunk_size_bytes, base_delay_ms, jitter_variance_ms) = match profile_str.to_lowercase().as_str() {
            "iot_thermostat" => (CamouflageProfile::SmartThermostat, 512, 1000, 200),  // Tiny pings, slow
            "iot_camera" => (CamouflageProfile::DoorbellCamera, 1400, 33, 5),          // 30fps video frames
            "iot_speaker" => (CamouflageProfile::SmartSpeaker, 1024, 100, 20),         // Compressed audio bursts
            _ => (CamouflageProfile::Disabled, 65536, 0, 0),                           // Unrestricted
        };

        if profile != CamouflageProfile::Disabled {
            tracing::info!("Pillar 15.3: Covert Traffic Engine initialized. DarkNet proxy masked as: {:?}", profile);
        }

        Self {
            profile,
            max_chunk_size_bytes,
            base_delay_ms,
            jitter_variance_ms,
        }
    }

    /// Takes a massive WASM or Hashgraph payload and fragments it into 
    /// tiny, disguised chunks, pushing them asynchronously into the network channel.
    pub async fn transmit_covertly(
        &self, 
        mut payload: Vec<u8>, 
        outbound_tx: mpsc::Sender<Vec<u8>>
    ) -> Result<(), &'static str> {
        
        if self.profile == CamouflageProfile::Disabled {
            return outbound_tx.send(payload).await.map_err(|_| "Failed to transmit raw payload");
        }

        let mut rng = thread_rng();
        let total_bytes = payload.len();
        let mut bytes_sent = 0;

        trace!("Covert Engine: Fragmenting {} byte payload to evade DPI...", total_bytes);

        while bytes_sent < total_bytes {
            // 1. Fragment the payload
            let remaining = total_bytes - bytes_sent;
            let chunk_size = std::cmp::min(self.max_chunk_size_bytes, remaining);
            let mut chunk: Vec<u8> = payload.drain(..chunk_size).collect();

            // 2. Pad the chunk with cryptographic static to match the exact IoT profile size
            // DPI often flags unusually small or perfectly sequential packet sizes.
            if chunk.len() < self.max_chunk_size_bytes {
                let padding_needed = self.max_chunk_size_bytes - chunk.len();
                let mut padding = vec![0u8; padding_needed];
                rng.fill(&mut padding[..]);
                chunk.append(&mut padding);
            }

            // 3. Inject Mathematical Jitter
            // Prevent statistical traffic correlation attacks
            let jitter: i64 = rng.gen_range(-(self.jitter_variance_ms as i64)..=(self.jitter_variance_ms as i64));
            let delay = std::cmp::max(1, (self.base_delay_ms as i64) + jitter) as u64;

            sleep(Duration::from_millis(delay)).await;

            // 4. Transmit the camouflaged chunk
            if outbound_tx.send(chunk).await.is_err() {
                return Err("Network channel severed during covert transmission.");
            }
            
            bytes_sent += chunk_size;
        }

        debug!("Covert Engine: Successfully obfuscated and transmitted {} bytes.", total_bytes);
        Ok(())
    }

    /// Spawns a background task that constantly generates "Fake" BFT Hashgraph events.
    /// This prevents an adversary from noticing when the node suddenly becomes active.
    /// Silence is an anomaly. The baseline noise must remain constant.
    pub fn spawn_baseline_noise_generator(self: Arc<Self>, outbound_tx: mpsc::Sender<Vec<u8>>) {
        if self.profile == CamouflageProfile::Disabled { return; }

        tokio::spawn(async move {
            let mut rng = thread_rng();
            loop {
                // Generate a fake 2KB encrypted payload
                let mut fake_payload = vec![0u8; self.max_chunk_size_bytes];
                rng.fill(&mut fake_payload[..]);

                // Introduce massive jitter for baseline noise (e.g., pinging every 5 to 15 seconds)
                let delay = rng.gen_range(5000..15000);
                sleep(Duration::from_millis(delay)).await;

                // Fire the fake traffic into the network
                if outbound_tx.send(fake_payload).await.is_err() {
                    break; // Channel closed, node is shutting down
                }
                trace!("Covert Engine: Emitted 1 frame of baseline cryptographic noise.");
            }
        });
    }
}
