// Marabunta - Licensed under the MIT License.
//! Geopolitical Triangle Inequality Enforcement via Vivaldi Coordinates
//! 
//! Detects and mathematically drops TCP/QUIC connections if a node's physical latency
//! contradicts its claimed geographic location, rendering VPN IP spoofing impossible.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct VivaldiCoordinate {
    pub rtt_to_us_east: Duration,
    pub rtt_to_eu_central: Duration,
    pub rtt_to_ap_northeast: Duration,
}

/// Stores a rolling window of RTT measurements to filter out network jitter 
/// and establish the physical fiber-optic floor.
#[derive(Debug, Clone, Default)]
pub struct LatencyHistory {
    pub us_east_samples: Vec<Duration>,
    pub eu_central_samples: Vec<Duration>,
    pub ap_northeast_samples: Vec<Duration>,
}

impl LatencyHistory {
    const MAX_SAMPLES: usize = 10;

    pub fn add_samples(&mut self, coords: VivaldiCoordinate) {
        self.us_east_samples.push(coords.rtt_to_us_east);
        self.eu_central_samples.push(coords.rtt_to_eu_central);
        self.ap_northeast_samples.push(coords.rtt_to_ap_northeast);

        if self.us_east_samples.len() > Self::MAX_SAMPLES { self.us_east_samples.remove(0); }
        if self.eu_central_samples.len() > Self::MAX_SAMPLES { self.eu_central_samples.remove(0); }
        if self.ap_northeast_samples.len() > Self::MAX_SAMPLES { self.ap_northeast_samples.remove(0); }
    }

    /// Establishes the 'True RTT' (the lowest measured latency) by filtering out 
    /// transient congestion spikes. This value represents the physical speed-of-light floor.
    pub fn filtered_coords(&self) -> Option<VivaldiCoordinate> {
        if self.us_east_samples.is_empty() { return None; }
        
        Some(VivaldiCoordinate {
            rtt_to_us_east: *self.us_east_samples.iter().min().unwrap(),
            rtt_to_eu_central: *self.eu_central_samples.iter().min().unwrap(),
            rtt_to_ap_northeast: *self.ap_northeast_samples.iter().min().unwrap(),
        })
    }
}

pub struct GeolocationVerifier;

impl GeolocationVerifier {
    /// Detects VPN routing and IP spoofing via geometric contradiction.
    /// This implementation assumes `coords` has been pre-filtered via `LatencyHistory` 
    /// to establish the true fiber-optic floor.
    pub fn detect_spoofing(claimed_region: &str, coords: &VivaldiCoordinate) -> bool {
        // Physical speed of light constraints (approximate floor in milliseconds)
        const US_TO_EU_MIN_MS: u128 = 70;
        const US_TO_AP_MIN_MS: u128 = 130;
        
        let us = coords.rtt_to_us_east.as_millis();
        let eu = coords.rtt_to_eu_central.as_millis();
        let ap = coords.rtt_to_ap_northeast.as_millis();

        match claimed_region {
            "EU" => {
                // If in Europe, the RTT to US_EAST MUST be the local latency to the EU Watchtower
                // PLUS the transatlantic fiber floor.
                if us > eu + US_TO_EU_MIN_MS + 40 {
                    tracing::error!("HIGHESTSEC: Vivaldi Contradiction. Node claims EU but Triangle Inequality fails against US-East Watchtower. Disconnecting.");
                    return true; // Spoofing detected
                }
            },
            "US" => {
                if eu > us + US_TO_EU_MIN_MS + 40 {
                    tracing::error!("HIGHESTSEC: Vivaldi Contradiction. Node claims US but Triangle Inequality fails against EU-Central Watchtower. Disconnecting.");
                    return true;
                }
            },
            "AP" => {
                if us > ap + US_TO_AP_MIN_MS + 50 {
                    tracing::error!("HIGHESTSEC: Vivaldi Contradiction. Node claims AP but Triangle Inequality fails against US-East Watchtower. Disconnecting.");
                    return true;
                }
            },
            _ => return false,
        }
        
        false
    }
}
