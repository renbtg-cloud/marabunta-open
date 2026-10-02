// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Defines the precise nature of the simulated Chaos failure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeathType {
    /// Total network isolation. The node drops all incoming and outgoing UDP/TCP traffic 
    /// (except the API port for dashboard telemetry).
    /// *Resuscitation:* Manual via API, or automatically after `duration`.
    NetworkPartition { duration: Option<Duration> },
    
    /// The node's `WorkEngine` begins silently returning garbage math (failing ZKP or attestation)
    /// to test the Aggregator's Byzantine Fault Tolerance.
    ByzantineCorruption { duration: Option<Duration> },
    
    /// The node reports a massive thermal spike, triggering biological load shedding.
    /// It refuses to accept new work chunks but continues to route Kademlia traffic.
    ThermalPanic { duration: Option<Duration> },
    
    /// The node begins dropping exactly 50% of its packets, simulating a severely
    /// degraded, high-latency satellite link.
    JitterStorm { duration: Option<Duration> },
    
    /// The "Hard Kill". The node physically executes `std::process::exit(1)`.
    /// *Resuscitation:* Impossible without external supervisor (e.g. systemd).
    FatalCrash,
}

/// The state of a node undergoing a Chaos Event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosState {
    pub is_active: bool,
    pub current_death: Option<DeathType>,
    pub started_at: Option<std::time::SystemTime>,
}

impl Default for ChaosState {
    fn default() -> Self {
        Self {
            is_active: false,
            current_death: None,
            started_at: None,
        }
    }
}
