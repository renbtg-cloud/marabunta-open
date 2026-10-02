// Marabunta - Licensed under the MIT License.
use crate::chaos::types::{ChaosState, DeathType};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::SystemTime;

/// The central controller for simulated network failures (War Games).
#[derive(Clone)]
pub struct ChaosEngine {
    state: Arc<RwLock<ChaosState>>,
}

impl Default for ChaosEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ChaosEngine {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ChaosState::default())),
        }
    }

    /// Trigger a simulated death event on this node.
    pub fn strike(&self, death_type: DeathType) {
        tracing::warn!("CHAOS STRIKE INITIATED: Node entering simulated death state: {:?}", death_type);
        
        let mut st = self.state.write();
        st.is_active = true;
        st.current_death = Some(death_type.clone());
        st.started_at = Some(SystemTime::now());

        if let DeathType::FatalCrash = death_type {
            tracing::error!("CHAOS FATAL CRASH: Executing physical process exit.");
            std::process::exit(1);
        }
    }

    /// Revive the node from a simulated death event.
    pub fn revive(&self) {
        let mut st = self.state.write();
        if st.is_active {
            tracing::info!("CHAOS REVIVE: Node returning to normal operational state.");
            st.is_active = false;
            st.current_death = None;
            st.started_at = None;
        }
    }

    /// Get the current chaos state of the node.
    pub fn get_state(&self) -> ChaosState {
        self.state.read().clone()
    }

    /// Helper to cleanly disable the active chaos state internally
    fn internal_revive(&self) {
        let mut st = self.state.write();
        if st.is_active {
            tracing::info!("CHAOS DURATION EXPIRED: Node automatically resuscitating.");
            st.is_active = false;
            st.current_death = None;
            st.started_at = None;
        }
    }

    /// Check if the current chaos event has expired based on its duration.
    /// Returns true if the node is actively in chaos, false if it is normal.
    fn check_liveness(&self) -> bool {
        let st = self.state.read();
        if !st.is_active {
            return false;
        }

        let (started_at, duration) = match (st.started_at, &st.current_death) {
            (Some(s), Some(DeathType::NetworkPartition { duration: Some(d) })) => (s, *d),
            (Some(s), Some(DeathType::ByzantineCorruption { duration: Some(d) })) => (s, *d),
            (Some(s), Some(DeathType::ThermalPanic { duration: Some(d) })) => (s, *d),
            (Some(s), Some(DeathType::JitterStorm { duration: Some(d) })) => (s, *d),
            _ => return true, // Infinite duration or missing timestamp
        };
        
        drop(st);

        if let Ok(elapsed) = started_at.elapsed() {
            if elapsed >= duration {
                self.internal_revive();
                return false;
            }
        }
        
        true
    }

    /// Helper to determine if the node should physically drop Kademlia network packets.
    pub fn should_drop_network(&self) -> bool {
        if !self.check_liveness() { return false; }
        
        let st = self.state.read();
        match &st.current_death {
            Some(DeathType::NetworkPartition { .. }) => true,
            Some(DeathType::JitterStorm { .. }) => {
                // Drop 50% of packets
                rand::random::<bool>()
            },
            _ => false,
        }
    }

    /// Helper to determine if the node should reject new WorkEngine chunks.
    pub fn should_reject_work(&self) -> bool {
        if !self.check_liveness() { return false; }
        
        let st = self.state.read();
        matches!(&st.current_death, Some(DeathType::NetworkPartition { .. }) | Some(DeathType::ThermalPanic { .. }))
    }

    /// Helper to determine if the node should intentionally corrupt mathematical outputs.
    pub fn should_corrupt_math(&self) -> bool {
        if !self.check_liveness() { return false; }
        
        let st = self.state.read();
        matches!(&st.current_death, Some(DeathType::ByzantineCorruption { .. }))
    }
}
