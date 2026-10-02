// Marabunta - Licensed under the MIT License.
//! Transport profile transition for the Marabunta protocol.
//!
//! Manages Open/Adaptive profiles with hysteresis, network health assessment,
//! and fallback chain orchestration across all 6 transport methods.

use crate::marabunta::config;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Transport profile: Open (normal) or Adaptive (under duress).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportProfile {
    Open,
    Adaptive,
}

/// Network health metrics used for transition decisions.
#[derive(Debug, Clone)]
pub struct NetworkHealth {
    pub relay_connectivity: f32,
    pub packet_loss_rate: f32,
    pub connection_reset_rate: f32,
    pub dns_resolution_rate: f32,
    pub throughput_ratio: f32,
}

impl Default for NetworkHealth {
    fn default() -> Self {
        Self {
            relay_connectivity: 1.0,
            packet_loss_rate: 0.0,
            connection_reset_rate: 0.0,
            dns_resolution_rate: 1.0,
            throughput_ratio: 1.0,
        }
    }
}

/// Transition engine managing profile switching with hysteresis.
pub struct TransitionEngine {
    current_profile: TransportProfile,
    last_transition: Instant,
    health_history: Vec<(Instant, NetworkHealth)>,
    stable_since: Option<Instant>,
}

impl TransitionEngine {
    pub fn new() -> Self {
        Self {
            current_profile: TransportProfile::Open,
            last_transition: Instant::now(),
            health_history: Vec::new(),
            stable_since: None,
        }
    }

    /// Current transport profile.
    pub fn current_profile(&self) -> TransportProfile {
        self.current_profile
    }

    /// Record a health measurement.
    pub fn record_health(&mut self, health: NetworkHealth) {
        self.health_history.push((Instant::now(), health));
        // Keep last 60 measurements
        if self.health_history.len() > 60 {
            self.health_history.remove(0);
        }
    }

    /// Check if a transition should occur and perform it.
    /// Returns Some(new_profile) if transition happened.
    pub fn check_transition(&mut self, health: &NetworkHealth) -> Option<TransportProfile> {
        // Hysteresis: don't transition if within the cooldown period
        if self.last_transition.elapsed()
            < Duration::from_secs(config::HYSTERESIS_DURATION_S)
        {
            return None;
        }

        match self.current_profile {
            TransportProfile::Open => {
                if self.should_switch_to_adaptive(health) {
                    self.current_profile = TransportProfile::Adaptive;
                    self.last_transition = Instant::now();
                    self.stable_since = None;
                    Some(TransportProfile::Adaptive)
                } else {
                    None
                }
            }
            TransportProfile::Adaptive => {
                if self.should_return_to_open(health) {
                    self.current_profile = TransportProfile::Open;
                    self.last_transition = Instant::now();
                    self.stable_since = None;
                    Some(TransportProfile::Open)
                } else {
                    None
                }
            }
        }
    }

    /// ANY trigger condition switches to Adaptive.
    fn should_switch_to_adaptive(&self, h: &NetworkHealth) -> bool {
        h.relay_connectivity < config::TRANSITION_RELAY_CONNECTIVITY_LOW
            || h.packet_loss_rate > config::TRANSITION_PACKET_LOSS_HIGH
            || h.connection_reset_rate > config::TRANSITION_RESET_RATE_HIGH
            || h.dns_resolution_rate < config::TRANSITION_DNS_RATE_LOW
            || h.throughput_ratio < config::TRANSITION_THROUGHPUT_LOW
    }

    /// ALL metrics must be in the clear zone, sustained for hysteresis period.
    fn should_return_to_open(&mut self, h: &NetworkHealth) -> bool {
        let all_clear = h.relay_connectivity > config::TRANSITION_RELAY_CONNECTIVITY_OK
            && h.packet_loss_rate < config::TRANSITION_PACKET_LOSS_OK
            && h.connection_reset_rate < config::TRANSITION_RESET_RATE_OK
            && h.dns_resolution_rate > config::TRANSITION_DNS_RATE_OK
            && h.throughput_ratio > config::TRANSITION_THROUGHPUT_OK;

        if all_clear {
            match self.stable_since {
                None => {
                    self.stable_since = Some(Instant::now());
                    false
                }
                Some(since) => {
                    since.elapsed() >= Duration::from_secs(config::HYSTERESIS_DURATION_S)
                }
            }
        } else {
            self.stable_since = None;
            false
        }
    }

    /// Get relay count for current profile.
    pub fn relay_count(&self) -> usize {
        match self.current_profile {
            TransportProfile::Open => config::OPEN_RELAY_COUNT,
            TransportProfile::Adaptive => config::ADAPTIVE_RELAY_COUNT,
        }
    }
}

impl Default for TransitionEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Dark mode: exponential retry when all transports fail.
pub struct DarkMode {
    retry_index: usize,
    last_attempt: Option<Instant>,
    active: bool,
}

impl DarkMode {
    pub fn new() -> Self {
        Self {
            retry_index: 0,
            last_attempt: None,
            active: false,
        }
    }

    /// Enter dark mode.
    pub fn activate(&mut self) {
        self.active = true;
        self.retry_index = 0;
        self.last_attempt = Some(Instant::now());
    }

    /// Check if it's time to retry.
    pub fn should_retry(&self) -> bool {
        if !self.active {
            return false;
        }
        let interval = self.current_interval();
        self.last_attempt
            .map_or(true, |last| last.elapsed() >= interval)
    }

    /// Record a retry attempt.
    pub fn record_attempt(&mut self) {
        self.last_attempt = Some(Instant::now());
        if self.retry_index < config::DARK_MODE_RETRY_INTERVALS_S.len() - 1 {
            self.retry_index += 1;
        }
    }

    /// Deactivate dark mode (connection recovered).
    pub fn deactivate(&mut self) {
        self.active = false;
        self.retry_index = 0;
    }

    /// Is dark mode active?
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Current retry interval.
    pub fn current_interval(&self) -> Duration {
        let secs = config::DARK_MODE_RETRY_INTERVALS_S
            .get(self.retry_index)
            .copied()
            .unwrap_or(43_200);
        Duration::from_secs(secs)
    }
}

impl Default for DarkMode {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good_health() -> NetworkHealth {
        NetworkHealth {
            relay_connectivity: 0.95,
            packet_loss_rate: 0.01,
            connection_reset_rate: 0.5,
            dns_resolution_rate: 0.98,
            throughput_ratio: 0.8,
        }
    }

    fn bad_health() -> NetworkHealth {
        NetworkHealth {
            relay_connectivity: 0.2,
            packet_loss_rate: 0.5,
            connection_reset_rate: 15.0,
            dns_resolution_rate: 0.3,
            throughput_ratio: 0.05,
        }
    }

    #[test]
    fn test_initial_profile_is_open() {
        let engine = TransitionEngine::new();
        assert_eq!(engine.current_profile(), TransportProfile::Open);
    }

    #[test]
    fn test_should_switch_to_adaptive() {
        let mut engine = TransitionEngine::new();
        // Force past hysteresis
        engine.last_transition = Instant::now() - Duration::from_secs(600);

        let result = engine.check_transition(&bad_health());
        assert_eq!(result, Some(TransportProfile::Adaptive));
        assert_eq!(engine.current_profile(), TransportProfile::Adaptive);
    }

    #[test]
    fn test_hysteresis_blocks_transition() {
        let mut engine = TransitionEngine::new();
        // Last transition was just now — hysteresis should block
        let result = engine.check_transition(&bad_health());
        assert_eq!(result, None);
        assert_eq!(engine.current_profile(), TransportProfile::Open);
    }

    #[test]
    fn test_single_trigger_sufficient() {
        let mut engine = TransitionEngine::new();
        engine.last_transition = Instant::now() - Duration::from_secs(600);

        // Only packet loss is bad
        let health = NetworkHealth {
            relay_connectivity: 0.9,
            packet_loss_rate: 0.5, // > 0.3 threshold
            connection_reset_rate: 0.5,
            dns_resolution_rate: 0.9,
            throughput_ratio: 0.8,
        };
        let result = engine.check_transition(&health);
        assert_eq!(result, Some(TransportProfile::Adaptive));
    }

    #[test]
    fn test_relay_count_by_profile() {
        let mut engine = TransitionEngine::new();
        assert_eq!(engine.relay_count(), config::OPEN_RELAY_COUNT);

        engine.current_profile = TransportProfile::Adaptive;
        assert_eq!(engine.relay_count(), config::ADAPTIVE_RELAY_COUNT);
    }

    #[test]
    fn test_dark_mode_activation() {
        let mut dm = DarkMode::new();
        assert!(!dm.is_active());
        dm.activate();
        assert!(dm.is_active());
        assert_eq!(dm.current_interval(), Duration::from_secs(60));
    }

    #[test]
    fn test_dark_mode_retry_escalation() {
        let mut dm = DarkMode::new();
        dm.activate();
        assert_eq!(dm.current_interval(), Duration::from_secs(60));
        dm.record_attempt();
        assert_eq!(dm.current_interval(), Duration::from_secs(300));
        dm.record_attempt();
        assert_eq!(dm.current_interval(), Duration::from_secs(1_800));
        dm.record_attempt();
        assert_eq!(dm.current_interval(), Duration::from_secs(7_200));
        dm.record_attempt();
        assert_eq!(dm.current_interval(), Duration::from_secs(43_200));
    }

    #[test]
    fn test_dark_mode_deactivation() {
        let mut dm = DarkMode::new();
        dm.activate();
        dm.record_attempt();
        dm.record_attempt();
        dm.deactivate();
        assert!(!dm.is_active());
        assert_eq!(dm.current_interval(), Duration::from_secs(60)); // reset
    }
}
