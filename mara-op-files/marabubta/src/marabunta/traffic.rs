// Marabunta - Licensed under the MIT License.
//! Traffic shaping and PAD generation for the Marabunta protocol.
//!
//! Implements browsing-mimetic Markov model for timing, PAD frame generation
//! at idle, and volume shaping to maintain cover traffic patterns.

use crate::marabunta::config;
use crate::marabunta::frame::{Frame, FrameType};
use rand::Rng;
use std::time::Duration;

/// States in the browsing-mimetic Markov model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarkovState {
    /// Idle browsing — occasional small transfers.
    Idle,
    /// Burst download — page load simulation.
    BurstDownload,
    /// Upload — form submit / file upload simulation.
    Upload,
    /// Think time — user reading a page.
    ThinkTime,
}

/// Browsing-mimetic traffic model using a simple Markov chain.
pub struct MarkovModel {
    state: MarkovState,
    state_ticks: u32,
}

impl MarkovModel {
    pub fn new() -> Self {
        Self {
            state: MarkovState::Idle,
            state_ticks: 0,
        }
    }

    /// Get current state.
    pub fn state(&self) -> MarkovState {
        self.state
    }

    /// Advance the model by one tick. Returns the new state.
    pub fn tick(&mut self) -> MarkovState {
        self.state_ticks += 1;

        let mut rng = rand::thread_rng();
        let roll: f32 = rng.gen();

        self.state = match self.state {
            MarkovState::Idle => {
                if self.state_ticks > 10 && roll < 0.15 {
                    self.state_ticks = 0;
                    MarkovState::BurstDownload
                } else if roll < 0.05 {
                    self.state_ticks = 0;
                    MarkovState::Upload
                } else {
                    MarkovState::Idle
                }
            }
            MarkovState::BurstDownload => {
                if self.state_ticks > 3 && roll < 0.4 {
                    self.state_ticks = 0;
                    MarkovState::ThinkTime
                } else if self.state_ticks > 8 {
                    self.state_ticks = 0;
                    MarkovState::ThinkTime
                } else {
                    MarkovState::BurstDownload
                }
            }
            MarkovState::Upload => {
                if self.state_ticks > 2 && roll < 0.5 {
                    self.state_ticks = 0;
                    MarkovState::ThinkTime
                } else if self.state_ticks > 5 {
                    self.state_ticks = 0;
                    MarkovState::Idle
                } else {
                    MarkovState::Upload
                }
            }
            MarkovState::ThinkTime => {
                if self.state_ticks > 5 && roll < 0.3 {
                    self.state_ticks = 0;
                    MarkovState::BurstDownload
                } else if self.state_ticks > 15 {
                    self.state_ticks = 0;
                    MarkovState::Idle
                } else if roll < 0.1 {
                    self.state_ticks = 0;
                    MarkovState::Idle
                } else {
                    MarkovState::ThinkTime
                }
            }
        };

        self.state
    }
}

impl Default for MarkovModel {
    fn default() -> Self {
        Self::new()
    }
}

/// Traffic shaper that controls send timing and PAD generation.
pub struct TrafficShaper {
    markov: MarkovModel,
    sequence_counter: u32,
}

impl TrafficShaper {
    pub fn new() -> Self {
        Self {
            markov: MarkovModel::new(),
            sequence_counter: 0,
        }
    }

    /// Get the next recommended delay before sending.
    pub fn next_send_delay(&mut self) -> Duration {
        self.markov.tick();

        let base_ms = match self.markov.state() {
            MarkovState::Idle => 500,
            MarkovState::BurstDownload => 50,
            MarkovState::Upload => 100,
            MarkovState::ThinkTime => 2000,
        };

        jitter(base_ms, config::GOSSIP_JITTER_MS as u32)
    }

    /// Whether to generate a PAD frame (based on current state).
    pub fn should_send_pad(&self) -> bool {
        matches!(
            self.markov.state(),
            MarkovState::Idle | MarkovState::ThinkTime
        )
    }

    /// Generate a PAD frame with the next sequence number.
    pub fn generate_pad(&mut self) -> Frame {
        self.sequence_counter = self.sequence_counter.wrapping_add(1);
        Frame::pad(self.sequence_counter)
    }

    /// Shape volume: compute recommended number of bytes to send based on
    /// the download/upload ratio target.
    pub fn shape_volume(&self, actual_bytes: usize) -> ShapedVolume {
        let ratio = config::DOWNLOAD_UPLOAD_RATIO;
        let download = (actual_bytes as f32 * ratio) as usize;
        let upload = actual_bytes - download;
        ShapedVolume { download, upload }
    }

    /// Get the current Markov state.
    pub fn current_state(&self) -> MarkovState {
        self.markov.state()
    }
}

impl Default for TrafficShaper {
    fn default() -> Self {
        Self::new()
    }
}

/// Volume shaping result.
#[derive(Debug, Clone, Copy)]
pub struct ShapedVolume {
    pub download: usize,
    pub upload: usize,
}

/// Add timing jitter to a base delay.
pub fn jitter(base_ms: u32, range_ms: u32) -> Duration {
    let mut rng = rand::thread_rng();
    let offset: i32 = rng.gen_range(-(range_ms as i32)..=(range_ms as i32));
    let ms = (base_ms as i64 + offset as i64).max(1) as u64;
    Duration::from_millis(ms)
}

/// Compute idle PAD interval within the configured bandwidth range.
pub fn idle_pad_interval() -> Duration {
    let target_kbps = {
        let mut rng = rand::thread_rng();
        rng.gen_range(config::IDLE_BANDWIDTH_MIN_KBPS..=config::IDLE_BANDWIDTH_MAX_KBPS)
    };
    // frame_size(1024 bytes) / target_kbps * 1000 = ms per frame
    let ms_per_frame = (1024 * 1000) / (target_kbps as u64 * 1024);
    Duration::from_millis(ms_per_frame.max(50))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_markov_initial_state() {
        let m = MarkovModel::new();
        assert_eq!(m.state(), MarkovState::Idle);
    }

    #[test]
    fn test_markov_transitions() {
        let mut m = MarkovModel::new();
        let mut states = std::collections::HashSet::new();
        // Run enough ticks to visit multiple states
        for _ in 0..500 {
            states.insert(m.tick());
        }
        // Should have visited at least 2 different states
        assert!(states.len() >= 2);
    }

    #[test]
    fn test_jitter_bounds() {
        for _ in 0..100 {
            let d = jitter(500, 100);
            assert!(d.as_millis() >= 400);
            assert!(d.as_millis() <= 600);
        }
    }

    #[test]
    fn test_jitter_minimum_one_ms() {
        // Even with large jitter on small base, result should be >= 1ms
        for _ in 0..100 {
            let d = jitter(1, 100);
            assert!(d.as_millis() >= 1);
        }
    }

    #[test]
    fn test_traffic_shaper_delay() {
        let mut ts = TrafficShaper::new();
        let d = ts.next_send_delay();
        assert!(d.as_millis() >= 1);
    }

    #[test]
    fn test_generate_pad() {
        let mut ts = TrafficShaper::new();
        let pad = ts.generate_pad();
        assert_eq!(pad.frame_type, FrameType::Pad);
        assert_eq!(pad.sequence, 1);

        let pad2 = ts.generate_pad();
        assert_eq!(pad2.sequence, 2);
    }

    #[test]
    fn test_shape_volume() {
        let ts = TrafficShaper::new();
        let shaped = ts.shape_volume(1000);
        assert_eq!(shaped.download + shaped.upload, 1000);
        // Download should be ~80% of total
        assert!(shaped.download >= 700 && shaped.download <= 900);
    }

    #[test]
    fn test_idle_pad_interval_bounds() {
        for _ in 0..100 {
            let d = idle_pad_interval();
            assert!(d.as_millis() >= 50);
            // At 5 KB/s for 1024 bytes: ~200ms; at 15 KB/s: ~67ms
            assert!(d.as_millis() <= 250);
        }
    }
}
