// Marabunta - Licensed under the MIT License.
//! Progress reporting for Marabunta tasks
//!
//! Provides thread-safe progress tracking with stages and messages.
//! Progress can be reported as a percentage (0-100) with optional
//! stage names and descriptive messages.

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU8, Ordering};

/// Maximum length for stage names
pub const MAX_STAGE_NAME_LEN: usize = 128;

/// Maximum length for progress messages
pub const MAX_MESSAGE_LEN: usize = 512;

/// Progress state for a task
#[derive(Debug)]
pub struct ProgressState {
    /// Current progress percentage (0-100)
    percent: AtomicU8,
    /// Current stage name
    stage: RwLock<String>,
    /// Current progress message
    message: RwLock<String>,
}

impl ProgressState {
    /// Create a new progress state at 0%
    pub fn new() -> Self {
        Self {
            percent: AtomicU8::new(0),
            stage: RwLock::new(String::new()),
            message: RwLock::new(String::new()),
        }
    }

    /// Get current progress percentage
    #[inline]
    pub fn get_percent(&self) -> u8 {
        self.percent.load(Ordering::Relaxed)
    }

    /// Set progress percentage (clamped to 0-100)
    #[inline]
    pub fn set_percent(&self, percent: u8) {
        self.percent.store(percent.min(100), Ordering::Relaxed);
    }

    /// Increment progress by delta (clamped to 100)
    #[inline]
    pub fn increment(&self, delta: u8) {
        loop {
            let current = self.percent.load(Ordering::Relaxed);
            let new = current.saturating_add(delta).min(100);
            if self
                .percent
                .compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Get current stage name
    pub fn get_stage(&self) -> String {
        self.stage.read().clone()
    }

    /// Set current stage name (truncated to MAX_STAGE_NAME_LEN)
    pub fn set_stage(&self, stage: &str) {
        let truncated = if stage.len() > MAX_STAGE_NAME_LEN {
            &stage[..MAX_STAGE_NAME_LEN]
        } else {
            stage
        };
        *self.stage.write() = truncated.to_string();
    }

    /// Get current progress message
    pub fn get_message(&self) -> String {
        self.message.read().clone()
    }

    /// Set current progress message (truncated to MAX_MESSAGE_LEN)
    pub fn set_message(&self, message: &str) {
        let truncated = if message.len() > MAX_MESSAGE_LEN {
            &message[..MAX_MESSAGE_LEN]
        } else {
            message
        };
        *self.message.write() = truncated.to_string();
    }

    /// Reset progress to initial state
    pub fn reset(&self) {
        self.percent.store(0, Ordering::Relaxed);
        *self.stage.write() = String::new();
        *self.message.write() = String::new();
    }

    /// Get a snapshot of the current progress
    pub fn snapshot(&self) -> ProgressSnapshot {
        ProgressSnapshot {
            percent: self.get_percent(),
            stage: self.get_stage(),
            message: self.get_message(),
        }
    }
}

impl Default for ProgressState {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable snapshot of progress state
#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    /// Progress percentage (0-100)
    pub percent: u8,
    /// Current stage name
    pub stage: String,
    /// Current progress message
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_new_progress_starts_at_zero() {
        let state = ProgressState::new();
        assert_eq!(state.get_percent(), 0);
        assert!(state.get_stage().is_empty());
        assert!(state.get_message().is_empty());
    }

    #[test]
    fn test_set_percent() {
        let state = ProgressState::new();
        state.set_percent(50);
        assert_eq!(state.get_percent(), 50);
    }

    #[test]
    fn test_percent_clamped_to_100() {
        let state = ProgressState::new();
        state.set_percent(150);
        assert_eq!(state.get_percent(), 100);
    }

    #[test]
    fn test_increment() {
        let state = ProgressState::new();
        state.set_percent(10);
        state.increment(5);
        assert_eq!(state.get_percent(), 15);
    }

    #[test]
    fn test_increment_clamped() {
        let state = ProgressState::new();
        state.set_percent(99);
        state.increment(10);
        assert_eq!(state.get_percent(), 100);
    }

    #[test]
    fn test_set_stage() {
        let state = ProgressState::new();
        state.set_stage("Processing data");
        assert_eq!(state.get_stage(), "Processing data");
    }

    #[test]
    fn test_stage_truncated() {
        let state = ProgressState::new();
        let long_stage = "x".repeat(MAX_STAGE_NAME_LEN + 50);
        state.set_stage(&long_stage);
        assert_eq!(state.get_stage().len(), MAX_STAGE_NAME_LEN);
    }

    #[test]
    fn test_set_message() {
        let state = ProgressState::new();
        state.set_message("Doing important work");
        assert_eq!(state.get_message(), "Doing important work");
    }

    #[test]
    fn test_message_truncated() {
        let state = ProgressState::new();
        let long_message = "y".repeat(MAX_MESSAGE_LEN + 50);
        state.set_message(&long_message);
        assert_eq!(state.get_message().len(), MAX_MESSAGE_LEN);
    }

    #[test]
    fn test_reset() {
        let state = ProgressState::new();
        state.set_percent(75);
        state.set_stage("Stage 3");
        state.set_message("Almost done");

        state.reset();

        assert_eq!(state.get_percent(), 0);
        assert!(state.get_stage().is_empty());
        assert!(state.get_message().is_empty());
    }

    #[test]
    fn test_snapshot() {
        let state = ProgressState::new();
        state.set_percent(42);
        state.set_stage("Testing");
        state.set_message("Running tests");

        let snapshot = state.snapshot();

        assert_eq!(snapshot.percent, 42);
        assert_eq!(snapshot.stage, "Testing");
        assert_eq!(snapshot.message, "Running tests");
    }

    #[test]
    fn test_thread_safety() {
        let state = Arc::new(ProgressState::new());
        let mut handles = vec![];

        // Spawn multiple threads that increment progress
        for _ in 0..10 {
            let state_clone = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    state_clone.increment(1);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All increments should be counted (10 threads * 10 increments = 100)
        assert_eq!(state.get_percent(), 100);
    }

    #[test]
    fn test_concurrent_stage_and_message() {
        let state = Arc::new(ProgressState::new());
        let mut handles = vec![];

        for i in 0..5 {
            let state_clone = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    state_clone.set_stage(&format!("Thread {} Stage {}", i, j));
                    state_clone.set_message(&format!("Thread {} Message {}", i, j));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Just verify no panic occurred and we can still read values
        let _ = state.get_stage();
        let _ = state.get_message();
    }
}
