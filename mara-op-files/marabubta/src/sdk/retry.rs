// Marabunta - Licensed under the MIT License.
//! Retry logic with exponential backoff for the Marabunta SDK
//!
//! This module provides configurable retry strategies with:
//!
//! - Exponential backoff with optional jitter
//! - Configurable max retries and delays
//! - Custom retry predicates
//! - Integration with async operations
//!
//! # Example
//!
//! ```rust
//! use marabunta_compute::sdk::retry::{RetryConfig, RetryStrategy, with_retry};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = RetryConfig::new()
//!         .with_max_retries(5)
//!         .with_initial_delay(Duration::from_millis(100))
//!         .with_max_delay(Duration::from_secs(10))
//!         .with_jitter(true);
//!
//!     let result = with_retry(&config, || async {
//!         // Your fallible operation here
//!         Ok::<_, String>("success")
//!     }).await;
//! }
//! ```

use std::future::Future;
use std::time::Duration;

use rand::Rng;
use tokio::time::sleep;

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries)
    pub max_retries: u32,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Backoff multiplier (e.g., 2.0 for doubling)
    pub multiplier: f64,
    /// Whether to add random jitter to delays
    pub jitter: bool,
    /// Jitter factor (0.0 to 1.0) - max percentage of delay to add/subtract
    pub jitter_factor: f64,
    /// Retryable status codes (for HTTP-based retries)
    pub retryable_status_codes: Vec<u16>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
            jitter_factor: 0.25,
            retryable_status_codes: vec![408, 429, 500, 502, 503, 504],
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a configuration with no retries
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Create a configuration optimized for quick retries
    pub fn quick() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(500),
            multiplier: 1.5,
            jitter: true,
            jitter_factor: 0.2,
            retryable_status_codes: vec![408, 429, 500, 502, 503, 504],
        }
    }

    /// Create a configuration optimized for long-running operations
    pub fn persistent() -> Self {
        Self {
            max_retries: 10,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300),
            multiplier: 2.0,
            jitter: true,
            jitter_factor: 0.25,
            retryable_status_codes: vec![408, 429, 500, 502, 503, 504],
        }
    }

    /// Set maximum number of retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set initial delay
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set maximum delay
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set backoff multiplier
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier.max(1.0);
        self
    }

    /// Enable or disable jitter
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Set jitter factor
    pub fn with_jitter_factor(mut self, factor: f64) -> Self {
        self.jitter_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// Set retryable status codes
    pub fn with_retryable_status_codes(mut self, codes: Vec<u16>) -> Self {
        self.retryable_status_codes = codes;
        self
    }

    /// Check if a status code is retryable
    pub fn is_retryable_status(&self, status: u16) -> bool {
        self.retryable_status_codes.contains(&status)
    }
}

/// Strategy for calculating retry delays
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum RetryStrategy {
    /// Fixed delay between retries
    Fixed,
    /// Exponential backoff (delay doubles each retry)
    #[default]
    Exponential,
    /// Linear backoff (delay increases by initial_delay each retry)
    Linear,
    /// Fibonacci backoff
    Fibonacci,
}


/// State tracker for retry operations
#[derive(Debug, Clone)]
pub struct RetryState {
    config: RetryConfig,
    strategy: RetryStrategy,
    attempt: u32,
    last_delay: Duration,
    prev_delay: Duration, // For Fibonacci
}

impl RetryState {
    /// Create a new retry state
    pub fn new(config: RetryConfig) -> Self {
        Self::with_strategy(config, RetryStrategy::Exponential)
    }

    /// Create a new retry state with a specific strategy
    pub fn with_strategy(config: RetryConfig, strategy: RetryStrategy) -> Self {
        Self {
            last_delay: config.initial_delay,
            prev_delay: Duration::ZERO,
            config,
            strategy,
            attempt: 0,
        }
    }

    /// Get the current attempt number (0-indexed)
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Check if more retries are available
    pub fn has_retries_remaining(&self) -> bool {
        self.attempt < self.config.max_retries
    }

    /// Get the number of retries remaining
    pub fn retries_remaining(&self) -> u32 {
        self.config.max_retries.saturating_sub(self.attempt)
    }

    /// Calculate the delay for the next retry
    pub fn next_delay(&mut self) -> Option<Duration> {
        if !self.has_retries_remaining() {
            return None;
        }

        let base_delay = match self.strategy {
            RetryStrategy::Fixed => self.config.initial_delay,
            RetryStrategy::Exponential => {
                let multiplied =
                    self.config.initial_delay.as_secs_f64() * self.config.multiplier.powi(self.attempt as i32);
                Duration::from_secs_f64(multiplied)
            }
            RetryStrategy::Linear => {
                let added = self.config.initial_delay.as_secs_f64() * (1.0 + self.attempt as f64);
                Duration::from_secs_f64(added)
            }
            RetryStrategy::Fibonacci => {
                if self.attempt == 0 {
                    self.config.initial_delay
                } else if self.attempt == 1 {
                    let next = self.config.initial_delay;
                    self.prev_delay = self.config.initial_delay;
                    self.last_delay = next;
                    next
                } else {
                    let next = self.last_delay + self.prev_delay;
                    self.prev_delay = self.last_delay;
                    self.last_delay = next;
                    next
                }
            }
        };

        // Apply max delay cap
        let capped_delay = base_delay.min(self.config.max_delay);

        // Apply jitter if enabled
        let final_delay = if self.config.jitter {
            apply_jitter(capped_delay, self.config.jitter_factor)
        } else {
            capped_delay
        };

        self.attempt += 1;
        Some(final_delay)
    }

    /// Record an attempt without getting a delay
    pub fn record_attempt(&mut self) {
        self.attempt += 1;
    }

    /// Reset the retry state
    pub fn reset(&mut self) {
        self.attempt = 0;
        self.last_delay = self.config.initial_delay;
        self.prev_delay = Duration::ZERO;
    }
}

/// Apply jitter to a duration
fn apply_jitter(duration: Duration, jitter_factor: f64) -> Duration {
    let mut rng = rand::thread_rng();
    let jitter_range = duration.as_secs_f64() * jitter_factor;
    let jitter = rng.gen_range(-jitter_range..=jitter_range);
    let new_duration = (duration.as_secs_f64() + jitter).max(0.0);
    Duration::from_secs_f64(new_duration)
}

/// Error wrapper that tracks retry information
#[derive(Debug)]
pub struct RetryError<E> {
    /// The last error that occurred
    pub error: E,
    /// Number of attempts made
    pub attempts: u32,
    /// Total time spent retrying
    pub total_time: Duration,
}

impl<E: std::fmt::Display> std::fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Retry failed after {} attempts ({:?}): {}",
            self.attempts, self.total_time, self.error
        )
    }
}

impl<E: std::error::Error + 'static> std::error::Error for RetryError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Execute an async operation with retry logic
///
/// # Example
///
/// ```rust,no_run
/// use marabunta_compute::sdk::retry::{RetryConfig, with_retry};
///
/// #[tokio::main]
/// async fn main() {
///     let config = RetryConfig::new().with_max_retries(3);
///
///     let result = with_retry(&config, || async {
///         // Simulate an operation that might fail
///         if rand::random::<bool>() {
///             Ok("success")
///         } else {
///             Err("failed")
///         }
///     }).await;
/// }
/// ```
pub async fn with_retry<F, Fut, T, E>(config: &RetryConfig, operation: F) -> Result<T, RetryError<E>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    with_retry_strategy(config, RetryStrategy::Exponential, operation).await
}

/// Execute an async operation with retry logic and a specific strategy
pub async fn with_retry_strategy<F, Fut, T, E>(
    config: &RetryConfig,
    strategy: RetryStrategy,
    operation: F,
) -> Result<T, RetryError<E>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let start = std::time::Instant::now();
    let mut state = RetryState::with_strategy(config.clone(), strategy);

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                if let Some(delay) = state.next_delay() {
                    sleep(delay).await;
                } else {
                    return Err(RetryError {
                        error,
                        attempts: state.attempt,
                        total_time: start.elapsed(),
                    });
                }
            }
        }
    }
}

/// Execute an async operation with retry logic and a custom retry predicate
pub async fn with_retry_if<F, Fut, T, E, P>(
    config: &RetryConfig,
    operation: F,
    should_retry: P,
) -> Result<T, RetryError<E>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&E) -> bool,
{
    let start = std::time::Instant::now();
    let mut state = RetryState::new(config.clone());

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                if !should_retry(&error) {
                    return Err(RetryError {
                        error,
                        attempts: state.attempt + 1,
                        total_time: start.elapsed(),
                    });
                }

                if let Some(delay) = state.next_delay() {
                    sleep(delay).await;
                } else {
                    return Err(RetryError {
                        error,
                        attempts: state.attempt,
                        total_time: start.elapsed(),
                    });
                }
            }
        }
    }
}

/// A builder for creating retry operations
pub struct RetryBuilder<F> {
    config: RetryConfig,
    strategy: RetryStrategy,
    operation: F,
}

impl<F, Fut, T, E> RetryBuilder<F>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    /// Create a new retry builder
    pub fn new(operation: F) -> Self {
        Self {
            config: RetryConfig::default(),
            strategy: RetryStrategy::Exponential,
            operation,
        }
    }

    /// Set the retry configuration
    pub fn with_config(mut self, config: RetryConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the retry strategy
    pub fn with_strategy(mut self, strategy: RetryStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set maximum retries
    pub fn max_retries(mut self, max: u32) -> Self {
        self.config.max_retries = max;
        self
    }

    /// Set initial delay
    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.config.initial_delay = delay;
        self
    }

    /// Set maximum delay
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.config.max_delay = delay;
        self
    }

    /// Enable jitter
    pub fn with_jitter(mut self) -> Self {
        self.config.jitter = true;
        self
    }

    /// Execute the operation with retries
    pub async fn execute(self) -> Result<T, RetryError<E>> {
        with_retry_strategy(&self.config, self.strategy, self.operation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert!(config.jitter);
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::new()
            .with_max_retries(5)
            .with_initial_delay(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(60))
            .with_multiplier(3.0)
            .with_jitter(false);

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay, Duration::from_millis(200));
        assert_eq!(config.max_delay, Duration::from_secs(60));
        assert_eq!(config.multiplier, 3.0);
        assert!(!config.jitter);
    }

    #[test]
    fn test_retry_state_exponential() {
        let config = RetryConfig::new()
            .with_max_retries(3)
            .with_initial_delay(Duration::from_millis(100))
            .with_jitter(false);

        let mut state = RetryState::new(config);

        // First retry: 100ms
        let d1 = state.next_delay().unwrap();
        assert_eq!(d1, Duration::from_millis(100));

        // Second retry: 200ms
        let d2 = state.next_delay().unwrap();
        assert_eq!(d2, Duration::from_millis(200));

        // Third retry: 400ms
        let d3 = state.next_delay().unwrap();
        assert_eq!(d3, Duration::from_millis(400));

        // No more retries
        assert!(state.next_delay().is_none());
    }

    #[test]
    fn test_retry_state_linear() {
        let config = RetryConfig::new()
            .with_max_retries(3)
            .with_initial_delay(Duration::from_millis(100))
            .with_jitter(false);

        let mut state = RetryState::with_strategy(config, RetryStrategy::Linear);

        let d1 = state.next_delay().unwrap();
        assert_eq!(d1, Duration::from_millis(100)); // 100 * (1 + 0)

        let d2 = state.next_delay().unwrap();
        assert_eq!(d2, Duration::from_millis(200)); // 100 * (1 + 1)

        let d3 = state.next_delay().unwrap();
        assert_eq!(d3, Duration::from_millis(300)); // 100 * (1 + 2)
    }

    #[test]
    fn test_retry_state_fixed() {
        let config = RetryConfig::new()
            .with_max_retries(3)
            .with_initial_delay(Duration::from_millis(100))
            .with_jitter(false);

        let mut state = RetryState::with_strategy(config, RetryStrategy::Fixed);

        assert_eq!(state.next_delay().unwrap(), Duration::from_millis(100));
        assert_eq!(state.next_delay().unwrap(), Duration::from_millis(100));
        assert_eq!(state.next_delay().unwrap(), Duration::from_millis(100));
    }

    #[test]
    fn test_retry_state_max_delay_cap() {
        let config = RetryConfig::new()
            .with_max_retries(10)
            .with_initial_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(5))
            .with_jitter(false);

        let mut state = RetryState::new(config);

        // After a few iterations, delay should be capped at 5s
        for _ in 0..5 {
            let delay = state.next_delay().unwrap();
            assert!(delay <= Duration::from_secs(5));
        }
    }

    #[tokio::test]
    async fn test_with_retry_success_first_try() {
        let config = RetryConfig::new().with_max_retries(3);

        let result = with_retry(&config, || async { Ok::<_, &str>("success") }).await;

        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_with_retry_success_after_retries() {
        let config = RetryConfig::new()
            .with_max_retries(3)
            .with_initial_delay(Duration::from_millis(1))
            .with_jitter(false);

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = with_retry(&config, || {
            let attempts = attempts_clone.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current < 2 {
                    Err("not yet")
                } else {
                    Ok("success")
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_with_retry_max_retries_exceeded() {
        let config = RetryConfig::new()
            .with_max_retries(2)
            .with_initial_delay(Duration::from_millis(1))
            .with_jitter(false);

        let result = with_retry(&config, || async { Err::<(), _>("always fails") }).await;

        let err = result.unwrap_err();
        assert_eq!(err.error, "always fails");
        assert_eq!(err.attempts, 2);
    }

    #[tokio::test]
    async fn test_with_retry_if_predicate() {
        let config = RetryConfig::new()
            .with_max_retries(3)
            .with_initial_delay(Duration::from_millis(1))
            .with_jitter(false);

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        // Only retry on "retryable" errors
        let result = with_retry_if(
            &config,
            || {
                let attempts = attempts_clone.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(if current < 1 { "retryable" } else { "fatal" })
                }
            },
            |err| *err == "retryable",
        )
        .await;

        let err = result.unwrap_err();
        assert_eq!(err.error, "fatal");
        assert_eq!(err.attempts, 2);
    }

    #[tokio::test]
    async fn test_retry_builder() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = RetryBuilder::new(|| {
            let attempts = attempts_clone.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current < 1 {
                    Err("fail")
                } else {
                    Ok("success")
                }
            }
        })
        .max_retries(3)
        .initial_delay(Duration::from_millis(1))
        .with_strategy(RetryStrategy::Fixed)
        .execute()
        .await;

        assert_eq!(result.unwrap(), "success");
    }

    #[test]
    fn test_no_retry_config() {
        let config = RetryConfig::no_retry();
        assert_eq!(config.max_retries, 0);

        let mut state = RetryState::new(config);
        assert!(state.next_delay().is_none());
    }

    #[test]
    fn test_is_retryable_status() {
        let config = RetryConfig::default();
        assert!(config.is_retryable_status(500));
        assert!(config.is_retryable_status(503));
        assert!(!config.is_retryable_status(400));
        assert!(!config.is_retryable_status(404));
    }
}
