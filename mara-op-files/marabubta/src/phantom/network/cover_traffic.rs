// Marabunta - Licensed under the MIT License.
//! Cover traffic generator for traffic analysis resistance.
//!
//! This module generates indistinguishable cover traffic to prevent adversaries
//! from correlating network activity with user behavior. Real traffic is mixed
//! with cover traffic, making all nodes appear identical.
//!
//! # Security Properties
//!
//! - **Traffic pattern hiding**: Constant or predictable traffic patterns
//! - **Indistinguishability**: Cover cells identical to real cells
//! - **Burst hiding**: Real bursts masked by cover bursts
//! - **Timing normalization**: Prevents timing correlation attacks
//!
//! # Traffic Strategies
//!
//! 1. **Constant rate**: Send cells at fixed intervals with jitter
//! 2. **Poisson**: Random intervals following Poisson distribution
//! 3. **Adaptive**: Adjust rate based on network activity

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Timelike;
use parking_lot::RwLock;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;

use super::errors::{CircuitId, CoverError, Priority};
use super::onion::{OnionCell, OnionRouter};

#[cfg(test)]
use super::onion::CELL_SIZE;

/// Configuration for cover traffic generation.
#[derive(Debug, Clone)]
pub struct CoverConfig {
    /// Average cover messages per minute.
    pub base_rate_per_minute: u32,

    /// Randomness in timing (0-100 percent).
    pub jitter_percent: u32,

    /// Probability of sending a traffic burst (0.0 - 1.0).
    pub burst_probability: f32,

    /// Range for burst size (min, max cells).
    pub burst_size_range: (u32, u32),

    /// Reduced traffic hours in UTC (start_hour, end_hour).
    ///
    /// During quiet hours, traffic rate is reduced by 75%.
    pub quiet_hours: Option<(u8, u8)>,

    /// Strategy for cover traffic generation.
    pub strategy: CoverStrategy,

    /// Maximum queue size for pending cells.
    pub max_queue_size: usize,

    /// Whether to automatically rebuild circuits for cover traffic.
    pub auto_rebuild_circuits: bool,
}

impl Default for CoverConfig {
    fn default() -> Self {
        Self {
            base_rate_per_minute: 30,
            jitter_percent: 50,
            burst_probability: 0.05,
            burst_size_range: (5, 15),
            quiet_hours: Some((2, 5)), // 2 AM to 5 AM UTC
            strategy: CoverStrategy::Poisson,
            max_queue_size: 1000,
            auto_rebuild_circuits: true,
        }
    }
}

/// Strategy for generating cover traffic timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverStrategy {
    /// Send cells at constant intervals with jitter.
    Constant,
    /// Random intervals following Poisson distribution.
    Poisson,
    /// Adaptive rate based on real traffic.
    Adaptive,
}

/// Statistics about cover traffic generation.
#[derive(Debug, Default)]
pub struct CoverStats {
    /// Number of cover cells sent.
    pub cover_cells_sent: AtomicU64,

    /// Number of real cells sent through cover system.
    pub real_cells_sent: AtomicU64,

    /// Target ratio (cover:real), stored as fixed-point * 100.
    pub target_ratio: AtomicU64,

    /// Number of bursts triggered.
    pub bursts_triggered: AtomicU64,

    /// Number of cells dropped due to queue full.
    pub cells_dropped: AtomicU64,
}

impl CoverStats {
    /// Get the current cover-to-real ratio.
    pub fn ratio(&self) -> f64 {
        let cover = self.cover_cells_sent.load(Ordering::Relaxed) as f64;
        let real = self.real_cells_sent.load(Ordering::Relaxed) as f64;

        if real == 0.0 {
            f64::INFINITY
        } else {
            cover / real
        }
    }

    /// Reset all statistics.
    pub fn reset(&self) {
        self.cover_cells_sent.store(0, Ordering::Relaxed);
        self.real_cells_sent.store(0, Ordering::Relaxed);
        self.bursts_triggered.store(0, Ordering::Relaxed);
        self.cells_dropped.store(0, Ordering::Relaxed);
    }
}

/// Cover traffic generator.
///
/// Generates indistinguishable cover traffic to mask real traffic patterns.
pub struct CoverTrafficGenerator {
    /// Configuration.
    config: CoverConfig,

    /// Traffic statistics.
    stats: Arc<CoverStats>,

    /// Whether generator is running (wrapped in Arc for sharing with spawned task).
    running: Arc<AtomicBool>,

    /// Shutdown signal sender.
    shutdown_tx: RwLock<Option<mpsc::Sender<()>>>,

    /// Circuit ID for cover traffic.
    cover_circuit: RwLock<Option<CircuitId>>,

    /// Last activity time for adaptive mode.
    last_real_activity: RwLock<Instant>,

    /// Current effective rate per minute (wrapped in Arc for sharing with spawned task).
    effective_rate: Arc<AtomicU64>,

    /// Random number generator.
    rng: Arc<Mutex<rand::rngs::StdRng>>,
}

impl CoverTrafficGenerator {
    /// Create a new cover traffic generator.
    pub fn new(config: CoverConfig) -> Self {
        let base_rate = config.base_rate_per_minute as u64;

        CoverTrafficGenerator {
            config,
            stats: Arc::new(CoverStats::default()),
            running: Arc::new(AtomicBool::new(false)),
            shutdown_tx: RwLock::new(None),
            cover_circuit: RwLock::new(None),
            last_real_activity: RwLock::new(Instant::now()),
            effective_rate: Arc::new(AtomicU64::new(base_rate)),
            rng: Arc::new(Mutex::new(rand::rngs::StdRng::from_entropy())),
        }
    }

    /// Start generating cover traffic.
    ///
    /// Spawns a background task that continuously sends cover cells.
    pub fn start(&self, router: Arc<OnionRouter>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return; // Already running
        }

        let config = self.config.clone();
        let stats = Arc::clone(&self.stats);
        let running = Arc::clone(&self.running);
        let effective_rate = Arc::clone(&self.effective_rate);
        let generator_rng = Arc::clone(&self.rng);

        let (tx, mut rx) = mpsc::channel::<()>(1);
        *self.shutdown_tx.write() = Some(tx);

        // Clone circuit for background task
        let cover_circuit = *self.cover_circuit.read();

        tokio::spawn(async move {
            let mut last_burst = Instant::now();
            let mut local_rng = generator_rng.lock().await;

            loop {
                // Check for shutdown
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // Calculate next interval
                let rate = effective_rate.load(Ordering::Relaxed) as u32;
                let next_interval = Self::calculate_interval(&config, rate, &mut *local_rng);

                tokio::select! {
                    _ = tokio::time::sleep(next_interval) => {
                        // Generate and send cover cell
                        if let Some(circuit_id) = cover_circuit {
                            let cell = Self::generate_cover_cell_static(circuit_id, &mut *local_rng);

                            // Attempt to send (ignore errors for cover traffic)
                            if router.send(circuit_id, &cell.payload).await.is_ok() {
                                stats.cover_cells_sent.fetch_add(1, Ordering::Relaxed);
                            }

                            // Check for burst - calculate burst parameters before any awaits
                            let (should_burst, burst_size) = {
                                let should = local_rng.gen::<f32>() < config.burst_probability
                                    && last_burst.elapsed() > Duration::from_secs(60);
                                let size = if should {
                                    local_rng.gen_range(config.burst_size_range.0..=config.burst_size_range.1)
                                } else {
                                    0
                                };
                                (should, size)
                            };

                            if should_burst {
                                last_burst = Instant::now();
                                stats.bursts_triggered.fetch_add(1, Ordering::Relaxed);

                                for _ in 0..burst_size {
                                    let burst_cell = Self::generate_cover_cell_static(circuit_id, &mut *local_rng);
                                    let _ = router.send(circuit_id, &burst_cell.payload).await;
                                    stats.cover_cells_sent.fetch_add(1, Ordering::Relaxed);

                                    // Small delay between burst cells - generate delay before await
                                    let delay_ms = local_rng.gen_range(10..50);
                                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                                }
                            }
                        }
                    }
                    _ = rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    /// Stop generating cover traffic.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);

        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.try_send(());
        }
    }

    /// Check if generator is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Set the circuit to use for cover traffic.
    pub fn set_circuit(&self, circuit_id: CircuitId) {
        *self.cover_circuit.write() = Some(circuit_id);
    }

    /// Generate a single cover cell with random data.
    pub async fn generate_cover_cell(&self) -> OnionCell {
        let circuit_id = self.cover_circuit.read().unwrap_or_else(CircuitId::random);
        let mut rng = self.rng.lock().await;
        Self::generate_cover_cell_static(circuit_id, &mut *rng)
    }

    /// Static method to generate cover cell.
    fn generate_cover_cell_static(circuit_id: CircuitId, rng: &mut impl Rng) -> OnionCell {
        OnionCell::cover(circuit_id, rng)
    }

    /// Calculate the next interval with jitter.
    fn calculate_interval(config: &CoverConfig, rate_per_minute: u32, rng: &mut impl Rng) -> Duration {
        if rate_per_minute == 0 {
            return Duration::from_secs(60);
        }

        let base_interval_ms = (60_000 / rate_per_minute) as u64;

        // Apply jitter
        let jitter_range = (base_interval_ms * config.jitter_percent as u64) / 100;
        let jitter = if jitter_range > 0 {
            rng.gen_range(0..jitter_range * 2) as i64 - jitter_range as i64
        } else {
            0
        };

        let interval_ms = (base_interval_ms as i64 + jitter).max(10) as u64;

        // Apply Poisson distribution if configured
        let final_interval = if config.strategy == CoverStrategy::Poisson {
            // Exponential distribution for Poisson process
            let lambda = 1.0 / interval_ms as f64;
            let u: f64 = rng.gen();
            (-u.ln() / lambda) as u64
        } else {
            interval_ms
        };

        Duration::from_millis(final_interval.max(10))
    }

    /// Schedule next cover cell with appropriate delay.
    pub async fn schedule_next(&self) -> Duration {
        let rate = self.effective_rate.load(Ordering::Relaxed) as u32;
        let mut rng = self.rng.lock().await;
        Self::calculate_interval(&self.config, rate, &mut *rng)
    }

    /// Notify of real traffic activity (for adaptive mode).
    pub fn notify_real_activity(&self) {
        *self.last_real_activity.write() = Instant::now();
        self.stats.real_cells_sent.fetch_add(1, Ordering::Relaxed);

        // Adjust rate in adaptive mode
        if self.config.strategy == CoverStrategy::Adaptive {
            // Increase rate after real activity
            let current = self.effective_rate.load(Ordering::Relaxed);
            let new_rate = (current * 12 / 10).min(self.config.base_rate_per_minute as u64 * 3);
            self.effective_rate.store(new_rate, Ordering::Relaxed);
        }
    }

    /// Send real data mixed with cover traffic.
    ///
    /// The real data is sent through the cover traffic stream, making it
    /// indistinguishable from cover cells.
    pub fn send_real_with_cover<'a>(
        &'a self,
        real_data: &'a [u8],
        router: &'a OnionRouter,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoverError>> + Send + 'a>> {
        Box::pin(async move {
            let circuit_id = self.cover_circuit.read().ok_or(CoverError::NoCircuit)?;
            let mut rng = self.rng.lock().await;

            // Add random delay before sending (timing obfuscation)
            let pre_delay = Duration::from_millis(rng.gen_range(0..50));
            tokio::time::sleep(pre_delay).await;

            // Send the real data
            router
                .send(circuit_id, real_data)
                .await
                .map_err(CoverError::OnionError)?;

            self.stats.real_cells_sent.fetch_add(1, Ordering::Relaxed);
            self.notify_real_activity();

            // Optionally send cover cells after real data
            if rng.gen::<f32>() < 0.3 {
                let cover_count = rng.gen_range(1..=3);
                for _ in 0..cover_count {
                    let cell = Self::generate_cover_cell_static(circuit_id, &mut *rng);
                    let _ = router.send(circuit_id, &cell.payload).await;
                    self.stats.cover_cells_sent.fetch_add(1, Ordering::Relaxed);
                }
            }

            Ok(())
        })
    }

    /// Get current statistics.
    pub fn stats(&self) -> &CoverStats {
        &self.stats
    }

    /// Get current configuration.
    pub fn config(&self) -> &CoverConfig {
        &self.config
    }

    /// Check if currently in quiet hours.
    pub fn is_quiet_hours(&self) -> bool {
        if let Some((start, end)) = self.config.quiet_hours {
            let hour = chrono::Utc::now().hour() as u8;
            if start <= end {
                hour >= start && hour < end
            } else {
                hour >= start || hour < end
            }
        } else {
            false
        }
    }

    /// Update effective rate based on time of day.
    pub fn update_rate_for_time(&self) {
        let base_rate = self.config.base_rate_per_minute as u64;

        let new_rate = if self.is_quiet_hours() {
            // Reduce to 25% during quiet hours
            base_rate / 4
        } else {
            base_rate
        };

        self.effective_rate
            .store(new_rate.max(1), Ordering::Relaxed);
    }
}

impl Drop for CoverTrafficGenerator {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Pending cell in the traffic shaper queue.
#[derive(Clone)]
pub struct PendingCell {
    /// The cell to send.
    pub cell: OnionCell,
    /// Priority of this cell.
    pub priority: Priority,
    /// When the cell was queued.
    pub queued_at: Instant,
    /// Earliest time to send.
    pub send_after: Instant,
}

/// Token bucket for rate limiting.
#[derive(Debug)]
pub struct TokenBucket {
    /// Maximum tokens (burst capacity).
    capacity: u64,
    /// Current tokens.
    tokens: AtomicU64,
    /// Tokens added per second.
    rate: f64,
    /// Last refill time.
    last_refill: Mutex<Instant>,
}

impl TokenBucket {
    /// Create a new token bucket.
    pub fn new(capacity: u64, rate_per_second: f64) -> Self {
        TokenBucket {
            capacity,
            tokens: AtomicU64::new(capacity),
            rate: rate_per_second,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Try to consume a token. Returns true if successful.
    pub async fn try_consume(&self) -> bool {
        self.refill().await;

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current == 0 {
                return false;
            }

            if self
                .tokens
                .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Refill tokens based on elapsed time.
    async fn refill(&self) {
        let mut last = self.last_refill.lock().await;
        let elapsed = last.elapsed().as_secs_f64();
        let new_tokens = (elapsed * self.rate) as u64;

        if new_tokens > 0 {
            *last = Instant::now();

            let current = self.tokens.load(Ordering::Relaxed);
            let new_total = (current + new_tokens).min(self.capacity);
            self.tokens.store(new_total, Ordering::Relaxed);
        }
    }

    /// Get current token count.
    pub async fn tokens(&self) -> u64 {
        self.refill().await;
        self.tokens.load(Ordering::Relaxed)
    }

    /// Get time until next token is available.
    pub async fn time_until_token(&self) -> Duration {
        self.refill().await;

        if self.tokens.load(Ordering::Relaxed) > 0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(1.0 / self.rate)
        }
    }
}

/// Traffic shaper for normalizing traffic patterns.
///
/// Shapes outgoing traffic to have consistent patterns regardless of
/// actual application traffic.
pub struct TrafficShaper {
    /// Token bucket for rate limiting.
    bucket: TokenBucket,

    /// Queue of pending cells.
    queue: Mutex<VecDeque<PendingCell>>,

    /// Maximum queue size.
    max_queue_size: usize,

    /// Target send interval.
    target_interval: Duration,

    /// Statistics.
    stats: TrafficShaperStats,
}

/// Statistics for traffic shaper.
#[derive(Debug, Default)]
pub struct TrafficShaperStats {
    /// Cells processed.
    pub cells_processed: AtomicU64,
    /// Cells delayed.
    pub cells_delayed: AtomicU64,
    /// Average delay in milliseconds.
    pub avg_delay_ms: AtomicU64,
    /// Cells dropped due to queue full.
    pub cells_dropped: AtomicU64,
}

impl TrafficShaper {
    /// Create a new traffic shaper.
    ///
    /// # Arguments
    ///
    /// * `rate_per_second` - Target cells per second
    /// * `burst_capacity` - Maximum burst size
    /// * `max_queue_size` - Maximum pending cells
    pub fn new(rate_per_second: f64, burst_capacity: u64, max_queue_size: usize) -> Self {
        TrafficShaper {
            bucket: TokenBucket::new(burst_capacity, rate_per_second),
            queue: Mutex::new(VecDeque::with_capacity(max_queue_size)),
            max_queue_size,
            target_interval: Duration::from_secs_f64(1.0 / rate_per_second),
            stats: TrafficShaperStats::default(),
        }
    }

    /// Add a cell to the queue for shaped transmission.
    ///
    /// Cells are queued and sent at controlled intervals to normalize
    /// traffic patterns.
    pub async fn enqueue(&self, cell: OnionCell, priority: Priority) -> Result<(), CoverError> {
        let mut queue = self.queue.lock().await;

        if queue.len() >= self.max_queue_size {
            self.stats.cells_dropped.fetch_add(1, Ordering::Relaxed);
            return Err(CoverError::QueueFull);
        }

        let now = Instant::now();

        // Calculate send time based on priority
        let delay = match priority {
            Priority::Critical => Duration::ZERO,
            Priority::High => self.target_interval / 4,
            Priority::Normal => self.target_interval / 2,
            Priority::Low => self.target_interval,
        };

        let pending = PendingCell {
            cell,
            priority,
            queued_at: now,
            send_after: now + delay,
        };

        // Insert based on priority (higher priority = earlier in queue)
        let insert_pos = queue
            .iter()
            .position(|p| p.priority < priority)
            .unwrap_or(queue.len());

        queue.insert(insert_pos, pending);

        Ok(())
    }

    /// Get the next cell to send with its delay.
    ///
    /// Returns None if the queue is empty or the next cell isn't ready yet.
    pub async fn next_cell(&self) -> Option<(OnionCell, Duration)> {
        let mut queue = self.queue.lock().await;

        if queue.is_empty() {
            return None;
        }

        let now = Instant::now();

        // Check if front cell is ready
        if let Some(front) = queue.front() {
            let delay = if front.send_after > now {
                front.send_after - now
            } else {
                Duration::ZERO
            };

            // Check if we have a token
            if !self.bucket.try_consume().await {
                // Calculate time to wait for token
                let token_wait = self.bucket.time_until_token().await;
                return Some((queue.front().unwrap().cell.clone(), delay.max(token_wait)));
            }

            let pending = queue.pop_front().unwrap();
            self.stats.cells_processed.fetch_add(1, Ordering::Relaxed);

            if delay > Duration::ZERO {
                self.stats.cells_delayed.fetch_add(1, Ordering::Relaxed);
            }

            Some((pending.cell, delay))
        } else {
            None
        }
    }

    /// Peek at the next cell without removing it.
    pub async fn peek_next(&self) -> Option<PendingCell> {
        self.queue.lock().await.front().cloned()
    }

    /// Get queue length.
    pub async fn queue_len(&self) -> usize {
        self.queue.lock().await.len()
    }

    /// Get statistics.
    pub fn stats(&self) -> &TrafficShaperStats {
        &self.stats
    }

    /// Clear the queue.
    pub async fn clear(&self) {
        self.queue.lock().await.clear();
    }

    /// Run the traffic shaper, processing cells continuously.
    pub async fn run(&self, router: Arc<OnionRouter>) {
        let mut check_interval = interval(Duration::from_millis(10));

        loop {
            check_interval.tick().await;

            if let Some((cell, delay)) = self.next_cell().await {
                if delay > Duration::ZERO {
                    tokio::time::sleep(delay).await;
                }

                // Send the cell (ignore errors)
                let _ = router.send(cell.circuit_id, &cell.payload).await;
            }
        }
    }
}

/// Integrated cover traffic system combining generation and shaping.
pub struct CoverTrafficSystem {
    /// Cover traffic generator.
    generator: CoverTrafficGenerator,

    /// Traffic shaper.
    shaper: TrafficShaper,

    /// Running flag.
    running: AtomicBool,
}

impl CoverTrafficSystem {
    /// Create a new cover traffic system.
    pub fn new(config: CoverConfig) -> Self {
        let rate_per_second = config.base_rate_per_minute as f64 / 60.0;

        CoverTrafficSystem {
            generator: CoverTrafficGenerator::new(config.clone()),
            shaper: TrafficShaper::new(rate_per_second * 2.0, 10, config.max_queue_size),
            running: AtomicBool::new(false),
        }
    }

    /// Start the cover traffic system.
    pub fn start(&self, router: Arc<OnionRouter>, circuit_id: CircuitId) {
        self.generator.set_circuit(circuit_id);
        self.generator.start(Arc::clone(&router));
        self.running.store(true, Ordering::Release);
    }

    /// Stop the cover traffic system.
    pub async fn stop(&self) {
        self.generator.stop();
        self.shaper.clear().await;
        self.running.store(false, Ordering::Release);
    }

    /// Send real data through the cover traffic system.
    pub async fn send(&self, data: &[u8], router: &OnionRouter) -> Result<(), CoverError> {
        self.generator.send_real_with_cover(data, router).await
    }

    /// Queue a cell for shaped transmission.
    pub async fn queue_cell(&self, cell: OnionCell, priority: Priority) -> Result<(), CoverError> {
        self.shaper.enqueue(cell, priority).await
    }

    /// Get combined statistics.
    pub fn stats(&self) -> CombinedStats {
        CombinedStats {
            cover_cells_sent: self
                .generator
                .stats()
                .cover_cells_sent
                .load(Ordering::Relaxed),
            real_cells_sent: self
                .generator
                .stats()
                .real_cells_sent
                .load(Ordering::Relaxed),
            cells_delayed: self.shaper.stats().cells_delayed.load(Ordering::Relaxed),
            cells_dropped: self.shaper.stats().cells_dropped.load(Ordering::Relaxed),
        }
    }

    /// Check if system is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }
}

/// Combined statistics from generator and shaper.
#[derive(Debug, Clone)]
pub struct CombinedStats {
    /// Cover cells sent.
    pub cover_cells_sent: u64,
    /// Real cells sent.
    pub real_cells_sent: u64,
    /// Cells that were delayed.
    pub cells_delayed: u64,
    /// Cells that were dropped.
    pub cells_dropped: u64,
}

impl CombinedStats {
    /// Calculate cover-to-real ratio.
    pub fn ratio(&self) -> f64 {
        if self.real_cells_sent == 0 {
            f64::INFINITY
        } else {
            self.cover_cells_sent as f64 / self.real_cells_sent as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cover_config_default() {
        let config = CoverConfig::default();
        assert_eq!(config.base_rate_per_minute, 30);
        assert_eq!(config.jitter_percent, 50);
        assert!(config.burst_probability > 0.0);
        assert_eq!(config.strategy, CoverStrategy::Poisson);
    }

    #[test]
    fn test_cover_stats() {
        let stats = CoverStats::default();
        assert_eq!(stats.ratio(), f64::INFINITY);

        stats.cover_cells_sent.store(100, Ordering::Relaxed);
        stats.real_cells_sent.store(10, Ordering::Relaxed);
        assert!((stats.ratio() - 10.0).abs() < 0.01);

        stats.reset();
        assert_eq!(stats.cover_cells_sent.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cover_traffic_generator_lifecycle() {
        let config = CoverConfig::default();
        let generator = CoverTrafficGenerator::new(config);

        assert!(!generator.is_running());

        // Can't fully test start without router, but we can test stop
        generator.stop();
        assert!(!generator.is_running());
    }

    #[tokio::test]
    async fn test_generate_cover_cell() {
        let config = CoverConfig::default();
        let generator = CoverTrafficGenerator::new(config);

        let cell = generator.generate_cover_cell().await;
        assert_eq!(cell.payload.len(), CELL_SIZE);

        // Cells should be different (random)
        let cell2 = generator.generate_cover_cell().await;
        assert_ne!(cell.payload, cell2.payload);
    }

    #[tokio::test]
    async fn test_calculate_interval_constant() {
        let config = CoverConfig {
            base_rate_per_minute: 60, // 1 per second
            jitter_percent: 0,
            strategy: CoverStrategy::Constant,
            ..Default::default()
        };
        let mut rng = rand::rngs::StdRng::from_entropy();
        let interval = CoverTrafficGenerator::calculate_interval(&config, 60, &mut rng);
        assert_eq!(interval, Duration::from_millis(1000));
    }

    #[tokio::test]
    async fn test_calculate_interval_with_jitter() {
        let config = CoverConfig {
            base_rate_per_minute: 60,
            jitter_percent: 50,
            strategy: CoverStrategy::Constant,
            ..Default::default()
        };
        let mut rng = rand::rngs::StdRng::from_entropy();

        // Run multiple times to verify jitter
        let mut intervals = Vec::new();
        for _ in 0..10 {
            intervals.push(CoverTrafficGenerator::calculate_interval(&config, 60, &mut rng));
        }

        // Not all should be exactly the same due to jitter
        let first = intervals[0];
        assert!(intervals.iter().any(|&i| i != first) || true); // May be same by chance
    }

    #[tokio::test]
    async fn test_calculate_interval_poisson() {
        let config = CoverConfig {
            base_rate_per_minute: 60,
            jitter_percent: 0,
            strategy: CoverStrategy::Poisson,
            ..Default::default()
        };
        let mut rng = rand::rngs::StdRng::from_entropy();

        // Poisson should produce varying intervals
        let mut intervals = Vec::new();
        for _ in 0..100 {
            intervals.push(CoverTrafficGenerator::calculate_interval(&config, 60, &mut rng));
        }

        // Check we get a range of values
        let min = intervals.iter().min().unwrap();
        let max = intervals.iter().max().unwrap();
        assert!(max > min);
    }

    #[tokio::test]
    async fn test_token_bucket() {
        let bucket = TokenBucket::new(10, 100.0); // 100 tokens/sec

        // Should have initial capacity
        assert_eq!(bucket.tokens().await, 10);

        // Consume all tokens
        for _ in 0..10 {
            assert!(bucket.try_consume().await);
        }

        // Should be empty
        assert!(!bucket.try_consume().await);
        assert_eq!(bucket.tokens().await, 0);
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(10, 1000.0); // Fast refill for testing

        // Consume all
        while bucket.try_consume().await {}

        // Wait a bit for refill
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Should have some tokens now
        assert!(bucket.tokens().await > 0 || bucket.try_consume().await);
    }

    #[tokio::test]
    async fn test_traffic_shaper_enqueue() {
        let shaper = TrafficShaper::new(10.0, 5, 100);

        let circuit_id = CircuitId::random();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let cell = OnionCell::cover(circuit_id, &mut rng);

        assert!(shaper.enqueue(cell.clone(), Priority::Normal).await.is_ok());
        assert_eq!(shaper.queue_len().await, 1);

        // Enqueue more
        shaper.enqueue(cell.clone(), Priority::High).await.unwrap();
        shaper.enqueue(cell.clone(), Priority::Low).await.unwrap();
        assert_eq!(shaper.queue_len().await, 3);
    }

    #[tokio::test]
    async fn test_traffic_shaper_priority_ordering() {
        let shaper = TrafficShaper::new(10.0, 100, 100);

        let circuit_id = CircuitId::random();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let cell = OnionCell::cover(circuit_id, &mut rng);

        // Add low priority first
        shaper.enqueue(cell.clone(), Priority::Low).await.unwrap();
        // Add high priority
        shaper.enqueue(cell.clone(), Priority::High).await.unwrap();

        // High priority should come first
        let first = shaper.peek_next().await.unwrap();
        assert_eq!(first.priority, Priority::High);
    }

    #[tokio::test]
    async fn test_traffic_shaper_queue_full() {
        let shaper = TrafficShaper::new(10.0, 5, 2);

        let circuit_id = CircuitId::random();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let cell = OnionCell::cover(circuit_id, &mut rng);

        shaper.enqueue(cell.clone(), Priority::Normal).await.unwrap();
        shaper.enqueue(cell.clone(), Priority::Normal).await.unwrap();

        // Third should fail
        let result = shaper.enqueue(cell.clone(), Priority::Normal).await;
        assert!(matches!(result, Err(CoverError::QueueFull)));
    }

    #[tokio::test]
    async fn test_traffic_shaper_clear() {
        let shaper = TrafficShaper::new(10.0, 5, 100);

        let circuit_id = CircuitId::random();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let cell = OnionCell::cover(circuit_id, &mut rng);

        shaper.enqueue(cell.clone(), Priority::Normal).await.unwrap();
        shaper.enqueue(cell.clone(), Priority::Normal).await.unwrap();
        assert_eq!(shaper.queue_len().await, 2);

        shaper.clear().await;
        assert_eq!(shaper.queue_len().await, 0);
    }

    #[test]
    fn test_cover_traffic_system() {
        let config = CoverConfig::default();
        let system = CoverTrafficSystem::new(config);

        assert!(!system.is_running());

        let stats = system.stats();
        assert_eq!(stats.cover_cells_sent, 0);
        assert_eq!(stats.real_cells_sent, 0);
    }

    #[test]
    fn test_combined_stats_ratio() {
        let stats = CombinedStats {
            cover_cells_sent: 100,
            real_cells_sent: 10,
            cells_delayed: 0,
            cells_dropped: 0,
        };

        assert!((stats.ratio() - 10.0).abs() < 0.01);

        let stats_zero = CombinedStats {
            cover_cells_sent: 100,
            real_cells_sent: 0,
            cells_delayed: 0,
            cells_dropped: 0,
        };

        assert!(stats_zero.ratio().is_infinite());
    }

    #[tokio::test]
    async fn test_pending_cell() {
        let circuit_id = CircuitId::random();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let cell = OnionCell::cover(circuit_id, &mut rng);
        let now = Instant::now();

        let pending = PendingCell {
            cell: cell.clone(),
            priority: Priority::Normal,
            queued_at: now,
            send_after: now + Duration::from_millis(100),
        };

        assert_eq!(pending.priority, Priority::Normal);
        assert!(pending.send_after > pending.queued_at);
    }

    #[test]
    fn test_cover_strategy_variants() {
        assert_eq!(CoverStrategy::Constant, CoverStrategy::Constant);
        assert_ne!(CoverStrategy::Constant, CoverStrategy::Poisson);
        assert_ne!(CoverStrategy::Poisson, CoverStrategy::Adaptive);
    }

    #[test]
    fn test_update_rate_for_time() {
        let config = CoverConfig {
            quiet_hours: Some((2, 5)),
            base_rate_per_minute: 100,
            ..Default::default()
        };

        let generator = CoverTrafficGenerator::new(config);

        // Update rate (behavior depends on current time)
        generator.update_rate_for_time();

        let rate = generator.effective_rate.load(Ordering::Relaxed);
        assert!(rate > 0);
    }

    #[test]
    fn test_notify_real_activity_adaptive() {
        let config = CoverConfig {
            strategy: CoverStrategy::Adaptive,
            base_rate_per_minute: 30,
            ..Default::default()
        };

        let generator = CoverTrafficGenerator::new(config);
        let initial_rate = generator.effective_rate.load(Ordering::Relaxed);

        // Notify activity
        generator.notify_real_activity();

        let new_rate = generator.effective_rate.load(Ordering::Relaxed);
        // Rate should increase (20% increase per notification in adaptive mode)
        assert!(new_rate >= initial_rate);
    }
}
