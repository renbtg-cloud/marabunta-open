// Marabunta - Licensed under the MIT License.
//! SecurityDomains against device fingerprinting.
//!
//! This module implements various techniques to prevent the coordinator or
//! network observers from fingerprinting participants based on their device
//! characteristics, timing patterns, or behavior.
//!
//! # Fingerprinting Threats
//!
//! - **Hardware fingerprinting**: CPU, memory, storage characteristics
//! - **Software fingerprinting**: OS version, installed software
//! - **Timing fingerprinting**: Processing speed, network latency patterns
//! - **Behavioral fingerprinting**: Usage patterns, activity times
//!
//! # SecurityDomain Strategies
//!
//! 1. **Characteristic normalization**: Report common/generic values
//! 2. **Timing jitter**: Add random delays to normalize timing
//! 3. **Identity rotation**: Periodically change apparent identity
//! 4. **Bandwidth normalization**: Cap/pad bandwidth to common tiers

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rand::Rng;

use super::errors::SecurityDomainError;
use super::onion::OnionRouter;

/// Configuration for fingerprint security_domains.
#[derive(Debug, Clone)]
pub struct SecurityDomainConfig {
    /// Whether to randomize operation timing.
    pub randomize_timing: bool,

    /// Whether to normalize reported bandwidth.
    pub normalize_bandwidth: bool,

    /// Whether to report fake device characteristics.
    pub fake_characteristics: bool,

    /// How often to rotate identity.
    pub rotate_identity_interval: Duration,

    /// Maximum timing jitter in milliseconds.
    pub max_timing_jitter_ms: u64,

    /// Minimum timing jitter in milliseconds.
    pub min_timing_jitter_ms: u64,

    /// Target latency for normalization in milliseconds.
    pub target_latency_ms: u32,

    /// Whether to randomize the order of operations.
    pub randomize_operation_order: bool,

    /// Bandwidth tiers to normalize to (in KB/s).
    pub bandwidth_tiers: Vec<u32>,

    /// Whether to add random activity during idle periods.
    pub idle_activity: bool,
}

impl Default for SecurityDomainConfig {
    fn default() -> Self {
        Self {
            randomize_timing: true,
            normalize_bandwidth: true,
            fake_characteristics: true,
            rotate_identity_interval: Duration::from_secs(3600), // 1 hour
            max_timing_jitter_ms: 100,
            min_timing_jitter_ms: 10,
            target_latency_ms: 200,
            randomize_operation_order: true,
            bandwidth_tiers: vec![1024, 2048, 5120, 10240, 20480], // 1, 2, 5, 10, 20 MB/s
            idle_activity: true,
        }
    }
}

/// Fake device profile to report instead of real characteristics.
#[derive(Debug, Clone)]
pub struct FakeProfile {
    /// Fake number of CPU cores.
    pub cpu_cores: u32,

    /// Fake RAM in gigabytes.
    pub ram_gb: u32,

    /// Generic OS string.
    pub os_string: String,

    /// Fake benchmark score.
    pub benchmark_score: u32,

    /// Artificial network latency to add.
    pub network_latency_ms: u32,

    /// Fake timezone offset.
    pub timezone_offset_hours: i8,

    /// Fake locale.
    pub locale: String,

    /// Generation timestamp.
    pub generated_at: Instant,

    /// Unique profile ID (for tracking rotations).
    pub profile_id: u64,
}

impl FakeProfile {
    /// Generate a new random fake profile.
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();

        // Common CPU core counts
        let cpu_options = [2, 4, 4, 4, 6, 8, 8, 8, 12, 16];
        let cpu_cores = cpu_options[rng.gen_range(0..cpu_options.len())];

        // Common RAM sizes (powers of 2)
        let ram_options = [4, 8, 8, 8, 16, 16, 32];
        let ram_gb = ram_options[rng.gen_range(0..ram_options.len())];

        // Generic OS strings
        let os_options = ["Linux", "Linux", "Linux", "Windows", "Windows", "macOS"];
        let os_string = os_options[rng.gen_range(0..os_options.len())].to_string();

        // Benchmark score with jitter (±30%)
        let base_score = 1000u32;
        let jitter = rng.gen_range(700..1300) as f32 / 1000.0;
        let benchmark_score = (base_score as f32 * jitter) as u32;

        // Random added latency
        let network_latency_ms = rng.gen_range(50..150);

        // Common timezone offsets
        let tz_options = [-8, -7, -5, -4, 0, 1, 2, 8, 9];
        let timezone_offset_hours = tz_options[rng.gen_range(0..tz_options.len())];

        // Common locales
        let locale_options = ["en_US", "en_GB", "de_DE", "fr_FR", "ja_JP", "zh_CN"];
        let locale = locale_options[rng.gen_range(0..locale_options.len())].to_string();

        FakeProfile {
            cpu_cores,
            ram_gb,
            os_string,
            benchmark_score,
            network_latency_ms,
            timezone_offset_hours,
            locale,
            generated_at: Instant::now(),
            profile_id: rng.gen(),
        }
    }

    /// Get profile age.
    pub fn age(&self) -> Duration {
        self.generated_at.elapsed()
    }

    /// Check if profile needs rotation.
    pub fn needs_rotation(&self, max_age: Duration) -> bool {
        self.age() > max_age
    }
}

impl Default for FakeProfile {
    fn default() -> Self {
        Self::generate()
    }
}

/// Device characteristics to report (fake or real).
#[derive(Debug, Clone)]
pub struct DeviceCharacteristics {
    /// CPU core count.
    pub cpu_cores: u32,
    /// RAM in GB.
    pub ram_gb: u32,
    /// Operating system.
    pub os: String,
    /// Benchmark score.
    pub benchmark_score: u32,
    /// Network latency.
    pub latency_ms: u32,
    /// Available bandwidth in KB/s.
    pub bandwidth_kbps: u32,
    /// Timezone.
    pub timezone: String,
    /// Locale.
    pub locale: String,
}

/// Main fingerprint security_domain system.
pub struct FingerprintSecurityDomain {
    /// Configuration.
    config: SecurityDomainConfig,

    /// Current fake profile.
    fake_profile: RwLock<FakeProfile>,

    /// Statistics.
    stats: SecurityDomainStats,

    /// Last rotation time.
    last_rotation: RwLock<Instant>,
}

/// Statistics about security_domain operations.
#[derive(Debug, Default)]
pub struct SecurityDomainStats {
    /// Number of timing jitters applied.
    pub jitters_applied: AtomicU64,
    /// Total jitter time in milliseconds.
    pub total_jitter_ms: AtomicU64,
    /// Number of identity rotations.
    pub identity_rotations: AtomicU64,
    /// Number of bandwidth normalizations.
    pub bandwidth_normalizations: AtomicU64,
    /// Number of fake characteristics served.
    pub fake_characteristics_served: AtomicU64,
}

impl FingerprintSecurityDomain {
    /// Create a new fingerprint security_domain system.
    pub fn new(config: SecurityDomainConfig) -> Self {
        FingerprintSecurityDomain {
            config,
            fake_profile: RwLock::new(FakeProfile::generate()),
            stats: SecurityDomainStats::default(),
            last_rotation: RwLock::new(Instant::now()),
        }
    }

    /// Generate a new fake device profile.
    pub fn generate_fake_profile(&self) -> FakeProfile {
        let profile = FakeProfile::generate();
        *self.fake_profile.write() = profile.clone();
        profile
    }

    /// Get the current fake profile.
    pub fn current_profile(&self) -> FakeProfile {
        self.fake_profile.read().clone()
    }

    /// Add timing jitter to a base duration.
    ///
    /// Returns a duration with random jitter added.
    pub async fn jittered_delay(&self, base: Duration) -> Duration {
        if !self.config.randomize_timing {
            return base;
        }

        let jitter_range = self.config.max_timing_jitter_ms - self.config.min_timing_jitter_ms;
        let jitter_ms = if jitter_range > 0 {
            let mut rng = rand::thread_rng();
            self.config.min_timing_jitter_ms + rng.gen_range(0..jitter_range)
        } else {
            self.config.min_timing_jitter_ms
        };

        self.stats.jitters_applied.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_jitter_ms
            .fetch_add(jitter_ms, Ordering::Relaxed);

        // Actually sleep for the jitter
        tokio::time::sleep(Duration::from_millis(jitter_ms)).await;

        base + Duration::from_millis(jitter_ms)
    }

    /// Normalize bandwidth to the nearest common tier.
    ///
    /// Returns a normalized bandwidth value that matches common hardware.
    pub fn normalize_bandwidth(&self, actual_kbps: u32) -> u32 {
        if !self.config.normalize_bandwidth {
            return actual_kbps;
        }

        // Find the closest tier that doesn't exceed actual bandwidth
        let normalized = self
            .config
            .bandwidth_tiers
            .iter()
            .filter(|&&tier| tier <= actual_kbps)
            .max()
            .copied()
            .unwrap_or(self.config.bandwidth_tiers[0]);

        self.stats
            .bandwidth_normalizations
            .fetch_add(1, Ordering::Relaxed);

        normalized
    }

    /// Get device characteristics to report.
    ///
    /// Returns fake characteristics if configured, otherwise real ones.
    pub fn reported_characteristics(&self) -> DeviceCharacteristics {
        self.stats
            .fake_characteristics_served
            .fetch_add(1, Ordering::Relaxed);

        if self.config.fake_characteristics {
            let profile = self.fake_profile.read();
            DeviceCharacteristics {
                cpu_cores: profile.cpu_cores,
                ram_gb: profile.ram_gb,
                os: profile.os_string.clone(),
                benchmark_score: profile.benchmark_score,
                latency_ms: profile.network_latency_ms,
                bandwidth_kbps: self.normalize_bandwidth(10240), // Default to 10 MB/s tier
                timezone: format!("UTC{:+}", profile.timezone_offset_hours),
                locale: profile.locale.clone(),
            }
        } else {
            // Return actual characteristics (less private)
            self.get_real_characteristics()
        }
    }

    /// Get real device characteristics.
    fn get_real_characteristics(&self) -> DeviceCharacteristics {
        // In practice, would use sysinfo crate
        DeviceCharacteristics {
            cpu_cores: num_cpus::get() as u32,
            ram_gb: 16, // Placeholder
            os: std::env::consts::OS.to_string(),
            benchmark_score: 1000,
            latency_ms: 50,
            bandwidth_kbps: 10240,
            timezone: "UTC".to_string(),
            locale: "en_US".to_string(),
        }
    }

    /// Rotate identity (new fake profile and optionally new circuit).
    pub async fn rotate_identity(&self, router: &OnionRouter) -> Result<(), SecurityDomainError> {
        // Generate new fake profile
        let new_profile = FakeProfile::generate();
        *self.fake_profile.write() = new_profile;
        *self.last_rotation.write() = Instant::now();

        self.stats
            .identity_rotations
            .fetch_add(1, Ordering::Relaxed);

        // Build a new circuit with the new identity
        router
            .build_circuit()
            .await
            .map_err(|e| SecurityDomainError::RotationFailed(e.to_string()))?;

        Ok(())
    }

    /// Check if identity rotation is due.
    pub fn rotation_due(&self) -> bool {
        self.last_rotation.read().elapsed() > self.config.rotate_identity_interval
    }

    /// Perform rotation if due.
    pub async fn maybe_rotate(&self, router: &OnionRouter) -> Result<bool, SecurityDomainError> {
        if self.rotation_due() {
            self.rotate_identity(router).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get security_domain statistics.
    pub fn stats(&self) -> &SecurityDomainStats {
        &self.stats
    }

    /// Get current configuration.
    pub fn config(&self) -> &SecurityDomainConfig {
        &self.config
    }

    /// Get time until next scheduled rotation.
    pub fn time_until_rotation(&self) -> Duration {
        let elapsed = self.last_rotation.read().elapsed();
        self.config.rotate_identity_interval.saturating_sub(elapsed)
    }
}

/// Latency normalizer to make all operations appear to take similar time.
pub struct LatencyNormalizer {
    /// Target latency for all operations.
    target_latency_ms: u32,

    /// Additional random jitter.
    jitter_ms: u32,

    /// Statistics.
    stats: LatencyNormalizerStats,
}

/// Statistics for latency normalization.
#[derive(Debug, Default)]
pub struct LatencyNormalizerStats {
    /// Operations normalized.
    pub operations_normalized: AtomicU64,
    /// Total delay added in milliseconds.
    pub total_delay_ms: AtomicU64,
    /// Operations that exceeded target.
    pub operations_exceeded: AtomicU64,
}

impl LatencyNormalizer {
    /// Create a new latency normalizer.
    pub fn new(target_latency_ms: u32, jitter_ms: u32) -> Self {
        LatencyNormalizer {
            target_latency_ms,
            jitter_ms,
            stats: LatencyNormalizerStats::default(),
        }
    }

    /// Normalize the timing of an async operation.
    ///
    /// Adds delay so that the operation appears to take the target time.
    pub async fn normalize<T, F>(&self, operation: F) -> T
    where
        F: Future<Output = T>,
    {
        let start = Instant::now();
        let result = operation.await;
        let elapsed = start.elapsed();

        self.stats
            .operations_normalized
            .fetch_add(1, Ordering::Relaxed);

        // Calculate required delay
        let mut rng = rand::thread_rng();
        let jitter = if self.jitter_ms > 0 {
            rng.gen_range(0..self.jitter_ms)
        } else {
            0
        };

        let target = Duration::from_millis((self.target_latency_ms + jitter) as u64);

        if elapsed < target {
            let delay = target - elapsed;
            self.stats
                .total_delay_ms
                .fetch_add(delay.as_millis() as u64, Ordering::Relaxed);
            tokio::time::sleep(delay).await;
        } else {
            self.stats
                .operations_exceeded
                .fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    /// Normalize a synchronous operation by adding sleep after.
    pub fn normalize_sync<T, F>(&self, operation: F) -> T
    where
        F: FnOnce() -> T,
    {
        let start = Instant::now();
        let result = operation();
        let elapsed = start.elapsed();

        self.stats
            .operations_normalized
            .fetch_add(1, Ordering::Relaxed);

        let mut rng = rand::thread_rng();
        let jitter = if self.jitter_ms > 0 {
            rng.gen_range(0..self.jitter_ms)
        } else {
            0
        };

        let target = Duration::from_millis((self.target_latency_ms + jitter) as u64);

        if elapsed < target {
            let delay = target - elapsed;
            self.stats
                .total_delay_ms
                .fetch_add(delay.as_millis() as u64, Ordering::Relaxed);
            std::thread::sleep(delay);
        } else {
            self.stats
                .operations_exceeded
                .fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    /// Get statistics.
    pub fn stats(&self) -> &LatencyNormalizerStats {
        &self.stats
    }

    /// Get target latency.
    pub fn target_latency(&self) -> Duration {
        Duration::from_millis(self.target_latency_ms as u64)
    }
}

/// Operation timing randomizer for reordering operations.
pub struct OperationRandomizer {
    /// Whether randomization is enabled.
    enabled: bool,

    /// Maximum delay for randomization.
    max_delay_ms: u32,

    /// Statistics.
    stats: RandomizerStats,
}

/// Statistics for operation randomization.
#[derive(Debug, Default)]
pub struct RandomizerStats {
    /// Operations randomized.
    pub operations_randomized: AtomicU64,
    /// Total randomization delay.
    pub total_delay_ms: AtomicU64,
}

impl OperationRandomizer {
    /// Create a new operation randomizer.
    pub fn new(enabled: bool, max_delay_ms: u32) -> Self {
        OperationRandomizer {
            enabled,
            max_delay_ms,
            stats: RandomizerStats::default(),
        }
    }

    /// Add random delay before an operation.
    pub async fn randomize_before<T, F>(&self, operation: F) -> T
    where
        F: Future<Output = T>,
    {
        if self.enabled && self.max_delay_ms > 0 {
            let delay_ms = rand::thread_rng().gen_range(0..self.max_delay_ms);
            self.stats
                .operations_randomized
                .fetch_add(1, Ordering::Relaxed);
            self.stats
                .total_delay_ms
                .fetch_add(delay_ms as u64, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
        }

        operation.await
    }

    /// Get statistics.
    pub fn stats(&self) -> &RandomizerStats {
        &self.stats
    }
}

/// Builder for fingerprint security_domain configuration.
pub struct SecurityDomainConfigBuilder {
    config: SecurityDomainConfig,
}

impl SecurityDomainConfigBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        SecurityDomainConfigBuilder {
            config: SecurityDomainConfig::default(),
        }
    }

    /// Enable or disable timing randomization.
    pub fn randomize_timing(mut self, enable: bool) -> Self {
        self.config.randomize_timing = enable;
        self
    }

    /// Enable or disable bandwidth normalization.
    pub fn normalize_bandwidth(mut self, enable: bool) -> Self {
        self.config.normalize_bandwidth = enable;
        self
    }

    /// Enable or disable fake characteristics.
    pub fn fake_characteristics(mut self, enable: bool) -> Self {
        self.config.fake_characteristics = enable;
        self
    }

    /// Set identity rotation interval.
    pub fn rotate_identity_interval(mut self, interval: Duration) -> Self {
        self.config.rotate_identity_interval = interval;
        self
    }

    /// Set timing jitter range.
    pub fn timing_jitter(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.config.min_timing_jitter_ms = min_ms;
        self.config.max_timing_jitter_ms = max_ms;
        self
    }

    /// Set target latency.
    pub fn target_latency(mut self, latency_ms: u32) -> Self {
        self.config.target_latency_ms = latency_ms;
        self
    }

    /// Set bandwidth tiers.
    pub fn bandwidth_tiers(mut self, tiers: Vec<u32>) -> Self {
        self.config.bandwidth_tiers = tiers;
        self
    }

    /// Build the configuration.
    pub fn build(self) -> SecurityDomainConfig {
        self.config
    }
}

impl Default for SecurityDomainConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Integrated security_domain system combining all security_domain mechanisms.
pub struct IntegratedSecurityDomain {
    /// Core fingerprint security_domain.
    fingerprint: FingerprintSecurityDomain,

    /// Latency normalizer.
    latency: LatencyNormalizer,

    /// Operation randomizer.
    randomizer: OperationRandomizer,
}

impl IntegratedSecurityDomain {
    /// Create a new integrated security_domain system.
    pub fn new(config: SecurityDomainConfig) -> Self {
        let latency = LatencyNormalizer::new(config.target_latency_ms, 50);
        let randomizer = OperationRandomizer::new(config.randomize_operation_order, 100);

        IntegratedSecurityDomain {
            fingerprint: FingerprintSecurityDomain::new(config),
            latency,
            randomizer,
        }
    }

    /// Get reported device characteristics with all security_domains applied.
    pub fn characteristics(&self) -> DeviceCharacteristics {
        self.fingerprint.reported_characteristics()
    }

    /// Execute an operation with all timing security_domains.
    pub async fn execute_with_security_domain<T, F>(&self, operation: F) -> T
    where
        F: Future<Output = T>,
    {
        // Add randomization
        self.randomizer
            .randomize_before(async {
                // Normalize latency
                self.latency.normalize(operation).await
            })
            .await
    }

    /// Rotate identity with all associated security_domains.
    pub async fn rotate(&self, router: &OnionRouter) -> Result<(), SecurityDomainError> {
        self.fingerprint.rotate_identity(router).await
    }

    /// Check if any maintenance is needed.
    pub fn needs_maintenance(&self) -> bool {
        self.fingerprint.rotation_due()
    }

    /// Get the fingerprint security_domain component.
    pub fn fingerprint_security_domain(&self) -> &FingerprintSecurityDomain {
        &self.fingerprint
    }

    /// Get the latency normalizer.
    pub fn latency_normalizer(&self) -> &LatencyNormalizer {
        &self.latency
    }

    /// Get the operation randomizer.
    pub fn operation_randomizer(&self) -> &OperationRandomizer {
        &self.randomizer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_domain_config_default() {
        let config = SecurityDomainConfig::default();
        assert!(config.randomize_timing);
        assert!(config.normalize_bandwidth);
        assert!(config.fake_characteristics);
        assert!(!config.bandwidth_tiers.is_empty());
    }

    #[test]
    fn test_security_domain_config_builder() {
        let config = SecurityDomainConfigBuilder::new()
            .randomize_timing(false)
            .normalize_bandwidth(false)
            .target_latency(500)
            .build();

        assert!(!config.randomize_timing);
        assert!(!config.normalize_bandwidth);
        assert_eq!(config.target_latency_ms, 500);
    }

    #[test]
    fn test_fake_profile_generation() {
        let profile1 = FakeProfile::generate();
        let profile2 = FakeProfile::generate();

        // Should have valid values
        assert!(profile1.cpu_cores > 0);
        assert!(profile1.ram_gb > 0);
        assert!(!profile1.os_string.is_empty());

        // Profile IDs should be different
        assert_ne!(profile1.profile_id, profile2.profile_id);
    }

    #[test]
    fn test_fake_profile_age() {
        let profile = FakeProfile::generate();
        assert!(profile.age() < Duration::from_secs(1));

        assert!(!profile.needs_rotation(Duration::from_secs(3600)));
        assert!(profile.needs_rotation(Duration::from_nanos(1)));
    }

    #[test]
    fn test_fingerprint_security_domain_creation() {
        let config = SecurityDomainConfig::default();
        let security_domain = FingerprintSecurityDomain::new(config);

        let profile = security_domain.current_profile();
        assert!(profile.cpu_cores > 0);
    }

    #[test]
    fn test_fingerprint_security_domain_generate_profile() {
        let config = SecurityDomainConfig::default();
        let security_domain = FingerprintSecurityDomain::new(config);

        let profile1 = security_domain.current_profile();
        let profile2 = security_domain.generate_fake_profile();

        // New profile should be different
        assert_ne!(profile1.profile_id, profile2.profile_id);

        // Current should be updated
        assert_eq!(security_domain.current_profile().profile_id, profile2.profile_id);
    }

    #[test]
    fn test_normalize_bandwidth() {
        let config = SecurityDomainConfig {
            bandwidth_tiers: vec![1024, 5120, 10240],
            ..Default::default()
        };
        let security_domain = FingerprintSecurityDomain::new(config);

        // Exact match
        assert_eq!(security_domain.normalize_bandwidth(5120), 5120);

        // Between tiers - should pick lower
        assert_eq!(security_domain.normalize_bandwidth(7000), 5120);

        // Above highest tier
        assert_eq!(security_domain.normalize_bandwidth(20000), 10240);

        // Below lowest tier
        assert_eq!(security_domain.normalize_bandwidth(500), 1024);
    }

    #[test]
    fn test_normalize_bandwidth_disabled() {
        let config = SecurityDomainConfig {
            normalize_bandwidth: false,
            ..Default::default()
        };
        let security_domain = FingerprintSecurityDomain::new(config);

        assert_eq!(security_domain.normalize_bandwidth(7777), 7777);
    }

    #[test]
    fn test_reported_characteristics() {
        let config = SecurityDomainConfig::default();
        let security_domain = FingerprintSecurityDomain::new(config);

        let chars = security_domain.reported_characteristics();
        assert!(chars.cpu_cores > 0);
        assert!(chars.ram_gb > 0);
        assert!(!chars.os.is_empty());
    }

    #[test]
    fn test_reported_characteristics_real() {
        let config = SecurityDomainConfig {
            fake_characteristics: false,
            ..Default::default()
        };
        let security_domain = FingerprintSecurityDomain::new(config);

        let chars = security_domain.reported_characteristics();
        // Should return real OS
        assert!(!chars.os.is_empty());
    }

    #[test]
    fn test_rotation_due() {
        let config = SecurityDomainConfig {
            rotate_identity_interval: Duration::from_millis(1),
            ..Default::default()
        };
        let security_domain = FingerprintSecurityDomain::new(config);

        // Not due immediately
        assert!(!security_domain.rotation_due());

        // Wait a bit
        std::thread::sleep(Duration::from_millis(10));
        assert!(security_domain.rotation_due());
    }

    #[test]
    fn test_time_until_rotation() {
        let config = SecurityDomainConfig {
            rotate_identity_interval: Duration::from_secs(3600),
            ..Default::default()
        };
        let security_domain = FingerprintSecurityDomain::new(config);

        let time = security_domain.time_until_rotation();
        assert!(time > Duration::from_secs(3590));
        assert!(time <= Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn test_jittered_delay() {
        let config = SecurityDomainConfig {
            randomize_timing: true,
            min_timing_jitter_ms: 10,
            max_timing_jitter_ms: 50,
            ..Default::default()
        };
        let security_domain = FingerprintSecurityDomain::new(config);

        let base = Duration::from_millis(100);
        let start = Instant::now();
        let result = security_domain.jittered_delay(base).await;
        let elapsed = start.elapsed();

        // Should have added some jitter
        assert!(result >= base);
        assert!(elapsed >= Duration::from_millis(10)); // At least min jitter
    }

    #[tokio::test]
    async fn test_jittered_delay_disabled() {
        let config = SecurityDomainConfig {
            randomize_timing: false,
            ..Default::default()
        };
        let security_domain = FingerprintSecurityDomain::new(config);

        let base = Duration::from_millis(100);
        let result = security_domain.jittered_delay(base).await;

        assert_eq!(result, base);
    }

    #[test]
    fn test_latency_normalizer_creation() {
        let normalizer = LatencyNormalizer::new(200, 50);
        assert_eq!(normalizer.target_latency(), Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_latency_normalizer_normalize() {
        let normalizer = LatencyNormalizer::new(100, 10);

        let start = Instant::now();
        let _result = normalizer
            .normalize(async {
                // Fast operation
                tokio::time::sleep(Duration::from_millis(10)).await;
                42
            })
            .await;
        let elapsed = start.elapsed();

        // Should have normalized to at least target
        assert!(elapsed >= Duration::from_millis(90)); // Allow some slack
    }

    #[tokio::test]
    async fn test_latency_normalizer_fast_operation() {
        let normalizer = LatencyNormalizer::new(50, 0);

        let result = normalizer.normalize(async { 42 }).await;
        assert_eq!(result, 42);

        // Stats should show normalization
        assert!(
            normalizer
                .stats()
                .operations_normalized
                .load(Ordering::Relaxed)
                > 0
        );
    }

    #[test]
    fn test_latency_normalizer_sync() {
        let normalizer = LatencyNormalizer::new(50, 0);

        let start = Instant::now();
        let result = normalizer.normalize_sync(|| 42);
        let elapsed = start.elapsed();

        assert_eq!(result, 42);
        assert!(elapsed >= Duration::from_millis(40)); // Allow some slack
    }

    #[test]
    fn test_operation_randomizer_creation() {
        let randomizer = OperationRandomizer::new(true, 100);
        assert_eq!(
            randomizer
                .stats()
                .operations_randomized
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn test_operation_randomizer_enabled() {
        let randomizer = OperationRandomizer::new(true, 50);

        let result = randomizer.randomize_before(async { 42 }).await;
        assert_eq!(result, 42);
        assert!(
            randomizer
                .stats()
                .operations_randomized
                .load(Ordering::Relaxed)
                > 0
        );
    }

    #[tokio::test]
    async fn test_operation_randomizer_disabled() {
        let randomizer = OperationRandomizer::new(false, 100);

        let start = Instant::now();
        let _result = randomizer.randomize_before(async { 42 }).await;
        let elapsed = start.elapsed();

        // Should be very fast when disabled
        assert!(elapsed < Duration::from_millis(10));
        assert_eq!(
            randomizer
                .stats()
                .operations_randomized
                .load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_integrated_security_domain_creation() {
        let config = SecurityDomainConfig::default();
        let security_domain = IntegratedSecurityDomain::new(config);

        let chars = security_domain.characteristics();
        assert!(chars.cpu_cores > 0);
    }

    #[tokio::test]
    async fn test_integrated_security_domain_execute() {
        let config = SecurityDomainConfig {
            target_latency_ms: 50,
            randomize_operation_order: true,
            ..Default::default()
        };
        let security_domain = IntegratedSecurityDomain::new(config);

        let result = security_domain.execute_with_security_domain(async { 42 }).await;
        assert_eq!(result, 42);
    }

    #[test]
    fn test_integrated_security_domain_needs_maintenance() {
        let config = SecurityDomainConfig {
            rotate_identity_interval: Duration::from_millis(1),
            ..Default::default()
        };
        let security_domain = IntegratedSecurityDomain::new(config);

        assert!(!security_domain.needs_maintenance());
        std::thread::sleep(Duration::from_millis(10));
        assert!(security_domain.needs_maintenance());
    }

    #[test]
    fn test_device_characteristics() {
        let chars = DeviceCharacteristics {
            cpu_cores: 8,
            ram_gb: 16,
            os: "Linux".to_string(),
            benchmark_score: 1000,
            latency_ms: 50,
            bandwidth_kbps: 10240,
            timezone: "UTC+0".to_string(),
            locale: "en_US".to_string(),
        };

        assert_eq!(chars.cpu_cores, 8);
        assert_eq!(chars.os, "Linux");
    }

    #[test]
    fn test_security_domain_stats() {
        let stats = SecurityDomainStats::default();

        stats.jitters_applied.fetch_add(10, Ordering::Relaxed);
        stats.total_jitter_ms.fetch_add(500, Ordering::Relaxed);

        assert_eq!(stats.jitters_applied.load(Ordering::Relaxed), 10);
        assert_eq!(stats.total_jitter_ms.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn test_fake_profile_common_values() {
        // Generate many profiles and check distribution
        let profiles: Vec<_> = (0..100).map(|_| FakeProfile::generate()).collect();

        // All should have reasonable CPU cores
        assert!(profiles
            .iter()
            .all(|p| p.cpu_cores >= 2 && p.cpu_cores <= 16));

        // All should have reasonable RAM
        assert!(profiles.iter().all(|p| p.ram_gb >= 4 && p.ram_gb <= 32));

        // OS should be one of the common options
        let valid_os = ["Linux", "Windows", "macOS"];
        assert!(profiles
            .iter()
            .all(|p| valid_os.contains(&p.os_string.as_str())));
    }
}
