// Marabunta - Licensed under the MIT License.
//! Token bucket rate limiter implementation
//!
//! This module provides the core rate limiting logic using the token bucket algorithm.
//! The implementation supports:
//!
//! - Configurable rates per endpoint
//! - Per-client and global limits
//! - Burst allowance
//! - Thread-safe concurrent access
//!
//! # Algorithm
//!
//! The token bucket algorithm works as follows:
//! - Each bucket has a capacity (burst size) and a refill rate (requests per second)
//! - Tokens are added to the bucket at the refill rate
//! - Each request consumes one token
//! - If no tokens are available, the request is rate limited
//!
//! # Example
//!
//! ```
//! use marabunta_compute::ratelimit::{RateLimiter, RateLimit, RateLimitKey};
//! use std::net::IpAddr;
//!
//! // Create a rate limiter with default settings
//! let limiter = RateLimiter::new();
//!
//! // Check rate limit for a request
//! let ip: IpAddr = "192.168.1.1".parse().unwrap();
//! let key = RateLimitKey::by_ip(ip, "/api/jobs");
//! let result = limiter.check(&key, &RateLimit::standard());
//!
//! if result.is_allowed() {
//!     println!("Request allowed");
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use super::storage::{InMemoryStorage, RateLimitStorage};
use super::types::{AuthLevel, RateLimit, RateLimitKey, RateLimitResult};

/// Token bucket state for a single rate limit key.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    /// Current number of tokens available
    pub tokens: f64,
    /// Maximum capacity (burst size)
    pub capacity: u32,
    /// Token refill rate (tokens per second)
    pub refill_rate: f64,
    /// Last time tokens were updated
    pub last_update: Instant,
}

impl TokenBucket {
    /// Create a new token bucket with full capacity.
    pub fn new(capacity: u32, refill_rate: f64) -> Self {
        Self {
            tokens: capacity as f64,
            capacity,
            refill_rate,
            last_update: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time.
    pub fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update);
        let tokens_to_add = elapsed.as_secs_f64() * self.refill_rate;

        self.tokens = (self.tokens + tokens_to_add).min(self.capacity as f64);
        self.last_update = now;
    }

    /// Try to consume a token. Returns true if successful.
    pub fn try_consume(&mut self) -> bool {
        self.refill();

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Get the time until the next token is available.
    pub fn time_until_next_token(&self) -> Duration {
        if self.tokens >= 1.0 {
            Duration::ZERO
        } else {
            let tokens_needed = 1.0 - self.tokens;
            Duration::from_secs_f64(tokens_needed / self.refill_rate)
        }
    }

    /// Get the current number of available tokens as an integer.
    pub fn available_tokens(&self) -> u32 {
        self.tokens.floor() as u32
    }

    /// Get the time until the bucket is fully refilled.
    pub fn time_until_reset(&self) -> Duration {
        let tokens_needed = self.capacity as f64 - self.tokens;
        if tokens_needed <= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(tokens_needed / self.refill_rate)
        }
    }
}

/// Configuration for the rate limiter.
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Default rate limit for endpoints without specific limits
    pub default_limit: RateLimit,
    /// Per-endpoint rate limit overrides
    pub endpoint_limits: HashMap<String, RateLimit>,
    /// Global rate limits (across all clients)
    pub global_limits: HashMap<String, RateLimit>,
    /// Paths exempt from rate limiting (e.g., health checks)
    pub exempt_paths: Vec<String>,
    /// Rate limits by auth level
    pub auth_level_limits: HashMap<AuthLevel, RateLimit>,
    /// Enable distributed rate limiting (for multi-coordinator)
    pub distributed: bool,
    /// Cleanup interval for expired buckets
    pub cleanup_interval: Duration,
    /// Maximum age for inactive buckets before cleanup
    pub bucket_ttl: Duration,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        let mut endpoint_limits = HashMap::new();
        // Override limits for specific endpoints
        endpoint_limits.insert("/api/jobs".to_string(), RateLimit::new(50.0, 100));
        endpoint_limits.insert("/api/nodes/:id/override".to_string(), RateLimit::strict());

        let mut global_limits = HashMap::new();
        // Global limit for expensive operations
        global_limits.insert("/api/forecast".to_string(), RateLimit::new(100.0, 500));

        let mut auth_level_limits = HashMap::new();
        auth_level_limits.insert(AuthLevel::Anonymous, RateLimit::new(20.0, 40));
        auth_level_limits.insert(AuthLevel::Authenticated, RateLimit::new(100.0, 200));
        auth_level_limits.insert(AuthLevel::Premium, RateLimit::new(500.0, 1000));
        auth_level_limits.insert(AuthLevel::Admin, RateLimit::permissive());

        Self {
            default_limit: RateLimit::standard(),
            endpoint_limits,
            global_limits,
            exempt_paths: vec![
                "/health".to_string(),
                "/ready".to_string(),
                "/metrics".to_string(),
            ],
            auth_level_limits,
            distributed: false,
            cleanup_interval: Duration::from_secs(60),
            bucket_ttl: Duration::from_secs(3600),
        }
    }
}

impl RateLimiterConfig {
    /// Create a new config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the default rate limit.
    pub fn with_default_limit(mut self, limit: RateLimit) -> Self {
        self.default_limit = limit;
        self
    }

    /// Add an endpoint-specific rate limit.
    pub fn with_endpoint_limit(mut self, endpoint: impl Into<String>, limit: RateLimit) -> Self {
        self.endpoint_limits.insert(endpoint.into(), limit);
        self
    }

    /// Add a global rate limit for an endpoint.
    pub fn with_global_limit(mut self, endpoint: impl Into<String>, limit: RateLimit) -> Self {
        self.global_limits.insert(endpoint.into(), limit);
        self
    }

    /// Add an exempt path.
    pub fn with_exempt_path(mut self, path: impl Into<String>) -> Self {
        self.exempt_paths.push(path.into());
        self
    }

    /// Set rate limit for an auth level.
    pub fn with_auth_level_limit(mut self, level: AuthLevel, limit: RateLimit) -> Self {
        self.auth_level_limits.insert(level, limit);
        self
    }

    /// Enable distributed rate limiting.
    pub fn distributed(mut self, enabled: bool) -> Self {
        self.distributed = enabled;
        self
    }

    /// Get the rate limit for a given endpoint and auth level.
    pub fn get_limit(&self, endpoint: &str, auth_level: AuthLevel) -> RateLimit {
        // Check for endpoint-specific limit
        if let Some(limit) = self.endpoint_limits.get(endpoint) {
            return self.apply_auth_multiplier(*limit, auth_level);
        }

        // Check for pattern-matched endpoint
        for (pattern, limit) in &self.endpoint_limits {
            if self.matches_pattern(pattern, endpoint) {
                return self.apply_auth_multiplier(*limit, auth_level);
            }
        }

        // Check for auth-level specific limit
        if let Some(limit) = self.auth_level_limits.get(&auth_level) {
            return *limit;
        }

        // Fall back to default
        self.apply_auth_multiplier(self.default_limit, auth_level)
    }

    /// Apply auth level multiplier to a rate limit.
    fn apply_auth_multiplier(&self, limit: RateLimit, auth_level: AuthLevel) -> RateLimit {
        let multiplier = auth_level.multiplier();
        RateLimit::new(
            limit.requests_per_second * multiplier,
            (limit.burst_size as f64 * multiplier) as u32,
        )
    }

    /// Check if an endpoint matches a pattern (supports :param placeholders).
    fn matches_pattern(&self, pattern: &str, endpoint: &str) -> bool {
        let pattern_parts: Vec<&str> = pattern.split('/').collect();
        let endpoint_parts: Vec<&str> = endpoint.split('/').collect();

        if pattern_parts.len() != endpoint_parts.len() {
            return false;
        }

        pattern_parts
            .iter()
            .zip(endpoint_parts.iter())
            .all(|(p, e)| p.starts_with(':') || *p == *e)
    }

    /// Check if a path is exempt from rate limiting.
    pub fn is_exempt(&self, path: &str) -> bool {
        self.exempt_paths
            .iter()
            .any(|exempt| path == exempt || path.starts_with(&format!("{}/", exempt)))
    }
}

/// The main rate limiter struct.
///
/// Thread-safe rate limiter using the token bucket algorithm.
/// Supports both in-memory and distributed storage backends.
pub struct RateLimiter {
    /// Configuration
    config: RateLimiterConfig,
    /// Storage backend for rate limit state
    #[allow(dead_code)]
    storage: Arc<dyn RateLimitStorage>,
    /// Local bucket cache for fast lookups
    local_buckets: RwLock<HashMap<RateLimitKey, TokenBucket>>,
    /// Last cleanup time
    last_cleanup: RwLock<Instant>,
}

impl RateLimiter {
    /// Create a new rate limiter with default configuration.
    pub fn new() -> Self {
        Self::with_config(RateLimiterConfig::default())
    }

    /// Create a new rate limiter with custom configuration.
    pub fn with_config(config: RateLimiterConfig) -> Self {
        Self {
            config,
            storage: Arc::new(InMemoryStorage::new()),
            local_buckets: RwLock::new(HashMap::new()),
            last_cleanup: RwLock::new(Instant::now()),
        }
    }

    /// Create a new rate limiter with a custom storage backend.
    pub fn with_storage(config: RateLimiterConfig, storage: Arc<dyn RateLimitStorage>) -> Self {
        Self {
            config,
            storage,
            local_buckets: RwLock::new(HashMap::new()),
            last_cleanup: RwLock::new(Instant::now()),
        }
    }

    /// Check if a request should be rate limited.
    ///
    /// # Arguments
    ///
    /// * `key` - The rate limit key identifying the client and endpoint
    /// * `limit` - The rate limit to apply
    ///
    /// # Returns
    ///
    /// A `RateLimitResult` indicating whether the request is allowed or limited.
    pub fn check(&self, key: &RateLimitKey, limit: &RateLimit) -> RateLimitResult {
        self.maybe_cleanup();

        // Check exempt paths first
        if self.config.is_exempt(key.endpoint()) {
            return RateLimitResult::Allowed {
                remaining: u32::MAX,
                reset_after: Duration::ZERO,
            };
        }

        let mut buckets = self.local_buckets.write();
        let bucket = buckets
            .entry(key.clone())
            .or_insert_with(|| TokenBucket::new(limit.burst_size, limit.requests_per_second));

        if bucket.try_consume() {
            RateLimitResult::Allowed {
                remaining: bucket.available_tokens(),
                reset_after: bucket.time_until_reset(),
            }
        } else {
            RateLimitResult::Limited {
                retry_after: bucket.time_until_next_token(),
            }
        }
    }

    /// Check rate limit with auth level consideration.
    pub fn check_with_auth(&self, key: &RateLimitKey, auth_level: AuthLevel) -> RateLimitResult {
        let limit = self.config.get_limit(key.endpoint(), auth_level);
        self.check(key, &limit)
    }

    /// Check global rate limit for an endpoint.
    ///
    /// This checks a shared rate limit across all clients for expensive operations.
    pub fn check_global(&self, endpoint: &str) -> RateLimitResult {
        let limit = self
            .config
            .global_limits
            .get(endpoint)
            .copied()
            .unwrap_or(RateLimit::permissive());

        let key = RateLimitKey::global(endpoint);
        self.check(&key, &limit)
    }

    /// Check both per-client and global limits.
    ///
    /// Returns the more restrictive result.
    pub fn check_hierarchical(&self, key: &RateLimitKey, auth_level: AuthLevel) -> RateLimitResult {
        // Check per-client limit first
        let client_result = self.check_with_auth(key, auth_level);

        // If client limit was hit, no need to check global
        if client_result.is_limited() {
            return client_result;
        }

        // If the key is already global, skip the separate global check to avoid double-consuming
        if matches!(key, RateLimitKey::Global { .. }) {
            return client_result;
        }

        // Check global limit
        let global_result = self.check_global(key.endpoint());

        // Return the more restrictive result
        if global_result.is_limited() {
            global_result
        } else {
            // Return client result with potentially lower remaining count
            match (client_result, global_result) {
                (
                    RateLimitResult::Allowed {
                        remaining: r1,
                        reset_after: reset1,
                    },
                    RateLimitResult::Allowed { remaining: r2, .. },
                ) => RateLimitResult::Allowed {
                    remaining: r1.min(r2),
                    reset_after: reset1,
                },
                _ => client_result,
            }
        }
    }

    /// Get the current state for a rate limit key.
    pub fn get_state(&self, key: &RateLimitKey) -> Option<TokenBucket> {
        self.local_buckets.read().get(key).cloned()
    }

    /// Reset the rate limit for a specific key.
    pub fn reset(&self, key: &RateLimitKey) {
        self.local_buckets.write().remove(key);
    }

    /// Reset all rate limits.
    pub fn reset_all(&self) {
        self.local_buckets.write().clear();
    }

    /// Get the configuration.
    pub fn config(&self) -> &RateLimiterConfig {
        &self.config
    }

    /// Perform cleanup of expired buckets if needed.
    fn maybe_cleanup(&self) {
        let should_cleanup = {
            let last = self.last_cleanup.read();
            last.elapsed() > self.config.cleanup_interval
        };

        if should_cleanup {
            self.cleanup();
        }
    }

    /// Clean up expired buckets.
    fn cleanup(&self) {
        let now = Instant::now();
        let ttl = self.config.bucket_ttl;

        let mut buckets = self.local_buckets.write();
        buckets.retain(|_, bucket| now.duration_since(bucket.last_update) < ttl);

        *self.last_cleanup.write() = now;
    }

    /// Get statistics about the rate limiter.
    pub fn stats(&self) -> RateLimiterStats {
        let buckets = self.local_buckets.read();
        let total_buckets = buckets.len();
        let active_buckets = buckets
            .values()
            .filter(|b| b.last_update.elapsed() < Duration::from_secs(60))
            .count();

        RateLimiterStats {
            total_buckets,
            active_buckets,
            cleanup_interval: self.config.cleanup_interval,
            bucket_ttl: self.config.bucket_ttl,
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the rate limiter state.
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    /// Total number of tracked buckets
    pub total_buckets: usize,
    /// Number of buckets active in the last minute
    pub active_buckets: usize,
    /// Cleanup interval
    pub cleanup_interval: Duration,
    /// Bucket TTL
    pub bucket_ttl: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use std::thread::sleep;

    #[test]
    fn test_token_bucket_creation() {
        let bucket = TokenBucket::new(100, 10.0);
        assert_eq!(bucket.capacity, 100);
        assert_eq!(bucket.refill_rate, 10.0);
        assert_eq!(bucket.tokens, 100.0);
    }

    #[test]
    fn test_token_bucket_consume() {
        let mut bucket = TokenBucket::new(10, 1.0);

        // Should be able to consume 10 tokens
        for _ in 0..10 {
            assert!(bucket.try_consume());
        }

        // 11th should fail
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_token_bucket_refill() {
        let mut bucket = TokenBucket::new(10, 100.0); // 100 tokens/second

        // Consume all tokens
        for _ in 0..10 {
            bucket.try_consume();
        }
        assert!(!bucket.try_consume());

        // Wait for refill (100ms should give ~10 tokens at 100/sec)
        sleep(Duration::from_millis(110));
        bucket.refill();

        assert!(bucket.try_consume());
    }

    #[test]
    fn test_rate_limiter_basic() {
        let limiter = RateLimiter::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let key = RateLimitKey::by_ip(ip, "/api/test");
        let limit = RateLimit::new(100.0, 10);

        // First 10 requests should be allowed
        for _ in 0..10 {
            let result = limiter.check(&key, &limit);
            assert!(result.is_allowed());
        }

        // 11th should be limited
        let result = limiter.check(&key, &limit);
        assert!(result.is_limited());
    }

    #[test]
    fn test_rate_limiter_exempt_paths() {
        let config = RateLimiterConfig::default().with_exempt_path("/health");
        let limiter = RateLimiter::with_config(config);

        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let key = RateLimitKey::by_ip(ip, "/health");

        // Exempt paths should always be allowed
        for _ in 0..1000 {
            let result = limiter.check(&key, &RateLimit::strict());
            assert!(result.is_allowed());
        }
    }

    #[test]
    fn test_rate_limiter_different_clients() {
        let limiter = RateLimiter::new();
        let limit = RateLimit::new(100.0, 5);

        let ip1: IpAddr = "192.168.1.1".parse().unwrap();
        let ip2: IpAddr = "192.168.1.2".parse().unwrap();
        let key1 = RateLimitKey::by_ip(ip1, "/api/test");
        let key2 = RateLimitKey::by_ip(ip2, "/api/test");

        // Exhaust client 1's limit
        for _ in 0..5 {
            limiter.check(&key1, &limit);
        }
        assert!(limiter.check(&key1, &limit).is_limited());

        // Client 2 should still have tokens
        assert!(limiter.check(&key2, &limit).is_allowed());
    }

    #[test]
    fn test_rate_limiter_reset() {
        let limiter = RateLimiter::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let key = RateLimitKey::by_ip(ip, "/api/test");
        let limit = RateLimit::new(100.0, 5);

        // Exhaust limit
        for _ in 0..5 {
            limiter.check(&key, &limit);
        }
        assert!(limiter.check(&key, &limit).is_limited());

        // Reset
        limiter.reset(&key);

        // Should be allowed again
        assert!(limiter.check(&key, &limit).is_allowed());
    }

    #[test]
    fn test_config_pattern_matching() {
        let config =
            RateLimiterConfig::default().with_endpoint_limit("/api/nodes/:id", RateLimit::strict());

        // Should match pattern
        let limit = config.get_limit("/api/nodes/123", AuthLevel::Anonymous);
        assert_eq!(
            limit.requests_per_second,
            RateLimit::strict().requests_per_second
        );
    }

    #[test]
    fn test_config_auth_level_multiplier() {
        let config = RateLimiterConfig::default();

        let anon = config.get_limit("/api/test", AuthLevel::Anonymous);
        let auth = config.get_limit("/api/test", AuthLevel::Authenticated);
        let admin = config.get_limit("/api/test", AuthLevel::Admin);

        // Higher auth levels should get higher limits
        assert!(auth.requests_per_second > anon.requests_per_second);
        assert!(admin.requests_per_second > auth.requests_per_second);
    }

    #[test]
    fn test_rate_limiter_stats() {
        let limiter = RateLimiter::new();
        let limit = RateLimit::standard();

        // Add some buckets
        for i in 0..5 {
            let ip: IpAddr = format!("192.168.1.{}", i).parse().unwrap();
            let key = RateLimitKey::by_ip(ip, "/api/test");
            limiter.check(&key, &limit);
        }

        let stats = limiter.stats();
        assert_eq!(stats.total_buckets, 5);
        assert_eq!(stats.active_buckets, 5);
    }

    #[test]
    fn test_rate_limit_result_info() {
        let allowed = RateLimitResult::Allowed {
            remaining: 99,
            reset_after: Duration::from_secs(60),
        };
        assert_eq!(allowed.remaining(), Some(99));
        assert!(allowed.retry_after().is_none());

        let limited = RateLimitResult::Limited {
            retry_after: Duration::from_secs(30),
        };
        assert!(limited.remaining().is_none());
        assert_eq!(limited.retry_after(), Some(Duration::from_secs(30)));
    }
}
