// Marabunta - Licensed under the MIT License.
//! Rate limiting types for Marabunta Compute APIs
//!
//! This module defines the core types used throughout the rate limiting system,
//! including rate limit configurations, keys for client identification, and results.

use std::net::IpAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for a rate limit rule.
///
/// Uses the token bucket algorithm parameters:
/// - `requests_per_second`: The sustained rate at which requests are allowed
/// - `burst_size`: Maximum number of requests allowed in a burst
///
/// # Example
///
/// ```
/// use marabunta_compute::ratelimit::RateLimit;
///
/// // Allow 100 requests per second with bursts up to 200
/// let limit = RateLimit::new(100.0, 200);
///
/// // More restrictive limit for expensive operations
/// let strict_limit = RateLimit::new(10.0, 20);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RateLimit {
    /// Requests allowed per second (token refill rate)
    pub requests_per_second: f64,
    /// Maximum burst size (bucket capacity)
    pub burst_size: u32,
}

impl RateLimit {
    /// Create a new rate limit configuration.
    ///
    /// # Arguments
    ///
    /// * `requests_per_second` - The sustained rate at which requests are allowed
    /// * `burst_size` - Maximum number of requests allowed in a burst
    pub fn new(requests_per_second: f64, burst_size: u32) -> Self {
        Self {
            requests_per_second,
            burst_size,
        }
    }

    /// Create a permissive rate limit for internal/health endpoints.
    pub fn permissive() -> Self {
        Self {
            requests_per_second: 10000.0,
            burst_size: 50000,
        }
    }

    /// Create a standard rate limit for regular API endpoints.
    pub fn standard() -> Self {
        Self {
            requests_per_second: 100.0,
            burst_size: 200,
        }
    }

    /// Create a strict rate limit for expensive operations.
    pub fn strict() -> Self {
        Self {
            requests_per_second: 10.0,
            burst_size: 20,
        }
    }

    /// Calculate the time between token refills.
    pub fn refill_interval(&self) -> Duration {
        if self.requests_per_second <= 0.0 {
            Duration::from_secs(u64::MAX)
        } else {
            Duration::from_secs_f64(1.0 / self.requests_per_second)
        }
    }
}

impl Default for RateLimit {
    fn default() -> Self {
        Self::standard()
    }
}

/// Key used to identify a rate limit bucket.
///
/// Rate limits can be applied at different granularities:
/// - By IP address (for unauthenticated requests)
/// - By API key (for authenticated requests)
/// - By tenant ID (for multi-tenant deployments)
/// - Global (across all clients)
///
/// # Example
///
/// ```
/// use std::net::IpAddr;
/// use marabunta_compute::ratelimit::RateLimitKey;
///
/// // Rate limit by IP address
/// let ip: IpAddr = "192.168.1.1".parse().unwrap();
/// let key = RateLimitKey::by_ip(ip, "/api/jobs");
///
/// // Rate limit by API key
/// let key = RateLimitKey::by_api_key("my-api-key", "/api/jobs");
///
/// // Global rate limit for an endpoint
/// let key = RateLimitKey::global("/api/dashboard");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RateLimitKey {
    /// Rate limit by client IP address
    Ip { address: IpAddr, endpoint: String },
    /// Rate limit by API key
    ApiKey { key: String, endpoint: String },
    /// Rate limit by tenant ID
    Tenant { tenant_id: String, endpoint: String },
    /// Global rate limit for an endpoint (across all clients)
    Global { endpoint: String },
    /// Combined key for hierarchical rate limiting
    Combined {
        client_id: String,
        endpoint: String,
        scope: RateLimitScope,
    },
}

impl RateLimitKey {
    /// Create a rate limit key by IP address.
    pub fn by_ip(address: IpAddr, endpoint: impl Into<String>) -> Self {
        Self::Ip {
            address,
            endpoint: endpoint.into(),
        }
    }

    /// Create a rate limit key by API key.
    pub fn by_api_key(key: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self::ApiKey {
            key: key.into(),
            endpoint: endpoint.into(),
        }
    }

    /// Create a rate limit key by tenant ID.
    pub fn by_tenant(tenant_id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self::Tenant {
            tenant_id: tenant_id.into(),
            endpoint: endpoint.into(),
        }
    }

    /// Create a global rate limit key.
    pub fn global(endpoint: impl Into<String>) -> Self {
        Self::Global {
            endpoint: endpoint.into(),
        }
    }

    /// Create a combined rate limit key.
    pub fn combined(
        client_id: impl Into<String>,
        endpoint: impl Into<String>,
        scope: RateLimitScope,
    ) -> Self {
        Self::Combined {
            client_id: client_id.into(),
            endpoint: endpoint.into(),
            scope,
        }
    }

    /// Get the endpoint associated with this key.
    pub fn endpoint(&self) -> &str {
        match self {
            Self::Ip { endpoint, .. } => endpoint,
            Self::ApiKey { endpoint, .. } => endpoint,
            Self::Tenant { endpoint, .. } => endpoint,
            Self::Global { endpoint } => endpoint,
            Self::Combined { endpoint, .. } => endpoint,
        }
    }
}

/// Scope for rate limiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitScope {
    /// Per-client limit
    PerClient,
    /// Per-endpoint limit (across all clients)
    PerEndpoint,
    /// Global limit (across all endpoints and clients)
    Global,
}

/// Result of a rate limit check.
///
/// # Example
///
/// ```
/// use marabunta_compute::ratelimit::RateLimitResult;
/// use std::time::Duration;
///
/// // Check the result of a rate limit check
/// let result = RateLimitResult::Allowed {
///     remaining: 95,
///     reset_after: Duration::from_secs(60),
/// };
///
/// match result {
///     RateLimitResult::Allowed { remaining, .. } => {
///         println!("Request allowed, {} requests remaining", remaining);
///     }
///     RateLimitResult::Limited { retry_after } => {
///         println!("Rate limited, retry after {:?}", retry_after);
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RateLimitResult {
    /// Request is allowed
    Allowed {
        /// Number of requests remaining in the current window
        remaining: u32,
        /// Time until the rate limit resets
        reset_after: Duration,
    },
    /// Request is rate limited
    Limited {
        /// Time to wait before retrying
        retry_after: Duration,
    },
}

impl RateLimitResult {
    /// Check if the request was allowed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed { .. })
    }

    /// Check if the request was rate limited.
    pub fn is_limited(&self) -> bool {
        matches!(self, Self::Limited { .. })
    }

    /// Get the retry-after duration if rate limited.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Limited { retry_after } => Some(*retry_after),
            Self::Allowed { .. } => None,
        }
    }

    /// Get the remaining requests if allowed.
    pub fn remaining(&self) -> Option<u32> {
        match self {
            Self::Allowed { remaining, .. } => Some(*remaining),
            Self::Limited { .. } => None,
        }
    }
}

/// Authentication level for rate limit tiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuthLevel {
    /// Unauthenticated requests (most restrictive)
    #[default]
    Anonymous,
    /// Basic authenticated user
    Authenticated,
    /// Premium/paid tier user
    Premium,
    /// Admin or internal service (least restrictive)
    Admin,
}

impl AuthLevel {
    /// Get the rate limit multiplier for this auth level.
    ///
    /// Higher auth levels get higher limits.
    pub fn multiplier(&self) -> f64 {
        match self {
            Self::Anonymous => 1.0,
            Self::Authenticated => 5.0,
            Self::Premium => 20.0,
            Self::Admin => 100.0,
        }
    }
}

/// Rate limit information returned in API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    /// Maximum requests allowed in the window
    pub limit: u32,
    /// Requests remaining in the current window
    pub remaining: u32,
    /// Unix timestamp when the rate limit resets
    pub reset_at: u64,
    /// Seconds until the rate limit resets
    pub reset_after_secs: u64,
}

impl RateLimitInfo {
    /// Create new rate limit info.
    pub fn new(limit: u32, remaining: u32, reset_after: Duration) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            limit,
            remaining,
            reset_at: now + reset_after.as_secs(),
            reset_after_secs: reset_after.as_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_creation() {
        let limit = RateLimit::new(100.0, 200);
        assert_eq!(limit.requests_per_second, 100.0);
        assert_eq!(limit.burst_size, 200);
    }

    #[test]
    fn test_rate_limit_presets() {
        let permissive = RateLimit::permissive();
        assert!(permissive.requests_per_second >= 10000.0);

        let standard = RateLimit::standard();
        assert_eq!(standard.requests_per_second, 100.0);
        assert_eq!(standard.burst_size, 200);

        let strict = RateLimit::strict();
        assert_eq!(strict.requests_per_second, 10.0);
        assert_eq!(strict.burst_size, 20);
    }

    #[test]
    fn test_rate_limit_refill_interval() {
        let limit = RateLimit::new(100.0, 200);
        let interval = limit.refill_interval();
        assert_eq!(interval, Duration::from_millis(10));

        let slow = RateLimit::new(1.0, 10);
        assert_eq!(slow.refill_interval(), Duration::from_secs(1));
    }

    #[test]
    fn test_rate_limit_key_by_ip() {
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let key = RateLimitKey::by_ip(ip, "/api/jobs");

        assert_eq!(key.endpoint(), "/api/jobs");
        match key {
            RateLimitKey::Ip { address, .. } => {
                assert_eq!(address, ip);
            }
            _ => panic!("Expected Ip variant"),
        }
    }

    #[test]
    fn test_rate_limit_key_by_api_key() {
        let key = RateLimitKey::by_api_key("my-key", "/api/dashboard");
        assert_eq!(key.endpoint(), "/api/dashboard");
    }

    #[test]
    fn test_rate_limit_key_global() {
        let key = RateLimitKey::global("/api/health");
        assert_eq!(key.endpoint(), "/api/health");
    }

    #[test]
    fn test_rate_limit_result_allowed() {
        let result = RateLimitResult::Allowed {
            remaining: 99,
            reset_after: Duration::from_secs(60),
        };

        assert!(result.is_allowed());
        assert!(!result.is_limited());
        assert_eq!(result.remaining(), Some(99));
        assert!(result.retry_after().is_none());
    }

    #[test]
    fn test_rate_limit_result_limited() {
        let result = RateLimitResult::Limited {
            retry_after: Duration::from_secs(30),
        };

        assert!(!result.is_allowed());
        assert!(result.is_limited());
        assert!(result.remaining().is_none());
        assert_eq!(result.retry_after(), Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_auth_level_multiplier() {
        assert_eq!(AuthLevel::Anonymous.multiplier(), 1.0);
        assert_eq!(AuthLevel::Authenticated.multiplier(), 5.0);
        assert_eq!(AuthLevel::Premium.multiplier(), 20.0);
        assert_eq!(AuthLevel::Admin.multiplier(), 100.0);
    }

    #[test]
    fn test_rate_limit_info() {
        let info = RateLimitInfo::new(100, 95, Duration::from_secs(60));
        assert_eq!(info.limit, 100);
        assert_eq!(info.remaining, 95);
        assert_eq!(info.reset_after_secs, 60);
    }
}
