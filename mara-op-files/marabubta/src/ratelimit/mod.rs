// Marabunta - Licensed under the MIT License.
//! Rate limiting for Marabunta Compute APIs
//!
//! This module provides comprehensive rate limiting functionality for the Marabunta Compute
//! control plane and API endpoints. It implements the token bucket algorithm with
//! support for per-client limits, global limits, burst allowance, and auth-level tiers.
//!
//! # Architecture
//!
//! The rate limiting system consists of four main components:
//!
//! - **Types** (`types.rs`): Core data structures for rate limits, keys, and results
//! - **Limiter** (`limiter.rs`): Token bucket implementation with configurable rates
//! - **Storage** (`storage.rs`): Backend storage for rate limit state (in-memory and distributed)
//! - **Middleware** (`middleware.rs`): Axum middleware for automatic rate limiting
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use axum::Router;
//! use marabunta_compute::ratelimit::{RateLimiter, RateLimitLayer};
//!
//! // Create rate limiter with default config
//! let limiter = Arc::new(RateLimiter::new());
//!
//! // Add rate limiting to your router
//! let app = Router::new()
//!     .route("/api/jobs", axum::routing::get(|| async { "jobs" }))
//!     .layer(RateLimitLayer::new(limiter));
//! ```
//!
//! # Token Bucket Algorithm
//!
//! The token bucket algorithm works as follows:
//!
//! 1. Each client (identified by IP, API key, or tenant) has a "bucket" of tokens
//! 2. The bucket has a maximum capacity (burst size)
//! 3. Tokens are added to the bucket at a fixed rate (requests per second)
//! 4. Each API request consumes one token
//! 5. If no tokens are available, the request is rate limited
//!
//! This approach allows for smooth rate limiting while permitting occasional bursts
//! of traffic up to the burst size.
//!
//! # Configuration
//!
//! Rate limits can be configured at multiple levels:
//!
//! - **Default limits**: Applied to all endpoints without specific overrides
//! - **Per-endpoint limits**: Specific rates for particular API paths
//! - **Global limits**: Limits shared across all clients (for expensive operations)
//! - **Auth-level limits**: Different limits based on authentication tier
//! - **Exempt paths**: Endpoints excluded from rate limiting (e.g., health checks)
//!
//! ```rust
//! use marabunta_compute::ratelimit::{RateLimit, RateLimiterConfig, AuthLevel};
//!
//! let config = RateLimiterConfig::new()
//!     .with_default_limit(RateLimit::new(100.0, 200))
//!     .with_endpoint_limit("/api/jobs", RateLimit::new(50.0, 100))
//!     .with_global_limit("/api/forecast", RateLimit::new(10.0, 20))
//!     .with_exempt_path("/health")
//!     .with_auth_level_limit(AuthLevel::Admin, RateLimit::permissive());
//! ```
//!
//! # Rate Limit Headers
//!
//! The middleware automatically includes standard rate limit headers in responses:
//!
//! - `X-RateLimit-Limit`: Maximum requests allowed
//! - `X-RateLimit-Remaining`: Requests remaining in current window
//! - `X-RateLimit-Reset`: Unix timestamp when the limit resets
//! - `X-RateLimit-Reset-After`: Seconds until the limit resets
//! - `Retry-After`: Seconds to wait before retrying (on 429 responses)
//!
//! # Multi-Coordinator Support
//!
//! For distributed deployments with multiple coordinators, use the `DistributedStorage`
//! backend to synchronize rate limits across nodes:
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use marabunta_compute::ratelimit::{RateLimiter, RateLimiterConfig};
//! use marabunta_compute::ratelimit::storage::DistributedStorage;
//!
//! // Create distributed storage
//! let storage = Arc::new(DistributedStorage::new("coordinator-1"));
//!
//! // Add peer coordinators
//! storage.add_peer("coordinator-2:8080");
//! storage.add_peer("coordinator-3:8080");
//!
//! // Create rate limiter with distributed storage
//! let config = RateLimiterConfig::new().distributed(true);
//! let limiter = RateLimiter::with_storage(config, storage);
//! ```
//!
//! # Integration with Control Plane
//!
//! The rate limiting module integrates with the control plane API:
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use axum::Router;
//! use marabunta_compute::ratelimit::{RateLimiter, RateLimitLayer, RateLimiterConfig, RateLimit};
//! use marabunta_compute::control_plane::create_control_plane_router;
//!
//! // Configure rate limits for control plane endpoints
//! let config = RateLimiterConfig::new()
//!     .with_endpoint_limit("/api/dashboard", RateLimit::new(60.0, 120))
//!     .with_endpoint_limit("/api/nodes/:id/override", RateLimit::strict())
//!     .with_global_limit("/api/forecast", RateLimit::new(10.0, 50))
//!     .with_exempt_path("/health")
//!     .with_exempt_path("/ready")
//!     .with_exempt_path("/metrics");
//!
//! let limiter = Arc::new(RateLimiter::with_config(config));
//!
//! // Apply to control plane router
//! // let control_plane = create_control_plane_router(state)
//! //     .layer(RateLimitLayer::new(limiter));
//! ```
//!
//! # Error Responses
//!
//! When a request is rate limited, the middleware returns:
//!
//! ```json
//! {
//!   "error": "Rate limit exceeded. Please try again later.",
//!   "status": 429,
//!   "retry_after_secs": 5,
//!   "rate_limit": {
//!     "limit": 100,
//!     "remaining": 0,
//!     "reset_at": 1704067200
//!   }
//! }
//! ```

pub mod limiter;
pub mod middleware;
pub mod storage;
pub mod types;

// Re-export commonly used types
pub use limiter::{RateLimiter, RateLimiterConfig, RateLimiterStats, TokenBucket};
pub use middleware::{
    headers, RateLimitLayer, RateLimitLayerBuilder, RateLimitMiddlewareConfig, RateLimitState,
    RateLimitedResponse,
};
pub use storage::{DistributedStorage, InMemoryStorage, RateLimitStorage, StorageStats};
pub use types::{
    AuthLevel, RateLimit, RateLimitInfo, RateLimitKey, RateLimitResult, RateLimitScope,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_end_to_end_rate_limiting() {
        // Create a rate limiter with strict limits for testing
        let mut config = RateLimiterConfig::new()
            .with_default_limit(RateLimit::new(100.0, 5))
            .with_exempt_path("/health");
        // Clear overrides so the default_limit is used
        config.auth_level_limits.clear();
        config.endpoint_limits.clear();

        let limiter = RateLimiter::with_config(config);

        // Test regular endpoint (using a path that won't have overrides)
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let key = RateLimitKey::by_ip(ip, "/api/test");

        // First 5 requests should be allowed
        for i in 0..5 {
            let result = limiter.check_with_auth(&key, AuthLevel::Anonymous);
            assert!(result.is_allowed(), "Request {} should be allowed", i);
        }

        // 6th request should be rate limited
        let result = limiter.check_with_auth(&key, AuthLevel::Anonymous);
        assert!(result.is_limited());
        assert!(result.retry_after().is_some());
    }

    #[test]
    fn test_auth_level_tiers() {
        let limiter = RateLimiter::new();

        // Check that different auth levels get different limits
        let anon_limit = limiter
            .config()
            .get_limit("/api/test", AuthLevel::Anonymous);
        let auth_limit = limiter
            .config()
            .get_limit("/api/test", AuthLevel::Authenticated);
        let admin_limit = limiter.config().get_limit("/api/test", AuthLevel::Admin);

        assert!(auth_limit.requests_per_second > anon_limit.requests_per_second);
        assert!(admin_limit.requests_per_second > auth_limit.requests_per_second);
    }

    #[test]
    fn test_exempt_paths() {
        let config = RateLimiterConfig::new()
            .with_default_limit(RateLimit::new(100.0, 1))
            .with_exempt_path("/health")
            .with_exempt_path("/metrics");

        let limiter = RateLimiter::with_config(config);

        // Exempt paths should always be allowed
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let health_key = RateLimitKey::by_ip(ip, "/health");
        let metrics_key = RateLimitKey::by_ip(ip, "/metrics/prometheus");

        for _ in 0..100 {
            assert!(limiter
                .check(&health_key, &RateLimit::strict())
                .is_allowed());
            assert!(limiter
                .check(&metrics_key, &RateLimit::strict())
                .is_allowed());
        }
    }

    #[test]
    fn test_hierarchical_limiting() {
        let config = RateLimiterConfig::new()
            .with_default_limit(RateLimit::new(100.0, 10))
            .with_global_limit("/api/expensive", RateLimit::new(100.0, 2));

        let limiter = RateLimiter::with_config(config);

        let ip1: IpAddr = "192.168.1.1".parse().unwrap();
        let ip2: IpAddr = "192.168.1.2".parse().unwrap();

        let key1 = RateLimitKey::by_ip(ip1, "/api/expensive");
        let key2 = RateLimitKey::by_ip(ip2, "/api/expensive");

        // First 2 requests (from different clients) should hit global limit
        assert!(limiter
            .check_hierarchical(&key1, AuthLevel::Anonymous)
            .is_allowed());
        assert!(limiter
            .check_hierarchical(&key2, AuthLevel::Anonymous)
            .is_allowed());

        // 3rd request should be blocked by global limit
        let result = limiter.check_hierarchical(&key1, AuthLevel::Anonymous);
        assert!(result.is_limited());
    }

    #[test]
    fn test_rate_limit_presets() {
        let permissive = RateLimit::permissive();
        let standard = RateLimit::standard();
        let strict = RateLimit::strict();

        assert!(permissive.requests_per_second > standard.requests_per_second);
        assert!(standard.requests_per_second > strict.requests_per_second);
    }

    #[test]
    fn test_distributed_storage() {
        let storage = DistributedStorage::new("node-1");

        storage.add_peer("node-2:8080");
        assert_eq!(storage.peers().len(), 1);

        // Should work like local storage
        let result = storage.try_consume("test-key", 10, 1.0);
        assert!(result.is_some());
    }

    #[test]
    fn test_rate_limit_result_methods() {
        let allowed = RateLimitResult::Allowed {
            remaining: 99,
            reset_after: Duration::from_secs(60),
        };

        assert!(allowed.is_allowed());
        assert!(!allowed.is_limited());
        assert_eq!(allowed.remaining(), Some(99));
        assert!(allowed.retry_after().is_none());

        let limited = RateLimitResult::Limited {
            retry_after: Duration::from_secs(5),
        };

        assert!(!limited.is_allowed());
        assert!(limited.is_limited());
        assert!(limited.remaining().is_none());
        assert_eq!(limited.retry_after(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_rate_limit_key_variants() {
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        let ip_key = RateLimitKey::by_ip(ip, "/api/jobs");
        assert_eq!(ip_key.endpoint(), "/api/jobs");

        let api_key = RateLimitKey::by_api_key("my-key", "/api/jobs");
        assert_eq!(api_key.endpoint(), "/api/jobs");

        let tenant_key = RateLimitKey::by_tenant("tenant-1", "/api/jobs");
        assert_eq!(tenant_key.endpoint(), "/api/jobs");

        let global_key = RateLimitKey::global("/api/dashboard");
        assert_eq!(global_key.endpoint(), "/api/dashboard");
    }

    #[test]
    fn test_limiter_reset() {
        let limiter = RateLimiter::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let key = RateLimitKey::by_ip(ip, "/api/test");
        let limit = RateLimit::new(100.0, 5);

        // Exhaust the limit
        for _ in 0..5 {
            limiter.check(&key, &limit);
        }
        assert!(limiter.check(&key, &limit).is_limited());

        // Reset and verify we can make requests again
        limiter.reset(&key);
        assert!(limiter.check(&key, &limit).is_allowed());
    }

    #[test]
    fn test_limiter_stats() {
        let limiter = RateLimiter::new();

        // Make some requests
        for i in 0..5 {
            let ip: IpAddr = format!("192.168.1.{}", i).parse().unwrap();
            let key = RateLimitKey::by_ip(ip, "/api/test");
            limiter.check(&key, &RateLimit::standard());
        }

        let stats = limiter.stats();
        assert_eq!(stats.total_buckets, 5);
        assert!(stats.active_buckets > 0);
    }
}
