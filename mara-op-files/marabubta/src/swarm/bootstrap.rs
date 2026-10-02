// Marabunta - Licensed under the MIT License.
//! Bootstrap / seed discovery module for the Marabunta Swarm.
//!
//! When a new node starts, it needs to find the swarm. The bootstrap module:
//!
//! 1. Connects to one or more seed/bootstrap servers.
//! 2. Sends a [`SwarmMessage::BootstrapRequest`] with the node's identity and traits.
//! 3. Receives a [`SwarmMessage::BootstrapResponse`] with a list of known peers.
//! 4. Populates the [`KnowledgeStore`] with the initial peer set.
//! 5. After bootstrapping, the gossip protocol takes over for ongoing discovery.
//!
//! The module also provides a [`BootstrapServer`] that any swarm node with the
//! [`Trait::CanDiscover`] capability can run. The server responds to incoming
//! bootstrap requests with a sample of live peers from its knowledge store.
//!
//! # Architecture note
//!
//! The transport layer is fire-and-forget with callback-based message handling.
//! Bootstrap works asynchronously:
//!
//! 1. [`BootstrapClient`] sends `BootstrapRequest` to each seed server.
//! 2. The seed server's message handler invokes [`BootstrapServer::handle_request`].
//! 3. The seed server sends back a `BootstrapResponse`.
//! 4. Our message handler receives it and calls [`BootstrapClient::handle_bootstrap_response`].
//! 5. The knowledge store gets populated.
//!
//! [`BootstrapClient::bootstrap`] polls the knowledge store for growth rather than
//! blocking on a direct response, because the response arrives via the general
//! message-handling pipeline.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use tracing::{debug, info, warn};

use super::config::{BOOTSTRAP_MAX_RETRIES, BOOTSTRAP_PEER_REQUEST_COUNT, BOOTSTRAP_RETRY_INTERVAL, DNS_CACHE_TTL};
use super::knowledge::KnowledgeStore;
use super::transport::SwarmTransport;
use super::types::{
    NodeId, NodeInfo, NodeStatus, PeerInfo, ResourceSnapshot, SwarmError, SwarmMessage, SwarmResult,
    Trait,
};

// ============================================================================
// Constants
// ============================================================================

/// Timeout for waiting for knowledge store growth after sending bootstrap
/// requests to all seed servers in a single attempt. This bounds how long
/// we wait before concluding that no seed server responded.
const BOOTSTRAP_POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// How often we poll the knowledge store for new nodes during a single
/// bootstrap attempt.
const BOOTSTRAP_POLL_INTERVAL: Duration = Duration::from_millis(250);

// ============================================================================
// BootstrapResult
// ============================================================================

/// Result of a successful bootstrap attempt.
#[derive(Debug)]
pub struct BootstrapResult {
    /// Number of new peers discovered and merged into the knowledge store.
    pub peers_discovered: usize,
    /// The seed server address that (most likely) responded. When multiple
    /// seeds are contacted, this is the first seed in the list that was
    /// contacted during the successful attempt.
    pub bootstrap_server: SocketAddr,
    /// How many attempts were made before success (1-based).
    pub attempts: u32,
}

// ============================================================================
// BootstrapClient
// ============================================================================

/// Bootstrap client -- connects to seed servers to join the swarm.
///
/// Created at startup with a list of seed server addresses. The [`bootstrap`]
/// method sends `BootstrapRequest` messages and polls the knowledge store for
/// the arrival of new peers, retrying across multiple attempts if needed.
///
/// [`bootstrap`]: BootstrapClient::bootstrap
pub struct BootstrapClient {
    node_id: NodeId,
    knowledge: Arc<KnowledgeStore>,
    transport: Arc<SwarmTransport>,
    seed_servers: Vec<SocketAddr>,
}

impl BootstrapClient {
    /// Create a new bootstrap client.
    ///
    /// # Arguments
    ///
    /// * `node_id` -- This node's unique identity.
    /// * `knowledge` -- Shared knowledge store where discovered peers are merged.
    /// * `transport` -- Transport layer used to send messages to seed servers.
    /// * `seed_servers` -- Addresses of known bootstrap/seed servers.
    pub fn new(
        node_id: NodeId,
        knowledge: Arc<KnowledgeStore>,
        transport: Arc<SwarmTransport>,
        seed_servers: Vec<SocketAddr>,
    ) -> Self {
        Self {
            node_id,
            knowledge,
            transport,
            seed_servers,
        }
    }

    /// Attempt to bootstrap from any available seed server.
    ///
    /// For each attempt (up to [`BOOTSTRAP_MAX_RETRIES`]):
    ///
    /// 1. Send a `BootstrapRequest` to every seed server.
    /// 2. Poll the knowledge store for new nodes (with a timeout of
    ///    [`BOOTSTRAP_POLL_TIMEOUT`]).
    /// 3. If the knowledge store grows, return success.
    /// 4. If no response is detected, sleep [`BOOTSTRAP_RETRY_INTERVAL`] and
    ///    retry.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::BootstrapFailed`] if all retries are exhausted
    /// without discovering any peers.
    pub async fn bootstrap(
        &self,
        my_traits: &HashSet<Trait>,
        my_address: Option<SocketAddr>,
    ) -> SwarmResult<BootstrapResult> {
        if self.seed_servers.is_empty() {
            return Err(SwarmError::BootstrapFailed {
                attempts: 0,
                reason: "no seed servers configured".to_string(),
            });
        }

        info!(
            node_id = %self.node_id,
            seed_count = self.seed_servers.len(),
            seeds = ?self.seed_servers,
            "starting bootstrap",
        );

        let request = SwarmMessage::BootstrapRequest {
            node_id: self.node_id,
            address: my_address,
            traits: my_traits.clone(),
        };

        for attempt in 1..=BOOTSTRAP_MAX_RETRIES {
            debug!(
                node_id = %self.node_id,
                attempt = attempt,
                max_retries = BOOTSTRAP_MAX_RETRIES,
                "bootstrap attempt",
            );

            // Record the knowledge store size before sending requests.
            let node_count_before = self.knowledge.node_count();

            // Send BootstrapRequest to every seed server.
            let mut seeds_contacted: u32 = 0;
            let mut first_contacted_seed: Option<SocketAddr> = None;

            for seed_addr in &self.seed_servers {
                match self.transport.send(*seed_addr, request.clone()).await {
                    Ok(()) => {
                        seeds_contacted += 1;
                        if first_contacted_seed.is_none() {
                            first_contacted_seed = Some(*seed_addr);
                        }
                        debug!(
                            seed = %seed_addr,
                            "sent bootstrap request to seed",
                        );
                    }
                    Err(e) => {
                        warn!(
                            seed = %seed_addr,
                            error = %e,
                            "failed to send bootstrap request to seed",
                        );
                    }
                }
            }

            if seeds_contacted == 0 {
                warn!(
                    attempt = attempt,
                    "could not reach any seed server, will retry",
                );
                if attempt < BOOTSTRAP_MAX_RETRIES {
                    tokio::time::sleep(BOOTSTRAP_RETRY_INTERVAL).await;
                }
                continue;
            }

            // Poll the knowledge store for growth. The response arrives via the
            // message handler pipeline (BootstrapResponse -> handle_bootstrap_response),
            // so we observe it indirectly through the node count increasing.
            let poll_deadline =
                tokio::time::Instant::now() + BOOTSTRAP_POLL_TIMEOUT;

            let mut peers_discovered: usize = 0;

            while tokio::time::Instant::now() < poll_deadline {
                tokio::time::sleep(BOOTSTRAP_POLL_INTERVAL).await;

                let current_count = self.knowledge.node_count();
                if current_count > node_count_before {
                    peers_discovered = current_count - node_count_before;
                    break;
                }
            }

            if peers_discovered > 0 {
                let bootstrap_server =
                    first_contacted_seed.unwrap_or(self.seed_servers[0]);

                info!(
                    node_id = %self.node_id,
                    peers_discovered = peers_discovered,
                    bootstrap_server = %bootstrap_server,
                    attempts = attempt,
                    "bootstrap successful",
                );

                return Ok(BootstrapResult {
                    peers_discovered,
                    bootstrap_server,
                    attempts: attempt,
                });
            }

            // No peers discovered in this attempt.
            warn!(
                node_id = %self.node_id,
                attempt = attempt,
                seeds_contacted = seeds_contacted,
                "no bootstrap response received, will retry",
            );

            if attempt < BOOTSTRAP_MAX_RETRIES {
                tokio::time::sleep(BOOTSTRAP_RETRY_INTERVAL).await;
            }
        }

        Err(SwarmError::BootstrapFailed {
            attempts: BOOTSTRAP_MAX_RETRIES,
            reason: format!(
                "exhausted all {} attempts across {} seed servers",
                BOOTSTRAP_MAX_RETRIES,
                self.seed_servers.len(),
            ),
        })
    }

    /// Handle a `BootstrapResponse`: populate the knowledge store with
    /// discovered peers.
    ///
    /// Called by the message handler when a `BootstrapResponse` arrives from
    /// a seed server. Each peer in the response is converted to a [`NodeInfo`]
    /// and merged into the knowledge store.
    ///
    /// Returns the number of peers that were actually new (i.e., not already
    /// present or stale duplicates).
    pub fn handle_bootstrap_response(&self, peers: Vec<PeerInfo>) -> usize {
        let peer_count = peers.len();
        let mut merged: usize = 0;

        for peer in peers {
            let node_info = NodeInfo {
                node_id: peer.node_id,
                last_seen: Utc::now(),
                traits: peer.traits,
                load: peer.load,
                capacity: ResourceSnapshot::default(),
                address: Some(peer.address),
                via: self.node_id,
                status: NodeStatus::Alive,
                generation: 0,
                trust_level: Default::default(),
                failure_domains: vec![],
                attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            is_training: false,
            is_pgwire_active: false,
            chaos_state: Default::default(),
            };

            if self.knowledge.merge_node(node_info) {
                merged += 1;
            }
        }

        info!(
            node_id = %self.node_id,
            peers_received = peer_count,
            peers_merged = merged,
            "processed bootstrap response",
        );

        merged
    }
}

// ============================================================================
// BootstrapServer
// ============================================================================

/// Bootstrap server -- runs on nodes with the [`Trait::CanDiscover`] capability.
///
/// When a new node sends a `BootstrapRequest`, the server:
///
/// 1. Registers the requesting node in the local knowledge store (so it
///    becomes part of the swarm immediately).
/// 2. Gathers up to [`BOOTSTRAP_PEER_REQUEST_COUNT`] live peers from the
///    knowledge store.
/// 3. Returns a `BootstrapResponse` with the peer list.
pub struct BootstrapServer {
    node_id: NodeId,
    knowledge: Arc<KnowledgeStore>,
}

impl BootstrapServer {
    /// Create a new bootstrap server.
    ///
    /// # Arguments
    ///
    /// * `node_id` -- This node's identity (used as the `via` field when
    ///   registering the requesting node).
    /// * `knowledge` -- Shared knowledge store for peer lookup and registration.
    pub fn new(node_id: NodeId, knowledge: Arc<KnowledgeStore>) -> Self {
        Self { node_id, knowledge }
    }

    /// Handle an incoming bootstrap request.
    ///
    /// Registers the requesting node, then returns a [`SwarmMessage::BootstrapResponse`]
    /// containing up to [`BOOTSTRAP_PEER_REQUEST_COUNT`] live peers that have
    /// reachable addresses.
    ///
    /// # Arguments
    ///
    /// * `request_node_id` -- Identity of the node requesting bootstrap.
    /// * `request_address` -- Network address of the requester (if known).
    /// * `request_traits` -- Capabilities the requester claims.
    pub fn handle_request(
        &self,
        request_node_id: NodeId,
        request_address: Option<SocketAddr>,
        request_traits: HashSet<Trait>,
    ) -> SwarmMessage {
        info!(
            server_node = %self.node_id,
            request_node = %request_node_id,
            request_address = ?request_address,
            request_traits = request_traits.len(),
            "handling bootstrap request",
        );

        // Register the requesting node so it becomes known to the swarm
        // immediately (even before gossip propagates it).
        self.register_new_node(request_node_id, request_address, request_traits);

        // Gather live peers from the knowledge store.
        let live_nodes = self.knowledge.get_live_nodes();

        // Convert to PeerInfo, filtering to only nodes that have a reachable
        // address. Exclude the requesting node itself (they don't need to
        // discover themselves). Limit to BOOTSTRAP_PEER_REQUEST_COUNT.
        let peers: Vec<PeerInfo> = live_nodes
            .into_iter()
            .filter(|n| n.node_id != request_node_id)
            .filter_map(|n| {
                n.address.map(|addr| PeerInfo {
                    node_id: n.node_id,
                    address: addr,
                    traits: n.traits,
                    load: n.load,
                })
            })
            .take(BOOTSTRAP_PEER_REQUEST_COUNT)
            .collect();

        debug!(
            server_node = %self.node_id,
            request_node = %request_node_id,
            peers_returned = peers.len(),
            "bootstrap response prepared",
        );

        SwarmMessage::BootstrapResponse { peers }
    }

    /// Register a new node in the knowledge store.
    ///
    /// Creates a [`NodeInfo`] entry for the requesting node and merges it
    /// into the store. If the node already exists with newer information,
    /// the merge is a no-op (last-writer-wins by timestamp).
    fn register_new_node(
        &self,
        node_id: NodeId,
        address: Option<SocketAddr>,
        traits: HashSet<Trait>,
    ) {
        let node_info = NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits,
            load: 0.0,
            capacity: ResourceSnapshot::default(),
            address,
            via: self.node_id,
            status: NodeStatus::Alive,
            generation: 0,
            trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            is_training: false,
            is_pgwire_active: false,
            chaos_state: Default::default(),
            };

        let is_new = self.knowledge.merge_node(node_info);

        if is_new {
            info!(
                server_node = %self.node_id,
                new_node = %node_id,
                address = ?address,
                "registered new node from bootstrap request",
            );
        } else {
            debug!(
                server_node = %self.node_id,
                node = %node_id,
                "bootstrap request from already-known node",
            );
        }
    }
}

// ============================================================================
// DNS cache
// ============================================================================

/// A time-bounded cache for DNS resolution results.
///
/// Each hostname maps to a list of resolved [`SocketAddr`] values and the
/// [`Instant`] at which the resolution was performed. Entries older than
/// `ttl` are considered stale and re-resolved on the next lookup.
pub struct DnsCache {
    /// hostname -> (resolved_addrs, resolved_at)
    cache: DashMap<String, (Vec<SocketAddr>, Instant)>,
    /// How long a cached entry remains valid.
    ttl: Duration,
}

impl DnsCache {
    /// Create a new DNS cache with the given time-to-live.
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: DashMap::new(),
            ttl,
        }
    }

    /// Create a DNS cache with the default TTL from config.
    pub fn with_default_ttl() -> Self {
        Self::new(DNS_CACHE_TTL)
    }

    /// Resolve a hostname (with port) using the cache.
    ///
    /// If a fresh cached entry exists, it is returned immediately.
    /// Otherwise, `tokio::net::lookup_host` is called and the result
    /// is stored in the cache.
    ///
    /// Returns an error if DNS resolution fails and no cached entry
    /// exists.
    pub async fn resolve_with_cache(&self, host_port: &str) -> Result<Vec<SocketAddr>, std::io::Error> {
        // Check cache first.
        if let Some(entry) = self.cache.get(host_port) {
            let (ref addrs, resolved_at) = *entry;
            if resolved_at.elapsed() < self.ttl {
                debug!(
                    host = %host_port,
                    cached_addrs = addrs.len(),
                    age_ms = resolved_at.elapsed().as_millis() as u64,
                    "DNS cache hit",
                );
                return Ok(addrs.clone());
            }
            // Stale -- will re-resolve below.
            debug!(
                host = %host_port,
                age_ms = resolved_at.elapsed().as_millis() as u64,
                "DNS cache entry stale, re-resolving",
            );
        }

        // Resolve via tokio DNS.
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host(host_port).await?.collect();

        if !addrs.is_empty() {
            self.cache.insert(host_port.to_string(), (addrs.clone(), Instant::now()));
            debug!(
                host = %host_port,
                resolved_addrs = addrs.len(),
                "DNS resolution cached",
            );
        }

        Ok(addrs)
    }

    /// Returns the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Remove all entries from the cache.
    pub fn clear(&self) {
        self.cache.clear();
    }
}

// ============================================================================
// Seed address parsing
// ============================================================================

/// Synchronous seed address parser (numeric IP:port only).
///
/// This is the original `parse_seed_addresses` function, retained as a
/// private fallback. It only handles numeric IP addresses; DNS hostnames
/// are skipped with a warning.
fn parse_seed_addresses_sync(seeds: &[String]) -> Vec<SocketAddr> {
    let mut addresses = Vec::with_capacity(seeds.len());

    for seed in seeds {
        match seed.parse::<SocketAddr>() {
            Ok(addr) => {
                debug!(seed = %seed, addr = %addr, "parsed seed address");
                addresses.push(addr);
            }
            Err(_) => {
                // This is a hostname or invalid address -- the sync parser
                // cannot resolve DNS. The async variant should be used instead.
                debug!(
                    seed = %seed,
                    "sync parser: not a numeric IP:port, skipping",
                );
            }
        }
    }

    addresses
}

/// Resolve seed server addresses from string slices, with DNS support.
///
/// For each seed string:
/// - Try `str::parse::<SocketAddr>()` first (fast path for numeric IPs like "1.2.3.4:4200").
/// - If that fails, try `tokio::net::lookup_host` for DNS resolution.
/// - If DNS returns multiple A/AAAA records, ALL of them are included.
/// - On DNS failure, the seed is logged at warn level and skipped.
///
/// Returns a deduplicated `Vec<SocketAddr>`.
///
/// # Examples
///
/// ```ignore
/// let seeds = vec![
///     "192.168.1.10:4200".to_string(),
///     "localhost:4200".to_string(),
/// ];
/// let addrs = resolve_seed_addresses(&seeds).await;
/// assert!(addrs.len() >= 2); // localhost may resolve to both 127.0.0.1 and [::1]
/// ```
pub async fn resolve_seed_addresses(seeds: &[String]) -> Vec<SocketAddr> {
    let mut addresses = Vec::with_capacity(seeds.len());
    let mut seen = HashSet::new();

    for seed in seeds {
        // Fast path: try parsing as a numeric SocketAddr.
        if let Ok(addr) = seed.parse::<SocketAddr>() {
            debug!(seed = %seed, addr = %addr, "parsed seed address (numeric)");
            if seen.insert(addr) {
                addresses.push(addr);
            }
            continue;
        }

        // Slow path: DNS resolution.
        match tokio::net::lookup_host(seed.as_str()).await {
            Ok(resolved) => {
                let mut count = 0;
                for addr in resolved {
                    if seen.insert(addr) {
                        addresses.push(addr);
                        count += 1;
                    }
                }
                if count > 0 {
                    info!(
                        seed = %seed,
                        resolved_count = count,
                        "DNS-resolved seed address",
                    );
                } else {
                    warn!(
                        seed = %seed,
                        "DNS resolution returned no addresses",
                    );
                }
            }
            Err(e) => {
                warn!(
                    seed = %seed,
                    error = %e,
                    "DNS resolution failed for seed, skipping",
                );
            }
        }
    }

    if addresses.is_empty() && !seeds.is_empty() {
        warn!(
            seed_count = seeds.len(),
            "none of the configured seed addresses could be resolved",
        );
    }

    addresses
}

/// Backward-compatible synchronous seed address parser.
///
/// Delegates to the synchronous parser which only handles numeric IP:port
/// strings. DNS hostnames are silently skipped. Prefer
/// [`resolve_seed_addresses`] in async contexts.
pub fn parse_seed_addresses(seeds: &[String]) -> Vec<SocketAddr> {
    parse_seed_addresses_sync(seeds)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------------
    // parse_seed_addresses
    // ------------------------------------------------------------------------

    #[test]
    fn test_parse_valid_ipv4() {
        let seeds = vec![
            "192.168.1.10:4200".to_string(),
            "10.0.0.1:8080".to_string(),
        ];
        let addrs = parse_seed_addresses(&seeds);
        assert_eq!(addrs.len(), 2);
        assert_eq!(
            addrs[0],
            "192.168.1.10:4200".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(addrs[1], "10.0.0.1:8080".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_parse_valid_ipv6() {
        let seeds = vec!["[::1]:4200".to_string()];
        let addrs = parse_seed_addresses(&seeds);
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0], "[::1]:4200".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_parse_mixed_valid_and_invalid() {
        let seeds = vec![
            "192.168.1.10:4200".to_string(),
            "not-an-address".to_string(),
            "[::1]:4200".to_string(),
            "hostname:1234".to_string(),
        ];
        let addrs = parse_seed_addresses(&seeds);
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn test_parse_all_invalid() {
        let seeds = vec![
            "not-valid".to_string(),
            "also-not-valid".to_string(),
        ];
        let addrs = parse_seed_addresses(&seeds);
        assert!(addrs.is_empty());
    }

    #[test]
    fn test_parse_empty_input() {
        let seeds: Vec<String> = Vec::new();
        let addrs = parse_seed_addresses(&seeds);
        assert!(addrs.is_empty());
    }

    #[test]
    fn test_parse_preserves_order() {
        let seeds = vec![
            "10.0.0.3:3000".to_string(),
            "10.0.0.1:1000".to_string(),
            "10.0.0.2:2000".to_string(),
        ];
        let addrs = parse_seed_addresses(&seeds);
        assert_eq!(addrs.len(), 3);
        assert_eq!(addrs[0], "10.0.0.3:3000".parse::<SocketAddr>().unwrap());
        assert_eq!(addrs[1], "10.0.0.1:1000".parse::<SocketAddr>().unwrap());
        assert_eq!(addrs[2], "10.0.0.2:2000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn test_parse_missing_port() {
        let seeds = vec!["192.168.1.10".to_string()];
        let addrs = parse_seed_addresses(&seeds);
        assert!(addrs.is_empty());
    }

    // ------------------------------------------------------------------------
    // BootstrapResult
    // ------------------------------------------------------------------------

    #[test]
    fn test_bootstrap_result_debug() {
        let result = BootstrapResult {
            peers_discovered: 5,
            bootstrap_server: "192.168.1.1:4200".parse().unwrap(),
            attempts: 2,
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("peers_discovered"));
        assert!(debug_str.contains("5"));
        assert!(debug_str.contains("192.168.1.1:4200"));
        assert!(debug_str.contains("attempts"));
    }

    // ------------------------------------------------------------------------
    // resolve_seed_addresses (async DNS)
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_resolve_numeric_ip_passthrough() {
        let seeds = vec![
            "192.168.1.10:4200".to_string(),
            "10.0.0.1:8080".to_string(),
        ];
        let addrs = resolve_seed_addresses(&seeds).await;
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0], "192.168.1.10:4200".parse::<SocketAddr>().unwrap());
        assert_eq!(addrs[1], "10.0.0.1:8080".parse::<SocketAddr>().unwrap());
    }

    #[tokio::test]
    async fn test_resolve_localhost() {
        let seeds = vec!["localhost:4200".to_string()];
        let addrs = resolve_seed_addresses(&seeds).await;
        // localhost should resolve to at least one address (127.0.0.1 or [::1])
        assert!(!addrs.is_empty(), "localhost should resolve to at least one address");
        for addr in &addrs {
            assert_eq!(addr.port(), 4200);
        }
    }

    #[tokio::test]
    async fn test_resolve_mixed_ips_and_hostnames() {
        let seeds = vec![
            "192.168.1.10:4200".to_string(),
            "localhost:4200".to_string(),
        ];
        let addrs = resolve_seed_addresses(&seeds).await;
        // At least 2: the numeric IP + at least one from localhost
        assert!(addrs.len() >= 2, "should have at least 2 addresses, got {}", addrs.len());
        assert!(addrs.contains(&"192.168.1.10:4200".parse::<SocketAddr>().unwrap()));
    }

    #[tokio::test]
    async fn test_resolve_dns_failure_gracefully_skipped() {
        let seeds = vec![
            "this-hostname-does-not-exist-xyz.invalid:4200".to_string(),
            "192.168.1.1:4200".to_string(),
        ];
        let addrs = resolve_seed_addresses(&seeds).await;
        // The invalid hostname should be skipped; the numeric IP should remain
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0], "192.168.1.1:4200".parse::<SocketAddr>().unwrap());
    }

    #[tokio::test]
    async fn test_resolve_deduplicates() {
        let seeds = vec![
            "192.168.1.10:4200".to_string(),
            "192.168.1.10:4200".to_string(),
        ];
        let addrs = resolve_seed_addresses(&seeds).await;
        assert_eq!(addrs.len(), 1);
    }

    #[tokio::test]
    async fn test_resolve_empty_input() {
        let seeds: Vec<String> = Vec::new();
        let addrs = resolve_seed_addresses(&seeds).await;
        assert!(addrs.is_empty());
    }

    // ------------------------------------------------------------------------
    // DnsCache
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn test_dns_cache_hit() {
        let cache = DnsCache::new(Duration::from_secs(60));

        // First call: cache miss, populates cache.
        let addrs1 = cache.resolve_with_cache("localhost:4200").await.unwrap();
        assert!(!addrs1.is_empty());
        assert_eq!(cache.len(), 1);

        // Second call: cache hit, returns the same addresses.
        let addrs2 = cache.resolve_with_cache("localhost:4200").await.unwrap();
        assert_eq!(addrs1, addrs2);
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn test_dns_cache_miss_then_hit() {
        let cache = DnsCache::new(Duration::from_secs(60));
        assert!(cache.is_empty());

        let addrs = cache.resolve_with_cache("localhost:4200").await.unwrap();
        assert!(!addrs.is_empty());
        assert!(!cache.is_empty());
    }

    #[tokio::test]
    async fn test_dns_cache_expiry() {
        // Use a very short TTL so the entry expires immediately.
        let cache = DnsCache::new(Duration::from_millis(1));

        let addrs1 = cache.resolve_with_cache("localhost:4200").await.unwrap();
        assert!(!addrs1.is_empty());

        // Wait for the TTL to expire.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // The cache entry is stale; it should re-resolve (but still return results).
        let addrs2 = cache.resolve_with_cache("localhost:4200").await.unwrap();
        assert!(!addrs2.is_empty());
    }

    #[tokio::test]
    async fn test_dns_cache_clear() {
        let cache = DnsCache::new(Duration::from_secs(60));
        let _ = cache.resolve_with_cache("localhost:4200").await.unwrap();
        assert_eq!(cache.len(), 1);
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_dns_cache_default_ttl() {
        let cache = DnsCache::with_default_ttl();
        assert_eq!(cache.ttl, DNS_CACHE_TTL);
    }
}
