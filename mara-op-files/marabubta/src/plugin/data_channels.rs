// Marabunta - Licensed under the MIT License.
//! Data channel store for plugin ↔ dashboard communication.
//!
//! Plugins push named data channels (e.g. `fin.portfolio`, `fin.greeks`)
//! via the wire protocol.  The [`DataChannelStore`] holds the latest value
//! for each channel and provides a [`tokio::sync::broadcast`] for
//! incremental updates (consumed by the WebSocket dashboard feed).
//!
//! Query forwarding: REST/WS clients can send a query to a channel and
//! receive a response from the owning plugin via oneshot-based correlation.


use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot};
use tracing::warn;

// ============================================================================
// Types
// ============================================================================

/// A single channel entry stored in the [`DataChannelStore`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelEntry {
    /// Channel name (e.g. `"fin.portfolio"`).
    pub channel: String,
    /// Plugin ID that owns this channel.
    pub plugin_id: String,
    /// Traits declared by the plugin (e.g. `["CubeFaceProvider"]`).
    pub plugin_traits: Vec<String>,
    /// Latest payload pushed by the plugin.
    pub payload: serde_json::Value,
    /// When the channel was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Incremental update broadcast to subscribers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelUpdate {
    /// Channel name.
    pub channel: String,
    /// Plugin ID that pushed the update.
    pub plugin_id: String,
    /// Updated payload.
    pub payload: serde_json::Value,
    /// Timestamp of the update.
    pub updated_at: DateTime<Utc>,
}

/// A pending query waiting for a plugin response.
struct PendingQuery {
    tx: oneshot::Sender<serde_json::Value>,
}

// ============================================================================
// DataChannelStore
// ============================================================================

/// Thread-safe store for plugin data channels.
///
/// - `push()` upserts a channel entry and broadcasts to all subscribers.
/// - `get()` returns the latest snapshot for a single channel.
/// - `list()` returns all channel entries.
/// - `list_by_trait()` filters channels by plugin trait.
/// - `subscribe()` returns a broadcast receiver for incremental updates.
/// - `submit_query()` / `deliver_query_response()` implement request/response
///   correlation for channel queries.
pub struct DataChannelStore {
    channels: DashMap<String, ChannelEntry>,
    broadcast_tx: broadcast::Sender<ChannelUpdate>,
    pending_queries: DashMap<String, PendingQuery>,
    /// Map from plugin_id → set of traits, updated on push.
    plugin_traits: DashMap<String, Vec<String>>,
}

impl DataChannelStore {
    /// Create a new data channel store.
    pub fn new() -> Self {
        let (broadcast_tx, _) = broadcast::channel(256);
        Self {
            channels: DashMap::new(),
            broadcast_tx,
            pending_queries: DashMap::new(),
            plugin_traits: DashMap::new(),
        }
    }

    /// Push (upsert) a channel entry and broadcast the update.
    pub fn push(
        &self,
        channel: String,
        plugin_id: String,
        plugin_traits: Vec<String>,
        payload: serde_json::Value,
    ) {
        let now = Utc::now();

        let entry = ChannelEntry {
            channel: channel.clone(),
            plugin_id: plugin_id.clone(),
            plugin_traits: plugin_traits.clone(),
            payload: payload.clone(),
            updated_at: now,
        };

        self.channels.insert(channel.clone(), entry);
        self.plugin_traits
            .insert(plugin_id.clone(), plugin_traits);

        let update = ChannelUpdate {
            channel,
            plugin_id,
            payload,
            updated_at: now,
        };

        // Broadcast — ignoring errors (no active receivers is fine).
        let _ = self.broadcast_tx.send(update);
    }

    /// Get the latest entry for a channel.
    pub fn get(&self, channel: &str) -> Option<ChannelEntry> {
        self.channels.get(channel).map(|r| r.value().clone())
    }

    /// List all channel entries.
    pub fn list(&self) -> Vec<ChannelEntry> {
        self.channels.iter().map(|r| r.value().clone()).collect()
    }

    /// List channels whose owning plugin declares the given trait.
    pub fn list_by_trait(&self, trait_name: &str) -> Vec<ChannelEntry> {
        // Collect plugin_ids that have the trait.
        let matching_plugins: Vec<String> = self
            .plugin_traits
            .iter()
            .filter(|entry| entry.value().iter().any(|t| t == trait_name))
            .map(|entry| entry.key().clone())
            .collect();

        self.channels
            .iter()
            .filter(|entry| matching_plugins.contains(&entry.value().plugin_id))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Subscribe to incremental channel updates.
    pub fn subscribe(&self) -> broadcast::Receiver<ChannelUpdate> {
        self.broadcast_tx.subscribe()
    }

    /// Submit a query targeted at a specific channel.
    ///
    /// Returns a request_id and a oneshot receiver that will eventually
    /// contain the plugin's response.
    pub fn submit_query(&self) -> (String, oneshot::Receiver<serde_json::Value>) {
        let request_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_queries
            .insert(request_id.clone(), PendingQuery { tx });
        (request_id, rx)
    }

    /// Deliver a query response from a plugin, resolving the pending oneshot.
    ///
    /// Returns `true` if a matching pending query was found and resolved.
    pub fn deliver_query_response(
        &self,
        request_id: &str,
        result: serde_json::Value,
    ) -> bool {
        if let Some((_, pending)) = self.pending_queries.remove(request_id) {
            let _ = pending.tx.send(result);
            true
        } else {
            warn!(request_id = %request_id, "no pending query found for response");
            false
        }
    }

    /// Number of channels currently stored.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Number of pending queries.
    pub fn pending_query_count(&self) -> usize {
        self.pending_queries.len()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn push_and_get() {
        let store = DataChannelStore::new();
        store.push(
            "fin.portfolio".into(),
            "plugin-fin-abc123".into(),
            vec!["CubeFaceProvider".into()],
            json!({"positions": 42}),
        );

        let entry = store.get("fin.portfolio").unwrap();
        assert_eq!(entry.channel, "fin.portfolio");
        assert_eq!(entry.plugin_id, "plugin-fin-abc123");
        assert_eq!(entry.payload["positions"], 42);
    }

    #[test]
    fn push_overwrites_existing() {
        let store = DataChannelStore::new();
        store.push("ch1".into(), "p1".into(), vec![], json!({"v": 1}));
        store.push("ch1".into(), "p1".into(), vec![], json!({"v": 2}));

        let entry = store.get("ch1").unwrap();
        assert_eq!(entry.payload["v"], 2);
    }

    #[test]
    fn get_missing_returns_none() {
        let store = DataChannelStore::new();
        assert!(store.get("nonexistent").is_none());
    }

    #[test]
    fn list_returns_all() {
        let store = DataChannelStore::new();
        store.push("a".into(), "p1".into(), vec![], json!(1));
        store.push("b".into(), "p2".into(), vec![], json!(2));
        store.push("c".into(), "p3".into(), vec![], json!(3));

        let all = store.list();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn list_by_trait_filters_correctly() {
        let store = DataChannelStore::new();
        store.push(
            "fin.portfolio".into(),
            "p1".into(),
            vec!["CubeFaceProvider".into()],
            json!(1),
        );
        store.push(
            "sys.metrics".into(),
            "p2".into(),
            vec!["MetricsProvider".into()],
            json!(2),
        );
        store.push(
            "fin.greeks".into(),
            "p1".into(),
            vec!["CubeFaceProvider".into()],
            json!(3),
        );

        let cube_channels = store.list_by_trait("CubeFaceProvider");
        assert_eq!(cube_channels.len(), 2);

        let metrics_channels = store.list_by_trait("MetricsProvider");
        assert_eq!(metrics_channels.len(), 1);

        let empty = store.list_by_trait("Nonexistent");
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn subscribe_receives_updates() {
        let store = DataChannelStore::new();
        let mut rx = store.subscribe();

        store.push("ch1".into(), "p1".into(), vec![], json!({"val": 42}));

        let update = rx.recv().await.unwrap();
        assert_eq!(update.channel, "ch1");
        assert_eq!(update.payload["val"], 42);
    }

    #[tokio::test]
    async fn query_roundtrip() {
        let store = DataChannelStore::new();
        let (request_id, rx) = store.submit_query();

        assert_eq!(store.pending_query_count(), 1);

        // Simulate plugin response.
        let delivered = store.deliver_query_response(
            &request_id,
            json!({"decomposition": "result"}),
        );
        assert!(delivered);
        assert_eq!(store.pending_query_count(), 0);

        let result = rx.await.unwrap();
        assert_eq!(result["decomposition"], "result");
    }

    #[test]
    fn deliver_response_for_unknown_request_returns_false() {
        let store = DataChannelStore::new();
        let delivered = store.deliver_query_response("unknown-id", json!(null));
        assert!(!delivered);
    }

    #[test]
    fn channel_count() {
        let store = DataChannelStore::new();
        assert_eq!(store.channel_count(), 0);
        store.push("a".into(), "p1".into(), vec![], json!(1));
        store.push("b".into(), "p2".into(), vec![], json!(2));
        assert_eq!(store.channel_count(), 2);
    }
}
