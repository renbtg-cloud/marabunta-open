// Marabunta - Licensed under the MIT License.
//! Topic-based pub/sub broker for the plugin system.
//!
//! Provides a publish/subscribe messaging layer that lets plugins communicate
//! by publishing messages to named topics and subscribing to receive events.
//! Topics are created lazily on first publish or subscribe. Dead subscribers
//! (whose receivers have been dropped) are pruned automatically during publish
//! and can also be cleaned up explicitly via [`PubSubBroker::prune_dead_subscribers`].

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::plugin::config::{
    PUBSUB_MAX_PENDING_EVENTS, PUBSUB_MAX_SUBSCRIBERS_PER_TOPIC, PUBSUB_MAX_TOPICS,
};
use crate::plugin::types::{
    PluginError, PluginResult, PublishRequest, PublishResponse, SubscribeEvent, SubscribeRequest,
};

// ============================================================================
// Constants
// ============================================================================

/// Maximum length of a topic name in bytes.
const MAX_TOPIC_NAME_LENGTH: usize = 256;

// ============================================================================
// Internal types
// ============================================================================

/// Per-topic state including subscriber list and stats.
struct TopicState {
    /// Active subscribers for this topic.
    subscribers: Vec<SubscriberEntry>,
    /// Total number of messages published to this topic.
    message_count: u64,
    /// When this topic was first created.
    created_at: Instant,
}

impl TopicState {
    fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            message_count: 0,
            created_at: Instant::now(),
        }
    }
}

/// A single subscriber attached to a topic.
struct SubscriberEntry {
    /// Unique subscriber identifier (UUID).
    id: String,
    /// Channel sender for delivering events to the subscriber.
    sender: mpsc::UnboundedSender<SubscribeEvent>,
    /// When this subscriber was added.
    subscribed_at: Instant,
}

// ============================================================================
// PubSubBroker
// ============================================================================

/// Topic-based pub/sub broker that supports publish, subscribe, and
/// automatic cleanup of dead subscribers.
///
/// Thread-safe: uses `DashMap` for concurrent topic access and `AtomicUsize`
/// for subscriber counting. All methods take `&self`.
pub struct PubSubBroker {
    /// Identity of the node running this broker.
    node_id: String,
    /// Topic name -> topic state. Concurrent hash map for lock-free reads.
    topics: DashMap<String, TopicState>,
    /// Global subscriber count across all topics.
    subscriber_count: AtomicUsize,
    /// Maximum number of topics allowed.
    max_topics: usize,
    /// Maximum subscribers per topic.
    max_subscribers_per_topic: usize,
    /// Maximum pending events per subscriber (used for capacity checks).
    max_pending_events: usize,
}

impl PubSubBroker {
    /// Create a new broker with default limits from `crate::plugin::config`.
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            topics: DashMap::new(),
            subscriber_count: AtomicUsize::new(0),
            max_topics: PUBSUB_MAX_TOPICS,
            max_subscribers_per_topic: PUBSUB_MAX_SUBSCRIBERS_PER_TOPIC,
            max_pending_events: PUBSUB_MAX_PENDING_EVENTS,
        }
    }

    /// Builder method to override the default capacity limits.
    pub fn with_limits(
        mut self,
        max_topics: usize,
        max_subs_per_topic: usize,
        max_pending: usize,
    ) -> Self {
        self.max_topics = max_topics;
        self.max_subscribers_per_topic = max_subs_per_topic;
        self.max_pending_events = max_pending;
        self
    }

    /// Publish a message to a topic.
    ///
    /// If the topic does not exist it is created lazily. The event is fanned
    /// out to every live subscriber. Dead subscribers (whose receivers have
    /// been dropped) are pruned as a side-effect.
    ///
    /// Returns [`PublishResponse`] with `success = true` and the number of
    /// recipients that actually received the event.
    pub fn publish(&self, request: PublishRequest) -> PluginResult<PublishResponse> {
        Self::validate_topic_name(&request.topic)?;

        let timestamp = Utc::now().timestamp_millis() as u64;

        let event = SubscribeEvent {
            topic: request.topic.clone(),
            payload: request.payload,
            from_node: self.node_id.clone(),
            timestamp,
        };

        // Get or create the topic.
        let mut topic = self.topics.entry(request.topic.clone()).or_insert_with(|| {
            debug!(topic = %request.topic, "lazily creating topic on publish");
            TopicState::new()
        });

        topic.message_count += 1;

        // Fan out the event to all subscribers, tracking which are dead.
        let mut recipients: u32 = 0;
        let mut dead_indices: Vec<usize> = Vec::new();

        for (idx, subscriber) in topic.subscribers.iter().enumerate() {
            match subscriber.sender.send(event.clone()) {
                Ok(()) => {
                    recipients += 1;
                    trace!(
                        topic = %event.topic,
                        subscriber_id = %subscriber.id,
                        "delivered event to subscriber"
                    );
                }
                Err(_) => {
                    // Receiver dropped — mark for removal.
                    dead_indices.push(idx);
                }
            }
        }

        // Remove dead subscribers in reverse order to preserve indices.
        if !dead_indices.is_empty() {
            debug!(
                topic = %event.topic,
                dead_count = dead_indices.len(),
                "pruning dead subscribers during publish"
            );
            for &idx in dead_indices.iter().rev() {
                topic.subscribers.swap_remove(idx);
                self.subscriber_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        Ok(PublishResponse {
            success: true,
            recipients,
        })
    }

    /// Subscribe to a topic.
    ///
    /// If the topic does not exist it is created lazily. Returns an unbounded
    /// receiver that will deliver [`SubscribeEvent`] messages whenever another
    /// caller publishes to this topic.
    ///
    /// The caller is responsible for reading from the receiver. If the
    /// receiver is dropped, the broker will automatically detect and prune
    /// the dead subscriber on the next publish or explicit prune call.
    pub fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> PluginResult<mpsc::UnboundedReceiver<SubscribeEvent>> {
        Self::validate_topic_name(&request.topic)?;

        // Check global subscriber limit.
        let current = self.subscriber_count.load(Ordering::Relaxed);
        // Use a generous global limit derived from max_topics * max_subs_per_topic,
        // but cap it to avoid absurd numbers.
        let global_limit = self.max_topics.saturating_mul(self.max_subscribers_per_topic);
        if current >= global_limit {
            return Err(PluginError::CapacityExceeded(format!(
                "global subscriber limit reached ({current})"
            )));
        }

        // Check topic-level subscriber limit.
        // We must also check whether adding this topic would exceed the topic limit.
        let topic_exists = self.topics.contains_key(&request.topic);
        if !topic_exists && self.topics.len() >= self.max_topics {
            return Err(PluginError::CapacityExceeded(format!(
                "maximum number of topics reached ({})",
                self.max_topics
            )));
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let subscriber_id = Uuid::new_v4().to_string();

        let entry = SubscriberEntry {
            id: subscriber_id.clone(),
            sender: tx,
            subscribed_at: Instant::now(),
        };

        let mut topic = self.topics.entry(request.topic.clone()).or_insert_with(|| {
            debug!(topic = %request.topic, "lazily creating topic on subscribe");
            TopicState::new()
        });

        if topic.subscribers.len() >= self.max_subscribers_per_topic {
            return Err(PluginError::CapacityExceeded(format!(
                "topic '{}' has reached the subscriber limit ({})",
                request.topic, self.max_subscribers_per_topic
            )));
        }

        topic.subscribers.push(entry);
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);

        debug!(
            topic = %request.topic,
            subscriber_id = %subscriber_id,
            "new subscriber added"
        );

        Ok(rx)
    }

    /// Remove a specific subscriber from a topic by subscriber ID.
    ///
    /// Returns `true` if the subscriber was found and removed, `false` otherwise.
    pub fn unsubscribe(&self, topic: &str, subscriber_id: &str) -> bool {
        if let Some(mut topic_state) = self.topics.get_mut(topic) {
            let initial_len = topic_state.subscribers.len();
            topic_state
                .subscribers
                .retain(|s| s.id != subscriber_id);
            let removed = initial_len - topic_state.subscribers.len();
            if removed > 0 {
                self.subscriber_count.fetch_sub(removed, Ordering::Relaxed);
                debug!(
                    topic = %topic,
                    subscriber_id = %subscriber_id,
                    "subscriber removed"
                );
                return true;
            }
        }
        false
    }

    /// Return the number of topics currently tracked by the broker.
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    /// Return the total number of active subscribers across all topics.
    pub fn total_subscribers(&self) -> usize {
        self.subscriber_count.load(Ordering::Relaxed)
    }

    /// Return the number of subscribers for a given topic, or 0 if the topic
    /// does not exist.
    pub fn topic_subscriber_count(&self, topic: &str) -> usize {
        self.topics
            .get(topic)
            .map(|t| t.subscribers.len())
            .unwrap_or(0)
    }

    /// Walk all topics and remove subscribers whose channels are closed
    /// (i.e., the receiver has been dropped).
    ///
    /// Returns the total number of dead subscribers that were pruned.
    pub fn prune_dead_subscribers(&self) -> usize {
        let mut total_pruned: usize = 0;

        // Collect topic keys first to avoid holding DashMap iterators across
        // mutable access.
        let keys: Vec<String> = self.topics.iter().map(|r| r.key().clone()).collect();

        for key in keys {
            if let Some(mut topic) = self.topics.get_mut(&key) {
                let before = topic.subscribers.len();
                topic.subscribers.retain(|s| !s.sender.is_closed());
                let removed = before - topic.subscribers.len();
                if removed > 0 {
                    total_pruned += removed;
                    self.subscriber_count.fetch_sub(removed, Ordering::Relaxed);
                    debug!(
                        topic = %key,
                        removed = removed,
                        "pruned dead subscribers"
                    );
                }
            }
        }

        if total_pruned > 0 {
            warn!(total_pruned = total_pruned, "dead subscriber cleanup complete");
        }

        total_pruned
    }

    /// Remove topics that have zero subscribers.
    ///
    /// Returns the number of topics removed.
    pub fn remove_empty_topics(&self) -> usize {
        let keys: Vec<String> = self.topics.iter().map(|r| r.key().clone()).collect();

        let mut removed: usize = 0;
        for key in keys {
            // Use `remove_if` to atomically check and remove.
            let was_removed = self
                .topics
                .remove_if(&key, |_, topic| topic.subscribers.is_empty());
            if was_removed.is_some() {
                removed += 1;
                debug!(topic = %key, "removed empty topic");
            }
        }

        if removed > 0 {
            debug!(removed = removed, "empty topic cleanup complete");
        }

        removed
    }

    // ========================================================================
    // Validation helpers
    // ========================================================================

    /// Validate a topic name.
    ///
    /// Rules:
    /// - Non-empty.
    /// - At most 256 bytes.
    /// - Only printable ASCII excluding control characters and whitespace
    ///   other than the topic separator characters: alphanumeric, `-`, `_`,
    ///   `.`, `:`, `/`.
    fn validate_topic_name(topic: &str) -> PluginResult<()> {
        if topic.is_empty() {
            return Err(PluginError::PubSub("topic name cannot be empty".into()));
        }
        if topic.len() > MAX_TOPIC_NAME_LENGTH {
            return Err(PluginError::PubSub(format!(
                "topic name too long ({} bytes, max {MAX_TOPIC_NAME_LENGTH})",
                topic.len()
            )));
        }
        if !topic
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '/'))
        {
            return Err(PluginError::PubSub(format!(
                "topic name '{}' contains invalid characters; \
                 allowed: alphanumeric, '-', '_', '.', ':', '/'",
                topic
            )));
        }
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> PubSubBroker {
        PubSubBroker::new("test-node".into())
    }

    // ------------------------------------------------------------------
    // Topic name validation
    // ------------------------------------------------------------------

    #[test]
    fn validate_rejects_empty_topic() {
        let b = broker();
        let res = b.publish(PublishRequest {
            topic: String::new(),
            payload: vec![],
        });
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(err.contains("empty"), "unexpected error: {err}");
    }

    #[test]
    fn validate_rejects_topic_too_long() {
        let b = broker();
        let long_name = "a".repeat(MAX_TOPIC_NAME_LENGTH + 1);
        let res = b.publish(PublishRequest {
            topic: long_name,
            payload: vec![],
        });
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(err.contains("too long"), "unexpected error: {err}");
    }

    #[test]
    fn validate_rejects_invalid_chars() {
        let b = broker();
        let res = b.publish(PublishRequest {
            topic: "topic with spaces".into(),
            payload: vec![],
        });
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(err.contains("invalid characters"), "unexpected error: {err}");
    }

    #[test]
    fn validate_accepts_valid_topic_names() {
        let b = broker();
        let valid_topics = [
            "simple",
            "with-dashes",
            "with_underscores",
            "with.dots",
            "with:colons",
            "with/slashes",
            "pg:catalog:sales.orders",
            "events/node-123/heartbeat",
            "a",
        ];
        for topic in valid_topics {
            let res = b.publish(PublishRequest {
                topic: topic.into(),
                payload: vec![1, 2, 3],
            });
            assert!(res.is_ok(), "topic '{topic}' should be valid but got: {res:?}");
        }
    }

    // ------------------------------------------------------------------
    // Publish to empty topic
    // ------------------------------------------------------------------

    #[test]
    fn publish_to_empty_topic_succeeds_with_zero_recipients() {
        let b = broker();
        let resp = b
            .publish(PublishRequest {
                topic: "empty-topic".into(),
                payload: vec![1, 2, 3],
            })
            .unwrap();
        assert!(resp.success);
        assert_eq!(resp.recipients, 0);
        // Topic should have been lazily created.
        assert_eq!(b.topic_count(), 1);
    }

    // ------------------------------------------------------------------
    // Publish with subscribers
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn publish_delivers_to_subscriber() {
        let b = broker();
        let mut rx = b
            .subscribe(SubscribeRequest {
                topic: "events".into(),
            })
            .unwrap();

        let resp = b
            .publish(PublishRequest {
                topic: "events".into(),
                payload: b"hello".to_vec(),
            })
            .unwrap();

        assert!(resp.success);
        assert_eq!(resp.recipients, 1);

        let event = rx.recv().await.unwrap();
        assert_eq!(event.topic, "events");
        assert_eq!(event.payload, b"hello");
        assert_eq!(event.from_node, "test-node");
        assert!(event.timestamp > 0);
    }

    // ------------------------------------------------------------------
    // Subscribe and receive events
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn subscribe_receives_multiple_events() {
        let b = broker();
        let mut rx = b
            .subscribe(SubscribeRequest {
                topic: "stream".into(),
            })
            .unwrap();

        for i in 0u8..5 {
            b.publish(PublishRequest {
                topic: "stream".into(),
                payload: vec![i],
            })
            .unwrap();
        }

        for i in 0u8..5 {
            let event = rx.recv().await.unwrap();
            assert_eq!(event.payload, vec![i]);
        }
    }

    // ------------------------------------------------------------------
    // Multiple subscribers on same topic
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let b = broker();

        let mut rx1 = b
            .subscribe(SubscribeRequest {
                topic: "broadcast".into(),
            })
            .unwrap();
        let mut rx2 = b
            .subscribe(SubscribeRequest {
                topic: "broadcast".into(),
            })
            .unwrap();
        let mut rx3 = b
            .subscribe(SubscribeRequest {
                topic: "broadcast".into(),
            })
            .unwrap();

        let resp = b
            .publish(PublishRequest {
                topic: "broadcast".into(),
                payload: b"multi".to_vec(),
            })
            .unwrap();

        assert_eq!(resp.recipients, 3);
        assert_eq!(b.total_subscribers(), 3);
        assert_eq!(b.topic_subscriber_count("broadcast"), 3);

        for rx in [&mut rx1, &mut rx2, &mut rx3] {
            let event = rx.recv().await.unwrap();
            assert_eq!(event.payload, b"multi");
        }
    }

    // ------------------------------------------------------------------
    // Dead subscriber cleanup
    // ------------------------------------------------------------------

    #[test]
    fn dead_subscriber_pruned_on_publish() {
        let b = broker();

        let rx = b
            .subscribe(SubscribeRequest {
                topic: "cleanup".into(),
            })
            .unwrap();

        assert_eq!(b.total_subscribers(), 1);

        // Drop the receiver, making the subscriber dead.
        drop(rx);

        let resp = b
            .publish(PublishRequest {
                topic: "cleanup".into(),
                payload: b"ping".to_vec(),
            })
            .unwrap();

        // The dead subscriber should have been pruned, so 0 recipients.
        assert_eq!(resp.recipients, 0);
        assert_eq!(b.total_subscribers(), 0);
        assert_eq!(b.topic_subscriber_count("cleanup"), 0);
    }

    #[test]
    fn prune_dead_subscribers_explicit() {
        let b = broker();

        let rx1 = b
            .subscribe(SubscribeRequest {
                topic: "prune-test".into(),
            })
            .unwrap();
        let _rx2 = b
            .subscribe(SubscribeRequest {
                topic: "prune-test".into(),
            })
            .unwrap();

        assert_eq!(b.total_subscribers(), 2);

        // Drop one receiver.
        drop(rx1);

        let pruned = b.prune_dead_subscribers();
        assert_eq!(pruned, 1);
        assert_eq!(b.total_subscribers(), 1);
        assert_eq!(b.topic_subscriber_count("prune-test"), 1);
    }

    // ------------------------------------------------------------------
    // Topic creation on subscribe
    // ------------------------------------------------------------------

    #[test]
    fn subscribe_creates_topic_lazily() {
        let b = broker();
        assert_eq!(b.topic_count(), 0);

        let _rx = b
            .subscribe(SubscribeRequest {
                topic: "lazy-topic".into(),
            })
            .unwrap();

        assert_eq!(b.topic_count(), 1);
        assert_eq!(b.topic_subscriber_count("lazy-topic"), 1);
    }

    // ------------------------------------------------------------------
    // Topic limit enforcement
    // ------------------------------------------------------------------

    #[test]
    fn topic_limit_enforced_on_subscribe() {
        let b = PubSubBroker::new("test-node".into()).with_limits(2, 256, 10_000);

        let _rx1 = b
            .subscribe(SubscribeRequest {
                topic: "topic-1".into(),
            })
            .unwrap();
        let _rx2 = b
            .subscribe(SubscribeRequest {
                topic: "topic-2".into(),
            })
            .unwrap();

        // Third topic should be rejected.
        let res = b.subscribe(SubscribeRequest {
            topic: "topic-3".into(),
        });
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("maximum number of topics"),
            "unexpected error: {err}"
        );
    }

    // ------------------------------------------------------------------
    // Subscriber limit enforcement
    // ------------------------------------------------------------------

    #[test]
    fn subscriber_limit_per_topic_enforced() {
        let b = PubSubBroker::new("test-node".into()).with_limits(4096, 2, 10_000);

        let _rx1 = b
            .subscribe(SubscribeRequest {
                topic: "limited".into(),
            })
            .unwrap();
        let _rx2 = b
            .subscribe(SubscribeRequest {
                topic: "limited".into(),
            })
            .unwrap();

        // Third subscriber on same topic should be rejected.
        let res = b.subscribe(SubscribeRequest {
            topic: "limited".into(),
        });
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("subscriber limit"),
            "unexpected error: {err}"
        );
    }

    // ------------------------------------------------------------------
    // Unsubscribe
    // ------------------------------------------------------------------

    #[test]
    fn unsubscribe_removes_subscriber() {
        let b = broker();

        // We need to extract the subscriber ID. Since subscribe() returns only
        // the receiver, we rely on the fact that there's exactly one subscriber
        // and test unsubscribe by inspecting counts.
        let _rx = b
            .subscribe(SubscribeRequest {
                topic: "unsub-topic".into(),
            })
            .unwrap();

        assert_eq!(b.total_subscribers(), 1);

        // We don't have the subscriber ID directly, so peek into the topic state.
        let sub_id = {
            let topic = b.topics.get("unsub-topic").unwrap();
            topic.subscribers[0].id.clone()
        };

        let removed = b.unsubscribe("unsub-topic", &sub_id);
        assert!(removed);
        assert_eq!(b.total_subscribers(), 0);
        assert_eq!(b.topic_subscriber_count("unsub-topic"), 0);
    }

    #[test]
    fn unsubscribe_nonexistent_returns_false() {
        let b = broker();
        assert!(!b.unsubscribe("no-such-topic", "no-such-subscriber"));
    }

    #[test]
    fn unsubscribe_wrong_id_returns_false() {
        let b = broker();
        let _rx = b
            .subscribe(SubscribeRequest {
                topic: "unsub-topic".into(),
            })
            .unwrap();
        assert!(!b.unsubscribe("unsub-topic", "wrong-id"));
        assert_eq!(b.total_subscribers(), 1);
    }

    // ------------------------------------------------------------------
    // Remove empty topics
    // ------------------------------------------------------------------

    #[test]
    fn remove_empty_topics_cleans_up() {
        let b = broker();

        // Create a topic by publishing (no subscribers).
        b.publish(PublishRequest {
            topic: "empty-1".into(),
            payload: vec![],
        })
        .unwrap();
        b.publish(PublishRequest {
            topic: "empty-2".into(),
            payload: vec![],
        })
        .unwrap();

        // Create a topic that has a subscriber.
        let _rx = b
            .subscribe(SubscribeRequest {
                topic: "has-sub".into(),
            })
            .unwrap();

        assert_eq!(b.topic_count(), 3);

        let removed = b.remove_empty_topics();
        assert_eq!(removed, 2);
        assert_eq!(b.topic_count(), 1);
        assert!(b.topics.contains_key("has-sub"));
    }

    // ------------------------------------------------------------------
    // Stats helpers
    // ------------------------------------------------------------------

    #[test]
    fn topic_subscriber_count_returns_zero_for_missing_topic() {
        let b = broker();
        assert_eq!(b.topic_subscriber_count("nonexistent"), 0);
    }

    #[test]
    fn total_subscribers_tracks_across_topics() {
        let b = broker();

        let _rx1 = b
            .subscribe(SubscribeRequest {
                topic: "topic-a".into(),
            })
            .unwrap();
        let _rx2 = b
            .subscribe(SubscribeRequest {
                topic: "topic-b".into(),
            })
            .unwrap();
        let _rx3 = b
            .subscribe(SubscribeRequest {
                topic: "topic-a".into(),
            })
            .unwrap();

        assert_eq!(b.total_subscribers(), 3);
        assert_eq!(b.topic_subscriber_count("topic-a"), 2);
        assert_eq!(b.topic_subscriber_count("topic-b"), 1);
    }

    // ------------------------------------------------------------------
    // With limits builder
    // ------------------------------------------------------------------

    #[test]
    fn with_limits_overrides_defaults() {
        let b = PubSubBroker::new("node".into()).with_limits(10, 5, 100);
        assert_eq!(b.max_topics, 10);
        assert_eq!(b.max_subscribers_per_topic, 5);
        assert_eq!(b.max_pending_events, 100);
    }

    // ------------------------------------------------------------------
    // Event metadata
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn event_contains_correct_metadata() {
        let b = PubSubBroker::new("origin-node-42".into());
        let mut rx = b
            .subscribe(SubscribeRequest {
                topic: "meta-check".into(),
            })
            .unwrap();

        let before = Utc::now().timestamp_millis() as u64;

        b.publish(PublishRequest {
            topic: "meta-check".into(),
            payload: b"data".to_vec(),
        })
        .unwrap();

        let after = Utc::now().timestamp_millis() as u64;

        let event = rx.recv().await.unwrap();
        assert_eq!(event.topic, "meta-check");
        assert_eq!(event.from_node, "origin-node-42");
        assert_eq!(event.payload, b"data");
        assert!(
            event.timestamp >= before && event.timestamp <= after,
            "timestamp {} should be between {before} and {after}",
            event.timestamp
        );
    }

    // ------------------------------------------------------------------
    // Mixed dead and alive subscribers
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn publish_with_mixed_live_and_dead_subscribers() {
        let b = broker();

        let dead_rx = b
            .subscribe(SubscribeRequest {
                topic: "mixed".into(),
            })
            .unwrap();
        let mut live_rx = b
            .subscribe(SubscribeRequest {
                topic: "mixed".into(),
            })
            .unwrap();

        assert_eq!(b.total_subscribers(), 2);

        // Kill one subscriber.
        drop(dead_rx);

        let resp = b
            .publish(PublishRequest {
                topic: "mixed".into(),
                payload: b"test".to_vec(),
            })
            .unwrap();

        // Only one should have received it.
        assert_eq!(resp.recipients, 1);
        assert_eq!(b.total_subscribers(), 1);

        let event = live_rx.recv().await.unwrap();
        assert_eq!(event.payload, b"test");
    }

    // ------------------------------------------------------------------
    // Publish increments message count
    // ------------------------------------------------------------------

    #[test]
    fn publish_increments_message_count() {
        let b = broker();
        for _ in 0..5 {
            b.publish(PublishRequest {
                topic: "counter".into(),
                payload: vec![],
            })
            .unwrap();
        }
        let topic = b.topics.get("counter").unwrap();
        assert_eq!(topic.message_count, 5);
    }
}
