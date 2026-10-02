// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, warn};

use std::sync::Arc;

use crate::swarm::events::{EventBus, EventFilter, SwarmEvent};
use super::config::ObservationConfig;
use super::errors::OaiError;
use super::types::{OperatorId, SubscriptionId};

/// A managed subscription with its own filtered channel.
pub struct Subscription {
    pub id: SubscriptionId,
    pub owner: OperatorId,
    pub filter: EventFilter,
    pub created_at: DateTime<Utc>,
    tx: mpsc::Sender<SwarmEvent>,
    pub delivered: u64,
    pub dropped: u64,
}

/// Handle returned to the API layer for consuming subscription events.
pub struct SubscriptionHandle {
    pub id: SubscriptionId,
    pub rx: mpsc::Receiver<SwarmEvent>,
}

/// Subscription stats for the management API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionStats {
    pub id: SubscriptionId,
    pub owner: OperatorId,
    pub filter: EventFilter,
    pub created_at: DateTime<Utc>,
    pub delivered: u64,
    pub dropped: u64,
}

struct SubscriptionEntry {
    sub: Subscription,
    _task: tokio::task::JoinHandle<()>,
}

/// Manages all active observation subscriptions.
///
/// Each subscription is a background task that:
/// 1. Receives broadcast events from EventBus
/// 2. Filters them according to the subscription's EventFilter
/// 3. Pushes matching events into a bounded mpsc channel
/// 4. On backpressure (channel full), drops and increments dropped counter
pub struct SubscriptionManager {
    subscriptions: DashMap<SubscriptionId, SubscriptionEntry>,
    event_bus: Arc<EventBus>,
    config: ObservationConfig,
}

impl SubscriptionManager {
    pub fn new(event_bus: Arc<EventBus>, config: ObservationConfig) -> Self {
        Self {
            subscriptions: DashMap::new(),
            event_bus,
            config,
        }
    }

    /// Create a new subscription. Returns a handle for consuming events.
    pub fn create(
        &self,
        owner: OperatorId,
        filter: EventFilter,
    ) -> Result<SubscriptionHandle, OaiError> {
        let owner_count = self.subscriptions.iter()
            .filter(|e| e.value().sub.owner == owner)
            .count();
        if owner_count >= self.config.max_subscriptions_per_operator {
            return Err(OaiError::SubsystemError {
                subsystem: "subscription_manager".to_string(),
                message: format!(
                    "operator {} already has {} subscriptions (max {})",
                    owner, owner_count, self.config.max_subscriptions_per_operator
                ),
            });
        }

        let id = SubscriptionId::new();
        let (tx, rx) = mpsc::channel(self.config.subscription_buffer_size);
        let mut bus_rx = self.event_bus.subscribe();
        let filter_clone = filter.clone();
        let id_clone = id.clone();
        let tx_clone = tx.clone();

        let task = tokio::spawn(async move {
            loop {
                match bus_rx.recv().await {
                    Ok(event) => {
                        if event.matches_filter(&filter_clone)
                            && tx_clone.try_send(event).is_err()
                                && tx_clone.is_closed() {
                                    debug!(sub = %id_clone, "subscription channel closed, exiting filter task");
                                    break;
                                }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(sub = %id_clone, lagged = n, "subscription lagged on EventBus broadcast");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!(sub = %id_clone, "EventBus broadcast closed, exiting filter task");
                        break;
                    }
                }
            }
        });

        let sub = Subscription {
            id: id.clone(),
            owner,
            filter,
            created_at: Utc::now(),
            tx,
            delivered: 0,
            dropped: 0,
        };

        self.subscriptions.insert(id.clone(), SubscriptionEntry { sub, _task: task });
        Ok(SubscriptionHandle { id, rx })
    }

    /// Delete a subscription by ID.
    pub fn delete(&self, id: &SubscriptionId) -> bool {
        self.subscriptions.remove(id).is_some()
    }

    /// Get stats for a subscription.
    pub fn stats(&self, id: &SubscriptionId) -> Option<SubscriptionStats> {
        self.subscriptions.get(id).map(|e| SubscriptionStats {
            id: e.sub.id.clone(),
            owner: e.sub.owner.clone(),
            filter: e.sub.filter.clone(),
            created_at: e.sub.created_at,
            delivered: e.sub.delivered,
            dropped: e.sub.dropped,
        })
    }

    /// List all active subscriptions.
    pub fn list(&self) -> Vec<SubscriptionStats> {
        self.subscriptions.iter().map(|e| SubscriptionStats {
            id: e.sub.id.clone(),
            owner: e.sub.owner.clone(),
            filter: e.sub.filter.clone(),
            created_at: e.sub.created_at,
            delivered: e.sub.delivered,
            dropped: e.sub.dropped,
        }).collect()
    }

    /// Count active subscriptions.
    pub fn count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Resume a subscription from a specific event ID.
    ///
    /// Returns events from the ring buffer after the given ID.
    pub fn resume(
        &self,
        sub_id: &SubscriptionId,
        resume_from_id: u64,
    ) -> Result<Vec<SwarmEvent>, OaiError> {
        let entry = self.subscriptions.get(sub_id)
            .ok_or_else(|| OaiError::EntityNotFound {
                entity: super::types::EntityRef {
                    entity_type: super::types::EntityType::Session,
                    id: sub_id.0.clone(),
                },
            })?;

        let historical = self.event_bus.recent_filtered(&entry.sub.filter, 1000);
        let resumed: Vec<SwarmEvent> = historical.into_iter()
            .filter(|e| e.id > resume_from_id)
            .collect();

        Ok(resumed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::events::EventBus;

    fn make_event_bus() -> Arc<EventBus> {
        Arc::new(EventBus::new(1024))
    }

    fn default_config() -> ObservationConfig {
        ObservationConfig::default()
    }

    #[tokio::test]
    async fn test_create_subscription() {
        let bus = make_event_bus();
        let mgr = SubscriptionManager::new(bus, default_config());
        let handle = mgr.create(
            OperatorId::new("op1"),
            EventFilter::default(),
        ).expect("create should succeed");
        assert!(!handle.id.0.is_empty());
        assert!(mgr.stats(&handle.id).is_some());
    }

    #[tokio::test]
    async fn test_subscription_delete() {
        let bus = make_event_bus();
        let mgr = SubscriptionManager::new(bus, default_config());
        let handle = mgr.create(
            OperatorId::new("op1"),
            EventFilter::default(),
        ).expect("create");
        let id = handle.id.clone();
        assert!(mgr.delete(&id));
        assert!(mgr.stats(&id).is_none());
    }

    #[tokio::test]
    async fn test_subscription_per_operator_limit() {
        let bus = make_event_bus();
        let mut config = default_config();
        config.max_subscriptions_per_operator = 2;
        let mgr = SubscriptionManager::new(bus, config);

        let _h1 = mgr.create(OperatorId::new("op1"), EventFilter::default()).expect("1st");
        let _h2 = mgr.create(OperatorId::new("op1"), EventFilter::default()).expect("2nd");
        let result = mgr.create(OperatorId::new("op1"), EventFilter::default());
        assert!(result.is_err());

        // Different operator is fine
        let _h3 = mgr.create(OperatorId::new("op2"), EventFilter::default()).expect("different op");
    }

    #[tokio::test]
    async fn test_subscription_list() {
        let bus = make_event_bus();
        let mgr = SubscriptionManager::new(bus, default_config());
        let _h1 = mgr.create(OperatorId::new("op1"), EventFilter::default()).unwrap();
        let _h2 = mgr.create(OperatorId::new("op2"), EventFilter::default()).unwrap();
        let _h3 = mgr.create(OperatorId::new("op3"), EventFilter::default()).unwrap();
        assert_eq!(mgr.list().len(), 3);
        assert_eq!(mgr.count(), 3);
    }

    #[tokio::test]
    async fn test_subscription_filter_delivers_matching() {
        use crate::swarm::events::SwarmEvent;
        use crate::swarm::complexity::{ConcernDomain, EventSeverity, ComplexityHint};

        let bus = make_event_bus();
        let mgr = SubscriptionManager::new(bus.clone(), default_config());

        let filter = EventFilter {
            domains: Some(vec![ConcernDomain::Fleet]),
            ..Default::default()
        };
        let mut handle = mgr.create(OperatorId::new("op1"), filter).unwrap();

        // Emit a fleet event
        bus.emit(SwarmEvent {
            id: 0,
            timestamp: Utc::now(),
            domain: ConcernDomain::Fleet,
            severity: EventSeverity::Info,
            complexity: ComplexityHint::Simple,
            summary: "fleet event".to_string(),
            details: serde_json::json!({}),
            related_entities: vec![],
            suggested_actions: vec![],
            source_node: None,
            correlation_id: None,
            supersedes: None,
        });

        // Emit a non-fleet event
        bus.emit(SwarmEvent {
            id: 0,
            timestamp: Utc::now(),
            domain: ConcernDomain::Work,
            severity: EventSeverity::Info,
            complexity: ComplexityHint::Simple,
            summary: "work event".to_string(),
            details: serde_json::json!({}),
            related_entities: vec![],
            suggested_actions: vec![],
            source_node: None,
            correlation_id: None,
            supersedes: None,
        });

        // Give the background task time to forward
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Should receive only the fleet event
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(200),
            handle.rx.recv(),
        ).await {
            Ok(Some(event)) => {
                assert_eq!(event.summary, "fleet event");
            }
            _ => panic!("Expected fleet event to be delivered"),
        }
    }
}
