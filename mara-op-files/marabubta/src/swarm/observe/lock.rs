// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::errors::OaiError;
use super::types::{EntityRef, InterventionId, OperatorId};

/// An active lock on an entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterventionLock {
    pub entity: EntityRef,
    pub intervention_id: InterventionId,
    pub operator: OperatorId,
    pub action: String,
    pub acquired_at: DateTime<Utc>,
    /// When this lock expires (TTL-based auto-release).
    pub expires_at: DateTime<Utc>,
}

/// Per-entity lock manager using DashMap.
pub struct LockManager {
    locks: DashMap<EntityRef, InterventionLock>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: DashMap::new(),
        }
    }

    /// Try to acquire a lock on an entity.
    ///
    /// Returns Ok(lock) on success.
    /// Returns Err(InterventionConflict) if already locked by another intervention.
    /// Expired locks are transparently cleaned up.
    pub fn try_acquire(
        &self,
        entity: EntityRef,
        intervention_id: InterventionId,
        operator: OperatorId,
        action: String,
        ttl: Duration,
    ) -> Result<InterventionLock, OaiError> {
        let now = Utc::now();
        let expires_at = now
            + chrono::Duration::from_std(ttl)
                .unwrap_or_else(|_| chrono::Duration::seconds(30));

        // Check for existing lock
        if let Some(existing) = self.locks.get(&entity) {
            if existing.expires_at <= now {
                // Expired, remove and continue
                drop(existing);
                self.locks.remove(&entity);
            } else {
                // Active lock by another intervention
                return Err(OaiError::InterventionConflict {
                    entity: entity.clone(),
                    existing_intervention: existing.intervention_id.clone(),
                    existing_operator: existing.operator.clone(),
                    existing_action: existing.action.clone(),
                    started_at: existing.acquired_at.to_rfc3339(),
                });
            }
        }

        let lock = InterventionLock {
            entity: entity.clone(),
            intervention_id,
            operator,
            action,
            acquired_at: now,
            expires_at,
        };

        self.locks.insert(entity, lock.clone());
        Ok(lock)
    }

    /// Release a lock.
    pub fn release(&self, entity: &EntityRef) {
        self.locks.remove(entity);
    }

    /// Release a lock only if it matches the given intervention ID.
    pub fn release_if_owner(
        &self,
        entity: &EntityRef,
        intervention_id: &InterventionId,
    ) -> bool {
        if let Some(entry) = self.locks.get(entity) {
            if entry.intervention_id == *intervention_id {
                drop(entry);
                self.locks.remove(entity);
                return true;
            }
        }
        false
    }

    /// List all active locks (excluding expired).
    pub fn list_active(&self) -> Vec<InterventionLock> {
        let now = Utc::now();
        self.locks
            .iter()
            .filter(|e| e.value().expires_at > now)
            .map(|e| e.value().clone())
            .collect()
    }

    /// Purge expired locks. Returns count of purged locks.
    pub fn purge_expired(&self) -> usize {
        let now = Utc::now();
        let expired: Vec<EntityRef> = self
            .locks
            .iter()
            .filter(|e| e.value().expires_at <= now)
            .map(|e| e.key().clone())
            .collect();
        let count = expired.len();
        for key in expired {
            self.locks.remove(&key);
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::EntityType;
    use std::time::Duration;

    fn test_entity(id: &str) -> EntityRef {
        EntityRef {
            entity_type: EntityType::Node,
            id: id.to_string(),
        }
    }

    #[test]
    fn test_acquire_lock() {
        let mgr = LockManager::new();
        let result = mgr.try_acquire(
            test_entity("n1"),
            InterventionId::new(),
            OperatorId::new("op1"),
            "drain".to_string(),
            Duration::from_secs(30),
        );
        assert!(result.is_ok());
        let lock = result.unwrap();
        assert_eq!(lock.entity.id, "n1");
        assert_eq!(lock.action, "drain");
    }

    #[test]
    fn test_acquire_lock_conflict() {
        let mgr = LockManager::new();
        let _lock1 = mgr
            .try_acquire(
                test_entity("n1"),
                InterventionId::new(),
                OperatorId::new("op1"),
                "drain".to_string(),
                Duration::from_secs(30),
            )
            .unwrap();
        let result = mgr.try_acquire(
            test_entity("n1"),
            InterventionId::new(),
            OperatorId::new("op2"),
            "cordon".to_string(),
            Duration::from_secs(30),
        );
        assert!(result.is_err());
        if let Err(OaiError::InterventionConflict { existing_action, .. }) = result {
            assert_eq!(existing_action, "drain");
        } else {
            panic!("Expected InterventionConflict");
        }
    }

    #[test]
    fn test_acquire_lock_expired() {
        let mgr = LockManager::new();
        // Acquire with a very short TTL (already expired by the time we check)
        let entity = test_entity("n1");
        let now = Utc::now();
        let lock = InterventionLock {
            entity: entity.clone(),
            intervention_id: InterventionId::new(),
            operator: OperatorId::new("op1"),
            action: "drain".to_string(),
            acquired_at: now - chrono::Duration::seconds(60),
            expires_at: now - chrono::Duration::seconds(1),
        };
        mgr.locks.insert(entity.clone(), lock);

        // Should succeed since old lock is expired
        let result = mgr.try_acquire(
            entity,
            InterventionId::new(),
            OperatorId::new("op2"),
            "cordon".to_string(),
            Duration::from_secs(30),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_release_lock() {
        let mgr = LockManager::new();
        let entity = test_entity("n1");
        let _lock = mgr
            .try_acquire(
                entity.clone(),
                InterventionId::new(),
                OperatorId::new("op1"),
                "drain".to_string(),
                Duration::from_secs(30),
            )
            .unwrap();
        mgr.release(&entity);

        // Should succeed after release
        let result = mgr.try_acquire(
            entity,
            InterventionId::new(),
            OperatorId::new("op2"),
            "cordon".to_string(),
            Duration::from_secs(30),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_release_if_owner_correct() {
        let mgr = LockManager::new();
        let entity = test_entity("n1");
        let iid = InterventionId::new();
        let _lock = mgr
            .try_acquire(
                entity.clone(),
                iid.clone(),
                OperatorId::new("op1"),
                "drain".to_string(),
                Duration::from_secs(30),
            )
            .unwrap();
        assert!(mgr.release_if_owner(&entity, &iid));
    }

    #[test]
    fn test_release_if_owner_wrong() {
        let mgr = LockManager::new();
        let entity = test_entity("n1");
        let _lock = mgr
            .try_acquire(
                entity.clone(),
                InterventionId::new(),
                OperatorId::new("op1"),
                "drain".to_string(),
                Duration::from_secs(30),
            )
            .unwrap();
        let wrong_id = InterventionId::new();
        assert!(!mgr.release_if_owner(&entity, &wrong_id));
    }

    #[test]
    fn test_list_active() {
        let mgr = LockManager::new();
        for i in 0..3 {
            mgr.try_acquire(
                test_entity(&format!("n{}", i)),
                InterventionId::new(),
                OperatorId::new("op1"),
                "drain".to_string(),
                Duration::from_secs(30),
            )
            .unwrap();
        }
        assert_eq!(mgr.list_active().len(), 3);
    }

    #[test]
    fn test_purge_expired() {
        let mgr = LockManager::new();
        let now = Utc::now();

        // Insert 2 expired locks
        for i in 0..2 {
            let entity = test_entity(&format!("expired{}", i));
            let lock = InterventionLock {
                entity: entity.clone(),
                intervention_id: InterventionId::new(),
                operator: OperatorId::new("op1"),
                action: "drain".to_string(),
                acquired_at: now - chrono::Duration::seconds(60),
                expires_at: now - chrono::Duration::seconds(1),
            };
            mgr.locks.insert(entity, lock);
        }

        // Insert 1 active lock
        mgr.try_acquire(
            test_entity("active"),
            InterventionId::new(),
            OperatorId::new("op1"),
            "drain".to_string(),
            Duration::from_secs(300),
        )
        .unwrap();

        let purged = mgr.purge_expired();
        assert_eq!(purged, 2);
        assert_eq!(mgr.list_active().len(), 1);
    }
}
