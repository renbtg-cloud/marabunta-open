// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::types::InterventionId;

/// Records a reversible intervention for potential rollback.
#[derive(Debug, Clone, Serialize)]
pub struct RollbackRecord {
    pub intervention_id: InterventionId,
    pub action: String,
    pub entity_snapshot_before: serde_json::Value,
    pub executed_at: DateTime<Utc>,
    pub rollback_deadline: DateTime<Utc>,
    pub post_conditions: Vec<PostCondition>,
}

/// A post-condition that must hold after intervention execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostCondition {
    pub description: String,
    /// A function name / check type (the actual check is done by the engine).
    pub check: String,
    pub expected: serde_json::Value,
}

/// Result of post-condition verification.
#[derive(Debug, Clone, Serialize)]
pub enum PostConditionResult {
    /// All post-conditions passed.
    AllPassed,
    /// Some post-conditions failed; rollback was attempted.
    RolledBack {
        failed_conditions: Vec<String>,
        rollback_success: bool,
    },
    /// Verification timed out; rollback was attempted.
    Timeout { rollback_success: bool },
}

/// Manages rollback records for optimistic execution.
pub struct RollbackManager {
    /// Active rollback records (intervention_id → record).
    records: DashMap<InterventionId, RollbackRecord>,
    /// Default post-condition verification timeout.
    verification_timeout: Duration,
}

impl RollbackManager {
    pub fn new(verification_timeout: Duration) -> Self {
        Self {
            records: DashMap::new(),
            verification_timeout,
        }
    }

    /// Register a rollback record after a reversible intervention executes.
    pub fn register(
        &self,
        intervention_id: InterventionId,
        action: String,
        entity_snapshot_before: serde_json::Value,
        post_conditions: Vec<PostCondition>,
    ) -> RollbackRecord {
        let now = Utc::now();
        let deadline = now
            + chrono::Duration::from_std(self.verification_timeout)
                .unwrap_or_else(|_| chrono::Duration::seconds(10));

        let record = RollbackRecord {
            intervention_id: intervention_id.clone(),
            action,
            entity_snapshot_before,
            executed_at: now,
            rollback_deadline: deadline,
            post_conditions,
        };

        self.records.insert(intervention_id, record.clone());
        record
    }

    /// Mark a rollback record as verified (post-conditions passed).
    pub fn mark_verified(&self, intervention_id: &InterventionId) {
        self.records.remove(intervention_id);
    }

    /// Get a pending rollback record.
    pub fn get(&self, intervention_id: &InterventionId) -> Option<RollbackRecord> {
        self.records.get(intervention_id).map(|r| r.value().clone())
    }

    /// Get all pending rollback records (for background verification loop).
    pub fn pending(&self) -> Vec<RollbackRecord> {
        self.records.iter().map(|r| r.value().clone()).collect()
    }

    /// Remove a record (after rollback is complete).
    pub fn remove(&self, intervention_id: &InterventionId) {
        self.records.remove(intervention_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_register_rollback() {
        let mgr = RollbackManager::new(Duration::from_secs(10));
        let iid = InterventionId::new();
        let record = mgr.register(
            iid.clone(),
            "drain".to_string(),
            serde_json::json!({"status": "alive"}),
            vec![PostCondition {
                description: "node drained".to_string(),
                check: "node_status".to_string(),
                expected: serde_json::json!("drained"),
            }],
        );
        assert_eq!(record.action, "drain");
        assert!(mgr.get(&iid).is_some());
    }

    #[test]
    fn test_mark_verified() {
        let mgr = RollbackManager::new(Duration::from_secs(10));
        let iid = InterventionId::new();
        mgr.register(
            iid.clone(),
            "drain".to_string(),
            serde_json::json!({}),
            vec![],
        );
        assert!(mgr.get(&iid).is_some());
        mgr.mark_verified(&iid);
        assert!(mgr.get(&iid).is_none());
    }

    #[test]
    fn test_pending_list() {
        let mgr = RollbackManager::new(Duration::from_secs(10));
        let iid1 = InterventionId::new();
        let iid2 = InterventionId::new();
        mgr.register(iid1, "drain".to_string(), serde_json::json!({}), vec![]);
        mgr.register(iid2, "cordon".to_string(), serde_json::json!({}), vec![]);
        assert_eq!(mgr.pending().len(), 2);
    }
}
