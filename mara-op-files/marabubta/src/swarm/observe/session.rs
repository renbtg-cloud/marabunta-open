// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use super::types::{OperatorId, SessionId};

/// An incident session groups related interventions for post-incident review.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncidentSession {
    pub id: SessionId,
    pub operator: OperatorId,
    pub title: String,
    pub description: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub intervention_count: u64,
}

/// Session store backed by DashMap.
pub struct SessionStore {
    sessions: DashMap<String, IncidentSession>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
        }
    }

    pub fn create(
        &self,
        operator: OperatorId,
        title: String,
        description: Option<String>,
    ) -> IncidentSession {
        let session = IncidentSession {
            id: SessionId::new(),
            operator,
            title,
            description,
            started_at: Utc::now(),
            ended_at: None,
            intervention_count: 0,
        };
        self.sessions
            .insert(session.id.0.clone(), session.clone());
        session
    }

    pub fn end_session(&self, id: &SessionId) -> Option<IncidentSession> {
        self.sessions.get_mut(&id.0).map(|mut s| {
            s.ended_at = Some(Utc::now());
            s.clone()
        })
    }

    pub fn get(&self, id: &SessionId) -> Option<IncidentSession> {
        self.sessions.get(&id.0).map(|s| s.clone())
    }

    pub fn list(&self) -> Vec<IncidentSession> {
        self.sessions.iter().map(|s| s.value().clone()).collect()
    }

    pub fn list_active(&self) -> Vec<IncidentSession> {
        self.sessions
            .iter()
            .filter(|s| s.ended_at.is_none())
            .map(|s| s.value().clone())
            .collect()
    }

    pub fn increment_intervention_count(&self, id: &SessionId) {
        if let Some(mut s) = self.sessions.get_mut(&id.0) {
            s.intervention_count += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_session() {
        let store = SessionStore::new();
        let session = store.create(
            OperatorId::new("op1"),
            "Test Incident".to_string(),
            Some("Testing session store".to_string()),
        );
        assert_eq!(session.title, "Test Incident");
        assert!(session.ended_at.is_none());
        assert_eq!(session.intervention_count, 0);
    }

    #[test]
    fn test_end_session() {
        let store = SessionStore::new();
        let session = store.create(
            OperatorId::new("op1"),
            "Test".to_string(),
            None,
        );
        let ended = store.end_session(&session.id);
        assert!(ended.is_some());
        assert!(ended.as_ref().map_or(false, |s| s.ended_at.is_some()));
    }

    #[test]
    fn test_get_session() {
        let store = SessionStore::new();
        let session = store.create(
            OperatorId::new("op1"),
            "Test".to_string(),
            None,
        );
        let fetched = store.get(&session.id);
        assert!(fetched.is_some());
        assert_eq!(fetched.as_ref().map(|s| &s.title).cloned(), Some("Test".to_string()));
    }

    #[test]
    fn test_list_sessions() {
        let store = SessionStore::new();
        store.create(OperatorId::new("op1"), "S1".to_string(), None);
        store.create(OperatorId::new("op2"), "S2".to_string(), None);
        assert_eq!(store.list().len(), 2);
    }

    #[test]
    fn test_list_active_sessions() {
        let store = SessionStore::new();
        let s1 = store.create(OperatorId::new("op1"), "S1".to_string(), None);
        store.create(OperatorId::new("op2"), "S2".to_string(), None);
        store.end_session(&s1.id);
        assert_eq!(store.list_active().len(), 1);
    }

    #[test]
    fn test_increment_intervention_count() {
        let store = SessionStore::new();
        let session = store.create(
            OperatorId::new("op1"),
            "Test".to_string(),
            None,
        );
        store.increment_intervention_count(&session.id);
        store.increment_intervention_count(&session.id);
        let fetched = store.get(&session.id).expect("session exists");
        assert_eq!(fetched.intervention_count, 2);
    }

    #[test]
    fn test_end_nonexistent_session() {
        let store = SessionStore::new();
        let result = store.end_session(&SessionId::new());
        assert!(result.is_none());
    }

    #[test]
    fn test_session_serializes() {
        let session = IncidentSession {
            id: SessionId::new(),
            operator: OperatorId::new("op1"),
            title: "Test".to_string(),
            description: None,
            started_at: Utc::now(),
            ended_at: None,
            intervention_count: 0,
        };
        let json = serde_json::to_value(&session).expect("serialize");
        assert_eq!(json["title"], "Test");
    }
}
