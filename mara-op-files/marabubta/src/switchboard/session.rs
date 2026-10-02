// Marabunta - Licensed under the MIT License.
//! Session store for the Switchboard module.
//!
//! Manages lifecycle of switchboard sessions from creation through computation.
//! Sessions track the full pipeline state: ingested expressions, classification
//! results, presented menus, compute results, and cross-session back-references.
//!
//! Uses `DashMap` for lock-free concurrent access. Expired sessions are pruned
//! on access and via periodic cleanup sweeps.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::config::{MAX_SESSIONS, SESSION_TTL};
use super::errors::{SwitchboardError, SwitchboardResult};
use super::types::{
    BackReference, ClassificationResult, ComputeResult, NormalizedExpression, OptionMenu, SessionId,
};

// ============================================================================
// SessionState
// ============================================================================

/// Current state of a switchboard session, representing its position in the
/// pipeline lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Expression has been ingested and normalized, awaiting classification.
    Ingested,

    /// Expression has been classified into a mathematical category.
    Classified,

    /// Option menu has been generated and presented to the user.
    MenuPresented,

    /// Computation is in progress on a selected handler.
    Computing,

    /// Computation completed successfully.
    Completed,

    /// Session ended in failure (handler error, timeout, etc.).
    Failed,
}

impl fmt::Display for SessionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ingested => write!(f, "ingested"),
            Self::Classified => write!(f, "classified"),
            Self::MenuPresented => write!(f, "menu_presented"),
            Self::Computing => write!(f, "computing"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

impl SessionState {
    /// Returns true if this is a terminal state (Completed or Failed).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }

    /// Returns the set of valid next states from the current state.
    pub fn valid_transitions(&self) -> &[SessionState] {
        match self {
            Self::Ingested => &[Self::Classified, Self::Failed],
            Self::Classified => &[Self::MenuPresented, Self::Failed],
            Self::MenuPresented => &[Self::Computing, Self::Failed],
            Self::Computing => &[Self::Completed, Self::Failed],
            Self::Completed => &[],
            Self::Failed => &[],
        }
    }

    /// Returns true if transitioning to `next` is valid from this state.
    pub fn can_transition_to(&self, next: &SessionState) -> bool {
        self.valid_transitions().contains(next)
    }
}

// ============================================================================
// SwitchboardSession
// ============================================================================

/// A single switchboard session tracking the full pipeline lifecycle.
///
/// Created when an expression is ingested. Accumulates classification results,
/// menu options, compute results, and back-references as the session progresses
/// through the pipeline stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchboardSession {
    /// Unique session identifier.
    pub id: SessionId,

    /// Current pipeline state.
    pub state: SessionState,

    /// History of normalized expressions submitted in this session.
    pub expressions: Vec<NormalizedExpression>,

    /// Classification result (populated after classification stage).
    pub classification: Option<ClassificationResult>,

    /// Option menu presented to the user (populated after menu generation).
    pub menu: Option<OptionMenu>,

    /// Accumulated compute results from handler executions.
    pub results: Vec<ComputeResult>,

    /// Back-references to expressions in other sessions.
    pub back_references: Vec<BackReference>,

    /// When this session was created.
    pub created_at: DateTime<Utc>,

    /// Last activity timestamp, updated on every state transition and access.
    pub last_activity: DateTime<Utc>,

    /// Optional user identifier for session association.
    pub user_id: Option<String>,
}

impl SwitchboardSession {
    /// Create a new session in the `Ingested` state with the given ID.
    pub fn new(id: SessionId) -> Self {
        let now = Utc::now();
        Self {
            id,
            state: SessionState::Ingested,
            expressions: Vec::new(),
            classification: None,
            menu: None,
            results: Vec::new(),
            back_references: Vec::new(),
            created_at: now,
            last_activity: now,
            user_id: None,
        }
    }

    /// Attach a user ID to this session.
    pub fn with_user(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// Add a normalized expression to the session history.
    pub fn add_expression(&mut self, expr: NormalizedExpression) {
        self.expressions.push(expr);
        self.touch();
    }

    /// Transition to the `Classified` state with the given classification result.
    ///
    /// Returns an error if the current state does not allow this transition.
    pub fn set_classified(&mut self, result: ClassificationResult) -> SwitchboardResult<()> {
        self.transition_to(SessionState::Classified)?;
        self.classification = Some(result);
        Ok(())
    }

    /// Transition to the `MenuPresented` state with the given option menu.
    ///
    /// Returns an error if the current state does not allow this transition.
    pub fn set_menu_presented(&mut self, menu: OptionMenu) -> SwitchboardResult<()> {
        self.transition_to(SessionState::MenuPresented)?;
        self.menu = Some(menu);
        Ok(())
    }

    /// Transition to the `Computing` state.
    ///
    /// Returns an error if the current state does not allow this transition.
    pub fn set_computing(&mut self) -> SwitchboardResult<()> {
        self.transition_to(SessionState::Computing)
    }

    /// Transition to the `Completed` state with the given compute result.
    ///
    /// Returns an error if the current state does not allow this transition.
    pub fn set_completed(&mut self, result: ComputeResult) -> SwitchboardResult<()> {
        self.transition_to(SessionState::Completed)?;
        self.results.push(result);
        Ok(())
    }

    /// Transition to the `Failed` state.
    ///
    /// Returns an error if the current state is already terminal.
    pub fn set_failed(&mut self) -> SwitchboardResult<()> {
        self.transition_to(SessionState::Failed)
    }

    /// Add a back-reference to another session's expression.
    pub fn add_back_reference(&mut self, back_ref: BackReference) {
        self.back_references.push(back_ref);
        self.touch();
    }

    /// Add a compute result without changing state (e.g., partial results).
    pub fn add_result(&mut self, result: ComputeResult) {
        self.results.push(result);
        self.touch();
    }

    /// Returns true if the session has exceeded its TTL.
    pub fn is_expired(&self) -> bool {
        let elapsed = Utc::now()
            .signed_duration_since(self.last_activity)
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);
        elapsed > SESSION_TTL
    }

    /// Returns the number of expressions in this session.
    pub fn expression_count(&self) -> usize {
        self.expressions.len()
    }

    /// Look up an expression by index.
    pub fn get_expression(&self, index: usize) -> Option<&NormalizedExpression> {
        self.expressions.get(index)
    }

    /// Update the last activity timestamp to now.
    fn touch(&mut self) {
        self.last_activity = Utc::now();
    }

    /// Attempt a state transition, checking validity.
    fn transition_to(&mut self, next: SessionState) -> SwitchboardResult<()> {
        if !self.state.can_transition_to(&next) {
            return Err(SwitchboardError::Internal(format!(
                "invalid state transition: {} -> {}",
                self.state, next
            )));
        }
        self.state = next;
        self.touch();
        Ok(())
    }
}

// ============================================================================
// SessionStore
// ============================================================================

/// Concurrent session store backed by `DashMap`.
///
/// Provides thread-safe creation, retrieval, update, and cleanup of switchboard
/// sessions. Enforces a maximum session count and TTL-based expiry.
pub struct SessionStore {
    sessions: DashMap<SessionId, SwitchboardSession>,
    max_sessions: usize,
}

impl SessionStore {
    /// Create a new session store with the default maximum session capacity.
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
            max_sessions: MAX_SESSIONS,
        }
    }

    /// Create a new session store with an explicit capacity hint.
    ///
    /// The `cap` value is used both as the DashMap capacity hint and as the
    /// maximum allowed concurrent sessions.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            sessions: DashMap::with_capacity(cap),
            max_sessions: cap,
        }
    }

    /// Create a new session, optionally associated with a user.
    ///
    /// Returns `MaxSessionsReached` if the store is at capacity.
    pub fn create_session(&self, user_id: Option<String>) -> SwitchboardResult<SessionId> {
        if self.sessions.len() >= self.max_sessions {
            return Err(SwitchboardError::MaxSessionsReached(self.max_sessions));
        }

        let id = SessionId::new();
        let session = match user_id {
            Some(uid) => SwitchboardSession::new(id).with_user(uid),
            None => SwitchboardSession::new(id),
        };

        self.sessions.insert(id, session);
        Ok(id)
    }

    /// Retrieve a clone of the session with the given ID.
    ///
    /// Returns `SessionNotFound` if the ID does not exist, or `SessionExpired`
    /// if the session has exceeded its TTL (the expired session is also removed).
    pub fn get_session(&self, id: &SessionId) -> SwitchboardResult<SwitchboardSession> {
        let entry = self
            .sessions
            .get(id)
            .ok_or_else(|| SwitchboardError::SessionNotFound(id.to_string()))?;

        let session = entry.value().clone();
        // Drop the guard before potentially removing
        drop(entry);

        if session.is_expired() {
            self.sessions.remove(id);
            return Err(SwitchboardError::SessionExpired(id.to_string()));
        }

        Ok(session)
    }

    /// Apply a mutation to an existing session in-place.
    ///
    /// The closure `f` receives a mutable reference to the session. Returns
    /// `SessionNotFound` if the ID does not exist, or `SessionExpired` if
    /// the session has exceeded its TTL.
    pub fn update_session(
        &self,
        id: &SessionId,
        f: impl FnOnce(&mut SwitchboardSession),
    ) -> SwitchboardResult<()> {
        let mut entry = self
            .sessions
            .get_mut(id)
            .ok_or_else(|| SwitchboardError::SessionNotFound(id.to_string()))?;

        if entry.value().is_expired() {
            // Drop the guard before removing
            let id_copy = *id;
            drop(entry);
            self.sessions.remove(&id_copy);
            return Err(SwitchboardError::SessionExpired(id_copy.to_string()));
        }

        f(entry.value_mut());
        Ok(())
    }

    /// Remove a session by ID, returning the removed session if it existed.
    pub fn remove_session(&self, id: &SessionId) -> Option<SwitchboardSession> {
        self.sessions.remove(id).map(|(_, session)| session)
    }

    /// Remove all sessions that have exceeded their TTL.
    ///
    /// Returns the number of sessions removed.
    pub fn cleanup_expired(&self) -> usize {
        let before = self.sessions.len();
        self.sessions.retain(|_, session| !session.is_expired());
        before - self.sessions.len()
    }

    /// Returns the current number of active sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Resolve a back-reference to the expression it points to.
    ///
    /// Looks up the referenced session, checks TTL, and retrieves the expression
    /// at the specified index. Returns `SessionNotFound` if the session does not
    /// exist, `SessionExpired` if it has exceeded TTL, or `Internal` if the
    /// expression index is out of bounds.
    pub fn resolve_back_reference(
        &self,
        back_ref: &BackReference,
    ) -> SwitchboardResult<NormalizedExpression> {
        let session = self.get_session(&back_ref.session_id)?;

        session
            .get_expression(back_ref.expression_index)
            .cloned()
            .ok_or_else(|| {
                SwitchboardError::Internal(format!(
                    "expression index {} out of bounds (session {} has {} expressions)",
                    back_ref.expression_index,
                    back_ref.session_id,
                    session.expression_count(),
                ))
            })
    }

    /// Returns true if the store contains a session with the given ID.
    pub fn contains(&self, id: &SessionId) -> bool {
        self.sessions.contains_key(id)
    }

    /// Returns an iterator over all session IDs currently in the store.
    ///
    /// Note: This provides a snapshot; sessions may be added or removed
    /// concurrently.
    pub fn session_ids(&self) -> Vec<SessionId> {
        self.sessions.iter().map(|entry| *entry.key()).collect()
    }

    /// Returns sessions belonging to a specific user.
    pub fn sessions_for_user(&self, user_id: &str) -> Vec<SwitchboardSession> {
        self.sessions
            .iter()
            .filter_map(|entry| {
                let session = entry.value();
                if session.user_id.as_deref() == Some(user_id) {
                    Some(session.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionStore")
            .field("session_count", &self.sessions.len())
            .field("max_sessions", &self.max_sessions)
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switchboard::types::{
        Category, ClassificationResult, ComputeResult, HandlerId, NormalizedExpression, OptionMenu,
        Operation,
    };

    /// Helper: create a simple normalized expression for testing.
    fn make_expression(latex: &str) -> NormalizedExpression {
        NormalizedExpression::new(latex.to_string(), latex.to_string())
    }

    /// Helper: create a simple classification result for testing.
    fn make_classification() -> ClassificationResult {
        ClassificationResult::new(Category::Algebra, vec![Operation::Solve], 0.95)
    }

    /// Helper: create a simple option menu for testing.
    fn make_menu(session_id: SessionId) -> OptionMenu {
        OptionMenu::new(session_id, "x^2 + 1 = 0".to_string())
    }

    /// Helper: create a simple compute result for testing.
    fn make_compute_result() -> ComputeResult {
        ComputeResult::success(
            HandlerId::new("sympy".to_string()),
            "SymPy".to_string(),
            42,
        )
        .with_latex("x = \\pm i".to_string())
    }

    // -----------------------------------------------------------------------
    // Test 1: Create and retrieve session
    // -----------------------------------------------------------------------
    #[test]
    fn test_create_and_retrieve_session() {
        let store = SessionStore::new();
        let id = store.create_session(None).unwrap();

        let session = store.get_session(&id).unwrap();
        assert_eq!(session.id, id);
        assert_eq!(session.state, SessionState::Ingested);
        assert!(session.expressions.is_empty());
        assert!(session.classification.is_none());
        assert!(session.menu.is_none());
        assert!(session.results.is_empty());
        assert!(session.back_references.is_empty());
        assert!(session.user_id.is_none());
        assert_eq!(store.session_count(), 1);
    }

    // -----------------------------------------------------------------------
    // Test 2: Session not found error
    // -----------------------------------------------------------------------
    #[test]
    fn test_session_not_found() {
        let store = SessionStore::new();
        let fake_id = SessionId::new();

        match store.get_session(&fake_id) {
            Err(SwitchboardError::SessionNotFound(msg)) => {
                assert_eq!(msg, fake_id.to_string());
            }
            other => panic!("expected SessionNotFound, got: {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Test 3: Session expiry detection
    // -----------------------------------------------------------------------
    #[test]
    fn test_session_expiry_detection() {
        let store = SessionStore::new();
        let id = store.create_session(None).unwrap();

        // Manually backdate the session to simulate expiry
        store
            .update_session(&id, |session| {
                let expired_time = Utc::now() - chrono::Duration::seconds(SESSION_TTL.as_secs() as i64 + 60);
                session.last_activity = expired_time;
                session.created_at = expired_time;
            })
            .unwrap();

        // Now retrieval should detect the expiry
        match store.get_session(&id) {
            Err(SwitchboardError::SessionExpired(msg)) => {
                assert_eq!(msg, id.to_string());
            }
            other => panic!("expected SessionExpired, got: {:?}", other),
        }

        // Session should have been removed
        assert_eq!(store.session_count(), 0);
    }

    // -----------------------------------------------------------------------
    // Test 4: Max sessions limit
    // -----------------------------------------------------------------------
    #[test]
    fn test_max_sessions_limit() {
        let store = SessionStore::with_capacity(3);

        store.create_session(None).unwrap();
        store.create_session(None).unwrap();
        store.create_session(None).unwrap();

        match store.create_session(None) {
            Err(SwitchboardError::MaxSessionsReached(max)) => {
                assert_eq!(max, 3);
            }
            other => panic!("expected MaxSessionsReached, got: {:?}", other),
        }

        assert_eq!(store.session_count(), 3);
    }

    // -----------------------------------------------------------------------
    // Test 5: State transitions (full pipeline)
    // -----------------------------------------------------------------------
    #[test]
    fn test_state_transitions_full_pipeline() {
        let store = SessionStore::new();
        let id = store.create_session(None).unwrap();

        // Add expression
        store
            .update_session(&id, |s| {
                s.add_expression(make_expression("x^2 + 1 = 0"));
            })
            .unwrap();

        // Ingested -> Classified
        store
            .update_session(&id, |s| {
                s.set_classified(make_classification()).unwrap();
            })
            .unwrap();

        let session = store.get_session(&id).unwrap();
        assert_eq!(session.state, SessionState::Classified);
        assert!(session.classification.is_some());

        // Classified -> MenuPresented
        store
            .update_session(&id, |s| {
                s.set_menu_presented(make_menu(id)).unwrap();
            })
            .unwrap();

        let session = store.get_session(&id).unwrap();
        assert_eq!(session.state, SessionState::MenuPresented);
        assert!(session.menu.is_some());

        // MenuPresented -> Computing
        store
            .update_session(&id, |s| {
                s.set_computing().unwrap();
            })
            .unwrap();

        let session = store.get_session(&id).unwrap();
        assert_eq!(session.state, SessionState::Computing);

        // Computing -> Completed
        store
            .update_session(&id, |s| {
                s.set_completed(make_compute_result()).unwrap();
            })
            .unwrap();

        let session = store.get_session(&id).unwrap();
        assert_eq!(session.state, SessionState::Completed);
        assert_eq!(session.results.len(), 1);
        assert!(session.state.is_terminal());
    }

    // -----------------------------------------------------------------------
    // Test 6: Invalid state transition
    // -----------------------------------------------------------------------
    #[test]
    fn test_invalid_state_transition() {
        let mut session = SwitchboardSession::new(SessionId::new());

        // Ingested -> Computing should fail (must go through Classified + MenuPresented)
        let result = session.set_computing();
        assert!(result.is_err());
        match result {
            Err(SwitchboardError::Internal(msg)) => {
                assert!(msg.contains("invalid state transition"));
                assert!(msg.contains("ingested"));
                assert!(msg.contains("computing"));
            }
            other => panic!("expected Internal error, got: {:?}", other),
        }

        // State should not have changed
        assert_eq!(session.state, SessionState::Ingested);
    }

    // -----------------------------------------------------------------------
    // Test 7: Back-reference resolution
    // -----------------------------------------------------------------------
    #[test]
    fn test_back_reference_resolution() {
        let store = SessionStore::new();
        let id = store.create_session(None).unwrap();

        // Add two expressions
        store
            .update_session(&id, |s| {
                s.add_expression(make_expression("x^2 + 1"));
                s.add_expression(make_expression("y = 2x + 3"));
            })
            .unwrap();

        // Resolve back-reference to expression 0
        let back_ref = BackReference::new(id, 0);
        let expr = store.resolve_back_reference(&back_ref).unwrap();
        assert_eq!(expr.latex, "x^2 + 1");

        // Resolve back-reference to expression 1
        let back_ref = BackReference::new(id, 1);
        let expr = store.resolve_back_reference(&back_ref).unwrap();
        assert_eq!(expr.latex, "y = 2x + 3");

        // Out-of-bounds index should fail
        let back_ref = BackReference::new(id, 99);
        match store.resolve_back_reference(&back_ref) {
            Err(SwitchboardError::Internal(msg)) => {
                assert!(msg.contains("out of bounds"));
                assert!(msg.contains("99"));
            }
            other => panic!("expected Internal error, got: {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Test 8: Back-reference to nonexistent session
    // -----------------------------------------------------------------------
    #[test]
    fn test_back_reference_nonexistent_session() {
        let store = SessionStore::new();
        let fake_id = SessionId::new();
        let back_ref = BackReference::new(fake_id, 0);

        match store.resolve_back_reference(&back_ref) {
            Err(SwitchboardError::SessionNotFound(_)) => {}
            other => panic!("expected SessionNotFound, got: {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Test 9: Cleanup removes expired sessions
    // -----------------------------------------------------------------------
    #[test]
    fn test_cleanup_expired_sessions() {
        let store = SessionStore::new();

        // Create 3 sessions, expire 2 of them
        let id1 = store.create_session(None).unwrap();
        let id2 = store.create_session(None).unwrap();
        let _id3 = store.create_session(None).unwrap();

        let expired_time = Utc::now() - chrono::Duration::seconds(SESSION_TTL.as_secs() as i64 + 60);

        store
            .update_session(&id1, |s| {
                s.last_activity = expired_time;
            })
            .unwrap();
        store
            .update_session(&id2, |s| {
                s.last_activity = expired_time;
            })
            .unwrap();

        assert_eq!(store.session_count(), 3);

        let removed = store.cleanup_expired();
        assert_eq!(removed, 2);
        assert_eq!(store.session_count(), 1);
    }

    // -----------------------------------------------------------------------
    // Test 10: Update session works
    // -----------------------------------------------------------------------
    #[test]
    fn test_update_session() {
        let store = SessionStore::new();
        let id = store.create_session(None).unwrap();

        store
            .update_session(&id, |s| {
                s.add_expression(make_expression("\\int x dx"));
                s.add_expression(make_expression("\\sum_{i=0}^{n} i"));
            })
            .unwrap();

        let session = store.get_session(&id).unwrap();
        assert_eq!(session.expression_count(), 2);
        assert_eq!(session.expressions[0].latex, "\\int x dx");
        assert_eq!(session.expressions[1].latex, "\\sum_{i=0}^{n} i");
    }

    // -----------------------------------------------------------------------
    // Test 11: Session with user ID
    // -----------------------------------------------------------------------
    #[test]
    fn test_session_with_user_id() {
        let store = SessionStore::new();
        let id = store
            .create_session(Some("alice@example.com".to_string()))
            .unwrap();

        let session = store.get_session(&id).unwrap();
        assert_eq!(session.user_id, Some("alice@example.com".to_string()));
    }

    // -----------------------------------------------------------------------
    // Test 12: Sessions for user lookup
    // -----------------------------------------------------------------------
    #[test]
    fn test_sessions_for_user() {
        let store = SessionStore::new();

        store
            .create_session(Some("alice".to_string()))
            .unwrap();
        store
            .create_session(Some("alice".to_string()))
            .unwrap();
        store
            .create_session(Some("bob".to_string()))
            .unwrap();
        store.create_session(None).unwrap();

        let alice_sessions = store.sessions_for_user("alice");
        assert_eq!(alice_sessions.len(), 2);
        for s in &alice_sessions {
            assert_eq!(s.user_id, Some("alice".to_string()));
        }

        let bob_sessions = store.sessions_for_user("bob");
        assert_eq!(bob_sessions.len(), 1);

        let nobody_sessions = store.sessions_for_user("nobody");
        assert_eq!(nobody_sessions.len(), 0);
    }

    // -----------------------------------------------------------------------
    // Test 13: Remove session
    // -----------------------------------------------------------------------
    #[test]
    fn test_remove_session() {
        let store = SessionStore::new();
        let id = store.create_session(None).unwrap();

        assert!(store.contains(&id));
        assert_eq!(store.session_count(), 1);

        let removed = store.remove_session(&id);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, id);
        assert!(!store.contains(&id));
        assert_eq!(store.session_count(), 0);

        // Removing again should return None
        let removed_again = store.remove_session(&id);
        assert!(removed_again.is_none());
    }

    // -----------------------------------------------------------------------
    // Test 14: Failure state transition from any non-terminal state
    // -----------------------------------------------------------------------
    #[test]
    fn test_failure_from_any_state() {
        // Ingested -> Failed
        let mut s1 = SwitchboardSession::new(SessionId::new());
        assert!(s1.set_failed().is_ok());
        assert_eq!(s1.state, SessionState::Failed);

        // Classified -> Failed
        let mut s2 = SwitchboardSession::new(SessionId::new());
        s2.set_classified(make_classification()).unwrap();
        assert!(s2.set_failed().is_ok());
        assert_eq!(s2.state, SessionState::Failed);

        // MenuPresented -> Failed
        let mut s3 = SwitchboardSession::new(SessionId::new());
        s3.set_classified(make_classification()).unwrap();
        s3.set_menu_presented(make_menu(s3.id)).unwrap();
        assert!(s3.set_failed().is_ok());
        assert_eq!(s3.state, SessionState::Failed);

        // Computing -> Failed
        let mut s4 = SwitchboardSession::new(SessionId::new());
        s4.set_classified(make_classification()).unwrap();
        s4.set_menu_presented(make_menu(s4.id)).unwrap();
        s4.set_computing().unwrap();
        assert!(s4.set_failed().is_ok());
        assert_eq!(s4.state, SessionState::Failed);

        // Failed -> Failed should NOT work (terminal)
        let mut s5 = SwitchboardSession::new(SessionId::new());
        s5.set_failed().unwrap();
        assert!(s5.set_failed().is_err());

        // Completed -> Failed should NOT work (terminal)
        let mut s6 = SwitchboardSession::new(SessionId::new());
        s6.set_classified(make_classification()).unwrap();
        s6.set_menu_presented(make_menu(s6.id)).unwrap();
        s6.set_computing().unwrap();
        s6.set_completed(make_compute_result()).unwrap();
        assert!(s6.set_failed().is_err());
    }
}
