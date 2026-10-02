// Marabunta - Licensed under the MIT License.
//! Integration tests for the Observe-and-Interfere (OAI) subsystem.
//!
//! These tests exercise the OAI pipeline end-to-end: observation views,
//! guard evaluation, lock management, impact assessment, intervention
//! execution, session tracking, and subscription management.
//!
//! Tests construct subsystems directly (like admission tests) for speed
//! and isolation. The feature-gate tests verify SwarmNode wiring.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;

    use crate::swarm::audit::AuditLog;
    use crate::swarm::config::SwarmConfig;
    use crate::swarm::events::EventBus;
    use crate::swarm::fleet::FleetManager;
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::observe::config::{ImpactConfig, OaiConfig, ObservationConfig};
    use crate::swarm::observe::engine::{InterventionEngine, InterventionRequest};
    use crate::swarm::observe::errors::OaiError;
    use crate::swarm::observe::guard::GuardEvaluator;
    use crate::swarm::observe::impact::ImpactAssessor;
    use crate::swarm::observe::lock::LockManager;
    use crate::swarm::observe::operator::{OperatorStore, ProfilePreset};
    use crate::swarm::observe::rollback::RollbackManager;
    use crate::swarm::observe::session::SessionStore;
    use crate::swarm::observe::subscription::SubscriptionManager;
    use crate::swarm::observe::tier1::Tier1Handler;
    use crate::swarm::observe::tier2::Tier2Handler;
    use crate::swarm::observe::types::{
        EntityRef, EntityType, InterventionAction, InterventionId, InterventionTier,
        InterventionUrgency, OperatorId,
    };
    use crate::swarm::observe::veto::VetoChecker;
    use crate::swarm::observe::views;
    use crate::swarm::types::{NodeId, NodeInfo, NodeStatus, ResourceSnapshot, Trait};

    // ========================================================================
    // Helpers
    // ========================================================================

    fn make_node_info(node_id: NodeId, status: NodeStatus) -> NodeInfo {
        NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits: std::collections::HashSet::from([Trait::CanExecute]),
            load: 0.2,
            capacity: ResourceSnapshot::default(),
            address: Some("127.0.0.1:4200".parse().expect("valid addr")),
            via: node_id,
            status,
            generation: 1,
            trust_level: Default::default(),
        }
    }

    fn make_knowledge_with_nodes(self_id: NodeId, extra_count: usize) -> Arc<KnowledgeStore> {
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        for _ in 0..extra_count {
            let nid = NodeId::new();
            knowledge.merge_node(make_node_info(nid, NodeStatus::Alive));
        }
        knowledge
    }

    fn make_fleet(_knowledge: &Arc<KnowledgeStore>) -> Arc<FleetManager> {
        Arc::new(FleetManager::new())
    }

    fn make_oai_config_4node() -> OaiConfig {
        let mut config = OaiConfig::default();
        config.impact.minimum_viable_nodes = 2;
        config.impact.max_blast_radius_pct = 25.0;
        config
    }

    struct TestEngineKit {
        engine: InterventionEngine,
        op_store: Arc<OperatorStore>,
    }

    fn make_intervention_engine(
        knowledge: &Arc<KnowledgeStore>,
        fleet: &Arc<FleetManager>,
        event_bus: &Arc<EventBus>,
        audit_log: &Arc<AuditLog>,
        oai_config: &OaiConfig,
    ) -> TestEngineKit {
        let guard_eval = Arc::new(GuardEvaluator::new(
            Arc::clone(knowledge),
            Arc::clone(fleet),
        ));
        let lock_mgr = Arc::new(LockManager::new());
        let impact = Arc::new(ImpactAssessor::new(
            Arc::clone(knowledge),
            Arc::clone(fleet),
            oai_config.impact.clone(),
        ));
        let rollback = Arc::new(RollbackManager::new(
            Duration::from_millis(oai_config.impact.post_condition_timeout_ms),
        ));
        let op_store = Arc::new(OperatorStore::new());
        let _veto = Arc::new(VetoChecker::new(None, None));

        let mut engine = InterventionEngine::new(
            oai_config.clone(),
            guard_eval,
            lock_mgr,
            impact,
            rollback,
            Arc::clone(&op_store),
            Arc::clone(audit_log),
            Arc::clone(event_bus),
        );

        engine.register_handler(Arc::new(Tier1Handler::new(Arc::clone(fleet))));
        engine.register_handler(Arc::new(Tier2Handler::new(Arc::clone(knowledge))));

        TestEngineKit { engine, op_store }
    }

    // ========================================================================
    // Feature-gate tests
    // ========================================================================

    #[test]
    fn test_oai_disabled_by_default() {
        let config = SwarmConfig::default();
        assert!(!config.enable_observe_and_interfere);
    }

    #[test]
    fn test_oai_config_defaults() {
        let config = OaiConfig::default();
        assert!(config.impact.minimum_viable_nodes > 0);
        assert!(config.impact.max_blast_radius_pct > 0.0);
        assert!(config.observation.max_subscriptions_per_operator > 0);
    }

    #[test]
    fn test_swarmnode_oai_disabled() {
        let config = SwarmConfig::default();
        let node = crate::swarm::SwarmNode::new(config).expect("node creation");
        assert!(node.oai_subscription_manager().is_none());
        assert!(node.oai_intervention_engine().is_none());
        assert!(node.oai_operator_store().is_none());
        assert!(node.oai_guard_evaluator().is_none());
        assert!(node.oai_lock_manager().is_none());
        assert!(node.oai_session_store().is_none());
        assert!(node.oai_veto_checker().is_none());
    }

    #[test]
    fn test_swarmnode_oai_enabled() {
        let mut config = SwarmConfig::default();
        config.enable_observe_and_interfere = true;
        config.observe_and_interfere = Some(OaiConfig::default());

        let node = crate::swarm::SwarmNode::new(config).expect("node creation");
        assert!(node.oai_subscription_manager().is_some());
        assert!(node.oai_intervention_engine().is_some());
        assert!(node.oai_operator_store().is_some());
        assert!(node.oai_guard_evaluator().is_some());
        assert!(node.oai_lock_manager().is_some());
        assert!(node.oai_session_store().is_some());
        assert!(node.oai_veto_checker().is_some());
    }

    // ========================================================================
    // Topology and views
    // ========================================================================

    #[test]
    fn test_observe_4node_topology() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge_with_nodes(self_id, 4);
        let fleet = make_fleet(&knowledge);

        let topology = views::build_topology(&knowledge, &fleet);

        // KnowledgeStore.get_all_nodes() returns merged nodes (self not tracked)
        assert_eq!(topology.total_nodes, 4);
        assert_eq!(topology.alive_nodes, 4);
        assert!(!topology.nodes.is_empty());
    }

    #[test]
    fn test_observe_topology_with_dead_nodes() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));

        // 2 alive, 1 dead
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();
        knowledge.merge_node(make_node_info(n1, NodeStatus::Alive));
        knowledge.merge_node(make_node_info(n2, NodeStatus::Alive));
        knowledge.merge_node(make_node_info(n3, NodeStatus::Dead));

        let fleet = make_fleet(&knowledge);
        let topology = views::build_topology(&knowledge, &fleet);

        // Self not in knowledge store, so total = 3 merged nodes
        assert_eq!(topology.total_nodes, 3);
        assert_eq!(topology.alive_nodes, 2);
    }

    #[test]
    fn test_observe_resource_heatmap() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge_with_nodes(self_id, 3);
        let heatmap = views::build_resource_heatmap(&knowledge);
        assert!(!heatmap.entries.is_empty());
    }

    // ========================================================================
    // Operator model
    // ========================================================================

    #[test]
    fn test_operator_registration_and_presets() {
        let store = OperatorStore::new();

        let op = store.register_from_preset(
            OperatorId::new("op-engineer"),
            "Test Engineer".to_string(),
            ProfilePreset::Engineer,
        );
        assert_eq!(op.id, OperatorId::new("op-engineer"));
        assert_eq!(op.name, "Test Engineer");

        let retrieved = store.get(&OperatorId::new("op-engineer"));
        assert!(retrieved.is_some());

        // Spectator should have minimal permissions
        let spectator = store.register_from_preset(
            OperatorId::new("op-spectator"),
            "Watcher".to_string(),
            ProfilePreset::Spectator,
        );
        assert!(spectator.check_tier_permission(InterventionTier::NodeLifecycle).is_err());

        // Engineer should be allowed tier 1-3
        assert!(op.check_tier_permission(InterventionTier::NodeLifecycle).is_ok());
        assert!(op.check_tier_permission(InterventionTier::WorkControl).is_ok());
        assert!(op.check_tier_permission(InterventionTier::Organic).is_ok());
    }

    #[test]
    fn test_operator_all_presets() {
        let store = OperatorStore::new();

        let presets = [
            ProfilePreset::Spectator,
            ProfilePreset::Researcher,
            ProfilePreset::Engineer,
            ProfilePreset::PlatformAdmin,
            ProfilePreset::SecurityOps,
            ProfilePreset::Unrestricted,
        ];

        for (i, preset) in presets.iter().enumerate() {
            let id = OperatorId::new(format!("op-{}", i));
            let op = store.register_from_preset(id.clone(), format!("Op {}", i), *preset);
            assert_eq!(op.id, id);
        }

        // All 6 registered
        assert_eq!(store.get(&OperatorId::new("op-0")).is_some(), true);
        assert_eq!(store.get(&OperatorId::new("op-5")).is_some(), true);
    }

    // ========================================================================
    // Guard evaluation
    // ========================================================================

    #[test]
    fn test_auto_guard_node_alive() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let target = NodeId::new();
        knowledge.merge_node(make_node_info(target, NodeStatus::Alive));

        let fleet = make_fleet(&knowledge);
        let guard_eval = GuardEvaluator::new(Arc::clone(&knowledge), fleet);

        let guard = guard_eval.auto_guard_node(&target);
        assert!(guard.is_some(), "auto_guard should succeed for alive node");

        // The guard should pass when evaluated immediately
        let guard = guard.expect("guard exists");
        let eval = guard_eval.evaluate(&guard);
        assert!(eval.passed, "guard should pass for alive node");
    }

    #[test]
    fn test_guard_violation_after_node_death() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let target = NodeId::new();
        knowledge.merge_node(make_node_info(target, NodeStatus::Alive));

        let fleet = make_fleet(&knowledge);
        let guard_eval = GuardEvaluator::new(Arc::clone(&knowledge), Arc::clone(&fleet));

        // Capture guard while alive
        let guard = guard_eval.auto_guard_node(&target).expect("guard created");

        // Kill the node
        let mut dead_info = make_node_info(target, NodeStatus::Dead);
        dead_info.generation = 2; // bump generation to ensure merge wins
        dead_info.last_seen = Utc::now();
        knowledge.merge_node(dead_info);

        // Guard should now fail
        let eval = guard_eval.evaluate(&guard);
        assert!(!eval.passed, "guard should fail after node death");
    }

    // ========================================================================
    // Lock management
    // ========================================================================

    #[test]
    fn test_lock_acquire_and_release() {
        let lock_mgr = LockManager::new();
        let entity = EntityRef {
            entity_type: EntityType::Node,
            id: NodeId::new().to_string(),
        };
        let intv_id = InterventionId::new();
        let op_id = OperatorId::new("op-1");

        let lock = lock_mgr.try_acquire(
            entity.clone(),
            intv_id.clone(),
            op_id,
            "drain".to_string(),
            Duration::from_secs(300),
        );
        assert!(lock.is_ok(), "first acquire should succeed");

        // Second acquire on same entity should fail
        let lock2 = lock_mgr.try_acquire(
            entity.clone(),
            InterventionId::new(),
            OperatorId::new("op-2"),
            "drain".to_string(),
            Duration::from_secs(300),
        );
        assert!(lock2.is_err(), "second acquire should conflict");

        // Release and re-acquire should work
        lock_mgr.release(&entity);
        let lock3 = lock_mgr.try_acquire(
            entity.clone(),
            InterventionId::new(),
            OperatorId::new("op-2"),
            "drain".to_string(),
            Duration::from_secs(300),
        );
        assert!(lock3.is_ok(), "acquire after release should succeed");
    }

    #[test]
    fn test_lock_purge_expired() {
        let lock_mgr = LockManager::new();
        let entity = EntityRef {
            entity_type: EntityType::Node,
            id: NodeId::new().to_string(),
        };

        // Acquire with 0-second TTL (already expired)
        let _ = lock_mgr.try_acquire(
            entity.clone(),
            InterventionId::new(),
            OperatorId::new("op"),
            "test".to_string(),
            Duration::from_secs(0),
        );

        // Give it a moment to expire
        std::thread::sleep(Duration::from_millis(10));

        let purged = lock_mgr.purge_expired();
        assert!(purged >= 1, "should purge at least 1 expired lock");

        // Entity should now be available
        let active = lock_mgr.list_active();
        assert!(
            !active.iter().any(|l| l.entity == entity),
            "expired lock should be gone"
        );
    }

    #[test]
    fn test_competing_interventions() {
        let lock_mgr = LockManager::new();
        let entity = EntityRef {
            entity_type: EntityType::Node,
            id: NodeId::new().to_string(),
        };

        // First operator locks
        let result1 = lock_mgr.try_acquire(
            entity.clone(),
            InterventionId::new(),
            OperatorId::new("op-1"),
            "drain".to_string(),
            Duration::from_secs(300),
        );
        assert!(result1.is_ok());

        // Second operator gets conflict
        let result2 = lock_mgr.try_acquire(
            entity.clone(),
            InterventionId::new(),
            OperatorId::new("op-2"),
            "cordon".to_string(),
            Duration::from_secs(300),
        );

        match result2 {
            Err(OaiError::InterventionConflict { .. }) => {} // expected
            other => panic!(
                "expected InterventionConflict, got {:?}",
                other.map(|_| "Ok")
            ),
        }
    }

    // ========================================================================
    // Impact assessment
    // ========================================================================

    #[test]
    fn test_impact_assessment_4nodes() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));

        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();
        knowledge.merge_node(make_node_info(n1, NodeStatus::Alive));
        knowledge.merge_node(make_node_info(n2, NodeStatus::Alive));
        knowledge.merge_node(make_node_info(n3, NodeStatus::Alive));

        let fleet = make_fleet(&knowledge);
        let config = make_oai_config_4node();
        let assessor = ImpactAssessor::new(
            Arc::clone(&knowledge),
            Arc::clone(&fleet),
            config.impact,
        );

        // Draining 1 of 4 = 25% blast radius (at boundary)
        let assessment = assessor.assess_node_intervention(&n1, "drain");
        assert!(assessment.total_entities >= 3); // registered nodes (may not count self)
    }

    #[test]
    fn test_impact_blast_radius_minimal() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let n1 = NodeId::new();
        knowledge.merge_node(make_node_info(n1, NodeStatus::Alive));

        let fleet = make_fleet(&knowledge);
        let config = make_oai_config_4node();
        let assessor = ImpactAssessor::new(knowledge, fleet, config.impact);

        // Only 2 nodes total (self + 1) → draining 1 is 50% blast radius
        let assessment = assessor.assess_node_intervention(&n1, "drain");
        assert!(
            assessment.blast_radius_pct > 20.0,
            "blast radius should be significant with few nodes"
        );
    }

    // ========================================================================
    // Session lifecycle
    // ========================================================================

    #[test]
    fn test_incident_session_lifecycle() {
        let store = SessionStore::new();

        // Create
        let session = store.create(
            OperatorId::new("test-op"),
            "Node failure investigation".to_string(),
            Some("Node-2 showing anomalies".to_string()),
        );
        assert!(session.ended_at.is_none());
        assert_eq!(session.intervention_count, 0);
        assert_eq!(session.operator, OperatorId::new("test-op"));

        // Increment
        store.increment_intervention_count(&session.id);
        store.increment_intervention_count(&session.id);
        let updated = store.get(&session.id).expect("session exists");
        assert_eq!(updated.intervention_count, 2);

        // List active
        let active = store.list_active();
        assert_eq!(active.len(), 1);

        // End
        let ended = store.end_session(&session.id).expect("session ended");
        assert!(ended.ended_at.is_some());

        // List active should be empty now
        let active = store.list_active();
        assert_eq!(active.len(), 0);

        // Still in full list
        let all = store.list();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_multiple_sessions() {
        let store = SessionStore::new();

        let s1 = store.create(
            OperatorId::new("op-1"),
            "Session 1".to_string(),
            None,
        );
        let s2 = store.create(
            OperatorId::new("op-2"),
            "Session 2".to_string(),
            None,
        );

        assert_eq!(store.list_active().len(), 2);

        store.end_session(&s1.id);
        assert_eq!(store.list_active().len(), 1);
        assert_eq!(store.list().len(), 2);

        store.end_session(&s2.id);
        assert_eq!(store.list_active().len(), 0);
    }

    // ========================================================================
    // Subscription management
    // ========================================================================

    #[tokio::test]
    async fn test_subscription_create_and_delete() {
        let event_bus = Arc::new(EventBus::new(1024));
        let config = ObservationConfig::default();
        let mgr = SubscriptionManager::new(event_bus, config);

        let filter = crate::swarm::events::EventFilter::default();
        let handle = mgr.create(OperatorId::new("test"), filter);
        assert!(handle.is_ok(), "subscription creation should succeed");

        let handle = handle.expect("handle");
        let sub_id = handle.id.clone();

        // Stats should be available
        let stats = mgr.stats(&sub_id);
        assert!(stats.is_some());

        // Count should be 1
        assert_eq!(mgr.count(), 1);

        // Delete
        assert!(mgr.delete(&sub_id));
        assert_eq!(mgr.count(), 0);
    }

    #[tokio::test]
    async fn test_subscription_list() {
        let event_bus = Arc::new(EventBus::new(1024));
        let config = ObservationConfig::default();
        let mgr = SubscriptionManager::new(event_bus, config);

        let _h1 = mgr
            .create(
                OperatorId::new("op-1"),
                crate::swarm::events::EventFilter::default(),
            )
            .expect("sub 1");
        let _h2 = mgr
            .create(
                OperatorId::new("op-2"),
                crate::swarm::events::EventFilter::default(),
            )
            .expect("sub 2");

        let list = mgr.list();
        assert_eq!(list.len(), 2);
    }

    // ========================================================================
    // Rollback manager
    // ========================================================================

    #[test]
    fn test_rollback_manager_creation() {
        let mgr = RollbackManager::new(Duration::from_secs(30));
        // Rollback manager should be created without error
        // (its methods are exercised through intervention engine)
        drop(mgr);
    }

    // ========================================================================
    // Veto checker
    // ========================================================================

    #[test]
    fn test_veto_checker_no_neuromancer() {
        let checker = VetoChecker::new(None, None);
        // Without neuromancer subsystems, veto should not trigger
        let node_id = NodeId::new();
        let result = checker.check_node_intervention(&node_id, "drain");
        assert!(result.is_ok(), "veto should pass without neuromancer");
    }

    // ========================================================================
    // Full intervention pipeline
    // ========================================================================

    #[test]
    fn test_intervention_engine_dry_run() {
        let self_id = NodeId::new();
        let knowledge = make_knowledge_with_nodes(self_id, 3);
        let fleet = make_fleet(&knowledge);
        let event_bus = Arc::new(EventBus::new(1024));
        let audit_log = Arc::new(AuditLog::new());
        let oai_config = make_oai_config_4node();

        let kit = make_intervention_engine(
            &knowledge, &fleet, &event_bus, &audit_log, &oai_config,
        );

        // Register operator in the engine's own operator store
        kit.op_store.register_from_preset(
            OperatorId::new("test-op"),
            "Test".to_string(),
            ProfilePreset::Engineer,
        );

        // Pick a known node for the target
        let target_id = knowledge
            .get_live_nodes()
            .into_iter()
            .find(|n| n.node_id != self_id)
            .expect("at least one other node")
            .node_id;

        let request = InterventionRequest {
            id: InterventionId::new(),
            action: InterventionAction {
                tier: InterventionTier::NodeLifecycle,
                name: "drain".to_string(),
            },
            target: EntityRef {
                entity_type: EntityType::Node,
                id: target_id.to_string(),
            },
            guard: None,
            urgency: InterventionUrgency::Operational,
            operator_id: OperatorId::new("test-op"),
            session_id: None,
            skip_guard: true,
            force: false,
            dry_run: true,
            params: serde_json::json!({"timeout_secs": 60, "reason": "test"}),
        };

        let result = kit.engine.execute(request);
        assert!(result.is_ok(), "dry-run should succeed: {:?}", result.err());

        let result = result.expect("result");
        // In dry-run mode, executed should be false
        assert!(!result.executed, "dry-run should not actually execute");
    }

    // ========================================================================
    // User journey: quant researcher fixing failed chunks
    // ========================================================================

    #[test]
    fn test_user_journey_session_and_observation() {
        // 1. Researcher creates an incident session
        let session_store = SessionStore::new();
        let session = session_store.create(
            OperatorId::new("researcher-1"),
            "Fixing failed chunks".to_string(),
            Some("2 of 20 chunks failed in job-42".to_string()),
        );
        assert!(session.ended_at.is_none());

        // 2. Researcher observes topology (4 nodes in knowledge store)
        let self_id = NodeId::new();
        let knowledge = make_knowledge_with_nodes(self_id, 4);
        let fleet = make_fleet(&knowledge);
        let topology = views::build_topology(&knowledge, &fleet);
        assert_eq!(topology.total_nodes, 4);

        // 3. Researcher observes resource heatmap
        let heatmap = views::build_resource_heatmap(&knowledge);
        assert!(!heatmap.entries.is_empty());

        // 4. Track interventions via session
        session_store.increment_intervention_count(&session.id);
        session_store.increment_intervention_count(&session.id);

        // 5. End session
        let ended = session_store.end_session(&session.id).expect("ended");
        assert!(ended.ended_at.is_some());
        assert_eq!(ended.intervention_count, 2);
    }

    // ========================================================================
    // Config boundary tests (maxNodes=4)
    // ========================================================================

    #[test]
    fn test_4node_config_boundaries() {
        let config = make_oai_config_4node();
        assert_eq!(config.impact.minimum_viable_nodes, 2);
        assert_eq!(config.impact.max_blast_radius_pct, 25.0);
    }

    #[test]
    fn test_observation_config_defaults() {
        let config = ObservationConfig::default();
        assert!(config.max_subscriptions_per_operator > 0);
        assert!(config.subscription_buffer_size > 0);
    }

    // ========================================================================
    // Error taxonomy
    // ========================================================================

    #[test]
    fn test_error_exit_codes() {
        use crate::swarm::observe::errors::{
            EXIT_CANCELLED, EXIT_CONFLICT, EXIT_ERROR, EXIT_GUARD_FAILED, EXIT_OK,
        };

        assert_eq!(EXIT_OK, 0);
        assert_eq!(EXIT_ERROR, 1);
        assert_eq!(EXIT_GUARD_FAILED, 2);
        assert_eq!(EXIT_CANCELLED, 3);
        assert_eq!(EXIT_CONFLICT, 4);
    }

    #[test]
    fn test_error_http_status_mapping() {
        let sample_entity = EntityRef {
            entity_type: EntityType::Node,
            id: "node-1".to_string(),
        };

        let err = OaiError::GuardViolated {
            entity: sample_entity.clone(),
            field: "status".to_string(),
            expected: serde_json::json!("Alive"),
            actual: serde_json::json!("Dead"),
            drift_ms: 500,
        };
        assert_eq!(err.http_status(), 409);
        assert_eq!(err.code(), "GUARD_VIOLATED");
        assert_eq!(err.exit_code(), 2);

        let err = OaiError::InterventionConflict {
            entity: sample_entity,
            existing_intervention: InterventionId::new(),
            existing_operator: OperatorId::new("op-2"),
            existing_action: "drain".to_string(),
            started_at: "2024-01-01T00:00:00Z".to_string(),
        };
        assert_eq!(err.http_status(), 409);
        assert_eq!(err.code(), "INTERVENTION_CONFLICT");
        assert_eq!(err.exit_code(), 4);
    }

    // ========================================================================
    // End-to-end wiring (SwarmNode + accessors)
    // ========================================================================

    #[test]
    fn test_swarmnode_oai_accessors_wired() {
        let mut config = SwarmConfig::default();
        config.enable_observe_and_interfere = true;
        config.observe_and_interfere = Some(make_oai_config_4node());

        let node = crate::swarm::SwarmNode::new(config).expect("node creation");

        // All 9 OAI subsystems should be present
        assert!(node.oai_subscription_manager().is_some());
        assert!(node.oai_operator_store().is_some());
        assert!(node.oai_guard_evaluator().is_some());
        assert!(node.oai_lock_manager().is_some());
        assert!(node.oai_impact_assessor().is_some());
        assert!(node.oai_rollback_manager().is_some());
        assert!(node.oai_intervention_engine().is_some());
        assert!(node.oai_session_store().is_some());
        assert!(node.oai_veto_checker().is_some());

        // Non-OAI accessors should still work
        let _knowledge = node.knowledge();
        let _fleet = node.fleet();
        let _event_bus = node.event_bus();
    }
}
