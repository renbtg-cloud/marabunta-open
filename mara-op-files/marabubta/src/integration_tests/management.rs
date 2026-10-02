// Marabunta - Licensed under the MIT License.
//! Integration tests for the management layer.
//!
//! Tests EventBus, AlertEngine, SwarmMetrics, SLA monitoring, FleetManager,
//! HealthCheckEngine, Membrane, Agreement, Constellation, Capacity, and Audit
//! subsystems in isolation (no full SwarmNode required).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use uuid::Uuid;

use crate::swarm::alerting::{
    AlertCondition, AlertContext, AlertEngine, AlertRule, AlertRuleStore, AlertState,
    SilenceWindow,
};
use crate::swarm::audit::{
    AuditActor, AuditEntry, AuditFilter, AuditLog, AuditOutcome, AuditTarget,
};
use crate::swarm::capacity::{
    BottleneckSeverity, CapacityDataPoint, CapacityPlanner, WhatIfScenario,
};
use crate::swarm::complexity::{
    ConcernDomain, ComplexityHint, ComplexityStyle, EventSeverity, ManagedAction,
};
use crate::swarm::events::{EntityRef, EntityType, EventBus, EventFilter, SwarmEvent};
use crate::swarm::fleet::{
    FleetManager, FleetNodeState, FleetStore, RollingUpdatePlan, UpdatePhase,
};
use crate::swarm::healthcheck::{
    HealthProbe, HealthState, ProbeHealth, ProbeResult, ProbeTarget, ProbeType,
};
use crate::swarm::membrane::{
    CrossingDirection, DataCategory, MembraneId, RateLimitAlgorithm, RateLimitConfig,
    SwarmId as MembraneSwarmId, TransformRule,
};
use crate::swarm::metrics::{MetricsSnapshot, SwarmMetrics};
use crate::swarm::sla::{SlaDefinition, SlaMetric, SlaWindow};
use crate::swarm::agreement::{
    BillingModel, ConstellationBuilder, CrossingLog, LendingBilling, LendingMeter,
    LendingPolicy, NegotiationAction, PsycheFacets, SwarmId as AgreementSwarmId,
    SwarmSummary, Agreement, AgreementId, AgreementNegotiation, AgreementStatus, AgreementStore,
    new_draft_agreement,
};
use crate::swarm::types::NodeId;

// ============================================================================
// Helper functions
// ============================================================================

/// Create a fresh EventBus with a reasonable buffer.
fn create_test_event_bus() -> EventBus {
    EventBus::new(1024)
}

/// Create a test NodeId from a u64.
fn make_node_id(n: u64) -> NodeId {
    // NodeId is typically a Uuid-based type. We create a deterministic one.
    NodeId(Uuid::from_u128(n as u128))
}

/// Create a simple SwarmEvent for testing.
fn make_test_event(
    domain: ConcernDomain,
    severity: EventSeverity,
    summary: &str,
) -> SwarmEvent {
    SwarmEvent {
        id: 0,
        timestamp: Utc::now(),
        domain,
        severity,
        complexity: ComplexityHint::Simple,
        summary: summary.to_string(),
        details: serde_json::Value::Null,
        related_entities: Vec::new(),
        suggested_actions: Vec::new(),
        source_node: None,
        correlation_id: None,
        supersedes: None,
    }
}

/// Create an AuditActor for testing.
fn make_test_actor(name: &str) -> AuditActor {
    AuditActor {
        actor_type: "node".to_string(),
        id: name.to_string(),
        display_name: None,
    }
}

/// Create an AuditTarget for testing.
fn make_test_target(target_type: &str, id: &str) -> AuditTarget {
    AuditTarget {
        target_type: target_type.to_string(),
        id: id.to_string(),
        display_name: None,
    }
}

/// Create a CapacityDataPoint with controlled values.
fn make_capacity_point(
    cpu_total: u32,
    cpu_used: f64,
    mem_total: u64,
    mem_used: u64,
    disk_total: u64,
    disk_used: u64,
    alive: usize,
) -> CapacityDataPoint {
    CapacityDataPoint {
        timestamp: Utc::now(),
        total_cpu: cpu_total,
        used_cpu: cpu_used,
        total_memory: mem_total,
        used_memory: mem_used,
        total_disk: disk_total,
        used_disk: disk_used,
        total_bandwidth: 1000.0,
        used_bandwidth: 200.0,
        alive_nodes: alive,
        total_nodes: alive + 1,
        active_jobs: 5,
        pending_chunks: 10,
    }
}

// ============================================================================
// EventBus tests (1-6)
// ============================================================================

#[tokio::test]
async fn test_01_event_bus_emit_subscribe_roundtrip() {
    let bus = create_test_event_bus();
    let mut rx = bus.subscribe();

    let event = make_test_event(ConcernDomain::Health, EventSeverity::Info, "Node joined");
    let id = bus.emit(event);

    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("should receive within 1s")
        .expect("should not be a recv error");

    assert_eq!(received.id, id);
    assert_eq!(received.domain, ConcernDomain::Health);
    assert_eq!(received.severity, EventSeverity::Info);
    assert_eq!(received.summary, "Node joined");
}

#[tokio::test]
async fn test_02_event_bus_filtered_query_by_domain_and_severity() {
    let bus = create_test_event_bus();

    // Emit events across different domains and severities
    bus.emit(make_test_event(ConcernDomain::Health, EventSeverity::Info, "health info"));
    bus.emit(make_test_event(ConcernDomain::Health, EventSeverity::Error, "health error"));
    bus.emit(make_test_event(ConcernDomain::Work, EventSeverity::Warning, "work warning"));
    bus.emit(make_test_event(ConcernDomain::Security, EventSeverity::Critical, "sec critical"));

    // Filter: Health domain, severity >= Warning
    let filter = EventFilter {
        domains: Some(vec![ConcernDomain::Health]),
        min_severity: Some(EventSeverity::Warning),
        ..Default::default()
    };

    let results = bus.recent_filtered(&filter, 100);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].summary, "health error");
}

#[tokio::test]
async fn test_03_event_bus_ring_buffer_overflow() {
    // The ring buffer maximum is 10,000. Emit 15,000 and verify only last 10,000 remain.
    let bus = create_test_event_bus();

    for i in 0..15_000u64 {
        bus.emit_simple(
            ConcernDomain::Health,
            EventSeverity::Info,
            format!("event-{}", i),
        );
    }

    let all = bus.recent_events(20_000);
    assert_eq!(all.len(), 10_000);

    // The oldest event in the buffer should be event-5000
    assert_eq!(all[0].summary, "event-5000");
    // The newest should be event-14999
    assert_eq!(all[9999].summary, "event-14999");
}

#[tokio::test]
async fn test_04_event_bus_concurrent_emit() {
    let bus = Arc::new(create_test_event_bus());
    let mut handles = Vec::new();

    for thread_id in 0..10u32 {
        let bus_clone = Arc::clone(&bus);
        handles.push(tokio::spawn(async move {
            for i in 0..100u32 {
                bus_clone.emit_simple(
                    ConcernDomain::Work,
                    EventSeverity::Info,
                    format!("t{}-e{}", thread_id, i),
                );
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let stats = bus.stats();
    assert_eq!(stats.total_emitted, 1000);
    assert_eq!(stats.buffer_size, 1000);
}

#[tokio::test]
async fn test_05_event_bus_stats_increment() {
    let bus = create_test_event_bus();

    bus.emit_simple(ConcernDomain::Health, EventSeverity::Info, "a");
    bus.emit_simple(ConcernDomain::Health, EventSeverity::Warning, "b");
    bus.emit_simple(ConcernDomain::Work, EventSeverity::Error, "c");

    let stats = bus.stats();
    assert_eq!(stats.total_emitted, 3);
    assert_eq!(stats.buffer_size, 3);
    assert_eq!(stats.buffer_capacity, 10_000);
    assert_eq!(*stats.per_domain.get("health").unwrap(), 2);
    assert_eq!(*stats.per_domain.get("work").unwrap(), 1);
    assert_eq!(*stats.per_severity.get("info").unwrap(), 1);
    assert_eq!(*stats.per_severity.get("warning").unwrap(), 1);
    assert_eq!(*stats.per_severity.get("error").unwrap(), 1);
}

#[tokio::test]
async fn test_06_event_bus_sse_format() {
    let bus = create_test_event_bus();

    let event = make_test_event(ConcernDomain::Psyche, EventSeverity::Notice, "archetype shift");
    bus.emit(event);

    let events = bus.recent_events(1);
    assert_eq!(events.len(), 1);

    let sse = events[0].to_sse_data();
    // SSE data should be valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&sse)
        .expect("SSE data should be valid JSON");
    assert_eq!(parsed["summary"], "archetype shift");
    assert_eq!(parsed["domain"], "psyche");
}

// ============================================================================
// AlertEngine tests (7-12)
// ============================================================================

#[tokio::test]
async fn test_07_alert_engine_rule_fires_immediately() {
    let store = Arc::new(AlertRuleStore::new());
    // Add a custom rule with no for_duration
    store.add_rule(
        AlertRule::new(
            "test-high-cpu",
            "CPU is too high",
            AlertCondition::MetricAbove {
                name: "cpu_usage".to_string(),
                threshold: 0.8,
                for_duration: 0,
            },
            EventSeverity::Error,
            ConcernDomain::Health,
        ),
    );

    let engine = AlertEngine::new(store);

    let mut ctx = AlertContext::new();
    ctx.set_metric("cpu_usage", 0.95);
    engine.evaluate_all(&ctx);

    let alerts = engine.active_alerts();
    let test_alert = alerts.iter().find(|a| a.rule_name == "test-high-cpu");
    assert!(test_alert.is_some(), "alert should have fired");
    assert_eq!(test_alert.unwrap().state, AlertState::Firing);
    assert_eq!(test_alert.unwrap().fire_count, 1);
}

#[tokio::test]
async fn test_08_alert_engine_rule_resolves_when_condition_clears() {
    let store = Arc::new(AlertRuleStore::new());
    store.add_rule(
        AlertRule::new(
            "test-high-mem",
            "Memory is too high",
            AlertCondition::MetricAbove {
                name: "mem_usage".to_string(),
                threshold: 0.9,
                for_duration: 0,
            },
            EventSeverity::Warning,
            ConcernDomain::Health,
        ),
    );

    let engine = AlertEngine::new(store);

    // Fire the alert
    let mut ctx = AlertContext::new();
    ctx.set_metric("mem_usage", 0.95);
    engine.evaluate_all(&ctx);

    let alerts = engine.active_alerts();
    assert_eq!(alerts.len(), 1);

    // Clear the condition
    let mut ctx2 = AlertContext::new();
    ctx2.set_metric("mem_usage", 0.5);
    engine.evaluate_all(&ctx2);

    // The alert should be resolved
    let all = engine.all_alerts();
    let resolved = all.iter().find(|a| a.rule_name == "test-high-mem");
    assert!(resolved.is_some());
    assert_eq!(resolved.unwrap().state, AlertState::Resolved);
}

#[tokio::test]
async fn test_09_alert_engine_inhibition_suppresses_child() {
    let store = Arc::new(AlertRuleStore::new());

    // Parent rule: fires when "parent_metric" > 0.5
    store.add_rule(
        AlertRule::new(
            "parent-alert",
            "Parent alert",
            AlertCondition::MetricAbove {
                name: "parent_metric".to_string(),
                threshold: 0.5,
                for_duration: 0,
            },
            EventSeverity::Critical,
            ConcernDomain::Health,
        ),
    );

    // Child rule: inhibited by parent
    store.add_rule(
        AlertRule::new(
            "child-alert",
            "Child alert",
            AlertCondition::MetricAbove {
                name: "child_metric".to_string(),
                threshold: 0.5,
                for_duration: 0,
            },
            EventSeverity::Warning,
            ConcernDomain::Health,
        )
        .inhibited_by_rule("parent-alert"),
    );

    let engine = AlertEngine::new(store);

    // Both conditions are true
    let mut ctx = AlertContext::new();
    ctx.set_metric("parent_metric", 0.9);
    ctx.set_metric("child_metric", 0.9);
    engine.evaluate_all(&ctx);

    let alerts = engine.active_alerts();
    // Parent should fire, child should be inhibited
    let parent = alerts.iter().find(|a| a.rule_name == "parent-alert");
    let child = alerts.iter().find(|a| a.rule_name == "child-alert");

    assert!(parent.is_some(), "parent alert should fire");
    assert!(child.is_none(), "child alert should be inhibited");
}

#[tokio::test]
async fn test_10_alert_engine_silence_window() {
    let store = Arc::new(AlertRuleStore::new());
    store.add_rule(
        AlertRule::new(
            "test-silenced",
            "This alert will be silenced",
            AlertCondition::MetricAbove {
                name: "some_metric".to_string(),
                threshold: 0.5,
                for_duration: 0,
            },
            EventSeverity::Warning,
            ConcernDomain::Health,
        )
        .with_label("team", "ops"),
    );

    let engine = AlertEngine::new(store);

    // Add a silence window matching the label
    let mut matchers = HashMap::new();
    matchers.insert("team".to_string(), "ops".to_string());
    engine.add_silence_window(SilenceWindow {
        id: "silence-1".to_string(),
        matchers,
        starts_at: Utc::now() - ChronoDuration::hours(1),
        ends_at: Utc::now() + ChronoDuration::hours(1),
        created_by: "admin".to_string(),
        reason: "maintenance".to_string(),
    });

    // Trigger the alert
    let mut ctx = AlertContext::new();
    ctx.set_metric("some_metric", 0.9);
    engine.evaluate_all(&ctx);

    let all = engine.all_alerts();
    let alert = all.iter().find(|a| a.rule_name == "test-silenced");
    assert!(alert.is_some());
    assert_eq!(alert.unwrap().state, AlertState::Silenced);
}

#[tokio::test]
async fn test_11_alert_engine_acknowledge() {
    let store = Arc::new(AlertRuleStore::new());
    store.add_rule(
        AlertRule::new(
            "test-ack",
            "Acknowledge test",
            AlertCondition::MetricAbove {
                name: "ack_metric".to_string(),
                threshold: 0.1,
                for_duration: 0,
            },
            EventSeverity::Error,
            ConcernDomain::Work,
        ),
    );

    let engine = AlertEngine::new(store);

    let mut ctx = AlertContext::new();
    ctx.set_metric("ack_metric", 0.5);
    engine.evaluate_all(&ctx);

    engine.acknowledge_alert("test-ack", "oncall-engineer");

    let all = engine.all_alerts();
    let alert = all.iter().find(|a| a.rule_name == "test-ack").unwrap();
    assert_eq!(alert.state, AlertState::Acknowledged);
    assert_eq!(alert.acknowledged_by.as_deref(), Some("oncall-engineer"));
}

#[tokio::test]
async fn test_12_builtin_rules_load_correctly() {
    let store = AlertRuleStore::new();

    // The store should have 15 built-in rules
    let rules = store.all_rules();
    assert!(rules.len() >= 15, "should have at least 15 built-in rules, got {}", rules.len());

    // Verify a few specific built-in rules exist
    assert!(store.get_rule("node-death-storm").is_some());
    assert!(store.get_rule("partition-detected").is_some());
    assert!(store.get_rule("gossip-stall").is_some());
    assert!(store.get_rule("disk-pressure").is_some());
    assert!(store.get_rule("psyche-war-room").is_some());
    assert!(store.get_rule("capacity-exhaustion").is_some());

    // Verify all built-in rules are enabled
    for rule in &rules {
        if rule.is_builtin {
            assert!(rule.enabled, "built-in rule '{}' should be enabled", rule.name);
        }
    }

    // Verify condition validation passes for all rules
    for rule in &rules {
        assert!(rule.condition.validate().is_ok(), "rule '{}' has invalid condition", rule.name);
    }
}

// ============================================================================
// AlertCondition unit tests (13-16)
// ============================================================================

#[tokio::test]
async fn test_13_alert_condition_metric_above() {
    let ctx = {
        let mut c = AlertContext::new();
        c.set_metric("cpu", 0.9);
        c
    };

    let cond = AlertCondition::MetricAbove {
        name: "cpu".to_string(),
        threshold: 0.8,
        for_duration: 0,
    };
    assert!(cond.evaluate(&ctx));

    let cond_below = AlertCondition::MetricAbove {
        name: "cpu".to_string(),
        threshold: 0.95,
        for_duration: 0,
    };
    assert!(!cond_below.evaluate(&ctx));
}

#[tokio::test]
async fn test_14_alert_condition_facet_below() {
    let ctx = {
        let mut c = AlertContext::new();
        c.set_facet("resilience", 20);
        c
    };

    let cond = AlertCondition::FacetBelow {
        facet: "resilience".to_string(),
        level: 30,
        for_duration: 0,
    };
    assert!(cond.evaluate(&ctx));

    let cond_above = AlertCondition::FacetBelow {
        facet: "resilience".to_string(),
        level: 10,
        for_duration: 0,
    };
    assert!(!cond_above.evaluate(&ctx));
}

#[tokio::test]
async fn test_15_alert_condition_archetype_active() {
    let ctx = {
        let mut c = AlertContext::new();
        c.add_archetype("war-room");
        c
    };

    let cond = AlertCondition::ArchetypeActive {
        name: "war-room".to_string(),
    };
    assert!(cond.evaluate(&ctx));

    let cond_inactive = AlertCondition::ArchetypeActive {
        name: "cruise-control".to_string(),
    };
    assert!(!cond_inactive.evaluate(&ctx));
}

#[tokio::test]
async fn test_16_alert_condition_composite_all_any_not() {
    let ctx = {
        let mut c = AlertContext::new();
        c.set_metric("cpu", 0.9);
        c.set_metric("mem", 0.3);
        c
    };

    // All: cpu > 0.8 AND mem > 0.5 -> false (mem is 0.3)
    let all_cond = AlertCondition::All(vec![
        AlertCondition::MetricAbove {
            name: "cpu".to_string(),
            threshold: 0.8,
            for_duration: 0,
        },
        AlertCondition::MetricAbove {
            name: "mem".to_string(),
            threshold: 0.5,
            for_duration: 0,
        },
    ]);
    assert!(!all_cond.evaluate(&ctx));

    // Any: cpu > 0.8 OR mem > 0.5 -> true (cpu is 0.9)
    let any_cond = AlertCondition::Any(vec![
        AlertCondition::MetricAbove {
            name: "cpu".to_string(),
            threshold: 0.8,
            for_duration: 0,
        },
        AlertCondition::MetricAbove {
            name: "mem".to_string(),
            threshold: 0.5,
            for_duration: 0,
        },
    ]);
    assert!(any_cond.evaluate(&ctx));

    // Not: NOT(cpu > 0.95) -> true (cpu is 0.9)
    let not_cond = AlertCondition::Not(Box::new(AlertCondition::MetricAbove {
        name: "cpu".to_string(),
        threshold: 0.95,
        for_duration: 0,
    }));
    assert!(not_cond.evaluate(&ctx));
}

// ============================================================================
// EventFilter tests (17-19)
// ============================================================================

#[tokio::test]
async fn test_17_event_filter_keyword_match() {
    let bus = create_test_event_bus();

    bus.emit(make_test_event(ConcernDomain::Health, EventSeverity::Info, "Node alpha joined"));
    bus.emit(make_test_event(ConcernDomain::Health, EventSeverity::Info, "Node beta left"));
    bus.emit(make_test_event(ConcernDomain::Work, EventSeverity::Info, "Job completed"));

    let filter = EventFilter {
        keywords: Some(vec!["alpha".to_string()]),
        ..Default::default()
    };

    let results = bus.recent_filtered(&filter, 100);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].summary, "Node alpha joined");
}

#[tokio::test]
async fn test_18_event_filter_entity_type() {
    let bus = create_test_event_bus();

    let node_id = make_node_id(1);
    let mut event_with_entity = make_test_event(ConcernDomain::Health, EventSeverity::Warning, "node suspect");
    event_with_entity.related_entities.push(EntityRef::node(&node_id));
    bus.emit(event_with_entity);

    bus.emit(make_test_event(ConcernDomain::Work, EventSeverity::Info, "job submitted"));

    let filter = EventFilter {
        entity_types: Some(vec![EntityType::Node]),
        ..Default::default()
    };

    let results = bus.recent_filtered(&filter, 100);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].summary, "node suspect");
}

#[tokio::test]
async fn test_19_event_filter_exclude_keywords() {
    let bus = create_test_event_bus();

    bus.emit(make_test_event(ConcernDomain::Health, EventSeverity::Info, "normal heartbeat"));
    bus.emit(make_test_event(ConcernDomain::Health, EventSeverity::Info, "node joined"));
    bus.emit(make_test_event(ConcernDomain::Health, EventSeverity::Info, "normal status"));

    let filter = EventFilter {
        exclude_keywords: Some(vec!["normal".to_string()]),
        ..Default::default()
    };

    let results = bus.recent_filtered(&filter, 100);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].summary, "node joined");
}

// ============================================================================
// AlertEngine advanced tests (20-22)
// ============================================================================

#[tokio::test]
async fn test_20_alert_engine_summary() {
    let store = Arc::new(AlertRuleStore::new());
    store.add_rule(
        AlertRule::new(
            "summary-test-1",
            "First alert",
            AlertCondition::MetricAbove {
                name: "m1".to_string(),
                threshold: 0.1,
                for_duration: 0,
            },
            EventSeverity::Error,
            ConcernDomain::Health,
        ),
    );
    store.add_rule(
        AlertRule::new(
            "summary-test-2",
            "Second alert",
            AlertCondition::MetricAbove {
                name: "m2".to_string(),
                threshold: 0.1,
                for_duration: 0,
            },
            EventSeverity::Warning,
            ConcernDomain::Work,
        ),
    );

    let engine = AlertEngine::new(store);

    let mut ctx = AlertContext::new();
    ctx.set_metric("m1", 0.5);
    ctx.set_metric("m2", 0.5);
    engine.evaluate_all(&ctx);

    let summary = engine.alert_summary();
    assert_eq!(summary.total_active, 2);
    assert_eq!(*summary.by_severity.get("error").unwrap_or(&0), 1);
    assert_eq!(*summary.by_severity.get("warning").unwrap_or(&0), 1);
    assert_eq!(*summary.by_domain.get("health").unwrap_or(&0), 1);
    assert_eq!(*summary.by_domain.get("work").unwrap_or(&0), 1);
}

#[tokio::test]
async fn test_21_alert_engine_resolve_moves_to_history() {
    let store = Arc::new(AlertRuleStore::new());
    store.add_rule(
        AlertRule::new(
            "history-test",
            "Will be resolved",
            AlertCondition::MetricAbove {
                name: "ht".to_string(),
                threshold: 0.1,
                for_duration: 0,
            },
            EventSeverity::Warning,
            ConcernDomain::Health,
        ),
    );

    let engine = AlertEngine::new(store);

    let mut ctx = AlertContext::new();
    ctx.set_metric("ht", 0.5);
    engine.evaluate_all(&ctx);

    assert_eq!(engine.active_alerts().len(), 1);

    engine.resolve_alert("history-test");

    // After explicit resolve, alert should be moved to history
    assert_eq!(engine.active_alerts().len(), 0);
    let history = engine.alert_history();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].rule_name, "history-test");
}

#[tokio::test]
async fn test_22_alert_engine_event_rate_condition() {
    let store = Arc::new(AlertRuleStore::new());
    store.add_rule(
        AlertRule::new(
            "rate-test",
            "Event rate too high",
            AlertCondition::EventRate {
                domain: "health".to_string(),
                severity: "error".to_string(),
                rate_per_min: 5.0,
                window_secs: 60,
            },
            EventSeverity::Error,
            ConcernDomain::Health,
        ),
    );

    let engine = AlertEngine::new(store);

    let mut ctx = AlertContext::new();
    ctx.set_event_rate("health:error", 10.0);
    engine.evaluate_all(&ctx);

    let alerts = engine.active_alerts();
    let alert = alerts.iter().find(|a| a.rule_name == "rate-test");
    assert!(alert.is_some(), "event rate alert should fire");
    assert_eq!(alert.unwrap().state, AlertState::Firing);
}

// ============================================================================
// FleetStore tests (23-26)
// ============================================================================

#[tokio::test]
async fn test_23_fleet_store_drain_lifecycle() {
    let store = FleetStore::new();
    let node = make_node_id(100);

    // Initially normal
    assert!(matches!(store.get_state(&node), FleetNodeState::Normal));

    // Set to draining
    store.set_state(
        node,
        FleetNodeState::Draining {
            started_at: Utc::now(),
            timeout_secs: 300,
            active_chunks_at_start: 5,
            reason: "maintenance".to_string(),
        },
        Some("admin".to_string()),
    );

    assert!(matches!(store.get_state(&node), FleetNodeState::Draining { .. }));
    assert!(!store.is_node_accepting_work(&node));
    assert!(store.is_node_gossiping(&node));

    // Transition to cordoned
    store.set_state(
        node,
        FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: "drain complete".to_string(),
        },
        Some("admin".to_string()),
    );

    assert!(matches!(store.get_state(&node), FleetNodeState::Cordoned { .. }));
    assert!(!store.is_node_accepting_work(&node));

    // Check transition history
    let info = store.get_info(&node).expect("should have fleet info");
    assert_eq!(info.transitions.len(), 2);
    assert_eq!(info.transitions[0].to_state, "cordoned");
    assert_eq!(info.transitions[1].to_state, "draining");
}

#[tokio::test]
async fn test_24_fleet_store_cordon_uncordon() {
    let store = FleetStore::new();
    let node = make_node_id(101);

    store.set_state(
        node,
        FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: "manual cordon".to_string(),
        },
        None,
    );

    assert!(!store.is_node_accepting_work(&node));

    // Uncordon (return to Normal)
    store.set_state(node, FleetNodeState::Normal, Some("admin".to_string()));

    assert!(store.is_node_accepting_work(&node));

    let info = store.get_info(&node).unwrap();
    assert_eq!(info.transitions.len(), 2);
    assert_eq!(info.transitions[0].to_state, "normal");
}

#[tokio::test]
async fn test_25_fleet_store_quarantine_blocks_gossip() {
    let store = FleetStore::new();
    let node = make_node_id(102);

    store.set_state(
        node,
        FleetNodeState::Quarantined {
            since: Utc::now(),
            reason: "misbehaving".to_string(),
            quarantined_by: Some("health-check".to_string()),
        },
        Some("health-check".to_string()),
    );

    assert!(!store.is_node_accepting_work(&node));
    assert!(!store.is_node_gossiping(&node));
}

#[tokio::test]
async fn test_26_fleet_store_count_by_state() {
    let store = FleetStore::new();

    store.set_state(
        make_node_id(1),
        FleetNodeState::Draining {
            started_at: Utc::now(),
            timeout_secs: 60,
            active_chunks_at_start: 2,
            reason: "test".to_string(),
        },
        None,
    );
    store.set_state(
        make_node_id(2),
        FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: "test".to_string(),
        },
        None,
    );
    store.set_state(
        make_node_id(3),
        FleetNodeState::Quarantined {
            since: Utc::now(),
            reason: "test".to_string(),
            quarantined_by: None,
        },
        None,
    );
    store.set_state(
        make_node_id(4),
        FleetNodeState::Quarantined {
            since: Utc::now(),
            reason: "test2".to_string(),
            quarantined_by: None,
        },
        None,
    );

    let counts = store.count_by_state();
    assert_eq!(counts.draining, 1);
    assert_eq!(counts.cordoned, 1);
    assert_eq!(counts.quarantined, 2);
    assert_eq!(counts.updating, 0);
    assert_eq!(counts.normal, 0); // Only explicitly set nodes are counted
}

// ============================================================================
// FleetManager tests (27-30)
// ============================================================================

#[tokio::test]
async fn test_27_fleet_manager_drain_node() {
    let manager = FleetManager::new();
    let node = make_node_id(200);

    let result = manager.drain_node(node, 300, "planned maintenance".to_string());
    assert!(result.is_ok());

    let state = manager.fleet_store().get_state(&node);
    assert!(matches!(state, FleetNodeState::Draining { .. }));
    if let FleetNodeState::Draining { timeout_secs, reason, .. } = &state {
        assert_eq!(*timeout_secs, 300);
        assert_eq!(reason, "planned maintenance");
    }
}

#[tokio::test]
async fn test_28_rolling_update_plan_validation() {
    // Valid plan
    let valid_plan = RollingUpdatePlan {
        target_version: "2.0.0".to_string(),
        canary_percentage: 0.1,
        region_order: vec!["us-east".to_string()],
        pause_on_failure: true,
        max_unavailable_pct: 0.2,
        health_check_wait_secs: 60,
        rollback_on_health_failure: true,
        batch_size: 5,
        dry_run: false,
    };
    assert!(valid_plan.validate().is_ok());

    // Invalid: empty version
    let invalid_version = RollingUpdatePlan {
        target_version: "".to_string(),
        ..valid_plan.clone()
    };
    assert!(invalid_version.validate().is_err());

    // Invalid: canary_percentage too low
    let invalid_canary = RollingUpdatePlan {
        canary_percentage: 0.001,
        ..valid_plan.clone()
    };
    assert!(invalid_canary.validate().is_err());

    // Invalid: batch_size = 0
    let invalid_batch = RollingUpdatePlan {
        batch_size: 0,
        ..valid_plan.clone()
    };
    assert!(invalid_batch.validate().is_err());
}

#[tokio::test]
async fn test_29_fleet_manager_summary() {
    let manager = FleetManager::new();

    // Drain a node
    let _ = manager.drain_node(make_node_id(300), 60, "test".to_string());

    // Cordon a node
    manager.fleet_store().set_state(
        make_node_id(301),
        FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: "test".to_string(),
        },
        None,
    );

    let counts = manager.fleet_store().count_by_state();
    assert_eq!(counts.draining, 1);
    assert_eq!(counts.cordoned, 1);
}

#[tokio::test]
async fn test_30_fleet_node_state_display() {
    let draining = FleetNodeState::Draining {
        started_at: Utc::now(),
        timeout_secs: 300,
        active_chunks_at_start: 5,
        reason: "maintenance".to_string(),
    };
    let display = format!("{}", draining);
    assert!(display.contains("Draining"));
    assert!(display.contains("300s"));
    assert!(display.contains("maintenance"));

    let quarantined = FleetNodeState::Quarantined {
        since: Utc::now(),
        reason: "anomaly".to_string(),
        quarantined_by: Some("detector".to_string()),
    };
    let display2 = format!("{}", quarantined);
    assert!(display2.contains("Quarantined"));
    assert!(display2.contains("anomaly"));
    assert!(display2.contains("detector"));
}

// ============================================================================
// FleetManager advanced tests (31-33)
// ============================================================================

#[tokio::test]
async fn test_31_fleet_store_list_by_state() {
    let store = FleetStore::new();

    store.set_state(
        make_node_id(501),
        FleetNodeState::Draining {
            started_at: Utc::now(),
            timeout_secs: 60,
            active_chunks_at_start: 3,
            reason: "test".to_string(),
        },
        None,
    );
    store.set_state(
        make_node_id(502),
        FleetNodeState::Draining {
            started_at: Utc::now(),
            timeout_secs: 120,
            active_chunks_at_start: 1,
            reason: "test2".to_string(),
        },
        None,
    );
    store.set_state(
        make_node_id(503),
        FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: "cordon".to_string(),
        },
        None,
    );

    let draining = store.list_by_state(|s| matches!(s, FleetNodeState::Draining { .. }));
    assert_eq!(draining.len(), 2);

    let cordoned = store.list_by_state(|s| matches!(s, FleetNodeState::Cordoned { .. }));
    assert_eq!(cordoned.len(), 1);
    assert_eq!(cordoned[0].node_id, make_node_id(503));
}

#[tokio::test]
async fn test_32_fleet_store_clear_normal_entries() {
    let store = FleetStore::new();

    store.set_state(
        make_node_id(601),
        FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: "test".to_string(),
        },
        None,
    );
    store.set_state(make_node_id(602), FleetNodeState::Normal, None);

    assert_eq!(store.len(), 2);

    store.clear_normal_entries();
    assert_eq!(store.len(), 1);
    assert!(store.get_info(&make_node_id(601)).is_some());
    assert!(store.get_info(&make_node_id(602)).is_none());
}

#[tokio::test]
async fn test_33_fleet_store_transition_history_capped() {
    let store = FleetStore::new();
    let node = make_node_id(700);

    // Perform more transitions than the cap (50)
    for i in 0..60 {
        if i % 2 == 0 {
            store.set_state(
                node,
                FleetNodeState::Cordoned {
                    since: Utc::now(),
                    reason: format!("iter-{}", i),
                },
                None,
            );
        } else {
            store.set_state(node, FleetNodeState::Normal, None);
        }
    }

    let info = store.get_info(&node).unwrap();
    assert!(info.transitions.len() <= 50, "transitions should be capped at 50");
}

// ============================================================================
// HealthCheck types tests (34-36)
// ============================================================================

#[tokio::test]
async fn test_34_probe_health_state_transitions() {
    let mut health = ProbeHealth {
        state: HealthState::Unknown,
        consecutive_failures: 0,
        consecutive_successes: 0,
        last_result: None,
        success_rate: 0.0,
        avg_latency_ms: 0.0,
        history: std::collections::VecDeque::new(),
    };

    let failure_threshold = 3;
    let success_threshold = 2;

    // Record 3 failures -> should become Unhealthy
    for _ in 0..3 {
        let result = ProbeResult {
            probe_name: "test-probe".to_string(),
            node_id: make_node_id(1),
            timestamp: Utc::now(),
            success: false,
            latency_ms: 100,
            message: None,
            details: None,
        };
        health.history.push_front(result.clone());
        health.last_result = Some(result);
        health.consecutive_failures += 1;
        health.consecutive_successes = 0;
        if health.consecutive_failures >= failure_threshold {
            health.state = HealthState::Unhealthy;
        }
    }
    assert_eq!(health.state, HealthState::Unhealthy);
    assert_eq!(health.consecutive_failures, 3);

    // Record 2 successes -> should become Healthy
    for _ in 0..2 {
        let result = ProbeResult {
            probe_name: "test-probe".to_string(),
            node_id: make_node_id(1),
            timestamp: Utc::now(),
            success: true,
            latency_ms: 10,
            message: None,
            details: None,
        };
        health.history.push_front(result.clone());
        health.last_result = Some(result);
        health.consecutive_successes += 1;
        health.consecutive_failures = 0;
        if health.consecutive_successes >= success_threshold {
            health.state = HealthState::Healthy;
        }
    }
    assert_eq!(health.state, HealthState::Healthy);
    assert_eq!(health.consecutive_successes, 2);
}

#[tokio::test]
async fn test_35_probe_type_display() {
    let gossip = ProbeType::GossipResponse { timeout_ms: 5000 };
    assert_eq!(format!("{}", gossip), "gossip-response(timeout=5000ms)");

    let tcp = ProbeType::TcpConnect { port: 8080, timeout_ms: 3000 };
    assert_eq!(format!("{}", tcp), "tcp-connect(port=8080, timeout=3000ms)");

    let mem = ProbeType::MemoryPressure { max_used_pct: 90.0 };
    assert_eq!(format!("{}", mem), "memory-pressure(max=90%)");
}

#[tokio::test]
async fn test_36_health_state_display() {
    assert_eq!(format!("{}", HealthState::Healthy), "healthy");
    assert_eq!(format!("{}", HealthState::Degraded), "degraded");
    assert_eq!(format!("{}", HealthState::Unhealthy), "unhealthy");
    assert_eq!(format!("{}", HealthState::Unknown), "unknown");
}

// ============================================================================
// SwarmMetrics tests (37-41)
// ============================================================================

#[tokio::test]
async fn test_37_metrics_registration_and_encode() {
    let metrics = SwarmMetrics::new();

    // Set some values
    metrics.update_node_counts(5, 1, 0, 0, 0, 0, 0);
    metrics.update_job_counts(10, 3, 20, 2, 1);

    let text = metrics.encode();
    assert!(!text.is_empty(), "encoded metrics should not be empty");
    assert!(text.contains("swarm_nodes_total"), "should contain node counts");
    assert!(text.contains("swarm_jobs_total"), "should contain job counts");
}

#[tokio::test]
async fn test_38_metrics_update_helpers() {
    let metrics = SwarmMetrics::new();

    metrics.update_node_counts(10, 2, 1, 0, 0, 0, 0);
    assert_eq!(metrics.nodes_total.with_label_values(&["alive"]).get(), 10);
    assert_eq!(metrics.nodes_total.with_label_values(&["suspect"]).get(), 2);
    assert_eq!(metrics.nodes_total.with_label_values(&["dead"]).get(), 1);

    metrics.update_job_counts(5, 3, 100, 10, 2);
    assert_eq!(metrics.jobs_total.with_label_values(&["pending"]).get(), 5);
    assert_eq!(metrics.jobs_total.with_label_values(&["running"]).get(), 3);
    assert_eq!(metrics.jobs_total.with_label_values(&["completed"]).get(), 100);

    metrics.update_chunk_counts(20, 5, 80, 3);
    assert_eq!(metrics.chunks_total.with_label_values(&["pending"]).get(), 20);
    assert_eq!(metrics.pending_chunks.get(), 20);
    assert_eq!(metrics.active_chunks.get(), 5);
}

#[tokio::test]
async fn test_39_metrics_concurrent_updates() {
    let metrics = Arc::new(SwarmMetrics::new());
    let mut handles = Vec::new();

    for _ in 0..10 {
        let m = Arc::clone(&metrics);
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                m.record_gossip_sent();
                m.record_gossip_received();
                m.record_event_emitted();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let text = metrics.encode();
    assert!(text.contains("swarm_gossip_messages_total"));
    assert!(text.contains("swarm_event_bus_emitted_total"));

    // Verify counters are correct
    assert_eq!(
        metrics.gossip_messages_total.with_label_values(&["sent"]).get(),
        1000
    );
    assert_eq!(
        metrics.gossip_messages_total.with_label_values(&["received"]).get(),
        1000
    );
    assert_eq!(metrics.event_bus_emitted_total.get(), 1000);
}

#[tokio::test]
async fn test_40_metrics_all_names_present() {
    let metrics = SwarmMetrics::new();

    // Touch at least one label value for vector metrics to appear in output
    metrics.update_node_counts(1, 0, 0, 0, 0, 0, 0);
    metrics.update_job_counts(1, 0, 0, 0, 0);
    metrics.update_chunk_counts(1, 0, 0, 0);
    metrics.record_gossip_sent();
    metrics.record_gossip_rtt("us-east", 0.01);
    metrics.record_chunk_execution(true, "shell", 1.5);
    metrics.record_api_request("GET", "/health", "200", 0.01);
    metrics.record_auth_request(true);
    metrics.record_policy_evaluation(true);
    metrics.record_membrane_crossing("m1", "inbound", "job", 1024);
    metrics.set_uptime(100.0);
    metrics.record_psyche_computation(0.05);

    let text = metrics.encode();

    let expected_prefixes = [
        "swarm_nodes_total",
        "swarm_jobs_total",
        "swarm_chunks_total",
        "swarm_gossip_messages_total",
        "swarm_gossip_rtt_seconds",
        "swarm_chunk_duration_seconds",
        "swarm_api_request_total",
        "swarm_auth_requests_total",
        "swarm_membrane_crossings_total",
        "swarm_uptime_seconds",
        "swarm_psyche_computation_duration_seconds",
    ];

    for prefix in &expected_prefixes {
        assert!(
            text.contains(prefix),
            "encoded output should contain '{}'. Full output length: {}",
            prefix,
            text.len()
        );
    }
}

#[tokio::test]
async fn test_41_metrics_histogram_buckets() {
    let metrics = SwarmMetrics::new();

    // Record several observations in the gossip RTT histogram
    metrics.record_gossip_rtt("us-east", 0.001);
    metrics.record_gossip_rtt("us-east", 0.01);
    metrics.record_gossip_rtt("us-east", 0.1);
    metrics.record_gossip_rtt("us-east", 1.0);

    let text = metrics.encode();
    assert!(text.contains("swarm_gossip_rtt_seconds_bucket"));
    assert!(text.contains("swarm_gossip_rtt_seconds_count"));
    assert!(text.contains("swarm_gossip_rtt_seconds_sum"));

    // Record chunk executions
    metrics.record_chunk_execution(true, "shell", 0.5);
    metrics.record_chunk_execution(true, "shell", 5.0);
    metrics.record_chunk_execution(false, "python", 30.0);

    let text2 = metrics.encode();
    assert!(text2.contains("swarm_chunk_duration_seconds_bucket"));
    assert!(text2.contains("swarm_chunks_executed_total"));
}

// ============================================================================
// SLA types tests (42-48)
// ============================================================================

#[tokio::test]
async fn test_42_sla_definition_compliance_check() {
    let availability_sla = SlaDefinition {
        name: "swarm-availability".to_string(),
        description: "99.9% uptime".to_string(),
        metric: SlaMetric::Availability,
        target: 99.9,
        window: SlaWindow::Rolling { hours: 720 },
        breach_severity: EventSeverity::Critical,
        enabled: true,
        created_at: Utc::now(),
    };

    // 99.95 >= 99.9 -> compliant
    assert!(availability_sla.is_compliant(99.95));
    // 99.5 < 99.9 -> not compliant
    assert!(!availability_sla.is_compliant(99.5));
}

#[tokio::test]
async fn test_43_sla_latency_compliance_inverted() {
    let latency_sla = SlaDefinition {
        name: "api-latency".to_string(),
        description: "p99 < 200ms".to_string(),
        metric: SlaMetric::ApiLatencyP99Ms,
        target: 200.0,
        window: SlaWindow::Rolling { hours: 24 },
        breach_severity: EventSeverity::Error,
        enabled: true,
        created_at: Utc::now(),
    };

    // For latency metrics, measured <= target -> compliant
    assert!(latency_sla.is_compliant(150.0));
    assert!(latency_sla.is_compliant(200.0));
    assert!(!latency_sla.is_compliant(250.0));
    assert!(latency_sla.is_upper_bound_metric());
}

#[tokio::test]
async fn test_44_sla_metric_display_and_parse() {
    let metrics = vec![
        SlaMetric::Availability,
        SlaMetric::ChunkSuccessRate,
        SlaMetric::ChunkLatencyP99Ms,
        SlaMetric::JobCompletionRate,
        SlaMetric::Custom("my_metric".to_string()),
    ];

    for metric in &metrics {
        let s = metric.to_string();
        let parsed: SlaMetric = s.parse().expect(&format!("should parse '{}'", s));
        assert_eq!(&parsed, metric);
    }
}

#[tokio::test]
async fn test_45_sla_window_hours() {
    let rolling = SlaWindow::Rolling { hours: 720 };
    assert_eq!(rolling.hours(), 720);

    let daily = SlaWindow::Calendar { period: "day".to_string() };
    assert_eq!(daily.hours(), 24);

    let weekly = SlaWindow::Calendar { period: "week".to_string() };
    assert_eq!(weekly.hours(), 168);

    let monthly = SlaWindow::Calendar { period: "month".to_string() };
    assert_eq!(monthly.hours(), 720);
}

#[tokio::test]
async fn test_46_sla_window_display() {
    let rolling = SlaWindow::Rolling { hours: 24 };
    assert_eq!(format!("{}", rolling), "rolling_24h");

    let calendar = SlaWindow::Calendar { period: "month".to_string() };
    assert_eq!(format!("{}", calendar), "calendar_month");
}

// ============================================================================
// MetricsSnapshot tests (47-48)
// ============================================================================

#[tokio::test]
async fn test_47_metrics_snapshot_capture() {
    let metrics = SwarmMetrics::new();
    metrics.update_node_counts(10, 2, 1, 0, 0, 0, 0);
    metrics.update_job_counts(5, 3, 100, 10, 2);
    metrics.set_uptime(3600.0);
    metrics.record_gossip_sent();
    metrics.record_gossip_sent();

    let snapshot = MetricsSnapshot::capture(&metrics);

    assert_eq!(*snapshot.nodes.get("alive").unwrap(), 10);
    assert_eq!(*snapshot.nodes.get("suspect").unwrap(), 2);
    assert_eq!(*snapshot.nodes.get("dead").unwrap(), 1);
    assert_eq!(*snapshot.jobs.get("completed").unwrap(), 100);
    assert_eq!(snapshot.gossip_sent, 2);
    assert_eq!(snapshot.uptime_secs, 3600.0);
}

#[tokio::test]
async fn test_48_metrics_snapshot_diff() {
    let metrics = SwarmMetrics::new();

    // Take first snapshot
    metrics.update_node_counts(5, 0, 0, 0, 0, 0, 0);
    metrics.record_gossip_sent();
    let snap1 = MetricsSnapshot::capture(&metrics);

    // Add more activity
    metrics.update_node_counts(10, 1, 0, 0, 0, 0, 0);
    for _ in 0..5 {
        metrics.record_gossip_sent();
    }
    let snap2 = MetricsSnapshot::capture(&metrics);

    let diff = snap2.diff(&snap1);
    assert_eq!(diff.gossip_sent_delta, 5);
    assert_eq!(*diff.current_nodes.get("alive").unwrap(), 10);
    assert_eq!(*diff.current_nodes.get("suspect").unwrap(), 1);
}

// ============================================================================
// Complexity types tests (49-52)
// ============================================================================

#[tokio::test]
async fn test_49_concern_domain_all_variants() {
    let all = ConcernDomain::all();
    assert_eq!(all.len(), 8);
    assert!(all.contains(&ConcernDomain::Health));
    assert!(all.contains(&ConcernDomain::Work));
    assert!(all.contains(&ConcernDomain::Data));
    assert!(all.contains(&ConcernDomain::Fleet));
    assert!(all.contains(&ConcernDomain::Security));
    assert!(all.contains(&ConcernDomain::Cost));
    assert!(all.contains(&ConcernDomain::Psyche));
    assert!(all.contains(&ConcernDomain::MultiSwarm));
}

#[tokio::test]
async fn test_50_concern_domain_parse_display() {
    for domain in ConcernDomain::all() {
        let s = domain.to_string();
        let parsed: ConcernDomain = s.parse().unwrap();
        assert_eq!(parsed, domain);
    }
}

#[tokio::test]
async fn test_51_event_severity_ordering() {
    assert!(EventSeverity::Info < EventSeverity::Notice);
    assert!(EventSeverity::Notice < EventSeverity::Warning);
    assert!(EventSeverity::Warning < EventSeverity::Error);
    assert!(EventSeverity::Error < EventSeverity::Critical);

    assert!(EventSeverity::Critical.is_at_least(&EventSeverity::Info));
    assert!(EventSeverity::Warning.is_at_least(&EventSeverity::Warning));
    assert!(!EventSeverity::Info.is_at_least(&EventSeverity::Error));
}

#[tokio::test]
async fn test_52_complexity_style_all_variants() {
    let all = ComplexityStyle::all();
    assert_eq!(all.len(), 6);
    assert!(all.contains(&ComplexityStyle::Glanceable));
    assert!(all.contains(&ComplexityStyle::Browseable));
    assert!(all.contains(&ComplexityStyle::Observable));
    assert!(all.contains(&ComplexityStyle::Orchestrated));
    assert!(all.contains(&ComplexityStyle::Investigative));
    assert!(all.contains(&ComplexityStyle::Scriptable));

    // Test parse/display roundtrip
    for style in &all {
        let s = style.to_string();
        let parsed: ComplexityStyle = s.parse().unwrap();
        assert_eq!(&parsed, style);
    }
}

// ============================================================================
// ManagedAction tests (53-56)
// ============================================================================

#[tokio::test]
async fn test_53_managed_action_read_only() {
    assert!(ManagedAction::ViewNodes.is_read_only());
    assert!(ManagedAction::ViewJobs.is_read_only());
    assert!(ManagedAction::ViewFleet.is_read_only());
    assert!(ManagedAction::ViewEvents.is_read_only());
    assert!(ManagedAction::StreamEvents.is_read_only());
    assert!(ManagedAction::ViewAll.is_read_only());

    assert!(!ManagedAction::DrainNode.is_read_only());
    assert!(!ManagedAction::SubmitJob.is_read_only());
    assert!(!ManagedAction::StartRollingUpdate.is_read_only());
    assert!(!ManagedAction::QuarantineNode.is_read_only());
    assert!(!ManagedAction::ManageAll.is_read_only());
}

#[tokio::test]
async fn test_54_managed_action_domain_mapping() {
    assert_eq!(
        ManagedAction::DrainNode.required_domain(),
        ConcernDomain::Health
    );
    assert_eq!(
        ManagedAction::SubmitJob.required_domain(),
        ConcernDomain::Work
    );
    assert_eq!(
        ManagedAction::StartRollingUpdate.required_domain(),
        ConcernDomain::Fleet
    );
    assert_eq!(
        ManagedAction::RevokeToken.required_domain(),
        ConcernDomain::Security
    );
    assert_eq!(
        ManagedAction::ViewEnergy.required_domain(),
        ConcernDomain::Cost
    );
}

#[tokio::test]
async fn test_55_managed_action_parse_roundtrip() {
    let actions = [
        "view_nodes", "drain_node", "submit_job", "start_rolling_update",
        "view_psyche", "propose_agreement", "view_all", "manage_all",
    ];

    for name in &actions {
        let parsed: ManagedAction = name.parse().unwrap();
        let display = parsed.to_string();
        assert_eq!(&display, name);
    }
}

#[tokio::test]
async fn test_56_concern_domain_default_styles() {
    // Health should have Glanceable
    let health_styles = ConcernDomain::Health.default_styles();
    assert!(health_styles.contains(&ComplexityStyle::Glanceable));

    // Security should have Investigative
    let sec_styles = ConcernDomain::Security.default_styles();
    assert!(sec_styles.contains(&ComplexityStyle::Investigative));

    // Fleet should have Orchestrated
    let fleet_styles = ConcernDomain::Fleet.default_styles();
    assert!(fleet_styles.contains(&ComplexityStyle::Orchestrated));
}

// ============================================================================
// TransformRule tests (57-60)
// ============================================================================

#[tokio::test]
async fn test_57_transform_filter_keeps_specified_fields() {
    let transform = TransformRule::Filter {
        keep_fields: vec!["name".to_string(), "value".to_string()],
    };

    let data = serde_json::json!({
        "name": "test",
        "value": 42,
        "secret": "hidden",
        "extra": "removed"
    });

    let result = transform.apply(&data).unwrap();
    assert_eq!(result["name"], "test");
    assert_eq!(result["value"], 42);
    assert!(result.get("secret").is_none());
    assert!(result.get("extra").is_none());
}

#[tokio::test]
async fn test_58_transform_anonymize_hashes_fields() {
    let transform = TransformRule::Anonymize {
        fields: vec!["email".to_string()],
    };

    let data = serde_json::json!({
        "email": "user@example.com",
        "name": "John"
    });

    let result = transform.apply(&data).unwrap();
    // The email should be hashed (64-char hex string)
    let email = result["email"].as_str().unwrap();
    assert_eq!(email.len(), 64);
    assert!(email.chars().all(|c| c.is_ascii_hexdigit()));
    // Name should be unchanged
    assert_eq!(result["name"], "John");
}

#[tokio::test]
async fn test_59_transform_redact_replaces_patterns() {
    let transform = TransformRule::Redact {
        pattern: r"\d{3}-\d{2}-\d{4}".to_string(),
        replacement: "[REDACTED]".to_string(),
    };

    let data = serde_json::json!({
        "ssn": "123-45-6789",
        "name": "test"
    });

    let result = transform.apply(&data).unwrap();
    assert_eq!(result["ssn"], "[REDACTED]");
    assert_eq!(result["name"], "test");
}

#[tokio::test]
async fn test_60_transform_size_limit_rejects_large_payloads() {
    let transform = TransformRule::SizeLimit { max_bytes: 50 };

    let small_data = serde_json::json!({"k": "v"});
    assert!(transform.apply(&small_data).is_ok());

    let large_data = serde_json::json!({
        "key": "a very long value that will exceed the fifty byte limit for sure"
    });
    assert!(transform.apply(&large_data).is_err());
}

// ============================================================================
// Membrane tests (61-64)
// ============================================================================

#[tokio::test]
async fn test_61_data_category_all_builtin() {
    let categories = DataCategory::all_builtin();
    assert!(categories.len() >= 12, "should have at least 12 built-in categories");
    assert!(categories.contains(&DataCategory::JobSubmission));
    assert!(categories.contains(&DataCategory::NodeHealth));
    assert!(categories.contains(&DataCategory::AuditRecord));
}

#[tokio::test]
async fn test_62_data_category_parse_display_roundtrip() {
    let categories = DataCategory::all_builtin();
    for cat in &categories {
        let s = cat.to_string();
        let parsed: DataCategory = s.parse().unwrap();
        assert_eq!(&parsed, cat);
    }

    // Custom category
    let custom: DataCategory = "custom:my_data".parse().unwrap();
    assert_eq!(custom, DataCategory::Custom("my_data".to_string()));
}

#[tokio::test]
async fn test_63_crossing_direction_display_parse() {
    let directions = [
        CrossingDirection::Inbound,
        CrossingDirection::Outbound,
        CrossingDirection::Bidirectional,
    ];

    for dir in &directions {
        let s = dir.to_string();
        let parsed: CrossingDirection = s.parse().unwrap();
        assert_eq!(&parsed, dir);
    }
}

#[tokio::test]
async fn test_64_transform_validate() {
    // Valid transforms
    let valid_filter = TransformRule::Filter {
        keep_fields: vec!["a".to_string()],
    };
    assert!(valid_filter.validate().is_ok());

    let valid_anon = TransformRule::Anonymize {
        fields: vec!["email".to_string()],
    };
    assert!(valid_anon.validate().is_ok());

    // Invalid: empty fields
    let invalid_filter = TransformRule::Filter {
        keep_fields: vec![],
    };
    assert!(invalid_filter.validate().is_err());

    let invalid_anon = TransformRule::Anonymize { fields: vec![] };
    assert!(invalid_anon.validate().is_err());

    // Invalid: bad regex
    let invalid_redact = TransformRule::Redact {
        pattern: "[invalid".to_string(),
        replacement: "x".to_string(),
    };
    assert!(invalid_redact.validate().is_err());

    // Invalid: zero size limit
    let invalid_size = TransformRule::SizeLimit { max_bytes: 0 };
    assert!(invalid_size.validate().is_err());
}

// ============================================================================
// Agreement tests (65-72)
// ============================================================================

#[tokio::test]
async fn test_65_agreement_negotiation_full_lifecycle() {
    let party_a = AgreementSwarmId::new();
    let party_b = AgreementSwarmId::new();

    let agreement = new_draft_agreement(
        party_a,
        party_b,
        "Test Alliance".to_string(),
        "A test agreement".to_string(),
    );

    let mut negotiation = AgreementNegotiation::new(agreement);

    // Step 1: Propose
    assert!(negotiation.propose(party_a).is_ok());
    assert!(matches!(
        negotiation.agreement().status,
        AgreementStatus::Proposed { .. }
    ));

    // Step 2: Accept (by the other party)
    assert!(negotiation.accept(party_b).is_ok());
    assert!(matches!(negotiation.agreement().status, AgreementStatus::Accepted));

    // Step 3: Sign by party A
    let key_a = b"secret_key_a";
    assert!(negotiation.sign(party_a, key_a).is_ok());
    // Still accepted (need both signatures)
    assert!(matches!(negotiation.agreement().status, AgreementStatus::Accepted));

    // Step 4: Sign by party B -> becomes Active
    let key_b = b"secret_key_b";
    assert!(negotiation.sign(party_b, key_b).is_ok());
    assert!(matches!(negotiation.agreement().status, AgreementStatus::Active));
    assert!(negotiation.agreement().effective_at.is_some());

    // Verify signatures
    assert!(negotiation.agreement().verify_signatures(key_a, key_b));

    // Should have 4 messages in history
    assert_eq!(negotiation.messages().len(), 4);
}

#[tokio::test]
async fn test_66_agreement_invalid_transitions() {
    let party_a = AgreementSwarmId::new();
    let party_b = AgreementSwarmId::new();

    let agreement = new_draft_agreement(
        party_a,
        party_b,
        "Invalid Test".to_string(),
        "Testing invalid transitions".to_string(),
    );

    let mut negotiation = AgreementNegotiation::new(agreement);

    // Cannot accept a draft agreement
    assert!(negotiation.accept(party_b).is_err());

    // Cannot sign a draft agreement
    assert!(negotiation.sign(party_a, b"key").is_err());

    // Propose it
    assert!(negotiation.propose(party_a).is_ok());

    // Proposer cannot accept their own proposal
    assert!(negotiation.accept(party_a).is_err());

    // Proposer cannot counter-propose their own proposal
    let modified = new_draft_agreement(party_a, party_b, "Modified".to_string(), "mod".to_string());
    assert!(negotiation.counter_propose(party_a, modified).is_err());
}

#[tokio::test]
async fn test_67_agreement_rejection() {
    let party_a = AgreementSwarmId::new();
    let party_b = AgreementSwarmId::new();

    let agreement = new_draft_agreement(
        party_a,
        party_b,
        "Reject Test".to_string(),
        "Will be rejected".to_string(),
    );

    let mut negotiation = AgreementNegotiation::new(agreement);
    negotiation.propose(party_a).unwrap();

    // Party B rejects
    assert!(negotiation.reject(party_b, "terms unacceptable".to_string()).is_ok());
    assert!(matches!(
        negotiation.agreement().status,
        AgreementStatus::Terminated { .. }
    ));

    // Cannot propose after termination
    assert!(negotiation.propose(party_a).is_err());
}

#[tokio::test]
async fn test_68_agreement_store_crud() {
    let store = AgreementStore::new();
    let party_a = AgreementSwarmId::new();
    let party_b = AgreementSwarmId::new();
    let party_c = AgreementSwarmId::new();

    let agreement1 = new_draft_agreement(
        party_a,
        party_b,
        "Agreement 1".to_string(),
        "First agreement".to_string(),
    );
    let id1 = agreement1.id;
    store.upsert(agreement1);

    let agreement2 = new_draft_agreement(
        party_a,
        party_c,
        "Agreement 2".to_string(),
        "Second agreement".to_string(),
    );
    store.upsert(agreement2);

    assert_eq!(store.count(), 2);

    // Find by swarm
    let treaties_for_a = store.find_by_swarm(party_a);
    assert_eq!(treaties_for_a.len(), 2);

    let treaties_for_c = store.find_by_swarm(party_c);
    assert_eq!(treaties_for_c.len(), 1);

    // Get by ID
    let retrieved = store.get(&id1).unwrap();
    assert_eq!(retrieved.name, "Agreement 1");

    // Remove
    let removed = store.remove(&id1);
    assert!(removed.is_some());
    assert_eq!(store.count(), 1);
}

#[tokio::test]
async fn test_69_agreement_validation() {
    let party = AgreementSwarmId::new();

    // Invalid: same party on both sides
    let invalid = Agreement {
        id: AgreementId::new(),
        version: 1,
        party_a: party,
        party_b: party,
        name: "Self Agreement".to_string(),
        description: "".to_string(),
        permeability: Vec::new(),
        severance: Default::default(),
        lending: Default::default(),
        status: AgreementStatus::Draft,
        signed_by_a: None,
        signed_by_b: None,
        effective_at: None,
        expires_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        amendment_history: Vec::new(),
    };

    let errors = invalid.validate();
    assert!(!errors.is_empty());
    assert!(errors.iter().any(|e| e.contains("different swarms")));
}

#[tokio::test]
async fn test_70_lending_meter_start_stop() {
    let meter = LendingMeter::new();
    let lender = AgreementSwarmId::new();
    let borrower = AgreementSwarmId::new();

    // Start lending
    assert!(meter
        .start_lending(lender, borrower, 20, 4, 8192, Some(3600))
        .is_ok());

    assert_eq!(meter.active_count(), 1);
    assert_eq!(meter.total_lent_pct(lender), 20);
    assert_eq!(meter.total_borrowed_pct(borrower), 20);

    // Cannot start a duplicate session
    assert!(meter
        .start_lending(lender, borrower, 10, 2, 4096, None)
        .is_err());

    // Stop lending
    let billing = LendingBilling {
        model: BillingModel::Metered,
        rate_per_core_hour: 0.10,
        rate_per_gb_hour: 0.02,
        minimum_charge_secs: 60,
    };

    let record = meter.stop_lending(lender, borrower, &billing).unwrap();
    assert_eq!(record.lender, lender);
    assert_eq!(record.borrower, borrower);
    assert!(record.core_hours >= 0.0);
    assert!(record.total_cost >= 0.0);
    assert_eq!(meter.active_count(), 0);

    // Billing history should have one record
    let history = meter.billing_history();
    assert_eq!(history.len(), 1);
}

#[tokio::test]
async fn test_71_lending_meter_evaluate_blocked_by_psyche() {
    let meter = LendingMeter::new();

    let policy = LendingPolicy {
        enabled: true,
        min_local_resilience: 50,
        min_local_vitality: 50,
        block_during_archetypes: vec!["war-room".to_string()],
        ..Default::default()
    };

    // Low resilience blocks lending
    let low_resilience = PsycheFacets {
        resilience: 30,
        vitality: 80,
        active_archetypes: vec![],
    };
    assert!(meter.evaluate_lending_allowed(&policy, &low_resilience).is_err());

    // Low vitality blocks lending
    let low_vitality = PsycheFacets {
        resilience: 80,
        vitality: 30,
        active_archetypes: vec![],
    };
    assert!(meter.evaluate_lending_allowed(&policy, &low_vitality).is_err());

    // Blocked archetype blocks lending
    let war_room = PsycheFacets {
        resilience: 80,
        vitality: 80,
        active_archetypes: vec!["war-room".to_string()],
    };
    assert!(meter.evaluate_lending_allowed(&policy, &war_room).is_err());

    // Healthy state allows lending
    let healthy = PsycheFacets {
        resilience: 80,
        vitality: 80,
        active_archetypes: vec!["cruise-control".to_string()],
    };
    assert!(meter.evaluate_lending_allowed(&policy, &healthy).is_ok());
}

#[tokio::test]
async fn test_72_constellation_builder_basic() {
    let agreement_store = Arc::new(AgreementStore::new());
    let local = AgreementSwarmId::new();
    let remote = AgreementSwarmId::new();

    // Insert an active agreement
    let mut agreement = new_draft_agreement(
        local,
        remote,
        "Alliance".to_string(),
        "Cross-swarm alliance".to_string(),
    );
    agreement.status = AgreementStatus::Active;
    agreement_store.upsert(agreement);

    let summaries = vec![
        SwarmSummary {
            id: local,
            name: "Local Swarm".to_string(),
            alive_nodes: 10,
            total_cores: 40,
            total_memory_mb: 65536,
        },
        SwarmSummary {
            id: remote,
            name: "Remote Swarm".to_string(),
            alive_nodes: 5,
            total_cores: 20,
            total_memory_mb: 32768,
        },
    ];

    let builder = ConstellationBuilder::new(agreement_store)
        .with_local_swarm(local)
        .with_summaries(summaries);

    let view = builder.build();

    assert_eq!(view.nodes.len(), 2);
    assert_eq!(view.edges.len(), 1);
    assert!(view.edges[0].2.is_active);

    // Local swarm should be marked as local
    let local_node = view.nodes.iter().find(|n| n.swarm_id == local).unwrap();
    assert!(local_node.is_local);

    let remote_node = view.nodes.iter().find(|n| n.swarm_id == remote).unwrap();
    assert!(!remote_node.is_local);

    // Stats
    assert_eq!(view.stats.total_swarms, 2);
    assert_eq!(view.stats.total_treaties, 1);
    assert_eq!(view.stats.active_treaties, 1);
}

// ============================================================================
// Capacity tests (73-76)
// ============================================================================

#[tokio::test]
async fn test_73_capacity_data_point_utilization() {
    let point = make_capacity_point(16, 12.0, 32768, 24000, 500000, 400000, 8);

    let cpu_util = point.cpu_utilization();
    assert!((cpu_util - 0.75).abs() < 0.01, "cpu util should be ~0.75, got {}", cpu_util);

    let mem_util = point.memory_utilization();
    let expected_mem = 24000.0 / 32768.0;
    assert!((mem_util - expected_mem).abs() < 0.01);

    let disk_util = point.disk_utilization();
    assert!((disk_util - 0.8).abs() < 0.01);
}

#[tokio::test]
async fn test_74_capacity_planner_forecast_with_linear_growth() {
    let planner = CapacityPlanner::new();

    // Add data points with linearly growing CPU usage
    let base_time = Utc::now() - ChronoDuration::hours(24);
    for i in 0..100 {
        let mut point = make_capacity_point(100, 30.0 + (i as f64 * 0.5), 100000, 50000, 1000000, 500000, 10);
        point.timestamp = base_time + ChronoDuration::minutes(i * 15);
        planner.add_data_point(point);
    }

    let forecast = planner.forecast("cpu");
    assert!(forecast.is_some(), "forecast should be available with 100 data points");

    let fc = forecast.unwrap();
    assert_eq!(fc.resource, "cpu");
    assert!(fc.current_utilization > 0.0);
    assert!(fc.confidence > 0.0);
    // With rising data, trend should be Rising
    assert_eq!(fc.trend, crate::swarm::capacity::Trend::Rising);
}

#[tokio::test]
async fn test_75_capacity_planner_identify_bottlenecks() {
    let planner = CapacityPlanner::new();

    // Add points with very high CPU utilization
    for i in 0..10 {
        let mut point = make_capacity_point(100, 96.0, 100000, 50000, 1000000, 500000, 10);
        point.timestamp = Utc::now() - ChronoDuration::minutes((9 - i) * 5);
        planner.add_data_point(point);
    }

    let bottlenecks = planner.identify_bottlenecks();
    // CPU at 96% should be identified as a bottleneck
    let cpu_bottleneck = bottlenecks.iter().find(|b| b.resource == "cpu");
    assert!(cpu_bottleneck.is_some(), "should identify CPU bottleneck at 96% utilization");
    assert_eq!(cpu_bottleneck.unwrap().severity, BottleneckSeverity::Imminent);
}

#[tokio::test]
async fn test_76_capacity_planner_what_if() {
    let planner = CapacityPlanner::new();

    // Add some data points
    for i in 0..20 {
        let mut point = make_capacity_point(100, 80.0, 100000, 70000, 1000000, 500000, 10);
        point.timestamp = Utc::now() - ChronoDuration::minutes((19 - i) * 5);
        planner.add_data_point(point);
    }

    let scenario = WhatIfScenario {
        name: "Add 5 nodes".to_string(),
        add_nodes: 5,
        add_cpu_cores: 40,
        add_memory_mb: 32768,
        add_disk_mb: 500000,
        job_growth_pct: 0.0,
    };

    let r = planner.what_if(&scenario);
    assert_eq!(r.scenario.name, "Add 5 nodes");
    // Adding capacity should reduce utilization
    let cpu_util = r.projected_utilization.get("cpu");
    assert!(cpu_util.is_some());
    assert!(*cpu_util.unwrap() < 0.8, "adding CPU should reduce utilization below 80%");
}

// ============================================================================
// Audit tests (77-80)
// ============================================================================

#[tokio::test]
async fn test_77_audit_log_record_and_query() {
    let log = AuditLog::new();

    let id1 = log.record(
        make_test_actor("node-1"),
        "job.submit".to_string(),
        make_test_target("job", "job-123"),
        AuditOutcome::Success,
        serde_json::json!({"chunks": 10}),
    );

    let id2 = log.record(
        make_test_actor("node-2"),
        "job.cancel".to_string(),
        make_test_target("job", "job-456"),
        AuditOutcome::Failure { reason: "not found".to_string() },
        serde_json::json!({}),
    );

    assert_eq!(log.len(), 2);
    assert!(id1 < id2);

    // Query all
    let all = log.query(&AuditFilter::default());
    assert_eq!(all.len(), 2);

    // Query by action
    let submit_filter = AuditFilter {
        action: Some("job.submit".to_string()),
        ..Default::default()
    };
    let submits = log.query(&submit_filter);
    assert_eq!(submits.len(), 1);
    assert_eq!(submits[0].action, "job.submit");

    // Query by outcome
    let failure_filter = AuditFilter {
        outcome: Some("failure".to_string()),
        ..Default::default()
    };
    let failures = log.query(&failure_filter);
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].target.id, "job-456");
}

#[tokio::test]
async fn test_78_audit_log_hash_chain_integrity() {
    let log = AuditLog::new();

    for i in 0..10 {
        log.record(
            make_test_actor(&format!("node-{}", i)),
            format!("action-{}", i),
            make_test_target("job", &format!("job-{}", i)),
            AuditOutcome::Success,
            serde_json::json!({"index": i}),
        );
    }

    let verification = log.verify_chain();
    assert!(verification.valid, "chain should be valid");
    assert_eq!(verification.total_entries, 10);
    assert_eq!(verification.valid_entries, 10);
    assert_eq!(verification.invalid_entries, 0);
    assert!(verification.first_invalid_id.is_none());
}

#[tokio::test]
async fn test_79_audit_log_tamper_detection() {
    let log = AuditLog::new();

    log.record(
        make_test_actor("node-1"),
        "action-1".to_string(),
        make_test_target("job", "job-1"),
        AuditOutcome::Success,
        serde_json::json!({}),
    );

    log.record(
        make_test_actor("node-2"),
        "action-2".to_string(),
        make_test_target("job", "job-2"),
        AuditOutcome::Success,
        serde_json::json!({}),
    );

    // Verify the individual entry's self-check
    let entries = log.recent(10);
    assert_eq!(entries.len(), 2);

    // Each entry should verify against its own content
    for entry in &entries {
        assert!(entry.verify(), "entry {} should verify", entry.id);
    }

    // The chain should link correctly: entry[1].prev_hash == entry[0].hash
    assert_eq!(entries[1].prev_hash, entries[0].hash);
}

#[tokio::test]
async fn test_80_audit_log_persistence_roundtrip() {
    // Create a temp directory for the test
    let dir = std::env::temp_dir().join(format!("audit_test_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("audit.ndjson");

    // Write entries to disk
    {
        let log = AuditLog::with_persistence(path.clone());
        for i in 0..5 {
            log.record(
                make_test_actor(&format!("actor-{}", i)),
                format!("action-{}", i),
                make_test_target("target", &format!("id-{}", i)),
                AuditOutcome::Success,
                serde_json::json!({"step": i}),
            );
        }
        log.flush_to_disk().expect("flush should succeed");
    }

    // Load from disk into a new log
    {
        let log2 = AuditLog::with_persistence(path.clone());
        let loaded = log2.load_from_disk().expect("load should succeed");
        assert_eq!(loaded, 5);
        assert_eq!(log2.len(), 5);

        // Verify chain integrity after reload
        let verification = log2.verify_chain();
        assert!(verification.valid, "chain should be valid after reload");
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================================
// Additional integration tests (81-85)
// ============================================================================

#[tokio::test]
async fn test_81_audit_log_export_csv() {
    let log = AuditLog::new();

    log.record(
        make_test_actor("admin"),
        "fleet.drain".to_string(),
        make_test_target("node", "node-42"),
        AuditOutcome::Success,
        serde_json::json!({}),
    );

    let csv = log.export_csv();
    assert!(csv.starts_with("id,timestamp,actor_type,actor_id,action,target_type,target_id,outcome,hash\n"));
    assert!(csv.contains("fleet.drain"));
    assert!(csv.contains("node-42"));
    assert!(csv.contains("success"));
}

#[tokio::test]
async fn test_82_audit_log_export_ndjson() {
    let log = AuditLog::new();

    log.record(
        make_test_actor("system"),
        "node.join".to_string(),
        make_test_target("node", "node-1"),
        AuditOutcome::Success,
        serde_json::json!({}),
    );
    log.record(
        make_test_actor("system"),
        "node.leave".to_string(),
        make_test_target("node", "node-2"),
        AuditOutcome::Success,
        serde_json::json!({}),
    );

    let ndjson = log.export_ndjson();
    let lines: Vec<&str> = ndjson.trim().split('\n').collect();
    assert_eq!(lines.len(), 2);

    // Each line should be valid JSON
    for line in &lines {
        let _: serde_json::Value = serde_json::from_str(line)
            .expect("each NDJSON line should be valid JSON");
    }
}

#[tokio::test]
async fn test_83_audit_log_max_entries_cap() {
    let log = AuditLog::new().with_max_entries(100);

    for i in 0..200 {
        log.record(
            make_test_actor("system"),
            format!("action-{}", i),
            make_test_target("job", &format!("job-{}", i)),
            AuditOutcome::Success,
            serde_json::json!({}),
        );
    }

    assert_eq!(log.len(), 100);

    // The oldest entry should be action-100 (first 100 were evicted)
    let recent = log.recent(100);
    assert_eq!(recent[0].action, "action-100");
    assert_eq!(recent[99].action, "action-199");
}

#[tokio::test]
async fn test_84_audit_filter_pagination() {
    let log = AuditLog::new();

    for i in 0..50 {
        log.record(
            make_test_actor("node"),
            "action".to_string(),
            make_test_target("job", &format!("job-{}", i)),
            AuditOutcome::Success,
            serde_json::json!({}),
        );
    }

    // Page 1: offset=0, limit=10
    let page1 = log.query(&AuditFilter {
        limit: Some(10),
        offset: Some(0),
        ..Default::default()
    });
    assert_eq!(page1.len(), 10);
    assert_eq!(page1[0].target.id, "job-0");

    // Page 2: offset=10, limit=10
    let page2 = log.query(&AuditFilter {
        limit: Some(10),
        offset: Some(10),
        ..Default::default()
    });
    assert_eq!(page2.len(), 10);
    assert_eq!(page2[0].target.id, "job-10");

    // Count
    let total = log.count(&AuditFilter::default());
    assert_eq!(total, 50);
}

#[tokio::test]
async fn test_85_crossing_log_record_and_count() {
    let mut log = CrossingLog::new();
    let swarm_a = AgreementSwarmId::new();
    let swarm_b = AgreementSwarmId::new();
    let swarm_c = AgreementSwarmId::new();

    log.record(swarm_a, swarm_b, "jobs".to_string());
    log.record(swarm_a, swarm_b, "data".to_string());
    log.record(swarm_b, swarm_a, "gossip".to_string());
    log.record(swarm_a, swarm_c, "jobs".to_string());

    assert_eq!(log.entries().len(), 4);
    assert_eq!(log.count_between(swarm_a, swarm_b), 3);
    assert_eq!(log.count_between(swarm_a, swarm_c), 1);
    assert_eq!(log.count_between(swarm_b, swarm_c), 0);
}

// ============================================================================
// Additional cross-module integration tests (86-90)
// ============================================================================

#[tokio::test]
async fn test_86_event_bus_with_entities() {
    let bus = create_test_event_bus();

    let node_id = make_node_id(42);
    let entities = vec![
        EntityRef::node(&node_id),
        EntityRef::new(EntityType::Swarm, "swarm-1"),
    ];

    let event_id = bus.emit_with_entities(
        ConcernDomain::Fleet,
        EventSeverity::Warning,
        "Node being drained",
        entities,
        serde_json::json!({"reason": "maintenance"}),
    );

    let events = bus.recent_events(1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, event_id);
    assert_eq!(events[0].related_entities.len(), 2);
    assert_eq!(events[0].related_entities[0].entity_type, EntityType::Node);
    assert_eq!(events[0].related_entities[1].entity_type, EntityType::Swarm);
}

#[tokio::test]
async fn test_87_entity_ref_display() {
    let node_id = make_node_id(1);
    let entity = EntityRef::node(&node_id);
    let display = format!("{}", entity);
    assert!(display.starts_with("node:"));

    let named = EntityRef::with_name(EntityType::Job, "job-123", "Data Pipeline");
    let named_display = format!("{}", named);
    assert!(named_display.contains("job:job-123"));
    assert!(named_display.contains("Data Pipeline"));
}

#[tokio::test]
async fn test_88_metrics_record_chunk_execution() {
    let metrics = SwarmMetrics::new();

    metrics.record_chunk_execution(true, "shell", 2.5);
    metrics.record_chunk_execution(true, "python", 1.0);
    metrics.record_chunk_execution(false, "shell", 10.0);

    assert_eq!(
        metrics.chunks_executed_total.with_label_values(&["success", "shell"]).get(),
        1
    );
    assert_eq!(
        metrics.chunks_executed_total.with_label_values(&["success", "python"]).get(),
        1
    );
    assert_eq!(
        metrics.chunks_executed_total.with_label_values(&["failure", "shell"]).get(),
        1
    );
}

#[tokio::test]
async fn test_89_metrics_fleet_and_admission() {
    let metrics = SwarmMetrics::new();

    metrics.update_fleet_counts(2, 1, 0, 3);
    assert_eq!(metrics.fleet_nodes_draining.get(), 2);
    assert_eq!(metrics.fleet_nodes_cordoned.get(), 1);
    assert_eq!(metrics.fleet_nodes_quarantined.get(), 0);
    assert_eq!(metrics.fleet_nodes_updating.get(), 3);

    metrics.admission_pending.set(5);
    metrics.admission_probation.set(2);
    metrics.record_admission_decision("admitted");
    metrics.record_admission_decision("admitted");
    metrics.record_admission_decision("rejected");

    assert_eq!(metrics.admission_pending.get(), 5);
    assert_eq!(metrics.admission_total.with_label_values(&["admitted"]).get(), 2);
    assert_eq!(metrics.admission_total.with_label_values(&["rejected"]).get(), 1);
}

#[tokio::test]
async fn test_90_metrics_membrane_and_treaties() {
    let metrics = SwarmMetrics::new();

    metrics.record_membrane_crossing("m1", "inbound", "job", 2048);
    metrics.record_membrane_crossing("m1", "outbound", "data", 4096);
    metrics.set_membrane_status("m1", true);
    metrics.active_treaties.set(3);
    metrics.capacity_lending_pct.set(0.15);

    let text = metrics.encode();
    assert!(text.contains("swarm_membrane_crossings_total"));
    assert!(text.contains("swarm_membrane_crossing_bytes_total"));
    assert!(text.contains("swarm_membrane_status"));
    assert!(text.contains("swarm_active_treaties"));
    assert!(text.contains("swarm_capacity_lending_pct"));

    assert_eq!(metrics.active_treaties.get(), 3);
}
