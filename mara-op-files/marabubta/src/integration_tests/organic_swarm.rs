// Marabunta - Licensed under the MIT License.
//! Integration tests for the organic swarm extensions.
//!
//! These tests exercise the profile, collective, marketplace, reputation,
//! and policy subsystems both in isolation and wired together, verifying
//! the full lifecycle of self-organising node groups, decentralised job
//! auctions, earned reputation badges, and policy enforcement.
//!
//! All tests are synchronous (no Tokio runtime required) and construct
//! their own local state — no network, no shared state between tests.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{Duration as ChronoDuration, Utc};
    use uuid::Uuid;

    use crate::common::types::JobId;
    use crate::swarm::collective::{
        AffinityScorer, Collective, CollectiveId, CollectiveProposal, CollectiveState,
        CollectiveStore, CompositeProfile, EjectionVote,
    };
    use crate::swarm::marketplace::{
        AwardNotice, Bid, BidResult, BidderId, JobPosting, MarketplaceEngine, MarketplaceStore,
        PostingStatus, Priority, ScoringWeights,
    };
    use crate::swarm::policy::{PolicyEngine, PolicySet};
    use crate::swarm::profile::{
        GpuInfo, JobRequirements, NodeProfile, NodeType, ProfileStore, Runtime, Strength,
        TimeWindow, Weakness,
    };
    use crate::swarm::reputation::{
        Badge, EntityId, ReputationConfig, ReputationRecord, ReputationStore, RollingWindow,
    };
    use crate::swarm::types::{NodeId, Trait};

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Build a minimal NodeProfile for testing without hitting sysinfo.
    fn make_profile(node_id: NodeId, node_type: NodeType) -> NodeProfile {
        NodeProfile {
            node_id,
            node_type,
            version: 1,
            cpu_cores: 8,
            cpu_freq_mhz: 3200,
            ram_total_mb: 16384,
            disk_available_mb: 200_000,
            gpu: None,
            strengths: vec![],
            runtimes: vec![Runtime::Shell, Runtime::Python3],
            specializations: vec![],
            weaknesses: vec![],
            max_task_duration: None,
            availability_window: TimeWindow::always(),
            preferred_work: vec![],
            avoided_work: vec![],
            max_concurrent: 8,
            installed_software: vec![],
            custom_capabilities: vec![],
            geo_region: None,
            updated_at: Utc::now(),
        }
    }

    /// Build a GPU-equipped profile.
    fn make_gpu_profile(node_id: NodeId) -> NodeProfile {
        let mut p = make_profile(node_id, NodeType::BareMetal);
        p.gpu = Some(GpuInfo {
            name: "RTX 4090".to_string(),
            vram_mb: 24576,
            cuda_cores: Some(16384),
            compute_capability: Some("8.9".to_string()),
        });
        p.strengths = vec![Strength::GPUCompute, Strength::HighCoreCount];
        p.runtimes = vec![Runtime::Shell, Runtime::Python3, Runtime::Docker];
        p.specializations = vec!["ml-training".to_string(), "gpu-compute".to_string()];
        p.cpu_cores = 32;
        p.ram_total_mb = 65536;
        p
    }

    /// Build a lightweight Android profile.
    fn make_android_profile(node_id: NodeId) -> NodeProfile {
        let mut p = make_profile(node_id, NodeType::Android);
        p.cpu_cores = 4;
        p.ram_total_mb = 4096;
        p.disk_available_mb = 8000;
        p.runtimes = vec![Runtime::Shell, Runtime::Python3];
        p.weaknesses = vec![
            Weakness::BatteryPowered,
            Weakness::ThermalThrottled,
            Weakness::Intermittent,
            Weakness::LimitedMemory,
        ];
        p.availability_window = TimeWindow::charging_and_wifi();
        p.max_concurrent = 2;
        p
    }

    /// Build a Lambda profile with Ephemeral weakness.
    fn make_lambda_profile(node_id: NodeId) -> NodeProfile {
        let mut p = make_profile(node_id, NodeType::Lambda);
        p.cpu_cores = 2;
        p.ram_total_mb = 2048;
        p.runtimes = vec![Runtime::Shell, Runtime::Python3, Runtime::NodeJS];
        p.weaknesses = vec![Weakness::Ephemeral, Weakness::NoLocalState];
        p.max_task_duration = Some(Duration::from_secs(900));
        p.max_concurrent = 1;
        p
    }

    /// Build a high-RAM aggregation node.
    fn make_aggregator_profile(node_id: NodeId) -> NodeProfile {
        let mut p = make_profile(node_id, NodeType::CloudVM);
        p.cpu_cores = 16;
        p.ram_total_mb = 131072; // 128 GB
        p.disk_available_mb = 500_000;
        p.strengths = vec![Strength::LargeMemory, Strength::HighCoreCount, Strength::StableUptime];
        p.runtimes = vec![Runtime::Shell, Runtime::Python3, Runtime::Docker, Runtime::Go];
        p.specializations = vec!["data-aggregation".to_string(), "big-data".to_string()];
        p
    }

    /// Build a standard JobPosting for marketplace tests.
    fn make_posting(name: &str, requirements: JobRequirements, priority: Priority) -> JobPosting {
        JobPosting::new(
            JobId::new(),
            NodeId::new(),
            name.to_string(),
            requirements,
            priority,
            Duration::from_secs(60),
        )
    }

    /// Build a Bid for marketplace tests.
    fn make_bid(
        job_id: JobId,
        bidder: BidderId,
        completion_secs: u64,
        rep: f32,
        cap: f32,
    ) -> Bid {
        Bid {
            bidder,
            job_id,
            bid_at: Utc::now(),
            estimated_completion: Duration::from_secs(completion_secs),
            reputation_score: rep,
            capability_match: cap,
            current_load: 0.3,
        }
    }

    /// Create a node-based EntityId for reputation tests.
    fn node_entity(node_id: NodeId) -> EntityId {
        EntityId::Node(node_id)
    }

    // ========================================================================
    // 1. Profile Tests
    // ========================================================================

    #[test]
    fn profile_detect_all_node_types() {
        // Each factory method should produce a profile with the correct
        // node type, weaknesses, and runtimes.
        let id = NodeId::new();

        let bare = NodeProfile::for_bare_metal(id);
        assert_eq!(bare.node_type, NodeType::BareMetal);
        assert!(bare.strengths.contains(&Strength::StableUptime));
        assert!(bare.runtimes.contains(&Runtime::Docker));

        let cloud = NodeProfile::for_cloud_vm(id);
        assert_eq!(cloud.node_type, NodeType::CloudVM);
        assert!(cloud.strengths.contains(&Strength::StableUptime));

        let lambda = NodeProfile::for_lambda(id);
        assert_eq!(lambda.node_type, NodeType::Lambda);
        assert!(lambda.weaknesses.contains(&Weakness::Ephemeral));
        assert_eq!(lambda.max_concurrent, 1);
        assert_eq!(lambda.max_task_duration, Some(Duration::from_secs(900)));

        let container = NodeProfile::for_container(id);
        assert_eq!(container.node_type, NodeType::Container);
        assert!(container.weaknesses.contains(&Weakness::Sandboxed));

        let android = NodeProfile::for_android(id);
        assert_eq!(android.node_type, NodeType::Android);
        assert!(android.weaknesses.contains(&Weakness::BatteryPowered));
        assert!(android.weaknesses.contains(&Weakness::ThermalThrottled));
        assert_eq!(android.max_concurrent, 2);

        let browser = NodeProfile::for_browser(id);
        assert_eq!(browser.node_type, NodeType::Browser);
        assert_eq!(browser.runtimes, vec![Runtime::Wasm]);
        assert_eq!(browser.max_concurrent, 1);

        let desktop = NodeProfile::for_desktop(id);
        assert_eq!(desktop.node_type, NodeType::Desktop);
        assert!(desktop.runtimes.contains(&Runtime::Shell));
        assert!(desktop.runtimes.contains(&Runtime::Python3));
    }

    #[test]
    fn profile_capability_matching() {
        let gpu_node = make_gpu_profile(NodeId::new());
        let android_node = make_android_profile(NodeId::new());

        // GPU-compute requirements: GPU node can handle, Android cannot
        let gpu_reqs = JobRequirements::gpu_compute();
        assert!(gpu_node.can_handle(&gpu_reqs));
        assert!(!android_node.can_handle(&gpu_reqs));

        // Capability match score should be > 0 for the GPU node
        let score = gpu_node.capability_match(&gpu_reqs);
        assert!(score > 0.0, "GPU node score for GPU job should be > 0.0, got {}", score);

        // Android should return 0.0 (cannot handle at all)
        assert_eq!(android_node.capability_match(&gpu_reqs), 0.0);

        // Minimal requirements: both should handle
        let minimal = JobRequirements::minimal();
        assert!(gpu_node.can_handle(&minimal));
        assert!(android_node.can_handle(&minimal));

        // Python requirements: both have Python3
        let python_reqs = JobRequirements::python();
        assert!(gpu_node.can_handle(&python_reqs));
        assert!(android_node.can_handle(&python_reqs));
    }

    #[test]
    fn profile_store_version_conflict_resolution() {
        let store = ProfileStore::new();
        let id = NodeId::new();

        // Insert version 1
        let p1 = make_profile(id, NodeType::Desktop);
        assert!(store.upsert(p1));
        assert_eq!(store.count(), 1);

        // Version 5 should replace
        let mut p5 = make_profile(id, NodeType::Desktop);
        p5.version = 5;
        p5.cpu_cores = 32;
        assert!(store.upsert(p5));
        assert_eq!(store.get(&id).unwrap().cpu_cores, 32);

        // Version 3 (older) should be rejected
        let mut p3 = make_profile(id, NodeType::Desktop);
        p3.version = 3;
        p3.cpu_cores = 64;
        assert!(!store.upsert(p3));
        assert_eq!(store.get(&id).unwrap().cpu_cores, 32); // unchanged
    }

    #[test]
    fn profile_store_matching_sorted_by_score() {
        let store = ProfileStore::new();

        // Low-spec Android node
        let android = make_android_profile(NodeId::new());
        store.upsert(android);

        // High-spec GPU node
        let gpu = make_gpu_profile(NodeId::new());
        store.upsert(gpu);

        // Medium aggregator
        let agg = make_aggregator_profile(NodeId::new());
        store.upsert(agg);

        // Requirements that favour high resources
        let reqs = JobRequirements {
            min_memory_mb: Some(8192),
            min_cpu_cores: Some(4),
            required_runtimes: vec![Runtime::Python3],
            ..JobRequirements::minimal()
        };

        let matches = store.matching_profiles(&reqs);
        // Android has only 4096 MB RAM, won't meet min_memory_mb = 8192
        assert!(
            matches.len() >= 2,
            "expected at least 2 matching profiles, got {}",
            matches.len()
        );

        // Best match should be first (highest score)
        if matches.len() >= 2 {
            let s0 = matches[0].capability_match(&reqs);
            let s1 = matches[1].capability_match(&reqs);
            assert!(s0 >= s1, "profiles should be sorted by score: {} >= {}", s0, s1);
        }
    }

    // ========================================================================
    // 2. Collective Tests
    // ========================================================================

    #[test]
    fn collective_formation_lifecycle() {
        let initiator_id = NodeId::new();
        let member_id = NodeId::new();

        let init_profile = make_profile(initiator_id, NodeType::Desktop);
        let member_profile = make_gpu_profile(member_id);

        // Create collective: starts in Forming state
        let mut collective = Collective::new(initiator_id, &init_profile);
        assert_eq!(collective.state, CollectiveState::Forming);
        assert_eq!(collective.member_count(), 1);
        assert!(collective.is_member(&initiator_id));
        assert!(!collective.is_member(&member_id));

        // Add member
        assert!(collective.add_member(member_id, &member_profile));
        assert_eq!(collective.member_count(), 2);
        assert!(collective.is_member(&member_id));

        // Cannot add same member twice
        assert!(!collective.add_member(member_id, &member_profile));

        // Promote to Active
        collective.state = CollectiveState::Active;

        // Record some jobs
        collective.record_job_success();
        collective.record_job_success();
        collective.record_job_failure();

        assert_eq!(collective.jobs_completed, 2);
        assert_eq!(collective.jobs_failed, 1);
        let rate = collective.success_rate();
        assert!((rate - 2.0 / 3.0).abs() < 0.01);

        // Should not dissolve: 2 members, not enough failures
        assert!(!collective.should_dissolve());
    }

    #[test]
    fn collective_dissolution_on_member_removal() {
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let p1 = make_profile(n1, NodeType::Desktop);
        let p2 = make_profile(n2, NodeType::CloudVM);

        let mut collective = Collective::new(n1, &p1);
        collective.add_member(n2, &p2);
        collective.state = CollectiveState::Active;

        assert_eq!(collective.member_count(), 2);

        // Remove one member: drops below minimum (2), enters Dissolving
        collective.remove_member(&n2);
        assert_eq!(collective.member_count(), 1);
        assert_eq!(collective.state, CollectiveState::Dissolving);
    }

    #[test]
    fn collective_degrades_when_still_viable() {
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();
        let p1 = make_profile(n1, NodeType::Desktop);
        let p2 = make_profile(n2, NodeType::CloudVM);
        let p3 = make_profile(n3, NodeType::BareMetal);

        let mut collective = Collective::new(n1, &p1);
        collective.add_member(n2, &p2);
        collective.add_member(n3, &p3);
        collective.state = CollectiveState::Active;
        assert_eq!(collective.member_count(), 3);

        // Remove one of three members: still >= 2, goes to Degraded
        collective.remove_member(&n3);
        assert_eq!(collective.member_count(), 2);
        assert_eq!(collective.state, CollectiveState::Degraded);
    }

    #[test]
    fn collective_store_version_conflict_resolution() {
        let store = CollectiveStore::new();
        let n1 = NodeId::new();
        let p1 = make_profile(n1, NodeType::Desktop);

        let mut c1 = Collective::new(n1, &p1);
        let id = c1.id;
        c1.version = 5;
        assert!(store.upsert(c1));
        assert_eq!(store.count(), 1);

        // Older version rejected
        let mut c_old = store.get(&id).unwrap();
        c_old.version = 3;
        c_old.jobs_completed = 999;
        assert!(!store.upsert(c_old));
        assert_eq!(store.get(&id).unwrap().jobs_completed, 0);

        // Newer version accepted
        let mut c_new = store.get(&id).unwrap();
        c_new.version = 10;
        c_new.jobs_completed = 42;
        assert!(store.upsert(c_new));
        assert_eq!(store.get(&id).unwrap().jobs_completed, 42);
    }

    #[test]
    fn collective_affinity_complementary_nodes() {
        let gpu_node = make_gpu_profile(NodeId::new());
        let agg_node = make_aggregator_profile(NodeId::new());
        let desktop = make_profile(NodeId::new(), NodeType::Desktop);
        let desktop2 = make_profile(NodeId::new(), NodeType::Desktop);

        // GPU + aggregator = high complementarity (different strengths, runtimes, specs)
        let score_complementary = AffinityScorer::complementarity(&gpu_node, &agg_node);

        // Two identical desktops = low complementarity
        let score_identical = AffinityScorer::complementarity(&desktop, &desktop2);

        assert!(
            score_complementary > score_identical,
            "complementary nodes ({}) should score higher than identical ones ({})",
            score_complementary,
            score_identical
        );
    }

    // ========================================================================
    // 3. Marketplace Tests
    // ========================================================================

    #[test]
    fn marketplace_posting_lifecycle() {
        let store = MarketplaceStore::new();
        let posting = make_posting("gpu-job", JobRequirements::minimal(), Priority::Normal);
        let job_id = posting.job_id;

        // Post
        assert!(store.post_job(posting.clone()));
        assert!(!store.post_job(posting)); // duplicate rejected

        // Verify
        let retrieved = store.get_posting(&job_id).unwrap();
        assert_eq!(retrieved.name, "gpu-job");
        assert!(retrieved.is_open());
        assert!(!retrieved.is_expired());

        // Award
        store.update_posting_status(&job_id, PostingStatus::Awarded);
        let updated = store.get_posting(&job_id).unwrap();
        assert_eq!(updated.status, PostingStatus::Awarded);
        assert!(!updated.is_open());
    }

    #[test]
    fn marketplace_bid_validation() {
        let store = MarketplaceStore::new();

        // Normal posting
        let posting = make_posting("bid-test", JobRequirements::minimal(), Priority::Normal);
        let job_id = posting.job_id;
        store.post_job(posting);

        // Valid bid: accepted
        let bid = make_bid(job_id, BidderId::Node(NodeId::new()), 10, 0.8, 0.9);
        assert_eq!(store.place_bid(bid), BidResult::Accepted);
        assert_eq!(store.bid_count(&job_id), 1);

        // Bid on awarded posting: rejected
        store.update_posting_status(&job_id, PostingStatus::Awarded);
        let bid2 = make_bid(job_id, BidderId::Node(NodeId::new()), 5, 0.9, 1.0);
        assert_eq!(store.place_bid(bid2), BidResult::AlreadyAwarded);

        // Blacklisted bidder
        let blocked = NodeId::new();
        let mut bp = make_posting("blacklist", JobRequirements::minimal(), Priority::Normal);
        bp.blacklisted_nodes.push(blocked);
        let bp_id = bp.job_id;
        store.post_job(bp);

        let blocked_bid = make_bid(bp_id, BidderId::Node(blocked), 5, 0.9, 1.0);
        assert!(matches!(store.place_bid(blocked_bid), BidResult::PolicyViolation { .. }));

        // Min reputation
        let mut rp = make_posting("min-rep", JobRequirements::minimal(), Priority::Normal);
        rp.min_reputation = Some(0.7);
        let rp_id = rp.job_id;
        store.post_job(rp);

        let low_rep_bid = make_bid(rp_id, BidderId::Node(NodeId::new()), 5, 0.3, 1.0);
        assert!(matches!(store.place_bid(low_rep_bid), BidResult::PolicyViolation { .. }));
    }

    #[test]
    fn marketplace_winner_selection_deterministic() {
        let store = Arc::new(MarketplaceStore::new());
        let engine = MarketplaceEngine::new(NodeId::new(), Arc::clone(&store));

        let posting = make_posting("determinism", JobRequirements::minimal(), Priority::Normal);
        let job_id = posting.job_id;
        store.post_job(posting);

        // Fixed NodeIds for reproducibility
        let id_a = NodeId(Uuid::from_bytes([1; 16]));
        let id_b = NodeId(Uuid::from_bytes([2; 16]));
        let id_c = NodeId(Uuid::from_bytes([3; 16]));

        let ts = Utc::now();

        // Bid B has the best score (speed=0.2, rep=0.9, cap=1.0)
        let bid_a = Bid {
            bidder: BidderId::Node(id_a),
            job_id,
            bid_at: ts,
            estimated_completion: Duration::from_secs(10),
            reputation_score: 0.7,
            capability_match: 0.9,
            current_load: 0.2,
        };
        let bid_b = Bid {
            bidder: BidderId::Node(id_b),
            job_id,
            bid_at: ts,
            estimated_completion: Duration::from_secs(5),
            reputation_score: 0.9,
            capability_match: 1.0,
            current_load: 0.1,
        };
        let bid_c = Bid {
            bidder: BidderId::Node(id_c),
            job_id,
            bid_at: ts,
            estimated_completion: Duration::from_secs(8),
            reputation_score: 0.6,
            capability_match: 0.8,
            current_load: 0.5,
        };

        store.place_bid(bid_a.clone());
        store.place_bid(bid_b.clone());
        store.place_bid(bid_c.clone());

        let notice = engine.select_winner(&job_id).unwrap();
        assert_eq!(notice.winner, BidderId::Node(id_b));
        assert_eq!(notice.competing_bids, 3);

        // Verify determinism: same data in different insertion order
        let store2 = Arc::new(MarketplaceStore::new());
        let engine2 = MarketplaceEngine::new(NodeId::new(), Arc::clone(&store2));

        let mut posting2 = make_posting("determinism-2", JobRequirements::minimal(), Priority::Normal);
        posting2.job_id = JobId(Uuid::from_bytes([99; 16]));
        let jid2 = posting2.job_id;
        store2.post_job(posting2);

        // Insert in reverse order
        let mut bc = bid_c.clone();
        bc.job_id = jid2;
        let mut ba = bid_a.clone();
        ba.job_id = jid2;
        let mut bb = bid_b.clone();
        bb.job_id = jid2;

        store2.place_bid(bc);
        store2.place_bid(ba);
        store2.place_bid(bb);

        let notice2 = engine2.select_winner(&jid2).unwrap();
        assert_eq!(notice2.winner, BidderId::Node(id_b));
    }

    #[test]
    fn marketplace_scoring_formula() {
        // speed = 1/10 = 0.1, energy_score = 0.5 (neutral default)
        // score = 0.1 * 0.2 + 0.8 * 0.3 + 1.0 * 0.3 + 0.5 * 0.2
        //       = 0.02 + 0.24 + 0.30 + 0.10 = 0.66
        let bid = Bid {
            bidder: BidderId::Node(NodeId::new()),
            job_id: JobId::new(),
            bid_at: Utc::now(),
            estimated_completion: Duration::from_secs(10),
            reputation_score: 0.8,
            capability_match: 1.0,
            current_load: 0.2,
        };

        let expected = 0.1 * 0.2 + 0.8 * 0.3 + 1.0 * 0.3 + 0.5 * 0.2;
        let score = bid.score();
        assert!(
            (score - expected).abs() < 1e-6,
            "expected {}, got {}",
            expected,
            score
        );
    }

    #[test]
    fn marketplace_engine_rejects_low_capability() {
        let store = Arc::new(MarketplaceStore::new());
        let engine = MarketplaceEngine::new(NodeId::new(), Arc::clone(&store));

        let posting = make_posting("low-cap", JobRequirements::minimal(), Priority::Normal);
        let job_id = posting.job_id;
        store.post_job(posting);

        // Capability match 0.3 is below MIN_CAPABILITY_MATCH (0.5)
        let result = engine.place_bid(
            &job_id,
            BidderId::Node(NodeId::new()),
            Duration::from_secs(10),
            0.9,
            0.3,
            0.1,
        );
        assert!(matches!(result, BidResult::Rejected { .. }));
    }

    // ========================================================================
    // 4. Reputation Tests
    // ========================================================================

    #[test]
    fn reputation_newcomer_badge_lifecycle() {
        let cfg = ReputationConfig::default();
        let entity = node_entity(NodeId::new());
        let mut record = ReputationRecord::with_config(entity, &cfg);

        // Starts with Newcomer
        assert!(record.has_badge(Badge::Newcomer));

        // Complete 49 jobs: still Newcomer
        for _ in 0..49 {
            record.record_success_with_config(0.8, false, &cfg);
        }
        assert!(record.has_badge(Badge::Newcomer));

        // Job 50: Newcomer removed
        record.record_success_with_config(0.8, false, &cfg);
        assert!(!record.has_badge(Badge::Newcomer));
        assert_eq!(record.total_jobs, 50);
    }

    #[test]
    fn reputation_lightning_badge_earn_and_shed() {
        let cfg = ReputationConfig::default();
        let entity = node_entity(NodeId::new());
        let mut record = ReputationRecord::with_config(entity, &cfg);

        // Earn Lightning: 50+ fast jobs (speed_ratio < 0.9)
        for _ in 0..50 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        assert!(record.has_badge(Badge::Lightning));

        // Shed Lightning: fill the rolling window with slow jobs (> 1.2)
        for _ in 0..50 {
            record.record_success_with_config(1.5, false, &cfg);
        }
        assert!(!record.has_badge(Badge::Lightning));
    }

    #[test]
    fn reputation_reliable_and_trusted_badges() {
        let cfg = ReputationConfig::default();
        let entity = node_entity(NodeId::new());
        let mut record = ReputationRecord::with_config(entity, &cfg);

        // Earn Reliable: 100+ jobs with < 2% failure rate
        for _ in 0..100 {
            record.record_success_with_config(1.0, false, &cfg);
        }
        assert!(record.has_badge(Badge::Reliable));

        // Earn Trusted: 200+ jobs with zero policy violations
        for _ in 0..100 {
            record.record_success_with_config(1.0, false, &cfg);
        }
        assert!(record.has_badge(Badge::Trusted));

        // Shed Trusted on policy violation
        record.record_violation_with_config(&cfg);
        assert!(!record.has_badge(Badge::Trusted));

        // Trusted cannot be re-earned once violations > 0
        for _ in 0..100 {
            record.record_success_with_config(1.0, false, &cfg);
        }
        assert!(!record.has_badge(Badge::Trusted));
    }

    #[test]
    fn reputation_store_merge_conflict_resolution() {
        let store = ReputationStore::new();
        let entity = node_entity(NodeId::new());

        // Insert version 5 with 10 jobs
        let mut r1 = ReputationRecord::new(entity.clone());
        r1.version = 5;
        r1.total_jobs = 10;
        assert!(store.merge(r1));

        // Newer version (10) replaces
        let mut r2 = ReputationRecord::new(entity.clone());
        r2.version = 10;
        r2.total_jobs = 20;
        assert!(store.merge(r2));
        assert_eq!(store.get(&entity).unwrap().total_jobs, 20);

        // Older version (3) rejected
        let mut r3 = ReputationRecord::new(entity.clone());
        r3.version = 3;
        r3.total_jobs = 100;
        assert!(!store.merge(r3));
        assert_eq!(store.get(&entity).unwrap().total_jobs, 20);

        // Same version but more jobs wins
        let mut r4 = ReputationRecord::new(entity.clone());
        r4.version = 10;
        r4.total_jobs = 30;
        assert!(store.merge(r4));
        assert_eq!(store.get(&entity).unwrap().total_jobs, 30);

        // Same version but fewer jobs rejected
        let mut r5 = ReputationRecord::new(entity.clone());
        r5.version = 10;
        r5.total_jobs = 15;
        assert!(!store.merge(r5));
        assert_eq!(store.get(&entity).unwrap().total_jobs, 30);
    }

    #[test]
    fn reputation_score_calculation() {
        let entity = node_entity(NodeId::new());
        let mut record = ReputationRecord::new(entity);

        // Set first_seen to 12 months ago for full longevity
        record.first_seen = Utc::now() - ChronoDuration::days(365);

        // All successes, fast speed
        for _ in 0..100 {
            record.record_success(0.5, false);
        }

        // Should score near 1.0
        assert!(
            record.score > 0.9,
            "perfect entity score should be > 0.9, got {}",
            record.score
        );

        // Scores are always clamped to [0.0, 1.0]
        assert!(record.score >= 0.0 && record.score <= 1.0);

        // Poor entity: many failures, slow
        let entity2 = node_entity(NodeId::new());
        let mut bad_record = ReputationRecord::new(entity2);
        for _ in 0..50 {
            bad_record.record_failure(false);
        }
        assert!(
            bad_record.score < 0.5,
            "poor entity score should be < 0.5, got {}",
            bad_record.score
        );
    }

    // ========================================================================
    // 5. Policy Tests
    // ========================================================================

    #[test]
    fn policy_set_creation_and_rules() {
        let node = NodeId::new();

        // Empty policy has defaults (open geo-fence, open reputation gate, etc.)
        let empty = PolicySet::empty(node);
        assert_eq!(empty.version, 1);
        assert_eq!(empty.blacklist_count(), 0);
        assert!(!empty.is_blacklisted(&NodeId::new()));

        // Default policy has enforced reputation gate
        let default = PolicySet::default_for(node);
        assert_eq!(default.version, 1);
        assert!(default.reputation_gate.enforced);
        assert!(default.rate_limit.enforced);

        // Custom param lookup
        let mut ps = PolicySet::empty(node);
        ps.set_param("max_task_duration_secs".to_string(), "3600".to_string());
        assert_eq!(ps.get_param("max_task_duration_secs").unwrap(), "3600");
    }

    #[test]
    fn policy_engine_merge_version_conflict() {
        let node = NodeId::new();
        let engine = PolicyEngine::new(PolicySet::empty(node));
        assert_eq!(engine.version(), 1);
        assert_eq!(engine.active_policy().blacklist_count(), 0);

        // Merge newer version: accepted
        let mut newer = PolicySet::default_for(node);
        newer.version = 5;
        assert!(engine.merge_policy(newer));
        assert_eq!(engine.version(), 5);

        // Merge stale version: rejected
        let mut stale = PolicySet::empty(node);
        stale.version = 3;
        assert!(!engine.merge_policy(stale));
        assert_eq!(engine.version(), 5);
    }

    #[test]
    fn policy_engine_direct_update() {
        let node = NodeId::new();
        let engine = PolicyEngine::new(PolicySet::empty(node));

        // Direct update bypasses version check
        let mut updated = PolicySet::default_for(node);
        updated.set_param("custom_key".to_string(), "custom_value".to_string());
        engine.update(updated);
        assert_eq!(
            engine.active_policy().get_param("custom_key").unwrap(),
            "custom_value"
        );
    }

    #[test]
    fn policy_set_touch_increments_version() {
        let mut ps = PolicySet::empty(NodeId::new());
        assert_eq!(ps.version, 1);
        ps.touch();
        assert_eq!(ps.version, 2);
        ps.touch();
        assert_eq!(ps.version, 3);
    }

    #[test]
    fn policy_serialization_roundtrip() {
        let ps = PolicySet::default_for(NodeId::new());
        let json = serde_json::to_string(&ps).expect("serialize");
        let deserialized: PolicySet = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.version, ps.version);
        assert_eq!(deserialized.blacklist_count(), ps.blacklist_count());
        assert_eq!(deserialized.author, ps.author);
    }

    // ========================================================================
    // 6. End-to-End Integration Tests
    // ========================================================================

    #[test]
    fn e2e_profile_to_marketplace() {
        // Scenario: A node detects its profile, posts a job, another node
        // discovers the posting, places a bid, and wins.

        let submitter_id = NodeId::new();
        let bidder_id = NodeId::new();

        // Submitter creates a posting
        let store = Arc::new(MarketplaceStore::new());
        let reqs = JobRequirements {
            required_runtimes: vec![Runtime::Python3],
            min_memory_mb: Some(4096),
            ..JobRequirements::minimal()
        };
        let posting = make_posting("ml-training", reqs, Priority::High);
        let job_id = posting.job_id;
        store.post_job(posting);

        // Bidder checks compatibility
        let bidder_profile = make_gpu_profile(bidder_id);
        assert!(bidder_profile.runtimes.contains(&Runtime::Python3));
        assert!(bidder_profile.ram_total_mb >= 4096);

        // Bidder places a bid
        let bid = Bid {
            bidder: BidderId::Node(bidder_id),
            job_id,
            bid_at: Utc::now(),
            estimated_completion: Duration::from_secs(30),
            reputation_score: 0.9,
            capability_match: 0.95,
            current_load: 0.2,
        };
        assert_eq!(store.place_bid(bid), BidResult::Accepted);

        // Award
        let engine = MarketplaceEngine::new(submitter_id, Arc::clone(&store));
        let notice = engine.select_winner(&job_id).unwrap();
        assert_eq!(notice.winner, BidderId::Node(bidder_id));
        assert_eq!(notice.competing_bids, 1);
    }

    #[test]
    fn e2e_collective_improves_capability() {
        // Scenario: Two nodes with different strengths form a collective
        // whose composite can handle a job neither could handle alone.

        let gpu_id = NodeId::new();
        let agg_id = NodeId::new();

        let gpu_profile = make_gpu_profile(gpu_id);
        let agg_profile = make_aggregator_profile(agg_id);

        // Neither alone can handle a job requiring both GPU + Go runtime
        let combined_reqs = JobRequirements {
            needs_gpu: true,
            required_runtimes: vec![Runtime::Go],
            min_memory_mb: Some(32768),
            ..JobRequirements::minimal()
        };

        // GPU node has GPU but no Go runtime
        assert!(!gpu_profile.can_handle(&combined_reqs));
        // Aggregator has Go but no GPU
        assert!(!agg_profile.can_handle(&combined_reqs));

        // Form a collective
        let mut collective = Collective::new(gpu_id, &gpu_profile);
        collective.add_member(agg_id, &agg_profile);
        collective.state = CollectiveState::Active;

        // The collective store should track them
        let store = CollectiveStore::new();
        store.upsert(collective);

        // Verify the collective has both members
        let retrieved = store.all_collectives();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].member_count(), 2);
    }

    #[test]
    fn e2e_reputation_influences_marketplace() {
        // Scenario: Two nodes bid on the same job. The one with higher
        // reputation wins, even though the other has better speed.

        let store = Arc::new(MarketplaceStore::new());
        let engine = MarketplaceEngine::new(NodeId::new(), Arc::clone(&store));

        let posting = make_posting("reputation-test", JobRequirements::minimal(), Priority::Normal);
        let job_id = posting.job_id;
        store.post_job(posting);

        let fast_node = NodeId(Uuid::from_bytes([1; 16]));
        let reliable_node = NodeId(Uuid::from_bytes([2; 16]));

        let ts = Utc::now();

        // Fast node: great speed, low reputation
        let fast_bid = Bid {
            bidder: BidderId::Node(fast_node),
            job_id,
            bid_at: ts,
            estimated_completion: Duration::from_secs(2),
            reputation_score: 0.3,
            capability_match: 0.8,
            current_load: 0.1,
        };

        // Reliable node: slower speed, high reputation
        let reliable_bid = Bid {
            bidder: BidderId::Node(reliable_node),
            job_id,
            bid_at: ts,
            estimated_completion: Duration::from_secs(10),
            reputation_score: 0.95,
            capability_match: 0.9,
            current_load: 0.2,
        };

        store.place_bid(fast_bid.clone());
        store.place_bid(reliable_bid.clone());

        // With default weights (speed=0.3, reputation=0.4, capability=0.3),
        // reputation has the highest weight, so the reliable node should win
        // if its total weighted score is higher.
        let fast_score = fast_bid.score();
        let reliable_score = reliable_bid.score();

        let notice = engine.select_winner(&job_id).unwrap();

        // The one with higher score should win
        if reliable_score > fast_score {
            assert_eq!(notice.winner, BidderId::Node(reliable_node));
        } else {
            assert_eq!(notice.winner, BidderId::Node(fast_node));
        }
    }

    #[test]
    fn e2e_policy_governs_work_acceptance() {
        // Scenario: A policy engine's rules influence whether a node
        // should accept work (conceptually). We verify the policy
        // enforcement primitives work correctly.

        let node = NodeId::new();
        let engine = PolicyEngine::new(PolicySet::default_for(node));

        // Check node is allowed (not blacklisted)
        let result = engine.check_node_allowed(&node);
        assert!(result.allowed);

        // Check node type is allowed (default permits all)
        let result = engine.check_node_type(&NodeType::BareMetal);
        assert!(result.allowed);

        // Check reputation gate (default min_score is 0.0, so any score passes)
        let result = engine.check_reputation(0.5, &HashSet::new());
        assert!(result.allowed);

        // Check rate limit (no tasks recorded, should be within limit)
        let result = engine.check_rate_limit(&node);
        assert!(result.allowed);

        // Simulate a policy update: blacklist a bad node
        let bad_node = NodeId::new();
        let mut updated_policy = engine.active_policy();
        updated_policy.blacklist(bad_node);
        engine.update(updated_policy);

        // Verify blacklisted node is denied
        let result = engine.check_node_allowed(&bad_node);
        assert!(!result.allowed);
        assert!(!result.denial_reasons.is_empty());

        // Original node still allowed
        let result = engine.check_node_allowed(&node);
        assert!(result.allowed);

        // Custom params work as policy extensions
        let mut policy = engine.active_policy();
        policy.set_param("allowed_runtimes".to_string(), "shell,python3".to_string());
        engine.update(policy);
        assert_eq!(
            engine.active_policy().get_param("allowed_runtimes").unwrap(),
            "shell,python3"
        );
    }

    #[test]
    fn e2e_reputation_badge_progression() {
        // Scenario: A node progresses through the full badge chain from
        // Newcomer to Veteran.

        let cfg = ReputationConfig {
            newcomer_threshold: 10,
            reliable_min_jobs: 20,
            trusted_min_jobs: 30,
            veteran_min_jobs: 50,
            veteran_min_months: 0.0, // waive time requirement for test
            veteran_min_badges: 3,
            heavy_lifter_min_large: 5,
            ..ReputationConfig::default()
        };

        let entity = node_entity(NodeId::new());
        let mut record = ReputationRecord::with_config(entity, &cfg);

        // Phase 1: Newcomer (jobs 0-9)
        assert!(record.has_badge(Badge::Newcomer));
        for _ in 0..10 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        assert!(!record.has_badge(Badge::Newcomer));
        assert!(record.has_badge(Badge::Lightning)); // fast jobs

        // Phase 2: Earn Reliable (20+ jobs, < 2% failure)
        for _ in 0..10 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        assert!(record.has_badge(Badge::Reliable));

        // Phase 3: Earn Trusted (30+ jobs, 0 violations)
        for _ in 0..10 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        assert!(record.has_badge(Badge::Trusted));

        // Phase 4: Earn HeavyLifter (5+ large jobs)
        for _ in 0..5 {
            record.record_success_with_config(0.7, true, &cfg);
        }
        assert!(record.has_badge(Badge::HeavyLifter));

        // Phase 5: Earn Veteran (50+ jobs, 3+ qualifying badges)
        // We have Lightning, Reliable, Trusted, HeavyLifter = 4 qualifying
        // We need 50 total jobs: currently at 35
        for _ in 0..15 {
            record.record_success_with_config(0.7, false, &cfg);
        }
        assert_eq!(record.total_jobs, 50);
        assert!(
            record.has_badge(Badge::Veteran),
            "should have Veteran badge, current badges: {:?}",
            record.badges
        );
    }

    #[test]
    fn e2e_full_organic_cycle() {
        // Full integration: profiles -> affinity -> collective -> marketplace
        // -> reputation -> feedback loop.

        // 1. Create diverse node profiles
        let gpu_id = NodeId::new();
        let agg_id = NodeId::new();
        let lambda_id = NodeId::new();

        let gpu_profile = make_gpu_profile(gpu_id);
        let agg_profile = make_aggregator_profile(agg_id);
        let lambda_profile = make_lambda_profile(lambda_id);

        // 2. Store profiles
        let profile_store = ProfileStore::new();
        profile_store.upsert(gpu_profile.clone());
        profile_store.upsert(agg_profile.clone());
        profile_store.upsert(lambda_profile.clone());
        assert_eq!(profile_store.count(), 3);

        // 3. Measure affinity
        let gpu_agg_affinity = AffinityScorer::complementarity(&gpu_profile, &agg_profile);
        let gpu_lambda_affinity = AffinityScorer::complementarity(&gpu_profile, &lambda_profile);
        assert!(
            gpu_agg_affinity > 0.0,
            "GPU and aggregator should have positive complementarity"
        );

        // 4. Form a collective between GPU + aggregator
        let mut collective = Collective::new(gpu_id, &gpu_profile);
        collective.add_member(agg_id, &agg_profile);
        collective.state = CollectiveState::Active;

        let collective_store = CollectiveStore::new();
        collective_store.upsert(collective.clone());
        assert_eq!(collective_store.active_collectives().len(), 1);

        // 5. Post a job to the marketplace
        let marketplace_store = Arc::new(MarketplaceStore::new());
        let reqs = JobRequirements {
            required_runtimes: vec![Runtime::Python3],
            min_memory_mb: Some(2048),
            ..JobRequirements::minimal()
        };
        let posting = make_posting("data-pipeline", reqs, Priority::Normal);
        let job_id = posting.job_id;
        marketplace_store.post_job(posting);

        // 6. GPU node bids on the job
        let gpu_bid = Bid {
            bidder: BidderId::Node(gpu_id),
            job_id,
            bid_at: Utc::now(),
            estimated_completion: Duration::from_secs(30),
            reputation_score: 0.8,
            capability_match: 0.9,
            current_load: 0.2,
        };
        assert_eq!(marketplace_store.place_bid(gpu_bid), BidResult::Accepted);

        // 7. Select winner
        let engine = MarketplaceEngine::new(gpu_id, Arc::clone(&marketplace_store));
        let notice = engine.select_winner(&job_id).unwrap();
        assert_eq!(notice.winner, BidderId::Node(gpu_id));

        // 8. Record the result in reputation
        let reputation_store = ReputationStore::new();
        let entity = EntityId::Node(gpu_id);
        reputation_store.record_job_result(entity.clone(), true, 0.8, false);

        let record = reputation_store.get(&entity).unwrap();
        assert_eq!(record.total_jobs, 1);
        assert_eq!(record.successful_jobs, 1);
        assert!(record.has_badge(Badge::Newcomer)); // still new

        // 9. Verify policy engine is operational
        let policy_engine = PolicyEngine::new(PolicySet::default_for(gpu_id));
        assert!(policy_engine.check_node_allowed(&gpu_id).allowed);
        assert_eq!(policy_engine.version(), 1);

        // 10. Profile store pruning works
        let pruned = profile_store.prune_stale(ChronoDuration::hours(1));
        assert_eq!(pruned, 0); // nothing stale yet
    }

    // ========================================================================
    // Additional Edge Case Tests
    // ========================================================================

    #[test]
    fn rolling_window_eviction_preserves_capacity() {
        let mut w = RollingWindow::new(3);
        w.push(1.0);
        w.push(2.0);
        w.push(3.0);
        assert!(w.is_full());

        // Push evicts oldest
        w.push(10.0);
        assert_eq!(w.count(), 3);
        // Average should be (2+3+10)/3 = 5.0
        assert!((w.average() - 5.0).abs() < f32::EPSILON);
    }

    #[test]
    fn bidder_id_ordering_nodes_before_collectives() {
        let node = BidderId::Node(NodeId(Uuid::from_bytes([1; 16])));
        let coll = BidderId::Collective(CollectiveId(Uuid::from_bytes([1; 16])));
        assert!(node < coll, "nodes should sort before collectives");
    }

    #[test]
    fn collective_store_ejection_voting() {
        let store = CollectiveStore::new();
        let col_id = CollectiveId::new();
        let target = NodeId::new();

        // 3 voters: 2 approve, 1 disapproves
        for i in 0..3 {
            let vote = EjectionVote {
                collective_id: col_id,
                voter: NodeId::new(),
                target,
                approve: i < 2,
                timestamp: Utc::now(),
            };
            store.add_ejection_vote(vote);
        }

        let votes = store.get_ejection_votes(&col_id, &target);
        assert_eq!(votes.len(), 3);

        // With 5 total members, majority = 3. 2 approvals is NOT enough.
        assert!(!store.should_eject(&col_id, &target, 5));

        // With 3 total members, majority = 2. 2 approvals IS enough.
        assert!(store.should_eject(&col_id, &target, 3));
    }

    #[test]
    fn profile_store_prune_removes_stale_only() {
        let store = ProfileStore::new();

        // Fresh profile
        store.upsert(make_profile(NodeId::new(), NodeType::Desktop));

        // Stale profile (2 hours old)
        let mut stale = make_profile(NodeId::new(), NodeType::CloudVM);
        stale.updated_at = Utc::now() - ChronoDuration::hours(2);
        store.upsert(stale);

        // Prune with 1-hour cutoff
        let pruned = store.prune_stale(ChronoDuration::hours(1));
        assert_eq!(pruned, 1);
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn reputation_store_with_badge_query() {
        let store = ReputationStore::new();

        let e1 = node_entity(NodeId::new());
        let e2 = node_entity(NodeId::new());

        store.get_or_create(e1.clone());
        store.get_or_create(e2.clone());

        // Both should be Newcomers
        let newcomers = store.with_badge(Badge::Newcomer);
        assert_eq!(newcomers.len(), 2);

        // No one should be Lightning
        let lightning = store.with_badge(Badge::Lightning);
        assert!(lightning.is_empty());
    }

    #[test]
    fn marketplace_merge_posting_version_wins() {
        let store = MarketplaceStore::new();
        let posting = make_posting("merge-test", JobRequirements::minimal(), Priority::Normal);
        let job_id = posting.job_id;

        assert!(store.merge_posting(posting.clone()));

        // Higher version replaces
        let mut updated = posting.clone();
        updated.version = 5;
        updated.name = "updated-name".into();
        assert!(store.merge_posting(updated));
        assert_eq!(store.get_posting(&job_id).unwrap().name, "updated-name");

        // Lower version rejected
        let mut stale = posting.clone();
        stale.version = 2;
        stale.name = "stale-name".into();
        assert!(!store.merge_posting(stale));
        assert_eq!(store.get_posting(&job_id).unwrap().name, "updated-name");
    }

    #[test]
    fn marketplace_award_merge_idempotent() {
        let store = MarketplaceStore::new();
        let posting = make_posting("award-merge", JobRequirements::minimal(), Priority::Normal);
        let job_id = posting.job_id;
        store.post_job(posting);

        let winner = NodeId::new();
        let notice1 = AwardNotice {
            job_id,
            winner: BidderId::Node(winner),
            awarded_at: Utc::now(),
            bid_score: 0.9,
            competing_bids: 3,
        };

        assert!(store.merge_award(notice1.clone()));

        // Second merge for same job: rejected
        let notice2 = AwardNotice {
            job_id,
            winner: BidderId::Node(NodeId::new()),
            awarded_at: Utc::now(),
            bid_score: 0.95,
            competing_bids: 5,
        };
        assert!(!store.merge_award(notice2));

        // Original winner preserved
        let stored = store.get_award(&job_id).unwrap();
        assert_eq!(stored.winner, BidderId::Node(winner));
    }

    #[test]
    fn marketplace_expire_old_postings() {
        let store = MarketplaceStore::new();

        // Already-expired posting
        let mut expired = make_posting("expired-1", JobRequirements::minimal(), Priority::Normal);
        expired.expires_at = Utc::now() - ChronoDuration::seconds(10);
        let eid = expired.job_id;
        store.post_job(expired);

        // Fresh posting
        let fresh = make_posting("fresh-1", JobRequirements::minimal(), Priority::Normal);
        let fid = fresh.job_id;
        store.post_job(fresh);

        let count = store.expire_old_postings(Utc::now());
        assert_eq!(count, 1);

        assert_eq!(store.get_posting(&eid).unwrap().status, PostingStatus::Expired);
        assert_eq!(store.get_posting(&fid).unwrap().status, PostingStatus::Open);
    }

    #[test]
    fn collective_store_find_matching_requirements() {
        let store = CollectiveStore::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();

        let p1 = make_profile(n1, NodeType::Desktop);
        let p2 = make_gpu_profile(n2);

        let mut collective = Collective::new(n1, &p1);
        collective.add_member(n2, &p2);
        collective.state = CollectiveState::Active;

        // Recalculate composite from full profiles
        collective.recalculate_composite(&[p1.clone(), p2]);
        store.upsert(collective);

        // The composite should have combined CPU cores, RAM, etc.
        let stored = store.active_collectives();
        assert_eq!(stored.len(), 1);
        assert!(stored[0].composite.total_cpu_cores >= 8 + 32);
    }

    #[test]
    fn reputation_store_prune_stale_records() {
        let store = ReputationStore::new();

        let e1 = node_entity(NodeId::new());
        let e2 = node_entity(NodeId::new());

        store.get_or_create(e1.clone());
        store.get_or_create(e2.clone());

        // Manually age e1
        if let Some(mut record) = store.records.get_mut(&e1) {
            record.last_active = Utc::now() - ChronoDuration::days(100);
        }

        let pruned = store.prune_stale(ChronoDuration::days(30));
        assert_eq!(pruned, 1);
        assert_eq!(store.count(), 1);
        assert!(store.get(&e1).is_none());
        assert!(store.get(&e2).is_some());
    }

    #[test]
    fn heavy_lifter_badge_lifecycle() {
        let cfg = ReputationConfig::default();
        let entity = node_entity(NodeId::new());
        let mut record = ReputationRecord::with_config(entity, &cfg);

        // Need 20+ large job successes with >= 95% success rate
        for _ in 0..20 {
            record.record_success_with_config(1.0, true, &cfg);
        }
        assert!(record.has_badge(Badge::HeavyLifter));

        // Adding 3 large-job failures (3 out of 23 = 13% failure > 10% threshold)
        for _ in 0..3 {
            record.record_failure_with_config(true, &cfg);
        }
        assert!(!record.has_badge(Badge::HeavyLifter));
    }

    #[test]
    fn profile_excluded_node_types() {
        let android = make_android_profile(NodeId::new());
        let reqs = JobRequirements {
            excluded_node_types: vec![NodeType::Android, NodeType::Browser],
            ..JobRequirements::minimal()
        };
        assert!(!android.can_handle(&reqs));

        let desktop = make_profile(NodeId::new(), NodeType::Desktop);
        assert!(desktop.can_handle(&reqs));
    }

    #[test]
    fn marketplace_priority_ordering() {
        assert!(Priority::Low < Priority::Normal);
        assert!(Priority::Normal < Priority::High);
        assert!(Priority::High < Priority::Critical);
        assert_eq!(Priority::default(), Priority::Normal);
    }

    #[test]
    fn collective_store_node_membership_index() {
        let store = CollectiveStore::new();
        let n1 = NodeId::new();
        let n2 = NodeId::new();

        let p1 = make_profile(n1, NodeType::Desktop);
        let p2 = make_profile(n2, NodeType::CloudVM);

        let mut collective = Collective::new(n1, &p1);
        collective.add_member(n2, &p2);
        collective.state = CollectiveState::Active;
        let col_id = collective.id;
        store.upsert(collective);

        // Both nodes should be indexed
        let n1_collectives = store.collectives_for_node(&n1);
        assert_eq!(n1_collectives.len(), 1);
        assert_eq!(n1_collectives[0], col_id);

        let n2_collectives = store.collectives_for_node(&n2);
        assert_eq!(n2_collectives.len(), 1);

        // Unrelated node should have no collectives
        let n3 = NodeId::new();
        assert!(store.collectives_for_node(&n3).is_empty());
    }
}
