// Marabunta - Licensed under the MIT License.
//! Integration tests for the Neuromancer subsystem.
//!
//! These tests exercise cross-module interactions between the various
//! Neuromancer components: kill chain (Crocodile -> Viper -> Elektra),
//! Crow forensic logging, Engram fossil cache, Lazarus checkpoints,
//! Spider anomaly detection, Wild Dogs pack hunting, Ghost phantom
//! assembly, and Sandman speculative dreaming.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use tempfile::TempDir;

    use crate::swarm::neuromancer::bus::NeuromancerBus;
    use crate::swarm::neuromancer::config::*;
    use crate::swarm::neuromancer::crocodile::Crocodile;
    use crate::swarm::neuromancer::crow::Crow;
    use crate::swarm::neuromancer::elektra::Elektra;
    use crate::swarm::neuromancer::engram::Engram;
    use crate::swarm::neuromancer::ghost::{
        MarabuntaTask, Ghost, RawWorkUnit, RawWorkUnitResult, SumTask,
    };
    use crate::swarm::neuromancer::lazarus::Lazarus;
    use crate::swarm::neuromancer::sandman::{DreamReason, Sandman};
    use crate::swarm::neuromancer::spider::Spider;
    use crate::swarm::neuromancer::types::*;
    use crate::swarm::neuromancer::viper::Viper;
    use crate::swarm::neuromancer::wild_dogs::WildDogs;

    // ========================================================================
    // Test 1: Full kill chain — honeypot -> ThreatConfirmed -> quarantine
    //         -> kill -> autopsy
    // ========================================================================

    #[test]
    fn test_full_kill_chain() {
        let bus = Arc::new(NeuromancerBus::default());

        // -- Crocodile --
        let croc_config = CrocodileConfig {
            honeypot_ratio: 100,
            honeypot_timeout: Duration::from_secs(300),
            fast_track_timeout: Duration::from_secs(30),
            max_active_honeypots: 10,
            max_retries: 0,
        };
        let mut crocodile = Crocodile::new(croc_config, bus.clone());

        // -- Viper --
        let viper_config = ViperConfig {
            grace_period: Duration::from_secs(600),
            max_quarantined: 20,
        };
        let mut viper = Viper::new(viper_config, bus.clone());

        // -- Elektra --
        let elektra_config = ElektraConfig {
            blacklist_path: std::path::PathBuf::from("/tmp/test_neuromancer_killchain.json"),
        };
        let mut elektra = Elektra::new(elektra_config, bus.clone());

        // === Step 1: Deploy honeypot ===
        let malicious_node = NodeId::new();
        let task_id = crocodile.deploy_honeypot(malicious_node).unwrap();
        assert_eq!(crocodile.active_count(), 1);

        // === Step 2: Malicious node returns wrong hash ===
        let wrong_hash = [0xDEu8; 32];
        let was_honeypot = crocodile.handle_task_completed(task_id, wrong_hash, malicious_node);
        assert!(was_honeypot, "task should be recognized as a honeypot");
        assert_eq!(crocodile.active_count(), 0);

        // Crocodile emitted ThreatConfirmed internally -- we simulate the
        // downstream reaction: build evidence chain as Crocodile would.
        let mut evidence = EvidenceChain::new();
        evidence.push(EvidenceItem {
            event_type: "HoneypotMismatch".into(),
            details: "expected hash differs from actual".into(),
            timestamp: SystemTime::now(),
            confidence: 1.0,
        });

        // === Step 3: Viper quarantines the node ===
        viper.quarantine_node(malicious_node, &evidence);
        assert!(viper.is_quarantined(&malicious_node));
        assert_eq!(viper.quarantined_count(), 1);

        // === Step 4: Elektra executes the kill ===
        let autopsy = elektra
            .execute_kill(malicious_node, evidence)
            .expect("kill should succeed");

        // Verify autopsy report
        assert_eq!(autopsy.attack_vector, "result_tampering");
        assert!(!autopsy.recommendations.is_empty());

        // Verify final state
        assert!(elektra.is_blacklisted(&malicious_node));
        assert_eq!(elektra.kill_count(), 1);

        // Kill log should contain the record
        let log = elektra.kill_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].node, malicious_node);
        assert_eq!(log[0].autopsy.attack_vector, "result_tampering");

        // Double-kill is a no-op
        let second_kill = elektra.execute_kill(malicious_node, EvidenceChain::new());
        assert!(second_kill.is_none(), "already-blacklisted node should be no-op");
    }

    // ========================================================================
    // Test 2: Crow records kill chain timeline
    // ========================================================================

    #[test]
    fn test_crow_records_kill_chain_timeline() {
        let tmp = TempDir::new().unwrap();
        let bus = Arc::new(NeuromancerBus::default());
        let local_node = NodeId::new();

        let crow_config = CrowConfig {
            data_dir: tmp.path().to_path_buf(),
            ring_buffer_capacity: 100,
            dedup_set_capacity: 200,
            retention_days: 30,
            max_log_size_mb: 1024,
            rotation: "daily".into(),
            gossip_share_window_minutes: 10,
            compact_after_days: 7,
        };
        let crow = Crow::new(crow_config, bus.clone(), local_node).unwrap();

        let malicious_node = NodeId::new();
        let now = SystemTime::now();

        // Record the kill chain events in order:
        // 1. AnomalyDetected
        let anomaly_event = MarabuntaEvent::AnomalyDetected {
            node: malicious_node,
            score: 4.5,
            details: "suspicious behaviour detected".into(),
            timestamp: now,
        };
        crow.record(anomaly_event, local_node, 0).unwrap();

        // 2. ThreatConfirmed
        let mut evidence = EvidenceChain::new();
        evidence.push(EvidenceItem {
            event_type: "HoneypotMismatch".into(),
            details: "wrong hash returned".into(),
            timestamp: now,
            confidence: 1.0,
        });
        let threat_event = MarabuntaEvent::ThreatConfirmed {
            node: malicious_node,
            evidence: evidence.clone(),
            timestamp: now,
        };
        crow.record(threat_event, local_node, 0).unwrap();

        // 3. NodeQuarantined
        let quarantine_event = MarabuntaEvent::NodeQuarantined {
            node: malicious_node,
            reason: "HoneypotMismatch(conf=1.00)".into(),
            timestamp: now,
        };
        crow.record(quarantine_event, local_node, 0).unwrap();

        // 4. NodeKilled
        let kill_event = MarabuntaEvent::NodeKilled {
            node: malicious_node,
            evidence: evidence.clone(),
            timestamp: now,
        };
        crow.record(kill_event, local_node, 0).unwrap();

        // 5. AutopsyCompleted
        let autopsy_event = MarabuntaEvent::AutopsyCompleted {
            node: malicious_node,
            findings: AutopsyReport {
                attack_vector: "result_tampering".into(),
                affected_tasks: vec![],
                affected_fossils: vec![],
                behavioral_signature: vec![1.0],
                recommendations: vec!["evict fossils".into()],
            },
            timestamp: now,
        };
        crow.record(autopsy_event, local_node, 0).unwrap();

        // Query the security log
        let sec_log = crow.security_log().unwrap();
        assert_eq!(sec_log.len(), 5, "should have 5 security events");

        // Verify event ordering
        let event_types: Vec<&str> = sec_log.iter().map(|e| e.event.type_name()).collect();
        assert_eq!(
            event_types,
            vec![
                "AnomalyDetected",
                "ThreatConfirmed",
                "NodeQuarantined",
                "NodeKilled",
                "AutopsyCompleted"
            ]
        );

        // Verify hash chain integrity
        for i in 1..sec_log.len() {
            assert_eq!(
                sec_log[i].prev_hash,
                sec_log[i - 1].entry_hash,
                "hash chain broken at index {}",
                i
            );
        }

        // First entry's prev_hash should be genesis (all zeros)
        assert_eq!(sec_log[0].prev_hash, [0u8; 32]);
    }

    // ========================================================================
    // Test 3: Engram store + purge on kill
    // ========================================================================

    #[test]
    fn test_engram_store_purge_on_kill() {
        let tmp = TempDir::new().unwrap();
        let bus = Arc::new(NeuromancerBus::default());
        let local_node = NodeId::new();

        let engram_config = EngramConfig {
            max_storage_mb: 100,
            eviction_trigger_percent: 90,
            eviction_target_percent: 75,
            data_dir: tmp.path().to_path_buf(),
        };
        let engram = Engram::new(engram_config, bus.clone(), local_node).unwrap();

        let bad_node = NodeId::new();
        let good_node = NodeId::new();

        // Store fossils from the bad node
        let prov_bad = ProvenanceInfo {
            task_id: [10u8; 32],
            node: bad_node,
            input_hash: [10u8; 32],
            computation_type: "matrix_mul".into(),
            computation_duration_ms: 500,
        };
        engram
            .store(
                "matrix_mul",
                b"bad_input_data",
                b"bad_result_data",
                prov_bad,
                false,
                false,
            )
            .unwrap();

        // Store fossils from the good node
        let prov_good = ProvenanceInfo {
            task_id: [20u8; 32],
            node: good_node,
            input_hash: [20u8; 32],
            computation_type: "sort".into(),
            computation_duration_ms: 200,
        };
        engram
            .store(
                "sort",
                b"good_input_data",
                b"good_result_data",
                prov_good,
                false,
                false,
            )
            .unwrap();

        // Verify both fossils are accessible
        let bad_data = engram.lookup("matrix_mul", b"bad_input_data").unwrap();
        assert!(bad_data.is_some(), "bad node fossil should be accessible before purge");

        let good_data = engram.lookup("sort", b"good_input_data").unwrap();
        assert!(good_data.is_some(), "good node fossil should be accessible");

        // Simulate kill: purge all fossils from the bad node
        engram.purge_by_node(bad_node).unwrap();

        // Bad node's fossil should now be inaccessible (untrusted)
        let purged_data = engram.lookup("matrix_mul", b"bad_input_data").unwrap();
        assert!(
            purged_data.is_none(),
            "purged fossil should return None after kill"
        );

        // Good node's fossil should still work
        let still_good = engram.lookup("sort", b"good_input_data").unwrap();
        assert!(
            still_good.is_some(),
            "unrelated fossil should still be accessible after purge"
        );
        assert_eq!(still_good.unwrap(), b"good_result_data");
    }

    // ========================================================================
    // Test 4: Lazarus checkpoint roundtrip with Reed-Solomon
    // ========================================================================

    #[test]
    fn test_lazarus_checkpoint_roundtrip_with_reed_solomon() {
        let bus = Arc::new(NeuromancerBus::default());
        let local_node = NodeId::new();

        // Use 4 data + 2 parity fragments
        let lazarus_config = LazarusConfig {
            data_fragments: 4,
            redundancy_fragments: 2,
            retention_per_task: 3,
            max_checkpoint_size: 500 * 1024 * 1024,
            ..LazarusConfig::default()
        };
        let mut lazarus = Lazarus::new(lazarus_config, bus.clone(), local_node);

        let task_id: TaskId = [42u8; 32];
        // Create a non-trivial blob (400 bytes of patterned data)
        let checkpoint_blob: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();

        // === Disperse the checkpoint ===
        let ckpt = lazarus
            .disperse_checkpoint(task_id, &checkpoint_blob)
            .unwrap();

        assert_eq!(ckpt.task_id, task_id);
        assert_eq!(ckpt.blob_size, checkpoint_blob.len() as u64);
        assert_eq!(ckpt.fragment_map.total_fragments, 6); // 4 data + 2 parity

        // Verify all 6 fragments are stored
        for i in 0..6u32 {
            let frag = lazarus.get_fragment(ckpt.id, i);
            assert!(frag.is_ok(), "fragment {} should exist", i);
        }

        // === Remove 2 data fragments (simulate node failures) ===
        lazarus.remove_fragment(ckpt.id, 0);
        lazarus.remove_fragment(ckpt.id, 2);

        // Verify fragments are actually gone
        assert!(lazarus.get_fragment(ckpt.id, 0).is_err());
        assert!(lazarus.get_fragment(ckpt.id, 2).is_err());

        // === Resurrect via Reed-Solomon reconstruction ===
        let result = lazarus.resurrect_task(task_id).unwrap();

        // Verify the reconstructed blob matches the original
        assert_eq!(
            result.blob, checkpoint_blob,
            "reconstructed blob must match original"
        );
        assert_eq!(
            result.fragments_reconstructed, 2,
            "should report 2 fragments reconstructed"
        );
        assert_eq!(result.checkpoint.id, ckpt.id);

        // === Verify that removing too many fragments fails ===
        // Create another checkpoint
        let task_id_2: TaskId = [99u8; 32];
        let blob_2 = b"another checkpoint blob for failure testing".to_vec();
        let ckpt_2 = lazarus.disperse_checkpoint(task_id_2, &blob_2).unwrap();

        // Remove 3 fragments (more than the 2 parity shards can handle)
        lazarus.remove_fragment(ckpt_2.id, 0);
        lazarus.remove_fragment(ckpt_2.id, 1);
        lazarus.remove_fragment(ckpt_2.id, 3);

        let fail_result = lazarus.resurrect_task(task_id_2);
        assert!(
            fail_result.is_err(),
            "should fail when too many fragments are missing"
        );
    }

    // ========================================================================
    // Test 5: Spider anomaly detection
    // ========================================================================

    #[test]
    fn test_spider_anomaly_detection() {
        let bus = Arc::new(NeuromancerBus::default());
        let local_node = NodeId::new();

        let spider_config = SpiderConfig {
            collection_interval: Duration::from_secs(1),
            ema_alpha: 0.1,
            anomaly_threshold: 3.0,
            anomaly_sustained_duration: Duration::from_secs(30),
            preemptive_checkpoint_threshold: 2.0,
            gossip_interval: Duration::from_secs(10),
            stale_node_timeout: Duration::from_secs(60),
            warmup_samples: 20,
        };
        let mut spider = Spider::new(local_node, bus.clone(), spider_config);

        // === Phase 1: Build baseline with normal metrics ===
        let make_normal_metrics = || NodeMetrics {
            cpu_usage_percent: 25.0,
            memory_usage_percent: 40.0,
            disk_usage_percent: 50.0,
            disk_io_read_bytes_sec: 0,
            disk_io_write_bytes_sec: 0,
            network_rx_bytes_sec: 1000,
            network_tx_bytes_sec: 500,
            open_file_descriptors: 100,
            active_tasks: 3,
            temperature_celsius: None,
            fan_speed_rpm: None,
            uptime_seconds: 10000,
            timestamp: SystemTime::now(),
        };

        // Feed 30 normal samples (past warmup of 20)
        for _ in 0..30 {
            spider.update_baseline(&make_normal_metrics());
        }

        // During warmup (samples < 20), anomaly score should have been 0
        // After warmup, normal metrics should produce low anomaly score
        let normal_score = spider.compute_anomaly_score(&make_normal_metrics());
        assert!(
            normal_score < 1.0,
            "normal metrics should produce low anomaly score, got {}",
            normal_score
        );

        // === Phase 2: Feed anomalous metrics ===
        let anomalous_metrics = NodeMetrics {
            cpu_usage_percent: 99.0, // Way above baseline ~25%
            memory_usage_percent: 40.0,
            disk_usage_percent: 50.0,
            disk_io_read_bytes_sec: 0,
            disk_io_write_bytes_sec: 0,
            network_rx_bytes_sec: 1000,
            network_tx_bytes_sec: 500,
            open_file_descriptors: 100,
            active_tasks: 3,
            temperature_celsius: None,
            fan_speed_rpm: None,
            uptime_seconds: 10000,
            timestamp: SystemTime::now(),
        };

        let anomaly_score = spider.compute_anomaly_score(&anomalous_metrics);
        assert!(
            anomaly_score >= 3.0,
            "CPU spike to 99% should produce anomaly score >= 3.0, got {}",
            anomaly_score
        );

        // === Phase 3: Verify z-score edge cases ===
        // Near-zero stddev with large deviation
        let z = Spider::z_score(100.0, 25.0, 0.0);
        assert_eq!(z, 5.0, "near-zero stddev with large deviation should return 5.0");

        // Normal z-score computation
        let z2 = Spider::z_score(50.0, 25.0, 5.0);
        assert!(
            (z2 - 5.0).abs() < f64::EPSILON,
            "z-score should be |50-25|/5 = 5.0, got {}",
            z2
        );
    }

    // ========================================================================
    // Test 6: Wild Dogs pack hunt on correlated threats
    // ========================================================================

    #[test]
    fn test_wild_dogs_pack_hunt_on_correlated_threats() {
        let bus = Arc::new(NeuromancerBus::default());
        let mut rx = bus.subscribe();

        let dogs_config = WildDogsConfig {
            correlation_window: Duration::from_secs(300),
            min_cluster_size: 3,
            similarity_threshold: 0.7,
            max_concurrent_hunts: 5,
        };
        let mut dogs = WildDogs::new(dogs_config, bus.clone());

        let now = SystemTime::now();

        // Create 4 threats with similar evidence vectors (high cosine similarity)
        let threat_nodes: Vec<NodeId> = (0..4).map(|_| NodeId::new()).collect();

        for &node in &threat_nodes {
            let mut evidence = EvidenceChain::new();
            evidence.push(EvidenceItem {
                event_type: "HoneypotMismatch".into(),
                details: "hash mismatch".into(),
                timestamp: now,
                confidence: 0.95,
            });
            evidence.push(EvidenceItem {
                event_type: "SybilPattern".into(),
                details: "correlated identity".into(),
                timestamp: now,
                confidence: 0.90,
            });
            evidence.push(EvidenceItem {
                event_type: "ResourceAbuse".into(),
                details: "excessive CPU".into(),
                timestamp: now,
                confidence: 0.85,
            });

            dogs.on_threat_confirmed(node, evidence, now);
        }

        // Should have initiated a pack hunt (>= 3 similar threats)
        assert!(
            dogs.hunt_count() >= 1,
            "should have initiated at least one pack hunt, got {}",
            dogs.hunt_count()
        );
        assert!(dogs.active_hunt_count() >= 1);

        // The bus should have received a PackHuntInitiated event
        let event = rx.try_recv().expect("expected PackHuntInitiated event");
        match event {
            MarabuntaEvent::PackHuntInitiated {
                targets, pattern, ..
            } => {
                assert!(
                    targets.len() >= 3,
                    "pack hunt should target at least 3 nodes, got {}",
                    targets.len()
                );
                assert!(
                    pattern.contains("correlated_threat_cluster"),
                    "pattern should describe the cluster"
                );
            }
            other => panic!("expected PackHuntInitiated, got {:?}", other.type_name()),
        }

        // === Verify dissimilar threats do NOT trigger a hunt ===
        let bus2 = Arc::new(NeuromancerBus::default());
        let dogs_config2 = WildDogsConfig {
            correlation_window: Duration::from_secs(300),
            min_cluster_size: 3,
            similarity_threshold: 0.7,
            max_concurrent_hunts: 5,
        };
        let mut dogs2 = WildDogs::new(dogs_config2, bus2.clone());

        // Orthogonal evidence vectors
        let mut ev1 = EvidenceChain::new();
        ev1.push(EvidenceItem {
            event_type: "A".into(),
            details: "x".into(),
            timestamp: now,
            confidence: 1.0,
        });
        ev1.push(EvidenceItem {
            event_type: "B".into(),
            details: "y".into(),
            timestamp: now,
            confidence: 0.0,
        });

        let mut ev2 = EvidenceChain::new();
        ev2.push(EvidenceItem {
            event_type: "C".into(),
            details: "z".into(),
            timestamp: now,
            confidence: 0.0,
        });
        ev2.push(EvidenceItem {
            event_type: "D".into(),
            details: "w".into(),
            timestamp: now,
            confidence: 1.0,
        });

        let mut ev3 = EvidenceChain::new();
        ev3.push(EvidenceItem {
            event_type: "E".into(),
            details: "v".into(),
            timestamp: now,
            confidence: 0.5,
        });
        ev3.push(EvidenceItem {
            event_type: "F".into(),
            details: "u".into(),
            timestamp: now,
            confidence: 0.0,
        });

        dogs2.on_threat_confirmed(NodeId::new(), ev1, now);
        dogs2.on_threat_confirmed(NodeId::new(), ev2, now);
        dogs2.on_threat_confirmed(NodeId::new(), ev3, now);

        assert_eq!(
            dogs2.hunt_count(),
            0,
            "dissimilar threats should not trigger a pack hunt"
        );
    }

    // ========================================================================
    // Test 7: Ghost decompose-compose with SumTask
    // ========================================================================

    #[test]
    fn test_ghost_decompose_compose_with_sum_task() {
        let bus = Arc::new(NeuromancerBus::default());
        let mut rx = bus.subscribe();

        let ghost_config = GhostConfig { enable_cache: true };
        let mut ghost = Ghost::new(ghost_config, bus.clone());

        // Use SumTask to decompose input
        let sum_task = SumTask;
        let input: Vec<i64> = vec![10, 20, 30, 40, 50];
        let expected_sum: i64 = input.iter().sum(); // 150

        // === Step 1: Decompose via MarabuntaTask ===
        let work_units = sum_task.decompose(&input);
        assert_eq!(work_units.len(), 5, "should produce one unit per element");

        // Verify each unit has the correct serialized value
        for (i, unit) in work_units.iter().enumerate() {
            let val: i64 = serde_json::from_slice(&unit.input).unwrap();
            assert_eq!(val, input[i]);
            assert!(unit.dependencies.is_empty());
        }

        // === Step 2: Submit to Ghost ===
        let contributing_nodes = vec![NodeId::new(), NodeId::new()];
        let phantom_id = ghost
            .submit_raw_task(rand::random::<[u8; 32]>(), work_units, contributing_nodes)
            .unwrap();

        // Verify PhantomAssembled event was emitted
        let event = rx.try_recv().unwrap();
        match event {
            MarabuntaEvent::PhantomAssembled {
                phantom_id: pid,
                contributing_nodes: nodes,
                ..
            } => {
                assert_eq!(pid, phantom_id);
                assert_eq!(nodes.len(), 2);
            }
            other => panic!("expected PhantomAssembled, got {:?}", other.type_name()),
        }

        // === Step 3: Execute the phantom ===
        let raw_results = ghost.execute_phantom_raw(phantom_id).unwrap();
        assert_eq!(raw_results.len(), 5);

        // === Step 4: Compose results via MarabuntaTask ===
        // Ghost V1 simulation: input == output, so raw_results[i] == serialized input[i]
        let work_unit_results: Vec<RawWorkUnitResult> = raw_results
            .iter()
            .map(|(&idx, output)| RawWorkUnitResult {
                index: idx,
                output: output.clone(),
            })
            .collect();

        let final_sum = sum_task.compose(work_unit_results);
        assert_eq!(
            final_sum, expected_sum,
            "composed sum should equal input sum"
        );

        // === Step 5: Dissolve the phantom ===
        ghost.dissolve_phantom(phantom_id).unwrap();

        let dissolve_event = rx.try_recv().unwrap();
        match dissolve_event {
            MarabuntaEvent::PhantomDissolved {
                phantom_id: pid, ..
            } => {
                assert_eq!(pid, phantom_id);
            }
            other => panic!("expected PhantomDissolved, got {:?}", other.type_name()),
        }

        assert_eq!(ghost.active_phantom_count(), 0);
    }

    // ========================================================================
    // Test 8: Sandman pattern detection end-to-end
    // ========================================================================

    #[test]
    fn test_sandman_pattern_detection_end_to_end() {
        let bus = Arc::new(NeuromancerBus::default());
        let mut rx = bus.subscribe();

        let sandman_config = SandmanConfig {
            enabled: true,
            idle_threshold: 0.3,
            max_dream_resources: 0.5,
            min_sequence_length: 3,
            analysis_interval: Duration::from_secs(60),
            max_active_dreams: 20,
        };
        let mut sandman = Sandman::new(sandman_config, bus.clone());

        let base_time = SystemTime::now();

        // === Phase 1: Feed a linear sequence of task results ===
        // Parameters: [1.0], [2.0], [3.0], [4.0], [5.0]
        for i in 0..5u32 {
            sandman.record_task(
                "linear_compute".to_string(),
                vec![(i + 1) as f64],
                base_time + Duration::from_secs(i as u64 * 60),
            );
        }

        // === Phase 2: Feed a geometric sequence ===
        // Parameters: [100.0], [200.0], [400.0]
        for (i, val) in [100.0_f64, 200.0, 400.0].iter().enumerate() {
            sandman.record_task(
                "geo_compute".to_string(),
                vec![*val],
                base_time + Duration::from_secs(i as u64 * 30),
            );
        }

        // === Phase 3: Analyze patterns ===
        let seeds = sandman.analyze_patterns();
        assert!(
            !seeds.is_empty(),
            "should detect at least one pattern from the recorded tasks"
        );

        // Verify linear pattern was detected for "linear_compute"
        let linear_seed = seeds
            .iter()
            .find(|s| s.reason == DreamReason::LinearSequence && s.seed.task_type == "linear_compute");
        assert!(
            linear_seed.is_some(),
            "should detect linear sequence in linear_compute tasks"
        );
        let linear = linear_seed.unwrap();
        let predicted_linear: Vec<f64> =
            serde_json::from_slice(&linear.seed.predicted_input).unwrap();
        assert!(
            (predicted_linear[0] - 6.0).abs() < 0.01,
            "linear prediction should be 6.0, got {}",
            predicted_linear[0]
        );

        // Verify geometric pattern was detected for "geo_compute"
        let geo_seed = seeds
            .iter()
            .find(|s| s.reason == DreamReason::GeometricSequence && s.seed.task_type == "geo_compute");
        assert!(
            geo_seed.is_some(),
            "should detect geometric sequence in geo_compute tasks"
        );
        let geo = geo_seed.unwrap();
        let predicted_geo: Vec<f64> =
            serde_json::from_slice(&geo.seed.predicted_input).unwrap();
        assert!(
            (predicted_geo[0] - 800.0).abs() < 1.0,
            "geometric prediction should be 800.0, got {}",
            predicted_geo[0]
        );

        // === Phase 4: Start a dream from the linear seed ===
        let dream_task_id = sandman.start_dream(linear.clone());
        assert_ne!(dream_task_id, [0u8; 32]);
        assert_eq!(sandman.stats().dreams_started, 1);

        // Verify DreamStarted event was emitted
        let dream_event = rx.try_recv().expect("expected DreamStarted event");
        match dream_event {
            MarabuntaEvent::DreamStarted {
                task_id, reason, ..
            } => {
                assert_eq!(task_id, dream_task_id);
                assert!(reason.contains("linear sequence"));
            }
            other => panic!("expected DreamStarted, got {:?}", other.type_name()),
        }

        // === Phase 5: Complete the dream ===
        let dream_result = serde_json::to_vec(&vec![6.0_f64]).unwrap();
        sandman.complete_dream(dream_task_id, dream_result);
        assert_eq!(sandman.stats().dreams_completed, 1);

        // Verify DreamCompleted event was emitted
        let complete_event = rx.try_recv().expect("expected DreamCompleted event");
        match complete_event {
            MarabuntaEvent::DreamCompleted { task_id, .. } => {
                assert_eq!(task_id, dream_task_id);
            }
            other => panic!("expected DreamCompleted, got {:?}", other.type_name()),
        }

        // === Phase 6: Start and yield another dream ===
        let geo_dream_id = sandman.start_dream(geo.clone());
        // Drain DreamStarted
        let _ = rx.try_recv();

        sandman.yield_dream(geo_dream_id, "swarm became busy");
        assert_eq!(sandman.stats().dreams_yielded, 1);

        let yield_event = rx.try_recv().expect("expected DreamYielded event");
        match yield_event {
            MarabuntaEvent::DreamYielded { task_id, reason, .. } => {
                assert_eq!(task_id, geo_dream_id);
                assert_eq!(reason, "swarm became busy");
            }
            other => panic!("expected DreamYielded, got {:?}", other.type_name()),
        }

        // Final stats check
        assert_eq!(sandman.stats().dreams_started, 2);
        assert_eq!(sandman.stats().dreams_completed, 1);
        assert_eq!(sandman.stats().dreams_yielded, 1);
    }
}
