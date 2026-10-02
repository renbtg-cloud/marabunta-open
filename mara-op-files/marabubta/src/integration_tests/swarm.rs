// Marabunta - Licensed under the MIT License.
//! Swarm integration tests
//!
//! Tests that verify swarm components work together correctly.
//! All tests run on the host machine without any external dependencies.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{Duration as ChronoDuration, Utc};
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::common::types::{JobId, TaskPayload};
    use crate::swarm::config::*;
    use crate::swarm::failure::FailureDetector;
    use crate::swarm::gossip::GossipEngine;
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::traits::{evaluate_traits, measure_resources};
    use crate::swarm::auth::NodeIdentity;
    use crate::swarm::transport::{decode_message, encode_message, encode_signed};
    use crate::swarm::types::*;

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Create a NodeInfo with the given parameters.
    fn make_node_info(
        node_id: NodeId,
        last_seen: chrono::DateTime<Utc>,
        status: NodeStatus,
        generation: u64,
    ) -> NodeInfo {
        NodeInfo {
            node_id,
            last_seen,
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.1,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: node_id,
            status,
            generation,
            trust_level: Default::default(),
            failure_domains: vec![],
                attestation: LocationAttestation::SelfAttested,
        }
    }

    /// Create a NodeInfo with address.
    fn make_node_info_with_addr(
        node_id: NodeId,
        addr: SocketAddr,
        last_seen: chrono::DateTime<Utc>,
    ) -> NodeInfo {
        NodeInfo {
            node_id,
            last_seen,
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.1,
            capacity: ResourceSnapshot::default(),
            address: Some(addr),
            via: node_id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
            failure_domains: vec![],
                attestation: LocationAttestation::SelfAttested,
        }
    }

    /// Create a SwarmJobInfo for testing.
    fn make_job_info(job_id: JobId, chunks_total: u32) -> SwarmJobInfo {
        SwarmJobInfo {
            job_id,
            status: SwarmJobStatus::Pending,
            chunks_total,
            chunks_completed: 0,
            chunks_failed: 0,
            submitter: NodeId::new(),
            first_seen: Utc::now(),
            updated_at: Utc::now(),
            payload_type: "shell".to_string(),
            priority: 1,
            data_residency: None,
            required_zone_id: "test-zone".to_string(),
            verification_strategy: VerificationStrategy::None,
        }
    }

    /// Create a pending Assignment.
    fn make_assignment(chunk_id: ChunkId, job_id: JobId) -> Assignment {
        Assignment {
            chunk_id,
            job_id,
            assigned_to: None,
            assigned_at: Utc::now(),
            status: ChunkStatus::Pending,
            result: None,
            attempts: 0,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        }
    }

    /// Create a Chunk.
    fn make_chunk(chunk_id: ChunkId, job_id: JobId, sequence: u32) -> Chunk {
        Chunk {
            id: chunk_id,
            job_id,
            sequence,
            payload: TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec![format!("chunk-{}", sequence)],
            },
            created_at: Utc::now(),
        }
    }

    /// Build a ResourceSnapshot with explicit values.
    fn make_resources(
        cpu_available: f32,
        memory_available_mb: u64,
        disk_available_mb: u64,
        bandwidth_mbps: f32,
    ) -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_cores: 4,
            cpu_available,
            memory_total_mb: 8192,
            memory_available_mb,
            disk_total_mb: 102400,
            disk_available_mb,
            network_bandwidth_mbps: bandwidth_mbps,
                current_tdp_watts: 15.0,
                ask_usd_per_megagas: 0.0001,
        }
    }

    /// Build connectivity info for a publicly-reachable node.
    fn public_connectivity() -> ConnectivityInfo {
        ConnectivityInfo {
            is_publicly_reachable: true,
            nat_type: NatType::Public,
            has_relay_access: false,
            listen_address: Some("1.2.3.4:4200".parse().unwrap()),
            avg_rtt_ms: Some(5.0),
        }
    }

    // ========================================================================
    // Test 1: Knowledge store merge semantics
    // ========================================================================

    #[test]
    fn test_knowledge_store_merge_semantics() {
        let store = KnowledgeStore::new(NodeId::new());
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        // Store A: knows about node_a with timestamp T1, and node_b with
        // timestamp T2.
        let t1 = Utc::now() - ChronoDuration::seconds(10);
        let t2 = Utc::now() - ChronoDuration::seconds(5);

        let store_a = KnowledgeStore::new(NodeId::new());
        store_a.merge_node(make_node_info(node_a, t1, NodeStatus::Alive, 1));
        store_a.merge_node(make_node_info(node_b, t2, NodeStatus::Alive, 1));

        // Store B: knows about node_a with a newer timestamp T3, and node_b
        // with an older timestamp T0.
        let t3 = Utc::now();
        let t0 = Utc::now() - ChronoDuration::seconds(20);

        let store_b = KnowledgeStore::new(NodeId::new());
        store_b.merge_node(make_node_info(node_a, t3, NodeStatus::Alive, 1));
        store_b.merge_node(make_node_info(node_b, t0, NodeStatus::Alive, 1));

        // Merge B's knowledge into A. Node_a should update (T3 > T1),
        // node_b should NOT update (T0 < T2).
        let all_b = store_b.get_all_nodes();
        for node_info in all_b {
            store_a.merge_node(node_info);
        }

        let stored_a = store_a.get_node(&node_a).unwrap();
        assert_eq!(
            stored_a.last_seen, t3,
            "node_a should have been updated to newer timestamp T3"
        );

        let stored_b = store_a.get_node(&node_b).unwrap();
        assert_eq!(
            stored_b.last_seen, t2,
            "node_b should retain its newer timestamp T2, not regress to T0"
        );

        // Verify generation counter is respected: same timestamp but higher
        // generation wins.
        let t_same = Utc::now();
        store.merge_node(make_node_info(node_a, t_same, NodeStatus::Alive, 1));
        store.merge_node(make_node_info(node_a, t_same, NodeStatus::Alive, 5));
        assert_eq!(
            store.get_node(&node_a).unwrap().generation,
            5,
            "higher generation should win when timestamps are equal"
        );

        // Same timestamp, lower generation should NOT replace.
        store.merge_node(make_node_info(node_a, t_same, NodeStatus::Alive, 2));
        assert_eq!(
            store.get_node(&node_a).unwrap().generation,
            5,
            "lower generation should not replace higher generation at same timestamp"
        );
    }

    // ========================================================================
    // Test 2: Assignment conflict resolution
    // ========================================================================

    #[test]
    fn test_assignment_conflict_resolution() {
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        let node_early = NodeId::new();
        let node_late = NodeId::new();

        let t_early = Utc::now() - ChronoDuration::seconds(5);
        let t_late = Utc::now();

        // Assignment A: earlier timestamp.
        let assign_a = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(node_early),
            assigned_at: t_early,
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };

        // Assignment B: later timestamp.
        let assign_b = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(node_late),
            assigned_at: t_late,
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };

        // Earlier timestamp wins.
        assert!(
            assign_a.wins_against(&assign_b),
            "earlier timestamp should win"
        );
        assert!(
            !assign_b.wins_against(&assign_a),
            "later timestamp should lose"
        );

        // Test tiebreaker: same timestamp, lower NodeId wins.
        let node_low = NodeId(Uuid::from_bytes([0u8; 16]));
        let node_high = NodeId(Uuid::from_bytes([0xFF; 16]));
        let t_same = Utc::now();

        let assign_low = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(node_low),
            assigned_at: t_same,
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };

        let assign_high = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(node_high),
            assigned_at: t_same,
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };

        assert!(
            assign_low.wins_against(&assign_high),
            "lower NodeId should break ties"
        );
        assert!(
            !assign_high.wins_against(&assign_low),
            "higher NodeId should lose ties"
        );

        // Assigned vs unassigned: assigned always wins.
        let assign_some = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(node_low),
            assigned_at: t_same,
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };
        let assign_none = Assignment {
            chunk_id,
            job_id,
            assigned_to: None,
            assigned_at: t_same,
            status: ChunkStatus::Pending,
            result: None,
            attempts: 0,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };
        assert!(
            assign_some.wins_against(&assign_none),
            "assigned should beat unassigned"
        );
        assert!(
            !assign_none.wins_against(&assign_some),
            "unassigned should lose to assigned"
        );
    }

    // ========================================================================
    // Test 3: Trait evaluation
    // ========================================================================

    #[test]
    fn test_trait_evaluation() {
        // Part A: measure_resources returns non-zero values.
        let resources = measure_resources();
        assert!(resources.cpu_cores > 0, "must detect at least one CPU core");
        assert!(
            resources.memory_total_mb > 0,
            "must detect total memory"
        );

        // Part B: evaluate_traits with known resource values.
        // High resources, low load, public connectivity -> all traits.
        let high_res = make_resources(0.9, 4096, 51200, 100.0);
        let conn = public_connectivity();
        let force = HashSet::new();
        let deny = HashSet::new();

        let traits = evaluate_traits(&high_res, 0.1, &conn, &force, &deny);
        assert!(traits.contains(&Trait::CanExecute));
        assert!(traits.contains(&Trait::CanForward));
        assert!(traits.contains(&Trait::CanAggregate));
        assert!(traits.contains(&Trait::CanStoreState));
        assert!(traits.contains(&Trait::CanDiscover));
        assert!(traits.contains(&Trait::CanRelay));

        // Part C: Low CPU should shed CanExecute.
        let low_cpu = make_resources(0.05, 4096, 51200, 100.0);
        let traits = evaluate_traits(&low_cpu, 0.1, &conn, &force, &deny);
        assert!(
            !traits.contains(&Trait::CanExecute),
            "low CPU should shed CanExecute"
        );

        // Part D: High load should shed heavy traits.
        let traits = evaluate_traits(&high_res, 0.85, &conn, &force, &deny);
        assert!(
            !traits.contains(&Trait::CanExecute),
            "overloaded node should shed CanExecute"
        );
        assert!(
            !traits.contains(&Trait::CanRelay),
            "overloaded node should shed CanRelay"
        );

        // Part E: force_traits override.
        let mut force = HashSet::new();
        force.insert(Trait::CanExecute);
        force.insert(Trait::CanRelay);
        let zero_res = make_resources(0.0, 0, 0, 0.0);
        let no_conn = ConnectivityInfo::default();
        let traits = evaluate_traits(&zero_res, 1.0, &no_conn, &force, &HashSet::new());
        assert!(
            traits.contains(&Trait::CanExecute),
            "force_traits should override resource checks"
        );
        assert!(
            traits.contains(&Trait::CanRelay),
            "force_traits should override resource checks"
        );

        // Part F: deny_traits override.
        let mut deny = HashSet::new();
        deny.insert(Trait::CanExecute);
        let traits = evaluate_traits(&high_res, 0.1, &conn, &HashSet::new(), &deny);
        assert!(
            !traits.contains(&Trait::CanExecute),
            "deny_traits should remove trait"
        );

        // Part G: force_traits wins when trait is in both force and deny.
        let mut force = HashSet::new();
        force.insert(Trait::CanExecute);
        let mut deny = HashSet::new();
        deny.insert(Trait::CanExecute);
        let traits = evaluate_traits(&zero_res, 1.0, &no_conn, &force, &deny);
        assert!(
            traits.contains(&Trait::CanExecute),
            "force_traits should beat deny_traits for same trait"
        );
    }

    // ========================================================================
    // Test 4: Gossip message building
    // ========================================================================

    #[test]
    fn test_gossip_message_building() {
        let node_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(node_id));

        // Populate knowledge store with some data.
        for _ in 0..60 {
            let nid = NodeId::new();
            let addr: SocketAddr = "127.0.0.1:4200".parse().unwrap();
            knowledge.merge_node(make_node_info_with_addr(nid, addr, Utc::now()));
        }
        for _ in 0..30 {
            let jid = JobId::new();
            knowledge.merge_job(make_job_info(jid, 5));
        }
        for _ in 0..120 {
            let cid = ChunkId::new();
            let jid = JobId::new();
            knowledge.merge_assignment(make_assignment(cid, jid));
        }

        // Create gossip engine.
        let (tx, _rx) = mpsc::channel(64);
        let engine = GossipEngine::new(node_id, 1, knowledge.clone(), tx);

        // Build a message.
        let my_traits = HashSet::from([Trait::CanExecute, Trait::CanForward]);
        let my_load = 0.3;
        let my_capacity = make_resources(0.7, 2048, 10240, 50.0);

        let message = engine.build_message(&my_traits, my_load, &my_capacity);

        // Verify self-report.
        assert_eq!(message.sender_id, node_id);
        assert_eq!(message.generation, 1);
        assert_eq!(message.my_traits, my_traits);
        assert!((message.my_load - my_load).abs() < f32::EPSILON);
        assert_eq!(message.my_capacity.cpu_cores, 4);

        // Verify bounded samples respect MAX_* limits.
        assert!(
            message.known_nodes.len() <= MAX_NODES_PER_MESSAGE,
            "nodes in message ({}) should be <= MAX_NODES_PER_MESSAGE ({})",
            message.known_nodes.len(),
            MAX_NODES_PER_MESSAGE,
        );
        assert!(
            message.known_jobs.len() <= MAX_JOBS_PER_MESSAGE,
            "jobs in message ({}) should be <= MAX_JOBS_PER_MESSAGE ({})",
            message.known_jobs.len(),
            MAX_JOBS_PER_MESSAGE,
        );
        assert!(
            message.known_assignments.len() <= MAX_ASSIGNMENTS_PER_MESSAGE,
            "assignments in message ({}) should be <= MAX_ASSIGNMENTS_PER_MESSAGE ({})",
            message.known_assignments.len(),
            MAX_ASSIGNMENTS_PER_MESSAGE,
        );

        // Verify message actually contains data (store had enough entries).
        assert!(
            !message.known_nodes.is_empty(),
            "message should contain at least some known nodes"
        );
        assert!(
            !message.known_jobs.is_empty(),
            "message should contain at least some known jobs"
        );
        assert!(
            !message.known_assignments.is_empty(),
            "message should contain at least some known assignments"
        );
    }

    // ========================================================================
    // Test 5: Failure detection sweep
    // ========================================================================

    #[test]
    fn test_failure_detection_sweep() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));

        let alive_node = NodeId::new();
        let suspect_candidate = NodeId::new();
        let dead_candidate = NodeId::new();

        // Alive node: seen just now.
        knowledge.merge_node(make_node_info(alive_node, Utc::now(), NodeStatus::Alive, 1));

        // Suspect candidate: seen 15 seconds ago (> SUSPECT_THRESHOLD = 10s,
        // but < DEAD_THRESHOLD = 30s).
        let t_suspect = Utc::now() - ChronoDuration::seconds(15);
        knowledge.merge_node(make_node_info(
            suspect_candidate,
            t_suspect,
            NodeStatus::Alive,
            1,
        ));

        // Dead candidate: seen 60 seconds ago (> DEAD_THRESHOLD = 30s).
        let t_dead = Utc::now() - ChronoDuration::seconds(60);
        knowledge.merge_node(make_node_info(dead_candidate, t_dead, NodeStatus::Alive, 1));

        // Give the dead candidate an in-progress chunk so we can verify reassignment.
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        knowledge.merge_assignment(make_assignment(chunk_id, job_id));
        knowledge.claim_chunk(&chunk_id, dead_candidate);

        // Run the failure detector sweep.
        let detector = FailureDetector::new(knowledge.clone(), self_id);
        let report = detector.sweep();

        // Verify suspect detection.
        assert!(
            report.nodes_suspected.contains(&suspect_candidate),
            "node silent for 15s should be suspected"
        );

        // Verify dead detection.
        assert!(
            report.nodes_declared_dead.contains(&dead_candidate),
            "node silent for 60s should be declared dead"
        );

        // Verify chunk reassignment.
        assert!(
            report.chunks_reassigned.contains(&chunk_id),
            "chunk from dead node should be reassigned"
        );

        // Verify the alive node was not affected.
        assert!(
            !report.nodes_suspected.contains(&alive_node),
            "alive node should not be suspected"
        );
        assert!(
            !report.nodes_declared_dead.contains(&alive_node),
            "alive node should not be declared dead"
        );

        // Verify the knowledge store reflects the new statuses.
        assert_eq!(
            knowledge.get_node(&suspect_candidate).unwrap().status,
            NodeStatus::Suspect,
        );
        assert_eq!(
            knowledge.get_node(&dead_candidate).unwrap().status,
            NodeStatus::Dead,
        );
        assert_eq!(
            knowledge.get_assignment(&chunk_id).unwrap().status,
            ChunkStatus::Pending,
            "chunk from dead node should be back to Pending"
        );
    }

    // ========================================================================
    // Test 6: Work submission and chunk creation
    // ========================================================================

    #[test]
    fn test_work_submission_and_chunk_creation() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let job_id = JobId::new();

        let payloads = vec![
            TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec!["hello".to_string()],
            },
            TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec!["world".to_string()],
            },
            TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec!["test".to_string()],
            },
        ];

        let chunk_count = payloads.len() as u32;

        // Simulate work submission: create chunks and add to knowledge store.
        let mut chunk_ids = Vec::new();
        for (i, payload) in payloads.into_iter().enumerate() {
            let chunk_id = ChunkId::new();
            chunk_ids.push(chunk_id);

            let chunk = Chunk {
                id: chunk_id,
                job_id,
                sequence: i as u32,
                payload,
                created_at: Utc::now(),
            };

            // Add the chunk to the pending queue.
            knowledge.add_pending_chunk(chunk).expect("queue full");

            // Create a Pending assignment for this chunk.
            knowledge.merge_assignment(make_assignment(chunk_id, job_id));
        }

        // Create the job info.
        let job_info = SwarmJobInfo {
            job_id,
            status: SwarmJobStatus::Pending,
            chunks_total: chunk_count,
            chunks_completed: 0,
            chunks_failed: 0,
            submitter: self_id,
            first_seen: Utc::now(),
            updated_at: Utc::now(),
            payload_type: "shell".to_string(),
            priority: 1,
            data_residency: None,
            required_zone_id: "test-zone".to_string(),
            verification_strategy: VerificationStrategy::None,
        };
        knowledge.merge_job(job_info);

        // Verify chunks were created.
        assert_eq!(
            knowledge.pending_chunk_count(),
            3,
            "should have 3 pending chunks"
        );

        // Verify job info.
        let stored_job = knowledge.get_job(&job_id).unwrap();
        assert_eq!(stored_job.chunks_total, 3);
        assert_eq!(stored_job.status, SwarmJobStatus::Pending);
        assert_eq!(stored_job.submitter, self_id);

        // Verify assignments are Pending.
        for cid in &chunk_ids {
            let assignment = knowledge.get_assignment(cid).unwrap();
            assert_eq!(assignment.status, ChunkStatus::Pending);
            assert_eq!(assignment.job_id, job_id);
            assert!(assignment.assigned_to.is_none());
        }

        // Verify we can dequeue chunks in FIFO order.
        let first = knowledge.take_pending_chunk().unwrap();
        assert_eq!(first.sequence, 0);
        let second = knowledge.take_pending_chunk().unwrap();
        assert_eq!(second.sequence, 1);
        let third = knowledge.take_pending_chunk().unwrap();
        assert_eq!(third.sequence, 2);
        assert!(knowledge.take_pending_chunk().is_none());
    }

    // ========================================================================
    // Test 7: Chunk claiming
    // ========================================================================

    #[test]
    fn test_chunk_claiming() {
        let knowledge = Arc::new(KnowledgeStore::new(NodeId::new()));
        let job_id = JobId::new();

        // Create several pending assignments.
        let chunk_a = ChunkId::new();
        let chunk_b = ChunkId::new();
        knowledge.merge_assignment(make_assignment(chunk_a, job_id));
        knowledge.merge_assignment(make_assignment(chunk_b, job_id));

        let node_1 = NodeId::new();
        let node_2 = NodeId::new();

        // Node 1 claims chunk_a.
        assert!(
            knowledge.claim_chunk(&chunk_a, node_1),
            "first claim should succeed"
        );

        // Verify the assignment was updated.
        let assignment = knowledge.get_assignment(&chunk_a).unwrap();
        assert_eq!(assignment.status, ChunkStatus::InProgress);
        assert_eq!(assignment.assigned_to, Some(node_1));
        assert_eq!(assignment.attempts, 1);

        // Node 2 tries to claim the same chunk -- should fail.
        assert!(
            !knowledge.claim_chunk(&chunk_a, node_2),
            "second claim on same chunk should fail"
        );

        // Verify the original assignment is unchanged.
        let assignment = knowledge.get_assignment(&chunk_a).unwrap();
        assert_eq!(
            assignment.assigned_to,
            Some(node_1),
            "original claimant should be preserved"
        );

        // Node 2 can claim a different chunk.
        assert!(
            knowledge.claim_chunk(&chunk_b, node_2),
            "claim on different chunk should succeed"
        );
        let assignment_b = knowledge.get_assignment(&chunk_b).unwrap();
        assert_eq!(assignment_b.assigned_to, Some(node_2));

        // Claiming a nonexistent chunk should fail.
        assert!(
            !knowledge.claim_chunk(&ChunkId::new(), node_1),
            "claim on nonexistent chunk should fail"
        );
    }

    // ========================================================================
    // Test 8: Result aggregation
    // ========================================================================

    #[test]
    fn test_result_aggregation() {
        let knowledge = Arc::new(KnowledgeStore::new(NodeId::new()));
        let job_id = JobId::new();

        // Create a job with 3 chunks.
        let chunk_ids: Vec<ChunkId> = (0..3).map(|_| ChunkId::new()).collect();
        let chunks_total = chunk_ids.len() as u32;

        let job_info = SwarmJobInfo {
            job_id,
            status: SwarmJobStatus::InProgress,
            chunks_total,
            chunks_completed: 0,
            chunks_failed: 0,
            submitter: NodeId::new(),
            first_seen: Utc::now(),
            updated_at: Utc::now(),
            payload_type: "shell".to_string(),
            priority: 1,
            data_residency: None,
            required_zone_id: "test-zone".to_string(),
            verification_strategy: VerificationStrategy::None,
        };
        knowledge.merge_job(job_info);

        let node_id = NodeId::new();

        // Create assignments and mark each as completed with results.
        for (i, &cid) in chunk_ids.iter().enumerate() {
            let mut assignment = make_assignment(cid, job_id);
            assignment.status = ChunkStatus::Completed;
            assignment.assigned_to = Some(node_id);
            assignment.result = Some(ChunkResult {
                success: true,
                output: format!("output-{}", i).into_bytes(),
                stdout: format!("stdout-{}", i),
                stderr: String::new(),
                duration_ms: 100 * (i as u64 + 1),
                completed_at: Utc::now(),
                fuel_consumed: 0,
                execution_error: None,
                is_e2ee: false,
                blind_execution_proof: None,
                journal_dump: None,
                output_blob_hash: None,
            });
            knowledge.merge_assignment(assignment);
        }

        // Aggregate results by collecting assignments for the job.
        let assignments = knowledge.get_assignments_for_job(&job_id);
        assert_eq!(assignments.len(), 3);

        // All should be completed.
        let all_completed = assignments
            .iter()
            .all(|a| a.status == ChunkStatus::Completed);
        assert!(all_completed, "all chunks should be completed");

        // Collect results, sorted by chunk_id index.
        let mut results: Vec<(&ChunkId, &ChunkResult)> = assignments
            .iter()
            .filter_map(|a| {
                let idx = chunk_ids.iter().position(|c| *c == a.chunk_id)?;
                a.result.as_ref().map(|r| (&chunk_ids[idx], r))
            })
            .collect();
        results.sort_by_key(|(cid, _)| chunk_ids.iter().position(|c| c == *cid).unwrap());

        assert_eq!(results.len(), 3, "should have 3 results");

        // Verify result contents.
        for (i, (_cid, result)) in results.iter().enumerate() {
            assert!(result.success);
            assert_eq!(
                String::from_utf8_lossy(&result.output),
                format!("output-{}", i)
            );
            assert_eq!(result.stdout, format!("stdout-{}", i));
            assert_eq!(result.duration_ms, 100 * (i as u64 + 1));
        }

        // Verify job progress tracking works.
        knowledge.update_job_progress(&job_id, 3, 0);
        let stored_job = knowledge.get_job(&job_id).unwrap();
        assert_eq!(stored_job.chunks_completed, 3);
        assert_eq!(stored_job.chunks_failed, 0);
    }

    // ========================================================================
    // Test 9: Bootstrap peer parsing
    // ========================================================================

    #[test]
    fn test_bootstrap_peer_parsing() {
        // Parse IPv4 address.
        let addr: Result<SocketAddr, _> = "127.0.0.1:4200".parse();
        assert!(addr.is_ok(), "should parse IPv4 address");
        let addr = addr.unwrap();
        assert_eq!(addr.port(), 4200);
        assert!(addr.ip().is_loopback());

        // Parse IPv6 address.
        let addr6: Result<SocketAddr, _> = "[::1]:4200".parse();
        assert!(addr6.is_ok(), "should parse IPv6 address");
        let addr6 = addr6.unwrap();
        assert_eq!(addr6.port(), 4200);
        assert!(addr6.ip().is_loopback());

        // Parse non-loopback IPv4.
        let addr_ext: Result<SocketAddr, _> = "192.168.1.100:9000".parse();
        assert!(addr_ext.is_ok(), "should parse non-loopback IPv4");
        assert_eq!(addr_ext.unwrap().port(), 9000);

        // Parse full IPv6.
        let addr_full: Result<SocketAddr, _> = "[2001:db8::1]:4200".parse();
        assert!(addr_full.is_ok(), "should parse full IPv6 address");

        // Invalid addresses should fail.
        let invalid_cases = vec![
            "not-an-address",
            "127.0.0.1",       // missing port
            ":4200",           // missing host
            "999.999.999.999:4200", // invalid octets
            "",
        ];

        for case in &invalid_cases {
            let result: Result<SocketAddr, _> = case.parse();
            assert!(
                result.is_err(),
                "should reject invalid address: '{}'",
                case
            );
        }

        // Simulate bootstrap parsing: filter valid addresses from a mixed list.
        let seeds = vec![
            "127.0.0.1:4200".to_string(),
            "invalid".to_string(),
            "[::1]:4200".to_string(),
            "also-invalid:not-a-port".to_string(),
            "10.0.0.1:5000".to_string(),
        ];

        let parsed: Vec<SocketAddr> = seeds
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        assert_eq!(parsed.len(), 3, "should parse 3 valid addresses from 5 inputs");
        assert_eq!(parsed[0], "127.0.0.1:4200".parse::<SocketAddr>().unwrap());
        assert_eq!(parsed[1], "[::1]:4200".parse::<SocketAddr>().unwrap());
        assert_eq!(parsed[2], "10.0.0.1:5000".parse::<SocketAddr>().unwrap());
    }

    // ========================================================================
    // Test 10: SwarmNode creation (config-based, no network)
    // ========================================================================

    #[test]
    fn test_swarm_node_creation_via_config() {
        // We test the constituent parts that a SwarmNode would create
        // internally, since SwarmNode::new() is async and requires
        // network binding. This verifies the same initialization logic.
        let config = SwarmConfig::default();

        // Node identity: generate and verify.
        let node_id = NodeId::new();
        assert_ne!(
            node_id.0,
            uuid::Uuid::nil(),
            "NodeId should not be nil"
        );

        // Knowledge store creation.
        let knowledge = Arc::new(KnowledgeStore::new(node_id));
        assert_eq!(knowledge.node_count(), 0);
        assert_eq!(knowledge.job_count(), 0);
        assert_eq!(knowledge.assignment_count(), 0);

        // Initial trait evaluation.
        let resources = measure_resources();
        let connectivity = ConnectivityInfo::default();
        let force_traits: HashSet<Trait> = HashSet::new();
        let deny_traits: HashSet<Trait> = HashSet::new();
        let load = 0.0_f32; // fresh node has no load

        let initial_traits = evaluate_traits(
            &resources,
            load,
            &connectivity,
            &force_traits,
            &deny_traits,
        );

        // A fresh node with default connectivity (not publicly reachable)
        // should at least have CanExecute if CPU is available.
        if resources.cpu_available > THRESHOLD_EXECUTE_CPU {
            assert!(
                initial_traits.contains(&Trait::CanExecute),
                "fresh node with available CPU should claim CanExecute"
            );
        }

        // Verify config defaults.
        assert_eq!(config.listen_addr, "0.0.0.0:4200");
        assert_eq!(config.max_concurrent_chunks, MAX_CONCURRENT_CHUNKS);
        assert_eq!(config.gossip_interval, GOSSIP_INTERVAL);
        assert_eq!(config.gossip_fanout, GOSSIP_FANOUT);
        assert!(config.bootstrap_servers.is_empty());
    }

    // ========================================================================
    // Test 11: Transport encode/decode roundtrip
    // ========================================================================

    #[test]
    fn test_transport_encode_decode_roundtrip() {
        // Generate a test identity for signing all messages.
        let identity = NodeIdentity::generate();

        // Test Gossip message roundtrip.
        let gossip_msg = SwarmMessage::Gossip(GossipMessage {
            sender_id: NodeId::new(),
            timestamp: Utc::now(),
            generation: 42,
            my_traits: HashSet::from([Trait::CanExecute, Trait::CanRelay]),
            my_load: 0.35,
            my_capacity: make_resources(0.65, 2048, 10240, 50.0),
            known_nodes: vec![],
            known_jobs: vec![],
            known_assignments: vec![],
            known_profiles: vec![],
            known_collectives: vec![],
            known_reputation: vec![],
            policy: None,
            known_postings: vec![],
            known_awards: vec![],
            known_probations: vec![],
            known_admission_decisions: vec![],
            known_blobs: vec![],
            software_inventories: vec![],
            witness_reports: vec![],
            envelope_signature: vec![],
            sender_public_key: vec![],
            psyche_snapshot: None,
            fleet_states: vec![],
            active_alerts: vec![],
            sla_compliance: vec![],
            pg_nodes: vec![],
            zone_certificates: vec![],
            zone_revocations: vec![],
            key_rotations: vec![],
            key_revocations: vec![],
        });
        let fed_id = FederationId::generate();
        let encoded = encode_signed(&gossip_msg, &identity, fed_id).expect("encode gossip");
        assert!(!encoded.is_empty());
        let decoded = decode_message(&encoded, fed_id).expect("decode gossip");
        if let SwarmMessage::Gossip(g) = &decoded {
            if let SwarmMessage::Gossip(orig) = &gossip_msg {
                assert_eq!(g.sender_id, orig.sender_id);
                assert_eq!(g.generation, orig.generation);
                assert_eq!(g.my_traits, orig.my_traits);
                assert!((g.my_load - orig.my_load).abs() < f32::EPSILON);
            }
        } else {
            panic!("decoded message should be Gossip variant");
        }

        // Test Ping roundtrip.
        let ping = SwarmMessage::Ping {
            from: NodeId::new(),
            nonce: 12345678,
            pow_nonce: None,
        };
        let fed_id = FederationId::generate();
        let encoded = encode_signed(&ping, &identity, fed_id).expect("encode ping");
        let decoded = decode_message(&encoded, fed_id).expect("decode ping");
        if let SwarmMessage::Ping { from, nonce, pow_nonce: _ } = &decoded {
            if let SwarmMessage::Ping {
                from: orig_from,
                nonce: orig_nonce,
                pow_nonce: _,
            } = &ping
            {
                assert_eq!(from, orig_from);
                assert_eq!(nonce, orig_nonce);
            }
        } else {
            panic!("decoded message should be Ping variant");
        }

        // Test Pong roundtrip.
        let pong = SwarmMessage::Pong {
            from: NodeId::new(),
            nonce: 87654321,
        };
        let fed_id = FederationId::generate();
        let encoded = encode_signed(&pong, &identity, fed_id).expect("encode pong");
        let decoded = decode_message(&encoded, fed_id).expect("decode pong");
        if let SwarmMessage::Pong { nonce, .. } = &decoded {
            assert_eq!(*nonce, 87654321);
        } else {
            panic!("decoded message should be Pong variant");
        }

        // Test ChunkData roundtrip.
        let chunk_data = SwarmMessage::ChunkData {
            chunk: Chunk {
                id: ChunkId::new(),
                job_id: JobId::new(),
                sequence: 7,
                payload: TaskPayload::Shell {
                    command: "ls".to_string(),
                    args: vec!["-la".to_string()],
                },
                created_at: Utc::now(),
            },
            from: NodeId::new(),
        };
        let fed_id = FederationId::generate();
        let encoded = encode_signed(&chunk_data, &identity, fed_id).expect("encode chunk data");
        let decoded = decode_message(&encoded, fed_id).expect("decode chunk data");
        if let SwarmMessage::ChunkData { chunk, .. } = &decoded {
            assert_eq!(chunk.sequence, 7);
        } else {
            panic!("decoded message should be ChunkData variant");
        }

        // Test BootstrapRequest roundtrip.
        let bootstrap_req = SwarmMessage::BootstrapRequest {
            node_id: NodeId::new(),
            address: Some("192.168.1.1:4200".parse().unwrap()),
            traits: HashSet::from([Trait::CanExecute, Trait::CanForward]),
        };
        let fed_id = FederationId::generate();
        let encoded = encode_signed(&bootstrap_req, &identity, fed_id).expect("encode bootstrap req");
        let decoded = decode_message(&encoded, fed_id).expect("decode bootstrap req");
        if let SwarmMessage::BootstrapRequest { address, traits, .. } = &decoded {
            assert_eq!(*address, Some("192.168.1.1:4200".parse().unwrap()));
            assert!(traits.contains(&Trait::CanExecute));
            assert!(traits.contains(&Trait::CanForward));
        } else {
            panic!("decoded message should be BootstrapRequest variant");
        }

        // Test ChunkResult roundtrip.
        let chunk_result_msg = SwarmMessage::ChunkResult {
            chunk_id: ChunkId::new(),
            job_id: JobId::new(),
            result: ChunkResult {
                success: true,
                output: b"hello world".to_vec(),
                stdout: "hello world\n".to_string(),
                stderr: String::new(),
                duration_ms: 42,
                completed_at: Utc::now(),
                fuel_consumed: 0,
                execution_error: None,
                is_e2ee: false,
                blind_execution_proof: None,
                journal_dump: None,
                output_blob_hash: None,
            },
            from: NodeId::new(),
        };
        let fed_id = FederationId::generate();
        let encoded = encode_signed(&chunk_result_msg, &identity, fed_id).expect("encode chunk result");
        let decoded = decode_message(&encoded, fed_id).expect("decode chunk result");
        if let SwarmMessage::ChunkResult { result, .. } = &decoded {
            assert!(result.success);
            assert_eq!(result.output, b"hello world");
            assert_eq!(result.duration_ms, 42);
        } else {
            panic!("decoded message should be ChunkResult variant");
        }
    }

    // ========================================================================
    // Test 12: Knowledge store pruning
    // ========================================================================

    #[test]
    fn test_knowledge_store_pruning() {
        let self_id = NodeId::new();
        let knowledge = KnowledgeStore::new(self_id);

        // Add nodes with very old timestamps (past NODE_TIMEOUT = 120s).
        let old_node_1 = NodeId::new();
        let old_node_2 = NodeId::new();
        let fresh_node = NodeId::new();

        knowledge.merge_node(make_node_info(
            old_node_1,
            Utc::now() - ChronoDuration::seconds(300),
            NodeStatus::Dead,
            1,
        ));
        knowledge.merge_node(make_node_info(
            old_node_2,
            Utc::now() - ChronoDuration::seconds(200),
            NodeStatus::Dead,
            1,
        ));
        knowledge.merge_node(make_node_info(
            fresh_node,
            Utc::now(),
            NodeStatus::Alive,
            1,
        ));

        // Add completed jobs: one old (> 5 min), one fresh.
        let old_job = JobId::new();
        let fresh_job = JobId::new();
        let mut old_job_info = make_job_info(old_job, 5);
        old_job_info.status = SwarmJobStatus::Completed;
        old_job_info.updated_at = Utc::now() - ChronoDuration::minutes(10);
        knowledge.merge_job(old_job_info);

        let mut fresh_job_info = make_job_info(fresh_job, 3);
        fresh_job_info.status = SwarmJobStatus::InProgress;
        knowledge.merge_job(fresh_job_info);

        // Add completed assignments: one old (> 10 min), one fresh.
        let old_chunk = ChunkId::new();
        let fresh_chunk = ChunkId::new();

        let mut old_assignment = make_assignment(old_chunk, old_job);
        old_assignment.status = ChunkStatus::Completed;
        old_assignment.assigned_at = Utc::now() - ChronoDuration::minutes(15);
        knowledge.merge_assignment(old_assignment);

        let fresh_assignment = make_assignment(fresh_chunk, fresh_job);
        knowledge.merge_assignment(fresh_assignment);

        // Record counts before pruning.
        assert_eq!(knowledge.node_count(), 3);
        assert_eq!(knowledge.job_count(), 2);
        assert_eq!(knowledge.assignment_count(), 2);

        // Prune stale entries.
        let stats = knowledge.prune_stale();

        // Verify stale entries are removed.
        assert_eq!(stats.nodes_removed, 2, "2 old nodes should be pruned");
        assert_eq!(stats.jobs_removed, 1, "1 old completed job should be pruned");
        assert_eq!(
            stats.assignments_removed, 1,
            "1 old completed assignment should be pruned"
        );

        // Verify fresh entries are kept.
        assert_eq!(knowledge.node_count(), 1);
        assert!(knowledge.get_node(&fresh_node).is_some());
        assert!(knowledge.get_node(&old_node_1).is_none());
        assert!(knowledge.get_node(&old_node_2).is_none());

        assert_eq!(knowledge.job_count(), 1);
        assert!(knowledge.get_job(&fresh_job).is_some());
        assert!(knowledge.get_job(&old_job).is_none());

        assert_eq!(knowledge.assignment_count(), 1);
        assert!(knowledge.get_assignment(&fresh_chunk).is_some());
        assert!(knowledge.get_assignment(&old_chunk).is_none());

        // Verify self is never pruned even if very old.
        let mut self_node_info = make_node_info(
            self_id,
            Utc::now() - ChronoDuration::seconds(500),
            NodeStatus::Alive,
            1,
        );
        knowledge.merge_node(self_node_info);
        let stats2 = knowledge.prune_stale();
        assert_eq!(stats2.nodes_removed, 0, "self node should never be pruned");
        assert!(knowledge.get_node(&self_id).is_some());
    }

    // ========================================================================
    // Test 13: Trait dynamics simulation
    // ========================================================================

    #[test]
    fn test_trait_dynamics_simulation() {
        let conn = public_connectivity();
        let force = HashSet::new();
        let deny = HashSet::new();

        // Phase 1: High resources (idle node) -> full trait set.
        let high_res = make_resources(0.9, 4096, 51200, 100.0);
        let traits_idle = evaluate_traits(&high_res, 0.1, &conn, &force, &deny);

        assert!(traits_idle.contains(&Trait::CanExecute));
        assert!(traits_idle.contains(&Trait::CanForward));
        assert!(traits_idle.contains(&Trait::CanAggregate));
        assert!(traits_idle.contains(&Trait::CanStoreState));
        assert!(traits_idle.contains(&Trait::CanDiscover));
        assert!(traits_idle.contains(&Trait::CanRelay));

        // Phase 2: Simulate load increase -> traits are shed progressively.
        // At load 0.55: CanForward (threshold 0.5) and CanRelay (threshold 0.5) shed.
        let traits_moderate = evaluate_traits(&high_res, 0.55, &conn, &force, &deny);
        assert!(
            traits_moderate.contains(&Trait::CanExecute),
            "CanExecute survives at 0.55 load"
        );
        assert!(
            !traits_moderate.contains(&Trait::CanForward),
            "CanForward shed at 0.55 load"
        );
        assert!(
            !traits_moderate.contains(&Trait::CanRelay),
            "CanRelay shed at 0.55 load"
        );
        assert!(
            traits_moderate.contains(&Trait::CanAggregate),
            "CanAggregate survives at 0.55 load"
        );
        assert!(
            traits_moderate.contains(&Trait::CanStoreState),
            "CanStoreState survives at 0.55 load"
        );

        // At load 0.65: CanStoreState (threshold 0.6) also shed.
        let traits_loaded = evaluate_traits(&high_res, 0.65, &conn, &force, &deny);
        assert!(traits_loaded.contains(&Trait::CanExecute));
        assert!(!traits_loaded.contains(&Trait::CanStoreState));
        assert!(traits_loaded.contains(&Trait::CanAggregate));

        // At load 0.75: CanAggregate (threshold 0.7) also shed.
        let traits_heavy = evaluate_traits(&high_res, 0.75, &conn, &force, &deny);
        assert!(traits_heavy.contains(&Trait::CanExecute));
        assert!(!traits_heavy.contains(&Trait::CanAggregate));

        // At load 0.85: CanExecute (threshold 0.8) also shed; only CanDiscover remains.
        let traits_overloaded = evaluate_traits(&high_res, 0.85, &conn, &force, &deny);
        assert!(!traits_overloaded.contains(&Trait::CanExecute));
        assert!(
            traits_overloaded.contains(&Trait::CanDiscover),
            "CanDiscover depends only on connectivity, not load"
        );

        // Phase 3: Load decreases back to idle -> all traits return.
        let traits_recovered = evaluate_traits(&high_res, 0.1, &conn, &force, &deny);
        assert_eq!(
            traits_recovered, traits_idle,
            "after load decrease, traits should return to full set"
        );
    }

    // ========================================================================
    // Test 14: Gossip merge convergence
    // ========================================================================

    #[test]
    fn test_gossip_merge_convergence() {
        // Create 3 knowledge stores simulating 3 distinct nodes.
        let id_a = NodeId::new();
        let id_b = NodeId::new();
        let id_c = NodeId::new();

        let store_a = KnowledgeStore::new(id_a);
        let store_b = KnowledgeStore::new(id_b);
        let store_c = KnowledgeStore::new(id_c);

        // Each node knows about itself and one unique peer.
        let peer_1 = NodeId::new();
        let peer_2 = NodeId::new();
        let peer_3 = NodeId::new();

        let t_now = Utc::now();

        // Store A knows: self (id_a), peer_1.
        store_a.merge_node(make_node_info(id_a, t_now, NodeStatus::Alive, 1));
        store_a.merge_node(make_node_info(peer_1, t_now, NodeStatus::Alive, 1));

        // Store B knows: self (id_b), peer_2.
        store_b.merge_node(make_node_info(id_b, t_now, NodeStatus::Alive, 1));
        store_b.merge_node(make_node_info(peer_2, t_now, NodeStatus::Alive, 1));

        // Store C knows: self (id_c), peer_3.
        store_c.merge_node(make_node_info(id_c, t_now, NodeStatus::Alive, 1));
        store_c.merge_node(make_node_info(peer_3, t_now, NodeStatus::Alive, 1));

        // Also give each store some unique jobs.
        let job_a = JobId::new();
        let job_b = JobId::new();
        let job_c = JobId::new();
        store_a.merge_job(make_job_info(job_a, 5));
        store_b.merge_job(make_job_info(job_b, 3));
        store_c.merge_job(make_job_info(job_c, 7));

        // Round 1: A sends to B, B sends to C, C sends to A.
        // (Simulate by copying all knowledge.)
        let exchange = |from: &KnowledgeStore, to: &KnowledgeStore| {
            for node_info in from.get_all_nodes() {
                to.merge_node(node_info);
            }
            for job_info in from.sample_jobs(100) {
                to.merge_job(job_info);
            }
        };

        exchange(&store_a, &store_b);
        exchange(&store_b, &store_c);
        exchange(&store_c, &store_a);

        // Round 2: Another full exchange round to ensure convergence.
        exchange(&store_a, &store_b);
        exchange(&store_b, &store_c);
        exchange(&store_c, &store_a);

        // Also propagate in reverse direction for full convergence.
        exchange(&store_b, &store_a);
        exchange(&store_c, &store_b);
        exchange(&store_a, &store_c);

        // After gossip exchange, all 3 stores should know about all 6 nodes.
        let expected_nodes: HashSet<NodeId> =
            HashSet::from([id_a, id_b, id_c, peer_1, peer_2, peer_3]);

        for (name, store) in [("A", &store_a), ("B", &store_b), ("C", &store_c)] {
            let all_nodes = store.get_all_nodes();
            let known_ids: HashSet<NodeId> = all_nodes.iter().map(|n| n.node_id).collect();
            assert_eq!(
                known_ids, expected_nodes,
                "Store {} should know about all 6 nodes after convergence",
                name
            );
        }

        // All 3 stores should know about all 3 jobs.
        let expected_jobs: HashSet<JobId> = HashSet::from([job_a, job_b, job_c]);

        for (name, store) in [("A", &store_a), ("B", &store_b), ("C", &store_c)] {
            let has_a = store.get_job(&job_a).is_some();
            let has_b = store.get_job(&job_b).is_some();
            let has_c = store.get_job(&job_c).is_some();
            assert!(
                has_a && has_b && has_c,
                "Store {} should know about all 3 jobs after convergence (a={}, b={}, c={})",
                name, has_a, has_b, has_c
            );
        }
    }

    // ========================================================================
    // Test 15: Dead node work reassignment
    // ========================================================================

    #[test]
    fn test_dead_node_work_reassignment() {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let doomed_node = NodeId::new();
        let rescuer_node = NodeId::new();
        let job_id = JobId::new();

        // Add the doomed node (alive, with recent last_seen).
        knowledge.merge_node(make_node_info(
            doomed_node,
            Utc::now(),
            NodeStatus::Alive,
            1,
        ));

        // Add the rescuer node.
        knowledge.merge_node(make_node_info(
            rescuer_node,
            Utc::now(),
            NodeStatus::Alive,
            1,
        ));

        // Create 3 chunks assigned to the doomed node.
        let chunk_1 = ChunkId::new();
        let chunk_2 = ChunkId::new();
        let chunk_3 = ChunkId::new();

        knowledge.merge_assignment(make_assignment(chunk_1, job_id));
        knowledge.merge_assignment(make_assignment(chunk_2, job_id));
        knowledge.merge_assignment(make_assignment(chunk_3, job_id));

        // Doomed node claims all 3 chunks.
        assert!(knowledge.claim_chunk(&chunk_1, doomed_node));
        assert!(knowledge.claim_chunk(&chunk_2, doomed_node));
        assert!(knowledge.claim_chunk(&chunk_3, doomed_node));

        // Verify all chunks are InProgress and assigned to doomed_node.
        for &cid in &[chunk_1, chunk_2, chunk_3] {
            let a = knowledge.get_assignment(&cid).unwrap();
            assert_eq!(a.status, ChunkStatus::InProgress);
            assert_eq!(a.assigned_to, Some(doomed_node));
        }

        // Mark the doomed node as dead.
        knowledge.mark_node_dead(&doomed_node);
        assert_eq!(
            knowledge.get_node(&doomed_node).unwrap().status,
            NodeStatus::Dead
        );

        // Release all chunks from the dead node.
        let released = knowledge.release_chunks_from_node(&doomed_node);
        assert_eq!(
            released.len(),
            3,
            "all 3 InProgress chunks should be released"
        );

        // Verify all chunks are back to Pending.
        for &cid in &[chunk_1, chunk_2, chunk_3] {
            let a = knowledge.get_assignment(&cid).unwrap();
            assert_eq!(
                a.status,
                ChunkStatus::Pending,
                "released chunk should be Pending"
            );
            assert_eq!(
                a.assigned_to, None,
                "released chunk should have no assignee"
            );
        }

        // Verify another node can now claim the released chunks.
        assert!(
            knowledge.claim_chunk(&chunk_1, rescuer_node),
            "rescuer should be able to claim released chunk 1"
        );
        assert!(
            knowledge.claim_chunk(&chunk_2, rescuer_node),
            "rescuer should be able to claim released chunk 2"
        );
        assert!(
            knowledge.claim_chunk(&chunk_3, rescuer_node),
            "rescuer should be able to claim released chunk 3"
        );

        // Verify all chunks are now assigned to the rescuer.
        for &cid in &[chunk_1, chunk_2, chunk_3] {
            let a = knowledge.get_assignment(&cid).unwrap();
            assert_eq!(a.status, ChunkStatus::InProgress);
            assert_eq!(a.assigned_to, Some(rescuer_node));
        }

        // The doomed node's old claims are gone; it has no assignments.
        let doomed_assignments = knowledge.get_assignments_for_node(&doomed_node);
        assert!(
            doomed_assignments.is_empty(),
            "dead node should have no remaining assignments"
        );

        // The rescuer has all 3.
        let rescuer_assignments = knowledge.get_assignments_for_node(&rescuer_node);
        assert_eq!(
            rescuer_assignments.len(),
            3,
            "rescuer should have all 3 assignments"
        );
    }
}
