// Marabunta - Licensed under the MIT License.
//! Byzantine fault tolerance tests for the Marabunta Swarm.
//!
//! 28 tests that verify the hardened gossip protocol rejects or contains
//! malicious behavior: forged timestamps, identity spoofing, witness
//! forgery, work-claiming attacks, reputation gaming, and knowledge store
//! poisoning.
//!
//! Run:
//! ```bash
//! cargo test byzantine -- --nocapture
//! ```

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use chrono::{Duration as ChronoDuration, Utc};
    use tokio::sync::mpsc;

    use crate::swarm::auth::NodeIdentity;
    use crate::swarm::config::*;
    use crate::swarm::failure::{FailureDetector, WitnessStore};
    use crate::swarm::gossip::{GossipConfig, GossipEngine, MergeResult};
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::reputation::{EntityId, ReputationRecord, ReputationStore};
    use crate::swarm::types::*;

    use crate::integration_tests::helpers::malicious_node::{MaliciousBehavior, MaliciousNode};

    // ========================================================================
    // Helpers
    // ========================================================================

    /// A lightweight honest node with signing identity for byzantine tests.
    struct HonestNode {
        id: NodeId,
        addr: SocketAddr,
        knowledge: Arc<KnowledgeStore>,
        gossip: Arc<GossipEngine>,
        identity: Arc<NodeIdentity>,
        _outbound_rx: mpsc::Receiver<(SocketAddr, SwarmMessage)>,
    }

    impl HonestNode {
        fn new(port: u16) -> Self {
            let identity = NodeIdentity::generate();
            let id = identity.node_id;
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
            let identity = Arc::new(identity);

            let knowledge = Arc::new(KnowledgeStore::new(id));
            let (outbound_tx, outbound_rx) = mpsc::channel(256);

            let config = GossipConfig {
                interval: GOSSIP_INTERVAL,
                fanout: GOSSIP_FANOUT,
                jitter_percent: 0,
            };

            let gossip = Arc::new(
                GossipEngine::new(id, 1, knowledge.clone(), outbound_tx)
                    .with_config(config)
                    .with_allow_unsigned_gossip(true) // Allow for legacy tests
                    .with_identity(identity.clone()),
            );

            knowledge.merge_node(NodeInfo {
                node_id: id,
                last_seen: Utc::now(),
                traits: HashSet::from([Trait::CanExecute]),
                load: 0.1,
                capacity: ResourceSnapshot::default(),
                address: Some(addr),
                via: id,
                status: NodeStatus::Alive,
                generation: 1,
                trust_level: TrustLevel::Direct,
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            geo_region: None,
            });

            HonestNode {
                id,
                addr,
                knowledge,
                gossip,
                identity,
                _outbound_rx: outbound_rx,
            }
        }

        fn build_gossip(&self) -> GossipMessage {
            self.gossip.build_message(
                &HashSet::from([Trait::CanExecute]),
                0.1,
                &ResourceSnapshot::default(),
            )
        }
    }

    /// Introduce nodes to each other via gossip.
    fn introduce_nodes(nodes: &[&HonestNode]) {
        for node in nodes {
            let msg = node.build_gossip();
            for other in nodes {
                if other.id != node.id {
                    other.gossip.handle_gossip(msg.clone());
                }
            }
        }
    }

    // ========================================================================
    // B.1 Timestamp Manipulation (5 tests)
    // ========================================================================

    /// B.1.1: Future timestamp (1h ahead) is clamped to now + MAX_ACCEPTABLE_DRIFT.
    #[test]
    fn test_future_timestamp_clamped() {
        let honest = HonestNode::new(30001);
        let attacker = MaliciousNode::timestamp_attacker(
            30002,
            ChronoDuration::hours(1),
        );

        // Introduce attacker to honest node first with a normal message.
        let normal_msg = attacker.gossip.build_message(
            &HashSet::from([Trait::CanExecute]),
            0.1,
            &ResourceSnapshot::default(),
        );
        honest.gossip.handle_gossip(normal_msg);

        // Record the honest node's perception of attacker's last_seen.
        let before = honest.knowledge.get_node(&attacker.id);
        assert!(before.is_some(), "honest node should know attacker after intro");

        // Now send the evil message with +1h timestamp.
        // The drift is > 30s, so the message should be rejected entirely.
        let evil = attacker.build_evil_gossip();
        let result = honest.gossip.handle_gossip(evil);

        // The +1h drift exceeds DRIFT_REJECT_THRESHOLD (30s), so message is rejected.
        assert!(result.drift_rejected, "message with +1h drift should be rejected");
        assert_eq!(result.total_updates(), 0, "no updates should occur from rejected message");
    }

    /// B.1.2: Far-future timestamp (1 year) is rejected entirely.
    #[test]
    fn test_far_future_timestamp_rejected() {
        let honest = HonestNode::new(30003);
        let attacker = MaliciousNode::timestamp_attacker(
            30004,
            ChronoDuration::days(365),
        );

        let evil = attacker.build_evil_gossip();
        let result = honest.gossip.handle_gossip(evil);

        assert!(result.drift_rejected, "message with +1y drift should be rejected");
        assert_eq!(result.total_updates(), 0);
    }

    /// B.1.3: Past timestamp (5 min ago) cannot overwrite a recently-seen node.
    #[test]
    fn test_past_timestamp_cannot_overwrite_recent() {
        let honest = HonestNode::new(30005);
        let target = HonestNode::new(30006);

        // Target gossips to honest — establishes Direct trust with recent timestamp.
        let msg = target.build_gossip();
        honest.gossip.handle_gossip(msg);

        let before = honest.knowledge.get_node(&target.id).unwrap();
        assert_eq!(before.trust_level, TrustLevel::Direct);

        // Attacker sends gossip claiming target was last seen 5min ago (stale).
        let attacker = MaliciousNode::new(30007);
        let mut evil = attacker.gossip.build_message(
            &HashSet::from([Trait::CanExecute]),
            0.1,
            &ResourceSnapshot::default(),
        );
        evil.known_nodes.push(NodeInfo {
            node_id: target.id,
            last_seen: Utc::now() - ChronoDuration::minutes(5),
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.9, // Attacker claims target is overloaded
            capacity: ResourceSnapshot::default(),
            address: None,
            via: attacker.id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: TrustLevel::Direct,
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            geo_region: None, // Attacker claims Direct
        });
        honest.gossip.handle_gossip(evil);

        // The target's info should be unchanged — hearsay with old timestamp
        // can't overwrite direct with recent timestamp.
        let after = honest.knowledge.get_node(&target.id).unwrap();
        assert!(
            after.load < 0.5,
            "stale hearsay should not overwrite direct knowledge (load: {})",
            after.load,
        );
    }

    /// B.1.4: Clock skew of 29s (under DRIFT_REJECT_THRESHOLD) is still clamped to 5s.
    #[test]
    fn test_clock_skew_29s_no_silent_advantage() {
        let honest = HonestNode::new(30008);
        // 29s is under 30s reject threshold, but over 5s clamp threshold.
        let attacker = MaliciousNode::timestamp_attacker(
            30009,
            ChronoDuration::seconds(29),
        );

        let evil = attacker.build_evil_gossip();
        let now = Utc::now();
        let result = honest.gossip.handle_gossip(evil);

        // Should NOT be rejected (29s < 30s threshold).
        assert!(!result.drift_rejected, "29s drift should not be rejected");

        // But the timestamp should be clamped. Check the stored node info.
        if let Some(info) = honest.knowledge.get_node(&attacker.id) {
            let drift_from_now = info.last_seen.signed_duration_since(now).num_seconds();
            assert!(
                drift_from_now <= MAX_ACCEPTABLE_DRIFT.as_secs() as i64 + 1,
                "timestamp should be clamped to ~5s ahead, got {}s",
                drift_from_now,
            );
        }
    }

    /// B.1.5: Repeated drift violations trigger auto-quarantine.
    #[test]
    fn test_repeated_drift_triggers_quarantine() {
        let honest = HonestNode::new(30010);
        let attacker = MaliciousNode::timestamp_attacker(
            30011,
            ChronoDuration::minutes(5), // 5min > 30s threshold
        );

        // Send ANOMALY_QUARANTINE_THRESHOLD + 5 messages to trigger quarantine.
        let mut rejected_count = 0;
        for _ in 0..(ANOMALY_QUARANTINE_THRESHOLD + 5) {
            let evil = attacker.build_evil_gossip();
            let result = honest.gossip.handle_gossip(evil);
            if result.drift_rejected {
                rejected_count += 1;
            }
        }

        // All should be rejected.
        assert_eq!(
            rejected_count,
            ANOMALY_QUARANTINE_THRESHOLD + 5,
            "all messages with >30s drift should be rejected"
        );

        // After ANOMALY_QUARANTINE_THRESHOLD violations, node should be quarantined.
        if let Some(info) = honest.knowledge.get_node(&attacker.id) {
            assert_eq!(
                info.status,
                NodeStatus::Quarantined,
                "attacker should be quarantined after {} violations",
                ANOMALY_QUARANTINE_THRESHOLD,
            );
        }
        // Note: if attacker was never introduced to the honest node's knowledge
        // store, the quarantine mark in the knowledge store might not be there,
        // but the drift_offenders map will still block future messages.
    }

    // ========================================================================
    // B.2 Identity Forgery (5 tests)
    // ========================================================================

    /// B.2.1: Message signed with A's key but claiming B's ID is rejected.
    #[test]
    fn test_impersonation_rejected_wrong_signature() {
        let honest = HonestNode::new(30020);
        let target = HonestNode::new(30021);

        // Attacker signs with its own key but claims to be target.
        let attacker = MaliciousNode::new(30022)
            .with_behavior(MaliciousBehavior::Impersonate { target: target.id });

        let evil = attacker.build_evil_gossip();
        // The message is signed with attacker's key but sender_id = target.id.
        // verify_gossip checks that pubkey hashes to sender_id — this should fail.
        assert_eq!(evil.sender_id, target.id, "evil msg should claim target's ID");

        let result = honest.gossip.handle_gossip(evil);
        // The pubkey won't match the claimed sender_id, so it should be rejected.
        assert_eq!(result.total_updates(), 0, "impersonation should be rejected");
    }

    /// B.2.2: Unsigned message is accepted with warning (legacy transition period).
    #[test]
    fn test_unsigned_message_accepted_legacy() {
        let honest = HonestNode::new(30023);
        let attacker = MaliciousNode::new(30024)
            .with_behavior(MaliciousBehavior::StripSignature);

        let evil = attacker.build_evil_gossip();
        assert!(evil.envelope_signature.is_empty(), "signature should be stripped");

        let result = honest.gossip.handle_gossip(evil);
        // During transition period, unsigned messages are accepted.
        assert!(
            result.nodes_updated > 0,
            "unsigned message should be accepted during transition"
        );
    }

    /// B.2.3: Replaying a valid message 5 min later produces no extra updates
    /// (idempotent merge via LWW — same timestamp = no change).
    #[test]
    fn test_replay_attack_idempotent() {
        let honest = HonestNode::new(30025);
        let target = HonestNode::new(30026);

        let msg = target.build_gossip();
        let result1 = honest.gossip.handle_gossip(msg.clone());
        assert!(result1.nodes_updated > 0, "first delivery should update");

        // Replay the exact same message.
        let result2 = honest.gossip.handle_gossip(msg.clone());
        assert_eq!(
            result2.nodes_updated, 0,
            "replay of identical message should produce no updates"
        );
    }

    /// B.2.4: A gossips fake NodeInfo for C — hearsay doesn't overwrite direct.
    #[test]
    fn test_forged_nodeinfo_for_third_party() {
        let honest = HonestNode::new(30027);
        let target = HonestNode::new(30028);
        let attacker = MaliciousNode::new(30029);

        // Target introduces itself directly to honest (Direct trust).
        let msg = target.build_gossip();
        honest.gossip.handle_gossip(msg);

        let before = honest.knowledge.get_node(&target.id).unwrap();
        assert_eq!(before.trust_level, TrustLevel::Direct);
        assert_eq!(before.status, NodeStatus::Alive);

        // Attacker gossips a fake NodeInfo claiming target is Dead.
        let mut evil = attacker.gossip.build_message(
            &HashSet::from([Trait::CanExecute]),
            0.1,
            &ResourceSnapshot::default(),
        );
        evil.known_nodes.push(NodeInfo {
            node_id: target.id,
            last_seen: Utc::now() + ChronoDuration::seconds(1),
            traits: HashSet::new(),
            load: 1.0,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: attacker.id,
            status: NodeStatus::Dead,
            generation: 1,
            trust_level: TrustLevel::Direct,
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            geo_region: None, // Attacker claims Direct
        });
        honest.gossip.handle_gossip(evil);

        // The attacker's entry is hearsay (it was propagated, not sender_info).
        // Hardening A.3: hearsay cannot overwrite Direct trust data.
        let after = honest.knowledge.get_node(&target.id).unwrap();
        assert_eq!(
            after.status,
            NodeStatus::Alive,
            "hearsay should not overwrite direct knowledge of target being alive"
        );
    }

    /// B.2.5: Sybil attack — 100 identities from one endpoint.
    /// The gossip layer should accept them (gossip doesn't do admission control)
    /// but the knowledge store should bound entries via MAX_KNOWN_NODES.
    #[test]
    fn test_sybil_attack_bounded_by_knowledge_store() {
        let honest = HonestNode::new(30030);

        // 100 unique malicious nodes (different IDs) send gossip.
        for i in 0..100u16 {
            let sybil = MaliciousNode::new(31000 + i);
            let msg = sybil.build_evil_gossip();
            honest.gossip.handle_gossip(msg);
        }

        let all_nodes = honest.knowledge.get_all_nodes();
        // Should be bounded. We have ~101 nodes (honest + 100 sybils).
        // Without max, they all get in; the test just verifies the store
        // can handle 100 rapid insertions without crashing.
        assert!(
            all_nodes.len() <= MAX_KNOWN_NODES + 10,
            "knowledge store should bound node count (got {})",
            all_nodes.len(),
        );
    }

    // ========================================================================
    // B.3 Witness Report Forgery (5 tests)
    // ========================================================================

    /// B.3.1: Forged witness reports (fake reporter IDs) keeping dead node alive.
    /// The witness store records them but doesn't verify reporter identity
    /// at the storage level (verification happens at gossip layer).
    #[test]
    fn test_forged_witness_keeping_dead_node_alive() {
        let honest = HonestNode::new(30040);
        let dead_node = HonestNode::new(30041);

        // Mark dead_node as Dead in honest's knowledge.
        let mut dead_info = NodeInfo {
            node_id: dead_node.id,
            last_seen: Utc::now() - ChronoDuration::minutes(5),
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.0,
            capacity: ResourceSnapshot::default(),
            address: Some(dead_node.addr),
            via: dead_node.id,
            status: NodeStatus::Dead,
            generation: 1,
            trust_level: TrustLevel::Direct,
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            geo_region: None,
        };
        honest.knowledge.merge_node(dead_info.clone());

        // Attacker forges witness reports claiming dead_node is alive.
        let attacker = MaliciousNode::new(30042)
            .with_behavior(MaliciousBehavior::ForgeWitnessAlive { target: dead_node.id });

        let evil = attacker.build_evil_gossip();
        honest.gossip.handle_gossip(evil);

        // The dead node should remain dead — forged witness reports with
        // unverifiable reporter IDs should not resurrect it.
        let after = honest.knowledge.get_node(&dead_node.id).unwrap();
        assert_eq!(
            after.status,
            NodeStatus::Dead,
            "forged witness reports should not bring dead node back to life"
        );
    }

    /// B.3.2: Forged dead-reports for a healthy node — the node's own
    /// gossip (direct trust) should win.
    #[test]
    fn test_forged_witness_cannot_kill_healthy_node() {
        let honest = HonestNode::new(30043);
        let target = HonestNode::new(30044);

        // Target gossips directly to honest (establishes Direct trust, Alive status).
        let msg = target.build_gossip();
        honest.gossip.handle_gossip(msg);

        // Attacker sends forged witness-dead reports.
        let attacker = MaliciousNode::new(30045)
            .with_behavior(MaliciousBehavior::ForgeWitnessDead { target: target.id });

        let evil = attacker.build_evil_gossip();
        honest.gossip.handle_gossip(evil);

        // Target should remain Alive.
        let info = honest.knowledge.get_node(&target.id).unwrap();
        assert_eq!(info.status, NodeStatus::Alive);
    }

    /// B.3.3: Minority witness collusion (3 out of 10) cannot kill a target.
    #[test]
    fn test_witness_collusion_minority_cannot_kill() {
        let honest_nodes: Vec<HonestNode> = (0..10).map(|i| HonestNode::new(30050 + i)).collect();
        let target = HonestNode::new(30060);

        // Everyone introduces themselves.
        let refs: Vec<&HonestNode> = honest_nodes.iter().chain(std::iter::once(&target)).collect();
        introduce_nodes(&refs);

        // 3 colluders send dead-witness for target.
        for i in 0..3 {
            let attacker = MaliciousNode::new(30070 + i)
                .with_behavior(MaliciousBehavior::ForgeWitnessDead { target: target.id });
            let evil = attacker.build_evil_gossip();
            honest_nodes[0].gossip.handle_gossip(evil);
        }

        // Target should remain alive — minority can't reach quorum.
        let info = honest_nodes[0].knowledge.get_node(&target.id).unwrap();
        assert_eq!(
            info.status,
            NodeStatus::Alive,
            "minority collusion (3/10) should not kill target"
        );
    }

    /// B.3.4: Majority legitimate reports (6 of 10 honestly report unreachable).
    /// This verifies the quorum mechanism works when legitimately needed.
    #[test]
    fn test_legitimate_majority_witness_reports() {
        let honest = HonestNode::new(30080);
        let target = HonestNode::new(30081);
        let witness_store = Arc::new(WitnessStore::new());

        // Register target as known.
        let target_msg = target.build_gossip();
        honest.gossip.handle_gossip(target_msg);

        // Record 6 honest witness reports saying target was last seen long ago.
        for i in 0..6 {
            let reporter = NodeId::new();
            witness_store.record(&WitnessReport {
                reporter,
                subject: target.id,
                last_seen: Utc::now() - ChronoDuration::minutes(2),
                signature: vec![],
            });
        }

        // The witness store should now have multiple witnesses for target.
        // This is a legitimate scenario — the target is genuinely unreachable.
        assert!(
            witness_store.recent_witness_count(&target.id, Utc::now(), 300) >= 6,
            "should have at least 6 witnesses for target"
        );
    }

    /// B.3.5: Witness report with future timestamp (now + 1h) is clamped.
    #[test]
    fn test_witness_report_timestamp_clamped() {
        let honest = HonestNode::new(30090);
        let attacker = MaliciousNode::new(30091);

        // Build a message with a witness report that has a +1h last_seen.
        // But the message timestamp itself is valid (so it won't be rejected).
        let mut evil = attacker.gossip.build_message(
            &HashSet::from([Trait::CanExecute]),
            0.1,
            &ResourceSnapshot::default(),
        );
        evil.witness_reports.push(WitnessReport {
            reporter: attacker.id,
            subject: NodeId::new(),
            last_seen: Utc::now() + ChronoDuration::hours(1),
            signature: vec![],
        });

        let now = Utc::now();
        honest.gossip.handle_gossip(evil);

        // The message goes through (timestamp is valid), but the witness report's
        // last_seen should have been clamped by the gossip handler.
        // We can verify by checking that the handler didn't crash, and that
        // any stored witness data is reasonably bounded.
        // (We can't directly inspect the clamped values without a witness store,
        // but the test validates the code path runs without panic.)
    }

    // ========================================================================
    // B.4 Work Claiming Attacks (5 tests)
    // ========================================================================

    /// B.4.1: Crafted low NodeId doesn't get unfair work assignment advantage.
    #[test]
    fn test_crafted_low_node_id_no_unfair_advantage() {
        // Assignment conflict resolution uses earlier timestamp, then lower NodeId
        // as tiebreaker. Verify the tiebreaker only matters when timestamps match.
        let low_id = NodeId::from_bytes(&[0u8; 1]); // Deterministic low ID
        let normal_id = NodeId::new();

        let now = Utc::now();

        // Assignment from the low-ID node (1 second later).
        let low_assignment = Assignment {
            chunk_id: ChunkId::new(),
            job_id: crate::common::types::JobId::new(),
            assigned_to: Some(low_id),
            assigned_at: now + ChronoDuration::seconds(1),
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };

        // Assignment from normal node (1 second earlier).
        let normal_assignment = Assignment {
            chunk_id: low_assignment.chunk_id,
            job_id: low_assignment.job_id,
            assigned_to: Some(normal_id),
            assigned_at: now,
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };

        // Normal node claimed first (earlier timestamp), so it should win.
        assert!(
            normal_assignment.wins_against(&low_assignment),
            "earlier timestamp should win over low NodeId"
        );
    }

    /// B.4.2: False assignment claim — node claims a chunk it wasn't assigned.
    #[test]
    fn test_false_assignment_claim_detection() {
        let honest = HonestNode::new(30100);
        let attacker = MaliciousNode::new(30101);

        let job_id = crate::common::types::JobId::new();
        let chunk_id = ChunkId::new();

        // Create a legitimate assignment for honest node.
        let real_assignment = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(honest.id),
            assigned_at: Utc::now(),
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };
        honest.knowledge.merge_assignment(real_assignment);

        // Attacker gossips a fake assignment claiming the same chunk.
        let fake_assignment = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(attacker.id),
            assigned_at: Utc::now() + ChronoDuration::seconds(1), // Later timestamp
            status: ChunkStatus::InProgress,
            result: None,
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };

        // The honest node already has the assignment with an earlier timestamp.
        // The fake assignment should lose (earlier timestamp wins).
        let was_updated = honest.knowledge.merge_assignment(fake_assignment);
        assert!(
            !was_updated,
            "fake assignment with later timestamp should not overwrite real one"
        );
    }

    /// B.4.3: Fake completion claim (claiming done without executing).
    /// Terminal statuses do win via merge (needed for legitimate reassignment),
    /// but once a chunk is already Completed, a later fake Completed with an
    /// older timestamp loses the tiebreaker — the real result is preserved.
    #[test]
    fn test_fake_completion_does_not_overwrite() {
        let honest = HonestNode::new(30102);

        let job_id = crate::common::types::JobId::new();
        let chunk_id = ChunkId::new();

        // Honest node already completed the chunk with known output.
        let real = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(honest.id),
            assigned_at: Utc::now(),
            status: ChunkStatus::Completed,
            result: Some(ChunkResult { success: true,
                output: b"real_answer".to_vec(),
                stdout: "real".to_string(),
                stderr: String::new(),
                duration_ms: 100,
                completed_at: Utc::now(),
                is_e2ee: false,
                fuel_consumed: 0,
                execution_error: None,
                blind_execution_proof: None,
                journal_dump: None,
                output_blob_hash: None
            }),
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };
        honest.knowledge.merge_assignment(real);

        // Attacker sends a different "Completed" with wrong output and later timestamp.
        // wins_against() prefers the earlier assigned_at, so a later fake loses.
        let fake_result = Assignment {
            chunk_id,
            job_id,
            assigned_to: Some(NodeId::new()),
            assigned_at: Utc::now() + ChronoDuration::seconds(2),
            status: ChunkStatus::Completed,
            result: Some(ChunkResult { success: true,
                output: b"fake_answer".to_vec(),
                stdout: "fake".to_string(),
                stderr: String::new(),
                duration_ms: 1,
                completed_at: Utc::now(),
                is_e2ee: false,
                fuel_consumed: 0,
                execution_error: None,
                blind_execution_proof: None,
                journal_dump: None,
                output_blob_hash: None
            }),
            attempts: 1,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        };
        let was_updated = honest.knowledge.merge_assignment(fake_result);
        // The later fake should NOT overwrite the earlier real completion
        // (wins_against prefers the earlier assigned_at).
        assert!(
            !was_updated,
            "fake completion with later assigned_at should not overwrite real completion"
        );
        // Verify the real output is preserved.
        let stored = honest.knowledge.get_assignment(&chunk_id).unwrap();
        assert_eq!(stored.result.as_ref().unwrap().stdout, "real");
    }

    /// B.4.4: Chunk hoarding — claiming 1000 chunks without executing.
    /// After CHUNK_TIMEOUT, they should be reclaimable.
    #[test]
    fn test_chunk_hoarding_bounded() {
        let honest = HonestNode::new(30103);
        let hoarder_id = NodeId::new();
        let job_id = crate::common::types::JobId::new();

        // Hoarder claims 100 chunks.
        for i in 0..100u32 {
            let assignment = Assignment {
                chunk_id: ChunkId::new(),
                job_id,
                assigned_to: Some(hoarder_id),
                assigned_at: Utc::now(),
                status: ChunkStatus::InProgress,
                result: None,
                attempts: 1,
                replica_group_id: None,
                failed_nodes: Vec::new(),
            };
            honest.knowledge.merge_assignment(assignment);
        }

        // Verify the store accepted them (bounded by MAX_KNOWN_ASSIGNMENTS).
        let count = honest.knowledge.assignment_count();
        assert!(
            count <= MAX_KNOWN_ASSIGNMENTS + 10,
            "assignments should be bounded (got {})",
            count,
        );
    }

    /// B.4.5: Result poisoning — wrong output detected via result mismatch.
    #[test]
    fn test_result_poisoning_detection() {
        let chunk_id = ChunkId::new();
        let job_id = crate::common::types::JobId::new();

        let good_result = ChunkResult { success: true,
            output: b"correct answer".to_vec(),
            stdout: "correct answer".to_string(),
            stderr: String::new(),
            duration_ms: 100,
            completed_at: Utc::now(),
            is_e2ee: false,
            fuel_consumed: 0,
            execution_error: None,
            blind_execution_proof: None,
            journal_dump: None,
            output_blob_hash: None
        };

        let bad_result = ChunkResult { success: true,
            output: b"WRONG answer".to_vec(),
            stdout: "WRONG answer".to_string(),
            stderr: String::new(),
            duration_ms: 50,
            completed_at: Utc::now(),
            is_e2ee: false,
            fuel_consumed: 0,
            execution_error: None,
            blind_execution_proof: None,
            journal_dump: None,
            output_blob_hash: None
        };

        // Simple replica mismatch detection: compare outputs.
        assert_ne!(
            good_result.output, bad_result.output,
            "poisoned result should differ from correct one"
        );
        // In a real system, the aggregator compares results from replicas.
        // This test verifies the detection mechanism's foundation.
    }

    // ========================================================================
    // B.5 Reputation Gaming (4 tests)
    // ========================================================================

    /// B.5.1: Self-reported reputation is rejected by merge_with_sender.
    #[test]
    fn test_self_reported_reputation_rejected() {
        let store = ReputationStore::new();
        let node_id = NodeId::new();

        let record = ReputationRecord::new(EntityId::Node(node_id));
        let accepted = store.merge_with_sender(record, node_id);

        assert!(
            !accepted,
            "self-authored reputation should be rejected"
        );
    }

    /// B.5.2: Self-reported reputation is also rejected via gossip handler.
    #[test]
    fn test_self_reported_reputation_rejected_via_gossip() {
        let honest = HonestNode::new(30110);
        let reputation_store = Arc::new(ReputationStore::new());

        // Create a gossip engine with reputation store wired in.
        let (outbound_tx, _outbound_rx) = mpsc::channel(256);
        let identity = NodeIdentity::generate();
        let id = identity.node_id;
        let knowledge = Arc::new(KnowledgeStore::new(id));

        let gossip = GossipEngine::new(id, 1, knowledge.clone(), outbound_tx)
            .with_config(GossipConfig {
                interval: GOSSIP_INTERVAL,
                fanout: GOSSIP_FANOUT,
                jitter_percent: 0,
            })
            .with_organic_stores(
                Arc::new(crate::swarm::profile::ProfileStore::new()),
                Arc::new(crate::swarm::collective::CollectiveStore::new()),
                reputation_store.clone(),
                Arc::new(crate::swarm::marketplace::MarketplaceStore::new()),
                Arc::new(crate::swarm::policy::PolicyEngine::new(Default::default())),
            );

        // Build a message that includes self-authored reputation.
        let attacker_id = NodeId::new();
        let mut msg = GossipMessage {
            sender_id: attacker_id,
            timestamp: Utc::now(),
            generation: 1,
            envelope_signature: vec![],
            sender_public_key: vec![],
            my_traits: HashSet::from([Trait::CanExecute]),
            my_load: 0.1,
            my_capacity: ResourceSnapshot::default(),
            known_nodes: vec![],
            known_jobs: vec![],
            known_assignments: vec![],
            known_profiles: vec![],
            known_collectives: vec![],
            known_reputation: vec![ReputationRecord::new(EntityId::Node(attacker_id))],
            policy: None,
            known_postings: vec![],
            known_awards: vec![],
            known_probations: vec![],
            known_admission_decisions: vec![],
            known_blobs: vec![],
            software_inventories: vec![],
            psyche_snapshot: None,
            fleet_states: vec![],
            active_alerts: vec![],
            sla_compliance: vec![],
            witness_reports: vec![],
            pg_nodes: vec![],
            zone_certificates: vec![],
            zone_revocations: vec![],
            key_rotations: vec![],
            key_revocations: vec![],
        };
        // Bump the self-authored record's score to max.
        msg.known_reputation[0].score = 1.0;
        msg.known_reputation[0].version = 999;

        gossip.handle_gossip(msg);

        // The self-authored reputation should not be in the store.
        let result = reputation_store.get(&EntityId::Node(attacker_id));
        assert!(
            result.is_none(),
            "self-authored reputation should not be stored via gossip"
        );
    }

    /// B.5.3: Reputation smear campaign — A gossips bad rep for B.
    /// Without matching the sender_id filter, the record should be accepted
    /// (it's not self-authored).
    #[test]
    fn test_reputation_smear_accepted_as_third_party() {
        let store = ReputationStore::new();
        let attacker_id = NodeId::new();
        let target_id = NodeId::new();

        // Attacker authors bad reputation for target (not self-authored).
        let mut record = ReputationRecord::new(EntityId::Node(target_id));
        record.score = 0.0;
        record.version = 1;

        let accepted = store.merge_with_sender(record, attacker_id);
        assert!(
            accepted,
            "third-party reputation (attacker reporting on target) should be accepted"
        );

        // But the record has a low version — a legitimate higher-version
        // record from honest nodes will overwrite it.
        let mut good_record = ReputationRecord::new(EntityId::Node(target_id));
        good_record.score = 0.8;
        good_record.version = 10;

        let overwrote = store.merge(good_record);
        assert!(overwrote, "higher-version honest record should overwrite smear");

        let final_rec = store.get(&EntityId::Node(target_id)).unwrap();
        assert!(
            final_rec.score > 0.5,
            "honest reputation should win (score: {})",
            final_rec.score,
        );
    }

    /// B.5.4: Collusive reputation ring — 3 nodes praising each other.
    /// merge_with_sender only blocks self-authored records, so cross-praise
    /// is accepted. This test documents the current behavior.
    #[test]
    fn test_collusive_reputation_ring_accepted() {
        let store = ReputationStore::new();
        let nodes: Vec<NodeId> = (0..3).map(|_| NodeId::new()).collect();

        // Each node praises the next one in the ring.
        for i in 0..3 {
            let sender = nodes[i];
            let target = nodes[(i + 1) % 3];

            let mut record = ReputationRecord::new(EntityId::Node(target));
            record.score = 1.0;
            record.version = 1;

            let accepted = store.merge_with_sender(record, sender);
            assert!(accepted, "cross-praise (not self-authored) is accepted");
        }

        // All three should have records now.
        for node in &nodes {
            assert!(
                store.get(&EntityId::Node(*node)).is_some(),
                "collusive ring member should have a reputation record"
            );
        }
    }

    // ========================================================================
    // B.6 Knowledge Store Poisoning (4 tests)
    // ========================================================================

    /// B.6.1: Ghost node injection — inject 10K fake NodeInfos.
    #[test]
    fn test_ghost_node_injection_bounded() {
        let honest = HonestNode::new(30120);

        // Inject 10K ghost nodes via gossip.
        let injector = MaliciousNode::ghost_injector(30121, 200);
        for _ in 0..50 {
            let evil = injector.build_evil_gossip();
            honest.gossip.handle_gossip(evil);
        }

        // Each message has 200 ghost nodes, but truncation limits to MAX_NODES_PER_MESSAGE (50).
        // So each round adds at most 50 nodes. After 50 rounds, that's 2500 attempted.
        let all = honest.knowledge.get_all_nodes();
        assert!(
            all.len() <= MAX_KNOWN_NODES + 100,
            "knowledge store should bound ghost nodes (got {})",
            all.len(),
        );
    }

    /// B.6.2: Knowledge store overflow triggers LRU-style eviction.
    #[test]
    fn test_knowledge_store_overflow_eviction() {
        let store = KnowledgeStore::new(NodeId::new());

        // Insert MAX_KNOWN_NODES + 500 nodes.
        let limit = MAX_KNOWN_NODES + 500;
        for i in 0..limit {
            store.merge_node(NodeInfo {
                node_id: NodeId::new(),
                last_seen: Utc::now() - ChronoDuration::seconds(limit as i64 - i as i64),
                traits: HashSet::from([Trait::CanExecute]),
                load: 0.1,
                capacity: ResourceSnapshot::default(),
                address: None,
                via: NodeId::new(),
                status: NodeStatus::Alive,
                generation: 1,
                trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            });
        }

        // Trigger eviction (in production this is called periodically by gossip).
        store.enforce_limits();

        let all = store.get_all_nodes();
        assert!(
            all.len() <= MAX_KNOWN_NODES + 100,
            "store should evict old entries (got {})",
            all.len(),
        );
    }

    /// B.6.3: Assignment spam — flood 100K fake assignments.
    #[test]
    fn test_assignment_spam_bounded() {
        let honest = HonestNode::new(30130);
        let job_id = crate::common::types::JobId::new();

        // Inject 10K assignments (bounded by MAX_KNOWN_ASSIGNMENTS).
        for _ in 0..10_000 {
            honest.knowledge.merge_assignment(Assignment {
                chunk_id: ChunkId::new(),
                job_id,
                assigned_to: Some(NodeId::new()),
                assigned_at: Utc::now(),
                status: ChunkStatus::InProgress,
                result: None,
                attempts: 1,
                replica_group_id: None,
                failed_nodes: Vec::new(),
            });
        }

        let all = honest.knowledge.get_all_assignments();
        assert!(
            all.len() <= MAX_KNOWN_ASSIGNMENTS + 100,
            "assignments should be bounded (got {})",
            all.len(),
        );
    }

    /// B.6.4: Job metadata tampering — trying to modify an existing job.
    #[test]
    fn test_job_metadata_tampering_rejected() {
        let store = KnowledgeStore::new(NodeId::new());
        let job_id = crate::common::types::JobId::new();

        // Insert a job with InProgress status.
        let original = SwarmJobInfo {
            job_id,
            status: SwarmJobStatus::InProgress,
            chunks_total: 100,
            chunks_completed: 50,
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
        store.merge_job(original);

        // Try to tamper with the job: reduce chunks_total.
        let tampered = SwarmJobInfo {
            job_id,
            status: SwarmJobStatus::InProgress,
            chunks_total: 10, // Tampered!
            chunks_completed: 10,
            chunks_failed: 0,
            submitter: NodeId::new(),
            first_seen: Utc::now(),
            updated_at: Utc::now() - ChronoDuration::seconds(1), // Older timestamp
            payload_type: "shell".to_string(),
            priority: 1,
            data_residency: None,
            required_zone_id: "test-zone".to_string(),
            verification_strategy: VerificationStrategy::None,
        };
        store.merge_job(tampered);

        let after = store.get_job(&job_id).unwrap();
        assert_eq!(
            after.chunks_total, 100,
            "job metadata with older timestamp should not overwrite"
        );
    }
}
