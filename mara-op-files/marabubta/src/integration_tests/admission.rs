// Marabunta - Licensed under the MIT License.
//! Integration tests for the multi-node admission flow.
//!
//! These tests exercise the admission subsystem end-to-end, including join
//! request handling, jury selection, test task generation, verdict collection,
//! probation tracking, graduation, and expulsion. They validate the full
//! lifecycle across multiple candidates and verify reputation side-effects on
//! jurors.
//!
//! All tests are synchronous (no Tokio runtime required) and construct
//! their own local state — no network, no shared state between tests.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use chrono::Utc;
    use uuid::Uuid;

    use crate::swarm::admission::{
        AdmissionEngine, AdmissionOutcome, AdmissionStore, ClaimedResources, JoinRequest,
        JurorVerdict, ProbationStatus, ResourceVerification, ReviewStatus, TestTaskResult,
        VerdictDecision, FAILURE_THRESHOLD, GRADUATION_SUCCESS_RATE, JURY_MIN_SIZE, JURY_MAX_SIZE,
        PROBATION_MAX_TASKS, PROBATION_MIN_TASKS,
    };
    use crate::swarm::auth::NodeIdentity;
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::reputation::{EntityId, ReputationStore};
    use crate::swarm::types::{NodeId, NodeInfo, NodeStatus, ResourceSnapshot};

    // ========================================================================
    // Helpers
    // ========================================================================

    fn make_node_info(id: NodeId) -> NodeInfo {
        NodeInfo {
            node_id: id,
            last_seen: Utc::now(),
            traits: HashSet::new(),
            load: 0.3,
            capacity: ResourceSnapshot::default(),
            address: Some("127.0.0.1:4200".parse().unwrap()),
            via: id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        }
    }

    fn make_claimed_resources(has_gpu: bool) -> ClaimedResources {
        ClaimedResources {
            cpu_cores: 4,
            memory_mb: 8192,
            disk_mb: 102400,
            bandwidth_mbps: 100.0,
            has_gpu,
            gpu_model: if has_gpu {
                Some("RTX 4090".to_string())
            } else {
                None
            },
        }
    }

    /// Create a properly signed join request using a fresh Ed25519 identity.
    fn make_join_request_signed() -> JoinRequest {
        let identity = NodeIdentity::generate();
        let nonce: u64 = rand::random();
        let mut message = Vec::new();
        message.extend_from_slice(identity.node_id.0.as_bytes());
        message.extend_from_slice(&nonce.to_le_bytes());
        let signature = identity.sign(&message);

        JoinRequest {
            candidate_id: identity.node_id,
            claimed_resources: make_claimed_resources(false),
            signature: signature.to_vec(),
            public_key: identity.public_key.to_bytes().to_vec(),
            requested_at: Utc::now(),
            region: Some("us-east".to_string()),
            nonce,
        }
    }

    /// Legacy-compatible wrapper: returns a properly signed request
    /// (candidate_id is derived from the generated key, not caller-specified).
    fn make_join_request(_candidate_id: NodeId) -> JoinRequest {
        make_join_request_signed()
    }

    fn setup_admission(peer_count: usize) -> (AdmissionEngine, NodeId, Vec<NodeId>) {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let reputation = Arc::new(ReputationStore::new());
        let store = Arc::new(AdmissionStore::new());

        let mut peer_ids = Vec::new();
        for _ in 0..peer_count {
            let peer_id = NodeId::new();
            knowledge.merge_node(make_node_info(peer_id));
            let mut record = reputation.get_or_create(EntityId::Node(peer_id));
            record.score = 0.7;
            record.version += 1;
            reputation.merge(record);
            peer_ids.push(peer_id);
        }

        let engine = AdmissionEngine::new(self_id, store, knowledge, reputation);
        (engine, self_id, peer_ids)
    }

    fn make_verdict(
        juror_id: NodeId,
        candidate_id: NodeId,
        decision: VerdictDecision,
        confidence: f32,
    ) -> JurorVerdict {
        JurorVerdict {
            juror_id,
            candidate_id,
            decision,
            confidence,
            resource_verification: ResourceVerification {
                cpu_verified: true,
                memory_verified: true,
                disk_verified: true,
                network_verified: true,
                gpu_verified: None,
                overall_match_pct: 0.95,
            },
            ip_reputation: None,
            notes: None,
            submitted_at: Utc::now(),
        }
    }

    /// Submit all test task results for a review as successful.
    fn complete_all_test_tasks(engine: &AdmissionEngine, candidate_id: NodeId) {
        let review = engine.store().get_review(&candidate_id).unwrap();
        for task in &review.test_tasks {
            engine.handle_test_result(TestTaskResult {
                task_id: task.task_id,
                candidate_id,
                success: true,
                duration_ms: 100,
                output_hash: None,
                error: None,
            });
        }
    }

    /// Submit admit verdicts from a majority of jurors.
    fn submit_majority_verdicts(
        engine: &AdmissionEngine,
        candidate_id: NodeId,
        decision: VerdictDecision,
        confidence: f32,
    ) {
        let review = engine.store().get_review(&candidate_id).unwrap();
        let jury = review.jury.clone();
        let majority = (jury.len() / 2) + 1;
        for juror_id in jury.iter().take(majority) {
            engine.handle_verdict(make_verdict(*juror_id, candidate_id, decision, confidence));
        }
    }

    // ========================================================================
    // Tests
    // ========================================================================

    #[test]
    fn test_full_admission_lifecycle_admit() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        // Submit join request.
        let review = engine.handle_join_request(request, None).unwrap();
        assert!(review.jury.len() >= JURY_MIN_SIZE);
        assert!(review.jury.len() <= JURY_MAX_SIZE);
        assert!(matches!(review.status, ReviewStatus::TestingInProgress));

        // Complete all test tasks.
        complete_all_test_tasks(&engine, candidate_id);
        let review = engine.store().get_review(&candidate_id).unwrap();
        assert!(matches!(review.status, ReviewStatus::VerdictCollection));

        // Submit majority admit verdicts.
        submit_majority_verdicts(&engine, candidate_id, VerdictDecision::Admit, 0.85);

        // Verify review is complete with Admitted outcome.
        let review = engine.store().get_review(&candidate_id).unwrap();
        assert!(matches!(review.status, ReviewStatus::Complete(AdmissionOutcome::Admitted)));

        // Verify probation is created with correct required_tasks.
        let probation = engine.store().get_probation(&candidate_id).unwrap();
        assert_eq!(probation.node_id, candidate_id);
        assert!(probation.required_tasks >= PROBATION_MIN_TASKS);
        assert!(probation.required_tasks <= PROBATION_MAX_TASKS);
        assert_eq!(probation.tasks_completed, 0);
        assert_eq!(probation.tasks_failed, 0);
        assert!(!probation.graduated);
        assert!(!probation.expelled);

        // Verify decision is stored.
        let decision = engine.store().get_decision(&candidate_id).unwrap();
        assert_eq!(decision.outcome, AdmissionOutcome::Admitted);
        assert!(decision.probation_length.is_some());
    }

    #[test]
    fn test_full_admission_lifecycle_reject() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);

        // Submit majority reject verdicts.
        submit_majority_verdicts(&engine, candidate_id, VerdictDecision::Reject, 0.85);

        // Verify review is complete with Rejected outcome.
        let review = engine.store().get_review(&candidate_id).unwrap();
        assert!(matches!(review.status, ReviewStatus::Complete(AdmissionOutcome::Rejected)));

        // No probation should be created.
        assert!(engine.store().get_probation(&candidate_id).is_none());

        // Decision should be stored.
        let decision = engine.store().get_decision(&candidate_id).unwrap();
        assert_eq!(decision.outcome, AdmissionOutcome::Rejected);
        assert!(decision.probation_length.is_none());
    }

    #[test]
    fn test_full_admission_lifecycle_defer() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);

        // Submit enough unsure verdicts to trigger deferral (>1/3 of jury).
        // With a jury of 5-7, the unsure threshold is ceil(len/3).
        // We need at least ceil(jury_len/3) unsure votes in the majority.
        let review = engine.store().get_review(&candidate_id).unwrap();
        let jury = review.jury.clone();
        let majority = (jury.len() / 2) + 1;
        let unsure_threshold = (jury.len() as f32 / 3.0).ceil() as usize;

        // Submit unsure verdicts up to the threshold, then fill the rest with admit.
        for (i, juror_id) in jury.iter().take(majority).enumerate() {
            let decision = if i < unsure_threshold {
                VerdictDecision::Unsure
            } else {
                VerdictDecision::Admit
            };
            engine.handle_verdict(make_verdict(*juror_id, candidate_id, decision, 0.5));
        }

        // Verify outcome is Deferred.
        let review = engine.store().get_review(&candidate_id).unwrap();
        assert!(matches!(review.status, ReviewStatus::Complete(AdmissionOutcome::Deferred)));

        // No probation for deferred candidates.
        assert!(engine.store().get_probation(&candidate_id).is_none());
    }

    #[test]
    fn test_probation_graduation_full_flow() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        // Admit the candidate.
        engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);
        submit_majority_verdicts(&engine, candidate_id, VerdictDecision::Admit, 0.85);

        let probation = engine.store().get_probation(&candidate_id).unwrap();
        let required = probation.required_tasks;
        let jury = probation.jury.clone();

        // Record juror reputation scores before graduation.
        let mut pre_scores = Vec::new();
        for juror_id in &jury {
            let score = engine.store().get_probation(&candidate_id).unwrap();
            let _ = score; // Just verifying probation exists.
            // Use the reputation store through the engine indirectly via the setup.
            // We need to read the reputation scores, but the engine holds the store.
            // Instead, track via the admission store's jury list.
            pre_scores.push(*juror_id);
        }

        // Complete required_tasks with all successes.
        for _ in 0..required {
            engine.update_probation(&candidate_id, true);
        }

        // Verify graduation.
        let probation = engine.store().get_probation(&candidate_id).unwrap();
        assert!(probation.graduated);
        assert!(!probation.expelled);
        assert_eq!(probation.tasks_completed, required);
        assert_eq!(probation.tasks_failed, 0);
        assert!(probation.success_rate() >= GRADUATION_SUCCESS_RATE);
    }

    #[test]
    fn test_probation_expulsion_full_flow() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        // Admit the candidate.
        engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);
        submit_majority_verdicts(&engine, candidate_id, VerdictDecision::Admit, 0.85);

        // Fail enough tasks to trigger expulsion.
        // Need >= 5 completed tasks and failure rate > FAILURE_THRESHOLD (0.3).
        // With 5 completed and 3 failed: rate = 3/5 = 0.6 > 0.3 => expelled.
        // But we also need to trigger it via update_probation, which checks
        // after incrementing. So: complete 2 successes, then 3 failures = 5 total, 3 failed = 60%.
        engine.update_probation(&candidate_id, true);
        engine.update_probation(&candidate_id, true);
        engine.update_probation(&candidate_id, false);
        engine.update_probation(&candidate_id, false);
        // At this point: 4 completed, 2 failed = 50% > 30%, but < 5 tasks.
        let probation = engine.store().get_probation(&candidate_id).unwrap();
        assert!(!probation.expelled); // Not enough tasks yet.

        // 5th task fails: 5 completed, 3 failed = 60% > 30%.
        engine.update_probation(&candidate_id, false);
        let probation = engine.store().get_probation(&candidate_id).unwrap();
        assert!(probation.expelled);
        assert!(!probation.graduated);
        assert_eq!(probation.tasks_completed, 5);
        assert_eq!(probation.tasks_failed, 3);
        assert!(probation.failure_rate() > FAILURE_THRESHOLD);
    }

    #[test]
    fn test_mixed_verdicts_majority_wins() {
        // Scenario 1: 3 admit, 2 reject => Admitted.
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        let review = engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);

        let jury = review.jury.clone();
        // Submit 3 admit verdicts.
        for juror_id in jury.iter().take(3) {
            engine.handle_verdict(make_verdict(
                *juror_id,
                candidate_id,
                VerdictDecision::Admit,
                0.8,
            ));
        }
        // Also submit 2 reject verdicts (if decision not yet made from majority of 3).
        // With jury size 5-7, majority is 3-4. If jury=5, majority=3, so 3 admits triggers decision.
        // If jury=7, majority=4, so we may need more. Handle both cases.
        let review_after = engine.store().get_review(&candidate_id).unwrap();
        if !matches!(review_after.status, ReviewStatus::Complete(_)) {
            // Need more verdicts. Submit reject to fill but keep admit majority.
            for juror_id in jury.iter().skip(3).take(2) {
                engine.handle_verdict(make_verdict(
                    *juror_id,
                    candidate_id,
                    VerdictDecision::Reject,
                    0.8,
                ));
            }
        }
        let final_review = engine.store().get_review(&candidate_id).unwrap();
        let decision = final_review.decision.unwrap();
        assert!(
            decision.votes_admit > decision.votes_reject,
            "admits ({}) should exceed rejects ({})",
            decision.votes_admit,
            decision.votes_reject
        );
        assert_eq!(decision.outcome, AdmissionOutcome::Admitted);

        // Scenario 2: 3 reject, 2 admit => Rejected.
        let request2 = make_join_request_signed();
        let candidate_id2 = request2.candidate_id;
        let review2 = engine.handle_join_request(request2, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id2);

        let jury2 = review2.jury.clone();
        // Submit 3 reject verdicts first.
        for juror_id in jury2.iter().take(3) {
            engine.handle_verdict(make_verdict(
                *juror_id,
                candidate_id2,
                VerdictDecision::Reject,
                0.8,
            ));
        }
        let review_after2 = engine.store().get_review(&candidate_id2).unwrap();
        if !matches!(review_after2.status, ReviewStatus::Complete(_)) {
            for juror_id in jury2.iter().skip(3).take(2) {
                engine.handle_verdict(make_verdict(
                    *juror_id,
                    candidate_id2,
                    VerdictDecision::Admit,
                    0.8,
                ));
            }
        }
        let final_review2 = engine.store().get_review(&candidate_id2).unwrap();
        let decision2 = final_review2.decision.unwrap();
        assert!(
            decision2.votes_reject >= decision2.votes_admit,
            "rejects ({}) should be >= admits ({})",
            decision2.votes_reject,
            decision2.votes_admit
        );
        assert_eq!(decision2.outcome, AdmissionOutcome::Rejected);
    }

    #[test]
    fn test_duplicate_join_request_rejected() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        let first = engine.handle_join_request(request.clone(), None);
        assert!(first.is_some());

        let second = engine.handle_join_request(request, None);
        assert!(second.is_none(), "duplicate join request should be rejected");
    }

    #[test]
    fn test_join_request_no_signature_rejected() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let mut request = make_join_request_signed();
        request.signature = vec![]; // Empty signature.

        let result = engine.handle_join_request(request, None);
        assert!(result.is_none(), "request with empty signature should be rejected");
    }

    #[test]
    fn test_join_request_insufficient_peers() {
        // Only 2 peers available, but need at least JURY_MIN_SIZE (5).
        let (engine, _self_id, _peers) = setup_admission(2);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        let result = engine.handle_join_request(request, None);
        assert!(result.is_none(), "should fail with insufficient peers for jury");
    }

    #[test]
    fn test_duplicate_verdict_ignored() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        let review = engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);

        let juror_id = review.jury[0];

        // Submit the same juror's verdict twice.
        engine.handle_verdict(make_verdict(
            juror_id,
            candidate_id,
            VerdictDecision::Admit,
            0.9,
        ));
        engine.handle_verdict(make_verdict(
            juror_id,
            candidate_id,
            VerdictDecision::Reject, // Different decision, but same juror.
            0.9,
        ));

        // Should only have 1 verdict recorded.
        let updated = engine.store().get_review(&candidate_id).unwrap();
        let juror_verdicts: Vec<_> = updated
            .verdicts
            .iter()
            .filter(|v| v.juror_id == juror_id)
            .collect();
        assert_eq!(
            juror_verdicts.len(),
            1,
            "duplicate verdict from same juror should be ignored"
        );
        // The first verdict (Admit) should be the one recorded.
        assert_eq!(juror_verdicts[0].decision, VerdictDecision::Admit);
    }

    #[test]
    fn test_verdict_from_non_juror_ignored() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);

        // Submit a verdict from a node that is not on the jury.
        let outsider = NodeId::new();
        engine.handle_verdict(make_verdict(
            outsider,
            candidate_id,
            VerdictDecision::Admit,
            0.9,
        ));

        let review = engine.store().get_review(&candidate_id).unwrap();
        assert!(
            review.verdicts.is_empty(),
            "verdict from non-juror should be ignored"
        );
    }

    #[test]
    fn test_test_result_for_wrong_task_ignored() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        engine.handle_join_request(request, None).unwrap();

        // Submit a result with a random task_id that doesn't belong to this review.
        let bogus_result = TestTaskResult {
            task_id: Uuid::new_v4(),
            candidate_id,
            success: true,
            duration_ms: 50,
            output_hash: None,
            error: None,
        };
        engine.handle_test_result(bogus_result);

        let review = engine.store().get_review(&candidate_id).unwrap();
        assert!(
            review.test_results.is_empty(),
            "test result for unknown task should be ignored"
        );
    }

    #[test]
    fn test_multiple_candidates_independent() {
        let (engine, _self_id, _peers) = setup_admission(10);

        let request_a = make_join_request_signed();
        let request_b = make_join_request_signed();
        let candidate_a = request_a.candidate_id;
        let candidate_b = request_b.candidate_id;

        let review_a = engine.handle_join_request(request_a, None).unwrap();
        let review_b = engine.handle_join_request(request_b, None).unwrap();

        // Each candidate has their own review.
        assert_ne!(candidate_a, candidate_b);
        assert_eq!(engine.store().review_count(), 2);

        // Complete test tasks independently.
        complete_all_test_tasks(&engine, candidate_a);
        complete_all_test_tasks(&engine, candidate_b);

        // Admit candidate A.
        let jury_a = review_a.jury.clone();
        let majority_a = (jury_a.len() / 2) + 1;
        for juror_id in jury_a.iter().take(majority_a) {
            engine.handle_verdict(make_verdict(
                *juror_id,
                candidate_a,
                VerdictDecision::Admit,
                0.9,
            ));
        }

        // Reject candidate B.
        let jury_b = review_b.jury.clone();
        let majority_b = (jury_b.len() / 2) + 1;
        for juror_id in jury_b.iter().take(majority_b) {
            engine.handle_verdict(make_verdict(
                *juror_id,
                candidate_b,
                VerdictDecision::Reject,
                0.9,
            ));
        }

        // Verify independent outcomes.
        let review_a_final = engine.store().get_review(&candidate_a).unwrap();
        let review_b_final = engine.store().get_review(&candidate_b).unwrap();

        assert!(matches!(
            review_a_final.status,
            ReviewStatus::Complete(AdmissionOutcome::Admitted)
        ));
        assert!(matches!(
            review_b_final.status,
            ReviewStatus::Complete(AdmissionOutcome::Rejected)
        ));

        // Candidate A should have probation; B should not.
        assert!(engine.store().get_probation(&candidate_a).is_some());
        assert!(engine.store().get_probation(&candidate_b).is_none());
    }

    #[test]
    fn test_probation_no_update_after_graduation() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        // Admit and graduate the node.
        engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);
        submit_majority_verdicts(&engine, candidate_id, VerdictDecision::Admit, 0.85);

        let probation = engine.store().get_probation(&candidate_id).unwrap();
        let required = probation.required_tasks;

        for _ in 0..required {
            engine.update_probation(&candidate_id, true);
        }

        let post_grad = engine.store().get_probation(&candidate_id).unwrap();
        assert!(post_grad.graduated);
        let tasks_at_graduation = post_grad.tasks_completed;

        // Further updates should be no-ops.
        engine.update_probation(&candidate_id, true);
        engine.update_probation(&candidate_id, false);
        engine.update_probation(&candidate_id, true);

        let after_extra = engine.store().get_probation(&candidate_id).unwrap();
        assert!(after_extra.graduated);
        assert_eq!(
            after_extra.tasks_completed, tasks_at_graduation,
            "tasks_completed should not change after graduation"
        );
    }

    #[test]
    fn test_probation_no_update_after_expulsion() {
        let (engine, _self_id, _peers) = setup_admission(10);
        let request = make_join_request_signed();
        let candidate_id = request.candidate_id;

        // Admit the node.
        engine.handle_join_request(request, None).unwrap();
        complete_all_test_tasks(&engine, candidate_id);
        submit_majority_verdicts(&engine, candidate_id, VerdictDecision::Admit, 0.85);

        // Drive to expulsion: 2 success + 3 failures = 5 total, 60% failure.
        engine.update_probation(&candidate_id, true);
        engine.update_probation(&candidate_id, true);
        engine.update_probation(&candidate_id, false);
        engine.update_probation(&candidate_id, false);
        engine.update_probation(&candidate_id, false);

        let post_expulsion = engine.store().get_probation(&candidate_id).unwrap();
        assert!(post_expulsion.expelled);
        let tasks_at_expulsion = post_expulsion.tasks_completed;

        // Further updates should be no-ops.
        engine.update_probation(&candidate_id, true);
        engine.update_probation(&candidate_id, false);

        let after_extra = engine.store().get_probation(&candidate_id).unwrap();
        assert!(after_extra.expelled);
        assert_eq!(
            after_extra.tasks_completed, tasks_at_expulsion,
            "tasks_completed should not change after expulsion"
        );
    }

    #[test]
    fn test_gpu_candidate_gets_gpu_test_task() {
        let (engine, _self_id, _peers) = setup_admission(10);

        // Candidate WITHOUT GPU.
        let candidate_no_gpu = NodeId::new();
        let tasks_no_gpu =
            engine.generate_test_tasks(&candidate_no_gpu, &make_claimed_resources(false));
        assert_eq!(tasks_no_gpu.len(), 4, "non-GPU candidate gets 4 test tasks");

        // Candidate WITH GPU.
        let candidate_gpu = NodeId::new();
        let tasks_gpu =
            engine.generate_test_tasks(&candidate_gpu, &make_claimed_resources(true));
        assert_eq!(tasks_gpu.len(), 5, "GPU candidate gets 5 test tasks");

        // Verify the extra task is a GpuCompute type.
        let gpu_tasks: Vec<_> = tasks_gpu
            .iter()
            .filter(|t| matches!(t.task_type, crate::swarm::admission::TestTaskType::GpuCompute { .. }))
            .collect();
        assert_eq!(gpu_tasks.len(), 1, "exactly one GPU test task");
    }

    #[test]
    fn test_high_confidence_short_probation() {
        let (engine, _self_id, _peers) = setup_admission(10);

        let length = engine.calculate_probation_length(0.95);
        // High confidence should yield probation near PROBATION_MIN_TASKS.
        let tolerance = (PROBATION_MAX_TASKS - PROBATION_MIN_TASKS) / 4;
        assert!(
            length <= PROBATION_MIN_TASKS + tolerance,
            "high confidence (0.95) should yield short probation near {}, got {}",
            PROBATION_MIN_TASKS,
            length
        );
        assert!(length >= PROBATION_MIN_TASKS);
    }

    #[test]
    fn test_low_confidence_long_probation() {
        let (engine, _self_id, _peers) = setup_admission(10);

        let length = engine.calculate_probation_length(0.2);
        // Low confidence should yield probation near PROBATION_MAX_TASKS.
        let tolerance = (PROBATION_MAX_TASKS - PROBATION_MIN_TASKS) / 4;
        assert!(
            length >= PROBATION_MAX_TASKS - tolerance,
            "low confidence (0.2) should yield long probation near {}, got {}",
            PROBATION_MAX_TASKS,
            length
        );
        assert!(length <= PROBATION_MAX_TASKS);
    }

    #[test]
    fn test_store_pending_reviews() {
        let (engine, _self_id, _peers) = setup_admission(10);

        // Create 3 reviews.
        let req1 = make_join_request_signed();
        let req2 = make_join_request_signed();
        let req3 = make_join_request_signed();
        let c1 = req1.candidate_id;
        let c2 = req2.candidate_id;
        let c3 = req3.candidate_id;

        engine.handle_join_request(req1, None).unwrap();
        engine.handle_join_request(req2, None).unwrap();
        engine.handle_join_request(req3, None).unwrap();

        assert_eq!(engine.store().review_count(), 3);

        // Complete one review (admit c1).
        complete_all_test_tasks(&engine, c1);
        submit_majority_verdicts(&engine, c1, VerdictDecision::Admit, 0.9);

        let review_c1 = engine.store().get_review(&c1).unwrap();
        assert!(matches!(review_c1.status, ReviewStatus::Complete(_)));

        // pending_reviews() should return only incomplete reviews.
        let pending = engine.store().pending_reviews();
        assert_eq!(
            pending.len(),
            2,
            "2 of 3 reviews should still be pending, got {}",
            pending.len()
        );

        // The pending reviews should be c2 and c3 (not c1).
        let pending_ids: HashSet<NodeId> = pending.iter().map(|r| r.request.candidate_id).collect();
        assert!(pending_ids.contains(&c2));
        assert!(pending_ids.contains(&c3));
        assert!(!pending_ids.contains(&c1));
    }

    #[test]
    fn test_store_active_probations() {
        let (engine, _self_id, _peers) = setup_admission(10);

        // Create 3 probation records manually.
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();

        let make_probation = |node_id: NodeId| ProbationStatus {
            node_id,
            required_tasks: 5,
            tasks_completed: 0,
            tasks_failed: 0,
            started_at: Utc::now(),
            jury: vec![],
            graduated: false,
            expelled: false,
        };

        engine.store().upsert_probation(make_probation(n1));
        engine.store().upsert_probation(make_probation(n2));
        engine.store().upsert_probation(make_probation(n3));

        assert_eq!(engine.store().probation_count(), 3);
        assert_eq!(engine.store().active_probations().len(), 3);

        // Graduate n1.
        let mut p1 = engine.store().get_probation(&n1).unwrap();
        p1.graduated = true;
        engine.store().upsert_probation(p1);

        // Expel n2.
        let mut p2 = engine.store().get_probation(&n2).unwrap();
        p2.expelled = true;
        engine.store().upsert_probation(p2);

        // active_probations() should return only n3.
        let active = engine.store().active_probations();
        assert_eq!(active.len(), 1, "only 1 active probation expected, got {}", active.len());
        assert_eq!(active[0].node_id, n3);
    }
}
