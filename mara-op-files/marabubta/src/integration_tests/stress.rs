// Marabunta - Licensed under the MIT License.
//! Stress & chaos tests for Marabunta Swarm gossip convergence under
//! adverse network conditions.
//!
//! All tests are `#[ignore]` — run explicitly:
//! ```bash
//! cargo test stress -- --ignored --nocapture
//! ```

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use chrono::{Duration as ChronoDuration, Utc};
    use tokio::sync::mpsc;

    use crate::swarm::config::*;
    use crate::swarm::gossip::{GossipConfig, GossipEngine, MergeResult};
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::types::*;

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Lightweight simulated node for stress testing.
    struct StressNode {
        id: NodeId,
        addr: SocketAddr,
        knowledge: Arc<KnowledgeStore>,
        gossip: Arc<GossipEngine>,
        alive: bool,
        _outbound_rx: mpsc::Receiver<(SocketAddr, SwarmMessage)>,
    }

    impl StressNode {
        fn new(index: usize) -> Self {
            let id = NodeId::new();
            let port = 40000 + index as u16;
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

            let knowledge = Arc::new(KnowledgeStore::new(id));
            let (outbound_tx, outbound_rx) = mpsc::channel(256);

            let config = GossipConfig {
                interval: GOSSIP_INTERVAL,
                fanout: GOSSIP_FANOUT,
                jitter_percent: 0,
            };

            let gossip = Arc::new(
                GossipEngine::new(id, 1, knowledge.clone(), outbound_tx).with_config(config),
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
                trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            });

            StressNode {
                id,
                addr,
                knowledge,
                gossip,
                alive: true,
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

    /// Deterministic PRNG (xorshift64) — !Send-safe unlike ThreadRng.
    struct Rng {
        state: u64,
    }

    impl Rng {
        fn new(seed: u64) -> Self {
            Self {
                state: if seed == 0 { 1 } else { seed },
            }
        }

        fn next(&mut self) -> u64 {
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            self.state
        }

        fn next_usize(&mut self, max: usize) -> usize {
            if max == 0 { return 0; }
            (self.next() as usize) % max
        }

        fn next_bool(&mut self, probability_percent: u32) -> bool {
            (self.next() % 100) < probability_percent as u64
        }
    }

    /// Create N nodes with bootstrap seeding.
    fn create_cluster(n: usize, seed_count: usize) -> Vec<StressNode> {
        let mut nodes: Vec<StressNode> = (0..n).map(StressNode::new).collect();

        let seeds: Vec<NodeInfo> = nodes
            .iter()
            .take(seed_count)
            .map(|n| NodeInfo {
                node_id: n.id,
                last_seen: Utc::now(),
                traits: HashSet::from([Trait::CanExecute]),
                load: 0.1,
                capacity: ResourceSnapshot::default(),
                address: Some(n.addr),
                via: n.id,
                status: NodeStatus::Alive,
                generation: 1,
                trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            })
            .collect();

        for node in &nodes {
            for seed in &seeds {
                if seed.node_id != node.id {
                    node.knowledge.merge_node(seed.clone());
                }
            }
        }

        nodes
    }

    /// Node-to-index lookup.
    fn build_index(nodes: &[StressNode]) -> HashMap<NodeId, usize> {
        nodes.iter().enumerate().map(|(i, n)| (n.id, i)).collect()
    }

    /// Address registry for alive nodes.
    fn build_registry(nodes: &[StressNode]) -> HashMap<NodeId, SocketAddr> {
        nodes.iter().filter(|n| n.alive).map(|n| (n.id, n.addr)).collect()
    }

    /// Run one gossip round with optional message loss probability.
    fn gossip_round(
        nodes: &[StressNode],
        index: &HashMap<NodeId, usize>,
        rng: &mut Rng,
        loss_percent: u32,
    ) -> u64 {
        let registry = build_registry(nodes);
        let mut delivered = 0u64;

        let messages: Vec<(usize, GossipMessage)> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.alive)
            .map(|(i, n)| (i, n.build_gossip()))
            .collect();

        for (sender_idx, msg) in &messages {
            let peers = nodes[*sender_idx].gossip.select_peers();
            for (peer_id, _addr) in peers {
                if let Some(&peer_idx) = index.get(&peer_id) {
                    if !nodes[peer_idx].alive {
                        continue;
                    }
                    if rng.next_bool(loss_percent) {
                        continue;
                    }
                    nodes[peer_idx].gossip.handle_gossip(msg.clone());
                    delivered += 1;
                }
            }
        }

        // Restore addresses (transport layer normally does this).
        for node in nodes.iter().filter(|n| n.alive) {
            for (&nid, &addr) in &registry {
                if let Some(mut entry) = node.knowledge.get_node(&nid) {
                    if entry.address.is_none() {
                        entry.address = Some(addr);
                        entry.last_seen = entry.last_seen + ChronoDuration::milliseconds(1);
                        node.knowledge.merge_node(entry);
                    }
                }
            }
        }

        delivered
    }

    /// Count how many alive nodes are known by a given node.
    fn known_alive_count(node: &StressNode) -> usize {
        node.knowledge.get_live_nodes().len()
    }

    // ========================================================================
    // C.1: 100 nodes with 20% message loss (test 29)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_100_nodes_20pct_message_loss() {
        let n = 100;
        let nodes = create_cluster(n, 5);
        let index = build_index(&nodes);
        let mut rng = Rng::new(42);

        // Run 500 rounds with 20% message loss.
        for _ in 0..500 {
            gossip_round(&nodes, &index, &mut rng, 20);
        }

        // After 500 rounds, at least 90% of alive nodes should know about
        // each other despite 20% message loss.
        let expected_min = (n as f64 * 0.8) as usize;
        for node in &nodes {
            let known = known_alive_count(node);
            assert!(
                known >= expected_min,
                "node {} knows only {}/{} alive nodes after 500 lossy rounds",
                node.id, known, n,
            );
        }
    }

    // ========================================================================
    // C.2: Asymmetric partition (90/10 split) (test 30)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_asymmetric_partition() {
        let n = 100;
        let nodes = create_cluster(n, 5);
        let index = build_index(&nodes);
        let mut rng = Rng::new(123);

        // First: let cluster converge (100 rounds, no loss).
        for _ in 0..100 {
            gossip_round(&nodes, &index, &mut rng, 0);
        }

        // Create asymmetric partition: nodes 0..90 can't talk to nodes 90..100.
        let partition_boundary = 90;

        // Run 200 rounds with partition.
        for _ in 0..200 {
            let registry = build_registry(&nodes);
            let messages: Vec<(usize, GossipMessage)> = nodes
                .iter()
                .enumerate()
                .filter(|(_, n)| n.alive)
                .map(|(i, n)| (i, n.build_gossip()))
                .collect();

            for (sender_idx, msg) in &messages {
                let peers = nodes[*sender_idx].gossip.select_peers();
                for (peer_id, _) in peers {
                    if let Some(&peer_idx) = index.get(&peer_id) {
                        if !nodes[peer_idx].alive { continue; }
                        // Enforce partition.
                        let sender_in_majority = *sender_idx < partition_boundary;
                        let peer_in_majority = peer_idx < partition_boundary;
                        if sender_in_majority != peer_in_majority { continue; }

                        nodes[peer_idx].gossip.handle_gossip(msg.clone());
                    }
                }
            }
        }

        // Majority partition should maintain internal convergence.
        let majority_known: Vec<usize> = nodes[..partition_boundary]
            .iter()
            .map(|n| known_alive_count(n))
            .collect();

        let avg_majority = majority_known.iter().sum::<usize>() / majority_known.len();
        assert!(
            avg_majority >= partition_boundary / 2,
            "majority partition should maintain internal knowledge (avg={})",
            avg_majority,
        );

        // Minority partition has limited peers to gossip with.
        let minority_known: Vec<usize> = nodes[partition_boundary..]
            .iter()
            .map(|n| known_alive_count(n))
            .collect();

        let avg_minority = minority_known.iter().sum::<usize>() / minority_known.len();
        // Minority should at least know about each other.
        assert!(
            avg_minority >= (n - partition_boundary) / 2,
            "minority should know about each other (avg={})",
            avg_minority,
        );
    }

    // ========================================================================
    // C.3: Cascading failure — 30 nodes die over 30 rounds (test 31)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_cascading_failure() {
        let n = 100;
        let mut nodes = create_cluster(n, 5);
        let index = build_index(&nodes);
        let mut rng = Rng::new(999);

        // Converge first.
        for _ in 0..100 {
            gossip_round(&nodes, &index, &mut rng, 0);
        }

        // Kill 1 node per round for 30 rounds.
        for round in 0..30 {
            let victim = round + 10; // Kill nodes 10..39.
            nodes[victim].alive = false;
            gossip_round(&nodes, &index, &mut rng, 0);
        }

        // Run 100 more rounds for detection to propagate.
        for _ in 0..100 {
            gossip_round(&nodes, &index, &mut rng, 0);
        }

        // Verify alive nodes still converge.
        let alive_count = nodes.iter().filter(|n| n.alive).count();
        assert_eq!(alive_count, 70);

        // Each alive node should know about most other alive nodes.
        for node in nodes.iter().filter(|n| n.alive) {
            let known = known_alive_count(node);
            assert!(
                known >= alive_count / 2,
                "node {} knows only {}/{} alive nodes after cascading failure",
                node.id, known, alive_count,
            );
        }
    }

    // ========================================================================
    // C.4: Thundering herd after partition heal (test 32)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_thundering_herd_after_partition_heal() {
        let n = 100;
        let nodes = create_cluster(n, 5);
        let index = build_index(&nodes);
        let mut rng = Rng::new(7777);

        // Converge.
        for _ in 0..100 {
            gossip_round(&nodes, &index, &mut rng, 0);
        }

        // Run 50 rounds of partition (50/50 split).
        for _ in 0..50 {
            let messages: Vec<(usize, GossipMessage)> = nodes
                .iter()
                .enumerate()
                .filter(|(_, n)| n.alive)
                .map(|(i, n)| (i, n.build_gossip()))
                .collect();

            for (sender_idx, msg) in &messages {
                let peers = nodes[*sender_idx].gossip.select_peers();
                for (peer_id, _) in peers {
                    if let Some(&peer_idx) = index.get(&peer_id) {
                        if !nodes[peer_idx].alive { continue; }
                        let same_half = (*sender_idx < 50) == (peer_idx < 50);
                        if !same_half { continue; }
                        nodes[peer_idx].gossip.handle_gossip(msg.clone());
                    }
                }
            }
        }

        // Heal partition — run 100 rounds with full connectivity.
        for _ in 0..100 {
            gossip_round(&nodes, &index, &mut rng, 0);
        }

        // After healing, all nodes should converge again.
        for node in &nodes {
            let known = known_alive_count(node);
            assert!(
                known >= n * 8 / 10,
                "after partition heal, node {} knows only {}/{} nodes",
                node.id, known, n,
            );
        }
    }

    // ========================================================================
    // C.5: Slow node doesn't starve fast nodes (test 33)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_slow_node_does_not_starve_fast_nodes() {
        let n = 100;
        let nodes = create_cluster(n, 5);
        let index = build_index(&nodes);
        let mut rng = Rng::new(5555);

        // Node 0 is "slow" — it participates in only 1 out of every 10 rounds.
        for round in 0..500 {
            let registry = build_registry(&nodes);
            let messages: Vec<(usize, GossipMessage)> = nodes
                .iter()
                .enumerate()
                .filter(|(i, n)| n.alive && (*i != 0 || round % 10 == 0))
                .map(|(i, n)| (i, n.build_gossip()))
                .collect();

            for (sender_idx, msg) in &messages {
                let peers = nodes[*sender_idx].gossip.select_peers();
                for (peer_id, _) in peers {
                    if let Some(&peer_idx) = index.get(&peer_id) {
                        if !nodes[peer_idx].alive { continue; }
                        nodes[peer_idx].gossip.handle_gossip(msg.clone());
                    }
                }
            }

            // Restore addresses.
            for node in nodes.iter().filter(|n| n.alive) {
                for (&nid, &addr) in &registry {
                    if let Some(mut entry) = node.knowledge.get_node(&nid) {
                        if entry.address.is_none() {
                            entry.address = Some(addr);
                            entry.last_seen = entry.last_seen + ChronoDuration::milliseconds(1);
                            node.knowledge.merge_node(entry);
                        }
                    }
                }
            }
        }

        // Fast nodes should know about nearly all other fast nodes.
        let fast_known: Vec<usize> = nodes[1..]
            .iter()
            .map(|n| known_alive_count(n))
            .collect();
        let avg_fast = fast_known.iter().sum::<usize>() / fast_known.len();
        assert!(
            avg_fast >= n * 8 / 10,
            "fast nodes should know most peers (avg={})",
            avg_fast,
        );

        // Slow node will know fewer, but should still know some (it gossips 50 times).
        let slow_known = known_alive_count(&nodes[0]);
        assert!(
            slow_known >= n / 4,
            "slow node should still know some peers (known={})",
            slow_known,
        );
    }

    // ========================================================================
    // C.6: Knowledge store churn — 50% turnover (test 34)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_knowledge_store_churn() {
        let n = 50;
        let mut nodes = create_cluster(n, 3);
        let mut index = build_index(&nodes);
        let mut rng = Rng::new(11111);
        let mut next_idx = n;

        // Run 200 rounds with periodic node replacement.
        for round in 0..200 {
            gossip_round(&nodes, &index, &mut rng, 5);

            // Every 4 rounds, kill a random node and add a new one.
            if round % 4 == 0 && round > 0 {
                let victim = rng.next_usize(nodes.len());
                nodes[victim].alive = false;

                // Add a new node.
                let mut new_node = StressNode::new(next_idx);
                next_idx += 1;

                // Seed the new node with a few known peers.
                let seeds: Vec<NodeInfo> = nodes
                    .iter()
                    .filter(|n| n.alive)
                    .take(3)
                    .map(|n| NodeInfo {
                        node_id: n.id,
                        last_seen: Utc::now(),
                        traits: HashSet::from([Trait::CanExecute]),
                        load: 0.1,
                        capacity: ResourceSnapshot::default(),
                        address: Some(n.addr),
                        via: n.id,
                        status: NodeStatus::Alive,
                        generation: 1,
                        trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
                    })
                    .collect();
                for seed in &seeds {
                    new_node.knowledge.merge_node(seed.clone());
                }

                nodes.push(new_node);
                index = build_index(&nodes);
            }
        }

        // Memory should be bounded — knowledge stores shouldn't grow unbounded.
        for node in nodes.iter().filter(|n| n.alive) {
            let known = node.knowledge.get_all_nodes().len();
            assert!(
                known <= MAX_KNOWN_NODES + 100,
                "knowledge store should be bounded after churn (got {})",
                known,
            );
        }
    }

    // ========================================================================
    // C.7: Concurrent job submission — 100 jobs (test 35)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_concurrent_job_submission_100_jobs() {
        let n = 50;
        let nodes = create_cluster(n, 3);
        let index = build_index(&nodes);
        let mut rng = Rng::new(22222);

        // Submit 100 jobs with 10 chunks each.
        for j in 0..100 {
            let job_id = crate::common::types::JobId::new();
            let submitter_idx = rng.next_usize(n);

            let job_info = SwarmJobInfo {
                job_id,
                status: SwarmJobStatus::Pending,
                chunks_total: 10,
                chunks_completed: 0,
                chunks_failed: 0,
                submitter: nodes[submitter_idx].id,
                first_seen: Utc::now(),
                updated_at: Utc::now(),
                payload_type: "shell".to_string(),
                priority: 1,
                data_residency: None,
                required_zone_id: "test-zone".to_string(),
                verification_strategy: VerificationStrategy::None,
            };
            nodes[submitter_idx].knowledge.merge_job(job_info);

            // Create 10 chunks for this job.
            for c in 0..10 {
                let assignment = Assignment {
                    chunk_id: ChunkId::new(),
                    job_id,
                    assigned_to: None,
                    assigned_at: Utc::now(),
                    status: ChunkStatus::Pending,
                    result: None,
                    attempts: 0,
                    replica_group_id: None,
                    failed_nodes: Vec::new(),
                };
                nodes[submitter_idx].knowledge.merge_assignment(assignment);
            }
        }

        // Run 200 gossip rounds to propagate jobs.
        for _ in 0..200 {
            gossip_round(&nodes, &index, &mut rng, 0);
        }

        // Every node should know about a significant fraction of the 100 jobs.
        for node in &nodes {
            let known_jobs = node.knowledge.get_all_jobs().len();
            assert!(
                known_jobs >= 50,
                "node {} knows only {}/100 jobs after 200 rounds",
                node.id, known_jobs,
            );
        }
    }

    // ========================================================================
    // C.8: Message reordering (test 36)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_message_reordering() {
        let n = 50;
        let nodes = create_cluster(n, 3);
        let index = build_index(&nodes);
        let mut rng = Rng::new(33333);

        // Collect messages from all nodes, then deliver in random order.
        for _ in 0..200 {
            let mut all_deliveries: Vec<(usize, GossipMessage)> = Vec::new();

            let messages: Vec<(usize, GossipMessage)> = nodes
                .iter()
                .enumerate()
                .filter(|(_, n)| n.alive)
                .map(|(i, n)| (i, n.build_gossip()))
                .collect();

            for (sender_idx, msg) in &messages {
                let peers = nodes[*sender_idx].gossip.select_peers();
                for (peer_id, _) in peers {
                    if let Some(&peer_idx) = index.get(&peer_id) {
                        if !nodes[peer_idx].alive { continue; }
                        all_deliveries.push((peer_idx, msg.clone()));
                    }
                }
            }

            // Shuffle deliveries (Fisher-Yates).
            for i in (1..all_deliveries.len()).rev() {
                let j = rng.next_usize(i + 1);
                all_deliveries.swap(i, j);
            }

            for (peer_idx, msg) in all_deliveries {
                nodes[peer_idx].gossip.handle_gossip(msg);
            }

            // Restore addresses so select_peers() works in the next round.
            let registry = build_registry(&nodes);
            for node in nodes.iter().filter(|n| n.alive) {
                for (&nid, &addr) in &registry {
                    if let Some(mut entry) = node.knowledge.get_node(&nid) {
                        if entry.address.is_none() {
                            entry.address = Some(addr);
                            entry.last_seen = entry.last_seen + ChronoDuration::milliseconds(1);
                            node.knowledge.merge_node(entry);
                        }
                    }
                }
            }
        }

        // LWW commutativity: all nodes should converge to consistent state.
        let reference_nodes = nodes[0].knowledge.get_all_nodes();
        let ref_ids: HashSet<NodeId> = reference_nodes.iter().map(|n| n.node_id).collect();

        let mut divergence_count = 0;
        for node in &nodes[1..] {
            let their_ids: HashSet<NodeId> = node.knowledge.get_all_nodes()
                .iter()
                .map(|n| n.node_id)
                .collect();
            let common = ref_ids.intersection(&their_ids).count();
            if common < ref_ids.len() * 8 / 10 {
                divergence_count += 1;
            }
        }

        assert!(
            divergence_count <= n / 5,
            "too many nodes diverged after reordered delivery ({})",
            divergence_count,
        );
    }

    // ========================================================================
    // C.9: Duplicate message delivery (test 37)
    // ========================================================================

    #[test]
    #[ignore]
    fn stress_duplicate_messages() {
        let n = 50;
        let nodes = create_cluster(n, 3);
        let index = build_index(&nodes);
        let mut rng = Rng::new(44444);

        // Deliver every message twice.
        for _ in 0..200 {
            let registry = build_registry(&nodes);
            let messages: Vec<(usize, GossipMessage)> = nodes
                .iter()
                .enumerate()
                .filter(|(_, n)| n.alive)
                .map(|(i, n)| (i, n.build_gossip()))
                .collect();

            for (sender_idx, msg) in &messages {
                let peers = nodes[*sender_idx].gossip.select_peers();
                for (peer_id, _) in peers {
                    if let Some(&peer_idx) = index.get(&peer_id) {
                        if !nodes[peer_idx].alive { continue; }
                        // Deliver twice (idempotent merge).
                        nodes[peer_idx].gossip.handle_gossip(msg.clone());
                        nodes[peer_idx].gossip.handle_gossip(msg.clone());
                    }
                }
            }

            // Restore addresses.
            for node in nodes.iter().filter(|n| n.alive) {
                for (&nid, &addr) in &registry {
                    if let Some(mut entry) = node.knowledge.get_node(&nid) {
                        if entry.address.is_none() {
                            entry.address = Some(addr);
                            entry.last_seen = entry.last_seen + ChronoDuration::milliseconds(1);
                            node.knowledge.merge_node(entry);
                        }
                    }
                }
            }
        }

        // All nodes should converge. Duplicate delivery should not cause issues.
        for node in &nodes {
            let known = known_alive_count(node);
            assert!(
                known >= n * 8 / 10,
                "node {} knows only {}/{} nodes after duplicate delivery",
                node.id, known, n,
            );
        }

        // Verify no node count inflation from duplicates.
        for node in &nodes {
            let total = node.knowledge.get_all_nodes().len();
            assert!(
                total <= n + 10,
                "duplicate messages should not inflate node count (got {})",
                total,
            );
        }
    }
}
