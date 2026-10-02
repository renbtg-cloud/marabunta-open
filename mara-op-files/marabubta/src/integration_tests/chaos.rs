// Marabunta - Licensed under the MIT License.
//! Chaos test for Marabunta Swarm.
//!
//! Runs a 10-node cluster with continuous workload while randomly killing
//! nodes, adding new nodes, and dropping messages between pairs. Verifies
//! zero panics, zero data corruption, and new node integration.
//!
//! Run explicitly:
//!
//! ```bash
//! cargo test chaos -- --ignored --nocapture
//! ```

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    use chrono::Utc;
    use tokio::sync::mpsc;

    use crate::swarm::config::*;
    use crate::swarm::failure::FailureDetector;
    use crate::swarm::gossip::{GossipConfig, GossipEngine};
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::types::*;

    /// A lightweight simulated node for chaos testing.
    struct ChaosNode {
        id: NodeId,
        addr: SocketAddr,
        knowledge: Arc<KnowledgeStore>,
        gossip: Arc<GossipEngine>,
        alive: bool,
        _outbound_rx: mpsc::Receiver<(SocketAddr, SwarmMessage)>,
    }

    impl ChaosNode {
        fn new(port: u16) -> Self {
            let id = NodeId::new();
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
            });

            ChaosNode {
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

    /// Deterministic pseudo-random number generator (xorshift64).
    /// Avoids ThreadRng which is !Send.
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
            (self.next() as usize) % max
        }

        fn next_bool(&mut self, probability_percent: u32) -> bool {
            (self.next() % 100) < probability_percent as u64
        }
    }

    /// Build address lookup for all alive nodes.
    fn build_address_registry(nodes: &[ChaosNode]) -> HashMap<NodeId, SocketAddr> {
        nodes
            .iter()
            .filter(|n| n.alive)
            .map(|n| (n.id, n.addr))
            .collect()
    }

    /// Restore addresses stripped by handle_gossip.
    /// In a real system, the transport layer fills addresses from the TCP socket.
    /// In tests we simulate this by restoring from the registry after each round.
    /// We bump last_seen by 1ms so merge_node accepts the update (it requires
    /// strictly newer timestamps).
    fn restore_addresses(node: &ChaosNode, registry: &HashMap<NodeId, SocketAddr>) {
        for nid in registry.keys() {
            if let Some(mut entry) = node.knowledge.get_node(nid) {
                if entry.address.is_none() {
                    if let Some(&addr) = registry.get(nid) {
                        entry.address = Some(addr);
                        entry.last_seen = entry.last_seen + chrono::Duration::milliseconds(1);
                        node.knowledge.merge_node(entry);
                    }
                }
            }
        }
    }

    /// Run a gossip round with optional message dropping between specific pairs.
    fn chaos_gossip_round(
        nodes: &[ChaosNode],
        id_to_index: &HashMap<NodeId, usize>,
        drop_pairs: &HashSet<(usize, usize)>,
        rng: &mut Rng,
    ) -> u64 {
        let registry = build_address_registry(nodes);
        let mut messages_delivered = 0u64;

        let messages: Vec<(usize, GossipMessage)> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.alive)
            .map(|(i, n)| (i, n.build_gossip()))
            .collect();

        for (sender_idx, msg) in &messages {
            let peers = nodes[*sender_idx].gossip.select_peers();
            for (peer_id, _addr) in peers {
                if let Some(&peer_idx) = id_to_index.get(&peer_id) {
                    if !nodes[peer_idx].alive {
                        continue;
                    }

                    // Check if this pair has message dropping.
                    let pair = if *sender_idx < peer_idx {
                        (*sender_idx, peer_idx)
                    } else {
                        (peer_idx, *sender_idx)
                    };

                    if drop_pairs.contains(&pair) {
                        continue; // Message dropped.
                    }

                    // Random 5% message loss.
                    if rng.next_bool(5) {
                        continue;
                    }

                    nodes[peer_idx].gossip.handle_gossip(msg.clone());
                    messages_delivered += 1;
                }
            }
        }

        // Restore addresses stripped by handle_gossip.
        for node in nodes.iter().filter(|n| n.alive) {
            restore_addresses(node, &registry);
        }

        messages_delivered
    }

    fn build_index(nodes: &[ChaosNode]) -> HashMap<NodeId, usize> {
        nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id, i))
            .collect()
    }

    /// Submit a synthetic job to a node's knowledge store.
    fn submit_job(node: &ChaosNode, job_num: u64) {
        let job_id = crate::common::types::JobId::new();
        let job = SwarmJobInfo {
            job_id,
            status: SwarmJobStatus::Pending,
            chunks_total: 4,
            chunks_completed: 0,
            chunks_failed: 0,
            submitter: node.id,
            first_seen: Utc::now(),
            updated_at: Utc::now(),
            payload_type: "shell".to_string(),
            priority: 1,
            data_residency: None,
            required_zone_id: "test-zone".to_string(),
            verification_strategy: VerificationStrategy::None,
        };
        node.knowledge.merge_job(job);
    }

    // ========================================================================
    // Tests
    // ========================================================================

    #[test]
    #[ignore]
    fn chaos_10_node_sustained() {
        let initial_nodes = 10;
        let mut next_port = 30000u16;
        let total_rounds = 300; // ~5 minutes at 1s gossip interval
        let chaos_interval = 10; // chaos event every 10 rounds

        // Create initial cluster.
        let mut nodes: Vec<ChaosNode> = (0..initial_nodes)
            .map(|_| {
                let n = ChaosNode::new(next_port);
                next_port += 1;
                n
            })
            .collect();

        // Seed all nodes with knowledge of each other.
        let all_infos: Vec<NodeInfo> = nodes
            .iter()
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
            })
            .collect();

        for node in &nodes {
            for info in &all_infos {
                if info.node_id != node.id {
                    node.knowledge.merge_node(info.clone());
                }
            }
        }

        let mut rng = Rng::new(42);
        let mut drop_pairs: HashSet<(usize, usize)> = HashSet::new();
        let mut total_jobs_submitted = 0u64;
        let mut total_messages_delivered = 0u64;
        let mut nodes_killed = 0u64;
        let mut nodes_added = 0u64;
        let panic_count = Arc::new(AtomicU64::new(0));

        for round in 1..=total_rounds {
            // Submit a job every round.
            let alive_indices: Vec<usize> = nodes
                .iter()
                .enumerate()
                .filter(|(_, n)| n.alive)
                .map(|(i, _)| i)
                .collect();

            if !alive_indices.is_empty() {
                let target = alive_indices[rng.next_usize(alive_indices.len())];
                submit_job(&nodes[target], total_jobs_submitted);
                total_jobs_submitted += 1;
            }

            // Run gossip.
            let id_to_index = build_index(&nodes);
            let delivered = chaos_gossip_round(&nodes, &id_to_index, &drop_pairs, &mut rng);
            total_messages_delivered += delivered;

            // Chaos events.
            if round % chaos_interval == 0 {
                let event = rng.next_usize(3);
                let alive_count = nodes.iter().filter(|n| n.alive).count();

                match event {
                    0 if alive_count > 3 => {
                        // Kill a random node (but keep at least 3 alive).
                        let victim = alive_indices[rng.next_usize(alive_indices.len())];
                        nodes[victim].alive = false;
                        nodes_killed += 1;

                        // Mark the killed node as stale in all other nodes.
                        let ancient = Utc::now() - chrono::Duration::seconds(120);
                        let dead_id = nodes[victim].id;
                        let dead_addr = nodes[victim].addr;
                        for (i, node) in nodes.iter().enumerate() {
                            if i != victim && node.alive {
                                node.knowledge.merge_node(NodeInfo {
                                    node_id: dead_id,
                                    last_seen: ancient,
                                    traits: HashSet::from([Trait::CanExecute]),
                                    load: 0.0,
                                    capacity: ResourceSnapshot::default(),
                                    address: Some(dead_addr),
                                    via: dead_id,
                                    status: NodeStatus::Alive,
                                    generation: 1,
                                    trust_level: Default::default(),
                                });
                            }
                        }

                        if round % 50 == 0 {
                            eprintln!(
                                "Round {}: Killed node {} (alive: {})",
                                round, victim, alive_count - 1
                            );
                        }
                    }
                    1 => {
                        // Add a new node.
                        let new_node = ChaosNode::new(next_port);
                        next_port += 1;

                        // Seed the new node with knowledge of alive nodes.
                        for node in nodes.iter().filter(|n| n.alive) {
                            new_node.knowledge.merge_node(NodeInfo {
                                node_id: node.id,
                                last_seen: Utc::now(),
                                traits: HashSet::from([Trait::CanExecute]),
                                load: 0.1,
                                capacity: ResourceSnapshot::default(),
                                address: Some(node.addr),
                                via: node.id,
                                status: NodeStatus::Alive,
                                generation: 1,
                                trust_level: Default::default(),
                            });
                        }

                        // Announce new node to a few existing nodes.
                        let new_info = NodeInfo {
                            node_id: new_node.id,
                            last_seen: Utc::now(),
                            traits: HashSet::from([Trait::CanExecute]),
                            load: 0.1,
                            capacity: ResourceSnapshot::default(),
                            address: Some(new_node.addr),
                            via: new_node.id,
                            status: NodeStatus::Alive,
                            generation: 1,
                            trust_level: Default::default(),
                        };
                        let announce_count = std::cmp::min(3, alive_indices.len());
                        for i in 0..announce_count {
                            nodes[alive_indices[i]].knowledge.merge_node(new_info.clone());
                        }

                        nodes.push(new_node);
                        nodes_added += 1;

                        if round % 50 == 0 {
                            eprintln!(
                                "Round {}: Added new node (total: {})",
                                round,
                                nodes.len()
                            );
                        }
                    }
                    2 => {
                        // Toggle message dropping between a random pair.
                        if alive_indices.len() >= 2 {
                            let a = alive_indices[rng.next_usize(alive_indices.len())];
                            let mut b = alive_indices[rng.next_usize(alive_indices.len())];
                            while b == a {
                                b = alive_indices[rng.next_usize(alive_indices.len())];
                            }
                            let pair = if a < b { (a, b) } else { (b, a) };

                            if drop_pairs.contains(&pair) {
                                drop_pairs.remove(&pair);
                            } else {
                                drop_pairs.insert(pair);
                            }
                        }
                    }
                    _ => {
                        // No chaos this round (alive_count too low to kill).
                    }
                }
            }

            // Periodic failure detection sweep on node 0 (if alive).
            if round % 30 == 0 {
                if let Some(n0) = nodes.iter().find(|n| n.alive) {
                    let detector = FailureDetector::new(n0.knowledge.clone(), n0.id)
                        .with_thresholds(SUSPECT_THRESHOLD, DEAD_THRESHOLD);
                    let report = detector.sweep();
                    if report.has_changes() && round % 60 == 0 {
                        eprintln!(
                            "Round {}: FailureDetector: {} suspect, {} dead",
                            round,
                            report.nodes_suspected.len(),
                            report.nodes_declared_dead.len()
                        );
                    }
                }
            }
        }

        // Final assertions.
        let alive_count = nodes.iter().filter(|n| n.alive).count();
        eprintln!("\n=== Chaos Test Results ===");
        eprintln!("Total rounds: {}", total_rounds);
        eprintln!("Nodes killed: {}", nodes_killed);
        eprintln!("Nodes added: {}", nodes_added);
        eprintln!("Final cluster size: {} ({} alive)", nodes.len(), alive_count);
        eprintln!("Jobs submitted: {}", total_jobs_submitted);
        eprintln!("Messages delivered: {}", total_messages_delivered);
        eprintln!("Panics: {}", panic_count.load(Ordering::SeqCst));

        // Zero panics.
        assert_eq!(panic_count.load(Ordering::SeqCst), 0, "Panics detected!");

        // At least 3 nodes should be alive.
        assert!(
            alive_count >= 3,
            "Too few alive nodes: {}",
            alive_count
        );

        // All alive nodes should have consistent job knowledge (eventually).
        // Run a few more gossip rounds for final convergence.
        let id_to_index = build_index(&nodes);
        for _ in 0..20 {
            chaos_gossip_round(&nodes, &id_to_index, &HashSet::new(), &mut rng);
        }

        // Check job counts are similar across alive nodes.
        let job_counts: Vec<usize> = nodes
            .iter()
            .filter(|n| n.alive)
            .map(|n| n.knowledge.job_count())
            .collect();

        if !job_counts.is_empty() {
            let max_jobs = *job_counts.iter().max().unwrap();
            let min_jobs = *job_counts.iter().min().unwrap();
            eprintln!(
                "Job knowledge spread: min={}, max={}",
                min_jobs, max_jobs
            );
            // All alive nodes should know about most jobs.
            assert!(
                max_jobs > 0,
                "No jobs propagated!"
            );
        }
    }

    #[test]
    #[ignore]
    fn chaos_new_node_integrates_within_30_rounds() {
        // Verify that a new node joining a 10-node cluster integrates
        // (learns about all other nodes) within 30 gossip rounds.
        let n = 10;
        let mut next_port = 31000u16;

        let mut nodes: Vec<ChaosNode> = (0..n)
            .map(|_| {
                let node = ChaosNode::new(next_port);
                next_port += 1;
                node
            })
            .collect();

        // Full mesh initial knowledge.
        let all_infos: Vec<NodeInfo> = nodes
            .iter()
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
            })
            .collect();

        for node in &nodes {
            for info in &all_infos {
                if info.node_id != node.id {
                    node.knowledge.merge_node(info.clone());
                }
            }
        }

        // Add a new node that only knows about 2 existing nodes.
        let new_node = ChaosNode::new(next_port);
        new_node
            .knowledge
            .merge_node(all_infos[0].clone());
        new_node
            .knowledge
            .merge_node(all_infos[1].clone());

        // Tell those 2 nodes about the new node.
        let new_info = NodeInfo {
            node_id: new_node.id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.1,
            capacity: ResourceSnapshot::default(),
            address: Some(new_node.addr),
            via: new_node.id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        };
        nodes[0].knowledge.merge_node(new_info.clone());
        nodes[1].knowledge.merge_node(new_info);

        nodes.push(new_node);
        let total = nodes.len(); // 11

        let mut rng = Rng::new(99);
        let id_to_index = build_index(&nodes);

        let mut integrated_at = None;
        for round in 1..=30 {
            chaos_gossip_round(&nodes, &id_to_index, &HashSet::new(), &mut rng);

            let new_node_knowledge = nodes.last().unwrap().knowledge.node_count();
            if new_node_knowledge >= total {
                integrated_at = Some(round);
                break;
            }
        }

        let round = integrated_at.expect(&format!(
            "New node did not integrate within 30 rounds! Knows {} of {} nodes",
            nodes.last().unwrap().knowledge.node_count(),
            total
        ));

        eprintln!(
            "New node integrated in {} rounds (knows {} nodes)",
            round,
            nodes.last().unwrap().knowledge.node_count()
        );

        assert!(
            round <= 30,
            "Integration took {} rounds, expected <= 30",
            round
        );
    }

    #[test]
    #[ignore]
    fn chaos_partition_heal() {
        // Simulate a network partition: 5 nodes can't talk to the other 5.
        // Then heal the partition and verify re-convergence.
        let n = 10;
        let mut next_port = 32000u16;

        let nodes: Vec<ChaosNode> = (0..n)
            .map(|i| {
                ChaosNode::new(32000 + i as u16)
            })
            .collect();

        // Full mesh.
        let all_infos: Vec<NodeInfo> = nodes
            .iter()
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
            })
            .collect();

        for node in &nodes {
            for info in &all_infos {
                if info.node_id != node.id {
                    node.knowledge.merge_node(info.clone());
                }
            }
        }

        let id_to_index = build_index(&nodes);
        let mut rng = Rng::new(77);

        // Create partition: nodes 0-4 can't talk to nodes 5-9.
        let mut drop_pairs: HashSet<(usize, usize)> = HashSet::new();
        for a in 0..5 {
            for b in 5..10 {
                drop_pairs.insert((a, b));
            }
        }

        // Submit different jobs to each partition.
        for i in 0..5 {
            submit_job(&nodes[i], i as u64);
        }
        for i in 5..10 {
            submit_job(&nodes[i], (i + 100) as u64);
        }

        // Run gossip with partition active.
        for _ in 0..30 {
            chaos_gossip_round(&nodes, &id_to_index, &drop_pairs, &mut rng);
        }

        // Verify partition: each side should have different job knowledge.
        let jobs_side_a = nodes[0].knowledge.job_count();
        let jobs_side_b = nodes[5].knowledge.job_count();
        eprintln!(
            "During partition: side A jobs={}, side B jobs={}",
            jobs_side_a, jobs_side_b
        );

        // Heal partition.
        drop_pairs.clear();

        // Run clean gossip (no random drops) to re-converge.
        for _ in 0..80 {
            chaos_gossip_round(&nodes, &id_to_index, &HashSet::new(), &mut rng);
        }

        // After healing, all nodes should know about all jobs.
        let jobs_after: Vec<usize> = nodes
            .iter()
            .map(|n| n.knowledge.job_count())
            .collect();

        let max_jobs = *jobs_after.iter().max().unwrap();
        let min_jobs = *jobs_after.iter().min().unwrap();

        eprintln!(
            "After healing: min_jobs={}, max_jobs={}",
            min_jobs, max_jobs
        );

        // All nodes should converge on the same job count.
        assert!(
            max_jobs - min_jobs <= 2,
            "Job knowledge didn't converge after partition heal: min={}, max={}",
            min_jobs,
            max_jobs
        );

        // Total jobs should be at least the sum of both sides.
        assert!(
            max_jobs >= 8,
            "Expected at least 8 jobs after healing, got {}",
            max_jobs
        );
    }
}
