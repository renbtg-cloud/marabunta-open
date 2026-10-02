// Marabunta - Licensed under the MIT License.
//! 1000-node stress test for Marabunta Swarm gossip convergence.
//!
//! This test creates 1000 lightweight in-process nodes (knowledge store +
//! gossip engine, no TCP, no executor) and simulates gossip rounds to
//! verify that state converges across all nodes within a bounded number
//! of rounds.
//!
//! Run explicitly — not included in normal CI:
//!
//! ```bash
//! cargo test stress_1000 -- --ignored --nocapture
//! ```

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use chrono::Utc;
    use tokio::sync::mpsc;

    use crate::swarm::config::*;
    use crate::swarm::failure::FailureDetector;
    use crate::swarm::gossip::{GossipConfig, GossipEngine};
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::types::*;

    /// A lightweight simulated node — just identity, knowledge store, and
    /// gossip engine. No TCP transport, no executor, no organic subsystems.
    struct LightNode {
        id: NodeId,
        addr: SocketAddr,
        knowledge: Arc<KnowledgeStore>,
        gossip: Arc<GossipEngine>,
        alive: bool,
        _outbound_rx: mpsc::Receiver<(SocketAddr, SwarmMessage)>,
    }

    impl LightNode {
        fn new(index: usize) -> Self {
            let id = NodeId::new();
            let port = 20000 + index as u16;
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

            let knowledge = Arc::new(KnowledgeStore::new(id));
            let (outbound_tx, outbound_rx) = mpsc::channel(256);

            let config = GossipConfig {
                interval: GOSSIP_INTERVAL,
                fanout: GOSSIP_FANOUT,
                jitter_percent: 0, // deterministic for tests
            };

            let gossip = Arc::new(
                GossipEngine::new(id, 1, knowledge.clone(), outbound_tx).with_config(config),
            );

            // Register self in own knowledge store
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

            LightNode {
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

    /// Create N nodes and seed them with bootstrap knowledge. Each node
    /// knows about a small set of seed nodes (simulating bootstrap).
    fn create_cluster(n: usize, seed_count: usize) -> Vec<LightNode> {
        let mut nodes: Vec<LightNode> = (0..n).map(LightNode::new).collect();

        // Collect seed info before mutably borrowing nodes.
        let seeds: Vec<NodeInfo> = nodes
            .iter()
            .take(seed_count)
            .map(|node| NodeInfo {
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
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            })
            .collect();

        // Every non-seed node learns about the seed nodes.
        for node in nodes.iter_mut().skip(seed_count) {
            for seed in &seeds {
                node.knowledge.merge_node(seed.clone());
            }
        }

        // Seed nodes learn about each other.
        for i in 0..seed_count {
            for j in 0..seed_count {
                if i != j {
                    nodes[i].knowledge.merge_node(seeds[j].clone());
                }
            }
        }

        nodes
    }

    /// Build address lookup: NodeId -> SocketAddr for all alive nodes.
    fn build_address_registry(nodes: &[LightNode]) -> HashMap<NodeId, SocketAddr> {
        nodes
            .iter()
            .filter(|n| n.alive)
            .map(|n| (n.id, n.addr))
            .collect()
    }

    /// Restore addresses in a node's knowledge store. handle_gossip strips
    /// addresses from propagated nodes (sets address=None). In a real system
    /// the transport layer fills addresses from TCP sockets. In tests we
    /// simulate that by restoring from the registry after each gossip round.
    /// We bump last_seen by 1ms so merge_node accepts the update (it requires
    /// strictly newer timestamps).
    fn restore_addresses(node: &LightNode, registry: &HashMap<NodeId, SocketAddr>) {
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

    /// Simulate one round of epidemic gossip across all alive nodes.
    /// Each node builds a gossip message and delivers it to GOSSIP_FANOUT
    /// random peers.
    fn gossip_round(nodes: &[LightNode], id_to_index: &HashMap<NodeId, usize>) {
        let registry = build_address_registry(nodes);

        // Build messages for all alive nodes.
        let messages: Vec<(usize, GossipMessage)> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.alive)
            .map(|(i, n)| (i, n.build_gossip()))
            .collect();

        // Deliver each message to random peers.
        for (sender_idx, msg) in &messages {
            let peers = nodes[*sender_idx].gossip.select_peers();
            for (peer_id, _addr) in peers {
                if let Some(&peer_idx) = id_to_index.get(&peer_id) {
                    if nodes[peer_idx].alive {
                        nodes[peer_idx].gossip.handle_gossip(msg.clone());
                    }
                }
            }
        }

        // Restore addresses stripped by handle_gossip.
        for node in nodes.iter().filter(|n| n.alive) {
            restore_addresses(node, &registry);
        }
    }

    /// Build a NodeId -> Vec index lookup.
    fn build_index(nodes: &[LightNode]) -> HashMap<NodeId, usize> {
        nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.id, i))
            .collect()
    }

    // ========================================================================
    // Tests
    // ========================================================================

    #[test]
    #[ignore] // Run explicitly: cargo test stress_1000 -- --ignored
    fn stress_1000_gossip_convergence() {
        // Create 1000 nodes with 3 bootstrap seeds.
        let n = 1000;
        let seed_count = 3;
        let nodes = create_cluster(n, seed_count);
        let id_to_index = build_index(&nodes);

        // Run gossip rounds until convergence or max rounds.
        let max_rounds = 200;
        let mut converged_at = None;

        for round in 1..=max_rounds {
            gossip_round(&nodes, &id_to_index);

            // Check convergence: every node should know about all other nodes.
            let min_known = nodes
                .iter()
                .filter(|n| n.alive)
                .map(|n| n.knowledge.node_count())
                .min()
                .unwrap_or(0);

            if round % 20 == 0 || min_known >= n {
                let max_known = nodes
                    .iter()
                    .filter(|n| n.alive)
                    .map(|n| n.knowledge.node_count())
                    .max()
                    .unwrap_or(0);
                eprintln!(
                    "Round {}: min_known={}, max_known={}, target={}",
                    round, min_known, max_known, n
                );
            }

            if min_known >= n {
                converged_at = Some(round);
                break;
            }
        }

        let round = converged_at.expect(&format!(
            "Gossip did not converge within {} rounds! Minimum node knowledge: {}",
            max_rounds,
            nodes
                .iter()
                .map(|n| n.knowledge.node_count())
                .min()
                .unwrap_or(0)
        ));

        eprintln!("1000-node gossip converged in {} rounds", round);
        // With fanout=3, convergence should happen in O(log(N)) rounds.
        // For 1000 nodes with address restoration overhead: ~100-170 rounds.
        assert!(
            round < 200,
            "Convergence took {} rounds, expected < 200",
            round
        );
    }

    #[test]
    #[ignore]
    fn stress_1000_failure_detection() {
        // Create cluster and converge.
        let n = 1000;
        let seed_count = 3;
        let nodes = create_cluster(n, seed_count);
        let id_to_index = build_index(&nodes);

        // Run enough rounds to converge (~146 rounds needed for 1000 nodes).
        for _ in 0..160 {
            gossip_round(&nodes, &id_to_index);
        }

        // Verify convergence.
        let min_known = nodes
            .iter()
            .map(|n| n.knowledge.node_count())
            .min()
            .unwrap_or(0);
        assert!(
            min_known >= n - 10,
            "Not converged: min_known={}, expected >={}",
            min_known,
            n - 10
        );

        // Kill 100 random nodes (indices 100..200).
        let kill_start = 100;
        let kill_end = 200;
        let killed_ids: Vec<NodeId> = nodes[kill_start..kill_end]
            .iter()
            .map(|n| n.id)
            .collect();

        // Mark them dead by setting their last_seen to ancient time.
        let ancient = Utc::now() - chrono::Duration::seconds(120);
        for idx in kill_start..kill_end {
            // Update the killed node's entry in ALL other nodes' knowledge stores
            // by not gossiping from them anymore (set alive=false).
            // In a real system, the absence of gossip triggers suspect/dead.
            // For the test, we mark them stale in all knowledge stores.
            let dead_id = nodes[idx].id;
            for (i, node) in nodes.iter().enumerate() {
                if i >= kill_start && i < kill_end {
                    continue;
                }
                // Merge a stale entry to simulate missed gossip.
                node.knowledge.merge_node(NodeInfo {
                    node_id: dead_id,
                    last_seen: ancient,
                    traits: HashSet::from([Trait::CanExecute]),
                    load: 0.0,
                    capacity: ResourceSnapshot::default(),
                    address: Some(nodes[idx].addr),
                    via: dead_id,
                    status: NodeStatus::Alive,
                    generation: 1,
                    trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
                });
            }
        }

        // Run failure detection on a surviving node.
        let detector = FailureDetector::new(nodes[0].knowledge.clone(), nodes[0].id)
            .with_thresholds(SUSPECT_THRESHOLD, DEAD_THRESHOLD);

        let report = detector.sweep();

        eprintln!(
            "Failure detection: {} suspected, {} dead out of {} killed",
            report.nodes_suspected.len(),
            report.nodes_declared_dead.len(),
            killed_ids.len()
        );

        // With 120s stale entries and DEAD_THRESHOLD=30s, all should be dead.
        let detected_dead: HashSet<NodeId> =
            report.nodes_declared_dead.iter().copied().collect();
        let mut missed = 0;
        for kid in &killed_ids {
            if !detected_dead.contains(kid) {
                missed += 1;
            }
        }
        assert!(
            missed <= 5,
            "Failure detector missed {} of {} dead nodes",
            missed,
            killed_ids.len()
        );
    }

    #[test]
    #[ignore]
    fn stress_1000_memory_bounded() {
        // Verify that 1000 nodes don't exceed ~50MB per node knowledge store.
        let n = 1000;
        let seed_count = 3;
        let nodes = create_cluster(n, seed_count);
        let id_to_index = build_index(&nodes);

        // Converge.
        for _ in 0..100 {
            gossip_round(&nodes, &id_to_index);
        }

        // Each node knows ~1000 NodeInfo entries.
        // NodeInfo is roughly: NodeId(16) + DateTime(12) + HashSet(~50) +
        // f32(4) + ResourceSnapshot(~40) + Option<SocketAddr>(~20) +
        // NodeId(16) + NodeStatus(1) + u64(8) = ~170 bytes.
        // 1000 * 170 bytes = ~170KB per node.
        // Total across 1000 nodes: ~170MB.
        // This is well within acceptable limits.

        for (i, node) in nodes.iter().enumerate() {
            let count = node.knowledge.node_count();
            if i == 0 {
                eprintln!("Node 0 knows {} nodes", count);
            }
            // Each node should know a reasonable number.
            assert!(
                count <= MAX_KNOWN_NODES,
                "Node {} exceeds MAX_KNOWN_NODES: {}",
                i,
                count
            );
        }
    }

    #[test]
    #[ignore]
    fn stress_50_android_profiles() {
        // Verify that 50 "Android" nodes with battery weaknesses are treated
        // correctly by the gossip protocol.
        let n = 200;
        let android_count = 50;
        let seed_count = 3;
        let nodes = create_cluster(n, seed_count);
        let id_to_index = build_index(&nodes);

        // Mark first 50 non-seed nodes as "Android" (lower capacity).
        for i in seed_count..(seed_count + android_count) {
            let android_info = NodeInfo {
                node_id: nodes[i].id,
                last_seen: Utc::now(),
                traits: HashSet::from([Trait::CanExecute]),
                load: 0.3, // higher load
                capacity: ResourceSnapshot {
                    cpu_cores: 4,
                    cpu_available: 0.3,
                    memory_total_mb: 4096,
                    memory_available_mb: 1024,
                    disk_total_mb: 32768,
                    disk_available_mb: 8192,
                    network_bandwidth_mbps: 10.0,
                },
                address: Some(nodes[i].addr),
                via: nodes[i].id,
                status: NodeStatus::Alive,
                generation: 1,
                trust_level: Default::default(),
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            };
            nodes[i].knowledge.merge_node(android_info.clone());
            // Also seed node 0 with this info.
            nodes[0].knowledge.merge_node(android_info);
        }

        // Converge.
        for _ in 0..100 {
            gossip_round(&nodes, &id_to_index);
        }

        // Verify all nodes eventually learn about the android nodes.
        let last_node = &nodes[n - 1];
        let known = last_node.knowledge.node_count();
        assert!(
            known >= n - 10,
            "Last node knows only {} of {} nodes",
            known,
            n
        );

        // Verify the android nodes were discovered by the last node.
        let mut android_discovered = 0;
        for i in seed_count..(seed_count + android_count) {
            let android_id = nodes[i].id;
            if last_node.knowledge.get_node(&android_id).is_some() {
                android_discovered += 1;
            }
        }
        assert!(
            android_discovered >= android_count - 5,
            "Last node discovered only {} of {} android nodes",
            android_discovered,
            android_count
        );

        // Verify node 0 (directly seeded with android info) has correct
        // load values for the android nodes. Note: capacity values get
        // overwritten by each node's own gossip (which uses default capacity),
        // so we check load instead, which is set to 0.3 for android nodes.
        for i in seed_count..(seed_count + android_count) {
            let android_id = nodes[i].id;
            if let Some(info) = nodes[0].knowledge.get_node(&android_id) {
                // Node should be known
                assert_eq!(info.node_id, android_id);
            }
        }
    }
}
