use std::fs;

fn main() {
    let mut code = fs::read_to_string("src/swarm/mod.rs").unwrap();
    
    // 1. Setup Trait
    if !code.contains("use crate::swarm::planetary::ledger::ConsensusLedger;") {
        code = code.replace(
            "use crate::swarm::types::{NodeId, SwarmMessage};",
            "use crate::swarm::types::{NodeId, SwarmMessage};\nuse crate::swarm::planetary::ledger::ConsensusLedger;"
        );
    }

    // 2. Instantiate and wire everything early to break the capture loop
    let target_early = r#"        let gateway_rx_container = std::sync::Arc::new(tokio::sync::Mutex::new(Some(gateway_rx)));"#;
    let replacement_early = r#"        let gateway_rx_container = std::sync::Arc::new(tokio::sync::Mutex::new(Some(gateway_rx)));
        
        let hashgraph_engine = std::sync::Arc::new(crate::swarm::planetary::ledger::HashgraphEngine::new(100));
        let bft_outbound_tx = outbound_tx.clone();
        let bft_knowledge = knowledge.clone();
        let bft_hashgraph_daemon = hashgraph_engine.clone();
        let bft_id_daemon = id;
        
        tokio::spawn(async move {
            let mut last_processed: usize = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let events = bft_hashgraph_daemon.get_recent_events(last_processed);
                last_processed += events.len();
                for ev in events {
                    if let Ok(dag_json) = serde_json::to_string(&ev) {
                        let msg = crate::swarm::types::SwarmMessage::BftEvent {
                            dag_event_json: dag_json,
                            from: bft_id_daemon,
                        };
                        for target in bft_knowledge.get_live_nodes().into_iter().take(5) {
                            if target.node_id != bft_id_daemon {
                                if let Some(addr) = target.address {
                                    let _ = bft_outbound_tx.send((addr, msg.clone())).await;
                                }
                            }
                        }
                    }
                }
            }
        });"#;
    code = code.replace(target_early, replacement_early);

    // 3. Inject correctly into the transport closure
    let target_closure = r#"        let outbound_tx_clone_for_strikes = self._outbound_tx.clone();
        Arc::new(move |_peer_addr: SocketAddr, message: SwarmMessage| {"#;
    let replacement_closure = r#"        let outbound_tx_clone_for_strikes = self._outbound_tx.clone();
        let bft_engine_msgs = hashgraph_engine.clone();
        let bft_id_msgs = id;
        let bft_knowledge_msgs = knowledge.clone();
        Arc::new(move |_peer_addr: SocketAddr, message: SwarmMessage| {"#;
    code = code.replace(target_closure, replacement_closure);

    // 4. Arms
    let target_arms = r#"                SwarmMessage::FetchBlob { hash, reply_to } => {"#;
    let replacement_arms = r#"                SwarmMessage::BftEvent { dag_event_json, from } => {
                    let bft_clone = bft_engine_msgs.clone();
                    let bft_id = bft_id_msgs;
                    let tx_inner = outbound_tx_clone_for_strikes.clone();
                    let knowledge_inner = bft_knowledge_msgs.clone();
                    if let Ok(ev) = serde_json::from_str::<crate::swarm::planetary::ledger::DagEvent>(&dag_event_json) {
                        bft_clone.insert_event(ev.clone());
                        let vote = crate::swarm::types::SwarmMessage::BftVote {
                            event_hash: ev.hash,
                            signature_bytes: vec![1, 2, 3],
                            from: bft_id,
                        };
                        tokio::spawn(async move {
                            if let Some(addr) = knowledge_inner.get_node(&from).and_then(|n| n.address) {
                                let _ = tx_inner.send((addr, vote)).await;
                            }
                        });
                    }
                }
                SwarmMessage::BftVote { event_hash, signature_bytes: _, from: _ } => {
                    bft_engine_msgs.record_vote(event_hash);
                }
                SwarmMessage::FetchBlob { hash, reply_to } => {"#;
    code = code.replace(target_arms, replacement_arms);

    fs::write("src/swarm/mod.rs", code).unwrap();
}
