use std::fs;

fn main() {
    let mut code = fs::read_to_string("src/swarm/mod.rs").unwrap();
    
    code = code.replace(
        "use crate::swarm::types::{NodeId, SwarmMessage};",
        "use crate::swarm::types::{NodeId, SwarmMessage};\nuse crate::swarm::planetary::ledger::ConsensusLedger;"
    );

    let target_init = r#"        let gateway_rx_container = std::sync::Arc::new(tokio::sync::Mutex::new(Some(gateway_rx)));"#;
    let replacement_init = r#"        let gateway_rx_container = std::sync::Arc::new(tokio::sync::Mutex::new(Some(gateway_rx)));
        
        let hashgraph_engine = std::sync::Arc::new(crate::swarm::planetary::ledger::HashgraphEngine::new(100));
        let sovereign_router = std::sync::Arc::new(crate::swarm::ext_events::bft_ledger::SovereignEventRouter::new(
            id,
            hashgraph_engine.clone(),
            blob_store.clone(),
            knowledge.clone(),
            transport.clone(), // We use the Transport interface directly via Arc
        ));
        let _ext_event_broker = std::sync::Arc::new(crate::swarm::ext_events::broker::ExtEventBroker::new(
            sovereign_router.clone()
        ));
        
        let bft_hashgraph_gossip = hashgraph_engine.clone();
        let bft_outbound_gossip = outbound_tx.clone();
        let bft_knowledge_gossip = knowledge.clone();
        let bft_id = id;
        
        tokio::spawn(async move {
            let mut last_processed: usize = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let events = bft_hashgraph_gossip.get_recent_events(last_processed);
                last_processed += events.len();
                for ev in events {
                    if let Ok(dag_json) = serde_json::to_string(&ev) {
                        let msg = crate::swarm::types::SwarmMessage::BftEvent {
                            dag_event_json: dag_json,
                            from: bft_id,
                        };
                        for target in bft_knowledge_gossip.get_live_nodes().into_iter().take(5) {
                            if target.node_id != bft_id {
                                if let Some(addr) = target.address {
                                    let _ = bft_outbound_gossip.send((addr, msg.clone())).await;
                                }
                            }
                        }
                    }
                }
            }
        });"#;
    code = code.replace(target_init, replacement_init);

    let target_closure = r#"        let outbound_tx_clone_for_strikes = self._outbound_tx.clone();
        Arc::new(move |_peer_addr: SocketAddr, message: SwarmMessage| {"#;
    let replacement_closure = r#"        let outbound_tx_clone_for_strikes = self._outbound_tx.clone();
        let hashgraph_engine_msg = hashgraph_engine.clone();
        let knowledge_msg = _knowledge.clone();
        let my_id_msg = id;
        Arc::new(move |_peer_addr: SocketAddr, message: SwarmMessage| {"#;
    code = code.replace(target_closure, replacement_closure);

    let target_arms = r#"                SwarmMessage::FetchBlob { hash, reply_to } => {"#;
    let replacement_arms = r#"                SwarmMessage::BftEvent { dag_event_json, from } => {
                    let bft_engine_inner = hashgraph_engine_msg.clone();
                    let knowledge_inner = knowledge_msg.clone();
                    let tx_inner = outbound_tx_clone_for_strikes.clone();
                    let my_id = my_id_msg;
                    if let Ok(ev) = serde_json::from_str::<crate::swarm::planetary::ledger::DagEvent>(&dag_event_json) {
                        bft_engine_inner.insert_event(ev.clone());
                        let vote = crate::swarm::types::SwarmMessage::BftVote {
                            event_hash: ev.hash,
                            signature_bytes: vec![1, 2, 3],
                            from: my_id,
                        };
                        tokio::spawn(async move {
                            if let Some(addr) = knowledge_inner.get_node(&from).and_then(|n| n.address) {
                                let _ = tx_inner.send((addr, vote)).await;
                            }
                        });
                    }
                }
                SwarmMessage::BftVote { event_hash, signature_bytes: _, from: _ } => {
                    hashgraph_engine_msg.record_vote(event_hash);
                }
                SwarmMessage::FetchBlob { hash, reply_to } => {"#;
    code = code.replace(target_arms, replacement_arms);

    fs::write("src/swarm/mod.rs", code).unwrap();
}
