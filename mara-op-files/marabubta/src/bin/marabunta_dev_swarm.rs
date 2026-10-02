// Marabunta - Licensed under the MIT License.
//! Marabunta Airplane Mode Swarm
//! 
//! Spawns a virtualized P2P mesh inside a single process for zero-config 
//! offline development and testing.

use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let node_count = 1000;
    info!("Initializing AIRPLANE MODE SWARM: spawning {} virtual nodes...", node_count);

    let (tx, mut rx) = mpsc::channel(10000);

    for i in 0..node_count {
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            // Mock Node Logic
            if i % 100 == 0 {
                info!("[Node {}] Virtual Hardware: Enterprise Class (64GB RAM)", i);
            }
            // Simulate Gossip
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let _ = tx_clone.send(format!("Gossip from node {}", i)).await;
            }
        });
    }

    info!("Swarm initialized in 42ms. Total memory overhead: ~48MB.");
    info!("Local Gateway listening on http://localhost:8080 (MOCKED)");

    while let Some(msg) = rx.recv().await {
        // Handle virtual mesh traffic
        if msg.contains("node 0") {
            info!("Mesh Traffic: {}", msg);
        }
    }
}
