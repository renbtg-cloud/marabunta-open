# MARABUNTA REDONE: CORE ARCHITECTURE (CODE-BASED)

## 1. The Thermodynamic Root (Identity & Hardware)
- **The Chrysalis Grinder:** L3 cache saturation, SHA-256 Proof-of-Work to derive `NodeId`. Defeating ASICs through memory-bus bottlenecking. (`src/swarm/pow_worker.rs`)
- **The Thermal Guillotine:** Wasmtime epoch interruption. Detecting 91C or sustained load heuristics to physically assassinate runaway algorithms. (`src/swarm/thermal.rs`, `src/highestsec/sandbox.rs`)

## 2. Kademlia & The Fluid Topology (Network Routing)
- **XOR Density Estimation:** Extrapolating planetary scale (10M+ nodes) from a RAM-capped 10k routing table using prefix-length probabilities. (`src/swarm/knowledge.rs`)
- **Adaptive Gossip Jitter:** Throttling epidemic heartbeats inversely to swarm size to prevent global DDoS. (`src/swarm/gossip.rs`)
- **O(1) Data Gravity:** Utilizing `DashSet` for instant pending-chunk claims to prevent CPU spin death. (`src/swarm/work.rs`)

## 3. The Execution Membrane (WASM & Data Plane)
- **Zero-Footprint WASI Streaming:** The 64MB `Arc<RwLock>` shared cache. Streaming 1.5TB datasets into 4GB linear memory limits using HTTP Range requests without OOMing the host. (`src/highestsec/sandbox.rs`)
- **Data Plane Decoupling:** Gossiping 32-byte Blake3 hashes instead of 2GB result matrices. Direct S3 exfiltration via presigned URLs to prevent Orchestrator disk implosion. (`src/swarm/work.rs`, `src/swarm/blobstore.rs`)

## 4. Enterprise Sovereignty (Airgaps & Relays)
- **The Egress Diode:** Spinning up local TCP HTTP proxies (port 8080) to feed air-gapped internal LAN nodes. (`src/api/gateway.rs`)
- **Hierarchical Back-Routing:** Encapsulating local aggregator results into `SwarmMessage::EgressRelay` envelopes to tunnel through Corporate Firewalls via the Membrane Gateway. (`src/swarm/mod.rs`)

## 5. The Cryptographic Economy (MMX Settlement)
- **Ed25519 Tax Receipts:** Signing the `chunk_id:worker:amount:timestamp` payload to mathematically prevent CSV replay attacks. (`src/marabunta/federation.rs`)
- **SQLite Concurrency:** Utilizing `PRAGMA journal_mode=WAL` and `wal_autocheckpoint=1000` with an MPSC background thread to achieve high-throughput settlement without Tokio starvation. (`src/marabunta/federation.rs`)