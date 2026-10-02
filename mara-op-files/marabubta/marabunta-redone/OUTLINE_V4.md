# MARABUNTA REDONE: THE ARCHITECTURE OF A SOVEREIGN SWARM

## PREFACE: The Extinction of the Hyperscaler
- **The Core Thesis:** Cloud computing is dead under the weight of its own data gravity and egress extortion. True planetary compute requires moving the math to the data, not the data to the math.
- **The 10-Million Node Reality:** The physical limitations of coordinating millions of untrusted, asymmetrical consumer devices (gamers, library desktops, air-gapped corporate sub-swarms).

## VOLUME 1: The Thermodynamic Foundation (Identity & Hardware)
- **1.1 Identity in a Zero-Trust Vacuum:** The Chrysalis Grinder. How `src/swarm/pow_worker.rs` uses SHA-256 and L3 cache saturation (`core_id`, `hardware_seed`) to mathematically neuter ASICs and tie cryptographic identity to physical thermodynamic expenditure.
- **1.2 The Thermal Guillotine:** Biological load shedding. How `src/highestsec/sandbox.rs` uses Wasmtime epoch interruption (`engine.increment_epoch()`) to violently assassinate runaway physics algorithms before they melt citizen silicon.
- **1.3 The PARANOID Micro-Audit:** Defeating virtualized swap-space. Using `std::hint::black_box` to force physical 256MB RAM allocations within 2,500ms, proving hardware latency.

## VOLUME 2: The Topology of Chaos (Kademlia & Gossip)
- **2.1 The Flat DHT Illusion & XOR Density:** How a 10M node swarm prevents RAM exhaustion. Why `MAX_KNOWN_NODES = 10,000` requires statistical Kademlia Density Estimation (prefix-length probability) to accurately project global scale without cloning 10 million objects.
- **2.2 Thermodynamic Gossip Jitter:** Preventing the 460-million packet/sec DDoS. How `src/swarm/gossip.rs` dynamically scales its epidemic heartbeat interval inversely to the swarm size using `rand::thread_rng().gen_range()`.
- **2.3 O(1) Data Gravity (The CPU Spin Death):** Why scanning millions of Kademlia assignments kills the CPU, and how the `DashSet` `unassigned_index` enables instantaneous `O(1)` chunk claiming.

## VOLUME 3: The Execution Membrane (WASM & The Data Plane)
- **3.1 Zero-Footprint WASI Streaming:** Bypassing the 4GB WebAssembly limit. How the `mrb_dataset_stream_read` host-function maps HTTP Range requests into WASM linear memory.
- **3.2 The 64MB Concurrency Cache:** How a 64-core machine executing 64 concurrent physics simulations avoids allocating 4GB of redundant RAM by explicitly sharing a single `Arc<RwLock>` 64MB buffer, reducing S3 egress by 98.4%.
- **3.3 Decoupling the Data Plane (The MPSC RAM Bomb):** Why gossiping 2GB seismic results over Kademlia crashes the daemon. How `WorkEngine` writes results to the local `BlobStore` and gossips a lightweight 32-byte Blake3 hash instead.
- **3.4 Direct S3 Exfiltration (The Funnel Fix):** Preventing the Orchestrator's 2TB NVMe drive from imploding by forcing workers to upload massive results directly to AWS via presigned URLs.

## VOLUME 4: Enterprise Sovereignty (The Air-Gapped Sub-Swarm)
- **4.1 The Corporate Intranet Illusion:** Why standard P2P fails in midsize industries (the 499 hidden workers and the Secretary's MacBook).
- **4.2 The Egress Diode (Phantom Proxy):** How `src/api/gateway.rs` spins up a dedicated `tokio::net::TcpListener` on port 8080, acting as a lightweight HTTP Forward Proxy to feed air-gapped nodes the 1.5TB datalake without exposing the LAN.
- **4.3 Hierarchical Back-Routing:** Overcoming `EHOSTUNREACH`. How internal aggregators wrap their physics results in a `SwarmMessage::EgressRelay` envelope to tunnel securely through the designated Membrane Gateway back to the global Swarm.

## VOLUME 5: The Cryptographic Economy (MMX Settlement)
- **5.1 The "Tax Fraud" Ledger:** Why local SQLite databases are inherently insecure, and how Marabunta mathematically anchors the economy.
- **5.2 Ed25519 Replay Immunity:** How the Orchestrator injects a monotonic `verified_at` UNIX timestamp into the signature payload (`chunk_id:worker:amount:timestamp`), guaranteeing every Norwegian tax receipt is cryptographically distinct and un-forgeable.
- **5.3 The 10M TPS Wall (SQLite Batching):** How `PRAGMA journal_mode=WAL` and `wal_autocheckpoint=1000` combined with an MPSC background writer thread prevents 10,000 parallel settlements from deadlocking the Tokio thread pool.

## ANNEX A: The Developer Experience (IDE Integration & Tooling)
- The Marabunta Compiler Toolchain.
- VSCode / IntelliJ Plugins for seamless WASM deployment.
- CI/CD integration for automated job submission and artifact fetching.

## ANNEX B: The Sovereign Analytics Engine (The Panopticon)
- The GUI architecture and lazy DOM rendering.
- Real-time Sub-Swarm Constellation visualization (WebGL/D3 force-directed topologies).
- Historical MMX ledger insights and compliance auditing.

## ANNEX C: The Planetary Deployment Pipeline (Kubernetes & Bare Metal)
- Daemon auto-updates and configuration management.
- Multi-architecture containerization (Docker, Podman) for academic grids.
- Binary packaging (deb, rpm, msi) for seamless residential installation.