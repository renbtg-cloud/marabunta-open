# Analysis of ANNEX A: The Sovereign Science Fabric (10 Million+ Nodes)

This document provides a deep-dive architectural analysis of "ANNEX A" operating at an ultra-massive scale (10,000,000+ distributed nodes), specifically focusing on the integration of Brazil's Gamer Grid, Norway's Civic Tax Swarm, and the UK's Night-Shift Academic Net.

While the fundamental peer-to-peer WASM and Kademlia theories hold up, analyzing the `marabunta-core` codebase reveals catastrophic bottlenecks and memory leaks that will physically crash the orchestrators and render any UI/GUI completely unusable at this scale.

## Problem 1: The GUI / Control Plane OOM Bomb
**Location:** `src/control_plane/metrics_collector.rs` and `src/infrastructure/registry.rs`

When a user opens the web Dashboard or the TUI and requests the list of nodes, the API calls `DashboardDataSource::get_nodes()`. This method constructs a `NodeQuery` but critically **fails to apply a limit** before sending it to the registry:
```rust
let nodes = self.registry.query(&query).await;
nodes.into_iter().skip(...).take(...)
```
Inside `registry.query()`, the system iterates over the `HashMap<NodeId, InfrastructureNode>` and executes `.cloned().collect()`. 

**The Impact:** At 10 million active nodes (e.g., millions of Brazilian gamers and Norwegian tax nodes online), a single HTTP GET request to `/api/nodes` forces the Rust backend to clone 10 million heavy `InfrastructureNode` structs into a massive `Vec`. This instantly allocates 10-20GB of RAM per request, while blocking the async executor (`tokio::task::block_in_place`), completely freezing the Orchestrator and crashing it with an Out-Of-Memory (OOM) panic. The GUI will never load.

## Problem 2: The UI WebSocket Tsunami
**Location:** `src/control_plane/websocket.rs`

The Control Plane uses WebSockets to stream live events (`DashboardEvent::NodeStatusChanged`, `NodeMetricsUpdate`) to connected GUI clients. 
At 8:00 PM UK time, hundreds of thousands of "Night-Shift Academic Net" library desktops and Raspberry Pis wake up and connect to the Kademlia DHT. At 8:00 AM, they all disconnect.

**The Impact:** The Orchestrator will attempt to broadcast 500,000+ JSON payload status changes per second over the WebSocket connections. Even if the Rust backend's broadcast channels survive, the connected Web browsers running the Dashboard will instantly crash. The DOM/JavaScript thread cannot render 500,000 live DOM updates per second.

## Problem 3: The "Tax Fraud Fix" Amnesiac RAM Leak
**Location:** `src/marabunta/federation.rs`

To ensure Norwegian citizens receive their tax deductions, the codebase implements persistent SQLite execution receipts. A comment explicitly states: `// 🛑 TAX AUDIT FIX: Explicitly query the SQLite database with filters ... prevents the RAM leak.`

However, inside `pub fn settle_verified_work()`, the developer left this line intact:
```rust
self.settlement_ledger.push(PendingPayment { ... });
```
**The Impact:** Every single time a WASM chunk is verified and paid (millions of times a day across the Swarm), a `PendingPayment` is pushed to the `settlement_ledger` `Vec`. This vector is **never cleared**. The Orchestrator will bleed hundreds of megabytes of RAM per hour until it is killed by the OS OOM killer. The "RAM leak fix" was documented but never fully implemented.

## Problem 4: SQLite WAL Mode Throughput Collapse
**Location:** `src/marabunta/federation.rs`

The persistent settlement ledger uses a Tokio MPSC channel (`tokio::sync::mpsc::channel::<SettlementTask>(10_000)`) connected to a single background blocking thread that batches up to 100 SQLite inserts per transaction.

**The Impact:** 10 million nodes processing 350-Petabyte Monte Carlo fluid dynamics (The Abyssal Treaty) will generate tens of thousands of chunk settlements per second. A single SQLite writer thread batching 100 records simply cannot keep up with 50,000+ IOPS. The 10,000-item MPSC channel will instantly hit capacity. Once full, the Tokio runtime will either block (halting all new work assignments) or drop execution receipts, enraging the Brazilian Gamers who will lose their MMX micro-credits and cosmetic skins.

## Problem 5: The "PARANOID" UK Raspberry Pi Slaughter
**Location:** `marabunta_bible.pdf` / `src/swarm/verification.rs`

The FWI (Full Waveform Inversion) simulation demands a `verify_mode: "PARANOID"`. To prevent script kiddies with slow HDDs from bottlenecking the 350PB pipeline, the Orchestrator forces bidding nodes to "allocate a 256MB matrix, execute a rapid deterministic transformation, and return the cryptographic hash within 2500ms."

**The Impact:** The UK Academic Net relies on "library desktops and Raspberry Pi lab clusters." The vast majority of older library PCs and almost all Raspberry Pis cannot pass a brutal 256MB matrix manipulation within a hard 2500ms real-time limit. The `VerificationEngine` will instantly reject their bids, drop their reputation scores, and eventually apply `Badge::Malicious` to the entire UK Academic Net, blacklisting the universities from the swarm entirely.
