# Annex A (v503): Planetary-Scale Execution Analysis
## The 10-Million-Node 350PB Seismic Wave-Equation Workload

### Abstract
A deep architectural inspection of the Marabunta `WorkEngine`, `KnowledgeStore`, and `GossipEngine` was conducted against the physical constraints of a 10-million-node swarm (incorporating Brazilian Residential CGNATs, UK Academic DPI Firewalls, and Norwegian Settlement instances). 

While the recent Orchestration decoupling (Data, Telemetry, Verification, Settlement Sinks) physically enables the theoretical workflow, attempting to run the 1,000,000-chunk workload *as currently implemented* will result in total systemic collapse of the Orchestrator and the Membrane Gateways within the first 60 seconds.

### Apocalyptic Bottlenecks Discovered

#### 1. The OOM Kademlia Asphyxiation (The WASM Thundering Herd)
**Location:** `src/swarm/mod.rs` (FetchBlob/PushBlob Handler)
**The Problem:** When the 1,000,000 chunks are generated, edge nodes immediately emit `SwarmMessage::FetchBlob` to the Orchestrator to download the 20MB WASM binary. The Orchestrator responds via `tokio::spawn` by reading the 20MB payload and cloning it into a `SwarmMessage::PushBlob`. 
If 10,000 Brazilian edge nodes (0.1% of the swarm) request the blob concurrently, the Orchestrator instantly attempts to allocate **200 Gigabytes of RAM** (`10,000 x 20MB`). The Linux OOM Killer will immediately terminate the Marabunta process.

#### 2. The Gossip Entropy Trap (The Pheromone Dilution)
**Location:** `src/swarm/gossip.rs` & `src/swarm/config.rs`
**The Problem:** The Orchestrator generates 1,000,000 `Assignment` structs (status: `Pending`). The `GossipEngine` broadcasts a random sample of `MAX_ASSIGNMENTS_PER_MESSAGE` (100) every 5 seconds. 
Mathematically, it will take the Orchestrator over 13 hours just to broadcast the existence of every chunk *once*. The edge nodes will never find the work because the `Assignment` space is too diluted. The Kademlia Random Walk mathematically fails for job sizes exceeding 50,000 chunks.

#### 3. The SQLite I/O Deadlock (The Event Horizon)
**Location:** `src/swarm/knowledge.rs` (`persist_assignment`)
**The Problem:** The `SqlitePersistence` layer uses a synchronous `Mutex<rusqlite::Connection>`. Every time an edge node claims a chunk or completes a chunk, the Orchestrator locks the SQLite database and performs a disk write. 
With 1,000,000 chunks traversing 4 state changes, that is 4,000,000 disk writes. Even with WAL enabled, a swarm of this size will generate 5,000+ status updates per second. The sync Mutex will block the Tokio worker threads, causing Kademlia network reads to time out, triggering false "Node Dead" cascades across the entire Kademlia routing table.

#### 4. The Membrane Gateway Panic (The UK Academic Funnel)
**Location:** `src/swarm/mod.rs` (`SwarmMessage::EgressRelay`)
**The Problem:** UK nodes behind strict DPI firewalls rely on `EgressRelay` to push their 5MB output tensors to a gateway Enterprise node. The gateway blindly shoves these relayed messages into its `outbound_tx` channel. If 2,000 UK nodes finish their chunk simultaneously, the gateway attempts to stuff 10GB of memory into an async channel, causing the `CanRelay` Enterprise nodes to memory-panic and drop offline, permanently severing the UK sub-swarm.

---

### Required Architectural Patches (Do we fix these?)

1. **Blob Backpressure (`Arc<Vec<u8>>` + Semaphore):** We must stop cloning `PushBlob` payloads. The WASM payload must be held in an `Arc`, and the Orchestrator must employ a strict `tokio::sync::Semaphore(50)` to limit concurrent outbound binary streams, forcing edge nodes to wait or fetch from secondary seeds.
2. **"Pull" Work Stealing (The Queen Ant Method):** We must stop gossiping `Assignment`s globally. We should only gossip `SwarmJobInfo`. Edge nodes see the job, realize they match the topology, and send a direct `RequestWorkBatch(JobId)` RPC to the Orchestrator. The Orchestrator hands them 5 chunks dynamically.
3. **Async SQLite WAL Channel:** Move all SQLite disk writes to an unbounded `tokio::sync::mpsc` channel processed by a dedicated, single background thread using `BEGIN TRANSACTION; ... COMMIT;` batches every 500ms.
4. **Membrane Rate-Limiting:** Membrane Gateways must actively inspect their channel capacity. If it exceeds 80%, they must respond to edge nodes with a `RelayCapacityExceeded` control message, forcing the edge node to exponential backoff.
