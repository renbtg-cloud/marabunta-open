# Annex A (v600): Planetary-Scale Execution Analysis
## The 10-Million-Node 350PB Seismic Wave-Equation Workload

### Abstract
Following the implementation of the apocalyptic survival patches in v503 (Blob Semaphore, Async SQLite, Queen Ant Work Stealing, Membrane Backpressure), a subsequent architectural inspection of the `WorkEngine` and `GossipEngine` was conducted.

While the Orchestrator is now immune to OOM panics and I/O deadlocks, the shift to "Queen Ant" Work Stealing broke the edge node execution pipeline. The edge nodes are structurally starving for work because their local polling loop still assumes the Orchestrator is globally broadcasting `Pending` Kademlia assignments.

### Apocalyptic Bottlenecks Discovered

#### 1. The Edge Node Starvation Trap (Queen Ant Desync)
**Location:** `src/swarm/work.rs` (`try_claim_work`)
**The Problem:** The Orchestrator stopped gossiping `Assignment`s globally (to fix the Pheromone Dilution trap). It now relies on Edge nodes to "pull" chunks using `RequestWorkBatch`.
However, Edge nodes` `try_claim_work` loop still strictly looks for local chunks in the `Pending` state via `get_unassigned_chunks()`. Because they never receive `Pending` chunks over Gossip, the Edge nodes assume the swarm is idle. They never emit `RequestWorkBatch`, and the 1,000,000 chunks sit on the Orchestrator forever. 
Furthermore, if an Edge node *did* emit `RequestWorkBatch`, the Orchestrator replies with `WorkBatchResponse` containing `InProgress` assignments. The Edge node would merge these into its local DHT, but because they are `InProgress`, `try_claim_work` skips them completely, resulting in zombie chunks.

#### 2. The Identity Handshake Deadlock (Transport Connection Pools)
**Location:** `src/swarm/transport/mod.rs` & `src/swarm/work.rs`
**The Problem:** At 10 million nodes, if 10,000 nodes simultaneously ping the Orchestrator for `RequestWorkBatch`, the Orchestrator creates 10,000 pooled connections. The `CONNECTION_IDLE_TIMEOUT` is hardcoded, meaning these sockets will consume TCP state for minutes even though `RequestWorkBatch` is a single RPC. This isn`t file-descriptor exhaustion (we have a limit), but it *is* an FD starvation issue. The Orchestrator will aggressively evict active connections to handle new ones, dropping legitimate `PushBlob` chunks mid-flight.

---

### Required Architectural Patches (Do we fix these?)

1. **Queen Ant Active Polling (Edge Node Fix):** Rewrite `WorkEngine::try_claim_work` to iterate over local `SwarmJobInfo` (the Pheromone). If the Edge node matches the Job`s `Topology` and `Requirements`, it explicitly sends `RequestWorkBatch` to the Orchestrator. 
2. **Pre-Assigned Execution (Edge Node Fix):** Add a phase to `try_claim_work` where the Edge node scans its local `KnowledgeStore` for `Assignment`s where `assigned_to == self_id` and `status == InProgress`, bypassing the legacy `Pending -> InProgress` state transition. It immediately starts executing these.
3. **Transport Stream Prioritization:** Modify `SwarmTransport::send_many` to ensure heavy/vital connections (`PushBlob`, `FetchBlob`, `ChunkResult`) are pinned and exempt from the `enforce_pool_limit` LRU eviction, so the Orchestrator doesn`t drop a 20MB WASM transfer to service a lightweight Kademlia ping.
