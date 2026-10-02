# Deep Technical Execution: Annex A (The Sovereign Science Fabric)

This document provides a step-by-step, code-level breakdown of how the Marabunta Swarm physically executes the 350-Petabyte Monte Carlo fluid dynamics scenario described in Annex A. It details the exact APIs, Rust structs, and network messages used, identifies where the current implementation would hit physical limits, and provides the architectural assumptions required to proceed.

---

## Phase 1: Genesis & The Trust Equilibrium

**The Scenario:** CERN initiates the Swarm. Max Planck, Oak Ridge, PIT, GCHQ, and Sirius join.
**The Command:** `mrb swarm init-wolfpack --purpose "GlobalFluidDynamicsSim_v4" --threshold 5-of-6`

### Code-Level Execution
1.  **CLI Parsing:** The `marabunta-cli` binary parses the command and issues an HTTP POST to the local Daemon at `/api/v1/jobs` (or a dedicated federation endpoint).
2.  **Identity Generation:** The node initializes the `ChrysalisGrinder` (`src/swarm/pow_worker.rs`). 
    *   It allocates a 32MB L3 cache scratchpad. 
    *   It runs a 64-iteration read-modify-write loop, bounding the thread to memory latency.
    *   It XORs the resulting `accumulator` into the UUID seed bytes until `NodeId::meets_chrysalis_pow()` returns true (26 leading zero-equivalent bits).
3.  **Topology Mapping:** As nodes connect, `SwarmTransport::process_inbound_stream` (`src/swarm/transport/mod.rs`) measures the TCP handshake latency. 
    *   CERN-to-CERN (<2ms) is flagged as `DatacenterLocal`.
    *   CERN-to-Sirius (120ms) defaults to standard Kademlia UDP/QUIC routing.

**Potential Breakage:** The CLI currently lacks the exact `init-wolfpack` subcommand arguments to seamlessly translate into a `WolfPackCoalition` struct.
**The Fix:** We assume the CLI parser routes the `--threshold` flag to the `FederationManager::create_coalition()` API, generating the required `SwarmMessage::WolfPackProposal`.

---

## Phase 2: Dispatching the 350PB Job

**The Scenario:** The Monte Carlo WASM binary and 350PB of data are dispatched to the swarm.
**The JCL Manifest:** `verify_mode: "PARANOID"`

### Code-Level Execution
1.  **Job Submission:** The `WorkEngine` registers a `SwarmJobInfo` struct. 
2.  **Bidding (MMX):** The `FederationManager::create_bid` (`src/marabunta/federation.rs`) executes on every node. 
    *   It queries `num_cpus::get()` to calculate `throughput_guarantee`.
    *   It checks `market_price` against the local `config.toml` minimum floor.
3.  **Claiming Work:** `WorkEngine::try_claim_work` evaluates the bids. Because `verify_mode` is `PARANOID`, the Orchestrator forces nodes to run a memory-latency trap before assigning chunks.

**Potential Breakage:** Pushing 350PB of data through `SwarmMessage::PushBlob` will instantly exhaust the disk space of the Orchestrator and crash the Swarm. WebAssembly (wasm32) also has a 4GB linear memory limit.
**The Fix:** The job is submitted not as a massive blob, but with a `dataset_shard_uri`. The `wasm_executor.rs` uses WASI host-calls (`MantisJournal`) to stream the fluid dynamics data in 10MB overlapping windows directly from an S3-compatible datalake, bypassing the 4GB WASM limit and preventing local disk exhaustion.

---

## Phase 3: The North Korean Data Poisoning Attempt

**The Scenario:** PIT modifies hypervisor memory to skew the simulation.
**The Vector:** Deterministic Hash Mismatch.

### Code-Level Execution
1.  **Execution:** PIT runs the WASM chunk. It intercepts the WASI memory array and injects a bias.
2.  **Result Submission:** PIT sends a `ChunkResult` back to the Orchestrator. Because the memory was altered, the resulting `output` byte array hashes to `0xBAD...`.
3.  **Verification (`src/swarm/verification.rs`):** The Orchestrator's `VerificationEngine::submit_result` routes the chunk to a `ReplicaSet`.
4.  **Consensus Failure:** The `MajorityConsensus` strategy compares PIT's hash against CERN and Oak Ridge. PIT is flagged as an `outlier_node`.
5.  **The Local Slashing:** The engine triggers `self.slash_node(&pit_node_id)`:
    *   `self.store.reset_trust()` zeroes internal verification metrics.
    *   `self.reputation.slash_score()` sets the global score to 0.0 and applies `Badge::Malicious`.
    *   `self.knowledge.update_node_status()` sets PIT to `NodeStatus::Dead`.
6.  **Result:** The Kademlia routing table severs all ties with PIT. The bad chunk is dropped.

**Potential Breakage:** None. This mechanism is physically implemented, mathematically sound, and fully wired.

---

## Phase 4: The American Topological Eclipse

**The Scenario:** Oak Ridge attempts to flood the network with 50,000 Sybil nodes to eclipse the DHT.
**The Vector:** Brute-forcing the `ChrysalisGrinder`.

### Code-Level Execution
1.  **The Grind:** Oak Ridge spins up 20,000 cores. Because the `ChrysalisGrinder` enforces the 32MB L3 cache latency loop, GPU acceleration is useless. It takes Oak Ridge ~10 seconds per identity, slowing the Sybil attack to a crawl.
2.  **The Reputation Gate:** The newly minted identities connect. They are stamped with `Badge::Newcomer`.
3.  **The Block:** `VerificationEngine::SpotCheckDecider` sees `Badge::Newcomer` (which carries a `-0.05` reputation weight in `src/swarm/strategy.rs`). The Orchestrator refuses to trust them for `MajorityConsensus` on chubby data, rendering the Eclipse useless for data manipulation.

**Potential Breakage:** Oak Ridge could still theoretically exhaust the Orchestrator's TCP connection pool by opening 50,000 simultaneous sockets.
**The Fix:** We assume `src/swarm/transport/mod.rs` implements standard IP-based rate limiting and concurrent connection bounding per `/24` subnet.

---

## Phase 5: The Kinetic Thermal Strike (Brazil)

**The Scenario:** DDoS attack on Sirius/LNLS pushes hardware to 95°C.
**The Vector:** Thermodynamic Arbitrage & The Thermal Guillotine.

### Code-Level Execution
1.  **Polling:** `HardwareMonitor::run_daemon` (`src/swarm/thermal.rs`) queries `sysinfo`.
2.  **80°C (Warning):** The CPU hits 80°C. 
    *   `transition_state` detects `ThermalState::Warning`.
    *   It triggers `work_engine.yield_active_load()`.
    *   The node extracts all active `Chunk` payloads and broadcasts `SwarmMessage::YieldChunk` to 3 neighbors.
    *   The node calls `.abort()` on the local Tokio `JoinHandle`s.
3.  **The Re-queue:** The Orchestrator receives `YieldChunk`. `KnowledgeStore::handle_chunk_yield()` places the chunks at the very front of the `pending_chunks` VecDeque.
4.  **90°C (Panic - The Guillotine):** If the temperature continues to rise (e.g., ambient AC failure), `transition_state` hits `ThermalState::Panic`.
    *   It triggers `work_engine.shed_active_load()`.
    *   It iterates the `task_handles` Mutex and brutally executes `.abort()` on any remaining WASM threads, preventing physical silicon damage.

**Potential Breakage:** None. The graceful yield and the ruthless guillotine are fully wired.

---

## Phase 6: The Psyche Dashboard (Human Overwatch)

**The Scenario:** Operators monitor the chaos via the Management UI.
**The Vector:** The `PsycheCalculator` API.

### Code-Level Execution
1.  **The Frontend:** `management-ui/app.js` runs a `setInterval` calling `fetch('/api/v1/psyche/breakdown')`.
2.  **The Backend:** `src/swarm/api.rs` receives the GET request and routes it to `get_psyche_breakdown()`.
3.  **The ML Model:** The `PsycheCalculator` (`src/swarm/psyche.rs`) processes the recent chaos:
    *   `EventBus` processed the `ThermalState::Warning` and the `Malicious` badge assignment.
    *   The calculator updates the sliding windows.
    *   **Cohesion** drops because a node (PIT) acted maliciously.
    *   **Efficiency** drops temporarily because chunks were yielded from Brazil.
    *   **Resilience** spikes because the swarm successfully re-queued the yielded chunks and slashed the attacker without crashing.
4.  **Inference:** The ONNX Tensor (`tract-onnx`) evaluates the 7 facets and predicts a 12% probability of a cascading partition. The JSON is serialized and rendered on the Grafana-tier UI.

**Potential Breakage:** None. The API plumbing is fully connected, and the ONNX model evaluates the live dashboard data in real-time.