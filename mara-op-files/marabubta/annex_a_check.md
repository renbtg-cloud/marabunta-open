# Annex A Verification Report: The Sovereign Science Fabric & The Economic Syndicate
**Target Document:** `marabunta_bible/14_annex_a.md` (Post-LLM/DiLoCo Revision)
**Claim:** A decentralized, trustless, high-performance compute fabric capable of executing a 350-Petabyte Monte Carlo fluid dynamics simulation across 6 geopolitically hostile datacenters. It relies on WASM determinism, memory-hard Proof-of-Work, thermal load shedding, and the Marabunta Mercantile Exchange (MMX) for thermodynamic arbitrage.

Here is the exhaustive reality of how well these claims are actually implemented in the codebase.

---

### 1. The Crucible: Kademlia Genesis & ChrysalisGrinder PoW
**Status: 100% Functionally Honest**

Annex A claims that the US Eclipse attack was defeated because creating 50,000 Sybil identities required brute-forcing the `ChrysalisGrinder` PoW engine, which saturated the physical L3 cache and neutralized GPU advantages.
*   **The Reality:** This is physically accurate. We specifically fixed `src/swarm/pow_worker.rs` to execute a 32MB read-modify-write scratchpad traversal and mathematically XOR the accumulator back into the random seed *before* checking the 26-bit difficulty constraint. The NodeId generation is genuinely bottlenecked by RAM latency. A hostile actor attempting to mint 50,000 identities will face immense thermodynamic and temporal costs.

### 2. The Fast Path: DatacenterLocal Routing
**Status: Functionally Honest**

Annex A claims the Swarm detects `< 2ms` latency between intra-datacenter nodes and elevates the connection to `DatacenterLocal` routing, prioritizing bandwidth.
*   **The Reality:** The codebase successfully implements this. `src/swarm/transport/mod.rs` correctly pings peers and classifies sub-2ms connections as `DatacenterLocal`. While it no longer lies about bypassing the Linux kernel with fake InfiniBand/RDMA drivers (we explicitly stubbed `rdma.rs`), the orchestrator *does* accurately prioritize these low-latency TCP routes for chunk distribution.

### 3. Execution Verification: WASM Determinism vs. North Korea
**Status: Mechanically Sound, Economically Hollow**

Annex A claims that when the Pyongyang node altered its hypervisor memory to inject a bias, the output's SHA-256 hash diverged from the deterministic consensus, instantly triggering a BFT Ledger rejection, blacklisting the node, and slashing its stake.
*   **The Reality:** The `wasm_executor.rs` and the `MantisJournal` do, in fact, execute WASM binaries in strict sandboxes and cryptographically hash the outputs. However, the *punishment* phase is simulated. We completely gutted the BFT Hashgraph (`src/swarm/planetary/ledger.rs`) because it was an infinite 3-second timeout loop that couldn't communicate with the network. The orchestrator will detect the hash mismatch, but there is no global blockchain ledger to "slash their stake" or permanently blacklist the `NodeId` across the Swarm. The network simply rejects the corrupted payload and moves on.

### 4. The Kinetic Thermal Strike: User-Space Load Shedding
**Status: Safely Stubbed (But Exaggerated)**

Annex A claims that when the Brazilian nodes hit 95°C during a DDoS attack, the user-space Thermal Guardian instantly bypassed standard scheduling and issued ruthless `SIGKILL` commands to the malicious WASM payloads.
*   **The Reality:** We dismantled the fake Ring-0 eBPF kernel preemption in `src/marabunta/bpf_loader.rs`. The node relies entirely on the user-space `src/swarm/thermal.rs` poller via `sysinfo`. While it correctly reads CPU temperatures and broadcasts `ThermalState::Critical` events to the `EventBus`, there is no physical `SIGKILL` dispatched to the underlying `wasmtime` PIDs. It merely stops accepting new jobs. The hardware is protected from accepting more load, but it doesn't ruthlessly assassinate active processes. 

### 5. Thermodynamic Arbitrage: The MMX Spot Market Hunt
**Status: Half-Baked (Bidding Works, Migration Does Not)**

Annex A claims the Swarm dynamically routes auxiliary WASM payloads to Texas when solar grids overproduce, and hot-migrates memory footprints to Iceland when prices spike.
*   **The Reality:** We successfully fixed the bidding algorithm in `src/marabunta/federation.rs`. Nodes now intelligently read their physical core count and refuse to bid below their configured `min_bid_price_cents`. They will naturally sit out of unviable auctions (like Texas nodes turning down work when electricity spikes). However, the "hot-migration of memory footprints to Reykjavik" is pure fiction. There is no code to suspend an active WASM runtime, serialize its stack memory to a `.mrb-dump`, and resume it on another continent. If a node shuts down mid-execution due to economics, the job simply fails and the orchestrator must re-run the chunk from scratch elsewhere.

### 6. The Management GUIs & Dashboards
**Status: Visually Spectacular Facades (UI/UX Theatre)**

The repository includes three separate user interfaces: `/dashboard/`, `/job-ui/`, and `/management-ui/`. The source code (`management-ui/app.js`) claims to be a "Datadog/Grafana-tier management dashboard" built in pure ES2022 with zero dependencies.
*   **The Reality:** The frontend code is incredibly well-written, responsive, and visually complex (featuring 7-facet "Psyche" visualizations, Constellation maps, and SLA tracking). However, it is largely wired up to non-existent or mocked backend endpoints.
*   **The Disconnect:** The UI's `DataManager` attempts to fetch data from endpoints like `/api/v1/psyche`, `/api/v1/capacity/forecast`, and `/api/v1/timetravel`. A review of the Axum router in `src/swarm/api.rs` reveals that while basic endpoints (`/nodes`, `/jobs`, `/blobs`, `/events`) are physically implemented and return real data, the advanced analytics (Psyche, Forecasts, Bottlenecks) are either missing entirely from the router or exist merely as background functions (`src/swarm/ctl.rs`) that are never exposed via Axum. The UI is a beautiful Hollywood set built on top of a foundational, but much simpler, backend API.

---

### Final Verdict
The new Annex A accurately describes the physical routing and cryptographic limits of the current Marabunta codebase. The Monte Carlo execution, the memory-hard Chrysalis PoW, and the low-latency Datacenter mapping are **real and functional.**

However, the **economic consequences** (slashing stakes via BFT), the **ruthless thermal process assassination**, and the **hot-migration of active memory** remain aspirational science fiction. The UIs are stunning but fundamentally disconnected from the backend router for all advanced "Psyche" telemetry.