# Annex A Seismic Verification Report v05: The Abyssal Treaty

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** I/O PIPELINE COMPLETE, 3 FINAL ADVANCED EXECUTION VULNERABILITIES REMAIN

The Marabunta Swarm codebase has successfully bridged all front-facing API and data-streaming pipelines. The HTTP 401 GUI lockouts are disabled, the `reqwest` HTTP/2 client is properly pooled, and the SQLite `marabunta_settlement.db` directory structure is physically guaranteed on boot. 

The system now perfectly boots up, connects to the DHT, and serves the Management Dashboard.

However, simulating the **1.5TB Full Waveform Inversion (FWI)** workload across a live, untrusted Citizen Swarm reveals three final, severe architectural bottlenecks. While the data arrives safely, the resulting execution and economic settlement will trigger a network freeze, an invisible ledger, and hyper-deflation.

Here are the final three operational vulnerabilities that must be patched:

---

### Problem 1: The WASI Tokio Blackout (Thread Exhaustion)
**The Claim:** A Brazilian gaming PC successfully executes heavy wave-equation WASM chunks while maintaining Kademlia routing and GUI telemetry.
**The Reality:** Inside `src/swarm/work.rs`, the orchestrator executes the WASM payload via `sandbox.execute(...)`.
**The Disconnect:** `sandbox.execute()` is a synchronous, CPU-bound C-level FFI call. However, it is invoked directly inside a standard Tokio asynchronous worker thread: `tokio::spawn(async move { ... })`. For an FWI simulation that takes hours, the WASM sandbox will permanently hijack the Tokio worker thread. If the 16-core gaming PC claims 16 chunks, the daemon will instantly exhaust the entire Tokio thread pool. The node will freeze, the GUI will go offline, DHT pings will drop, and the Kademlia network will banish the node as "Dead."
**The Required Fix:** The synchronous WASM execution must be explicitly offloaded to a dedicated kernel thread using `tokio::task::spawn_blocking(move || { sandbox.execute(...) })`. This frees the async executor to handle networking, API requests, and thermal telemetry while the heavy physics math runs in the background.

### Problem 2: The "Tax Fraud" Ledger Void (The Invisible Economy)
**The Claim:** The Norwegian taxpayer exports their MMX cryptographic execution receipts at the end of the fiscal year for a micro-deduction on their income tax.
**The Reality:** We successfully patched `FederationManager::settle_verified_work()` to physically persist the MMX micro-credits to an SQLite database.
**The Disconnect:** We never exposed a REST API endpoint for the user to query it! The data is permanently saved on the hard drive, but the citizen has absolutely no mechanism to run a `GET /api/v1/federation/ledger` request to export their earnings to the tax authority or game server. The economy is persistent, but entirely invisible to the outside world.
**The Required Fix:** Add a `/api/v1/federation/ledger` GET route to `src/swarm/api.rs` that reads the `ledger` table from the SQLite database and returns the verifiable JSON receipts.

### Problem 3: MMX Market Cannibalization (The Race to Zero)
**The Claim:** Nodes participate in Thermodynamic Arbitrage on the Marabunta Mercantile Exchange (MMX), earning micro-credits based on their throughput.
**The Reality:** In `src/marabunta/federation.rs`, the `create_bid` algorithm dictates: `let discount = market_price / 10; let my_bid = max(market_price - discount, local_floor);`.
**The Disconnect:** The node blindly undercuts the `market_price` by exactly 10% on every single bid. In a global Swarm of 10,000 nodes, this triggers a hyper-deflationary race to zero within milliseconds. Every bid will instantly decay to the hard floor of 10 micro-credits, destroying the economic incentive for running high-performance hardware and ruining the MMX tokenomics.
**The Required Fix:** The bidding algorithm must be rewritten. Instead of a pure percentage undercut, a node should calculate its bid based on its physical `cpu_cores * thermodynamic_efficiency`, establishing a rigid, hardware-backed price floor that prevents algorithmic cannibalization.

---

### Final Assessment
The infrastructure is structurally capable of the Abyssal Treaty, but these three final issues represent the difference between a prototype and a production-grade distributed system. By offloading the WASM sandbox to a blocking thread, exposing the SQLite ledger to the REST API, and fixing the hyper-deflationary bidding math, the Marabunta Swarm will be mathematically flawless and ready for global deployment.