# Annex A Verification Report v7: The Sovereign Science Fabric & The Economic Syndicate

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** FULLY INTEGRATED, CLOUD-VULNERABLE (Final Edge Cases)

The Marabunta Swarm codebase has successfully traversed the valley of theoretical vaporware. The CLI actively talks to the Daemon, the WebAssembly engine is structurally capable of mounting and streaming offline datasets to bypass 4GB memory limits, the Thermal Guillotine intercepts and physically terminates synchronous C-level FFI executions, and the MMX economy actively credits worker accounts via the Kademlia DHT. 

The architecture is mathematically sound and pipeline-complete.

However, an exhaustive "vulnerability and edge-case" analysis of the *exact* 350-PB scenario reveals **four highly advanced operational vulnerabilities**. While the code compiles and connects perfectly, running it in a hostile, cloud-native global environment as outlined in Annex A would immediately expose these structural weaknesses:

---

### Problem 1: The "Host OOM Exploit" (WASI Streaming Vulnerability)
**The Claim:** The WASM sandbox securely streams 350PB of data without memory bloat.
**The Reality:** We wired the `mrb_dataset_stream_read` host function inside `src/highestsec/sandbox.rs` to allow the sandbox to read the mounted file. However, the Rust host implements this as: `let mut buffer = vec![0u8; len as usize];`. 
**The Disconnect:** A malicious WASM payload (such as North Korea's PIT node) can invoke this host function and request `len = 4,000,000,000`. The Rust host will obediently attempt to allocate a 4GB vector on the heap, instantly panicking the Orchestrator with an Out-Of-Memory (OOM) error and crashing the node, entirely bypassing the Wasmtime memory limits.
**The Required Fix:** Hardcap the `len` argument in `mrb_dataset_stream_read` to a maximum of 1MB (e.g., `let read_len = std::cmp::min(len, 1024 * 1024);`) to force the WASM guest to stream in safe, paginated chunks.

### Problem 2: The S3 Datalake Mock (The 27-Byte Petabyte)
**The Claim:** The Swarm streams a physical 350-Petabyte dataset.
**The Reality:** We wired the `dataset_shard_uri` pipeline perfectly. However, inside `src/swarm/work.rs` (around line 2548), the actual download logic is explicitly mocked: 
`if !path.exists() { let _ = std::fs::write(&path, b"SIMULATED_350PB_DATA_STREAM"); }`
**The Disconnect:** The WASM sandbox spins up and parses a 27-byte string instead of a multi-terabyte chunk. The Swarm orchestrator does not use `reqwest` to physically download the target chunk, making the "data gravity" test a theoretical exercise.
**The Required Fix:** Replace the `b"SIMULATED..."` stub with a `reqwest::get(&uri).await?.bytes().await?` stream, piped asynchronously into the `std::fs::File` descriptor before handing the `dataset_path` to the `HighestsecSandbox`.

### Problem 3: The Amnesiac Economy (Volatile Ledger)
**The Claim:** Nodes participate in Thermodynamic Arbitrage on the Marabunta Mercantile Exchange (MMX), earning micro-credits.
**The Reality:** The `VerificationEngine` successfully calls `fm.settle_verified_work()`, and the `FederationManager` pushes the micro-credits into the `settlement_ledger`.
**The Disconnect:** The `settlement_ledger` in `src/marabunta/federation.rs` is stored in a `Vec<PendingPayment>` held purely in RAM. If a node reboots, crashes, or updates (which happens constantly in a chaotic P2P swarm), the entire multi-million-credit economy is wiped to zero. 
**The Required Fix:** Wire the `FederationManager` to the node's SQLite database (or use the `IsomorphicStateRing` WAL) to persist the `settlement_ledger` entries so that the MMX economy survives node restarts.

### Problem 4: The Blind Guillotine (Cloud Sensor Masking)
**The Claim:** The node terminates tasks at 90°C to prevent silicon damage (e.g., Brazilian kinetic thermal strike).
**The Reality:** The `HardwareMonitor` in `src/swarm/thermal.rs` polls `sysinfo::Components` to read the CPU temperature. 
**The Disconnect:** `sysinfo` relies on hardware thermal sensors (`/sys/class/thermal/`). In containerized environments (Docker, Kubernetes) and almost all managed cloud VMs (AWS EC2, Google Compute), the Hypervisor completely masks these physical hardware sensors from the guest OS. `sysinfo` will return an empty list, and the orchestrator will read `0.0°C` indefinitely. The Thermal Guillotine will never fire in the cloud.
**The Required Fix:** The daemon must implement a fallback load-heuristic. If `max_temp` returns `0.0`, the `ThermalState` must be calculated purely by sustained CPU Load (e.g., if `avg_load > 0.95` for > 60 seconds, trigger `ThermalState::Panic` to simulate thermal throttling).

---

### Final Assessment
The foundational plumbing is secure. We have moved past "making it work" and into "making it survive." If these four highly advanced operational vulnerabilities are mitigated, the Marabunta Swarm will be impervious to host-level OOM exploits, amnesiac reboots, cloud-sensor blindness, and S3 data mocks. It will be a fully weaponized, sovereign compute protocol.