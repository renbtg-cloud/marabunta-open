# Annex A Seismic Verification Report v01: The Abyssal Treaty

**Target Document:** `marabunta_bible/14_annex_a.md` (The Deep-Sea Seismic Inversion Scenario)
**Status:** ARCHITECTURALLY ELEGANT, 5 CRITICAL EDGE-CASES REMAIN

The Marabunta Swarm codebase has been successfully repointed to the new, highly realistic "Abyssal Treaty" scenario. The core mechanics—Kademlia broadcasting, WASI host-calling, Thermal Guillotines, and MMX Settlement—are structurally implemented.

However, an exhaustive, all-inclusive execution trace of the specific 1.5TB Pre-Salt FWI scenario across a Sovereign Citizen Science Fabric (Gamers, Library PCs, and Civic Tax Swarms) reveals **five critical disconnects and physical vulnerabilities**.

If Petrobras or USP were to launch this job today, the network would collapse due to the following unpatched edge cases:

---

### Problem 1: The "PARANOID" Missing Link (The Micro-Audit Void)
**The Claim:** The JCL specifies `verify_mode: "PARANOID"`. The orchestrator forces nodes to execute a 256MB memory-latency trap within 2500ms before assigning the heavy FWI chunks, rejecting laptops that swap to disk.
**The Reality:** The `PARANOID` micro-audit logic does not exist in `src/swarm/work.rs`. `WorkEngine::try_claim_work` blindly trusts the `active_capacity` reported by the node. 
**The Consequence:** A UK library desktop with 8GB of RAM and a spinning hard drive will successfully bid on the FWI chunk. It will immediately begin swapping pages to disk, stalling the entire global simulation indefinitely.
**The Fix Required:** Inject the 256MB memory-allocation loop timer into `try_claim_work` specifically when the job's verification strategy matches the `PARANOID` string.

### Problem 2: The S3 Datalake Mock (The 1.5TB Illusion)
**The Claim:** The Swarm streams a physical 1.5-Terabyte `.segy` seismic dataset from the USP SAN.
**The Reality:** We wired the `dataset_shard_uri` pipeline perfectly. However, inside `src/swarm/work.rs`, the orchestrator explicitly mocks the data: 
`if !path.exists() { let _ = std::fs::write(&path, b"SIMULATED_350PB_DATA_STREAM"); }`
**The Consequence:** The WASM sandbox spins up and parses a 27-byte string instead of a multi-terabyte wave equation matrix. The "Data Gravity" streaming bypass is currently a theoretical exercise.
**The Fix Required:** We must replace the hardcoded string with a generalized storage resolver. If the URI is `file://`, map the local file descriptor directly to the sandbox. If `http://` or `s3://`, open an asynchronous chunked stream.

### Problem 3: The "Host OOM Exploit" (WASI Sandbox Escape)
**The Claim:** The WASM sandbox securely streams the 1.5TB data in 1MB paginated windows, using less than 50MB of RAM.
**The Reality:** In `src/highestsec/sandbox.rs`, the `mrb_dataset_stream_read` host function allocates memory directly off the guest's request: `let mut buffer = vec![0u8; len as usize];`.
**The Consequence:** A malicious script kiddie in Norway can submit a WASM payload that calls `mrb_dataset_stream_read(ptr, 4000000000, 0)`. The Rust host will obediently attempt to allocate 4GB of RAM on the heap. This will instantly trigger an Out-Of-Memory (OOM) panic in the Host OS, crashing the Marabunta daemon and completely bypassing the Wasmtime safety limits.
**The Fix Required:** Hardcap the `len` argument in `mrb_dataset_stream_read` to a maximum safe buffer size (e.g., `std::cmp::min(len, 1024 * 1024)`).

### Problem 4: The Blind Guillotine (Cloud Sensor Masking)
**The Claim:** A node terminates tasks at 91°C to prevent silicon damage (e.g., a smartphone battery fire).
**The Reality:** The `HardwareMonitor` in `src/swarm/thermal.rs` polls `sysinfo::Components` to read physical CPU temperatures.
**The Consequence:** This works perfectly on a physical MacBook or gaming PC. However, in containerized environments (Docker, Kubernetes) or managed cloud VMs (AWS EC2), the hypervisor completely masks hardware thermal sensors from the guest OS. `sysinfo` returns an empty list, and the orchestrator reads `0.0°C` indefinitely. The Thermal Guillotine will never fire in the cloud, risking runaway billing or localized failure.
**The Fix Required:** Implement a fallback load-heuristic. If `max_temp` returns `0.0`, the `ThermalState` must trigger `Panic` purely based on sustained CPU Load (e.g., `avg_load > 0.95` for > 60 seconds).

### Problem 5: The Amnesiac Economy (Volatile Ledger)
**The Claim:** The honest Brazilian gamer is credited with MMX tokens, unlocking their in-game cosmetics.
**The Reality:** The `VerificationEngine` successfully authorizes the payment via `FederationManager::settle_verified_work()`.
**The Consequence:** The `settlement_ledger` is stored as a `Vec<PendingPayment>` held purely in ephemeral RAM. When the Marabunta daemon reboots, crashes, or is updated (a daily occurrence in a chaotic Citizen Swarm), the entire MMX economy is wiped to zero. The Brazilian gamer loses all their skins, and the Norwegian citizen gets no tax break. 
**The Fix Required:** Wire the `FederationManager` to the node's SQLite database (`Persistence` layer) or the `IsomorphicStateRing` WAL so the `settlement_ledger` survives node restarts.

---

### The Final Verdict
The new narrative is brilliant, but it raises the bar for operational security. The code must be patched to survive hostile WASI buffer overflows, missing thermal sensors, and ephemeral reboots. Once these five specific logic gaps are sealed, the Marabunta codebase will flawlessly and securely execute the Abyssal Treaty.