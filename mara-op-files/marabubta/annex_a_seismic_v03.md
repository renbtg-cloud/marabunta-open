# Annex A Seismic Verification Report v03: The Abyssal Treaty

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** MECHANICALLY SOUND, 3 ASYNCHRONOUS DATA-GRAVITY VULNERABILITIES REMAIN

The Marabunta Swarm codebase is an architectural marvel. The core structural defenses required for the "Abyssal Treaty" scenario (Citizen Swarms, Thermal Guillotines, BFT Coalition Genesis, and Persistent MMX Settlement) are fully operational. The `PARANOID` micro-audit physically bypasses LLVM optimizers, the daemon explicitly drops lonely Wolfpack formations, and the SQLite ledger is strictly thread-safe.

However, a final, exhaustive trace focusing purely on **asynchronous I/O and Data Gravity** reveals three critical execution voids. If a Brazilian gamer or a Norwegian taxpayer attempts to stream the 1.5TB seismic dataset today, the node will either silently fail the calculation, freeze the network, or explode the hard drive.

Here are the final three operational vulnerabilities that must be patched to survive the real-world deployment of Annex A:

---

### Problem 1: The 1.5TB Datalake Mirage (The Missing Argument)
**The Claim:** The WASM Sandbox securely streams the 1.5TB seismic `.segy` file.
**The Reality:** The `mrb_dataset_stream_read` host-function correctly exists inside `src/highestsec/sandbox.rs`, ready to pipe physical file descriptors into the WASI context.
**The Disconnect:** In `src/swarm/work.rs` (line 2917), the orchestrator intercepts the `dataset_shard_uri` from the JCL and successfully appends the metadata to the JSON input. However, the subsequent call to `sandbox.execute()` explicitly hardcodes `None` for the `dataset_path` argument! The orchestrator forgets to hand the file descriptor to the sandbox. The FWI algorithm will execute on an empty buffer, rendering the output mathematically useless.
**The Required Fix:** Update `execute_monte_carlo` to instantiate the `dataset_path` from the URI and pass it as the 8th argument into `sandbox.execute(...)`.

### Problem 2: SSD Exhaustion (The Pre-Fetch Crash)
**The Claim:** The Swarm natively bypasses WebAssembly's 4GB RAM limit by streaming the 1.5TB file into a tiny 50MB footprint.
**The Reality:** If we wire the `dataset_path` using a standard `reqwest` block, the daemon will naturally attempt to download the *entire* 1.5TB file into `/tmp/mrb_stream_xxx.bin` before the WASM payload boots.
**The Consequence:** A citizen's laptop with a 256GB NVMe drive will suffer a catastrophic `No space left on device` OS panic. We solved the 4GB RAM limit, but we immediately bricked their hard drive.
**The Required Fix:** True "Data Gravity" streaming requires zero disk footprint. We must update the `SandboxState` to hold the raw `dataset_shard_uri` (instead of a `std::fs::File`). The `mrb_dataset_stream_read` host-function must then execute synchronous HTTP `Range` Requests directly over the network (`bytes=offset-(offset+len)`), injecting the bytes instantly into the WASM linear memory and bypassing the host's SSD entirely.

### Problem 3: The "PARANOID" Async Starvation (The Thread Lock)
**The Claim:** The `PARANOID` micro-audit forces a 256MB memory-latency trap on laptops attempting to claim heavy FWI chunks.
**The Reality:** We successfully forced the compiler to respect the physical execution via `std::hint::black_box(&mut trap_buffer[i])` in `try_claim_work`.
**The Disconnect:** `try_claim_work` is an asynchronous Tokio loop. By forcing the orchestrator to physically iterate and mutate a 256MB vector for 2,500 milliseconds, we are intentionally committing an "Async Blocking" violation. If 100 gamers bid for chunks simultaneously, the node will lock 100 core Tokio worker threads. The orchestrator will freeze, dropping Kademlia pings and disconnecting from the DHT.
**The Required Fix:** Just like the SQLite MMX settlement lock, the physical 256MB `trap_buffer` audit must be offloaded into a `tokio::task::spawn_blocking` closure so it can execute on a dedicated kernel thread without starving the asynchronous event loop.

---

### Final Assessment
The core narrative holds absolute cryptographic and thermodynamic truth. We are one patch away from absolute physical truth. By fixing the `dataset_path` drop, routing the S3 stream directly to memory via HTTP Range requests, and offloading the `PARANOID` RAM trap to a blocking thread, the Marabunta Swarm will seamlessly execute the Abyssal Treaty without a single hardware compromise.