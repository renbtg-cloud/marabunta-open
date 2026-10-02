# Annex A Seismic Verification Report v02: The Abyssal Treaty

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** STRUCTURALLY COMPLETE, 4 ADVANCED EXECUTION VULNERABILITIES REMAIN

The Marabunta Swarm codebase has successfully integrated the core requirements of the "Abyssal Treaty" scenario. The CLI triggers the network, the Thermal Guillotine kills runaway processes using Wasmtime's epoch interrupts, and the MMX economy physically persists settlement ledgers to an SQLite database. 

However, a second, deeper "extreme edge-case" execution trace reveals that while the logic is wired, the **laws of physics, compiler optimizations, and asynchronous I/O** will cause the Swarm to fail spectacularly when subjected to the true 1.5TB scale described in Annex A.

Here are four advanced operational vulnerabilities that must be fixed to survive the real world:

---

### Problem 1: The SSD Exhaustion (Fake Streaming)
**The Claim:** The Swarm streams a 1.5TB dataset into the 4GB WASM sandbox, bypassing memory limits without crashing the host's hardware.
**The Reality:** We patched `execute_monte_carlo` in `src/swarm/work.rs` to use `reqwest` to download the file. However, the code reads:
`while let Ok(Some(chunk)) = response.chunk().await { let _ = file.write_all(&chunk); }`
The orchestrator fully downloads the entire 1.5TB file into `/tmp/mrb_stream_xxx.bin` *before* the WASM sandbox even starts! 
**The Consequence:** A Norwegian citizen running the daemon on a MacBook Air with a 256GB SSD will suffer a catastrophic `No space left on device` OS-level crash. We saved the RAM, but we instantly bricked their hard drive.
**The Required Fix:** We must implement a true **named pipe (FIFO)** or an async ring-buffer. The `reqwest` stream must be piped directly into the `mrb_dataset_stream_read` host function on-demand, discarding bytes immediately after the WASM guest reads them.

### Problem 2: The LLVM Optimizer Bypass (The "PARANOID" Illusion)
**The Claim:** The `PARANOID` micro-audit allocates 256MB of RAM and writes `0x42` to it to test memory latency, rejecting laptops that swap to disk.
**The Reality:** The logic in `try_claim_work` allocates the vector, loops over it, and drops it. 
**The Consequence:** The Marabunta binaries are compiled with `cargo build --release`. Rust's LLVM backend is incredibly aggressive with Dead Code Elimination (DCE). Because the `trap_buffer` is never read from or used in a side-effecting way, LLVM will optimize the entire allocation and loop away to nothing. The 2500ms timer will measure 0ms. A 2GB laptop swapping to an old spinning hard drive will instantly pass the test.
**The Required Fix:** We must wrap the buffer manipulation in `std::hint::black_box(&mut trap_buffer)` to explicitly forbid the compiler from optimizing away our physical hardware latency trap.

### Problem 3: SQLite Async Starvation (The Settlement Lock)
**The Claim:** The `FederationManager` settles the economy by writing verified chunk rewards to `marabunta_settlement.db`.
**The Reality:** The database connection is held behind a synchronous `parking_lot::Mutex` and accessed directly inside the `VerificationEngine`'s Tokio async execution flow.
**The Consequence:** During a massive parameter sweep, hundreds of chunks might be verified simultaneously. Hundreds of Tokio async worker threads will contend for a synchronous OS-level file lock on the SQLite database. This will block the async executors, causing a devastating "starvation panic" where the node becomes entirely unresponsive to Kademlia network pings, leading its peers to assume it is dead and drop it from the DHT.
**The Required Fix:** SQLite writes must be offloaded using `tokio::task::spawn_blocking`, or we must push the settlement requests into an asynchronous `mpsc::channel` consumed by a dedicated background ledger thread.

### Problem 4: The Lonely Wolfpack (Zero-Peer Genesis)
**The Claim:** `mrb swarm init-wolfpack` broadcasts the BFT sub-swarm invitation to allied academic peers.
**The Reality:** The CLI sends the POST request to the Daemon, and the Daemon queries `state.knowledge.find_closest_nodes(&h, 10)` to broadcast the `WolfPackProposal`. 
**The Consequence:** If the USP orchestrator has just booted up and hasn't successfully synced with the Kademlia DHT yet, `find_closest_nodes` will return an empty vector. The Daemon will cheerfully create the Wolfpack locally and return a success message to the CLI, but no other node on Earth will receive the proposal. 
**The Required Fix:** The `init_wolfpack` API handler must check `if neighbors.is_empty()`. If it has zero peers, it must return an HTTP 503 `ServiceUnavailable` error ("DHT not synced, cannot broadcast Genesis") to prevent false positives.

---

### Final Assessment
The core narrative holds strong, but moving 1.5 Terabytes across an untrusted network exposes the brutal realities of hardware limitations. By patching these four specific execution vulnerabilities—especially the `/tmp/` disk explosion and the LLVM `black_box` bypass—the Marabunta codebase will achieve total, physical readiness for the Abyssal Treaty.