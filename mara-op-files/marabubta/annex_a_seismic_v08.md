# Annex A Seismic Verification Report v08: The Abyssal Treaty (The Silent Deaths)

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** ARCHITECTURALLY PREPARED, 3 SILENT CONCURRENCY AND I/O CORRUPTIONS REMAIN

The Marabunta Swarm codebase is structurally armed to execute the Abyssal Treaty scenario. All pipelines (CLI, WASM streaming, DHT, and GUI) are connected and verified. 

However, a final, "all-inclusive" stress test simulating the *exact* physical conditions of an untrusted, global internet (dropped packets, concurrent I/O locking, and overloaded Kademlia buffers) reveals **three silent corruption vectors**. 

These are not compilation errors or missing pipelines; they are silent, non-fatal operational failures that will subtly destroy the integrity of the physics simulation and the economic ledger without crashing the Daemon.

---

### Problem 1: The Brittle Stream (Premature EOF Corruption)
**The Claim:** The 1.5TB dataset streams seamlessly into the WASM sandbox bypassing the 4GB RAM limit.
**The Reality:** The `mrb_dataset_stream_read` host-function in `src/highestsec/sandbox.rs` correctly executes an HTTP Range request for the 1MB window. 
**The Silent Death:** The network code dictates:
```rust
match client.get(&uri).header("Range", range_header).send().await {
    Ok(resp) => resp.bytes().await.unwrap_or_default(),
    Err(_) => bytes::Bytes::new(),
}
```
If a citizen's home Wi-Fi drops a packet, or if the USP SAN times out during the 1.5TB stream, the `reqwest` client returns an `Err(_)`. The Rust host silently swallows the error and returns a 0-byte array to the WASM guest. In C/WASM, reading 0 bytes signifies an End-Of-File (EOF). The physics simulation will silently assume the dataset has ended, calculate a mathematically corrupted Seismic Velocity Model, and return it to the orchestrator as a "success."
**The Required Fix:** The host function must not return `0` on network errors. It must implement a robust exponential backoff loop (`tokio::time::sleep`) to retry the HTTP Range request. If the connection is definitively dead, it must intentionally trap the WASM engine (`Err(wasmtime::Trap::new("Network Stream Fatal Timeout"))`) to force a visible failure and a chunk retry, rather than allowing silent data corruption.

### Problem 2: The `SQLITE_BUSY` Massacre (Amnesiac Economy Part 2)
**The Claim:** The MMX settlement ledger successfully persists micro-credits to disk, surviving node reboots.
**The Reality:** We wrapped the SQLite `INSERT` in a `tokio::task::spawn_blocking` closure, perfectly solving the Tokio async starvation issue. 
**The Silent Death:** In `src/marabunta/federation.rs`, we open the connection (`rusqlite::Connection::open`) without configuring the SQLite Pragmas. By default, SQLite uses the `DELETE` journal mode, which acquires an exclusive OS-level lock on the entire database file during writes. When the orchestrator finishes a massive parameter sweep and attempts to settle 500 verified chunks simultaneously, 500 blocking threads will race for the lock. SQLite will instantly return `database is locked` (`SQLITE_BUSY`) for 499 of them. The `execute` command fails silently, the micro-credits vanish into the void, and 499 citizens lose their tax breaks without a single error log reaching the UI.
**The Required Fix:** Immediately after opening the SQLite connection, the orchestrator must execute `PRAGMA journal_mode=WAL;` and `PRAGMA synchronous=NORMAL;`. This enables Write-Ahead Logging, allowing infinite concurrent reads and highly concurrent, lock-free writes across the thread pool.

### Problem 3: The Bounded Kademlia Genesis Drop (The Ghost Wolfpack)
**The Claim:** `mrb swarm init-wolfpack` actively interrogates the routing table and successfully broadcasts the BFT token to the DHT.
**The Reality:** The `/api/v1/federation/wolfpack` handler uses `state.work_engine.outbound_tx().try_send((addr, msg.clone()))` to push the 10 Genesis broadcasts into the Kademlia network queue.
**The Silent Death:** The `outbound_tx` channel is a bounded `mpsc::channel`. If the node is currently busy gossiping its health metrics or resolving DHT routing tables, the queue might be near capacity. `try_send` instantly fails and silently drops the message if the channel is full. The UI reports "Sub-Swarm broadcast to Kademlia neighbors," but the packets were instantly dropped in memory. The Genesis ceremony fails invisibly.
**The Required Fix:** The API handler must check the return value of `try_send`. If it returns `Err(TrySendError::Full)`, the API must pause (`tokio::time::sleep`) and retry, or use a permitted `send().await` to ensure the cryptographic broadcast is explicitly queued for the network socket.

---

### Final Assessment
The Abyssal Treaty relies on zero-trust cryptography and thermodynamic reality. However, relying on a 0-byte EOF trap and a synchronous SQLite lock violates the physical realities of the public internet. 

By implementing an HTTP retry loop in the WASM stream, enforcing `PRAGMA WAL` in the MMX ledger, and ensuring reliable DHT channel queuing, the Marabunta Swarm will eradicate these three final "Silent Deaths" and achieve uncompromising, fault-tolerant execution.