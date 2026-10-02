# Annex A Seismic Verification Report v14: The Abyssal Treaty (The Systemic Decay)

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** MECHANICALLY SOUND, 4 LONG-TERM SYSTEMIC DECAY VULNERABILITIES DETECTED

The Marabunta Swarm codebase is structurally, asynchronously, and atomically prepared for the Abyssal Treaty scenario. The CLI triggers the Kademlia genesis, the WebAssembly engine streams the 1.5TB seismic dataset, the Thermal Guillotine protects the hardware, and the MMX economy is persisted securely to disk.

However, moving beyond the immediate execution of a single simulation and projecting the Swarm’s behavior over **months of continuous operation at planetary scale (10 million+ nodes)** reveals a final class of vulnerabilities. These are "Long-Term Systemic Decay" vectors. They will not crash the network on day one, but they will slowly suffocate the nodes, bloat the storage, and lock out the users.

Here are the final four systemic vulnerabilities that must be addressed for true planetary longevity:

---

### Problem 1: The Ipify DDoS (The 429 Blackout)
**The Claim:** Residential nodes (gamers, citizens) behind Carrier-Grade NAT automatically discover their public IPs and join the global mesh.
**The Reality:** We implemented a background loop in `src/swarm/mod.rs` that makes an HTTP GET request to `https://api.ipify.org?format=json` every 5 minutes.
**The Decay:** If the Abyssal Treaty goes viral and 10 million Brazilian and Norwegian citizens boot up the daemon, they will collectively fire 33,333 requests per second at a free, public API. Ipify will instantly interpret this as a massive DDoS attack and return `HTTP 429 Too Many Requests` or `503`. The `reqwest` JSON parser will fail, the nodes will lose their ability to resolve external IPs, and the entire residential Swarm will silently drop off the global DHT.
**The Required Fix:** The orchestrator cannot rely on a single centralized API. It must either randomize its queries across a hardcoded list of public STUN servers (e.g., Google, Cloudflare, Twilio) with extreme exponential jitter, or—preferably—use the Kademlia DHT itself to ask connected peers, "What IP address do you see me connecting from?"

### Problem 2: The Infinite WAL (The SQLite Disk Bomb)
**The Claim:** The `FederationManager` safely persists millions of MMX micro-credits to disk via an SQLite database, preventing async starvation via Write-Ahead Logging (WAL).
**The Reality:** We successfully enabled `PRAGMA journal_mode=WAL;` in `src/marabunta/federation.rs`.
**The Decay:** While WAL perfectly solves the concurrency lock issue, we never configured a checkpointing mechanism (`PRAGMA wal_autocheckpoint`). In a high-throughput environment settling hundreds of chunks per second, the `-wal` file will append indefinitely without ever folding back into the main `.db` file. Over weeks of operation, a citizen's 256GB SSD will slowly fill up with a massive, multi-gigabyte SQLite WAL file, eventually triggering the same `No space left on device` panic we fixed for the dataset streaming.
**The Required Fix:** The `FederationManager` must explicitly execute `PRAGMA wal_autocheckpoint = 1000;` on startup, or a background Tokio thread must periodically issue a `PRAGMA wal_checkpoint(TRUNCATE);` to flush the logs and reclaim disk space.

### Problem 3: The 32-bit WASI Offset Overflow (The 4GB Infinite Loop)
**The Claim:** The WASM sandbox seamlessly streams the 1.5TB `.segy` seismic dataset without hitting the 4GB RAM limit.
**The Reality:** Our `mrb_dataset_stream_read` host-function correctly accepts a 64-bit offset (`offset: u64`) to fetch bytes from the remote S3 bucket via HTTP Range requests.
**The Decay:** The WebAssembly standard targeted by most compilers (`wasm32-wasi`) uses 32-bit pointers and 32-bit integers for standard `size_t` file offsets. If the C++ or Rust physics solver compiled by USP uses a standard 32-bit integer to track its read position within the file, the offset will mathematically overflow and wrap back to `0` the moment it hits `4,294,967,295` bytes (4GB). The simulation will silently loop over the first 4GB of the 1.5TB dataset for weeks, generating deeply flawed seismic velocity models.
**The Required Fix:** The Marabunta Application Binary Interface (ABI) documentation must strictly mandate the use of explicit 64-bit integers (`uint64_t` or `u64`) for all external data stream offsets, or the orchestrator must enforce compilation to the `wasm64` standard.

### Problem 4: The Dashboard DOM Crash (The 100k Node Render)
**The Claim:** The local Management UI displays the full constellation and capacity metrics of the global Swarm.
**The Reality:** The frontend `management-ui/app.js` dynamically builds HTML tables and SVG elements based on the JSON payloads returned by the Daemon APIs (`/api/v1/nodes`, `/api/v1/capacity`).
**The Decay:** If the Abyssal Treaty scales to 100,000 nodes, the backend will cheerfully return a massive JSON array containing 100,000 peer records. The vanilla JavaScript frontend will attempt to iterate over this array and inject 100,000 `<tr>` elements into the DOM. The user's web browser (Chrome/Firefox) will freeze, consume gigabytes of RAM, and violently crash with an "Aw, Snap! Out of Memory" error. The UI is architecturally incapable of rendering the hyperscale it claims to orchestrate.
**The Required Fix:** The backend APIs in `src/swarm/api.rs` must enforce hard pagination (e.g., `?limit=100&offset=0`), and the frontend must implement virtualized scrolling or paginated data-tables to cap the DOM element count regardless of Swarm size.

---

### Final Assessment
The core execution mechanics are perfect, but long-term exposure to reality demands strict hygiene. By decentralizing the IP discovery (preventing an accidental DDoS), enforcing SQLite WAL checkpoints, guaranteeing 64-bit ABI offsets, and protecting the browser DOM with pagination, the Marabunta Swarm will achieve true planetary longevity.