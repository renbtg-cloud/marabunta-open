# Annex A Seismic Verification Report v40: The Abyssal Treaty Analysis

**Target Document:** `marabunta_bible/14_annex_a.md` (and related marketing collateral)
**Analyst Tone:** Highly Critical, Pragmatic, Adversarial.
**Status:** CATASTROPHIC ARCHITECTURAL FAILURE AT 10M SCALE.

I have completed a merciless, line-by-line audit of the Rust codebase and the `management-ui` JavaScript implementation. You claim this system is a "Sovereign Analytics Engine" capable of executing a 1.5TB seismic inversion across 10 million nodes. 

Your claims are disconnected from the physical reality of your own source code. The architecture is a house of cards that collapses before the simulation even begins. Here is the empirical proof of your system's inability to survive Annex A:

### 1. The Vapourware GUI ("Sovereign Analytics Engine")
**Claim:** Annex B boasts "WebGL/D3 force-directed topologies" and "Real-time Sub-Swarm Constellation visualization."
**Code Reality:** I audited `management-ui/app.js`. There is absolutely zero WebGL or D3 code. Your `ConstellationGraph` class (line 1294) is an empty HTML stub that renders a basic `div` with the hardcoded text: *"No remote swarms connected"*. Your "Sovereign Analytics Engine" is an amateurish, vanilla JavaScript DOM table. It cannot render a 10-node LAN party, let alone a 10-million node planetary constellation.

### 2. The 405 Method Not Allowed Void (Blind Telemetry)
**Claim:** The GUI tracks the status of massive 1.5TB jobs.
**Code Reality:** The frontend `JobList` periodically calls `api.jobs()`, which issues a `GET /api/v1/jobs` HTTP request. However, I audited the Axum router in `src/swarm/api.rs` (line 884). You implemented `post(submit_job)`, but you **forgot to implement the GET route for listing jobs**. The orchestrator will instantly return a `405 Method Not Allowed`. The UI is completely blind to the very workloads it claims to be orchestrating. The dashboard is permanently blank.

### 3. The 1,000,000-Chunk RAM Bomb (OOM Panic)
**Claim:** The Orchestrator effortlessly broadcasts massive parameter sweeps to the DHT.
**Code Reality:** When a user submits a job, `src/swarm/api.rs::build_tasks_from_request` intercepts it. If `ChunkStrategy::Fixed { count }` is used to split the 1.5TB job into 1,000,000 tasks, your code executes a blind loop: it allocates a `Vec::with_capacity(count)` and pushes 1,000,000 deeply cloned structs containing the script strings and arguments directly into contiguous RAM. The USP Orchestrator will instantly trigger the Linux OOM Killer and crash with a `SIGKILL` before a single task hits Kademlia.

### 4. The 50,000 Assignment Amnesia (Infinite Loop)
**Claim:** The Swarm tracks the execution state of millions of physics chunks globally.
**Code Reality:** In `src/swarm/knowledge.rs::enforce_limits()`, you explicitly hardcoded `MAX_KNOWN_ASSIGNMENTS = 50_000` to prevent Kademlia from blowing out the node's RAM. 
When your massive Seismic job begins, Kademlia immediately hits this ceiling. The code chronologically evicts the oldest tracking data. The Orchestrator literally "forgets" that 99% of the simulation exists. Because the chunks vanish from the tracking table, the Orchestrator will assume they failed and re-issue them. The 10-million node Swarm will spend years endlessly re-calculating the exact same 50,000 chunks in a parasitic loop that never mathematically converges.

### Final Verdict
Stop claiming this is a production-ready supercomputer. You have built an interesting proof-of-concept that functions perfectly at a scale of 50 nodes. At 10 million nodes, the orchestrator forgets its tasks, OOM-crashes upon job submission, and provides a completely fake, broken frontend.

The "Abyssal Treaty" is structurally un-executable until you implement lazy-evaluated chunk streaming, SQLite-backed assignment tracking, and actual D3 graphics.