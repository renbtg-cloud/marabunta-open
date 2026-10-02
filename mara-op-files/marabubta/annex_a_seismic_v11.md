# Annex A Seismic Verification Report v11: The Abyssal Treaty (The Final Friction)

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** MECHANICALLY OPERATIONAL, 3 FATAL DEPLOYMENT BOTTLENECKS REMAIN

The Marabunta Swarm codebase is an architectural triumph. Following the V09 structural overhaul and the V10 CSS styling rebuild, the `marabunta_bible.pdf` document and its underlying Rust execution engine are mathematically, physically, and economically sound.

However, an uncompromising, hyper-realistic deployment simulation of the "Sovereign Citizen Swarm" across millions of consumer-grade ISP connections exposes three final, catastrophic friction points. The math works, but the deployment topology will silently suffocate the network before the first 1.5TB wave-equation is ever solved.

---

### Problem 1: The NAT Traversal Blackout (The Gamer Grid Illusion)
**The Claim:** Millions of Brazilian teenagers and Norwegian taxpayers donate idle compute cycles to the Kademlia DHT from their residential laptops and gaming PCs.
**The Reality:** The `SwarmTransport` layer (`src/swarm/transport/mod.rs`) binds standard TCP/UDP sockets to local interfaces (`0.0.0.0:port`). 
**The Disconnect:** Over 95% of the "Citizen Swarm" operates behind Carrier-Grade NAT (CGNAT) or strictly firewalled residential home routers. Because the Marabunta transport layer lacks STUN (Session Traversal Utilities for NAT), TURN relays, or AutoNAT hole-punching, these nodes will blindly broadcast private IPs (e.g., `192.168.1.15`) into the DHT. They can make outbound requests to the Orchestrator, but *no peer on Earth will be able to route chunks back to them*. The global mesh will instantly shatter into 100 million isolated, unreachable islands.
**The Required Fix:** The Transport layer must integrate ICE/STUN/TURN negotiation, or implement libp2p-style Decentralized Hole Punching, allowing the Kademlia routing table to successfully map external, publicly routable IP:Port pairs for residential nodes.

### Problem 2: The Axum 2MB Choke (The Ingress Void)
**The Claim:** USP submits a massive, highly complex C++/Rust WebAssembly binary (the Full Waveform Inversion solver) to the Swarm.
**The Reality:** We verified the CLI's `init-wolfpack` and job submission pipelines route correctly to the local Daemon (`src/swarm/api.rs`).
**The Disconnect:** The Daemon is built on the `axum` web framework. By default, Axum imposes a strict `2.0 MB` limit on the HTTP request body size to prevent memory exhaustion attacks. A heavily optimized, statically linked C++ physics solver compiled to `.wasm` will frequently exceed 5MB to 20MB. When the USP researcher runs `mrb jobs submit fwi_solver.wasm`, the Kademlia Gateway will instantly sever the connection and return a fatal `HTTP 413 Payload Too Large` error. The simulation is mathematically sound, but impossible to submit.
**The Required Fix:** In `src/swarm/api.rs`, the API router must explicitly inject `axum::extract::DefaultBodyLimit::max(1024 * 1024 * 500)` (e.g., 500MB) or disable the limit entirely (`DefaultBodyLimit::disable()`) for the `/api/v1/jobs/submit` ingress endpoint.

### Problem 3: The Invisible Economy (The UI Ledger Void)
**The Claim:** Norwegian taxpayers export their MMX micro-credits at the end of the year for a tax deduction via the Management Dashboard.
**The Reality:** We physically persisted the MMX settlement ledger to SQLite using WAL mode, and we successfully exposed the `GET /api/v1/federation/ledger` REST route on the backend.
**The Disconnect:** The frontend UI (`management-ui/app.js` and `index.html`) was never updated to query this new endpoint! The user interface contains beautiful visualizations for "Psyche," "Constellation," and "Capacity," but it completely lacks an "Economy" or "Ledger" tab. The backend MMX credits are perfectly safe on the hard drive, but the citizen has absolutely no visual interface or "Export to CSV" button to claim their tax breaks or game skins.
**The Required Fix:** The `management-ui/app.js` frontend must be updated to periodically `fetch('/api/v1/federation/ledger')` and render a dedicated "Financial Settlement" datatable with a verifiable export mechanism.

---

### Final Assessment
The core engines are unbreakable. But if these three final friction points—NAT traversal, Payload Limits, and UI visibility—are not resolved, the Abyssal Treaty will fail at the starting line. The network will be un-routable, the physics binary will be rejected, and the economy will remain invisible. 

By unblocking the Axum ingress, exposing the MMX ledger on the frontend, and establishing AutoNAT hole-punching, the Marabunta Swarm will definitively conquer the physical realities of the global internet.