# Annex A Seismic Verification Report v30: The Abyssal Treaty (The Sisyphus Collapse)

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** ARCHITECTURALLY BANKRUPT AT SCALE. 3 NEW PLANETARY EXTINCTION EVENTS DETECTED.

You fixed the node-level bottlenecks. You fixed the enterprise air-gap routing for the Control Plane. You ensured identities persist and SQLite doesn't explode. 

However, taking a massive step back and modeling the **Macro-Physics** of the 350-Petabyte "Abyssal Treaty" scenario across a 10-million node network reveals that your architecture suffers from profound logical amnesia and data plane starvation.

The nodes won't crash. But the simulation will never finish, and the Enterprise sub-swarms will still suffocate. Here are the three fatal macro-physics realities proving this architecture cannot survive Annex A:

---

### Problem 1: The Sisyphus Anomaly (The 50k Assignment Amnesia)
**The Claim:** The USP Orchestrator distributes a 350-Petabyte job broken into millions of discrete wave-equation chunks, managing the global queue via the `KnowledgeStore`.
**The Reality:** The `KnowledgeStore` caps its memory footprint via a hard limit: `MAX_KNOWN_ASSIGNMENTS = 50_000` (defined in `src/swarm/config.rs`). 
**The Catastrophic Flaw:** When a 1.5TB job is chopped into 100,000 discrete chunk assignments, the Orchestrator's local Kademlia map will instantly saturate its 50,000 limit. To prevent an OOM panic, the daemon chronologically evicts the oldest assignments. The Orchestrator literally *forgets* that the first 50,000 chunks were assigned or completed. Because they no longer exist in the local map, the orchestrator assumes they failed and re-issues them to the Swarm. The 10-million nodes will spend the next 5 years endlessly recalculating the exact same 50,000 chunks in a permanent, amnesiac loop. The 350PB simulation will never exceed 50% completion.
**The Required Fix:** The Orchestrator must not use an ephemeral, capped `DashMap` to track massive job state. The master assignment queue must be offloaded to the persistent SQLite database, where millions of rows can be queried via paginated SQL without OOM limits.

### Problem 2: The Phantom Proxy (Data Plane Starvation)
**The Claim:** The designated Membrane Gateway (the Secretary's MacBook) bridges the air-gapped corporate LAN to the global Swarm, acting as an Egress Diode for both Kademlia control traffic and S3 Data Gravity.
**The Reality:** We patched `SwarmMessage::EgressRelay` to successfully forward Kademlia gossip. We patched the WASM sandbox to configure its `reqwest::Client` to use the Gateway as an HTTP proxy (`reqwest::Proxy::all(proxy_url)`).
**The Catastrophic Flaw:** The Secretary's MacBook *is not running an HTTP Proxy Server*. It only listens for Marabunta TCP/UDP UDP Swarm traffic. When the 499 internal office computers attempt to fetch the 1.5TB datalake, their `reqwest` clients will send HTTP `CONNECT` requests to the Gateway's Marabunta port. The Gateway will reject the malformed bytes, the HTTP stream will timeout, and the 499 internal workers will starve. You fixed the Control Plane, but you completely ignored the Data Plane. The Enterprise sub-swarm is still paralyzed.
**The Required Fix:** The `marabunta-visor` daemon on the Gateway node must explicitly spin up a minimal, forward-HTTP Proxy (`hyper` or `tower`) on a secondary port (e.g., `8080`) specifically dedicated to proxying outgoing S3 HTTP traffic for the internal cluster.

### Problem 3: The Orchestrator Disk Implosion (The 350-Petabyte Funnel)
**The Claim:** The Swarm successfully bypasses MPSC RAM bombs by having workers write their 2GB outputs to their local `BlobStore` and gossiping a 32-byte Blake3 hash. The USP Orchestrator fetches the results asynchronously.
**The Reality:** The USP Orchestrator receives the 100,000 `BLOB_POINTER` hashes and downloads the 2GB result matrices from the workers to aggregate the final 3D seismic model.
**The Catastrophic Flaw:** The orchestrator node (e.g., a standard AWS EC2 instance or university server) has a 2TB NVMe drive. By pulling 100,000 individual 2GB blobs to aggregate the 350-Petabyte dataset locally, the Orchestrator will fill its physical hard drive to 100% capacity within the first few hours. The host OS will throw `No space left on device`, the SQLite DB will corrupt, and the master node will violently crash, taking the entire simulation down with it.
**The Required Fix:** The Orchestrator cannot aggregate Petabytes of output locally. It must act as a strict Data-Plane Router. When it receives a `BLOB_POINTER`, it must command the worker node to upload the 2GB result directly back to the USP AWS S3 bucket via presigned URLs, bypassing the Orchestrator's local disk entirely.

---

### Pragmatic Conclusion
You patched the node, but you fundamentally miscalculated the physics of Petabyte-scale aggregation and routing. 

Until you migrate the master assignment queue to SQLite (curing the Sisyphus Anomaly), spin up a dedicated HTTP forward-proxy on the Membrane (unblocking the Data Plane), and utilize S3 presigned URLs for result aggregation (preventing Orchestrator disk implosion), the "Abyssal Treaty" is nothing more than an elaborate suicide mechanism for your network. Stop pretending this system can process 350 Petabytes.