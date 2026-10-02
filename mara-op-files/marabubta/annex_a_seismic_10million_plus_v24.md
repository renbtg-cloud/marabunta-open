# Annex A Seismic Verification Report v26: The Abyssal Treaty (The Architecture of Deception)

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** STRUCTURALLY INCOMPETENT. PLANETARY-SCALE ILLUSIONS EXPOSED.

I have completed a merciless, unvarnished code audit of the Marabunta Swarm targeting the **10-million+ node** "Abyssal Treaty" scenario. 

I must formally retract the conclusions of previous reports. The so-called "Planetary Scale Resolutions" were theoretical hallucinations—the actual Rust code committed to the repository does not support the claims made in the documentation. 

If USP or Petrobras deployed this software today to 10 million nodes, the network would instantly suffer catastrophic Out-Of-Memory (OOM) panics, memory leaks, amnesia, and routing failure. 

Here are the fatal, hardcoded bottlenecks that prove the current Rust implementation cannot survive 10 million nodes:

---

### Problem 1: The Amnesiac Economy & The Infinite RAM Leak
**The Claim:** The MMX settlement ledger is rigorously persisted to an SQLite database (`marabunta_settlement.db`) and exposed via the UI.
**The Reality:** The `GET /api/v1/federation/ledger` endpoint (`src/swarm/api.rs:8094`) reads *exclusively* from `fm.settlement_ledger.iter()`, which is a volatile `Vec<PendingPayment>` held in RAM. 
**The Catastrophic Flaw:** 
1. **The Amnesia:** `FederationManager::new()` never executes a `SELECT` query to populate the `Vec` from SQLite on boot. If a node reboots, the UI reads an empty `Vec` and displays 0 MMX credits, even though the data is on disk. The economy is write-only.
2. **The Memory Leak:** `settle_verified_work()` continuously executes `self.settlement_ledger.push(...)`. In a 10M node swarm settling millions of chunks, this `Vec` grows infinitely in RAM. The orchestrator will eventually OOM panic from holding millions of receipts in an unbounded array.

### Problem 2: The Fake DHT (Chronological Eviction breaks XOR)
**The Claim:** The Swarm uses a Kademlia DHT to route Data Gravity payloads.
**The Reality:** To cap RAM, `KnowledgeStore::enforce_limits` caps the routing table to `MAX_KNOWN_NODES = 10,000` by evicting the *oldest* (`last_seen`) nodes.
**The Catastrophic Flaw:** Kademlia routing requires strictly maintaining k-buckets based on mathematical XOR distance from the local `NodeId`. By evicting based purely on chronological time, the routing table devolves into a cache of the loudest, most recent nodes. You have completely destroyed the XOR metric space. "Data Gravity" routing (finding the exact node mathematically closest to the 1.5TB `.segy` file) in a 10-million node swarm is impossible; the network will fragment into isolated echo chambers.

### Problem 3: The 1.5TB Job Broadcast Freeze (Gossip Saturation)
**The Claim:** The Swarm orchestrates massive 1.5TB parameter sweeps.
**The Reality:** `GossipMessage` is capped at 500 assignments (`MAX_ASSIGNMENTS_PER_MESSAGE`).
**The Catastrophic Flaw:** A 1.5TB FWI job might contain 1,000,000 discrete chunk assignments. Distributing 1,000,000 assignments 500 at a time across a 10-million node swarm via Epidemic Gossip (with a 10-second adaptive jitter) will take *hours* just to distribute the job manifest metadata before a single byte of physics is executed. The network cannot schedule hyperscale tasks efficiently using epidemic assignment payloads; it requires a BitTorrent-style manifest magnet link or a dedicated DHT scatter-gather.

---

### Pragmatic Conclusion
Your "Abyssal Treaty" is a compelling marketing document, but the Rust implementation is fundamentally amateur at scale. 

You cannot achieve planetary resilience with chronologically evicted routing tables, unbounded RAM vectors, and naive epidemic job distribution. Until you implement true k-bucket XOR routing, paginated SQLite API reads, and BitTorrent-style manifest magnet links, your 10-million node swarm is mathematically doomed to collapse under its own weight. Stop pretending this is production-ready.