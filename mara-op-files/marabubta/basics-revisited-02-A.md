# The Physics of the Swarm: Revisiting the Basics (Part 02-A)

A rigorous third-pass audit of the core Marabunta physics engine was conducted, explicitly excluding previously identified fault lines (Purgatory Black Hole, Orphaned ZKPs, Gateway Asphyxiation). The focus was shifted to edge-case deadlocks, topological isolation, and unhandled adversarial networking vectors.

## 1. The Reality Anchor Paradox & The Arrogance of "Pace"
*   **Current State:** Submitters can define an `associated_topology` (The Reality Anchor). Edge nodes drop the job if they do not match. If there are not enough matching nodes globally, the job hangs.
*   **The Gap:** A naive orchestrator would attempt to "auto-detect" starvation by dividing `chunks_done / total_chunks`. This is an arrogant assumption. For embarrassingly parallel jobs (Monte Carlo), this math works. For chaotic workloads (Genetic Algorithms, recursive data generation), "pace" is mathematically undefinable by the base binary. If Marabunta tries to guess the pace of a genetic algorithm, it will spam the user with false-positive starvation alerts, violating the core philosophy: *Never try to outsmart or disobey the user.*
*   **The Fix:** Implement a user-defined `PaceDetector` in the job manifest. 
    *   The user can explicitly declare `PaceDetector::NaN`, ordering Marabunta to never calculate pace.
    *   The user can select presets like `PaceDetector::LinearChunks`.
    *   The user can inject a `PaceDetector::TuringCompleteOracle` (a Rhai script) that evaluates custom telemetry to define pace.
    *   If the user-defined pace fails, Marabunta does *not* auto-correct. It emits a telemetry warning. The user retains absolute sovereignty to issue a `JobMutation` (e.g., relaxing the topology or extending the deadline) via the API. Telemetry over Tyranny.

## 2. The Eager Replication Flood (BitTorrent Asphyxiation)
*   **Current State:** Heavy datasets (WASM binaries, 5GB tensors) are downloaded via Kademlia BitTorrent logic. Nodes pull the 32-byte `BlobHash` from the DHT and request the chunks via `FetchBlob`.
*   **The Gap:** If 100,000 edge nodes try to grab the exact same 20MB WASM binary from the original Orchestrator at the exact same millisecond, the Orchestrator`s outbound bandwidth will saturate immediately. The Linux OOM killer will terminate the Marabunta daemon before the first 100 nodes finish downloading.
*   **The Fix:** Implement **Eager Blob Replication (Seeding)**. Before the Orchestrator gossips the job manifest, it must forcefully push the WASM binary to 50 random backbone nodes (`PushBlob`). The DHT must update the index so that the 100,000 edge nodes download the binary from 50 different seeders, fully decentralizing the bandwidth load.

## 3. The Identity Handshake Deadlock (Connection Saturation)
*   **Current State:** The `SwarmTransport` handles incoming Kademlia connections. When a connection is established, the nodes perform a cryptographic handshake to verify their `NodeId`.
*   **The Gap:** A malicious actor (or a severe network partition recovery event) can open 10,000 TCP connections to a single node and stall the cryptographic handshake (sending bytes incredibly slowly). The target node`s `ConnectionPool` hits its `max_inbound_connections` limit. Legitimate `SettlementClaim` or `ChunkResult` messages are dropped because the target node has no free TCP slots.
*   **The Fix:** Implement **Connection Vitality Tiers**. The `ConnectionPool` must reserve 10% of its slots exclusively for "Vital" messages (Settlements, Proofs, Reality Anchor updates). Stalled or slow-read connections must be aggressively evicted (LRU) to guarantee liquidity for the economic engine.
