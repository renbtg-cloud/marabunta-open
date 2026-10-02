# ANNEX A: The Sovereign Science Fabric & The Economic Syndicate

## A.1 The Geopolitical Context: The Abyssal Treaty

In the era of localized sovereign computing, the monolithic public cloud is an economic bottleneck for fundamental science. The egress fees alone render petabyte-scale physics simulations financially impossible for independent academic and research institutions.

To break this cartel, three nations with deeply aligned petrochemical interests—Brazil, Norway, and the United Kingdom—established the **Abyssal Treaty**. Their shared objective: processing thousands of terabytes of raw acoustic `.segy` data to perform Full Waveform Inversion (FWI) on deep-sea seismic reflection models (e.g., the Brazilian Pre-Salt, the Norwegian Barents Sea, and the UK North Sea).

Rather than renting supercomputers, they pooled their sovereign citizen networks into a single, time-zone-arbitraged Marabunta Swarm.

### The Three Pillars of the Fabric:
1. **Brazil (The Gamer Grid):** Bundled as an opt-in background daemon within a massively popular indie video game developed by IME/USP alumni. Millions of Brazilian teenagers with high-end, L3-heavy Ryzen CPUs and DDR5 RAM donate compute cycles while they sleep in exchange for MMX micro-credits, which unlock exclusive cosmetic skins. 2. **Norway (The Civic Tax Swarm):** A progressive Scandinavian digital initiative where citizens run the Marabunta daemon on idle MacBooks and home servers. At the end of the fiscal year, cryptographically verified MMX execution receipts are burned in exchange for micro-deductions on their income tax. 3. **The UK (The Night-Shift Academic Net):** Imperial College and allied universities link hundreds of thousands of library desktops and Raspberry Pi lab clusters that sit idle from 8:00 PM to 8:00 AM.

Because the sun never sets on this fabric, thermodynamic efficiency is absolute. When it is 3:00 AM in São Paulo, the cold, sleeping Brazilian grid crunches Norwegian seismic data. When it is 3:00 AM in Oslo, the frozen Scandinavian desktops process Brazilian Pre-Salt models.

However, this environment is inherently hostile. You cannot trust a sleeping teenager's overclocked PC, you cannot trust a library desktop, and you must account for opportunistic script kiddies attempting to defraud the Norwegian tax authority. The Marabunta core architecture was explicitly designed to survive this chaos.

---

## A.2 Phase 1: The Zero-Trust Genesis

To initiate the joint compute session, the lead researchers at USP, NTNU, and Imperial College must establish a cryptographic perimeter.

The USP orchestrator opens the terminal: `mrb swarm init-wolfpack --purpose "Abyssal_FWI_Treaty" --threshold 2-of-3`

Behind the scenes, this is not a mere database entry. The Marabunta CLI triggers the physical **ChrysalisGrinder**. It allocates a 32MB scratchpad and saturates the local L3 cache, executing a memory-hard Proof-of-Work loop until it derives a `NodeId` with 26 leading zero-bits. This mathematically enforces a ~4-second physical cost per identity, neutralizing Sybil botnet attacks.

The CLI then builds the JSON payload and executes an HTTP POST to the local Daemon's `/api/v1/federation/wolfpack` endpoint. The Daemon broadcasts the `SwarmMessage::WolfPackProposal` across the Kademlia DHT. A secure, ephemeral BFT (Byzantine Fault Tolerant) computing coalition is born across three continents.

---

## A.3 Phase 2: Data Gravity & The 1.5TB WASM Bypass

The raw seismic survey block is 1.5 Terabytes. It sits on a secure academic Storage Area Network (SAN) in São Paulo.

A Norwegian citizen's MacBook Air claims a chunk of the wave-equation simulation. Standard WebAssembly (wasm32) imposes a hard 4GB linear memory limit. Naively passing the dataset via the DHT (`SwarmMessage::PushBlob`) would instantly trigger a host-level Out-Of-Memory (OOM) panic, crashing the Swarm.

Instead, the USP JCL manifest specifies a `dataset_shard_uri`.

The Marabunta `WorkEngine` intercepts this URI. Inside the `HighestsecSandbox`, the FWI algorithm calls a custom host-function: `mrb_dataset_stream_read`. The Rust host securely maps the remote SAN file descriptor and streams the 1.5TB dataset across the Atlantic in paginated 1MB windows. The dense wave-equation math executes flawlessly inside the Norwegian MacBook's WASM sandbox using less than 50MB of RAM, bypassing the architectural limitations of WebAssembly via secure, air-gapped host-calls.

---

## A.4 Phase 3: The "PARANOID" Micro-Audit

FWI math is brutally dense. If a UK library desktop with a spinning HDD attempts to run the simulation, swapping memory pages to disk, it will stall the global pipeline.

To filter the weak hardware, the researchers deploy a custom `verify_mode` labeled `PARANOID`. Before the Orchestrator assigns a heavy wave-equation chunk, the `WorkEngine` forces a pre-flight micro-audit. It demands the bidding node allocate a 256MB matrix, execute a rapid deterministic transformation, and return the cryptographic hash within 2500ms.

A cloud-hosted VM with over-provisioned virtual RAM attempts to swap to a cheap SSD. It fails the 2500ms timer. The Orchestrator instantly rejects the node's bid. Only physical, bare-metal hardware with high-speed DDR5 RAM wins the heavy FWI chunks.

---

## A.5 Phase 4: The Thermal Guillotine

A teenager in London leaves their smartphone charging on the bedside table with the academic daemon running. The acoustic wave math is so intense it pushes the mobile ARM processor to 91°C, risking a catastrophic battery fire.

The Marabunta `HardwareMonitor` daemon continuously polls the device sensors. It detects the critical spike and transitions to `ThermalState::Panic`.

The `WorkEngine` iterates over the active sandboxes and fires `sandbox.kill_engine()`. This forcefully increments the Wasmtime epoch, triggering an uncatchable `WasmtimeTrap::Interrupt` from deep outside the synchronous C-level FFI boundary.

The WASM thread is violently physically assassinated. The silicon cools down, preventing physical damage to the citizen's device. The unfinished chunk is gracefully yielded back to the DHT (`SwarmMessage::YieldChunk`) to be computed by a colder node in Brazil.

---

## A.6 Phase 5: The Script Kiddie & The MMX Settlement

A script kiddie in Oslo realizes that the Norwegian government is issuing tax breaks based on Marabunta execution receipts. They write a Python script to spoof the API, instantly returning fake, randomized FWI stability indices without spending a single CPU cycle on the math.

The USP Orchestrator utilizes `MajorityConsensus`. It routes the exact same FWI chunk to the Norwegian script kiddie and an honest Brazilian gaming PC.

The script kiddie returns `0xBAD...`. The gamer returns `0x4F2...`. The hashes mismatch.

The `VerificationEngine` flags the Norwegian node as an outlier. It executes the `slash_node()` protocol: 
1. The node's Kademlia trust score is dropped to `0.0`. 
2. It is permanently branded with `Badge::Malicious` in the global `ReputationStore`. 
3. Its `NodeId` is blacklisted, isolating it from the DHT.

Finally, the `FederationManager` settles the economy. It executes `settle_verified_work()` based on the `agreed_price` inside the `Assignment` struct. The honest Brazilian gamer is credited with MMX tokens in the SQLite ledger, unlocking their in-game cosmetics. The Norwegian script kiddie earns 0 credits, receiving no tax break, and is permanently exiled from the Sovereign Science Fabric.
