# VOLUME 4: ENTERPRISE SOVEREIGNTY

## 4.1 The Corporate Intranet Illusion

Standard peer-to-peer (P2P) architectures assume a flat, universally routable public internet. When executing workloads across a midsize industry or university—where 99% of nodes reside behind strict corporate firewalls with zero inbound or outbound WAN access—traditional Kademlia DHTs instantly fail. 

A sub-swarm of 500 air-gapped office workstations cannot download massive datasets from AWS S3, nor can they route computed results back to a global orchestrator. The network segments shatter into isolated enclaves.

To seamlessly integrate these high-density clusters, Marabunta introduces **Hierarchical Membrane Routing**.

## 4.2 The Egress Diode (Air-Gapped Proxying)

A designated edge node with authorized WAN access (e.g., an IT administrator's workstation) is promoted to a **Membrane Gateway**. This node acts as an **Egress Diode**, establishing an absolute one-way proxy between the corporate DMZ and the global Swarm.

When air-gapped internal workers attempt to stream the 1.5TB datalake via the Zero-Footprint WASI host-function, the Membrane Gateway intercepts the traffic. It natively spins up a lightweight HTTP Forward Proxy on a designated port.

Let $B_{wan}$ be the external bandwidth of the Gateway, and $\sum B_{lan}$ be the aggregate consumption of the internal workers. The Gateway serves as a localized caching funnel, satisfying the condition:

$$ \sum B_{lan} \le B_{wan} 	imes Cache_{hit\_rate} $$

Internal nodes safely pull global state without ever exposing the corporate LAN to external internet routing tables.

```bash
# Illustrative: Promoting a node to a Membrane Gateway
$ mrb network promote --role gateway --internal 192.168.1.100:9001
```

## 4.3 Hierarchical Back-Routing

When the internal corporate aggregator completes stitching together a 50MB wave-equation result, it cannot open a direct TCP socket to the global USP Orchestrator ($O$) due to firewall restrictions (`EHOSTUNREACH`). 

Instead, the internal aggregator ($n_i$) encapsulates the payload into an `EgressRelay` message. The Membrane Gateway ($G_m$) unwraps this envelope, cryptographically signs it with its own identity, and forwards it to the global mesh. The routing trajectory becomes a Directed Acyclic Graph (DAG):

$$ Route(n_i 	o O) = (n_i \xrightarrow{	ext{LAN}} G_m) \land (G_m \xrightarrow{	ext{WAN}} O) $$

This allows the Swarm to dynamically nest infinite layers of isolated sub-swarms, aggregating petabytes of compute power without compromising enterprise security postures.

```http
// Illustrative: Membrane Gateway relay telemetry
GET /api/v1/membrane/telemetry
{
  "active_tunnels": 499,
  "bytes_exfiltrated": 52428800,
  "dropped_packets": 0
}
```

# VOLUME 5: THE CRYPTOGRAPHIC ECONOMY

## 5.1 The Ledger of Sovereignty

A distributed execution engine requires an underlying economic mechanism to incentivize the donation of idle compute cycles. However, issuing digital credits using localized databases is inherently insecure. A malicious actor with filesystem access could arbitrarily modify their local SQLite ledger to inflate their balance.

Marabunta mathematically anchors its micro-credit system (MMX) to the cryptographic execution trace of the physics solver.

## 5.2 Ed25519 Replay Immunity

To prevent database tampering and receipt forgery, every MMX settlement is explicitly signed by the Orchestrator's `NodeIdentity`. 

To prevent Replay Attacks—where a worker exports a valid 10 MMX receipt and duplicates it 1,000 times to inflate their tax deduction—the Orchestrator injects a strictly monotonic UNIX timestamp into the signature payload. The signature $\sigma$ is an `Ed25519` function over the concatenated byte-array of the chunk ID ($C_{id}$), worker ID ($W_{id}$), credit amount ($A$), and timestamp ($T$):

$$ \sigma = Sign_{Ed25519}\Big(sk_{node},\; H_{Blake3}(C_{id} \parallel W_{id} \parallel A \parallel T)\Big) $$

When a tax authority or corporate auditor verifies the exported CSV, the cryptographic script guarantees that every single row is mathematically distinct and bound to a specific quantum of time.

```bash
# Illustrative: Exporting verifiable MMX cryptographic receipts
$ mrb economy export --format csv --signed
```

## 5.3 The 10M TPS Wall (SQLite Batching)

While SQLite provides robust local storage, a regional hub settling thousands of transactions per second for a 10-million node swarm will instantly saturate the host operating system's thread pool, deadlocking the daemon due to file locks.

Marabunta bypasses physical NVMe IOPS limits by enforcing `PRAGMA journal_mode=WAL` (Write-Ahead Logging) and `wal_autocheckpoint=1000`. Settlement requests are pushed into a lock-free MPSC (Multi-Producer, Single-Consumer) background channel. A dedicated writer thread opportunistically batches up to 100 settlements into a single atomic SQLite transaction. This transforms 10,000 blocking disk I/O operations into 100 streamlined sequential writes, achieving massive throughput without async starvation.

# ANNEX A: THE DEVELOPER EXPERIENCE

To ensure frictionless adoption by enterprise engineering teams, Marabunta provides a comprehensive, cloud-native developer experience.

*   **The Compiler Toolchain:** Natively transpiles C++, Rust, and Python payloads into optimized WebAssembly (`wasm32-wasi`) binaries, automatically injecting the required Marabunta memory bounds and streaming host-functions.
*   **IDE Integration:** Dedicated VSCode and IntelliJ plugins allow researchers to execute `O(1)` local sandbox testing and push payloads directly to the Kademlia DHT from their editor.
*   **CI/CD Pipelines:** Native GitHub Actions and GitLab CI integrations for automated job submission, deterministic execution validation, and asynchronous artifact fetching.

```bash
# Illustrative: Compiling and deploying a job via the CLI toolchain
$ mrb build ./fwi_solver --target wasm32-wasi
$ mrb deploy ./fwi_solver.wasm --priority high
```

# ANNEX B: THE SOVEREIGN ANALYTICS ENGINE

Operating a swarm of 10 million nodes requires unprecedented observability. The Marabunta Management Dashboard (The Panopticon) visualizes macroscopic network physics without crashing the observer's hardware.

*   **Lazy DOM Rendering:** The backend utilizes lazy Kademlia iterators, while the frontend employs strict 500-element FIFO ring-buffers. The UI guarantees a flat memory footprint, preventing browser Out-Of-Memory (OOM) crashes even when processing 5,000 telemetry events per second.
*   **Constellation Telemetry:** WebGL and D3.js force-directed graphs render the topology of macro-coalitions and shifting economic treaties in real-time, aggressively culling edge matrices to preserve GPU rasterizers.
*   **Ledger Auditing:** Real-time visibility into the MMX SQLite database via paginated endpoints, providing instantaneous compliance and economic tracking.

# ANNEX C: PLANETARY DEPLOYMENT

Marabunta is designed as a frictionless contagion, capable of organically infiltrating diverse infrastructures.

*   **Bare Metal & Residential:** Lightweight, zero-dependency binaries packaged as `.msi`, `.deb`, and `.rpm` for seamless installation on consumer gaming PCs and library desktops.
*   **Academic Grids:** Multi-architecture containerization (Docker, Podman) for effortless scaling across university HPC clusters.
*   **Enterprise Kubernetes:** Native Helm charts and Kubernetes Operators for highly available, auto-updating deployments within corporate DMZs.

# ANNEX D: WAR GAMES

Marabunta is mathematically hardened against the physical realities of the public internet. Our architecture is empirically validated through continuous Chaos Engineering.

*   **The Mantis Flight Data Recorder:** Constant WASI snapshotting enables deterministic Instruction-Set Replay. If a node in Brazil crashes during a massive simulation, the exact CPU state and memory boundaries can be perfectly reconstructed and debugged offline.
*   **Network Partitions:** We routinely inject synthetic latency, simulated BGP hijacks, and split-brain geographic partitions into the control plane to mathematically prove the Swarm's ability to self-heal and re-route without centralized coordination.

```bash
# Illustrative: Injecting synthetic split-brain chaos into the local node
$ mrb chaos inject partition --duration 300s --drop-rate 0.99
```

# ANNEX E: COMING SOON...

*   **Distributed DiLoCo (LLM Training):** Being tested.
*   **Sealed Computing (FHE):** Being developed.
*   **IBM-CICS (COBOL, Assembly etc. w/ 2-phase commit):** Being tested.