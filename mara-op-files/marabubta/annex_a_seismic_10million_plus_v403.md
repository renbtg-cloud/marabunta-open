# Physical Execution Audit: ANNEX A at 10 Million+ Scale (v4.0.3)

After repairing the Orchestrator's submission logic, Kademlia chunk routing, and preventing the 20-Terabyte `TaskPayload` memory bomb, the 1,000,000 chunks for the 350PB Monte Carlo simulation can finally be dispatched into the swarm. 

However, the actual execution and verification lifecycles are critically compromised. The system contains severe stubs and thread-leaking logic that will cause physics nodes to melt, the datalake to output garbage, and the verification engine to leak RAM.

## 1. The Async Deadlock & Thermal Guillotine Illusion
**Location:** `src/swarm/work.rs` & `src/highestsec/sandbox.rs`
Prior to v4.0.3, `HighestsecSandbox::execute` (a synchronous, blocking CPU-bound Wasmtime loop) ran directly on the tokio async worker pool. A small node pulling 4 concurrent chunks would instantly deadlock its entire network stack, freezing Kademlia routing and WebSockets.
While we have now wrapped this in `tokio::task::spawn_blocking`, this exposes the **Thermal Guillotine Illusion**. When the daemon detects a 90°C thermal runaway, it calls `.abort()` on the tokio `JoinHandle`. However, aborting a `spawn_blocking` task only detaches it; the underlying OS thread continues running the tight WASM loop indefinitely. The required interrupt, `sandbox.kill_engine()` (which advances the Wasmtime epoch), is **never invoked anywhere in the codebase**. The CPU will continue to burn, completely ignoring the Thermal Guillotine.

## 2. The 1.5TB Datalake Bypass is a 21-Byte Stub
**Location:** `src/swarm/work.rs` (`execute_monte_carlo`)
Annex A states that heavy FWI workloads bypass the WASM 4GB linear memory limit by securely streaming remote S3 datalakes directly into a local file descriptor via `mrb_dataset_stream_read`.
**Reality:** The actual streaming logic is a mocked stub. When an S3 URI is detected, the code manually creates `/tmp/marabunta_datalake_{uuid}.bin` and writes exactly 21 bytes into it: `b"DUMMY_DATASET_PAYLOAD"`. The 10-million-node Swarm is physically executing the 350-Petabyte wave-equation Monte Carlo simulation against a static 21-byte string, silently returning useless mathematical garbage back to CERN.

## 3. The CGNAT / UDP Transport Facade (Brazil Gamer Grid)
**Location:** `src/swarm/hyperscale/nat_traversal.rs` & `src/swarm/transport/mod.rs`
Annex A explicitly relies on 5 million Brazilian residential nodes bypassing Symmetric Carrier-Grade NATs using WebRTC ICE UDP hole punching.
**Reality:** While the ICE candidates are gathered and gossiped, `establish_peer_connection()` is entirely fake—it returns `Ok` without ever opening a UDP socket. Furthermore, the core `SwarmTransport` stack is hardcoded entirely to `tokio::net::TcpSocket`. The transport layer has no capability to dial UDP hole-punched coordinates. All 5 million Brazilian nodes remain permanently isolated from P2P chunk distribution.

## 4. The Verification RAM Leak (`purge_job` Dead Code)
**Location:** `src/swarm/verification.rs`
When 1,000,000 chunks complete their "PARANOID" micro-audits and redundant execution, the results flow into the `VerificationEngine`. `add_record` and `add_statistical_sample` aggregate millions of data points across the swarm.
**Reality:** The required cleanup method, `pub fn purge_job()`, is completely dead code. It is never invoked by the `FederationManager`, the `WorkEngine`, or the Orchestrator after a job is successfully settled. The Orchestrator's verification engine will continuously bleed gigabytes of RAM by indefinitely holding every statistical verification trace from every completed job.

---
**Conclusion:** Annex A's "Sovereign Science Fabric" remains a high-level facade. While the distributed routing mechanisms and database queues are functional, the physical workload execution layers (Thermal Preemption, Datalake I/O, UDP Mesh, and RAM cleanup) are stubbed out prototypes.