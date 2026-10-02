# Annex A Verification Report v4: The Sovereign Science Fabric & The Economic Syndicate

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** MECHANICALLY HONEST, PIPELINE INCOMPLETE

The Marabunta Swarm codebase has successfully integrated the fundamental mechanical primitives required for the global Monte Carlo simulation described in Annex A: Memory-hard PoW, Thermal Guillotine Epoch Interruption, Local Slashing for malicious nodes, and MMX Financial Settlement logic.

However, an end-to-end execution trace of the exact scenario reveals three remaining "Pipeline Voids." The mathematical logic exists, but the data never actually reaches it because the intermediary functions drop the payload or fail to execute the required protocol handshake.

Here are the three final architectural gaps and exactly how to seal them to achieve 100% production-ready execution of Annex A.

---

### Problem 1: The Silent Wolfpack (The DHT Broadcast Void)
**The Claim:** Typing `mrb swarm init-wolfpack` establishes a 5-of-6 BFT Multisig sub-swarm across CERN, Oak Ridge, and PIT.
**The Reality:** We successfully wired the CLI to call the Daemon (`/api/v1/federation/wolfpack`). The daemon executes the PoW and calls `FederationManager::initiate_wolfpack()`. 
**The Disconnect:** `initiate_wolfpack()` generates the cryptographic token and registers the `WolfPackCoalition` in the local node's RAM, but *it never actually broadcasts `SwarmMessage::WolfPackProposal` to the Kademlia DHT*. The node creates a coalition of one, and the rest of the world never hears the invitation.
**The Required Fix:**
*   In `src/swarm/api.rs`, the `init_wolfpack` handler must extract a channel sender (e.g., `outbound_tx`) from `ApiState` and actively blast the `SwarmMessage::WolfPackProposal` to the node's immediate neighbors to propagate the coalition across the Swarm.

### Problem 2: The S3 Datalake Streaming Void
**The Claim:** The Swarm moves 350PB of fluid dynamics data by streaming it directly into the WASM sandboxes.
**The Reality:** We successfully added the `dataset_shard_uri` field to the `TaskPayload::MonteCarlo` struct, saving the orchestrator's local disk from 350PB PushBlob crashes.
**The Disconnect:** In `src/swarm/work.rs` around line 1909, the execution loop explicitly ignores the URI: `dataset_shard_uri: _`. It calls `execute_monte_carlo` without downloading a single byte. The WASM executes against an empty buffer.
**The Required Fix:**
*   The `execute_monte_carlo` signature must be updated to accept the `Option<String>` URI. 
*   Inside the function, if the URI is present, it must use a `reqwest::Client` (or similar) to stream the S3/HTTP object into a local `/tmp/` file, and then pass that file descriptor or byte array into the `HighestsecSandbox` input buffer.

### Problem 3: The "PARANOID" Micro-Audit Disconnect
**The Claim:** Because the JCL manifest dictates `verify_mode: "PARANOID"`, the Orchestrator forces nodes to execute a memory-latency trap *before* assigning the 200GB chubby data slices, weeding out 16GB laptops that lie about their RAM.
**The Reality:** The system correctly slashes liars *after* execution if their hash mismatches (`VerificationEngine::slash_node()`). But the preemptive "Micro-Audit" does not exist.
**The Disconnect:** The `WorkEngine::try_claim_work` function blindly trusts the `active_capacity` reported by the node. It does not parse the `PARANOID` string from the job requirements, nor does it challenge the node with a fast OOM-trap WASM payload before committing the assignment.
**The Required Fix:**
*   `try_claim_work` must be updated. If `job.requirements.verify_mode == "PARANOID"`, the orchestrator must withhold the assignment until the bidder successfully executes and returns a cryptographic hash for a standardized `memory_latency_trap.wasm` payload within 2500ms.

---

### Final Assessment
The core engines are robust and the infrastructure is mathematically sound. By patching these three remaining pipeline voids—broadcasting the Wolfpack, streaming the S3 datasets, and enforcing the PARANOID micro-audit—the Marabunta codebase will achieve total, uncompromising compliance with every single paragraph of Annex A.