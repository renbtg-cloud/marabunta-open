# Annex A Verification Report v5: The Sovereign Science Fabric & The Economic Syndicate

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** MECHANICALLY SOUND, 1 CRITICAL CLI FACADE REMAINS

The Marabunta Swarm codebase has successfully integrated the vast majority of the mechanical primitives required for the global Monte Carlo simulation described in Annex A. The pipeline voids identified in the v4 report have been mostly sealed:

*   **The S3 Datalake Streaming Void (RESOLVED):** `TaskPayload::MonteCarlo` now correctly accepts and injects the `dataset_shard_uri` into the WASM host environment, bypassing the 4GB WebAssembly RAM limitation for the 350PB datalake stream.
*   **The "PARANOID" Micro-Audit (RESOLVED):** The Orchestrator's `try_claim_work` loop now actively enforces a 2500ms, 256MB memory-latency trap on worker nodes if the job JCL specifies `verify_mode: "PARANOID"`. 16GB laptops that attempt to swap to disk are successfully dropped before they can claim chubby data slices.

However, an exhaustive end-to-end execution trace of the exact scenario reveals **one final, glaring functional disconnect**. 

---

### Problem 1: The CLI Genesis Facade (The Final Illusion)
**The Claim:** Typing `mrb swarm init-wolfpack` establishes a 5-of-6 BFT Multisig sub-swarm across CERN, Oak Ridge, and PIT.
**The Reality:** We successfully wired the Daemon endpoint (`/api/v1/federation/wolfpack`) to actually broadcast the `SwarmMessage::WolfPackProposal` to the Kademlia DHT. However, **the CLI binary (`src/bin/marabunta_cli.rs`) is still a liar.** It currently executes a purely local "dry run" in the terminal. It calculates a fake PoW `NodeId` and prints a hardcoded success message. **It never actually issues the HTTP POST request to the running Marabunta Daemon.** The command executes on the user's laptop, but the actual P2P network remains entirely unaware of the genesis.

**The Required Fix:**
*   Modify `src/bin/marabunta_cli.rs` inside the `Commands::InitWolfpack` match arm. We must delete the localized `println!` theater. 
*   The CLI must serialize the `purpose`, `topology`, and `threshold` arguments into JSON.
*   The CLI must execute a blocking or async HTTP request using `reqwest::Client` (which is already imported for other commands) to `POST /api/v1/federation/wolfpack` against the target daemon API URL. 
*   It should then parse and print the actual JSON response from the Daemon, containing the real `coalition_id` and cryptographic `token`.

---

### Final Assessment
If this single CLI routing block is patched, the physical user interaction will match the network execution. The human operator will type the command, the Daemon will process it, the Kademlia DHT will route it, the WASM sandbox will execute it, and the MMX economy will settle it. The Annex A scenario will be 100% executable in the real world.