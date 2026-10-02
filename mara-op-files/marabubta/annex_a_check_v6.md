# Annex A Verification Report v6: The Sovereign Science Fabric & The Economic Syndicate

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** MECHANICALLY SOUND, API & I/O PIPELINE SEVERED

The Marabunta Swarm codebase is architecturally brilliant. The core engines (Memory-Hard PoW, Thermal Guillotines, Local Slashing, and MMX Financial Settlement) are fully operational and mathematically sound.

However, an exhaustive, "all-inclusive" execution trace of the exact global scenario reveals three final, critical **I/O and Pipeline Disconnects**. The engines work, but the data cannot reach them. The system is a high-performance sports car with the fuel line severed and the dashboard unplugged.

Here are the three remaining problems that prevent the Annex A scenario from executing successfully in the real world.

---

### Problem 1: The CLI Genesis Facade (The Final Illusion)
**The Claim:** Typing `mrb swarm init-wolfpack` generates an identity and issues a BFT Multisig sub-swarm generation command to the Daemon.
**The Reality:** We successfully wired the Daemon endpoint (`/api/v1/federation/wolfpack`) to actually broadcast the `SwarmMessage::WolfPackProposal` to the Kademlia DHT. However, **the CLI binary (`src/bin/marabunta_cli.rs`) is still a liar.** It currently executes a purely local "dry run" in the terminal. It calculates a fake PoW `NodeId` and prints a hardcoded success message. **It never actually issues the HTTP POST request to the running Marabunta Daemon.** The command executes on the user's laptop, but the actual P2P network remains entirely unaware of the genesis.
**The Required Fix:**
*   Modify `src/bin/marabunta_cli.rs` inside the `Commands::InitWolfpack` match arm. We must delete the localized `println!` theater. 
*   The CLI must serialize the `purpose`, `topology`, and `threshold` arguments into JSON.
*   The CLI must execute a blocking or async HTTP request using `reqwest::Client` to `POST /api/v1/federation/wolfpack` against the target daemon API URL. 

### Problem 2: The 350PB Data Gravity Illusion (WASI Network Panic)
**The Claim:** The Swarm avoids WASM's 4GB memory limit by streaming 350PB of Monte Carlo fluid dynamics data via `dataset_shard_uri` from an S3 datalake.
**The Reality:** In `src/swarm/work.rs`, we successfully added the `dataset_shard_uri` to the `TaskPayload` and appended the metadata `{"dataset_stream_mounted": "uri"}` to the JSON input for the WebAssembly sandbox. 
**The Disconnect:** The `HighestsecSandbox` runs a strict `wasm32-wasi` environment. We have not granted the WASI host network capabilities, nor have we implemented a host-call interceptor in `MantisJournal` to download the S3 file on behalf of the guest. When the WASM payload attempts to open an HTTP socket to stream the 350PB dataset, Wasmtime will instantly trigger a `WasmtimeTrap` due to missing network capabilities, and the chunk will fail.
**The Required Fix:**
*   The Rust host (`src/swarm/work.rs` or `sandbox.rs`) must physically download the object from the URI via `reqwest`, write it to a local `/tmp/` file, and pass the open file descriptor directly into the WASI context via `add_preopened_dir` or standard `stdin` piping, allowing the guest to stream it locally without network access.

### Problem 3: The Management GUI Vacuum (HTTP 404s)
**The Claim:** Human operators monitor the live Swarm telemetry on a Datadog-tier Management UI.
**The Reality:** We successfully wired the filesystem directory so the `/ui` HTTP route serves the `index.html` frontend from `management-ui/app.js`. And we wired the `/api/v1/psyche/*` backend endpoints.
**The Disconnect:** The `app.js` dashboard heavily relies on polling other endpoints: `/api/v1/capacity`, `/api/v1/alerts`, `/api/v1/sla`, and `/api/v1/constellation`. If we look at the Axum handlers in `src/swarm/api.rs`, these endpoints explicitly return `ApiError::NotFound("management layer not enabled")` if their respective engines are not present in the `ApiState`.
In `src/swarm/mod.rs` (around line 2500), we initialized `api_server.state` but we left `capacity_planner`, `alert_engine`, `sla_monitor`, and `constellation_builder` as `None`! The frontend will load a beautiful UI shell, but the widgets will instantly crash with 404/503 errors.
**The Required Fix:**
*   In `src/swarm/mod.rs`, right after `api_server.state.psyche_calculator = Some(Arc::clone(&self.psyche_calculator));`, we must inject the rest of the management singletons (`alert_engine`, `sla_monitor`, `capacity_planner`, `constellation_builder`, `fleet_manager`, `vision_store`, and `event_bus`) into the `api_server.state` struct so the Axum router can resolve the UI data requests.

---

### Final Assessment
The core computational engines of the Marabunta Swarm are physically ready for global deployment. If these three critical I/O and pipeline gaps are patched, the human operator will be able to issue commands, the WASM sandbox will stream petabytes of data, and the UI will reflect the thermodynamic chaos in real-time.