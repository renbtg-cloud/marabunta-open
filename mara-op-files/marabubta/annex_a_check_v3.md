# Annex A Verification Report v3: The Sovereign Science Fabric & The Economic Syndicate

**Target Document:** `marabunta_bible/14_annex_a.md`
**Status:** ARCHITECTURALLY HONEST, FUNCTIONALLY DISCONNECTED

The Marabunta Swarm codebase is structurally sound. The backend logic for memory-hard Proof of Work, Thermal Guillotines, Local Slashing, and Thermodynamic Arbitrage (MMX Bidding) has all been wired into the core `WorkEngine` and `VerificationEngine`.

However, an exhaustive end-to-end execution trace reveals three critical **Functional Disconnects**. The backend logic works perfectly, but the system fails to trigger or deliver the results because the front-facing "pipes" (CLI, Execution Engine, Web Server) are either stubbed or ignoring the payload.

Here are the three remaining problems and how to fix them to achieve 100% real-world execution.

---

### Problem 1: The CLI Genesis Disconnect (Still a Facade)
**The Claim:** Typing `mrb swarm init-wolfpack` generates an identity and issues a BFT Multisig sub-swarm generation command to the Daemon.
**The Reality:** We successfully wired the Daemon endpoint (`/api/v1/federation/wolfpack`) in a previous fix. However, the `marabunta-cli` binary (`src/bin/marabunta_cli.rs`) still executes a purely local "dry run". It calculates the PoW `NodeId` but then immediately executes a series of `println!` statements and exits. It completely fails to use `reqwest::Client` to POST the payload to the running Marabunta Daemon.
**The Required Fix:**
*   Modify `src/bin/marabunta_cli.rs` inside the `Commands::InitWolfpack` match arm. Instead of printing fake IDs, it must serialize the `purpose`, `topology`, and `threshold` into JSON and execute a `client.post(&format!("{}/api/v1/federation/wolfpack", cli.api_url))` command to actually trigger the daemon.

### Problem 2: The 350PB Streaming Void (Ignored URIs)
**The Claim:** The Swarm avoids WASM's 4GB memory limit by streaming 350PB of Monte Carlo fluid dynamics data via `dataset_shard_uri` from an S3 datalake.
**The Reality:** We added `dataset_shard_uri` to the `TaskPayload` enum so the network can transmit the reference. However, inside `src/swarm/work.rs`, the execution loop explicitly ignores the data. The pattern match reads: `TaskPayload::MonteCarlo { seed, iterations, params, dataset_shard_uri: _ }`. The WASM sandbox spins up and runs the algorithm without ever downloading or processing a single byte of external data.
**The Required Fix:**
*   Before calling `execute_monte_carlo`, the `WorkEngine` must actually resolve the URI. It should asynchronously download the dataset (or stream it into a `/tmp/` file) and pass the physical byte array (or file descriptor) into the `HighestsecSandbox` as an `input` buffer.

### Problem 3: The Management GUI Blackout
**The Claim:** Human operators can monitor the Psyche telemetry and thermal chaos on a Datadog-tier Management UI.
**The Reality:** We successfully wired the `/api/v1/psyche/*` backend endpoints. But the actual HTML/JS files for the dashboard (`management-ui/app.js`) are inaccessible. The `/ui` HTTP route in `src/swarm/api.rs` relies on `state.management_ui_dir`. But in `src/swarm/mod.rs`, `api_server.state.management_ui_dir` is permanently initialized to `None` and is never updated with the path to the physical UI folder. Browsing to the dashboard instantly returns an HTTP 404 "management UI not configured" error. The UI is locked out.
**The Required Fix:**
*   In `src/swarm/mod.rs` (around line 2580), right before `tokio::spawn(async move { api_server.run().await })`, we must inject the local filesystem path: `api_server.state.management_ui_dir = Some(std::env::current_dir().unwrap().join("management-ui").to_string_lossy().to_string());`

---

### Final Assessment
We are inches from the finish line. The backend engines are mathematically sound. By patching these three "plumbing" disconnects, the user's terminal commands will successfully reach the daemon, the daemon will actually process the massive datalake streams, and the operator will finally be able to see the live chaos on the Management Dashboard.