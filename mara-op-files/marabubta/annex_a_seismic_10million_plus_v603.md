# Annex A (v603): Planetary-Scale Execution Analysis
## The 10-Million-Node 350PB Seismic Wave-Equation Workload

### Abstract
Following the resolution of the WASM Bootstorm Starvation and the injection of the Settlement Plane triggers, a final state-machine trace was performed on the Orchestrator and Kademlia Inbound components.
Two catastrophic logic bugs were discovered that cause the job to permanently hang at 99.99% and the entire financial settlement system to act as a silent black hole.

### Apocalyptic Bottlenecks Discovered

#### 1. The Financial Black Hole (Missing Ledger Ingress)
**Location:** `src/swarm/mod.rs` (SwarmMessage Kademlia Inbound Loop)
**The Problem:** In v602, we correctly configured the Edge nodes to evaluate the `job.orchestration.settlement` block and emit a `SwarmMessage::SettlementClaim` containing their Zero-Knowledge Proofs and fuel burn data to the designated Ledger nodes (e.g., the Norwegian sub-swarm).
However, the Ledger nodes **have no inbound handler** for `SwarmMessage::SettlementClaim`. The Kademlia inbound thread hits the `_ => {}` catch-all match arm and silently drops the payment request into the void. 10 million nodes will compute the job, emit their invoices, and the Ledger will silently delete them.

#### 2. The 99.99% Zeno Paradox (Job Completion Deadlock)
**Location:** `src/swarm/work.rs` & `src/swarm/knowledge.rs` (`update_job_progress`)
**The Problem:** When an edge node completes a chunk, it calls `engine_knowledge.update_job_progress(&chunk.job_id, 1, 0)`.
However, looking at `knowledge.rs:850`, `update_job_progress` is implemented as an absolute max setter (`if completed > entry.chunks_completed`), not an incrementer (`+=`). Because `work.rs` passes `1` instead of `current + 1`, the job`s `chunks_completed` counter will be permanently stuck at `1`.
Furthermore, even if it successfully counted to 1,000,000, there is zero logic in the Orchestrator to check if `chunks_completed == chunks_total` and physically transition the `SwarmJobStatus` from `InProgress` to `Completed`. The 350PB job will finish physically, but the User/API will forever see it as `InProgress`.

---

### Required Architectural Patches (Do we fix these?)

1. **Ledger Ingress Wire-up (The Black Hole Fix):** Add the `SwarmMessage::SettlementClaim` handler to `src/swarm/mod.rs`. It must verify the ZKP (or queue it for async verification) and credit the node`s account/reputation via the `MarketplaceEngine` or `Ledger`.
2. **Atomic Job Finalization (The Zeno Paradox Fix):** 
   - Change `update_job_progress` in `work.rs` to fetch the current job stats, atomically increment them, and call `update_job_progress` with the correct absolute totals.
   - Add a check: `if job.chunks_completed >= job.chunks_total`, mutate the status to `Completed` and log the final Global Job End metric.
